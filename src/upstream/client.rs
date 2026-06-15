use super::error::UpstreamError;
use axum::http::{HeaderMap, Method};
use bytes::Bytes;
use reqwest::Client;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct UpstreamResponse {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: Bytes,
}

pub struct UpstreamClient {
    client: Client,
    pub baseline: String,
    pub control: String,
    pub candidate: String,
    pub timeout: Duration,
}

/// RFC 2616 §13.5.1 hop-by-hop headers — must not be forwarded by proxies.
const HOP_BY_HOP_HEADERS: &[&str] = &[
    "connection",
    "keep-alive",
    "transfer-encoding",
    "te",
    "upgrade",
];

impl UpstreamClient {
    pub fn new(baseline: String, control: String, candidate: String, timeout: Duration) -> Self {
        // Upstream targets are direct (in-cluster) services; never route them
        // through HTTP_PROXY/HTTPS_PROXY from the environment.
        let client = Client::builder()
            .timeout(timeout)
            .pool_max_idle_per_host(2)
            .no_proxy()
            .build()
            .expect("failed to build reqwest client");

        Self {
            client,
            baseline,
            control,
            candidate,
            timeout,
        }
    }

    #[tracing::instrument(skip(self, headers, body), fields(target, method = %method, path = %path))]
    pub async fn send(
        &self,
        target: &str,
        method: &Method,
        path: &str,
        headers: &HeaderMap,
        body: Bytes,
    ) -> Result<UpstreamResponse, UpstreamError> {
        tracing::Span::current().record("target", target);

        // The scheme is derived from the address: an explicit `http://` /
        // `https://` prefix is honored, otherwise `http://` is assumed.
        let url = if target.contains("://") {
            format!("{target}{path}")
        } else {
            format!("http://{target}{path}")
        };

        let mut builder = self.client.request(method.clone(), &url);

        // Forward all headers except hop-by-hop
        for (name, value) in headers.iter() {
            if HOP_BY_HOP_HEADERS.contains(&name.as_str()) {
                continue;
            }
            builder = builder.header(name, value);
        }

        builder = builder.body(body);

        let resp = builder.send().await.map_err(|e| {
            if e.is_timeout() {
                UpstreamError::timeout(target, e)
            } else {
                UpstreamError::connection(target, e)
            }
        })?;

        let status = resp.status().as_u16();
        let mut resp_headers = HeaderMap::new();
        for (name, value) in resp.headers().iter() {
            // Only strip transfer-encoding from response (we buffer the body).
            // Keep content-length as-is — no decompression, so it should match.
            if name.as_str() == "transfer-encoding" {
                continue;
            }

            if HOP_BY_HOP_HEADERS.contains(&name.as_str()) {
                continue;
            }

            resp_headers.insert(name, value.clone());
        }
        let body = resp
            .bytes()
            .await
            .map_err(|e| UpstreamError::connection(target, e))?;

        tracing::debug!(status, body_len = body.len(), target = %target ,"upstream response received");

        Ok(UpstreamResponse {
            status,
            headers: resp_headers,
            body,
        })
    }

    pub fn targets(&self) -> [&str; 3] {
        [&self.baseline, &self.candidate, &self.control]
    }
}
