use super::error::ProxyError;
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

#[allow(dead_code)]
pub struct UpstreamClient {
    client: Client,
    pub primary: String,
    pub secondary: String,
    pub candidate: String,
    pub protocol: String,
    #[allow(dead_code)]
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
    pub fn new(
        primary: String,
        secondary: String,
        candidate: String,
        protocol: String,
        timeout: Duration,
    ) -> Self {
        let client = Client::builder()
            .timeout(timeout)
            .pool_max_idle_per_host(2)
            .build()
            .expect("failed to build reqwest client");

        Self {
            client,
            primary,
            secondary,
            candidate,
            protocol,
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
    ) -> Result<UpstreamResponse, ProxyError> {
        tracing::Span::current().record("target", target);

        let url = format!("{}://{}{}", self.protocol, target, path);

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
                ProxyError::timeout(target, e)
            } else {
                ProxyError::connection(target, e)
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
            .map_err(|e| ProxyError::connection(target, e))?;

        tracing::debug!(status, body_len = body.len(), target = %target ,"upstream response received");

        Ok(UpstreamResponse {
            status,
            headers: resp_headers,
            body,
        })
    }

    #[allow(dead_code)]
    pub fn targets(&self) -> [&str; 3] {
        [&self.primary, &self.candidate, &self.secondary]
    }
}
