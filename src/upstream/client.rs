use super::error::UpstreamError;
use super::header::is_hop_by_hop;
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

        let url = format!("{}{path}", super::normalize_base(target));

        let mut builder = self.client.request(method.clone(), &url);

        for (name, value) in headers.iter() {
            if is_hop_by_hop(name.as_str()) {
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
            // Hop-by-hop headers include transfer-encoding: the body is buffered, so
            // dropping it keeps the relayed content-length authoritative.
            if is_hop_by_hop(name.as_str()) {
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
}
