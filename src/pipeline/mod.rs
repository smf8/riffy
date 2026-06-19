use crate::upstream::client::UpstreamResponse;
use axum::http::{HeaderMap, Method};
use bytes::Bytes;
use tokio::sync::mpsc;

pub mod consumer;
pub mod curl;
mod decode;
pub mod metrics;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone)]
pub struct RequestSnapshot {
    pub method: Method,
    pub path_and_query: String,
    pub headers: HeaderMap,
    pub body: Bytes,
    pub redact_credentials: bool,
}

pub struct AnalysisMessage {
    // Endpoint resolution happens in the consumer, off the proxy hot path.
    pub path: String,
    pub received_at: std::time::Instant,
    pub baseline_response: UpstreamResponse,
    pub candidate_response: Option<UpstreamResponse>,
    pub control_response: Option<UpstreamResponse>,
    pub request: Option<RequestSnapshot>,
}

pub fn channel(
    capacity: usize,
) -> (
    mpsc::Sender<AnalysisMessage>,
    mpsc::Receiver<AnalysisMessage>,
) {
    mpsc::channel(capacity)
}
