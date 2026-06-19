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

/// A snapshot of the originating request, captured only when the matched
/// endpoint enables `capture_request_curl`. Assembled in the proxy handler's
/// background task (never on the hot path) and rendered into a replayable curl
/// command by the consumer when a diff is recorded.
#[derive(Debug, Clone)]
pub struct RequestSnapshot {
    pub method: Method,
    /// Path plus query string, used verbatim in the curl URL.
    pub path_and_query: String,
    pub headers: HeaderMap,
    pub body: Bytes,
    /// Redact credential header values (`!store_credentials_header`).
    pub redact_credentials: bool,
}

/// Everything the analysis pipeline needs about one proxied request.
/// Produced by the proxy handler's background task, consumed by `Consumer`.
pub struct AnalysisMessage {
    /// Raw request path; endpoint resolution happens in the consumer,
    /// off the proxy hot path.
    pub path: String,
    /// When the proxy received the request — measures pipeline lag.
    pub received_at: std::time::Instant,
    pub baseline_response: UpstreamResponse,
    pub candidate_response: Option<UpstreamResponse>,
    pub control_response: Option<UpstreamResponse>,
    /// Present only when the endpoint enabled request capture; rendered into a
    /// curl command by the consumer for diffs that are stored.
    pub request: Option<RequestSnapshot>,
}

/// Create the bounded proxy → consumer channel. `capacity` is configurable via
/// `pipeline.channel-capacity`; a full channel sheds the newest message with a
/// warning rather than queueing unbounded.
pub fn channel(
    capacity: usize,
) -> (
    mpsc::Sender<AnalysisMessage>,
    mpsc::Receiver<AnalysisMessage>,
) {
    mpsc::channel(capacity)
}
