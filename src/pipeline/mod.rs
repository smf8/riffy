use crate::upstream::client::UpstreamResponse;
use tokio::sync::mpsc;

pub mod consumer;
mod decode;

#[cfg(test)]
mod tests;

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
