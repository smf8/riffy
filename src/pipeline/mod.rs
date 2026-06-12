use crate::proxy::upstream::UpstreamResponse;
use tokio::sync::mpsc;

pub mod consumer;
mod decode;

#[cfg(test)]
mod tests;

/// Bounded capacity of the proxy → consumer channel. When the consumer falls
/// behind, new entries are dropped with a warning (backpressure by shedding).
pub const ANALYSIS_CHANNEL_CAPACITY: usize = 1024;

/// Everything the analysis pipeline needs about one proxied request.
/// Produced by the proxy handler's background task, consumed by `Consumer`.
pub struct AnalysisMessage {
    /// Raw request path; endpoint resolution happens in the consumer,
    /// off the proxy hot path.
    pub path: String,
    /// When the proxy received the request — measures pipeline lag.
    pub received_at: std::time::Instant,
    pub primary_response: UpstreamResponse,
    pub candidate_response: Option<UpstreamResponse>,
    pub secondary_response: Option<UpstreamResponse>,
}

pub fn channel() -> (
    mpsc::Sender<AnalysisMessage>,
    mpsc::Receiver<AnalysisMessage>,
) {
    mpsc::channel(ANALYSIS_CHANNEL_CAPACITY)
}
