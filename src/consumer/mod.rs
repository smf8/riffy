use std::sync::Arc;

use crate::endpoint::EndpointMatcher;
use crate::storage::SampleStore;
use crate::upstream::client::UpstreamResponse;
use axum::http::{HeaderMap, Method};
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub mod curl;
pub mod metrics;
mod sample;

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
    // Raw path; endpoint resolution runs in the consumer, off the hot path.
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

pub struct Consumer {
    rx: mpsc::Receiver<AnalysisMessage>,
    matcher: Arc<EndpointMatcher>,
    store: Arc<dyn SampleStore>,
    max_body_bytes: usize,
}

impl Consumer {
    pub fn new(
        rx: mpsc::Receiver<AnalysisMessage>,
        matcher: Arc<EndpointMatcher>,
        store: Arc<dyn SampleStore>,
        max_body_bytes: usize,
    ) -> Self {
        Self {
            rx,
            matcher,
            store,
            max_body_bytes,
        }
    }

    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(self.run())
    }

    async fn run(mut self) {
        while let Some(msg) = self.rx.recv().await {
            self.handle(msg).await;
        }
        tracing::info!("analysis consumer stopped");
    }

    async fn handle(&self, msg: AnalysisMessage) {
        let Some(endpoint) = self.matcher.resolve(&msg.path) else {
            // The proxy already skips fan-out for unregistered endpoints; this just
            // keeps the consumer bounded to configured ones.
            return;
        };

        let Some(sample) = sample::assemble(&endpoint, &msg, self.max_body_bytes).await else {
            return;
        };

        if let Err(e) = self.store.append_sample(&sample).await {
            tracing::warn!(error = %e, "failed to append raw sample");
            return;
        }

        metrics::record_sample_stored(&endpoint, msg.received_at.elapsed());
    }
}
