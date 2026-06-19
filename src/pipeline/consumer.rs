use std::sync::Arc;

use super::curl::build_curl;
use super::decode::decode_body;
use super::AnalysisMessage;
use crate::endpoint::EndpointMatcher;
use crate::storage::{RawSample, SampleStore};
use crate::upstream::client::UpstreamResponse;
use chrono::Utc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

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
            // Safety net: the proxy handler already skips fan-out for unregistered
            // endpoints, but this keeps sampling bounded to configured endpoints.
            return;
        };
        let baseline_status = msg.baseline_response.status;

        // The baseline body must be readable JSON within the size cap, or the
        // sample carries nothing to diff later — skip it entirely (as before).
        let Some(baseline_body) =
            decoded_json_text(&msg.baseline_response, self.max_body_bytes).await
        else {
            tracing::warn!(
                endpoint = %endpoint,
                "skipping sample: baseline body is not storable json (non-json or over size cap)"
            );
            return;
        };

        let (candidate_status, candidate_body) = upstream_body(
            &msg.candidate_response,
            baseline_status,
            self.max_body_bytes,
        )
        .await;
        let (control_status, control_body) =
            upstream_body(&msg.control_response, baseline_status, self.max_body_bytes).await;

        let sample = RawSample {
            endpoint: endpoint.clone(),
            timestamp: Utc::now(),
            baseline_status,
            baseline_body,
            candidate_status,
            candidate_body,
            control_status,
            control_body,
            request_curl: msg.request.as_ref().map(build_curl),
        };

        if let Err(e) = self.store.append_sample(&sample).await {
            tracing::warn!(error = %e, "failed to append raw sample");
            return;
        }

        crate::pipeline::metrics::record_sample_stored(&endpoint, msg.received_at.elapsed());
    }
}

/// Bodies are only stored for an upstream that answered baseline's status — those
/// are exactly the cases the read-time diff compares. A different status is the
/// signal itself (recorded at read time), so its body is not stored; a failed
/// upstream yields `(None, None)`.
async fn upstream_body(
    response: &Option<UpstreamResponse>,
    baseline_status: u16,
    max_body_bytes: usize,
) -> (Option<u16>, Option<String>) {
    match response {
        None => (None, None),
        Some(r) if r.status == baseline_status => {
            (Some(r.status), decoded_json_text(r, max_body_bytes).await)
        }
        Some(r) => (Some(r.status), None),
    }
}

async fn decoded_json_text(response: &UpstreamResponse, max_body_bytes: usize) -> Option<String> {
    let decoded = decode_body(response).await?;
    if decoded.len() > max_body_bytes {
        tracing::debug!(
            len = decoded.len(),
            max = max_body_bytes,
            "skipping body over size cap"
        );
        return None;
    }
    if serde_json::from_slice::<serde::de::IgnoredAny>(&decoded).is_err() {
        tracing::debug!("skipping non-json body");
        return None;
    }
    // Validated as JSON above, so the bytes are valid UTF-8.
    String::from_utf8(decoded.into_owned()).ok()
}
