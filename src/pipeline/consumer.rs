use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use super::curl::build_curl;
use super::decode::decode_body;
use super::AnalysisMessage;
use crate::analysis::counters::LiveCounters;
use crate::analysis::suppress::EndpointSuppressPaths;
use crate::compare::flatten::{flatten_value, DiffType, FieldDiff, STATUS_FIELD};
use crate::endpoint::EndpointMatcher;
use crate::storage::{DiffEntry, DiffStore};
use crate::upstream::client::UpstreamResponse;
use chrono::Utc;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

/// Single-task consumer of the analysis channel: resolves endpoints, diffs
/// the response triplet, updates the in-memory counter buffer, appends
/// per-request diff entries to the store, and periodically flushes the raw
/// counts to the store. Regression classification happens at read time, not
/// here.
pub struct Consumer {
    rx: mpsc::Receiver<AnalysisMessage>,
    matcher: Arc<EndpointMatcher>,
    collector: Arc<LiveCounters>,
    store: Arc<dyn DiffStore>,
    aggregation_interval: Duration,
    suppress: Arc<EndpointSuppressPaths>,
}

impl Consumer {
    pub fn new(
        rx: mpsc::Receiver<AnalysisMessage>,
        matcher: Arc<EndpointMatcher>,
        collector: Arc<LiveCounters>,
        store: Arc<dyn DiffStore>,
        aggregation_interval: Duration,
        suppress: Arc<EndpointSuppressPaths>,
    ) -> Self {
        Self {
            rx,
            matcher,
            collector,
            store,
            aggregation_interval,
            suppress,
        }
    }

    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(self.run())
    }

    async fn run(mut self) {
        let mut ticker = tokio::time::interval(self.aggregation_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                maybe_msg = self.rx.recv() => match maybe_msg {
                    Some(msg) => self.handle(msg).await,
                    None => break,
                },
                _ = ticker.tick() => self.flush_aggregation().await,
            }
        }

        // Channel closed (shutdown): flush one final snapshot before exiting.
        self.flush_aggregation().await;
        tracing::info!("analysis consumer stopped");
    }

    async fn handle(&self, msg: AnalysisMessage) {
        let Some(endpoint) = self.matcher.resolve(&msg.path) else {
            // Unregistered endpoint — the handler already skips fan-out for
            // these; this is a safety net so analysis stays endpoint-bounded.
            return;
        };
        let baseline_status = msg.baseline_response.status;

        let Some(baseline) = parse_json_body(&msg.baseline_response).await else {
            tracing::warn!(
                endpoint = %endpoint,
                "skipping analysis: baseline response body is not readable json"
            );
            return;
        };

        // Statuses are checked before bodies: a body is only compared when
        // its upstream answered with the same status as baseline. A different
        // status is itself the regression signal and is reported directly.
        let mut raw_diffs = diff_against(&baseline, baseline_status, &msg.candidate_response).await;
        let mut noise_diffs = diff_against(&baseline, baseline_status, &msg.control_response).await;

        self.suppress.suppress(&endpoint, &mut raw_diffs);
        self.suppress.suppress(&endpoint, &mut noise_diffs);

        self.collector.record(&endpoint, &raw_diffs, &noise_diffs);

        let candidate_status = msg.candidate_response.as_ref().map(|r| r.status);
        let control_status = msg.control_response.as_ref().map(|r| r.status);

        // Identical responses produce no entry — only the endpoint total moves.
        // A status mismatch surfaces as a `STATUS_FIELD` diff (see `diff_against`),
        // so the emptiness check already covers it.
        if raw_diffs.is_empty() && noise_diffs.is_empty() {
            return;
        }

        let entry = DiffEntry {
            endpoint,
            timestamp: Utc::now(),
            raw_fields: raw_diffs,
            noise_fields: noise_diffs,
            baseline_status,
            candidate_status,
            control_status,
            // Rendered here (off the hot path) only for diffs we actually store,
            // and only when the endpoint enabled capture.
            request_curl: msg.request.as_ref().map(build_curl),
        };

        // Store failures are non-fatal: log and keep consuming.
        if let Err(e) = self.store.append_diff(&entry).await {
            tracing::warn!(error = %e, "failed to append diff entry");
        }

        crate::pipeline::metrics::record_diff_published(
            &entry.endpoint,
            entry.raw_fields.len(),
            entry.noise_fields.len(),
            msg.received_at.elapsed(),
        );
    }

    async fn flush_aggregation(&self) {
        // Drain the buffered count deltas and add them to the store. Raw counts
        // only — the regression verdict is computed at read time, so the flush
        // stays a cheap buffer drain. On a store failure the deltas are pushed
        // back into the buffer so the next flush retries them (no lost counts).
        let deltas = self.collector.drain();
        if deltas.is_empty() {
            return;
        }

        if let Err(e) = self.store.add_aggregation(&deltas).await {
            tracing::warn!(error = %e, "failed to add aggregation; restoring counters for retry");
            self.collector.restore(&deltas);
        }
    }
}

/// Field-by-field diff of baseline against one comparable upstream response.
/// Empty when the upstream failed, answered with a different status, or its
/// body is not readable JSON.
async fn diff_against(
    baseline: &Value,
    baseline_status: u16,
    response: &Option<UpstreamResponse>,
) -> HashMap<String, FieldDiff> {
    match response {
        Some(r) if r.status == baseline_status => match parse_json_body(r).await {
            Some(other) => flatten_value(baseline, &other),
            None => HashMap::new(),
        },
        // Responded with a different status — the body is not compared (R23);
        // the status divergence itself is the signal, recorded as a reserved
        // pseudo-field so it is counted and queryable like any other diff.
        Some(r) => {
            let mut diffs = HashMap::new();
            diffs.insert(
                STATUS_FIELD.to_owned(),
                FieldDiff {
                    left: Some(serde_json::json!(baseline_status)),
                    right: Some(serde_json::json!(r.status)),
                    diff_type: DiffType::StatusMismatch,
                },
            );
            diffs
        }
        // Upstream failed or was not called — not a status mismatch.
        None => HashMap::new(),
    }
}

/// Decompress (when content-encoded) and JSON-parse a response body.
//
// NOTE (unbounded body size): the full upstream body is buffered upstream
// (`client.rs`) and decoded + parsed here with no max-size guard, so a very
// large analyzed response can spike memory on the analysis side. The baseline
// body must be buffered regardless (the hot path returns it to the client), but
// the candidate/control bodies analyzed here could be capped. Deferred — add a
// configurable byte limit that skips analysis (and truncates samples) above it.
async fn parse_json_body(response: &UpstreamResponse) -> Option<Value> {
    let body = decode_body(response).await?;
    match serde_json::from_slice(&body) {
        Ok(value) => Some(value),
        Err(e) => {
            tracing::debug!(error = %e, "skipping non-json body in analysis");
            None
        }
    }
}
