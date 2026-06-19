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

        self.flush_aggregation().await;
        tracing::info!("analysis consumer stopped");
    }

    async fn handle(&self, msg: AnalysisMessage) {
        let Some(endpoint) = self.matcher.resolve(&msg.path) else {
            // Safety net: the proxy handler already skips fan-out for unregistered
            // endpoints, but this keeps analysis bounded to configured endpoints.
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

        // Statuses are checked before bodies: a body is only compared when its
        // upstream answered with the same status as baseline. A different status
        // is itself the regression signal and is reported via STATUS_FIELD.
        let mut raw_diffs = diff_against(&baseline, baseline_status, &msg.candidate_response).await;
        let mut noise_diffs = diff_against(&baseline, baseline_status, &msg.control_response).await;

        self.suppress.suppress(&endpoint, &mut raw_diffs);
        self.suppress.suppress(&endpoint, &mut noise_diffs);

        self.collector.record(&endpoint, &raw_diffs, &noise_diffs);

        let candidate_status = msg.candidate_response.as_ref().map(|r| r.status);
        let control_status = msg.control_response.as_ref().map(|r| r.status);

        // Identical responses produce no entry. A status mismatch surfaces as a
        // STATUS_FIELD diff (see diff_against), so this emptiness check covers it.
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
            request_curl: msg.request.as_ref().map(build_curl),
        };

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
        // Status divergence: skip body comparison; the status difference itself is
        // the signal, recorded as a pseudo-field so it counts and queries like any diff.
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
        None => HashMap::new(),
    }
}

// NOTE: the full upstream body is buffered with no max-size guard, so a very
// large analyzed response can spike memory on the analysis side. Deferred —
// add a configurable byte limit that skips analysis above it.
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
