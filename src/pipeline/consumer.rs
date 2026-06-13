use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use super::decode::decode_body;
use super::AnalysisMessage;
use crate::analysis::filter::DifferencesFilter;
use crate::analysis::DifferenceCollector;
use crate::compare::flatten::{flatten_value, FlatDiff};
use crate::endpoint::EndpointMatcher;
use crate::proxy::upstream::UpstreamResponse;
use crate::storage::{DiffEntry, DiffStore, EndpointAggregation, FieldAggregation};
use chrono::Utc;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

/// Single-task consumer of the analysis channel: resolves endpoints, diffs
/// the response triplet, updates counters, appends per-request diff entries
/// to the store and periodically snapshots aggregations.
pub struct Consumer {
    rx: mpsc::Receiver<AnalysisMessage>,
    matcher: Arc<EndpointMatcher>,
    collector: Arc<dyn DifferenceCollector>,
    filter: DifferencesFilter,
    store: Arc<dyn DiffStore>,
    aggregation_interval: Duration,
}

impl Consumer {
    pub fn new(
        rx: mpsc::Receiver<AnalysisMessage>,
        matcher: Arc<EndpointMatcher>,
        collector: Arc<dyn DifferenceCollector>,
        filter: DifferencesFilter,
        store: Arc<dyn DiffStore>,
        aggregation_interval: Duration,
    ) -> Self {
        Self {
            rx,
            matcher,
            collector,
            filter,
            store,
            aggregation_interval,
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
        let endpoint = self.matcher.resolve(&msg.path);
        let primary_status = msg.primary_response.status;

        let Some(primary) = parse_json_body(&msg.primary_response).await else {
            tracing::warn!(
                endpoint = %endpoint,
                "skipping analysis: primary response body is not readable json"
            );
            return;
        };

        // Statuses are checked before bodies: a body is only compared when
        // its upstream answered with the same status as primary. A different
        // status is itself the regression signal and is reported directly.
        let raw_diffs = diff_against(&primary, primary_status, &msg.candidate_response).await;
        let noise_diffs = diff_against(&primary, primary_status, &msg.secondary_response).await;

        self.collector.record(&endpoint, &raw_diffs, &noise_diffs);

        let candidate_status = msg.candidate_response.as_ref().map(|r| r.status);
        let secondary_status = msg.secondary_response.as_ref().map(|r| r.status);

        let status_mismatch = candidate_status.is_some_and(|s| s != primary_status)
            || secondary_status.is_some_and(|s| s != primary_status);

        // Identical responses produce no entry — only counters (total) move.
        if raw_diffs.is_empty() && noise_diffs.is_empty() && !status_mismatch {
            return;
        }

        let entry = DiffEntry {
            endpoint,
            timestamp: Utc::now(),
            raw_fields: raw_diffs,
            noise_fields: noise_diffs,
            primary_status,
            candidate_status,
            secondary_status,
        };

        // Store failures are non-fatal: log and keep consuming.
        if let Err(e) = self.store.append_diff(&entry).await {
            tracing::warn!(error = %e, "failed to append diff entry");
        }

        crate::telemetry::metrics::record_diff_published(
            &entry.endpoint,
            entry.raw_fields.len(),
            entry.noise_fields.len(),
            msg.received_at.elapsed(),
        );
    }

    async fn flush_aggregation(&self) {
        let snapshots = self.collector.snapshot();
        if snapshots.is_empty() {
            return;
        }

        let now = Utc::now();
        let aggregations: Vec<EndpointAggregation> = snapshots
            .into_iter()
            .map(|snapshot| EndpointAggregation {
                endpoint: snapshot.endpoint,
                total: snapshot.total,
                fields: snapshot
                    .fields
                    .into_iter()
                    .map(|field| {
                        let is_regression = self.filter.is_regression(&field);
                        (
                            field.path,
                            FieldAggregation {
                                raw_count: field.raw_count,
                                noise_count: field.noise_count,
                                is_regression,
                            },
                        )
                    })
                    .collect(),
                last_updated: now,
            })
            .collect();

        if let Err(e) = self.store.write_aggregation(&aggregations).await {
            tracing::warn!(error = %e, "failed to write aggregation snapshot");
        }
    }
}

/// Field-by-field diff of primary against one comparable upstream response.
/// Empty when the upstream failed, answered with a different status, or its
/// body is not readable JSON.
async fn diff_against(
    primary: &Value,
    primary_status: u16,
    response: &Option<UpstreamResponse>,
) -> HashMap<String, FlatDiff> {
    match response {
        Some(r) if r.status == primary_status => match parse_json_body(r).await {
            Some(other) => flatten_value(primary, &other),
            None => HashMap::new(),
        },
        _ => HashMap::new(),
    }
}

/// Decompress (when content-encoded) and JSON-parse a response body.
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
