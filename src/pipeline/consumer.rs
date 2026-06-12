use std::sync::Arc;
use std::time::Duration;

use super::decode::decode_body;
use super::AnalysisMessage;
use crate::analysis::filter::DifferencesFilter;
use crate::analysis::{DifferenceAnalyzer, DifferenceCollector};
use crate::endpoint::EndpointMatcher;
use crate::redis::{DiffEntry, DiffStore, EndpointAggregation, FieldAggregation};
use chrono::Utc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

/// Single-task consumer of the analysis channel: resolves endpoints, diffs
/// the response triplet, updates counters, appends per-request diff entries
/// to the store and periodically snapshots aggregations.
pub struct Consumer<C: DifferenceCollector, S: DiffStore> {
    rx: mpsc::Receiver<AnalysisMessage>,
    matcher: Arc<EndpointMatcher>,
    analyzer: DifferenceAnalyzer<C>,
    collector: Arc<C>,
    filter: DifferencesFilter,
    store: Arc<S>,
    aggregation_interval: Duration,
}

impl<C, S> Consumer<C, S>
where
    C: DifferenceCollector + 'static,
    S: DiffStore + 'static,
{
    pub fn new(
        rx: mpsc::Receiver<AnalysisMessage>,
        matcher: Arc<EndpointMatcher>,
        collector: Arc<C>,
        filter: DifferencesFilter,
        store: Arc<S>,
        aggregation_interval: Duration,
    ) -> Self {
        Self {
            rx,
            matcher,
            analyzer: DifferenceAnalyzer::new(collector.clone()),
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

        let Some(primary_body) = decode_body(&msg.primary_response).await else {
            tracing::warn!(
                endpoint = %endpoint,
                "skipping analysis: undecodable primary response body"
            );
            return;
        };

        let candidate_body = match &msg.candidate_response {
            Some(response) => decode_body(response).await,
            None => None,
        };
        let secondary_body = match &msg.secondary_response {
            Some(response) => decode_body(response).await,
            None => None,
        };

        let analyzed = match self.analyzer.analyze(
            &endpoint,
            &primary_body,
            candidate_body.as_deref(),
            secondary_body.as_deref(),
        ) {
            Ok(analyzed) => analyzed,
            Err(e) => {
                tracing::debug!(endpoint = %endpoint, error = %e, "analysis skipped");
                return;
            }
        };

        let candidate_status = msg.candidate_response.as_ref().map(|r| r.status);
        let secondary_status = msg.secondary_response.as_ref().map(|r| r.status);

        let status_mismatch = candidate_status.is_some_and(|s| s != msg.primary_response.status)
            || secondary_status.is_some_and(|s| s != msg.primary_response.status);

        // Identical responses produce no entry — only counters (total) move.
        if analyzed.raw_diffs.is_empty() && analyzed.noise_diffs.is_empty() && !status_mismatch {
            return;
        }

        let entry = DiffEntry {
            endpoint,
            timestamp: Utc::now(),
            raw_fields: analyzed.raw_diffs,
            noise_fields: analyzed.noise_diffs,
            primary_status: msg.primary_response.status,
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
