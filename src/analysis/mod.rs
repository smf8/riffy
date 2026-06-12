use std::collections::HashMap;
use std::sync::Arc;

use crate::compare::flatten::{flatten_value, FlatDiff};
use serde_json::Value;

pub mod collector;
pub mod error;
pub mod filter;
pub mod joined;

#[cfg(test)]
mod tests;

use error::AnalysisError;
use joined::JoinedEndpoint;

/// Sink for per-request difference observations, keyed by endpoint and field
/// path. Implementations must be safe for concurrent use.
pub trait DifferenceCollector: Send + Sync {
    /// Record one analyzed request: bump the endpoint total and the raw/noise
    /// counters of every differing field path.
    fn record(
        &self,
        endpoint: &str,
        raw: &HashMap<String, FlatDiff>,
        noise: &HashMap<String, FlatDiff>,
    );

    /// Snapshot all per-endpoint counters joined into `JoinedEndpoint`s.
    fn snapshot(&self) -> Vec<JoinedEndpoint>;
}

/// Diffs computed for one request triplet.
pub struct AnalyzedRequest {
    /// primary vs candidate — potential regressions.
    pub raw_diffs: HashMap<String, FlatDiff>,
    /// primary vs secondary — the noise baseline.
    pub noise_diffs: HashMap<String, FlatDiff>,
}

/// Computes raw (primary vs candidate) and noise (primary vs secondary)
/// diffs for one request and feeds the collector counters.
pub struct DifferenceAnalyzer<C: DifferenceCollector> {
    collector: Arc<C>,
}

impl<C: DifferenceCollector> DifferenceAnalyzer<C> {
    pub fn new(collector: Arc<C>) -> Self {
        Self { collector }
    }

    /// Analyze one request. The primary body must be valid JSON; candidate
    /// and secondary are each skipped (empty diff map) when absent or not
    /// valid JSON, since a failed upstream must not poison the counters.
    pub fn analyze(
        &self,
        endpoint: &str,
        primary: &[u8],
        candidate: Option<&[u8]>,
        secondary: Option<&[u8]>,
    ) -> Result<AnalyzedRequest, AnalysisError> {
        let primary: Value =
            serde_json::from_slice(primary).map_err(AnalysisError::PrimaryJsonParse)?;

        let raw_diffs = candidate
            .and_then(parse_lenient)
            .map(|c| flatten_value(&primary, &c))
            .unwrap_or_default();

        let noise_diffs = secondary
            .and_then(parse_lenient)
            .map(|s| flatten_value(&primary, &s))
            .unwrap_or_default();

        self.collector.record(endpoint, &raw_diffs, &noise_diffs);

        Ok(AnalyzedRequest {
            raw_diffs,
            noise_diffs,
        })
    }
}

fn parse_lenient(body: &[u8]) -> Option<Value> {
    match serde_json::from_slice(body) {
        Ok(v) => Some(v),
        Err(e) => {
            tracing::debug!(error = %e, "skipping non-json body in analysis");
            None
        }
    }
}
