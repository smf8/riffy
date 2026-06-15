use std::collections::HashMap;

use crate::compare::flatten::FieldDiff;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub mod error;
mod memory;
mod redis;

#[cfg(test)]
mod tests;

pub use memory::InMemoryDiffStore;
pub use redis::RedisDiffStore;

use error::StoreError;

/// Redis key for the per-request diff stream (`{app}:{resource}:{type}`).
pub const DIFF_STREAM_KEY: &str = "riffy:diffs";
/// Redis key prefix for per-endpoint aggregation hashes; the endpoint is
/// appended as `riffy:agg:{endpoint}`.
pub const AGGREGATION_KEY_PREFIX: &str = "riffy:agg";

/// One per-request diff record, destined for the Redis stream.
#[derive(Debug, Clone)]
pub struct DiffEntry {
    pub endpoint: String,
    pub timestamp: DateTime<Utc>,
    /// baseline vs candidate field diffs.
    pub raw_fields: HashMap<String, FieldDiff>,
    /// baseline vs control field diffs.
    pub noise_fields: HashMap<String, FieldDiff>,
    pub baseline_status: u16,
    pub candidate_status: Option<u16>,
    pub control_status: Option<u16>,
}

/// Per-endpoint counter aggregation. Used in two directions with the same
/// shape: on the write side the counts are a *delta* drained from the live
/// buffer and added to the store (`add_aggregation`); on the read side the
/// counts are the *cumulative* totals returned from the store.
#[derive(Debug, Clone)]
pub struct EndpointAggregation {
    pub endpoint: String,
    pub total: u64,
    pub fields: HashMap<String, FieldAggregation>,
    pub last_updated: DateTime<Utc>,
}

/// Stored raw counters for one field path. The regression verdict and the
/// relative/absolute percentages are derived from these at read time against
/// the live thresholds — never persisted, so changing a threshold reclassifies
/// every endpoint instantly with no re-flush.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FieldAggregation {
    pub raw_count: u64,
    pub noise_count: u64,
}

impl FieldAggregation {
    /// `|raw − noise| / (raw + noise) × 100`. Zero when both counters are zero.
    pub fn relative_difference(&self) -> f64 {
        let raw = self.raw_count as f64;
        let noise = self.noise_count as f64;
        let denominator = raw + noise;
        if denominator == 0.0 {
            return 0.0;
        }
        (raw - noise).abs() / denominator * 100.0
    }

    /// `|raw − noise| / endpoint_total × 100`. Zero when no requests recorded.
    pub fn absolute_difference(&self, endpoint_total: u64) -> f64 {
        if endpoint_total == 0 {
            return 0.0;
        }
        (self.raw_count as f64 - self.noise_count as f64).abs() / endpoint_total as f64 * 100.0
    }
}

/// One stored per-request difference observed at a single field path, as
/// returned by the read API. `raw` is the baseline-vs-candidate diff at this
/// path, `noise` the baseline-vs-control diff; at least one is present.
#[derive(Debug, Clone, Serialize)]
pub struct DiffSample {
    pub timestamp: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw: Option<FieldDiff>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub noise: Option<FieldDiff>,
}

/// A newest-first page of `DiffSample`s for one endpoint + field path.
#[derive(Debug, Clone, Serialize)]
pub struct SamplePage {
    pub items: Vec<DiffSample>,
    pub limit: usize,
    pub offset: usize,
    /// `true` when at least one older matching sample exists beyond this page.
    pub has_more: bool,
}

/// Storage for per-request diffs and periodic aggregation snapshots.
/// Abstracted so the Redis implementation can be swapped for an in-memory
/// one in tests and local development. `async_trait` keeps it usable as a
/// plain `dyn DiffStore` trait object; the boxing it adds only affects the
/// analysis side, never the proxy hot path.
#[async_trait::async_trait]
pub trait DiffStore: Send + Sync {
    async fn append_diff(&self, entry: &DiffEntry) -> Result<(), StoreError>;

    /// Add a batch of per-endpoint count *deltas* to the store, accumulating
    /// into the existing totals (atomic add, not overwrite). Multiple riffy
    /// instances sharing one backend therefore sum into the same totals instead
    /// of clobbering each other. `last_updated` is set to the delta's value.
    async fn add_aggregation(&self, deltas: &[EndpointAggregation]) -> Result<(), StoreError>;

    /// Read the latest aggregation snapshot for one endpoint, or `None` if the
    /// endpoint has no snapshot yet. Read side of the query API — never the
    /// proxy hot path.
    async fn get_aggregation(
        &self,
        endpoint: &str,
    ) -> Result<Option<EndpointAggregation>, StoreError>;

    /// List the latest aggregation snapshot for every recorded endpoint.
    async fn list_aggregations(&self) -> Result<Vec<EndpointAggregation>, StoreError>;

    /// Clear the stored aggregation counts for one endpoint (admin reset).
    /// Per-request samples are not purged — they are bounded by the stream cap
    /// and age out on their own; only the statistics are reset.
    async fn reset_aggregation(&self, endpoint: &str) -> Result<(), StoreError>;

    /// Page through recorded per-request diff samples for one endpoint + field
    /// path, newest first. `offset`/`limit` paginate the matching samples.
    async fn recent_samples(
        &self,
        endpoint: &str,
        path: &str,
        limit: usize,
        offset: usize,
    ) -> Result<SamplePage, StoreError>;
}
