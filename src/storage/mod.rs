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

/// Periodic per-endpoint counter snapshot, destined for a Redis hash.
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldAggregation {
    pub raw_count: u64,
    pub noise_count: u64,
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

    async fn write_aggregation(
        &self,
        aggregations: &[EndpointAggregation],
    ) -> Result<(), StoreError>;

    /// Read the latest aggregation snapshot for one endpoint, or `None` if the
    /// endpoint has no snapshot yet. Read side of the query API — never the
    /// proxy hot path.
    async fn get_aggregation(
        &self,
        endpoint: &str,
    ) -> Result<Option<EndpointAggregation>, StoreError>;

    /// List the latest aggregation snapshot for every recorded endpoint.
    async fn list_aggregations(&self) -> Result<Vec<EndpointAggregation>, StoreError>;

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
