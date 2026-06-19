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
    /// Replayable curl command for the originating request, with a
    /// `$RIFFY_TARGET` placeholder host. `Some` only when the endpoint enabled
    /// `capture_request_curl`.
    pub request_curl: Option<String>,
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

/// The current time-bucket id for a bucket size in seconds.
pub(crate) fn current_bucket(bucket_secs: u64) -> u64 {
    (Utc::now().timestamp().max(0) as u64) / bucket_secs.max(1)
}

/// Number of buckets covered by the read window (at least one).
pub(crate) fn window_bucket_count(window_secs: u64, bucket_secs: u64) -> u64 {
    (window_secs / bucket_secs.max(1)).max(1)
}

/// Merge one bucket's aggregation into a windowed accumulator: sum the counts,
/// keep the latest `last_updated`.
pub(crate) fn merge_aggregation(acc: &mut Option<EndpointAggregation>, other: EndpointAggregation) {
    match acc {
        None => *acc = Some(other),
        Some(a) => {
            a.total += other.total;
            if other.last_updated > a.last_updated {
                a.last_updated = other.last_updated;
            }
            for (path, field) in other.fields {
                let entry = a.fields.entry(path).or_default();
                entry.raw_count += field.raw_count;
                entry.noise_count += field.noise_count;
            }
        }
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
    /// Replayable curl for the request that produced this sample (placeholder
    /// host). `None` when capture was disabled for the endpoint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_curl: Option<String>,
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

    /// Add a batch of per-endpoint count *deltas* into the current time bucket
    /// (atomic add, not overwrite). Buckets older than the retention window age
    /// out, so reads reflect only recent traffic. Multiple riffy instances
    /// sharing one backend sum into the same buckets instead of clobbering.
    async fn add_aggregation(&self, deltas: &[EndpointAggregation]) -> Result<(), StoreError>;

    /// Sum one endpoint's buckets over the retention window, or `None` when it
    /// has no counts within the window. Read side of the query API — never the
    /// proxy hot path.
    async fn get_aggregation(
        &self,
        endpoint: &str,
    ) -> Result<Option<EndpointAggregation>, StoreError>;

    /// Windowed aggregation for every endpoint with recent activity.
    async fn list_aggregations(&self) -> Result<Vec<EndpointAggregation>, StoreError>;

    /// Clear all stored aggregation buckets for one endpoint (admin reset).
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
