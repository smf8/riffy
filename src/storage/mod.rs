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

pub const DIFF_STREAM_KEY: &str = "riffy:diffs";
/// Key prefix for per-endpoint aggregation hashes; endpoint is appended as `riffy:agg:{endpoint}`.
pub const AGGREGATION_KEY_PREFIX: &str = "riffy:agg";

#[derive(Debug, Clone)]
pub struct DiffEntry {
    pub endpoint: String,
    pub timestamp: DateTime<Utc>,
    /// baseline vs candidate diffs.
    pub raw_fields: HashMap<String, FieldDiff>,
    /// baseline vs control diffs.
    pub noise_fields: HashMap<String, FieldDiff>,
    pub baseline_status: u16,
    pub candidate_status: Option<u16>,
    pub control_status: Option<u16>,
    pub request_curl: Option<String>,
}

/// Used in two directions with the same shape: write side = *delta* drained from
/// the live buffer; read side = *cumulative* totals returned from the store.
#[derive(Debug, Clone)]
pub struct EndpointAggregation {
    pub endpoint: String,
    pub total: u64,
    pub fields: HashMap<String, FieldAggregation>,
    pub last_updated: DateTime<Utc>,
}

/// Raw counters for one field. The regression verdict and percentages are derived
/// at read time from the live thresholds — never persisted, so changing a
/// threshold reclassifies every endpoint instantly without a re-flush.
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

pub(crate) fn current_bucket(bucket_secs: u64) -> u64 {
    (Utc::now().timestamp().max(0) as u64) / bucket_secs.max(1)
}

pub(crate) fn window_bucket_count(window_secs: u64, bucket_secs: u64) -> u64 {
    (window_secs / bucket_secs.max(1)).max(1)
}

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

#[derive(Debug, Clone, Serialize)]
pub struct DiffSample {
    pub timestamp: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw: Option<FieldDiff>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub noise: Option<FieldDiff>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_curl: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SamplePage {
    pub items: Vec<DiffSample>,
    pub limit: usize,
    pub offset: usize,
    /// `true` when at least one older matching sample exists beyond this page.
    pub has_more: bool,
}

/// Abstracted so the Redis backend can be swapped for in-memory in tests.
/// `async_trait` boxing only affects the analysis side, never the proxy hot path.
#[async_trait::async_trait]
pub trait DiffStore: Send + Sync {
    async fn append_diff(&self, entry: &DiffEntry) -> Result<(), StoreError>;

    /// Atomic add (not overwrite) into the current time bucket. Multiple riffy
    /// instances sharing one backend sum into the same bucket instead of clobbering.
    async fn add_aggregation(&self, deltas: &[EndpointAggregation]) -> Result<(), StoreError>;

    /// Sum one endpoint's buckets over the retention window. `None` when no
    /// counts exist within the window. Never called on the proxy hot path.
    async fn get_aggregation(
        &self,
        endpoint: &str,
    ) -> Result<Option<EndpointAggregation>, StoreError>;

    async fn list_aggregations(&self) -> Result<Vec<EndpointAggregation>, StoreError>;

    /// Clear stored aggregation buckets for one endpoint (admin reset).
    /// Per-request samples are not purged — they are bounded by the stream cap
    /// and age out on their own.
    async fn reset_aggregation(&self, endpoint: &str) -> Result<(), StoreError>;

    async fn recent_samples(
        &self,
        endpoint: &str,
        path: &str,
        limit: usize,
        offset: usize,
    ) -> Result<SamplePage, StoreError>;
}
