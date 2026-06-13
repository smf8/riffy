use std::collections::HashMap;

use crate::compare::flatten::FlatDiff;
use chrono::{DateTime, Utc};
use serde::Serialize;

pub mod error;
mod memory;
mod redis;

#[allow(unused_imports)]
pub use memory::InMemoryDiffStore;
pub use redis::RedisDiffStore;

use error::StoreError;

/// One per-request diff record, destined for the Redis stream.
#[derive(Debug, Clone)]
pub struct DiffEntry {
    pub endpoint: String,
    pub timestamp: DateTime<Utc>,
    /// primary vs candidate field diffs.
    pub raw_fields: HashMap<String, FlatDiff>,
    /// primary vs secondary field diffs.
    pub noise_fields: HashMap<String, FlatDiff>,
    pub primary_status: u16,
    pub candidate_status: Option<u16>,
    pub secondary_status: Option<u16>,
}

/// Periodic per-endpoint counter snapshot, destined for a Redis hash.
#[derive(Debug, Clone)]
pub struct EndpointAggregation {
    pub endpoint: String,
    pub total: u64,
    pub fields: HashMap<String, FieldAggregation>,
    pub last_updated: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FieldAggregation {
    pub raw_count: u64,
    pub noise_count: u64,
    pub is_regression: bool,
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
}
