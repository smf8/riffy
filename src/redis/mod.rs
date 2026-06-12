use std::collections::HashMap;
use std::future::Future;

use crate::compare::flatten::FlatDiff;
use chrono::{DateTime, Utc};
use serde::Serialize;

pub mod error;
mod memory;
mod store;

#[allow(unused_imports)]
pub use memory::InMemoryDiffStore;
pub use store::RedisDiffStore;

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
/// one in tests and local development.
pub trait DiffStore: Send + Sync {
    fn append_diff(&self, entry: &DiffEntry)
        -> impl Future<Output = Result<(), StoreError>> + Send;

    fn write_aggregation(
        &self,
        aggregations: &[EndpointAggregation],
    ) -> impl Future<Output = Result<(), StoreError>> + Send;
}
