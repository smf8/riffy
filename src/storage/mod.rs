use chrono::{DateTime, Utc};

pub mod error;
mod memory;
mod redis;

#[cfg(test)]
mod tests;

pub use memory::InMemorySampleStore;
pub use redis::RedisSampleStore;

use error::StoreError;

/// Key prefix for per-endpoint sample streams; the endpoint is appended as
/// `riffy:samples:{endpoint}`. The endpoint index set lives at
/// `riffy:samples:__endpoints__`.
pub const SAMPLE_KEY_PREFIX: &str = "riffy:samples";

/// One sampled request: the raw responses of all three upstreams. The producer
/// only records these; diffing, suppression, and detection run at read time over
/// the stored samples. Bodies are decoded JSON text (decoded off the hot path in
/// the consumer); a non-JSON body is stored as `None`.
#[derive(Debug, Clone, PartialEq)]
pub struct RawSample {
    /// Stable handle assigned by the store on write (Redis stream id / in-memory
    /// sequence); empty when the producer constructs the sample. Used to fetch the
    /// full sample for the inspect view.
    pub id: String,
    pub endpoint: String,
    pub timestamp: DateTime<Utc>,
    pub baseline_status: u16,
    pub baseline_body: String,
    /// `None` when the upstream call failed.
    pub candidate_status: Option<u16>,
    /// `Some` only when the candidate answered baseline's status with a JSON body
    /// within the size cap — exactly the cases the read-time diff compares.
    pub candidate_body: Option<String>,
    pub control_status: Option<u16>,
    pub control_body: Option<String>,
    pub request_curl: Option<String>,
}

/// Abstracted so the Redis backend can be swapped for in-memory in tests and
/// local dev. `async_trait` boxing only affects the analysis side, never the
/// proxy hot path.
#[async_trait::async_trait]
pub trait SampleStore: Send + Sync {
    async fn append_sample(&self, sample: &RawSample) -> Result<(), StoreError>;

    /// All samples recorded for one endpoint within the retention window,
    /// newest-first. Never called on the proxy hot path.
    async fn fetch_samples(&self, endpoint: &str) -> Result<Vec<RawSample>, StoreError>;

    /// One stored sample by its id (for the inspect view). `None` if absent.
    async fn get_sample(&self, endpoint: &str, id: &str) -> Result<Option<RawSample>, StoreError>;

    /// Every endpoint that currently has stored samples.
    async fn list_endpoints(&self) -> Result<Vec<String>, StoreError>;

    /// Drop all stored samples for one endpoint (admin reset).
    async fn delete_endpoint(&self, endpoint: &str) -> Result<(), StoreError>;
}
