use bytes::Bytes;
use chrono::{DateTime, Utc};

pub mod error;
mod memory;
mod redis;

#[cfg(test)]
mod tests;

pub use memory::InMemorySampleStore;
pub use redis::RedisSampleStore;

use error::StoreError;

pub const SAMPLE_KEY_PREFIX: &str = "riffy:samples";

#[derive(Debug, Clone, PartialEq)]
pub struct RawSample {
    pub id: String,
    pub endpoint: String,
    pub timestamp: DateTime<Utc>,
    pub baseline_status: u16,
    pub baseline_body: Bytes,
    pub baseline_headers: String,
    pub candidate_status: Option<u16>,
    pub candidate_body: Option<Bytes>,
    pub candidate_headers: Option<String>,
    pub control_status: Option<u16>,
    pub control_body: Option<Bytes>,
    pub control_headers: Option<String>,
    pub request_curl: Option<String>,
}

#[async_trait::async_trait]
pub trait SampleStore: Send + Sync {
    async fn append_sample(&self, sample: &RawSample) -> Result<(), StoreError>;

    async fn fetch_samples(&self, endpoint: &str) -> Result<Vec<RawSample>, StoreError>;

    async fn get_sample(&self, endpoint: &str, id: &str) -> Result<Option<RawSample>, StoreError>;

    async fn list_endpoints(&self) -> Result<Vec<String>, StoreError>;

    async fn delete_endpoint(&self, endpoint: &str) -> Result<(), StoreError>;
}
