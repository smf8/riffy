use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::error::StoreError;
use super::{RawSample, SampleStore};
use chrono::Utc;
use tokio::sync::Mutex;

const DEFAULT_WINDOW_SECS: u64 = 3600;

pub struct InMemorySampleStore {
    endpoints: Mutex<HashMap<String, VecDeque<RawSample>>>,
    cap: usize,
    window_secs: u64,
    next_id: AtomicU64,
}

impl Default for InMemorySampleStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemorySampleStore {
    pub fn new() -> Self {
        Self::with_capacity(usize::MAX)
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self::with_retention(cap, Duration::from_secs(DEFAULT_WINDOW_SECS))
    }

    pub fn with_retention(cap: usize, window: Duration) -> Self {
        Self {
            endpoints: Mutex::new(HashMap::new()),
            cap: cap.max(1),
            window_secs: window.as_secs().max(1),
            next_id: AtomicU64::new(0),
        }
    }

    fn within_window(&self, sample: &RawSample, now: chrono::DateTime<Utc>) -> bool {
        let age = now.signed_duration_since(sample.timestamp);
        age.num_seconds() <= self.window_secs as i64
    }
}

#[async_trait::async_trait]
impl SampleStore for InMemorySampleStore {
    async fn append_sample(&self, sample: &RawSample) -> Result<(), StoreError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut stored = sample.clone();
        stored.id = id.to_string();

        let mut endpoints = self.endpoints.lock().await;
        let samples = endpoints.entry(sample.endpoint.clone()).or_default();
        samples.push_back(stored);
        while samples.len() > self.cap {
            samples.pop_front();
        }
        Ok(())
    }

    async fn fetch_samples(&self, endpoint: &str) -> Result<Vec<RawSample>, StoreError> {
        let now = Utc::now();
        let endpoints = self.endpoints.lock().await;
        let Some(samples) = endpoints.get(endpoint) else {
            return Ok(Vec::new());
        };
        Ok(samples
            .iter()
            .rev()
            .filter(|s| self.within_window(s, now))
            .cloned()
            .collect())
    }

    async fn get_sample(&self, endpoint: &str, id: &str) -> Result<Option<RawSample>, StoreError> {
        let endpoints = self.endpoints.lock().await;
        Ok(endpoints
            .get(endpoint)
            .and_then(|samples| samples.iter().find(|s| s.id == id).cloned()))
    }

    async fn list_endpoints(&self) -> Result<Vec<String>, StoreError> {
        Ok(self.endpoints.lock().await.keys().cloned().collect())
    }

    async fn delete_endpoint(&self, endpoint: &str) -> Result<(), StoreError> {
        self.endpoints.lock().await.remove(endpoint);
        Ok(())
    }
}
