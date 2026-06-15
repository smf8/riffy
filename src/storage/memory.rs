use std::collections::{HashMap, VecDeque};

use super::error::StoreError;
use super::{DiffEntry, DiffSample, DiffStore, EndpointAggregation, SamplePage};
use tokio::sync::Mutex;

/// In-memory `DiffStore` for tests and local development without Redis.
pub struct InMemoryDiffStore {
    entries: Mutex<VecDeque<DiffEntry>>,
    aggregations: Mutex<HashMap<String, EndpointAggregation>>,
    /// Max retained samples; the front (oldest) is dropped past this.
    cap: usize,
}

impl Default for InMemoryDiffStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryDiffStore {
    /// Unbounded sample retention — the default used by tests and local dev.
    pub fn new() -> Self {
        Self::with_capacity(usize::MAX)
    }

    /// Bounded sample retention: at most `cap` per-request samples are kept.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entries: Mutex::new(VecDeque::new()),
            aggregations: Mutex::new(HashMap::new()),
            cap,
        }
    }

    pub async fn entries(&self) -> Vec<DiffEntry> {
        self.entries.lock().await.iter().cloned().collect()
    }

    pub async fn aggregation(&self, endpoint: &str) -> Option<EndpointAggregation> {
        self.aggregations.lock().await.get(endpoint).cloned()
    }
}

#[async_trait::async_trait]
impl DiffStore for InMemoryDiffStore {
    async fn append_diff(&self, entry: &DiffEntry) -> Result<(), StoreError> {
        let mut entries = self.entries.lock().await;
        entries.push_back(entry.clone());
        while entries.len() > self.cap {
            entries.pop_front();
        }
        Ok(())
    }

    async fn add_aggregation(&self, deltas: &[EndpointAggregation]) -> Result<(), StoreError> {
        let mut map = self.aggregations.lock().await;
        for delta in deltas {
            let entry = map
                .entry(delta.endpoint.clone())
                .or_insert_with(|| EndpointAggregation {
                    endpoint: delta.endpoint.clone(),
                    total: 0,
                    fields: HashMap::new(),
                    last_updated: delta.last_updated,
                });
            entry.total += delta.total;
            entry.last_updated = delta.last_updated;
            for (path, field_delta) in &delta.fields {
                let field = entry.fields.entry(path.clone()).or_default();
                field.raw_count += field_delta.raw_count;
                field.noise_count += field_delta.noise_count;
            }
        }
        Ok(())
    }

    async fn get_aggregation(
        &self,
        endpoint: &str,
    ) -> Result<Option<EndpointAggregation>, StoreError> {
        Ok(self.aggregations.lock().await.get(endpoint).cloned())
    }

    async fn list_aggregations(&self) -> Result<Vec<EndpointAggregation>, StoreError> {
        Ok(self.aggregations.lock().await.values().cloned().collect())
    }

    async fn reset_aggregation(&self, endpoint: &str) -> Result<(), StoreError> {
        self.aggregations.lock().await.remove(endpoint);
        Ok(())
    }

    async fn recent_samples(
        &self,
        endpoint: &str,
        path: &str,
        limit: usize,
        offset: usize,
    ) -> Result<SamplePage, StoreError> {
        // Look one sample past the requested window to know whether more exist.
        let want = offset.saturating_add(limit).saturating_add(1);
        let mut matches = Vec::new();

        let entries = self.entries.lock().await;
        for entry in entries.iter().rev() {
            if entry.endpoint != endpoint {
                continue;
            }
            let raw = entry.raw_fields.get(path).cloned();
            let noise = entry.noise_fields.get(path).cloned();
            if raw.is_none() && noise.is_none() {
                continue;
            }
            matches.push(DiffSample {
                timestamp: entry.timestamp,
                raw,
                noise,
            });
            if matches.len() >= want {
                break;
            }
        }

        let has_more = matches.len() > offset.saturating_add(limit);
        let items = matches.into_iter().skip(offset).take(limit).collect();
        Ok(SamplePage {
            items,
            limit,
            offset,
            has_more,
        })
    }
}
