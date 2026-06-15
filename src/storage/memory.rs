use std::collections::HashMap;

use super::error::StoreError;
use super::{DiffEntry, DiffSample, DiffStore, EndpointAggregation, SamplePage};
use tokio::sync::Mutex;

/// In-memory `DiffStore` for tests and local development without Redis.
#[derive(Default)]
pub struct InMemoryDiffStore {
    entries: Mutex<Vec<DiffEntry>>,
    aggregations: Mutex<HashMap<String, EndpointAggregation>>,
}

impl InMemoryDiffStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn entries(&self) -> Vec<DiffEntry> {
        self.entries.lock().await.clone()
    }

    pub async fn aggregation(&self, endpoint: &str) -> Option<EndpointAggregation> {
        self.aggregations.lock().await.get(endpoint).cloned()
    }
}

#[async_trait::async_trait]
impl DiffStore for InMemoryDiffStore {
    async fn append_diff(&self, entry: &DiffEntry) -> Result<(), StoreError> {
        self.entries.lock().await.push(entry.clone());
        Ok(())
    }

    async fn write_aggregation(
        &self,
        aggregations: &[EndpointAggregation],
    ) -> Result<(), StoreError> {
        let mut map = self.aggregations.lock().await;
        for aggregation in aggregations {
            map.insert(aggregation.endpoint.clone(), aggregation.clone());
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
