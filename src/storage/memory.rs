use std::collections::HashMap;

use super::error::StoreError;
use super::{DiffEntry, DiffStore, EndpointAggregation};
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
}
