use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Duration;

use super::error::StoreError;
use super::{
    current_bucket, merge_aggregation, window_bucket_count, DiffEntry, DiffSample, DiffStore,
    EndpointAggregation, SamplePage,
};
use chrono::Utc;
use tokio::sync::Mutex;

/// Default bucket/window used by `new()` / `with_capacity()` (tests, local dev).
const DEFAULT_BUCKET_SECS: u64 = 60;
const DEFAULT_WINDOW_SECS: u64 = 3600;

/// In-memory `DiffStore` for tests and local development without Redis.
/// Aggregation counts are kept in per-endpoint time buckets and summed over the
/// retention window on read; buckets older than the window are evicted.
pub struct InMemoryDiffStore {
    entries: Mutex<VecDeque<DiffEntry>>,
    aggregations: Mutex<HashMap<String, BTreeMap<u64, EndpointAggregation>>>,
    /// Max retained samples; the front (oldest) is dropped past this.
    cap: usize,
    bucket_secs: u64,
    window_secs: u64,
}

impl Default for InMemoryDiffStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryDiffStore {
    /// Unbounded sample retention with default windowing — the default used by
    /// tests and local dev.
    pub fn new() -> Self {
        Self::with_capacity(usize::MAX)
    }

    /// Bounded sample retention with default windowing.
    pub fn with_capacity(cap: usize) -> Self {
        Self::with_retention(
            cap,
            Duration::from_secs(DEFAULT_BUCKET_SECS),
            Duration::from_secs(DEFAULT_WINDOW_SECS),
        )
    }

    /// Full constructor: sample cap plus the aggregation bucket size and read
    /// window.
    pub fn with_retention(cap: usize, bucket: Duration, window: Duration) -> Self {
        Self {
            entries: Mutex::new(VecDeque::new()),
            aggregations: Mutex::new(HashMap::new()),
            cap,
            bucket_secs: bucket.as_secs().max(1),
            window_secs: window.as_secs().max(1),
        }
    }

    pub async fn entries(&self) -> Vec<DiffEntry> {
        self.entries.lock().await.iter().cloned().collect()
    }

    /// Windowed aggregation for one endpoint (inherent test helper).
    pub async fn aggregation(&self, endpoint: &str) -> Option<EndpointAggregation> {
        self.windowed(endpoint).await
    }

    /// Sum one endpoint's buckets over the read window.
    async fn windowed(&self, endpoint: &str) -> Option<EndpointAggregation> {
        let current = current_bucket(self.bucket_secs);
        let count = window_bucket_count(self.window_secs, self.bucket_secs);
        let from = current.saturating_sub(count.saturating_sub(1));

        let map = self.aggregations.lock().await;
        let buckets = map.get(endpoint)?;

        let mut acc = None;
        for (_bucket, agg) in buckets.range(from..=current) {
            merge_aggregation(&mut acc, agg.clone());
        }
        acc
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
        let now = Utc::now();
        let bucket = current_bucket(self.bucket_secs);
        // Keep the window's worth of buckets (plus the current one).
        let keep_from =
            bucket.saturating_sub(window_bucket_count(self.window_secs, self.bucket_secs));

        let mut map = self.aggregations.lock().await;
        for delta in deltas {
            let buckets = map.entry(delta.endpoint.clone()).or_default();
            let agg = buckets
                .entry(bucket)
                .or_insert_with(|| EndpointAggregation {
                    endpoint: delta.endpoint.clone(),
                    total: 0,
                    fields: HashMap::new(),
                    last_updated: now,
                });
            agg.total += delta.total;
            agg.last_updated = now;
            for (path, field_delta) in &delta.fields {
                let field = agg.fields.entry(path.clone()).or_default();
                field.raw_count += field_delta.raw_count;
                field.noise_count += field_delta.noise_count;
            }
            buckets.retain(|&b, _| b >= keep_from);
        }
        Ok(())
    }

    async fn get_aggregation(
        &self,
        endpoint: &str,
    ) -> Result<Option<EndpointAggregation>, StoreError> {
        Ok(self.windowed(endpoint).await)
    }

    async fn list_aggregations(&self) -> Result<Vec<EndpointAggregation>, StoreError> {
        let current = current_bucket(self.bucket_secs);
        let count = window_bucket_count(self.window_secs, self.bucket_secs);
        let from = current.saturating_sub(count.saturating_sub(1));

        let map = self.aggregations.lock().await;
        let mut out = Vec::new();
        for buckets in map.values() {
            let mut acc = None;
            for (_bucket, agg) in buckets.range(from..=current) {
                merge_aggregation(&mut acc, agg.clone());
            }
            if let Some(agg) = acc {
                out.push(agg);
            }
        }
        Ok(out)
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
