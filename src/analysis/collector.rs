use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use super::joined::{JoinedEndpoint, JoinedField};
use super::DifferenceCollector;
use crate::compare::flatten::FlatDiff;
use dashmap::DashMap;

/// Lock-free in-memory counters: endpoint → (total, field path → raw/noise).
#[derive(Default)]
pub struct InMemoryDifferenceCollector {
    endpoints: DashMap<String, EndpointStats>,
}

#[derive(Default)]
struct EndpointStats {
    total: AtomicU64,
    fields: DashMap<String, FieldCounters>,
}

#[derive(Default)]
struct FieldCounters {
    raw: AtomicU64,
    noise: AtomicU64,
}

impl InMemoryDifferenceCollector {
    pub fn new() -> Self {
        Self::default()
    }
}

impl DifferenceCollector for InMemoryDifferenceCollector {
    fn record(
        &self,
        endpoint: &str,
        raw: &HashMap<String, FlatDiff>,
        noise: &HashMap<String, FlatDiff>,
    ) {
        let stats = self.endpoints.entry(endpoint.to_owned()).or_default();

        stats.total.fetch_add(1, Ordering::Relaxed);

        for path in raw.keys() {
            stats
                .fields
                .entry(path.clone())
                .or_default()
                .raw
                .fetch_add(1, Ordering::Relaxed);
        }

        for path in noise.keys() {
            stats
                .fields
                .entry(path.clone())
                .or_default()
                .noise
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> Vec<JoinedEndpoint> {
        self.endpoints
            .iter()
            .map(|entry| {
                let total = entry.value().total.load(Ordering::Relaxed);
                let fields = entry
                    .value()
                    .fields
                    .iter()
                    .map(|field| JoinedField {
                        path: field.key().clone(),
                        raw_count: field.value().raw.load(Ordering::Relaxed),
                        noise_count: field.value().noise.load(Ordering::Relaxed),
                        endpoint_total: total,
                    })
                    .collect();

                JoinedEndpoint {
                    endpoint: entry.key().clone(),
                    total,
                    fields,
                }
            })
            .collect()
    }
}
