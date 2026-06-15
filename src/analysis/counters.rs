use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use super::snapshot::{EndpointSnapshot, FieldSnapshot};
use super::DiffCounters;
use crate::compare::flatten::FieldDiff;
use dashmap::DashMap;

/// Lock-free in-memory counters: endpoint → (total, field path → raw/noise).
#[derive(Default)]
pub struct LiveCounters {
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

impl LiveCounters {
    pub fn new() -> Self {
        Self::default()
    }
}

impl DiffCounters for LiveCounters {
    fn record(
        &self,
        endpoint: &str,
        raw: &HashMap<String, FieldDiff>,
        noise: &HashMap<String, FieldDiff>,
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

    fn snapshot(&self) -> Vec<EndpointSnapshot> {
        self.endpoints
            .iter()
            .map(|entry| {
                let total = entry.value().total.load(Ordering::Relaxed);
                let fields = entry
                    .value()
                    .fields
                    .iter()
                    .map(|field| FieldSnapshot {
                        path: field.key().clone(),
                        raw_count: field.value().raw.load(Ordering::Relaxed),
                        noise_count: field.value().noise.load(Ordering::Relaxed),
                        endpoint_total: total,
                    })
                    .collect();

                EndpointSnapshot {
                    endpoint: entry.key().clone(),
                    total,
                    fields,
                }
            })
            .collect()
    }
}
