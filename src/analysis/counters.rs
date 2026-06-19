use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::compare::flatten::FieldDiff;
use crate::storage::{EndpointAggregation, FieldAggregation};
use chrono::Utc;
use dashmap::DashMap;

#[derive(Default)]
pub struct LiveCounters {
    endpoints: DashMap<String, EndpointStats>,
}

#[derive(Default)]
struct EndpointStats {
    total: AtomicU64,
    per_path_counters: DashMap<String, FieldDiffCounters>,
}

#[derive(Default)]
struct FieldDiffCounters {
    raw_diff_count: AtomicU64,
    noise_diff_count: AtomicU64,
}

impl LiveCounters {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(
        &self,
        endpoint: &str,
        raw: &HashMap<String, FieldDiff>,
        noise: &HashMap<String, FieldDiff>,
    ) {
        let endpoint_stats = self.endpoints.entry(endpoint.to_owned()).or_default();

        endpoint_stats.total.fetch_add(1, Ordering::Relaxed);

        for path in raw.keys() {
            endpoint_stats
                .per_path_counters
                .entry(path.clone())
                .or_default()
                .raw_diff_count
                .fetch_add(1, Ordering::Relaxed);
        }

        for path in noise.keys() {
            endpoint_stats
                .per_path_counters
                .entry(path.clone())
                .or_default()
                .noise_diff_count
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn drain(&self) -> Vec<EndpointAggregation> {
        let now = Utc::now();
        let mut out = Vec::new();

        for endpoint in self.endpoints.iter() {
            let total = endpoint.value().total.swap(0, Ordering::Relaxed);
            if total == 0 {
                // No requests this interval — field counters are zero too.
                continue;
            }

            let mut fields = HashMap::new();
            for field in endpoint.value().per_path_counters.iter() {
                let raw_count = field.value().raw_diff_count.swap(0, Ordering::Relaxed);
                let noise_count = field.value().noise_diff_count.swap(0, Ordering::Relaxed);
                if raw_count == 0 && noise_count == 0 {
                    continue;
                }
                fields.insert(
                    field.key().clone(),
                    FieldAggregation {
                        raw_count,
                        noise_count,
                    },
                );
            }

            out.push(EndpointAggregation {
                endpoint: endpoint.key().clone(),
                total,
                fields,
                last_updated: now,
            });
        }

        out
    }

    pub fn reset_endpoint(&self, endpoint: &str) {
        self.endpoints.remove(endpoint);
    }

    pub fn restore(&self, deltas: &[EndpointAggregation]) {
        for delta in deltas {
            let endpoint = self.endpoints.entry(delta.endpoint.clone()).or_default();
            endpoint.total.fetch_add(delta.total, Ordering::Relaxed);
            for (path, field) in &delta.fields {
                let counters = endpoint.per_path_counters.entry(path.clone()).or_default();
                counters
                    .raw_diff_count
                    .fetch_add(field.raw_count, Ordering::Relaxed);
                counters
                    .noise_diff_count
                    .fetch_add(field.noise_count, Ordering::Relaxed);
            }
        }
    }
}
