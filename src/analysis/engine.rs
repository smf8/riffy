use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Value};

use super::classify::{EndpointClassifiers, FieldCounts};
use super::suppress::SuppressRules;
use crate::compare::flatten::{flatten_value, DiffType, FieldDiff, STATUS_FIELD};
use crate::storage::RawSample;

/// Read-time analysis: turns stored raw samples into diffs, applies the
/// suppression rules it owns *during* diffing, aggregates counts, and runs the
/// regression verdict. The producer records raw samples only — none of this runs
/// on the write side. The suppression rules are swapped atomically, so a runtime
/// edit takes effect on the next query with no restart.
pub struct DiffEngine {
    suppress: ArcSwap<SuppressRules>,
    classifiers: EndpointClassifiers,
}

/// Per-endpoint diff tallies summed across the windowed samples.
#[derive(Debug, Clone)]
pub struct EndpointCounts {
    pub endpoint: String,
    pub total: u64,
    pub fields: HashMap<String, FieldCounts>,
    pub last_updated: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiffSample {
    pub timestamp: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw: Option<FieldDiff>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub noise: Option<FieldDiff>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_curl: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SamplePage {
    pub items: Vec<DiffSample>,
    pub limit: usize,
    pub offset: usize,
    /// `true` when at least one older matching sample exists beyond this page.
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiffDetail {
    pub endpoint: String,
    pub path: String,
    pub total: u64,
    pub raw_count: u64,
    pub noise_count: u64,
    pub is_regression: bool,
    pub relative_difference: f64,
    pub absolute_difference: f64,
    pub last_updated: Option<DateTime<Utc>>,
    pub samples: SamplePage,
}

impl DiffEngine {
    pub fn new(suppress: SuppressRules, classifiers: EndpointClassifiers) -> Self {
        Self {
            suppress: ArcSwap::from_pointee(suppress),
            classifiers,
        }
    }

    pub fn rules(&self) -> Arc<SuppressRules> {
        self.suppress.load_full()
    }

    /// Replace one endpoint's suppression rules (empty list clears it). Takes
    /// effect on the next query; `rcu` makes concurrent edits safe.
    pub fn set_suppress(&self, endpoint: &str, paths: Vec<String>) {
        self.suppress
            .rcu(|cur| Arc::new(cur.with_endpoint(endpoint, paths.clone())));
    }

    /// Sum per-field raw/noise diff counts across the windowed samples.
    /// `None` when there are no samples for the endpoint.
    pub fn aggregate(&self, endpoint: &str, samples: &[RawSample]) -> Option<EndpointCounts> {
        if samples.is_empty() {
            return None;
        }
        let rules = self.suppress.load_full();
        let mut fields: HashMap<String, FieldCounts> = HashMap::new();
        for sample in samples {
            let (raw, noise) = diff_sample(endpoint, sample, &rules);
            for path in raw.keys() {
                fields.entry(path.clone()).or_default().raw_count += 1;
            }
            for path in noise.keys() {
                fields.entry(path.clone()).or_default().noise_count += 1;
            }
        }

        let last_updated = samples
            .iter()
            .map(|s| s.timestamp)
            .max()
            .unwrap_or_else(Utc::now);

        Some(EndpointCounts {
            endpoint: endpoint.to_owned(),
            total: samples.len() as u64,
            fields,
            last_updated,
        })
    }

    /// Counts and a paginated, newest-first list of per-sample diffs for one
    /// field path. `samples` must be newest-first.
    pub fn detail(
        &self,
        endpoint: &str,
        path: &str,
        samples: &[RawSample],
        limit: usize,
        offset: usize,
    ) -> DiffDetail {
        let rules = self.suppress.load_full();
        let total = samples.len() as u64;
        let last_updated = samples.first().map(|s| s.timestamp);

        let mut counts = FieldCounts::default();
        // Collect one past the page so `has_more` is known without a second pass;
        // counts still scan every sample.
        let want = offset.saturating_add(limit).saturating_add(1);
        let mut matches: Vec<DiffSample> = Vec::new();

        for sample in samples {
            let (raw, noise) = diff_sample(endpoint, sample, &rules);
            let raw_at = raw.get(path).cloned();
            let noise_at = noise.get(path).cloned();
            if raw_at.is_some() {
                counts.raw_count += 1;
            }
            if noise_at.is_some() {
                counts.noise_count += 1;
            }
            if (raw_at.is_some() || noise_at.is_some()) && matches.len() < want {
                matches.push(DiffSample {
                    timestamp: sample.timestamp,
                    raw: raw_at,
                    noise: noise_at,
                    request_curl: sample.request_curl.clone(),
                });
            }
        }

        let has_more = matches.len() > offset.saturating_add(limit);
        let items = matches.into_iter().skip(offset).take(limit).collect();

        let is_regression = if path == STATUS_FIELD {
            // A status divergence is categorically a regression when the candidate
            // diverges more than the control — independent of percentage thresholds,
            // which would dilute a rare but critical status difference.
            counts.raw_count > counts.noise_count
        } else {
            self.classifiers
                .for_endpoint(endpoint)
                .is_regression(&counts, total)
        };

        DiffDetail {
            endpoint: endpoint.to_owned(),
            path: path.to_owned(),
            total,
            raw_count: counts.raw_count,
            noise_count: counts.noise_count,
            is_regression,
            relative_difference: counts.relative_difference(),
            absolute_difference: counts.absolute_difference(total),
            last_updated,
            samples: SamplePage {
                items,
                limit,
                offset,
                has_more,
            },
        }
    }
}

/// Reproduces the former consumer-side `diff_against` logic for one sample, with
/// suppression applied inline. Returns `(raw = baseline vs candidate, noise =
/// baseline vs control)`.
fn diff_sample(
    endpoint: &str,
    sample: &RawSample,
    rules: &SuppressRules,
) -> (HashMap<String, FieldDiff>, HashMap<String, FieldDiff>) {
    let baseline: Value = match serde_json::from_str(&sample.baseline_body) {
        Ok(v) => v,
        // baseline_body is validated as JSON at write time; treat a corrupt body
        // defensively as "no diffs" rather than panicking a read.
        Err(_) => return (HashMap::new(), HashMap::new()),
    };

    let raw = diff_one(
        endpoint,
        &baseline,
        sample.baseline_status,
        sample.candidate_status,
        sample.candidate_body.as_deref(),
        rules,
    );
    let noise = diff_one(
        endpoint,
        &baseline,
        sample.baseline_status,
        sample.control_status,
        sample.control_body.as_deref(),
        rules,
    );
    (raw, noise)
}

fn diff_one(
    endpoint: &str,
    baseline: &Value,
    baseline_status: u16,
    other_status: Option<u16>,
    other_body: Option<&str>,
    rules: &SuppressRules,
) -> HashMap<String, FieldDiff> {
    let mut diffs = match other_status {
        // Upstream failed: contributes nothing.
        None => HashMap::new(),
        // Same status: compare bodies. A missing/unparseable body yields no diffs.
        Some(status) if status == baseline_status => {
            match other_body.and_then(|b| serde_json::from_str::<Value>(b).ok()) {
                Some(other) => flatten_value(baseline, &other),
                None => HashMap::new(),
            }
        }
        // Status divergence: the difference itself is the signal, recorded as a
        // pseudo-field so it counts and queries like any diff; body not compared.
        Some(status) => {
            let mut m = HashMap::new();
            m.insert(
                STATUS_FIELD.to_owned(),
                FieldDiff {
                    left: Some(json!(baseline_status)),
                    right: Some(json!(status)),
                    diff_type: DiffType::StatusMismatch,
                },
            );
            m
        }
    };

    // Apply suppression directly while computing the diff.
    diffs.retain(|path, _| !rules.is_suppressed(endpoint, path));
    diffs
}
