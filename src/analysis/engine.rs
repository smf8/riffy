use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Value};

use super::classify::{EndpointClassifiers, FieldCounts};
use super::suppress::SuppressRules;
use crate::compare::flatten::{
    flatten_value, DiffType, FieldDiff, HEADER_FIELD_PREFIX, STATUS_FIELD,
};
use crate::storage::RawSample;

pub struct DiffEngine {
    suppress: ArcSwap<SuppressRules>,
    classifiers: EndpointClassifiers,
}

#[derive(Debug, Clone)]
pub struct EndpointCounts {
    pub endpoint: String,
    pub total: u64,
    pub fields: HashMap<String, FieldCounts>,
    pub last_updated: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiffSample {
    pub id: String,
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

    pub fn set_suppress(&self, endpoint: &str, paths: Vec<String>) -> Result<(), regex::Error> {
        // Validate up front so a bad regex is rejected before any swap.
        SuppressRules::compile(&paths)?;
        self.suppress.rcu(|cur| {
            // Patterns were validated just above, so this recompile cannot fail.
            Arc::new(
                cur.with_endpoint(endpoint, paths.clone())
                    .expect("suppress patterns validated above"),
            )
        });
        Ok(())
    }

    pub fn is_regression(
        &self,
        endpoint: &str,
        path: &str,
        counts: &FieldCounts,
        total: u64,
    ) -> bool {
        if path == STATUS_FIELD {
            // Status divergence is categorically a regression when candidate
            // diverges more than control, regardless of percentage thresholds.
            counts.raw_count > counts.noise_count
        } else {
            self.classifiers
                .for_endpoint(endpoint)
                .is_regression(counts, total)
        }
    }

    pub fn regressions(&self, counts: &EndpointCounts) -> Vec<String> {
        let mut out: Vec<String> = counts
            .fields
            .iter()
            .filter(|(path, field)| self.is_regression(&counts.endpoint, path, field, counts.total))
            .map(|(path, _)| path.clone())
            .collect();
        out.sort();
        out
    }

    pub fn aggregate(
        &self,
        endpoint: &str,
        samples: &[RawSample],
        extra: &SuppressRules,
    ) -> Option<EndpointCounts> {
        if samples.is_empty() {
            return None;
        }
        let rules = self.suppress.load_full();
        let mut fields: HashMap<String, FieldCounts> = HashMap::new();
        for sample in samples {
            let (raw, noise) = diff_sample(endpoint, sample, &rules, extra);
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

    pub fn detail(
        &self,
        endpoint: &str,
        path: &str,
        samples: &[RawSample],
        extra: &SuppressRules,
        limit: usize,
        offset: usize,
    ) -> DiffDetail {
        let rules = self.suppress.load_full();
        let total = samples.len() as u64;
        let last_updated = samples.first().map(|s| s.timestamp);

        let mut counts = FieldCounts::default();
        // Fetch one past the page to learn `has_more` without a second pass.
        let want = offset.saturating_add(limit).saturating_add(1);
        let mut matches: Vec<DiffSample> = Vec::new();

        for sample in samples {
            let (raw, noise) = diff_sample(endpoint, sample, &rules, extra);
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
                    id: sample.id.clone(),
                    timestamp: sample.timestamp,
                    raw: raw_at,
                    noise: noise_at,
                    request_curl: sample.request_curl.clone(),
                });
            }
        }

        let has_more = matches.len() > offset.saturating_add(limit);
        let items = matches.into_iter().skip(offset).take(limit).collect();

        let is_regression = self.is_regression(endpoint, path, &counts, total);

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

struct Baseline<'a> {
    body: &'a Value,
    headers: &'a Value,
    status: u16,
}

struct Other<'a> {
    status: Option<u16>,
    body: Option<&'a str>,
    headers: Option<&'a str>,
}

fn diff_sample(
    endpoint: &str,
    sample: &RawSample,
    rules: &SuppressRules,
    extra: &SuppressRules,
) -> (HashMap<String, FieldDiff>, HashMap<String, FieldDiff>) {
    let baseline_body: Value = match serde_json::from_str(&sample.baseline_body) {
        Ok(v) => v,
        // Bodies are JSON-validated at write time; degrade a corrupt read to "no diffs".
        Err(_) => return (HashMap::new(), HashMap::new()),
    };
    // A corrupt or pre-headers value yields no header diffs rather than failing the read.
    let baseline_headers: Value =
        serde_json::from_str(&sample.baseline_headers).unwrap_or_else(|_| json!({}));
    let baseline = Baseline {
        body: &baseline_body,
        headers: &baseline_headers,
        status: sample.baseline_status,
    };

    let raw = diff_one(
        endpoint,
        &baseline,
        &Other {
            status: sample.candidate_status,
            body: sample.candidate_body.as_deref(),
            headers: sample.candidate_headers.as_deref(),
        },
        rules,
        extra,
    );
    let noise = diff_one(
        endpoint,
        &baseline,
        &Other {
            status: sample.control_status,
            body: sample.control_body.as_deref(),
            headers: sample.control_headers.as_deref(),
        },
        rules,
        extra,
    );
    (raw, noise)
}

fn diff_one(
    endpoint: &str,
    baseline: &Baseline,
    other: &Other,
    rules: &SuppressRules,
    extra: &SuppressRules,
) -> HashMap<String, FieldDiff> {
    let mut diffs = match other.status {
        None => HashMap::new(),
        Some(status) if status == baseline.status => {
            let mut diffs = match other.body.and_then(parse_json) {
                Some(other_body) => flatten_value(baseline.body, &other_body),
                None => HashMap::new(),
            };
            if let Some(other_headers) = other.headers.and_then(parse_json) {
                for (path, diff) in flatten_value(baseline.headers, &other_headers) {
                    diffs.insert(format!("{HEADER_FIELD_PREFIX}.{path}"), diff);
                }
            }
            diffs
        }
        Some(status) => {
            let mut m = HashMap::new();
            m.insert(
                STATUS_FIELD.to_owned(),
                FieldDiff {
                    left: Some(json!(baseline.status)),
                    right: Some(json!(status)),
                    diff_type: DiffType::StatusMismatch,
                },
            );
            m
        }
    };

    diffs.retain(|path, _| {
        !rules.is_suppressed(endpoint, path) && !extra.is_suppressed(endpoint, path)
    });
    diffs
}

fn parse_json(text: &str) -> Option<Value> {
    serde_json::from_str(text).ok()
}
