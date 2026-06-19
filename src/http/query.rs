use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::analysis::classify::EndpointClassifiers;
use crate::analysis::counters::LiveCounters;
use crate::compare::flatten::STATUS_FIELD;
use crate::error::AppError;
use crate::storage::{DiffStore, EndpointAggregation, FieldAggregation, SamplePage};

const DEFAULT_SAMPLE_LIMIT: usize = 20;
const MAX_SAMPLE_LIMIT: usize = 100;
// Cap on `offset` to prevent unbounded scans from pathological values.
const MAX_SAMPLE_OFFSET: usize = 100_000;

#[derive(Debug, Deserialize)]
pub struct PathsQuery {
    pub endpoint: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct EndpointPaths {
    pub endpoint: String,
    pub total: u64,
    pub paths: Vec<String>,
    pub last_updated: DateTime<Utc>,
}

impl From<EndpointAggregation> for EndpointPaths {
    fn from(aggregation: EndpointAggregation) -> Self {
        let mut paths: Vec<String> = aggregation.fields.into_keys().collect();
        paths.sort();
        Self {
            endpoint: aggregation.endpoint,
            total: aggregation.total,
            paths,
            last_updated: aggregation.last_updated,
        }
    }
}

pub async fn list_paths(
    State(store): State<Arc<dyn DiffStore>>,
    Query(query): Query<PathsQuery>,
) -> Result<Response, AppError> {
    match query.endpoint {
        Some(endpoint) => {
            let aggregation = store.get_aggregation(&endpoint).await?.ok_or_else(|| {
                AppError::NotFound(format!("no diffs recorded for endpoint '{endpoint}'"))
            })?;
            Ok(Json(EndpointPaths::from(aggregation)).into_response())
        }
        None => {
            let mut aggregations = store.list_aggregations().await?;
            aggregations.sort_by(|a, b| a.endpoint.cmp(&b.endpoint));
            let endpoints: Vec<EndpointPaths> =
                aggregations.into_iter().map(EndpointPaths::from).collect();
            Ok(Json(json!({ "endpoints": endpoints })).into_response())
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct DetailQuery {
    pub endpoint: String,
    pub path: String,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Serialize)]
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

pub async fn diff_detail(
    State(store): State<Arc<dyn DiffStore>>,
    State(classifiers): State<Arc<EndpointClassifiers>>,
    Query(query): Query<DetailQuery>,
) -> Result<Response, AppError> {
    let limit = query
        .limit
        .unwrap_or(DEFAULT_SAMPLE_LIMIT)
        .clamp(1, MAX_SAMPLE_LIMIT);
    let offset = query.offset.unwrap_or(0).min(MAX_SAMPLE_OFFSET);

    let aggregation = store.get_aggregation(&query.endpoint).await?;
    let field = aggregation
        .as_ref()
        .and_then(|aggregation| aggregation.fields.get(&query.path));

    let total = aggregation.as_ref().map(|a| a.total).unwrap_or(0);
    let last_updated = aggregation.as_ref().map(|a| a.last_updated);
    let (raw_count, noise_count) = match field {
        Some(field) => (field.raw_count, field.noise_count),
        None => (0, 0),
    };

    let samples = store
        .recent_samples(&query.endpoint, &query.path, limit, offset)
        .await?;

    if field.is_none() && samples.items.is_empty() && !samples.has_more && offset == 0 {
        return Err(AppError::NotFound(format!(
            "no diffs recorded for endpoint '{}' path '{}'",
            query.endpoint, query.path
        )));
    }

    let counts = FieldAggregation {
        raw_count,
        noise_count,
    };
    let is_regression = if query.path == STATUS_FIELD {
        // A status divergence is categorically a regression when the candidate
        // diverges more than the control — independent of percentage thresholds,
        // which would dilute a rare but critical status difference.
        counts.raw_count > counts.noise_count
    } else {
        classifiers
            .for_endpoint(&query.endpoint)
            .is_regression(&counts, total)
    };

    Ok(Json(DiffDetail {
        endpoint: query.endpoint,
        path: query.path,
        total,
        raw_count,
        noise_count,
        is_regression,
        relative_difference: counts.relative_difference(),
        absolute_difference: counts.absolute_difference(total),
        last_updated,
        samples,
    })
    .into_response())
}

#[derive(Debug, Clone, Serialize)]
pub struct UpstreamTargets {
    pub baseline: String,
    pub candidate: String,
    pub control: String,
}

impl UpstreamTargets {
    pub fn from_addresses(baseline: &str, candidate: &str, control: &str) -> Self {
        Self {
            baseline: normalize_base(baseline),
            candidate: normalize_base(candidate),
            control: normalize_base(control),
        }
    }
}

fn normalize_base(addr: &str) -> String {
    if addr.contains("://") {
        addr.to_owned()
    } else {
        format!("http://{addr}")
    }
}

pub async fn upstreams(State(targets): State<Arc<UpstreamTargets>>) -> Json<UpstreamTargets> {
    Json((*targets).clone())
}

#[derive(Debug, Deserialize)]
pub struct ResetQuery {
    pub endpoint: String,
}

pub async fn reset_stats(
    State(store): State<Arc<dyn DiffStore>>,
    State(counters): State<Arc<LiveCounters>>,
    Query(query): Query<ResetQuery>,
) -> Result<StatusCode, AppError> {
    if store.get_aggregation(&query.endpoint).await?.is_none() {
        return Err(AppError::NotFound(format!(
            "no statistics recorded for endpoint '{}'",
            query.endpoint
        )));
    }

    // Clear the buffer first so an in-flight flush can't re-add stale counts after the store is cleared.
    counters.reset_endpoint(&query.endpoint);
    store.reset_aggregation(&query.endpoint).await?;

    Ok(StatusCode::NO_CONTENT)
}
