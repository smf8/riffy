//! Read-side query API (admin server): inspect recorded diffs.
//! `GET /diffs/paths` lists endpoints and their diffing field paths;
//! `GET /diffs/detail` returns one field's aggregated stats plus a paginated,
//! newest-first list of the actual diff samples.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::analysis::classify::RegressionClassifier;
use crate::analysis::counters::LiveCounters;
use crate::error::AppError;
use crate::storage::{DiffStore, EndpointAggregation, FieldAggregation, SamplePage};

const DEFAULT_SAMPLE_LIMIT: usize = 20;
const MAX_SAMPLE_LIMIT: usize = 100;
/// Cap on `offset` so a pathological value can't trigger an unbounded scan.
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

/// `GET /diffs/paths` — list the field paths that have diffs, per endpoint.
/// With `?endpoint=<ep>` it returns just that endpoint (404 if unknown).
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

/// `GET /diffs/detail?endpoint=<ep>&path=<p>` — aggregated stats for one field
/// plus a paginated, newest-first list of the actual diff samples at that path.
pub async fn diff_detail(
    State(store): State<Arc<dyn DiffStore>>,
    State(classifier): State<RegressionClassifier>,
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

    // Nothing recorded for this endpoint+path: no aggregation field and no
    // samples at the start of the stream.
    if field.is_none() && samples.items.is_empty() && !samples.has_more && offset == 0 {
        return Err(AppError::NotFound(format!(
            "no diffs recorded for endpoint '{}' path '{}'",
            query.endpoint, query.path
        )));
    }

    // Derive the verdict and percentages from the stored raw counts at read
    // time against the live thresholds (the store persists counts only).
    let counts = FieldAggregation {
        raw_count,
        noise_count,
    };
    let is_regression = classifier.is_regression(&counts, total);

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

#[derive(Debug, Deserialize)]
pub struct ResetQuery {
    pub endpoint: String,
}

/// `DELETE /diffs?endpoint=<ep>` — clear all recorded statistics for one
/// endpoint: its stored aggregation counts and any counts still buffered in the
/// live counters. Per-request samples are left to age out via the stream cap.
/// 404 if the endpoint has no recorded statistics.
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

    // Clear the buffer first so an in-flight flush can't re-add stale counts
    // after the store is cleared.
    counters.reset_endpoint(&query.endpoint);
    store.reset_aggregation(&query.endpoint).await?;

    Ok(StatusCode::NO_CONTENT)
}
