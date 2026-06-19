use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::analysis::engine::{DiffEngine, EndpointCounts};
use crate::error::AppError;
use crate::storage::SampleStore;

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

impl From<EndpointCounts> for EndpointPaths {
    fn from(counts: EndpointCounts) -> Self {
        let mut paths: Vec<String> = counts.fields.into_keys().collect();
        paths.sort();
        Self {
            endpoint: counts.endpoint,
            total: counts.total,
            paths,
            last_updated: counts.last_updated,
        }
    }
}

pub async fn list_paths(
    State(store): State<Arc<dyn SampleStore>>,
    State(engine): State<Arc<DiffEngine>>,
    Query(query): Query<PathsQuery>,
) -> Result<Response, AppError> {
    match query.endpoint {
        Some(endpoint) => {
            let samples = store.fetch_samples(&endpoint).await?;
            let counts = engine.aggregate(&endpoint, &samples).ok_or_else(|| {
                AppError::NotFound(format!("no diffs recorded for endpoint '{endpoint}'"))
            })?;
            Ok(Json(EndpointPaths::from(counts)).into_response())
        }
        None => {
            let mut endpoints: Vec<EndpointPaths> = Vec::new();
            for endpoint in store.list_endpoints().await? {
                let samples = store.fetch_samples(&endpoint).await?;
                if let Some(counts) = engine.aggregate(&endpoint, &samples) {
                    endpoints.push(EndpointPaths::from(counts));
                }
            }
            endpoints.sort_by(|a, b| a.endpoint.cmp(&b.endpoint));
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

pub async fn diff_detail(
    State(store): State<Arc<dyn SampleStore>>,
    State(engine): State<Arc<DiffEngine>>,
    Query(query): Query<DetailQuery>,
) -> Result<Response, AppError> {
    let limit = query
        .limit
        .unwrap_or(DEFAULT_SAMPLE_LIMIT)
        .clamp(1, MAX_SAMPLE_LIMIT);
    let offset = query.offset.unwrap_or(0).min(MAX_SAMPLE_OFFSET);

    let samples = store.fetch_samples(&query.endpoint).await?;
    if samples.is_empty() {
        return Err(AppError::NotFound(format!(
            "no diffs recorded for endpoint '{}'",
            query.endpoint
        )));
    }

    let detail = engine.detail(&query.endpoint, &query.path, &samples, limit, offset);

    if detail.raw_count == 0
        && detail.noise_count == 0
        && detail.samples.items.is_empty()
        && !detail.samples.has_more
        && offset == 0
    {
        return Err(AppError::NotFound(format!(
            "no diffs recorded for endpoint '{}' path '{}'",
            query.endpoint, query.path
        )));
    }

    Ok(Json(detail).into_response())
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
    State(store): State<Arc<dyn SampleStore>>,
    Query(query): Query<ResetQuery>,
) -> Result<StatusCode, AppError> {
    let endpoints = store.list_endpoints().await?;
    if !endpoints.iter().any(|e| e == &query.endpoint) {
        return Err(AppError::NotFound(format!(
            "no statistics recorded for endpoint '{}'",
            query.endpoint
        )));
    }

    store.delete_endpoint(&query.endpoint).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
pub struct SuppressQuery {
    pub endpoint: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SuppressEndpointQuery {
    pub endpoint: String,
}

#[derive(Debug, Deserialize)]
pub struct SuppressBody {
    pub paths: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct EndpointSuppress {
    pub endpoint: String,
    pub paths: Vec<String>,
}

pub async fn list_suppress(
    State(engine): State<Arc<DiffEngine>>,
    Query(query): Query<SuppressQuery>,
) -> Response {
    let rules = engine.rules();
    match query.endpoint {
        Some(endpoint) => {
            let paths = rules.paths_for(&endpoint).to_vec();
            Json(EndpointSuppress { endpoint, paths }).into_response()
        }
        None => {
            let all: HashMap<String, Vec<String>> = rules.rules().clone();
            Json(json!({ "rules": all })).into_response()
        }
    }
}

pub async fn put_suppress(
    State(engine): State<Arc<DiffEngine>>,
    Query(query): Query<SuppressEndpointQuery>,
    Json(body): Json<SuppressBody>,
) -> Response {
    engine.set_suppress(&query.endpoint, body.paths.clone());
    Json(EndpointSuppress {
        endpoint: query.endpoint,
        paths: body.paths,
    })
    .into_response()
}

pub async fn delete_suppress(
    State(engine): State<Arc<DiffEngine>>,
    Query(query): Query<SuppressEndpointQuery>,
) -> StatusCode {
    engine.set_suppress(&query.endpoint, Vec::new());
    StatusCode::NO_CONTENT
}
