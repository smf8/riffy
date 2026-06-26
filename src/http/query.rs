use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::analysis::engine::{DiffEngine, EndpointCounts};
use crate::analysis::suppress::SuppressRules;
use crate::error::AppError;
use crate::storage::{RawSample, SampleStore};

const DEFAULT_SAMPLE_LIMIT: usize = 20;
const MAX_SAMPLE_LIMIT: usize = 100;
// Cap on `offset` to prevent unbounded scans from pathological values.
const MAX_SAMPLE_OFFSET: usize = 100_000;

fn exclude_rules(endpoint: &str, exclude: Option<&str>) -> Result<SuppressRules, AppError> {
    let patterns: Vec<String> = exclude
        .map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|p| !p.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    SuppressRules::for_endpoint(endpoint, &patterns)
        .map_err(|e| AppError::BadRequest(format!("invalid exclude pattern: {e}")))
}

#[derive(Debug, Serialize)]
pub struct PathSummary {
    pub path: String,
    pub raw_count: u64,
    pub noise_count: u64,
    pub is_regression: bool,
}

#[derive(Debug, Serialize)]
pub struct EndpointPaths {
    pub endpoint: String,
    pub total: u64,
    pub regressions: usize,
    pub paths: Vec<PathSummary>,
    pub last_updated: DateTime<Utc>,
}

fn endpoint_paths(engine: &DiffEngine, counts: EndpointCounts) -> EndpointPaths {
    let mut paths: Vec<PathSummary> = counts
        .fields
        .iter()
        .map(|(path, field)| PathSummary {
            path: path.clone(),
            raw_count: field.raw_count,
            noise_count: field.noise_count,
            is_regression: engine.is_regression(&counts.endpoint, path, field, counts.total),
        })
        .collect();
    paths.sort_by(|a, b| a.path.cmp(&b.path));
    let regressions = paths.iter().filter(|p| p.is_regression).count();
    EndpointPaths {
        endpoint: counts.endpoint,
        total: counts.total,
        regressions,
        paths,
        last_updated: counts.last_updated,
    }
}

#[derive(Debug, Deserialize)]
pub struct PathsQuery {
    pub endpoint: Option<String>,
    pub exclude: Option<String>,
}

pub async fn list_paths(
    State(store): State<Arc<dyn SampleStore>>,
    State(engine): State<Arc<DiffEngine>>,
    Query(query): Query<PathsQuery>,
) -> Result<Response, AppError> {
    match query.endpoint {
        Some(endpoint) => {
            let extra = exclude_rules(&endpoint, query.exclude.as_deref())?;
            let samples = store.fetch_samples(&endpoint).await?;
            let counts = engine
                .aggregate(&endpoint, &samples, &extra)
                .ok_or_else(|| {
                    AppError::NotFound(format!("no diffs recorded for endpoint '{endpoint}'"))
                })?;
            Ok(Json(endpoint_paths(&engine, counts)).into_response())
        }
        None => {
            let empty = SuppressRules::default();
            let mut endpoints: Vec<EndpointPaths> = Vec::new();
            for endpoint in store.list_endpoints().await? {
                let samples = store.fetch_samples(&endpoint).await?;
                if let Some(counts) = engine.aggregate(&endpoint, &samples, &empty) {
                    endpoints.push(endpoint_paths(&engine, counts));
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
    pub exclude: Option<String>,
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
    let extra = exclude_rules(&query.endpoint, query.exclude.as_deref())?;

    let samples = store.fetch_samples(&query.endpoint).await?;
    if samples.is_empty() {
        return Err(AppError::NotFound(format!(
            "no diffs recorded for endpoint '{}'",
            query.endpoint
        )));
    }

    let detail = engine.detail(
        &query.endpoint,
        &query.path,
        &samples,
        &extra,
        limit,
        offset,
    );

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

#[derive(Debug, Deserialize)]
pub struct SampleQuery {
    pub endpoint: String,
    pub id: String,
}

#[derive(Debug, Serialize)]
struct UpstreamResponseView {
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<Value>,
}

impl UpstreamResponseView {
    fn new(status: Option<u16>, body: Option<&str>, headers: Option<&str>) -> Self {
        Self {
            status,
            body: body.map(parse_body),
            headers: headers.map(parse_body),
        }
    }
}

fn parse_body(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_owned()))
}

pub async fn get_sample(
    State(store): State<Arc<dyn SampleStore>>,
    Query(query): Query<SampleQuery>,
) -> Result<Response, AppError> {
    let sample = store
        .get_sample(&query.endpoint, &query.id)
        .await?
        .ok_or_else(|| {
            AppError::NotFound(format!(
                "no sample '{}' for endpoint '{}'",
                query.id, query.endpoint
            ))
        })?;
    Ok(Json(sample_view(&sample)).into_response())
}

fn sample_view(sample: &RawSample) -> Value {
    json!({
        "id": sample.id,
        "endpoint": sample.endpoint,
        "timestamp": sample.timestamp,
        "request_curl": sample.request_curl,
        "baseline": UpstreamResponseView::new(
            Some(sample.baseline_status),
            Some(str::from_utf8(sample.baseline_body.as_ref()).unwrap_or_default()),
            Some(&sample.baseline_headers),
        ),
        "candidate": UpstreamResponseView::new(
            sample.candidate_status,
            sample.candidate_body.as_ref().and_then(|c| str::from_utf8(c.as_ref()).ok()),
            sample.candidate_headers.as_deref(),
        ),
        "control": UpstreamResponseView::new(
            sample.control_status,
            sample.control_body.as_ref().and_then(|c| str::from_utf8(c.as_ref()).ok()),
            sample.control_headers.as_deref(),
        ),
    })
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
            let paths = rules.paths_for(&endpoint);
            Json(EndpointSuppress { endpoint, paths }).into_response()
        }
        None => {
            let all: HashMap<String, Vec<String>> = rules.rules();
            Json(json!({ "rules": all })).into_response()
        }
    }
}

pub async fn put_suppress(
    State(engine): State<Arc<DiffEngine>>,
    Query(query): Query<SuppressEndpointQuery>,
    Json(body): Json<SuppressBody>,
) -> Result<Response, AppError> {
    engine
        .set_suppress(&query.endpoint, body.paths.clone())
        .map_err(|e| AppError::BadRequest(format!("invalid suppress pattern: {e}")))?;
    Ok(Json(EndpointSuppress {
        endpoint: query.endpoint,
        paths: body.paths,
    })
    .into_response())
}

pub async fn delete_suppress(
    State(engine): State<Arc<DiffEngine>>,
    Query(query): Query<SuppressEndpointQuery>,
) -> Result<StatusCode, AppError> {
    engine
        .set_suppress(&query.endpoint, Vec::new())
        .map_err(|e| AppError::BadRequest(format!("invalid suppress pattern: {e}")))?;
    Ok(StatusCode::NO_CONTENT)
}
