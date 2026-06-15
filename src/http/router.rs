use super::{forward, query};
use crate::analysis::classify::RegressionClassifier;
use crate::analysis::counters::LiveCounters;
use crate::config::Riffy;
use crate::endpoint::EndpointMatcher;
use crate::pipeline::AnalysisMessage;
use crate::storage::DiffStore;
use crate::telemetry::metrics::{render_metrics, track_proxy};
use crate::upstream::client::UpstreamClient;
use axum::extract::FromRef;
use axum::routing::{any, delete, get};
use axum::{middleware, Router};
use metrics_exporter_prometheus::PrometheusHandle;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Riffy>,
    pub upstream: Arc<UpstreamClient>,
    pub analysis_tx: mpsc::Sender<AnalysisMessage>,
    pub matcher: Arc<EndpointMatcher>,
}

/// Client-facing router: every path falls through to the forwarding handler.
pub fn create_router(state: AppState) -> Router {
    Router::new()
        .fallback(any(forward::forward))
        .layer(middleware::from_fn_with_state(state.clone(), track_proxy))
        .with_state(state)
}

/// Shared state for the admin server: the optional Prometheus handle, the diff
/// store backing the read API, and the classifier used to derive regression
/// verdicts from stored raw counts at read time. `FromRef` lets each handler
/// extract only the substate it needs.
#[derive(Clone)]
pub struct AdminState {
    pub metrics: Option<PrometheusHandle>,
    pub store: Arc<dyn DiffStore>,
    pub classifier: RegressionClassifier,
    pub counters: Arc<LiveCounters>,
}

impl FromRef<AdminState> for Option<PrometheusHandle> {
    fn from_ref(state: &AdminState) -> Self {
        state.metrics.clone()
    }
}

impl FromRef<AdminState> for Arc<dyn DiffStore> {
    fn from_ref(state: &AdminState) -> Self {
        state.store.clone()
    }
}

impl FromRef<AdminState> for RegressionClassifier {
    fn from_ref(state: &AdminState) -> Self {
        state.classifier
    }
}

impl FromRef<AdminState> for Arc<LiveCounters> {
    fn from_ref(state: &AdminState) -> Self {
        state.counters.clone()
    }
}

/// Admin router: health check, Prometheus metrics, and the diff query API.
/// `/metrics` renders an empty body when metrics are disabled (no handle
/// installed).
pub fn admin_router(state: AdminState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(render_metrics))
        .route("/diffs/paths", get(query::list_paths))
        .route("/diffs/detail", get(query::diff_detail))
        .route("/diffs", delete(query::reset_stats))
        .with_state(state)
}

async fn healthz() -> axum::http::StatusCode {
    axum::http::StatusCode::NO_CONTENT
}
