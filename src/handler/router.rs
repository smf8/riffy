use crate::config::Riffy;
use crate::endpoint::EndpointMatcher;
use crate::pipeline::AnalysisMessage;
use crate::proxy::upstream::UpstreamClient;
use crate::telemetry::metrics::{render_metrics, track_proxy};
use axum::routing::{any, get};
use axum::{middleware, Router};
use metrics_exporter_prometheus::PrometheusHandle;
use std::sync::Arc;
use tokio::sync::mpsc;

use super::proxy;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Riffy>,
    pub upstream: Arc<UpstreamClient>,
    pub analysis_tx: mpsc::Sender<AnalysisMessage>,
    pub matcher: Arc<EndpointMatcher>,
}

/// Client-facing router: every path falls through to the proxy handler.
pub fn create_router(state: AppState) -> Router {
    Router::new()
        .fallback(any(proxy::proxy_handler))
        .layer(middleware::from_fn_with_state(state.clone(), track_proxy))
        .with_state(state)
}

/// Admin router: health check + Prometheus metrics. `/metrics` renders an
/// empty body when metrics are disabled (no handle installed).
pub fn admin_router(metrics_handle: Option<PrometheusHandle>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(render_metrics))
        .with_state(metrics_handle)
}

async fn healthz() -> axum::http::StatusCode {
    axum::http::StatusCode::NO_CONTENT
}
