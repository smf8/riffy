use crate::config::Riffy;
use crate::endpoint::EndpointMatcher;
use crate::pipeline::AnalysisMessage;
use crate::proxy::upstream::UpstreamClient;
use crate::telemetry::metrics::track_proxy;
use axum::routing::any;
use axum::{middleware, Router};
use std::sync::Arc;
use tokio::sync::mpsc;

use super::handler;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Riffy>,
    pub upstream: Arc<UpstreamClient>,
    pub analysis_tx: mpsc::Sender<AnalysisMessage>,
    pub matcher: Arc<EndpointMatcher>,
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .fallback(any(handler::proxy_handler))
        .layer(middleware::from_fn_with_state(state.clone(), track_proxy))
        .with_state(state)
}
