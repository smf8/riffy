use crate::config::Riffy;
use crate::proxy::upstream::UpstreamClient;
use axum::routing::any;
use axum::Router;
use std::sync::Arc;
use tokio::sync::mpsc;

use super::handler;

pub struct AnalysisMessage {
    pub endpoint: String,
    pub method: String,
    #[allow(dead_code)]
    pub path: String,
    #[allow(dead_code)]
    pub primary_response: Option<UpstreamResponse>,
    #[allow(dead_code)]
    pub candidate_response: Option<UpstreamResponse>,
    #[allow(dead_code)]
    pub secondary_response: Option<UpstreamResponse>,
}

use crate::proxy::upstream::UpstreamResponse;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Riffy>,
    pub upstream: Arc<UpstreamClient>,
    pub analysis_tx: mpsc::Sender<AnalysisMessage>,
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .fallback(any(handler::proxy_handler))
        .with_state(state)
}
