use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

/// Top-level HTTP error boundary.
/// Each module defines its own error type that converts into this.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("{0}")]
    Upstream(#[source] crate::upstream::error::UpstreamError),

    #[error("storage error: {0}")]
    Storage(#[from] crate::storage::error::StoreError),

    #[error("{0}")]
    NotFound(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            AppError::Upstream(e) => {
                tracing::error!(error = %e, "upstream request error");
                (e.status_code(), e.to_string())
            }
            AppError::Storage(e) => {
                // Don't leak backend details (e.g. Redis URIs) to clients.
                tracing::error!(error = %e, "storage error serving query");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal storage error".to_owned(),
                )
            }
            AppError::NotFound(message) => (StatusCode::NOT_FOUND, message.clone()),
        };

        (status, Json(json!({ "error": message }))).into_response()
    }
}

impl From<crate::upstream::error::UpstreamError> for AppError {
    fn from(e: crate::upstream::error::UpstreamError) -> Self {
        AppError::Upstream(e)
    }
}

// impl From<crate::config::error::ConfigError> for AppError {
//     fn from(e: crate::config::error::ConfigError) -> Self {
//         AppError::Config(e)
//     }
// }
//
// impl From<crate::redis::error::RedisError> for AppError {
//     fn from(e: crate::redis::error::RedisError) -> Self {
//         AppError::Redis(e)
//     }
// }
//
// impl From<crate::compare::error::CompareError> for AppError {
//     fn from(e: crate::compare::error::CompareError) -> Self {
//         AppError::Compare(e)
//     }
// }
//
// impl From<crate::analysis::error::AnalysisError> for AppError {
//     fn from(e: crate::analysis::error::AnalysisError) -> Self {
//         AppError::Analysis(e)
//     }
// }
