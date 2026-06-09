use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

/// Top-level HTTP error boundary.
/// Each module defines its own error type that converts into this.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("{0}")]
    Proxy(#[source] crate::proxy::error::ProxyError),
    // #[error("{0}")]
    // Config(#[source] crate::config::error::ConfigError),
    //
    // #[error("{0}")]
    // Redis(#[source] crate::redis::error::RedisError),
    //
    // #[error("{0}")]
    // Compare(#[source] crate::compare::error::CompareError),
    //
    // #[error("{0}")]
    // Analysis(#[source] crate::analysis::error::AnalysisError),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            AppError::Proxy(e) => (e.status_code(), e.to_string()),
        };

        tracing::error!(error = %self, "request error");

        (status, Json(json!({ "error": message }))).into_response()
    }
}

impl From<crate::proxy::error::ProxyError> for AppError {
    fn from(e: crate::proxy::error::ProxyError) -> Self {
        AppError::Proxy(e)
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
