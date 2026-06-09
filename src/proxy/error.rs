use axum::http::StatusCode;
use thiserror::Error;

/// Errors from upstream proxy operations.
#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("upstream {target} timeout: {source}")]
    Timeout {
        target: String,
        #[source]
        source: reqwest::Error,
    },

    #[error("upstream {target} error: {source}")]
    Connection {
        target: String,
        #[source]
        source: reqwest::Error,
    },
}

impl ProxyError {
    pub fn timeout(target: impl Into<String>, source: reqwest::Error) -> Self {
        Self::Timeout {
            target: target.into(),
            source,
        }
    }

    pub fn connection(target: impl Into<String>, source: reqwest::Error) -> Self {
        Self::Connection {
            target: target.into(),
            source,
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            ProxyError::Timeout { .. } => StatusCode::GATEWAY_TIMEOUT,
            ProxyError::Connection { .. } => StatusCode::BAD_GATEWAY,
        }
    }
}
