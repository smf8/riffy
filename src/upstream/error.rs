use axum::http::StatusCode;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum UpstreamError {
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

impl UpstreamError {
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
            UpstreamError::Timeout { .. } => StatusCode::GATEWAY_TIMEOUT,
            UpstreamError::Connection { .. } => StatusCode::BAD_GATEWAY,
        }
    }
}
