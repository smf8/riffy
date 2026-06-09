use thiserror::Error;

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum AnalysisError {
    #[error("analysis error: {0}")]
    Internal(String),
}
