use thiserror::Error;

#[derive(Debug, Error)]
pub enum AnalysisError {
    #[error("primary body is not valid json: {0}")]
    PrimaryJsonParse(#[source] serde_json::Error),
}
