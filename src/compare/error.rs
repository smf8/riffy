use thiserror::Error;

#[derive(Debug, Error)]
pub enum CompareError {
    #[error("json parse error: {0}")]
    JsonParse(#[source] serde_json::Error),
}
