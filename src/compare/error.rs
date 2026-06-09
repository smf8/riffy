use thiserror::Error;

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum CompareError {
    #[error("json parse error: {0}")]
    JsonParse(#[source] serde_json::Error),
}
