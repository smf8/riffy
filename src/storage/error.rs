use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("redis command failed: {0}")]
    Redis(#[source] redis::RedisError),

    #[error("serialization failed: {0}")]
    Serialize(#[source] serde_json::Error),
}
