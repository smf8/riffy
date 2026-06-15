use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("redis command failed: {0}")]
    Redis(#[source] redis::RedisError),

    #[error("serialization failed: {0}")]
    Serialize(#[source] serde_json::Error),

    #[error("deserialization failed: {0}")]
    Deserialize(#[source] serde_json::Error),

    #[error("corrupt stored data: {0}")]
    Corrupt(String),
}
