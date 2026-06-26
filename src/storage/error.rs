use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("redis command failed: {0}")]
    Redis(#[source] redis::RedisError),

    #[error("corrupt stored data: {0}")]
    Corrupt(String),
}
