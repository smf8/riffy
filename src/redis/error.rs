use thiserror::Error;

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum RedisError {
    #[error("redis error: {0}")]
    Connection(#[source] redis::RedisError),
}
