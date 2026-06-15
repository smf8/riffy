use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("config load error: {0}")]
    Load(#[source] config::ConfigError),
}
