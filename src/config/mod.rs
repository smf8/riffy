use config::{Config, Environment, File, FileFormat};
use serde::Deserialize;
use std::time::Duration;

#[cfg(test)]
mod tests;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Riffy {
    pub service_name: String,
    pub proxy: Proxy,
    #[serde(default)]
    pub pipeline: Pipeline,
    pub upstream: Upstream,
    pub threshold: Threshold,
    pub endpoints: Vec<EndpointPattern>,
    /// Redis is opt-in: when this section is absent the diff store falls back
    /// to an in-memory implementation (no persistence across restarts).
    #[serde(default)]
    pub redis: Option<RedisConfig>,
    pub server: Server,
    pub logging: Logging,
    pub metrics: Metrics,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Proxy {
    pub port: u16,
    #[serde(default)]
    pub allow_http_side_effects: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Pipeline {
    /// Bounded capacity of the proxy → analysis-consumer channel. When the
    /// consumer falls behind, new messages are dropped with a warning
    /// (backpressure by shedding, never unbounded queueing).
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
}

fn default_channel_capacity() -> usize {
    1024
}

impl Default for Pipeline {
    fn default() -> Self {
        Self {
            channel_capacity: default_channel_capacity(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Upstream {
    pub baseline: String,
    pub control: String,
    pub candidate: String,
    #[serde(default = "default_protocol")]
    pub protocol: String,
    #[serde(default = "default_timeout", with = "humantime_serde")]
    pub timeout: Duration,
}

fn default_protocol() -> String {
    "http".to_string()
}

fn default_timeout() -> Duration {
    Duration::from_secs(30)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Threshold {
    #[serde(default = "default_relative_threshold")]
    pub relative: f64,
    #[serde(default = "default_absolute_threshold")]
    pub absolute: f64,
}

fn default_relative_threshold() -> f64 {
    20.0
}

fn default_absolute_threshold() -> f64 {
    0.03
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct EndpointPattern {
    pub pattern: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct RedisConfig {
    pub uri: String,
    #[serde(default = "default_stream_key")]
    pub stream_key: String,
    #[serde(default = "default_aggregation_interval", with = "humantime_serde")]
    pub aggregation_interval: Duration,
    #[serde(default = "default_agg_prefix")]
    pub aggregation_key_prefix: String,
}

fn default_stream_key() -> String {
    "riffy:diffs".to_string()
}

fn default_aggregation_interval() -> Duration {
    Duration::from_secs(1)
}

fn default_agg_prefix() -> String {
    "riffy:agg".to_string()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Server {
    pub address: String,
    pub port: u16,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Logging {
    #[serde(default = "default_log_level")]
    pub level: String,
}

fn default_log_level() -> String {
    "info".to_string()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Metrics {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_metrics_port")]
    pub port: u16,
}

fn default_metrics_port() -> u16 {
    9090
}

fn default_true() -> bool {
    true
}

pub fn load() -> anyhow::Result<Riffy> {
    // Layered: the example file is the base, an optional `config.yaml` overrides
    // it, and `RIFFY_`-prefixed env vars override both. Nested keys use a `__`
    // separator (e.g. `RIFFY_SERVER__PORT` → `server.port`).
    let config: Riffy = Config::builder()
        .add_source(File::new("config.example", FileFormat::Yaml).required(false))
        .add_source(File::new("config", FileFormat::Yaml).required(false))
        .add_source(Environment::with_prefix("RIFFY").separator("__"))
        .build()?
        .try_deserialize()?;
    config.validate()?;
    Ok(config)
}

impl Riffy {
    /// Startup-time sanity checks beyond serde's type validation.
    pub fn validate(&self) -> anyhow::Result<()> {
        use anyhow::ensure;

        ensure!(
            !self.service_name.trim().is_empty(),
            "riffy.service-name must not be empty"
        );

        for (role, host) in [
            ("baseline", &self.upstream.baseline),
            ("control", &self.upstream.control),
            ("candidate", &self.upstream.candidate),
        ] {
            ensure!(!host.trim().is_empty(), "upstream.{role} must not be empty");
        }

        ensure!(
            matches!(self.upstream.protocol.as_str(), "http" | "https"),
            "upstream.protocol must be http or https, got '{}'",
            self.upstream.protocol
        );

        for endpoint in &self.endpoints {
            ensure!(
                endpoint.pattern.starts_with('/'),
                "endpoint pattern '{}' must start with '/'",
                endpoint.pattern
            );
        }

        ensure!(
            self.threshold.relative >= 0.0,
            "threshold.relative must be >= 0"
        );
        ensure!(
            self.threshold.absolute >= 0.0,
            "threshold.absolute must be >= 0"
        );
        ensure!(
            self.proxy.port != self.server.port,
            "proxy.port and server.port must differ"
        );
        ensure!(
            self.pipeline.channel_capacity > 0,
            "pipeline.channel-capacity must be > 0"
        );
        if let Some(redis) = &self.redis {
            ensure!(!redis.uri.trim().is_empty(), "redis.uri must not be empty");
        }

        Ok(())
    }
}
