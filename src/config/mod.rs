use config::{Config, Environment, File, FileFormat};
use serde::Deserialize;
use std::time::Duration;

#[cfg(test)]
mod tests;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Riffy {
    #[serde(default)]
    pub proxy: Proxy,
    #[serde(default)]
    pub pipeline: Pipeline,
    pub upstream: Upstream,
    /// Endpoints to analyze; each carries its own regression thresholds
    /// (defaulting to the diffy values when omitted).
    pub endpoints: Vec<EndpointConfig>,
    #[serde(default)]
    pub storage: Storage,
    #[serde(default)]
    pub server: Server,
    #[serde(default)]
    pub logging: Logging,
    #[serde(default)]
    pub metrics: Metrics,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct Proxy {
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
    /// Upstream addresses; the scheme is derived from the address itself
    /// (e.g. `https://host:port`), defaulting to `http://` when none is given.
    pub baseline: String,
    pub control: String,
    pub candidate: String,
    #[serde(default = "default_timeout", with = "humantime_serde")]
    pub timeout: Duration,
}

fn default_timeout() -> Duration {
    Duration::from_secs(30)
}

/// Per-field regression thresholds (diffy's noise filter). Defaults are the
/// diffy values: 20% relative, 0.03% absolute.
#[derive(Debug, Clone, Copy, Deserialize)]
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

impl Default for Threshold {
    fn default() -> Self {
        Self {
            relative: default_relative_threshold(),
            absolute: default_absolute_threshold(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct EndpointConfig {
    pub pattern: String,
    /// Per-endpoint regression thresholds; omitted → diffy defaults.
    #[serde(default)]
    pub threshold: Threshold,
}

/// Storage for diffs and aggregation snapshots. `aggregation-interval` and
/// `stream-cap` are common to every backend (they govern flush cadence and
/// sample retention regardless of where data lands); `backend` selects between
/// Redis and the in-memory store.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Storage {
    #[serde(default = "default_aggregation_interval", with = "humantime_serde")]
    pub aggregation_interval: Duration,
    #[serde(default = "default_stream_cap")]
    pub stream_cap: usize,
    #[serde(default)]
    pub backend: StorageBackend,
}

#[derive(Debug, Deserialize, Default)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum StorageBackend {
    /// In-memory store (no persistence across restarts) — the default.
    #[default]
    InMemory,
    /// Redis-backed store. Stream and aggregation keys are fixed constants
    /// (`storage::DIFF_STREAM_KEY` / `storage::AGGREGATION_KEY_PREFIX`).
    Redis { uri: String },
}

fn default_aggregation_interval() -> Duration {
    Duration::from_secs(1)
}

fn default_stream_cap() -> usize {
    10_000
}

impl Default for Storage {
    fn default() -> Self {
        Self {
            aggregation_interval: default_aggregation_interval(),
            stream_cap: default_stream_cap(),
            backend: StorageBackend::default(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Server {
    #[serde(default = "default_address")]
    pub address: String,
    #[serde(default = "default_proxy_port")]
    pub proxy_port: u16,
    #[serde(default = "default_admin_port")]
    pub admin_port: u16,
}

fn default_address() -> String {
    "0.0.0.0".to_string()
}

fn default_proxy_port() -> u16 {
    7677
}

fn default_admin_port() -> u16 {
    7678
}

impl Default for Server {
    fn default() -> Self {
        Self {
            address: default_address(),
            proxy_port: default_proxy_port(),
            admin_port: default_admin_port(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Logging {
    #[serde(default = "default_log_level")]
    pub level: String,
    /// OTLP trace export (to a Jaeger collector). Off by default; the endpoint
    /// still points at a local Jaeger so it is ready to enable.
    #[serde(default)]
    pub otlp: Otlp,
}

fn default_log_level() -> String {
    "info".to_string()
}

impl Default for Logging {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            otlp: Otlp::default(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Otlp {
    #[serde(default)]
    pub enabled: bool,
    /// OTLP/HTTP base endpoint of the collector (Jaeger's OTLP receiver on
    /// 4318). The `/v1/traces` path is appended by the exporter.
    #[serde(default = "default_otlp_endpoint")]
    pub endpoint: String,
}

fn default_otlp_endpoint() -> String {
    "http://localhost:4318".to_string()
}

impl Default for Otlp {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: default_otlp_endpoint(),
        }
    }
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

impl Default for Metrics {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            port: default_metrics_port(),
        }
    }
}

pub fn load() -> anyhow::Result<Riffy> {
    // Layered: the example file is the base, an optional `config.yaml` overrides
    // it, and `RIFFY_`-prefixed env vars override both. Nested keys use a `__`
    // separator (e.g. `RIFFY_SERVER__PROXY_PORT` → `server.proxy-port`).
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

        for (role, host) in [
            ("baseline", &self.upstream.baseline),
            ("control", &self.upstream.control),
            ("candidate", &self.upstream.candidate),
        ] {
            ensure!(!host.trim().is_empty(), "upstream.{role} must not be empty");
        }

        for endpoint in &self.endpoints {
            ensure!(
                endpoint.pattern.starts_with('/'),
                "endpoint pattern '{}' must start with '/'",
                endpoint.pattern
            );
            ensure!(
                endpoint.threshold.relative >= 0.0,
                "endpoint '{}' threshold.relative must be >= 0",
                endpoint.pattern
            );
            ensure!(
                endpoint.threshold.absolute >= 0.0,
                "endpoint '{}' threshold.absolute must be >= 0",
                endpoint.pattern
            );
        }

        ensure!(
            self.server.proxy_port != self.server.admin_port,
            "server.proxy-port and server.admin-port must differ"
        );
        ensure!(
            self.pipeline.channel_capacity > 0,
            "pipeline.channel-capacity must be > 0"
        );
        ensure!(
            self.storage.stream_cap > 0,
            "storage.stream-cap must be > 0"
        );
        if let StorageBackend::Redis { uri } = &self.storage.backend {
            ensure!(
                !uri.trim().is_empty(),
                "storage.backend.uri must not be empty"
            );
        }

        Ok(())
    }
}
