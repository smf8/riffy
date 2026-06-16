use anyhow::Context;
use config::builder::{ConfigBuilder, DefaultState};
use config::{Config, Environment, File, FileFormat};
use serde::Deserialize;
use std::path::PathBuf;
use std::time::Duration;

#[cfg(test)]
mod tests;

/// Minimal config values supplied on the command line. These override every
/// file/env source so an operator can run riffy without a config file.
#[derive(Debug, Default)]
pub struct CliOverrides {
    /// Explicit config file path; when set it replaces the default `config`
    /// lookup in the working directory.
    pub config_path: Option<PathBuf>,
    pub baseline: Option<String>,
    pub control: Option<String>,
    pub candidate: Option<String>,
    /// Endpoint patterns to analyze (each with default thresholds); when
    /// non-empty they replace the configured endpoint list.
    pub endpoints: Vec<String>,
}

/// Built-in defaults, embedded at compile time and layered first. Section
/// defaults live here rather than in per-field `#[serde(default)]` attributes;
/// see `default.yaml`.
pub(crate) const DEFAULT_CONFIG: &str = include_str!("default.yaml");

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Riffy {
    pub proxy: Proxy,
    pub pipeline: Pipeline,
    pub upstream: Upstream,
    /// Endpoints to analyze; each carries its own regression thresholds
    /// (defaulting to the diffy values when omitted).
    pub endpoints: Vec<EndpointConfig>,
    pub storage: Storage,
    pub server: Server,
    pub logging: Logging,
    pub metrics: Metrics,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Proxy {
    pub allow_http_side_effects: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Pipeline {
    /// Bounded capacity of the proxy → analysis-consumer channel. When the
    /// consumer falls behind, new messages are dropped with a warning
    /// (backpressure by shedding, never unbounded queueing).
    pub channel_capacity: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Upstream {
    /// Upstream addresses; the scheme is derived from the address itself
    /// (e.g. `https://host:port`), defaulting to `http://` when none is given.
    pub baseline: String,
    pub control: String,
    pub candidate: String,
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
}

/// Per-field regression thresholds (diffy's noise filter). Defaults are the
/// diffy values: 20% relative, 0.03% absolute. These are per-endpoint, so they
/// stay in code rather than `default.yaml`.
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
    #[serde(with = "humantime_serde")]
    pub aggregation_interval: Duration,
    pub stream_cap: usize,
    pub backend: StorageBackend,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum StorageBackend {
    /// In-memory store (no persistence across restarts) — the default.
    InMemory,
    /// Redis-backed store. Stream and aggregation keys are fixed constants
    /// (`storage::DIFF_STREAM_KEY` / `storage::AGGREGATION_KEY_PREFIX`).
    Redis { uri: String },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Server {
    pub address: String,
    pub proxy_port: u16,
    pub admin_port: u16,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Logging {
    pub level: String,
    /// OTLP trace export (to a Jaeger collector). Off by default; the endpoint
    /// still points at a local Jaeger so it is ready to enable.
    pub otlp: Otlp,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Otlp {
    pub enabled: bool,
    /// OTLP/HTTP base endpoint of the collector (Jaeger's OTLP receiver on
    /// 4318). The `/v1/traces` path is appended by the exporter.
    pub endpoint: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Metrics {
    pub enabled: bool,
    pub port: u16,
}

pub fn load(cli: &CliOverrides) -> anyhow::Result<Riffy> {
    // Layered, lowest → highest priority: embedded defaults, the config file
    // (CLI `--config` path or `config` in the cwd), `RIFFY_` env vars (nested
    // via a `__` separator, e.g. `RIFFY_SERVER__PROXY_PORT`), then the CLI value
    // overrides. `config.example.yaml` is documentation only — not auto-loaded.
    let mut builder =
        Config::builder().add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Yaml));
    builder = match &cli.config_path {
        Some(path) => builder.add_source(File::from(path.as_path()).required(true)),
        None => builder.add_source(File::new("config", FileFormat::Yaml).required(false)),
    };
    builder = builder.add_source(Environment::with_prefix("RIFFY").separator("__"));
    builder = apply_cli_overrides(builder, cli)?;

    let config: Riffy = builder.build()?.try_deserialize()?;
    config.validate()?;
    Ok(config)
}

/// Layer the CLI value overrides onto the config builder as the highest-priority
/// source. Built as JSON (via serde_json, so values are escaped correctly) and
/// merged like any other source: scalar upstream fields deep-merge, the
/// endpoint list replaces.
pub(crate) fn apply_cli_overrides(
    builder: ConfigBuilder<DefaultState>,
    cli: &CliOverrides,
) -> anyhow::Result<ConfigBuilder<DefaultState>> {
    use serde_json::{json, Map, Value};

    let mut root = Map::new();

    let mut upstream = Map::new();
    if let Some(v) = &cli.baseline {
        upstream.insert("baseline".to_owned(), json!(v));
    }
    if let Some(v) = &cli.control {
        upstream.insert("control".to_owned(), json!(v));
    }
    if let Some(v) = &cli.candidate {
        upstream.insert("candidate".to_owned(), json!(v));
    }
    if !upstream.is_empty() {
        root.insert("upstream".to_owned(), Value::Object(upstream));
    }

    if !cli.endpoints.is_empty() {
        let endpoints: Vec<Value> = cli
            .endpoints
            .iter()
            .map(|p| json!({ "pattern": p }))
            .collect();
        root.insert("endpoints".to_owned(), Value::Array(endpoints));
    }

    if root.is_empty() {
        return Ok(builder);
    }

    let json = serde_json::to_string(&Value::Object(root)).context("serializing CLI overrides")?;
    Ok(builder.add_source(File::from_str(&json, FileFormat::Json)))
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
