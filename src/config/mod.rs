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

#[derive(Debug, Deserialize, garde::Validate)]
#[serde(rename_all = "snake_case")]
pub struct Riffy {
    #[garde(dive)]
    pub proxy: Proxy,
    #[garde(dive)]
    pub pipeline: Pipeline,
    #[garde(dive)]
    pub upstream: Upstream,
    #[garde(dive)]
    pub endpoints: Vec<EndpointConfig>,
    #[garde(dive)]
    pub storage: Storage,
    #[garde(dive)]
    pub server: Server,
    #[garde(dive)]
    pub logging: Logging,
    #[garde(dive)]
    pub metrics: Metrics,
}

#[derive(Debug, Deserialize, garde::Validate)]
#[serde(rename_all = "snake_case")]
pub struct Proxy {
    #[garde(skip)]
    pub allow_http_side_effects: bool,
}

#[derive(Debug, Deserialize, garde::Validate)]
#[serde(rename_all = "snake_case")]
pub struct Pipeline {
    #[garde(range(min = 1))]
    pub channel_capacity: usize,
}

#[derive(Debug, Deserialize, garde::Validate)]
#[serde(rename_all = "snake_case")]
pub struct Upstream {
    #[garde(length(min = 1))]
    pub baseline: String,
    #[garde(length(min = 1))]
    pub control: String,
    #[garde(length(min = 1))]
    pub candidate: String,
    #[garde(skip)]
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
}

/// Per-field regression thresholds (diffy's noise filter). Defaults are the
/// diffy values: 20% relative, 0.03% absolute. These are per-endpoint, so they
/// stay in code rather than `default.yaml`.
#[derive(Debug, Clone, Copy, Deserialize, garde::Validate)]
#[serde(rename_all = "snake_case")]
pub struct Threshold {
    #[garde(range(min = 0.0))]
    #[serde(default = "default_relative_threshold")]
    pub relative: f64,
    #[garde(range(min = 0.0))]
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

#[derive(Debug, Deserialize, garde::Validate)]
#[serde(rename_all = "snake_case")]
pub struct EndpointConfig {
    #[garde(prefix("/"))]
    pub pattern: String,
    #[garde(dive)]
    #[serde(default)]
    pub threshold: Threshold,
    /// Dot-separated JSON paths to exclude from diff analysis for this endpoint.
    /// Subtree suppression: `"a.b"` also suppresses `"a.b.c"`, `"a.b.d.e"`, etc.
    #[garde(skip)]
    #[serde(default)]
    pub suppress_paths: Vec<String>,
}

/// Storage for diffs and aggregation snapshots. `aggregation-interval` and
/// `stream-cap` are common to every backend (they govern flush cadence and
/// sample retention regardless of where data lands); `backend` selects between
/// Redis and the in-memory store.
#[derive(Debug, Deserialize, garde::Validate)]
#[serde(rename_all = "snake_case")]
pub struct Storage {
    #[garde(skip)]
    #[serde(with = "humantime_serde")]
    pub aggregation_interval: Duration,
    #[garde(range(min = 1))]
    pub stream_cap: usize,
    /// Read/retention window: aggregation counts older than this age out, so the
    /// regression verdict reflects only recent traffic.
    #[garde(skip)]
    #[serde(with = "humantime_serde")]
    pub window: Duration,
    /// Time-bucket granularity within the window (counts are bucketed at this
    /// resolution).
    #[garde(skip)]
    #[serde(with = "humantime_serde")]
    pub bucket: Duration,
    #[garde(dive)]
    pub backend: StorageBackend,
}

#[derive(Debug, Deserialize, garde::Validate)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StorageBackend {
    InMemory,
    Redis {
        #[garde(length(min = 1))]
        uri: String,
    },
}

#[derive(Debug, Deserialize, garde::Validate)]
#[serde(rename_all = "snake_case")]
pub struct Server {
    #[garde(skip)]
    pub address: String,
    #[garde(skip)]
    pub proxy_port: u16,
    #[garde(skip)]
    pub admin_port: u16,
}

#[derive(Debug, Deserialize, garde::Validate)]
#[serde(rename_all = "snake_case")]
pub struct Logging {
    #[garde(skip)]
    pub level: String,
    /// OTLP trace export (to a Jaeger collector). Off by default; the endpoint
    /// still points at a local Jaeger so it is ready to enable.
    #[garde(dive)]
    pub otlp: Otlp,
}

#[derive(Debug, Deserialize, garde::Validate)]
#[serde(rename_all = "snake_case")]
pub struct Otlp {
    #[garde(skip)]
    pub enabled: bool,
    /// OTLP/HTTP base endpoint of the collector (Jaeger's OTLP receiver on
    /// 4318). The `/v1/traces` path is appended by the exporter.
    #[garde(skip)]
    pub endpoint: String,
}

#[derive(Debug, Deserialize, garde::Validate)]
#[serde(rename_all = "snake_case")]
pub struct Metrics {
    #[garde(skip)]
    pub enabled: bool,
    #[garde(skip)]
    pub port: u16,
}

pub fn load(cli: &CliOverrides) -> anyhow::Result<Riffy> {
    // Layered, lowest → highest priority: embedded defaults, the config file
    // (CLI `--config` path or `config` in the cwd), `RIFFY__` env vars (nested
    // via a `__` separator, e.g. `RIFFY__SERVER__PROXY_PORT`), then the CLI value
    // overrides. `config.example.yaml` is documentation only — not auto-loaded.
    let mut builder =
        Config::builder().add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Yaml));
    builder = match &cli.config_path {
        Some(path) => builder.add_source(File::from(path.as_path()).required(true)),
        None => builder.add_source(File::new("config", FileFormat::Yaml).required(false)),
    };
    builder = builder.add_source(
        Environment::with_prefix("RIFFY")
            .separator("__")
            .try_parsing(true),
    );
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

        <Self as garde::Validate>::validate(self).context("invalid configuration")?;

        // Cross-field checks that garde cannot express declaratively:
        ensure!(
            self.server.proxy_port != self.server.admin_port,
            "server.proxy-port and server.admin-port must differ"
        );
        ensure!(
            self.storage.bucket.as_secs() >= 1,
            "storage.bucket must be >= 1s"
        );
        ensure!(
            self.storage.window >= self.storage.bucket,
            "storage.window must be >= storage.bucket"
        );

        Ok(())
    }
}
