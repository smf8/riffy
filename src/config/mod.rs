use anyhow::Context;
use config::builder::{ConfigBuilder, DefaultState};
use config::{Config, Environment, File, FileFormat};
use serde::Deserialize;
use std::path::PathBuf;
use std::time::Duration;

#[cfg(test)]
mod tests;

#[derive(Debug, Default)]
pub struct CliOverrides {
    pub config_path: Option<PathBuf>,
    pub baseline: Option<String>,
    pub control: Option<String>,
    pub candidate: Option<String>,
    pub endpoints: Vec<String>,
}

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
    pub jaeger: Jaeger,
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

/// diffy defaults: 20% relative, 0.03% absolute. Kept in code (not default.yaml)
/// because they are per-endpoint and duplicated into each EndpointConfig.
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

fn default_sample_rate() -> f64 {
    1.0
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
    /// JSON paths excluded from diff analysis. Three match modes (see
    /// `analysis::suppress`): plain subtree (`"a.b"` also hides `"a.b.c"`),
    /// `*` glob (one segment), and `re:<regex>` (matches the field or any of its
    /// children).
    #[garde(skip)]
    #[serde(default)]
    pub suppress_paths: Vec<String>,
    /// Fraction of requests to fan out to candidate/control (0.0–1.0).
    /// Sampled-out requests are still proxied baseline-only; analysis is skipped.
    #[garde(range(min = 0.0, max = 1.0))]
    #[serde(default = "default_sample_rate")]
    pub sample_rate: f64,
    /// Capture the originating request as a replayable curl on each stored diff.
    /// Opt-in because it persists request headers/body to storage.
    #[garde(skip)]
    #[serde(default)]
    pub capture_request_curl: bool,
    /// Store credential header values verbatim in the captured curl.
    /// When false, credential headers are listed but their values are redacted.
    /// Only meaningful when `capture_request_curl` is set.
    #[garde(skip)]
    #[serde(default)]
    pub store_credentials_header: bool,
}

#[derive(Debug, Deserialize, garde::Validate)]
#[serde(rename_all = "snake_case")]
pub struct Storage {
    /// Per-endpoint cap on retained raw samples; oldest is dropped past it.
    #[garde(range(min = 1))]
    pub sample_cap: usize,
    /// Samples older than this window are ignored at read time, so the regression
    /// verdict reflects only recent traffic.
    #[garde(skip)]
    #[serde(with = "humantime_serde")]
    pub window: Duration,
    /// A decoded upstream body over this size is not stored: baseline over the cap
    /// skips the whole sample, candidate/control over it are stored without a body.
    #[garde(range(min = 1))]
    pub max_body_bytes: usize,
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
}

#[derive(Debug, Deserialize, garde::Validate)]
#[serde(rename_all = "snake_case")]
pub struct Jaeger {
    #[garde(skip)]
    pub enabled: bool,
    /// OTLP/HTTP base endpoint of the Jaeger collector (port 4318).
    /// The exporter appends `/v1/traces` automatically.
    #[garde(skip)]
    pub endpoint: String,
    /// Uses `TraceIdRatioBased` wrapped in `ParentBased`, so child spans
    /// follow the parent's sampling decision.
    #[garde(range(min = 0.0, max = 1.0))]
    pub sampling_rate: f64,
}

#[derive(Debug, Deserialize, garde::Validate)]
#[serde(rename_all = "snake_case")]
pub struct Metrics {
    #[garde(skip)]
    pub enabled: bool,
}

pub fn load(cli: &CliOverrides) -> anyhow::Result<Riffy> {
    // Priority (lowest → highest): embedded defaults, config file (CLI path or
    // `config` in cwd), `RIFFY__` env vars (`__` separator), then CLI overrides.
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
    pub fn validate(&self) -> anyhow::Result<()> {
        use anyhow::ensure;

        <Self as garde::Validate>::validate(self).context("invalid configuration")?;

        // Cross-field checks that garde cannot express declaratively:
        ensure!(
            self.server.proxy_port != self.server.admin_port,
            "server.proxy-port and server.admin-port must differ"
        );

        // Compiling the suppression rules validates any `re:` regex patterns.
        for endpoint in &self.endpoints {
            crate::analysis::suppress::SuppressRules::compile(&endpoint.suppress_paths)
                .with_context(|| {
                    format!("invalid suppress_paths for endpoint '{}'", endpoint.pattern)
                })?;
        }

        Ok(())
    }
}
