//! Verifies the config-rs wiring: serde kebab-case renames, `#[serde(default)]`
//! fields, `humantime` durations, per-endpoint thresholds, and the internally
//! tagged storage backend enum all deserialize through `config::Config`.

use crate::config::{Riffy, StorageBackend};
use config::{Config, File, FileFormat};
use std::time::Duration;

const MINIMAL_YAML: &str = r#"
upstream:
  baseline: "http://localhost:9100"
  control: "http://localhost:9200"
  candidate: "http://localhost:9000"
endpoints:
  - pattern: "/api/v1/users/:id"
"#;

fn parse(yaml: &str) -> Riffy {
    Config::builder()
        .add_source(File::from_str(yaml, FileFormat::Yaml))
        .build()
        .unwrap()
        .try_deserialize()
        .unwrap()
}

#[test]
fn deserializes_minimal_config_with_defaults() {
    let cfg = parse(MINIMAL_YAML);

    // Omitted sections fall back to their defaults.
    assert_eq!(cfg.server.proxy_port, 7677);
    assert_eq!(cfg.server.admin_port, 7678);
    assert_eq!(cfg.pipeline.channel_capacity, 1024);
    assert_eq!(cfg.storage.stream_cap, 10_000);
    assert_eq!(cfg.storage.aggregation_interval, Duration::from_secs(1));
    assert!(matches!(cfg.storage.backend, StorageBackend::InMemory));
    assert_eq!(cfg.upstream.timeout, Duration::from_secs(30));
    assert!(!cfg.proxy.allow_http_side_effects);

    // OTLP export is off by default, with the endpoint pointing at local Jaeger.
    assert!(!cfg.logging.otlp.enabled);
    assert_eq!(cfg.logging.otlp.endpoint, "http://localhost:4318");

    // Endpoint without an explicit threshold gets the diffy defaults.
    let endpoint = &cfg.endpoints[0];
    assert_eq!(endpoint.threshold.relative, 20.0);
    assert_eq!(endpoint.threshold.absolute, 0.03);

    assert!(cfg.validate().is_ok());
}

#[test]
fn parses_redis_backend_and_per_endpoint_thresholds() {
    let yaml = r#"
upstream:
  baseline: "http://localhost:9100"
  control: "http://localhost:9200"
  candidate: "http://localhost:9000"
endpoints:
  - pattern: "/a/:id"
    threshold:
      relative: 50.0
      absolute: 0.1
storage:
  aggregation-interval: 5s
  stream-cap: 500
  backend:
    type: redis
    uri: "redis://example:6379"
"#;
    let cfg = parse(yaml);

    match cfg.storage.backend {
        StorageBackend::Redis { ref uri } => assert_eq!(uri, "redis://example:6379"),
        other => panic!("expected redis backend, got {other:?}"),
    }
    assert_eq!(cfg.storage.aggregation_interval, Duration::from_secs(5));
    assert_eq!(cfg.storage.stream_cap, 500);

    // The explicit per-endpoint threshold overrides the diffy defaults.
    assert_eq!(cfg.endpoints[0].threshold.relative, 50.0);
    assert_eq!(cfg.endpoints[0].threshold.absolute, 0.1);
}
