use crate::config::{apply_cli_overrides, CliOverrides, Riffy, StorageBackend, DEFAULT_CONFIG};
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
        .add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Yaml))
        .add_source(File::from_str(yaml, FileFormat::Yaml))
        .build()
        .unwrap()
        .try_deserialize()
        .unwrap()
}

#[test]
fn embedded_defaults_fill_omitted_sections() {
    let cfg = parse(MINIMAL_YAML);

    // All of these come from the embedded default.yaml, not the user config.
    assert_eq!(cfg.server.proxy_port, 7677);
    assert_eq!(cfg.server.admin_port, 7678);
    assert_eq!(cfg.consumer.channel_capacity, 1024);
    assert_eq!(cfg.storage.sample_cap, 10_000);
    assert_eq!(cfg.storage.window, Duration::from_secs(3600));
    assert_eq!(cfg.storage.max_body_bytes, 262_144);
    assert!(matches!(cfg.storage.backend, StorageBackend::InMemory));
    assert_eq!(cfg.upstream.timeout, Duration::from_secs(30));
    assert!(!cfg.proxy.allow_http_side_effects);
    assert!(!cfg.jaeger.enabled);
    assert_eq!(cfg.jaeger.endpoint, "http://localhost:4318");
    assert_eq!(cfg.jaeger.sampling_rate, 1.0);

    // Endpoint without an explicit threshold gets the diffy defaults.
    let endpoint = &cfg.endpoints[0];
    assert_eq!(endpoint.threshold.relative, 20.0);
    assert_eq!(endpoint.threshold.absolute, 0.03);

    assert!(cfg.validate().is_ok());
}

#[test]
fn partial_storage_override_keeps_other_defaults() {
    // Override only one storage field; the rest must deep-merge from defaults.
    let yaml = format!("{MINIMAL_YAML}\nstorage:\n  max_body_bytes: 99\n");
    let cfg = parse(&yaml);

    assert_eq!(cfg.storage.max_body_bytes, 99);
    assert_eq!(cfg.storage.sample_cap, 10_000);
    assert_eq!(cfg.storage.window, Duration::from_secs(3600));
    assert!(matches!(cfg.storage.backend, StorageBackend::InMemory));
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
  sample_cap: 500
  window: 30m
  backend:
    type: redis
    uri: "redis://example:6379"
"#;
    let cfg = parse(yaml);

    match cfg.storage.backend {
        StorageBackend::Redis { ref uri } => assert_eq!(uri, "redis://example:6379"),
        other => panic!("expected redis backend, got {other:?}"),
    }
    assert_eq!(cfg.storage.window, Duration::from_secs(1800));
    assert_eq!(cfg.storage.sample_cap, 500);

    // The explicit per-endpoint threshold overrides the diffy defaults.
    assert_eq!(cfg.endpoints[0].threshold.relative, 50.0);
    assert_eq!(cfg.endpoints[0].threshold.absolute, 0.1);
}

#[test]
fn jaeger_defaults_and_override() {
    // Defaults from embedded config.
    let cfg = parse(MINIMAL_YAML);
    assert!(!cfg.jaeger.enabled);
    assert_eq!(cfg.jaeger.endpoint, "http://localhost:4318");
    assert_eq!(cfg.jaeger.sampling_rate, 1.0);

    // Override sampling_rate and endpoint.
    let yaml = format!(
        "{MINIMAL_YAML}\njaeger:\n  enabled: true\n  endpoint: \"http://j:4318\"\n  sampling_rate: 0.1\n"
    );
    let cfg = parse(&yaml);
    assert!(cfg.jaeger.enabled);
    assert_eq!(cfg.jaeger.endpoint, "http://j:4318");
    assert_eq!(cfg.jaeger.sampling_rate, 0.1);
}

#[test]
fn suppress_paths_parse_and_default_to_empty() {
    let yaml = r#"
upstream:
  baseline: "http://localhost:9100"
  control: "http://localhost:9200"
  candidate: "http://localhost:9000"
endpoints:
  - pattern: "/a/:id"
    suppress_paths:
      - "meta.ts"
      - "debug"
  - pattern: "/b"
"#;
    let cfg = parse(yaml);
    assert_eq!(
        cfg.endpoints[0].suppress_paths,
        vec!["meta.ts".to_owned(), "debug".to_owned()]
    );
    assert!(cfg.endpoints[1].suppress_paths.is_empty());
}

#[test]
fn capture_request_curl_flags_parse_and_default_to_false() {
    let yaml = r#"
upstream:
  baseline: "http://localhost:9100"
  control: "http://localhost:9200"
  candidate: "http://localhost:9000"
endpoints:
  - pattern: "/a/:id"
    capture_request_curl: true
    store_credentials_header: true
  - pattern: "/b"
"#;
    let cfg = parse(yaml);

    assert!(cfg.endpoints[0].capture_request_curl);
    assert!(cfg.endpoints[0].store_credentials_header);
    // Omitted on the second endpoint — both default to false.
    assert!(!cfg.endpoints[1].capture_request_curl);
    assert!(!cfg.endpoints[1].store_credentials_header);
}

#[test]
fn cli_overrides_supply_required_fields() {
    // Embedded defaults + CLI args only (no config file) is enough to run.
    let cli = CliOverrides {
        baseline: Some("http://b:1".to_owned()),
        control: Some("http://c:1".to_owned()),
        candidate: Some("http://d:1".to_owned()),
        endpoints: vec!["/x/:id".to_owned()],
        ..Default::default()
    };
    let builder = Config::builder().add_source(File::from_str(DEFAULT_CONFIG, FileFormat::Yaml));
    let builder = apply_cli_overrides(builder, &cli).unwrap();
    let cfg: Riffy = builder.build().unwrap().try_deserialize().unwrap();

    assert_eq!(cfg.upstream.baseline, "http://b:1");
    assert_eq!(cfg.upstream.control, "http://c:1");
    assert_eq!(cfg.upstream.candidate, "http://d:1");
    assert_eq!(cfg.endpoints.len(), 1);
    assert_eq!(cfg.endpoints[0].pattern, "/x/:id");
    // CLI endpoints fall back to the diffy default thresholds.
    assert_eq!(cfg.endpoints[0].threshold.relative, 20.0);

    assert!(cfg.validate().is_ok());
}
