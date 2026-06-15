//! Verifies the config-rs wiring: serde kebab-case renames, `#[serde(default)]`
//! fields, `humantime` durations, and the optional Redis section all
//! deserialize through `config::Config` into `Riffy`.

use crate::config::Riffy;
use config::{Config, File, FileFormat};
use std::time::Duration;

const MINIMAL_YAML: &str = r#"
service-name: "test"
proxy:
  port: 8880
upstream:
  baseline: "localhost:9100"
  control: "localhost:9200"
  candidate: "localhost:9000"
endpoints:
  - pattern: "/api/v1/users/:id"
threshold:
  relative: 20.0
  absolute: 0.03
server:
  address: "0.0.0.0"
  port: 8888
logging:
  level: info
metrics:
  enabled: true
  port: 9090
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

    assert_eq!(cfg.service_name, "test");
    assert_eq!(cfg.proxy.port, 8880);
    assert_eq!(cfg.server.port, 8888);

    // Omitted optional sections fall back to their defaults.
    assert!(cfg.redis.is_none());
    assert_eq!(cfg.pipeline.channel_capacity, 1024);
    assert_eq!(cfg.upstream.protocol, "http");
    assert_eq!(cfg.upstream.timeout, Duration::from_secs(30));
    assert!(!cfg.proxy.allow_http_side_effects);

    assert!(cfg.validate().is_ok());
}

#[test]
fn parses_redis_section_and_humantime_durations() {
    let yaml = format!(
        "{MINIMAL_YAML}\nredis:\n  uri: \"redis://localhost:6379\"\n  aggregation-interval: 5s\n"
    );
    let cfg = parse(&yaml);

    let redis = cfg.redis.expect("redis section present");
    assert_eq!(redis.uri, "redis://localhost:6379");
    assert_eq!(redis.aggregation_interval, Duration::from_secs(5));
    // Defaulted within the redis section.
    assert_eq!(redis.stream_key, "riffy:diffs");
}
