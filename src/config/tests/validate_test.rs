use crate::config::*;
use std::time::Duration;

fn valid_config() -> Riffy {
    Riffy {
        service_name: "test-service".to_owned(),
        proxy: Proxy {
            port: 8880,
            allow_http_side_effects: false,
        },
        upstream: Upstream {
            primary: "localhost:9100".to_owned(),
            secondary: "localhost:9200".to_owned(),
            candidate: "localhost:9000".to_owned(),
            protocol: "http".to_owned(),
            timeout: Duration::from_secs(30),
        },
        threshold: Threshold {
            relative: 20.0,
            absolute: 0.03,
        },
        endpoints: vec![EndpointPattern {
            pattern: "/api/v1/users/:id".to_owned(),
        }],
        redis: Some(RedisConfig {
            uri: "redis://localhost:6379".to_owned(),
            stream_key: "riffy:diffs".to_owned(),
            aggregation_interval: Duration::from_secs(10),
            aggregation_key_prefix: "riffy:agg".to_owned(),
        }),
        server: Server {
            address: "0.0.0.0".to_owned(),
            port: 8888,
        },
        logging: Logging {
            level: "info".to_owned(),
        },
        metrics: Metrics {
            enabled: true,
            port: 9090,
        },
    }
}

#[test]
fn valid_config_passes() {
    assert!(valid_config().validate().is_ok());
}

#[test]
fn empty_service_name_fails() {
    let mut cfg = valid_config();
    cfg.service_name = "  ".to_owned();
    assert!(cfg.validate().is_err());
}

#[test]
fn empty_upstream_fails() {
    let mut cfg = valid_config();
    cfg.upstream.candidate = String::new();
    assert!(cfg.validate().is_err());
}

#[test]
fn invalid_protocol_fails() {
    let mut cfg = valid_config();
    cfg.upstream.protocol = "ftp".to_owned();
    assert!(cfg.validate().is_err());
}

#[test]
fn pattern_without_leading_slash_fails() {
    let mut cfg = valid_config();
    cfg.endpoints[0].pattern = "api/v1/users/:id".to_owned();
    assert!(cfg.validate().is_err());
}

#[test]
fn negative_threshold_fails() {
    let mut cfg = valid_config();
    cfg.threshold.relative = -1.0;
    assert!(cfg.validate().is_err());
}

#[test]
fn conflicting_ports_fail() {
    let mut cfg = valid_config();
    cfg.server.port = cfg.proxy.port;
    assert!(cfg.validate().is_err());
}

#[test]
fn empty_redis_uri_fails() {
    let mut cfg = valid_config();
    cfg.redis.as_mut().unwrap().uri = String::new();
    assert!(cfg.validate().is_err());
}

#[test]
fn absent_redis_section_is_valid() {
    let mut cfg = valid_config();
    cfg.redis = None;
    assert!(cfg.validate().is_ok());
}
