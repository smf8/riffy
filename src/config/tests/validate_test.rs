use crate::config::*;
use std::time::Duration;

fn valid_config() -> Riffy {
    Riffy {
        proxy: Proxy {
            allow_http_side_effects: false,
        },
        pipeline: Pipeline {
            channel_capacity: 1024,
        },
        upstream: Upstream {
            baseline: "http://localhost:9100".to_owned(),
            control: "http://localhost:9200".to_owned(),
            candidate: "http://localhost:9000".to_owned(),
            timeout: Duration::from_secs(30),
        },
        endpoints: vec![EndpointConfig {
            pattern: "/api/v1/users/:id".to_owned(),
            threshold: Threshold {
                relative: 20.0,
                absolute: 0.03,
            },
            suppress_paths: vec![],
            sample_rate: 1.0,
            capture_request_curl: false,
            store_credentials_header: false,
        }],
        storage: Storage {
            sample_cap: 10_000,
            window: Duration::from_secs(3600),
            max_body_bytes: 262_144,
            backend: StorageBackend::Redis {
                uri: "redis://localhost:6379".to_owned(),
            },
        },
        server: Server {
            address: "0.0.0.0".to_owned(),
            proxy_port: 7677,
            admin_port: 7678,
        },
        logging: Logging {
            level: "info".to_owned(),
        },
        jaeger: Jaeger {
            enabled: false,
            endpoint: "http://localhost:4318".to_owned(),
            sampling_rate: 1.0,
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
fn empty_upstream_fails() {
    let mut cfg = valid_config();
    cfg.upstream.candidate = String::new();
    assert!(cfg.validate().is_err());
}

#[test]
fn pattern_without_leading_slash_fails() {
    let mut cfg = valid_config();
    cfg.endpoints[0].pattern = "api/v1/users/:id".to_owned();
    assert!(cfg.validate().is_err());
}

#[test]
fn negative_endpoint_threshold_fails() {
    let mut cfg = valid_config();
    cfg.endpoints[0].threshold.relative = -1.0;
    assert!(cfg.validate().is_err());
}

#[test]
fn conflicting_ports_fail() {
    let mut cfg = valid_config();
    cfg.server.admin_port = cfg.server.proxy_port;
    assert!(cfg.validate().is_err());
}

#[test]
fn empty_redis_uri_fails() {
    let mut cfg = valid_config();
    cfg.storage.backend = StorageBackend::Redis { uri: String::new() };
    assert!(cfg.validate().is_err());
}

#[test]
fn in_memory_backend_is_valid() {
    let mut cfg = valid_config();
    cfg.storage.backend = StorageBackend::InMemory;
    assert!(cfg.validate().is_ok());
}

#[test]
fn zero_channel_capacity_fails() {
    let mut cfg = valid_config();
    cfg.pipeline.channel_capacity = 0;
    assert!(cfg.validate().is_err());
}

#[test]
fn zero_sample_cap_fails() {
    let mut cfg = valid_config();
    cfg.storage.sample_cap = 0;
    assert!(cfg.validate().is_err());
}

#[test]
fn zero_max_body_bytes_fails() {
    let mut cfg = valid_config();
    cfg.storage.max_body_bytes = 0;
    assert!(cfg.validate().is_err());
}

#[test]
fn sampling_rate_above_one_fails() {
    let mut cfg = valid_config();
    cfg.jaeger.sampling_rate = 1.1;
    assert!(cfg.validate().is_err());
}

#[test]
fn sampling_rate_below_zero_fails() {
    let mut cfg = valid_config();
    cfg.jaeger.sampling_rate = -0.1;
    assert!(cfg.validate().is_err());
}

#[test]
fn sampling_rate_boundary_values_are_valid() {
    let mut cfg = valid_config();
    cfg.jaeger.sampling_rate = 0.0;
    assert!(cfg.validate().is_ok());
    cfg.jaeger.sampling_rate = 1.0;
    assert!(cfg.validate().is_ok());
}

#[test]
fn invalid_suppress_regex_fails() {
    let mut cfg = valid_config();
    cfg.endpoints[0].suppress_paths = vec!["re:(".to_owned()];
    assert!(cfg.validate().is_err());
}

#[test]
fn valid_suppress_regex_passes() {
    let mut cfg = valid_config();
    cfg.endpoints[0].suppress_paths = vec!["re:.*_at$".to_owned(), "meta.*".to_owned()];
    assert!(cfg.validate().is_ok());
}

#[test]
fn endpoint_sample_rate_above_one_fails() {
    let mut cfg = valid_config();
    cfg.endpoints[0].sample_rate = 1.1;
    assert!(cfg.validate().is_err());
}

#[test]
fn endpoint_sample_rate_below_zero_fails() {
    let mut cfg = valid_config();
    cfg.endpoints[0].sample_rate = -0.1;
    assert!(cfg.validate().is_err());
}

#[test]
fn endpoint_sample_rate_boundary_values_are_valid() {
    let mut cfg = valid_config();
    cfg.endpoints[0].sample_rate = 0.0;
    assert!(cfg.validate().is_ok());
    cfg.endpoints[0].sample_rate = 1.0;
    assert!(cfg.validate().is_ok());
}
