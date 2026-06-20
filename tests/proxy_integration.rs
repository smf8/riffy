use std::future::IntoFuture;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::routing::any;
use axum::{Json, Router};
use riffy::analysis::classify::EndpointClassifiers;
use riffy::analysis::engine::DiffEngine;
use riffy::analysis::suppress::SuppressRules;
use riffy::config::{
    EndpointConfig, Jaeger, Logging, Metrics, Pipeline, Proxy, Riffy, Server, Storage,
    StorageBackend, Threshold, Upstream,
};
use riffy::endpoint::EndpointMatcher;
use riffy::http::router::{create_router, AppState};
use riffy::pipeline::consumer::Consumer;
use riffy::storage::{InMemorySampleStore, RawSample, SampleStore};
use riffy::upstream::UpstreamClient;
use serde_json::{json, Value};

const EP: &str = "/api/v1/users/:id";

async fn spawn_json_upstream(body: Value) -> SocketAddr {
    let app = Router::new().fallback(any(move || {
        let body = body.clone();
        async move { Json(body) }
    }));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(axum::serve(listener, app).into_future());
    addr
}

fn test_config() -> Riffy {
    Riffy {
        proxy: Proxy {
            allow_http_side_effects: false,
        },
        pipeline: Pipeline {
            channel_capacity: 1024,
        },
        upstream: Upstream {
            baseline: String::new(),
            control: String::new(),
            candidate: String::new(),
            timeout: Duration::from_secs(5),
        },
        endpoints: vec![EndpointConfig {
            pattern: EP.to_owned(),
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
            backend: StorageBackend::InMemory,
        },
        server: Server {
            address: "127.0.0.1".to_owned(),
            proxy_port: 1,
            admin_port: 2,
        },
        logging: Logging {
            level: "info".to_owned(),
        },
        jaeger: Jaeger {
            enabled: false,
            endpoint: "http://localhost:4318".to_owned(),
            sampling_rate: 1.0,
        },
        metrics: Metrics { enabled: false },
    }
}

struct TestProxy {
    addr: SocketAddr,
    store: Arc<InMemorySampleStore>,
}

async fn spawn_proxy(
    baseline: SocketAddr,
    control: SocketAddr,
    candidate: SocketAddr,
) -> TestProxy {
    spawn_proxy_with_config(baseline, control, candidate, test_config()).await
}

async fn spawn_proxy_with_config(
    baseline: SocketAddr,
    control: SocketAddr,
    candidate: SocketAddr,
    mut config: Riffy,
) -> TestProxy {
    config.upstream.baseline = baseline.to_string();
    config.upstream.control = control.to_string();
    config.upstream.candidate = candidate.to_string();

    let upstream = UpstreamClient::new(
        baseline.to_string(),
        control.to_string(),
        candidate.to_string(),
        Duration::from_secs(5),
    );

    let (analysis_tx, analysis_rx) = riffy::pipeline::channel(1024);
    let store = Arc::new(InMemorySampleStore::new());
    let matcher = Arc::new(EndpointMatcher::new(
        &config
            .endpoints
            .iter()
            .map(|e| e.pattern.clone())
            .collect::<Vec<_>>(),
    ));

    Consumer::new(
        analysis_rx,
        matcher.clone(),
        store.clone(),
        config.storage.max_body_bytes,
    )
    .spawn();

    let state = AppState {
        config: Arc::new(config),
        upstream: Arc::new(upstream),
        analysis_tx,
        matcher,
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(axum::serve(listener, create_router(state)).into_future());

    TestProxy { addr, store }
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder().no_proxy().build().unwrap()
}

async fn wait_for_samples(store: &InMemorySampleStore) -> Vec<RawSample> {
    for _ in 0..200 {
        let samples = store.fetch_samples(EP).await.unwrap();
        if !samples.is_empty() {
            return samples;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("no raw samples arrived within 2s");
}

fn engine() -> DiffEngine {
    DiffEngine::new(
        SuppressRules::from_config(&[]).unwrap(),
        EndpointClassifiers::from_config(&[]),
    )
}

#[tokio::test]
async fn client_gets_baseline_response_and_raw_sample_is_recorded() {
    let baseline_body = json!({"name": "alice", "version": 1});
    let baseline = spawn_json_upstream(baseline_body.clone()).await;
    let control = spawn_json_upstream(json!({"name": "alice", "version": 1})).await;
    let candidate = spawn_json_upstream(json!({"name": "bob", "version": 1})).await;

    let proxy = spawn_proxy(baseline, control, candidate).await;

    let response = http_client()
        .get(format!("http://{}/api/v1/users/42", proxy.addr))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    let body: Value = response.json().await.unwrap();
    assert_eq!(body, baseline_body);

    let samples = wait_for_samples(&proxy.store).await;
    assert_eq!(samples.len(), 1);
    let s = &samples[0];
    assert_eq!(s.endpoint, EP);
    assert_eq!(s.baseline_status, 200);
    assert_eq!(s.candidate_status, Some(200));
    assert!(s.candidate_body.is_some());

    // The diff (name) is derived at read time from the stored raw sample.
    let counts = engine()
        .aggregate(EP, &samples, &SuppressRules::default())
        .unwrap();
    assert!(counts.fields.contains_key("name"));
    assert!(!counts.fields.contains_key("version"));
}

#[tokio::test]
async fn identical_upstreams_still_record_a_sample() {
    let body = json!({"a": 1});
    let baseline = spawn_json_upstream(body.clone()).await;
    let control = spawn_json_upstream(body.clone()).await;
    let candidate = spawn_json_upstream(body.clone()).await;

    let proxy = spawn_proxy(baseline, control, candidate).await;

    let response = http_client()
        .get(format!("http://{}/api/v1/users/1", proxy.addr))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    // Producer records raw data unconditionally; the read-time diff finds nothing.
    let samples = wait_for_samples(&proxy.store).await;
    assert_eq!(samples.len(), 1);
    let counts = engine()
        .aggregate(EP, &samples, &SuppressRules::default())
        .unwrap();
    assert!(counts.fields.is_empty());
}

#[tokio::test]
async fn mutating_methods_skip_fanout_but_proxy_baseline() {
    let baseline_body = json!({"a": 1});
    let baseline = spawn_json_upstream(baseline_body.clone()).await;
    let control = spawn_json_upstream(baseline_body.clone()).await;
    let candidate = spawn_json_upstream(json!({"a": 2})).await;

    let proxy = spawn_proxy(baseline, control, candidate).await;

    let response = http_client()
        .post(format!("http://{}/api/v1/users/1", proxy.addr))
        .body("{}")
        .send()
        .await
        .unwrap();

    // Baseline response is always returned — reverse proxy role is preserved.
    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body, baseline_body);

    // No fan-out to candidate/control, so no sample is recorded.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(proxy.store.fetch_samples(EP).await.unwrap().is_empty());
}

#[tokio::test]
async fn sample_rate_zero_skips_fanout() {
    let baseline_body = json!({"name": "alice"});
    let baseline = spawn_json_upstream(baseline_body.clone()).await;
    let control = spawn_json_upstream(json!({"name": "alice"})).await;
    let candidate = spawn_json_upstream(json!({"name": "bob"})).await;

    let mut config = test_config();
    config.endpoints[0].sample_rate = 0.0;

    let proxy = spawn_proxy_with_config(baseline, control, candidate, config).await;

    let response = http_client()
        .get(format!("http://{}/api/v1/users/1", proxy.addr))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body, baseline_body);

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(proxy.store.fetch_samples(EP).await.unwrap().is_empty());
}
