//! End-to-end test: three mock upstreams behind the riffy proxy, verifying
//! the client always receives the baseline response and the analysis pipeline
//! records diffs through the `DiffStore` boundary.

use std::future::IntoFuture;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::routing::any;
use axum::{Json, Router};
use riffy::analysis::counters::LiveCounters;
use riffy::config::{
    EndpointPattern, Logging, Metrics, Pipeline, Proxy, Riffy, Server, Threshold, Upstream,
};
use riffy::endpoint::EndpointMatcher;
use riffy::http::router::{create_router, AppState};
use riffy::pipeline::consumer::Consumer;
use riffy::storage::{DiffEntry, InMemoryDiffStore};
use riffy::upstream::UpstreamClient;
use serde_json::{json, Value};

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
        service_name: "riffy-test".to_owned(),
        proxy: Proxy {
            port: 0,
            allow_http_side_effects: false,
        },
        pipeline: Pipeline {
            channel_capacity: 1024,
        },
        upstream: Upstream {
            baseline: String::new(),
            control: String::new(),
            candidate: String::new(),
            protocol: "http".to_owned(),
            timeout: Duration::from_secs(5),
        },
        threshold: Threshold {
            relative: 20.0,
            absolute: 0.03,
        },
        endpoints: vec![EndpointPattern {
            pattern: "/api/v1/users/:id".to_owned(),
        }],
        // The proxy integration test drives the in-memory store directly, so
        // the config needs no redis section.
        redis: None,
        server: Server {
            address: "127.0.0.1".to_owned(),
            port: 1,
        },
        logging: Logging {
            level: "info".to_owned(),
        },
        metrics: Metrics {
            enabled: false,
            port: 0,
        },
    }
}

struct TestProxy {
    addr: SocketAddr,
    store: Arc<InMemoryDiffStore>,
}

/// Boot the full stack — proxy router + analysis consumer — against the given
/// upstream addresses, with an in-memory store standing in for Redis.
async fn spawn_proxy(
    baseline: SocketAddr,
    control: SocketAddr,
    candidate: SocketAddr,
) -> TestProxy {
    let upstream = UpstreamClient::new(
        baseline.to_string(),
        control.to_string(),
        candidate.to_string(),
        "http".to_owned(),
        Duration::from_secs(5),
    );

    let (analysis_tx, analysis_rx) = riffy::pipeline::channel(1024);
    let collector = Arc::new(LiveCounters::new());
    let store = Arc::new(InMemoryDiffStore::new());
    let matcher = Arc::new(EndpointMatcher::new(&["/api/v1/users/:id".to_owned()]));

    Consumer::new(
        analysis_rx,
        matcher.clone(),
        collector,
        store.clone(),
        Duration::from_secs(3600),
    )
    .spawn();

    let state = AppState {
        config: Arc::new(test_config()),
        upstream: Arc::new(upstream),
        analysis_tx,
        matcher,
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(axum::serve(listener, create_router(state)).into_future());

    TestProxy { addr, store }
}

/// Test HTTP client that ignores HTTP_PROXY/HTTPS_PROXY from the environment
/// so localhost servers are reached directly.
fn http_client() -> reqwest::Client {
    reqwest::Client::builder().no_proxy().build().unwrap()
}

/// Poll the store until at least one diff entry lands (the pipeline is async).
async fn wait_for_entries(store: &InMemoryDiffStore) -> Vec<DiffEntry> {
    for _ in 0..200 {
        let entries = store.entries().await;
        if !entries.is_empty() {
            return entries;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("no diff entries arrived within 2s");
}

#[tokio::test]
async fn client_gets_baseline_response_and_diffs_are_recorded() {
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

    // Client must always see the baseline response.
    let body: Value = response.json().await.unwrap();
    assert_eq!(body, baseline_body);

    // The pipeline asynchronously records the candidate regression.
    let entries = wait_for_entries(&proxy.store).await;
    assert_eq!(entries.len(), 1);

    let entry = &entries[0];
    assert_eq!(entry.endpoint, "/api/v1/users/:id");
    assert!(entry.raw_fields.contains_key("name"));
    assert!(entry.noise_fields.is_empty());
    assert_eq!(entry.baseline_status, 200);
    assert_eq!(entry.candidate_status, Some(200));
}

#[tokio::test]
async fn identical_upstreams_produce_no_diff_entries() {
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

    // Give the background pipeline a moment, then confirm nothing was stored.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(proxy.store.entries().await.is_empty());
}

#[tokio::test]
async fn mutating_methods_are_blocked() {
    let body = json!({"a": 1});
    let baseline = spawn_json_upstream(body.clone()).await;
    let control = spawn_json_upstream(body.clone()).await;
    let candidate = spawn_json_upstream(body.clone()).await;

    let proxy = spawn_proxy(baseline, control, candidate).await;

    let client = http_client();
    let response = client
        .post(format!("http://{}/api/v1/users/1", proxy.addr))
        .body("{}")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 405);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(proxy.store.entries().await.is_empty());
}
