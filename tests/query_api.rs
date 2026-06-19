//! End-to-end tests for the read-side query API on the admin server. The
//! producer stores only raw samples; `GET /diffs/*` diffs them on the fly via
//! the DiffEngine, and `/suppress` edits the engine's rules at runtime.

use std::future::IntoFuture;
use std::net::SocketAddr;
use std::sync::Arc;

use chrono::Utc;
use riffy::analysis::classify::EndpointClassifiers;
use riffy::analysis::engine::DiffEngine;
use riffy::analysis::suppress::SuppressRules;
use riffy::http::query::UpstreamTargets;
use riffy::http::router::{admin_router, AdminState};
use riffy::storage::{InMemorySampleStore, RawSample, SampleStore};
use serde_json::Value;

fn raw(
    endpoint: &str,
    baseline: &str,
    candidate: Option<&str>,
    control: Option<&str>,
) -> RawSample {
    RawSample {
        endpoint: endpoint.to_owned(),
        timestamp: Utc::now(),
        baseline_status: 200,
        baseline_body: baseline.to_owned(),
        candidate_status: candidate.map(|_| 200),
        candidate_body: candidate.map(|b| b.to_owned()),
        control_status: control.map(|_| 200),
        control_body: control.map(|b| b.to_owned()),
        request_curl: None,
    }
}

async fn spawn_admin(store: Arc<InMemorySampleStore>) -> (SocketAddr, Arc<DiffEngine>) {
    spawn_admin_with_engine(
        store,
        Arc::new(DiffEngine::new(
            SuppressRules::from_config(&[]),
            EndpointClassifiers::from_config(&[]),
        )),
    )
    .await
}

async fn spawn_admin_with_engine(
    store: Arc<InMemorySampleStore>,
    engine: Arc<DiffEngine>,
) -> (SocketAddr, Arc<DiffEngine>) {
    let state = AdminState {
        metrics: None,
        store,
        engine: engine.clone(),
        upstreams: Arc::new(UpstreamTargets::from_addresses(
            "baseline:9100",
            "https://candidate:9000",
            "http://control:9200",
        )),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(axum::serve(listener, admin_router(state)).into_future());
    (addr, engine)
}

/// Test HTTP client that ignores HTTP_PROXY/HTTPS_PROXY so localhost is direct.
fn http_client() -> reqwest::Client {
    reqwest::Client::builder().no_proxy().build().unwrap()
}

#[tokio::test]
async fn list_paths_lists_all_endpoints_and_filters_by_endpoint() {
    let store = Arc::new(InMemorySampleStore::new());

    // Three user samples diff at user.name and user.email (candidate differs,
    // control matches baseline). One health sample is identical.
    for _ in 0..3 {
        store
            .append_sample(&raw(
                "/api/v1/users/:id",
                r#"{"user":{"name":"a","email":"e"}}"#,
                Some(r#"{"user":{"name":"b","email":"f"}}"#),
                Some(r#"{"user":{"name":"a","email":"e"}}"#),
            ))
            .await
            .unwrap();
    }
    store
        .append_sample(&raw(
            "/api/v1/health",
            r#"{"ok":true}"#,
            Some(r#"{"ok":true}"#),
            Some(r#"{"ok":true}"#),
        ))
        .await
        .unwrap();

    let (addr, _) = spawn_admin(store).await;
    let client = http_client();

    let body: Value = client
        .get(format!("http://{addr}/diffs/paths"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let endpoints = body["endpoints"].as_array().unwrap();
    assert_eq!(endpoints.len(), 2);
    assert_eq!(endpoints[0]["endpoint"], "/api/v1/health");
    assert_eq!(endpoints[1]["endpoint"], "/api/v1/users/:id");

    let resp = client
        .get(format!("http://{addr}/diffs/paths"))
        .query(&[("endpoint", "/api/v1/users/:id")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["total"], 3);
    let paths = body["paths"].as_array().unwrap();
    assert_eq!(paths.len(), 2);
    assert_eq!(paths[0], "user.email");
    assert_eq!(paths[1], "user.name");

    let resp = client
        .get(format!("http://{addr}/diffs/paths"))
        .query(&[("endpoint", "/nope")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn diff_detail_returns_stats_and_paginated_samples() {
    let store = Arc::new(InMemorySampleStore::new());

    for i in 0..3 {
        let mut s = raw(
            "/api/v1/users/:id",
            &format!(r#"{{"user":{{"name":"alice{i}"}}}}"#),
            Some(&format!(r#"{{"user":{{"name":"bob{i}"}}}}"#)),
            Some(&format!(r#"{{"user":{{"name":"alice{i}"}}}}"#)),
        );
        s.request_curl = Some(format!("curl -X GET '$RIFFY_TARGET/api/v1/users/{i}'"));
        store.append_sample(&s).await.unwrap();
    }

    let (addr, _) = spawn_admin(store).await;
    let client = http_client();

    let resp = client
        .get(format!("http://{addr}/diffs/detail"))
        .query(&[
            ("endpoint", "/api/v1/users/:id"),
            ("path", "user.name"),
            ("limit", "2"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["raw_count"], 3);
    assert_eq!(body["is_regression"], true);
    let items = body["samples"]["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(body["samples"]["has_more"], true);
    assert_eq!(items[0]["raw"]["right"], "bob2");
    assert_eq!(
        items[0]["request_curl"],
        "curl -X GET '$RIFFY_TARGET/api/v1/users/2'"
    );

    let body: Value = client
        .get(format!("http://{addr}/diffs/detail"))
        .query(&[
            ("endpoint", "/api/v1/users/:id"),
            ("path", "user.name"),
            ("limit", "2"),
            ("offset", "2"),
        ])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["samples"]["items"].as_array().unwrap().len(), 1);
    assert_eq!(body["samples"]["has_more"], false);

    let resp = client
        .get(format!("http://{addr}/diffs/detail"))
        .query(&[("endpoint", "/api/v1/users/:id"), ("path", "nope")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn status_field_flags_regression_below_thresholds() {
    let store = Arc::new(InMemorySampleStore::new());

    // 1 status divergence diluted across 5000 requests: relative = 100% (> 20)
    // but absolute = 0.02% (< 0.03), so the normal classifier would say "not a
    // regression". The reserved :status field must flag it anyway (raw > noise).
    let body = r#"{"a":1}"#;
    for _ in 0..4999 {
        store
            .append_sample(&raw("/api/v1/users/:id", body, Some(body), Some(body)))
            .await
            .unwrap();
    }
    let mut mismatch = raw("/api/v1/users/:id", body, Some(body), Some(body));
    mismatch.candidate_status = Some(500);
    mismatch.candidate_body = None;
    store.append_sample(&mismatch).await.unwrap();

    let (addr, _) = spawn_admin(store).await;
    let client = http_client();

    let body: Value = client
        .get(format!("http://{addr}/diffs/detail"))
        .query(&[("endpoint", "/api/v1/users/:id"), ("path", ":status")])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["total"], 5000);
    assert_eq!(body["is_regression"], true);
}

#[tokio::test]
async fn suppress_rules_apply_at_read_time() {
    let store = Arc::new(InMemorySampleStore::new());
    store
        .append_sample(&raw(
            "/api/v1/users/:id",
            r#"{"a":1,"b":1}"#,
            Some(r#"{"a":2,"b":2}"#),
            Some(r#"{"a":1,"b":1}"#),
        ))
        .await
        .unwrap();

    let (addr, _) = spawn_admin(store).await;
    let client = http_client();

    // Both fields diff initially.
    let body: Value = client
        .get(format!("http://{addr}/diffs/paths"))
        .query(&[("endpoint", "/api/v1/users/:id")])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["paths"].as_array().unwrap().len(), 2);

    // Suppress "a" at runtime.
    let resp = client
        .put(format!("http://{addr}/suppress"))
        .query(&[("endpoint", "/api/v1/users/:id")])
        .json(&serde_json::json!({ "paths": ["a"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Now only "b" remains — applied at read time, no restart.
    let body: Value = client
        .get(format!("http://{addr}/diffs/paths"))
        .query(&[("endpoint", "/api/v1/users/:id")])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let paths = body["paths"].as_array().unwrap();
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0], "b");

    // GET reflects the stored rule.
    let body: Value = client
        .get(format!("http://{addr}/suppress"))
        .query(&[("endpoint", "/api/v1/users/:id")])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["paths"][0], "a");

    // Clearing brings "a" back.
    let resp = client
        .delete(format!("http://{addr}/suppress"))
        .query(&[("endpoint", "/api/v1/users/:id")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    let body: Value = client
        .get(format!("http://{addr}/diffs/paths"))
        .query(&[("endpoint", "/api/v1/users/:id")])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["paths"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn upstreams_returns_scheme_normalized_bases() {
    let store = Arc::new(InMemorySampleStore::new());
    let (addr, _) = spawn_admin(store).await;
    let client = http_client();

    let body: Value = client
        .get(format!("http://{addr}/upstreams"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body["baseline"], "http://baseline:9100");
    assert_eq!(body["candidate"], "https://candidate:9000");
    assert_eq!(body["control"], "http://control:9200");
}

#[tokio::test]
async fn admin_serves_ui_assets() {
    let store = Arc::new(InMemorySampleStore::new());
    let (addr, _) = spawn_admin(store).await;
    let client = http_client();

    let resp = client.get(format!("http://{addr}/")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let ct = resp.headers()["content-type"].to_str().unwrap().to_owned();
    assert!(ct.starts_with("text/html"), "content-type was {ct}");
    let body = resp.text().await.unwrap();
    assert!(body.contains("Riffy"));
    assert!(body.contains("/alpine.js"));

    let resp = client
        .get(format!("http://{addr}/alpine.js"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let ct = resp.headers()["content-type"].to_str().unwrap().to_owned();
    assert!(ct.contains("javascript"), "content-type was {ct}");
}

#[tokio::test]
async fn reset_stats_clears_endpoint_and_404s_when_absent() {
    let store = Arc::new(InMemorySampleStore::new());
    store
        .append_sample(&raw(
            "/api/v1/users/:id",
            r#"{"a":1}"#,
            Some(r#"{"a":2}"#),
            Some(r#"{"a":1}"#),
        ))
        .await
        .unwrap();

    let (addr, _) = spawn_admin(store).await;
    let client = http_client();

    let resp = client
        .delete(format!("http://{addr}/diffs"))
        .query(&[("endpoint", "/api/v1/users/:id")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    let resp = client
        .get(format!("http://{addr}/diffs/paths"))
        .query(&[("endpoint", "/api/v1/users/:id")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    let resp = client
        .delete(format!("http://{addr}/diffs"))
        .query(&[("endpoint", "/api/v1/users/:id")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}
