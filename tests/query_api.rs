//! End-to-end tests for the read-side query API on the admin server. The
//! producer stores only raw samples; `GET /diffs/*` diffs them on the fly via
//! the DiffEngine, and `/suppress` edits the engine's rules at runtime.

use std::future::IntoFuture;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
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
        id: String::new(),
        endpoint: endpoint.to_owned(),
        timestamp: Utc::now(),
        baseline_status: 200,
        baseline_body: Bytes::from(baseline.to_owned()),
        baseline_headers: "{}".to_owned(),
        candidate_status: candidate.map(|_| 200),
        candidate_body: candidate.map(|b| Bytes::from(b.to_owned())),
        candidate_headers: candidate.map(|_| "{}".to_owned()),
        control_status: control.map(|_| 200),
        control_body: control.map(|b| Bytes::from(b.to_owned())),
        control_headers: control.map(|_| "{}".to_owned()),
        request_curl: None,
    }
}

async fn spawn_admin(store: Arc<InMemorySampleStore>) -> (SocketAddr, Arc<DiffEngine>) {
    spawn_admin_with_engine(
        store,
        Arc::new(DiffEngine::new(
            SuppressRules::from_config(&[]).unwrap(),
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
    assert_eq!(body["regressions"], 2);
    let paths = body["paths"].as_array().unwrap();
    assert_eq!(paths.len(), 2);
    // paths are PathSummary objects, sorted by path, carrying counts + verdict.
    assert_eq!(paths[0]["path"], "user.email");
    assert_eq!(paths[0]["raw_count"], 3);
    assert_eq!(paths[0]["noise_count"], 0);
    assert_eq!(paths[0]["is_regression"], true);
    assert_eq!(paths[1]["path"], "user.name");

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
    assert_eq!(paths[0]["path"], "b");

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
async fn exclude_param_previews_without_persisting() {
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

    // Preview with "a" excluded: only "b" shows, but nothing is persisted.
    let body: Value = client
        .get(format!("http://{addr}/diffs/paths"))
        .query(&[("endpoint", "/api/v1/users/:id"), ("exclude", "a")])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let paths = body["paths"].as_array().unwrap();
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0]["path"], "b");

    // Without the param the stored rules are unchanged — both fields are back.
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

    // GET /suppress confirms nothing was persisted.
    let body: Value = client
        .get(format!("http://{addr}/suppress"))
        .query(&[("endpoint", "/api/v1/users/:id")])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(body["paths"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn regex_suppress_rule_ignores_field_and_invalid_is_rejected() {
    let store = Arc::new(InMemorySampleStore::new());
    store
        .append_sample(&raw(
            "/api/v1/users/:id",
            r#"{"created_at":1,"name":"a"}"#,
            Some(r#"{"created_at":2,"name":"b"}"#),
            Some(r#"{"created_at":1,"name":"a"}"#),
        ))
        .await
        .unwrap();

    let (addr, _) = spawn_admin(store).await;
    let client = http_client();

    // A regex rule ignores created_at; only name remains.
    let resp = client
        .put(format!("http://{addr}/suppress"))
        .query(&[("endpoint", "/api/v1/users/:id")])
        .json(&serde_json::json!({ "paths": ["re:.*_at$"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

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
    assert_eq!(paths[0]["path"], "name");

    // An invalid regex is rejected with 400.
    let resp = client
        .put(format!("http://{addr}/suppress"))
        .query(&[("endpoint", "/api/v1/users/:id")])
        .json(&serde_json::json!({ "paths": ["re:("] }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn get_sample_returns_full_request_and_responses() {
    let store = Arc::new(InMemorySampleStore::new());
    let mut s = raw(
        "/api/v1/users/:id",
        r#"{"a":1}"#,
        Some(r#"{"a":2}"#),
        Some(r#"{"a":1}"#),
    );
    s.request_curl = Some("curl -X GET '$RIFFY_TARGET/api/v1/users/7'".to_owned());
    s.baseline_headers = r#"{"content-type":"application/json"}"#.to_owned();
    s.candidate_headers = Some(r#"{"content-type":"application/json"}"#.to_owned());
    store.append_sample(&s).await.unwrap();

    let (addr, _) = spawn_admin(store).await;
    let client = http_client();

    // Learn the sample id from the detail page.
    let detail: Value = client
        .get(format!("http://{addr}/diffs/detail"))
        .query(&[("endpoint", "/api/v1/users/:id"), ("path", "a")])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = detail["samples"]["items"][0]["id"].as_str().unwrap();

    let body: Value = client
        .get(format!("http://{addr}/diffs/sample"))
        .query(&[("endpoint", "/api/v1/users/:id"), ("id", id)])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["baseline"]["status"], 200);
    assert_eq!(body["baseline"]["body"]["a"], 1);
    assert_eq!(body["candidate"]["body"]["a"], 2);
    assert_eq!(body["control"]["body"]["a"], 1);
    assert_eq!(
        body["baseline"]["headers"]["content-type"],
        "application/json"
    );
    assert_eq!(
        body["candidate"]["headers"]["content-type"],
        "application/json"
    );
    assert!(body["request_curl"]
        .as_str()
        .unwrap()
        .contains("$RIFFY_TARGET"));

    // Unknown id → 404.
    let resp = client
        .get(format!("http://{addr}/diffs/sample"))
        .query(&[("endpoint", "/api/v1/users/:id"), ("id", "nope")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn header_diff_surfaces_under_headers_namespace_and_suppresses() {
    let store = Arc::new(InMemorySampleStore::new());

    // Bodies are identical; only the candidate's content-type header differs from
    // baseline, while control matches baseline. The header diff must surface under
    // the `:headers` namespace and read as a regression (raw > noise).
    let body = r#"{"ok":true}"#;
    for _ in 0..3 {
        let mut s = raw("/api/v1/users/:id", body, Some(body), Some(body));
        s.baseline_headers = r#"{"content-type":"application/json"}"#.to_owned();
        s.candidate_headers = Some(r#"{"content-type":"text/html"}"#.to_owned());
        s.control_headers = Some(r#"{"content-type":"application/json"}"#.to_owned());
        store.append_sample(&s).await.unwrap();
    }

    let (addr, _) = spawn_admin(store).await;
    let client = http_client();

    let resp: Value = client
        .get(format!("http://{addr}/diffs/paths"))
        .query(&[("endpoint", "/api/v1/users/:id")])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let paths = resp["paths"].as_array().unwrap();
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0]["path"], ":headers.content-type");
    assert_eq!(paths[0]["raw_count"], 3);
    assert_eq!(paths[0]["noise_count"], 0);
    assert_eq!(paths[0]["is_regression"], true);

    // A `:headers` subtree rule hides every header path at once.
    let resp = client
        .put(format!("http://{addr}/suppress"))
        .query(&[("endpoint", "/api/v1/users/:id")])
        .json(&serde_json::json!({ "paths": [":headers"] }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp: Value = client
        .get(format!("http://{addr}/diffs/paths"))
        .query(&[("endpoint", "/api/v1/users/:id")])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(resp["paths"].as_array().unwrap().is_empty());
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
