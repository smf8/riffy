//! End-to-end tests for the read-side diff query API on the admin server:
//! `GET /diffs/paths` and `GET /diffs/detail`, served from an in-memory store.

use std::collections::HashMap;
use std::future::IntoFuture;
use std::net::SocketAddr;
use std::sync::Arc;

use chrono::Utc;
use riffy::analysis::classify::RegressionClassifier;
use riffy::analysis::counters::LiveCounters;
use riffy::compare::flatten::{DiffType, FieldDiff};
use riffy::http::router::{admin_router, AdminState};
use riffy::storage::{
    DiffEntry, DiffStore, EndpointAggregation, FieldAggregation, InMemoryDiffStore,
};
use serde_json::{json, Value};

async fn spawn_admin(store: Arc<dyn DiffStore>) -> SocketAddr {
    let state = AdminState {
        metrics: None,
        store,
        classifier: RegressionClassifier::new(20.0, 0.03),
        counters: Arc::new(LiveCounters::new()),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(axum::serve(listener, admin_router(state)).into_future());
    addr
}

/// Test HTTP client that ignores HTTP_PROXY/HTTPS_PROXY so localhost is direct.
fn http_client() -> reqwest::Client {
    reqwest::Client::builder().no_proxy().build().unwrap()
}

fn field(raw: u64, noise: u64) -> FieldAggregation {
    FieldAggregation {
        raw_count: raw,
        noise_count: noise,
    }
}

#[tokio::test]
async fn list_paths_lists_all_endpoints_and_filters_by_endpoint() {
    let store = Arc::new(InMemoryDiffStore::new());

    let mut users_fields = HashMap::new();
    users_fields.insert("user.name".to_owned(), field(5, 1));
    users_fields.insert("user.email".to_owned(), field(2, 0));
    store
        .add_aggregation(&[
            EndpointAggregation {
                endpoint: "/api/v1/users/:id".to_owned(),
                total: 100,
                fields: users_fields,
                last_updated: Utc::now(),
            },
            EndpointAggregation {
                endpoint: "/api/v1/health".to_owned(),
                total: 50,
                fields: HashMap::new(),
                last_updated: Utc::now(),
            },
        ])
        .await
        .unwrap();

    let addr = spawn_admin(store).await;
    let client = http_client();

    // All endpoints, sorted by endpoint key.
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

    // Filtered to one endpoint: paths sorted.
    let resp = client
        .get(format!("http://{addr}/diffs/paths"))
        .query(&[("endpoint", "/api/v1/users/:id")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["total"], 100);
    let paths = body["paths"].as_array().unwrap();
    assert_eq!(paths.len(), 2);
    assert_eq!(paths[0], "user.email");
    assert_eq!(paths[1], "user.name");

    // Unknown endpoint → 404.
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
    let store = Arc::new(InMemoryDiffStore::new());

    let mut fields = HashMap::new();
    fields.insert("user.name".to_owned(), field(3, 0));
    store
        .add_aggregation(&[EndpointAggregation {
            endpoint: "/api/v1/users/:id".to_owned(),
            total: 3,
            fields,
            last_updated: Utc::now(),
        }])
        .await
        .unwrap();

    for i in 0..3 {
        let mut raw_fields = HashMap::new();
        raw_fields.insert(
            "user.name".to_owned(),
            FieldDiff {
                left: Some(json!(format!("alice{i}"))),
                right: Some(json!(format!("bob{i}"))),
                diff_type: DiffType::Primitive,
            },
        );
        store
            .append_diff(&DiffEntry {
                endpoint: "/api/v1/users/:id".to_owned(),
                timestamp: Utc::now(),
                raw_fields,
                noise_fields: HashMap::new(),
                baseline_status: 200,
                candidate_status: Some(200),
                control_status: Some(200),
            })
            .await
            .unwrap();
    }

    let addr = spawn_admin(store).await;
    let client = http_client();

    // First page (limit 2): stats + newest-first samples + has_more.
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

    // Second page.
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

    // Unknown path → 404.
    let resp = client
        .get(format!("http://{addr}/diffs/detail"))
        .query(&[("endpoint", "/api/v1/users/:id"), ("path", "nope")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn reset_stats_clears_endpoint_and_404s_when_absent() {
    let store = Arc::new(InMemoryDiffStore::new());

    let mut fields = HashMap::new();
    fields.insert("user.name".to_owned(), field(5, 1));
    store
        .add_aggregation(&[EndpointAggregation {
            endpoint: "/api/v1/users/:id".to_owned(),
            total: 10,
            fields,
            last_updated: Utc::now(),
        }])
        .await
        .unwrap();

    let addr = spawn_admin(store).await;
    let client = http_client();

    // Reset clears the endpoint's stats → 204 No Content.
    let resp = client
        .delete(format!("http://{addr}/diffs"))
        .query(&[("endpoint", "/api/v1/users/:id")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // The endpoint's aggregation is gone now → a paths lookup 404s.
    let resp = client
        .get(format!("http://{addr}/diffs/paths"))
        .query(&[("endpoint", "/api/v1/users/:id")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    // Resetting an endpoint with no recorded stats → 404.
    let resp = client
        .delete(format!("http://{addr}/diffs"))
        .query(&[("endpoint", "/api/v1/users/:id")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}
