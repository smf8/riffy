use std::collections::HashMap;

use crate::compare::flatten::{DiffType, FlatDiff};
use crate::storage::{
    DiffEntry, DiffStore, EndpointAggregation, FieldAggregation, InMemoryDiffStore,
};
use chrono::Utc;
use serde_json::json;

fn flat(left: &str, right: &str) -> FlatDiff {
    FlatDiff {
        left: Some(json!(left)),
        right: Some(json!(right)),
        diff_type: DiffType::Primitive,
    }
}

/// A diff entry whose raw diff is at `raw_path` (if given).
fn entry(endpoint: &str, raw_path: Option<&str>, left: &str, right: &str) -> DiffEntry {
    let mut raw_fields = HashMap::new();
    if let Some(path) = raw_path {
        raw_fields.insert(path.to_owned(), flat(left, right));
    }
    DiffEntry {
        endpoint: endpoint.to_owned(),
        timestamp: Utc::now(),
        raw_fields,
        noise_fields: HashMap::new(),
        primary_status: 200,
        candidate_status: Some(200),
        secondary_status: Some(200),
    }
}

#[tokio::test]
async fn get_and_list_aggregations() {
    let store = InMemoryDiffStore::new();

    let mut fields = HashMap::new();
    fields.insert(
        "user.name".to_owned(),
        FieldAggregation {
            raw_count: 5,
            noise_count: 1,
            is_regression: true,
        },
    );
    store
        .write_aggregation(&[
            EndpointAggregation {
                endpoint: "/api/v1/users/:id".to_owned(),
                total: 10,
                fields,
                last_updated: Utc::now(),
            },
            EndpointAggregation {
                endpoint: "/api/v1/health".to_owned(),
                total: 3,
                fields: HashMap::new(),
                last_updated: Utc::now(),
            },
        ])
        .await
        .unwrap();

    let got = store
        .get_aggregation("/api/v1/users/:id")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.total, 10);
    assert_eq!(got.fields.len(), 1);

    assert!(store.get_aggregation("/missing").await.unwrap().is_none());

    let all = store.list_aggregations().await.unwrap();
    assert_eq!(all.len(), 2);
}

#[tokio::test]
async fn recent_samples_paginate_newest_first() {
    let store = InMemoryDiffStore::new();

    // Five matching entries (l0..l4) plus noise that must be excluded.
    for i in 0..5 {
        store
            .append_diff(&entry("/e", Some("x"), &format!("l{i}"), &format!("r{i}")))
            .await
            .unwrap();
    }
    store
        .append_diff(&entry("/e", Some("y"), "a", "b")) // wrong path
        .await
        .unwrap();
    store
        .append_diff(&entry("/other", Some("x"), "a", "b")) // wrong endpoint
        .await
        .unwrap();

    // First page: newest first, more available.
    let page = store.recent_samples("/e", "x", 2, 0).await.unwrap();
    assert_eq!(page.items.len(), 2);
    assert!(page.has_more);
    assert_eq!(page.items[0].raw.as_ref().unwrap().right, Some(json!("r4")));
    assert_eq!(page.items[1].raw.as_ref().unwrap().right, Some(json!("r3")));

    // Last page: one item, no more.
    let last = store.recent_samples("/e", "x", 2, 4).await.unwrap();
    assert_eq!(last.items.len(), 1);
    assert!(!last.has_more);
    assert_eq!(last.items[0].raw.as_ref().unwrap().right, Some(json!("r0")));

    // Unknown path: empty page.
    let empty = store.recent_samples("/e", "missing", 10, 0).await.unwrap();
    assert!(empty.items.is_empty());
    assert!(!empty.has_more);
}
