use std::collections::HashMap;
use std::time::Duration;

use crate::compare::flatten::{DiffType, FieldDiff};
use crate::storage::{
    DiffEntry, DiffStore, EndpointAggregation, FieldAggregation, InMemoryDiffStore,
};
use chrono::Utc;
use serde_json::json;

fn flat(left: &str, right: &str) -> FieldDiff {
    FieldDiff {
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
        baseline_status: 200,
        candidate_status: Some(200),
        control_status: Some(200),
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
        },
    );
    store
        .add_aggregation(&[
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
async fn add_aggregation_accumulates_rather_than_overwrites() {
    let store = InMemoryDiffStore::new();

    let delta = |total: u64, raw: u64, noise: u64| {
        let mut fields = HashMap::new();
        fields.insert(
            "user.name".to_owned(),
            FieldAggregation {
                raw_count: raw,
                noise_count: noise,
            },
        );
        vec![EndpointAggregation {
            endpoint: "/e".to_owned(),
            total,
            fields,
            last_updated: Utc::now(),
        }]
    };

    store.add_aggregation(&delta(10, 4, 1)).await.unwrap();
    store.add_aggregation(&delta(5, 2, 3)).await.unwrap();

    // Two flushes must sum, not clobber — this is what lets multiple instances
    // share one backend without losing each other's counts.
    let got = store.get_aggregation("/e").await.unwrap().unwrap();
    assert_eq!(got.total, 15);
    let field = got.fields.get("user.name").unwrap();
    assert_eq!(field.raw_count, 6);
    assert_eq!(field.noise_count, 4);
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

#[tokio::test]
async fn append_diff_trims_oldest_past_cap() {
    let store = InMemoryDiffStore::with_capacity(2);

    for i in 0..3 {
        store
            .append_diff(&entry("/e", Some("x"), &format!("l{i}"), &format!("r{i}")))
            .await
            .unwrap();
    }

    // Only the two newest survive; the oldest (l0) was trimmed from the front.
    let entries = store.entries().await;
    assert_eq!(entries.len(), 2);
    assert_eq!(
        entries[0].raw_fields.get("x").unwrap().left,
        Some(json!("l1"))
    );
    assert_eq!(
        entries[1].raw_fields.get("x").unwrap().left,
        Some(json!("l2"))
    );
}

fn total_delta(endpoint: &str, total: u64) -> Vec<EndpointAggregation> {
    vec![EndpointAggregation {
        endpoint: endpoint.to_owned(),
        total,
        fields: HashMap::new(),
        last_updated: Utc::now(),
    }]
}

#[tokio::test]
async fn windowed_aggregation_sums_recent_buckets() {
    // 1s buckets, 10s window: two adds ~1.1s apart land in different buckets
    // but both within the window, so reads sum them.
    let store = InMemoryDiffStore::with_retention(
        usize::MAX,
        Duration::from_secs(1),
        Duration::from_secs(10),
    );

    store.add_aggregation(&total_delta("/e", 3)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    store.add_aggregation(&total_delta("/e", 4)).await.unwrap();

    let agg = store.get_aggregation("/e").await.unwrap().unwrap();
    assert_eq!(agg.total, 7);
}

#[tokio::test]
async fn aggregation_ages_out_of_the_window() {
    // 1s buckets, 1s window: a count is visible now, gone once the window passes.
    let store = InMemoryDiffStore::with_retention(
        usize::MAX,
        Duration::from_secs(1),
        Duration::from_secs(1),
    );

    store.add_aggregation(&total_delta("/e", 5)).await.unwrap();
    assert!(store.get_aggregation("/e").await.unwrap().is_some());

    tokio::time::sleep(Duration::from_millis(2100)).await;
    assert!(store.get_aggregation("/e").await.unwrap().is_none());
}
