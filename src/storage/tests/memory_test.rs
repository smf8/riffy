use std::time::Duration;

use crate::storage::{InMemorySampleStore, RawSample, SampleStore};
use chrono::{DateTime, Utc};

fn sample_at(endpoint: &str, tag: &str, timestamp: DateTime<Utc>) -> RawSample {
    RawSample {
        endpoint: endpoint.to_owned(),
        timestamp,
        baseline_status: 200,
        baseline_body: format!(r#"{{"v":"{tag}"}}"#),
        candidate_status: Some(200),
        candidate_body: Some(format!(r#"{{"v":"{tag}-c"}}"#)),
        control_status: Some(200),
        control_body: Some(format!(r#"{{"v":"{tag}"}}"#)),
        request_curl: None,
    }
}

fn sample(endpoint: &str, tag: &str) -> RawSample {
    sample_at(endpoint, tag, Utc::now())
}

#[tokio::test]
async fn round_trip_newest_first() {
    let store = InMemorySampleStore::new();
    for i in 0..3 {
        store
            .append_sample(&sample("/e", &format!("s{i}")))
            .await
            .unwrap();
    }

    let got = store.fetch_samples("/e").await.unwrap();
    assert_eq!(got.len(), 3);
    assert_eq!(got[0].baseline_body, r#"{"v":"s2"}"#);
    assert_eq!(got[2].baseline_body, r#"{"v":"s0"}"#);
    assert_eq!(got[0].candidate_body.as_deref(), Some(r#"{"v":"s2-c"}"#));
}

#[tokio::test]
async fn list_and_delete_endpoints() {
    let store = InMemorySampleStore::new();
    store.append_sample(&sample("/a", "x")).await.unwrap();
    store.append_sample(&sample("/b", "y")).await.unwrap();

    let mut endpoints = store.list_endpoints().await.unwrap();
    endpoints.sort();
    assert_eq!(endpoints, vec!["/a".to_owned(), "/b".to_owned()]);

    store.delete_endpoint("/a").await.unwrap();
    assert_eq!(store.list_endpoints().await.unwrap(), vec!["/b".to_owned()]);
    assert!(store.fetch_samples("/a").await.unwrap().is_empty());
}

#[tokio::test]
async fn cap_trims_oldest_per_endpoint() {
    let store = InMemorySampleStore::with_capacity(2);
    for i in 0..3 {
        store
            .append_sample(&sample("/e", &format!("s{i}")))
            .await
            .unwrap();
    }
    let got = store.fetch_samples("/e").await.unwrap();
    assert_eq!(got.len(), 2);
    // Newest-first: s2 then s1; s0 was trimmed.
    assert_eq!(got[0].baseline_body, r#"{"v":"s2"}"#);
    assert_eq!(got[1].baseline_body, r#"{"v":"s1"}"#);
}

#[tokio::test]
async fn samples_outside_window_are_filtered() {
    let store = InMemorySampleStore::with_retention(usize::MAX, Duration::from_secs(60));

    let old = sample_at("/e", "old", Utc::now() - chrono::Duration::seconds(120));
    let fresh = sample_at("/e", "fresh", Utc::now());
    store.append_sample(&old).await.unwrap();
    store.append_sample(&fresh).await.unwrap();

    let got = store.fetch_samples("/e").await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].baseline_body, r#"{"v":"fresh"}"#);
}
