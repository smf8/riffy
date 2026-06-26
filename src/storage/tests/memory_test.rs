use std::time::Duration;

use crate::storage::{InMemorySampleStore, RawSample, SampleStore};
use bytes::Bytes;
use chrono::{DateTime, Utc};

fn sample_at(endpoint: &str, tag: &str, timestamp: DateTime<Utc>) -> RawSample {
    RawSample {
        id: String::new(),
        endpoint: endpoint.to_owned(),
        timestamp,
        baseline_status: 200,
        baseline_body: Bytes::from(format!(r#"{{"v":"{tag}"}}"#)),
        baseline_headers: r#"{"content-type":"application/json"}"#.to_owned(),
        candidate_status: Some(200),
        candidate_body: Some(Bytes::from(format!(r#"{{"v":"{tag}-c"}}"#))),
        candidate_headers: Some(r#"{"content-type":"application/json"}"#.to_owned()),
        control_status: Some(200),
        control_body: Some(Bytes::from(format!(r#"{{"v":"{tag}"}}"#))),
        control_headers: Some(r#"{"content-type":"application/json"}"#.to_owned()),
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
    assert_eq!(
        got[0].candidate_body,
        Some(Bytes::from_static(br#"{"v":"s2-c"}"#))
    );
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
    assert_eq!(got[0].baseline_body, r#"{"v":"s2"}"#);
    assert_eq!(got[1].baseline_body, r#"{"v":"s1"}"#);
}

#[tokio::test]
async fn get_sample_by_id_hit_and_miss() {
    let store = InMemorySampleStore::new();
    store.append_sample(&sample("/e", "x")).await.unwrap();
    store.append_sample(&sample("/e", "y")).await.unwrap();

    let fetched = store.fetch_samples("/e").await.unwrap();
    let id = &fetched[0].id;
    assert!(!id.is_empty());

    let got = store.get_sample("/e", id).await.unwrap().expect("hit");
    assert_eq!(&got.id, id);
    assert_eq!(got.baseline_body, fetched[0].baseline_body);

    assert!(store
        .get_sample("/e", "does-not-exist")
        .await
        .unwrap()
        .is_none());
    assert!(store.get_sample("/other", id).await.unwrap().is_none());
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
