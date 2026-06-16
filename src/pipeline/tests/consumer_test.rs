use std::sync::Arc;
use std::time::Duration;

use crate::analysis::counters::LiveCounters;
use crate::endpoint::EndpointMatcher;
use crate::pipeline::consumer::Consumer;
use crate::pipeline::AnalysisMessage;
use crate::storage::InMemoryDiffStore;
use crate::upstream::client::UpstreamResponse;
use axum::http::HeaderMap;
use bytes::Bytes;

fn response(status: u16, body: &str) -> UpstreamResponse {
    UpstreamResponse {
        status,
        headers: HeaderMap::new(),
        body: Bytes::copy_from_slice(body.as_bytes()),
    }
}

fn message(
    path: &str,
    baseline: &str,
    candidate: Option<&str>,
    control: Option<&str>,
) -> AnalysisMessage {
    AnalysisMessage {
        path: path.to_owned(),
        received_at: std::time::Instant::now(),
        baseline_response: response(200, baseline),
        candidate_response: candidate.map(|b| response(200, b)),
        control_response: control.map(|b| response(200, b)),
    }
}

/// Run a consumer over the given messages until the channel closes, then
/// return the store for assertions. The aggregation interval is long, so the
/// only flush is the final shutdown drain — which moves all buffered counts
/// into the store. All count assertions therefore read the store, matching the
/// "all reads go through the store" design.
async fn run_consumer(messages: Vec<AnalysisMessage>) -> Arc<InMemoryDiffStore> {
    let (tx, rx) = crate::pipeline::channel(1024);
    let collector = Arc::new(LiveCounters::new());
    let store = Arc::new(InMemoryDiffStore::new());
    let matcher = Arc::new(EndpointMatcher::new(&["/api/v1/users/:id".to_owned()]));

    let handle = Consumer::new(
        rx,
        matcher,
        collector,
        store.clone(),
        Duration::from_secs(3600),
    )
    .spawn();

    for msg in messages {
        tx.send(msg).await.unwrap();
    }
    drop(tx);
    handle.await.unwrap();

    store
}

#[tokio::test]
async fn writes_diff_entry_for_differing_responses() {
    let store = run_consumer(vec![message(
        "/api/v1/users/42",
        r#"{"name": "alice"}"#,
        Some(r#"{"name": "bob"}"#),
        Some(r#"{"name": "alice"}"#),
    )])
    .await;

    let entries = store.entries().await;
    assert_eq!(entries.len(), 1);

    let entry = &entries[0];
    assert_eq!(entry.endpoint, "/api/v1/users/:id");
    assert!(entry.raw_fields.contains_key("name"));
    assert!(entry.noise_fields.is_empty());
    assert_eq!(entry.baseline_status, 200);
    assert_eq!(entry.candidate_status, Some(200));
    assert_eq!(entry.control_status, Some(200));
}

#[tokio::test]
async fn identical_responses_produce_no_entry_but_count_total() {
    let body = r#"{"a": 1}"#;
    let store = run_consumer(vec![message(
        "/api/v1/users/1",
        body,
        Some(body),
        Some(body),
    )])
    .await;

    assert!(store.entries().await.is_empty());

    // The request still counts toward the endpoint total (flushed to the store).
    let aggregation = store.aggregation("/api/v1/users/:id").await.unwrap();
    assert_eq!(aggregation.total, 1);
    assert!(aggregation.fields.is_empty());
}

#[tokio::test]
async fn status_mismatch_alone_produces_entry() {
    let body = r#"{"a": 1}"#;
    let mut msg = message("/api/v1/users/1", body, Some(body), Some(body));
    msg.candidate_response.as_mut().unwrap().status = 500;

    let store = run_consumer(vec![msg]).await;

    let entries = store.entries().await;
    assert_eq!(entries.len(), 1);
    assert!(entries[0].raw_fields.is_empty());
    assert_eq!(entries[0].candidate_status, Some(500));
}

#[tokio::test]
async fn mismatched_status_skips_body_comparison() {
    // The candidate body differs, but with a different status the bodies must
    // never be compared — the status mismatch is the reported signal.
    let mut msg = message(
        "/api/v1/users/1",
        r#"{"a": 1}"#,
        Some(r#"{"a": 2}"#),
        Some(r#"{"a": 1}"#),
    );
    msg.candidate_response.as_mut().unwrap().status = 503;

    let store = run_consumer(vec![msg]).await;

    let entries = store.entries().await;
    assert_eq!(entries.len(), 1);
    assert!(entries[0].raw_fields.is_empty());
    assert_eq!(entries[0].candidate_status, Some(503));
    // No field counters moved, only the endpoint total.
    let aggregation = store.aggregation("/api/v1/users/:id").await.unwrap();
    assert_eq!(aggregation.total, 1);
    assert!(aggregation.fields.is_empty());
}

#[tokio::test]
async fn invalid_candidate_json_is_skipped() {
    let store = run_consumer(vec![message(
        "/api/v1/users/1",
        r#"{"a": 1}"#,
        Some("<html>"),
        Some(r#"{"a": 1}"#),
    )])
    .await;

    assert!(store.entries().await.is_empty());
    let aggregation = store.aggregation("/api/v1/users/:id").await.unwrap();
    assert_eq!(aggregation.total, 1);
}

#[tokio::test]
async fn missing_candidate_yields_entry_with_noise_only() {
    let store = run_consumer(vec![message(
        "/api/v1/users/1",
        r#"{"a": 1}"#,
        None,
        Some(r#"{"a": 2}"#),
    )])
    .await;

    let entries = store.entries().await;
    assert_eq!(entries.len(), 1);
    assert!(entries[0].raw_fields.is_empty());
    assert!(entries[0].noise_fields.contains_key("a"));
    assert_eq!(entries[0].candidate_status, None);
}

#[tokio::test]
async fn final_flush_writes_aggregation_snapshot() {
    let store = run_consumer(vec![
        message(
            "/api/v1/users/1",
            r#"{"n": 1}"#,
            Some(r#"{"n": 2}"#),
            Some(r#"{"n": 1}"#),
        ),
        message(
            "/api/v1/users/2",
            r#"{"n": 1}"#,
            Some(r#"{"n": 3}"#),
            Some(r#"{"n": 1}"#),
        ),
    ])
    .await;

    let aggregation = store.aggregation("/api/v1/users/:id").await.unwrap();
    assert_eq!(aggregation.total, 2);

    // The store holds raw counts only; the regression verdict is derived at
    // read time (covered by the classify tests), not persisted here.
    let field = aggregation.fields.get("n").unwrap();
    assert_eq!(field.raw_count, 2);
    assert_eq!(field.noise_count, 0);
}

#[tokio::test]
async fn non_json_baseline_is_skipped_entirely() {
    let store = run_consumer(vec![message(
        "/api/v1/users/9",
        "<html>",
        Some(r#"{"a": 1}"#),
        None,
    )])
    .await;

    assert!(store.entries().await.is_empty());
    // Never recorded, so nothing was flushed for this endpoint.
    assert!(store.aggregation("/api/v1/users/:id").await.is_none());
}

#[tokio::test]
async fn gzip_baseline_is_decompressed_and_analyzed() {
    use async_compression::tokio::bufread::GzipEncoder;
    use tokio::io::AsyncReadExt;

    let mut compressed = Vec::new();
    GzipEncoder::new(br#"{"a": 1}"#.as_slice())
        .read_to_end(&mut compressed)
        .await
        .unwrap();

    let mut msg = message("/api/v1/users/9", "", Some(r#"{"a": 2}"#), None);
    msg.baseline_response.body = Bytes::from(compressed);
    msg.baseline_response.headers.insert(
        axum::http::header::CONTENT_ENCODING,
        "gzip".parse().unwrap(),
    );

    let store = run_consumer(vec![msg]).await;

    let entries = store.entries().await;
    assert_eq!(entries.len(), 1);
    assert!(entries[0].raw_fields.contains_key("a"));
}

#[tokio::test]
async fn unsupported_encoding_on_baseline_is_skipped() {
    let mut msg = message("/api/v1/users/9", r#"{"a": 1}"#, Some(r#"{"a": 2}"#), None);
    msg.baseline_response.headers.insert(
        axum::http::header::CONTENT_ENCODING,
        "compress".parse().unwrap(),
    );

    let store = run_consumer(vec![msg]).await;

    assert!(store.entries().await.is_empty());
    assert!(store.aggregation("/api/v1/users/:id").await.is_none());
}

#[tokio::test]
async fn unregistered_path_is_dropped() {
    // The consumer only knows /api/v1/users/:id; an unregistered path is
    // dropped (not analyzed, not stored) so cardinality stays bounded.
    let store = run_consumer(vec![message(
        "/other/route?q=1",
        r#"{"a": 1}"#,
        Some(r#"{"a": 2}"#),
        None,
    )])
    .await;

    assert!(store.entries().await.is_empty());
    assert!(store.aggregation("/other/route").await.is_none());
}
