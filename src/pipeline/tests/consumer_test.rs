use std::sync::Arc;
use std::time::Duration;

use crate::analysis::collector::InMemoryDifferenceCollector;
use crate::analysis::filter::DifferencesFilter;
use crate::analysis::DifferenceCollector;
use crate::endpoint::EndpointMatcher;
use crate::pipeline::consumer::Consumer;
use crate::pipeline::AnalysisMessage;
use crate::proxy::upstream::UpstreamResponse;
use crate::storage::InMemoryDiffStore;
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
    primary: &str,
    candidate: Option<&str>,
    secondary: Option<&str>,
) -> AnalysisMessage {
    AnalysisMessage {
        path: path.to_owned(),
        received_at: std::time::Instant::now(),
        primary_response: response(200, primary),
        candidate_response: candidate.map(|b| response(200, b)),
        secondary_response: secondary.map(|b| response(200, b)),
    }
}

/// Run a consumer over the given messages until the channel closes, then
/// return the store and collector for assertions. The aggregation interval is
/// long so only the final shutdown flush writes snapshots.
async fn run_consumer(
    messages: Vec<AnalysisMessage>,
) -> (Arc<InMemoryDiffStore>, Arc<InMemoryDifferenceCollector>) {
    let (tx, rx) = crate::pipeline::channel();
    let collector = Arc::new(InMemoryDifferenceCollector::new());
    let store = Arc::new(InMemoryDiffStore::new());
    let matcher = Arc::new(EndpointMatcher::new(&["/api/v1/users/:id".to_owned()]));
    let filter = DifferencesFilter::new(20.0, 0.03);

    let handle = Consumer::new(
        rx,
        matcher,
        collector.clone(),
        filter,
        store.clone(),
        Duration::from_secs(3600),
    )
    .spawn();

    for msg in messages {
        tx.send(msg).await.unwrap();
    }
    drop(tx);
    handle.await.unwrap();

    (store, collector)
}

#[tokio::test]
async fn writes_diff_entry_for_differing_responses() {
    let (store, _) = run_consumer(vec![message(
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
    assert_eq!(entry.primary_status, 200);
    assert_eq!(entry.candidate_status, Some(200));
    assert_eq!(entry.secondary_status, Some(200));
}

#[tokio::test]
async fn identical_responses_produce_no_entry_but_count_total() {
    let body = r#"{"a": 1}"#;
    let (store, collector) = run_consumer(vec![message(
        "/api/v1/users/1",
        body,
        Some(body),
        Some(body),
    )])
    .await;

    assert!(store.entries().await.is_empty());

    let snapshot = collector.snapshot();
    assert_eq!(snapshot[0].total, 1);
}

#[tokio::test]
async fn status_mismatch_alone_produces_entry() {
    let body = r#"{"a": 1}"#;
    let mut msg = message("/api/v1/users/1", body, Some(body), Some(body));
    msg.candidate_response.as_mut().unwrap().status = 500;

    let (store, _) = run_consumer(vec![msg]).await;

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

    let (store, collector) = run_consumer(vec![msg]).await;

    let entries = store.entries().await;
    assert_eq!(entries.len(), 1);
    assert!(entries[0].raw_fields.is_empty());
    assert_eq!(entries[0].candidate_status, Some(503));
    // No field counters moved, only the endpoint total.
    let snapshot = collector.snapshot();
    assert_eq!(snapshot[0].total, 1);
    assert!(snapshot[0].fields.is_empty());
}

#[tokio::test]
async fn invalid_candidate_json_is_skipped() {
    let (store, collector) = run_consumer(vec![message(
        "/api/v1/users/1",
        r#"{"a": 1}"#,
        Some("<html>"),
        Some(r#"{"a": 1}"#),
    )])
    .await;

    assert!(store.entries().await.is_empty());
    assert_eq!(collector.snapshot()[0].total, 1);
}

#[tokio::test]
async fn missing_candidate_yields_entry_with_noise_only() {
    let (store, _) = run_consumer(vec![message(
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
    let (store, _) = run_consumer(vec![
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

    let field = aggregation.fields.get("n").unwrap();
    assert_eq!(field.raw_count, 2);
    assert_eq!(field.noise_count, 0);
    // raw=2 noise=0 total=2 → relative 100% > 20%, absolute 100% > 0.03%
    assert!(field.is_regression);
}

#[tokio::test]
async fn non_json_primary_is_skipped_entirely() {
    let (store, collector) =
        run_consumer(vec![message("/x", "<html>", Some(r#"{"a": 1}"#), None)]).await;

    assert!(store.entries().await.is_empty());
    assert!(collector.snapshot().is_empty());
}

#[tokio::test]
async fn gzip_primary_is_decompressed_and_analyzed() {
    use async_compression::tokio::bufread::GzipEncoder;
    use tokio::io::AsyncReadExt;

    let mut compressed = Vec::new();
    GzipEncoder::new(br#"{"a": 1}"#.as_slice())
        .read_to_end(&mut compressed)
        .await
        .unwrap();

    let mut msg = message("/x", "", Some(r#"{"a": 2}"#), None);
    msg.primary_response.body = Bytes::from(compressed);
    msg.primary_response.headers.insert(
        axum::http::header::CONTENT_ENCODING,
        "gzip".parse().unwrap(),
    );

    let (store, _) = run_consumer(vec![msg]).await;

    let entries = store.entries().await;
    assert_eq!(entries.len(), 1);
    assert!(entries[0].raw_fields.contains_key("a"));
}

#[tokio::test]
async fn unsupported_encoding_on_primary_is_skipped() {
    let mut msg = message("/x", r#"{"a": 1}"#, Some(r#"{"a": 2}"#), None);
    msg.primary_response.headers.insert(
        axum::http::header::CONTENT_ENCODING,
        "compress".parse().unwrap(),
    );

    let (store, collector) = run_consumer(vec![msg]).await;

    assert!(store.entries().await.is_empty());
    assert!(collector.snapshot().is_empty());
}

#[tokio::test]
async fn unmatched_path_uses_raw_path_as_endpoint() {
    let (store, _) = run_consumer(vec![message(
        "/other/route?q=1",
        r#"{"a": 1}"#,
        Some(r#"{"a": 2}"#),
        None,
    )])
    .await;

    let entries = store.entries().await;
    assert_eq!(entries[0].endpoint, "/other/route");
}
