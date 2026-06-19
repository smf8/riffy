use std::sync::Arc;
use std::time::Duration;

use crate::analysis::counters::LiveCounters;
use crate::analysis::suppress::EndpointSuppressPaths;
use crate::config::EndpointConfig;
use crate::endpoint::EndpointMatcher;
use crate::pipeline::consumer::Consumer;
use crate::pipeline::{AnalysisMessage, RequestSnapshot};
use crate::storage::InMemoryDiffStore;
use crate::upstream::client::UpstreamResponse;
use axum::http::{HeaderMap, Method};
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
        request: None,
    }
}

/// Run a consumer over the given messages until the channel closes, then
/// return the store for assertions. The aggregation interval is long, so the
/// only flush is the final shutdown drain — which moves all buffered counts
/// into the store. All count assertions therefore read the store, matching the
/// "all reads go through the store" design.
async fn run_consumer(messages: Vec<AnalysisMessage>) -> Arc<InMemoryDiffStore> {
    run_consumer_with_endpoints(messages, vec![]).await
}

async fn run_consumer_with_endpoints(
    messages: Vec<AnalysisMessage>,
    endpoints: Vec<EndpointConfig>,
) -> Arc<InMemoryDiffStore> {
    let (tx, rx) = crate::pipeline::channel(1024);
    let collector = Arc::new(LiveCounters::new());
    let store = Arc::new(InMemoryDiffStore::new());
    let matcher = Arc::new(EndpointMatcher::new(&["/api/v1/users/:id".to_owned()]));
    let suppress = Arc::new(EndpointSuppressPaths::from_config(&endpoints));

    let handle = Consumer::new(
        rx,
        matcher,
        collector,
        store.clone(),
        Duration::from_secs(3600),
        suppress,
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
    // The status divergence is recorded as the reserved :status pseudo-field.
    assert!(entries[0].raw_fields.contains_key(":status"));
    assert!(entries[0].noise_fields.is_empty());
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
    // Body not compared: only the status pseudo-field is recorded, not "a".
    assert!(entries[0].raw_fields.contains_key(":status"));
    assert!(!entries[0].raw_fields.contains_key("a"));
    assert_eq!(entries[0].candidate_status, Some(503));
    let aggregation = store.aggregation("/api/v1/users/:id").await.unwrap();
    assert_eq!(aggregation.total, 1);
    let status = aggregation.fields.get(":status").unwrap();
    assert_eq!(status.raw_count, 1);
    assert_eq!(status.noise_count, 0);
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

#[tokio::test]
async fn suppressed_path_is_excluded_from_diffs() {
    let endpoints = vec![crate::config::EndpointConfig {
        pattern: "/api/v1/users/:id".to_owned(),
        threshold: Default::default(),
        suppress_paths: vec!["name".to_owned()],
        sample_rate: 1.0,
        capture_request_curl: false,
        store_credentials_header: false,
    }];

    let store = run_consumer_with_endpoints(
        vec![message(
            "/api/v1/users/1",
            r#"{"name": "alice", "score": 10}"#,
            Some(r#"{"name": "bob", "score": 10}"#),
            Some(r#"{"name": "alice", "score": 10}"#),
        )],
        endpoints,
    )
    .await;

    // `name` is suppressed — only identical `score` remains, so no entry is stored.
    assert!(store.entries().await.is_empty());
}

#[tokio::test]
async fn suppressed_prefix_removes_subtree() {
    let endpoints = vec![crate::config::EndpointConfig {
        pattern: "/api/v1/users/:id".to_owned(),
        threshold: Default::default(),
        suppress_paths: vec!["meta".to_owned()],
        sample_rate: 1.0,
        capture_request_curl: false,
        store_credentials_header: false,
    }];

    let store = run_consumer_with_endpoints(
        vec![message(
            "/api/v1/users/1",
            r#"{"meta": {"ts": 1, "v": 2}, "id": 1}"#,
            Some(r#"{"meta": {"ts": 9, "v": 9}, "id": 1}"#),
            Some(r#"{"meta": {"ts": 1, "v": 2}, "id": 1}"#),
        )],
        endpoints,
    )
    .await;

    // `meta.ts` and `meta.v` are both suppressed by the `meta` prefix — no entry.
    assert!(store.entries().await.is_empty());
}

#[tokio::test]
async fn unsuppressed_sibling_is_still_recorded() {
    let endpoints = vec![crate::config::EndpointConfig {
        pattern: "/api/v1/users/:id".to_owned(),
        threshold: Default::default(),
        suppress_paths: vec!["name".to_owned()],
        sample_rate: 1.0,
        capture_request_curl: false,
        store_credentials_header: false,
    }];

    let store = run_consumer_with_endpoints(
        vec![message(
            "/api/v1/users/1",
            r#"{"name": "alice", "score": 10}"#,
            Some(r#"{"name": "bob", "score": 99}"#),
            Some(r#"{"name": "alice", "score": 10}"#),
        )],
        endpoints,
    )
    .await;

    let entries = store.entries().await;
    assert_eq!(entries.len(), 1);
    // `name` is suppressed, `score` is not.
    assert!(!entries[0].raw_fields.contains_key("name"));
    assert!(entries[0].raw_fields.contains_key("score"));
}

#[tokio::test]
async fn wildcard_suppress_path_filters_indexed_fields() {
    // items.*.id suppresses id within each array element but leaves name intact.
    let endpoints = vec![crate::config::EndpointConfig {
        pattern: "/api/v1/users/:id".to_owned(),
        threshold: Default::default(),
        suppress_paths: vec!["items.*.id".to_owned()],
        sample_rate: 1.0,
        capture_request_curl: false,
        store_credentials_header: false,
    }];

    let store = run_consumer_with_endpoints(
        vec![message(
            "/api/v1/users/1",
            // Baseline: id and name present
            r#"{"items": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]}"#,
            // Candidate: id differs AND name of first item differs
            Some(r#"{"items": [{"id": 9, "name": "z"}, {"id": 9, "name": "b"}]}"#),
            Some(r#"{"items": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]}"#),
        )],
        endpoints,
    )
    .await;

    let entries = store.entries().await;
    // items.0.name diff remains after suppression → one entry stored.
    assert_eq!(entries.len(), 1);
    // id fields are suppressed.
    assert!(!entries[0].raw_fields.contains_key("items.0.id"));
    assert!(!entries[0].raw_fields.contains_key("items.1.id"));
    // name diff is still present.
    assert!(entries[0].raw_fields.contains_key("items.0.name"));
}

#[tokio::test]
async fn captured_request_renders_curl_on_stored_entry() {
    let mut msg = message(
        "/api/v1/users/1",
        r#"{"v": 1}"#,
        Some(r#"{"v": 2}"#),
        Some(r#"{"v": 1}"#),
    );
    msg.request = Some(RequestSnapshot {
        method: Method::GET,
        path_and_query: "/api/v1/users/1?debug=1".to_owned(),
        headers: HeaderMap::new(),
        body: Bytes::new(),
        redact_credentials: true,
    });

    let store = run_consumer(vec![msg]).await;

    let entries = store.entries().await;
    assert_eq!(entries.len(), 1);
    let curl = entries[0].request_curl.as_ref().expect("curl captured");
    assert!(curl.starts_with("curl -X GET"));
    assert!(curl.contains("'$RIFFY_TARGET/api/v1/users/1?debug=1'"));
}

#[tokio::test]
async fn no_snapshot_means_no_curl() {
    // The default `message` helper carries `request: None`.
    let store = run_consumer(vec![message(
        "/api/v1/users/1",
        r#"{"v": 1}"#,
        Some(r#"{"v": 2}"#),
        Some(r#"{"v": 1}"#),
    )])
    .await;

    let entries = store.entries().await;
    assert_eq!(entries.len(), 1);
    assert!(entries[0].request_curl.is_none());
}
