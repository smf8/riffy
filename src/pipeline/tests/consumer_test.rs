use std::sync::Arc;

use crate::endpoint::EndpointMatcher;
use crate::pipeline::consumer::Consumer;
use crate::pipeline::{AnalysisMessage, RequestSnapshot};
use crate::storage::{InMemorySampleStore, RawSample, SampleStore};
use crate::upstream::client::UpstreamResponse;
use axum::http::{HeaderMap, Method};
use bytes::Bytes;

const EP: &str = "/api/v1/users/:id";
const NO_CAP: usize = 1 << 20;

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

async fn run_consumer(messages: Vec<AnalysisMessage>) -> Arc<InMemorySampleStore> {
    run_consumer_with_cap(messages, NO_CAP).await
}

async fn run_consumer_with_cap(
    messages: Vec<AnalysisMessage>,
    max_body_bytes: usize,
) -> Arc<InMemorySampleStore> {
    let (tx, rx) = crate::pipeline::channel(1024);
    let store = Arc::new(InMemorySampleStore::new());
    let matcher = Arc::new(EndpointMatcher::new(&[EP.to_owned()]));

    let handle = Consumer::new(rx, matcher, store.clone(), max_body_bytes).spawn();

    for msg in messages {
        tx.send(msg).await.unwrap();
    }
    drop(tx);
    handle.await.unwrap();

    store
}

async fn stored(store: &Arc<InMemorySampleStore>) -> Vec<RawSample> {
    store.fetch_samples(EP).await.unwrap()
}

#[tokio::test]
async fn stores_sample_with_both_bodies_for_matching_status() {
    let store = run_consumer(vec![message(
        "/api/v1/users/42",
        r#"{"name": "alice"}"#,
        Some(r#"{"name": "bob"}"#),
        Some(r#"{"name": "alice"}"#),
    )])
    .await;

    let samples = stored(&store).await;
    assert_eq!(samples.len(), 1);
    let s = &samples[0];
    assert_eq!(s.endpoint, EP);
    assert_eq!(s.baseline_status, 200);
    assert_eq!(s.baseline_body, r#"{"name": "alice"}"#);
    assert_eq!(s.candidate_status, Some(200));
    assert_eq!(s.candidate_body.as_deref(), Some(r#"{"name": "bob"}"#));
    assert_eq!(s.control_status, Some(200));
    assert_eq!(s.control_body.as_deref(), Some(r#"{"name": "alice"}"#));
}

#[tokio::test]
async fn identical_responses_still_store_a_sample() {
    // Producer records raw data unconditionally; "no diff" is decided at read time.
    let body = r#"{"a": 1}"#;
    let store = run_consumer(vec![message(
        "/api/v1/users/1",
        body,
        Some(body),
        Some(body),
    )])
    .await;
    assert_eq!(stored(&store).await.len(), 1);
}

#[tokio::test]
async fn status_mismatch_stores_status_without_body() {
    let body = r#"{"a": 1}"#;
    let mut msg = message("/api/v1/users/1", body, Some(body), Some(body));
    msg.candidate_response.as_mut().unwrap().status = 500;

    let store = run_consumer(vec![msg]).await;
    let samples = stored(&store).await;
    assert_eq!(samples.len(), 1);
    // Body is not stored for a divergent status — it is never compared at read time.
    assert_eq!(samples[0].candidate_status, Some(500));
    assert_eq!(samples[0].candidate_body, None);
}

#[tokio::test]
async fn same_status_invalid_candidate_json_discards_sample() {
    // Candidate answered baseline's status but with a non-JSON body: storing it as
    // a bodyless match would hide a real body regression, so the sample is dropped.
    let store = run_consumer(vec![message(
        "/api/v1/users/1",
        r#"{"a": 1}"#,
        Some("<html>"),
        Some(r#"{"a": 1}"#),
    )])
    .await;

    assert!(stored(&store).await.is_empty());
    assert!(store.list_endpoints().await.unwrap().is_empty());
}

#[tokio::test]
async fn missing_candidate_stored_as_none() {
    let store = run_consumer(vec![message(
        "/api/v1/users/1",
        r#"{"a": 1}"#,
        None,
        Some(r#"{"a": 2}"#),
    )])
    .await;

    let samples = stored(&store).await;
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].candidate_status, None);
    assert_eq!(samples[0].candidate_body, None);
    assert_eq!(samples[0].control_body.as_deref(), Some(r#"{"a": 2}"#));
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

    assert!(stored(&store).await.is_empty());
    assert!(store.list_endpoints().await.unwrap().is_empty());
}

#[tokio::test]
async fn oversized_baseline_and_oversized_same_status_candidate_are_skipped() {
    // Cap below the candidate body but above the baseline. The candidate answered
    // baseline's status, so its over-cap body cannot be stored for comparison and
    // the whole sample is discarded rather than scored as a bodyless match.
    let store = run_consumer_with_cap(
        vec![message(
            "/api/v1/users/1",
            r#"{"a":1}"#,
            Some(r#"{"a":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}"#),
            Some(r#"{"a":1}"#),
        )],
        12,
    )
    .await;
    assert!(stored(&store).await.is_empty());

    // Now a baseline over the cap skips the whole sample.
    let store = run_consumer_with_cap(
        vec![message(
            "/api/v1/users/1",
            r#"{"a":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}"#,
            Some(r#"{"a":1}"#),
            None,
        )],
        12,
    )
    .await;
    assert!(stored(&store).await.is_empty());
}

#[tokio::test]
async fn gzip_baseline_is_decompressed_and_stored_decoded() {
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
    let samples = stored(&store).await;
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].baseline_body, r#"{"a": 1}"#);
}

#[tokio::test]
async fn unsupported_encoding_on_baseline_is_skipped() {
    let mut msg = message("/api/v1/users/9", r#"{"a": 1}"#, Some(r#"{"a": 2}"#), None);
    msg.baseline_response.headers.insert(
        axum::http::header::CONTENT_ENCODING,
        "compress".parse().unwrap(),
    );

    let store = run_consumer(vec![msg]).await;
    assert!(stored(&store).await.is_empty());
}

#[tokio::test]
async fn unregistered_path_is_dropped() {
    let store = run_consumer(vec![message(
        "/other/route?q=1",
        r#"{"a": 1}"#,
        Some(r#"{"a": 2}"#),
        None,
    )])
    .await;

    assert!(store.list_endpoints().await.unwrap().is_empty());
}

#[tokio::test]
async fn captured_request_renders_curl_on_stored_sample() {
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
    let samples = stored(&store).await;
    assert_eq!(samples.len(), 1);
    let curl = samples[0].request_curl.as_ref().expect("curl captured");
    assert!(curl.starts_with("curl -X GET"));
    assert!(curl.contains("'$RIFFY_TARGET/api/v1/users/1?debug=1'"));
}

#[tokio::test]
async fn no_snapshot_means_no_curl() {
    let store = run_consumer(vec![message(
        "/api/v1/users/1",
        r#"{"v": 1}"#,
        Some(r#"{"v": 2}"#),
        Some(r#"{"v": 1}"#),
    )])
    .await;

    let samples = stored(&store).await;
    assert_eq!(samples.len(), 1);
    assert!(samples[0].request_curl.is_none());
}
