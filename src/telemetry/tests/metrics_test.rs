use std::sync::{Arc, OnceLock};

use crate::telemetry::metrics::{ProxyRequestGuard, UpstreamTimer};
use axum::http::StatusCode;
use metrics_exporter_prometheus::PrometheusHandle;

/// The metrics recorder is process-global, so all tests share one instance.
/// Tests use unique endpoint labels to stay independent of each other.
fn recorder() -> &'static PrometheusHandle {
    static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
    HANDLE.get_or_init(|| {
        metrics_exporter_prometheus::PrometheusBuilder::new()
            .install_recorder()
            .expect("failed to install test recorder")
    })
}

fn endpoint(name: &str) -> Arc<str> {
    Arc::from(name)
}

/// Find rendered lines for `metric` that carry the given endpoint label.
fn lines_for<'a>(rendered: &'a str, metric: &str, endpoint: &str) -> Vec<&'a str> {
    let label = format!("endpoint=\"{endpoint}\"");
    rendered
        .lines()
        .filter(|line| line.starts_with(metric) && line.contains(&label))
        .collect()
}

#[test]
fn completed_request_records_real_status() {
    let handle = recorder();

    let guard = ProxyRequestGuard::start("GET".to_owned(), endpoint("/t/completed"));
    guard.complete(StatusCode::OK);

    let rendered = handle.render();
    let lines = lines_for(&rendered, "riffy_proxy_request_total", "/t/completed");
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("status=\"200\""));
}

#[test]
fn dropped_request_records_cancelled_status() {
    let handle = recorder();

    drop(ProxyRequestGuard::start(
        "GET".to_owned(),
        endpoint("/t/dropped"),
    ));

    let rendered = handle.render();
    let lines = lines_for(&rendered, "riffy_proxy_request_total", "/t/dropped");
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("status=\"cancelled\""));
}

#[test]
fn dropped_request_still_records_duration() {
    let handle = recorder();

    drop(ProxyRequestGuard::start(
        "GET".to_owned(),
        endpoint("/t/dropped-duration"),
    ));

    let rendered = handle.render();
    assert!(!lines_for(
        &rendered,
        "riffy_proxy_request_duration_seconds",
        "/t/dropped-duration"
    )
    .is_empty());
}

#[test]
fn request_is_recorded_exactly_once() {
    let handle = recorder();

    let guard = ProxyRequestGuard::start("GET".to_owned(), endpoint("/t/once"));
    guard.complete(StatusCode::OK);

    let rendered = handle.render();
    // One series with status=200, none with status=cancelled.
    let lines = lines_for(&rendered, "riffy_proxy_request_total", "/t/once");
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("status=\"200\""));
    assert!(lines[0].ends_with(" 1"));
}

#[test]
fn upstream_timer_finish_records_ok_outcome() {
    let handle = recorder();

    UpstreamTimer::start("primary", endpoint("/t/up-ok")).finish(true);

    let rendered = handle.render();
    let lines = lines_for(
        &rendered,
        "riffy_upstream_request_duration_seconds",
        "/t/up-ok",
    );
    assert!(!lines.is_empty());
    assert!(lines.iter().all(|l| l.contains("outcome=\"ok\"")));
    assert!(lines.iter().all(|l| l.contains("upstream=\"primary\"")));
}

#[test]
fn upstream_timer_finish_records_error_outcome() {
    let handle = recorder();

    UpstreamTimer::start("candidate", endpoint("/t/up-err")).finish(false);

    let rendered = handle.render();
    let lines = lines_for(
        &rendered,
        "riffy_upstream_request_duration_seconds",
        "/t/up-err",
    );
    assert!(!lines.is_empty());
    assert!(lines.iter().all(|l| l.contains("outcome=\"error\"")));
}

#[test]
fn dropped_upstream_timer_records_cancelled_outcome() {
    let handle = recorder();

    drop(UpstreamTimer::start("secondary", endpoint("/t/up-drop")));

    let rendered = handle.render();
    let lines = lines_for(
        &rendered,
        "riffy_upstream_request_duration_seconds",
        "/t/up-drop",
    );
    assert!(!lines.is_empty());
    assert!(lines.iter().all(|l| l.contains("outcome=\"cancelled\"")));
}
