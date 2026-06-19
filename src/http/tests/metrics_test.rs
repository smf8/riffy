use crate::http::metrics::proxy_request_timer;
use crate::test_support::{endpoint, lines_for, recorder};

#[test]
fn completed_request_records_real_status() {
    let handle = recorder();

    proxy_request_timer("GET".to_owned(), endpoint("/t/completed")).finish("200");

    let rendered = handle.render();
    let lines = lines_for(&rendered, "riffy_proxy_request_total", "/t/completed");
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("status=\"200\""));
}

#[test]
fn dropped_request_records_cancelled_status() {
    let handle = recorder();

    drop(proxy_request_timer(
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

    drop(proxy_request_timer(
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

    proxy_request_timer("GET".to_owned(), endpoint("/t/once")).finish("200");

    let rendered = handle.render();
    // One series with status=200, none with status=cancelled.
    let lines = lines_for(&rendered, "riffy_proxy_request_total", "/t/once");
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("status=\"200\""));
    assert!(lines[0].ends_with(" 1"));
}
