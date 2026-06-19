use crate::test_support::{endpoint, lines_for, recorder};
use crate::upstream::metrics::{outcome, request_timer};

#[test]
fn finish_records_ok_outcome() {
    let handle = recorder();

    request_timer("baseline", endpoint("/t/up-ok")).finish(outcome(true));

    let rendered = handle.render();
    let lines = lines_for(
        &rendered,
        "riffy_upstream_request_duration_seconds",
        "/t/up-ok",
    );
    assert!(!lines.is_empty());
    assert!(lines.iter().all(|l| l.contains("outcome=\"ok\"")));
    assert!(lines.iter().all(|l| l.contains("upstream=\"baseline\"")));
}

#[test]
fn finish_records_error_outcome() {
    let handle = recorder();

    request_timer("candidate", endpoint("/t/up-err")).finish(outcome(false));

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
fn dropped_timer_records_cancelled_outcome() {
    let handle = recorder();

    drop(request_timer("control", endpoint("/t/up-drop")));

    let rendered = handle.render();
    let lines = lines_for(
        &rendered,
        "riffy_upstream_request_duration_seconds",
        "/t/up-drop",
    );
    assert!(!lines.is_empty());
    assert!(lines.iter().all(|l| l.contains("outcome=\"cancelled\"")));
}
