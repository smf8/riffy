use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::telemetry::timer::{GuardedTimer, CANCELLED};

/// Shared log of the outcome labels a timer's `record` closure was called with.
type Log = Arc<Mutex<Vec<String>>>;

/// A `record` sink that logs the outcome labels it was called with, so a test
/// can assert exactly which observation(s) a timer produced.
fn sink() -> (Log, impl Fn(&str, Duration)) {
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let recorded = log.clone();
    let record = move |outcome: &str, _elapsed: Duration| {
        recorded.lock().expect("poisoned").push(outcome.to_owned());
    };
    (log, record)
}

#[test]
fn finish_records_the_given_outcome() {
    let (log, record) = sink();

    GuardedTimer::start(record).finish("ok");

    assert_eq!(*log.lock().expect("poisoned"), vec!["ok".to_owned()]);
}

#[test]
fn dropping_before_finish_records_cancelled() {
    let (log, record) = sink();

    drop(GuardedTimer::start(record));

    assert_eq!(*log.lock().expect("poisoned"), vec![CANCELLED.to_owned()]);
}

#[test]
fn finish_suppresses_the_drop_observation() {
    let (log, record) = sink();

    // finish() consumes the guard; its Drop must not add a second observation.
    GuardedTimer::start(record).finish("error");

    assert_eq!(*log.lock().expect("poisoned"), vec!["error".to_owned()]);
}
