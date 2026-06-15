use crate::analysis::counters::LiveCounters;
use crate::compare::flatten::flatten_value;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn record_increments_total_and_field_counters() {
    let collector = LiveCounters::new();

    let raw = flatten_value(&json!({"a": 1}), &json!({"a": 2}));
    let noise = HashMap::new();
    collector.record("/api/v1/users/:id", &raw, &noise);
    collector.record("/api/v1/users/:id", &raw, &noise);

    let deltas = collector.drain();
    assert_eq!(deltas.len(), 1);

    let endpoint = &deltas[0];
    assert_eq!(endpoint.endpoint, "/api/v1/users/:id");
    assert_eq!(endpoint.total, 2);
    assert_eq!(endpoint.fields.len(), 1);

    let field = endpoint.fields.get("a").unwrap();
    assert_eq!(field.raw_count, 2);
    assert_eq!(field.noise_count, 0);
}

#[test]
fn raw_and_noise_counters_are_independent() {
    let collector = LiveCounters::new();

    let raw = flatten_value(&json!({"a": 1}), &json!({"a": 2}));
    let noise = flatten_value(&json!({"b": 1}), &json!({"b": 2}));
    collector.record("/e", &raw, &noise);

    let deltas = collector.drain();
    let endpoint = &deltas[0];
    assert_eq!(endpoint.total, 1);

    let a = endpoint.fields.get("a").unwrap();
    assert_eq!((a.raw_count, a.noise_count), (1, 0));

    let b = endpoint.fields.get("b").unwrap();
    assert_eq!((b.raw_count, b.noise_count), (0, 1));
}

#[test]
fn same_field_in_raw_and_noise_bumps_both() {
    let collector = LiveCounters::new();

    let diffs = flatten_value(&json!({"a": 1}), &json!({"a": 2}));
    collector.record("/e", &diffs, &diffs);

    let deltas = collector.drain();
    let field = deltas[0].fields.get("a").unwrap();
    assert_eq!((field.raw_count, field.noise_count), (1, 1));
}

#[test]
fn endpoints_are_isolated() {
    let collector = LiveCounters::new();
    let diffs = flatten_value(&json!({"a": 1}), &json!({"a": 2}));
    let empty = HashMap::new();

    collector.record("/one", &diffs, &empty);
    collector.record("/two", &empty, &empty);

    let mut deltas = collector.drain();
    deltas.sort_by(|a, b| a.endpoint.cmp(&b.endpoint));

    assert_eq!(deltas.len(), 2);
    assert_eq!(deltas[0].endpoint, "/one");
    assert_eq!(deltas[0].fields.len(), 1);
    assert_eq!(deltas[1].endpoint, "/two");
    assert!(deltas[1].fields.is_empty());
}

#[test]
fn empty_diffs_still_count_toward_total() {
    let collector = LiveCounters::new();
    let empty = HashMap::new();

    collector.record("/e", &empty, &empty);
    collector.record("/e", &empty, &empty);
    collector.record("/e", &empty, &empty);

    let deltas = collector.drain();
    assert_eq!(deltas[0].total, 3);
    assert!(deltas[0].fields.is_empty());
}

#[test]
fn drain_resets_the_buffer() {
    let collector = LiveCounters::new();
    let diffs = flatten_value(&json!({"a": 1}), &json!({"a": 2}));

    collector.record("/e", &diffs, &HashMap::new());
    let first = collector.drain();
    assert_eq!(first[0].total, 1);

    // Buffer is empty after a drain — a second drain yields nothing.
    assert!(collector.drain().is_empty());

    // New activity is counted fresh, not added to the already-drained delta.
    collector.record("/e", &diffs, &HashMap::new());
    let second = collector.drain();
    assert_eq!(second[0].total, 1);
    assert_eq!(second[0].fields.get("a").unwrap().raw_count, 1);
}

#[test]
fn reset_endpoint_drops_buffered_counts() {
    let collector = LiveCounters::new();
    let diffs = flatten_value(&json!({"a": 1}), &json!({"a": 2}));

    collector.record("/one", &diffs, &HashMap::new());
    collector.record("/two", &diffs, &HashMap::new());

    collector.reset_endpoint("/one");

    // Only the untouched endpoint survives the reset.
    let deltas = collector.drain();
    assert_eq!(deltas.len(), 1);
    assert_eq!(deltas[0].endpoint, "/two");
}

#[test]
fn restore_reinjects_drained_counts() {
    let collector = LiveCounters::new();
    let diffs = flatten_value(&json!({"a": 1}), &json!({"a": 2}));

    collector.record("/e", &diffs, &HashMap::new());
    let drained = collector.drain();
    collector.restore(&drained);

    // After restore the counts are back, exactly as drained.
    let again = collector.drain();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].total, 1);
    assert_eq!(again[0].fields.get("a").unwrap().raw_count, 1);
}
