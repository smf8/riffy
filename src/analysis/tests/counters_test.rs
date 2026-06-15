use crate::analysis::counters::LiveCounters;
use crate::analysis::DiffCounters;
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

    let snapshot = collector.snapshot();
    assert_eq!(snapshot.len(), 1);

    let endpoint = &snapshot[0];
    assert_eq!(endpoint.endpoint, "/api/v1/users/:id");
    assert_eq!(endpoint.total, 2);
    assert_eq!(endpoint.fields.len(), 1);

    let field = &endpoint.fields[0];
    assert_eq!(field.path, "a");
    assert_eq!(field.raw_count, 2);
    assert_eq!(field.noise_count, 0);
    assert_eq!(field.endpoint_total, 2);
}

#[test]
fn raw_and_noise_counters_are_independent() {
    let collector = LiveCounters::new();

    let raw = flatten_value(&json!({"a": 1}), &json!({"a": 2}));
    let noise = flatten_value(&json!({"b": 1}), &json!({"b": 2}));
    collector.record("/e", &raw, &noise);

    let snapshot = collector.snapshot();
    let endpoint = &snapshot[0];
    assert_eq!(endpoint.total, 1);

    let a = endpoint.fields.iter().find(|f| f.path == "a").unwrap();
    assert_eq!((a.raw_count, a.noise_count), (1, 0));

    let b = endpoint.fields.iter().find(|f| f.path == "b").unwrap();
    assert_eq!((b.raw_count, b.noise_count), (0, 1));
}

#[test]
fn same_field_in_raw_and_noise_bumps_both() {
    let collector = LiveCounters::new();

    let diffs = flatten_value(&json!({"a": 1}), &json!({"a": 2}));
    collector.record("/e", &diffs, &diffs);

    let snapshot = collector.snapshot();
    let field = &snapshot[0].fields[0];
    assert_eq!((field.raw_count, field.noise_count), (1, 1));
}

#[test]
fn endpoints_are_isolated() {
    let collector = LiveCounters::new();
    let diffs = flatten_value(&json!({"a": 1}), &json!({"a": 2}));
    let empty = HashMap::new();

    collector.record("/one", &diffs, &empty);
    collector.record("/two", &empty, &empty);

    let mut snapshot = collector.snapshot();
    snapshot.sort_by(|a, b| a.endpoint.cmp(&b.endpoint));

    assert_eq!(snapshot.len(), 2);
    assert_eq!(snapshot[0].endpoint, "/one");
    assert_eq!(snapshot[0].fields.len(), 1);
    assert_eq!(snapshot[1].endpoint, "/two");
    assert!(snapshot[1].fields.is_empty());
}

#[test]
fn empty_diffs_still_count_toward_total() {
    let collector = LiveCounters::new();
    let empty = HashMap::new();

    collector.record("/e", &empty, &empty);
    collector.record("/e", &empty, &empty);
    collector.record("/e", &empty, &empty);

    let snapshot = collector.snapshot();
    assert_eq!(snapshot[0].total, 3);
    assert!(snapshot[0].fields.is_empty());
}
