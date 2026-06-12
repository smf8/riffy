use crate::analysis::collector::InMemoryDifferenceCollector;
use crate::analysis::{DifferenceAnalyzer, DifferenceCollector};
use crate::compare::flatten::DiffType;
use std::sync::Arc;

fn analyzer() -> (
    DifferenceAnalyzer<InMemoryDifferenceCollector>,
    Arc<InMemoryDifferenceCollector>,
) {
    let collector = Arc::new(InMemoryDifferenceCollector::new());
    (DifferenceAnalyzer::new(collector.clone()), collector)
}

#[test]
fn raw_and_noise_diffs_are_computed_against_primary() {
    let (analyzer, _) = analyzer();

    let primary = br#"{"name": "alice", "age": 30}"#;
    let candidate = br#"{"name": "bob", "age": 30}"#;
    let secondary = br#"{"name": "alice", "age": 31}"#;

    let result = analyzer
        .analyze("/e", primary, Some(candidate), Some(secondary))
        .unwrap();

    assert_eq!(result.raw_diffs.len(), 1);
    assert_eq!(
        result.raw_diffs.get("name").unwrap().diff_type,
        DiffType::Primitive
    );

    assert_eq!(result.noise_diffs.len(), 1);
    assert_eq!(
        result.noise_diffs.get("age").unwrap().diff_type,
        DiffType::Primitive
    );
}

#[test]
fn counters_are_updated_after_analyze() {
    let (analyzer, collector) = analyzer();

    let primary = br#"{"name": "alice"}"#;
    let candidate = br#"{"name": "bob"}"#;
    let secondary = br#"{"name": "alice"}"#;

    analyzer
        .analyze("/e", primary, Some(candidate), Some(secondary))
        .unwrap();

    let snapshot = collector.snapshot();
    assert_eq!(snapshot[0].total, 1);

    let field = &snapshot[0].fields[0];
    assert_eq!(field.path, "name");
    assert_eq!((field.raw_count, field.noise_count), (1, 0));
}

#[test]
fn invalid_primary_json_is_an_error() {
    let (analyzer, collector) = analyzer();

    let result = analyzer.analyze("/e", b"not json", None, None);
    assert!(result.is_err());
    // Failed analysis must not count toward totals.
    assert!(collector.snapshot().is_empty());
}

#[test]
fn missing_candidate_yields_empty_raw_diffs() {
    let (analyzer, _) = analyzer();

    let result = analyzer
        .analyze("/e", br#"{"a": 1}"#, None, Some(br#"{"a": 2}"#))
        .unwrap();

    assert!(result.raw_diffs.is_empty());
    assert_eq!(result.noise_diffs.len(), 1);
}

#[test]
fn invalid_candidate_json_is_skipped() {
    let (analyzer, _) = analyzer();

    let result = analyzer
        .analyze("/e", br#"{"a": 1}"#, Some(b"<html>"), Some(br#"{"a": 1}"#))
        .unwrap();

    assert!(result.raw_diffs.is_empty());
    assert!(result.noise_diffs.is_empty());
}

#[test]
fn identical_responses_yield_no_diffs_but_count_total() {
    let (analyzer, collector) = analyzer();

    let body = br#"{"a": 1}"#;
    let result = analyzer
        .analyze("/e", body, Some(body), Some(body))
        .unwrap();

    assert!(result.raw_diffs.is_empty());
    assert!(result.noise_diffs.is_empty());
    assert_eq!(collector.snapshot()[0].total, 1);
}
