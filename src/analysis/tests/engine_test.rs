use crate::analysis::classify::EndpointClassifiers;
use crate::analysis::engine::DiffEngine;
use crate::analysis::suppress::SuppressRules;
use crate::compare::flatten::{DiffType, STATUS_FIELD};
use crate::storage::RawSample;
use chrono::Utc;

const EP: &str = "/x";

fn engine() -> DiffEngine {
    DiffEngine::new(
        SuppressRules::from_config(&[]),
        EndpointClassifiers::from_config(&[]),
    )
}

fn sample(baseline: &str, candidate: Option<&str>, control: Option<&str>) -> RawSample {
    RawSample {
        endpoint: EP.to_owned(),
        timestamp: Utc::now(),
        baseline_status: 200,
        baseline_body: baseline.to_owned(),
        candidate_status: candidate.map(|_| 200),
        candidate_body: candidate.map(|b| b.to_owned()),
        control_status: control.map(|_| 200),
        control_body: control.map(|b| b.to_owned()),
        request_curl: None,
    }
}

#[test]
fn aggregate_counts_raw_and_noise_per_field() {
    let samples = vec![
        // candidate differs at "a"; control matches baseline -> raw only.
        sample(r#"{"a":1}"#, Some(r#"{"a":2}"#), Some(r#"{"a":1}"#)),
        // both differ at "a" -> raw and noise.
        sample(r#"{"a":1}"#, Some(r#"{"a":9}"#), Some(r#"{"a":8}"#)),
    ];
    let counts = engine().aggregate(EP, &samples).expect("counts");
    assert_eq!(counts.total, 2);
    let a = counts.fields.get("a").expect("field a");
    assert_eq!(a.raw_count, 2);
    assert_eq!(a.noise_count, 1);
}

#[test]
fn aggregate_empty_is_none() {
    assert!(engine().aggregate(EP, &[]).is_none());
}

#[test]
fn status_mismatch_surfaces_status_field() {
    let mut s = sample(r#"{"a":1}"#, None, Some(r#"{"a":1}"#));
    // candidate answered a different status with no stored body.
    s.candidate_status = Some(500);
    s.candidate_body = None;

    let counts = engine()
        .aggregate(EP, std::slice::from_ref(&s))
        .expect("counts");
    let status = counts.fields.get(STATUS_FIELD).expect("status field");
    assert_eq!(status.raw_count, 1);
    assert_eq!(status.noise_count, 0);

    let detail = engine().detail(EP, STATUS_FIELD, std::slice::from_ref(&s), 20, 0);
    assert!(detail.is_regression);
    let item = &detail.samples.items[0];
    assert!(matches!(
        item.raw.as_ref().unwrap().diff_type,
        DiffType::StatusMismatch
    ));
}

#[test]
fn failed_upstream_contributes_nothing() {
    // candidate failed (None) -> no raw diffs at all.
    let s = sample(r#"{"a":1}"#, None, Some(r#"{"a":1}"#));
    let counts = engine()
        .aggregate(EP, std::slice::from_ref(&s))
        .expect("counts");
    assert_eq!(counts.total, 1);
    assert!(counts.fields.is_empty());
}

#[test]
fn suppression_applied_during_diff() {
    let eng = engine();
    let s = sample(
        r#"{"a":1,"b":1}"#,
        Some(r#"{"a":2,"b":2}"#),
        Some(r#"{"a":1,"b":1}"#),
    );

    let before = eng.aggregate(EP, std::slice::from_ref(&s)).unwrap();
    assert!(before.fields.contains_key("a"));
    assert!(before.fields.contains_key("b"));

    eng.set_suppress(EP, vec!["a".to_owned()]);
    let after = eng.aggregate(EP, std::slice::from_ref(&s)).unwrap();
    assert!(!after.fields.contains_key("a"));
    assert!(after.fields.contains_key("b"));

    // Clearing brings it back at the next read — no restart.
    eng.set_suppress(EP, Vec::new());
    let cleared = eng.aggregate(EP, std::slice::from_ref(&s)).unwrap();
    assert!(cleared.fields.contains_key("a"));
}

#[test]
fn detail_counts_and_regression_verdict() {
    let samples = vec![
        sample(r#"{"a":1}"#, Some(r#"{"a":2}"#), Some(r#"{"a":1}"#)),
        sample(r#"{"a":1}"#, Some(r#"{"a":3}"#), Some(r#"{"a":1}"#)),
    ];
    let detail = engine().detail(EP, "a", &samples, 20, 0);
    assert_eq!(detail.total, 2);
    assert_eq!(detail.raw_count, 2);
    assert_eq!(detail.noise_count, 0);
    assert!(detail.is_regression);
    assert_eq!(detail.samples.items.len(), 2);
    assert!(!detail.samples.has_more);
}

#[test]
fn detail_paginates_newest_first() {
    let samples: Vec<RawSample> = (0..5)
        .map(|i| {
            sample(
                r#"{"a":1}"#,
                Some(&format!(r#"{{"a":{}}}"#, i + 100)),
                Some(r#"{"a":1}"#),
            )
        })
        .collect();

    let page1 = engine().detail(EP, "a", &samples, 2, 0);
    assert_eq!(page1.raw_count, 5);
    assert_eq!(page1.samples.items.len(), 2);
    assert!(page1.samples.has_more);

    let page3 = engine().detail(EP, "a", &samples, 2, 4);
    assert_eq!(page3.samples.items.len(), 1);
    assert!(!page3.samples.has_more);
}

#[test]
fn detail_unmatched_path_has_zero_counts() {
    let s = sample(r#"{"a":1}"#, Some(r#"{"a":2}"#), Some(r#"{"a":1}"#));
    let detail = engine().detail(EP, "missing", std::slice::from_ref(&s), 20, 0);
    assert_eq!(detail.raw_count, 0);
    assert_eq!(detail.noise_count, 0);
    assert!(detail.samples.items.is_empty());
}
