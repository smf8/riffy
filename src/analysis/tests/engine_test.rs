use crate::analysis::classify::EndpointClassifiers;
use crate::analysis::engine::DiffEngine;
use crate::analysis::suppress::SuppressRules;
use crate::compare::flatten::{DiffType, STATUS_FIELD};
use crate::storage::RawSample;
use bytes::Bytes;
use chrono::Utc;

const EP: &str = "/x";

fn engine() -> DiffEngine {
    DiffEngine::new(
        SuppressRules::from_config(&[]).unwrap(),
        EndpointClassifiers::from_config(&[]),
    )
}

fn none() -> SuppressRules {
    SuppressRules::default()
}

fn sample(baseline: &str, candidate: Option<&str>, control: Option<&str>) -> RawSample {
    RawSample {
        id: String::new(),
        endpoint: EP.to_owned(),
        timestamp: Utc::now(),
        baseline_status: 200,
        baseline_body: Bytes::from(baseline.to_owned()),
        baseline_headers: "{}".to_owned(),
        candidate_status: candidate.map(|_| 200),
        candidate_body: candidate.map(|b| Bytes::from(b.to_owned())),
        candidate_headers: candidate.map(|_| "{}".to_owned()),
        control_status: control.map(|_| 200),
        control_body: control.map(|b| Bytes::from(b.to_owned())),
        control_headers: control.map(|_| "{}".to_owned()),
        request_curl: None,
    }
}

fn sample_with_headers(
    baseline: (&str, &str),
    candidate: (&str, &str),
    control: (&str, &str),
) -> RawSample {
    RawSample {
        id: String::new(),
        endpoint: EP.to_owned(),
        timestamp: Utc::now(),
        baseline_status: 200,
        baseline_body: Bytes::from(baseline.0.to_owned()),
        baseline_headers: baseline.1.to_owned(),
        candidate_status: Some(200),
        candidate_body: Some(Bytes::from(candidate.0.to_owned())),
        candidate_headers: Some(candidate.1.to_owned()),
        control_status: Some(200),
        control_body: Some(Bytes::from(control.0.to_owned())),
        control_headers: Some(control.1.to_owned()),
        request_curl: None,
    }
}

#[test]
fn aggregate_counts_raw_and_noise_per_field() {
    let samples = vec![
        sample(r#"{"a":1}"#, Some(r#"{"a":2}"#), Some(r#"{"a":1}"#)),
        sample(r#"{"a":1}"#, Some(r#"{"a":9}"#), Some(r#"{"a":8}"#)),
    ];
    let counts = engine().aggregate(EP, &samples, &none()).expect("counts");
    assert_eq!(counts.total, 2);
    let a = counts.fields.get("a").expect("field a");
    assert_eq!(a.raw_count, 2);
    assert_eq!(a.noise_count, 1);
}

#[test]
fn aggregate_empty_is_none() {
    assert!(engine().aggregate(EP, &[], &none()).is_none());
}

#[test]
fn status_mismatch_surfaces_status_field() {
    let mut s = sample(r#"{"a":1}"#, None, Some(r#"{"a":1}"#));
    s.candidate_status = Some(500);
    s.candidate_body = None;

    let counts = engine()
        .aggregate(EP, std::slice::from_ref(&s), &none())
        .expect("counts");
    let status = counts.fields.get(STATUS_FIELD).expect("status field");
    assert_eq!(status.raw_count, 1);

    let detail = engine().detail(EP, STATUS_FIELD, std::slice::from_ref(&s), &none(), 20, 0);
    assert!(detail.is_regression);
    let item = &detail.samples.items[0];
    assert!(matches!(
        item.raw.as_ref().unwrap().diff_type,
        DiffType::StatusMismatch
    ));
}

#[test]
fn failed_upstream_contributes_nothing() {
    let s = sample(r#"{"a":1}"#, None, Some(r#"{"a":1}"#));
    let counts = engine()
        .aggregate(EP, std::slice::from_ref(&s), &none())
        .expect("counts");
    assert_eq!(counts.total, 1);
    assert!(counts.fields.is_empty());
}

#[test]
fn header_diff_is_namespaced_counted_and_suppressible() {
    let s = sample_with_headers(
        (r#"{"ok":true}"#, r#"{"content-type":"application/json"}"#),
        (r#"{"ok":true}"#, r#"{"content-type":"text/html"}"#),
        (r#"{"ok":true}"#, r#"{"content-type":"application/json"}"#),
    );
    let eng = engine();

    let counts = eng
        .aggregate(EP, std::slice::from_ref(&s), &none())
        .unwrap();
    let field = counts
        .fields
        .get(":headers.content-type")
        .expect("header field");
    assert_eq!(field.raw_count, 1);
    assert_eq!(field.noise_count, 0);
    assert_eq!(
        eng.regressions(&counts),
        vec![":headers.content-type".to_owned()]
    );

    eng.set_suppress(EP, vec![":headers".to_owned()]).unwrap();
    let after = eng
        .aggregate(EP, std::slice::from_ref(&s), &none())
        .unwrap();
    assert!(after.fields.is_empty());
}

#[test]
fn diverging_status_skips_header_comparison() {
    let mut s = sample_with_headers(
        (r#"{"ok":true}"#, r#"{"content-type":"application/json"}"#),
        (r#"{"ok":true}"#, r#"{"content-type":"text/html"}"#),
        (r#"{"ok":true}"#, r#"{"content-type":"application/json"}"#),
    );
    s.candidate_status = Some(500);
    s.candidate_body = None;
    s.candidate_headers = None;

    let counts = engine()
        .aggregate(EP, std::slice::from_ref(&s), &none())
        .unwrap();
    assert!(counts.fields.contains_key(STATUS_FIELD));
    assert!(!counts.fields.keys().any(|k| k.starts_with(":headers")));
}

#[test]
fn suppression_applied_during_diff() {
    let eng = engine();
    let s = sample(
        r#"{"a":1,"b":1}"#,
        Some(r#"{"a":2,"b":2}"#),
        Some(r#"{"a":1,"b":1}"#),
    );

    let before = eng
        .aggregate(EP, std::slice::from_ref(&s), &none())
        .unwrap();
    assert!(before.fields.contains_key("a"));
    assert!(before.fields.contains_key("b"));

    eng.set_suppress(EP, vec!["a".to_owned()]).unwrap();
    let after = eng
        .aggregate(EP, std::slice::from_ref(&s), &none())
        .unwrap();
    assert!(!after.fields.contains_key("a"));
    assert!(after.fields.contains_key("b"));

    eng.set_suppress(EP, Vec::new()).unwrap();
    let cleared = eng
        .aggregate(EP, std::slice::from_ref(&s), &none())
        .unwrap();
    assert!(cleared.fields.contains_key("a"));
}

#[test]
fn extra_suppress_excludes_without_persisting() {
    let eng = engine();
    let s = sample(
        r#"{"a":1,"b":1}"#,
        Some(r#"{"a":2,"b":2}"#),
        Some(r#"{"a":1,"b":1}"#),
    );
    let extra = SuppressRules::for_endpoint(EP, &["a".to_owned()]).unwrap();

    let preview = eng.aggregate(EP, std::slice::from_ref(&s), &extra).unwrap();
    assert!(!preview.fields.contains_key("a"));
    assert!(preview.fields.contains_key("b"));

    let stored = eng
        .aggregate(EP, std::slice::from_ref(&s), &none())
        .unwrap();
    assert!(stored.fields.contains_key("a"));
}

#[test]
fn invalid_regex_rule_is_rejected() {
    assert!(engine().set_suppress(EP, vec!["re:(".to_owned()]).is_err());
}

#[test]
fn regressions_rollup_lists_regressing_paths() {
    let eng = engine();
    let s = sample(
        r#"{"a":1,"b":1}"#,
        Some(r#"{"a":2,"b":1}"#),
        Some(r#"{"a":1,"b":9}"#),
    );
    let counts = eng
        .aggregate(EP, std::slice::from_ref(&s), &none())
        .unwrap();
    assert_eq!(eng.regressions(&counts), vec!["a".to_owned()]);
}

#[test]
fn detail_counts_and_regression_verdict() {
    let samples = vec![
        sample(r#"{"a":1}"#, Some(r#"{"a":2}"#), Some(r#"{"a":1}"#)),
        sample(r#"{"a":1}"#, Some(r#"{"a":3}"#), Some(r#"{"a":1}"#)),
    ];
    let detail = engine().detail(EP, "a", &samples, &none(), 20, 0);
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

    let page1 = engine().detail(EP, "a", &samples, &none(), 2, 0);
    assert_eq!(page1.raw_count, 5);
    assert_eq!(page1.samples.items.len(), 2);
    assert!(page1.samples.has_more);

    let page3 = engine().detail(EP, "a", &samples, &none(), 2, 4);
    assert_eq!(page3.samples.items.len(), 1);
    assert!(!page3.samples.has_more);
}

#[test]
fn detail_unmatched_path_has_zero_counts() {
    let s = sample(r#"{"a":1}"#, Some(r#"{"a":2}"#), Some(r#"{"a":1}"#));
    let detail = engine().detail(EP, "missing", std::slice::from_ref(&s), &none(), 20, 0);
    assert_eq!(detail.raw_count, 0);
    assert_eq!(detail.noise_count, 0);
    assert!(detail.samples.items.is_empty());
}
