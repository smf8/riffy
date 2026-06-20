use crate::analysis::suppress::SuppressRules;
use crate::config::EndpointConfig;

fn endpoint(pattern: &str, suppress: &[&str]) -> EndpointConfig {
    EndpointConfig {
        pattern: pattern.to_owned(),
        threshold: Default::default(),
        suppress_paths: suppress.iter().map(|s| s.to_string()).collect(),
        sample_rate: 1.0,
        capture_request_curl: false,
        store_credentials_header: false,
    }
}

fn rules(suppress: &[&str]) -> SuppressRules {
    SuppressRules::from_config(&[endpoint("/a", suppress)]).expect("valid patterns")
}

#[test]
fn exact_path_is_suppressed() {
    let s = rules(&["user.name"]);
    assert!(s.is_suppressed("/a", "user.name"));
    assert!(!s.is_suppressed("/a", "user.id"));
}

#[test]
fn child_of_suppressed_prefix_is_suppressed() {
    let s = rules(&["meta"]);
    assert!(s.is_suppressed("/a", "meta.ts"));
    assert!(s.is_suppressed("/a", "meta.v"));
    assert!(s.is_suppressed("/a", "meta"));
    assert!(!s.is_suppressed("/a", "id"));
}

#[test]
fn no_false_positive_on_prefix_substring() {
    // "meta" must not suppress "metadata" (different segment).
    let s = rules(&["meta"]);
    assert!(!s.is_suppressed("/a", "metadata"));
    assert!(s.is_suppressed("/a", "meta.x"));
}

#[test]
fn unknown_endpoint_suppresses_nothing() {
    let s = rules(&["x"]);
    assert!(!s.is_suppressed("/b", "x"));
}

#[test]
fn empty_suppress_list_is_noop() {
    let s = SuppressRules::from_config(&[endpoint("/a", &[])]).unwrap();
    assert!(!s.is_suppressed("/a", "x"));
    assert!(s.rules().is_empty());
}

#[test]
fn trailing_wildcard_matches_direct_children_only() {
    let s = rules(&["meta.*"]);
    assert!(s.is_suppressed("/a", "meta.ts"));
    assert!(s.is_suppressed("/a", "meta.v"));
    // Two levels deep — not matched by "meta.*"
    assert!(!s.is_suppressed("/a", "meta.nested.v"));
    assert!(!s.is_suppressed("/a", "id"));
}

#[test]
fn midpath_wildcard_matches_indexed_field() {
    let s = rules(&["items.*.id"]);
    assert!(s.is_suppressed("/a", "items.0.id"));
    assert!(s.is_suppressed("/a", "items.1.id"));
    assert!(!s.is_suppressed("/a", "items.0.name"));
    assert!(!s.is_suppressed("/a", "total"));
}

#[test]
fn wildcard_requires_exact_segment_count() {
    let s = rules(&["a.*"]);
    assert!(!s.is_suppressed("/a", "a.b.c"));
    assert!(s.is_suppressed("/a", "a.b"));
}

#[test]
fn regex_matches_field_and_children() {
    // A regex on a suffix: ignore any field ending in _at, and its children.
    let s = rules(&["re:.*_at$"]);
    assert!(s.is_suppressed("/a", "created_at"));
    assert!(s.is_suppressed("/a", "user.updated_at"));
    // children of a matched field are ignored too
    assert!(s.is_suppressed("/a", "updated_at.tz"));
    assert!(!s.is_suppressed("/a", "name"));
    assert!(!s.is_suppressed("/a", "at_start"));
}

#[test]
fn regex_anchored_to_segment_boundaries() {
    // "re:^meta$" matches the meta field (and its children) but not "metadata".
    let s = rules(&["re:^meta$"]);
    assert!(s.is_suppressed("/a", "meta"));
    assert!(s.is_suppressed("/a", "meta.ts"));
    assert!(!s.is_suppressed("/a", "metadata"));
}

#[test]
fn invalid_regex_is_rejected() {
    let bad = SuppressRules::from_config(&[endpoint("/a", &["re:("])]);
    assert!(bad.is_err());
}

#[test]
fn with_endpoint_replaces_and_clears() {
    let s = rules(&["x"]);

    let replaced = s
        .with_endpoint("/a", vec!["y".to_owned(), "z".to_owned()])
        .unwrap();
    assert!(!replaced.is_suppressed("/a", "x"));
    assert!(replaced.is_suppressed("/a", "y"));
    assert!(replaced.is_suppressed("/a", "z"));

    let cleared = replaced.with_endpoint("/a", Vec::new()).unwrap();
    assert!(!cleared.is_suppressed("/a", "y"));
    assert!(cleared.rules().is_empty());
}
