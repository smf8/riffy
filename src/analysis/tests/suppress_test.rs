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

#[test]
fn exact_path_is_suppressed() {
    let s = SuppressRules::from_config(&[endpoint("/a", &["user.name"])]);
    assert!(s.is_suppressed("/a", "user.name"));
    assert!(!s.is_suppressed("/a", "user.id"));
}

#[test]
fn child_of_suppressed_prefix_is_suppressed() {
    let s = SuppressRules::from_config(&[endpoint("/a", &["meta"])]);
    assert!(s.is_suppressed("/a", "meta.ts"));
    assert!(s.is_suppressed("/a", "meta.v"));
    assert!(s.is_suppressed("/a", "meta"));
    assert!(!s.is_suppressed("/a", "id"));
}

#[test]
fn no_false_positive_on_prefix_substring() {
    // "meta" must not suppress "metadata" (different segment).
    let s = SuppressRules::from_config(&[endpoint("/a", &["meta"])]);
    assert!(!s.is_suppressed("/a", "metadata"));
    assert!(s.is_suppressed("/a", "meta.x"));
}

#[test]
fn unknown_endpoint_suppresses_nothing() {
    let s = SuppressRules::from_config(&[endpoint("/a", &["x"])]);
    assert!(!s.is_suppressed("/b", "x"));
}

#[test]
fn empty_suppress_list_is_noop() {
    let s = SuppressRules::from_config(&[endpoint("/a", &[])]);
    assert!(!s.is_suppressed("/a", "x"));
    assert!(s.rules().is_empty());
}

#[test]
fn trailing_wildcard_matches_direct_children_only() {
    let s = SuppressRules::from_config(&[endpoint("/a", &["meta.*"])]);
    assert!(s.is_suppressed("/a", "meta.ts"));
    assert!(s.is_suppressed("/a", "meta.v"));
    // Two levels deep — not matched by "meta.*"
    assert!(!s.is_suppressed("/a", "meta.nested.v"));
    assert!(!s.is_suppressed("/a", "id"));
}

#[test]
fn midpath_wildcard_matches_indexed_field() {
    let s = SuppressRules::from_config(&[endpoint("/a", &["items.*.id"])]);
    assert!(s.is_suppressed("/a", "items.0.id"));
    assert!(s.is_suppressed("/a", "items.1.id"));
    assert!(!s.is_suppressed("/a", "items.0.name"));
    assert!(!s.is_suppressed("/a", "total"));
}

#[test]
fn wildcard_requires_exact_segment_count() {
    let s = SuppressRules::from_config(&[endpoint("/a", &["a.*"])]);
    assert!(!s.is_suppressed("/a", "a.b.c"));
    assert!(s.is_suppressed("/a", "a.b"));
}

#[test]
fn wildcard_does_not_match_extra_long_path() {
    let s = SuppressRules::from_config(&[endpoint("/a", &["x.*.z"])]);
    assert!(!s.is_suppressed("/a", "x.y.z.extra"));
}

#[test]
fn with_endpoint_replaces_and_clears() {
    let s = SuppressRules::from_config(&[endpoint("/a", &["x"])]);

    let replaced = s.with_endpoint("/a", vec!["y".to_owned(), "z".to_owned()]);
    assert!(!replaced.is_suppressed("/a", "x"));
    assert!(replaced.is_suppressed("/a", "y"));
    assert!(replaced.is_suppressed("/a", "z"));

    let cleared = replaced.with_endpoint("/a", Vec::new());
    assert!(!cleared.is_suppressed("/a", "y"));
    assert!(cleared.rules().is_empty());
}
