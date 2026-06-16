use std::collections::HashMap;

use crate::analysis::suppress::EndpointSuppressPaths;
use crate::compare::flatten::{DiffType, FieldDiff};
use crate::config::EndpointConfig;

fn field(key: &str) -> (String, FieldDiff) {
    (
        key.to_owned(),
        FieldDiff {
            left: None,
            right: None,
            diff_type: DiffType::Primitive,
        },
    )
}

fn endpoint(pattern: &str, suppress: &[&str]) -> EndpointConfig {
    EndpointConfig {
        pattern: pattern.to_owned(),
        threshold: Default::default(),
        suppress_paths: suppress.iter().map(|s| s.to_string()).collect(),
        sample_rate: 1.0,
    }
}

#[test]
fn exact_path_is_removed() {
    let s = EndpointSuppressPaths::from_config(&[endpoint("/a", &["user.name"])]);
    let mut diffs: HashMap<String, FieldDiff> = [field("user.name"), field("user.id")].into();
    s.suppress("/a", &mut diffs);
    assert!(!diffs.contains_key("user.name"));
    assert!(diffs.contains_key("user.id"));
}

#[test]
fn child_of_suppressed_prefix_is_removed() {
    let s = EndpointSuppressPaths::from_config(&[endpoint("/a", &["meta"])]);
    let mut diffs: HashMap<String, FieldDiff> =
        [field("meta.ts"), field("meta.v"), field("id")].into();
    s.suppress("/a", &mut diffs);
    assert!(!diffs.contains_key("meta.ts"));
    assert!(!diffs.contains_key("meta.v"));
    assert!(diffs.contains_key("id"));
}

#[test]
fn no_false_positive_on_prefix_substring() {
    // "meta" must not suppress "metadata" (different segment).
    let s = EndpointSuppressPaths::from_config(&[endpoint("/a", &["meta"])]);
    let mut diffs: HashMap<String, FieldDiff> = [field("metadata"), field("meta.x")].into();
    s.suppress("/a", &mut diffs);
    assert!(diffs.contains_key("metadata"));
    assert!(!diffs.contains_key("meta.x"));
}

#[test]
fn unknown_endpoint_leaves_diffs_intact() {
    let s = EndpointSuppressPaths::from_config(&[endpoint("/a", &["x"])]);
    let mut diffs: HashMap<String, FieldDiff> = [field("x")].into();
    s.suppress("/b", &mut diffs);
    assert!(diffs.contains_key("x"));
}

#[test]
fn empty_suppress_list_is_noop() {
    let s = EndpointSuppressPaths::from_config(&[endpoint("/a", &[])]);
    let mut diffs: HashMap<String, FieldDiff> = [field("x"), field("y")].into();
    s.suppress("/a", &mut diffs);
    assert_eq!(diffs.len(), 2);
}

// --- Wildcard (glob) tests ---

#[test]
fn trailing_wildcard_matches_direct_children_only() {
    // "meta.*" should match meta.ts and meta.v but NOT meta.nested.v
    let s = EndpointSuppressPaths::from_config(&[endpoint("/a", &["meta.*"])]);
    let mut diffs: HashMap<String, FieldDiff> = [
        field("meta.ts"),
        field("meta.v"),
        field("meta.nested.v"),
        field("id"),
    ]
    .into();
    s.suppress("/a", &mut diffs);
    assert!(!diffs.contains_key("meta.ts"));
    assert!(!diffs.contains_key("meta.v"));
    // Two levels deep — not matched by "meta.*"
    assert!(diffs.contains_key("meta.nested.v"));
    assert!(diffs.contains_key("id"));
}

#[test]
fn midpath_wildcard_matches_indexed_field() {
    // "items.*.id" should match items.0.id, items.1.id but not items.0.name
    let s = EndpointSuppressPaths::from_config(&[endpoint("/a", &["items.*.id"])]);
    let mut diffs: HashMap<String, FieldDiff> = [
        field("items.0.id"),
        field("items.1.id"),
        field("items.0.name"),
        field("total"),
    ]
    .into();
    s.suppress("/a", &mut diffs);
    assert!(!diffs.contains_key("items.0.id"));
    assert!(!diffs.contains_key("items.1.id"));
    assert!(diffs.contains_key("items.0.name"));
    assert!(diffs.contains_key("total"));
}

#[test]
fn wildcard_requires_exact_segment_count() {
    // "a.*" must not match "a.b.c" (wrong depth).
    let s = EndpointSuppressPaths::from_config(&[endpoint("/a", &["a.*"])]);
    let mut diffs: HashMap<String, FieldDiff> = [field("a.b.c"), field("a.b")].into();
    s.suppress("/a", &mut diffs);
    assert!(diffs.contains_key("a.b.c"));
    assert!(!diffs.contains_key("a.b"));
}

#[test]
fn wildcard_does_not_match_extra_long_path() {
    let s = EndpointSuppressPaths::from_config(&[endpoint("/a", &["x.*.z"])]);
    let mut diffs: HashMap<String, FieldDiff> = [field("x.y.z.extra")].into();
    s.suppress("/a", &mut diffs);
    assert!(diffs.contains_key("x.y.z.extra"));
}
