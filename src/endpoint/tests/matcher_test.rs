use crate::endpoint::EndpointMatcher;

fn patterns(raw: &[&str]) -> Vec<String> {
    raw.iter().map(|p| p.to_string()).collect()
}

fn matcher() -> EndpointMatcher {
    EndpointMatcher::new(&patterns(&[
        "/api/v1/users/:id",
        "/api/v1/orders/:order_id/items/:item_id",
        "/api/v1/health",
    ]))
}

#[test]
fn matches_single_param() {
    assert_eq!(
        matcher().resolve("/api/v1/users/42").as_deref(),
        Some("/api/v1/users/:id")
    );
}

#[test]
fn matches_multiple_params() {
    assert_eq!(
        matcher().resolve("/api/v1/orders/7/items/99").as_deref(),
        Some("/api/v1/orders/:order_id/items/:item_id")
    );
}

#[test]
fn matches_literal_pattern() {
    assert_eq!(
        matcher().resolve("/api/v1/health").as_deref(),
        Some("/api/v1/health")
    );
}

#[test]
fn strips_query_string_before_matching() {
    assert_eq!(
        matcher()
            .resolve("/api/v1/users/42?verbose=true")
            .as_deref(),
        Some("/api/v1/users/:id")
    );
}

#[test]
fn unmatched_path_returns_none() {
    assert_eq!(matcher().resolve("/api/v2/unknown"), None);
}

#[test]
fn unmatched_path_with_query_returns_none() {
    assert_eq!(matcher().resolve("/api/v2/unknown?a=1"), None);
}

#[test]
fn segment_count_must_match() {
    assert_eq!(matcher().resolve("/api/v1/users/42/extra"), None);
}

#[test]
fn trailing_slash_is_normalized() {
    assert_eq!(
        matcher().resolve("/api/v1/users/42/").as_deref(),
        Some("/api/v1/users/:id")
    );
}

#[test]
fn first_matching_pattern_wins() {
    let m = EndpointMatcher::new(&patterns(&["/a/:x", "/a/b"]));
    assert_eq!(m.resolve("/a/b").as_deref(), Some("/a/:x"));
}

#[test]
fn empty_matcher_matches_nothing() {
    let m = EndpointMatcher::new(&[]);
    assert_eq!(m.resolve("/anything"), None);
}
