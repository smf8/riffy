use crate::endpoint::EndpointMatcher;

fn matcher() -> EndpointMatcher {
    EndpointMatcher::new([
        "/api/v1/users/:id",
        "/api/v1/orders/:order_id/items/:item_id",
        "/api/v1/health",
    ])
}

#[test]
fn matches_single_param() {
    assert_eq!(matcher().resolve("/api/v1/users/42"), "/api/v1/users/:id");
}

#[test]
fn matches_multiple_params() {
    assert_eq!(
        matcher().resolve("/api/v1/orders/7/items/99"),
        "/api/v1/orders/:order_id/items/:item_id"
    );
}

#[test]
fn matches_literal_pattern() {
    assert_eq!(matcher().resolve("/api/v1/health"), "/api/v1/health");
}

#[test]
fn strips_query_string_before_matching() {
    assert_eq!(
        matcher().resolve("/api/v1/users/42?verbose=true"),
        "/api/v1/users/:id"
    );
}

#[test]
fn unmatched_path_falls_back_to_raw_path() {
    assert_eq!(matcher().resolve("/api/v2/unknown"), "/api/v2/unknown");
}

#[test]
fn unmatched_path_strips_query_string() {
    assert_eq!(matcher().resolve("/api/v2/unknown?a=1"), "/api/v2/unknown");
}

#[test]
fn segment_count_must_match() {
    assert_eq!(
        matcher().resolve("/api/v1/users/42/extra"),
        "/api/v1/users/42/extra"
    );
}

#[test]
fn trailing_slash_is_normalized() {
    assert_eq!(matcher().resolve("/api/v1/users/42/"), "/api/v1/users/:id");
}

#[test]
fn first_matching_pattern_wins() {
    let m = EndpointMatcher::new(["/a/:x", "/a/b"]);
    assert_eq!(m.resolve("/a/b"), "/a/:x");
}

#[test]
fn empty_matcher_returns_raw_path() {
    let m = EndpointMatcher::new(Vec::<String>::new());
    assert_eq!(m.resolve("/anything"), "/anything");
}
