use crate::upstream::header::headers_to_json;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use serde_json::json;

fn map(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in pairs {
        headers.append(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
    }
    headers
}

#[test]
fn single_valued_header_is_a_string() {
    let json = headers_to_json(&map(&[("content-type", "application/json")]));
    assert_eq!(json, json!({ "content-type": "application/json" }));
}

#[test]
fn repeated_header_becomes_an_ordered_array() {
    let json = headers_to_json(&map(&[("vary", "accept"), ("vary", "origin")]));
    assert_eq!(json, json!({ "vary": ["accept", "origin"] }));
}

#[test]
fn header_names_are_case_normalized() {
    let json = headers_to_json(&map(&[("Cache-Control", "no-store")]));
    assert_eq!(json, json!({ "cache-control": "no-store" }));
}

#[test]
fn volatile_and_sensitive_headers_are_dropped() {
    let json = headers_to_json(&map(&[
        ("content-type", "application/json"),
        ("date", "Wed, 25 Jun 2026 00:00:00 GMT"),
        ("content-length", "42"),
        ("content-encoding", "gzip"),
        ("set-cookie", "session=secret"),
    ]));
    assert_eq!(json, json!({ "content-type": "application/json" }));
}

#[test]
fn hop_by_hop_headers_are_dropped() {
    let json = headers_to_json(&map(&[
        ("etag", "\"abc\""),
        ("connection", "keep-alive"),
        ("keep-alive", "timeout=5"),
        ("transfer-encoding", "chunked"),
        ("upgrade", "h2c"),
    ]));
    assert_eq!(json, json!({ "etag": "\"abc\"" }));
}

#[test]
fn empty_map_is_an_empty_object() {
    assert_eq!(headers_to_json(&HeaderMap::new()), json!({}));
}
