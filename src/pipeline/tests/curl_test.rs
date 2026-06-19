use crate::pipeline::curl::{build_curl, TARGET_PLACEHOLDER};
use crate::pipeline::RequestSnapshot;
use axum::http::{HeaderMap, Method};
use bytes::Bytes;

fn snapshot(
    method: Method,
    path_and_query: &str,
    headers: &[(&str, &str)],
    body: &[u8],
    redact_credentials: bool,
) -> RequestSnapshot {
    let mut map = HeaderMap::new();
    for (name, value) in headers {
        map.append(
            axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            axum::http::HeaderValue::from_str(value).unwrap(),
        );
    }
    RequestSnapshot {
        method,
        path_and_query: path_and_query.to_owned(),
        headers: map,
        body: Bytes::copy_from_slice(body),
        redact_credentials,
    }
}

#[test]
fn renders_method_url_and_sorted_headers() {
    let curl = build_curl(&snapshot(
        Method::GET,
        "/api/v1/users/42?x=1",
        &[("x-trace", "abc"), ("accept", "application/json")],
        b"",
        true,
    ));

    let expected = format!(
        "curl -X GET \\\n  '{TARGET_PLACEHOLDER}/api/v1/users/42?x=1' \\\n  -H 'accept: application/json' \\\n  -H 'x-trace: abc'"
    );
    assert_eq!(curl, expected);
}

#[test]
fn redacts_credential_headers_when_enabled() {
    let curl = build_curl(&snapshot(
        Method::GET,
        "/x",
        &[
            ("authorization", "Bearer secret"),
            ("cookie", "session=abc"),
            ("x-api-key", "k"),
            ("accept", "application/json"),
        ],
        b"",
        true,
    ));

    assert!(curl.contains("-H 'authorization: <redacted>'"));
    assert!(curl.contains("-H 'cookie: <redacted>'"));
    assert!(curl.contains("-H 'x-api-key: <redacted>'"));
    // Non-credential headers are untouched.
    assert!(curl.contains("-H 'accept: application/json'"));
    assert!(!curl.contains("Bearer secret"));
    assert!(!curl.contains("session=abc"));
}

#[test]
fn stores_credential_headers_verbatim_when_disabled() {
    let curl = build_curl(&snapshot(
        Method::GET,
        "/x",
        &[("authorization", "Bearer secret")],
        b"",
        false,
    ));

    assert!(curl.contains("-H 'authorization: Bearer secret'"));
    assert!(!curl.contains("<redacted>"));
}

#[test]
fn skips_host_content_length_and_hop_by_hop_headers() {
    let curl = build_curl(&snapshot(
        Method::GET,
        "/x",
        &[
            ("host", "proxy.internal"),
            ("content-length", "3"),
            ("connection", "keep-alive"),
            ("transfer-encoding", "chunked"),
            ("accept", "application/json"),
        ],
        b"",
        true,
    ));

    assert!(!curl.contains("host:"));
    assert!(!curl.contains("content-length:"));
    assert!(!curl.contains("connection:"));
    assert!(!curl.contains("transfer-encoding:"));
    assert!(curl.contains("-H 'accept: application/json'"));
}

#[test]
fn inlines_text_body() {
    let curl = build_curl(&snapshot(
        Method::POST,
        "/x",
        &[("content-type", "application/json")],
        br#"{"q":1}"#,
        true,
    ));

    assert!(curl.starts_with("curl -X POST"));
    assert!(curl.contains(r#"--data-raw '{"q":1}'"#));
}

#[test]
fn escapes_single_quotes_in_body_and_headers() {
    let curl = build_curl(&snapshot(
        Method::POST,
        "/x?name=o'brien",
        &[("x-note", "it's fine")],
        b"a'b",
        true,
    ));

    // Each literal single quote becomes the shell-safe sequence '\''.
    assert!(curl.contains(r"'$RIFFY_TARGET/x?name=o'\''brien'"));
    assert!(curl.contains(r"-H 'x-note: it'\''s fine'"));
    assert!(curl.contains(r"--data-raw 'a'\''b'"));
}

#[test]
fn omits_binary_body_with_comment() {
    let curl = build_curl(&snapshot(
        Method::POST,
        "/x",
        &[],
        &[0xff, 0xfe, 0x00],
        true,
    ));

    assert!(curl.contains("# body omitted (binary)"));
    assert!(!curl.contains("--data-raw"));
    // The comment is on its own line, not folded into the command.
    assert!(!curl.contains(" \\\n  # body omitted"));
}

#[test]
fn omits_oversized_body_with_comment() {
    let big = vec![b'a'; 64 * 1024 + 1];
    let curl = build_curl(&snapshot(Method::POST, "/x", &[], &big, true));

    assert!(curl.contains("# body omitted ("));
    assert!(curl.contains("limit)"));
    assert!(!curl.contains("--data-raw"));
}

#[test]
fn empty_body_emits_no_data_flag() {
    let curl = build_curl(&snapshot(Method::GET, "/x", &[], b"", true));
    assert!(!curl.contains("--data-raw"));
    assert!(!curl.contains("# body omitted"));
}
