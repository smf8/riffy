//! Render a captured request as a replayable curl command.
//!
//! The host is left as a `$RIFFY_TARGET` placeholder; the dashboard substitutes
//! the chosen upstream (baseline / candidate / control) when copying. Building
//! happens in the consumer, off the proxy hot path.

use std::borrow::Cow;

use super::RequestSnapshot;
use axum::http::HeaderMap;
use bytes::Bytes;

/// Placeholder host substituted by the dashboard for baseline/candidate/control.
pub const TARGET_PLACEHOLDER: &str = "$RIFFY_TARGET";

/// Max request-body size embedded inline; larger (or non-UTF-8) bodies are
/// omitted with a comment so a stored curl never balloons or carries raw bytes.
const MAX_CURL_BODY_BYTES: usize = 64 * 1024;

/// Placeholder written in place of a redacted credential header value.
const REDACTED: &str = "<redacted>";

/// Request headers whose values are redacted unless the endpoint sets
/// `store_credentials_header`.
const CREDENTIAL_HEADERS: &[&str] = &[
    "authorization",
    "cookie",
    "proxy-authorization",
    "x-api-key",
    "x-auth-token",
];

/// Headers never emitted into the curl: curl derives `host` from the URL and
/// recomputes `content-length`, and hop-by-hop headers must not be replayed.
const SKIP_HEADERS: &[&str] = &[
    "host",
    "content-length",
    "connection",
    "keep-alive",
    "transfer-encoding",
    "te",
    "upgrade",
];

/// Render `snap` as a multi-line curl command. Deterministic: headers are
/// sorted, so the output is stable across runs (and testable).
pub fn build_curl(snap: &RequestSnapshot) -> String {
    let mut lines: Vec<String> = Vec::new();
    lines.push(format!("curl -X {}", snap.method.as_str()));
    lines.push(format!(
        "  {}",
        shell_quote(&format!("{TARGET_PLACEHOLDER}{}", snap.path_and_query))
    ));

    for (name, value) in sorted_headers(&snap.headers) {
        let rendered = if snap.redact_credentials && CREDENTIAL_HEADERS.contains(&name.as_str()) {
            REDACTED.to_owned()
        } else {
            value
        };
        lines.push(format!(
            "  -H {}",
            shell_quote(&format!("{name}: {rendered}"))
        ));
    }

    // The omitted-body note is a trailing comment with no line-continuation
    // backslash before it, so it never folds into the command.
    let mut trailer: Option<String> = None;
    match body_arg(&snap.body) {
        BodyArg::Inline(text) => lines.push(format!("  --data-raw {}", shell_quote(&text))),
        BodyArg::Omitted(reason) => trailer = Some(format!("# body omitted ({reason})")),
        BodyArg::None => {}
    }

    let command = lines.join(" \\\n");
    match trailer {
        Some(comment) => format!("{command}\n  {comment}"),
        None => command,
    }
}

/// Headers to emit, as `(name, value)` pairs sorted for deterministic output.
/// Skips `SKIP_HEADERS`; non-UTF-8 values are rendered lossily.
fn sorted_headers(headers: &HeaderMap) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = headers
        .iter()
        .filter(|(name, _)| !SKIP_HEADERS.contains(&name.as_str()))
        .map(|(name, value)| {
            (
                name.as_str().to_owned(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect();
    out.sort();
    out
}

enum BodyArg {
    None,
    Inline(String),
    Omitted(String),
}

fn body_arg(body: &Bytes) -> BodyArg {
    if body.is_empty() {
        return BodyArg::None;
    }
    if body.len() > MAX_CURL_BODY_BYTES {
        return BodyArg::Omitted(format!(
            "{} bytes > {MAX_CURL_BODY_BYTES} limit",
            body.len()
        ));
    }
    match std::str::from_utf8(body) {
        Ok(text) => BodyArg::Inline(text.to_owned()),
        Err(_) => BodyArg::Omitted("binary".to_owned()),
    }
}

/// Shell-quote `s` for safe inclusion in the rendered curl command, delegating
/// to `shell-escape` (POSIX `'...'` quoting, embedded `'` escaped as `'\''`).
/// Strings made up solely of shell-safe characters are returned unquoted.
fn shell_quote(s: &str) -> String {
    shell_escape::unix::escape(Cow::Borrowed(s)).into_owned()
}
