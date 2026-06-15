//! Minimal admin dashboard: a single embedded HTML page (Alpine.js) that
//! consumes the diff query API. No build step — the page and the vendored
//! Alpine runtime are baked into the binary via `include_str!` and served by
//! the admin router.

use axum::http::header;
use axum::response::IntoResponse;

const INDEX_HTML: &str = include_str!("ui/index.html");
const ALPINE_JS: &str = include_str!("ui/alpine.min.js");

/// `GET /` — the dashboard page.
pub async fn index() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        INDEX_HTML,
    )
}

/// `GET /alpine.js` — the vendored Alpine.js runtime.
pub async fn alpine_js() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        ALPINE_JS,
    )
}
