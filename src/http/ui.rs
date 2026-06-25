use axum::http::header;
use axum::response::IntoResponse;

const INDEX_HTML: &str = include_str!("ui/index.html");
const ALPINE_JS: &str = include_str!("ui/alpine.min.js");

pub async fn index() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        INDEX_HTML,
    )
}

pub async fn alpine_js() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        ALPINE_JS,
    )
}
