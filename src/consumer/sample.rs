use super::curl::build_curl;
use super::AnalysisMessage;
use crate::storage::RawSample;
use crate::upstream::body::decode_compressed_body;
use crate::upstream::client::UpstreamResponse;
use crate::upstream::header::headers_to_json;
use axum::http::HeaderMap;
use chrono::Utc;

pub(super) async fn assemble(
    endpoint: &str,
    msg: &AnalysisMessage,
    max_body_bytes: usize,
) -> Option<RawSample> {
    let baseline_status = msg.baseline_response.status;

    let Some(baseline_body) = storable_json(&msg.baseline_response, max_body_bytes).await else {
        tracing::warn!(
            endpoint = %endpoint,
            "skipping sample: baseline body is not storable json (non-json or over size cap)"
        );
        return None;
    };
    let baseline_headers = headers_text(&msg.baseline_response.headers);

    let candidate = secondary(
        endpoint,
        "candidate",
        &msg.candidate_response,
        baseline_status,
        max_body_bytes,
    )
    .await?;
    let control = secondary(
        endpoint,
        "control",
        &msg.control_response,
        baseline_status,
        max_body_bytes,
    )
    .await?;

    Some(RawSample {
        id: String::new(),
        endpoint: endpoint.to_owned(),
        timestamp: Utc::now(),
        baseline_status,
        baseline_body,
        baseline_headers,
        candidate_status: candidate.status,
        candidate_body: candidate.body,
        candidate_headers: candidate.headers,
        control_status: control.status,
        control_body: control.body,
        control_headers: control.headers,
        request_curl: msg.request.as_ref().map(build_curl),
    })
}

struct Secondary {
    status: Option<u16>,
    body: Option<String>,
    headers: Option<String>,
}

impl Secondary {
    fn failed() -> Self {
        Self {
            status: None,
            body: None,
            headers: None,
        }
    }

    fn status_only(status: u16) -> Self {
        Self {
            status: Some(status),
            body: None,
            headers: None,
        }
    }
}

// `None` discards the whole sample: a status match with an unstorable body, recorded
// as a bodyless match, would mask a real body/header regression.
async fn secondary(
    endpoint: &str,
    upstream: &str,
    response: &Option<UpstreamResponse>,
    baseline_status: u16,
    max_body_bytes: usize,
) -> Option<Secondary> {
    let Some(response) = response else {
        return Some(Secondary::failed());
    };
    if response.status != baseline_status {
        return Some(Secondary::status_only(response.status));
    }
    match storable_json(response, max_body_bytes).await {
        Some(body) => Some(Secondary {
            status: Some(response.status),
            body: Some(body),
            headers: Some(headers_text(&response.headers)),
        }),
        None => {
            tracing::error!(
                endpoint = %endpoint,
                upstream = %upstream,
                "discarding sample: upstream matched baseline status but returned an unstorable body (non-json or over size cap)"
            );
            None
        }
    }
}

async fn storable_json(response: &UpstreamResponse, max_body_bytes: usize) -> Option<String> {
    let decoded = decode_compressed_body(response).await?;
    if decoded.len() > max_body_bytes {
        tracing::debug!(
            len = decoded.len(),
            max = max_body_bytes,
            "skipping body over size cap"
        );
        return None;
    }
    if serde_json::from_slice::<serde::de::IgnoredAny>(&decoded).is_err() {
        tracing::debug!("skipping non-json body");
        return None;
    }
    String::from_utf8(decoded.into_owned()).ok()
}

fn headers_text(headers: &HeaderMap) -> String {
    serde_json::to_string(&headers_to_json(headers)).unwrap_or_else(|_| "{}".to_owned())
}
