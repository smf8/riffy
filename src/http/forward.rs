use std::sync::Arc;
use std::time::Instant;

use crate::error::AppError;
use crate::http::metrics::{ResolvedEndpoint, UNMATCHED_ENDPOINT};
use crate::http::router::AppState;
use crate::pipeline::{AnalysisMessage, RequestSnapshot};
use crate::upstream::client::UpstreamResponse;
use crate::upstream::metrics::{outcome, request_timer};
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method};
use axum::response::{IntoResponse, Response};
use axum::Extension;
use tracing::Instrument;

#[tracing::instrument(skip(state, endpoint, headers, body), fields(method = %method, path = %uri))]
pub async fn forward(
    State(state): State<AppState>,
    Extension(endpoint): Extension<ResolvedEndpoint>,
    method: Method,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Result<Response, AppError> {
    let received_at = Instant::now();
    let path_and_query = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");

    let endpoint_label: Arc<str> = endpoint
        .0
        .clone()
        .unwrap_or_else(|| Arc::from(UNMATCHED_ENDPOINT));

    let baseline_timer = request_timer("baseline", endpoint_label);
    let baseline_result = state
        .upstream
        .send(
            &state.upstream.baseline,
            &method,
            path_and_query,
            &headers,
            body.clone(),
        )
        .await;
    baseline_timer.finish(outcome(baseline_result.is_ok()));

    let baseline_response = baseline_result.map_err(|e| {
        tracing::error!(error = %e, "baseline upstream failed");
        AppError::from(e)
    })?;

    tracing::debug!(
        status = baseline_response.status,
        body_len = baseline_response.body.len(),
        "baseline response received"
    );

    let mut builder = axum::http::Response::builder().status(baseline_response.status);
    for (name, value) in baseline_response.headers.iter() {
        builder = builder.header(name, value);
    }
    let client_response = builder
        .body(axum::body::Body::from(baseline_response.body.clone()))
        .unwrap_or_else(|_| {
            axum::http::Response::builder()
                .status(500)
                .body(axum::body::Body::from("internal error"))
                .unwrap()
        });

    if !state.config.proxy.allow_http_side_effects {
        match method {
            Method::POST | Method::PUT | Method::PATCH | Method::DELETE => {
                tracing::warn!(method = %method, path = path_and_query, "blocked mutating method");
                return Ok(axum::http::StatusCode::METHOD_NOT_ALLOWED.into_response());
            }
            _ => {}
        }
    }

    dispatch_analysis(
        &state,
        &endpoint,
        Dispatch {
            method,
            path: uri.path().to_owned(),
            path_and_query: path_and_query.to_owned(),
            headers,
            body,
            received_at,
            baseline_response,
        },
    );

    Ok(client_response.into_response())
}

struct Dispatch {
    method: Method,
    // Endpoint resolution happens in the consumer, off the proxy hot path.
    path: String,
    path_and_query: String,
    headers: HeaderMap,
    body: Bytes,
    received_at: Instant,
    baseline_response: UpstreamResponse,
}

fn dispatch_analysis(state: &AppState, endpoint: &ResolvedEndpoint, req: Dispatch) {
    let Some(endpoint_key) = endpoint.0.clone() else {
        return;
    };

    let ep_cfg = state
        .config
        .endpoints
        .iter()
        .find(|e| e.pattern == endpoint_key.as_ref());

    // sample_rate=0.0 always skips; sample_rate=1.0 bypasses the RNG entirely.
    let sample_rate = ep_cfg.map(|e| e.sample_rate).unwrap_or(1.0);
    if sample_rate < 1.0 && rand::random::<f64>() >= sample_rate {
        return;
    }

    let capture_request_curl = ep_cfg.map(|e| e.capture_request_curl).unwrap_or(false);
    let store_credentials_header = ep_cfg.map(|e| e.store_credentials_header).unwrap_or(false);

    let upstream = state.upstream.clone();
    let analysis_tx = state.analysis_tx.clone();

    let Dispatch {
        method,
        path,
        path_and_query,
        headers,
        body,
        received_at,
        baseline_response,
    } = req;

    let analysis_span = tracing::info_span!("analysis", endpoint = %endpoint_key);

    tokio::spawn(
        async move {
            let candidate_body = body.clone();
            let control_body = body.clone();

            let candidate_future = async {
                let timer = request_timer("candidate", endpoint_key.clone());
                let result = upstream
                    .send(
                        &upstream.candidate,
                        &method,
                        &path_and_query,
                        &headers,
                        candidate_body,
                    )
                    .await;
                timer.finish(outcome(result.is_ok()));
                result
            };

            let control_future = async {
                let timer = request_timer("control", endpoint_key.clone());
                let result = upstream
                    .send(
                        &upstream.control,
                        &method,
                        &path_and_query,
                        &headers,
                        control_body,
                    )
                    .await;
                timer.finish(outcome(result.is_ok()));
                result
            };

            let (candidate_result, control_result) = tokio::join!(candidate_future, control_future);

            if let Err(ref e) = candidate_result {
                tracing::warn!(error = %e, "candidate upstream failed");
            }
            if let Err(ref e) = control_result {
                tracing::warn!(error = %e, "control upstream failed");
            }

            // Capture after join: the upstream calls hold borrows on method/headers/path.
            let request = capture_request_curl.then(move || RequestSnapshot {
                method,
                path_and_query,
                headers,
                body,
                redact_credentials: !store_credentials_header,
            });

            let msg = AnalysisMessage {
                path,
                received_at,
                baseline_response,
                candidate_response: candidate_result.ok(),
                control_response: control_result.ok(),
                request,
            };

            // try_send sheds load when the consumer lags rather than queueing unbounded.
            if let Err(e) = analysis_tx.try_send(msg) {
                tracing::warn!(error = %e, "analysis channel unavailable, dropping diff");
            }
        }
        .instrument(analysis_span),
    );
}
