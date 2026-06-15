use std::time::Instant;

use crate::error::AppError;
use crate::http::router::AppState;
use crate::pipeline::AnalysisMessage;
use crate::telemetry::metrics::{ResolvedEndpoint, UpstreamTimer};
use axum::body::Bytes;
use axum::extract::State;
use axum::http::Method;
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

    // Side-effect safety check
    if !state.config.proxy.allow_http_side_effects {
        match method {
            Method::POST | Method::PUT | Method::PATCH | Method::DELETE => {
                tracing::warn!(method = %method, path = path_and_query, "blocked mutating method");
                return Ok(axum::http::StatusCode::METHOD_NOT_ALLOWED.into_response());
            }
            _ => {}
        }
    }

    let path = uri.path();

    // 1. Forward to baseline FIRST — blocking hot path, zero added latency
    let baseline_timer = UpstreamTimer::start("baseline", endpoint.0.clone());
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
    baseline_timer.finish(baseline_result.is_ok());

    let baseline_response = baseline_result.map_err(|e| {
        tracing::error!(error = %e, "baseline upstream failed");
        AppError::from(e)
    })?;

    tracing::debug!(
        status = baseline_response.status,
        body_len = baseline_response.body.len(),
        "baseline response received"
    );

    // 2. Build response to client from baseline — return immediately
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

    // 3. Fire candidate + control in background for analysis
    let upstream = state.upstream.clone();
    let analysis_tx = state.analysis_tx.clone();
    let method_clone = method.clone();
    let path_owned = path.to_string();
    let path_and_query_owned = path_and_query.to_string();
    let headers_clone = headers.clone();
    let endpoint_key = endpoint.0.clone();

    let analysis_span = tracing::info_span!("analysis", endpoint = %path);

    tokio::spawn(
        async move {
            let candidate_body = body.clone();
            let control_body = body;

            let candidate_future = async {
                let timer = UpstreamTimer::start("candidate", endpoint_key.clone());
                let result = upstream
                    .send(
                        &upstream.candidate,
                        &method_clone,
                        &path_and_query_owned,
                        &headers_clone,
                        candidate_body,
                    )
                    .await;
                timer.finish(result.is_ok());
                result
            };

            let control_future = async {
                let timer = UpstreamTimer::start("control", endpoint_key.clone());
                let result = upstream
                    .send(
                        &upstream.control,
                        &method_clone,
                        &path_and_query_owned,
                        &headers_clone,
                        control_body,
                    )
                    .await;
                timer.finish(result.is_ok());
                result
            };

            let (candidate_result, control_result) = tokio::join!(candidate_future, control_future);

            if let Err(ref e) = candidate_result {
                tracing::warn!(error = %e, "candidate upstream failed");
            }
            if let Err(ref e) = control_result {
                tracing::warn!(error = %e, "control upstream failed");
            }

            let msg = AnalysisMessage {
                path: path_owned,
                received_at,
                baseline_response,
                candidate_response: candidate_result.ok(),
                control_response: control_result.ok(),
            };

            // try_send sheds load when the consumer lags instead of queueing
            // unbounded background tasks behind a full channel.
            if let Err(e) = analysis_tx.try_send(msg) {
                tracing::warn!(error = %e, "analysis channel unavailable, dropping diff");
            }
        }
        .instrument(analysis_span),
    );

    Ok(client_response.into_response())
}
