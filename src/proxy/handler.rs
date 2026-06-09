use crate::error::AppError;
use crate::proxy::router::{AnalysisMessage, AppState};
use axum::body::Bytes;
use axum::extract::State;
use axum::http::Method;
use axum::response::{IntoResponse, Response};
use tracing::Instrument;

#[tracing::instrument(skip(state, headers, body), fields(method = %method, path = %uri))]
pub async fn proxy_handler(
    State(state): State<AppState>,
    method: Method,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Result<Response, AppError> {
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

    // 1. Forward to primary FIRST — blocking hot path, zero added latency
    let primary_result = state
        .upstream
        .send(
            &state.upstream.primary,
            &method,
            path_and_query,
            &headers,
            body.clone(),
        )
        .await;

    let primary_response = primary_result.map_err(|e| {
        tracing::error!(error = %e, "primary upstream failed");
        AppError::from(e)
    })?;

    tracing::debug!(
        status = primary_response.status,
        body_len = primary_response.body.len(),
        "primary response received"
    );

    // 2. Build response to client from primary — return immediately
    let mut builder = axum::http::Response::builder().status(primary_response.status);
    for (name, value) in primary_response.headers.iter() {
        builder = builder.header(name, value);
    }
    let client_response = builder
        .body(axum::body::Body::from(primary_response.body.clone()))
        .unwrap_or_else(|_| {
            axum::http::Response::builder()
                .status(500)
                .body(axum::body::Body::from("internal error"))
                .unwrap()
        });

    // 3. Fire candidate + secondary in background for analysis
    let upstream = state.upstream.clone();
    let analysis_tx = state.analysis_tx.clone();
    let method_clone = method.clone();
    let path_owned = path.to_string();
    let path_and_query_owned = path_and_query.to_string();
    let headers_clone = headers.clone();

    let analysis_span = tracing::info_span!("analysis", endpoint = %path);

    tokio::spawn(
        async move {
            let candidate_future = upstream.send(
                &upstream.candidate,
                &method_clone,
                &path_and_query_owned,
                &headers_clone,
                body.clone(),
            );

            let secondary_future = upstream.send(
                &upstream.secondary,
                &method_clone,
                &path_and_query_owned,
                &headers_clone,
                body,
            );

            let (candidate_result, secondary_result) =
                tokio::join!(candidate_future, secondary_future);

            if let Err(ref e) = candidate_result {
                tracing::warn!(error = %e, "candidate upstream failed");
            }
            if let Err(ref e) = secondary_result {
                tracing::warn!(error = %e, "secondary upstream failed");
            }

            let msg = AnalysisMessage {
                endpoint: path_owned.clone(),
                method: method_clone.to_string(),
                path: path_owned,
                primary_response: Some(primary_response),
                candidate_response: candidate_result.ok(),
                secondary_response: secondary_result.ok(),
            };

            if analysis_tx.send(msg).await.is_err() {
                tracing::warn!("analysis channel closed, dropping diff");
            }
        }
        .instrument(analysis_span),
    );

    Ok(client_response.into_response())
}
