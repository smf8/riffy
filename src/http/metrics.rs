//! HTTP-layer metrics: the proxy request count + duration recorded by the
//! `track_proxy` middleware, the resolved-endpoint label shared with the
//! forwarding handler, and the admin `/metrics` exposition handler. The
//! drop-guard timing primitive lives in `crate::telemetry::timer`.

use std::sync::Arc;
use std::time::Duration;

use crate::http::router::AppState;
use crate::telemetry::timer::GuardedTimer;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use metrics_exporter_prometheus::PrometheusHandle;

/// Endpoint key resolved once by the metrics middleware and shared with the
/// proxy handler through request extensions. `None` means the path matched no
/// configured endpoint template — proxied, but excluded from analysis.
#[derive(Clone)]
pub struct ResolvedEndpoint(pub Option<Arc<str>>);

/// Metric label for requests whose path matched no configured endpoint
/// template — bounds `endpoint` label cardinality (all such paths collapse here).
pub const UNMATCHED_ENDPOINT: &str = "undefined";

/// Admin-server handler rendering the Prometheus exposition text.
/// Returns an empty body when metrics are disabled.
pub async fn render_metrics(State(handle): State<Option<PrometheusHandle>>) -> String {
    handle.map(|h| h.render()).unwrap_or_default()
}

/// Proxy middleware: resolves the endpoint template once, exposes it to the
/// handler, and records request count + duration exactly once — with the real
/// status on completion, or `status="cancelled"` when the request future is
/// dropped mid-flight. Metric calls are no-ops when no recorder is installed
/// (metrics disabled).
pub async fn track_proxy(State(state): State<AppState>, mut req: Request, next: Next) -> Response {
    let resolved: Option<Arc<str>> = state.matcher.resolve(req.uri().path()).map(Arc::from);
    req.extensions_mut()
        .insert(ResolvedEndpoint(resolved.clone()));

    // Unmatched paths collapse to a single label value so cardinality stays
    // bounded by the configured endpoint set.
    let label = resolved.unwrap_or_else(|| Arc::from(UNMATCHED_ENDPOINT));
    let timer = proxy_request_timer(req.method().to_string(), label);
    let response = next.run(req).await;
    timer.finish(response.status().as_str());

    response
}

/// Build the drop-guard timer for one proxied request. `finish` is called with
/// the response status code (e.g. `"200"`); a dropped timer records
/// `status="cancelled"`. The count and the duration are emitted together, so
/// the duration includes abandoned requests (time until the client gave up)
/// and carries no survivorship bias.
pub(crate) fn proxy_request_timer(
    method: String,
    endpoint: Arc<str>,
) -> GuardedTimer<impl Fn(&str, Duration)> {
    GuardedTimer::start(move |status, elapsed| {
        metrics::counter!(
            "riffy_proxy_request_total",
            "method" => method.clone(),
            "endpoint" => endpoint.to_string(),
            "status" => status.to_owned(),
        )
        .increment(1);

        metrics::histogram!(
            "riffy_proxy_request_duration_seconds",
            "method" => method.clone(),
            "endpoint" => endpoint.to_string(),
        )
        .record(elapsed.as_secs_f64());
    })
}
