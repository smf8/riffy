use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::http::router::AppState;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

/// Endpoint key resolved once by the metrics middleware and shared with the
/// proxy handler through request extensions.
#[derive(Clone)]
pub struct ResolvedEndpoint(pub Arc<str>);

/// Label value recorded when a tracked future is dropped before completing
/// (client disconnect, server shutdown, or panic unwind).
const CANCELLED: &str = "cancelled";

/// Install the global Prometheus recorder. Must be called once at startup.
pub fn install_prometheus() -> anyhow::Result<PrometheusHandle> {
    Ok(PrometheusBuilder::new().install_recorder()?)
}

/// Admin-server handler rendering the Prometheus exposition text.
/// Returns an empty body when metrics are disabled.
pub async fn render_metrics(State(handle): State<Option<PrometheusHandle>>) -> String {
    handle.map(|h| h.render()).unwrap_or_default()
}

/// Proxy middleware: resolves the endpoint template once, exposes it to the
/// handler, and records request count + duration exactly once — with the
/// real status on completion, or `status="cancelled"` when the request
/// future is dropped mid-flight. Metric calls are no-ops when no recorder is
/// installed (metrics disabled).
pub async fn track_proxy(State(state): State<AppState>, mut req: Request, next: Next) -> Response {
    let endpoint: Arc<str> = state.matcher.resolve(req.uri().path()).into();
    req.extensions_mut()
        .insert(ResolvedEndpoint(endpoint.clone()));

    let guard = ProxyRequestGuard::start(req.method().to_string(), endpoint);
    let response = next.run(req).await;
    guard.complete(response.status());

    response
}

/// Drop guard for one proxied request. Cancellation in this stack works by
/// dropping the in-flight future, so any code after an `.await` is skipped —
/// the `Drop` impl is the only place guaranteed to run, and it records the
/// request as cancelled when `complete()` was never reached.
pub struct ProxyRequestGuard {
    method: String,
    endpoint: Arc<str>,
    started: Instant,
    completed: bool,
}

impl ProxyRequestGuard {
    pub fn start(method: String, endpoint: Arc<str>) -> Self {
        Self {
            method,
            endpoint,
            started: Instant::now(),
            completed: false,
        }
    }

    /// Record with the real response status, consuming the guard.
    pub fn complete(mut self, status: StatusCode) {
        self.completed = true;
        self.record(status.as_u16().to_string());
    }

    fn record(&self, status: String) {
        metrics::counter!(
            "riffy_proxy_request_total",
            "method" => self.method.clone(),
            "endpoint" => self.endpoint.to_string(),
            "status" => status,
        )
        .increment(1);

        // Duration includes abandoned requests (time spent until the client
        // gave up), so the histogram carries no survivorship bias.
        metrics::histogram!(
            "riffy_proxy_request_duration_seconds",
            "method" => self.method.clone(),
            "endpoint" => self.endpoint.to_string(),
        )
        .record(self.started.elapsed().as_secs_f64());
    }
}

impl Drop for ProxyRequestGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.record(CANCELLED.to_owned());
        }
    }
}

/// Drop guard timing one upstream call. `finish()` records the duration with
/// `outcome="ok"` or `"error"`; dropping the timer mid-flight (the awaiting
/// future was cancelled) records `outcome="cancelled"` instead.
pub struct UpstreamTimer {
    upstream: &'static str,
    endpoint: Arc<str>,
    started: Instant,
    finished: bool,
}

impl UpstreamTimer {
    pub fn start(upstream: &'static str, endpoint: Arc<str>) -> Self {
        Self {
            upstream,
            endpoint,
            started: Instant::now(),
            finished: false,
        }
    }

    /// Record with the call result, consuming the timer.
    pub fn finish(mut self, success: bool) {
        self.finished = true;
        self.record(if success { "ok" } else { "error" });
    }

    fn record(&self, outcome: &'static str) {
        metrics::histogram!(
            "riffy_upstream_request_duration_seconds",
            "upstream" => self.upstream,
            "endpoint" => self.endpoint.to_string(),
            "outcome" => outcome,
        )
        .record(self.started.elapsed().as_secs_f64());
    }
}

impl Drop for UpstreamTimer {
    fn drop(&mut self) {
        if !self.finished {
            self.record(CANCELLED);
        }
    }
}

/// Record pipeline lag (request received → diff published) and the number of
/// differing fields, per endpoint. Runs in the detached consumer task, which
/// client cancellation can never drop — no guard needed here.
pub fn record_diff_published(
    endpoint: &str,
    raw_fields: usize,
    noise_fields: usize,
    lag: Duration,
) {
    metrics::histogram!("riffy_diff_pipeline_lag_seconds").record(lag.as_secs_f64());

    if raw_fields > 0 {
        metrics::counter!(
            "riffy_diff_fields_total",
            "endpoint" => endpoint.to_owned(),
            "diff_type" => "raw",
        )
        .increment(raw_fields as u64);
    }
    if noise_fields > 0 {
        metrics::counter!(
            "riffy_diff_fields_total",
            "endpoint" => endpoint.to_owned(),
            "diff_type" => "noise",
        )
        .increment(noise_fields as u64);
    }
}
