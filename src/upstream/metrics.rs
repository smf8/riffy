use std::sync::Arc;
use std::time::Duration;

use crate::telemetry::timer::GuardedTimer;

pub fn request_timer(
    upstream: &'static str,
    endpoint: Arc<str>,
) -> GuardedTimer<impl Fn(&str, Duration)> {
    GuardedTimer::start(move |outcome, elapsed| {
        metrics::histogram!(
            "riffy_upstream_request_duration_seconds",
            "upstream" => upstream,
            "endpoint" => endpoint.to_string(),
            "outcome" => outcome.to_owned(),
        )
        .record(elapsed.as_secs_f64());
    })
}

pub fn outcome(success: bool) -> &'static str {
    if success {
        "ok"
    } else {
        "error"
    }
}
