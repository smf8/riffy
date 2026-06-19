//! Upstream-call metrics: the duration of each baseline/candidate/control
//! request, labelled by upstream role, endpoint, and outcome. The drop-guard
//! timing primitive lives in `crate::telemetry::timer`.

use std::sync::Arc;
use std::time::Duration;

use crate::telemetry::timer::GuardedTimer;

/// Build the drop-guard timer for one upstream call. Call `finish(outcome(..))`
/// on completion; a dropped timer records `outcome="cancelled"` (the awaiting
/// future was cancelled).
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

/// Map a call's success into the `outcome` label vocabulary.
pub fn outcome(success: bool) -> &'static str {
    if success {
        "ok"
    } else {
        "error"
    }
}
