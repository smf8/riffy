//! Pipeline metrics: the request → diff publish lag and the count of differing
//! fields per endpoint. Recorded by the detached consumer task, which client
//! cancellation can never drop — so no drop guard is needed here.

use std::time::Duration;

/// Record pipeline lag (request received → diff published) and the number of
/// differing fields, per endpoint.
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
