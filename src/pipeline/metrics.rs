use std::time::Duration;

pub fn record_sample_stored(endpoint: &str, lag: Duration) {
    metrics::histogram!("riffy_sample_store_lag_seconds").record(lag.as_secs_f64());
    metrics::counter!(
        "riffy_samples_stored_total",
        "endpoint" => endpoint.to_owned(),
    )
    .increment(1);
}
