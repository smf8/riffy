//! Shared test-only helpers. The metrics recorder is process-global, so every
//! unit test that asserts on rendered metrics must use this single installed
//! instance — a second `install_recorder()` call fails.

use std::sync::{Arc, OnceLock};

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

/// The one process-global Prometheus recorder shared by all metric tests.
/// Tests stay independent of each other by using unique endpoint label values.
pub fn recorder() -> &'static PrometheusHandle {
    static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
    HANDLE.get_or_init(|| {
        PrometheusBuilder::new()
            .install_recorder()
            .expect("failed to install test recorder")
    })
}

pub fn endpoint(name: &str) -> Arc<str> {
    Arc::from(name)
}

/// Rendered lines for `metric` that carry the given endpoint label.
pub fn lines_for<'a>(rendered: &'a str, metric: &str, endpoint: &str) -> Vec<&'a str> {
    let label = format!("endpoint=\"{endpoint}\"");
    rendered
        .lines()
        .filter(|line| line.starts_with(metric) && line.contains(&label))
        .collect()
}
