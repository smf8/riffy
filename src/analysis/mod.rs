use std::collections::HashMap;

use crate::compare::flatten::FlatDiff;

pub mod collector;
pub mod filter;
pub mod joined;

#[cfg(test)]
mod tests;

use joined::JoinedEndpoint;

/// Sink for per-request difference observations, keyed by endpoint and field
/// path. Implementations must be safe for concurrent use.
pub trait DifferenceCollector: Send + Sync {
    /// Record one analyzed request: bump the endpoint total and the raw/noise
    /// counters of every differing field path.
    fn record(
        &self,
        endpoint: &str,
        raw: &HashMap<String, FlatDiff>,
        noise: &HashMap<String, FlatDiff>,
    );

    /// Snapshot all per-endpoint counters joined into `JoinedEndpoint`s.
    fn snapshot(&self) -> Vec<JoinedEndpoint>;
}
