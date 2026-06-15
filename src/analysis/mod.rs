use std::collections::HashMap;

use crate::compare::flatten::FieldDiff;

pub mod classify;
pub mod counters;
pub mod snapshot;

#[cfg(test)]
mod tests;

use snapshot::EndpointSnapshot;

/// Sink for per-request difference observations, keyed by endpoint and field
/// path. Implementations must be safe for concurrent use.
pub trait DiffCounters: Send + Sync {
    /// Record one analyzed request: bump the endpoint total and the raw/noise
    /// counters of every differing field path.
    fn record(
        &self,
        endpoint: &str,
        raw: &HashMap<String, FieldDiff>,
        noise: &HashMap<String, FieldDiff>,
    );

    /// Snapshot all per-endpoint counters joined into `EndpointSnapshot`s.
    fn snapshot(&self) -> Vec<EndpointSnapshot>;
}
