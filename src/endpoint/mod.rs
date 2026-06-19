//! Endpoint identification via path-template matching, backed by `matchit`
//! (the same radix-tree router axum uses internally).
//!
//! Config defines templates like `/api/v1/users/:id`. A request path resolves
//! to the matching template, or `None` when none matches — unmatched paths are
//! proxied but excluded from analysis (keeps the store and the metric label
//! cardinality bounded by the configured endpoint set).
//!
//! When templates overlap, the more specific one wins: a static segment beats a
//! `:param` at the same position (matchit's radix-tree priority), independent of
//! config order. Duplicate and trailing slashes are normalized, and the query
//! string is stripped, before matching.

#[cfg(test)]
mod tests;

use matchit::Router;

/// Splits a path into its non-empty segments, normalizing duplicate and
/// trailing slashes.
fn split_segments(path: &str) -> impl Iterator<Item = &str> {
    path.split('/').filter(|s| !s.is_empty())
}

/// Canonicalize a path or template: strip the query string and collapse
/// duplicate/trailing slashes, so `/a//b/` and `/a/b` resolve identically.
/// `matchit` is slash-strict, so both registration and lookup go through this.
fn canonical(path: &str) -> String {
    let path = path.split('?').next().unwrap_or(path);
    let mut out = String::new();
    for segment in split_segments(path) {
        out.push('/');
        out.push_str(segment);
    }
    if out.is_empty() {
        out.push('/');
    }
    out
}

pub struct EndpointMatcher {
    router: Router<String>,
}

impl EndpointMatcher {
    pub fn new(patterns: &[String]) -> Self {
        let mut router = Router::new();
        for pattern in patterns {
            // The route is the canonical template; the stored value is the
            // original config string, so a resolved endpoint matches the
            // pattern keys the classifiers and suppress lists are keyed by.
            // A conflicting template (e.g. two params at one position) is
            // skipped with a warning rather than aborting startup.
            if let Err(e) = router.insert(canonical(pattern), pattern.clone()) {
                tracing::warn!(pattern = %pattern, error = %e, "ignoring endpoint pattern");
            }
        }
        Self { router }
    }

    /// Resolve a request path to a configured endpoint template, or `None` when
    /// no template matches. The query string is stripped and slashes normalized
    /// before matching; overlapping templates resolve to the most specific one.
    pub fn resolve(&self, path: &str) -> Option<String> {
        self.router
            .at(&canonical(path))
            .ok()
            .map(|matched| (*matched.value).clone())
    }
}
