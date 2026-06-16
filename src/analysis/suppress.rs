use std::collections::HashMap;

use crate::compare::flatten::FieldDiff;
use crate::config::EndpointConfig;

/// Per-endpoint suppress lists: dot-separated JSON paths that are excluded from
/// diff analysis.
///
/// Two match modes, selected by whether the pattern contains `*`:
///
/// - **Subtree** (no `*`): suppresses the path itself and all descendants.
///   `"meta"` suppresses `"meta"`, `"meta.ts"`, `"meta.nested.v"`, etc.
///
/// - **Glob** (contains `*`): `*` matches exactly one dot-separated segment;
///   path length must equal pattern length.
///   `"meta.*"` matches `"meta.ts"` but not `"meta.nested.v"`.
///   `"items.*.id"` matches `"items.0.id"`, `"items.1.id"`, etc.
pub struct EndpointSuppressPaths {
    per_endpoint: HashMap<String, Vec<String>>,
}

impl EndpointSuppressPaths {
    pub fn from_config(endpoints: &[EndpointConfig]) -> Self {
        let per_endpoint = endpoints
            .iter()
            .filter(|e| !e.suppress_paths.is_empty())
            .map(|e| (e.pattern.clone(), e.suppress_paths.clone()))
            .collect();
        Self { per_endpoint }
    }

    /// Remove every key from `diffs` that matches a configured suppress path
    /// for `endpoint`.
    pub fn suppress(&self, endpoint: &str, diffs: &mut HashMap<String, FieldDiff>) {
        let Some(paths) = self.per_endpoint.get(endpoint) else {
            return;
        };
        diffs.retain(|k, _| !is_suppressed(k, paths));
    }
}

fn is_suppressed(path: &str, suppress_paths: &[String]) -> bool {
    suppress_paths.iter().any(|s| matches_pattern(path, s))
}

fn matches_pattern(path: &str, pattern: &str) -> bool {
    if pattern.contains('*') {
        glob_match(path, pattern)
    } else {
        // Subtree: exact match or any descendant.
        path == pattern || path.starts_with(&format!("{pattern}."))
    }
}

/// Segment-by-segment glob match: `*` matches exactly one dot-separated
/// segment; path and pattern must have the same number of segments.
fn glob_match(path: &str, pattern: &str) -> bool {
    let mut path_segs = path.split('.');
    let mut pat_segs = pattern.split('.');
    loop {
        match (path_segs.next(), pat_segs.next()) {
            (Some(p), Some(s)) => {
                if s != "*" && p != s {
                    return false;
                }
            }
            (None, None) => return true,
            // Different lengths — no match.
            _ => return false,
        }
    }
}
