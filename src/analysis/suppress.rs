use std::collections::HashMap;

use crate::config::EndpointConfig;

/// Per-endpoint suppress lists with two match modes selected by whether the pattern contains `*`:
///
/// - **Subtree** (no `*`): suppresses the path and all descendants.
///   `"meta"` suppresses `"meta"`, `"meta.ts"`, `"meta.nested.v"`, etc.
///
/// - **Glob** (contains `*`): `*` matches exactly one dot-separated segment;
///   path length must equal pattern length.
///   `"meta.*"` matches `"meta.ts"` but not `"meta.nested.v"`.
///
/// The whole set is swapped atomically behind an `ArcSwap` in the `DiffEngine`,
/// so it is read on every diff and replaced wholesale on a runtime edit.
#[derive(Debug, Clone, Default)]
pub struct SuppressRules {
    per_endpoint: HashMap<String, Vec<String>>,
}

impl SuppressRules {
    pub fn from_config(endpoints: &[EndpointConfig]) -> Self {
        let per_endpoint = endpoints
            .iter()
            .filter(|e| !e.suppress_paths.is_empty())
            .map(|e| (e.pattern.clone(), e.suppress_paths.clone()))
            .collect();
        Self { per_endpoint }
    }

    pub fn rules(&self) -> &HashMap<String, Vec<String>> {
        &self.per_endpoint
    }

    pub fn paths_for(&self, endpoint: &str) -> &[String] {
        self.per_endpoint
            .get(endpoint)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn is_suppressed(&self, endpoint: &str, path: &str) -> bool {
        self.paths_for(endpoint)
            .iter()
            .any(|s| matches_pattern(path, s))
    }

    /// Replace one endpoint's rules, returning the new set. An empty list clears
    /// the endpoint. Cloning the map is cheap relative to how rarely rules change.
    pub fn with_endpoint(&self, endpoint: &str, paths: Vec<String>) -> Self {
        let mut per_endpoint = self.per_endpoint.clone();
        if paths.is_empty() {
            per_endpoint.remove(endpoint);
        } else {
            per_endpoint.insert(endpoint.to_owned(), paths);
        }
        Self { per_endpoint }
    }
}

fn matches_pattern(path: &str, pattern: &str) -> bool {
    if pattern.contains('*') {
        glob_match(path, pattern)
    } else {
        // Subtree: exact match or any descendant.
        path == pattern || path.starts_with(&format!("{pattern}."))
    }
}

// `*` matches exactly one dot-separated segment; path and pattern must have the same segment count.
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
            _ => return false,
        }
    }
}
