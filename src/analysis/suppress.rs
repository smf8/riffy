use std::collections::HashMap;

use crate::config::EndpointConfig;
use regex::Regex;

/// Per-endpoint ignore lists. Each pattern selects one of three match modes:
///
/// - **Subtree** (no `*`, no `re:`): the path and all descendants.
///   `"meta"` matches `"meta"`, `"meta.ts"`, `"meta.nested.v"`, …
/// - **Glob** (contains `*`): `*` matches exactly one dot-separated segment;
///   path length must equal pattern length. `"meta.*"` matches `"meta.ts"` but
///   not `"meta.nested.v"`.
/// - **Regex** (`re:` prefix): the regex is matched against each dot-path; a
///   field is ignored if the regex matches it **or any of its ancestors**, so
///   children of a matched field are ignored too.
///
/// The whole set is swapped atomically behind an `ArcSwap` in the `DiffEngine`,
/// so it is read on every diff and replaced wholesale on a runtime edit. The raw
/// pattern strings are retained so the query API can echo them back.
#[derive(Debug, Clone, Default)]
pub struct SuppressRules {
    per_endpoint: HashMap<String, Vec<Rule>>,
}

#[derive(Debug, Clone)]
struct Rule {
    raw: String,
    matcher: Matcher,
}

#[derive(Debug, Clone)]
enum Matcher {
    Subtree,
    Glob,
    Regex(Regex),
}

const REGEX_PREFIX: &str = "re:";

impl SuppressRules {
    /// Build from config, validating any `re:` regex patterns.
    pub fn from_config(endpoints: &[EndpointConfig]) -> Result<Self, regex::Error> {
        let mut per_endpoint = HashMap::new();
        for endpoint in endpoints {
            if endpoint.suppress_paths.is_empty() {
                continue;
            }
            per_endpoint.insert(
                endpoint.pattern.clone(),
                compile_rules(&endpoint.suppress_paths)?,
            );
        }
        Ok(Self { per_endpoint })
    }

    /// Compile a single endpoint's patterns — used by config validation and to
    /// build ad-hoc rule sets (the preview `exclude` list).
    pub fn compile(patterns: &[String]) -> Result<Vec<String>, regex::Error> {
        compile_rules(patterns)?;
        Ok(patterns.to_vec())
    }

    /// An ad-hoc single-endpoint rule set (e.g. the read-time `exclude` preview).
    pub fn for_endpoint(endpoint: &str, patterns: &[String]) -> Result<Self, regex::Error> {
        let mut per_endpoint = HashMap::new();
        if !patterns.is_empty() {
            per_endpoint.insert(endpoint.to_owned(), compile_rules(patterns)?);
        }
        Ok(Self { per_endpoint })
    }

    pub fn rules(&self) -> HashMap<String, Vec<String>> {
        self.per_endpoint
            .iter()
            .map(|(ep, rules)| (ep.clone(), rules.iter().map(|r| r.raw.clone()).collect()))
            .collect()
    }

    pub fn paths_for(&self, endpoint: &str) -> Vec<String> {
        self.per_endpoint
            .get(endpoint)
            .map(|rules| rules.iter().map(|r| r.raw.clone()).collect())
            .unwrap_or_default()
    }

    pub fn is_suppressed(&self, endpoint: &str, path: &str) -> bool {
        self.per_endpoint
            .get(endpoint)
            .is_some_and(|rules| rules.iter().any(|r| r.matches(path)))
    }

    /// Replace one endpoint's rules, returning the new set. An empty list clears
    /// the endpoint. Invalid regex patterns are rejected.
    pub fn with_endpoint(
        &self,
        endpoint: &str,
        patterns: Vec<String>,
    ) -> Result<Self, regex::Error> {
        let mut per_endpoint = self.per_endpoint.clone();
        if patterns.is_empty() {
            per_endpoint.remove(endpoint);
        } else {
            per_endpoint.insert(endpoint.to_owned(), compile_rules(&patterns)?);
        }
        Ok(Self { per_endpoint })
    }
}

fn compile_rules(patterns: &[String]) -> Result<Vec<Rule>, regex::Error> {
    patterns
        .iter()
        .map(|raw| {
            let matcher = if let Some(expr) = raw.strip_prefix(REGEX_PREFIX) {
                Matcher::Regex(Regex::new(expr)?)
            } else if raw.contains('*') {
                Matcher::Glob
            } else {
                Matcher::Subtree
            };
            Ok(Rule {
                raw: raw.clone(),
                matcher,
            })
        })
        .collect()
}

impl Rule {
    fn matches(&self, path: &str) -> bool {
        match &self.matcher {
            // Subtree: exact match or any descendant.
            Matcher::Subtree => path == self.raw || path.starts_with(&format!("{}.", self.raw)),
            Matcher::Glob => glob_match(path, &self.raw),
            // Regex matches the path or any ancestor prefix, so children of a
            // matched field are ignored too.
            Matcher::Regex(re) => ancestor_prefixes(path).any(|prefix| re.is_match(prefix)),
        }
    }
}

/// Yields `a`, `a.b`, `a.b.c`, … for `"a.b.c"` — the path and each ancestor
/// prefix at dot boundaries (so a regex never matches a partial segment).
fn ancestor_prefixes(path: &str) -> impl Iterator<Item = &str> {
    path.match_indices('.')
        .map(move |(i, _)| &path[..i])
        .chain(std::iter::once(path))
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
