use std::collections::HashMap;

use crate::config::EndpointConfig;
use regex::Regex;

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

    pub fn compile(patterns: &[String]) -> Result<Vec<String>, regex::Error> {
        compile_rules(patterns)?;
        Ok(patterns.to_vec())
    }

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

// Each ancestor prefix at dot boundaries, so a regex can't match a partial segment.
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
