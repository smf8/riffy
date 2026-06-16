//! Endpoint identification via path template matching.
//!
//! Config defines templates like `/api/v1/users/:id`. A request path resolves
//! to the first matching template, or `None` when no template matches —
//! unmatched paths are proxied but excluded from analysis (keeps the store and
//! the metric label cardinality bounded by the configured endpoint set).

#[cfg(test)]
mod tests;

enum Segment {
    Literal(String),
    Param,
}

struct Pattern {
    raw: String,
    segments: Vec<Segment>,
}

impl Pattern {
    fn parse(raw: &str) -> Self {
        let segments = split_segments(raw)
            .map(|s| {
                if s.starts_with(':') {
                    Segment::Param
                } else {
                    Segment::Literal(s.to_owned())
                }
            })
            .collect();

        Self {
            raw: raw.to_owned(),
            segments,
        }
    }

    fn matches(&self, path_segments: &[&str]) -> bool {
        self.segments.len() == path_segments.len()
            && self
                .segments
                .iter()
                .zip(path_segments)
                .all(|(seg, part)| match seg {
                    Segment::Param => true,
                    Segment::Literal(lit) => lit == part,
                })
    }
}

/// Splits a path into its non-empty segments, normalizing duplicate and
/// trailing slashes.
fn split_segments(path: &str) -> impl Iterator<Item = &str> {
    path.split('/').filter(|s| !s.is_empty())
}

pub struct EndpointMatcher {
    patterns: Vec<Pattern>,
}

impl EndpointMatcher {
    pub fn new(patterns: &[String]) -> Self {
        Self {
            patterns: patterns.iter().map(|p| Pattern::parse(p)).collect(),
        }
    }

    /// Resolve a request path to a configured endpoint template, or `None` when
    /// no template matches. The query string is stripped before matching.
    pub fn resolve(&self, path: &str) -> Option<String> {
        let path = path.split('?').next().unwrap_or(path);
        let segments: Vec<&str> = split_segments(path).collect();

        self.patterns
            .iter()
            .find(|p| p.matches(&segments))
            .map(|p| p.raw.clone())
    }
}
