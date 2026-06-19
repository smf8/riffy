#[cfg(test)]
mod tests;

use matchit::Router;

fn split_segments(path: &str) -> impl Iterator<Item = &str> {
    path.split('/').filter(|s| !s.is_empty())
}

// matchit is slash-strict, so both registration and lookup go through canonical normalization.
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
            // Store the original config string so resolved endpoints match the keys
            // used by classifiers and suppress lists. Conflicting templates are skipped
            // rather than aborting startup.
            if let Err(e) = router.insert(canonical(pattern), pattern.clone()) {
                tracing::warn!(pattern = %pattern, error = %e, "ignoring endpoint pattern");
            }
        }
        Self { router }
    }

    pub fn resolve(&self, path: &str) -> Option<String> {
        self.router
            .at(&canonical(path))
            .ok()
            .map(|matched| (*matched.value).clone())
    }
}
