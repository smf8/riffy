/// Per-field join of the raw (primary vs candidate) and noise (primary vs
/// secondary) counters for one endpoint.
#[derive(Debug, Clone)]
pub struct JoinedField {
    pub path: String,
    pub raw_count: u64,
    pub noise_count: u64,
    /// Total requests analyzed for the owning endpoint.
    pub endpoint_total: u64,
}

impl JoinedField {
    /// `|raw − noise| / (raw + noise) × 100`. Zero when both counters are zero.
    pub fn relative_difference(&self) -> f64 {
        let raw = self.raw_count as f64;
        let noise = self.noise_count as f64;
        let denominator = raw + noise;
        if denominator == 0.0 {
            return 0.0;
        }
        (raw - noise).abs() / denominator * 100.0
    }

    /// `|raw − noise| / endpoint_total × 100`. Zero when no requests recorded.
    pub fn absolute_difference(&self) -> f64 {
        if self.endpoint_total == 0 {
            return 0.0;
        }
        (self.raw_count as f64 - self.noise_count as f64).abs() / self.endpoint_total as f64 * 100.0
    }
}

#[derive(Debug, Clone)]
pub struct JoinedEndpoint {
    pub endpoint: String,
    pub total: u64,
    pub fields: Vec<JoinedField>,
}
