use crate::config::Threshold;
use crate::storage::FieldAggregation;

/// Threshold predicate classifying a field's stored counts as a real
/// regression (diffy's noise filter): the raw counter must exceed the noise
/// counter both relatively and in absolute terms.
#[derive(Debug, Clone, Copy)]
pub struct RegressionClassifier {
    relative_threshold: f64,
    absolute_threshold: f64,
}

impl RegressionClassifier {
    pub fn new(relative_threshold: f64, absolute_threshold: f64) -> Self {
        Self {
            relative_threshold,
            absolute_threshold,
        }
    }

    pub fn from_config(threshold: &Threshold) -> Self {
        Self::new(threshold.relative, threshold.absolute)
    }

    pub fn is_regression(&self, field: &FieldAggregation, endpoint_total: u64) -> bool {
        field.raw_count > field.noise_count
            && field.relative_difference() > self.relative_threshold
            && field.absolute_difference(endpoint_total) > self.absolute_threshold
    }
}
