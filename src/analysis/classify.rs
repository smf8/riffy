use super::snapshot::FieldSnapshot;
use crate::config::Threshold;

/// Threshold predicate classifying a joined field as a real regression
/// (diffy's noise filter): the raw counter must exceed the noise counter both
/// relatively and in absolute terms.
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

    pub fn is_regression(&self, field: &FieldSnapshot) -> bool {
        field.raw_count > field.noise_count
            && field.relative_difference() > self.relative_threshold
            && field.absolute_difference() > self.absolute_threshold
    }
}
