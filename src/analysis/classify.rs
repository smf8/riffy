use std::collections::HashMap;

use crate::config::{EndpointConfig, Threshold};
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

/// Per-endpoint classifiers: each configured endpoint can carry its own
/// thresholds, so the classifier used at read time is looked up by endpoint.
/// Endpoints with no configured entry (e.g. unmatched raw paths) fall back to
/// the diffy defaults.
#[derive(Debug, Clone)]
pub struct EndpointClassifiers {
    per_endpoint: HashMap<String, RegressionClassifier>,
    default: RegressionClassifier,
}

impl EndpointClassifiers {
    pub fn from_config(endpoints: &[EndpointConfig]) -> Self {
        let per_endpoint = endpoints
            .iter()
            .map(|e| {
                (
                    e.pattern.clone(),
                    RegressionClassifier::from_config(&e.threshold),
                )
            })
            .collect();
        Self {
            per_endpoint,
            default: RegressionClassifier::from_config(&Threshold::default()),
        }
    }

    /// The classifier configured for `endpoint`, or the diffy-default one when
    /// the endpoint has no dedicated thresholds.
    pub fn for_endpoint(&self, endpoint: &str) -> &RegressionClassifier {
        self.per_endpoint.get(endpoint).unwrap_or(&self.default)
    }
}
