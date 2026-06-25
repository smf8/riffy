use std::collections::HashMap;

use crate::config::{EndpointConfig, Threshold};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct FieldCounts {
    pub raw_count: u64,
    pub noise_count: u64,
}

impl FieldCounts {
    pub fn relative_difference(&self) -> f64 {
        let raw = self.raw_count as f64;
        let noise = self.noise_count as f64;
        let denominator = raw + noise;
        if denominator == 0.0 {
            return 0.0;
        }
        (raw - noise).abs() / denominator * 100.0
    }

    pub fn absolute_difference(&self, endpoint_total: u64) -> f64 {
        if endpoint_total == 0 {
            return 0.0;
        }
        (self.raw_count as f64 - self.noise_count as f64).abs() / endpoint_total as f64 * 100.0
    }
}

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

    pub fn is_regression(&self, field: &FieldCounts, endpoint_total: u64) -> bool {
        field.raw_count > field.noise_count
            && field.relative_difference() > self.relative_threshold
            && field.absolute_difference(endpoint_total) > self.absolute_threshold
    }
}

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

    pub fn for_endpoint(&self, endpoint: &str) -> &RegressionClassifier {
        self.per_endpoint.get(endpoint).unwrap_or(&self.default)
    }
}
