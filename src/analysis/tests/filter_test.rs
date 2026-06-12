use crate::analysis::filter::DifferencesFilter;
use crate::analysis::joined::JoinedField;

fn field(raw: u64, noise: u64, total: u64) -> JoinedField {
    JoinedField {
        path: "user.name".to_owned(),
        raw_count: raw,
        noise_count: noise,
        endpoint_total: total,
    }
}

fn default_filter() -> DifferencesFilter {
    // Plan defaults: relative 20%, absolute 0.03%.
    DifferencesFilter::new(20.0, 0.03)
}

#[test]
fn clear_regression_passes() {
    // relative = 40/60*100 = 66.7 > 20; absolute = 40/100*100 = 40 > 0.03
    assert!(default_filter().is_regression(&field(50, 10, 100)));
}

#[test]
fn equal_raw_and_noise_is_not_regression() {
    assert!(!default_filter().is_regression(&field(10, 10, 100)));
}

#[test]
fn noise_above_raw_is_not_regression() {
    assert!(!default_filter().is_regression(&field(10, 50, 100)));
}

#[test]
fn below_relative_threshold_is_not_regression() {
    // relative = 1/9*100 = 11.1 < 20
    assert!(!default_filter().is_regression(&field(5, 4, 100)));
}

#[test]
fn below_absolute_threshold_is_not_regression() {
    // relative = 1/3*100 = 33.3 > 20; absolute = 1/100000*100 = 0.001 < 0.03
    assert!(!default_filter().is_regression(&field(2, 1, 100_000)));
}

#[test]
fn zero_noise_with_enough_raw_is_regression() {
    // relative = 100 > 20; absolute = 5/100*100 = 5 > 0.03
    assert!(default_filter().is_regression(&field(5, 0, 100)));
}
