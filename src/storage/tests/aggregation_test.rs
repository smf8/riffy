use crate::storage::FieldAggregation;

fn field(raw: u64, noise: u64) -> FieldAggregation {
    FieldAggregation {
        raw_count: raw,
        noise_count: noise,
    }
}

#[test]
fn relative_difference_basic() {
    // |50 - 10| / 60 * 100 = 66.67
    assert!((field(50, 10).relative_difference() - 66.666_666).abs() < 0.001);
}

#[test]
fn relative_difference_equal_counts_is_zero() {
    assert_eq!(field(10, 10).relative_difference(), 0.0);
}

#[test]
fn relative_difference_zero_counts_is_zero() {
    assert_eq!(field(0, 0).relative_difference(), 0.0);
}

#[test]
fn absolute_difference_basic() {
    // |50 - 10| / 100 * 100 = 40
    assert!((field(50, 10).absolute_difference(100) - 40.0).abs() < f64::EPSILON);
}

#[test]
fn absolute_difference_zero_total_is_zero() {
    assert_eq!(field(5, 1).absolute_difference(0), 0.0);
}

#[test]
fn noise_higher_than_raw_still_positive() {
    // Differences are absolute values regardless of direction.
    let f = field(10, 50);
    assert!(f.relative_difference() > 0.0);
    assert!(f.absolute_difference(100) > 0.0);
}
