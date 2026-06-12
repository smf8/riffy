use crate::analysis::joined::JoinedField;

fn field(raw: u64, noise: u64, total: u64) -> JoinedField {
    JoinedField {
        path: "user.name".to_owned(),
        raw_count: raw,
        noise_count: noise,
        endpoint_total: total,
    }
}

#[test]
fn relative_difference_basic() {
    // |50 - 10| / 60 * 100 = 66.67
    let f = field(50, 10, 100);
    assert!((f.relative_difference() - 66.666_666).abs() < 0.001);
}

#[test]
fn relative_difference_equal_counts_is_zero() {
    let f = field(10, 10, 100);
    assert_eq!(f.relative_difference(), 0.0);
}

#[test]
fn relative_difference_zero_counts_is_zero() {
    let f = field(0, 0, 100);
    assert_eq!(f.relative_difference(), 0.0);
}

#[test]
fn absolute_difference_basic() {
    // |50 - 10| / 100 * 100 = 40
    let f = field(50, 10, 100);
    assert!((f.absolute_difference() - 40.0).abs() < f64::EPSILON);
}

#[test]
fn absolute_difference_zero_total_is_zero() {
    let f = field(5, 1, 0);
    assert_eq!(f.absolute_difference(), 0.0);
}

#[test]
fn noise_higher_than_raw_still_positive() {
    // Differences are absolute values regardless of direction.
    let f = field(10, 50, 100);
    assert!(f.relative_difference() > 0.0);
    assert!(f.absolute_difference() > 0.0);
}
