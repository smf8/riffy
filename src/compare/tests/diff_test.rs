use crate::compare::{diff, Difference};
use serde_json::json;

#[test]
fn equal_null() {
    let result = diff(&json!(null), &json!(null));
    assert!(matches!(result, Difference::NoDifference(_)));
}

#[test]
fn equal_bool() {
    let result = diff(&json!(true), &json!(true));
    assert!(matches!(result, Difference::NoDifference(_)));
}

#[test]
fn equal_number() {
    let result = diff(&json!(42), &json!(42));
    assert!(matches!(result, Difference::NoDifference(_)));
}

#[test]
fn equal_string() {
    let result = diff(&json!("hello"), &json!("hello"));
    assert!(matches!(result, Difference::NoDifference(_)));
}

#[test]
fn equal_object() {
    let left = json!({"a": 1, "b": "x"});
    let right = json!({"a": 1, "b": "x"});
    let result = diff(&left, &right);
    assert!(matches!(result, Difference::NoDifference(_)));
}

#[test]
fn equal_array() {
    let left = json!([1, 2, 3]);
    let right = json!([1, 2, 3]);
    let result = diff(&left, &right);
    assert!(matches!(result, Difference::NoDifference(_)));
}

#[test]
fn primitive_bool_diff() {
    let result = diff(&json!(true), &json!(false));
    assert!(matches!(result, Difference::PrimitiveDifference { .. }));
}

#[test]
fn primitive_number_diff() {
    let result = diff(&json!(1), &json!(2));
    assert!(matches!(result, Difference::PrimitiveDifference { .. }));
}

#[test]
fn primitive_string_diff() {
    let result = diff(&json!("a"), &json!("b"));
    assert!(matches!(result, Difference::PrimitiveDifference { .. }));
}

#[test]
fn null_vs_non_null() {
    let result = diff(&json!(null), &json!(42));
    assert!(matches!(result, Difference::TypeDifference { .. }));
}

#[test]
fn type_mismatch_object_vs_array() {
    let left = json!({"a": 1});
    let right = json!([1]);
    let result = diff(&left, &right);
    assert!(matches!(result, Difference::TypeDifference { .. }));
}

#[test]
fn type_mismatch_string_vs_number() {
    let result = diff(&json!("1"), &json!(1));
    assert!(matches!(result, Difference::TypeDifference { .. }));
}

#[test]
fn object_shared_keys_diff_values() {
    let left = json!({"a": 1});
    let right = json!({"a": 2});
    let result = diff(&left, &right);
    match result {
        Difference::ObjectDifference { fields } => {
            assert!(matches!(
                fields.get("a"),
                Some(Difference::PrimitiveDifference { .. })
            ));
        }
        other => panic!("expected ObjectDifference, got {:?}", other),
    }
}

#[test]
fn object_missing_key() {
    let left = json!({"a": 1, "b": 2});
    let right = json!({"a": 1});
    let result = diff(&left, &right);
    match result {
        Difference::ObjectDifference { fields } => {
            assert!(matches!(
                fields.get("b"),
                Some(Difference::MissingField { .. })
            ));
        }
        other => panic!("expected ObjectDifference, got {:?}", other),
    }
}

#[test]
fn object_extra_key() {
    let left = json!({"a": 1});
    let right = json!({"a": 1, "b": 2});
    let result = diff(&left, &right);
    match result {
        Difference::ObjectDifference { fields } => {
            assert!(matches!(
                fields.get("b"),
                Some(Difference::ExtraField { .. })
            ));
        }
        other => panic!("expected ObjectDifference, got {:?}", other),
    }
}

#[test]
fn nested_object_diff() {
    let left = json!({"user": {"name": "alice", "age": 30}});
    let right = json!({"user": {"name": "bob", "age": 30}});
    let result = diff(&left, &right);
    match result {
        Difference::ObjectDifference { fields } => match fields.get("user") {
            Some(Difference::ObjectDifference { fields: inner }) => {
                assert!(matches!(
                    inner.get("name"),
                    Some(Difference::PrimitiveDifference { .. })
                ));
                assert!(!inner.contains_key("age"));
            }
            other => panic!("expected nested ObjectDifference, got {:?}", other),
        },
        other => panic!("expected ObjectDifference, got {:?}", other),
    }
}

#[test]
fn array_same_size_diff_values() {
    let left = json!([1, 2, 3]);
    let right = json!([1, 99, 3]);
    let result = diff(&left, &right);
    match result {
        Difference::IndexedDifference { elements } => {
            assert_eq!(elements.len(), 1);
            assert_eq!(elements[0].0, 1);
            assert!(matches!(
                elements[0].1,
                Difference::PrimitiveDifference { .. }
            ));
        }
        other => panic!("expected IndexedDifference, got {:?}", other),
    }
}

#[test]
fn array_different_size() {
    let left = json!([1, 2, 3]);
    let right = json!([1, 2, 3, 5]);
    let result = diff(&left, &right);
    match result {
        Difference::SeqSizeDifference {
            left_not_right,
            right_not_left,
        } => {
            assert!(left_not_right.is_empty());
            assert_eq!(right_not_left, vec![json!(5)]);
        }
        other => panic!("expected SeqSizeDifference, got {:?}", other),
    }
}

#[test]
fn array_different_size_with_duplicates() {
    let left = json!([1, 1, 2]);
    let right = json!([1, 2]);
    let result = diff(&left, &right);
    match result {
        Difference::SeqSizeDifference {
            left_not_right,
            right_not_left,
        } => {
            assert_eq!(left_not_right, vec![json!(1)]);
            assert!(right_not_left.is_empty());
        }
        other => panic!("expected SeqSizeDifference, got {:?}", other),
    }
}

#[test]
fn array_same_elements_different_order() {
    let left = json!([1, 2, 3]);
    let right = json!([3, 2, 1]);
    let result = diff(&left, &right);
    assert!(matches!(result, Difference::OrderingDifference));
}

#[test]
fn empty_objects() {
    let left = json!({});
    let right = json!({});
    let result = diff(&left, &right);
    assert!(matches!(result, Difference::NoDifference(_)));
}
