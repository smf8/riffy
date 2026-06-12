use crate::compare::flatten::{flatten_value, DiffType};
use serde_json::json;

#[test]
fn flatten_equal_values() {
    let left = json!({"a": 1});
    let right = json!({"a": 1});
    let result = flatten_value(&left, &right);
    assert!(result.is_empty());
}

#[test]
fn flatten_primitive_diff() {
    let left = json!({"a": 1});
    let right = json!({"a": 2});
    let result = flatten_value(&left, &right);
    let entry = result.get("a").unwrap();
    assert_eq!(entry.diff_type, DiffType::Primitive);
    assert_eq!(entry.left, Some(json!(1)));
    assert_eq!(entry.right, Some(json!(2)));
}

#[test]
fn flatten_nested_object() {
    let left = json!({"user": {"name": "alice", "age": 30}});
    let right = json!({"user": {"name": "bob", "age": 30}});
    let result = flatten_value(&left, &right);
    assert_eq!(result.len(), 1);
    let name = result.get("user.name").unwrap();
    assert_eq!(name.diff_type, DiffType::Primitive);
    assert_eq!(name.left, Some(json!("alice")));
    assert_eq!(name.right, Some(json!("bob")));
    assert!(!result.contains_key("user.age"));
}

#[test]
fn flatten_array_diff() {
    let left = json!({"items": [1, 2, 3]});
    let right = json!({"items": [1, 99, 3]});
    let result = flatten_value(&left, &right);
    let entry = result.get("items.1").unwrap();
    assert_eq!(entry.diff_type, DiffType::Primitive);
    assert_eq!(entry.left, Some(json!(2)));
    assert_eq!(entry.right, Some(json!(99)));
}

#[test]
fn flatten_array_ordering() {
    let left = json!({"items": [1, 2, 3]});
    let right = json!({"items": [3, 2, 1]});
    let result = flatten_value(&left, &right);
    let entry = result.get("items").unwrap();
    assert_eq!(entry.diff_type, DiffType::Ordering);
}

#[test]
fn flatten_array_size_diff() {
    let left = json!({"items": [1, 2]});
    let right = json!({"items": [1, 2, 3]});
    let result = flatten_value(&left, &right);
    let entry = result.get("items").unwrap();
    assert_eq!(entry.diff_type, DiffType::SeqSize);
    assert_eq!(entry.left, Some(json!([])));
    assert_eq!(entry.right, Some(json!([3])));
}

#[test]
fn flatten_type_mismatch() {
    let left = json!({"a": 1});
    let right = json!({"a": "1"});
    let result = flatten_value(&left, &right);
    let entry = result.get("a").unwrap();
    assert_eq!(entry.diff_type, DiffType::TypeMismatch);
}

#[test]
fn flatten_top_level_type_mismatch_uses_empty_path() {
    let left = json!({"a": 1});
    let right = json!([1]);
    let result = flatten_value(&left, &right);
    let entry = result.get("").unwrap();
    assert_eq!(entry.diff_type, DiffType::TypeMismatch);
}

#[test]
fn flatten_deeply_nested_array_of_objects() {
    let left = json!({"data": {"users": [{"id": 1}, {"id": 2}]}});
    let right = json!({"data": {"users": [{"id": 1}, {"id": 99}]}});
    let result = flatten_value(&left, &right);
    assert_eq!(result.len(), 1);
    let entry = result.get("data.users.1.id").unwrap();
    assert_eq!(entry.diff_type, DiffType::Primitive);
    assert_eq!(entry.left, Some(json!(2)));
    assert_eq!(entry.right, Some(json!(99)));
}

#[test]
fn flatten_missing_and_extra_fields() {
    let left = json!({"a": 1, "b": 2});
    let right = json!({"a": 1, "c": 3});
    let result = flatten_value(&left, &right);
    assert_eq!(result.get("b").unwrap().diff_type, DiffType::MissingField);
    assert_eq!(result.get("c").unwrap().diff_type, DiffType::ExtraField);
}
