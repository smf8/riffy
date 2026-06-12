use crate::compare::Difference;
use serde_json::Value;
use std::collections::HashMap;

/// Recursively compare two `serde_json::Value`s and return a structured diff.
pub fn diff(left: &Value, right: &Value) -> Difference {
    if left == right {
        return Difference::NoDifference(left.clone());
    }

    match (left, right) {
        (Value::Bool(_), Value::Bool(_))
        | (Value::String(_), Value::String(_))
        | (Value::Number(_), Value::Number(_)) => Difference::PrimitiveDifference {
            left: left.clone(),
            right: right.clone(),
        },
        (Value::Object(left_map), Value::Object(right_map)) => diff_objects(left_map, right_map),
        (Value::Array(left_arr), Value::Array(right_arr)) => diff_arrays(left_arr, right_arr),
        _ => Difference::TypeDifference {
            left_value: left.clone(),
            right_value: right.clone(),
        },
    }
}

fn diff_objects(
    left_map: &serde_json::Map<String, Value>,
    right_map: &serde_json::Map<String, Value>,
) -> Difference {
    let mut fields: HashMap<String, Difference> = HashMap::new();

    for (key, left_val) in left_map {
        match right_map.get(key) {
            Some(right_val) => match diff(left_val, right_val) {
                Difference::NoDifference(_) => {}
                child => {
                    fields.insert(key.clone(), child);
                }
            },
            None => {
                fields.insert(
                    key.clone(),
                    Difference::MissingField {
                        value: left_val.clone(),
                    },
                );
            }
        }
    }

    for (key, right_val) in right_map {
        if !left_map.contains_key(key) {
            fields.insert(
                key.clone(),
                Difference::ExtraField {
                    value: right_val.clone(),
                },
            );
        }
    }

    Difference::ObjectDifference { fields }
}

fn diff_arrays(left_arr: &[Value], right_arr: &[Value]) -> Difference {
    if left_arr.len() != right_arr.len() {
        return Difference::SeqSizeDifference {
            left_not_right: set_difference(left_arr, right_arr),
            right_not_left: set_difference(right_arr, left_arr),
        };
    }

    // Same size, same elements (as multisets), different positions → pure reordering.
    if set_difference(left_arr, right_arr).is_empty() {
        return Difference::OrderingDifference;
    }

    let elements = left_arr
        .iter()
        .zip(right_arr)
        .enumerate()
        .filter_map(|(i, (l, r))| match diff(l, r) {
            Difference::NoDifference(_) => None,
            d => Some((i, d)),
        })
        .collect();

    Difference::IndexedDifference { elements }
}

/// Returns elements from `a` that are not present in `b` (by value equality).
/// Preserves order and duplicates from `a` that exceed the count in `b`.
fn set_difference(a: &[Value], b: &[Value]) -> Vec<Value> {
    let mut b_counts: HashMap<String, usize> = HashMap::new();
    for val in b {
        *b_counts.entry(value_to_key(val)).or_insert(0) += 1;
    }

    a.iter()
        .filter(|val| match b_counts.get_mut(&value_to_key(val)) {
            // Consume one match from b; keep the value only once b is exhausted.
            Some(count) if *count > 0 => {
                *count -= 1;
                false
            }
            _ => true,
        })
        .cloned()
        .collect()
}

/// Convert a `Value` to a canonical string key for equality comparison.
/// Uses JSON serialization so that compound values are compared structurally.
fn value_to_key(val: &Value) -> String {
    serde_json::to_string(val).unwrap_or_else(|_| val.to_string())
}
