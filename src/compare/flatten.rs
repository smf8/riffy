use std::collections::HashMap;

use super::Difference;
use crate::compare::diff::diff;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffType {
    Primitive,
    MissingField,
    ExtraField,
    SeqSize,
    Ordering,
    TypeMismatch,
    StatusMismatch,
}

// A leading ':' can't occur in a flattened JSON dot-path, so it's collision-free.
pub const STATUS_FIELD: &str = ":status";

// ':headers.<name>' namespaces header diffs apart from body paths; a ':headers'
// subtree suppress rule then hides every header at once.
pub const HEADER_FIELD_PREFIX: &str = ":headers";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDiff {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub left: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub right: Option<Value>,
    pub diff_type: DiffType,
}

pub fn flatten(diff: &Difference, prefix: &str) -> HashMap<String, FieldDiff> {
    let mut out = HashMap::new();
    flatten_into(diff, prefix, &mut out);
    out
}

fn join_path(prefix: &str, segment: &str) -> String {
    if prefix.is_empty() {
        segment.to_owned()
    } else {
        format!("{prefix}.{segment}")
    }
}

fn flatten_into(diff: &Difference, path: &str, out: &mut HashMap<String, FieldDiff>) {
    match diff {
        Difference::NoDifference(_) => {}

        Difference::PrimitiveDifference { left, right } => {
            out.insert(
                path.to_owned(),
                FieldDiff {
                    left: Some(left.clone()),
                    right: Some(right.clone()),
                    diff_type: DiffType::Primitive,
                },
            );
        }

        Difference::ObjectDifference { fields } => {
            for (key, child) in fields {
                flatten_into(child, &join_path(path, key), out);
            }
        }

        Difference::MissingField { value } => {
            out.insert(
                path.to_owned(),
                FieldDiff {
                    left: Some(value.clone()),
                    right: None,
                    diff_type: DiffType::MissingField,
                },
            );
        }

        Difference::ExtraField { value } => {
            out.insert(
                path.to_owned(),
                FieldDiff {
                    left: None,
                    right: Some(value.clone()),
                    diff_type: DiffType::ExtraField,
                },
            );
        }

        Difference::IndexedDifference { elements } => {
            for (idx, child) in elements {
                flatten_into(child, &join_path(path, &idx.to_string()), out);
            }
        }

        Difference::SeqSizeDifference {
            left_not_right,
            right_not_left,
        } => {
            out.insert(
                path.to_owned(),
                FieldDiff {
                    left: Some(Value::Array(left_not_right.clone())),
                    right: Some(Value::Array(right_not_left.clone())),
                    diff_type: DiffType::SeqSize,
                },
            );
        }

        Difference::OrderingDifference => {
            out.insert(
                path.to_owned(),
                FieldDiff {
                    left: None,
                    right: None,
                    diff_type: DiffType::Ordering,
                },
            );
        }

        Difference::TypeDifference {
            left_value,
            right_value,
            ..
        } => {
            out.insert(
                path.to_owned(),
                FieldDiff {
                    left: Some(left_value.clone()),
                    right: Some(right_value.clone()),
                    diff_type: DiffType::TypeMismatch,
                },
            );
        }
    }
}

pub fn flatten_value(left: &Value, right: &Value) -> HashMap<String, FieldDiff> {
    let difference = diff(left, right);
    flatten(&difference, "")
}
