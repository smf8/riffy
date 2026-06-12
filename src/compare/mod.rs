use serde_json::Value;
use std::collections::HashMap;

mod diff;
pub mod error;
pub mod flatten;

#[cfg(test)]
mod tests;

#[allow(unused_imports)]
pub use diff::diff;

/// Recursive diff result mirroring diffy's type-dispatching on `serde_json::Value`.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone)]
pub enum Difference {
    /// Both values are structurally equal. Contains the value for convenience.
    /// The payload is unread today but kept for parity with diffy's ADT.
    #[allow(dead_code)]
    NoDifference(Value),

    /// Both sides are primitive (bool, number, string) but differ.
    PrimitiveDifference { left: Value, right: Value },

    /// Both sides are objects; per-key differences are stored here.
    ObjectDifference { fields: HashMap<String, Difference> },

    /// A key exists on the left but is absent on the right.
    MissingField { value: Value },

    /// A key exists on the right but is absent on the left.
    ExtraField { value: Value },

    /// Both sides are arrays of the same length with differing elements;
    /// element-by-element diff.
    IndexedDifference { elements: Vec<(usize, Difference)> },

    /// Both sides are arrays of different lengths.
    /// `left_not_right` and `right_not_left` are multiset differences.
    SeqSizeDifference {
        left_not_right: Vec<Value>,
        right_not_left: Vec<Value>,
    },

    /// Arrays of the same length containing the same elements (as multisets)
    /// in a different order.
    OrderingDifference,

    /// The two values have incompatible top-level types (e.g. object vs array).
    TypeDifference {
        left_value: Value,
        right_value: Value,
    },
}
