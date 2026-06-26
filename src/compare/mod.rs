use serde_json::Value;
use std::collections::HashMap;

mod diff;
pub mod flatten;

#[cfg(test)]
mod tests;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone)]
pub enum Difference {
    // Payload is unread but kept for parity with diffy's ADT.
    NoDifference(Value),

    PrimitiveDifference {
        left: Value,
        right: Value,
    },

    ObjectDifference {
        fields: HashMap<String, Difference>,
    },

    MissingField {
        value: Value,
    },

    ExtraField {
        value: Value,
    },

    IndexedDifference {
        elements: Vec<(usize, Difference)>,
    },

    SeqSizeDifference {
        left_not_right: Vec<Value>,
        right_not_left: Vec<Value>,
    },

    OrderingDifference,

    TypeDifference {
        left_value: Value,
        right_value: Value,
    },
}
