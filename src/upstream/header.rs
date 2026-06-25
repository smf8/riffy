use axum::http::HeaderMap;
use serde_json::{Map, Value};

// RFC 2616 §13.5.1 hop-by-hop headers.
pub const HOP_BY_HOP_HEADERS: &[&str] = &[
    "connection",
    "keep-alive",
    "transfer-encoding",
    "te",
    "upgrade",
];

// Volatile or sensitive; dropped at write time so a session token never reaches storage.
const IGNORED_COMPARE_HEADERS: &[&str] =
    &["date", "content-length", "content-encoding", "set-cookie"];

pub fn is_hop_by_hop(name: &str) -> bool {
    HOP_BY_HOP_HEADERS.contains(&name)
}

fn is_ignored_for_compare(name: &str) -> bool {
    is_hop_by_hop(name) || IGNORED_COMPARE_HEADERS.contains(&name)
}

pub fn headers_to_json(headers: &HeaderMap) -> Value {
    let mut map: Map<String, Value> = Map::new();
    for name in headers.keys() {
        let key = name.as_str();
        if is_ignored_for_compare(key) {
            continue;
        }
        let mut values = headers
            .get_all(name)
            .iter()
            .map(|v| Value::String(String::from_utf8_lossy(v.as_bytes()).into_owned()));
        let Some(first) = values.next() else {
            continue;
        };
        let value = match values.next() {
            None => first,
            Some(second) => {
                let mut arr = vec![first, second];
                arr.extend(values);
                Value::Array(arr)
            }
        };
        map.insert(key.to_owned(), value);
    }
    Value::Object(map)
}
