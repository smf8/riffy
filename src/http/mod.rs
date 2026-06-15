//! HTTP input layer: everything that accepts requests from the outside —
//! the forwarding handler, the admin endpoints (health, metrics), and the
//! diff query API. The upstream client that does the actual proxying lives in
//! `crate::upstream`.

pub mod forward;
pub mod query;
pub mod router;
pub mod ui;
