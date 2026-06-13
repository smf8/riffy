//! HTTP input layer: everything that accepts requests from the outside —
//! the proxy router/handler and the admin endpoints (health, metrics).
//! Actual upstream proxying lives in `crate::proxy`.

pub mod proxy;
pub mod router;
