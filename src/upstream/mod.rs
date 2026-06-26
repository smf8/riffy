pub mod body;
pub mod client;
pub mod error;
pub mod header;
pub mod metrics;

#[cfg(test)]
mod tests;

pub use client::UpstreamClient;

pub fn normalize_base(addr: &str) -> String {
    if addr.contains("://") {
        addr.to_owned()
    } else {
        format!("http://{addr}")
    }
}
