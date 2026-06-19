pub mod client;
pub mod error;
pub mod metrics;

pub use client::UpstreamClient;

#[cfg(test)]
mod tests;
