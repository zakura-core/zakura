//! Holds components of a Zebra node.

pub(crate) mod auth_download_height;
mod download_work;
pub mod health;
pub mod inbound;
#[allow(missing_docs)]
pub mod mempool;
pub mod metrics;
mod missing_parent;
#[allow(missing_docs)]
pub mod sync;
#[allow(missing_docs)]
pub mod tokio;
#[allow(missing_docs)]
pub mod tracing;
pub mod zcashd_compat;

#[cfg(feature = "internal-miner")]
pub mod miner;

pub use inbound::Inbound;
pub use sync::ChainSync;

/// Consumes, updates, and returns `Self`.
pub trait With<T> {
    /// Consumes `self`, updates it, and returns the updated version.
    fn with(self, _: T) -> Self;
}
