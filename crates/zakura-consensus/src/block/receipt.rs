//! Receipt priority for concurrent deliveries of complete blocks.
//!
//! A block's receipt order is normally drawn when its complete body reaches the verifier. A block
//! whose header was admitted first can instead keep the order reserved for that header, provided
//! its body follows within its deadline. That stops a small body from overtaking a larger one
//! whose header arrived earlier.
//!
//! Priority belongs to the block, not to a delivery attempt. [`AnnouncementClaims`] holds at most
//! one claim per block hash, never replaces it, and does not consume it when a body reads it. A
//! block announced once and withheld therefore cannot renew its place by being announced again.
//!
//! - `claims`: the claim table header sync writes, the body deadline, and the mined-block
//!   watermark.
//! - `registry`: the receipts the verifier gives bodies, shared by overlapping deliveries and
//!   resolved against the claims.
//! - `order`: the process-wide order both draw from, so claims and receipts compare.

mod claims;
mod order;
mod registry;

#[cfg(test)]
mod tests;

pub use claims::{AnnouncementClaims, PendingClaims};
pub(crate) use registry::ReceiptRegistry;
