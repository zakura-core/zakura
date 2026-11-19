//! Receipt priority for concurrent deliveries of complete blocks.
//!
//! A block's receipt order is normally drawn when its complete body reaches the verifier. A block
//! announced first, by native header sync admitting its header or by a legacy peer's first `inv`
//! for it, can instead keep the order reserved for that announcement, provided its body follows
//! within a deadline that is the same for every block size. That stops a small body from
//! overtaking a larger one announced earlier.
//!
//! Priority belongs to the block, not to a delivery attempt. [`AnnouncementClaims`] holds at most
//! one claim per block hash, never replaces or renews it, and does not consume it when a body
//! reads it. A block announced and withheld therefore cannot regain its place by being announced
//! again.
//!
//! - `claims`: the claim table header sync and legacy `inv`s write, the body deadline, and the
//!   blocks this node mined, which outrank rivals' claims at their heights.
//! - `registry`: the receipts the verifier gives bodies, shared by overlapping deliveries and
//!   resolved against the claims.
//! - `order`: the process-wide order both draw from, so claims and receipts compare.

mod claims;
mod order;
mod registry;

#[cfg(test)]
mod tests;

pub use claims::{AnnouncementClaims, PendingClaims};
pub(in crate::block) use registry::ReceiptRegistry;
