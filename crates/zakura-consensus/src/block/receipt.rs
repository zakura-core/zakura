//! Receipt priority for concurrent deliveries of complete blocks.
//!
//! A block's receipt order is normally drawn when its complete body reaches the verifier. A block
//! announced first, by native header sync admitting its header or by a legacy peer's first `inv`
//! for it, can instead keep the order reserved for that announcement, provided its body follows
//! within a deadline that is the same for every block size. That stops a small body from
//! overtaking a larger one announced earlier.
//!
//! A block can hold both a native header claim and a legacy announcement claim. Repeated
//! announcements preserve the original order of each held claim. Reading a claim does not
//! consume it, so a failed delivery cannot take that order from a later valid body.
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

pub(in crate::block) use claims::CLAIM_BODY_DEADLINE;
pub use claims::{AnnouncementClaims, PendingClaims};
pub(in crate::block) use registry::ReceiptRegistry;
