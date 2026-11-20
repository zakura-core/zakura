//! Receipt priority for concurrent deliveries of complete blocks.
//!
//! A block's receipt order is normally drawn when its complete body reaches the verifier. A block
//! announced first can instead keep the order reserved for that announcement, provided its body
//! follows within a deadline that is the same for every block size. That stops a small body from
//! overtaking a larger one announced earlier. A block is announced when native header sync
//! admits its header, when a legacy peer sends the first `inv` for it, or when the block header
//! at the start of a legacy `block` message carrying it arrives.
//!
//! A block can hold both a native header claim and one legacy sighting. Repeating either
//! preserves its original order and deadline. Reading a claim does not consume it, so a failed
//! delivery cannot take that order from a later valid body.
//!
//! - `claims`: the claim table header sync and legacy peers write, the body deadline, and the
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
pub use claims::{lab_knob, AnnouncementClaims, PendingClaims};
pub(in crate::block) use registry::ReceiptRegistry;
