//! Receipt priority for concurrent deliveries of complete blocks.
//!
//! A complete body that reaches the verifier normally draws a new order. A native header,
//! a single-block legacy inventory announcement, or the block header at the start of a legacy
//! block transfer can reserve an earlier place. The verifier picks the earliest eligible reservation still within
//! its deadline. Reading a claim leaves it available to other bodies under the same hash.
//!
//! - `claims`: header, inventory and transfer reservations, their deadline, and locally mined
//!   blocks that outrank earlier claims at their height.
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
