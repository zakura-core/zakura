//! Tests for receipt order and announcement claims, with their shared fixtures.

mod announced;
mod claims;
mod counter;
mod registry;

use std::{sync::Arc, time::Duration};

use tokio::time::Instant;
use zakura_chain::{
    block::{self, Block},
    serialization::ZcashDeserializeInto,
};
use zakura_node_services::block_memory::BlockHandle;

use super::{
    claims::{
        size_label, AnnouncementClaims, ClaimOutcome, ClaimSource, CLAIM_BODY_DEADLINE,
        MAX_ANNOUNCED_BLOCKS, MAX_ANNOUNCEMENT_CLAIMS,
    },
    order,
    registry::{ReceiptGuard, ReceiptRegistry, MAX_BODIES_PER_HASH},
};

/// Registers `block` the way the verifier does, under its hash, with no claim held.
fn register(registry: &ReceiptRegistry, block: Arc<Block>) -> ReceiptGuard {
    let block = BlockHandle::from(block);
    registry.register(block.hash(), &block)
}

fn hash(index: usize) -> block::Hash {
    let mut bytes = [0; 32];
    bytes[..8].copy_from_slice(&index.to_le_bytes());
    block::Hash(bytes)
}

/// The height every test claim is made at, one above [`TIP`].
const CLAIMED: block::Height = block::Height(101);
const TIP: block::Height = block::Height(100);

/// Puts a claim for `hash` in force at [`CLAIMED`], as header sync does after a header apply.
fn claim(claims: &AnnouncementClaims, hash: block::Hash, now: Instant) {
    claims.begin_at([(hash, CLAIMED)], TIP, now).commit();
}

/// Records a legacy `inv` for `hash` at `now`, with the verified tip at [`TIP`].
fn announce(claims: &AnnouncementClaims, hash: block::Hash, now: Instant) {
    claims.note_announcement_at(hash, TIP, now);
}

/// The order a body of `hash` gets when it arrives at `now`.
fn body_order(claims: &AnnouncementClaims, hash: block::Hash, now: Instant) -> u64 {
    claims
        .resolve(hash, Some(CLAIMED), now)
        .claimed_order()
        .unwrap_or_else(order::next)
}
