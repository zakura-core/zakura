//! Tests for receipt order and announcement claims, with their shared fixtures.

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
        body_deadline, size_label, AnnouncementClaims, ClaimOutcome, CLAIM_BASE_GRACE,
        MAX_ANNOUNCEMENT_CLAIMS, MAX_CLAIM_BODY_DEADLINE,
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

/// The order a body of `hash` with `bytes` serialized bytes gets when it arrives at `now`.
fn body_order(claims: &AnnouncementClaims, hash: block::Hash, bytes: usize, now: Instant) -> u64 {
    claims
        .resolve(hash, now, || bytes)
        .claimed_order()
        .unwrap_or_else(order::next)
}

fn claimed_order(claims: &AnnouncementClaims, hash: block::Hash) -> Option<u64> {
    claims.claimed_order(hash)
}
