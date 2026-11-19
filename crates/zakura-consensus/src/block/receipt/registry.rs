//! Receipts shared by overlapping deliveries of the same complete block.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::time::Instant;
use zakura_chain::{block, serialization::ZcashSerialize};
use zakura_node_services::block_memory::BlockHandle;

use super::{
    claims::{AnnouncementClaims, MinedGuard},
    order,
};

/// The most distinct bodies tracked under one block hash at a time.
///
/// A peer can deliver any number of bodies under a real header, all but one invalid, and each
/// delivery is compared with the bodies already tracked under that hash. Past this bound a body
/// gets a receipt of its own without being tracked, so an overlapping duplicate of it can only
/// receive a later receipt.
pub(super) const MAX_BODIES_PER_HASH: usize = 4;

/// The bodies with an active verification, by block hash.
type Active = Arc<Mutex<HashMap<block::Hash, Vec<Entry>>>>;

/// Gives each delivered block its receipt order.
///
/// Overlapping deliveries of the same complete block share its first receipt. Entries live only
/// while a verification of that body is active, so a block delivered again after every attempt
/// finishes or is cancelled gets a new receipt.
///
/// Deliveries are matched by block hash, then by body equality. Receipts are taken before the
/// header's commitment to the transactions' authorizing data is checked, so bodies that differ
/// only in that data share a block hash but not a receipt.
///
/// A body that shares no active receipt takes the order of its block's announcement claim if it
/// arrived inside the claim's deadline and no block this node mined at that height outranks the
/// claim. Otherwise it takes the next order. Every body under a claimed hash can therefore take
/// the same order, while still having a receipt of its own.
#[derive(Clone, Debug, Default)]
pub(in crate::block) struct ReceiptRegistry {
    pub(super) active: Active,
    claims: AnnouncementClaims,
}

#[derive(Debug)]
pub(super) struct Entry {
    /// The delivered body, held while a verification of it is active. The delivering
    /// verification holds the same block, and its decoded-memory charge, for at least as long.
    block: BlockHandle,
    order: u64,
    callers: usize,
}

/// Keeps a block's receipt active until its verification finishes or is cancelled.
pub(in crate::block) struct ReceiptGuard {
    active: Active,
    /// The hash and body this receipt is registered under, or `None` for an untracked receipt.
    pub(super) registered: Option<(block::Hash, BlockHandle)>,
    pub(in crate::block) order: u64,
}

impl ReceiptRegistry {
    /// A registry that ranks bodies by the claims header sync writes to `claims`.
    pub(in crate::block) fn new(claims: AnnouncementClaims) -> Self {
        Self {
            active: Active::default(),
            claims,
        }
    }

    /// Gives a receipt to `block`, whose hash is `hash`.
    ///
    /// Called synchronously by the verifier before returning its request future.
    pub(in crate::block) fn register(
        &self,
        hash: block::Hash,
        block: &BlockHandle,
    ) -> ReceiptGuard {
        self.register_at(hash, block, Instant::now())
    }

    pub(super) fn register_at(
        &self,
        hash: block::Hash,
        block: &BlockHandle,
        now: Instant,
    ) -> ReceiptGuard {
        let claim = self
            .claims
            .resolve(hash, now, || block.zcash_serialized_size());
        let (order, registered, drawn) = {
            let mut active = self
                .active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let bodies = active.entry(hash).or_default();
            let new_order = || claim.claimed_order().unwrap_or_else(order::next);
            // Handle equality compares the blocks, skipping headers and transactions they share.
            if let Some(entry) = bodies.iter_mut().find(|entry| entry.block == *block) {
                entry.callers = entry
                    .callers
                    .checked_add(1)
                    .expect("each active caller owns memory, so its count fits in usize");
                (entry.order, Some((hash, entry.block.clone())), false)
            } else if bodies.len() < MAX_BODIES_PER_HASH {
                let order = new_order();
                bodies.push(Entry {
                    block: block.clone(),
                    order,
                    callers: 1,
                });
                (order, Some((hash, block.clone())), true)
            } else {
                (new_order(), None, true)
            }
        };
        // A body that shares a receipt was counted when that receipt was drawn. An untracked body
        // is not counted, so junk bodies under a real header cannot skew the claim metrics.
        if drawn && registered.is_some() {
            claim.record(hash, order);
        }
        ReceiptGuard {
            active: self.active.clone(),
            registered,
            order,
        }
    }

    /// Records that the locally mined block `hash` at `height` took `order`. See
    /// [`AnnouncementClaims::mark_mined`].
    pub(in crate::block) fn mark_mined(
        &self,
        hash: block::Hash,
        height: block::Height,
        order: u64,
    ) -> MinedGuard {
        self.claims.mark_mined(hash, height, order)
    }
}

impl Drop for ReceiptGuard {
    fn drop(&mut self) {
        let Some((hash, block)) = &self.registered else {
            return;
        };
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let bodies = active
            .get_mut(hash)
            .expect("a live guard has a registered receipt");
        let index = bodies
            .iter()
            .position(|entry| std::ptr::eq(entry.block.view(), block.view()))
            .expect("a live guard holds the body its receipt is registered under");
        bodies[index].callers -= 1;
        // This guard still holds the body, so removing the entry does not free it under the lock.
        if bodies[index].callers == 0 {
            bodies.swap_remove(index);
        }
        if bodies.is_empty() {
            active.remove(hash);
        }
    }
}
