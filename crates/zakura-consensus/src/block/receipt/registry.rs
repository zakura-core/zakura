//! Receipts shared by overlapping deliveries of the same complete block.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use zakura_chain::{block, serialization::ZcashSerialize};
use zakura_node_services::block_memory::BlockHandle;

use super::{
    claims::{AnnouncementClaims, ClaimOutcome, MinedGuard},
    order::BodyArrival,
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
/// claim. Otherwise it keeps its own arrival order. Every body under a claimed hash can take
/// the same order, while still having a receipt of its own.
///
/// Deliveries that share a receipt also take turns verifying: see
/// [`ReceiptGuard::verification_turn`].
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
    /// Held by the one caller verifying this body. The others wait for it to finish.
    turn: VerificationTurn,
}

/// The lock that lets one delivery of a body verify at a time.
type VerificationTurn = Arc<tokio::sync::Mutex<()>>;

/// Keeps a block's receipt active until its verification finishes or is cancelled.
pub(in crate::block) struct ReceiptGuard {
    active: Active,
    /// The hash and body this receipt is registered under, or `None` for an untracked receipt.
    pub(super) registered: Option<(block::Hash, BlockHandle)>,
    pub(in crate::block) order: u64,
    /// The turn shared by every delivery of the registered body.
    turn: Option<VerificationTurn>,
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
        self.register_at(hash, block, BodyArrival::now())
    }

    pub(super) fn register_at(
        &self,
        hash: block::Hash,
        block: &BlockHandle,
        arrival: BodyArrival,
    ) -> ReceiptGuard {
        let (receipt, counted) = self.draw(hash, block, arrival);
        if let Some(claim) = counted {
            claim.record(hash, receipt.order, || block.zcash_serialized_size());
        }
        receipt
    }

    /// Gives `block` its receipt, and returns the claim outcome to count for it.
    ///
    /// Only a newly tracked receipt is counted. A body that shares a receipt was counted when
    /// that receipt was drawn, and an untracked body is not counted, so junk bodies under a real
    /// header cannot skew the claim metrics.
    pub(super) fn draw(
        &self,
        hash: block::Hash,
        block: &BlockHandle,
        arrival: BodyArrival,
    ) -> (ReceiptGuard, Option<ClaimOutcome>) {
        let claim = self.claims.resolve(hash, block.coinbase_height(), arrival);
        let new_order = claim.claimed_order().unwrap_or(arrival.order);
        let (order, registered, turn, counted) = {
            let mut active = self
                .active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let bodies = active.entry(hash).or_default();
            // Handle equality compares the blocks, skipping headers and transactions they share.
            if let Some(entry) = bodies.iter_mut().find(|entry| entry.block == *block) {
                entry.callers = entry
                    .callers
                    .checked_add(1)
                    .expect("each active caller owns memory, so its count fits in usize");
                (
                    entry.order,
                    Some((hash, entry.block.clone())),
                    Some(entry.turn.clone()),
                    None,
                )
            } else if bodies.len() < MAX_BODIES_PER_HASH {
                let turn = VerificationTurn::default();
                bodies.push(Entry {
                    block: block.clone(),
                    order: new_order,
                    callers: 1,
                    turn: turn.clone(),
                });
                (
                    new_order,
                    Some((hash, block.clone())),
                    Some(turn),
                    Some(claim),
                )
            } else {
                (new_order, None, None, None)
            }
        };
        let receipt = ReceiptGuard {
            active: self.active.clone(),
            registered,
            order,
            turn,
        };
        (receipt, counted)
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

impl ReceiptGuard {
    /// Waits until no other delivery of this body is verifying, and returns the turn.
    ///
    /// Legacy gossip, native block sync and BPv1 can each deliver the same block. Without turns,
    /// every delivery that arrives before the first one commits passes the duplicate check and
    /// verifies the whole block again. With them, a later delivery waits, then finds the block in
    /// state and reports a duplicate. If the earlier delivery fails or is cancelled, the waiting
    /// one verifies the body itself.
    ///
    /// Only deliveries of an equal body share a turn, so a peer's invalid body under a real
    /// header never delays the valid one. An untracked receipt verifies at once.
    pub(in crate::block) async fn verification_turn(
        &self,
    ) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        let turn = self.turn.clone()?;
        match turn.clone().try_lock_owned() {
            Ok(guard) => Some(guard),
            Err(_) => {
                metrics::counter!("state.semantic_commit.in_flight_waits").increment(1);
                Some(turn.lock_owned().await)
            }
        }
    }

    /// Takes the turn only if no other delivery of this body is verifying, and never waits.
    ///
    /// A miner's own submission must not wait behind a network copy of its block: it would lose
    /// its prepared fast path and return a duplicate that never publishes over BPv1. When it is
    /// first, later deliveries still wait for it as [`Self::verification_turn`] describes.
    pub(in crate::block) fn try_verification_turn(
        &self,
    ) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        self.turn.clone()?.try_lock_owned().ok()
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
