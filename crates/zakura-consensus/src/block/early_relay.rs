//! Early block relay: serves one verified tip child to negotiated peers before its commit ends.
//!
//! Legacy peers that both set the `NODE_COMPACT_BLOCKS` service bit can receive a block's
//! inventory and body while this node's contextual commit is still running. The draft Compact
//! Block Relay ZIP (zcash/zips#1429) defines this behavior, and Zebra implements it.
//!
//! This node publishes a block only after full semantic verification, including proofs and
//! signatures, and after state confirms that the block extends the selected tip with the
//! expected work and a body that matches its header commitments. Only the contextual rules
//! remain: double spends, anchors, and missing outputs.
//!
//! The cache holds one block, and serves it only while its parent is the best tip. A commit
//! failure or a tip change withdraws it. Bytes already written to a peer cannot be recalled, so
//! negotiated receivers do not score a sender for those later failures.
//!
//! A withdrawal keeps the slot reserved until the deadline or a tip change. One parent therefore
//! buys at most one early fan-out, even if a miner produces several invalid children of it.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{sync::watch, time::Instant};
use zakura_chain::{block, chain_tip::ChainTip};
use zakura_node_services::block_memory::BlockHandle;
use zakura_state::LatestChainTip;

/// How long a published block stays servable if its commit never finishes.
pub const EARLY_RELAY_LIFETIME: Duration = Duration::from_secs(15);

/// How long a rejected early-relay block stays rejected without verification.
pub const EARLY_RELAY_REJECTION_LIFETIME: Duration = Duration::from_secs(60);

/// The number of rejected early-relay block hashes this node remembers.
pub const MAX_EARLY_RELAY_REJECTIONS: usize = 8;

/// The block that this node currently serves before its commit finishes.
#[derive(Debug)]
struct Entry {
    hash: block::Hash,
    block: BlockHandle,
    owner: u64,
}

/// The early-relay slot of one parent, held from publication until `deadline` or a tip change.
#[derive(Debug)]
struct Reservation {
    parent: block::Hash,
    deadline: Instant,
}

#[derive(Debug, Default)]
struct Slots {
    entry: Option<Entry>,
    reservation: Option<Reservation>,
    next_owner: u64,
    /// Ring of recently rejected hashes and their expiry times.
    rejections: [Option<(block::Hash, Instant)>; MAX_EARLY_RELAY_REJECTIONS],
    next_rejection: usize,
}

impl Slots {
    fn is_rejected(&self, hash: block::Hash, now: Instant) -> bool {
        self.rejections
            .iter()
            .flatten()
            .any(|(rejected, deadline)| *rejected == hash && now < *deadline)
    }

    fn reject(&mut self, hash: block::Hash, now: Instant) {
        if self.is_rejected(hash, now) {
            return;
        }
        self.rejections[self.next_rejection] = Some((hash, now + EARLY_RELAY_REJECTION_LIFETIME));
        self.next_rejection = (self.next_rejection + 1) % MAX_EARLY_RELAY_REJECTIONS;
    }

    /// Returns the reservation if it still holds the slot: before its deadline, while its parent
    /// is the best tip.
    fn live_reservation(
        &self,
        best_tip: Option<block::Hash>,
        now: Instant,
    ) -> Option<&Reservation> {
        self.reservation.as_ref().filter(|reservation| {
            now < reservation.deadline && best_tip == Some(reservation.parent)
        })
    }
}

#[derive(Debug)]
struct Inner {
    slots: Mutex<Slots>,
    /// The hash of each published block, and `None` when it is withdrawn.
    published: watch::Sender<Option<block::Hash>>,
    tip: LatestChainTip,
}

/// The early-relay cache shared by the block verifier, the inbound service, and block gossip.
///
/// Only a node with `network.compact_block_relay` enabled creates one.
#[derive(Clone, Debug)]
pub struct EarlyRelay(Arc<Inner>);

impl EarlyRelay {
    /// Creates an empty cache that serves a block only while its parent is `tip`'s best tip.
    pub fn new(tip: LatestChainTip) -> Self {
        Self(Arc::new(Inner {
            slots: Mutex::default(),
            published: watch::Sender::new(None),
            tip,
        }))
    }

    fn slots(&self) -> std::sync::MutexGuard<'_, Slots> {
        self.0
            .slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn best_tip(&self) -> Option<block::Hash> {
        self.0.tip.best_tip_hash()
    }

    /// Publishes `block` for early relay, unless its parent is not the best tip, another block
    /// reserved the slot for that parent, or `hash` was recently rejected.
    ///
    /// The caller must have checked that the block passed full semantic verification and
    /// extends the selected tip with the expected work. The block is servable before
    /// subscribers learn its hash. Dropping the returned guard withdraws the block, but keeps
    /// the slot reserved until [`EARLY_RELAY_LIFETIME`] passes or the best tip changes.
    pub fn publish(&self, block: BlockHandle, hash: block::Hash) -> Option<EarlyRelayGuard> {
        let now = Instant::now();
        let parent = block.header().previous_block_hash;
        let best_tip = self.best_tip();
        let mut slots = self.slots();
        if best_tip != Some(parent)
            || slots.is_rejected(hash, now)
            || slots.live_reservation(best_tip, now).is_some()
        {
            return None;
        }
        slots.next_owner = slots.next_owner.wrapping_add(1);
        let owner = slots.next_owner;
        slots.entry = Some(Entry { hash, block, owner });
        slots.reservation = Some(Reservation {
            parent,
            deadline: now + EARLY_RELAY_LIFETIME,
        });
        drop(slots);

        self.0.published.send_replace(Some(hash));
        metrics::counter!("consensus.early_relay.published").increment(1);
        Some(EarlyRelayGuard {
            relay: self.clone(),
            hash,
            owner,
        })
    }

    /// Returns the published block with `hash`, if it is still servable: before its deadline,
    /// while its parent is the best tip.
    pub fn get(&self, hash: block::Hash) -> Option<BlockHandle> {
        let best_tip = self.best_tip();
        let slots = self.slots();
        let entry = slots.entry.as_ref().filter(|entry| entry.hash == hash)?;
        slots
            .live_reservation(best_tip, Instant::now())
            .is_some()
            .then(|| entry.block.clone())
    }

    /// Returns a receiver that sees the hash of each published block, and `None` when the block
    /// is withdrawn.
    ///
    /// The receiver starts at the current value, so callers should wait for a change first. A
    /// tip change or an expired deadline doesn't send `None`: callers wait for
    /// [`Self::withdrawn`] instead.
    pub fn subscribe(&self) -> watch::Receiver<Option<block::Hash>> {
        self.0.published.subscribe()
    }

    /// Returns when `hash` can no longer be served: it was withdrawn or replaced, its deadline
    /// passed, or the best tip changed.
    pub async fn withdrawn(&self, hash: block::Hash) {
        let mut published = self.subscribe();
        let mut tip = self.0.tip.clone();
        tip.mark_best_tip_seen();
        while self.get(hash).is_some() {
            let deadline = self
                .slots()
                .reservation
                .as_ref()
                .map_or_else(Instant::now, |reservation| reservation.deadline);
            tokio::select! {
                changed = published.changed() => if changed.is_err() { return },
                changed = tip.best_tip_changed() => if changed.is_err() { return },
                () = tokio::time::sleep_until(deadline) => {}
            }
        }
    }

    /// Returns true if `hash` failed validation after authentication within the last
    /// [`EARLY_RELAY_REJECTION_LIFETIME`].
    pub fn is_rejected(&self, hash: block::Hash) -> bool {
        self.slots().is_rejected(hash, Instant::now())
    }

    /// Remembers that `hash` failed a deterministic rule after its header and commitments
    /// checked out against a committed parent.
    ///
    /// Every body that matches such a header fails the same rule, so later deliveries skip
    /// verification. Call this only for deterministic failures, never for timeouts or missing
    /// context.
    pub fn reject(&self, hash: block::Hash) {
        let mut slots = self.slots();
        slots.reject(hash, Instant::now());
        let withdrawn = slots.entry.take_if(|entry| entry.hash == hash).is_some();
        drop(slots);
        if withdrawn {
            self.0.published.send_replace(None);
        }
        metrics::counter!("consensus.early_relay.rejected").increment(1);
    }

    fn withdraw(&self, owner: u64) {
        let withdrawn = self
            .slots()
            .entry
            .take_if(|entry| entry.owner == owner)
            .is_some();
        if withdrawn {
            self.0.published.send_replace(None);
        }
    }
}

/// Owns one published early-relay block until its commit finishes.
#[derive(Debug)]
#[must_use = "dropping the guard withdraws the published block"]
pub struct EarlyRelayGuard {
    relay: EarlyRelay,
    hash: block::Hash,
    owner: u64,
}

impl EarlyRelayGuard {
    /// Withdraws the block after a deterministic commit failure, and remembers its hash.
    pub fn reject(self) {
        self.relay.reject(self.hash);
    }
}

impl Drop for EarlyRelayGuard {
    /// Stops serving the block. After a successful commit, state serves it instead.
    fn drop(&mut self) {
        self.relay.withdraw(self.owner);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use zakura_chain::{block::Block, parameters::Network, serialization::ZcashDeserializeInto};
    use zakura_state::{ChainTipBlock, ChainTipSender, SemanticallyVerifiedBlock};

    use super::*;

    fn decode(bytes: &[u8]) -> Arc<Block> {
        bytes.zcash_deserialize_into().expect("test vector decodes")
    }

    fn tip_block(block: &Arc<Block>) -> ChainTipBlock {
        SemanticallyVerifiedBlock::from(block.clone()).into()
    }

    /// Returns a relay whose best tip is Mainnet genesis, and the sender that moves the tip.
    fn relay_at_genesis() -> (EarlyRelay, ChainTipSender) {
        let genesis = decode(&zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES);
        let (sender, tip, _change) = ChainTipSender::new(tip_block(&genesis), &Network::Mainnet);
        (EarlyRelay::new(tip), sender)
    }

    /// Mainnet block 1, a child of genesis.
    fn child() -> (BlockHandle, block::Hash) {
        let block = decode(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES);
        let hash = block.hash();
        (block.into(), hash)
    }

    /// A sibling of [`child`] with a different hash.
    fn sibling() -> (BlockHandle, block::Hash) {
        let mut block = decode(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES);
        Arc::make_mut(&mut Arc::make_mut(&mut block).header).nonce.0[0] ^= 1;
        let hash = block.hash();
        (block.into(), hash)
    }

    #[tokio::test(start_paused = true)]
    async fn published_block_is_servable_until_its_guard_drops() {
        let (relay, _tip) = relay_at_genesis();
        let (block, hash) = child();
        let mut published = relay.subscribe();

        let guard = relay.publish(block, hash).expect("the slot is free");
        assert!(published.has_changed().expect("sender is alive"));
        assert_eq!(*published.borrow_and_update(), Some(hash));
        assert!(relay.get(hash).is_some());

        drop(guard);
        assert!(relay.get(hash).is_none());
        assert_eq!(*published.borrow_and_update(), None);
        assert!(!relay.is_rejected(hash));
    }

    /// Only a child of the best tip is published.
    #[tokio::test(start_paused = true)]
    async fn only_a_child_of_the_best_tip_is_published() {
        let (relay, _tip) = relay_at_genesis();
        let grandchild = decode(&zakura_test::vectors::BLOCK_MAINNET_2_BYTES);
        let hash = grandchild.hash();
        assert!(relay.publish(grandchild.into(), hash).is_none());
    }

    /// A tip change withdraws the block and frees the slot, and wakes [`EarlyRelay::withdrawn`].
    #[tokio::test(start_paused = true)]
    async fn a_tip_change_withdraws_the_block() {
        let (relay, mut tip) = relay_at_genesis();
        let (block, hash) = child();
        let _guard = relay.publish(block, hash).expect("the slot is free");
        let withdrawn = tokio::spawn({
            let relay = relay.clone();
            async move { relay.withdrawn(hash).await }
        });
        tokio::task::yield_now().await;
        assert!(!withdrawn.is_finished());

        // Any other committed tip, here the block itself, ends early relay for this parent.
        tip.set_finalized_tip(tip_block(&decode(
            &zakura_test::vectors::BLOCK_MAINNET_1_BYTES,
        )));

        assert!(relay.get(hash).is_none(), "the parent is no longer the tip");
        withdrawn
            .await
            .expect("the wait finishes after the tip changes");
    }

    /// After a withdrawal, the slot stays reserved for the same parent until the deadline, so one
    /// parent buys at most one early fan-out.
    #[tokio::test(start_paused = true)]
    async fn a_withdrawn_block_keeps_the_slot_until_the_deadline() {
        let (relay, _tip) = relay_at_genesis();
        let (first, first_hash) = child();
        let (second, second_hash) = sibling();

        let first_guard = relay.publish(first, first_hash).expect("the slot is free");
        assert!(relay.publish(second.clone(), second_hash).is_none());
        drop(first_guard);
        assert!(
            relay.publish(second.clone(), second_hash).is_none(),
            "a withdrawal keeps the slot reserved"
        );

        tokio::time::advance(EARLY_RELAY_LIFETIME).await;
        let second_guard = relay
            .publish(second, second_hash)
            .expect("the deadline frees the slot");
        assert!(relay.get(second_hash).is_some());
        drop(second_guard);
    }

    /// A stale guard must not withdraw the block that replaced it.
    #[tokio::test(start_paused = true)]
    async fn an_expired_guard_does_not_withdraw_its_replacement() {
        let (relay, _tip) = relay_at_genesis();
        let (first, first_hash) = child();
        let (second, second_hash) = sibling();

        let first_guard = relay.publish(first, first_hash).expect("the slot is free");
        tokio::time::advance(EARLY_RELAY_LIFETIME).await;
        assert!(
            relay.get(first_hash).is_none(),
            "expired blocks are not served"
        );
        let _second_guard = relay
            .publish(second, second_hash)
            .expect("an expired block frees the slot");
        drop(first_guard);
        assert!(relay.get(second_hash).is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn rejected_hash_is_withdrawn_and_cannot_be_republished_until_it_expires() {
        let (relay, _tip) = relay_at_genesis();
        let (block, hash) = child();

        relay
            .publish(block.clone(), hash)
            .expect("the slot is free")
            .reject();
        assert!(relay.get(hash).is_none());
        assert!(relay.is_rejected(hash));
        assert!(relay.publish(block.clone(), hash).is_none());

        tokio::time::advance(EARLY_RELAY_REJECTION_LIFETIME).await;
        assert!(!relay.is_rejected(hash));
        assert!(relay.publish(block, hash).is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn rejection_ring_keeps_the_most_recent_hashes() {
        let (relay, _tip) = relay_at_genesis();
        let hashes: Vec<_> = (0..=MAX_EARLY_RELAY_REJECTIONS)
            .map(|byte| block::Hash([u8::try_from(byte).expect("small"); 32]))
            .collect();
        for hash in &hashes {
            relay.reject(*hash);
        }

        assert!(
            !relay.is_rejected(hashes[0]),
            "the oldest entry is overwritten"
        );
        assert!(hashes[1..].iter().all(|hash| relay.is_rejected(*hash)));
    }
}
