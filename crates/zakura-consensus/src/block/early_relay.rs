//! Early block relay: serves one verified tip child to negotiated peers before its commit ends.
//!
//! Legacy peers that both set the `NODE_EARLY_BLOCK_RELAY` service bit can receive a block's
//! inventory and body while this node's contextual commit is still running. Zebra's Ándale
//! release negotiates the same service bit.
//!
//! This node publishes a block only after full semantic verification, including proofs and
//! signatures, and after state confirms that the block extends the selected tip with the
//! expected work and a body that matches its header commitments. Only the contextual rules
//! remain: double spends, anchors, and missing outputs.
//!
//! The cache holds one block. A commit failure withdraws it. Bytes already written to a peer
//! cannot be recalled, so negotiated receivers do not score a sender for those later failures.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{sync::watch, time::Instant};
use zakura_chain::block;
use zakura_node_services::block_memory::BlockHandle;

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
    deadline: Instant,
    owner: u64,
}

#[derive(Debug, Default)]
struct Slots {
    entry: Option<Entry>,
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
}

#[derive(Debug)]
struct Inner {
    slots: Mutex<Slots>,
    published: watch::Sender<Option<block::Hash>>,
}

/// The early-relay cache shared by the block verifier, the inbound service, and block gossip.
///
/// Only a node with `network.experimental_early_block_relay` enabled creates one.
#[derive(Clone, Debug)]
pub struct EarlyRelay(Arc<Inner>);

impl Default for EarlyRelay {
    fn default() -> Self {
        Self(Arc::new(Inner {
            slots: Mutex::default(),
            published: watch::Sender::new(None),
        }))
    }
}

impl EarlyRelay {
    fn slots(&self) -> std::sync::MutexGuard<'_, Slots> {
        self.0
            .slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Publishes `block` for early relay, unless another live block holds the slot or `hash`
    /// was recently rejected.
    ///
    /// The caller must have checked that the block passed full semantic verification and
    /// extends the selected tip with the expected work. The block is servable before
    /// subscribers learn its hash. Dropping the returned guard withdraws the block.
    pub fn publish(&self, block: BlockHandle, hash: block::Hash) -> Option<EarlyRelayGuard> {
        let now = Instant::now();
        let mut slots = self.slots();
        if slots.is_rejected(hash, now)
            || slots
                .entry
                .as_ref()
                .is_some_and(|entry| now < entry.deadline)
        {
            return None;
        }
        slots.next_owner = slots.next_owner.wrapping_add(1);
        let owner = slots.next_owner;
        slots.entry = Some(Entry {
            hash,
            block,
            deadline: now + EARLY_RELAY_LIFETIME,
            owner,
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

    /// Returns the published block with `hash`, if it is still servable.
    pub fn get(&self, hash: block::Hash) -> Option<BlockHandle> {
        let slots = self.slots();
        let entry = slots.entry.as_ref()?;
        (entry.hash == hash && Instant::now() < entry.deadline).then(|| entry.block.clone())
    }

    /// Returns a receiver that sees the hash of each published block.
    ///
    /// The receiver starts at the current value, so callers should wait for a change first.
    pub fn subscribe(&self) -> watch::Receiver<Option<block::Hash>> {
        self.0.published.subscribe()
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
        if slots.entry.as_ref().is_some_and(|entry| entry.hash == hash) {
            slots.entry = None;
        }
        metrics::counter!("consensus.early_relay.rejected").increment(1);
    }

    fn withdraw(&self, owner: u64) {
        let mut slots = self.slots();
        if slots
            .entry
            .as_ref()
            .is_some_and(|entry| entry.owner == owner)
        {
            slots.entry = None;
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

    use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};

    use super::*;

    fn block_and_hash(bytes: &[u8]) -> (BlockHandle, block::Hash) {
        let block: Arc<Block> = bytes.zcash_deserialize_into().expect("test vector decodes");
        let hash = block.hash();
        (block.into(), hash)
    }

    #[tokio::test(start_paused = true)]
    async fn published_block_is_servable_until_its_guard_drops() {
        let relay = EarlyRelay::default();
        let (block, hash) = block_and_hash(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES);
        let mut published = relay.subscribe();

        let guard = relay.publish(block, hash).expect("the slot is free");
        assert!(published.has_changed().expect("sender is alive"));
        assert_eq!(*published.borrow_and_update(), Some(hash));
        assert!(relay.get(hash).is_some());

        drop(guard);
        assert!(relay.get(hash).is_none());
        assert!(!relay.is_rejected(hash));
    }

    #[tokio::test(start_paused = true)]
    async fn one_live_block_holds_the_slot_until_it_expires() {
        let relay = EarlyRelay::default();
        let (first, first_hash) = block_and_hash(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES);
        let (second, second_hash) = block_and_hash(&zakura_test::vectors::BLOCK_MAINNET_2_BYTES);

        let first_guard = relay.publish(first, first_hash).expect("the slot is free");
        assert!(relay.publish(second.clone(), second_hash).is_none());

        tokio::time::advance(EARLY_RELAY_LIFETIME).await;
        assert!(
            relay.get(first_hash).is_none(),
            "expired blocks are not served"
        );
        let second_guard = relay
            .publish(second, second_hash)
            .expect("an expired block frees the slot");

        // The stale guard must not withdraw the block that replaced it.
        drop(first_guard);
        assert!(relay.get(second_hash).is_some());
        drop(second_guard);
    }

    #[tokio::test(start_paused = true)]
    async fn rejected_hash_is_withdrawn_and_cannot_be_republished_until_it_expires() {
        let relay = EarlyRelay::default();
        let (block, hash) = block_and_hash(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES);

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
        let relay = EarlyRelay::default();
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
