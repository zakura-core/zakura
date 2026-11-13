//! Track the blocks a connection's peer has announced to us.
//!
//! Block gossip skips a peer that already told us about the block, unless a broadcast still
//! tracks its upload. This avoids sending redundant inventory and waiting for a request
//! from a peer that already claims to have the block.
//!
//! Each connection records only its own peer's announcements, so one peer cannot change
//! whether another peer is advertised to.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use tokio::time::Instant;
use zakura_chain::block;

use crate::constants::INVENTORY_ROTATION_INTERVAL;

/// How many announced hashes one connection remembers per generation.
///
/// A `getblocks` reply carries up to 500 hashes, so a smaller cap would be flushed by one
/// legitimate solicited `inv`. At 32 bytes per hash this is 16 KiB of hashes per generation,
/// plus the set's own overhead.
const ANNOUNCED_BLOCKS_LIMIT: usize = 512;

/// The block hashes this connection's peer has announced to us.
#[derive(Clone, Debug, Default)]
pub(crate) struct AnnouncedBlocks(Arc<Mutex<Generations>>);

/// Two generations of announcements, so entries expire without ever being shifted.
///
/// An announcement is the peer's claim, not proof that it kept the block, so it only has to
/// outlast a fan-out. Rotating whole generations bounds both the retained hashes and the work
/// per hash: an `inv` carries up to [`InventoryHash::max_allocation`] entries, and keeping an
/// insertion-ordered cache would shift the retained entries once per announced hash.
///
/// [`InventoryHash::max_allocation`]: crate::protocol::external::InventoryHash
#[derive(Debug)]
struct Generations {
    /// Announcements since the last rotation.
    current: HashSet<block::Hash>,
    /// Announcements from the generation before that, still readable but not refreshed.
    previous: HashSet<block::Hash>,
    rotated_at: Instant,
}

impl Default for Generations {
    fn default() -> Self {
        Self {
            current: HashSet::new(),
            previous: HashSet::new(),
            rotated_at: Instant::now(),
        }
    }
}

impl Generations {
    /// Retires generations that have outlived the rotation interval, or filled the cap.
    ///
    /// An entry stays readable for at least one interval and at most two, matching the window
    /// the inventory registry keeps its own entries. Expiring early only costs one redundant
    /// advert, so a full generation rotates ahead of its interval.
    fn rotate_if_due(&mut self) {
        let elapsed = self.rotated_at.elapsed();
        if elapsed >= 2 * INVENTORY_ROTATION_INTERVAL {
            // Nothing read or recorded for two intervals, so every entry in both generations
            // is at least one interval old: no caller can still rely on them.
            self.current.clear();
            self.previous.clear();
        } else if elapsed >= INVENTORY_ROTATION_INTERVAL
            || self.current.len() >= ANNOUNCED_BLOCKS_LIMIT
        {
            self.previous = std::mem::take(&mut self.current);
        } else {
            return;
        }
        self.rotated_at = Instant::now();
    }
}

impl AnnouncedBlocks {
    /// Records that the peer announced `hashes`, retiring older generations as needed.
    pub(crate) fn record(&self, hashes: impl IntoIterator<Item = block::Hash>) {
        let mut announced = self
            .0
            .lock()
            .expect("announced block mutex is never poisoned");
        for hash in hashes {
            announced.rotate_if_due();
            announced.current.insert(hash);
        }
    }

    /// Forgets `hashes` when the peer requests them or tells us it does not have them.
    pub(crate) fn forget(&self, hashes: impl IntoIterator<Item = block::Hash>) {
        let mut announced = self
            .0
            .lock()
            .expect("announced block mutex is never poisoned");
        for hash in hashes {
            announced.current.remove(&hash);
            announced.previous.remove(&hash);
        }
    }

    /// Returns whether the peer announced `hash` recently enough to still have it.
    pub(crate) fn announced(&self, hash: block::Hash) -> bool {
        let mut announced = self
            .0
            .lock()
            .expect("announced block mutex is never poisoned");
        announced.rotate_if_due();
        announced.current.contains(&hash) || announced.previous.contains(&hash)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn hash(index: usize) -> block::Hash {
        let mut bytes = [0; 32];
        bytes[..std::mem::size_of::<usize>()].copy_from_slice(&index.to_le_bytes());
        block::Hash(bytes)
    }

    /// Retained hashes and retained allocation are both bounded, and the newest survive.
    #[tokio::test(start_paused = true)]
    async fn announcements_are_bounded_and_keep_the_newest() {
        const EXCESS: usize = 50;
        let announced = AnnouncedBlocks::default();
        // One hash per insertion, so a generation fills one entry at a time.
        for index in 0..ANNOUNCED_BLOCKS_LIMIT + EXCESS {
            announced.record([hash(index)]);
        }
        let entries = announced
            .0
            .lock()
            .expect("announced block mutex is never poisoned");
        assert_eq!(entries.current.len(), EXCESS);
        assert_eq!(entries.previous.len(), ANNOUNCED_BLOCKS_LIMIT);
        drop(entries);
        assert!((0..ANNOUNCED_BLOCKS_LIMIT + EXCESS).all(|index| announced.announced(hash(index))));
        // One more generation retires the oldest hashes.
        for index in 0..ANNOUNCED_BLOCKS_LIMIT {
            announced.record([hash(ANNOUNCED_BLOCKS_LIMIT + EXCESS + index)]);
        }
        assert!(!announced.announced(hash(0)));
        assert!(announced.announced(hash(2 * ANNOUNCED_BLOCKS_LIMIT + EXCESS - 1)));
    }

    /// A maximum-sized inventory retains bounded entries and allocation, including when
    /// hashes repeat: one `inv` can carry 50,000 hashes.
    #[tokio::test(start_paused = true)]
    async fn maximum_inventory_retains_bounded_entries_and_allocation() {
        use crate::protocol::external::InventoryHash;
        use zakura_chain::serialization::TrustedPreallocate;

        let limit = usize::try_from(InventoryHash::max_allocation())
            .expect("the inventory limit fits in usize");
        for distinct in [limit, limit / 2, ANNOUNCED_BLOCKS_LIMIT] {
            let announced = AnnouncedBlocks::default();
            let hashes: Vec<_> = (0..limit).map(|index| hash(index % distinct)).collect();
            announced.record(hashes);
            let entries = announced
                .0
                .lock()
                .expect("announced block mutex is never poisoned");
            assert!(entries.current.len() <= ANNOUNCED_BLOCKS_LIMIT);
            assert!(entries.previous.len() <= ANNOUNCED_BLOCKS_LIMIT);
            assert!(
                entries.current.capacity() + entries.previous.capacity()
                    <= 4 * ANNOUNCED_BLOCKS_LIMIT,
                "a single inventory retained an oversized allocation: {} + {}",
                entries.current.capacity(),
                entries.previous.capacity()
            );
            // The last hash of the message is always still readable.
            drop(entries);
            assert!(announced.announced(hash((limit - 1) % distinct)));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_announcements_do_not_evict_other_hashes() {
        let announced = AnnouncedBlocks::default();
        announced.record([hash(1)]);
        for _ in 0..ANNOUNCED_BLOCKS_LIMIT * 2 {
            announced.record([hash(2)]);
        }
        assert!(announced.announced(hash(1)));
        assert!(announced.announced(hash(2)));
    }

    /// Filling a generation retires the one before it, and does not refresh its own entries.
    #[tokio::test(start_paused = true)]
    async fn filling_a_generation_retires_only_the_older_one() {
        let announced = AnnouncedBlocks::default();
        announced.record((0..ANNOUNCED_BLOCKS_LIMIT).map(hash));
        // Rotates, because the current generation is full.
        announced.record([hash(ANNOUNCED_BLOCKS_LIMIT)]);
        assert!((0..=ANNOUNCED_BLOCKS_LIMIT).all(|index| announced.announced(hash(index))));
        // Filling the new generation retires the first one.
        announced.record((0..ANNOUNCED_BLOCKS_LIMIT).map(|index| hash(index + 1_000)));
        assert!(!announced.announced(hash(0)));
        assert!(announced.announced(hash(ANNOUNCED_BLOCKS_LIMIT)));
    }

    /// An announcement outlives one rotation interval, and is gone after two.
    #[tokio::test(start_paused = true)]
    async fn announcements_expire_after_one_to_two_rotation_intervals() {
        let announced = AnnouncedBlocks::default();
        announced.record([hash(3)]);
        tokio::time::advance(INVENTORY_ROTATION_INTERVAL - Duration::from_millis(1)).await;
        assert!(
            announced.announced(hash(3)),
            "an announcement lives at least one rotation interval"
        );
        // This read rotates, moving the hash to the previous generation.
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(announced.announced(hash(3)));
        // The next rotation retires it.
        tokio::time::advance(INVENTORY_ROTATION_INTERVAL).await;
        assert!(!announced.announced(hash(3)));
    }

    /// A connection that neither records nor reads for two intervals retains nothing, so a
    /// quiet connection cannot keep suppressing adverts.
    #[tokio::test(start_paused = true)]
    async fn announcements_expire_without_intervening_reads() {
        let announced = AnnouncedBlocks::default();
        announced.record([hash(6), hash(7)]);
        tokio::time::advance(2 * INVENTORY_ROTATION_INTERVAL).await;
        assert!(!announced.announced(hash(6)));
        assert!(!announced.announced(hash(7)));
    }

    #[tokio::test(start_paused = true)]
    async fn forgotten_announcements_are_not_reported() {
        let announced = AnnouncedBlocks::default();
        announced.record([hash(4), hash(5)]);
        // Retire the generation holding both, so `forget` must reach the older one too.
        announced.record((0..ANNOUNCED_BLOCKS_LIMIT).map(|index| hash(index + 1_000)));
        announced.forget([hash(4)]);
        assert!(!announced.announced(hash(4)));
        assert!(announced.announced(hash(5)));
    }
}
