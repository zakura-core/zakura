//! Track the blocks a connection's peer has announced to us.
//!
//! Block gossip skips a peer that already told us about the block, unless a broadcast still
//! tracks its upload. This avoids sending redundant inventory and waiting for a request
//! from a peer that already claims to have the block.
//!
//! Each connection records only its own peer's announcements, so one peer cannot change
//! whether another peer is advertised to.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use indexmap::IndexMap;
use tokio::time::Instant;
use zakura_chain::block;

use crate::constants::INVENTORY_ROTATION_INTERVAL;

/// How many announced hashes one connection remembers.
///
/// A `getblocks` reply carries up to 500 hashes, so a smaller cap would be flushed by one
/// legitimate solicited `inv`. At 32 bytes per hash this is 16 KiB of hashes per connection,
/// plus the map's own overhead.
const ANNOUNCED_BLOCKS_LIMIT: usize = 512;

/// How long an announcement stays usable.
///
/// An announcement is the peer's claim, not proof that it kept the block. This matches the
/// window the inventory registry keeps its own entries, and outlasts any one fan-out.
const ANNOUNCEMENT_LIFETIME: Duration =
    Duration::from_secs(2 * INVENTORY_ROTATION_INTERVAL.as_secs());

/// The block hashes this connection's peer has announced to us, oldest first.
#[derive(Clone, Debug, Default)]
pub(crate) struct AnnouncedBlocks(Arc<Mutex<IndexMap<block::Hash, Instant>>>);

impl AnnouncedBlocks {
    /// Records `hashes`, keeping the map bounded even while processing a large message.
    pub(crate) fn record(&self, hashes: impl IntoIterator<Item = block::Hash>) {
        let now = Instant::now();
        let mut announced = self
            .0
            .lock()
            .expect("announced block mutex is never poisoned");
        for hash in hashes {
            // Re-inserting at the end keeps the map ordered oldest first.
            announced.shift_remove(&hash);
            // Evict before inserting: an inventory message can contain 50,000 hashes, and
            // trimming only after the loop would retain that allocation and make duplicate
            // refreshes shift an entire message's entries while holding the mutex.
            if announced.len() == ANNOUNCED_BLOCKS_LIMIT {
                announced.shift_remove_index(0);
            }
            announced.insert(hash, now);
        }
    }

    /// Forgets `hashes` when the peer requests them or tells us it does not have them.
    pub(crate) fn forget(&self, hashes: impl IntoIterator<Item = block::Hash>) {
        let mut announced = self
            .0
            .lock()
            .expect("announced block mutex is never poisoned");
        for hash in hashes {
            announced.shift_remove(&hash);
        }
    }

    /// Returns whether the peer announced `hash` recently enough to still have it.
    pub(crate) fn announced(&self, hash: block::Hash) -> bool {
        self.0
            .lock()
            .expect("announced block mutex is never poisoned")
            .get(&hash)
            .is_some_and(|announced| announced.elapsed() < ANNOUNCEMENT_LIFETIME)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(index: usize) -> block::Hash {
        let mut bytes = [0; 32];
        bytes[..std::mem::size_of::<usize>()].copy_from_slice(&index.to_le_bytes());
        block::Hash(bytes)
    }

    #[tokio::test(start_paused = true)]
    async fn announcements_are_bounded_and_keep_the_newest() {
        const EXCESS: usize = 50;
        let announced = AnnouncedBlocks::default();
        // One hash per insertion, so the oldest are evicted one at a time.
        for index in 0..ANNOUNCED_BLOCKS_LIMIT + EXCESS {
            announced.record([hash(index)]);
        }
        assert_eq!(
            announced
                .0
                .lock()
                .expect("announced block mutex is never poisoned")
                .len(),
            ANNOUNCED_BLOCKS_LIMIT
        );
        assert!((0..EXCESS).all(|index| !announced.announced(hash(index))));
        assert!(
            (EXCESS..ANNOUNCED_BLOCKS_LIMIT + EXCESS).all(|index| announced.announced(hash(index)))
        );
        // A single oversized inv is bounded the same way.
        let announced = AnnouncedBlocks::default();
        announced.record((0..ANNOUNCED_BLOCKS_LIMIT + EXCESS).map(hash));
        assert!(!announced.announced(hash(0)));
        assert!(announced.announced(hash(ANNOUNCED_BLOCKS_LIMIT + EXCESS - 1)));
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

    /// A maximum-sized message cannot leave a maximum-sized allocation behind, including
    /// when hashes repeat within the same message rather than across separate calls.
    #[tokio::test(start_paused = true)]
    async fn maximum_inventory_keeps_allocation_bounded() {
        use crate::protocol::external::InventoryHash;
        use zakura_chain::serialization::TrustedPreallocate;

        let limit = usize::try_from(InventoryHash::max_allocation())
            .expect("the inventory limit fits in usize");
        for distinct in [limit, limit / 2, ANNOUNCED_BLOCKS_LIMIT] {
            let announced = AnnouncedBlocks::default();
            announced.record((0..limit).map(|index| hash(index % distinct)));
            let entries = announced
                .0
                .lock()
                .expect("announced block mutex is never poisoned");
            assert_eq!(entries.len(), ANNOUNCED_BLOCKS_LIMIT);
            assert!(
                entries.capacity() <= 2 * ANNOUNCED_BLOCKS_LIMIT,
                "a single inventory retained an oversized allocation: {}",
                entries.capacity()
            );
            let newest: Vec<_> = (limit - ANNOUNCED_BLOCKS_LIMIT..limit)
                .map(|index| hash(index % distinct))
                .collect();
            assert_eq!(entries.keys().copied().collect::<Vec<_>>(), newest);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn refreshing_a_full_cache_preserves_other_hashes_and_renews_expiry() {
        let announced = AnnouncedBlocks::default();
        announced.record((0..ANNOUNCED_BLOCKS_LIMIT).map(hash));
        tokio::time::advance(ANNOUNCEMENT_LIFETIME / 2).await;
        announced.record([hash(0)]);
        assert!((0..ANNOUNCED_BLOCKS_LIMIT).all(|index| announced.announced(hash(index))));
        announced.record([hash(ANNOUNCED_BLOCKS_LIMIT)]);
        assert!(announced.announced(hash(0)));
        assert!(!announced.announced(hash(1)));
        tokio::time::advance(ANNOUNCEMENT_LIFETIME / 2).await;
        assert!(announced.announced(hash(0)));
        assert!(!announced.announced(hash(2)));
    }

    #[tokio::test(start_paused = true)]
    async fn announcements_expire() {
        let announced = AnnouncedBlocks::default();
        announced.record([hash(3)]);
        tokio::time::advance(ANNOUNCEMENT_LIFETIME - Duration::from_millis(1)).await;
        assert!(announced.announced(hash(3)));
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(!announced.announced(hash(3)));
    }

    #[tokio::test(start_paused = true)]
    async fn forgotten_announcements_are_not_reported() {
        let announced = AnnouncedBlocks::default();
        announced.record([hash(4), hash(5)]);
        announced.forget([hash(4)]);
        assert!(!announced.announced(hash(4)));
        assert!(announced.announced(hash(5)));
    }
}
