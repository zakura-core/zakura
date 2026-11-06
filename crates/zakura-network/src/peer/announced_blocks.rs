//! Track the blocks a connection's peer has announced to us.
//!
//! Block gossip skips a peer that already told us about the block: it has the block, so it
//! would never request it, and its advert would occupy a window entry until
//! [`BLOCK_REQUEST_WAIT`](super::block_gossip::BLOCK_REQUEST_WAIT) expires.
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
    /// Records that the peer announced `hashes`, evicting the oldest beyond the limit.
    pub(crate) fn record(&self, hashes: impl IntoIterator<Item = block::Hash>) {
        let now = Instant::now();
        let mut announced = self
            .0
            .lock()
            .expect("announced block mutex is never poisoned");
        for hash in hashes {
            // Re-inserting at the end keeps the map ordered oldest first.
            announced.shift_remove(&hash);
            announced.insert(hash, now);
        }
        let excess = announced.len().saturating_sub(ANNOUNCED_BLOCKS_LIMIT);
        announced.drain(..excess);
    }

    /// Forgets `hashes`, which the peer told us it does not have after all.
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
