//! Tracks full-serving peers that consistently fail emergency `FindBlocks`
//! discovery, so the peer set can disconnect them as a last-ditch recovery.
//!
//! A peer returning a single empty response may just be syncing itself; a peer
//! that does so repeatedly stalls the syncer by forcing retries to others. The
//! counter is scoped to one connection generation and locator head. Any useful
//! response from any peer clears all counts.

use std::collections::HashMap;

use zakura_chain::block;

/// Rate-limited empty or failed emergency `FindBlocks` responses tolerated
/// before the peer set disconnects a peer.
pub(super) const FIND_RESPONSE_STALL_THRESHOLD: usize = 3;

#[derive(Default)]
pub(super) struct FindResponseStallTracker {
    counts: HashMap<(u64, block::Hash), usize>,
}

impl FindResponseStallTracker {
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Records a stall for a connection generation and locator. Returns `true` once it reaches
    /// [`FIND_RESPONSE_STALL_THRESHOLD`] — the caller must then disconnect it.
    /// On threshold the entry is removed, so a reconnected peer starts fresh.
    pub(super) fn record_stall(&mut self, generation: u64, locator: block::Hash) -> bool {
        let key = (generation, locator);
        let count = self.counts.entry(key).or_default();
        *count += 1;

        if *count >= FIND_RESPONSE_STALL_THRESHOLD {
            self.counts.remove(&key);
            true
        } else {
            false
        }
    }

    /// Returns the current strike count.
    pub(super) fn count(&self, generation: u64, locator: block::Hash) -> usize {
        self.counts
            .get(&(generation, locator))
            .copied()
            .unwrap_or_default()
    }

    /// Clears every strike after any useful response, locator change, or exit
    /// from emergency mode.
    pub(super) fn clear(&mut self) {
        self.counts.clear();
    }

    /// Clears tracking for a disconnected connection generation.
    pub(super) fn clear_generation(&mut self, generation: u64) {
        self.counts
            .retain(|(tracked_generation, _), _| *tracked_generation != generation);
    }
}

#[cfg(test)]
mod tests;
