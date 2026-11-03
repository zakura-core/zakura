//! Tracks peers that consistently return empty or failed `FindBlocks` or
//! `FindHeaders` responses, so the peer set can disconnect them.
//!
//! A peer returning a single empty response may just be syncing itself; a peer
//! that does so repeatedly stalls the syncer by forcing retries to others. The
//! counter is per-peer and resets on any useful (non-empty) response.
//!
//! Only applies to `FindBlocks` and `FindHeaders`. An empty response to
//! `BlocksByHash`/`TransactionsById` is a legitimate "I don't have this
//! inventory" answer, so those don't feed the tracker.

use std::collections::HashMap;

use crate::PeerSocketAddr;

/// Consecutive empty or failed `FindBlocks`/`FindHeaders` responses tolerated
/// before the peer set disconnects a peer.
pub(super) const FIND_RESPONSE_STALL_THRESHOLD: usize = 3;

#[derive(Default)]
pub(super) struct FindResponseStallTracker {
    counts: HashMap<PeerSocketAddr, usize>,
}

impl FindResponseStallTracker {
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Records a stall for `addr`. Returns `true` once the peer reaches
    /// [`FIND_RESPONSE_STALL_THRESHOLD`] — the caller must then disconnect it.
    /// On threshold the entry is removed, so a reconnected peer starts fresh.
    pub(super) fn record_stall(&mut self, addr: PeerSocketAddr) -> bool {
        let count = self.counts.entry(addr).or_default();
        *count += 1;

        if *count >= FIND_RESPONSE_STALL_THRESHOLD {
            self.counts.remove(&addr);
            true
        } else {
            false
        }
    }

    /// Clears tracking for a peer that sent a useful response or disconnected.
    pub(super) fn clear(&mut self, addr: PeerSocketAddr) {
        self.counts.remove(&addr);
    }

    /// Keeps tracking only for peers that `is_live` accepts.
    pub(super) fn retain(&mut self, is_live: impl Fn(&PeerSocketAddr) -> bool) {
        self.counts.retain(|addr, _| is_live(addr));
    }

    /// Returns `true` if no peer has a pending stall count.
    pub(super) fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }

    /// Returns the number of peers with a pending stall count.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.counts.len()
    }
}

#[cfg(test)]
mod tests;
