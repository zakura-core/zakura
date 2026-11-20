//! The first legacy sighting of each block, kept briefly so its body can take that order.
//!
//! A legacy peer shows the node a block by sending a single-block `inv` for it, or by starting
//! a `block` message that carries it. Whichever comes first is the block's one legacy sighting
//! until that sighting expires.

use std::{collections::HashMap, time::Duration};

use tokio::time::Instant;
use zakura_chain::block;

use super::{super::order, resolution::ClaimSource, CLAIM_BODY_DEADLINE};

/// The most announced blocks the store holds.
///
/// Chosen so that peers cannot fill the store at the default connection limits. A legacy
/// connection passes at most 3 blocks per 2 seconds to the store and an entry lasts
/// [`ANNOUNCEMENT_LIFETIME`], 4 seconds, so one connection holds at most 9 unexpired entries:
/// three windows' worth, when the first was filled exactly 4 seconds before the last. The
/// default limits allow 450 connections at once. Every new connection has a fresh allowance,
/// and within one entry's lifetime at most about 73 can be opened: 32 inbound, which are
/// accepted in bursts of 16, and 41 outbound, which are dialed at most ten a second. So at
/// most 523 connections hold 4,707 entries. The bound stops holding once the configured
/// connection limit passes about 480, which is a `peerset_initial_target_size` of about 107
/// against the default 100. A `zakurad` test recomputes this from both crates' numbers, so it
/// fails if the allowance, the connection limits, this size or the lifetime change apart.
///
/// Honest announcements arrive about one block per connection per block interval, so this is
/// far above honest use. A full store refuses new hashes instead of dropping old ones, so even
/// where it can be filled, flooding cannot remove an announcement that is already in force. A
/// refused block is ranked by body arrival.
pub(in crate::block::receipt) const MAX_ANNOUNCED_BLOCKS: usize = 5_000;

/// How long a sighting counts.
///
/// An older sighting is treated as absent wherever it is read, whether or not it has been
/// removed from the store yet. So what a block can claim never depends on which other blocks
/// happened to be sighted in the meantime.
///
/// Twice the body deadline, so a body that arrives after its deadline is still counted as
/// expired rather than unclaimed.
pub(super) const ANNOUNCEMENT_LIFETIME: Duration = CLAIM_BODY_DEADLINE.saturating_mul(2);

/// Receipt order, time and highest verified tip at the block's first legacy sighting, and
/// which kind of message made it.
#[derive(Copy, Clone, Debug)]
pub(super) struct Announcement {
    pub(super) order: u64,
    pub(super) seen_at: Instant,
    pub(super) tip_at_announcement: block::Height,
    pub(super) source: ClaimSource,
}

impl Announcement {
    /// Whether this sighting still counts at `now`: it is no older than
    /// [`ANNOUNCEMENT_LIFETIME`].
    fn counts_at(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.seen_at) <= ANNOUNCEMENT_LIFETIME
    }
}

/// First legacy sightings by block hash, bounded by [`MAX_ANNOUNCED_BLOCKS`].
///
/// A sighting carries no height, so it expires by age rather than by the verified tip.
#[derive(Debug, Default)]
pub(super) struct Announcements(HashMap<block::Hash, Announcement>);

impl Announcements {
    /// Records `hash` as first sighted at `now` through `source`, unless it has a sighting
    /// that still counts. Such a sighting is never renewed or moved, whichever kind of message
    /// repeats it, so a peer cannot keep a withheld block's place fresh by showing it again.
    /// Once the sighting has expired, the block is sighted afresh, with a new and later order.
    ///
    /// Returns `false` if the store is full of unexpired sightings and refused `hash`.
    pub(super) fn note(
        &mut self,
        hash: block::Hash,
        tip_at_announcement: block::Height,
        source: ClaimSource,
        now: Instant,
    ) -> bool {
        if self.get(hash, now).is_some() {
            return true;
        }
        self.0.retain(|_, announced| announced.counts_at(now));
        if self.0.len() >= MAX_ANNOUNCED_BLOCKS {
            return false;
        }
        let order = order::next();
        tracing::info!(
            "LAB claim_drawn unix_ms={} hash={} source={} order={} apply_ms=0",
            super::lab_unix_ms(),
            hash,
            source.label(),
            order,
        );
        self.0.insert(
            hash,
            Announcement {
                order,
                seen_at: now,
                tip_at_announcement,
                source,
            },
        );
        true
    }

    /// The legacy sighting of `hash` that counts at `now`, if there is one. An expired
    /// sighting is not returned, even while it is still in the store.
    pub(super) fn get(&self, hash: block::Hash, now: Instant) -> Option<Announcement> {
        self.0
            .get(&hash)
            .copied()
            .filter(|announced| announced.counts_at(now))
    }

    /// The order of the sighting stored for `hash`, expired or not.
    #[cfg(test)]
    pub(super) fn stored_order(&self, hash: block::Hash) -> Option<u64> {
        self.0.get(&hash).map(|announced| announced.order)
    }

    /// Discards a legacy sighting saved while this hash's header apply was pending.
    pub(super) fn discard_header_fallback(&mut self, hash: block::Hash) {
        self.0.remove(&hash);
    }
}
