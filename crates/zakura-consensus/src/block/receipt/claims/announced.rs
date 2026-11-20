//! The first legacy sighting of each block, kept briefly so its body can take that order.
//!
//! A legacy peer shows the node a block by sending a single-block `inv` for it, or by starting
//! a `block` message that carries it. Whichever comes first is the block's one legacy sighting.

use std::{collections::HashMap, time::Duration};

use tokio::time::Instant;
use zakura_chain::block;

use super::{super::order, resolution::ClaimSource, CLAIM_BODY_DEADLINE};

/// The most announced blocks the store holds.
///
/// Honest announcements arrive about one block per connection per block interval, and each
/// entry lasts [`ANNOUNCEMENT_LIFETIME`], so this is far above honest use. A full store refuses
/// new hashes instead of dropping old ones, so flooding it cannot remove an announcement that is
/// already in force. A refused block is ranked by body arrival.
pub(in crate::block::receipt) const MAX_ANNOUNCED_BLOCKS: usize = 512;

/// How long an announcement is kept.
///
/// Twice the body deadline, so a body that arrives after its deadline is still counted as
/// expired rather than unclaimed.
const ANNOUNCEMENT_LIFETIME: Duration = CLAIM_BODY_DEADLINE.saturating_mul(2);

/// Receipt order, time and highest verified tip at the block's first legacy sighting, and
/// which kind of message made it.
#[derive(Copy, Clone, Debug)]
pub(super) struct Announcement {
    pub(super) order: u64,
    pub(super) seen_at: Instant,
    pub(super) tip_at_announcement: block::Height,
    pub(super) source: ClaimSource,
}

/// First legacy sightings by block hash, bounded by [`MAX_ANNOUNCED_BLOCKS`].
///
/// A sighting carries no height, so it expires by age rather than by the verified tip.
#[derive(Debug, Default)]
pub(super) struct Announcements(HashMap<block::Hash, Announcement>);

impl Announcements {
    /// Records `hash` as first sighted at `now` through `source`, unless it already has a
    /// sighting. A held sighting is never renewed or moved, whichever kind of message repeats
    /// it, so a peer cannot keep a withheld block's place fresh by showing it again.
    ///
    /// Returns `false` if the store is full of unexpired sightings and refused `hash`.
    pub(super) fn note(
        &mut self,
        hash: block::Hash,
        tip_at_announcement: block::Height,
        source: ClaimSource,
        now: Instant,
    ) -> bool {
        if self.0.contains_key(&hash) {
            return true;
        }
        self.0.retain(|_, announced| {
            now.saturating_duration_since(announced.seen_at) <= ANNOUNCEMENT_LIFETIME
        });
        if self.0.len() >= MAX_ANNOUNCED_BLOCKS {
            return false;
        }
        self.0.insert(
            hash,
            Announcement {
                order: order::next(),
                seen_at: now,
                tip_at_announcement,
                source,
            },
        );
        true
    }

    /// The first legacy sighting of `hash`, if one is held.
    pub(super) fn get(&self, hash: block::Hash) -> Option<Announcement> {
        self.0.get(&hash).copied()
    }

    /// Discards a legacy sighting saved as a fallback for this earlier header reservation.
    pub(super) fn discard_after_header(&mut self, hash: block::Hash, header_order: u64) {
        if let std::collections::hash_map::Entry::Occupied(entry) = self.0.entry(hash) {
            if entry.get().order > header_order {
                entry.remove();
            }
        }
    }
}
