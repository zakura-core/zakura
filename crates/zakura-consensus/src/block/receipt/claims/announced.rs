//! The first legacy `inv` seen for each block, kept briefly so its body can take that order.

use std::{collections::HashMap, time::Duration};

use tokio::time::Instant;
use zakura_chain::block;

use super::{super::order, CLAIM_BODY_DEADLINE};

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

/// Receipt order, time and highest verified tip at the block's first legacy announcement.
#[derive(Copy, Clone, Debug)]
pub(super) struct Announcement {
    pub(super) order: u64,
    pub(super) seen_at: Instant,
    pub(super) tip_at_announcement: block::Height,
}

/// First announcements by block hash, bounded by [`MAX_ANNOUNCED_BLOCKS`].
///
/// Announcements carry no height, so they expire by age rather than by the verified tip.
#[derive(Debug, Default)]
pub(super) struct Announcements(HashMap<block::Hash, Announcement>);

impl Announcements {
    /// Records `hash` as first announced at `now`, unless it already has an announcement.
    ///
    /// Returns `false` if the store is full of unexpired announcements and refused `hash`.
    pub(super) fn note(
        &mut self,
        hash: block::Hash,
        tip_at_announcement: block::Height,
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
            },
        );
        true
    }

    /// The first announcement of `hash`, if one is held.
    pub(super) fn get(&self, hash: block::Hash) -> Option<Announcement> {
        self.0.get(&hash).copied()
    }
}
