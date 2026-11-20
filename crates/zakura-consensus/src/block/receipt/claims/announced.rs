//! The earliest legacy announcement seen for each block, kept briefly so its body can take that
//! order.
//!
//! A block is announced by its first `inv`, or by the start of a `block` message carrying it,
//! whichever reserved the earlier order.

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

/// Receipt order, time and highest verified tip at the block's earliest legacy announcement.
#[derive(Copy, Clone, Debug)]
pub(super) struct Announcement {
    pub(super) order: u64,
    pub(super) seen_at: Instant,
    pub(super) tip_at_announcement: block::Height,
    pub(super) source: ClaimSource,
}

/// Earliest announcements by block hash, bounded by [`MAX_ANNOUNCED_BLOCKS`].
///
/// Announcements carry no height, so they expire by age rather than by the verified tip.
#[derive(Debug, Default)]
pub(super) struct Announcements(HashMap<block::Hash, Announcement>);

impl Announcements {
    /// Records `hash` as first announced by an `inv` at `now`, unless it already has an
    /// announcement, which is necessarily earlier.
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
        self.insert(hash, now, || Announcement {
            order: order::next(),
            seen_at: now,
            tip_at_announcement,
            source: ClaimSource::Inv,
        })
    }

    /// Records that a `block` message carrying `hash` reserved `order` when it started arriving
    /// at `started_at`, with the verified tip at `tip_at_announcement`, and completed at `now`.
    ///
    /// Of this reservation and an existing announcement, the block keeps the earlier one that
    /// is still inside its deadline at `now`, so it is never ranked behind an announcement that
    /// came after its message started, and an expired claim never displaces a usable one. If
    /// neither is usable, the existing one is kept. A replaced announcement takes this tip, which
    /// was read when its new order was reserved.
    ///
    /// Taking a later but usable reservation over an expired announcement gives the block no
    /// more than a block message without that announcement would get.
    ///
    /// Returns `false` if the store is full of unexpired announcements and refused `hash`.
    pub(super) fn note_block_message(
        &mut self,
        hash: block::Hash,
        tip_at_announcement: block::Height,
        order: u64,
        started_at: Instant,
        now: Instant,
    ) -> bool {
        let usable = |seen_at| now.saturating_duration_since(seen_at) <= CLAIM_BODY_DEADLINE;
        if let Some(announced) = self.0.get_mut(&hash) {
            let replace =
                usable(started_at) && (order < announced.order || !usable(announced.seen_at));
            if replace {
                *announced = Announcement {
                    order,
                    seen_at: started_at,
                    tip_at_announcement,
                    source: ClaimSource::BlockMessage,
                };
            }
            return true;
        }
        self.insert(hash, now, || Announcement {
            order,
            seen_at: started_at,
            tip_at_announcement,
            source: ClaimSource::BlockMessage,
        })
    }

    /// Inserts the announcement `announced` makes for `hash`, after dropping expired ones,
    /// unless the store is still full.
    fn insert(
        &mut self,
        hash: block::Hash,
        now: Instant,
        announced: impl FnOnce() -> Announcement,
    ) -> bool {
        self.0.retain(|_, announced| {
            now.saturating_duration_since(announced.seen_at) <= ANNOUNCEMENT_LIFETIME
        });
        if self.0.len() >= MAX_ANNOUNCED_BLOCKS {
            return false;
        }
        self.0.insert(hash, announced());
        true
    }

    /// The earliest announcement of `hash`, if one is held.
    pub(super) fn get(&self, hash: block::Hash) -> Option<Announcement> {
        self.0.get(&hash).copied()
    }

    /// Discards an `inv` saved as a fallback for this earlier header reservation.
    pub(super) fn discard_after_header(&mut self, hash: block::Hash, header_order: u64) {
        if let std::collections::hash_map::Entry::Occupied(entry) = self.0.entry(hash) {
            if entry.get().order > header_order {
                entry.remove();
            }
        }
    }
}
