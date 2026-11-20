//! Legacy reservations, kept independently until the verifier can check their eligibility.
//!
//! A block is announced by its first `inv`, or by the start of a `block` message carrying it,
//! whichever reserved the earlier order.

use std::{collections::HashMap, time::Duration};

use tokio::time::Instant;
use zakura_chain::block;
use zakura_node_services::block_receipt::BlockReservation;

use super::{super::order, in_claim_window, resolution::ClaimSource, CLAIM_BODY_DEADLINE};

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

/// One legacy sighting, with its own order, deadline origin and height eligibility.
#[derive(Copy, Clone, Debug)]
pub(super) struct Announcement {
    pub(super) order: u64,
    pub(super) seen_at: Instant,
    height: AnnouncementHeight,
}

/// An inventory hash has no height until the verifier sees a body. For a block message,
/// admission already checked the decoded coinbase height against the starting tip and floor.
#[derive(Copy, Clone, Debug)]
enum AnnouncementHeight {
    InventoryAtTip(block::Height),
    BlockAtHeight(block::Height),
}

impl Announcement {
    pub(super) fn source_at(&self, height: block::Height) -> Option<ClaimSource> {
        match self.height {
            AnnouncementHeight::InventoryAtTip(tip) if in_claim_window(height, tip) => {
                Some(ClaimSource::Inv)
            }
            AnnouncementHeight::BlockAtHeight(actual) if actual == height => {
                Some(ClaimSource::BlockMessage)
            }
            _ => None,
        }
    }
}

/// The first inventory announcement and each admitted block delivery have independent deadlines.
#[derive(Debug, Default)]
struct Candidates {
    inv: Option<Announcement>,
    blocks: Vec<Announcement>,
}

/// Both the number of distinct hashes and the total number of block-message candidates are
/// bounded by `MAX_ANNOUNCED_BLOCKS`. Inventory sightings never consume the delivery budget.
#[derive(Debug, Default)]
pub(super) struct Announcements(HashMap<block::Hash, Candidates>);

impl Announcements {
    /// Records the first inventory sighting for this hash. Duplicates do not renew its deadline.
    pub(super) fn note(
        &mut self,
        hash: block::Hash,
        tip_at_announcement: block::Height,
        now: Instant,
    ) -> bool {
        if self
            .0
            .get(&hash)
            .is_some_and(|candidates| candidates.inv.is_some())
        {
            return true;
        }
        self.prune(now);
        let Some(candidates) = self.admit(hash) else {
            return false;
        };
        candidates.inv = Some(Announcement {
            order: order::next(),
            seen_at: now,
            height: AnnouncementHeight::InventoryAtTip(tip_at_announcement),
        });
        true
    }

    /// Retains the candidate until receipt. Even a later delivery may still be eligible after an
    /// earlier one's deadline, or while an earlier claim is voided by a local mining submission.
    pub(super) fn note_block_message(
        &mut self,
        height: block::Height,
        reservation: BlockReservation,
        now: Instant,
    ) -> bool {
        if self.prune(now) >= MAX_ANNOUNCED_BLOCKS {
            return false;
        }
        let Some(candidates) = self.admit(reservation.hash) else {
            return false;
        };
        candidates.blocks.push(Announcement {
            order: reservation.order,
            seen_at: reservation.started_at,
            height: AnnouncementHeight::BlockAtHeight(height),
        });
        true
    }

    /// Drops old sightings individually and counts the remaining delivery candidates.
    fn prune(&mut self, now: Instant) -> usize {
        let retained = |candidate: &Announcement| {
            now.saturating_duration_since(candidate.seen_at) <= ANNOUNCEMENT_LIFETIME
        };
        let mut block_count = 0;
        self.0.retain(|_, candidates| {
            candidates.inv = candidates.inv.filter(retained);
            candidates.blocks.retain(retained);
            block_count += candidates.blocks.len();
            candidates.inv.is_some() || !candidates.blocks.is_empty()
        });
        block_count
    }

    fn admit(&mut self, hash: block::Hash) -> Option<&mut Candidates> {
        if self.0.len() >= MAX_ANNOUNCED_BLOCKS {
            return self.0.get_mut(&hash);
        }
        Some(self.0.entry(hash).or_default())
    }

    pub(super) fn get(&self, hash: block::Hash) -> impl Iterator<Item = &Announcement> {
        self.0
            .get(&hash)
            .into_iter()
            .flat_map(|candidates| candidates.inv.iter().chain(&candidates.blocks))
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
