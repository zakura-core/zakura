//! Claims on receipt order made when a block is announced, and the deadline for its body.
//!
//! A block is announced when native header sync admits its header, when a legacy peer first
//! sends an `inv` for it, or when the block header at the start of a legacy `block` message
//! carrying it arrives. The `announced` child holds the legacy sightings.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use tokio::time::Instant;
use zakura_chain::block::{self, HeightDiff};

use super::order;

mod announced;
mod resolution;

#[cfg(test)]
pub(super) use resolution::size_label;
pub(super) use resolution::{ClaimOutcome, ClaimSource};

use announced::Announcements;
#[cfg(test)]
pub(super) use announced::MAX_ANNOUNCED_BLOCKS;

/// How long after its claim a block's body still takes the claimed order, whatever its size.
///
/// Inside the deadline a withheld body looks the same as a slow honest delivery, so this is the
/// most a miner can gain by sitting on an announced block and still winning an equal-work tie.
/// The deadline is the same for every size because scaling it with size would not lower that
/// bound: a withholder pads its block until it gets the largest deadline. A size term would only
/// shorten the deadline for small blocks, which are nearly all blocks, and over several relay
/// hops their delivery time is mostly round trips, not transfer. The value is an estimate of
/// honest delivery, to be revisited against the `sync.block.announcement_claim.age.seconds`
/// histogram.
pub(in crate::block) const CLAIM_BODY_DEADLINE: Duration = Duration::from_secs(2);

/// How far above the verified tip a block can be and still get a claim.
///
/// Only blocks racing at the tip need one. Blocks further ahead belong to catch-up, and their
/// bodies cannot arrive inside a deadline anyway.
const MAX_CLAIM_HEIGHT_ABOVE_TIP: HeightDiff = 2;

/// The most claims [`AnnouncementClaims`] holds, counting those still being applied.
///
/// A full table refuses new claims instead of dropping old ones, so flooding it cannot remove a
/// claim that is already in force. A refused block is ranked by body arrival.
pub(super) const MAX_ANNOUNCEMENT_CLAIMS: usize = 64;

/// How long a verified mined block's record is kept.
///
/// A record only voids claims with an earlier order than its own. Each of those was seen before
/// the record was made, so it has expired once the record is older than
/// [`CLAIM_BODY_DEADLINE`], and an expired claim is ranked by arrival just as a voided one is.
/// The record is kept for twice that, so a body whose arrival time was read just inside a
/// claim's deadline still finds the record if it looks the claim up as much as one more
/// deadline later.
pub(super) const MINED_RECORD_LIFETIME: Duration = CLAIM_BODY_DEADLINE.saturating_mul(2);

/// A place in receipt order reserved for a block when its header arrived.
#[derive(Copy, Clone, Debug)]
struct Claim {
    height: block::Height,
    order: u64,
    seen_at: Instant,
    /// The header applies covering this block that are still running.
    applies: usize,
    /// Whether the header chain has accepted the header. Until then, the claim lasts only while
    /// an apply covering it is running.
    accepted: bool,
}

/// A block this node mined, and the order its submission took.
///
/// A node does not abandon a block it mined because a rival's header arrived first, so a claim
/// at the same height with an earlier order is not honoured while this is held.
#[derive(Debug)]
struct MinedBlock {
    height: block::Height,
    order: u64,
    /// When the first request for this block was recorded, which is after `order` was drawn.
    marked_at: Instant,
    /// The requests still verifying this block.
    pending: usize,
    /// Whether a request for this block has verified it.
    verified: bool,
}

#[derive(Debug)]
struct ClaimTable {
    /// The highest verified tip height seen. Claims below it are dropped, and no claim is made
    /// at or below it, so a dropped hash can never be claimed again.
    ///
    /// It never goes down. After a rollback below it, blocks at or below it get no claim and are
    /// ranked by arrival until the tip passes it again.
    floor: block::Height,
    by_hash: HashMap<block::Hash, Claim>,
    /// The first legacy sighting of each block. Its height is only known once the body arrives.
    announced: Announcements,
    /// Mined blocks that are verifying or verified, by hash. Failed ones are removed, and
    /// verified ones are retired by [`ClaimTable::retire_mined`].
    mined: HashMap<block::Hash, MinedBlock>,
}

impl Default for ClaimTable {
    fn default() -> Self {
        Self {
            floor: block::Height::MIN,
            by_hash: HashMap::new(),
            announced: Announcements::default(),
            mined: HashMap::new(),
        }
    }
}

/// Whether a block was close enough above the verified tip when first announced.
fn in_claim_window(height: block::Height, tip_at_announcement: block::Height) -> bool {
    (1..=MAX_CLAIM_HEIGHT_ABOVE_TIP).contains(&(height - tip_at_announcement))
}

impl ClaimTable {
    /// Raises the floor to `verified_tip`, dropping the claims below it.
    fn raise_floor(&mut self, verified_tip: block::Height) {
        self.floor = self.floor.max(verified_tip);
        let floor = self.floor;
        self.by_hash.retain(|hash, claim| {
            if claim.height < floor {
                self.announced.discard_header_fallback(*hash);
                return false;
            }
            true
        });
    }

    /// Drops the mined blocks recorded more than [`MINED_RECORD_LIFETIME`] before `now` that
    /// no request is still verifying. By then no claim they could void is still in force.
    fn retire_mined(&mut self, now: Instant) {
        self.mined.retain(|_, mined| {
            mined.pending > 0
                || now.saturating_duration_since(mined.marked_at) <= MINED_RECORD_LIFETIME
        });
    }
}

/// Receipt-order claims for blocks announced before their bodies arrived.
///
/// Header sync creates claims with [`AnnouncementClaims::begin`], legacy block `inv`s with
/// [`AnnouncementClaims::note_announcement`], legacy `block` messages with
/// [`AnnouncementClaims::note_block_header`], and the block verifier reads them by block hash,
/// so every body transport follows the same rule. The first sighting sets the order and
/// deadline for as long as it is held. A provisional header releases its place if every
/// covering apply fails, and a legacy sighting stops counting once it expires.
#[derive(Clone, Debug, Default)]
pub struct AnnouncementClaims(Arc<Mutex<ClaimTable>>);

/// The claims a header apply covers while it runs.
///
/// Bodies already take the claimed orders. [`PendingClaims::commit`] keeps the claims once the
/// header chain accepts the headers. Dropping this removes a claim that no accepted or running
/// apply covers.
#[derive(Debug)]
#[must_use = "claims are removed unless an apply covering them is committed"]
pub struct PendingClaims {
    claims: AnnouncementClaims,
    covered: Vec<block::Hash>,
}

impl AnnouncementClaims {
    /// Reserves a place in receipt order for each of `headers` that is one or two blocks above
    /// the highest verified tip seen, including `verified_tip`, and has no claim yet. A header
    /// whose block has a legacy sighting that still counts reserves nothing: that sighting
    /// already set the block's order and deadline.
    ///
    /// Call this when a header apply starts and commit the result once the header chain has
    /// accepted the headers. The order and time are taken now, and bodies take them from now
    /// on, so neither a rival body nor the block's own body arriving during the apply ranks it
    /// behind a header that arrived later. A header already reserved by a running apply keeps
    /// that earlier reservation, and this apply covers it too.
    pub fn begin(
        &self,
        headers: impl IntoIterator<Item = (block::Hash, block::Height)>,
        verified_tip: block::Height,
    ) -> PendingClaims {
        self.begin_at(headers, verified_tip, Instant::now())
    }

    pub(super) fn begin_at(
        &self,
        headers: impl IntoIterator<Item = (block::Hash, block::Height)>,
        verified_tip: block::Height,
        now: Instant,
    ) -> PendingClaims {
        let mut table = self.table();
        table.raise_floor(verified_tip);

        let mut covered = Vec::new();
        let mut refused: u64 = 0;
        for (hash, height) in headers {
            if !in_claim_window(height, table.floor) {
                continue;
            }
            if let Some(claim) = table.by_hash.get_mut(&hash) {
                if !claim.accepted {
                    claim.applies = claim.applies.saturating_add(1);
                    covered.push(hash);
                }
                continue;
            }
            // A legacy sighting that still counts already set this hash's order and deadline.
            if table.announced.get(hash, now).is_some() {
                continue;
            }
            if table.by_hash.len() >= MAX_ANNOUNCEMENT_CLAIMS {
                refused = refused.saturating_add(1);
                continue;
            }
            let claim = Claim {
                height,
                order: order::next(),
                seen_at: now,
                applies: 1,
                accepted: false,
            };
            table.by_hash.insert(hash, claim);
            covered.push(hash);
        }
        drop(table);

        if refused > 0 {
            metrics::counter!("sync.block.announcement_claim.refused", "source" => "header")
                .increment(refused);
        }
        PendingClaims {
            claims: self.clone(),
            covered,
        }
    }

    /// Records a legacy `inv` for `hash` as the block's legacy sighting, if it has none yet, so
    /// its body can take the order it was announced in. Raises the verified tip to
    /// `verified_tip` as [`AnnouncementClaims::begin`] does.
    ///
    /// An accepted header needs no legacy sighting. One made during a pending header apply is a
    /// fallback if every apply fails. A block keeps its legacy sighting until the sighting
    /// expires, so announcing or sending it again before then never renews its claim. The body
    /// takes the sighting's order only if it arrives within the deadline, its height was one or
    /// two above the tip at the sighting, and the highest tip seen since then has not passed
    /// that height.
    ///
    /// No header check is needed here: a valid block hash cannot exist before its proof of work,
    /// and the order only ranks a block after it is fully verified.
    pub fn note_announcement(&self, hash: block::Hash, verified_tip: block::Height) {
        self.note_announcement_at(hash, verified_tip, Instant::now())
    }

    pub(super) fn note_announcement_at(
        &self,
        hash: block::Hash,
        verified_tip: block::Height,
        now: Instant,
    ) {
        self.note_sighting(hash, verified_tip, ClaimSource::Inv, now)
    }

    /// Records that the block header at the start of a legacy `block` message arrived and
    /// hashes to `hash`, as the block's legacy sighting, if it has none yet.
    ///
    /// This is the same sighting an `inv` makes, under the same rules, taken before the block's
    /// transactions arrive. A large block therefore ranks ahead of a rival announced while it
    /// is still being received, and the place is in force at once, whichever copy of the block
    /// reaches the verifier first.
    ///
    /// `hash` is read from header bytes the node already holds, so, as with an `inv`, a place
    /// cannot be taken for a block that is mined later.
    pub fn note_block_header(&self, hash: block::Hash, verified_tip: block::Height) {
        self.note_block_header_at(hash, verified_tip, Instant::now())
    }

    pub(super) fn note_block_header_at(
        &self,
        hash: block::Hash,
        verified_tip: block::Height,
        now: Instant,
    ) {
        self.note_sighting(hash, verified_tip, ClaimSource::BlockMessage, now)
    }

    /// Records the legacy sighting of `hash` made through `source` at `now`.
    fn note_sighting(
        &self,
        hash: block::Hash,
        verified_tip: block::Height,
        source: ClaimSource,
        now: Instant,
    ) {
        let mut table = self.table();
        table.raise_floor(verified_tip);
        if table.by_hash.get(&hash).is_some_and(|claim| claim.accepted) {
            return;
        }
        let tip_at_announcement = table.floor;
        let noted = table.announced.note(hash, tip_at_announcement, source, now);
        drop(table);

        if !noted {
            metrics::counter!(
                "sync.block.announcement_claim.refused",
                "source" => source.label(),
            )
            .increment(1);
        }
    }

    /// Records that the locally mined block `hash` at `height` took `order`, so that a rival's
    /// earlier claim at `height` is not honoured.
    ///
    /// The record lasts while the returned guard, or any other guard for the same block, is
    /// alive. Once one of them is kept, it outlives them until it is older than
    /// [`MINED_RECORD_LIFETIME`], and is dropped the next time a mined block is recorded. A
    /// block recorded again after that gets a new record, with the new `order`.
    ///
    /// `order` must already be drawn. That is what dates every claim with an earlier order
    /// before this record.
    pub(super) fn mark_mined(
        &self,
        hash: block::Hash,
        height: block::Height,
        order: u64,
    ) -> MinedGuard {
        self.mark_mined_at(hash, height, order, Instant::now())
    }

    pub(super) fn mark_mined_at(
        &self,
        hash: block::Hash,
        height: block::Height,
        order: u64,
        now: Instant,
    ) -> MinedGuard {
        let mut table = self.table();
        table.retire_mined(now);
        let mined = table.mined.entry(hash).or_insert(MinedBlock {
            height,
            order,
            marked_at: now,
            pending: 0,
            verified: false,
        });
        mined.pending = mined.pending.saturating_add(1);
        MinedGuard {
            claims: self.clone(),
            hash,
            height,
        }
    }

    /// The most legacy sightings held at once. A block first sighted while that many are held
    /// is ranked by body arrival.
    #[cfg(any(test, feature = "proptest-impl"))]
    pub const MAX_LEGACY_SIGHTINGS: usize = announced::MAX_ANNOUNCED_BLOCKS;

    /// How long a legacy sighting counts towards [`Self::MAX_LEGACY_SIGHTINGS`].
    #[cfg(any(test, feature = "proptest-impl"))]
    pub const LEGACY_SIGHTING_LIFETIME: Duration = announced::ANNOUNCEMENT_LIFETIME;

    /// Whether a claim is held for `hash`.
    #[cfg(any(test, feature = "proptest-impl"))]
    pub fn is_claimed(&self, hash: block::Hash) -> bool {
        self.table().by_hash.contains_key(&hash)
    }

    /// The order claimed for `hash`, if a claim is held.
    #[cfg(test)]
    pub(super) fn claimed_order(&self, hash: block::Hash) -> Option<u64> {
        self.table().by_hash.get(&hash).map(|claim| claim.order)
    }

    /// The order of the legacy sighting stored for `hash`, expired or not.
    #[cfg(test)]
    pub(super) fn announced_order(&self, hash: block::Hash) -> Option<u64> {
        self.table().announced.stored_order(hash)
    }

    /// How many mined blocks have a record.
    #[cfg(test)]
    pub(super) fn mined_blocks(&self) -> usize {
        self.table().mined.len()
    }

    fn table(&self) -> MutexGuard<'_, ClaimTable> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl PendingClaims {
    /// Keeps the covered claims, because the header chain accepted their headers.
    pub fn commit(mut self) {
        self.finish(true);
    }

    fn finish(&mut self, accepted: bool) {
        if self.covered.is_empty() {
            return;
        }
        let mut table = self.claims.table();
        for hash in std::mem::take(&mut self.covered) {
            // The floor may have dropped the claim during the apply.
            let Some(claim) = table.by_hash.get_mut(&hash) else {
                continue;
            };
            claim.applies = claim.applies.saturating_sub(1);
            claim.accepted |= accepted;
            if accepted {
                table.announced.discard_header_fallback(hash);
            } else if claim.applies == 0 && !claim.accepted {
                table.by_hash.remove(&hash);
            }
        }
    }
}

impl Drop for PendingClaims {
    fn drop(&mut self) {
        self.finish(false);
    }
}

/// Keeps a locally mined block's record while its verification is active.
///
/// Dropping the last guard for a block removes its record, unless one of them was kept. A
/// record is never dropped while a guard for it is alive, so a guard always acts on the record
/// it was given.
pub(in crate::block) struct MinedGuard {
    claims: AnnouncementClaims,
    hash: block::Hash,
    /// The height of this submission's body. Other bodies can share its header hash.
    height: block::Height,
}

impl MinedGuard {
    /// Keeps the record after this guard is gone, because the mined block verified.
    pub(in crate::block) fn keep(self) {
        let mut table = self.claims.table();
        if let Some(mined) = table.mined.get_mut(&self.hash) {
            mined.verified = true;
            mined.height = self.height;
        }
    }
}

impl Drop for MinedGuard {
    fn drop(&mut self) {
        let mut table = self.claims.table();
        let Some(mined) = table.mined.get_mut(&self.hash) else {
            return;
        };
        mined.pending = mined.pending.saturating_sub(1);
        if mined.pending == 0 && !mined.verified {
            table.mined.remove(&self.hash);
        }
    }
}
