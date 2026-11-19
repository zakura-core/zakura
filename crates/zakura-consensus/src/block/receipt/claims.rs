//! Claims on receipt order made when a block is announced, and the deadline for its body.
//!
//! A block is announced when native header sync admits its header, or when a legacy peer first
//! sends an `inv` for it. The `announced` child holds the `inv` sightings.

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use tokio::time::Instant;
use zakura_chain::block::{self, HeightDiff};

use super::order;

mod announced;

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
pub(super) const CLAIM_BODY_DEADLINE: Duration = Duration::from_secs(2);

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
    /// Identifies this entry to its guards, so a guard for a pruned entry leaves a newer entry
    /// for the same block alone.
    id: u64,
    /// The requests still verifying this block.
    pending: usize,
    /// Whether a request for this block has verified it.
    verified: bool,
}

#[derive(Debug)]
struct ClaimTable {
    /// The highest verified tip height seen. Claims and mined blocks below it are dropped, and
    /// no claim is made at or below it, so a dropped hash can never be claimed again.
    ///
    /// It never goes down. After a rollback below it, blocks at or below it get no claim and are
    /// ranked by arrival until the tip passes it again.
    floor: block::Height,
    by_hash: HashMap<block::Hash, Claim>,
    /// The first legacy `inv` for each block. Its height is only known once the body arrives.
    announced: Announcements,
    /// Mined blocks that are verifying or verified, by hash. Failed ones are removed.
    mined: HashMap<block::Hash, MinedBlock>,
    /// The id given to the next [`MinedBlock`].
    next_mined_id: u64,
}

impl Default for ClaimTable {
    fn default() -> Self {
        Self {
            floor: block::Height::MIN,
            by_hash: HashMap::new(),
            announced: Announcements::default(),
            mined: HashMap::new(),
            next_mined_id: 0,
        }
    }
}

/// Whether a block was close enough above the verified tip when first announced.
fn in_claim_window(height: block::Height, tip_at_announcement: block::Height) -> bool {
    (1..=MAX_CLAIM_HEIGHT_ABOVE_TIP).contains(&(height - tip_at_announcement))
}

impl ClaimTable {
    /// Raises the floor to `verified_tip`, dropping the claims and mined blocks below it.
    fn raise_floor(&mut self, verified_tip: block::Height) {
        self.floor = self.floor.max(verified_tip);
        let floor = self.floor;
        self.by_hash.retain(|_, claim| claim.height >= floor);
        self.mined.retain(|_, mined| mined.height >= floor);
    }

    /// Whether a mined block other than `hash` outranks a claim on `order` at `height`.
    fn is_voided(&self, hash: block::Hash, height: block::Height, order: u64) -> bool {
        self.mined.iter().any(|(mined_hash, mined)| {
            *mined_hash != hash && mined.height == height && order < mined.order
        })
    }

    /// The claims `hash` holds for a body at `height`, each with whether it is voided.
    ///
    /// An announcement has no height of its own, so the body's height must be claimable.
    fn candidates(
        &self,
        hash: block::Hash,
        height: Option<block::Height>,
    ) -> [Option<(Candidate, bool)>; 2] {
        let header = self.by_hash.get(&hash).map(|claim| Candidate {
            source: ClaimSource::Header,
            height: claim.height,
            order: claim.order,
            seen_at: claim.seen_at,
        });
        let inv = height
            .filter(|height| *height >= self.floor)
            .and_then(|height| {
                let announced = self.announced.get(hash)?;
                in_claim_window(height, announced.tip_at_announcement).then_some(Candidate {
                    source: ClaimSource::Inv,
                    height,
                    order: announced.order,
                    seen_at: announced.seen_at,
                })
            });
        [header, inv].map(|candidate| {
            candidate.map(|candidate| {
                let voided = self.is_voided(hash, candidate.height, candidate.order);
                (candidate, voided)
            })
        })
    }
}

/// One way a block was announced, as a body resolving its claim sees it.
#[derive(Copy, Clone, Debug)]
struct Candidate {
    source: ClaimSource,
    height: block::Height,
    order: u64,
    seen_at: Instant,
}

/// How the claim a body resolved was made.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(super) enum ClaimSource {
    /// Native header sync admitted the block's header.
    Header,
    /// A legacy peer sent the first `inv` for the block.
    Inv,
}

impl ClaimSource {
    fn label(self) -> &'static str {
        match self {
            Self::Header => "header",
            Self::Inv => "inv",
        }
    }
}

/// How a body that shares no active receipt resolved its block's claim.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(super) enum ClaimOutcome {
    /// The block has no claim, so the body is ranked by its arrival.
    Unclaimed,
    /// The body arrived inside its deadline and takes the claimed order.
    Used {
        source: ClaimSource,
        order: u64,
        age: Duration,
        bytes: usize,
    },
    /// The body arrived after its deadline and is ranked by its arrival.
    Expired {
        source: ClaimSource,
        age: Duration,
        bytes: usize,
    },
    /// A block this node mined at the claim's height outranks it, so the body is ranked by its
    /// arrival.
    Voided {
        source: ClaimSource,
        age: Duration,
        bytes: usize,
    },
}

impl ClaimOutcome {
    /// The order this body takes from its claim, if it keeps the claim.
    pub(super) fn claimed_order(&self) -> Option<u64> {
        match self {
            Self::Used { order, .. } => Some(*order),
            _ => None,
        }
    }

    /// Records this outcome for the body of `hash`, which was given `order`.
    pub(super) fn record(&self, hash: block::Hash, order: u64) {
        let (outcome, claim) = match *self {
            Self::Unclaimed => ("none", None),
            Self::Used {
                source, age, bytes, ..
            } => ("used", Some((source, age, bytes))),
            Self::Expired { source, age, bytes } => ("expired", Some((source, age, bytes))),
            Self::Voided { source, age, bytes } => ("voided", Some((source, age, bytes))),
        };
        let size = claim.map_or("none", |(_, _, bytes)| size_label(bytes));
        let source = claim.map_or("none", |(source, _, _)| source.label());
        metrics::counter!(
            "sync.block.announcement_claim",
            "outcome" => outcome,
            "size" => size,
            "source" => source,
        )
        .increment(1);
        if let Some((_, age, _)) = claim {
            metrics::histogram!(
                "sync.block.announcement_claim.age.seconds",
                "size" => size,
                "source" => source,
            )
            .record(age.as_secs_f64());
        }
        tracing::trace!(
            %hash,
            outcome,
            source,
            order,
            "resolved the announcement claim for a block body"
        );
    }
}

/// The metric label for a block of `bytes` serialized bytes.
///
/// Transfer time grows with size, so the claim metrics are split by it.
pub(super) fn size_label(bytes: usize) -> &'static str {
    match bytes {
        0..100_000 => "under_100kB",
        100_000..500_000 => "under_500kB",
        500_000..1_000_000 => "under_1MB",
        _ => "1MB_or_more",
    }
}

/// Receipt-order claims for blocks announced before their bodies arrived.
///
/// Header sync creates claims with [`AnnouncementClaims::begin`], legacy block `inv`s with
/// [`AnnouncementClaims::note_announcement`], and the block verifier reads them by block hash,
/// so every body transport follows the same rule. A claim, once made, is never replaced or
/// renewed.
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
    /// the highest verified tip seen, including `verified_tip`, and has no claim yet.
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

    /// Records the first legacy `inv` for `hash`, so its body can take the order it was announced
    /// in, and raises the verified tip to `verified_tip` as [`AnnouncementClaims::begin`] does.
    ///
    /// A block that already has a header claim or an announcement keeps it, so announcing a block
    /// again never renews its claim. The body later takes the announcement's order only if it
    /// arrives within the deadline, its height was one or two above the tip at announcement,
    /// and the highest tip seen since then has not passed that height.
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
        let mut table = self.table();
        table.raise_floor(verified_tip);
        if table.by_hash.contains_key(&hash) {
            return;
        }
        let tip_at_announcement = table.floor;
        let noted = table.announced.note(hash, tip_at_announcement, now);
        drop(table);

        if !noted {
            metrics::counter!("sync.block.announcement_claim.refused", "source" => "inv")
                .increment(1);
        }
    }

    /// Resolves the claims held for `hash` against a body arriving at `now`, whose coinbase
    /// `height` places an announcement and whose serialized size `bytes` gives for the claim
    /// metrics.
    ///
    /// A block can hold a header claim and an announcement. The body takes the earlier order of
    /// those still in force, so a second claim never makes the outcome worse than the first
    /// alone. If neither is in force, the outcome reports the earlier one.
    ///
    /// Resolving does not use a claim up, so an invalid body under the same hash cannot take it
    /// from the real one. `bytes` is called only for a claimed block, after the table lock is
    /// released, because sizing a block walks its transactions.
    pub(super) fn resolve(
        &self,
        hash: block::Hash,
        height: Option<block::Height>,
        now: Instant,
        bytes: impl FnOnce() -> usize,
    ) -> ClaimOutcome {
        let mut candidates = self.table().candidates(hash, height);
        candidates.sort_by_key(|held| held.map_or(u64::MAX, |(candidate, _)| candidate.order));
        let age = |candidate: &Candidate| now.saturating_duration_since(candidate.seen_at);
        let mut held = candidates.iter().flatten();
        let in_force = held
            .clone()
            .find(|(candidate, voided)| !voided && age(candidate) <= CLAIM_BODY_DEADLINE);
        let Some(&(claim, voided)) = in_force.or_else(|| held.next()) else {
            return ClaimOutcome::Unclaimed;
        };

        let (source, age, bytes) = (claim.source, age(&claim), bytes());
        if voided {
            ClaimOutcome::Voided { source, age, bytes }
        } else if age <= CLAIM_BODY_DEADLINE {
            ClaimOutcome::Used {
                source,
                order: claim.order,
                age,
                bytes,
            }
        } else {
            ClaimOutcome::Expired { source, age, bytes }
        }
    }

    /// Records that the locally mined block `hash` at `height` took `order`, so that a rival's
    /// earlier claim at `height` is not honoured.
    ///
    /// The record lasts while the returned guard, or any other guard for the same block, is
    /// alive. Once one of them is kept, it lasts until header sync reports a verified tip above
    /// `height`, or this node mines a block more than [`MAX_CLAIM_HEIGHT_ABOVE_TIP`] above it.
    pub(super) fn mark_mined(
        &self,
        hash: block::Hash,
        height: block::Height,
        order: u64,
    ) -> MinedGuard {
        let mut table = self.table();
        let table = &mut *table;
        let next_mined_id = table.next_mined_id;
        let mined = match table.mined.entry(hash) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                table.next_mined_id = next_mined_id.wrapping_add(1);
                entry.insert(MinedBlock {
                    height,
                    order,
                    id: next_mined_id,
                    pending: 0,
                    verified: false,
                })
            }
        };
        mined.pending = mined.pending.saturating_add(1);
        MinedGuard {
            claims: self.clone(),
            hash,
            height,
            id: mined.id,
        }
    }

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

    /// The order of the first announcement held for `hash`.
    #[cfg(test)]
    pub(super) fn announced_order(&self, hash: block::Hash) -> Option<u64> {
        self.table()
            .announced
            .get(hash)
            .map(|announced| announced.order)
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
            if claim.applies == 0 && !claim.accepted {
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
/// Dropping the last guard for a block removes its record, unless one of them was kept.
pub(in crate::block) struct MinedGuard {
    claims: AnnouncementClaims,
    hash: block::Hash,
    /// The height of this submission's body. Other bodies can share its header hash.
    height: block::Height,
    id: u64,
}

impl MinedGuard {
    /// Keeps the record after this guard is gone, because the mined block verified.
    pub(in crate::block) fn keep(self) {
        let mut table = self.claims.table();
        if let Some(mined) = self.own_entry(&mut table) {
            mined.verified = true;
            mined.height = self.height;
            // Without header sync the floor never moves. Prune using the body that verified.
            table
                .mined
                .retain(|_, other| self.height - other.height <= MAX_CLAIM_HEIGHT_ABOVE_TIP);
        }
    }

    /// The entry this guard holds, unless it was pruned.
    fn own_entry<'a>(&self, table: &'a mut ClaimTable) -> Option<&'a mut MinedBlock> {
        table
            .mined
            .get_mut(&self.hash)
            .filter(|mined| mined.id == self.id)
    }
}

impl Drop for MinedGuard {
    fn drop(&mut self) {
        let mut table = self.claims.table();
        let Some(mined) = self.own_entry(&mut table) else {
            return;
        };
        mined.pending = mined.pending.saturating_sub(1);
        if mined.pending == 0 && !mined.verified {
            table.mined.remove(&self.hash);
        }
    }
}
