//! Claims on receipt order made when a block's header is admitted, and the deadline for its body.

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use tokio::time::Instant;
use zakura_chain::block::{self, HeightDiff};

use super::order;

/// The longest any block keeps its claim while its body is in transit, whatever its size.
///
/// Inside its deadline a withheld body looks the same as a slow honest delivery, so this cap is
/// the most a miner can gain by sitting on an announced block and still winning an equal-work
/// tie. A block padded to about 1.5 MiB gets all of it. The value is an estimate of honest
/// full-block delivery, to be revisited against the `sync.block.announcement_claim.age.seconds`
/// histogram.
pub(super) const MAX_CLAIM_BODY_DEADLINE: Duration = Duration::from_secs(2);

/// The part of [`body_deadline`] that does not depend on body size: scheduling, the round trip
/// of the body request, and the wait for the verifier.
pub(super) const CLAIM_BASE_GRACE: Duration = Duration::from_millis(500);

/// The slowest honest body transfer that still keeps a claim, in bytes per second.
const MIN_HONEST_BODY_BYTES_PER_SEC: u64 = 1024 * 1024;

/// How far above the verified tip a header can be and still get a claim.
///
/// Only blocks racing at the tip need one. Headers further ahead belong to catch-up, and their
/// bodies cannot arrive inside a deadline anyway.
const MAX_CLAIM_HEIGHT_ABOVE_TIP: HeightDiff = 2;

/// The most claims [`AnnouncementClaims`] holds, counting those still being applied.
///
/// A full table refuses new claims instead of dropping old ones, so flooding it cannot remove a
/// claim that is already in force. A refused block is ranked by body arrival.
pub(super) const MAX_ANNOUNCEMENT_CLAIMS: usize = 64;

/// How long after its claim a body of `bytes` serialized bytes still takes the claimed order.
///
/// Transfer time grows with size, so a small block gets less than a full one. Every block gets
/// at most [`MAX_CLAIM_BODY_DEADLINE`].
pub(super) fn body_deadline(bytes: usize) -> Duration {
    let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
    let transfer =
        Duration::from_micros(bytes.saturating_mul(1_000_000) / MIN_HONEST_BODY_BYTES_PER_SEC);
    CLAIM_BASE_GRACE
        .saturating_add(transfer)
        .min(MAX_CLAIM_BODY_DEADLINE)
}

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
            mined: HashMap::new(),
            next_mined_id: 0,
        }
    }
}

impl ClaimTable {
    /// Whether a mined block other than `hash` outranks `claim`.
    fn is_voided(&self, hash: block::Hash, claim: &Claim) -> bool {
        self.mined.iter().any(|(mined_hash, mined)| {
            *mined_hash != hash && mined.height == claim.height && claim.order < mined.order
        })
    }
}

/// How a body that shares no active receipt resolved its block's claim.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(super) enum ClaimOutcome {
    /// The block has no claim, so the body is ranked by its arrival.
    Unclaimed,
    /// The body arrived inside its deadline and takes the claimed order.
    Used {
        order: u64,
        age: Duration,
        bytes: usize,
    },
    /// The body arrived after its deadline and is ranked by its arrival.
    Expired { age: Duration, bytes: usize },
    /// A block this node mined at the claim's height outranks it, so the body is ranked by its
    /// arrival.
    Voided { age: Duration, bytes: usize },
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
            Self::Used { age, bytes, .. } => ("used", Some((age, bytes))),
            Self::Expired { age, bytes } => ("expired", Some((age, bytes))),
            Self::Voided { age, bytes } => ("voided", Some((age, bytes))),
        };
        let size = claim.map_or("none", |(_, bytes)| size_label(bytes));
        metrics::counter!(
            "sync.block.announcement_claim",
            "outcome" => outcome,
            "size" => size,
        )
        .increment(1);
        if let Some((age, _)) = claim {
            metrics::histogram!("sync.block.announcement_claim.age.seconds", "size" => size)
                .record(age.as_secs_f64());
        }
        tracing::trace!(%hash, outcome, order, "resolved the announcement claim for a block body");
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

/// Receipt-order claims for blocks whose headers were admitted before their bodies arrived.
///
/// Header sync creates claims with [`AnnouncementClaims::begin`], and the block verifier reads
/// them by block hash, so every body transport follows the same rule. A claim, once made, is
/// never replaced or renewed.
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
        table.floor = table.floor.max(verified_tip);
        let floor = table.floor;
        table.by_hash.retain(|_, claim| claim.height >= floor);
        table.mined.retain(|_, mined| mined.height >= floor);

        let mut covered = Vec::new();
        let mut refused: u64 = 0;
        for (hash, height) in headers {
            if !(1..=MAX_CLAIM_HEIGHT_ABOVE_TIP).contains(&(height - floor)) {
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
            metrics::counter!("sync.block.announcement_claim.refused").increment(refused);
        }
        PendingClaims {
            claims: self.clone(),
            covered,
        }
    }

    /// Resolves the claim held for `hash` against a body arriving at `now`, whose serialized
    /// size `bytes` gives.
    ///
    /// Resolving does not use the claim up, so an invalid body under the same header cannot take
    /// it from the real one. `bytes` is called only for a claimed block, after the table lock is
    /// released, because sizing a block walks its transactions.
    pub(super) fn resolve(
        &self,
        hash: block::Hash,
        now: Instant,
        bytes: impl FnOnce() -> usize,
    ) -> ClaimOutcome {
        let (claim, voided) = {
            let table = self.table();
            let Some(claim) = table.by_hash.get(&hash).copied() else {
                return ClaimOutcome::Unclaimed;
            };
            (claim, table.is_voided(hash, &claim))
        };
        let age = now.saturating_duration_since(claim.seen_at);
        let bytes = bytes();
        if voided {
            ClaimOutcome::Voided { age, bytes }
        } else if age <= body_deadline(bytes) {
            ClaimOutcome::Used {
                order: claim.order,
                age,
                bytes,
            }
        } else {
            ClaimOutcome::Expired { age, bytes }
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
        // Bounds the records on a node without header sync, whose floor never moves.
        table
            .mined
            .retain(|_, mined| height - mined.height <= MAX_CLAIM_HEIGHT_ABOVE_TIP);
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
    id: u64,
}

impl MinedGuard {
    /// Keeps the record after this guard is gone, because the mined block verified.
    pub(in crate::block) fn keep(self) {
        if let Some(mined) = self.own_entry(&mut self.claims.table()) {
            mined.verified = true;
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
