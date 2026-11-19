//! Claims on receipt order made when a block's header is admitted, and the deadline for its body.

use std::{
    collections::HashMap,
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
pub(crate) const MAX_CLAIM_BODY_DEADLINE: Duration = Duration::from_secs(2);

/// The part of [`body_deadline`] that does not depend on body size: scheduling, the round trip
/// of the body request, and the wait for the verifier.
pub(super) const CLAIM_BASE_GRACE: Duration = Duration::from_millis(500);

/// The slowest honest body transfer that still keeps a claim, in bytes per second.
const CLAIM_FLOOR_BYTES_PER_SEC: u64 = 1024 * 1024;

/// How far above the verified tip a header can be and still get a claim.
///
/// Only blocks racing at the tip need one. Headers further ahead belong to catch-up, and their
/// bodies cannot arrive inside a deadline anyway.
const MAX_CLAIM_HEIGHT_ABOVE_TIP: HeightDiff = 2;

/// The most claims [`AnnouncementClaims`] holds.
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
        Duration::from_micros(bytes.saturating_mul(1_000_000) / CLAIM_FLOOR_BYTES_PER_SEC);
    CLAIM_BASE_GRACE
        .saturating_add(transfer)
        .min(MAX_CLAIM_BODY_DEADLINE)
}

/// A place in receipt order reserved for a block when its header was admitted.
#[derive(Copy, Clone, Debug)]
struct Claim {
    height: block::Height,
    order: u64,
    seen_at: Instant,
}

/// The receipt order of the most recent locally mined block.
///
/// A node does not abandon a block it mined because a rival's header arrived first, so a claim
/// at this height with an earlier order is not honoured while this is set.
#[derive(Debug)]
struct MinedWatermark {
    hash: block::Hash,
    height: block::Height,
    order: u64,
    /// Identifies this watermark to the [`MinedGuard`]s that hold it.
    epoch: u64,
    /// The requests still verifying this block.
    pending: usize,
    /// Whether a request for this block has verified it.
    verified: bool,
}

#[derive(Debug)]
struct ClaimTable {
    /// The highest verified tip height seen. Claims are created only above it and dropped once
    /// they are below it, so a dropped hash can never be claimed again.
    floor: block::Height,
    by_hash: HashMap<block::Hash, Claim>,
    mined: Option<MinedWatermark>,
    /// The epoch given to the next [`MinedWatermark`].
    next_mined_epoch: u64,
}

impl Default for ClaimTable {
    fn default() -> Self {
        Self {
            floor: block::Height::MIN,
            by_hash: HashMap::new(),
            mined: None,
            next_mined_epoch: 0,
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
        order: u64,
        age: Duration,
        bytes: usize,
    },
    /// The body arrived after its deadline and is ranked by its arrival.
    Expired { age: Duration, bytes: usize },
    /// A locally mined block at the claim's height outranks it, so the body is ranked by its
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
        let size = claim.map_or("unclaimed", |(_, bytes)| size_label(bytes));
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
/// them by block hash, so every body transport follows the same rule.
#[derive(Clone, Debug, Default)]
pub struct AnnouncementClaims(Arc<Mutex<ClaimTable>>);

/// Claims reserved for headers that are being applied to the header chain.
///
/// They take effect only with [`PendingClaims::commit`]. Dropping this discards them.
#[derive(Debug)]
#[must_use = "reserved claims take effect only when committed"]
pub struct PendingClaims {
    claims: AnnouncementClaims,
    reserved: Vec<(block::Hash, Claim)>,
}

impl AnnouncementClaims {
    /// Reserves a place in receipt order for each of `headers` that is just above
    /// `verified_tip` and has no claim yet.
    ///
    /// Call this when a header apply starts and commit the result once the header chain has
    /// accepted the headers. The order and time are taken now, so a rival body that arrives
    /// during the apply does not overtake a header that arrived first.
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
        let reserved = headers
            .into_iter()
            .filter(|(hash, height)| {
                (1..=MAX_CLAIM_HEIGHT_ABOVE_TIP).contains(&(*height - floor))
                    && !table.by_hash.contains_key(hash)
            })
            .map(|(hash, height)| {
                let claim = Claim {
                    height,
                    order: order::next(),
                    seen_at: now,
                };
                (hash, claim)
            })
            .collect();
        PendingClaims {
            claims: self.clone(),
            reserved,
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
            let voided = table.mined.as_ref().is_some_and(|mined| {
                mined.hash != hash && mined.height == claim.height && claim.order < mined.order
            });
            (claim, voided)
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

    /// Records that the locally mined block `hash` took `order`, so that a rival's earlier claim
    /// at `height` is not honoured. The record lasts while the returned guard, or any other
    /// guard for the same block, is alive. Once one of them is kept, it stays until a newer
    /// mined block replaces it.
    pub(super) fn mark_mined(
        &self,
        hash: block::Hash,
        height: block::Height,
        order: u64,
    ) -> MinedGuard {
        let mut table = self.table();
        let epoch = match &mut table.mined {
            Some(mined) if mined.hash == hash => {
                mined.pending = mined.pending.saturating_add(1);
                mined.epoch
            }
            _ => {
                let epoch = table.next_mined_epoch;
                table.next_mined_epoch = epoch.wrapping_add(1);
                table.mined = Some(MinedWatermark {
                    hash,
                    height,
                    order,
                    epoch,
                    pending: 1,
                    verified: false,
                });
                epoch
            }
        };
        MinedGuard {
            claims: self.clone(),
            epoch,
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

    fn table(&self) -> MutexGuard<'_, ClaimTable> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl PendingClaims {
    /// Puts the reserved claims in force. A hash that gained a claim in the meantime keeps its
    /// first one, and a full table refuses the rest.
    pub fn commit(self) {
        if self.reserved.is_empty() {
            return;
        }
        let mut refused: u64 = 0;
        {
            let mut table = self.claims.table();
            for (hash, claim) in self.reserved {
                // A claim below the floor would be dropped by the next `begin`.
                if claim.height < table.floor || table.by_hash.contains_key(&hash) {
                    continue;
                }
                if table.by_hash.len() >= MAX_ANNOUNCEMENT_CLAIMS {
                    refused = refused.saturating_add(1);
                    continue;
                }
                table.by_hash.insert(hash, claim);
            }
        }
        if refused > 0 {
            metrics::counter!("sync.block.announcement_claim.refused").increment(refused);
        }
    }
}

/// Keeps a locally mined block's watermark while its verification is active.
///
/// Dropping the last guard for a block removes its watermark, unless one of them was kept.
pub(crate) struct MinedGuard {
    claims: AnnouncementClaims,
    epoch: u64,
}

impl MinedGuard {
    /// Keeps the watermark after this guard is gone, because the mined block verified.
    pub(crate) fn keep(self) {
        if let Some(mined) = &mut self.claims.table().mined {
            if mined.epoch == self.epoch {
                mined.verified = true;
            }
        }
    }
}

impl Drop for MinedGuard {
    fn drop(&mut self) {
        let mut table = self.claims.table();
        let Some(mined) = &mut table.mined else {
            return;
        };
        // A newer mined block has replaced the watermark this guard was holding.
        if mined.epoch != self.epoch {
            return;
        }
        mined.pending = mined.pending.saturating_sub(1);
        if mined.pending == 0 && !mined.verified {
            table.mined = None;
        }
    }
}
