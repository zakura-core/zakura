//! Resolve a block body's native header and legacy announcement claims at receipt time.

use std::{cell::LazyCell, time::Duration};

use tokio::time::Instant;
use zakura_chain::block;

use super::{in_claim_window, AnnouncementClaims, ClaimTable, CLAIM_BODY_DEADLINE};

impl AnnouncementClaims {
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
    pub(in crate::block::receipt) fn resolve(
        &self,
        hash: block::Hash,
        height: Option<block::Height>,
        now: Instant,
        bytes: impl FnOnce() -> usize,
    ) -> ClaimOutcome {
        let candidates = self.table().candidates(hash, height);
        let bytes = LazyCell::new(bytes);
        candidates
            .into_iter()
            .flatten()
            .map(|claim| (claim.order, claim.classify(now, *bytes)))
            .min_by_key(|(order, outcome)| (outcome.claimed_order().is_none(), *order))
            .map_or(ClaimOutcome::Unclaimed, |(_, outcome)| outcome)
    }
}

impl ClaimTable {
    /// Snapshots both eligible claims and the local mining decision under the same lock.
    fn candidates(
        &self,
        hash: block::Hash,
        height: Option<block::Height>,
    ) -> [Option<Candidate>; 2] {
        let candidate = |source, height, order, seen_at| Candidate {
            source,
            order,
            seen_at,
            voided: self.mined.iter().any(|(mined_hash, mined)| {
                *mined_hash != hash && mined.height == height && order < mined.order
            }),
        };

        let header = self.by_hash.get(&hash).map(|claim| {
            candidate(
                ClaimSource::Header,
                claim.height,
                claim.order,
                claim.seen_at,
            )
        });
        let inv = height.and_then(|height| {
            let announcement = self.announced.get(hash)?;
            if height < self.floor || !in_claim_window(height, announcement.tip_at_announcement) {
                return None;
            }
            Some(candidate(
                ClaimSource::Inv,
                height,
                announcement.order,
                announcement.seen_at,
            ))
        });
        [header, inv]
    }
}

/// A reservation on this block's receipt order at the instant its body arrived.
struct Candidate {
    source: ClaimSource,
    order: u64,
    seen_at: Instant,
    voided: bool,
}

impl Candidate {
    fn classify(self, now: Instant, bytes: usize) -> ClaimOutcome {
        let age = now.saturating_duration_since(self.seen_at);
        let source = self.source;
        if self.voided {
            ClaimOutcome::Voided { source, age, bytes }
        } else if age > CLAIM_BODY_DEADLINE {
            ClaimOutcome::Expired { source, age, bytes }
        } else {
            ClaimOutcome::Used {
                source,
                order: self.order,
                age,
                bytes,
            }
        }
    }
}

/// How the claim a body resolved was made.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(in crate::block::receipt) enum ClaimSource {
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
pub(in crate::block::receipt) enum ClaimOutcome {
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
    pub(in crate::block::receipt) fn claimed_order(&self) -> Option<u64> {
        match self {
            Self::Used { order, .. } => Some(*order),
            _ => None,
        }
    }

    /// Records this outcome for the body of `hash`, which was given `order`.
    pub(in crate::block::receipt) fn record(&self, hash: block::Hash, order: u64) {
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
pub(in crate::block::receipt) fn size_label(bytes: usize) -> &'static str {
    match bytes {
        0..100_000 => "under_100kB",
        100_000..500_000 => "under_500kB",
        500_000..1_000_000 => "under_1MB",
        _ => "1MB_or_more",
    }
}
