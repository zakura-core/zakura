//! Resolve a block body's native header and legacy announcement claims at receipt time.

use std::time::Duration;

use zakura_chain::block;

use super::{AnnouncementClaims, CLAIM_BODY_DEADLINE};

impl AnnouncementClaims {
    /// Gives a body the earliest usable reservation for its hash, without consuming it.
    /// If all eligible claims failed, report the first one's outcome for metrics.
    pub(in crate::block::receipt) fn resolve(
        &self,
        hash: block::Hash,
        height: Option<block::Height>,
        arrival: BodyArrival,
    ) -> ClaimOutcome {
        use ClaimOutcome::{Expired, Unclaimed, Used, Voided};

        let table = self.table();
        let classify = |source, height, order, seen_at| {
            if order >= arrival.order {
                return Unclaimed;
            }
            let Some(age) = arrival.time.checked_duration_since(seen_at) else {
                return Unclaimed;
            };
            let voided = table.mined.iter().any(|(mined_hash, mined)| {
                *mined_hash != hash && mined.height == height && order < mined.order
            });
            if voided {
                Voided { source, age }
            } else if age > CLAIM_BODY_DEADLINE {
                Expired { source, age }
            } else {
                Used { source, order, age }
            }
        };

        let legacy = table.announced.get(hash).filter_map(|candidate| {
            let height = height.filter(|height| *height >= table.floor)?;
            Some((
                candidate.source_at(height)?,
                height,
                candidate.order,
                candidate.seen_at,
            ))
        });
        let header = table.by_hash.get(&hash).map(|header| {
            (
                ClaimSource::Header,
                header.height,
                header.order,
                header.seen_at,
            )
        });
        legacy
            .chain(header)
            .map(|(source, height, order, seen_at)| {
                (order, classify(source, height, order, seen_at))
            })
            // Prefer a usable claim. If all failed, report the earliest eligible sighting for metrics.
            .min_by_key(|(order, outcome)| (outcome.claimed_order().is_none(), *order))
            .map_or(Unclaimed, |(_, outcome)| outcome)
    }
}

/// How the claim a body resolved was made.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(in crate::block::receipt) enum ClaimSource {
    /// Native header sync admitted the block's header.
    Header,
    /// A legacy peer sent the first `inv` for the block.
    Inv,
    /// A legacy `block` message carrying the block, counted from when it started arriving,
    /// holds the earliest legacy reservation. It need not have been pushed or preceded an `inv`.
    BlockMessage,
}

impl ClaimSource {
    fn label(self) -> &'static str {
        match self {
            Self::Header => "header",
            Self::Inv => "inv",
            Self::BlockMessage => "block",
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
    },
    /// The body arrived after its deadline and is ranked by its arrival.
    Expired { source: ClaimSource, age: Duration },
    /// A block this node mined at the claim's height outranks it, so the body is ranked by its
    /// arrival.
    Voided { source: ClaimSource, age: Duration },
}

impl ClaimOutcome {
    /// The order this body takes from its claim, if it keeps the claim.
    pub(in crate::block::receipt) fn claimed_order(&self) -> Option<u64> {
        match self {
            Self::Used { order, .. } => Some(*order),
            _ => None,
        }
    }

    /// Records metrics for a newly tracked receipt. Only a held claim needs the body's size.
    pub(in crate::block::receipt) fn record(
        &self,
        hash: block::Hash,
        order: u64,
        body_size: impl FnOnce() -> usize,
    ) {
        let (outcome, claim) = match *self {
            Self::Unclaimed => ("none", None),
            Self::Used { source, age, .. } => ("used", Some((source, age))),
            Self::Expired { source, age } => ("expired", Some((source, age))),
            Self::Voided { source, age } => ("voided", Some((source, age))),
        };
        let size = claim.map_or("none", |_| size_label(body_size()));
        let source = claim.map_or("none", |(source, _)| source.label());
        metrics::counter!(
            "sync.block.announcement_claim",
            "outcome" => outcome,
            "size" => size,
            "source" => source,
        )
        .increment(1);
        if let Some((_, age)) = claim {
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
