//! Generating the historical-treestate artifacts from an archive database.
//!
//! One contiguous replay from genesis to the top of the absent band produces both artifacts at
//! once: the frontier grid (`docs/design/historical-treestate-serving.md` §4.1) and the completed
//! subtree roots (§4.6).
//!
//! # Why this does not need a legacy-synced publisher
//!
//! §5 of the design specifies an archive, *legacy-synced* publisher host, because it assumed the
//! exporter reads per-height trees and subtree column families straight off disk. Replaying
//! instead lifts that requirement: the frontiers this produces are checked against the
//! authenticated roots in `commitment_roots_by_height`, which a fast-synced node has for every
//! height in its band, so any archive node can generate the frontier artifact and a consumer's
//! check is unchanged.
//!
//! The subtree roots come along for free, and inherit that verification. They are interior nodes
//! computed during the same replay, and the replay is deterministic between two root-checked
//! endpoints: producing the correct end root from a correct start root while computing a wrong
//! interior node would require a hash collision. That is a materially stronger position than
//! §4.6's "reviewed, trusted" story, which exists only because it assumed subtree roots could not
//! be checked without replaying each subtree's leaves — true for a serving node, not for a
//! publisher that is replaying regardless.

use std::time::{Duration, Instant};

use thiserror::Error;

use zakura_chain::block::Height;

use crate::service::{
    finalized_state::{
        treestate_artifact::{FrontierArtifact, FrontierEntry, SubtreeArtifact, SubtreeRecord},
        ZakuraDb,
    },
    read::{
        historical_tree::{
            replay_with_subtrees, verify_against_index, HistoricalTreeDerivationError, ShieldedPool,
        },
        DerivedFrontiers,
    },
};

/// Estimated replay cost of reading and applying one block, in microseconds.
///
/// Measured directly on Mainnet in regions that carry no commitments, where the cost is entirely
/// reading and deserialising the block body: 277 us/block median over heights 0-400k in the phase
/// A2 walk, and 323 us/block for a 1,000-block replay at height 99k during generation.
///
/// Do not derive this by discounting a per-derivation measurement for root-computation overhead.
/// A tree's root is cached until something is appended, so a block with no commitments pays no
/// root cost at all — the discount applies only where there is shielded activity, which is
/// already covered by [`COST_PER_COMMITMENT_US`]. Applying it here once produced a constant 2.5x
/// too low, which under-spaced quiet regions badly enough to blow the cold-request budget.
const COST_PER_BLOCK_US: u64 = 300;

/// Estimated replay cost of appending one note commitment, in microseconds.
///
/// Fitted so the model reproduces the measured whole-band replay time (6,879 s over 3,358,006
/// blocks and ~124M Sapling and Orchard commitments) given [`COST_PER_BLOCK_US`]. Appending
/// dominates wherever there is shielded activity.
const COST_PER_COMMITMENT_US: u64 = 47;

/// How the grid's height spacing is chosen.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GridSpacing {
    /// One entry every `blocks` heights.
    Uniform {
        /// The height spacing.
        blocks: u32,
    },

    /// One entry per `budget_us` of estimated replay cost.
    ///
    /// Replay cost varies by more than an order of magnitude across Mainnet, so a uniform grid
    /// either wastes entries where blocks are cheap or leaves a long tail where they are not.
    /// Spacing by estimated cost instead bounds the worst cold request rather than the average.
    ///
    /// The estimate is a deterministic function of the chain — block and commitment counts, not
    /// wall-clock timing — so two generator runs on different hardware still produce byte-identical
    /// artifacts, which the determinism gate requires.
    Adaptive {
        /// The per-entry cost budget, in microseconds.
        budget_us: u64,
    },
}

/// Why artifact generation could not complete.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum TreestateExportError {
    /// The database has no absent band, so there is nothing to generate.
    #[error("this database stores per-height trees at every height, so no artifact is needed")]
    NoAbsentBand,

    /// The grid spacing or cost budget was zero, which would produce an entry at every height.
    #[error("grid spacing and cost budget must be at least 1")]
    ZeroSpacing,

    /// A replay or its root check failed.
    #[error("generation failed at height {height:?}: {source}")]
    Derivation {
        /// The height being produced when generation failed.
        height: Height,
        /// The underlying failure.
        #[source]
        source: HistoricalTreeDerivationError,
    },
}

/// What one generation run produced.
#[derive(Clone, Debug)]
pub struct TreestateExport {
    /// The frontier grid.
    pub frontiers: FrontierArtifact,

    /// The completed subtree roots.
    pub subtrees: SubtreeArtifact,

    /// How long generation took.
    pub elapsed: Duration,

    /// How many blocks were replayed.
    pub replayed_blocks: u64,
}

/// Generates both artifacts for `db`'s absent band at the given grid `spacing`.
///
/// Replays contiguously from genesis, emitting a frontier entry every `spacing` heights and
/// recording every subtree that completes. Each emitted entry is root-checked against
/// `commitment_roots_by_height`, and generation stops at the first height that fails, so a
/// returned artifact is one whose every entry matched.
///
/// `on_progress` is called with each emitted entry's height, for long runs.
pub fn export(
    db: &ZakuraDb,
    spacing: GridSpacing,
    mut on_progress: impl FnMut(Height, u64),
) -> Result<TreestateExport, TreestateExportError> {
    match spacing {
        GridSpacing::Uniform { blocks: 0 } | GridSpacing::Adaptive { budget_us: 0 } => {
            return Err(TreestateExportError::ZeroSpacing)
        }
        _ => {}
    }

    let handoff = db
        .vct_synced_below()
        .ok_or(TreestateExportError::NoAbsentBand)?;
    let upgrade = db.vct_upgrade_height().unwrap_or(Height(0));
    if upgrade >= handoff {
        return Err(TreestateExportError::NoAbsentBand);
    }

    let start = Instant::now();
    // The band is half-open, so the last height it covers is `H - 1`.
    let last = Height(handoff.0 - 1);

    let mut subtrees = SubtreeArtifact {
        handoff,
        ..SubtreeArtifact::default()
    };
    let mut entries: Vec<FrontierEntry> = Vec::new();
    let mut replayed_blocks = 0u64;

    // Each grid step is one root-checked replay anchored on the previous step, so the whole band
    // is covered by a chain of verified endpoints rather than one unverified sweep.
    let mut frontiers = DerivedFrontiers::empty();
    let mut next_replay_from = 0u32;

    let mut next = upgrade;
    let mut accrued_cost: u64 = 0;

    loop {
        // Under a uniform grid the next entry is a fixed number of blocks ahead. Under an adaptive
        // one it is wherever the estimated cost budget runs out, which needs a block-by-block scan
        // of the commitment counts to find.
        let target = match spacing {
            GridSpacing::Uniform { blocks } => Height(next.0.saturating_add(blocks).min(last.0)),
            GridSpacing::Adaptive { budget_us } => {
                let mut candidate = next;
                while candidate < last {
                    let counts = db
                        .block(candidate.into())
                        .map(|block| {
                            block.sapling_note_commitments().count()
                                + block.orchard_note_commitments().count()
                                + block.ironwood_note_commitments().count()
                        })
                        .unwrap_or(0);

                    accrued_cost = accrued_cost
                        .saturating_add(COST_PER_BLOCK_US)
                        .saturating_add(COST_PER_COMMITMENT_US.saturating_mul(counts as u64));

                    if accrued_cost >= budget_us {
                        accrued_cost = 0;
                        break;
                    }

                    candidate = Height(candidate.0 + 1);
                }
                candidate
            }
        };

        let mut collected = Vec::new();
        frontiers = replay_with_subtrees(
            db,
            target,
            next_replay_from,
            frontiers,
            |pool, completed| collected.push((pool, completed)),
        )
        .map_err(|source| TreestateExportError::Derivation {
            height: target,
            source,
        })?;

        replayed_blocks += u64::from(target.0 - next_replay_from) + 1;
        next_replay_from = target.0 + 1;

        // The root check is what makes this entry, and every subtree recorded since the previous
        // entry, safe to publish.
        verify_against_index(db, target, &frontiers).map_err(|source| {
            TreestateExportError::Derivation {
                height: target,
                source,
            }
        })?;

        for (pool, completed) in collected {
            let record = SubtreeRecord {
                index: completed.index,
                end_height: completed.end_height,
                root: completed.root,
            };
            match pool {
                ShieldedPool::Sapling => subtrees.sapling.push(record),
                ShieldedPool::Orchard => subtrees.orchard.push(record),
                ShieldedPool::Ironwood => subtrees.ironwood.push(record),
            }
        }

        entries.push(FrontierEntry {
            height: target,
            sapling: frontiers.sapling.clone(),
            orchard: frontiers.orchard.clone(),
            ironwood: frontiers.ironwood.clone(),
        });
        on_progress(target, replayed_blocks);

        if target == last {
            break;
        }

        next = Height(target.0 + 1);
    }

    Ok(TreestateExport {
        frontiers: FrontierArtifact {
            spacing: match spacing {
                GridSpacing::Uniform { blocks } => blocks,
                // Recorded as provenance only; consumers locate entries by searching.
                GridSpacing::Adaptive { .. } => 0,
            },
            handoff,
            entries,
        },
        subtrees,
        elapsed: start.elapsed(),
        replayed_blocks,
    })
}
