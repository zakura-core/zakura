//! Producing the coupled frontier and completed-subtree artifacts for a release checkpoint.
//!
//! The embedded subtree artifact supplies the reviewed prefix through the current binary's
//! checkpoint. The finalized database supplies rows completed after that checkpoint. Combining
//! those two sources works for both legacy and verified-commitment-trees databases, and does not
//! need historical block bodies or per-height frontiers from the skipped range.

use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use thiserror::Error;

use zakura_chain::{
    block::Height,
    subtree::{NoteCommitmentSubtreeData, NoteCommitmentSubtreeIndex, TRACKED_SUBTREE_HEIGHT},
};

use crate::service::read::historical_tree::{
    replay_with_subtrees, stored_frontier_before_absent_band, stored_frontiers_at,
    verify_against_available_roots, DerivedFrontiers, HistoricalTreeDerivationError,
};

use super::{
    commitment_aux::{
        produce_settled_final_frontiers, FinalFrontiers, FinalFrontiersGenerationError,
    },
    treestate_artifact::{
        embedded_historical_subtrees, FrontierArtifact, FrontierEntry, SubtreeArtifact,
        SubtreeRecord, TreestateArtifactError,
    },
    ZakuraDb,
};

/// Why the release treestate artifacts could not be produced.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ReleaseTreestateArtifactsError {
    /// This network does not have a reviewed subtree artifact to extend.
    #[error("the {network} network has no embedded subtree artifact to extend")]
    NoEmbeddedSubtreeArtifact {
        /// The configured network's display name.
        network: String,
    },

    /// The requested checkpoint does not advance the embedded artifact.
    #[error(
        "release checkpoint {target:?} must be above embedded subtree checkpoint {embedded:?}"
    )]
    CheckpointNotAdvanced {
        /// The checkpoint selected by the release-state exporter.
        target: Height,
        /// The checkpoint carried by the current binary.
        embedded: Height,
    },

    /// A VCT database may have skipped roots beyond the current binary's embedded artifact.
    #[error(
        "embedded subtree checkpoint {embedded:?} is below VCT database handoff {handoff:?}; install a newer zakura-checkpoints exporter"
    )]
    EmbeddedArtifactBeforeVctHandoff {
        /// The checkpoint carried by the current binary.
        embedded: Height,
        /// The checkpoint used to fast sync this database.
        handoff: Height,
    },

    /// The final frontier could not be produced from the finalized database.
    #[error("cannot produce the release frontier: {0}")]
    Frontier(#[from] FinalFrontiersGenerationError),

    /// A target frontier requires more completed subtrees than the artifact format can index.
    #[error(
        "{pool} frontier requires {found} completed subtrees, which the artifact format cannot represent"
    )]
    UnrepresentableSubtreeCount {
        /// Which pool has too many completed subtrees.
        pool: &'static str,
        /// The completed-subtree count implied by the frontier.
        found: u64,
    },

    /// The target frontier contains fewer completed subtrees than the embedded artifact.
    #[error(
        "{pool} frontier at {target:?} has {target_count} completed subtrees, below the embedded artifact's {embedded_count}"
    )]
    FrontierBeforeEmbeddedArtifact {
        /// Which pool went backwards.
        pool: &'static str,
        /// The new release checkpoint.
        target: Height,
        /// The number of roots in the new frontier.
        target_count: usize,
        /// The number of roots in the embedded artifact.
        embedded_count: usize,
    },

    /// A database row disagrees with the reviewed embedded record at the same index.
    #[error("stored {pool} subtree {index} does not match the embedded artifact")]
    EmbeddedSubtreeMismatch {
        /// Which pool disagreed.
        pool: &'static str,
        /// The subtree index that disagreed.
        index: u16,
    },

    /// The embedded prefix and retained database rows do not cover every target subtree.
    #[error(
        "{pool} subtree roots are incomplete at {target:?}: expected {expected} records, found {found}"
    )]
    IncompleteSubtrees {
        /// Which pool has a gap.
        pool: &'static str,
        /// The new release checkpoint.
        target: Height,
        /// The number of records required by the target frontier.
        expected: usize,
        /// The number of contiguous records available.
        found: usize,
    },

    /// A retained row lies within the target height but beyond the target frontier's count.
    #[error(
        "stored {pool} subtree {index} completes at {end_height:?}, but the frontier at {target:?} only contains {expected} completed subtrees"
    )]
    SubtreeBeyondFrontier {
        /// Which pool has the extra row.
        pool: &'static str,
        /// The extra row's subtree index.
        index: u16,
        /// The height recorded on the extra row.
        end_height: Height,
        /// The target checkpoint.
        target: Height,
        /// The number of completed subtrees in the target frontier.
        expected: usize,
    },

    /// The combined roots do not match the target frontier.
    #[error("combined subtree roots do not match the frontier at {target:?}: {source}")]
    SubtreeRootsUnverified {
        /// The target checkpoint.
        target: Height,
        /// Why frontier verification failed.
        #[source]
        source: TreestateArtifactError,
    },
}

/// The two binary artifacts produced together for one release checkpoint.
#[derive(Clone, Debug)]
pub struct ReleaseTreestateArtifacts {
    /// The checkpoint selected by this release-state run.
    pub last_checkpoint: Height,

    /// The checkpoint of the reviewed embedded subtree prefix.
    pub previous_last_checkpoint: Height,

    /// Serialized final-frontier bytes for [`Self::last_checkpoint`].
    pub final_frontiers: Vec<u8>,

    /// Serialized completed-subtree bytes for [`Self::last_checkpoint`].
    pub historical_subtrees: Vec<u8>,

    /// The number of roots copied from retained database rows after the embedded prefix.
    pub added_subtree_roots: usize,

    /// The total number of roots proven against the new final frontier.
    pub verified_subtree_roots: usize,
}

/// Produces the frontier and completed-subtree artifacts for `last_checkpoint` as one pair.
///
/// The database must be quiesced by the caller. The current binary's reviewed subtree artifact is
/// used as the prefix, retained database rows extend it, and the complete result is verified once
/// against the newly produced frontier before either byte vector is returned. A VCT database whose
/// handoff is newer than the embedded prefix requires a newer exporter.
pub fn produce_release_treestate_artifacts(
    db: &ZakuraDb,
    last_checkpoint: Height,
) -> Result<ReleaseTreestateArtifacts, ReleaseTreestateArtifactsError> {
    let network = db.network();
    let previous = embedded_historical_subtrees(&network).ok_or_else(|| {
        ReleaseTreestateArtifactsError::NoEmbeddedSubtreeArtifact {
            network: network.to_string(),
        }
    })?;
    if last_checkpoint <= previous.last_checkpoint {
        return Err(ReleaseTreestateArtifactsError::CheckpointNotAdvanced {
            target: last_checkpoint,
            embedded: previous.last_checkpoint,
        });
    }
    let uncovered_handoff = db
        .vct_synced_below()
        .filter(|handoff| previous.last_checkpoint < *handoff);
    if let Some(handoff) = uncovered_handoff {
        return Err(
            ReleaseTreestateArtifactsError::EmbeddedArtifactBeforeVctHandoff {
                embedded: previous.last_checkpoint,
                handoff,
            },
        );
    }

    // Produce this first so the database's quiesced state supplies both artifacts' common bound.
    let frontiers = produce_settled_final_frontiers(db, last_checkpoint)?;
    let final_frontiers = frontiers.to_bytes();
    let previous_last_checkpoint = previous.last_checkpoint;
    let previous_root_count = artifact_root_count(&previous);
    let subtrees = extend_subtree_artifact(db, previous, &frontiers)?;
    let verified_subtree_roots = subtrees
        .verify_against_frontiers(&frontiers.sapling, &frontiers.orchard, &frontiers.ironwood)
        .map(|counts| counts.total())
        .map_err(
            |source| ReleaseTreestateArtifactsError::SubtreeRootsUnverified {
                target: last_checkpoint,
                source,
            },
        )?;
    let added_subtree_roots = verified_subtree_roots
        .checked_sub(previous_root_count)
        .expect("the extended artifact contains the complete embedded prefix");
    let historical_subtrees = subtrees.encode(&network);

    Ok(ReleaseTreestateArtifacts {
        last_checkpoint,
        previous_last_checkpoint,
        final_frontiers,
        historical_subtrees,
        added_subtree_roots,
        verified_subtree_roots,
    })
}

/// Extends all three pools in `previous` with retained rows through `frontiers.height`.
fn extend_subtree_artifact(
    db: &ZakuraDb,
    previous: SubtreeArtifact,
    frontiers: &FinalFrontiers,
) -> Result<SubtreeArtifact, ReleaseTreestateArtifactsError> {
    Ok(SubtreeArtifact {
        last_checkpoint: frontiers.height,
        sapling: extend_pool(
            "sapling",
            frontiers.height,
            frontiers.sapling.count(),
            previous.sapling,
            db.sapling_subtree_list_by_index_range(..),
            |root| root.to_bytes(),
        )?,
        orchard: extend_pool(
            "orchard",
            frontiers.height,
            frontiers.orchard.count(),
            previous.orchard,
            db.orchard_subtree_list_by_index_range(..),
            |root| root.to_repr(),
        )?,
        ironwood: extend_pool(
            "ironwood",
            frontiers.height,
            frontiers.ironwood.count(),
            previous.ironwood,
            db.ironwood_subtree_list_by_index_range(..),
            |root| root.to_repr(),
        )?,
    })
}

/// Keeps the embedded prefix and appends the retained rows needed by the target frontier.
fn extend_pool<Node>(
    pool: &'static str,
    target: Height,
    leaf_count: u64,
    mut records: Vec<SubtreeRecord>,
    stored: BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<Node>>,
    root_bytes: impl Fn(&Node) -> [u8; 32],
) -> Result<Vec<SubtreeRecord>, ReleaseTreestateArtifactsError> {
    let completed_subtrees = leaf_count >> TRACKED_SUBTREE_HEIGHT;
    if completed_subtrees > u64::from(u16::MAX) + 1 {
        return Err(
            ReleaseTreestateArtifactsError::UnrepresentableSubtreeCount {
                pool,
                found: completed_subtrees,
            },
        );
    }
    let expected = usize::try_from(completed_subtrees).map_err(|_| {
        ReleaseTreestateArtifactsError::UnrepresentableSubtreeCount {
            pool,
            found: completed_subtrees,
        }
    })?;

    if records.len() > expected {
        return Err(
            ReleaseTreestateArtifactsError::FrontierBeforeEmbeddedArtifact {
                pool,
                target,
                target_count: expected,
                embedded_count: records.len(),
            },
        );
    }

    for (index, data) in stored {
        if data.end_height > target {
            continue;
        }

        let ordinal = usize::from(index.0);
        if ordinal >= expected {
            return Err(ReleaseTreestateArtifactsError::SubtreeBeyondFrontier {
                pool,
                index: index.0,
                end_height: data.end_height,
                target,
                expected,
            });
        }

        let stored_record = SubtreeRecord {
            index,
            end_height: data.end_height,
            root: root_bytes(&data.root),
        };
        if let Some(embedded_record) = records.get(ordinal) {
            if *embedded_record != stored_record {
                return Err(ReleaseTreestateArtifactsError::EmbeddedSubtreeMismatch {
                    pool,
                    index: index.0,
                });
            }
        } else if ordinal == records.len() {
            records.push(stored_record);
        } else {
            return Err(ReleaseTreestateArtifactsError::IncompleteSubtrees {
                pool,
                target,
                expected,
                found: records.len(),
            });
        }
    }

    if records.len() != expected {
        return Err(ReleaseTreestateArtifactsError::IncompleteSubtrees {
            pool,
            target,
            expected,
            found: records.len(),
        });
    }

    Ok(records)
}

/// Returns the total roots in all three pools.
fn artifact_root_count(artifact: &SubtreeArtifact) -> usize {
    artifact.sapling.len() + artifact.orchard.len() + artifact.ironwood.len()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use zakura_chain::{
        ironwood, orchard, parameters::Network, sapling, sprout, subtree::NoteCommitmentSubtree,
    };

    use crate::{
        config::Config,
        service::finalized_state::{
            DiskWriteBatch, STATE_COLUMN_FAMILIES_IN_CODE, STATE_DATABASE_KIND,
        },
        state_database_format_version_in_code,
    };

    use super::*;

    fn ephemeral_db() -> ZakuraDb {
        ZakuraDb::new(
            &Config::ephemeral(),
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &Network::Mainnet,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("opening an ephemeral database should succeed")
    }

    fn sapling_note_commitment(value: u64) -> sapling::tree::NoteCommitmentUpdate {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&value.to_le_bytes());
        Option::<sapling::tree::NoteCommitmentUpdate>::from(
            sapling::tree::NoteCommitmentUpdate::from_bytes(&bytes),
        )
        .expect("small little-endian integers are canonical")
    }

    fn sapling_tree_with_completed_subtrees(count: u64) -> sapling::tree::NoteCommitmentTree {
        let mut tree = sapling::tree::NoteCommitmentTree::default();
        let leaves = count << TRACKED_SUBTREE_HEIGHT;
        const BATCH: u64 = 4096;
        let mut value = 0u64;
        while value < leaves {
            let end = (value + BATCH).min(leaves);
            let batch: Vec<_> = (value..end).map(sapling_note_commitment).collect();
            tree.append_batch(&batch).expect("test tree is not full");
            value = end;
        }
        tree
    }

    #[test]
    fn embedded_prefix_and_later_database_rows_form_one_verified_artifact() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_db();
        let first_tree = sapling_tree_with_completed_subtrees(1);
        let target_tree = sapling_tree_with_completed_subtrees(2);
        let (_, first_root) = first_tree
            .completed_subtree_index_and_root()
            .expect("the first tree completes one subtree");
        let (second_index, second_root) = target_tree
            .completed_subtree_index_and_root()
            .expect("the target tree completes its second subtree");

        // This models a VCT database: index 0 is only in the embedded artifact, while the row
        // completed after the old handoff is retained locally.
        let mut batch = DiskWriteBatch::new();
        batch.insert_sapling_subtree(
            &db,
            &NoteCommitmentSubtree::new(second_index, Height(19), second_root),
        );
        db.write_batch(batch)
            .expect("writing the later row succeeds");

        let previous = SubtreeArtifact {
            last_checkpoint: Height(10),
            sapling: vec![SubtreeRecord {
                index: NoteCommitmentSubtreeIndex(0),
                end_height: Height(7),
                root: first_root.to_bytes(),
            }],
            orchard: Vec::new(),
            ironwood: Vec::new(),
        };
        let frontiers = FinalFrontiers {
            height: Height(20),
            sapling: Arc::new(target_tree),
            orchard: Arc::new(orchard::tree::NoteCommitmentTree::default()),
            sprout: Arc::new(sprout::tree::NoteCommitmentTree::default()),
            ironwood: Arc::new(ironwood::tree::NoteCommitmentTree::default()),
        };

        let extended = extend_subtree_artifact(&db, previous, &frontiers)
            .expect("the embedded prefix and later row are complete");
        let verified = extended
            .verify_against_frontiers(&frontiers.sapling, &frontiers.orchard, &frontiers.ironwood)
            .expect("the combined roots match the target frontier");

        assert_eq!(extended.sapling.len(), 2);
        assert_eq!(extended.sapling[0].root, first_root.to_bytes());
        assert_eq!(extended.sapling[1].root, second_root.to_bytes());
        assert_eq!(verified.sapling, 2);
    }

    #[test]
    fn exporter_rejects_a_vct_database_with_a_newer_handoff() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_db();
        let embedded = embedded_historical_subtrees(&Network::Mainnet)
            .expect("Mainnet has an embedded subtree artifact")
            .last_checkpoint;
        let handoff = Height(embedded.0 + 1);
        let target = Height(handoff.0 + 1);

        let mut batch = DiskWriteBatch::new();
        batch.update_vct_upgrade_marker(&db, Height(0));
        batch.update_vct_sync_marker(&db, handoff);
        db.write_batch(batch)
            .expect("writing the VCT handoff succeeds");

        let error = produce_release_treestate_artifacts(&db, target)
            .expect_err("an older exporter cannot cover the VCT handoff");
        assert_eq!(
            error,
            ReleaseTreestateArtifactsError::EmbeddedArtifactBeforeVctHandoff { embedded, handoff }
        );
    }

    #[test]
    fn matching_overlap_is_allowed_but_rewriting_the_prefix_is_rejected() {
        let root = sapling_crypto::Node::from_bytes([7; 32]).unwrap();
        let embedded = SubtreeRecord {
            index: NoteCommitmentSubtreeIndex(0),
            end_height: Height(4),
            root: root.to_bytes(),
        };
        let mut stored = BTreeMap::new();
        stored.insert(
            embedded.index,
            NoteCommitmentSubtreeData::new(embedded.end_height, root),
        );

        assert_eq!(
            extend_pool(
                "sapling",
                Height(10),
                1 << TRACKED_SUBTREE_HEIGHT,
                vec![embedded],
                stored.clone(),
                |root| root.to_bytes(),
            ),
            Ok(vec![embedded])
        );

        stored.insert(
            embedded.index,
            NoteCommitmentSubtreeData::new(Height(5), root),
        );
        assert_eq!(
            extend_pool(
                "sapling",
                Height(10),
                1 << TRACKED_SUBTREE_HEIGHT,
                vec![embedded],
                stored,
                |root| root.to_bytes(),
            ),
            Err(ReleaseTreestateArtifactsError::EmbeddedSubtreeMismatch {
                pool: "sapling",
                index: 0,
            })
        );
    }

    #[test]
    fn missing_next_database_row_is_rejected() {
        let root = sapling_crypto::Node::from_bytes([7; 32]).unwrap();
        let embedded = SubtreeRecord {
            index: NoteCommitmentSubtreeIndex(0),
            end_height: Height(4),
            root: root.to_bytes(),
        };

        assert_eq!(
            extend_pool(
                "sapling",
                Height(10),
                2 << TRACKED_SUBTREE_HEIGHT,
                vec![embedded],
                BTreeMap::<
                    NoteCommitmentSubtreeIndex,
                    NoteCommitmentSubtreeData<sapling_crypto::Node>,
                >::new(),
                |root| root.to_bytes(),
            ),
            Err(ReleaseTreestateArtifactsError::IncompleteSubtrees {
                pool: "sapling",
                target: Height(10),
                expected: 2,
                found: 1,
            })
        );
    }
}

/// Estimated replay cost of one block, in microseconds, independent of its shielded activity.
///
/// Covers reading the block and walking its transactions. Fitted to quiet Mainnet windows (no
/// note commitments) on `roman-zakura-archive-vct-off` (2026-08-21): median ≈ 249 µs/block.
const COST_PER_BLOCK_US: u64 = 249;

/// Estimated replay cost of appending one note commitment, in microseconds.
///
/// Fitted jointly with [`COST_PER_BLOCK_US`] against 347 cold 500-block windows across Mainnet on
/// `roman-zakura-archive-vct-off` (2026-08-21). Appending dominates wherever there is shielded
/// activity. The previous 1_500 / 47 pair overestimated cost and over-sized the published grid.
const COST_PER_COMMITMENT_US: u64 = 30;

/// How the frontier grid's height spacing is chosen.
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
    /// artifacts.
    Adaptive {
        /// The per-entry cost budget, in microseconds.
        budget_us: u64,
    },
}

/// Why frontier grid generation could not complete.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum FrontierGridExportError {
    /// The database has no finalized tip, so it holds nothing to generate from.
    #[error("this database has no finalized tip to generate a frontier grid from")]
    NoFinalizedTip,

    /// The requested checkpoint is above what the database has finalized.
    #[error("cannot generate a grid for {target:?}: this database is only finalized to {tip:?}")]
    TargetAboveTip {
        /// The requested checkpoint.
        target: Height,
        /// The database's finalized tip.
        tip: Height,
    },

    /// The grid would cover no heights at all.
    #[error("a grid at {target:?} covers no heights")]
    EmptyRange {
        /// The requested checkpoint.
        target: Height,
    },

    /// The grid spacing or cost budget was zero, which would produce an entry at every height.
    #[error("grid spacing and cost budget must be at least 1")]
    ZeroSpacing,

    /// The grid being resumed from reaches at or above the requested checkpoint.
    #[error(
        "cannot resume from a grid whose last entry {seed:?} is not below the requested \
         checkpoint {target:?}"
    )]
    ResumeAboveTarget {
        /// The highest entry in the grid being resumed from.
        seed: Height,
        /// The requested checkpoint.
        target: Height,
    },

    /// A block body needed to place entries is not retained.
    #[error(
        "cannot place frontier grid entries: block body {missing:?} is not retained; generate \
         from an archive database"
    )]
    UnretainedBlockBody {
        /// The first height whose body was missing.
        missing: Height,
    },

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

/// What one frontier grid generation run produced.
#[derive(Clone, Debug)]
pub struct FrontierGridExport {
    /// The frontier grid.
    pub frontiers: FrontierArtifact,

    /// How long generation took.
    pub elapsed: Duration,

    /// How many blocks were replayed.
    pub replayed_blocks: u64,
}

/// Next on-grid height at or below `last`, or `None` if only a partial cell remains.
///
/// A partial cell is not published. Clamping it to `last` would place a tip-local entry that a
/// later export, at a higher `last`, would replace with the unclamped grid point — rewriting the
/// seam and breaking the append-only prefix contract. Serving still covers that tail: it replays
/// from the previous on-grid entry, which is already within one grid step.
fn next_grid_target(
    spacing: GridSpacing,
    next: Height,
    last: Height,
    accrued_cost: &mut u64,
    mut block_commitments: impl FnMut(Height) -> usize,
) -> Option<Height> {
    match spacing {
        GridSpacing::Uniform { blocks } => {
            let target = Height(next.0.saturating_add(blocks));
            (target <= last).then_some(target)
        }
        GridSpacing::Adaptive { budget_us } => {
            let mut candidate = next;
            while candidate <= last {
                let counts = block_commitments(candidate);
                *accrued_cost = accrued_cost
                    .saturating_add(COST_PER_BLOCK_US)
                    // The count is bounded by a block's own commitments, so it fits in u64.
                    .saturating_add(COST_PER_COMMITMENT_US.saturating_mul(counts as u64));
                if *accrued_cost >= budget_us {
                    *accrued_cost = 0;
                    return Some(candidate);
                }
                if candidate == last {
                    return None;
                }
                candidate = Height(candidate.0 + 1);
            }
            None
        }
    }
}

/// Generates the frontier grid covering `[0, target_checkpoint)` at the given `spacing`.
///
/// Each entry's frontiers come from whichever source the database actually holds at that height.
/// Per-height trees are read wherever they exist; retained block bodies are replayed only across
/// a verified-commitment-trees database's absent band `[U, H)`, where no tree was ever written.
/// A legacy archive database has trees at every height, so it generates the whole grid from reads
/// and never replays.
///
/// The walk always starts at genesis and its entry heights are a function of chain data alone, so
/// databases of different shapes, and the same database at different tips, produce grids that are
/// byte-prefix extensions of one another. That is the append-only contract the release-state
/// pipeline checks, and it is why the walk does not start at whatever boundary this particular
/// database happens to have.
///
/// A partial cell at `target_checkpoint` is omitted, so a later export at a higher checkpoint is
/// a prefix of this one plus new on-grid entries. Each emitted entry is root-checked against the
/// authenticated roots the database already holds, and generation stops at the first height that
/// fails, so a returned artifact is one whose every entry matched. That is what lets the grid
/// ship without trust: a consumer re-runs the same check before anchoring on an entry.
///
/// Completed subtree roots are not collected here. The release pipeline
/// ([`produce_release_treestate_artifacts`]) owns that artifact, and it does not need historical
/// block bodies to build it.
///
/// `resume_from` carries an earlier grid's entries forward instead of recomputing them. Each
/// carried entry is re-checked against this database's authenticated roots before it is accepted, so
/// resuming inherits no trust from the file it came from. Placement is unaffected: the cost
/// accumulator resets at every emitted entry, so continuing at `last carried entry + 1` puts the
/// remaining entries exactly where a walk from genesis would. Resuming also makes the output a
/// prefix-extension of the input by construction rather than by both runs agreeing on a budget.
///
/// `on_progress` is called with each emitted entry's height and the running replay count, for
/// long runs.
pub fn export_frontier_grid_to(
    db: &ZakuraDb,
    target_checkpoint: Height,
    spacing: GridSpacing,
    resume_from: Option<&FrontierArtifact>,
    mut on_progress: impl FnMut(Height, u64),
) -> Result<FrontierGridExport, FrontierGridExportError> {
    match spacing {
        GridSpacing::Uniform { blocks: 0 } | GridSpacing::Adaptive { budget_us: 0 } => {
            return Err(FrontierGridExportError::ZeroSpacing)
        }
        _ => {}
    }

    let tip = db
        .finalized_tip_height()
        .ok_or(FrontierGridExportError::NoFinalizedTip)?;
    if target_checkpoint > tip {
        return Err(FrontierGridExportError::TargetAboveTip {
            target: target_checkpoint,
            tip,
        });
    }
    // The covered range is half-open, so the last height the grid can publish is `target - 1`.
    let last = target_checkpoint.0.checked_sub(1).map(Height).ok_or(
        FrontierGridExportError::EmptyRange {
            target: target_checkpoint,
        },
    )?;

    // Only a fast-synced database has heights with no stored tree. Everything outside that band
    // is read rather than replayed, including the pre-upgrade range below `U`.
    let absent_band = db
        .vct_synced_below()
        .map(|handoff| (db.vct_upgrade_height().unwrap_or(Height(0)), handoff));

    let start = Instant::now();
    let mut entries: Vec<FrontierEntry> = Vec::new();
    let mut replayed_blocks = 0u64;

    // The replay cursor, carried across grid steps so the band is covered by one contiguous pass
    // anchored on the previous root-checked endpoint rather than restarted at every entry.
    let mut replay: Option<(DerivedFrontiers, u32)> = None;

    let mut next = Height(0);
    let mut accrued_cost: u64 = 0;

    if let Some(seed) = resume_from {
        if let Some(top) = seed.entries.last() {
            if top.height > last {
                return Err(FrontierGridExportError::ResumeAboveTarget {
                    seed: top.height,
                    target: target_checkpoint,
                });
            }
        }

        // Carried entries are re-checked here, so a stale, truncated, or hostile input costs a
        // failed export rather than an unverified entry in the published artifact.
        for entry in &seed.entries {
            let frontiers = DerivedFrontiers {
                sapling: entry.sapling.clone(),
                orchard: entry.orchard.clone(),
                ironwood: entry.ironwood.clone(),
            };
            verify_against_available_roots(db, entry.height, &frontiers).map_err(|source| {
                FrontierGridExportError::Derivation {
                    height: entry.height,
                    source,
                }
            })?;

            // A carried entry inside the absent band is a verified frontier at its height, which
            // is a nearer replay anchor than the trees below `U`.
            if absent_band
                .is_some_and(|(upgrade, handoff)| entry.height >= upgrade && entry.height < handoff)
            {
                replay = Some((frontiers, entry.height.0 + 1));
            }

            entries.push(entry.clone());
        }

        if let Some(top) = entries.last() {
            next = Height(top.height.0 + 1);
        }
    }

    // A pruned database answers the cost scan with a missing body, which would read as a free
    // block and space entries far too widely. The artifact would still be valid — every entry is
    // root-checked — but it would not bound cold requests, which is the only reason it exists.
    // Record the first such height and fail instead.
    let mut unretained: Option<Height> = None;

    // Under a uniform grid the next entry is a fixed number of blocks ahead. Under an adaptive
    // one it is wherever the estimated cost budget runs out, which needs a block-by-block scan
    // of the commitment counts to find. Either way, a target past `last` is a partial cell and
    // is not published.
    while let Some(target) = next_grid_target(spacing, next, last, &mut accrued_cost, |height| {
        match db.block(height.into()) {
            Some(block) => {
                block.sapling_note_commitments().count()
                    + block.orchard_note_commitments().count()
                    + block.ironwood_note_commitments().count()
            }
            None => {
                unretained.get_or_insert(height);
                0
            }
        }
    }) {
        if let Some(missing) = unretained {
            return Err(FrontierGridExportError::UnretainedBlockBody { missing });
        }

        let in_absent_band =
            absent_band.is_some_and(|(upgrade, handoff)| target >= upgrade && target < handoff);

        let frontiers = if in_absent_band {
            let (carried, replay_from) = match replay.take() {
                Some(cursor) => cursor,
                // Entering the band: anchor on the trees stored just below it, or on empty
                // frontiers when the band starts at genesis.
                None => {
                    let upgrade = absent_band
                        .expect("a height inside the band implies the band exists")
                        .0;
                    match stored_frontier_before_absent_band(db, upgrade).map_err(|source| {
                        FrontierGridExportError::Derivation {
                            height: upgrade,
                            source,
                        }
                    })? {
                        Some((anchor, frontiers)) => (frontiers, anchor.0 + 1),
                        None => (DerivedFrontiers::empty(), 0),
                    }
                }
            };

            let derived = replay_with_subtrees(db, target, replay_from, carried, |_, _| {})
                .map_err(|source| FrontierGridExportError::Derivation {
                    height: target,
                    source,
                })?;
            replayed_blocks += u64::from(target.0 - replay_from) + 1;
            derived
        } else {
            stored_frontiers_at(db, target).map_err(|source| {
                FrontierGridExportError::Derivation {
                    height: target,
                    source,
                }
            })?
        };

        // The root check is what makes this entry safe to publish.
        verify_against_available_roots(db, target, &frontiers).map_err(|source| {
            FrontierGridExportError::Derivation {
                height: target,
                source,
            }
        })?;

        entries.push(FrontierEntry {
            height: target,
            sapling: frontiers.sapling.clone(),
            orchard: frontiers.orchard.clone(),
            ironwood: frontiers.ironwood.clone(),
        });
        on_progress(target, replayed_blocks);

        if in_absent_band {
            replay = Some((frontiers, target.0 + 1));
        }

        if target == last {
            break;
        }

        next = Height(target.0 + 1);
    }

    if let Some(missing) = unretained {
        return Err(FrontierGridExportError::UnretainedBlockBody { missing });
    }

    Ok(FrontierGridExport {
        frontiers: FrontierArtifact {
            spacing: match spacing {
                GridSpacing::Uniform { blocks } => blocks,
                // Recorded as provenance only; consumers locate entries by searching.
                GridSpacing::Adaptive { .. } => 0,
            },
            last_checkpoint: target_checkpoint,
            entries,
        },
        elapsed: start.elapsed(),
        replayed_blocks,
    })
}

#[cfg(test)]
mod grid_target_tests {
    use super::*;

    fn published_grid_heights(
        spacing: GridSpacing,
        start: Height,
        last: Height,
        mut commitments: impl FnMut(Height) -> usize,
    ) -> Vec<u32> {
        let mut heights = Vec::new();
        let mut next = start;
        let mut accrued_cost = 0;
        while let Some(target) =
            next_grid_target(spacing, next, last, &mut accrued_cost, &mut commitments)
        {
            heights.push(target.0);
            if target == last {
                break;
            }
            next = Height(target.0 + 1);
        }
        heights
    }

    #[test]
    fn uniform_grid_omits_a_partial_final_cell() {
        let spacing = GridSpacing::Uniform { blocks: 10 };

        assert_eq!(
            published_grid_heights(spacing, Height(0), Height(25), |_| 0),
            vec![10, 21],
            "the cell that would clamp to 25 is not published"
        );
        assert_eq!(
            published_grid_heights(spacing, Height(0), Height(10), |_| 0),
            vec![10],
            "a target that lands exactly on last is on-grid and is published"
        );
        assert!(
            published_grid_heights(spacing, Height(0), Height(9), |_| 0).is_empty(),
            "a band shorter than one step publishes nothing rather than a clamped entry"
        );
    }

    #[test]
    fn uniform_grid_heights_are_prefix_compatible_across_tips() {
        let spacing = GridSpacing::Uniform { blocks: 10 };
        let earlier = published_grid_heights(spacing, Height(0), Height(25), |_| 0);
        let later = published_grid_heights(spacing, Height(0), Height(35), |_| 0);

        assert_eq!(&later[..earlier.len()], earlier.as_slice());
        assert_eq!(later, vec![10, 21, 32]);
        assert!(
            !later.contains(&25),
            "the height a clamp would have published at the earlier tip is not a later grid point"
        );
    }

    #[test]
    fn adaptive_grid_omits_a_partial_final_cell() {
        // Three empty blocks cost 747 µs, so a 700 µs budget fires every third height.
        let spacing = GridSpacing::Adaptive { budget_us: 700 };

        assert_eq!(
            published_grid_heights(spacing, Height(0), Height(10), |_| 0),
            vec![2, 5, 8],
            "the incomplete cell ending at last is omitted"
        );
        assert_eq!(
            published_grid_heights(spacing, Height(0), Height(11), |_| 0),
            vec![2, 5, 8, 11],
            "last is published when it is a real budget boundary"
        );
    }

    #[test]
    fn adaptive_grid_heights_are_prefix_compatible_across_tips() {
        let spacing = GridSpacing::Adaptive { budget_us: 700 };
        let earlier = published_grid_heights(spacing, Height(0), Height(10), |_| 0);
        let later = published_grid_heights(spacing, Height(0), Height(11), |_| 0);

        assert_eq!(&later[..earlier.len()], earlier.as_slice());
        assert_eq!(later, vec![2, 5, 8, 11]);
        assert!(
            !later.contains(&10),
            "the height a clamp would have published at the earlier tip is not a later grid point"
        );
    }
}
