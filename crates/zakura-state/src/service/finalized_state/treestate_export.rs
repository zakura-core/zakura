//! Producing the coupled frontier and completed-subtree artifacts for a release checkpoint.
//!
//! The embedded subtree artifact supplies the reviewed prefix through the current binary's
//! checkpoint. The finalized database supplies rows completed after that checkpoint. Combining
//! those two sources works for both legacy and verified-commitment-trees databases, and does not
//! need historical block bodies or per-height frontiers from the skipped range.

use std::collections::BTreeMap;

use thiserror::Error;

use zakura_chain::{
    block::Height,
    subtree::{NoteCommitmentSubtreeData, NoteCommitmentSubtreeIndex, TRACKED_SUBTREE_HEIGHT},
};

use super::{
    commitment_aux::{
        produce_settled_final_frontiers, FinalFrontiers, FinalFrontiersGenerationError,
    },
    treestate_artifact::{
        embedded_historical_subtrees, SubtreeArtifact, SubtreeRecord, TreestateArtifactError,
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
