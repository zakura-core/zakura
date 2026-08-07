//! Generating the completed-subtree-root artifact from an archive database.
//!
//! The artifact is bound to the network last checkpoint, not to how the source database was
//! synced. A legacy archive that still holds per-height frontiers at `H - 1` exports its stored
//! subtree rows directly. A verified-commitment-trees fast-synced database that lacks those
//! frontiers falls back to authenticated replay across the absent band (see
//! `docs/design/verified-commitment-trees.md` §16).
//!
//! # Direct export
//!
//! When Sapling, Orchard, and Ironwood frontiers are present at `H - 1`, each pool's leaf count
//! floor-divides to the number of completed subtrees the artifact must contain. The stored
//! `{pool}_note_commitment_subtree` rows are then required to be exactly the contiguous indexes
//! `0..expected`, each with `end_height < H`. That proves completeness against the frontiers; the
//! individual roots remain review-trusted, matching the artifact's published trust model.
//!
//! # Replay fallback
//!
//! When those frontiers are absent, generation requires a VCT absent-band marker that matches the
//! network checkpoint. Subtree roots are interior nodes computed during a replay whose endpoints
//! are checked against `commitment_roots_by_height`. Between two checked endpoints the replay is
//! pinned — producing the correct end root from a correct start root while computing a wrong
//! interior node would require a hash collision. Stored rows completed before the upgrade height
//! `U` are validated against the `U - 1` frontiers and prepended to the replayed band.

use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use thiserror::Error;

use zakura_chain::{
    block::Height,
    subtree::{NoteCommitmentSubtreeData, NoteCommitmentSubtreeIndex, TRACKED_SUBTREE_HEIGHT},
};

use crate::service::{
    finalized_state::{
        treestate_artifact::{SubtreeArtifact, SubtreeRecord, TreestateArtifactError},
        ZakuraDb,
    },
    read::{
        historical_tree::{
            replay_with_subtrees, verify_against_index, HistoricalTreeDerivationError, ShieldedPool,
        },
        DerivedFrontiers,
    },
};

/// How often the replay stops to check itself against the stored roots.
///
/// Only the endpoints are load-bearing for correctness, but checking along the way turns "the
/// export failed" into "the export failed at height N", which is the difference between a
/// diagnosable bug and a bisect over three million blocks.
const CHECKPOINT_INTERVAL: u32 = 100_000;

/// Why artifact generation could not complete.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum TreestateExportError {
    /// The network last checkpoint is zero, so there is no pre-last-checkpoint band to export.
    #[error("network last checkpoint is genesis; there is no historical subtree band to export")]
    InvalidLastCheckpoint,

    /// The finalized tip has not reached the height immediately below the network checkpoint.
    #[error(
        "finalized tip {tip:?} has not reached pre-last-checkpoint height {required:?} \
         for network checkpoint {last_checkpoint:?}"
    )]
    TipBelowLastCheckpoint {
        /// The database's finalized tip, if any.
        tip: Option<Height>,
        /// The height the tip must reach (`last_checkpoint - 1`).
        required: Height,
        /// The network last checkpoint.
        last_checkpoint: Height,
    },

    /// The generated roots do not match the frontiers they were generated against.
    ///
    /// Generation fails rather than writing roots that cannot be proven, which is the whole point
    /// of generating them from an authenticated database.
    #[error("generated subtree roots do not match the frontiers at {bound:?}: {source}")]
    SubtreeRootsUnverified {
        /// The height whose frontiers the roots were checked against.
        bound: Height,
        /// Why the check failed.
        #[source]
        source: TreestateArtifactError,
    },

    /// One or more pre-last-checkpoint frontiers are present while others are missing.
    #[error("pre-last-checkpoint frontiers are incomplete: missing {missing:?}")]
    IncompleteFrontiers {
        /// Pools whose frontier at `last_checkpoint - 1` is missing.
        missing: Vec<&'static str>,
    },

    /// Stored subtree rows for a pool do not form the contiguous completed range the frontier
    /// requires.
    #[error(
        "{pool} subtree rows below {bound:?} are incomplete or inconsistent: \
         expected contiguous indexes 0..{expected}, {detail}"
    )]
    IncompleteStoredSubtrees {
        /// Which pool failed validation.
        pool: &'static str,
        /// The height bound records must complete strictly below.
        bound: Height,
        /// How many completed subtrees the frontier required.
        expected: usize,
        /// What was wrong with the stored rows.
        detail: String,
    },

    /// A frontier requires more completed subtrees than the artifact format can index.
    #[error(
        "{pool} frontier requires {found} completed subtrees, which the artifact format cannot represent"
    )]
    UnrepresentableSubtreeCount {
        /// Which pool has too many completed subtrees.
        pool: &'static str,
        /// The completed-subtree count implied by the frontier.
        found: u64,
    },

    /// The database has no pre-last-checkpoint frontiers and no usable VCT absent band to replay.
    #[error(
        "cannot export historical subtrees: pre-last-checkpoint frontiers are absent and the database \
         has no verified-commitment-trees absent band for network checkpoint {last_checkpoint:?}"
    )]
    NoExportSource {
        /// The network last checkpoint.
        last_checkpoint: Height,
    },

    /// The VCT fast-sync marker does not match the network last checkpoint.
    #[error(
        "verified-commitment-trees last-checkpoint marker {marked:?} does not match \
         network checkpoint {last_checkpoint:?}"
    )]
    MismatchedVctLastCheckpoint {
        /// The last checkpoint recorded in the database marker.
        marked: Height,
        /// The network last checkpoint.
        last_checkpoint: Height,
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

/// What one generation run produced.
#[derive(Clone, Debug)]
pub struct TreestateExport {
    /// The completed subtree roots.
    pub subtrees: SubtreeArtifact,

    /// How long generation took.
    pub elapsed: Duration,

    /// How many blocks were replayed.
    ///
    /// Zero for a direct stored-row export from a legacy archive.
    pub replayed_blocks: u64,

    /// How many subtree roots were proven against the frontiers at the export bound.
    ///
    /// Always the artifact's total record count: generation fails rather than returning roots it
    /// could not prove.
    pub verified_roots: usize,
}

/// Proves `subtrees` against the frontiers it was generated against, at `bound`.
///
/// The count check in [`collect_stored_pool`] and the endpoint root checks in the replay both
/// leave the root values themselves untested — they are interior nodes. Folding them back into
/// the frontier's own interior nodes is what tests them, and it is nearly free here because the
/// frontiers are already in hand.
fn verify_exported_subtrees(
    subtrees: &SubtreeArtifact,
    frontiers: &DerivedFrontiers,
    bound: Height,
) -> Result<usize, TreestateExportError> {
    subtrees
        .verify_against_frontiers(&frontiers.sapling, &frontiers.orchard, &frontiers.ironwood)
        .map(|counts| counts.total())
        .map_err(|source| TreestateExportError::SubtreeRootsUnverified { bound, source })
}

/// Generates the subtree-root artifact for the network last checkpoint.
///
/// Prefers a direct export from stored subtree rows when the pre-last-checkpoint frontiers are present.
/// Otherwise requires a matching VCT absent-band marker and replays the band, checking each
/// checkpoint against `commitment_roots_by_height`.
pub fn export(
    db: &ZakuraDb,
    mut on_progress: impl FnMut(Height, u64),
) -> Result<TreestateExport, TreestateExportError> {
    let last_checkpoint = db.network().checkpoint_list().max_height();
    if last_checkpoint.0 == 0 {
        return Err(TreestateExportError::InvalidLastCheckpoint);
    }

    let pre_last_checkpoint = Height(last_checkpoint.0 - 1);
    let tip = db.finalized_tip_height();
    if tip.is_none_or(|tip| tip < pre_last_checkpoint) {
        return Err(TreestateExportError::TipBelowLastCheckpoint {
            tip,
            required: pre_last_checkpoint,
            last_checkpoint,
        });
    }

    let start = Instant::now();

    match probe_pre_last_checkpoint_frontiers(db, pre_last_checkpoint)? {
        FrontierProbe::Present(frontiers) => {
            let subtrees = export_stored(db, last_checkpoint, pre_last_checkpoint, &frontiers)?;
            let verified_roots =
                verify_exported_subtrees(&subtrees, &frontiers, pre_last_checkpoint)?;
            on_progress(pre_last_checkpoint, 0);
            Ok(TreestateExport {
                subtrees,
                elapsed: start.elapsed(),
                replayed_blocks: 0,
                verified_roots,
            })
        }
        FrontierProbe::Absent => export_by_replay(
            db,
            last_checkpoint,
            pre_last_checkpoint,
            start,
            &mut on_progress,
        ),
    }
}

/// Whether the three pre-last-checkpoint frontiers are all present, all absent, or mixed.
enum FrontierProbe {
    /// Sapling, Orchard, and Ironwood frontiers at the probed height.
    Present(DerivedFrontiers),
    /// All three frontiers are absent.
    Absent,
}

/// Probes Sapling, Orchard, and Ironwood frontiers at `height`.
///
/// Heights inside a VCT absent band are treated as absent without reading trees. Outside that
/// band, the sparse per-height tree store is searched with `latest_stored_*_tree`, which returns
/// the frontier that is the state at `height` without panicking when rows are missing. All three
/// pools must agree: a mixed presence is an inconsistent database and fails closed.
fn probe_pre_last_checkpoint_frontiers(
    db: &ZakuraDb,
    height: Height,
) -> Result<FrontierProbe, TreestateExportError> {
    if db.vct_tree_absent(height) {
        return Ok(FrontierProbe::Absent);
    }

    let sapling = db.latest_stored_sapling_tree(&height);
    let orchard = db.latest_stored_orchard_tree(&height);
    let ironwood = db.latest_stored_ironwood_tree(&height);

    match (sapling, orchard, ironwood) {
        (Some(sapling), Some(orchard), Some(ironwood)) => {
            Ok(FrontierProbe::Present(DerivedFrontiers {
                sapling,
                orchard,
                ironwood,
            }))
        }
        (None, None, None) => Ok(FrontierProbe::Absent),
        (sapling, orchard, ironwood) => {
            let mut missing = Vec::new();
            if sapling.is_none() {
                missing.push("sapling");
            }
            if orchard.is_none() {
                missing.push("orchard");
            }
            if ironwood.is_none() {
                missing.push("ironwood");
            }
            Err(TreestateExportError::IncompleteFrontiers { missing })
        }
    }
}

/// Exports stored subtree rows validated against `frontiers` at `bound`.
fn export_stored(
    db: &ZakuraDb,
    last_checkpoint: Height,
    bound: Height,
    frontiers: &DerivedFrontiers,
) -> Result<SubtreeArtifact, TreestateExportError> {
    Ok(SubtreeArtifact {
        last_checkpoint,
        sapling: collect_stored_pool(
            "sapling",
            bound,
            frontiers.sapling.count(),
            db.sapling_subtree_list_by_index_range(..),
            |root| root.to_bytes(),
        )?,
        orchard: collect_stored_pool(
            "orchard",
            bound,
            frontiers.orchard.count(),
            db.orchard_subtree_list_by_index_range(..),
            |root| root.to_repr(),
        )?,
        ironwood: collect_stored_pool(
            "ironwood",
            bound,
            frontiers.ironwood.count(),
            db.ironwood_subtree_list_by_index_range(..),
            |root| root.to_repr(),
        )?,
    })
}

/// Validates and collects stored subtree rows completed strictly below `bound`.
///
/// `leaf_count` is the pool's note-commitment count at `bound`. The expected completed-subtree
/// count is `leaf_count >> TRACKED_SUBTREE_HEIGHT`. Stored rows must supply exactly indexes
/// `0..expected`, each with `end_height < bound`, and no additional below-bound rows.
fn collect_stored_pool<Node>(
    pool: &'static str,
    bound: Height,
    leaf_count: u64,
    stored: BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<Node>>,
    root_bytes: impl Fn(&Node) -> [u8; 32],
) -> Result<Vec<SubtreeRecord>, TreestateExportError> {
    let completed_subtrees = leaf_count >> TRACKED_SUBTREE_HEIGHT;
    if completed_subtrees > u64::from(u16::MAX) + 1 {
        return Err(TreestateExportError::UnrepresentableSubtreeCount {
            pool,
            found: completed_subtrees,
        });
    }
    let expected = usize::try_from(completed_subtrees).map_err(|_| {
        TreestateExportError::UnrepresentableSubtreeCount {
            pool,
            found: completed_subtrees,
        }
    })?;

    let below_bound: Vec<_> = stored
        .into_iter()
        .filter(|(_, data)| data.end_height < bound)
        .collect();

    if below_bound.len() != expected {
        return Err(TreestateExportError::IncompleteStoredSubtrees {
            pool,
            bound,
            expected,
            detail: format!("found {} below-bound rows", below_bound.len()),
        });
    }

    let mut records = Vec::with_capacity(expected);
    for (offset, (index, data)) in below_bound.into_iter().enumerate() {
        let expected_index = NoteCommitmentSubtreeIndex(u16::try_from(offset).map_err(|_| {
            TreestateExportError::UnrepresentableSubtreeCount {
                pool,
                found: completed_subtrees,
            }
        })?);
        if index != expected_index {
            return Err(TreestateExportError::IncompleteStoredSubtrees {
                pool,
                bound,
                expected,
                detail: format!("expected index {}, found {}", expected_index.0, index.0),
            });
        }

        records.push(SubtreeRecord {
            index,
            end_height: data.end_height,
            root: root_bytes(&data.root),
        });
    }

    Ok(records)
}

/// Replays the VCT absent band and prepends any validated pre-upgrade stored rows.
fn export_by_replay(
    db: &ZakuraDb,
    last_checkpoint: Height,
    last: Height,
    start: Instant,
    on_progress: &mut impl FnMut(Height, u64),
) -> Result<TreestateExport, TreestateExportError> {
    let marked = db
        .vct_synced_below()
        .ok_or(TreestateExportError::NoExportSource { last_checkpoint })?;
    if marked != last_checkpoint {
        return Err(TreestateExportError::MismatchedVctLastCheckpoint {
            marked,
            last_checkpoint,
        });
    }

    let upgrade = db.vct_upgrade_height().unwrap_or(Height(0));
    if upgrade >= last_checkpoint {
        return Err(TreestateExportError::NoExportSource { last_checkpoint });
    }

    let (mut subtrees, mut frontiers) = if upgrade.0 == 0 {
        (
            SubtreeArtifact {
                last_checkpoint,
                ..SubtreeArtifact::default()
            },
            DerivedFrontiers::empty(),
        )
    } else {
        let pre_upgrade = Height(upgrade.0 - 1);
        let frontiers = match probe_pre_last_checkpoint_frontiers(db, pre_upgrade)? {
            FrontierProbe::Present(frontiers) => frontiers,
            FrontierProbe::Absent => {
                return Err(TreestateExportError::IncompleteFrontiers {
                    missing: vec!["sapling", "orchard", "ironwood"],
                });
            }
        };
        (
            export_stored(db, last_checkpoint, upgrade, &frontiers)?,
            frontiers,
        )
    };

    let mut replay_from = upgrade.0;
    let mut replayed_blocks = 0u64;

    loop {
        let target = Height(replay_from.saturating_add(CHECKPOINT_INTERVAL).min(last.0));

        let mut collected = Vec::new();
        frontiers = replay_with_subtrees(db, target, replay_from, frontiers, |pool, completed| {
            collected.push((pool, completed))
        })
        .map_err(|source| TreestateExportError::Derivation {
            height: target,
            source,
        })?;

        replayed_blocks += u64::from(target.0 - replay_from) + 1;
        replay_from = target.0 + 1;

        // The root check is what makes every subtree recorded since the previous check safe to
        // publish.
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

        on_progress(target, replayed_blocks);

        if target == last {
            break;
        }
    }

    // `frontiers` is the replay's final state, at `last`, which is the artifact's bound.
    let verified_roots = verify_exported_subtrees(&subtrees, &frontiers, last)?;

    Ok(TreestateExport {
        subtrees,
        elapsed: start.elapsed(),
        replayed_blocks,
        verified_roots,
    })
}

#[cfg(test)]
mod tests {
    use zakura_chain::{
        block::{self, Height},
        ironwood, orchard,
        parameters::{
            testnet::{ConfiguredActivationHeights, ConfiguredCheckpoints, ParametersBuilder},
            Network,
        },
        sapling,
        subtree::{NoteCommitmentSubtree, NoteCommitmentSubtreeIndex, TRACKED_SUBTREE_HEIGHT},
    };

    use crate::{
        config::Config,
        service::finalized_state::{
            DiskWriteBatch, WriteDisk, ZakuraDb, STATE_COLUMN_FAMILIES_IN_CODE, STATE_DATABASE_KIND,
        },
        state_database_format_version_in_code,
    };

    use super::*;

    const LAST_CHECKPOINT: Height = Height(10);
    const PRE_LAST_CHECKPOINT: Height = Height(9);

    fn export_test_network() -> Network {
        let genesis = "05a60a92d99d85997cce3b87616c089f6124d7342af37106edc76126334a2c38"
            .parse()
            .expect("testnet genesis hash parses");

        ParametersBuilder::default()
            .with_activation_heights(ConfiguredActivationHeights {
                before_overwinter: Some(1),
                overwinter: Some(2),
                sapling: Some(3),
                blossom: Some(4),
                heartwood: Some(5),
                canopy: Some(6),
                nu5: Some(7),
                nu6: Some(8),
                nu6_1: Some(9),
                nu6_2: Some(10),
                nu6_3: Some(11),
                nu7: Some(12),
            })
            .expect("activation heights are ordered")
            .disable_temporary_orchard_disabling_soft_fork()
            .with_checkpoints(ConfiguredCheckpoints::HeightsAndHashes(vec![
                (Height(0), genesis),
                (LAST_CHECKPOINT, block::Hash([2; 32])),
            ]))
            .expect("custom checkpoints are valid")
            .extend_funding_streams()
            .to_network()
            .expect("export test network builds")
    }

    fn ephemeral_db(network: &Network) -> ZakuraDb {
        ZakuraDb::new(
            &Config::ephemeral(),
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            network,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("opening an ephemeral database should succeed")
    }

    fn set_tip(db: &ZakuraDb, tip: Height) {
        let hash_by_height = db.db().cf_handle("hash_by_height").unwrap();
        let height_by_hash = db.db().cf_handle("height_by_hash").unwrap();
        let hash = block::Hash([1; 32]);
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(&hash_by_height, tip, hash);
        batch.zs_insert(&height_by_hash, hash, tip);
        db.write_batch(batch)
            .expect("canonical block index writes succeed");
    }

    fn store_empty_pre_last_checkpoint_frontiers(db: &ZakuraDb) {
        let mut batch = DiskWriteBatch::new();
        batch.create_sapling_tree(
            db,
            &PRE_LAST_CHECKPOINT,
            &sapling::tree::NoteCommitmentTree::default(),
        );
        batch.create_orchard_tree(
            db,
            &PRE_LAST_CHECKPOINT,
            &orchard::tree::NoteCommitmentTree::default(),
        );
        batch.create_ironwood_tree(
            db,
            &PRE_LAST_CHECKPOINT,
            &ironwood::tree::NoteCommitmentTree::default(),
        );
        db.write_batch(batch)
            .expect("writing empty pre-last-checkpoint frontiers succeeds");
    }

    fn sapling_note_commitment(value: u64) -> sapling::tree::NoteCommitmentUpdate {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&value.to_le_bytes());
        Option::<sapling::tree::NoteCommitmentUpdate>::from(
            sapling::tree::NoteCommitmentUpdate::from_bytes(&bytes),
        )
        .expect("small little-endian integers are canonical")
    }

    fn sapling_tree_with_completed_subtrees(count: usize) -> sapling::tree::NoteCommitmentTree {
        let mut tree = sapling::tree::NoteCommitmentTree::default();
        let leaves = (count as u64) << TRACKED_SUBTREE_HEIGHT;
        // Append in batches so the fixture stays cheap enough for unit tests.
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
    fn collect_stored_pool_requires_contiguous_below_bound_rows() {
        let bound = Height(10);
        let root = sapling_crypto::Node::from_bytes([7; 32]).unwrap();
        let mut stored = BTreeMap::new();
        stored.insert(
            NoteCommitmentSubtreeIndex(0),
            NoteCommitmentSubtreeData::new(Height(3), root),
        );
        stored.insert(
            NoteCommitmentSubtreeIndex(1),
            NoteCommitmentSubtreeData::new(Height(8), root),
        );
        // Completing at the bound itself must be ignored for the below-bound set.
        stored.insert(
            NoteCommitmentSubtreeIndex(2),
            NoteCommitmentSubtreeData::new(bound, root),
        );

        let records = collect_stored_pool(
            "sapling",
            bound,
            2 << TRACKED_SUBTREE_HEIGHT,
            stored.clone(),
            |root| root.to_bytes(),
        )
        .expect("two below-bound rows match the frontier");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].index.0, 0);
        assert_eq!(records[1].end_height, Height(8));

        stored.remove(&NoteCommitmentSubtreeIndex(1));
        assert!(matches!(
            collect_stored_pool(
                "sapling",
                bound,
                2 << TRACKED_SUBTREE_HEIGHT,
                stored,
                |root| root.to_bytes(),
            ),
            Err(TreestateExportError::IncompleteStoredSubtrees { expected: 2, .. })
        ));

        assert_eq!(
            collect_stored_pool(
                "sapling",
                bound,
                (u64::from(u16::MAX) + 2) << TRACKED_SUBTREE_HEIGHT,
                BTreeMap::<
                    NoteCommitmentSubtreeIndex,
                    NoteCommitmentSubtreeData<sapling_crypto::Node>,
                >::new(),
                |root| root.to_bytes(),
            ),
            Err(TreestateExportError::UnrepresentableSubtreeCount {
                pool: "sapling",
                found: u64::from(u16::MAX) + 2,
            })
        );
    }

    #[test]
    fn legacy_archive_exports_empty_stored_rows_without_vct_marker() {
        let _init_guard = zakura_test::init();
        let network = export_test_network();
        let db = ephemeral_db(&network);
        set_tip(&db, PRE_LAST_CHECKPOINT);
        store_empty_pre_last_checkpoint_frontiers(&db);

        let export = export(&db, |_, _| {}).expect("legacy direct export succeeds");
        assert_eq!(export.subtrees.last_checkpoint, LAST_CHECKPOINT);
        assert!(export.subtrees.sapling.is_empty());
        assert!(export.subtrees.orchard.is_empty());
        assert!(export.subtrees.ironwood.is_empty());
        assert_eq!(export.replayed_blocks, 0);
        assert!(db.vct_synced_below().is_none());
    }

    #[test]
    fn legacy_archive_exports_completed_subtree_rows_and_excludes_last_checkpoint_completion() {
        let _init_guard = zakura_test::init();
        let network = export_test_network();
        let db = ephemeral_db(&network);
        set_tip(&db, PRE_LAST_CHECKPOINT);

        let sapling_tree = sapling_tree_with_completed_subtrees(1);
        // Export proves the roots it emits against this tree, so the seeded row has to carry the
        // root the tree actually completed, the way a real database does.
        let (_, sapling_root) = sapling_tree
            .completed_subtree_index_and_root()
            .expect("the fixture tree completes exactly one subtree");
        let post_bound_root = sapling_crypto::Node::from_bytes([9; 32]).unwrap();
        let mut batch = DiskWriteBatch::new();
        batch.create_sapling_tree(&db, &PRE_LAST_CHECKPOINT, &sapling_tree);
        batch.create_orchard_tree(
            &db,
            &PRE_LAST_CHECKPOINT,
            &orchard::tree::NoteCommitmentTree::default(),
        );
        batch.create_ironwood_tree(
            &db,
            &PRE_LAST_CHECKPOINT,
            &ironwood::tree::NoteCommitmentTree::default(),
        );
        batch.insert_sapling_subtree(
            &db,
            &NoteCommitmentSubtree::new(0u16, Height(4), sapling_root),
        );
        // A subtree that completes at the last checkpoint itself is post-bound and must not be exported.
        batch.insert_sapling_subtree(
            &db,
            &NoteCommitmentSubtree::new(1u16, LAST_CHECKPOINT, post_bound_root),
        );
        db.write_batch(batch).expect("seeding stored rows succeeds");

        let export = export(&db, |_, _| {}).expect("legacy direct export succeeds");
        assert_eq!(export.replayed_blocks, 0);
        assert_eq!(export.subtrees.sapling.len(), 1);
        assert_eq!(export.subtrees.sapling[0].index.0, 0);
        assert_eq!(export.subtrees.sapling[0].end_height, Height(4));
        assert_eq!(export.subtrees.sapling[0].root, sapling_root.to_bytes());
        assert!(export.subtrees.orchard.is_empty());
        assert!(export.subtrees.ironwood.is_empty());
    }

    #[test]
    fn tip_below_last_checkpoint_is_rejected() {
        let _init_guard = zakura_test::init();
        let network = export_test_network();
        let db = ephemeral_db(&network);
        set_tip(&db, Height(8));

        assert_eq!(
            export(&db, |_, _| {}).expect_err("tip below last checkpoint fails"),
            TreestateExportError::TipBelowLastCheckpoint {
                tip: Some(Height(8)),
                required: PRE_LAST_CHECKPOINT,
                last_checkpoint: LAST_CHECKPOINT,
            }
        );
    }

    #[test]
    fn gapped_stored_rows_are_rejected() {
        let _init_guard = zakura_test::init();
        let network = export_test_network();
        let db = ephemeral_db(&network);
        set_tip(&db, PRE_LAST_CHECKPOINT);

        let sapling_tree = sapling_tree_with_completed_subtrees(2);
        let sapling_root = sapling_crypto::Node::from_bytes([3; 32]).unwrap();
        let mut batch = DiskWriteBatch::new();
        batch.create_sapling_tree(&db, &PRE_LAST_CHECKPOINT, &sapling_tree);
        batch.create_orchard_tree(
            &db,
            &PRE_LAST_CHECKPOINT,
            &orchard::tree::NoteCommitmentTree::default(),
        );
        batch.create_ironwood_tree(
            &db,
            &PRE_LAST_CHECKPOINT,
            &ironwood::tree::NoteCommitmentTree::default(),
        );
        batch.insert_sapling_subtree(
            &db,
            &NoteCommitmentSubtree::new(0u16, Height(2), sapling_root),
        );
        // Gap at index 1; index 2 alone cannot satisfy a contiguous 0..2 range.
        batch.insert_sapling_subtree(
            &db,
            &NoteCommitmentSubtree::new(2u16, Height(7), sapling_root),
        );
        db.write_batch(batch).expect("seeding gapped rows succeeds");

        assert!(matches!(
            export(&db, |_, _| {}).expect_err("gapped rows fail"),
            TreestateExportError::IncompleteStoredSubtrees {
                pool: "sapling",
                expected: 2,
                ..
            }
        ));
    }

    #[test]
    fn mismatched_vct_last_checkpoint_is_rejected() {
        let _init_guard = zakura_test::init();
        let network = export_test_network();
        let db = ephemeral_db(&network);
        set_tip(&db, PRE_LAST_CHECKPOINT);

        let mut batch = DiskWriteBatch::new();
        batch.update_vct_upgrade_marker(&db, Height(0));
        batch.update_vct_sync_marker(&db, Height(99));
        db.write_batch(batch).expect("seeding vct markers succeeds");

        assert_eq!(
            export(&db, |_, _| {}).expect_err("mismatched VCT last checkpoint fails"),
            TreestateExportError::MismatchedVctLastCheckpoint {
                marked: Height(99),
                last_checkpoint: LAST_CHECKPOINT,
            }
        );
    }

    #[test]
    fn absent_frontiers_without_vct_marker_fail_closed() {
        let _init_guard = zakura_test::init();
        let network = export_test_network();
        let db = ephemeral_db(&network);
        set_tip(&db, PRE_LAST_CHECKPOINT);

        assert_eq!(
            export(&db, |_, _| {}).expect_err("no export source fails"),
            TreestateExportError::NoExportSource {
                last_checkpoint: LAST_CHECKPOINT
            }
        );
    }

    #[test]
    fn vct_replay_path_is_selected_when_pre_last_checkpoint_frontiers_are_absent() {
        let _init_guard = zakura_test::init();
        let network = export_test_network();
        let db = ephemeral_db(&network);
        set_tip(&db, PRE_LAST_CHECKPOINT);

        let mut batch = DiskWriteBatch::new();
        batch.update_vct_upgrade_marker(&db, Height(0));
        batch.update_vct_sync_marker(&db, LAST_CHECKPOINT);
        db.write_batch(batch).expect("seeding vct markers succeeds");

        // The replay path is selected; with no retained bodies it fails while producing the
        // first checkpoint target (the whole pre-last-checkpoint band fits in one interval here).
        assert!(matches!(
            export(&db, |_, _| {}).expect_err("empty VCT replay fails closed"),
            TreestateExportError::Derivation {
                height: PRE_LAST_CHECKPOINT,
                source: HistoricalTreeDerivationError::MissingBlockBody { .. },
            }
        ));
    }

    #[test]
    fn vct_replay_with_upgrade_requires_pre_upgrade_frontiers() {
        let _init_guard = zakura_test::init();
        let network = export_test_network();
        let db = ephemeral_db(&network);
        set_tip(&db, PRE_LAST_CHECKPOINT);

        let mut batch = DiskWriteBatch::new();
        batch.update_vct_upgrade_marker(&db, Height(4));
        batch.update_vct_sync_marker(&db, LAST_CHECKPOINT);
        db.write_batch(batch).expect("seeding vct markers succeeds");

        assert_eq!(
            export(&db, |_, _| {}).expect_err("missing pre-upgrade frontiers fail"),
            TreestateExportError::IncompleteFrontiers {
                missing: vec!["sapling", "orchard", "ironwood"],
            }
        );
    }

    #[test]
    fn vct_replay_prepends_validated_pre_upgrade_rows_before_failing_on_bodies() {
        let _init_guard = zakura_test::init();
        let network = export_test_network();
        let db = ephemeral_db(&network);
        set_tip(&db, PRE_LAST_CHECKPOINT);

        let upgrade = Height(4);
        let pre_upgrade = Height(3);
        let sapling_tree = sapling_tree_with_completed_subtrees(1);
        let sapling_root = sapling_crypto::Node::from_bytes([5; 32]).unwrap();

        let mut batch = DiskWriteBatch::new();
        batch.update_vct_upgrade_marker(&db, upgrade);
        batch.update_vct_sync_marker(&db, LAST_CHECKPOINT);
        batch.create_sapling_tree(&db, &pre_upgrade, &sapling_tree);
        batch.create_orchard_tree(
            &db,
            &pre_upgrade,
            &orchard::tree::NoteCommitmentTree::default(),
        );
        batch.create_ironwood_tree(
            &db,
            &pre_upgrade,
            &ironwood::tree::NoteCommitmentTree::default(),
        );
        batch.insert_sapling_subtree(
            &db,
            &NoteCommitmentSubtree::new(0u16, Height(2), sapling_root),
        );
        db.write_batch(batch)
            .expect("seeding pre-upgrade state succeeds");

        // Pre-upgrade validation succeeds; replay then fails for missing bodies in the absent band.
        assert!(matches!(
            export(&db, |_, _| {}).expect_err("anchored VCT replay fails without bodies"),
            TreestateExportError::Derivation {
                height: PRE_LAST_CHECKPOINT,
                source: HistoricalTreeDerivationError::MissingBlockBody {
                    missing: Height(4),
                    ..
                },
            }
        ));
    }

    #[test]
    fn mixed_pre_last_checkpoint_frontiers_fail_closed() {
        let _init_guard = zakura_test::init();
        let network = export_test_network();
        let db = ephemeral_db(&network);
        set_tip(&db, PRE_LAST_CHECKPOINT);

        let mut batch = DiskWriteBatch::new();
        batch.create_sapling_tree(
            &db,
            &PRE_LAST_CHECKPOINT,
            &sapling::tree::NoteCommitmentTree::default(),
        );
        db.write_batch(batch)
            .expect("writing a single frontier succeeds");

        assert_eq!(
            export(&db, |_, _| {}).expect_err("mixed frontiers fail"),
            TreestateExportError::IncompleteFrontiers {
                missing: vec!["orchard", "ironwood"],
            }
        );
    }
}
