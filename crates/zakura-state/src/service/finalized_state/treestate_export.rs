//! Generating the completed-subtree-root artifact from an archive database.
//!
//! One contiguous replay across the absent band collects every subtree that completes, which is
//! what a fast-synced node needs to serve `z_getsubtreesbyindex` there (see
//! `docs/design/historical-treestate-serving.md` §4.6).
//!
//! # Why this does not need a legacy-synced publisher
//!
//! §5 of the design specifies an archive, *legacy-synced* publisher host, on the assumption that
//! the exporter reads the `{pool}_note_commitment_subtree` column families straight off disk.
//! Replaying instead lifts that requirement, and buys verification with it: subtree roots are
//! interior nodes computed during a replay whose endpoints are checked against the authenticated
//! roots in `commitment_roots_by_height`. Between two checked endpoints the replay is pinned —
//! producing the correct end root from a correct start root while computing a wrong interior node
//! would require a hash collision.
//!
//! That is stronger than §4.6's "reviewed, trusted" story, which exists only because it assumed
//! subtree roots could not be checked without replaying each subtree's leaves. That is true for a
//! serving node; it is not true for a publisher that is replaying regardless.

use std::time::{Duration, Instant};

use thiserror::Error;

use zakura_chain::block::Height;

use crate::service::{
    finalized_state::{
        treestate_artifact::{SubtreeArtifact, SubtreeRecord},
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
    /// The database has no absent band, so there is nothing to generate.
    #[error("this database stores per-height trees at every height, so no artifact is needed")]
    NoAbsentBand,

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
    pub replayed_blocks: u64,
}

/// Generates the subtree-root artifact for `db`'s absent band.
///
/// Replays contiguously from the bottom of the band, recording every subtree that completes and
/// checking the running frontiers against `commitment_roots_by_height` as it goes. Generation
/// stops at the first height that fails its check, so a returned artifact is one whose whole
/// replay reproduced this node's own authenticated roots.
pub fn export(
    db: &ZakuraDb,
    mut on_progress: impl FnMut(Height, u64),
) -> Result<TreestateExport, TreestateExportError> {
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
    let mut frontiers = DerivedFrontiers::empty();
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

    Ok(TreestateExport {
        subtrees,
        elapsed: start.elapsed(),
        replayed_blocks,
    })
}
