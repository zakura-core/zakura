//! Ordered spentness construction and its durable recovery boundary.
//!
//! A hinted run builds checkpoint state in three durable phases. Each block batch
//! stores the next progress record with its other writes:
//!
//! ```text
//! Applying { commitment, height, block_hash, next_ordinal, survivor_value }
//!   -> Rebuilding { commitment, indexed_height, replay_accounting, survivor_value }
//!   -> Complete { commitment, rollback_floor }
//! ```
//!
//! - `startup`: select, authenticate, and resume a run as the database opens.
//! - `apply`: insert artifact survivors while checkpoint blocks commit through H.
//! - `rebuild`: replay retained bodies at H to rebuild derived indexes.
//! - `final_audit`: check the rebuilt indexes before completion is published.
//! - `progress_audit`: offline cursor diagnostics.
//! - `record`: the durable progress record.
//! - `authority`: the release commitments, revocations, and handoff frontiers.
//!
//! Until a run completes, [`SpentnessStatus`] gates consumers of monetary state.

use std::{path::PathBuf, sync::Arc};

use thiserror::Error;
use tokio::sync::watch;
use zakura_chain::{
    block::Height,
    parameters::{
        spentness_hints::{self, Commitment, Mode, VerifiedArtifact},
        Network,
    },
    transaction::Transaction,
    transparent,
};

use super::{super::commitment_aux::FinalFrontiers, ZakuraDb};
use crate::{service::finalized_state::disk_format::OutputLocation, CommitCheckpointVerifiedError};

mod apply;
mod authority;
mod final_audit;
mod progress_audit;
mod rebuild;
mod record;
mod startup;

pub(crate) use authority::ReleaseAuthority;
#[cfg(test)]
pub(crate) use progress_audit::audit_progress_with_setup;
pub use progress_audit::{audit_spentness_progress, SpentnessProgressAudit};
pub(crate) use rebuild::ReplayCache;
pub(crate) use record::{Progress, METADATA};
pub use startup::{
    artifact_cache_path, spentness_artifact_requirement, spentness_cache_dir,
    SpentnessArtifactRequirement,
};

/// Operator settings for constructing or recovering hinted state.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SpentnessConfig {
    /// Select whether the node may use a release-pinned artifact.
    pub mode: Mode,
    /// Exact artifact selected before writable state opens.
    pub artifact: Option<PathBuf>,
}

/// Operator settings plus the release authority that decides which artifacts are trusted.
#[derive(Clone, Debug)]
pub(crate) struct SpentnessSetup {
    pub(crate) config: SpentnessConfig,
    pub(crate) authority: Arc<ReleaseAuthority>,
}

impl SpentnessSetup {
    /// Settings trusted by the compiled release for `network`.
    pub(crate) fn new(config: SpentnessConfig, network: &Network) -> Self {
        Self {
            config,
            authority: Arc::new(ReleaseAuthority::compiled(network)),
        }
    }

    /// Hints off. A completed hinted database still validates against the compiled release.
    pub(crate) fn ordinary(network: &Network) -> Self {
        Self::new(SpentnessConfig::default(), network)
    }
}

/// A local spentness construction failure. Peers must not receive blame for this error.
#[derive(Debug, Error)]
pub enum SpentnessError {
    /// Construction gates protect consumers until index rebuilding finishes.
    #[error(
        "spentness construction is incomplete; resume with spentness.mode enabled \
         before querying or exporting state"
    )]
    Incomplete,
    /// The construction writer failed or exited, so the gates never lift in this process.
    #[error("spentness construction stopped the state writer; restart to reconcile progress")]
    WriterStopped,
    /// The release revoked this commitment. Recognition never overrides revocation.
    #[error(
        "spentness commitment {digest} is revoked; restore an ordinary state or resync \
         without hints"
    )]
    Revoked {
        /// Revoked artifact digest.
        digest: String,
    },
    /// This binary does not recognize the commitment.
    #[error(
        "unknown spentness commitment {digest}; use a release that supports this construction"
    )]
    UnknownCommitment {
        /// Unrecognized artifact digest.
        digest: String,
    },
    /// The commitment names another chain's genesis block.
    #[error("spentness commitment belongs to another chain")]
    WrongChain,
    /// This release has no reviewed commitment for the network.
    #[error("no reviewed spentness commitment for this network")]
    NoReviewedCommitment,
    /// This release has no reviewed VCT frontiers at the commitment's terminal height.
    #[error("no retained VCT frontier for the spentness boundary at height {height}")]
    MissingHandoffFrontiers {
        /// The commitment's terminal height.
        height: u32,
    },
    /// Hinted construction depends on checkpoint sync and VCT fast sync.
    #[error("spentness hints require checkpoint_sync and vct_fast_sync")]
    RequiresFastSync,
    /// `require` mode cannot start hints midway through an ordinary database.
    #[error("spentness hints require an empty database or a recorded hinted run")]
    NonEmptyDatabase,
    /// An incomplete run must resume with the format and features that started it.
    #[error(
        "resume incomplete spentness construction with its original database format and \
         indexer feature"
    )]
    FormatChanged,
    /// Recovery needs the original artifact, not the latest release's artifact.
    #[error(
        "restore spentness artifact {digest} to {path:?} or configure \
         spentness.artifact_file with identical bytes"
    )]
    MissingArtifact {
        /// Expected complete artifact digest.
        digest: String,
        /// Content-addressed recovery path.
        path: PathBuf,
    },
    /// The durable record disagrees with the database it describes.
    #[error("spentness state is inconsistent: {0}")]
    Inconsistent(&'static str),
    /// The ordered writer received a block that the current phase cannot take.
    #[error("spentness writer cannot take this block: {0}")]
    WriteOrder(&'static str),
    /// Construction, replay, or audit found state that differs from retained history.
    #[error("spentness verification failed: {0}")]
    Mismatch(&'static str),
    /// The artifact failed authentication.
    #[error(transparent)]
    Artifact(#[from] spentness_hints::Error),
    /// Local storage failed.
    #[error("spentness storage: {0}")]
    Io(#[from] std::io::Error),
    /// The progress record could not be decoded.
    #[error("spentness progress encoding: {0}")]
    Codec(#[from] serde_json::Error),
    /// Monetary accounting exceeded its consensus bounds.
    #[error("spentness accounting: {0}")]
    Amount(#[from] zakura_chain::amount::Error),
    /// The atomic state batch failed.
    #[error("spentness database write: {0}")]
    Database(#[from] rocksdb::Error),
    /// A shielded or transparent pool calculation failed.
    #[error("spentness value pool: {0}")]
    ValueBalance(#[from] zakura_chain::value_balance::ValueBalanceError),
    /// The writer must reopen after an uncertain durable batch outcome.
    #[error("spentness batch outcome requires restart and reconciliation: {0}")]
    Commit(#[source] Box<CommitCheckpointVerifiedError>),
}

impl From<SpentnessError> for CommitCheckpointVerifiedError {
    fn from(error: SpentnessError) -> Self {
        Self::from_spentness(error)
    }
}

/// Availability of monetary state during spentness construction.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SpentnessStatus {
    /// Ordinary state or a completed hinted state can serve consumers.
    #[default]
    Usable,
    /// Checkpoint writes are constructing the terminal survivor set.
    Applying {
        /// Checkpoint commits through this height can proceed during construction.
        terminal_height: Height,
    },
    /// Body commits are paused while the writer rebuilds and verifies indexes.
    Rebuilding,
    /// A local failure stopped construction; restart must reconcile durable progress.
    Failed,
}

impl SpentnessStatus {
    /// Whether the writer can take a checkpoint body at `height` now.
    pub fn admits_checkpoint_body(self, height: Height) -> bool {
        match self {
            Self::Usable => true,
            Self::Applying { terminal_height } => height <= terminal_height,
            Self::Rebuilding | Self::Failed => false,
        }
    }
}

/// Wait until `ready` accepts the construction status.
///
/// Returns [`SpentnessError::WriterStopped`] if construction fails or the writer exits first.
pub async fn wait_for_spentness(
    status: &mut watch::Receiver<SpentnessStatus>,
    ready: impl Fn(SpentnessStatus) -> bool,
) -> Result<(), SpentnessError> {
    let current = *status
        .wait_for(|current| *current == SpentnessStatus::Failed || ready(*current))
        .await
        .map_err(|_| SpentnessError::WriterStopped)?;
    if current == SpentnessStatus::Failed {
        return Err(SpentnessError::WriterStopped);
    }
    Ok(())
}

/// The artifact and VCT handoff for a run that is still applying.
#[derive(Debug)]
struct ApplyingRun {
    artifact: VerifiedArtifact,
    handoff_frontiers: FinalFrontiers,
}

/// Construction state shared by a database handle and its clones.
#[derive(Clone, Debug)]
pub(super) struct Runtime {
    setup: SpentnessSetup,
    /// Present only while applying. The rebuild needs retained bodies, not the artifact.
    applying: Option<Arc<ApplyingRun>>,
    status: watch::Sender<SpentnessStatus>,
}

impl Runtime {
    pub(super) fn new(setup: SpentnessSetup) -> Self {
        Self {
            setup,
            applying: None,
            status: watch::channel(SpentnessStatus::Usable).0,
        }
    }
}

/// Every transparent output of `block` in artifact order.
fn outputs_in_order(
    block: &zakura_chain::block::Block,
    height: Height,
) -> impl Iterator<Item = (OutputLocation, &Transaction, &transparent::Output)> {
    block
        .transactions
        .iter()
        .enumerate()
        .flat_map(move |(tx_index, transaction)| {
            transaction
                .outputs()
                .iter()
                .enumerate()
                .map(move |(output_index, output)| {
                    let location = OutputLocation::from_usize(height, tx_index, output_index);
                    (location, transaction.as_ref(), output)
                })
        })
}

impl ZakuraDb {
    /// Read the durable progress record, or `None` for an ordinary database.
    pub(crate) fn spentness_progress(&self) -> Result<Option<Progress>, SpentnessError> {
        record::read_progress(&self.db)
    }

    /// The current construction status.
    pub(crate) fn spentness_status(&self) -> SpentnessStatus {
        *self.spentness.status.borrow()
    }

    /// Whether this database currently exposes an incomplete terminal-survivor set.
    pub fn spentness_incomplete(&self) -> bool {
        self.spentness_status() != SpentnessStatus::Usable
    }

    /// Whether the ordered writer has stopped at the terminal height to rebuild indexes.
    pub fn spentness_rebuilding(&self) -> bool {
        self.spentness_status() == SpentnessStatus::Rebuilding
    }

    pub(crate) fn subscribe_spentness(&self) -> watch::Receiver<SpentnessStatus> {
        self.spentness.status.subscribe()
    }

    /// The commitment for a run that is still applying.
    pub(crate) fn applying_spentness_commitment(&self) -> Option<&Commitment> {
        self.spentness
            .applying
            .as_ref()
            .map(|run| run.artifact.commitment())
    }

    /// The reviewed VCT frontiers at H, for a run that is still applying.
    pub(in crate::service::finalized_state) fn spentness_handoff_frontiers(
        &self,
    ) -> Option<&FinalFrontiers> {
        self.spentness
            .applying
            .as_ref()
            .map(|run| &run.handoff_frontiers)
    }

    /// Publish the status for a durable phase to every subscriber.
    pub(crate) fn publish_spentness_status(&self, progress: &Progress) {
        self.set_spentness_status(progress.status());
        if matches!(progress, Progress::Complete { .. }) {
            super::metrics::value_pool_metrics(&self.finalized_value_pool());
        }
    }

    /// Mark construction failed. Gates stay closed until a restart reconciles progress.
    pub(crate) fn fail_spentness(&self) {
        self.set_spentness_status(SpentnessStatus::Failed);
    }

    fn set_spentness_status(&self, status: SpentnessStatus) {
        self.spentness.status.send_replace(status);
        let usable = status == SpentnessStatus::Usable;
        metrics::gauge!("state.spentness.usable").set(if usable { 1.0 } else { 0.0 });
    }
}
