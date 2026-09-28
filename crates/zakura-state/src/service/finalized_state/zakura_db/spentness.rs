//! Ordered spentness construction and its durable recovery boundary.
//!
//! A hinted run builds checkpoint state in two durable phases. Each block batch
//! stores the next progress record with its other writes:
//!
//! ```text
//! Applying { commitment, height, block_hash, next_ordinal, omitted_outputs, resolved_spends }
//!   -> Complete { commitment, rollback_floor }
//! ```
//!
//! - `startup`: select, authenticate, and resume a run as the database opens.
//! - `apply`: read membership bits and resolve spends while checkpoint blocks commit
//!   through H, and prove at H that the omitted outputs are exactly the spent outputs.
//! - `live`: the omitted outputs that no block has spent yet, in memory and on disk.
//! - `record`: the durable progress record.
//! - `authority`: the release commitments, revocations, and handoff frontiers.
//!
//! Until a run completes, [`SpentnessStatus`] gates consumers of the UTXO set.

use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

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
mod live;
mod record;
mod startup;

pub(crate) use apply::HintedBlock;
pub(crate) use authority::ReleaseAuthority;
pub(crate) use record::{Progress, METADATA, OMITTED_OUTPUTS};
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
    /// An incomplete run cannot open with hints off or for read-only access.
    #[error(
        "spentness construction is incomplete; resume with spentness.mode enabled \
         before querying or exporting state"
    )]
    Incomplete,
    /// Construction gates deny this request until the run completes at H.
    #[error("state is unavailable while spentness construction is incomplete")]
    Unavailable,
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
    /// Construction found a spend or count that contradicts the artifact.
    ///
    /// Restarting repeats the failure, so the operator must discard the state.
    #[error(
        "spentness verification failed: {0}; delete the state cache directory and resync \
         with spentness.mode = \"off\""
    )]
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
    /// A local failure stopped construction; restart must reconcile durable progress.
    Failed,
}

impl SpentnessStatus {
    /// Whether the writer can take a checkpoint body at `height` now.
    pub fn admits_checkpoint_body(self, height: Height) -> bool {
        match self {
            Self::Usable => true,
            Self::Applying { terminal_height } => height <= terminal_height,
            Self::Failed => false,
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

/// The artifact, VCT handoff, and live omitted outputs for a run that is still applying.
#[derive(Debug)]
pub(crate) struct ApplyingRun {
    artifact: VerifiedArtifact,
    handoff_frontiers: FinalFrontiers,
    /// Only the writer uses the map, so the lock is uncontended.
    live: Mutex<live::LiveOutputs>,
}

impl ApplyingRun {
    /// The height H where construction completes.
    pub(crate) fn terminal_height(&self) -> Height {
        Height(self.artifact.commitment().terminal_height)
    }
}

/// Construction state shared by a database handle and its clones.
#[derive(Clone, Debug)]
pub(super) struct Runtime {
    setup: SpentnessSetup,
    /// Present only while applying.
    applying: Option<Arc<ApplyingRun>>,
    status: watch::Sender<SpentnessStatus>,
    /// Mirrors `status != Usable`, so request gates avoid the watch channel's lock.
    incomplete: Arc<AtomicBool>,
}

impl Runtime {
    pub(super) fn new(setup: SpentnessSetup) -> Self {
        Self {
            setup,
            applying: None,
            status: watch::channel(SpentnessStatus::Usable).0,
            incomplete: Arc::new(AtomicBool::new(false)),
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
        self.spentness.incomplete.load(Ordering::Acquire)
    }

    pub(crate) fn subscribe_spentness(&self) -> watch::Receiver<SpentnessStatus> {
        self.spentness.status.subscribe()
    }

    /// Drop this handle's reference to the applying run's artifact.
    ///
    /// Only the writer's handle needs the artifact, and it releases it at H.
    pub(crate) fn without_spentness_run(mut self) -> Self {
        self.spentness.applying = None;
        self
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
        let usable = status == SpentnessStatus::Usable;
        self.spentness.incomplete.store(!usable, Ordering::Release);
        self.spentness.status.send_replace(status);
        metrics::gauge!("state.spentness.usable").set(if usable { 1.0 } else { 0.0 });
    }
}
