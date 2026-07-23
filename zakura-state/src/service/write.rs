//! Writing blocks to the finalized and non-finalized states.

use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use indexmap::IndexMap;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::{
    mpsc::{error::TryRecvError, UnboundedReceiver, UnboundedSender},
    oneshot, watch,
};

use tracing::Span;
use zakura_chain::{
    block::{self, Height},
    parallel::tree::NoteCommitmentTrees,
};
use zakura_header_chain::{
    ApplyResult, BodyEvidence, CheckpointSet, EngineConfig, EngineConfigError, EngineMode,
    EngineSnapshot, EvidenceId, Frontier, FullStateEvidenceAuthority, FullStateFinalized,
    OperatorInvalidate, OperatorInvalidationId, OperatorReconsider, StateVersion, StoreError,
    StoreRead, SystemClock, TransitionContext, TransitionEvent, TransitionRequest, TrustedAnchor,
    VerifiedBodyEvidence, VerifiedChainChanged, VerifiedChangeCause, VerifiedHeaderRef,
};

use crate::{
    constants::MAX_BLOCK_REORG_HEIGHT,
    request::FinalizableBlock,
    service::{
        check,
        finalized_state::{
            header_chain::{
                migration::{initialize_header_chain_reconciled, HeaderChainInitializationError},
                HeaderChainReader, HeaderChainRuntime, HeaderChainStore, HeaderChainStoreError,
            },
            DiskWriteBatch, FinalizedState, NextVctBlock, ZakuraDb,
        },
        non_finalized_state::NonFinalizedState,
        queued_blocks::{QueuedCheckpointVerified, QueuedSemanticallyVerified},
        ChainTipBlock, ChainTipSender, InvalidateError, ReconsiderError,
    },
    CheckpointVerifiedBlock, CommitBlockError, CommitCheckpointVerifiedError,
    SemanticallyVerifiedBlock, ValidateContextError,
};

// These types are used in doc links
#[allow(unused_imports)]
use crate::service::{
    chain_tip::{ChainTipChange, LatestChainTip},
    non_finalized_state::Chain,
};

mod vct_write;

use vct_write::VctWriteManager;

/// Status published by the finalized write loop when a VCT fast-sync height needs a
/// replacement supplied root.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct VctRootRepairStatus {
    /// The state of the current root repair need.
    pub state: VctRootRepairState,
    /// Monotonic generation for repair attempts. A new generation means the previous
    /// replacement candidate was absent or rejected and the networking layer should try
    /// another bounded repair candidate.
    pub generation: u64,
}

impl Default for VctRootRepairStatus {
    fn default() -> Self {
        Self {
            state: VctRootRepairState::Idle,
            generation: 0,
        }
    }
}

/// Dependency-neutral VCT root repair state.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum VctRootRepairState {
    /// No VCT root repair is currently required.
    Idle,
    /// The finalized writer cannot commit this height until a verifiable supplied root is
    /// re-delivered through header sync.
    Unavailable {
        /// Height whose supplied roots are missing from the VCT source.
        height: block::Height,
    },
}

/// A full-state mutation staged until its matching header transition commits durably.
#[allow(dead_code)] // Constructed when the dark header engine is attached to the writer task.
pub struct PreparedFullStateTransition {
    /// Stable identity authenticated by the state writer.
    transition_id: EvidenceId,
    /// Verified frontier against which the mutation was prepared.
    old_frontier: Frontier,
    /// Exact new verified suffix, empty for a finality-only mutation.
    new_verified_path: Vec<VerifiedHeaderRef>,
    /// Complete in-memory state installed only after the durable commit.
    non_finalized_after: NonFinalizedState,
    /// Optional finalized-state writes combined with the header write batch.
    finalized_batch: Option<DiskWriteBatch>,
    /// Matching version-qualified header-engine evidence.
    header_request: TransitionRequest,
}

struct PreparedAuthority(EvidenceId);

impl FullStateEvidenceAuthority for PreparedAuthority {
    fn authorizes(&self, evidence: EvidenceId) -> bool {
        evidence == self.0
    }
}

#[allow(dead_code)] // Called when the dark header engine is attached to the writer task.
impl PreparedFullStateTransition {
    /// Construct a staged mutation only when its duplicated identity and verified path agree.
    pub fn new(
        transition_id: EvidenceId,
        old_frontier: Frontier,
        new_verified_path: Vec<VerifiedHeaderRef>,
        non_finalized_after: NonFinalizedState,
        finalized_batch: Option<DiskWriteBatch>,
        header_request: TransitionRequest,
    ) -> Result<Self, PreparedFullStateTransitionError> {
        if header_request.event.idempotency_key() != Some(transition_id) {
            return Err(PreparedFullStateTransitionError::IdentityMismatch);
        }
        if let TransitionEvent::VerifiedChainChanged(change) = &header_request.event {
            if change.old_tip != old_frontier || change.new_path != new_verified_path {
                return Err(PreparedFullStateTransitionError::VerifiedPathMismatch);
            }
        }
        Ok(Self {
            transition_id,
            old_frontier,
            new_verified_path,
            non_finalized_after,
            finalized_batch,
            header_request,
        })
    }

    /// Commit the combined batch, then swap memory, then publish the committed receipt.
    pub(super) fn commit(
        self,
        runtime: &HeaderChainRuntime,
        live_non_finalized: &mut NonFinalizedState,
        context: &TransitionContext<'_>,
    ) -> Result<ApplyResult, HeaderChainStoreError> {
        let finalized_after = match &self.header_request.event {
            TransitionEvent::FullStateFinalized(event) => Some(event.new_finalized),
            _ => None,
        };
        let expected_verified = self
            .non_finalized_after
            .best_tip()
            .map(|(height, hash)| Frontier::new(height, hash))
            .unwrap_or_else(|| {
                finalized_after
                    .unwrap_or_else(|| runtime.publisher().snapshot().frontiers.finalized)
            });
        let Self {
            transition_id,
            non_finalized_after,
            finalized_batch,
            header_request,
            ..
        } = self;
        let authority = PreparedAuthority(transition_id);
        let guarded_context = TransitionContext {
            config: context.config,
            clock: context.clock,
            full_state_authority: Some(&authority),
            startup_capability: context.startup_capability,
            retention_references: context.retention_references,
        };
        runtime.apply_combined_expected(
            header_request,
            &guarded_context,
            finalized_batch.unwrap_or_else(DiskWriteBatch::new),
            expected_verified,
            || *live_non_finalized = non_finalized_after,
        )
    }
}

/// Incoherent duplicated facts at the staging boundary.
#[allow(dead_code)] // Returned when the dark header engine stages writer mutations.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum PreparedFullStateTransitionError {
    /// The header request did not carry the exact state-writer transition identity.
    #[error("prepared full-state/header transition identities differ")]
    IdentityMismatch,
    /// A verified-chain event did not repeat the exact old frontier and new suffix.
    #[error("prepared full-state/header verified paths differ")]
    VerifiedPathMismatch,
}

/// Audited header runtime and immutable configuration injected into the state writer.
#[derive(Clone, Debug)]
pub(in crate::service) struct HeaderChainWriter {
    runtime: HeaderChainRuntime,
    config: EngineConfig,
    clock: SystemClock,
}

#[derive(Debug, Error)]
enum HeaderChainAttachmentError {
    #[error("finalized state has no authenticated genesis header at semantic handoff")]
    MissingGenesis,
    #[error("finalized genesis hash does not match the configured network")]
    GenesisMismatch,
    #[error("persisted header finality is not an ancestor of finalized full state")]
    FinalizedDivergence,
    #[error("finalized state is missing a header required to reconcile height {0:?}")]
    MissingFinalizedHeader(Height),
    #[error(transparent)]
    Config(#[from] EngineConfigError),
    #[error(transparent)]
    Store(#[from] HeaderChainStoreError),
    #[error(transparent)]
    Read(#[from] StoreError),
    #[error(transparent)]
    Initialization(#[from] HeaderChainInitializationError),
}

impl HeaderChainWriter {
    pub(in crate::service) fn new(runtime: HeaderChainRuntime, config: EngineConfig) -> Self {
        Self {
            runtime,
            config,
            clock: SystemClock,
        }
    }

    fn vct_successor(
        &self,
        db: &ZakuraDb,
        height: block::Height,
        hash: block::Hash,
    ) -> Option<NextVctBlock> {
        let successor = self
            .runtime
            .reader()
            .selected_successor(height, hash)
            .ok()??;
        let roots = db
            .supplied_commitment_roots_by_height_range(successor.height..=successor.height)
            .into_iter()
            .next()?;
        Some(NextVctBlock::from_header(
            successor.header,
            successor.height,
            roots.auth_data_root,
        ))
    }

    fn attach_at_semantic_handoff(
        finalized_state: &FinalizedState,
        non_finalized_state: &NonFinalizedState,
    ) -> Result<Self, HeaderChainAttachmentError> {
        let network = finalized_state.db.network();
        let (genesis_hash, genesis_header) = finalized_state
            .db
            .header_by_height(Height(0))
            .ok_or(HeaderChainAttachmentError::MissingGenesis)?;
        if genesis_hash != network.genesis_hash() {
            return Err(HeaderChainAttachmentError::GenesisMismatch);
        }
        let config = EngineConfig::new(
            EngineMode::Integrated,
            network.clone(),
            TrustedAnchor {
                frontier: Frontier::new(Height(0), genesis_hash),
                header: genesis_header,
            },
            CheckpointSet::new(
                network
                    .checkpoint_list()
                    .iter_cloned()
                    .map(|(height, hash)| Frontier::new(height, hash)),
            )?,
        )?;
        let restored_path = verified_path(non_finalized_state);
        let store = HeaderChainStore::new(finalized_state.db.header_chain_disk_db());
        let runtime = if store.is_initialized()? {
            let persisted_finalized = store.snapshot()?.frontiers.finalized;
            let (full_state_height, full_state_hash) = finalized_state
                .db
                .tip()
                .ok_or(HeaderChainAttachmentError::MissingGenesis)?;
            let full_state_finalized = Frontier::new(full_state_height, full_state_hash);
            let persisted_hash = finalized_state
                .db
                .header_by_height(persisted_finalized.height)
                .map(|(hash, _)| hash);
            if persisted_finalized.height > full_state_height
                || persisted_hash != Some(persisted_finalized.hash)
            {
                return Err(HeaderChainAttachmentError::FinalizedDivergence);
            }
            let mut finalized_path = Vec::new();
            let mut height = persisted_finalized.height;
            while height < full_state_height {
                height = height
                    .next()
                    .map_err(|_| HeaderChainAttachmentError::FinalizedDivergence)?;
                let (hash, header) = finalized_state
                    .db
                    .header_by_height(height)
                    .ok_or(HeaderChainAttachmentError::MissingFinalizedHeader(height))?;
                finalized_path.push(VerifiedHeaderRef {
                    height,
                    hash,
                    header,
                });
            }
            store
                .startup_reconciled(&config, full_state_finalized, finalized_path, restored_path)?
                .0
        } else {
            initialize_header_chain_reconciled(&finalized_state.db, &config, restored_path)?.0
        };
        Ok(Self::new(runtime, config))
    }

    fn context(&self) -> TransitionContext<'_> {
        TransitionContext {
            config: &self.config,
            clock: &self.clock,
            full_state_authority: None,
            startup_capability: None,
            retention_references: &[],
        }
    }

    fn record_body_unavailable(
        &self,
        expected_version: StateVersion,
        failure: zakura_header_chain::TransientBodyFailure,
    ) -> Result<ApplyResult, HeaderChainStoreError> {
        let authority = PreparedAuthority(failure.evidence);
        let mut context = self.context();
        context.full_state_authority = Some(&authority);
        self.runtime.apply(
            TransitionRequest {
                expected_version,
                event: TransitionEvent::BodyEvidence(BodyEvidence::Transient(failure)),
            },
            &context,
        )
    }

    fn restart_body_availability(
        &self,
        expected_version: StateVersion,
        discovery: zakura_header_chain::BodySupplierDiscovered,
    ) -> Result<ApplyResult, HeaderChainStoreError> {
        let authority = PreparedAuthority(discovery.evidence);
        let mut context = self.context();
        context.full_state_authority = Some(&authority);
        self.runtime.apply(
            TransitionRequest {
                expected_version,
                event: TransitionEvent::BodySupplierDiscovered(discovery),
            },
            &context,
        )
    }
}

fn verified_path(state: &NonFinalizedState) -> Vec<VerifiedHeaderRef> {
    state
        .best_chain()
        .into_iter()
        .flat_map(|chain| chain.blocks.values())
        .map(|block| VerifiedHeaderRef {
            height: block.height,
            hash: block.hash,
            header: block.block.header.clone(),
        })
        .collect()
}

fn verified_frontier(state: &NonFinalizedState, finalized: Frontier) -> Frontier {
    state
        .best_tip()
        .map(|(height, hash)| Frontier::new(height, hash))
        .unwrap_or(finalized)
}

fn full_state_evidence(
    tag: &[u8],
    version: StateVersion,
    target: block::Hash,
    path: &[VerifiedHeaderRef],
) -> EvidenceId {
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-full-state-header-transition-v1");
    hasher.update(tag);
    hasher.update(version.get().to_be_bytes());
    hasher.update(target.0);
    for header in path {
        hasher.update(header.height.0.to_be_bytes());
        hasher.update(header.hash.0);
    }
    EvidenceId::from_digest(hasher.finalize().into())
}

fn verified_request(
    writer: &HeaderChainWriter,
    before: &NonFinalizedState,
    after: &NonFinalizedState,
    accepted: Frontier,
) -> Result<(EvidenceId, Vec<VerifiedHeaderRef>, TransitionRequest), HeaderChainStoreError> {
    let snapshot = writer.runtime.publisher().snapshot();
    let old_path = verified_path(before);
    let new_path = verified_path(after);
    let old_frontier = verified_frontier(before, snapshot.frontiers.finalized);
    if old_frontier != snapshot.frontiers.verified_best {
        return Err(HeaderChainStoreError::VerifiedFrontierMismatch {
            expected: old_frontier,
            actual: snapshot.frontiers.verified_best,
        });
    }
    let best_changed =
        old_path.last().map(|header| header.hash) != new_path.last().map(|header| header.hash);
    let event_path;
    let event = if best_changed {
        let grows = new_path.len() > old_path.len()
            && new_path
                .iter()
                .zip(&old_path)
                .all(|(new, old)| new.hash == old.hash);
        event_path = if grows {
            new_path[old_path.len()..].to_vec()
        } else {
            new_path.clone()
        };
        let evidence = full_state_evidence(
            if grows { b"grow" } else { b"reset" },
            snapshot.state_version,
            accepted.hash,
            &event_path,
        );
        return Ok((
            evidence,
            event_path.clone(),
            TransitionRequest {
                expected_version: snapshot.state_version,
                event: TransitionEvent::VerifiedChainChanged(VerifiedChainChanged {
                    full_state_transition_id: evidence,
                    old_tip: old_frontier,
                    new_path: event_path,
                    cause: if grows {
                        VerifiedChangeCause::Grow
                    } else {
                        VerifiedChangeCause::Reset
                    },
                }),
            },
        ));
    } else {
        event_path = Vec::new();
        let evidence = full_state_evidence(
            b"verified-body",
            snapshot.state_version,
            accepted.hash,
            &event_path,
        );
        TransitionEvent::BodyEvidence(BodyEvidence::Verified(VerifiedBodyEvidence {
            hash: accepted.hash,
            evidence,
        }))
    };
    let evidence = event
        .idempotency_key()
        .expect("full-state evidence events always have an identity");
    Ok((
        evidence,
        event_path,
        TransitionRequest {
            expected_version: snapshot.state_version,
            event,
        },
    ))
}

fn operator_identity(target: block::Hash) -> (OperatorInvalidationId, [u8; 32]) {
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-operator-invalidation-v1");
    hasher.update(target.0);
    let digest: [u8; 32] = hasher.finalize().into();
    let mut id = [0; 16];
    id.copy_from_slice(&digest[..16]);
    (OperatorInvalidationId::new(id), digest)
}

fn finalization_request(
    writer: &HeaderChainWriter,
    new_finalized: Frontier,
) -> Result<(EvidenceId, TransitionRequest), HeaderChainStoreError> {
    let snapshot = writer.runtime.publisher().snapshot();
    let verified_path_proof = writer
        .runtime
        .verified_projection()?
        .into_iter()
        .take_while(|frontier| frontier.height <= new_finalized.height)
        .map(|frontier| frontier.hash)
        .collect::<Vec<_>>();
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-full-state-finalized-v1");
    hasher.update(snapshot.state_version.get().to_be_bytes());
    hasher.update(new_finalized.height.0.to_be_bytes());
    hasher.update(new_finalized.hash.0);
    for hash in &verified_path_proof {
        hasher.update(hash.0);
    }
    let evidence = EvidenceId::from_digest(hasher.finalize().into());
    Ok((
        evidence,
        TransitionRequest {
            expected_version: snapshot.state_version,
            event: TransitionEvent::FullStateFinalized(FullStateFinalized {
                full_state_transition_id: evidence,
                new_finalized,
                verified_path_proof,
            }),
        },
    ))
}

fn commit_contextual_finalization(
    writer: &HeaderChainWriter,
    finalized_state: &mut FinalizedState,
    live: &mut NonFinalizedState,
    prev_note_commitment_trees: Option<NoteCommitmentTrees>,
) -> Result<(block::Hash, NoteCommitmentTrees), CommitCheckpointVerifiedError> {
    let mut staged = live.clone();
    let finalizable = staged.finalize();
    let new_finalized = match &finalizable {
        FinalizableBlock::Contextual {
            contextually_verified,
            ..
        } => Frontier::new(contextually_verified.height, contextually_verified.hash),
        FinalizableBlock::Checkpoint { .. } => {
            unreachable!("non-finalized state only yields contextually verified blocks")
        }
    };
    let (evidence, request) = finalization_request(writer, new_finalized).map_err(|error| {
        CommitBlockError::HeaderChainError {
            error: error.to_string(),
        }
    })?;
    let old_frontier = writer
        .runtime
        .publisher()
        .snapshot()
        .frontiers
        .verified_best;
    let new_verified_path = verified_path(&staged);
    finalized_state.commit_finalized_direct_with(
        finalizable,
        prev_note_commitment_trees,
        None,
        "commit contextually-verified request",
        |_db, batch| {
            PreparedFullStateTransition::new(
                evidence,
                old_frontier,
                new_verified_path,
                staged,
                Some(batch),
                request,
            )
            .map_err(|error| CommitBlockError::HeaderChainError {
                error: error.to_string(),
            })?
            .commit(&writer.runtime, live, &writer.context())
            .map(|_| ())
            .map_err(|error| CommitBlockError::HeaderChainError {
                error: error.to_string(),
            })
            .map_err(Into::into)
        },
    )
}

fn commit_operator_change(
    writer: &HeaderChainWriter,
    live: &mut NonFinalizedState,
    staged: NonFinalizedState,
    target: block::Hash,
    invalidate: bool,
) -> Result<ApplyResult, HeaderChainStoreError> {
    let snapshot = writer.runtime.publisher().snapshot();
    let path = verified_path(&staged);
    let evidence = full_state_evidence(
        if invalidate {
            b"operator-invalidate"
        } else {
            b"operator-reconsider"
        },
        snapshot.state_version,
        target,
        &path,
    );
    let (id, operator_reason_digest) = operator_identity(target);
    let event = if invalidate {
        TransitionEvent::OperatorInvalidate(OperatorInvalidate {
            target,
            id,
            operator_reason_digest,
            evidence,
        })
    } else {
        TransitionEvent::OperatorReconsider(OperatorReconsider {
            target,
            id,
            evidence,
        })
    };
    PreparedFullStateTransition::new(
        evidence,
        snapshot.frontiers.verified_best,
        path,
        staged,
        None,
        TransitionRequest {
            expected_version: snapshot.state_version,
            event,
        },
    )
    .map_err(|_| HeaderChainStoreError::Incoherent("staged operator transition disagrees"))?
    .commit(&writer.runtime, live, &writer.context())
}

/// The maximum size of the parent error map.
///
/// We allow enough space for multiple concurrent chain forks with errors.
const PARENT_ERROR_MAP_LIMIT: usize = MAX_BLOCK_REORG_HEIGHT as usize * 2;

/// Run contextual validation on the prepared block and add it to the
/// non-finalized state if it is contextually valid.
#[tracing::instrument(
    level = "debug",
    skip(finalized_state, non_finalized_state, prepared),
    fields(
        height = ?prepared.height,
        hash = %prepared.hash,
        chains = non_finalized_state.chain_count()
    )
)]
pub(crate) fn validate_and_commit_non_finalized(
    finalized_state: &ZakuraDb,
    non_finalized_state: &mut NonFinalizedState,
    prepared: SemanticallyVerifiedBlock,
) -> Result<(), ValidateContextError> {
    check::initial_contextual_validity(finalized_state, non_finalized_state, &prepared)?;
    let parent_hash = prepared.block.header.previous_block_hash;

    if finalized_state.finalized_tip_hash() == parent_hash {
        non_finalized_state.commit_new_chain(prepared, finalized_state)?;
    } else {
        non_finalized_state.commit_block(prepared, finalized_state)?;
    }

    Ok(())
}

/// Update the [`LatestChainTip`], [`ChainTipChange`], and `non_finalized_state_sender`
/// channels with the latest non-finalized [`ChainTipBlock`] and
/// [`Chain`].
///
/// `last_zebra_mined_log_height` is used to rate-limit logging.
///
/// If `backup_dir_path` is `Some`, the non-finalized state is written to the backup
/// directory before updating the channels.
///
/// Returns the latest non-finalized chain tip height.
///
/// # Panics
///
/// If the `non_finalized_state` is empty.
#[instrument(
    level = "debug",
    skip(
        non_finalized_state,
        chain_tip_sender,
        non_finalized_state_sender,
        backup_dir_path,
    ),
    fields(chains = non_finalized_state.chain_count())
)]
fn update_latest_chain_channels(
    non_finalized_state: &NonFinalizedState,
    chain_tip_sender: &mut ChainTipSender,
    non_finalized_state_sender: &watch::Sender<NonFinalizedState>,
    backup_dir_path: Option<&Path>,
) -> block::Height {
    let best_chain = non_finalized_state.best_chain().expect("unexpected empty non-finalized state: must commit at least one block before updating channels");

    let tip_block = best_chain
        .tip_block()
        .expect("unexpected empty chain: must commit at least one block before updating channels")
        .clone();
    let tip_block = ChainTipBlock::from(tip_block);

    let tip_block_height = tip_block.height;

    if let Some(backup_dir_path) = backup_dir_path {
        non_finalized_state.write_to_backup(backup_dir_path);
    }

    // If the final receiver was just dropped, ignore the error.
    let _ = non_finalized_state_sender.send(non_finalized_state.clone());

    chain_tip_sender.set_best_non_finalized_tip(tip_block);

    tip_block_height
}

fn update_channels_after_operator_change(
    non_finalized_state: &NonFinalizedState,
    finalized_state: &FinalizedState,
    chain_tip_sender: &mut ChainTipSender,
    non_finalized_state_sender: &watch::Sender<NonFinalizedState>,
    backup_dir_path: Option<&Path>,
) {
    if non_finalized_state.is_chain_set_empty() {
        if let Some(backup_dir_path) = backup_dir_path {
            non_finalized_state.write_to_backup(backup_dir_path);
        }
        let _ = non_finalized_state_sender.send(non_finalized_state.clone());
        chain_tip_sender.clear_best_non_finalized_tip(
            finalized_state
                .db
                .tip_block()
                .map(CheckpointVerifiedBlock::from)
                .map(ChainTipBlock::from),
        );
    } else {
        update_latest_chain_channels(
            non_finalized_state,
            chain_tip_sender,
            non_finalized_state_sender,
            backup_dir_path,
        );
    }
}

/// A worker task that reads, validates, and writes blocks to the
/// `finalized_state` or `non_finalized_state`.
struct WriteBlockWorkerTask {
    finalized_block_write_receiver: UnboundedReceiver<QueuedCheckpointVerified>,
    non_finalized_block_write_receiver: UnboundedReceiver<NonFinalizedWriteMessage>,
    finalized_state: FinalizedState,
    non_finalized_state: NonFinalizedState,
    invalid_block_reset_sender: UnboundedSender<block::Hash>,
    /// Signals the [`crate::service::StateService`] that a non-finalized block was rejected by
    /// the write task, so its hash should be removed from
    /// `non_finalized_block_write_sent_hashes`.
    ///
    /// Without this, a rejected same-hash block locks out a later honest
    /// re-delivery of a block at the same hash as a "duplicate" until restart
    /// or reorg.
    non_finalized_rejected_sender: UnboundedSender<block::Hash>,
    chain_tip_sender: ChainTipSender,
    non_finalized_state_sender: watch::Sender<NonFinalizedState>,
    vct_root_repair_sender: watch::Sender<VctRootRepairStatus>,
    /// If `Some`, the non-finalized state is written to this backup directory
    /// synchronously before each channel update, instead of via the async backup task.
    backup_dir_path: Option<PathBuf>,
    header_chain: Option<HeaderChainWriter>,
    attach_header_chain_at_handoff: bool,
    header_chain_observers: HeaderChainObservers,
}

#[derive(Clone, Debug)]
pub(in crate::service) struct HeaderChainObservers {
    snapshot_sender: watch::Sender<Option<EngineSnapshot>>,
    reader_sender: watch::Sender<Option<HeaderChainReader>>,
}

impl HeaderChainObservers {
    pub(in crate::service) fn new(
        snapshot_sender: watch::Sender<Option<EngineSnapshot>>,
        reader_sender: watch::Sender<Option<HeaderChainReader>>,
    ) -> Self {
        Self {
            snapshot_sender,
            reader_sender,
        }
    }
}

/// The message type for the non-finalized block write task channel.
pub enum NonFinalizedWriteMessage {
    /// One complete peer target prepared outside the writer and admitted through the sole
    /// transition algorithm.
    ApplyHeaderChainInsert {
        expected_version: StateVersion,
        insert: Box<zakura_header_chain::InsertHeaders>,
        rsp_tx: oneshot::Sender<Result<ApplyResult, HeaderChainStoreError>>,
    },
    /// One retryable body-availability result admitted by integrated full state.
    RecordHeaderChainBodyUnavailable {
        expected_version: StateVersion,
        failure: zakura_header_chain::TransientBodyFailure,
        rsp_tx: oneshot::Sender<Result<ApplyResult, HeaderChainStoreError>>,
    },
    /// A changed authenticated supplier set restarts one persistent alarm.
    RestartHeaderChainBodyAvailability {
        expected_version: StateVersion,
        discovery: zakura_header_chain::BodySupplierDiscovered,
        rsp_tx: oneshot::Sender<Result<ApplyResult, HeaderChainStoreError>>,
    },
    /// A newly downloaded and semantically verified block prepared for
    /// contextual validation and insertion into the non-finalized state.
    Commit(QueuedSemanticallyVerified),
    /// The hash of a block that should be invalidated and removed from
    /// the non-finalized state, if present.
    Invalidate {
        hash: block::Hash,
        rsp_tx: oneshot::Sender<Result<block::Hash, InvalidateError>>,
    },
    /// The hash of a block that was previously invalidated but should be
    /// reconsidered and reinserted into the non-finalized state.
    Reconsider {
        hash: block::Hash,
        rsp_tx: oneshot::Sender<Result<Vec<block::Hash>, ReconsiderError>>,
    },
}

impl From<QueuedSemanticallyVerified> for NonFinalizedWriteMessage {
    fn from(block: QueuedSemanticallyVerified) -> Self {
        NonFinalizedWriteMessage::Commit(block)
    }
}

/// A worker with a task that reads, validates, and writes blocks to the
/// `finalized_state` or `non_finalized_state` and channels for sending
/// it blocks.
#[derive(Clone, Debug)]
pub struct BlockWriteSender {
    /// A channel to send blocks to the `block_write_task`,
    /// so they can be written to the [`NonFinalizedState`].
    pub non_finalized: Option<tokio::sync::mpsc::UnboundedSender<NonFinalizedWriteMessage>>,

    /// A channel to send blocks to the `block_write_task`,
    /// so they can be written to the [`FinalizedState`].
    ///
    /// This sender is dropped after the state has finished sending all the checkpointed blocks,
    /// and the lowest semantically verified block arrives.
    pub finalized: Option<tokio::sync::mpsc::UnboundedSender<QueuedCheckpointVerified>>,
}

impl BlockWriteSender {
    /// Creates a new [`BlockWriteSender`] with the given receivers and states.
    #[instrument(
        level = "debug",
        skip_all,
        fields(
            network = %non_finalized_state.network
        )
    )]
    pub fn spawn(
        finalized_state: FinalizedState,
        non_finalized_state: NonFinalizedState,
        chain_tip_sender: ChainTipSender,
        non_finalized_state_sender: watch::Sender<NonFinalizedState>,
        should_use_finalized_block_write_sender: bool,
        backup_dir_path: Option<PathBuf>,
        header_chain_observers: HeaderChainObservers,
    ) -> (
        Self,
        tokio::sync::mpsc::UnboundedReceiver<block::Hash>,
        tokio::sync::mpsc::UnboundedReceiver<block::Hash>,
        watch::Receiver<VctRootRepairStatus>,
        Option<Arc<std::thread::JoinHandle<()>>>,
    ) {
        Self::spawn_with_header_chain(
            finalized_state,
            non_finalized_state,
            chain_tip_sender,
            non_finalized_state_sender,
            should_use_finalized_block_write_sender,
            backup_dir_path,
            None,
            true,
            header_chain_observers,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::service) fn spawn_with_header_chain(
        finalized_state: FinalizedState,
        non_finalized_state: NonFinalizedState,
        chain_tip_sender: ChainTipSender,
        non_finalized_state_sender: watch::Sender<NonFinalizedState>,
        should_use_finalized_block_write_sender: bool,
        backup_dir_path: Option<PathBuf>,
        header_chain: Option<HeaderChainWriter>,
        attach_header_chain_at_handoff: bool,
        header_chain_observers: HeaderChainObservers,
    ) -> (
        Self,
        tokio::sync::mpsc::UnboundedReceiver<block::Hash>,
        tokio::sync::mpsc::UnboundedReceiver<block::Hash>,
        watch::Receiver<VctRootRepairStatus>,
        Option<Arc<std::thread::JoinHandle<()>>>,
    ) {
        // Security: The number of blocks in these channels is limited by
        //           the syncer and inbound lookahead limits.
        let (non_finalized_block_write_sender, non_finalized_block_write_receiver) =
            tokio::sync::mpsc::unbounded_channel();
        let (finalized_block_write_sender, finalized_block_write_receiver) =
            tokio::sync::mpsc::unbounded_channel();
        let (invalid_block_reset_sender, invalid_block_write_reset_receiver) =
            tokio::sync::mpsc::unbounded_channel();
        let (non_finalized_rejected_sender, non_finalized_rejected_receiver) =
            tokio::sync::mpsc::unbounded_channel();
        let (vct_root_repair_sender, vct_root_repair_receiver) =
            watch::channel(VctRootRepairStatus::default());

        let span = Span::current();
        let task = std::thread::spawn(move || {
            span.in_scope(|| {
                WriteBlockWorkerTask {
                    finalized_block_write_receiver,
                    non_finalized_block_write_receiver,
                    finalized_state,
                    non_finalized_state,
                    invalid_block_reset_sender,
                    non_finalized_rejected_sender,
                    chain_tip_sender,
                    non_finalized_state_sender,
                    vct_root_repair_sender,
                    backup_dir_path,
                    header_chain,
                    attach_header_chain_at_handoff,
                    header_chain_observers,
                }
                .run()
            })
        });

        (
            Self {
                non_finalized: Some(non_finalized_block_write_sender),
                finalized: should_use_finalized_block_write_sender
                    .then_some(finalized_block_write_sender),
            },
            invalid_block_write_reset_receiver,
            non_finalized_rejected_receiver,
            vct_root_repair_receiver,
            Some(Arc::new(task)),
        )
    }
}

impl WriteBlockWorkerTask {
    /// Reads blocks from the channels, writes them to the `finalized_state` or `non_finalized_state`,
    /// sends any errors on the `invalid_block_reset_sender`, then updates the `chain_tip_sender` and
    /// `non_finalized_state_sender`.
    #[instrument(
        level = "debug",
        skip(self),
        fields(
            network = %self.non_finalized_state.network
        )
    )]
    pub fn run(mut self) {
        let Self {
            finalized_block_write_receiver,
            non_finalized_block_write_receiver,
            finalized_state,
            non_finalized_state,
            invalid_block_reset_sender,
            non_finalized_rejected_sender,
            chain_tip_sender,
            non_finalized_state_sender,
            vct_root_repair_sender,
            backup_dir_path,
            header_chain,
            attach_header_chain_at_handoff,
            header_chain_observers,
        } = &mut self;

        let mut prev_finalized_note_commitment_trees: Option<NoteCommitmentTrees> = None;
        let mut deferred_non_finalized_messages = VecDeque::new();

        // Look-ahead buffering and root-stall tracking for the VCT fast-sync
        // checkpoint path. See [`VctWriteManager`].
        let mut vct_write_manager = VctWriteManager::new(vct_root_repair_sender.clone());

        // Write all the finalized blocks sent by the state,
        // until the state closes the finalized block channel's sender.
        loop {
            match non_finalized_block_write_receiver.try_recv() {
                Ok(msg) => deferred_non_finalized_messages.push_back(msg),
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => {}
            }

            let ordered_block = match vct_write_manager.take_ready() {
                Some(block) => block,
                None => match finalized_block_write_receiver.try_recv() {
                    Ok(block) => block,
                    Err(TryRecvError::Empty) => {
                        std::thread::park_timeout(Duration::from_millis(10));
                        continue;
                    }
                    Err(TryRecvError::Disconnected) => break,
                },
            };

            // TODO: split these checks into separate functions

            if invalid_block_reset_sender.is_closed() {
                info!("StateService closed the block reset channel. Is Zakura shutting down?");
                return;
            }

            // Discard any children of invalid blocks in the channel
            //
            // `commit_finalized()` requires blocks in height order.
            // So if there has been a block commit error,
            // we need to drop all the descendants of that block,
            // until we receive a block at the required next height.
            let next_valid_height = finalized_state
                .db
                .finalized_tip_height()
                .map(|height| (height + 1).expect("committed heights are valid"))
                .unwrap_or(Height(0));

            if ordered_block.0.height != next_valid_height {
                debug!(
                    ?next_valid_height,
                    invalid_height = ?ordered_block.0.height,
                    invalid_hash = ?ordered_block.0.hash,
                    "got a block that was the wrong height. \
                     Assuming a parent block failed, and dropping this block",
                );

                // The pipeline is broken; drop any look-ahead so commit resumes
                // from the real finalized tip.
                vct_write_manager.reset(finalized_state);

                // We don't want to send a reset here, because it could overwrite a valid sent hash
                std::mem::drop(ordered_block);
                continue;
            }

            // Peek the next block so VCT fast commits can verify the current
            // block's supplied roots against the successor's header.
            vct_write_manager.fill_successor(finalized_block_write_receiver, &ordered_block);

            // Fast VCT commits use the already-validated Zakura header store as their
            // successor witness. A checkpoint-verified body is not sufficient: NU5+
            // block hashes do not bind authorizing data, so an altered same-hash body
            // could supply the wrong auth-data root and make a valid current root look
            // invalid. The buffered body remains in the look-ahead for its own commit.
            let needs_vct_successor =
                finalized_state.vct_fast_needs_successor(ordered_block.0.height);
            let next_vct_block = if needs_vct_successor {
                header_chain.as_ref().and_then(|writer| {
                    writer.vct_successor(
                        &finalized_state.db,
                        ordered_block.0.height,
                        ordered_block.0.hash,
                    )
                })
            } else {
                None
            };

            if needs_vct_successor && next_vct_block.is_none() {
                let height = ordered_block.0.height;
                let wait =
                    vct_write_manager.on_retryable_error(height, false, false, ordered_block);
                std::thread::park_timeout(wait);
                continue;
            }

            // The successor header authenticates the current block's supplied roots.
            // Header-sync stores its ZIP-244 auth-data root alongside the contextually
            // validated header, so this check does not require the successor body.
            let prev_note_commitment_trees = prev_finalized_note_commitment_trees.take();
            let prev_note_commitment_trees_for_retry = prev_note_commitment_trees.clone();

            let next_block_took_vct_path =
                finalized_state.vct_fast_will_apply(ordered_block.0.height);

            // Try committing the block
            match finalized_state.commit_finalized(
                ordered_block,
                prev_note_commitment_trees,
                next_vct_block,
            ) {
                Ok((finalized, note_commitment_trees)) => {
                    // Whether this successful commit consumed header-carried
                    // tree-aux roots to skip the note-commitment frontier rebuild.
                    if next_block_took_vct_path {
                        metrics::counter!("state.vct.fast_path.hit").increment(1);
                    } else {
                        metrics::counter!("state.vct.fast_path.miss").increment(1);
                    }

                    // A successful commit clears any VCT root stall: log recovery and reset
                    // the stalled-height gauge if it had been raised.
                    vct_write_manager.on_commit_success();

                    let tip_block = ChainTipBlock::from(finalized);
                    prev_finalized_note_commitment_trees = Some(note_commitment_trees);
                    chain_tip_sender.set_finalized_tip(tip_block);
                }
                Err((ordered_block, error)) => {
                    // Retryable VCT root stalls (an absent/evicted root, or one not yet
                    // verifiable for lack of a stored successor header) park-and-retry the same
                    // block in place rather than resetting the queue. An absent root can only
                    // be filled by a re-delivery of its header range (roots are not
                    // individually re-requested), so it polls slowly; an await-successor
                    // stall just waits for the next header to be stored, so it polls faster.
                    if let Some(height) = error.vct_retryable_height() {
                        let root_unavailable = error.vct_supplied_root_unavailable_height();

                        prev_finalized_note_commitment_trees = prev_note_commitment_trees_for_retry;
                        let wait = vct_write_manager.on_retryable_error(
                            height,
                            root_unavailable.is_some(),
                            next_block_took_vct_path,
                            ordered_block,
                        );
                        std::thread::park_timeout(wait);
                        continue;
                    }

                    let finalized_tip = finalized_state.db.tip();
                    let _ = ordered_block.1.send(Err(error.clone()));

                    // The commit failed and the queue is being reset, so clear
                    // any buffered look-ahead block.
                    vct_write_manager.reset(finalized_state);

                    // The last block in the queue failed, so we can't commit the next block.
                    // Instead, we need to reset the state queue,
                    // and discard any children of the invalid block in the channel.
                    info!(
                        ?error,
                        last_valid_height = ?finalized_tip.map(|tip| tip.0),
                        last_valid_hash = ?finalized_tip.map(|tip| tip.1),
                        "committing a block to the finalized state failed, resetting state queue",
                    );

                    let send_result =
                        invalid_block_reset_sender.send(finalized_state.db.finalized_tip_hash());

                    if send_result.is_err() {
                        info!(
                            "StateService closed the block reset channel. Is Zakura shutting down?"
                        );
                        return;
                    }
                }
            }
        }

        // Do this check even if the channel got closed before any finalized blocks were sent.
        // This can happen if we're past the finalized tip.
        if invalid_block_reset_sender.is_closed() {
            info!("StateService closed the block reset channel. Is Zakura shutting down?");
            return;
        }

        if *attach_header_chain_at_handoff && header_chain.is_none() {
            *header_chain = Some(
                HeaderChainWriter::attach_at_semantic_handoff(finalized_state, non_finalized_state)
                    .expect(
                        "header-chain startup reconciliation must succeed before semantic writes",
                    ),
            );
        }
        if let Some(writer) = header_chain {
            // Publish the coherent reader before the snapshot that enables header-sync negotiation,
            // so every negotiated requester can immediately obtain its exact locator.
            header_chain_observers
                .reader_sender
                .send_replace(Some(writer.runtime.reader()));
            writer
                .runtime
                .publisher()
                .mirror_to(header_chain_observers.snapshot_sender.clone());
        }

        // Save any errors to propagate down to queued child blocks
        let mut parent_error_map: IndexMap<block::Hash, ValidateContextError> = IndexMap::new();

        while let Some(msg) = deferred_non_finalized_messages
            .pop_front()
            .or_else(|| non_finalized_block_write_receiver.blocking_recv())
        {
            let queued_child_and_rsp_tx = match msg {
                NonFinalizedWriteMessage::ApplyHeaderChainInsert {
                    expected_version,
                    insert,
                    rsp_tx,
                } => {
                    let result = header_chain
                        .as_ref()
                        .ok_or(HeaderChainStoreError::Uninitialized)
                        .and_then(|writer| {
                            writer.runtime.apply(
                                TransitionRequest {
                                    expected_version,
                                    event: TransitionEvent::InsertHeaders(insert),
                                },
                                &writer.context(),
                            )
                        });
                    let _ = rsp_tx.send(result);
                    None
                }
                NonFinalizedWriteMessage::RecordHeaderChainBodyUnavailable {
                    expected_version,
                    failure,
                    rsp_tx,
                } => {
                    let result = header_chain
                        .as_ref()
                        .ok_or(HeaderChainStoreError::Uninitialized)
                        .and_then(|writer| {
                            writer.record_body_unavailable(expected_version, failure)
                        });
                    let _ = rsp_tx.send(result);
                    None
                }
                NonFinalizedWriteMessage::RestartHeaderChainBodyAvailability {
                    expected_version,
                    discovery,
                    rsp_tx,
                } => {
                    let result = header_chain
                        .as_ref()
                        .ok_or(HeaderChainStoreError::Uninitialized)
                        .and_then(|writer| {
                            writer.restart_body_availability(expected_version, discovery)
                        });
                    let _ = rsp_tx.send(result);
                    None
                }
                NonFinalizedWriteMessage::Commit(queued_child) => Some(queued_child),
                NonFinalizedWriteMessage::Invalidate { hash, rsp_tx } => {
                    tracing::info!(?hash, "invalidating a block in the non-finalized state");
                    let result = if let Some(writer) = header_chain.as_ref() {
                        let mut staged = non_finalized_state.clone();
                        staged.invalidate_block(hash).and_then(|result| {
                            commit_operator_change(writer, non_finalized_state, staged, hash, true)
                                .map(|_| result)
                                .map_err(|error| InvalidateError::HeaderChain {
                                    error: error.to_string(),
                                })
                        })
                    } else {
                        non_finalized_state.invalidate_block(hash)
                    };
                    if result.is_ok() {
                        update_channels_after_operator_change(
                            non_finalized_state,
                            finalized_state,
                            chain_tip_sender,
                            non_finalized_state_sender,
                            backup_dir_path.as_deref(),
                        );
                    }
                    let _ = rsp_tx.send(result);
                    None
                }
                NonFinalizedWriteMessage::Reconsider { hash, rsp_tx } => {
                    tracing::info!(?hash, "reconsidering a block in the non-finalized state");
                    let result = if let Some(writer) = header_chain.as_ref() {
                        let mut staged = non_finalized_state.clone();
                        staged
                            .reconsider_block(hash, &finalized_state.db)
                            .and_then(|result| {
                                commit_operator_change(
                                    writer,
                                    non_finalized_state,
                                    staged,
                                    hash,
                                    false,
                                )
                                .map(|_| result)
                                .map_err(|error| {
                                    ReconsiderError::HeaderChain {
                                        error: error.to_string(),
                                    }
                                })
                            })
                    } else {
                        non_finalized_state.reconsider_block(hash, &finalized_state.db)
                    };
                    if result.is_ok() {
                        update_channels_after_operator_change(
                            non_finalized_state,
                            finalized_state,
                            chain_tip_sender,
                            non_finalized_state_sender,
                            backup_dir_path.as_deref(),
                        );
                    }
                    let _ = rsp_tx.send(result);
                    None
                }
            };

            let Some((queued_child, rsp_tx)) = queued_child_and_rsp_tx else {
                continue;
            };

            let child_hash = queued_child.hash;
            let parent_hash = queued_child.block.header.previous_block_hash;
            let child_height = queued_child.height;
            let parent_error = parent_error_map.get(&parent_hash);

            // If the parent block was marked as rejected, also reject all its children.
            //
            // At this point, we know that all the block's descendants
            // are invalid, because we checked all the consensus rules before
            // committing the failing ancestor block to the non-finalized state.
            let result: Result<(), CommitBlockError> = if let Some(parent_error) = parent_error {
                Err(Box::new(parent_error.clone()).into())
            } else {
                tracing::trace!(?child_hash, "validating queued child");
                if let Some(writer) = header_chain.as_ref() {
                    let mut staged = non_finalized_state.clone();
                    validate_and_commit_non_finalized(
                        &finalized_state.db,
                        &mut staged,
                        queued_child,
                    )
                    .map_err(|error| CommitBlockError::from(Box::new(error)))
                    .and_then(|()| {
                        let accepted = Frontier::new(child_height, child_hash);
                        let (evidence, event_path, request) =
                            verified_request(writer, non_finalized_state, &staged, accepted)
                                .map_err(|error| CommitBlockError::HeaderChainError {
                                    error: error.to_string(),
                                })?;
                        PreparedFullStateTransition::new(
                            evidence,
                            writer
                                .runtime
                                .publisher()
                                .snapshot()
                                .frontiers
                                .verified_best,
                            event_path,
                            staged,
                            None,
                            request,
                        )
                        .map_err(|error| CommitBlockError::HeaderChainError {
                            error: error.to_string(),
                        })?
                        .commit(&writer.runtime, non_finalized_state, &writer.context())
                        .map(|_| ())
                        .map_err(|error| {
                            CommitBlockError::HeaderChainError {
                                error: error.to_string(),
                            }
                        })
                    })
                } else {
                    validate_and_commit_non_finalized(
                        &finalized_state.db,
                        non_finalized_state,
                        queued_child,
                    )
                    .map_err(|error| CommitBlockError::from(Box::new(error)))
                }
            };

            // TODO: fix the test timing bugs that require the result to be sent
            //       after `update_latest_chain_channels()`,
            //       and send the result on rsp_tx here

            if let Err(ref error) = result {
                // If the block is invalid, mark any descendant blocks as rejected.
                if let CommitBlockError::ValidateContextError(error) = error {
                    parent_error_map.insert(child_hash, (**error).clone());
                }

                // Make sure the error map doesn't get too big.
                if parent_error_map.len() > PARENT_ERROR_MAP_LIMIT {
                    // We only add one hash at a time, so we only need to remove one extra here.
                    parent_error_map.shift_remove_index(0);
                }

                // Signal the StateService to drop this hash from
                // `non_finalized_block_write_sent_hashes`, so a subsequent
                // re-delivery of a block at the same hash is not short-circuited
                // as a "duplicate" against a rejected variant that never reached
                // any chain.
                //
                // If the receiver was dropped (the StateService is shutting
                // down), ignore the error: the lockout cannot matter once the
                // service exits.
                let _ = non_finalized_rejected_sender.send(child_hash);

                // Update the caller with the error.
                let _ = rsp_tx.send(result.map(|()| child_hash).map_err(Into::into));

                // Skip the things we only need to do for successfully committed blocks
                continue;
            }

            // A successfully committed block supersedes any contextual error
            // recorded for a different block body with the same header hash.
            parent_error_map.shift_remove(&child_hash);

            // Committing blocks to the finalized state keeps the same chain,
            // so we can update the chain seen by the rest of the application now.
            //
            // TODO: if this causes state request errors due to chain conflicts,
            //       fix the `service::read` bugs,
            //       or do the channel update after the finalized state commit
            let tip_block_height = update_latest_chain_channels(
                non_finalized_state,
                chain_tip_sender,
                non_finalized_state_sender,
                backup_dir_path.as_deref(),
            );

            // Update the caller with the result.
            let _ = rsp_tx.send(result.map(|()| child_hash).map_err(Into::into));

            while non_finalized_state
                .best_chain_len()
                .expect("just successfully inserted a non-finalized block above")
                > MAX_BLOCK_REORG_HEIGHT
            {
                tracing::trace!("finalizing block past the reorg limit");
                let commit_result = if let Some(writer) = header_chain.as_ref() {
                    commit_contextual_finalization(
                        writer,
                        finalized_state,
                        non_finalized_state,
                        prev_finalized_note_commitment_trees.take(),
                    )
                } else {
                    let finalizable = non_finalized_state.finalize();
                    finalized_state.commit_finalized_direct(
                        finalizable,
                        prev_finalized_note_commitment_trees.take(),
                        None,
                        "commit contextually-verified request",
                    )
                };
                prev_finalized_note_commitment_trees = commit_result
                    .expect(
                        "unexpected finalized block commit error: note commitment and history trees were already checked by the non-finalized state",
                    )
                    .1
                    .into();
                if header_chain.is_some() {
                    update_latest_chain_channels(
                        non_finalized_state,
                        chain_tip_sender,
                        non_finalized_state_sender,
                        backup_dir_path.as_deref(),
                    );
                }
            }

            // Update the metrics if semantic and contextual validation passes
            //
            // TODO: split this out into a function?
            metrics::counter!("state.full_verifier.committed.block.count").increment(1);
            metrics::counter!("zcash.chain.verified.block.total").increment(1);

            metrics::gauge!("state.full_verifier.committed.block.height")
                .set(tip_block_height.0 as f64);

            // This height gauge is updated for both fully verified and checkpoint blocks.
            // These updates can't conflict, because this block write task makes sure that blocks
            // are committed in order.
            metrics::gauge!("zcash.chain.verified.block.height").set(tip_block_height.0 as f64);

            tracing::trace!("finished processing queued block");
        }

        // We're finished receiving non-finalized blocks from the state, and
        // done writing to the finalized state, so we can force it to shut down.
        finalized_state.db.shutdown(true);
        std::mem::drop(self.finalized_state);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use zakura_chain::{
        block,
        parameters::{Network, NetworkUpgrade},
        serialization::ZcashDeserializeInto,
        transaction::{arbitrary::transaction_to_fake_v5, Transaction},
        value_balance::ValueBalance,
    };

    use crate::{
        arbitrary::Prepare,
        service::{
            finalized_state::{
                header_chain::{HeaderChainStore, HeaderChainStoreError},
                FinalizedState,
            },
            non_finalized_state::NonFinalizedState,
            write::{
                commit_contextual_finalization, commit_operator_change, verified_path,
                verified_request, HeaderChainWriter, PreparedFullStateTransition,
            },
        },
        tests::FakeChainHelper,
        CheckpointVerifiedBlock, Config,
    };
    use zakura_header_chain::{
        AlarmSet, BodyUnavailableSummary, BodyValidationState, ChainScore, CheckpointSet,
        EngineConfig, EngineMetadata, EngineMode, EvidenceId, FinalityEpoch, Frontier, FrontierSet,
        HeaderChainDiskVersion, HeaderGeneration, HeaderNode, HeaderValidationState, StateVersion,
        SuffixWork, TransientBodyFailure, TransientBodyFailureKind, TransitionFailure,
        TrustedAnchor, VerifiedGeneration, WorkCoordinate,
    };

    fn header_writer(
        finalized_state: &FinalizedState,
        network: &Network,
        anchor_height: block::Height,
        anchor_block: &Arc<zakura_chain::block::Block>,
    ) -> HeaderChainWriter {
        let frontier = Frontier::new(anchor_height, anchor_block.hash());
        let config = EngineConfig::new(
            EngineMode::Integrated,
            network.clone(),
            TrustedAnchor {
                frontier,
                header: anchor_block.header.clone(),
            },
            CheckpointSet::default(),
        )
        .expect("the full-state fixture anchor is coherent");
        let work = anchor_block
            .header
            .difficulty_threshold
            .to_work()
            .expect("the fixture target has exact work");
        let anchor = HeaderNode::from_durable_parts(
            anchor_block.header.clone(),
            frontier.hash,
            anchor_block.header.previous_block_hash,
            frontier.height,
            work,
            WorkCoordinate::new(frontier.hash, work.as_u256()),
            HeaderValidationState::Valid,
            Default::default(),
            BodyValidationState::Verified {
                evidence: EvidenceId::from_digest([0x70; 32]),
            },
            Vec::new(),
        )
        .expect("the anchor node fields agree");
        let metadata = EngineMetadata {
            disk_format: HeaderChainDiskVersion(1),
            mode: EngineMode::Integrated,
            network_id: config.network.kind(),
            anchor_manifest_digest: config.trust_anchor_digest(),
            work_origin: frontier,
            state_version: StateVersion::new(1),
            header_generation: HeaderGeneration::new(1),
            verified_generation: VerifiedGeneration::new(1),
            finality_epoch: FinalityEpoch::new(0),
            frontiers: FrontierSet {
                finalized: frontier,
                header_best: frontier,
                verified_best: frontier,
            },
            header_best_score: ChainScore::new(SuffixWork::zero(), frontier.hash),
            oldest_retained_height: frontier.height,
            alarms: AlarmSet::default(),
            last_transition_id: EvidenceId::from_digest([0x71; 32]),
        };
        let store = HeaderChainStore::new(finalized_state.db.db().clone());
        store
            .initialize(metadata, anchor)
            .expect("the fixture header store initializes");
        let (runtime, _) = store
            .startup(&config)
            .expect("the fixture header store audits");
        HeaderChainWriter::new(runtime, config)
    }

    #[test]
    fn production_body_unavailability_writer_authenticates_exact_evidence() {
        let _init_guard = zakura_test::init();
        let network = Network::Mainnet;
        let finalized_state = FinalizedState::new(
            &Config::ephemeral(),
            &network,
            #[cfg(feature = "elasticsearch")]
            false,
        )
        .expect("the fixture finalized state opens");
        let anchor = zakura_test::vectors::BLOCK_MAINNET_434873_BYTES
            .zcash_deserialize_into::<Arc<zakura_chain::block::Block>>()
            .expect("the anchor block deserializes");
        let anchor_height = anchor
            .coinbase_height()
            .expect("the anchor has a coinbase height");
        let writer = header_writer(&finalized_state, &network, anchor_height, &anchor);
        let result = writer.record_body_unavailable(
            StateVersion::new(1),
            TransientBodyFailure {
                hash: anchor.hash(),
                evidence: EvidenceId::from_digest([0x72; 32]),
                kind: TransientBodyFailureKind::Storage,
                availability: BodyUnavailableSummary {
                    attempts: 1,
                    suppliers: 1,
                    alarmed: false,
                    ..Default::default()
                },
            },
        );

        assert!(matches!(
            result,
            Err(HeaderChainStoreError::Transition(
                TransitionFailure::InvalidEvidence(
                    "body retry evidence cannot regress an already verified body"
                )
            ))
        ));
    }

    #[test]
    fn staged_grow_reset_invalidate_and_reconsider_keep_both_frontiers_atomic() {
        let _init_guard = zakura_test::init();
        let network = Network::Mainnet;
        let finalized_state = FinalizedState::new(
            &Config::ephemeral(),
            &network,
            #[cfg(feature = "elasticsearch")]
            false,
        )
        .expect("the fixture finalized state opens");
        finalized_state.set_finalized_value_pool(ValueBalance::fake_populated_pool());
        let parent = zakura_test::vectors::BLOCK_MAINNET_434873_BYTES
            .zcash_deserialize_into::<Arc<zakura_chain::block::Block>>()
            .expect("the parent block deserializes");
        let parent_height = parent
            .coinbase_height()
            .expect("the parent has a coinbase height");
        let writer = header_writer(&finalized_state, &network, parent_height, &parent);
        let mut live = NonFinalizedState::new(&network);

        let common = parent.make_fake_child().set_work(1);
        let first = common.make_fake_child().set_work(10);
        let first_frontier = Frontier::new(
            first.coinbase_height().expect("the child has a height"),
            first.hash(),
        );
        let mut staged = live.clone();
        staged
            .commit_new_chain(common.clone().prepare(), &finalized_state.db)
            .expect("the common full-state block validates");
        staged
            .commit_block(first.clone().prepare(), &finalized_state.db)
            .expect("the first full-state branch validates");
        let (evidence, event_path, request) =
            verified_request(&writer, &live, &staged, first_frontier)
                .expect("the grow evidence matches full state");
        PreparedFullStateTransition::new(
            evidence,
            writer
                .runtime
                .publisher()
                .snapshot()
                .frontiers
                .verified_best,
            event_path,
            staged,
            None,
            request,
        )
        .expect("the grow staging facts agree")
        .commit(&writer.runtime, &mut live, &writer.context())
        .expect("grow commits before swapping full state");
        assert_eq!(
            live.best_tip(),
            Some((first_frontier.height, first_frontier.hash))
        );
        assert_eq!(
            writer
                .runtime
                .publisher()
                .snapshot()
                .frontiers
                .verified_best,
            first_frontier
        );

        let replacement = common.make_fake_child().set_work(20);
        let replacement_frontier = Frontier::new(
            replacement
                .coinbase_height()
                .expect("the replacement has a height"),
            replacement.hash(),
        );
        let mut staged = live.clone();
        staged
            .commit_block(replacement.clone().prepare(), &finalized_state.db)
            .expect("the higher-work replacement validates");
        let (evidence, event_path, request) =
            verified_request(&writer, &live, &staged, replacement_frontier)
                .expect("the reset evidence matches full state");
        PreparedFullStateTransition::new(
            evidence,
            writer
                .runtime
                .publisher()
                .snapshot()
                .frontiers
                .verified_best,
            event_path,
            staged,
            None,
            request,
        )
        .expect("the reset staging facts agree")
        .commit(&writer.runtime, &mut live, &writer.context())
        .expect("reset commits before swapping full state");
        assert_eq!(
            writer
                .runtime
                .publisher()
                .snapshot()
                .frontiers
                .verified_best,
            replacement_frontier
        );

        let mut staged = live.clone();
        staged
            .invalidate_block(replacement_frontier.hash)
            .expect("the winning replacement invalidates in staged state");
        commit_operator_change(&writer, &mut live, staged, replacement_frontier.hash, true)
            .expect("invalidation commits both frontiers before swapping state");
        assert_eq!(
            live.best_tip(),
            Some((first_frontier.height, first_frontier.hash))
        );
        assert_eq!(
            writer
                .runtime
                .publisher()
                .snapshot()
                .frontiers
                .verified_best,
            first_frontier
        );

        let mut staged = live.clone();
        staged
            .reconsider_block(replacement_frontier.hash, &finalized_state.db)
            .expect("the replacement replays into staged state");
        commit_operator_change(&writer, &mut live, staged, replacement_frontier.hash, false)
            .expect("reconsider commits both frontiers before swapping state");
        assert_eq!(
            live.best_tip(),
            Some((replacement_frontier.height, replacement_frontier.hash))
        );
        assert_eq!(
            writer
                .runtime
                .publisher()
                .snapshot()
                .frontiers
                .verified_best,
            replacement_frontier
        );
        assert_eq!(
            verified_path(&live)
                .last()
                .map(|header| Frontier::new(header.height, header.hash)),
            Some(replacement_frontier)
        );

        let mut staged = live.clone();
        staged
            .invalidate_block(common.hash())
            .expect("invalidating the common root empties every full-state branch");
        commit_operator_change(&writer, &mut live, staged, common.hash(), true)
            .expect("empty full state commits its exact finalized fallback");
        assert!(live.is_chain_set_empty());
        let snapshot = writer.runtime.publisher().snapshot();
        assert_eq!(
            snapshot.frontiers.verified_best,
            snapshot.frontiers.finalized
        );
    }

    #[test]
    fn contextual_finalization_commits_full_state_header_rows_and_memory_together() {
        let _init_guard = zakura_test::init();
        let network = Network::Mainnet;
        let mut finalized_state = FinalizedState::new(
            &Config::ephemeral(),
            &network,
            #[cfg(feature = "elasticsearch")]
            false,
        )
        .expect("the fixture finalized state opens");
        let genesis = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into::<Arc<zakura_chain::block::Block>>()
            .expect("genesis deserializes");
        finalized_state
            .commit_finalized_direct(
                CheckpointVerifiedBlock::from(genesis.clone()).into(),
                None,
                None,
                "shared finalization fixture genesis",
            )
            .expect("genesis commits");
        let block1 = genesis.make_fake_child().set_work(10);
        let block1_height = block1.coinbase_height().expect("block one has a height");
        finalized_state
            .commit_finalized_direct(
                CheckpointVerifiedBlock::from(block1.clone()).into(),
                None,
                None,
                "shared finalization fixture block one",
            )
            .expect("block one commits");
        let writer = header_writer(&finalized_state, &network, block1_height, &block1);
        let mut block2 = block1.make_fake_child().set_work(10);
        let block2_height = block2.coinbase_height().expect("block two has a height");
        let mut block2_tx =
            transaction_to_fake_v5(&block2.transactions[0], &network, block2_height);
        let Transaction::V5 {
            network_upgrade, ..
        } = &mut block2_tx
        else {
            unreachable!("the fake-v5 converter always returns v5 for genesis transactions")
        };
        *network_upgrade = NetworkUpgrade::Nu5;
        Arc::make_mut(&mut block2).transactions[0] = Arc::new(block2_tx);
        let frontier = Frontier::new(block2_height, block2.hash());
        let mut live = NonFinalizedState::new(&network);
        let mut staged = live.clone();
        staged
            .commit_new_chain(block2.prepare(), &finalized_state.db)
            .expect("block two validates into staged full state");
        let (evidence, event_path, request) = verified_request(&writer, &live, &staged, frontier)
            .expect("block two produces exact verified growth");
        PreparedFullStateTransition::new(
            evidence,
            writer
                .runtime
                .publisher()
                .snapshot()
                .frontiers
                .verified_best,
            event_path,
            staged,
            None,
            request,
        )
        .expect("block two staging facts agree")
        .commit(&writer.runtime, &mut live, &writer.context())
        .expect("block two commits to both live views");

        commit_contextual_finalization(&writer, &mut finalized_state, &mut live, None)
            .expect("the finalized block and header transition commit together");

        assert!(live.is_chain_set_empty());
        assert_eq!(
            finalized_state.db.tip(),
            Some((frontier.height, frontier.hash))
        );
        let snapshot = writer.runtime.publisher().snapshot();
        assert_eq!(snapshot.frontiers.finalized, frontier);
        assert_eq!(snapshot.frontiers.verified_best, frontier);
        let (reopened, _) = HeaderChainStore::new(finalized_state.db.db().clone())
            .startup(&writer.config)
            .expect("the combined finalized/header transaction reopens coherently");
        assert_eq!(reopened.publisher().snapshot(), snapshot);
    }
}
