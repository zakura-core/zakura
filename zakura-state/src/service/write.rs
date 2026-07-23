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
    ApplyResult, AuxAuthentication, AuxEvidence, BodyEvidence, CheckpointSet, EngineConfig,
    EngineConfigError, EngineMode, EngineSnapshot, EvidenceId, Frontier,
    FullStateEvidenceAuthority, FullStateFinalized, OperatorInvalidate, OperatorInvalidationId,
    OperatorReconsider, StateVersion, StoreError, StoreRead, SystemClock, TransitionContext,
    TransitionEvent, TransitionRequest, TrustedAnchor, VerifiedBodyEvidence, VerifiedChainChanged,
    VerifiedChangeCause, VerifiedHeaderRef, WorkScope,
};

use crate::{
    constants::MAX_BLOCK_REORG_HEIGHT,
    request::FinalizableBlock,
    service::{
        check,
        finalized_state::{
            header_chain::{
                migration::{initialize_header_chain_reconciled, HeaderChainInitializationError},
                select_vct_aux_delivery, HeaderChainReader, HeaderChainRuntime, HeaderChainStore,
                HeaderChainStoreError,
            },
            DiskWriteBatch, FinalizedState, NextVctBlock, VctAuxRejection, VctAuxWindow, ZakuraDb,
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
pub use zakura_header_chain::{VctRootRepairState, VctRootRepairStatus};

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

#[derive(Debug)]
pub(crate) enum VctAuxWindowRead {
    Ready(Box<VctAuxWindow>),
    Missing { height: block::Height },
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

    pub(crate) fn vct_aux_window(
        &self,
        height: block::Height,
        hash: block::Hash,
    ) -> Result<VctAuxWindowRead, HeaderChainStoreError> {
        let Some(window) = self.runtime.reader().selected_aux_window(height, hash)? else {
            return Ok(VctAuxWindowRead::Missing { height });
        };
        let Some(current) = select_vct_aux_delivery(window.current_deliveries) else {
            return Ok(VctAuxWindowRead::Missing { height });
        };
        let Some(current_aux) = current.tree_aux else {
            return Ok(VctAuxWindowRead::Missing { height });
        };
        if current.header_hash != window.current.hash || current_aux.height != window.current.height
        {
            return Err(zakura_header_chain::StoreError::Incoherent(
                "selected VCT delivery disagrees with its retained header",
            )
            .into());
        }
        let successor = match window.successor {
            Some((successor, deliveries)) => {
                let Some(delivery) = select_vct_aux_delivery(deliveries) else {
                    return Ok(VctAuxWindowRead::Missing {
                        height: successor.height,
                    });
                };
                Some(
                    NextVctBlock::from_delivery(successor.header, successor.height, delivery)
                        .ok_or(zakura_header_chain::StoreError::Incoherent(
                            "selected VCT successor delivery disagrees with its retained header",
                        ))?,
                )
            }
            None => None,
        };
        Ok(VctAuxWindowRead::Ready(Box::new(VctAuxWindow {
            snapshot: window.snapshot,
            current,
            successor,
        })))
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

    fn record_body_invalid(
        &self,
        expected_version: StateVersion,
        invalid: zakura_header_chain::ConsensusBodyInvalid,
    ) -> Result<ApplyResult, HeaderChainStoreError> {
        let authority = PreparedAuthority(invalid.evidence);
        let mut context = self.context();
        context.full_state_authority = Some(&authority);
        self.runtime.apply(
            TransitionRequest {
                expected_version,
                event: TransitionEvent::BodyEvidence(BodyEvidence::ConsensusInvalid(invalid)),
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

    fn retry_body_availability(
        &self,
        expected_version: StateVersion,
        retry: zakura_header_chain::OperatorBodyRetry,
    ) -> Result<ApplyResult, HeaderChainStoreError> {
        self.runtime.apply(
            TransitionRequest {
                expected_version,
                event: TransitionEvent::OperatorBodyRetry(retry),
            },
            &self.context(),
        )
    }

    fn reject_vct_aux(
        &self,
        window: &VctAuxWindow,
        rejection: VctAuxRejection,
        failure: crate::error::VctCommitFailure,
    ) -> Result<Option<ApplyResult>, HeaderChainStoreError> {
        let deliveries = match rejection {
            VctAuxRejection::Current => vec![window.current],
            VctAuxRejection::Successor => window
                .successor
                .as_ref()
                .and_then(|successor| successor.delivery)
                .into_iter()
                .collect(),
            VctAuxRejection::Ambiguous => {
                let Some(successor) = window
                    .successor
                    .as_ref()
                    .and_then(|successor| successor.delivery)
                else {
                    return Ok(None);
                };
                vec![window.current, successor]
            }
            VctAuxRejection::None => return Ok(None),
        };
        if deliveries.is_empty() {
            return Ok(None);
        }

        let mut hasher = Sha256::new();
        hasher.update(b"zakura.vct.aux.rejection.v1");
        hasher.update([match failure {
            crate::error::VctCommitFailure::CurrentRoots => 1,
            crate::error::VctCommitFailure::SuccessorBoundary => 2,
        }]);
        for delivery in &deliveries {
            hasher.update(delivery.delivery_id.digest());
            hasher.update(delivery.header_hash.0);
        }
        let evidence = EvidenceId::from_digest(hasher.finalize().into());
        let first = deliveries
            .first()
            .expect("the empty auxiliary rejection returned above");
        let owner = WorkScope::for_body_work(&window.snapshot)
            .bind(first.owner.session_id, first.owner.request_id);
        let authority = PreparedAuthority(evidence);
        let mut context = self.context();
        context.full_state_authority = Some(&authority);

        self.runtime
            .apply(
                TransitionRequest {
                    expected_version: window.snapshot.state_version,
                    event: TransitionEvent::AuxEvidence(Box::new(AuxEvidence {
                        owner,
                        deliveries,
                        authentication: AuxAuthentication::Rejected { evidence },
                    })),
                },
                &context,
            )
            .map(Some)
    }

    fn vct_authentication_request(
        window: &VctAuxWindow,
    ) -> Option<(EvidenceId, TransitionRequest)> {
        if window.current.authentication != AuxAuthentication::Unauthenticated {
            return None;
        }
        let successor = window.successor.as_ref()?;
        let auth_data_root = successor.auth_data_root?;

        let mut hasher = Sha256::new();
        hasher.update(b"zakura.vct.aux.authentication.v1");
        hasher.update(window.current.delivery_id.digest());
        hasher.update(window.current.header_hash.0);
        hasher.update(successor.hash.0);
        hasher.update(<[u8; 32]>::from(auth_data_root));
        let evidence = EvidenceId::from_digest(hasher.finalize().into());
        let owner = WorkScope::for_body_work(&window.snapshot).bind(
            window.current.owner.session_id,
            window.current.owner.request_id,
        );

        Some((
            evidence,
            TransitionRequest {
                expected_version: window.snapshot.state_version,
                event: TransitionEvent::AuxEvidence(Box::new(AuxEvidence {
                    owner,
                    deliveries: vec![window.current],
                    authentication: AuxAuthentication::Authenticated {
                        evidence,
                        boundary_hash: successor.hash,
                    },
                })),
            },
        ))
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

fn classify_verified_change<'a>(
    old_path: &[VerifiedHeaderRef],
    new_path: &'a [VerifiedHeaderRef],
) -> (VerifiedChangeCause, &'a [VerifiedHeaderRef]) {
    let grows = new_path.len() > old_path.len()
        && new_path
            .iter()
            .zip(old_path)
            .all(|(new, old)| new.hash == old.hash);
    if grows {
        (VerifiedChangeCause::Grow, &new_path[old_path.len()..])
    } else {
        (VerifiedChangeCause::Reset, new_path)
    }
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
        let (cause, changed_path) = classify_verified_change(&old_path, &new_path);
        event_path = changed_path.to_vec();
        let evidence = full_state_evidence(
            match cause {
                VerifiedChangeCause::Grow => b"grow",
                VerifiedChangeCause::Reset => b"reset",
            },
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
                    cause,
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
    /// One commitment-matching deterministic body rejection admitted by the full verifier.
    RecordHeaderChainBodyInvalid {
        expected_version: StateVersion,
        invalid: zakura_header_chain::ConsensusBodyInvalid,
        rsp_tx: oneshot::Sender<Result<ApplyResult, HeaderChainStoreError>>,
    },
    /// A changed authenticated supplier set restarts one persistent alarm.
    RestartHeaderChainBodyAvailability {
        expected_version: StateVersion,
        discovery: zakura_header_chain::BodySupplierDiscovered,
        rsp_tx: oneshot::Sender<Result<ApplyResult, HeaderChainStoreError>>,
    },
    /// An authenticated operator request restarts one persistent alarm.
    RetryHeaderChainBodyAvailability {
        expected_version: StateVersion,
        retry: zakura_header_chain::OperatorBodyRetry,
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
            let requires_exact_vct_roots = header_chain.is_some()
                && finalized_state.vct_requires_exact_roots(ordered_block.0.height);
            let vct_aux_window = if requires_exact_vct_roots {
                match header_chain
                    .as_ref()
                    .expect("exact VCT roots are required only with an attached header chain")
                    .vct_aux_window(ordered_block.0.height, ordered_block.0.hash)
                {
                    Ok(VctAuxWindowRead::Ready(window)) => Some(*window),
                    Ok(VctAuxWindowRead::Missing { height }) => {
                        let wait = vct_write_manager.on_retryable_error(
                            height,
                            true,
                            false,
                            ordered_block,
                        );
                        std::thread::park_timeout(wait);
                        continue;
                    }
                    Err(error) => {
                        tracing::error!(
                            ?error,
                            height = ?ordered_block.0.height,
                            hash = ?ordered_block.0.hash,
                            "stopping finalized writer after incoherent header auxiliary read"
                        );
                        return;
                    }
                }
            } else {
                None
            };
            let has_exact_vct_roots = vct_aux_window.as_ref().is_some_and(|window| {
                window
                    .current_roots(ordered_block.0.height, ordered_block.0.hash)
                    .is_some()
            });
            let next_block_took_vct_path = requires_exact_vct_roots && has_exact_vct_roots;
            let needs_vct_successor = finalized_state
                .vct_fast_needs_successor(ordered_block.0.height, has_exact_vct_roots);

            if requires_exact_vct_roots && !has_exact_vct_roots {
                tracing::error!(
                    height = ?ordered_block.0.height,
                    hash = ?ordered_block.0.hash,
                    "stopping finalized writer after an incoherent ready VCT auxiliary window"
                );
                return;
            }

            if needs_vct_successor
                && vct_aux_window
                    .as_ref()
                    .and_then(|window| window.successor.as_ref())
                    .is_none()
            {
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
            let vct_aux_for_outcome = vct_aux_window.clone();
            let vct_authentication = header_chain.as_ref().and_then(|writer| {
                vct_aux_window
                    .as_ref()
                    .and_then(HeaderChainWriter::vct_authentication_request)
                    .map(|(evidence, request)| (writer.clone(), evidence, request))
            });
            let commit_height = ordered_block.0.height;

            // Try committing the block
            match finalized_state.commit_finalized_with_aux_and(
                ordered_block,
                prev_note_commitment_trees,
                vct_aux_window,
                |db, batch| {
                    let Some((writer, evidence, request)) = vct_authentication else {
                        db.header_chain_disk_db()
                            .write(batch)
                            .expect("unexpected rocksdb error while writing block");
                        return Ok(());
                    };
                    let authority = PreparedAuthority(evidence);
                    let mut context = writer.context();
                    context.full_state_authority = Some(&authority);
                    match writer
                        .runtime
                        .apply_combined(request, &context, batch, || {})
                    {
                        Ok(ApplyResult::Committed(_) | ApplyResult::NoChange(_)) => Ok(()),
                        Ok(ApplyResult::Stale(receipt)) => {
                            tracing::debug!(
                                ?receipt,
                                "VCT: exact auxiliary authentication became stale before commit"
                            );
                            Err(ValidateContextError::VctSuppliedRootAwaitingSuccessor {
                                height: commit_height,
                            }
                            .into())
                        }
                        Err(error) => Err(CommitBlockError::HeaderChainError {
                            error: error.to_string(),
                        }
                        .into()),
                    }
                },
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
                    let mut rejected_aux_height = None;
                    if let (Some(window), Some(failure)) =
                        (vct_aux_for_outcome.as_ref(), error.vct_failure())
                    {
                        let rejection = window.classify_failure(failure);
                        let attribution = match rejection {
                            VctAuxRejection::Current => "current",
                            VctAuxRejection::Successor => "successor",
                            VctAuxRejection::Ambiguous => "ambiguous",
                            VctAuxRejection::None => "none",
                        };
                        metrics::counter!(
                            "state.vct.aux.verification_failure.count",
                            "attribution" => attribution
                        )
                        .increment(1);
                        tracing::warn!(
                            ?failure,
                            attribution,
                            "VCT: classified exact auxiliary verification failure"
                        );

                        if let Some(writer) = header_chain.as_ref() {
                            match writer.reject_vct_aux(window, rejection, failure) {
                                Ok(Some(ApplyResult::Committed(_) | ApplyResult::NoChange(_))) => {
                                    rejected_aux_height = match rejection {
                                        VctAuxRejection::Current | VctAuxRejection::Ambiguous => {
                                            Some(ordered_block.0.height)
                                        }
                                        VctAuxRejection::Successor => window
                                            .successor
                                            .as_ref()
                                            .map(|successor| successor.height),
                                        VctAuxRejection::None => None,
                                    };
                                }
                                Ok(Some(ApplyResult::Stale(receipt))) => {
                                    tracing::debug!(
                                        ?receipt,
                                        "VCT: ignored stale auxiliary rejection"
                                    );
                                }
                                Ok(None) => {}
                                Err(rejection_error) => {
                                    tracing::error!(
                                        ?rejection_error,
                                        "VCT: could not persist auxiliary rejection"
                                    );
                                }
                            }
                        }
                    }

                    // Retryable VCT root stalls (an absent/evicted root, or one not yet
                    // verifiable for lack of a stored successor header) park-and-retry the same
                    // block in place rather than resetting the queue. An absent root can only
                    // be filled by a re-delivery of its header range (roots are not
                    // individually re-requested), so it polls slowly; an await-successor
                    // stall just waits for the next header to be stored, so it polls faster.
                    if let Some(height) = error.vct_retryable_height() {
                        let root_unavailable = error.vct_supplied_root_unavailable_height();
                        let repair_height = rejected_aux_height.unwrap_or(height);

                        prev_finalized_note_commitment_trees = prev_note_commitment_trees_for_retry;
                        let wait = vct_write_manager.on_retryable_error(
                            repair_height,
                            root_unavailable.is_some(),
                            rejected_aux_height.is_some(),
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
                NonFinalizedWriteMessage::RecordHeaderChainBodyInvalid {
                    expected_version,
                    invalid,
                    rsp_tx,
                } => {
                    let result = header_chain
                        .as_ref()
                        .ok_or(HeaderChainStoreError::Uninitialized)
                        .and_then(|writer| writer.record_body_invalid(expected_version, invalid));
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
                NonFinalizedWriteMessage::RetryHeaderChainBodyAvailability {
                    expected_version,
                    retry,
                    rsp_tx,
                } => {
                    let result = header_chain
                        .as_ref()
                        .ok_or(HeaderChainStoreError::Uninitialized)
                        .and_then(|writer| writer.retry_body_availability(expected_version, retry));
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
        block::{
            self, genesis::regtest_genesis_block, Block, ChainHistoryBlockTxAuthCommitmentHash,
        },
        fmt::HexDebug,
        history_tree::HistoryTree,
        parameters::{
            testnet::{ConfiguredActivationHeights, ConfiguredCheckpoints, ParametersBuilder},
            Network, NetworkUpgrade, GENESIS_PREVIOUS_BLOCK_HASH,
        },
        serialization::ZcashDeserializeInto,
        transaction::{arbitrary::transaction_to_fake_v5, Transaction},
        transparent,
        work::{difficulty::ParameterDifficulty as _, equihash},
    };

    use crate::{
        arbitrary::Prepare,
        service::{
            finalized_state::{
                header_chain::{HeaderChainStore, HeaderChainStoreError},
                FinalizedState, NextVctBlock, VctAuxRejection, VctAuxWindow,
            },
            non_finalized_state::NonFinalizedState,
            write::{
                classify_verified_change, commit_contextual_finalization, commit_operator_change,
                verified_request, HeaderChainWriter, PreparedFullStateTransition,
            },
        },
        tests::FakeChainHelper,
        CheckpointVerifiedBlock, Config,
    };
    use zakura_header_chain::{
        AdjustedDifficulty, AlarmSet, ApplyResult, AuxAuthentication, BodyRuleId,
        BodyUnavailableSummary, BodyValidationState, BranchId, ChainScore, CheckpointSet,
        ConsensusBodyInvalid, EngineConfig, EngineMetadata, EngineMode, EngineSnapshot, EvidenceId,
        FinalityEpoch, Frontier, FrontierSet, HeaderBatchInput, HeaderChainDiskVersion,
        HeaderContextFact, HeaderGeneration, HeaderNode, HeaderRules, HeaderValidationState,
        InsertHeaders, SourceId, StateVersion, SuffixWork, SystemClock, TargetCompletion,
        TransientBodyFailure, TransientBodyFailureKind, TransitionContext, TransitionEvent,
        TransitionFailure, TransitionRequest, TrustedAnchor, ValidationLease, VerifiedChangeCause,
        VerifiedGeneration, VerifiedHeaderRef, WorkCoordinate, WorkOwner, WorkScope,
        POW_ADJUSTMENT_BLOCK_SPAN,
    };

    #[test]
    fn vct_aux_selection_prefers_authenticated_complete_nonrejected_provenance() {
        let delivery = |byte: u8,
                        authentication: zakura_header_chain::AuxAuthentication,
                        has_aux: bool| zakura_header_chain::AuxDelivery {
            delivery_id: EvidenceId::from_digest([byte; 32]),
            header_hash: block::Hash([1; 32]),
            source: zakura_header_chain::SourceId::from_digest([byte; 32]),
            owner: zakura_header_chain::WorkOwner {
                state_version: StateVersion::new(1),
                header_generation: HeaderGeneration::new(2),
                verified_generation: Some(VerifiedGeneration::new(3)),
                branch: zakura_header_chain::BranchId::new(
                    block::Hash([4; 32]),
                    block::Hash([5; 32]),
                ),
                session_id: 6,
                request_id: std::num::NonZeroU64::new(7).expect("seven is nonzero"),
            },
            body_size: zakura_header_chain::BodySizeHint::Unknown,
            tree_aux: has_aux.then_some(zakura_header_chain::TreeAuxRecordV1 {
                height: block::Height(1),
                sapling_root: zakura_chain::sapling::tree::Root::default(),
                orchard_root: zakura_chain::orchard::tree::Root::default(),
                ironwood_root: zakura_chain::ironwood::tree::Root::default(),
                sapling_tx_count: 0,
                orchard_tx_count: 0,
                ironwood_tx_count: 0,
                auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([0; 32]),
            }),
            authentication,
        };
        let rejected = delivery(
            1,
            zakura_header_chain::AuxAuthentication::Rejected {
                evidence: EvidenceId::from_digest([8; 32]),
            },
            true,
        );
        let unauthenticated = delivery(
            2,
            zakura_header_chain::AuxAuthentication::Unauthenticated,
            true,
        );
        let authenticated = delivery(
            3,
            zakura_header_chain::AuxAuthentication::Authenticated {
                evidence: EvidenceId::from_digest([9; 32]),
                boundary_hash: block::Hash([10; 32]),
            },
            true,
        );
        let incomplete = delivery(
            0,
            zakura_header_chain::AuxAuthentication::Authenticated {
                evidence: EvidenceId::from_digest([11; 32]),
                boundary_hash: block::Hash([12; 32]),
            },
            false,
        );

        assert_eq!(
            super::select_vct_aux_delivery(vec![
                rejected,
                unauthenticated,
                authenticated,
                incomplete,
            ]),
            Some(authenticated)
        );
        assert_eq!(
            super::select_vct_aux_delivery(vec![rejected, incomplete]),
            None
        );

        let window = VctAuxWindow {
            snapshot: EngineSnapshot {
                mode: EngineMode::Integrated,
                state_version: StateVersion::new(1),
                header_generation: HeaderGeneration::new(2),
                verified_generation: VerifiedGeneration::new(3),
                frontiers: FrontierSet {
                    finalized: Frontier::new(block::Height(0), block::Hash([0; 32])),
                    header_best: Frontier::new(block::Height(1), block::Hash([1; 32])),
                    verified_best: Frontier::new(block::Height(0), block::Hash([0; 32])),
                },
                header_best_score: ChainScore::new(SuffixWork::zero(), block::Hash([1; 32])),
                oldest_retained_height: block::Height(0),
                alarms: AlarmSet::default(),
            },
            current: authenticated,
            successor: None,
        };
        let expected_roots = authenticated
            .tree_aux
            .map(|aux| (aux.sapling_root, aux.orchard_root, aux.ironwood_root))
            .expect("the authenticated fixture contains tree auxiliary data");
        assert_eq!(
            window.current_roots(block::Height(1), block::Hash([1; 32])),
            Some(expected_roots)
        );
        assert_eq!(
            window.current_roots(block::Height(2), block::Hash([1; 32])),
            None,
            "height-mismatched provenance fails closed"
        );
        assert_eq!(
            window.current_roots(block::Height(1), block::Hash([2; 32])),
            None,
            "hash-mismatched provenance fails closed"
        );
        assert!(
            HeaderChainWriter::vct_authentication_request(&window).is_none(),
            "already authenticated metadata needs no new transition"
        );

        let successor_block = zakura_test::vectors::BLOCK_MAINNET_434873_BYTES
            .zcash_deserialize_into::<Arc<zakura_chain::block::Block>>()
            .expect("the successor fixture deserializes");
        let successor_height = successor_block
            .coinbase_height()
            .expect("the successor fixture has a height");
        let successor_delivery = zakura_header_chain::AuxDelivery {
            header_hash: successor_block.hash(),
            tree_aux: Some(zakura_header_chain::TreeAuxRecordV1 {
                height: successor_height,
                auth_data_root: successor_block.auth_data_root(),
                ..unauthenticated
                    .tree_aux
                    .expect("the unauthenticated fixture contains tree auxiliary data")
            }),
            ..unauthenticated
        };
        let successor = NextVctBlock::from_delivery(
            successor_block.header.clone(),
            successor_height,
            successor_delivery,
        )
        .expect("the exact successor delivery constructs a witness");
        let auth_window = VctAuxWindow {
            snapshot: window.snapshot,
            current: unauthenticated,
            successor: Some(successor.clone()),
        };
        let (evidence, request) = HeaderChainWriter::vct_authentication_request(&auth_window)
            .expect("an unauthenticated current delivery and successor produce evidence");
        let TransitionEvent::AuxEvidence(event) = request.event else {
            panic!("VCT authentication uses the sole auxiliary evidence transition");
        };
        assert_eq!(event.deliveries, vec![unauthenticated]);
        assert_eq!(
            event.authentication,
            AuxAuthentication::Authenticated {
                evidence,
                boundary_hash: successor.hash,
            }
        );
    }

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

    fn commit_verified_change(
        writer: &HeaderChainWriter,
        live: &mut NonFinalizedState,
        staged: NonFinalizedState,
        accepted: Frontier,
    ) {
        let (evidence, event_path, request) = verified_request(writer, live, &staged, accepted)
            .expect("the generated full-state change has exact header evidence");
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
        .expect("the generated full-state and header paths agree")
        .commit(&writer.runtime, live, &writer.context())
        .expect("the generated full-state and header transition commits");
    }

    #[test]
    fn same_lower_forward_resets_use_identical_branch_path() {
        let header = regtest_genesis_block().header.clone();
        let reference = |height: u32, hash: u8| VerifiedHeaderRef {
            height: block::Height(height),
            hash: block::Hash([hash; 32]),
            header: header.clone(),
        };
        let old = vec![reference(1, 1), reference(2, 2), reference(3, 3)];
        let direct_growth = vec![
            reference(1, 1),
            reference(2, 2),
            reference(3, 3),
            reference(4, 4),
        ];
        let lower = vec![reference(1, 1), reference(2, 12)];
        let same_height = vec![reference(1, 1), reference(2, 12), reference(3, 13)];
        let forward = vec![
            reference(1, 1),
            reference(2, 12),
            reference(3, 13),
            reference(4, 14),
        ];

        let (cause, changed_path) = classify_verified_change(&old, &direct_growth);
        assert_eq!(cause, VerifiedChangeCause::Grow);
        assert_eq!(changed_path, &direct_growth[old.len()..]);

        for reset in [&lower, &same_height, &forward] {
            let (cause, changed_path) = classify_verified_change(&old, reset);
            assert_eq!(
                cause,
                VerifiedChangeCause::Reset,
                "height relative to the old tip cannot turn a divergent branch into growth"
            );
            assert_eq!(
                changed_path,
                reset.as_slice(),
                "every reset shape replaces the verified path from its exact branch identity"
            );
        }
    }

    fn assert_selected_header_matches_full_state(
        writer: &HeaderChainWriter,
        full_state: &NonFinalizedState,
    ) {
        let best = full_state
            .best_chain()
            .expect("the generated fork graph has a full-state best chain");
        let (_, tip_hash) = best.non_finalized_tip();
        let expected_work = best
            .blocks
            .values()
            .map(|block| {
                block
                    .block
                    .header
                    .difficulty_threshold
                    .to_work()
                    .expect("generated block targets have exact work")
                    .as_u256()
            })
            .fold(zakura_chain::work::difficulty::U256::zero(), |sum, work| {
                sum.checked_add(work)
                    .expect("the short generated graph cannot overflow cumulative work")
            });
        let snapshot = writer.runtime.publisher().snapshot();

        assert_eq!(snapshot.frontiers.header_best.hash, tip_hash);
        assert_eq!(snapshot.frontiers.verified_best.hash, tip_hash);
        assert_eq!(snapshot.header_best_score.tip_hash, tip_hash);
        assert_eq!(
            snapshot.header_best_score.suffix_work.as_u256(),
            expected_work
        );
    }

    #[test]
    fn df_01_observable_activation_headers_pass_shared_rules() {
        let _init_guard = zakura_test::init();

        for network in Network::iter() {
            let blocks = network.block_map();

            for upgrade in [
                NetworkUpgrade::Overwinter,
                NetworkUpgrade::Sapling,
                NetworkUpgrade::Blossom,
                NetworkUpgrade::Heartwood,
                NetworkUpgrade::Canopy,
                NetworkUpgrade::Nu5,
            ] {
                let height = upgrade
                    .activation_height(&network)
                    .expect("every production network configures this upgrade");
                let parent_height = height
                    .previous()
                    .expect("the tested upgrades activate after genesis");
                let vector_height = blocks
                    .range(height.0..)
                    .next()
                    .map(|(height, _)| *height)
                    .expect("an activation or post-activation vector exists");
                let candidate = blocks
                    .get(&vector_height)
                    .expect("the selected activation vector exists")
                    .zcash_deserialize_into::<Arc<zakura_chain::block::Block>>()
                    .expect("the activation vector deserializes");
                if vector_height == height.0 {
                    let parent = blocks
                        .get(&parent_height.0)
                        .expect("the exact activation vector has its parent vector")
                        .zcash_deserialize_into::<Arc<zakura_chain::block::Block>>()
                        .expect("the parent activation vector deserializes");
                    assert_eq!(candidate.header.previous_block_hash, parent.hash());
                }

                let parent_frontier =
                    Frontier::new(parent_height, candidate.header.previous_block_hash);
                let spacing = NetworkUpgrade::target_spacing_for_height(&network, height);
                let context_times: Vec<_> = (1..=POW_ADJUSTMENT_BLOCK_SPAN)
                    .map(|offset| {
                        let offset_i32 =
                            i32::try_from(offset).expect("the DAA context length fits in i32");
                        candidate.header.time - spacing * offset_i32
                    })
                    .collect();
                let candidate_bits =
                    u32::from_le_bytes(candidate.header.difficulty_threshold.to_le_bytes());
                let context_threshold = (-16..=16)
                    .filter_map(|delta| {
                        let bits = i64::from(candidate_bits).checked_add(i64::from(delta))?;
                        let bits = u32::try_from(bits).ok()?;
                        let threshold =
                            zakura_chain::work::difficulty::CompactDifficulty::from_le_bytes(
                                bits.to_le_bytes(),
                            );
                        let expected = AdjustedDifficulty::new_from_header_time(
                            candidate.header.time,
                            parent_height,
                            &network,
                            context_times.iter().copied().map(|time| (threshold, time)),
                        )
                        .expected_difficulty_threshold();
                        (expected == candidate.header.difficulty_threshold).then_some(threshold)
                    })
                    .next()
                    .expect("a nearby compact context exactly reproduces the historical target");
                let predecessors = (1..=POW_ADJUSTMENT_BLOCK_SPAN)
                    .map(|offset| {
                        let offset_u32 =
                            u32::try_from(offset).expect("the DAA context length fits in u32");
                        let fact_height = block::Height(
                            height
                                .0
                                .checked_sub(offset_u32)
                                .expect("production activations have a complete DAA window"),
                        );
                        let frontier = if offset == 1 {
                            parent_frontier
                        } else {
                            let offset_u8 = u8::try_from(offset)
                                .expect("the DAA context length fits in one byte");
                            Frontier::new(fact_height, block::Hash([offset_u8; 32]))
                        };
                        let offset_i32 =
                            i32::try_from(offset).expect("the DAA context length fits in i32");
                        HeaderContextFact {
                            frontier,
                            difficulty_threshold: context_threshold,
                            time: candidate.header.time - spacing * offset_i32,
                        }
                    })
                    .collect();
                let lease = ValidationLease::new(parent_frontier, predecessors, [0x51; 32]);
                let rules = HeaderRules::for_validation_lease(network.clone(), &lease)
                    .expect("production network parameters require proof of work");
                let batch = zakura_header_chain::prepare_headers(
                    HeaderBatchInput::new(std::slice::from_ref(&candidate.header)),
                    &lease,
                    &rules,
                    &SystemClock,
                )
                .unwrap_or_else(|error| {
                    panic!(
                        "{network:?} {upgrade:?} activation header must pass the shared observable rules: {error}"
                    )
                });
                assert_eq!(batch.headers()[0].height, height);
                assert_eq!(batch.headers()[0].hash, candidate.hash());
                assert_eq!(
                    batch.headers()[0].block_work,
                    candidate
                        .header
                        .difficulty_threshold
                        .to_work()
                        .expect("the historical target has exact work")
                );
            }
        }
    }

    #[test]
    fn df_01_generated_nu5_graph_matches_full_state_before_finalization() {
        let _init_guard = zakura_test::init();
        fn network(checkpoint_blocks: Option<&[Arc<Block>]>) -> Network {
            let builder = ParametersBuilder::default()
                .with_activation_heights(ConfiguredActivationHeights {
                    before_overwinter: Some(1),
                    overwinter: Some(10),
                    sapling: Some(15),
                    blossom: Some(20),
                    heartwood: Some(25),
                    canopy: Some(30),
                    nu5: Some(35),
                    nu6: Some(100),
                    nu6_1: Some(110),
                    nu6_2: Some(120),
                    nu6_3: Some(130),
                    nu7: Some(140),
                })
                .expect("the compressed activation schedule is ordered")
                .with_disable_pow(true)
                .extend_funding_streams();
            let builder = if let Some(blocks) = checkpoint_blocks {
                let genesis_hash = blocks
                    .first()
                    .expect("the generated checkpoint chain contains genesis")
                    .hash();
                builder
                    .with_genesis_hash(genesis_hash)
                    .expect("the generated genesis hash is canonical")
                    .with_checkpoints(ConfiguredCheckpoints::HeightsAndHashes(
                        blocks
                            .iter()
                            .take(31)
                            .map(|block| {
                                (
                                    block
                                        .coinbase_height()
                                        .expect("every generated checkpoint has a height"),
                                    block.hash(),
                                )
                            })
                            .collect(),
                    ))
                    .expect("the generated checkpoints are ordered")
            } else {
                builder
            };
            builder
                .to_network()
                .expect("the compressed custom network is valid")
        }

        fn chain(network: &Network) -> Vec<Arc<Block>> {
            let sapling_root = zakura_chain::sapling::tree::NoteCommitmentTree::default().root();
            let orchard_root = zakura_chain::orchard::tree::NoteCommitmentTree::default().root();
            let ironwood_root = zakura_chain::ironwood::tree::NoteCommitmentTree::default().root();
            let mut history_tree = HistoryTree::default();
            let mut previous_hash = GENESIS_PREVIOUS_BLOCK_HASH;
            let mut blocks = Vec::new();

            for height in (0..=40).map(block::Height) {
                let upgrade = NetworkUpgrade::current(network, height);
                let input = transparent::Input::Coinbase {
                    height,
                    data: if height == block::Height(0) {
                        transparent::GENESIS_COINBASE_SCRIPT_SIG.to_vec()
                    } else {
                        format!("DF-01 {height:?}").into_bytes()
                    },
                    sequence: 0,
                };
                let transaction = match upgrade {
                    NetworkUpgrade::Genesis | NetworkUpgrade::BeforeOverwinter => Transaction::V1 {
                        inputs: vec![input],
                        outputs: Vec::new(),
                        lock_time: zakura_chain::transaction::LockTime::unlocked(),
                    },
                    NetworkUpgrade::Overwinter => Transaction::V3 {
                        inputs: vec![input],
                        outputs: Vec::new(),
                        lock_time: zakura_chain::transaction::LockTime::unlocked(),
                        expiry_height: height,
                        joinsplit_data: None,
                    },
                    NetworkUpgrade::Sapling
                    | NetworkUpgrade::Blossom
                    | NetworkUpgrade::Heartwood
                    | NetworkUpgrade::Canopy => Transaction::V4 {
                        inputs: vec![input],
                        outputs: Vec::new(),
                        lock_time: zakura_chain::transaction::LockTime::unlocked(),
                        expiry_height: height,
                        joinsplit_data: None,
                        sapling_shielded_data: None,
                    },
                    NetworkUpgrade::Nu5 => Transaction::V5 {
                        network_upgrade: upgrade,
                        lock_time: zakura_chain::transaction::LockTime::unlocked(),
                        expiry_height: height,
                        inputs: vec![input],
                        outputs: Vec::new(),
                        sapling_shielded_data: None,
                        orchard_shielded_data: None,
                    },
                    _ => unreachable!("the deterministic graph stops during NU5"),
                };
                let transactions = vec![Arc::new(transaction)];
                let merkle_root = transactions.iter().cloned().collect();
                let time = chrono::DateTime::from_timestamp(
                    1_700_000_000_i64 + i64::from(height.0) * 150,
                    0,
                )
                .expect("the deterministic timestamp is in range");
                let header = zakura_chain::block::Header {
                    version: 4,
                    previous_block_hash: previous_hash,
                    merkle_root,
                    commitment_bytes: HexDebug([0; 32]),
                    time,
                    difficulty_threshold: network.target_difficulty_limit().to_compact(),
                    nonce: HexDebug([0; 32]),
                    solution: equihash::Solution::for_proposal(),
                };
                let mut block = Arc::new(Block {
                    header: Arc::new(header),
                    transactions,
                });
                let commitment = match upgrade {
                    NetworkUpgrade::Sapling | NetworkUpgrade::Blossom => {
                        <[u8; 32]>::from(sapling_root)
                    }
                    NetworkUpgrade::Heartwood
                        if NetworkUpgrade::Heartwood.activation_height(network) == Some(height) =>
                    {
                        [0; 32]
                    }
                    NetworkUpgrade::Heartwood | NetworkUpgrade::Canopy => history_tree
                        .hash()
                        .expect("the history tree exists after Heartwood activation")
                        .into(),
                    NetworkUpgrade::Nu5 => {
                        let history_root =
                            history_tree.hash().expect("the history tree exists at NU5");
                        ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
                            &history_root,
                            &block.auth_data_root(),
                        )
                        .into()
                    }
                    _ => [0; 32],
                };
                Arc::make_mut(&mut Arc::make_mut(&mut block).header).commitment_bytes =
                    commitment.into();
                previous_hash = block.hash();
                history_tree
                    .push(
                        network,
                        block.clone(),
                        &sapling_root,
                        &orchard_root,
                        &ironwood_root,
                    )
                    .expect("the deterministic history tree advances");
                blocks.push(block);
            }
            blocks
        }

        let preliminary = network(None);
        let preliminary_chain = chain(&preliminary);
        let network = network(Some(&preliminary_chain));
        let chain = chain(&network);
        assert_eq!(network.genesis_hash(), chain[0].hash());
        assert_eq!(
            chain.iter().map(|block| block.hash()).collect::<Vec<_>>(),
            preliminary_chain
                .iter()
                .map(|block| block.hash())
                .collect::<Vec<_>>(),
            "installing generated checkpoints must not change the generated graph"
        );

        let mut finalized_state = FinalizedState::new(
            &Config::ephemeral(),
            &network,
            #[cfg(feature = "elasticsearch")]
            false,
        )
        .expect("the differential finalized state opens");
        let mut live = NonFinalizedState::new(&network);
        for block in chain.iter().take(31) {
            finalized_state
                .commit_finalized_direct(
                    CheckpointVerifiedBlock::from(block.clone()).into(),
                    None,
                    None,
                    "DF-01 generated Canopy anchor",
                )
                .expect("the generated finalized prefix commits");
        }
        let canopy_anchor = Frontier::new(block::Height(30), chain[30].hash());
        let writer = HeaderChainWriter::attach_at_semantic_handoff(&finalized_state, &live)
            .expect("the header engine attaches at the exact full-state Canopy anchor");
        assert_eq!(
            writer.runtime.publisher().snapshot().frontiers.finalized,
            canopy_anchor
        );

        for (index, block) in chain.iter().cloned().enumerate().skip(31) {
            let height = block
                .coinbase_height()
                .expect("the generated block has a coinbase height");
            let parent_hash = block.header.previous_block_hash;
            let lease = writer
                .runtime
                .reader()
                .validation_context(parent_hash)
                .expect("the exact generated parent context read succeeds")
                .expect("the exact generated parent is retained");
            let rules = HeaderRules::for_validation_lease(network.clone(), &lease)
                .expect("the custom network authenticates its PoW waiver");
            let batch = zakura_header_chain::prepare_headers(
                HeaderBatchInput::new(std::slice::from_ref(&block.header)),
                &lease,
                &rules,
                &SystemClock,
            )
            .unwrap_or_else(|error| {
                panic!("generated header {height:?} must pass the shared observable rules: {error}")
            });
            assert_eq!(batch.headers()[0].height, height);
            assert_eq!(batch.headers()[0].hash, block.hash());

            let mut staged = live.clone();
            if index == 31 {
                staged
                    .commit_new_chain(block.clone().prepare(), &finalized_state.db)
                    .expect("the first generated body enters full state");
            } else {
                staged
                    .commit_block(block.clone().prepare(), &finalized_state.db)
                    .expect("the next generated body enters full state");
            }
            let accepted = Frontier::new(height, block.hash());
            commit_verified_change(&writer, &mut live, staged, accepted);
            assert_selected_header_matches_full_state(&writer, &live);
        }

        assert_eq!(
            NetworkUpgrade::current(
                &network,
                live.best_chain()
                    .expect("the generated full-state graph has a best chain")
                    .non_finalized_tip()
                    .0,
            ),
            NetworkUpgrade::Nu5
        );

        let incumbent = writer
            .runtime
            .publisher()
            .snapshot()
            .frontiers
            .verified_best;
        let mut replacement = chain[38].make_fake_child().set_work(1_000);
        let replacement_block = Arc::make_mut(&mut replacement);
        let Transaction::V5 { expiry_height, .. } =
            Arc::make_mut(&mut replacement_block.transactions[0])
        else {
            unreachable!("the replacement is after the generated NU5 activation")
        };
        *expiry_height = block::Height(40);
        Arc::make_mut(&mut replacement_block.header).merkle_root =
            replacement_block.transactions.iter().cloned().collect();
        let parent_history_root = live
            .best_chain()
            .expect("the generated full-state graph has a best chain")
            .history_tree(crate::HashOrHeight::Height(block::Height(38)))
            .expect("the replacement parent has a retained history tree")
            .hash()
            .expect("the replacement parent history tree is nonempty");
        let commitment: [u8; 32] = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
            &parent_history_root,
            &replacement.auth_data_root(),
        )
        .into();
        Arc::make_mut(&mut Arc::make_mut(&mut replacement).header).commitment_bytes =
            commitment.into();
        let replacement_frontier = Frontier::new(
            replacement
                .coinbase_height()
                .expect("the replacement has a height"),
            replacement.hash(),
        );
        let mut staged = live.clone();
        staged
            .commit_block(replacement.prepare(), &finalized_state.db)
            .expect("the harder supported-format replacement enters full state");
        commit_verified_change(&writer, &mut live, staged, replacement_frontier);
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
            .expect("the replacement invalidates in staged full state");
        commit_operator_change(&writer, &mut live, staged, replacement_frontier.hash, true)
            .expect("invalidation commits both frontiers before swapping full state");
        let invalidated = writer.runtime.publisher().snapshot();
        assert_eq!(invalidated.frontiers.verified_best, incumbent);
        assert_eq!(invalidated.frontiers.header_best, incumbent);

        let mut staged = live.clone();
        staged
            .reconsider_block(replacement_frontier.hash, &finalized_state.db)
            .expect("the replacement replays into staged full state");
        commit_operator_change(&writer, &mut live, staged, replacement_frontier.hash, false)
            .expect("reconsider commits both frontiers before swapping full state");
        let reconsidered = writer.runtime.publisher().snapshot();
        assert_eq!(reconsidered.frontiers.verified_best, replacement_frontier);
        assert_eq!(reconsidered.frontiers.header_best, replacement_frontier);

        let first_non_finalized = chain[31].hash();
        let mut staged = live.clone();
        staged
            .invalidate_block(first_non_finalized)
            .expect("invalidating the common root empties every full-state branch");
        commit_operator_change(&writer, &mut live, staged, first_non_finalized, true)
            .expect("empty full state commits its exact finalized fallback");
        assert!(live.is_chain_set_empty());
        let snapshot = writer.runtime.publisher().snapshot();
        assert_eq!(
            snapshot.frontiers.verified_best,
            snapshot.frontiers.finalized
        );
        assert_eq!(snapshot.frontiers.header_best, snapshot.frontiers.finalized);
    }

    #[test]
    fn production_body_unavailability_writer_authenticates_exact_evidence() {
        let _init_guard = zakura_test::init();
        let network = Network::new_regtest(Default::default());
        let finalized_state = FinalizedState::new(
            &Config::ephemeral(),
            &network,
            #[cfg(feature = "elasticsearch")]
            false,
        )
        .expect("the fixture finalized state opens");
        let anchor = regtest_genesis_block();
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
    fn in_02_header_valid_body_invalid_reselects_after_exact_authenticated_evidence() {
        let _init_guard = zakura_test::init();
        let network = Network::new_regtest(Default::default());
        let finalized_state = FinalizedState::new(
            &Config::ephemeral(),
            &network,
            #[cfg(feature = "elasticsearch")]
            false,
        )
        .expect("the fixture finalized state opens");
        let anchor = regtest_genesis_block();
        let anchor_height = anchor
            .coinbase_height()
            .expect("the regtest genesis block has a height");
        let writer = header_writer(&finalized_state, &network, anchor_height, &anchor);
        let initial = writer.runtime.publisher().snapshot();
        let anchor_frontier = initial.frontiers.finalized;
        let lease = writer
            .runtime
            .reader()
            .validation_context(anchor.hash())
            .expect("the anchor context read succeeds")
            .expect("the anchor context exists");
        let rules = HeaderRules::for_validation_lease(network.clone(), &lease)
            .expect("the regtest validation policy is coherent");
        let mut child_header = *anchor.header;
        child_header.previous_block_hash = anchor.hash();
        child_header.time += chrono::Duration::seconds(1);
        let child_header = Arc::new(child_header);
        let batch = zakura_header_chain::prepare_headers(
            HeaderBatchInput::new(std::slice::from_ref(&child_header)),
            &lease,
            &rules,
            &SystemClock,
        )
        .expect("the exact child passes production header validation");
        let child = Frontier::new(
            anchor_height
                .next()
                .expect("the genesis fixture has a next height"),
            child_header.hash(),
        );
        let owner = WorkOwner {
            state_version: initial.state_version,
            header_generation: initial.header_generation,
            verified_generation: None,
            branch: BranchId::new(anchor.hash(), child.hash),
            session_id: 1,
            request_id: std::num::NonZeroU64::new(2).expect("two is nonzero"),
        };
        writer
            .runtime
            .apply(
                TransitionRequest {
                    expected_version: initial.state_version,
                    event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                        owner,
                        source: SourceId::from_digest([3; 32]),
                        parent_hash: anchor.hash(),
                        target_tip_hash: child.hash,
                        completion: TargetCompletion::TargetComplete {
                            common_ancestor: anchor_frontier,
                        },
                        batch,
                        aux: Vec::new(),
                    })),
                },
                &TransitionContext {
                    config: &writer.config,
                    clock: &SystemClock,
                    full_state_authority: None,
                    retention_references: &[],
                },
            )
            .expect("the header-only child commits");
        let selected = writer.runtime.publisher().snapshot();
        assert_eq!(selected.frontiers.header_best, child);

        let evidence = EvidenceId::from_digest([4; 32]);
        let rule = BodyRuleId::new("test.commitment_matching_invalid");
        let result = writer
            .record_body_invalid(
                selected.state_version,
                ConsensusBodyInvalid {
                    hash: child.hash,
                    evidence,
                    rule: rule.clone(),
                    source: SourceId::from_digest([5; 32]),
                },
            )
            .expect("the exact verifier evidence reaches the production writer");
        assert!(matches!(result, ApplyResult::Committed(_)));
        let rejected = writer.runtime.publisher().snapshot();
        assert_eq!(rejected.frontiers.header_best, anchor_frontier);
        assert_eq!(
            rejected.state_version,
            selected
                .state_version
                .checked_next()
                .expect("the bounded fixture version advances")
        );
        assert_eq!(
            rejected.header_generation,
            selected
                .header_generation
                .checked_next()
                .expect("the bounded fixture generation advances")
        );
    }

    #[test]
    fn stale_vct_aux_rejection_has_zero_durable_effects() {
        let _init_guard = zakura_test::init();
        let network = Network::new_regtest(Default::default());
        let finalized_state = FinalizedState::new(
            &Config::ephemeral(),
            &network,
            #[cfg(feature = "elasticsearch")]
            false,
        )
        .expect("the fixture finalized state opens");
        let anchor = regtest_genesis_block();
        let anchor_height = anchor
            .coinbase_height()
            .expect("the anchor has a coinbase height");
        let writer = header_writer(&finalized_state, &network, anchor_height, &anchor);
        let before = writer.runtime.publisher().snapshot();
        let mut stale = before.clone();
        stale.state_version = StateVersion::new(0);
        let current = zakura_header_chain::AuxDelivery {
            delivery_id: EvidenceId::from_digest([0x73; 32]),
            header_hash: anchor.hash(),
            source: zakura_header_chain::SourceId::from_digest([0x74; 32]),
            owner: WorkScope::for_body_work(&stale)
                .bind(1, std::num::NonZeroU64::new(1).expect("one is nonzero")),
            body_size: zakura_header_chain::BodySizeHint::Unknown,
            tree_aux: None,
            authentication: AuxAuthentication::Unauthenticated,
        };
        let result = writer
            .reject_vct_aux(
                &VctAuxWindow {
                    snapshot: stale,
                    current,
                    successor: None,
                },
                VctAuxRejection::Current,
                crate::error::VctCommitFailure::CurrentRoots,
            )
            .expect("stale auxiliary evidence returns a typed receipt");

        assert!(matches!(result, Some(ApplyResult::Stale(_))));
        assert_eq!(
            writer.runtime.publisher().snapshot(),
            before,
            "stale auxiliary evidence publishes and mutates nothing"
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
        finalized_state
            .commit_finalized_direct(
                CheckpointVerifiedBlock::from(block1.clone()).into(),
                None,
                None,
                "shared finalization fixture block one",
            )
            .expect("block one commits");
        let mut live = NonFinalizedState::new(&network);
        let writer = HeaderChainWriter::attach_at_semantic_handoff(&finalized_state, &live)
            .expect("the header engine attaches from authenticated finalized state");
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
