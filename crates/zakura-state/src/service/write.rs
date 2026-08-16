//! Writing blocks to the finalized and non-finalized states.

use std::{
    collections::VecDeque,
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
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
    ApplyResult, AuxAuthentication, AuxEvidence, BodyWorkAuthority, CheckpointSet, Clock,
    EngineConfig, EngineConfigError, EngineMode, EngineSnapshot, EvidenceId, Frontier,
    FullStateEvidenceAuthority, FullStateFinalized, OperatorInvalidate, OperatorInvalidationId,
    OperatorReconsider, StateVersion, StoreError, SystemClock, TransitionContext, TransitionEvent,
    TransitionRequest, TrustedAnchor, VerifiedBlockAccepted, VerifiedChainChanged,
    VerifiedChangeCause, VerifiedHeaderRef,
};

use crate::{
    constants::MAX_BLOCK_REORG_HEIGHT,
    request::FinalizableBlock,
    service::{
        check,
        finalized_state::{
            header_chain::{
                migration::{initialize_header_chain_reconciled, HeaderChainInitializationError},
                select_vct_auxiliary_delivery, HeaderChainReader, HeaderChainRuntime,
                HeaderChainStore, HeaderChainStoreError, SelectedAuxiliaryWindow,
            },
            DiskWriteBatch, FinalizedState, VctAuthenticationProof, VctAuxiliaryFailureAttribution,
            VctAuxiliaryWindow, VctSuccessorWitness, ZakuraDb,
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

mod vct_authentication_sweep;
mod vct_write_retry;

use vct_authentication_sweep::VctAuthenticationSweeper;
use vct_write_retry::{VctWriteRetryCause, VctWriteRetryManager};
pub use zakura_header_chain::{VctRootRepairState, VctRootRepairStatus};

/// A full-state mutation staged until its matching header transition commits durably.
#[allow(dead_code)] // Constructed when the dark header engine is attached to the writer task.
pub struct PreparedFullStateTransition {
    /// Stable identity authenticated by the state writer.
    transition_id: EvidenceId,
    /// Verified frontier that the state writer used to prepare the mutation.
    old_frontier: Frontier,
    /// Exact new verified suffix, empty when the selected verified chain does not change.
    new_verified_path: Vec<VerifiedHeaderRef>,
    /// Complete in-memory state installed only after the durable commit.
    non_finalized_after: NonFinalizedState,
    /// Every non-finalized header that must remain represented in the projected durable DAG.
    staged_headers: Vec<VerifiedHeaderRef>,
    /// One retention reference for each staged non-finalized branch tip.
    staged_tips: Vec<block::Hash>,
    /// Optional finalized-state writes combined with the header write batch.
    finalized_batch: Option<DiskWriteBatch>,
    /// Matching version-qualified header-engine evidence.
    header_request: TransitionRequest,
}

struct PreparedAuthority(zakura_header_chain::TransitionFingerprint);

impl PreparedAuthority {
    fn for_event(event: &TransitionEvent) -> Result<Self, HeaderChainStoreError> {
        event
            .fingerprint()
            .map(Self)
            .ok_or(HeaderChainStoreError::Incoherent(
                "prepared full-state event has no stable identity",
            ))
    }
}

impl FullStateEvidenceAuthority for PreparedAuthority {
    fn authorizes_full_state(&self, event: &TransitionEvent) -> bool {
        event.fingerprint() == Some(self.0)
    }
}

struct PreparedSchedulerAuthority(zakura_header_chain::OperatorBodyRetry);

impl FullStateEvidenceAuthority for PreparedSchedulerAuthority {
    fn authorizes_full_state(&self, _event: &TransitionEvent) -> bool {
        false
    }

    fn authorizes_scheduler_retry(&self, retry: &zakura_header_chain::OperatorBodyRetry) -> bool {
        retry == &self.0
    }
}

struct PreparedHeaderCompletionAuthority(Box<zakura_header_chain::InsertHeaders>);

impl FullStateEvidenceAuthority for PreparedHeaderCompletionAuthority {
    fn authorizes_full_state(&self, _event: &TransitionEvent) -> bool {
        false
    }

    fn authorizes_header_completion(&self, insert: &zakura_header_chain::InsertHeaders) -> bool {
        insert == self.0.as_ref()
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
        let mut staged_headers = non_finalized_after
            .chain_iter()
            .flat_map(|chain| chain.blocks.values())
            .map(|block| VerifiedHeaderRef {
                height: block.height,
                hash: block.hash,
                header: block.block.header.clone(),
            })
            .collect::<Vec<_>>();
        staged_headers.sort_unstable_by_key(|header| (header.height, header.hash.0));
        staged_headers.dedup_by_key(|header| header.hash);
        let mut staged_tips = non_finalized_after
            .chain_iter()
            .filter_map(|chain| chain.blocks.last_key_value().map(|(_, block)| block.hash))
            .collect::<Vec<_>>();
        staged_tips.sort_unstable_by_key(|hash| hash.0);
        staged_tips.dedup();
        Ok(Self {
            transition_id,
            old_frontier,
            new_verified_path,
            non_finalized_after,
            staged_headers,
            staged_tips,
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
            transition_id: _,
            non_finalized_after,
            staged_headers,
            staged_tips,
            finalized_batch,
            header_request,
            ..
        } = self;
        let authority = PreparedAuthority::for_event(&header_request.event)?;
        let mut retention_references = context.retention_references.to_vec();
        retention_references.extend(staged_tips);
        retention_references.sort_unstable_by_key(|hash| hash.0);
        retention_references.dedup();
        let guarded_context = TransitionContext {
            config: context.config,
            clock: context.clock,
            full_state_authority: Some(&authority),
            retention_references: &retention_references,
        };
        match runtime.apply_combined_expected(
            header_request,
            &guarded_context,
            finalized_batch.unwrap_or_else(DiskWriteBatch::new),
            expected_verified,
            &staged_headers,
            || *live_non_finalized = non_finalized_after,
        )? {
            ApplyResult::Stale(receipt) => Err(HeaderChainStoreError::StaleFullStateTransition {
                current_version: receipt.current_version,
            }),
            ApplyResult::ResourceStalled(receipt) => {
                Err(HeaderChainStoreError::FullStateResourceStalled { receipt })
            }
            result => Ok(result),
        }
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
#[derive(Debug)]
pub(in crate::service) struct HeaderChainWriter {
    runtime: HeaderChainRuntime,
    config: EngineConfig,
    clock: SystemClock,
}

#[derive(Debug)]
pub(crate) enum VctAuxiliaryWindowRead {
    Ready(Box<VctAuxiliaryWindow>),
    Missing { height: block::Height },
}

#[derive(Debug, Error)]
pub(crate) enum HeaderChainAttachmentError {
    #[error("finalized state has no authenticated genesis header at semantic handoff")]
    MissingGenesis,
    #[error("finalized genesis hash does not match the configured network")]
    GenesisMismatch,
    #[error("persisted header finality is not an ancestor of finalized full state")]
    FinalizedDivergence,
    #[error(transparent)]
    Config(#[from] EngineConfigError),
    #[error(transparent)]
    Store(#[from] HeaderChainStoreError),
    #[error(transparent)]
    Read(#[from] StoreError),
    #[error(transparent)]
    Initialization(#[from] HeaderChainInitializationError),
    #[error(transparent)]
    Lifecycle(#[from] zakura_node_services::sync_lifecycle::LifecycleTransitionError),
}

#[derive(Clone, Debug, Error)]
#[error("header-chain attachment failed: {message}")]
pub(crate) struct BlockWriteTaskFailure {
    message: Arc<str>,
}

#[derive(Debug)]
pub(crate) enum BlockWriteTaskExit {
    Completed,
    HeaderChainAttachmentFailed(HeaderChainAttachmentError),
    HeaderChainRuntimeFailed(BlockWriteTaskFailure),
}

impl From<&HeaderChainAttachmentError> for BlockWriteTaskFailure {
    fn from(error: &HeaderChainAttachmentError) -> Self {
        Self {
            message: error.to_string().into(),
        }
    }
}

impl BlockWriteTaskFailure {
    fn runtime(context: &'static str, error: impl std::fmt::Display) -> Self {
        Self {
            message: format!("{context}: {error}").into(),
        }
    }

    fn panic() -> Self {
        Self {
            message: "block write task panicked".into(),
        }
    }
}

impl BlockWriteTaskExit {
    fn failure(&self) -> Option<BlockWriteTaskFailure> {
        match self {
            Self::Completed => None,
            Self::HeaderChainAttachmentFailed(error) => Some(error.into()),
            Self::HeaderChainRuntimeFailed(error) => Some(error.clone()),
        }
    }
}

fn header_chain_finalization_failure(error: CommitCheckpointVerifiedError) -> BlockWriteTaskExit {
    if matches!(error.inner(), CommitBlockError::HeaderChainError { .. }) {
        return BlockWriteTaskExit::HeaderChainRuntimeFailed(BlockWriteTaskFailure::runtime(
            "header-chain reorg-limit finalization failed",
            error,
        ));
    }

    panic!(
        "unexpected finalized block commit error after note commitment and history trees were \
         checked by the non-finalized state: {error:?}"
    );
}

impl HeaderChainWriter {
    pub(in crate::service) fn new(runtime: HeaderChainRuntime, config: EngineConfig) -> Self {
        Self {
            runtime,
            config,
            clock: SystemClock,
        }
    }

    /// Reads the selected VCT auxiliary delivery and its successor authentication boundary.
    pub(crate) fn vct_auxiliary_window(
        &self,
        height: block::Height,
        hash: block::Hash,
    ) -> Result<VctAuxiliaryWindowRead, HeaderChainStoreError> {
        let selected_window = self.runtime.selected_auxiliary_window(height, hash)?;
        Self::prepare_vct_auxiliary_window(height, selected_window)
    }

    /// Reads a VCT auxiliary window at a captured selected-projection index.
    pub(crate) fn vct_auxiliary_window_at_projection_index(
        &self,
        projection_index: usize,
        expected_frontier: Frontier,
    ) -> Result<VctAuxiliaryWindowRead, HeaderChainStoreError> {
        let selected_window = self
            .runtime
            .selected_auxiliary_window_at_projection_index(projection_index, expected_frontier)?;
        Self::prepare_vct_auxiliary_window(expected_frontier.height, selected_window)
    }

    fn prepare_vct_auxiliary_window(
        height: block::Height,
        selected_window: Option<SelectedAuxiliaryWindow>,
    ) -> Result<VctAuxiliaryWindowRead, HeaderChainStoreError> {
        let Some(selected_window) = selected_window else {
            return Ok(VctAuxiliaryWindowRead::Missing { height });
        };
        let Some(delivery) =
            select_vct_auxiliary_delivery(selected_window.delivery_header.auxiliary_deliveries)
        else {
            return Ok(VctAuxiliaryWindowRead::Missing { height });
        };
        let Some(delivery_auxiliary_data) = delivery.tree_aux else {
            return Ok(VctAuxiliaryWindowRead::Missing { height });
        };
        if delivery.header_hash != selected_window.delivery_header.header_node.hash
            || delivery_auxiliary_data.height != selected_window.delivery_header.header_node.height
        {
            return Err(zakura_header_chain::StoreError::Incoherent(
                "selected VCT delivery disagrees with its retained header",
            )
            .into());
        }
        let successor_height = selected_window
            .successor_header
            .as_ref()
            .map(|successor_header| successor_header.header_node.height);
        let successor = match selected_window.successor_header {
            Some(successor_header) => {
                select_vct_auxiliary_delivery(successor_header.auxiliary_deliveries)
                    .map(|successor_delivery| {
                        VctSuccessorWitness::from_delivery(
                            successor_header.header_node.header,
                            successor_header.header_node.height,
                            successor_delivery,
                        )
                        .ok_or(zakura_header_chain::StoreError::Incoherent(
                            "selected VCT successor delivery disagrees with its retained header",
                        ))
                    })
                    .transpose()?
            }
            None => None,
        };
        Ok(VctAuxiliaryWindowRead::Ready(Box::new(
            VctAuxiliaryWindow {
                engine_snapshot: selected_window.engine_snapshot,
                delivery_header: selected_window.delivery_header.header_node.header,
                delivery,
                successor_height,
                successor,
            },
        )))
    }

    #[cfg(test)]
    fn attach_at_semantic_handoff(
        finalized_state: &FinalizedState,
        non_finalized_state: &NonFinalizedState,
    ) -> Result<Self, HeaderChainAttachmentError> {
        Self::attach_at_semantic_handoff_with_progress(finalized_state, non_finalized_state, |_| {})
    }

    fn attach_at_semantic_handoff_with_progress<P>(
        finalized_state: &FinalizedState,
        non_finalized_state: &NonFinalizedState,
        report_progress: P,
    ) -> Result<Self, HeaderChainAttachmentError>
    where
        P: FnMut(zakura_node_services::sync_lifecycle::HeaderReconstructionProgress),
    {
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
        let restored_side_paths = verified_side_paths(non_finalized_state, &restored_path);
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
            store
                .startup_reconciled_streaming(
                    &config,
                    full_state_finalized,
                    restored_path,
                    |height| {
                        let (hash, header) = finalized_state
                            .db
                            .header_by_height(height)
                            .ok_or(HeaderChainStoreError::MissingCanonicalHeader(height))?;
                        Ok(VerifiedHeaderRef {
                            height,
                            hash,
                            header,
                        })
                    },
                    report_progress,
                )?
                .0
        } else {
            initialize_header_chain_reconciled(&finalized_state.db, &config, restored_path)?.0
        };
        restore_verified_side_paths(&runtime, &config, restored_side_paths)?;
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

    fn commit_checkpoint_finalized(
        &self,
        block: &CheckpointVerifiedBlock,
        full_state_batch: DiskWriteBatch,
        authentication: Option<TransitionRequest>,
    ) -> Result<(), HeaderChainStoreError> {
        let accepted = Frontier::new(block.height, block.hash);
        let snapshot = self.runtime.publisher().snapshot();
        if accepted.height <= snapshot.frontiers.finalized.height {
            return (accepted == snapshot.frontiers.finalized)
                .then_some(())
                .ok_or(HeaderChainStoreError::Incoherent(
                    "checkpoint full state conflicts with durable header finality",
                ));
        }
        if accepted.height
            != snapshot
                .frontiers
                .verified_best
                .height
                .next()
                .map_err(|_| {
                    HeaderChainStoreError::Incoherent(
                        "checkpoint full-state height does not extend the verified header frontier",
                    )
                })?
            || block.block.header.previous_block_hash != snapshot.frontiers.verified_best.hash
        {
            return Err(HeaderChainStoreError::Incoherent(
                "checkpoint full state does not extend the verified header frontier",
            ));
        }

        let path = vec![VerifiedHeaderRef {
            height: block.height,
            hash: block.hash,
            header: block.block.header.clone(),
        }];
        let evidence = full_state_evidence(
            b"checkpoint-grow",
            snapshot.state_version,
            block.hash,
            &path,
        );
        let checkpoint_event = TransitionEvent::VerifiedChainChanged(VerifiedChainChanged {
            full_state_transition_id: evidence,
            old_tip: snapshot.frontiers.verified_best,
            new_path: path,
            cause: VerifiedChangeCause::CheckpointFinalizedGrow,
        });
        let checkpoint_authority = PreparedAuthority::for_event(&checkpoint_event)?;
        let checkpoint_context = TransitionContext {
            config: &self.config,
            clock: &self.clock,
            full_state_authority: Some(&checkpoint_authority),
            retention_references: &[],
        };
        let checkpoint_request = TransitionRequest {
            expected_version: snapshot.state_version,
            event: checkpoint_event,
        };
        let result = if let Some(authentication) = authentication {
            let authentication_authority = PreparedAuthority::for_event(&authentication.event)?;
            let authentication_context = TransitionContext {
                config: &self.config,
                clock: &self.clock,
                full_state_authority: Some(&authentication_authority),
                retention_references: &[],
            };
            self.runtime.apply_aux_then_checkpoint_combined(
                authentication,
                &authentication_context,
                checkpoint_request,
                &checkpoint_context,
                full_state_batch,
                || {},
            )?
        } else {
            self.runtime.apply_combined(
                checkpoint_request,
                &checkpoint_context,
                full_state_batch,
                || {},
            )?
        };
        match result {
            ApplyResult::Stale(receipt) => {
                return Err(HeaderChainStoreError::StaleFullStateTransition {
                    current_version: receipt.current_version,
                });
            }
            ApplyResult::ResourceStalled(receipt) => {
                return Err(HeaderChainStoreError::FullStateResourceStalled { receipt });
            }
            ApplyResult::Committed | ApplyResult::NoChange(_) => {}
        }
        Ok(())
    }

    fn apply_deferred_reevaluation(&self) -> Result<(), HeaderChainStoreError> {
        let _ = self.runtime.apply(
            TransitionRequest {
                expected_version: self.runtime.publisher().snapshot().state_version,
                event: TransitionEvent::ReevaluateDeferred,
            },
            &self.context(),
        )?;
        Ok(())
    }

    fn apply_prepared_body_evidence(
        &self,
        prepared: crate::PreparedHeaderChainBodyEvidence,
    ) -> Result<ApplyResult, HeaderChainStoreError> {
        let (request, staged_authority) = prepared.into_parts();
        let event_evidence =
            request
                .event
                .idempotency_key()
                .ok_or(HeaderChainStoreError::Incoherent(
                    "prepared body evidence has no stable identity",
                ))?;
        if event_evidence != staged_authority {
            return Err(HeaderChainStoreError::Incoherent(
                "prepared body evidence differs from its staged authority",
            ));
        }
        let authority = PreparedAuthority::for_event(&request.event)?;
        let mut context = self.context();
        context.full_state_authority = Some(&authority);
        self.runtime.apply(request, &context)
    }

    fn retry_body_availability(
        &self,
        prepared: crate::PreparedHeaderChainBodyEvidence,
    ) -> Result<ApplyResult, HeaderChainStoreError> {
        let (request, staged_authority) = prepared.into_parts();
        let TransitionEvent::OperatorBodyRetry(retry) = request.event else {
            return Err(HeaderChainStoreError::Incoherent(
                "prepared scheduler retry contains another event domain",
            ));
        };
        if retry.evidence != staged_authority {
            return Err(HeaderChainStoreError::Incoherent(
                "prepared scheduler retry differs from its staged authority",
            ));
        }
        let authority = PreparedSchedulerAuthority(retry);
        let mut context = self.context();
        context.full_state_authority = Some(&authority);
        self.runtime.apply(
            TransitionRequest {
                expected_version: request.expected_version,
                event: TransitionEvent::OperatorBodyRetry(retry),
            },
            &context,
        )
    }

    /// Records the evidence that a VCT verification failure attributed to one or two deliveries.
    ///
    /// The writer rejects an attributable delivery. The writer disputes both deliveries when the
    /// boundary evidence cannot identify which delivery is invalid.
    fn record_vct_auxiliary_failure(
        &self,
        auxiliary_window: &VctAuxiliaryWindow,
        attribution: VctAuxiliaryFailureAttribution,
        failure: crate::error::VctCommitFailure,
    ) -> Result<Option<ApplyResult>, HeaderChainStoreError> {
        let deliveries = match attribution {
            VctAuxiliaryFailureAttribution::CurrentDelivery => vec![auxiliary_window.delivery],
            VctAuxiliaryFailureAttribution::SuccessorDelivery => auxiliary_window
                .successor
                .as_ref()
                .and_then(|successor| successor.delivery)
                .into_iter()
                .collect(),
            VctAuxiliaryFailureAttribution::AmbiguousDeliveries => {
                let Some(successor_delivery) = auxiliary_window
                    .successor
                    .as_ref()
                    .and_then(|successor| successor.delivery)
                else {
                    return Ok(None);
                };
                vec![auxiliary_window.delivery, successor_delivery]
            }
            VctAuxiliaryFailureAttribution::NoDelivery => return Ok(None),
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
        let first_delivery = deliveries
            .first()
            .expect("the empty auxiliary rejection returned above");
        let owner = BodyWorkAuthority::for_snapshot(&auxiliary_window.engine_snapshot).bind(
            first_delivery.owner.session_id(),
            first_delivery.owner.request_id(),
        );
        let authentication = if attribution.requires_dispute() {
            AuxAuthentication::Disputed { evidence }
        } else {
            AuxAuthentication::Rejected { evidence }
        };
        let request = TransitionRequest {
            expected_version: auxiliary_window.engine_snapshot.state_version,
            event: TransitionEvent::AuxEvidence(Box::new(AuxEvidence {
                owner,
                deliveries,
                authentication,
            })),
        };
        let authority = PreparedAuthority::for_event(&request.event)?;
        let mut context = self.context();
        context.full_state_authority = Some(&authority);

        self.runtime.apply(request, &context).map(Some)
    }

    /// Promote one verified delivery to authenticated outside a block commit.
    ///
    /// The committer authenticates the delivery in the block commit batch. The sweep has
    /// no block batch, so it applies the same transition separately. `Ok(None)` means the delivery
    /// already has terminal authentication evidence.
    fn authenticate_vct_aux(
        &self,
        auxiliary_window: &VctAuxiliaryWindow,
        proof: VctAuthenticationProof,
    ) -> Result<Option<ApplyResult>, HeaderChainStoreError> {
        let Some((_evidence, request)) = Self::vct_authentication_request(auxiliary_window, proof)
        else {
            return Ok(None);
        };
        let authority = PreparedAuthority::for_event(&request.event)?;
        let mut context = self.context();
        context.full_state_authority = Some(&authority);

        self.runtime.apply(request, &context).map(Some)
    }

    fn vct_authentication_request(
        auxiliary_window: &VctAuxiliaryWindow,
        proof: VctAuthenticationProof,
    ) -> Option<(EvidenceId, TransitionRequest)> {
        if !matches!(
            auxiliary_window.delivery.authentication,
            AuxAuthentication::Unauthenticated | AuxAuthentication::Disputed { .. }
        ) {
            return None;
        }
        let VctAuthenticationProof::Successor {
            delivery_id,
            delivery_header_hash,
            boundary_hash,
            boundary_auth_data_root,
        } = proof
        else {
            return None;
        };
        if delivery_id != auxiliary_window.delivery.delivery_id
            || delivery_header_hash != auxiliary_window.delivery.header_hash
        {
            return None;
        }

        let mut hasher = Sha256::new();
        hasher.update(b"zakura.vct.aux.authentication.v1");
        hasher.update(delivery_id.digest());
        hasher.update(delivery_header_hash.0);
        hasher.update(boundary_hash.0);
        hasher.update(<[u8; 32]>::from(boundary_auth_data_root));
        let evidence = EvidenceId::from_digest(hasher.finalize().into());
        let owner = BodyWorkAuthority::for_snapshot(&auxiliary_window.engine_snapshot).bind(
            auxiliary_window.delivery.owner.session_id(),
            auxiliary_window.delivery.owner.request_id(),
        );

        Some((
            evidence,
            TransitionRequest {
                expected_version: auxiliary_window.engine_snapshot.state_version,
                event: TransitionEvent::AuxEvidence(Box::new(AuxEvidence {
                    owner,
                    deliveries: vec![auxiliary_window.delivery],
                    authentication: AuxAuthentication::Authenticated {
                        evidence,
                        boundary_hash,
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

fn verified_side_paths(
    state: &NonFinalizedState,
    selected: &[VerifiedHeaderRef],
) -> Vec<Vec<VerifiedHeaderRef>> {
    let selected_tip = selected.last().map(|header| header.hash);
    let mut paths = state
        .chain_iter()
        .map(|chain| {
            chain
                .blocks
                .values()
                .map(|block| VerifiedHeaderRef {
                    height: block.height,
                    hash: block.hash,
                    header: block.block.header.clone(),
                })
                .collect::<Vec<_>>()
        })
        .filter(|path| {
            path.last()
                .is_some_and(|header| Some(header.hash) != selected_tip)
        })
        .collect::<Vec<_>>();
    paths.sort_unstable_by_key(|path| {
        path.last()
            .map(|header| (header.height, header.hash.0))
            .expect("empty full-state paths were filtered out")
    });
    paths.dedup();
    paths
}

fn restore_verified_side_paths(
    runtime: &HeaderChainRuntime,
    config: &EngineConfig,
    paths: Vec<Vec<VerifiedHeaderRef>>,
) -> Result<(), HeaderChainStoreError> {
    for path in paths {
        let snapshot = runtime.publisher().snapshot();
        let mut hasher = Sha256::new();
        hasher.update(b"zakura-full-state-startup-side-path-v1");
        hasher.update(config.trust_anchor_digest());
        hasher.update(snapshot.frontiers.finalized.height.0.to_be_bytes());
        hasher.update(snapshot.frontiers.finalized.hash.0);
        for header in &path {
            hasher.update(header.height.0.to_be_bytes());
            hasher.update(header.hash.0);
        }
        let evidence = EvidenceId::from_digest(hasher.finalize().into());
        let event = TransitionEvent::VerifiedBlockAccepted(VerifiedBlockAccepted {
            full_state_transition_id: evidence,
            path,
        });
        let authority = PreparedAuthority::for_event(&event)?;
        let result = runtime.apply(
            TransitionRequest {
                expected_version: snapshot.state_version,
                event,
            },
            &TransitionContext {
                config,
                clock: &SystemClock,
                full_state_authority: Some(&authority),
                retention_references: &[],
            },
        )?;
        match result {
            ApplyResult::Stale(receipt) => {
                return Err(HeaderChainStoreError::StaleFullStateTransition {
                    current_version: receipt.current_version,
                });
            }
            ApplyResult::ResourceStalled(receipt) => {
                return Err(HeaderChainStoreError::FullStateResourceStalled { receipt });
            }
            ApplyResult::Committed | ApplyResult::NoChange(_) => {}
        }
    }
    Ok(())
}

fn verified_path_through(
    state: &NonFinalizedState,
    accepted: Frontier,
) -> Result<Vec<VerifiedHeaderRef>, HeaderChainStoreError> {
    let path = state.chain_iter().find_map(|chain| {
        let height = chain.height_by_hash.get(&accepted.hash)?;
        (*height == accepted.height).then(|| {
            chain
                .blocks
                .range(..=accepted.height)
                .map(|(_, block)| VerifiedHeaderRef {
                    height: block.height,
                    hash: block.hash,
                    header: block.block.header.clone(),
                })
                .collect::<Vec<_>>()
        })
    });
    path.filter(|path| {
        path.last()
            .is_some_and(|header| header.hash == accepted.hash)
    })
    .ok_or(HeaderChainStoreError::Incoherent(
        "accepted full-state block is absent from its staged side path",
    ))
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
                VerifiedChangeCause::CheckpointFinalizedGrow => b"checkpoint-grow",
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
        let accepted_path = verified_path_through(after, accepted)?;
        let evidence = full_state_evidence(
            b"verified-side-path",
            snapshot.state_version,
            accepted.hash,
            &accepted_path,
        );
        TransitionEvent::VerifiedBlockAccepted(VerifiedBlockAccepted {
            full_state_transition_id: evidence,
            path: accepted_path,
        })
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
    hasher.update(b"zakura-operator-invalidation-id-v1");
    hasher.update(target.0);
    let id_digest: [u8; 32] = hasher.finalize().into();
    let mut id = [0; 16];
    id.copy_from_slice(&id_digest[..16]);
    let id = OperatorInvalidationId::new(id);
    let mut hasher = Sha256::new();
    hasher.update(b"zakura-operator-invalidation-v1");
    hasher.update(target.0);
    hasher.update(id.bytes());
    (id, hasher.finalize().into())
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
        |_db, batch, _proof| {
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
    let invalidation_evidence = (!invalidate)
        .then(|| writer.runtime.operator_invalidation_evidence(target, id))
        .transpose()?
        .flatten();
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
            invalidation_evidence,
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

/// The maximum size of the rejected ancestor map.
///
/// We allow enough space for multiple concurrent chain forks with errors.
const REJECTED_ANCESTOR_MAP_LIMIT: usize = MAX_BLOCK_REORG_HEIGHT as usize * 2;

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
    runtime_status_sender: watch::Sender<zakura_node_services::sync_lifecycle::HeaderRuntimeStatus>,
}

impl HeaderChainObservers {
    pub(in crate::service) fn new(
        snapshot_sender: watch::Sender<Option<EngineSnapshot>>,
        reader_sender: watch::Sender<Option<HeaderChainReader>>,
        runtime_status_sender: watch::Sender<
            zakura_node_services::sync_lifecycle::HeaderRuntimeStatus,
        >,
    ) -> Self {
        Self {
            snapshot_sender,
            reader_sender,
            runtime_status_sender,
        }
    }

    fn begin_reconstruction(
        &self,
    ) -> Result<zakura_node_services::sync_lifecycle::LifecycleEpoch, HeaderChainAttachmentError>
    {
        use zakura_node_services::sync_lifecycle::HeaderRuntimeTransition;

        let next = self
            .runtime_status_sender
            .borrow()
            .clone()
            .transition(HeaderRuntimeTransition::BeginReconstruction)?;
        let epoch = next.epoch();
        self.publish_runtime_status(next);
        Ok(epoch)
    }

    fn ready(
        &self,
        epoch: zakura_node_services::sync_lifecycle::LifecycleEpoch,
    ) -> Result<(), HeaderChainAttachmentError> {
        use zakura_node_services::sync_lifecycle::HeaderRuntimeTransition;

        let next = self.runtime_status_sender.borrow().clone().transition(
            HeaderRuntimeTransition::Ready {
                expected_epoch: epoch,
            },
        )?;
        self.publish_runtime_status(next);
        Ok(())
    }

    fn progress(
        &self,
        epoch: zakura_node_services::sync_lifecycle::LifecycleEpoch,
        progress: zakura_node_services::sync_lifecycle::HeaderReconstructionProgress,
    ) {
        use zakura_node_services::sync_lifecycle::HeaderRuntimeTransition;

        let current = self.runtime_status_sender.borrow().clone();
        match current.transition(HeaderRuntimeTransition::ReportProgress {
            expected_epoch: epoch,
            progress,
        }) {
            Ok(next) => self.publish_runtime_status(next),
            Err(error) => tracing::error!(
                ?error,
                ?progress,
                "could not publish header-runtime reconstruction progress"
            ),
        }
    }

    fn failed(
        &self,
        epoch: zakura_node_services::sync_lifecycle::LifecycleEpoch,
        error: &HeaderChainAttachmentError,
    ) {
        use zakura_node_services::sync_lifecycle::HeaderRuntimeTransition;

        let current = self.runtime_status_sender.borrow().clone();
        match current.transition(HeaderRuntimeTransition::Fail {
            expected_epoch: epoch,
            error: error.to_string().into(),
        }) {
            Ok(next) => self.publish_runtime_status(next),
            Err(lifecycle_error) => tracing::error!(
                ?lifecycle_error,
                attachment_error = %error,
                "could not publish failed header-runtime lifecycle"
            ),
        }
    }

    fn publish_runtime_status(
        &self,
        status: zakura_node_services::sync_lifecycle::HeaderRuntimeStatus,
    ) {
        let epoch = status.epoch();
        let phase = match &status {
            zakura_node_services::sync_lifecycle::HeaderRuntimeStatus::Detached { .. } => 0.0,
            zakura_node_services::sync_lifecycle::HeaderRuntimeStatus::Reconstructing {
                ..
            } => 1.0,
            zakura_node_services::sync_lifecycle::HeaderRuntimeStatus::Ready { .. } => 2.0,
            zakura_node_services::sync_lifecycle::HeaderRuntimeStatus::Failed { .. } => 3.0,
        };
        self.runtime_status_sender.send_replace(status.clone());
        // This diagnostic gauge may round very large epochs.
        // Lifecycle authority retains the checked `u64`.
        metrics::gauge!("state.header.runtime.epoch").set(epoch.get() as f64);
        metrics::gauge!("state.header.runtime.phase").set(phase);
        tracing::info!(?status, "header runtime lifecycle changed");
    }
}

/// The message type for the non-finalized block write task channel.
pub enum NonFinalizedWriteMessage {
    /// One complete peer target prepared outside the writer and admitted through the sole
    /// transition algorithm.
    ApplyHeaderChainInsert {
        prepared: crate::PreparedHeaderChainInsert,
        rsp_tx: oneshot::Sender<Result<ApplyResult, HeaderChainStoreError>>,
    },
    /// One retryable body-availability result that integrated full state admitted.
    RecordHeaderChainBodyUnavailable {
        prepared: crate::PreparedHeaderChainBodyEvidence,
        rsp_tx: oneshot::Sender<Result<ApplyResult, HeaderChainStoreError>>,
    },
    /// One commitment-matching deterministic body rejection that the full verifier admitted.
    RecordHeaderChainBodyInvalid {
        prepared: crate::PreparedHeaderChainBodyEvidence,
        rsp_tx: oneshot::Sender<Result<ApplyResult, HeaderChainStoreError>>,
    },
    /// A changed authenticated supplier set restarts one persistent alarm.
    RestartHeaderChainBodyAvailability {
        prepared: crate::PreparedHeaderChainBodyEvidence,
        rsp_tx: oneshot::Sender<Result<ApplyResult, HeaderChainStoreError>>,
    },
    /// An authenticated operator request restarts one persistent alarm.
    RetryHeaderChainBodyAvailability {
        prepared: crate::PreparedHeaderChainBodyEvidence,
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
        Arc<OnceLock<BlockWriteTaskFailure>>,
        Option<Arc<std::thread::JoinHandle<BlockWriteTaskExit>>>,
    ) {
        let attach_header_chain_at_handoff = finalized_state
            .db
            .config()
            .enable_zakura_header_seed_from_committed_blocks;
        Self::spawn_with_header_chain(
            finalized_state,
            non_finalized_state,
            chain_tip_sender,
            non_finalized_state_sender,
            should_use_finalized_block_write_sender,
            backup_dir_path,
            None,
            attach_header_chain_at_handoff,
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
        Arc<OnceLock<BlockWriteTaskFailure>>,
        Option<Arc<std::thread::JoinHandle<BlockWriteTaskExit>>>,
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
        let task_failure = Arc::new(OnceLock::new());
        let worker_task_failure = task_failure.clone();

        let span = Span::current();
        let task = std::thread::spawn(move || {
            span.in_scope(|| {
                let result = catch_unwind(AssertUnwindSafe(|| {
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
                }));
                match result {
                    Ok(result) => {
                        if let Some(failure) = result.failure() {
                            let _ = worker_task_failure.set(failure);
                        }
                        result
                    }
                    Err(panic) => {
                        let _ = worker_task_failure.set(BlockWriteTaskFailure::panic());
                        resume_unwind(panic)
                    }
                }
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
            task_failure,
            Some(Arc::new(task)),
        )
    }
}

trait DeferredHeaderMaintenance {
    fn earliest_deferred(
        &self,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, HeaderChainStoreError>;
    fn now(&self) -> chrono::DateTime<chrono::Utc>;
    fn reevaluate_deferred(&self) -> Result<(), HeaderChainStoreError>;
}

impl DeferredHeaderMaintenance for HeaderChainWriter {
    fn earliest_deferred(
        &self,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, HeaderChainStoreError> {
        self.runtime.earliest_deferred()
    }

    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        self.clock.now()
    }

    fn reevaluate_deferred(&self) -> Result<(), HeaderChainStoreError> {
        self.apply_deferred_reevaluation()
    }
}

fn receive_until_deferred_deadline<M: DeferredHeaderMaintenance>(
    receiver: &mut UnboundedReceiver<NonFinalizedWriteMessage>,
    maintenance: Option<&M>,
    deadline_runtime: &tokio::runtime::Runtime,
) -> Result<Option<NonFinalizedWriteMessage>, HeaderChainStoreError> {
    loop {
        match receiver.try_recv() {
            Ok(message) => return Ok(Some(message)),
            Err(TryRecvError::Disconnected) => return Ok(None),
            Err(TryRecvError::Empty) => {}
        }

        let Some(maintenance) = maintenance else {
            return Ok(receiver.blocking_recv());
        };
        let Some(deadline) = maintenance.earliest_deferred()? else {
            return Ok(receiver.blocking_recv());
        };
        let now = maintenance.now();
        if deadline <= now {
            maintenance.reevaluate_deferred()?;
            continue;
        }
        let wait = deadline
            .signed_duration_since(now)
            .to_std()
            .unwrap_or(Duration::ZERO);
        match deadline_runtime.block_on(async { tokio::time::timeout(wait, receiver.recv()).await })
        {
            Ok(message) => return Ok(message),
            Err(_) => maintenance.reevaluate_deferred()?,
        }
    }
}

fn handle_header_chain_control_message(
    header_chain: Option<&HeaderChainWriter>,
    message: NonFinalizedWriteMessage,
) -> Result<(), NonFinalizedWriteMessage> {
    match message {
        NonFinalizedWriteMessage::ApplyHeaderChainInsert { prepared, rsp_tx } => {
            let result = header_chain
                .ok_or(HeaderChainStoreError::Uninitialized)
                .and_then(|writer| {
                    let insert =
                        prepared
                            .into_insert()
                            .ok_or(HeaderChainStoreError::Transition(
                                zakura_header_chain::TransitionFailure::Authority,
                            ))?;
                    let authority = PreparedHeaderCompletionAuthority(insert.clone());
                    let mut context = writer.context();
                    context.full_state_authority = Some(&authority);
                    writer.runtime.apply(
                        TransitionRequest {
                            // Insertions carry typed asynchronous authority.
                            // The global version coordinate does not authorize insertion work.
                            expected_version: StateVersion::default(),
                            event: TransitionEvent::InsertHeaders(insert),
                        },
                        &context,
                    )
                });
            let _ = rsp_tx.send(result);
            Ok(())
        }
        NonFinalizedWriteMessage::RecordHeaderChainBodyUnavailable { prepared, rsp_tx }
        | NonFinalizedWriteMessage::RecordHeaderChainBodyInvalid { prepared, rsp_tx }
        | NonFinalizedWriteMessage::RestartHeaderChainBodyAvailability { prepared, rsp_tx } => {
            let result = header_chain
                .ok_or(HeaderChainStoreError::Uninitialized)
                .and_then(|writer| writer.apply_prepared_body_evidence(prepared));
            let _ = rsp_tx.send(result);
            Ok(())
        }
        NonFinalizedWriteMessage::RetryHeaderChainBodyAvailability { prepared, rsp_tx } => {
            let result = header_chain
                .ok_or(HeaderChainStoreError::Uninitialized)
                .and_then(|writer| writer.retry_body_availability(prepared));
            let _ = rsp_tx.send(result);
            Ok(())
        }
        message => Err(message),
    }
}

fn attach_header_chain_if_genesis_is_committed(
    header_chain: &mut Option<HeaderChainWriter>,
    attach_header_chain: bool,
    finalized_state: &FinalizedState,
    non_finalized_state: &NonFinalizedState,
    observers: &HeaderChainObservers,
) -> Result<bool, BlockWriteTaskExit> {
    if !attach_header_chain || observers.runtime_status_sender.borrow().is_ready() {
        return Ok(false);
    }
    if header_chain.is_none() && finalized_state.db.header_by_height(Height(0)).is_none() {
        return Ok(false);
    }

    let epoch = observers
        .begin_reconstruction()
        .map_err(BlockWriteTaskExit::HeaderChainAttachmentFailed)?;
    if header_chain.is_none() {
        let writer = HeaderChainWriter::attach_at_semantic_handoff_with_progress(
            finalized_state,
            non_finalized_state,
            |progress| observers.progress(epoch, progress),
        )
        .map_err(|error| {
            observers.failed(epoch, &error);
            BlockWriteTaskExit::HeaderChainAttachmentFailed(error)
        })?;
        *header_chain = Some(writer);
    }
    let writer = header_chain
        .as_ref()
        .expect("header runtime exists after successful attachment");
    observers
        .reader_sender
        .send_replace(Some(writer.runtime.reader()));
    writer
        .runtime
        .publisher()
        .mirror_to(observers.snapshot_sender.clone());
    observers
        .ready(epoch)
        .map_err(BlockWriteTaskExit::HeaderChainAttachmentFailed)?;
    Ok(true)
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
    pub fn run(mut self) -> BlockWriteTaskExit {
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
        let deadline_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("the state writer can construct its deferred-header deadline timer");

        // The retry manager parks checkpoint blocks that need VCT metadata repair.
        let mut vct_write_retry_manager = VctWriteRetryManager::new(vct_root_repair_sender.clone());
        // The authentication sweeper verifies selected VCT metadata before block commit.
        let mut vct_authentication_sweeper = VctAuthenticationSweeper::default();

        if let Err(exit) = attach_header_chain_if_genesis_is_committed(
            header_chain,
            *attach_header_chain_at_handoff,
            finalized_state,
            non_finalized_state,
            header_chain_observers,
        ) {
            return exit;
        }

        // Write all the finalized blocks sent by the state,
        // until the state closes the finalized block channel's sender.
        loop {
            match non_finalized_block_write_receiver.try_recv() {
                Ok(msg) => {
                    if let Err(msg) =
                        handle_header_chain_control_message(header_chain.as_ref(), msg)
                    {
                        deferred_non_finalized_messages.push_back(msg);
                    }
                }
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => {}
            }

            let ordered_block = match vct_write_retry_manager.take_retryable_block() {
                Some(block) => block,
                None => match finalized_block_write_receiver.try_recv() {
                    Ok(block) => block,
                    Err(TryRecvError::Empty) => {
                        // The sweep runs after both block queues become empty.
                        // The sweep yields when finalized block work arrives.
                        if let Some(writer) = header_chain.as_ref() {
                            vct_authentication_sweeper.sweep(
                                finalized_state,
                                writer,
                                &mut vct_write_retry_manager,
                                || !finalized_block_write_receiver.is_empty(),
                            );
                        }
                        std::thread::park_timeout(Duration::from_millis(10));
                        continue;
                    }
                    Err(TryRecvError::Disconnected) => break,
                },
            };

            // TODO: split these checks into separate functions

            if invalid_block_reset_sender.is_closed() {
                info!("StateService closed the block reset channel. Is Zakura shutting down?");
                return BlockWriteTaskExit::Completed;
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

                // The failed pipeline invalidates cached successor prevalidation.
                // Clear the cache so commit resumes from the real finalized tip.
                vct_write_retry_manager.reset(finalized_state);

                // We don't want to send a reset here, because it could overwrite a valid sent hash
                std::mem::drop(ordered_block);
                continue;
            }

            // Fast VCT commits use the already-validated Zakura header store as their
            // successor witness. A checkpoint-verified body is not sufficient: NU5+
            // block hashes do not bind authorizing data, so an altered same-hash body
            // could supply the wrong auth-data root and make a valid current root look
            // invalid.
            let requires_exact_vct_roots = header_chain.is_some()
                && finalized_state.vct_requires_exact_roots(ordered_block.0.height);
            let vct_auxiliary_window = if requires_exact_vct_roots {
                match header_chain
                    .as_ref()
                    .expect("exact VCT roots are required only with an attached header chain")
                    .vct_auxiliary_window(ordered_block.0.height, ordered_block.0.hash)
                {
                    Ok(VctAuxiliaryWindowRead::Ready(auxiliary_window)) => Some(*auxiliary_window),
                    Ok(VctAuxiliaryWindowRead::Missing { height }) => {
                        let wait = vct_write_retry_manager.on_retryable_error(
                            height,
                            VctWriteRetryCause::MissingRoot {
                                replacement_required: false,
                            },
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
                        return BlockWriteTaskExit::HeaderChainRuntimeFailed(
                            BlockWriteTaskFailure::runtime(
                                "incoherent header auxiliary read stopped the finalized writer",
                                error,
                            ),
                        );
                    }
                }
            } else {
                None
            };
            let has_exact_vct_roots =
                vct_auxiliary_window
                    .as_ref()
                    .is_some_and(|auxiliary_window| {
                        auxiliary_window
                            .delivery_roots(ordered_block.0.height, ordered_block.0.hash)
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
                return BlockWriteTaskExit::HeaderChainRuntimeFailed(
                    BlockWriteTaskFailure::runtime(
                        "incoherent ready VCT auxiliary window stopped the finalized writer",
                        format_args!(
                            "missing exact roots for {:?} at {:?}",
                            ordered_block.0.hash, ordered_block.0.height
                        ),
                    ),
                );
            }

            if needs_vct_successor
                && vct_auxiliary_window
                    .as_ref()
                    .and_then(|auxiliary_window| auxiliary_window.successor.as_ref())
                    .is_none()
            {
                let height = vct_auxiliary_window
                    .as_ref()
                    .and_then(|auxiliary_window| auxiliary_window.successor_height)
                    .or_else(|| ordered_block.0.height.next().ok())
                    .unwrap_or(ordered_block.0.height);
                let wait = vct_write_retry_manager.on_retryable_error(
                    height,
                    VctWriteRetryCause::MissingSuccessor,
                    ordered_block,
                );
                std::thread::park_timeout(wait);
                continue;
            }

            // The successor header authenticates the current block's supplied roots.
            // Header-sync stores its ZIP-244 auth-data root alongside the contextually
            // validated header, so this check does not require the successor body.
            let prev_note_commitment_trees = prev_finalized_note_commitment_trees.take();
            let prev_note_commitment_trees_for_retry = prev_note_commitment_trees.clone();
            let vct_auxiliary_window_for_outcome = vct_auxiliary_window.clone();
            let vct_authentication_window = vct_auxiliary_window.clone();
            let checkpoint_header_writer = header_chain.as_ref();
            let checkpoint_block = ordered_block.0.clone();

            // Try committing the block
            match finalized_state.commit_finalized_with_aux_and(
                ordered_block,
                prev_note_commitment_trees,
                vct_auxiliary_window,
                |db, batch, proof| {
                    let authentication = checkpoint_header_writer.and_then(|_writer| {
                        vct_authentication_window
                            .as_ref()
                            .and_then(|auxiliary_window| {
                                HeaderChainWriter::vct_authentication_request(
                                    auxiliary_window,
                                    proof,
                                )
                            })
                            .map(|(_evidence, request)| request)
                    });
                    if let Some(writer) = checkpoint_header_writer {
                        writer
                            .commit_checkpoint_finalized(&checkpoint_block, batch, authentication)
                            .map_err(|error| CommitBlockError::HeaderChainError {
                                error: error.to_string(),
                            })?;
                    } else {
                        db.header_chain_disk_db()
                            .write(batch)
                            .expect("unexpected rocksdb error while writing block");
                    }
                    Ok(())
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
                    vct_write_retry_manager.on_commit_success();

                    if let Err(exit) = attach_header_chain_if_genesis_is_committed(
                        header_chain,
                        *attach_header_chain_at_handoff,
                        finalized_state,
                        non_finalized_state,
                        header_chain_observers,
                    ) {
                        return exit;
                    }

                    let tip_block = ChainTipBlock::from(finalized);
                    prev_finalized_note_commitment_trees = Some(note_commitment_trees);
                    chain_tip_sender.set_finalized_tip(tip_block);
                }
                Err((ordered_block, error)) => {
                    let mut attributed_failure_repair_height = None;
                    if let (Some(auxiliary_window), Some(failure)) = (
                        vct_auxiliary_window_for_outcome.as_ref(),
                        error.vct_failure(),
                    ) {
                        let failure_attribution = auxiliary_window.attribute_failure(failure);
                        let attribution_label = failure_attribution.attribution_label();
                        metrics::counter!(
                            "state.vct.aux.verification_failure.count",
                            "attribution" => attribution_label
                        )
                        .increment(1);
                        tracing::warn!(
                            ?failure,
                            attribution = attribution_label,
                            "VCT: attributed exact auxiliary verification failure"
                        );

                        if let Some(writer) = header_chain.as_ref() {
                            match writer.record_vct_auxiliary_failure(
                                auxiliary_window,
                                failure_attribution,
                                failure,
                            ) {
                                Ok(Some(ApplyResult::Committed | ApplyResult::NoChange(_))) => {
                                    attributed_failure_repair_height = failure_attribution
                                        .repair_height(
                                            ordered_block.0.height,
                                            auxiliary_window
                                                .successor
                                                .as_ref()
                                                .map(|successor| successor.height),
                                        );
                                }
                                Ok(Some(ApplyResult::Stale(receipt))) => {
                                    tracing::debug!(
                                        ?receipt,
                                        "VCT: ignored stale auxiliary failure evidence"
                                    );
                                }
                                Ok(Some(ApplyResult::ResourceStalled(receipt))) => {
                                    tracing::warn!(
                                        ?receipt,
                                        "VCT: auxiliary failure evidence stopped by a committed resource alarm"
                                    );
                                }
                                Ok(None) => {}
                                Err(record_error) => {
                                    tracing::error!(
                                        ?record_error,
                                        "VCT: could not persist auxiliary failure evidence"
                                    );
                                }
                            }
                        }
                    }

                    // Retryable VCT root stalls park and retry the same block.
                    // The write loop does not reset the queue for these stalls.
                    // A later delivery of the same header range can fill an absent root.
                    // Header sync does not request individual roots.
                    // The write loop therefore polls absent-root stalls slowly.
                    // An await-successor stall waits only for state to store the next header.
                    // The write loop polls await-successor stalls faster.
                    if let Some(height) = error.vct_retryable_height() {
                        let root_unavailable = error.vct_supplied_root_unavailable_height();
                        let repair_height = attributed_failure_repair_height.unwrap_or(height);

                        prev_finalized_note_commitment_trees = prev_note_commitment_trees_for_retry;
                        let retry_cause = if root_unavailable.is_some() {
                            VctWriteRetryCause::MissingRoot {
                                replacement_required: attributed_failure_repair_height.is_some(),
                            }
                        } else {
                            VctWriteRetryCause::MissingSuccessor
                        };
                        let wait = vct_write_retry_manager.on_retryable_error(
                            repair_height,
                            retry_cause,
                            ordered_block,
                        );
                        std::thread::park_timeout(wait);
                        continue;
                    }

                    let finalized_tip = finalized_state.db.tip();
                    let _ = ordered_block.1.send(Err(error.clone()));

                    // The commit failed and the queue is being reset, so clear
                    // any buffered look-ahead block.
                    vct_write_retry_manager.reset(finalized_state);

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
                        return BlockWriteTaskExit::Completed;
                    }
                }
            }
        }

        // Do this check even if the channel got closed before any finalized blocks were sent.
        // This can happen if we're past the finalized tip.
        if invalid_block_reset_sender.is_closed() {
            info!("StateService closed the block reset channel. Is Zakura shutting down?");
            return BlockWriteTaskExit::Completed;
        }

        if let Err(exit) = attach_header_chain_if_genesis_is_committed(
            header_chain,
            *attach_header_chain_at_handoff,
            finalized_state,
            non_finalized_state,
            header_chain_observers,
        ) {
            return exit;
        }
        if *attach_header_chain_at_handoff && header_chain.is_none() {
            let epoch = match header_chain_observers.begin_reconstruction() {
                Ok(epoch) => epoch,
                Err(error) => return BlockWriteTaskExit::HeaderChainAttachmentFailed(error),
            };
            let error = HeaderChainAttachmentError::MissingGenesis;
            header_chain_observers.failed(epoch, &error);
            return BlockWriteTaskExit::HeaderChainAttachmentFailed(error);
        }

        // Track rejected ancestors so queued descendants can be rejected without
        // attributing the ancestor's validation failure to the descendant's peer.
        let mut rejected_ancestor_map: IndexMap<block::Hash, block::Hash> = IndexMap::new();

        loop {
            let msg = match deferred_non_finalized_messages.pop_front() {
                Some(msg) => Some(msg),
                None => match receive_until_deferred_deadline(
                    non_finalized_block_write_receiver,
                    header_chain.as_ref(),
                    &deadline_runtime,
                ) {
                    Ok(msg) => msg,
                    Err(error) => {
                        tracing::error!(
                            ?error,
                            "stopping state writer after deferred-header maintenance failure"
                        );
                        return BlockWriteTaskExit::HeaderChainRuntimeFailed(
                            BlockWriteTaskFailure::runtime(
                                "deferred-header maintenance stopped the state writer",
                                error,
                            ),
                        );
                    }
                },
            };
            let Some(msg) = msg else {
                break;
            };
            let queued_child_and_rsp_tx = match msg {
                NonFinalizedWriteMessage::ApplyHeaderChainInsert { prepared, rsp_tx } => {
                    let result = header_chain
                        .as_ref()
                        .ok_or(HeaderChainStoreError::Uninitialized)
                        .and_then(|writer| {
                            let insert =
                                prepared
                                    .into_insert()
                                    .ok_or(HeaderChainStoreError::Transition(
                                        zakura_header_chain::TransitionFailure::Authority,
                                    ))?;
                            let authority = PreparedHeaderCompletionAuthority(insert.clone());
                            let mut context = writer.context();
                            context.full_state_authority = Some(&authority);
                            writer.runtime.apply(
                                TransitionRequest {
                                    // Insertions carry typed asynchronous authority.
                                    // The global version coordinate does not authorize insertion work.
                                    expected_version: StateVersion::default(),
                                    event: TransitionEvent::InsertHeaders(insert),
                                },
                                &context,
                            )
                        });
                    let _ = rsp_tx.send(result);
                    None
                }
                NonFinalizedWriteMessage::RecordHeaderChainBodyUnavailable { prepared, rsp_tx } => {
                    let result = header_chain
                        .as_ref()
                        .ok_or(HeaderChainStoreError::Uninitialized)
                        .and_then(|writer| writer.apply_prepared_body_evidence(prepared));
                    let _ = rsp_tx.send(result);
                    None
                }
                NonFinalizedWriteMessage::RecordHeaderChainBodyInvalid { prepared, rsp_tx } => {
                    let result = header_chain
                        .as_ref()
                        .ok_or(HeaderChainStoreError::Uninitialized)
                        .and_then(|writer| writer.apply_prepared_body_evidence(prepared));
                    let _ = rsp_tx.send(result);
                    None
                }
                NonFinalizedWriteMessage::RestartHeaderChainBodyAvailability {
                    prepared,
                    rsp_tx,
                } => {
                    let result = header_chain
                        .as_ref()
                        .ok_or(HeaderChainStoreError::Uninitialized)
                        .and_then(|writer| writer.apply_prepared_body_evidence(prepared));
                    let _ = rsp_tx.send(result);
                    None
                }
                NonFinalizedWriteMessage::RetryHeaderChainBodyAvailability { prepared, rsp_tx } => {
                    let result = header_chain
                        .as_ref()
                        .ok_or(HeaderChainStoreError::Uninitialized)
                        .and_then(|writer| writer.retry_body_availability(prepared));
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
            let rejected_ancestor_hash = rejected_ancestor_map.get(&parent_hash).copied();

            // If the parent block was marked as rejected, also reject all its children.
            //
            // At this point, we know that all the block's descendants
            // are invalid, because we checked all the consensus rules before
            // committing the failing ancestor block to the non-finalized state.
            let result: Result<(), CommitBlockError> =
                if let Some(ancestor_hash) = rejected_ancestor_hash {
                    Err(Box::new(ValidateContextError::InvalidAncestorBlock(ancestor_hash)).into())
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

            if result.is_err() {
                // If the block is invalid, mark any descendant blocks as rejected.
                if matches!(result, Err(CommitBlockError::ValidateContextError(_))) {
                    rejected_ancestor_map
                        .insert(child_hash, rejected_ancestor_hash.unwrap_or(child_hash));
                }

                // Make sure the rejected ancestor map doesn't get too big.
                if rejected_ancestor_map.len() > REJECTED_ANCESTOR_MAP_LIMIT {
                    // We only add one hash at a time, so we only need to remove one extra here.
                    rejected_ancestor_map.shift_remove_index(0);
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
            rejected_ancestor_map.shift_remove(&child_hash);

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
                prev_finalized_note_commitment_trees = match commit_result {
                    Ok((_, trees)) => Some(trees),
                    Err(error) => {
                        tracing::error!(
                            ?error,
                            "stopping state writer after header-chain finalization failure"
                        );
                        return header_chain_finalization_failure(error);
                    }
                };
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
        BlockWriteTaskExit::Completed
    }
}

#[cfg(test)]
mod tests;
