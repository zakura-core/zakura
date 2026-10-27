//! [`EnginePort`]'s full state: committed blocks, body evidence and the
//! committed views, as zakura-state's writer and readers provide them.

use std::sync::Arc;

#[cfg(test)]
mod tests;

use zakura_chain::block;
use zakura_header_chain::{
    ApplyResult, CommittedHeaderChainView, CommittedStallReceipt, EngineSnapshot,
    FullStateEvidenceAuthority, HeaderChainEngine, HeaderSyncWorkOwner, HeaderValidationFacts,
    NoChangeReceipt, StaleReceipt, StoreError, TransitionContext, TransitionEffect,
    TransitionEvent, TransitionFailure, TransitionInput, TransitionRequest, VerifiedChangeCause,
};
use zakura_rt::sync::watch;

use super::EnginePort;
use crate::{
    full_state::{
        self as state, combined_retention_references, hint_hashes, hint_updates, verified_request,
        BodyMetadata, PreparedAuthority, SchedulerAuthority, StateError, StateIssuedAuthority,
        VerifiedChain,
    },
    header::fixture::ScenarioClock,
};

/// What a full-state transition did.
#[derive(Clone, Debug)]
pub struct Applied {
    /// The engine's result.
    pub result: ApplyResult,
    /// What the engine installed, or `None` if it installed nothing.
    pub installed: Option<Installed>,
    /// The verified-chain change a commit sent the engine, or `None` for a
    /// side path, a body record, or a commit the engine stalled, which
    /// changes no verified chain.
    pub cause: Option<VerifiedChangeCause>,
}

/// One installed transition.
#[derive(Clone, Debug)]
pub struct Installed {
    /// The transition's effect, which says whether it invalidated body work.
    pub effect: TransitionEffect,
    /// The view published once the engine installed it.
    pub view: CommittedHeaderChainView,
}

impl EnginePort {
    /// The committed-view watch block sync starts from, or after shutdown a
    /// closed one. It holds the latest view at once.
    pub fn subscribe_views(&self) -> watch::Receiver<Option<CommittedHeaderChainView>> {
        let sender = {
            let _engine = self.engine();
            self.views().sender()
        };
        match sender {
            Some(sender) => sender.subscribe(),
            None => watch::channel(None).1,
        }
    }

    /// The latest committed view.
    pub fn view(&self) -> CommittedHeaderChainView {
        let _engine = self.engine();
        self.views().view()
    }

    /// Runs `read` on full state, read together with the engine.
    pub fn with_full_state<R>(&self, read: impl FnOnce(&VerifiedChain) -> R) -> R {
        let _engine = self.engine();
        read(&self.full_state())
    }

    /// Commits `block`, which the verifier received `receipt`th, to full
    /// state and runs its verified-chain transition on the engine, as
    /// zakura-state does once the verifier accepts a body (`verified_request`
    /// and `PreparedFullStateTransition::commit` in `service/write.rs`).
    /// Full state changes only if the engine commits the transition, or
    /// finds it changes nothing. A stale or resource-stalled transition
    /// leaves full state unchanged, and zakura-state reports both as
    /// `HeaderChainError`.
    ///
    /// Full state's chain tips are the retention references, and the bodies
    /// its fork limit evicts go to the engine with the event.
    ///
    /// # Errors
    ///
    /// If `block` has no coinbase height, is already committed or its
    /// parent isn't, or the engine refuses the transition.
    ///
    /// # Panics
    ///
    /// If the commit would take the best chain past full state's
    /// non-finalized window. See [`VerifiedChain::with`].
    pub(crate) fn commit_block(
        &self,
        block: Arc<block::Block>,
        receipt: u64,
    ) -> Result<Applied, StateError> {
        let height = block.coinbase_height().ok_or(StoreError::Incoherent(
            "a committed block encodes its height",
        ))?;
        let accepted = zakura_header_chain::Frontier::new(height, block.hash());
        let mut engine = self.engine();
        let before = self.full_state().clone();
        if before.contains(accepted.hash) || !before.contains(block.header.previous_block_hash) {
            return Err(StoreError::Incoherent(
                "full state commits each block once, after its parent",
            )
            .into());
        }
        let after = before.with(block, height, receipt);
        let request = verified_request(&before, &after, accepted, &engine.snapshot())?;
        let tips = after.tips();
        let expected_verified = after.best_tip();
        let mut authority = PreparedAuthority::for_event(&request.event)?;
        authority.retention_references = tips.clone();
        authority.verified_tip = Some(expected_verified);
        authority.evicted_bodies = before.evicted_blocks(&after);
        self.apply_locked(
            &mut engine,
            request,
            &authority,
            &tips,
            Some(expected_verified),
            || *self.full_state() = after,
        )
    }

    /// Records body evidence, a supplier discovery or a scheduler retry, as
    /// zakura-state's `apply_prepared_body_evidence` and
    /// `retry_body_availability` do. The engine decides whether it's
    /// recorded, stale or a no-op.
    ///
    /// # Errors
    ///
    /// If `request` carries another event, or the engine refuses it.
    pub(crate) fn record_body_evidence(
        &self,
        request: TransitionRequest,
    ) -> Result<Applied, StateError> {
        let mut engine = self.engine();
        match &request.event {
            TransitionEvent::OperatorBodyRetry(retry) => {
                let authority = SchedulerAuthority(*retry);
                self.apply_locked(&mut engine, request, &authority, &[], None, || {})
            }
            TransitionEvent::BodyEvidence(_) | TransitionEvent::BodySupplierDiscovered(_) => {
                let authority = PreparedAuthority::for_event(&request.event)?;
                self.apply_locked(&mut engine, request, &authority, &[], None, || {})
            }
            _ => Err(StoreError::Incoherent("body evidence carries a body event").into()),
        }
    }

    /// The selected headers from `from` whose bodies full state lacks. See
    /// [`state::missing_block_body_metadata`].
    ///
    /// # Errors
    ///
    /// As [`state::missing_block_body_metadata`] does.
    pub fn missing_block_body_metadata(
        &self,
        from: block::Height,
        limit: u32,
    ) -> Result<BodyMetadata, StoreError> {
        let engine = self.engine();
        let chain = self.full_state();
        state::missing_block_body_metadata(&engine, &chain, from, limit)
    }

    /// Serving's bounded range read. See [`state::read_block_range`].
    ///
    /// # Errors
    ///
    /// As [`state::read_block_range`] does.
    pub fn read_block_range(
        &self,
        start: block::Height,
        count: u32,
        max_body_bytes: u32,
        cancelled: impl FnMut() -> bool,
    ) -> Result<Vec<(block::Height, Vec<u8>)>, StoreError> {
        let engine = self.engine();
        let chain = self.full_state();
        state::read_block_range(&engine, &chain, start, count, max_body_bytes, cancelled)
    }

    /// [`Self::commit_block`], for zakurad's driver fidelity test, whose
    /// verifier fake commits outside a simulator run. A run commits only
    /// through the block service, which publishes each commit's facts.
    ///
    /// # Errors
    ///
    /// As [`Self::commit_block`] does.
    #[cfg(feature = "driver-fidelity")]
    pub fn commit_block_for_driver_test(
        &self,
        block: Arc<block::Block>,
        receipt: u64,
    ) -> Result<Applied, StateError> {
        self.commit_block(block, receipt)
    }

    /// [`Self::record_body_evidence`], for zakurad's driver fidelity test,
    /// whose state-writer fake records outside a simulator run.
    ///
    /// # Errors
    ///
    /// As [`Self::record_body_evidence`] does.
    #[cfg(feature = "driver-fidelity")]
    pub fn record_body_evidence_for_driver_test(
        &self,
        request: TransitionRequest,
    ) -> Result<Applied, StateError> {
        self.record_body_evidence(request)
    }

    /// Closes the view watch, as service shutdown does.
    pub(crate) fn close_views(&self) {
        let sender = {
            let _engine = self.engine();
            self.views().close()
        };
        // Dropped after the locks, since the last sender's drop is a
        // scheduling point.
        drop(sender);
    }

    /// Plans, installs and publishes one full-state transition, as
    /// zakura-state's `apply_combined_inner` does for every event but a
    /// header insertion. `inner` authorizes the event, and
    /// `context_references` are the caller's retention references.
    /// `expected_verified`, if set, is full state's tip, which the engine's
    /// verified tip must match. `memory_swap` runs once the engine commits
    /// the change or finds none, as full state swaps its memory there.
    ///
    /// zakura-state also checks that the engine's graph keeps every staged
    /// full-state header. The engine inserts a verified path's headers
    /// itself, so that check never fails here, and it isn't ported.
    fn apply_locked(
        &self,
        engine: &mut HeaderChainEngine,
        request: TransitionRequest,
        inner: &dyn FullStateEvidenceAuthority,
        context_references: &[block::Hash],
        expected_verified: Option<zakura_header_chain::Frontier>,
        memory_swap: impl FnOnce(),
    ) -> Result<Applied, StateError> {
        let fork_set = matches!(
            &request.event,
            TransitionEvent::VerifiedChainChanged(_) | TransitionEvent::VerifiedBlockAccepted(_)
        ) && inner.authorizes_full_state(&request.event);
        // An authorized fork-set change carries its own tips.
        let active = if fork_set {
            Vec::new()
        } else {
            self.full_state().tips()
        };
        let retention_references = combined_retention_references(context_references, &active);
        let before = engine.snapshot();
        let cause = match &request.event {
            TransitionEvent::VerifiedChainChanged(event) => Some(event.cause),
            _ => None,
        };
        let idempotency_key = request.event.idempotency_key();
        let branch = request
            .event
            .header_sync_owner()
            .map(HeaderSyncWorkOwner::header_authority)
            .map(|authority| authority.branch)
            .or_else(|| request.event.body_owner().map(|owner| owner.branch));
        let input = self.full_state_input(engine, request, &before)?;
        let validation_leases = input
            .header_validation_facts()
            .map(|facts| facts.validation_leases.clone())
            .unwrap_or_default();
        let authority = StateIssuedAuthority {
            inner: Some(inner),
            validation_leases: &validation_leases,
            active_retention_references: &active,
            full_state_authorization_version: Some(before.state_version),
        };
        let context = TransitionContext {
            config: &self.config,
            clock: &ScenarioClock,
            full_state_authority: Some(&authority),
            retention_references: &retention_references,
        };
        let transition = match engine.plan_transition(input, &context) {
            Ok(transition) => transition,
            Err(TransitionFailure::Stale { current }) => {
                return Ok(Applied {
                    result: ApplyResult::Stale(StaleReceipt {
                        current_version: current,
                        branch,
                    }),
                    installed: None,
                    cause,
                })
            }
            Err(failure) => return Err(failure.into()),
        };
        let effect = transition.effect();
        if effect.is_resource_stalled() {
            let receipt = CommittedStallReceipt {
                state_version: transition.change_set().metadata.state_version,
                alarm_changed: transition.snapshot_before_commit().alarms.resource_stalled
                    != transition.change_set().metadata.alarms.resource_stalled,
                attempted_branch: branch,
            };
            if transition.is_no_change() {
                return Ok(Applied {
                    result: ApplyResult::ResourceStalled(receipt),
                    installed: None,
                    cause: None,
                });
            }
            let current = transition.snapshot_after_commit().clone();
            engine.install_committed_transition(transition)?;
            let view = self.publish_locked(current, effect, Vec::new());
            return Ok(Applied {
                result: ApplyResult::ResourceStalled(receipt),
                installed: Some(Installed { effect, view }),
                cause: None,
            });
        }
        if let Some(expected) = expected_verified {
            if transition.change_set().metadata.frontiers.verified_best != expected {
                return Err(StoreError::Incoherent(
                    "the engine's verified tip differs from full state's",
                )
                .into());
            }
        }
        if transition.is_no_change() {
            memory_swap();
            return Ok(Applied {
                result: ApplyResult::NoChange(NoChangeReceipt {
                    state_version: transition.snapshot_before_commit().state_version,
                    idempotency_key,
                }),
                installed: None,
                cause,
            });
        }
        let current = transition.snapshot_after_commit().clone();
        let hints = hint_hashes(engine, transition.change_set());
        engine.install_committed_transition(transition)?;
        memory_swap();
        let updates = hint_updates(engine, hints);
        let view = self.publish_locked(current, effect, updates);
        Ok(Applied {
            result: ApplyResult::Committed,
            installed: Some(Installed { effect, view }),
            cause,
        })
    }

    /// Binds `request` to the validation leases its event consumes, as
    /// zakura-state's `build_transition_input` does: a grow reads the old
    /// tip's lease, and a reset or side path reads the finalized block's.
    fn full_state_input(
        &self,
        engine: &HeaderChainEngine,
        request: TransitionRequest,
        before: &EngineSnapshot,
    ) -> Result<TransitionInput, StateError> {
        let expected_version = request.expected_version;
        let lease = |parent: zakura_header_chain::Frontier| {
            self.validation_lease(engine, parent.hash)?
                .ok_or(StoreError::Incoherent(
                    "a validation predecessor is missing from the retained graph",
                ))
        };
        Ok(match request.event {
            TransitionEvent::VerifiedChainChanged(event) => {
                let parent = match event.cause {
                    VerifiedChangeCause::Grow | VerifiedChangeCause::CheckpointFinalizedGrow => {
                        event.old_tip
                    }
                    VerifiedChangeCause::Reset => before.frontiers.finalized,
                };
                TransitionInput::VerifiedChainChanged {
                    expected_version,
                    event,
                    facts: HeaderValidationFacts {
                        validation_leases: vec![lease(parent)?],
                    },
                }
            }
            TransitionEvent::VerifiedBlockAccepted(event) => {
                TransitionInput::VerifiedBlockAccepted {
                    expected_version,
                    event,
                    facts: HeaderValidationFacts {
                        validation_leases: vec![lease(before.frontiers.finalized)?],
                    },
                }
            }
            TransitionEvent::BodyEvidence(event) => TransitionInput::BodyEvidence {
                expected_version,
                event,
            },
            TransitionEvent::BodySupplierDiscovered(event) => {
                TransitionInput::BodySupplierDiscovered {
                    expected_version,
                    event,
                }
            }
            TransitionEvent::OperatorBodyRetry(event) => TransitionInput::OperatorBodyRetry {
                expected_version,
                event,
            },
            _ => {
                return Err(StoreError::Incoherent(
                    "full state sends only verified-chain and body events",
                )
                .into())
            }
        })
    }
}
