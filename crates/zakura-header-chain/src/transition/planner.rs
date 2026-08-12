//! The sole pure mutation algorithm for durable header-chain state.

mod admission;
mod event_effects;
mod projected_state;
mod settlement;
mod write_set;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use thiserror::Error;

use crate::graph::GraphDelta;
use crate::{
    ChangeSet, CounterExhausted, DurableTransitionFacts, EngineLimits, EngineSnapshot, Frontier,
    GraphError, HeaderChainEngine, StoreError, TransitionCause, TransitionContext,
    TransitionRequest,
};

use admission::{
    authenticate_and_admit, rebase_header_insertion, validate_body_owner,
    validate_header_sync_owner, AdmittedRequest, HeaderInsertionRebase,
};
use event_effects::{apply_event_evidence, migrated_pin_refuted, ApplyEventContext};
use projected_state::ProjectedTransitionState;
use settlement::{
    apply_migrated_pin_alarm, derive_finality_and_retention, FinalityRetentionOutcome,
    SettlementInputs,
};
use write_set::{derive_plan, invariant_pins, no_change, resource_stalled, DerivePlanInputs};

/// A complete projected write set awaiting independent invariant verification.
#[derive(Clone, Debug)]
pub(crate) struct PlanCandidate {
    pub(super) before: EngineSnapshot,
    pub(super) change_set: ChangeSet,
    pub(super) graph_delta: GraphDelta,
    pub(super) cause: TransitionCause,
    pub(super) trust_pins: Arc<[Frontier]>,
    pub(super) limits: EngineLimits,
}

impl PlanCandidate {
    /// Return the opaque graph transition paired with this candidate.
    pub(super) const fn graph_delta(&self) -> &GraphDelta {
        &self.graph_delta
    }

    /// Return the candidate's classified transition cause.
    pub(super) const fn cause(&self) -> TransitionCause {
        self.cause
    }
}

/// A candidate accepted by the independent transition invariant verifier.
///
/// Production callers consume [`crate::EngineTransition`]; this verified type
/// stays crate-private so adapters cannot construct unchecked transitions.
#[derive(Clone, Debug)]
pub(crate) struct TransitionPlan {
    candidate: PlanCandidate,
}

impl TransitionPlan {
    fn from_verified(candidate: PlanCandidate) -> Self {
        Self { candidate }
    }

    #[cfg(test)]
    pub(super) const fn candidate(&self) -> &PlanCandidate {
        &self.candidate
    }

    /// Return the atomic write set for the state adapter.
    pub const fn change_set(&self) -> &ChangeSet {
        &self.candidate.change_set
    }

    /// Return the coherent state observed before planning.
    pub const fn before(&self) -> &EngineSnapshot {
        &self.candidate.before
    }

    /// Return the classified transition cause.
    pub const fn cause(&self) -> TransitionCause {
        self.candidate.cause
    }

    /// Return true when the evidence was valid but changed no durable fact.
    pub fn is_no_change(&self) -> bool {
        self.candidate.before.state_version == self.candidate.change_set.metadata.state_version
    }

    /// Return the opaque graph transition that matches the durable write set.
    pub(crate) const fn graph_delta(&self) -> &GraphDelta {
        &self.candidate.graph_delta
    }
}

#[cfg(test)]
impl std::ops::Deref for TransitionPlan {
    type Target = PlanCandidate;

    fn deref(&self) -> &Self::Target {
        &self.candidate
    }
}

#[cfg(test)]
impl std::ops::DerefMut for TransitionPlan {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.candidate
    }
}

/// Typed failure that the planner produces before it attempts a durable mutation.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum TransitionFailure {
    /// The caller's version or asynchronous owner was stale.
    #[error("stale transition work at state version {current:?}")]
    Stale {
        /// Current durable version.
        current: crate::StateVersion,
    },
    /// The store could not read durable rows coherently.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The projected graph would be incoherent.
    #[error(transparent)]
    Graph(#[from] GraphError),
    /// The transition exhausted a monotonic durable counter.
    #[error(transparent)]
    Counter(#[from] CounterExhausted),
    /// Persisted immutable configuration differs from this engine.
    #[error("persisted header-chain configuration does not match the active engine")]
    ConfigurationMismatch,
    /// This event is unavailable in the configured mode.
    #[error("transition event is not admitted in the configured engine mode")]
    Mode,
    /// Required internal authority did not authenticate the evidence.
    #[error("transition evidence lacks the required internal authority")]
    Authority,
    /// Prepared work no longer matches its durable validation context.
    #[error("prepared header context is stale")]
    StalePreparation,
    /// A caller reused one domain-local replay key for a different payload.
    #[error("transition replay key conflicts with the previously committed payload")]
    ConflictingReplay,
    /// Event fields contradict canonical headers or durable ancestry.
    #[error("invalid transition evidence: {0}")]
    InvalidEvidence(&'static str),
    /// Auxiliary delivery bounds refused this event before any durable mutation.
    ///
    /// Unlike [`TransitionCause::ResourceStalled`], this is a zero-effect planner
    /// failure: it does not raise the durable resource-stall alarm or produce a
    /// [`crate::CommittedStallReceipt`].
    #[error("header admission refused because auxiliary delivery limits are exceeded")]
    AuxiliaryLimitExceeded,
    /// The projected write set violated a commit invariant.
    #[error(transparent)]
    Invariant(#[from] super::InvariantViolation),
}

/// Derive one atomic transition without mutating the coherent engine.
pub(super) fn derive_transition_plan(
    engine: &HeaderChainEngine,
    durable: &DurableTransitionFacts,
    request: TransitionRequest,
    context: &TransitionContext<'_>,
) -> Result<TransitionPlan, TransitionFailure> {
    let candidate = derive_plan_candidate(engine, durable, request, context)?;
    // Phase 6: verify invariants
    verify_invariants(engine, &candidate)?;
    Ok(TransitionPlan::from_verified(candidate))
}

fn derive_plan_candidate(
    engine: &HeaderChainEngine,
    durable: &DurableTransitionFacts,
    request: TransitionRequest,
    context: &TransitionContext<'_>,
) -> Result<PlanCandidate, TransitionFailure> {
    // Phase 1: authenticate / admit
    let (before, mut metadata, admitted) = authenticate_and_admit(engine, request, context)?;

    // Phase 2: bind replay and freshness
    let bound = bind_replay_and_freshness(engine, durable, &before, &metadata, admitted)?;
    if let Some(cause) = bound.no_change_cause {
        return no_change(engine, before, metadata, bound.event, context, cause);
    }
    let event = bound.event;

    // Phase 3: apply event evidence
    let old_selected = engine.selected_projection();
    let old_verified = engine.verified_projection();
    let mut projected = ProjectedTransitionState::new(engine);
    let migrated_pin = migrated_pin_refuted(durable, &event)?;
    apply_migrated_pin_alarm(&mut metadata, migrated_pin);
    let event_context = ApplyEventContext {
        engine,
        durable,
        transition: context,
        before: &before,
        old_selected,
        migrated_pin_refuted: migrated_pin,
    };
    apply_event_evidence(&mut projected, &event, &event_context)?;

    // Phase 4: derive finality and retention
    let settlement = derive_finality_and_retention(SettlementInputs {
        engine,
        projected,
        metadata,
        before: &before,
        event: &event,
        header_rebase: bound.header_rebase,
        context,
        old_selected,
    })?;
    let settled = match settlement {
        FinalityRetentionOutcome::ResourceStalled => {
            return resource_stalled(engine, before, context);
        }
        FinalityRetentionOutcome::Settled(settled) => *settled,
    };

    // Phase 5: assemble writes
    assemble_writes(
        engine,
        before,
        old_selected,
        old_verified,
        settled,
        &event,
        context,
    )
}

struct BoundRequest {
    event: crate::TransitionEvent,
    header_rebase: HeaderInsertionRebase,
    no_change_cause: Option<TransitionCause>,
}

fn bind_replay_and_freshness(
    engine: &HeaderChainEngine,
    durable: &DurableTransitionFacts,
    before: &EngineSnapshot,
    metadata: &crate::EngineMetadata,
    request: AdmittedRequest,
) -> Result<BoundRequest, TransitionFailure> {
    let AdmittedRequest {
        mut event,
        expected_version,
    } = request;
    let header_rebase = rebase_header_insertion(&mut event, before, engine.graph(), durable)?;
    if let Some(owner) = event.header_sync_owner() {
        validate_header_sync_owner(owner, before)?;
    }
    if let Some(owner) = event.body_owner() {
        validate_body_owner(owner, before)?;
    }

    let fingerprint = event.fingerprint();
    if fingerprint.is_some() && metadata.last_transition == fingerprint {
        return Ok(BoundRequest {
            event,
            header_rebase,
            no_change_cause: Some(TransitionCause::Event),
        });
    }
    if metadata
        .last_transition
        .zip(fingerprint)
        .is_some_and(|(previous, current)| previous.conflicts_with(current))
    {
        return Err(TransitionFailure::ConflictingReplay);
    }
    let has_async_authority = event.header_sync_owner().is_some() || event.body_owner().is_some();
    if !has_async_authority && expected_version != before.state_version {
        return Err(TransitionFailure::Stale {
            current: before.state_version,
        });
    }
    Ok(BoundRequest {
        event,
        header_rebase,
        no_change_cause: (header_rebase == HeaderInsertionRebase::AlreadyApplied)
            .then_some(TransitionCause::HeaderWorkAlreadyApplied),
    })
}

fn assemble_writes<'a>(
    engine: &'a HeaderChainEngine,
    before: EngineSnapshot,
    old_selected: &'a [Frontier],
    old_verified: &'a [Frontier],
    settled: settlement::SettledTransition<'a>,
    event: &crate::TransitionEvent,
    context: &TransitionContext<'_>,
) -> Result<PlanCandidate, TransitionFailure> {
    let settlement::SettledTransition {
        projected,
        selected,
        finality_append,
        retention,
        cause,
        metadata,
    } = settled;
    derive_plan(DerivePlanInputs {
        before,
        metadata,
        base_graph: engine.graph(),
        projected,
        old_selected,
        old_verified,
        selected,
        finality_append,
        retention,
        fingerprint: event.fingerprint(),
        cause,
        trust_pins: invariant_pins(context),
        limits: context.config.limits,
    })
}

fn verify_invariants(
    engine: &HeaderChainEngine,
    candidate: &PlanCandidate,
) -> Result<(), TransitionFailure> {
    Ok(super::verify_candidate(engine, candidate)?)
}
