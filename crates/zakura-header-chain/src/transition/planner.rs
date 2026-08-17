//! The sole pure mutation algorithm for durable header-chain state.

mod admission;
mod event_effects;
pub(crate) mod plan;
mod projected_state;
mod replay;
#[cfg(feature = "test-support")]
pub mod retention;
#[cfg(not(feature = "test-support"))]
mod retention;
mod settlement;
mod violations;
mod write_set;

#[cfg(test)]
mod tests;

use thiserror::Error;

use crate::{
    CounterExhausted, EngineSnapshot, EngineTransition, GraphError, HeaderChainEngine, StoreError,
    TransitionContext, TransitionInput,
};

pub use violations::{
    AuxiliaryViolation, BodyViolation, FinalityViolation, HeaderPathKind, HeaderPathProblem,
    HeaderValidationCheck, HeaderValidationSource, HeaderViolation, InvalidTransitionEvidence,
    LimitViolation, OperatorViolation, PlannerCoherenceViolation, ProjectionKind,
};

pub(crate) use plan::PlanCandidate;

use admission::authenticate_and_admit;
use event_effects::{migrated_pin_refuted, project_event_evidence, EventProjectionContext};
use projected_state::ProjectedTransitionState;
use replay::bind_replay_and_freshness;
use settlement::{
    apply_migrated_pin_alarm, derive_finality_and_retention, FinalityRetentionOutcome,
    SettlementInputs,
};
use write_set::{derive_plan, invariant_pins, no_change, resource_stalled, DerivePlanInputs};

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
    ///
    /// Reserved for genuine store I/O / coherence failures. Missing adapter-supplied
    /// [`crate::TransitionInput`] facts use [`Self::MissingDurableFacts`] instead.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Required durable facts were absent from [`crate::TransitionInput`].
    ///
    /// Distinct from [`Self::Store`]: the planner never reads the durable store, so
    /// omitted validation leases, finality rebase history, or migrated-pin facts are
    /// adapter contract failures—not storage unavailability.
    #[error("transition input is missing required durable facts: {0}")]
    MissingDurableFacts(&'static str),
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
    #[error(transparent)]
    InvalidEvidence(#[from] InvalidTransitionEvidence),
    /// Auxiliary delivery bounds refused this event before any durable mutation.
    ///
    /// Unlike a [`crate::TransitionEffect`] with `resource_stalled`, this is a
    /// zero-effect planner failure: it does not raise the durable resource-stall
    /// alarm or produce a [`crate::CommittedStallReceipt`]. Distinct from
    /// [`crate::InvariantViolation::Limits`], which fails commit-time verification
    /// after projection. See [`crate::ApplyResult`] for the three-way mapping.
    #[error("header admission refused because auxiliary delivery limits are exceeded")]
    AuxiliaryLimitExceeded,
    /// The projected write set violated a commit invariant.
    #[error(transparent)]
    Invariant(#[from] super::InvariantViolation),
}

/// Derive one atomic transition without mutating the engine.
///
/// Pure: freezes `engine` as the snapshot before commit, projects `input` under
/// `context`, and returns an [`EngineTransition`] only after independent
/// commit-time invariant verification. Failure is [`TransitionFailure`] with
/// zero durable effects—distinct from a verified no-change plan.
pub(super) fn derive_transition_plan(
    engine: &HeaderChainEngine,
    input: TransitionInput,
    context: &TransitionContext<'_>,
) -> Result<EngineTransition, TransitionFailure> {
    let candidate = derive_plan_candidate(engine, input, context)?;
    // Phase 6: verify invariants
    super::verify_candidate(engine, &candidate)?;
    Ok(EngineTransition::from_verified(candidate))
}

/// Project `input` into an unverified [`PlanCandidate`] without mutating `engine`.
///
/// Callers must still run [`super::verify_candidate`]
/// before treating the result as commit-capable.
fn derive_plan_candidate(
    engine: &HeaderChainEngine,
    input: TransitionInput,
    context: &TransitionContext<'_>,
) -> Result<PlanCandidate, TransitionFailure> {
    // Phase 1: authenticate / admit
    let (snapshot_before_commit, mut metadata, admitted) =
        authenticate_and_admit(engine, &input, context)?;
    if snapshot_before_commit.alarms.resource_stalled
        && matches!(&admitted.event, crate::TransitionEvent::InsertHeaders(_))
    {
        let domain = admitted.event.domain();
        return resource_stalled(engine, snapshot_before_commit, domain, context);
    }

    // Phase 2: bind replay and freshness
    let bound_request =
        bind_replay_and_freshness(engine, &input, &snapshot_before_commit, &metadata, admitted)?;
    if let Some(effect) = bound_request.no_change_effect {
        return no_change(
            engine,
            snapshot_before_commit,
            metadata,
            bound_request.event,
            context,
            bound_request.domain,
            effect,
        );
    }
    let event = bound_request.event;
    let domain = bound_request.domain;

    // Phase 3: project event evidence
    let old_selected = engine.selected_projection();
    let old_verified = engine.verified_projection();
    let mut projected = ProjectedTransitionState::new(engine);
    let migrated_pin = migrated_pin_refuted(&input, &event)?;
    apply_migrated_pin_alarm(&mut metadata, migrated_pin);
    let event_context = EventProjectionContext {
        engine,
        input: &input,
        transition: context,
        snapshot_before_commit: &snapshot_before_commit,
        old_selected,
        migrated_pin_refuted: migrated_pin,
    };
    project_event_evidence(&mut projected, &event, &event_context)?;

    // Phase 4: derive finality and retention
    let settlement = derive_finality_and_retention(SettlementInputs {
        engine,
        projected,
        metadata,
        snapshot_before_commit: &snapshot_before_commit,
        event: &event,
        header_rebase: bound_request.header_rebase,
        context,
        old_selected,
        old_verified,
    })?;
    let settled = match settlement {
        FinalityRetentionOutcome::ResourceStalled => {
            return resource_stalled(engine, snapshot_before_commit, domain, context);
        }
        FinalityRetentionOutcome::Settled(settled) => *settled,
    };

    // Phase 5: assemble writes
    assemble_writes(
        engine,
        snapshot_before_commit,
        old_selected,
        old_verified,
        settled,
        &event,
        context,
    )
}

/// Assemble the durable write set and graph delta from a fully settled projection.
fn assemble_writes<'a>(
    engine: &'a HeaderChainEngine,
    snapshot_before_commit: EngineSnapshot,
    old_selected: &'a [crate::Frontier],
    old_verified: &'a [crate::Frontier],
    settled: settlement::SettledTransition<'a>,
    event: &crate::TransitionEvent,
    context: &TransitionContext<'_>,
) -> Result<PlanCandidate, TransitionFailure> {
    let settlement::SettledTransition {
        projected,
        selected,
        finality_append,
        finality_lineage,
        retention,
        effect,
        metadata,
    } = settled;
    derive_plan(DerivePlanInputs {
        snapshot_before_commit,
        metadata,
        base_graph: engine.graph(),
        projected,
        old_selected,
        old_verified,
        selected,
        finality_append,
        finality_lineage,
        retention,
        fingerprint: event.fingerprint(),
        domain: event.domain(),
        effect,
        trust_pins: invariant_pins(context),
        limits: context.config.limits,
    })
}
