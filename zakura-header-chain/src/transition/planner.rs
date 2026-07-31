//! The sole pure mutation algorithm for durable header-chain state.

use std::collections::{HashMap, HashSet};

use thiserror::Error;
use zakura_chain::block;

use crate::retention::RetentionPlan;
use crate::{
    BodyEvidence, BodyValidationState, ChangeSet, CounterExhausted, DurableTransitionFacts,
    EligibilityDelta, EligibilityReason, EngineLimits, EngineMetadata, EngineMode, EngineSnapshot,
    EventAdmission, EvidenceId, FinalityRecord, FinalitySource, Frontier, FrontierSet, GraphError,
    HeaderChainEngine, HeaderNode, HeaderValidationState, IndexChanges, MemHeaderStore,
    ProjectionDelta, StateVersion, StoreError, TargetCompletion, TransitionCause,
    TransitionContext, TransitionEvent, TransitionRequest, WorkOwner,
};

/// A complete write set plus the private projected graph it was verified against.
#[derive(Clone, Debug)]
pub struct TransitionPlan {
    pub(super) before: EngineSnapshot,
    pub(super) change_set: ChangeSet,
    pub(super) projected: MemHeaderStore,
    pub(super) cause: TransitionCause,
    pub(super) trust_pins: Vec<Frontier>,
    pub(super) limits: EngineLimits,
}

impl TransitionPlan {
    /// Return the atomic write set for the state adapter.
    pub const fn change_set(&self) -> &ChangeSet {
        &self.change_set
    }

    /// Return the coherent state observed before planning.
    pub const fn before(&self) -> &EngineSnapshot {
        &self.before
    }

    /// Return the classified transition cause.
    pub const fn cause(&self) -> TransitionCause {
        self.cause
    }

    /// Return true when the evidence was valid but changed no durable fact.
    pub fn is_no_change(&self) -> bool {
        self.before.state_version == self.change_set.metadata.state_version
    }

    /// Return the fully verified projected graph for an adapter's post-commit cache swap.
    pub const fn projected_graph(&self) -> &MemHeaderStore {
        &self.projected
    }
}

/// Typed failure produced before any durable mutation is attempted.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum TransitionFailure {
    /// The caller's version or asynchronous owner was stale.
    #[error("stale transition work at state version {current:?}")]
    Stale {
        /// Current durable version.
        current: StateVersion,
    },
    /// Durable rows could not be read coherently.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The projected graph would be incoherent.
    #[error(transparent)]
    Graph(#[from] GraphError),
    /// A monotonic durable counter was exhausted.
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
    /// Event fields contradict canonical headers or durable ancestry.
    #[error("invalid transition evidence: {0}")]
    InvalidEvidence(&'static str),
    /// Retention could not admit this event without evicting protected state.
    #[error("header admission refused because protected paths fill the resource bound")]
    ResourceStalled,
    /// The projected write set violated a commit invariant.
    #[error(transparent)]
    Invariant(#[from] super::InvariantViolation),
}

/// Derive one atomic transition without mutating the coherent engine.
pub(super) fn apply_transition_engine(
    engine: &HeaderChainEngine,
    durable: &DurableTransitionFacts,
    request: TransitionRequest,
    context: &TransitionContext<'_>,
) -> Result<TransitionPlan, TransitionFailure> {
    let before = engine.snapshot();
    let mut metadata = engine.metadata().clone();
    validate_snapshot(&before, &metadata, context)?;
    if metadata.last_transition_id
        == request
            .event
            .idempotency_key()
            .unwrap_or(metadata.last_transition_id)
        && request.event.idempotency_key().is_some()
    {
        let plan = no_change(engine, before, metadata, request.event, context)?;
        super::verify_plan(engine, &plan)?;
        return Ok(plan);
    }
    if request.expected_version != before.state_version {
        return Err(TransitionFailure::Stale {
            current: before.state_version,
        });
    }
    if let Some(owner) = request.event.work_owner() {
        validate_owner(owner, &before)?;
    }
    validate_authority(&request.event, context)?;

    let mut graph = engine.graph().clone();
    let old_nodes = node_map(&graph);
    let old_selected = engine.selected_projection().to_vec();
    let old_verified = engine.verified_projection().to_vec();
    let mut verified = old_verified.clone();
    let mut aux_changes = Vec::new();
    let mut finality = None;
    let mut operator_reason_changed = false;
    let migrated_pin_refuted = migrated_pin_refuted(durable, &request.event)?;
    let event_context = ApplyEventContext {
        engine,
        durable,
        transition: context,
        before: &before,
        old_selected: &old_selected,
        migrated_pin_refuted,
    };

    apply_event(
        &mut graph,
        &mut verified,
        &mut aux_changes,
        &mut operator_reason_changed,
        &request.event,
        &event_context,
    )?;
    if let Some(pin) = migrated_pin_refuted {
        metadata.alarms.migrated_pin_refuted = Some(pin);
    }
    if operator_reason_changed {
        verified = select_fully_verified_path(&graph)?;
    }
    let (mut header_best, _) = graph.select_header_best()?;

    if let TransitionEvent::FullStateFinalized(event) = &request.event {
        if event.new_finalized.height < before.frontiers.finalized.height {
            return Err(TransitionFailure::InvalidEvidence("finality retreated"));
        }
        if !verified.contains(&event.new_finalized) {
            return Err(TransitionFailure::InvalidEvidence(
                "integrated finality is not on the verified projection",
            ));
        }
        finality = Some((
            event.new_finalized,
            FinalitySource::FullState {
                evidence: event.full_state_transition_id,
            },
        ));
    } else if context.config.mode == EngineMode::HeadersOnly {
        let depth = context.config.limits.local_finality_depth.get();
        if header_best
            .height
            .0
            .saturating_sub(graph.finalized().height.0)
            > depth
        {
            let height = block::Height(header_best.height.0 - depth);
            let new_finalized = graph.ancestor(header_best.hash, height)?.ok_or(
                TransitionFailure::InvalidEvidence("selected ancestry is incomplete"),
            )?;
            finality = Some((
                new_finalized,
                FinalitySource::HeadersOnlyDepth {
                    selected_tip: header_best,
                },
            ));
        }
    }

    let mut cause = TransitionCause::Event;
    let mut finality_append = None;
    if let Some((new_finalized, source)) = finality {
        if new_finalized != graph.finalized() {
            let previous = graph.finalized();
            let epoch = metadata.finality_epoch.checked_next()?;
            graph.advance_finalized(new_finalized)?;
            verified.retain(|frontier| frontier.height >= new_finalized.height);
            if verified.first().copied() != Some(new_finalized) {
                verified.insert(0, new_finalized);
            }
            finality_append = Some(FinalityRecord {
                previous,
                current: new_finalized,
                source,
                epoch,
            });
            header_best = graph.select_header_best()?.0;
            if matches!(source, FinalitySource::HeadersOnlyDepth { .. }) {
                cause = TransitionCause::HeadersOnlyFinality;
            }
        }
    }

    if context.config.mode == EngineMode::HeadersOnly {
        verified = vec![graph.finalized()];
    }
    let verified_best = verified.last().copied().unwrap_or(graph.finalized());
    let retention = crate::retention::enforce_retention(
        &mut graph,
        header_best,
        verified_best,
        context.retention_references.iter().copied(),
        context.config.limits,
    )?;
    if retention.admission_refused {
        return Err(TransitionFailure::ResourceStalled);
    }
    header_best = graph.select_header_best()?.0;
    let selected = path(&graph, header_best)?;
    let verified = trim_projection(&graph, verified)?;
    let mut plan = derive_plan(
        before,
        metadata,
        graph,
        old_nodes,
        old_selected,
        old_verified,
        selected,
        verified,
        aux_changes,
        finality_append,
        retention,
        request.event.idempotency_key(),
        cause,
        invariant_pins(context),
        context.config.limits,
    )?;
    plan.change_set.aux_changes.retain(|change| match change {
        crate::AuxDelta::Put(delivery) => plan.projected.node(delivery.header_hash).is_some(),
        crate::AuxDelta::Delete { .. } => true,
    });
    for hash in &plan.change_set.delete_nodes {
        for delivery in engine.aux_deliveries(*hash) {
            plan.change_set.aux_changes.push(crate::AuxDelta::Delete {
                header_hash: *hash,
                delivery_id: delivery.delivery_id,
            });
        }
    }
    super::verify_plan(engine, &plan)?;
    Ok(plan)
}

fn migrated_pin_refuted(
    durable: &DurableTransitionFacts,
    event: &TransitionEvent,
) -> Result<Option<Frontier>, TransitionFailure> {
    let TransitionEvent::MigratedPinRefutation(event) = event else {
        return Ok(None);
    };
    match durable {
        DurableTransitionFacts::MigratedFinalityPin(preserved) => {
            Ok((*preserved == Some(event.pin)).then_some(event.pin))
        }
        _ => Err(StoreError::Unavailable("migrated finality fact was not supplied").into()),
    }
}

fn select_fully_verified_path(graph: &MemHeaderStore) -> Result<Vec<Frontier>, TransitionFailure> {
    let finalized = graph.finalized();
    let mut connected = HashSet::from([finalized.hash]);
    let mut nodes: Vec<_> = graph.nodes().collect();
    nodes.sort_unstable_by_key(|node| (node.height, node.hash.0));
    for node in nodes {
        if node.hash != finalized.hash
            && node.is_eligible()
            && matches!(node.body, BodyValidationState::Verified { .. })
            && connected.contains(&node.parent_hash)
        {
            connected.insert(node.hash);
        }
    }
    let tip = connected
        .into_iter()
        .map(|hash| {
            let node = graph
                .node(hash)
                .expect("verified candidates are retained graph nodes");
            graph
                .score(hash)
                .map(|score| (score, Frontier::new(node.height, hash)))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max_by_key(|(score, _)| *score)
        .map(|(_, frontier)| frontier)
        .ok_or(GraphError::UnknownNode(finalized.hash))?;
    path(graph, tip)
}

fn validate_snapshot(
    snapshot: &EngineSnapshot,
    metadata: &EngineMetadata,
    context: &TransitionContext<'_>,
) -> Result<(), TransitionFailure> {
    if snapshot.mode != context.config.mode
        || metadata.mode != context.config.mode
        || metadata.network_id != context.config.network.kind()
        || metadata.anchor_manifest_digest != context.config.trust_anchor_digest()
        || snapshot.state_version != metadata.state_version
        || snapshot.frontiers != metadata.frontiers
    {
        return Err(TransitionFailure::ConfigurationMismatch);
    }
    Ok(())
}

fn validate_owner(owner: WorkOwner, before: &EngineSnapshot) -> Result<(), TransitionFailure> {
    if owner.header_generation != before.header_generation
        || owner
            .verified_generation
            .is_some_and(|generation| generation != before.verified_generation)
        || owner.branch.anchor_hash != before.frontiers.finalized.hash
    {
        return Err(TransitionFailure::Stale {
            current: before.state_version,
        });
    }
    Ok(())
}

fn validate_authority(
    event: &TransitionEvent,
    context: &TransitionContext<'_>,
) -> Result<(), TransitionFailure> {
    match event.admission() {
        EventAdmission::AnyMode => Ok(()),
        EventAdmission::IntegratedFullState if context.config.mode != EngineMode::Integrated => {
            Err(TransitionFailure::Mode)
        }
        EventAdmission::IntegratedFullState => {
            let evidence = event
                .idempotency_key()
                .ok_or(TransitionFailure::Authority)?;
            if context
                .full_state_authority
                .is_some_and(|authority| authority.authorizes(evidence))
            {
                Ok(())
            } else {
                Err(TransitionFailure::Authority)
            }
        }
    }
}

struct ApplyEventContext<'a> {
    engine: &'a HeaderChainEngine,
    durable: &'a DurableTransitionFacts,
    transition: &'a TransitionContext<'a>,
    before: &'a EngineSnapshot,
    old_selected: &'a [Frontier],
    migrated_pin_refuted: Option<Frontier>,
}

fn retained_header_context(
    graph: &MemHeaderStore,
    parent: Frontier,
    durable: &DurableTransitionFacts,
) -> Result<
    Vec<(
        zakura_chain::work::difficulty::CompactDifficulty,
        chrono::DateTime<chrono::Utc>,
    )>,
    TransitionFailure,
> {
    let required = usize::try_from(parent.height.0)
        .map_err(|_| StoreError::Unavailable("retained parent height does not fit in memory"))?
        .checked_add(1)
        .ok_or(StoreError::Unavailable(
            "retained parent context length overflowed",
        ))?
        .min(crate::POW_ADJUSTMENT_BLOCK_SPAN);
    let mut context = Vec::with_capacity(required);
    let mut hash = parent.hash;
    while context.len() < required {
        let Some(node) = graph.node(hash) else {
            let DurableTransitionFacts::ValidationContext(lease) = durable else {
                return Err(
                    StoreError::Unavailable("retained predecessor context is incomplete").into(),
                );
            };
            if lease.parent() != parent || lease.predecessors().len() != required {
                #[cfg(test)]
                if !context.is_empty() {
                    return Ok(context);
                }
                return Err(
                    StoreError::Unavailable("durable predecessor context is incoherent").into(),
                );
            }
            return Ok(lease
                .predecessors()
                .iter()
                .map(|fact| (fact.difficulty_threshold, fact.time))
                .collect());
        };
        context.push((node.header.difficulty_threshold, node.header.time));
        hash = node.parent_hash;
    }
    Ok(context)
}

fn apply_event(
    graph: &mut MemHeaderStore,
    verified: &mut Vec<Frontier>,
    aux_changes: &mut Vec<crate::AuxDelta>,
    operator_reason_changed: &mut bool,
    event: &TransitionEvent,
    event_context: &ApplyEventContext<'_>,
) -> Result<(), TransitionFailure> {
    let engine = event_context.engine;
    let durable = event_context.durable;
    let context = event_context.transition;
    match event {
        TransitionEvent::InsertHeaders(event) => {
            let receipt = event.batch.receipt();
            if receipt.parent().hash != event.parent_hash
                || receipt.trust_anchor_digest() != context.config.trust_anchor_digest()
            {
                return Err(TransitionFailure::StalePreparation);
            }
            let parent_node = graph
                .node(event.parent_hash)
                .ok_or(GraphError::UnknownParent {
                    header: event.target_tip_hash,
                    parent: event.parent_hash,
                })?;
            let parent_frontier = Frontier::new(parent_node.height, parent_node.hash);
            if receipt.parent() != parent_frontier {
                return Err(TransitionFailure::StalePreparation);
            }
            let common_ancestor = match event.completion {
                TargetCompletion::TargetComplete { common_ancestor }
                | TargetCompletion::SelectedAuxiliaryRepair {
                    common_ancestor, ..
                } => common_ancestor,
            };
            if common_ancestor != parent_frontier {
                return Err(TransitionFailure::InvalidEvidence(
                    "target completion ancestor does not match the retained parent",
                ));
            }
            let mut contextual = retained_header_context(graph, parent_frontier, durable)?;
            let mut parent = parent_frontier;
            for prepared in event.batch.headers() {
                if prepared.header.previous_block_hash != parent.hash
                    || prepared.hash != prepared.header.hash()
                    || prepared.height
                        != parent
                            .height
                            .next()
                            .map_err(|_| GraphError::HeightOverflow {
                                parent: parent.hash,
                            })?
                    || prepared.block_work
                        != prepared.header.difficulty_threshold.to_work().ok_or(
                            TransitionFailure::InvalidEvidence("invalid prepared target"),
                        )?
                {
                    return Err(TransitionFailure::InvalidEvidence(
                        "prepared header batch is inconsistent",
                    ));
                }
                let adjustment = crate::AdjustedDifficulty::new_from_header_time(
                    prepared.header.time,
                    parent.height,
                    &context.config.network,
                    contextual.iter().copied(),
                );
                crate::validate_contextual_difficulty_and_time(
                    prepared.header.difficulty_threshold,
                    adjustment,
                )
                .map_err(|_| {
                    TransitionFailure::InvalidEvidence(
                        "prepared header failed retained contextual validation",
                    )
                })?;
                let validation = match prepared.validation {
                    HeaderValidationState::DeferredUntil(until) if until <= context.clock.now() => {
                        HeaderValidationState::Valid
                    }
                    state => state,
                };
                let reasons = anchor_reasons(context, prepared.height, prepared.hash);
                parent = match graph.insert(
                    prepared.header.clone(),
                    prepared.block_work,
                    validation,
                    reasons,
                    BodyValidationState::Unknown,
                )? {
                    crate::InsertResult::Inserted(frontier)
                    | crate::InsertResult::AlreadyPresent(frontier) => frontier,
                };
                contextual.insert(
                    0,
                    (prepared.header.difficulty_threshold, prepared.header.time),
                );
                contextual.truncate(crate::POW_ADJUSTMENT_BLOCK_SPAN);
            }
            if parent.hash != event.target_tip_hash {
                return Err(TransitionFailure::InvalidEvidence(
                    "target completion does not end at the pursued hash",
                ));
            }
            match event.completion {
                TargetCompletion::SelectedAuxiliaryRepair {
                    selected_target, ..
                } => {
                    if event.batch.headers().len() != 1
                        || event.aux.len() != 1
                        || event.aux[0].tree_aux.is_none()
                        || selected_target != parent
                        || event.owner.branch.target_tip_hash
                            != event_context.before.frontiers.header_best.hash
                        || event_context
                            .old_selected
                            .iter()
                            .find(|frontier| frontier.height == selected_target.height)
                            .map(|frontier| frontier.hash)
                            != Some(selected_target.hash)
                        || graph
                            .ancestor(event.owner.branch.target_tip_hash, selected_target.height)?
                            != Some(selected_target)
                    {
                        return Err(TransitionFailure::InvalidEvidence(
                            "auxiliary repair is not one exact selected header",
                        ));
                    }
                }
                TargetCompletion::TargetComplete { .. } => {
                    if event.owner.branch.target_tip_hash != event.target_tip_hash {
                        return Err(TransitionFailure::InvalidEvidence(
                            "target completion does not end at the pursued hash",
                        ));
                    }
                }
            }
            let batch_headers: HashMap<_, _> = event
                .batch
                .headers()
                .iter()
                .map(|header| (header.hash, header.height))
                .collect();
            let mut delivery_ids = HashSet::new();
            for delivery in &event.aux {
                let expected_height = batch_headers.get(&delivery.header_hash).copied();
                if !delivery_ids.insert(delivery.delivery_id)
                    || delivery.owner != event.owner
                    || delivery.source != event.source
                    || delivery.authentication != crate::AuxAuthentication::Unauthenticated
                    || expected_height.is_none()
                    || delivery
                        .tree_aux
                        .is_some_and(|aux| Some(aux.height) != expected_height)
                {
                    return Err(TransitionFailure::InvalidEvidence(
                        "auxiliary delivery does not match the admitted target",
                    ));
                }
                let indexed_count = graph
                    .node(delivery.header_hash)
                    .expect("the auxiliary header was checked above")
                    .aux_delivery_ids
                    .iter()
                    .filter(|delivery_id| **delivery_id == delivery.delivery_id)
                    .count();
                let stored = engine
                    .aux_deliveries(delivery.header_hash)
                    .iter()
                    .copied()
                    .find(|stored| stored.delivery_id == delivery.delivery_id);
                match (stored, indexed_count) {
                    (Some(stored), 1) if stored == *delivery => continue,
                    (None, 0) => {}
                    _ => {
                        return Err(TransitionFailure::InvalidEvidence(
                            "auxiliary delivery replay changes provenance or indexing",
                        ));
                    }
                }
                graph
                    .node_mut(delivery.header_hash)?
                    .aux_delivery_ids
                    .push(delivery.delivery_id);
                aux_changes.push(crate::AuxDelta::Put(Box::new(*delivery)));
            }
        }
        TransitionEvent::VerifiedChainChanged(event) => {
            if verified.last().copied() != Some(event.old_tip) {
                return Err(TransitionFailure::StalePreparation);
            }
            let mut parent = match event.cause {
                crate::VerifiedChangeCause::Grow => event.old_tip,
                crate::VerifiedChangeCause::Reset => graph.finalized(),
            };
            if matches!(event.cause, crate::VerifiedChangeCause::Reset) {
                verified.clear();
                verified.push(parent);
            }
            for header in &event.new_path {
                if header.header.hash() != header.hash
                    || header.header.previous_block_hash != parent.hash
                    || header.height
                        != parent
                            .height
                            .next()
                            .map_err(|_| GraphError::HeightOverflow {
                                parent: parent.hash,
                            })?
                {
                    return Err(TransitionFailure::InvalidEvidence(
                        "verified path is not continuous",
                    ));
                }
                if graph.node(header.hash).is_none() {
                    let work = header.header.difficulty_threshold.to_work().ok_or(
                        TransitionFailure::InvalidEvidence("invalid verified target"),
                    )?;
                    graph.insert(
                        header.header.clone(),
                        work,
                        HeaderValidationState::Valid,
                        anchor_reasons(context, header.height, header.hash),
                        BodyValidationState::Verified {
                            evidence: event.full_state_transition_id,
                        },
                    )?;
                } else {
                    graph.set_body_state(
                        header.hash,
                        BodyValidationState::Verified {
                            evidence: event.full_state_transition_id,
                        },
                    )?;
                }
                parent = Frontier::new(header.height, header.hash);
                verified.push(parent);
            }
        }
        TransitionEvent::VerifiedBlockAccepted(event) => {
            if event.path.is_empty() {
                return Err(TransitionFailure::InvalidEvidence(
                    "accepted full-state side path is empty",
                ));
            }
            let mut parent = graph.finalized();
            let last_index = event.path.len().saturating_sub(1);
            for (index, header) in event.path.iter().enumerate() {
                if header.header.hash() != header.hash
                    || header.header.previous_block_hash != parent.hash
                    || header.height
                        != parent
                            .height
                            .next()
                            .map_err(|_| GraphError::HeightOverflow {
                                parent: parent.hash,
                            })?
                {
                    return Err(TransitionFailure::InvalidEvidence(
                        "accepted full-state side path is not continuous",
                    ));
                }
                if graph.node(header.hash).is_none() {
                    let work = header.header.difficulty_threshold.to_work().ok_or(
                        TransitionFailure::InvalidEvidence("invalid accepted full-state header"),
                    )?;
                    graph.insert(
                        header.header.clone(),
                        work,
                        HeaderValidationState::Valid,
                        anchor_reasons(context, header.height, header.hash),
                        BodyValidationState::Verified {
                            evidence: event.full_state_transition_id,
                        },
                    )?;
                } else if index == last_index {
                    graph.set_body_state(
                        header.hash,
                        BodyValidationState::Verified {
                            evidence: event.full_state_transition_id,
                        },
                    )?;
                }
                parent = Frontier::new(header.height, header.hash);
            }
        }
        TransitionEvent::BodyEvidence(BodyEvidence::PayloadMismatch(_)) => {}
        TransitionEvent::BodyEvidence(BodyEvidence::Transient(event)) => {
            if event.availability.attempts == 0
                || event.availability.suppliers == 0
                || event.availability.started_at > event.availability.next_probe_at
            {
                return Err(TransitionFailure::InvalidEvidence(
                    "body retry evidence has an invalid episode summary",
                ));
            }
            if matches!(
                graph.node(event.hash).map(|node| &node.body),
                Some(BodyValidationState::Verified { .. })
            ) {
                return Err(TransitionFailure::InvalidEvidence(
                    "body retry evidence cannot regress an already verified body",
                ));
            }
            graph.set_body_state(
                event.hash,
                BodyValidationState::Unavailable(event.availability),
            )?;
        }
        TransitionEvent::BodySupplierDiscovered(event) => {
            if event.hash != graph.select_header_best()?.0.hash
                || event.availability.attempts != 0
                || event.availability.suppliers == 0
                || event.availability.alarmed
                || event.availability.started_at != event.availability.next_probe_at
            {
                return Err(TransitionFailure::InvalidEvidence(
                    "body supplier discovery has an invalid fresh episode",
                ));
            }
            let old = match graph.node(event.hash).map(|node| &node.body) {
                Some(BodyValidationState::Unavailable(summary)) if summary.alarmed => *summary,
                _ => {
                    return Err(TransitionFailure::InvalidEvidence(
                        "body supplier discovery requires the selected persistent alarm",
                    ));
                }
            };
            let has_new_supplier = event.availability.suppliers > old.suppliers
                || (event.availability.suppliers == old.suppliers
                    && event.availability.supplier_set_digest != old.supplier_set_digest);
            if !has_new_supplier {
                return Err(TransitionFailure::InvalidEvidence(
                    "body supplier discovery does not add an eligible supplier",
                ));
            }
            graph.set_body_state(
                event.hash,
                BodyValidationState::Unavailable(event.availability),
            )?;
        }
        TransitionEvent::OperatorBodyRetry(event) => {
            if event.hash != graph.select_header_best()?.0.hash
                || event.availability.attempts != 0
                || event.availability.suppliers == 0
                || event.availability.alarmed
                || event.availability.started_at != event.availability.next_probe_at
            {
                return Err(TransitionFailure::InvalidEvidence(
                    "operator body retry has an invalid fresh episode",
                ));
            }
            if !matches!(
                graph.node(event.hash).map(|node| &node.body),
                Some(BodyValidationState::Unavailable(summary)) if summary.alarmed
            ) {
                return Err(TransitionFailure::InvalidEvidence(
                    "operator body retry requires the selected persistent alarm",
                ));
            }
            graph.set_body_state(
                event.hash,
                BodyValidationState::Unavailable(event.availability),
            )?;
        }
        TransitionEvent::BodyEvidence(BodyEvidence::ConsensusInvalid(event)) => {
            if matches!(
                graph.node(event.hash).map(|node| &node.body),
                Some(BodyValidationState::Verified { .. })
            ) {
                return Err(TransitionFailure::InvalidEvidence(
                    "body invalid evidence cannot contradict an already verified body",
                ));
            }
            graph.set_consensus_body_invalid(event.hash, event.evidence, event.rule.clone())?;
        }
        TransitionEvent::BodyEvidence(BodyEvidence::Verified(event)) => {
            graph.set_body_state(
                event.hash,
                BodyValidationState::Verified {
                    evidence: event.evidence,
                },
            )?;
        }
        TransitionEvent::OperatorInvalidate(event) => {
            *operator_reason_changed = graph.add_reason(
                event.target,
                EligibilityReason::OperatorInvalid { id: event.id },
            )?;
        }
        TransitionEvent::OperatorReconsider(event) => {
            *operator_reason_changed =
                graph.remove_operator_invalidation(event.target, event.id)?;
        }
        TransitionEvent::FullStateFinalized(event) => {
            let expected: Vec<_> = verified
                .iter()
                .take_while(|frontier| frontier.height <= event.new_finalized.height)
                .map(|frontier| frontier.hash)
                .collect();
            if event.verified_path_proof != expected {
                return Err(TransitionFailure::InvalidEvidence(
                    "finality proof is not the exact verified ancestry",
                ));
            }
        }
        TransitionEvent::MigratedPinRefutation(event) => {
            if event.invalid_header.height > event.pin.height
                || event_context.migrated_pin_refuted != Some(event.pin)
            {
                return Err(TransitionFailure::InvalidEvidence(
                    "full-state refutation does not name an imported pin ancestor",
                ));
            }
        }
        TransitionEvent::AuxEvidence(event) => {
            if event.deliveries.is_empty() || event.deliveries.len() > 2 {
                return Err(TransitionFailure::InvalidEvidence(
                    "auxiliary evidence must name one or two exact deliveries",
                ));
            }

            for (index, event_delivery) in event.deliveries.iter().enumerate() {
                if event.deliveries[..index].iter().any(|prior| {
                    prior.header_hash == event_delivery.header_hash
                        && prior.delivery_id == event_delivery.delivery_id
                }) {
                    return Err(TransitionFailure::InvalidEvidence(
                        "auxiliary evidence names the same delivery more than once",
                    ));
                }
                let header = graph.node(event_delivery.header_hash).ok_or(
                    TransitionFailure::InvalidEvidence(
                        "auxiliary evidence references an unknown header",
                    ),
                )?;
                let header_frontier = Frontier::new(header.height, header.hash);
                if graph.ancestor(event.owner.branch.target_tip_hash, header.height)?
                    != Some(header_frontier)
                {
                    return Err(TransitionFailure::InvalidEvidence(
                        "auxiliary evidence is outside its owned branch",
                    ));
                }
                let existing = engine
                    .aux_deliveries(event_delivery.header_hash)
                    .iter()
                    .copied()
                    .find(|delivery| delivery.delivery_id == event_delivery.delivery_id)
                    .ok_or(TransitionFailure::InvalidEvidence(
                        "auxiliary evidence references an unknown delivery",
                    ))?;
                if existing != *event_delivery
                    || !header.aux_delivery_ids.contains(&existing.delivery_id)
                {
                    return Err(TransitionFailure::InvalidEvidence(
                        "auxiliary evidence changes delivery provenance",
                    ));
                }
                if existing.authentication == event.authentication {
                    continue;
                }
                if existing.authentication != crate::AuxAuthentication::Unauthenticated {
                    return Err(TransitionFailure::InvalidEvidence(
                        "an authenticated or rejected auxiliary delivery is immutable",
                    ));
                }
                if let crate::AuxAuthentication::Authenticated { boundary_hash, .. } =
                    event.authentication
                {
                    let boundary =
                        graph
                            .node(boundary_hash)
                            .ok_or(TransitionFailure::InvalidEvidence(
                                "auxiliary authentication boundary is unknown",
                            ))?;
                    let expected_height = header.height.next().map_err(|_| {
                        TransitionFailure::InvalidEvidence(
                            "auxiliary authentication boundary height overflowed",
                        )
                    })?;
                    let boundary_frontier = Frontier::new(boundary.height, boundary.hash);
                    if boundary.height != expected_height
                        || boundary.parent_hash != header.hash
                        || graph.ancestor(event.owner.branch.target_tip_hash, boundary.height)?
                            != Some(boundary_frontier)
                    {
                        return Err(TransitionFailure::InvalidEvidence(
                            "auxiliary authentication is not the owned one-header-later boundary",
                        ));
                    }
                } else if event.authentication == crate::AuxAuthentication::Unauthenticated {
                    return Err(TransitionFailure::InvalidEvidence(
                        "auxiliary evidence cannot remove authentication",
                    ));
                }
                let mut delivery = existing;
                delivery.authentication = event.authentication;
                aux_changes.push(crate::AuxDelta::Put(Box::new(delivery)));
            }
            if event.authentication == crate::AuxAuthentication::Unauthenticated {
                return Err(TransitionFailure::InvalidEvidence(
                    "auxiliary evidence cannot remove authentication",
                ));
            }
        }
        TransitionEvent::ReevaluateDeferred => {
            let due: Vec<_> = graph
                .nodes()
                .filter_map(|node| match node.validation {
                    HeaderValidationState::DeferredUntil(until) if until <= context.clock.now() => {
                        Some(node.hash)
                    }
                    _ => None,
                })
                .collect();
            for hash in due {
                graph.set_validation(hash, HeaderValidationState::Valid)?;
            }
        }
    }
    Ok(())
}

fn anchor_reasons(
    context: &TransitionContext<'_>,
    height: block::Height,
    hash: block::Hash,
) -> Vec<EligibilityReason> {
    let mut reasons = Vec::new();
    if let Some(pin) = context
        .config
        .settled_manifest
        .pin_for_network(&context.config.network)
    {
        if pin.activation.height == height && pin.activation.hash != hash {
            reasons.push(EligibilityReason::SettledUpgradeConflict {
                height,
                expected: pin.activation.hash,
            });
        }
    }
    if let Some(expected) = context.config.local_checkpoints.hash(height) {
        if expected != hash {
            reasons.push(EligibilityReason::CheckpointConflict { height, expected });
        }
    }
    reasons
}

#[allow(clippy::too_many_arguments)]
fn derive_plan(
    before: EngineSnapshot,
    mut metadata: EngineMetadata,
    graph: MemHeaderStore,
    old_nodes: HashMap<block::Hash, HeaderNode>,
    old_selected: Vec<Frontier>,
    old_verified: Vec<Frontier>,
    selected: Vec<Frontier>,
    verified: Vec<Frontier>,
    aux_changes: Vec<crate::AuxDelta>,
    finality_append: Option<FinalityRecord>,
    retention: RetentionPlan,
    event_id: Option<EvidenceId>,
    cause: TransitionCause,
    trust_pins: Vec<Frontier>,
    limits: EngineLimits,
) -> Result<TransitionPlan, TransitionFailure> {
    let new_nodes = node_map(&graph);
    let mut put_nodes: Vec<_> = new_nodes
        .values()
        .filter(|node| old_nodes.get(&node.hash) != Some(*node))
        .cloned()
        .collect();
    put_nodes.sort_unstable_by_key(|node| (node.height, node.hash.0));
    let mut delete_nodes: Vec<_> = old_nodes
        .keys()
        .filter(|hash| !new_nodes.contains_key(hash))
        .copied()
        .collect();
    delete_nodes.sort_unstable_by_key(|hash| hash.0);
    let mut eligibility_changes: Vec<_> = new_nodes
        .values()
        .filter_map(|node| {
            let old = old_nodes.get(&node.hash)?;
            (old.eligibility != node.eligibility).then(|| EligibilityDelta {
                hash: node.hash,
                before: old.eligibility.clone(),
                after: node.eligibility.clone(),
            })
        })
        .collect();
    eligibility_changes.sort_unstable_by_key(|delta| delta.hash.0);
    let selected_changed = selected != old_selected;
    let verified_changed = verified != old_verified;
    let header_topology_changed = !delete_nodes.is_empty()
        || put_nodes
            .iter()
            .any(|node| !old_nodes.contains_key(&node.hash));
    let header_validation_changed = new_nodes.values().any(|node| {
        old_nodes
            .get(&node.hash)
            .is_some_and(|old| old.validation != node.validation)
    });
    let header_best = *selected.last().ok_or(TransitionFailure::InvalidEvidence(
        "selected projection is empty",
    ))?;
    metadata.alarms.resource_stalled = retention.resource_stalled;
    let header_best_node = graph
        .node(header_best.hash)
        .ok_or(GraphError::UnknownNode(header_best.hash))?;
    metadata.alarms.header_best_body_unavailable = match &header_best_node.body {
        BodyValidationState::Unavailable(summary) if summary.alarmed => Some(*summary),
        _ => None,
    };
    let alarm_changed = metadata.alarms != before.alarms;
    let changed = !put_nodes.is_empty()
        || !delete_nodes.is_empty()
        || !aux_changes.is_empty()
        || finality_append.is_some()
        || selected_changed
        || verified_changed
        || alarm_changed;
    if changed {
        metadata.state_version = metadata.state_version.checked_next()?;
        if selected_changed
            || header_topology_changed
            || header_validation_changed
            || !eligibility_changes.is_empty()
            || finality_append.is_some()
        {
            metadata.header_generation = metadata.header_generation.checked_next()?;
        }
        if verified_changed || finality_append.is_some() {
            metadata.verified_generation = metadata.verified_generation.checked_next()?;
        }
        if let Some(record) = finality_append {
            metadata.finality_epoch = record.epoch;
        }
        if let Some(event_id) = event_id {
            metadata.last_transition_id = event_id;
        }
    }
    let verified_best = *verified.last().ok_or(TransitionFailure::InvalidEvidence(
        "verified projection is empty",
    ))?;
    metadata.frontiers = FrontierSet {
        finalized: graph.finalized(),
        header_best,
        verified_best,
    };
    metadata.header_best_score = graph.score(header_best.hash)?;
    metadata.oldest_retained_height = graph
        .nodes()
        .map(|node| node.height)
        .min()
        .unwrap_or(graph.finalized().height);
    let inserted = put_nodes
        .iter()
        .filter(|node| !old_nodes.contains_key(&node.hash))
        .map(|node| Frontier::new(node.height, node.hash))
        .collect();
    let change_set = ChangeSet {
        put_nodes,
        delete_nodes: delete_nodes.clone(),
        index_changes: IndexChanges {
            inserted,
            deleted: delete_nodes,
        },
        selected_projection: projection_delta(&old_selected, &selected),
        verified_projection: projection_delta(&old_verified, &verified),
        eligibility_changes,
        aux_changes,
        finality_append,
        metadata,
    };
    Ok(TransitionPlan {
        before,
        change_set,
        projected: graph,
        cause,
        trust_pins,
        limits,
    })
}

fn no_change(
    engine: &HeaderChainEngine,
    before: EngineSnapshot,
    metadata: EngineMetadata,
    event: TransitionEvent,
    context: &TransitionContext<'_>,
) -> Result<TransitionPlan, TransitionFailure> {
    validate_authority(&event, context)?;
    let graph = engine.graph().clone();
    Ok(TransitionPlan {
        before,
        change_set: ChangeSet {
            put_nodes: Vec::new(),
            delete_nodes: Vec::new(),
            index_changes: IndexChanges::default(),
            selected_projection: ProjectionDelta::default(),
            verified_projection: ProjectionDelta::default(),
            eligibility_changes: Vec::new(),
            aux_changes: Vec::new(),
            finality_append: None,
            metadata,
        },
        projected: graph,
        cause: TransitionCause::Event,
        trust_pins: invariant_pins(context),
        limits: context.config.limits,
    })
}

fn invariant_pins(context: &TransitionContext<'_>) -> Vec<Frontier> {
    let mut pins: Vec<_> = context.config.local_checkpoints.iter().collect();
    if let Some(pin) = context
        .config
        .settled_manifest
        .pin_for_network(&context.config.network)
    {
        pins.push(pin.activation);
    }
    pins.sort_unstable_by_key(|pin| (pin.height, pin.hash.0));
    pins
}

fn node_map(graph: &MemHeaderStore) -> HashMap<block::Hash, HeaderNode> {
    graph
        .nodes()
        .map(|node| (node.hash, node.clone()))
        .collect()
}

fn path(graph: &MemHeaderStore, tip: Frontier) -> Result<Vec<Frontier>, TransitionFailure> {
    let mut path = Vec::new();
    let mut current = tip;
    loop {
        path.push(current);
        if current == graph.finalized() {
            break;
        }
        let node = graph
            .node(current.hash)
            .ok_or(GraphError::UnknownNode(current.hash))?;
        current = Frontier::new(block::Height(current.height.0 - 1), node.parent_hash);
    }
    path.reverse();
    Ok(path)
}

fn trim_projection(
    graph: &MemHeaderStore,
    projection: Vec<Frontier>,
) -> Result<Vec<Frontier>, TransitionFailure> {
    let mut result: Vec<_> = projection
        .into_iter()
        .filter(|frontier| {
            frontier.height >= graph.finalized().height && graph.node(frontier.hash).is_some()
        })
        .collect();
    if result.first().copied() != Some(graph.finalized()) {
        result.insert(0, graph.finalized());
    }
    for pair in result.windows(2) {
        if pair[1].height.0 != pair[0].height.0 + 1
            || graph
                .node(pair[1].hash)
                .is_none_or(|node| node.parent_hash != pair[0].hash)
        {
            return Err(TransitionFailure::InvalidEvidence(
                "verified projection is not continuous",
            ));
        }
    }
    Ok(result)
}

fn projection_delta(old: &[Frontier], new: &[Frontier]) -> ProjectionDelta {
    let remove_before = new.first().and_then(|first| {
        old.first()
            .is_some_and(|old_first| old_first.height < first.height)
            .then_some(first.height)
    });
    let old_start = remove_before
        .map(|height| old.partition_point(|frontier| frontier.height < height))
        .unwrap_or(0);
    let comparable_old = &old[old_start..];
    let common = comparable_old
        .iter()
        .zip(new)
        .take_while(|(left, right)| left == right)
        .count();
    ProjectionDelta {
        remove_before,
        remove_from: comparable_old
            .get(common)
            .or_else(|| new.get(common))
            .map(|frontier| frontier.height),
        put: new[common..].to_vec(),
    }
}

#[cfg(test)]
mod tests;
