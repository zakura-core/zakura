//! The sole pure mutation algorithm for durable header-chain state.

use std::collections::{HashMap, HashSet, VecDeque};

use thiserror::Error;
use zakura_chain::block;

use crate::{
    BodyEvidence, BodyUnavailableSummary, BodyValidationState, ChangeSet, CounterExhausted,
    EligibilityDelta, EligibilityReason, EngineLimits, EngineMetadata, EngineMode, EngineSnapshot,
    EventAdmission, EvidenceId, FinalityRecord, FinalitySource, Frontier, FrontierSet, GraphError,
    HeaderNode, HeaderValidationState, IndexChanges, MemHeaderStore, ProjectionDelta,
    RetentionPlan, StateVersion, StoreError, StoreRead, TransitionCause, TransitionContext,
    TransitionEvent, TransitionRequest, WorkOwner,
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

    pub(crate) const fn projected(&self) -> &MemHeaderStore {
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

/// Derive one atomic transition without mutating `store`.
pub fn apply_transition<S: StoreRead>(
    store: &S,
    request: TransitionRequest,
    context: &TransitionContext<'_>,
) -> Result<TransitionPlan, TransitionFailure> {
    let before = store.snapshot()?;
    let metadata = store.metadata()?;
    validate_snapshot(&before, &metadata, context)?;
    if metadata.last_transition_id
        == request
            .event
            .idempotency_key()
            .unwrap_or(metadata.last_transition_id)
        && request.event.idempotency_key().is_some()
    {
        let plan = no_change(store, before, metadata, request.event, context)?;
        super::verify_plan(store, &plan)?;
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

    let mut graph = load_graph(store, before.frontiers.finalized)?;
    let old_nodes = node_map(&graph);
    let old_selected = projection(
        store,
        before.frontiers.finalized,
        before.frontiers.header_best,
        true,
    )?;
    let old_verified = projection(
        store,
        before.frontiers.finalized,
        before.frontiers.verified_best,
        false,
    )?;
    let mut verified = old_verified.clone();
    let mut aux_changes = Vec::new();
    let mut finality = None;

    apply_event(
        store,
        &mut graph,
        &mut verified,
        &mut aux_changes,
        &request.event,
        context,
    )?;
    graph.recompute_all_eligibility()?;
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
        std::iter::empty(),
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
    for hash in &plan.change_set.delete_nodes {
        for delivery in store.aux_deliveries(*hash)? {
            plan.change_set
                .aux_changes
                .push(crate::AuxDelta::Delete(delivery.delivery_id));
        }
    }
    super::verify_plan(store, &plan)?;
    Ok(plan)
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
    if owner.state_version != before.state_version
        || owner.header_generation != before.header_generation
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
        EventAdmission::StartupOnly if context.startup_capability.is_some() => Ok(()),
        EventAdmission::StartupOnly => Err(TransitionFailure::Authority),
    }
}

fn apply_event<S: StoreRead>(
    store: &S,
    graph: &mut MemHeaderStore,
    verified: &mut Vec<Frontier>,
    aux_changes: &mut Vec<crate::AuxDelta>,
    event: &TransitionEvent,
    context: &TransitionContext<'_>,
) -> Result<(), TransitionFailure> {
    match event {
        TransitionEvent::InsertHeaders(event) => {
            let lease = store.validation_context(event.parent_hash)?;
            if lease.context_digest != event.batch.lease_digest()
                || lease.parent.hash != event.parent_hash
            {
                return Err(TransitionFailure::StalePreparation);
            }
            let mut parent = lease.parent;
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
            }
            if parent.hash != event.target_tip_hash
                || event.owner.branch.target_tip_hash != event.target_tip_hash
            {
                return Err(TransitionFailure::InvalidEvidence(
                    "target completion does not end at the pursued hash",
                ));
            }
            for delivery in &event.aux {
                if delivery.owner != event.owner || graph.node(delivery.header_hash).is_none() {
                    return Err(TransitionFailure::InvalidEvidence(
                        "auxiliary delivery does not match the admitted target",
                    ));
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
        TransitionEvent::BodyEvidence(BodyEvidence::PayloadMismatch(_)) => {}
        TransitionEvent::BodyEvidence(BodyEvidence::Transient(event)) => {
            let previous = graph
                .node(event.hash)
                .ok_or(GraphError::UnknownNode(event.hash))?
                .body;
            let summary = match previous {
                BodyValidationState::Unavailable(summary) => BodyUnavailableSummary {
                    attempts: summary.attempts.saturating_add(1),
                    ..summary
                },
                _ => BodyUnavailableSummary {
                    attempts: 1,
                    ..BodyUnavailableSummary::default()
                },
            };
            graph.set_body_state(event.hash, BodyValidationState::Unavailable(summary))?;
        }
        TransitionEvent::BodyEvidence(BodyEvidence::ConsensusInvalid(event)) => {
            graph.set_consensus_body_invalid(event.hash, event.evidence, event.rule)?;
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
            graph.add_reason(
                event.target,
                EligibilityReason::OperatorInvalid { id: event.id },
            )?;
        }
        TransitionEvent::OperatorReconsider(event) => {
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
        TransitionEvent::AdvanceLocalCheckpoint(event) => {
            if event.authenticated_config_digest != context.config.trust_anchor_digest()
                || context
                    .config
                    .local_checkpoints
                    .hash(event.checkpoint.height)
                    != Some(event.checkpoint.hash)
            {
                return Err(TransitionFailure::InvalidEvidence(
                    "local checkpoint is not authenticated configuration",
                ));
            }
            for hash in graph.hashes_at_height(event.checkpoint.height) {
                if hash != event.checkpoint.hash {
                    graph.add_reason(
                        hash,
                        EligibilityReason::CheckpointConflict {
                            height: event.checkpoint.height,
                            expected: event.checkpoint.hash,
                        },
                    )?;
                }
            }
        }
        TransitionEvent::AuxEvidence(event) => {
            if graph.node(event.delivery.header_hash).is_none() {
                return Err(TransitionFailure::InvalidEvidence(
                    "auxiliary evidence references an unknown header",
                ));
            }
            let mut delivery = event.delivery;
            delivery.authentication = event.authentication;
            let node = graph.node_mut(delivery.header_hash)?;
            if !node.aux_delivery_ids.contains(&delivery.delivery_id) {
                node.aux_delivery_ids.push(delivery.delivery_id);
            }
            aux_changes.push(crate::AuxDelta::Put(Box::new(delivery)));
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
        TransitionEvent::Recover(_) => {}
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
    let changed = !put_nodes.is_empty()
        || !delete_nodes.is_empty()
        || !aux_changes.is_empty()
        || finality_append.is_some()
        || selected_changed
        || verified_changed;
    if changed {
        metadata.state_version = metadata.state_version.checked_next()?;
        if selected_changed || !eligibility_changes.is_empty() || finality_append.is_some() {
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
    let header_best = *selected.last().ok_or(TransitionFailure::InvalidEvidence(
        "selected projection is empty",
    ))?;
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
    metadata.alarms.resource_stalled = retention.resource_stalled;
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

fn no_change<S: StoreRead>(
    store: &S,
    before: EngineSnapshot,
    metadata: EngineMetadata,
    event: TransitionEvent,
    context: &TransitionContext<'_>,
) -> Result<TransitionPlan, TransitionFailure> {
    validate_authority(&event, context)?;
    let graph = load_graph(store, before.frontiers.finalized)?;
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

fn load_graph<S: StoreRead>(
    store: &S,
    finalized: Frontier,
) -> Result<MemHeaderStore, TransitionFailure> {
    let mut pending = VecDeque::from([finalized.hash]);
    let mut seen = HashSet::new();
    let mut nodes = Vec::new();
    while let Some(hash) = pending.pop_front() {
        if !seen.insert(hash) {
            continue;
        }
        let node = store.node(hash)?.ok_or(StoreError::Incoherent(
            "an indexed retained node is missing",
        ))?;
        if node.hash != hash || node.header.hash() != hash {
            return Err(TransitionFailure::InvalidEvidence(
                "durable node hash mismatch",
            ));
        }
        pending.extend(store.children(hash)?);
        nodes.push(node);
    }
    Ok(MemHeaderStore::from_nodes(finalized, nodes)?)
}

fn node_map(graph: &MemHeaderStore) -> HashMap<block::Hash, HeaderNode> {
    graph
        .nodes()
        .map(|node| (node.hash, node.clone()))
        .collect()
}

fn projection<S: StoreRead>(
    store: &S,
    finalized: Frontier,
    tip: Frontier,
    selected: bool,
) -> Result<Vec<Frontier>, TransitionFailure> {
    let mut result = Vec::new();
    for raw_height in finalized.height.0..=tip.height.0 {
        let height = block::Height(raw_height);
        let hash = if selected {
            store.selected_hash(height)?
        } else {
            store.verified_hash(height)?
        }
        .ok_or(StoreError::Incoherent("frontier projection has a gap"))?;
        result.push(Frontier::new(height, hash));
    }
    Ok(result)
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
    let common = old
        .iter()
        .zip(new)
        .take_while(|(left, right)| left == right)
        .count();
    if common == old.len() && common == new.len() {
        return ProjectionDelta::default();
    }
    ProjectionDelta {
        remove_from: old
            .get(common)
            .or_else(|| new.get(common))
            .map(|frontier| frontier.height),
        put: new[common..].to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU64, sync::Arc};

    use chrono::{DateTime, Utc};
    use zakura_chain::{
        block::genesis::regtest_genesis_block,
        parameters::{testnet::RegtestParameters, Network},
    };

    use super::*;
    use crate::{
        verify_plan, AlarmSet, BranchId, CheckpointSet, EngineConfig, FinalityEpoch,
        HeaderChainDiskVersion, HeaderContextFact, HeaderGeneration, PreparedHeader,
        PreparedHeaderBatch, SourceId, TargetCompletion, TrustedAnchor, ValidationLease,
        VerifiedGeneration,
    };

    #[derive(Clone)]
    struct TestStore {
        graph: MemHeaderStore,
        metadata: EngineMetadata,
        selected: Vec<Frontier>,
        verified: Vec<Frontier>,
        lease: ValidationLease,
        finality: Vec<FinalityRecord>,
    }

    impl TestStore {
        fn new(mode: EngineMode) -> (Self, EngineConfig) {
            let block = regtest_genesis_block();
            let frontier = Frontier::new(block::Height(0), block.hash());
            let work = block
                .header
                .difficulty_threshold
                .to_work()
                .expect("the regtest genesis target has valid work");
            let graph = MemHeaderStore::new(frontier, block.header.clone(), work, work.as_u256())
                .expect("the fixture anchor header matches its hash");
            let config = EngineConfig::new(
                mode,
                Network::new_regtest(RegtestParameters::default()),
                TrustedAnchor {
                    frontier,
                    header: block.header.clone(),
                },
                CheckpointSet::default(),
            )
            .expect("the fixture configuration is coherent");
            let score = graph.score(frontier.hash).expect("the anchor is retained");
            let metadata = EngineMetadata {
                disk_format: HeaderChainDiskVersion(1),
                mode,
                network_id: config.network.kind(),
                anchor_manifest_digest: config.trust_anchor_digest(),
                work_origin: frontier,
                state_version: StateVersion::new(0),
                header_generation: HeaderGeneration::new(0),
                verified_generation: VerifiedGeneration::new(0),
                finality_epoch: FinalityEpoch::new(0),
                frontiers: FrontierSet {
                    finalized: frontier,
                    header_best: frontier,
                    verified_best: frontier,
                },
                header_best_score: score,
                oldest_retained_height: frontier.height,
                alarms: AlarmSet::default(),
                last_transition_id: EvidenceId::from_digest([0xff; 32]),
            };
            let lease = ValidationLease {
                parent: frontier,
                predecessors: vec![HeaderContextFact {
                    frontier,
                    difficulty_threshold: block.header.difficulty_threshold,
                    time: block.header.time,
                }],
                trust_anchor_digest: config.trust_anchor_digest(),
                context_digest: [7; 32],
            };
            (
                Self {
                    graph,
                    metadata,
                    selected: vec![frontier],
                    verified: vec![frontier],
                    lease,
                    finality: Vec::new(),
                },
                config,
            )
        }

        fn snapshot(&self) -> EngineSnapshot {
            EngineSnapshot {
                mode: self.metadata.mode,
                state_version: self.metadata.state_version,
                header_generation: self.metadata.header_generation,
                verified_generation: self.metadata.verified_generation,
                frontiers: self.metadata.frontiers,
                header_best_score: self.metadata.header_best_score,
                oldest_retained_height: self.metadata.oldest_retained_height,
                alarms: self.metadata.alarms.clone(),
            }
        }

        fn commit(&mut self, plan: &TransitionPlan) {
            self.graph = plan.projected.clone();
            self.metadata = plan.change_set.metadata.clone();
            apply_projection(&mut self.selected, &plan.change_set.selected_projection);
            apply_projection(&mut self.verified, &plan.change_set.verified_projection);
            if let Some(record) = plan.change_set.finality_append {
                self.finality.push(record);
            }
            self.lease.parent = self.metadata.frontiers.header_best;
            self.lease.context_digest[0] = self.lease.context_digest[0].wrapping_add(1);
        }
    }

    impl StoreRead for TestStore {
        fn snapshot(&self) -> Result<EngineSnapshot, StoreError> {
            Ok(self.snapshot())
        }
        fn metadata(&self) -> Result<EngineMetadata, StoreError> {
            Ok(self.metadata.clone())
        }
        fn node(&self, hash: block::Hash) -> Result<Option<HeaderNode>, StoreError> {
            Ok(self.graph.node(hash).cloned())
        }
        fn children(&self, parent: block::Hash) -> Result<Vec<block::Hash>, StoreError> {
            Ok(self.graph.children(parent))
        }
        fn hashes_at_height(&self, height: block::Height) -> Result<Vec<block::Hash>, StoreError> {
            Ok(self.graph.hashes_at_height(height))
        }
        fn selected_hash(&self, height: block::Height) -> Result<Option<block::Hash>, StoreError> {
            Ok(self
                .selected
                .iter()
                .find(|item| item.height == height)
                .map(|item| item.hash))
        }
        fn verified_hash(&self, height: block::Height) -> Result<Option<block::Hash>, StoreError> {
            Ok(self
                .verified
                .iter()
                .find(|item| item.height == height)
                .map(|item| item.hash))
        }
        fn candidate_tips(&self) -> Result<Vec<(crate::ChainScore, block::Hash)>, StoreError> {
            self.graph
                .eligible_tips()
                .into_iter()
                .map(|tip| {
                    Ok((
                        self.graph
                            .score(tip.hash)
                            .map_err(|_| StoreError::Incoherent("invalid score"))?,
                        tip.hash,
                    ))
                })
                .collect()
        }
        fn validation_context(&self, parent: block::Hash) -> Result<ValidationLease, StoreError> {
            if parent != self.lease.parent.hash {
                return Err(StoreError::Incoherent("unexpected fixture lease parent"));
            }
            Ok(self.lease.clone())
        }
        fn aux_deliveries(
            &self,
            _hash: block::Hash,
        ) -> Result<Vec<crate::AuxDelivery>, StoreError> {
            Ok(Vec::new())
        }
        fn finality_history(&self) -> Result<Vec<FinalityRecord>, StoreError> {
            Ok(self.finality.clone())
        }
    }

    struct ManualClock(DateTime<Utc>);
    impl super::super::Clock for ManualClock {
        fn now(&self) -> DateTime<Utc> {
            self.0
        }
    }

    struct Authority;
    impl super::super::FullStateEvidenceAuthority for Authority {
        fn authorizes(&self, _evidence: EvidenceId) -> bool {
            true
        }
    }

    fn context<'a>(
        config: &'a EngineConfig,
        clock: &'a ManualClock,
        authority: Option<&'a Authority>,
    ) -> TransitionContext<'a> {
        let full_state_authority = authority.map(|item| {
            // This trait-object coercion preserves the borrowed fixture's lifetime and identity.
            item as &dyn super::super::FullStateEvidenceAuthority
        });
        TransitionContext {
            config,
            clock,
            full_state_authority,
            startup_capability: None,
        }
    }

    fn batch(
        parent: Frontier,
        count: u32,
        lease: [u8; 32],
        evidence: EvidenceId,
    ) -> PreparedHeaderBatch {
        let mut headers = Vec::new();
        let mut parent_hash = parent.hash;
        for offset in 1..=count {
            let mut header = *regtest_genesis_block().header;
            header.previous_block_hash = parent_hash;
            let mut nonce = [0; 32];
            nonce[..4].copy_from_slice(&offset.to_le_bytes());
            header.nonce = nonce.into();
            let header = Arc::new(header);
            let hash = header.hash();
            headers.push(PreparedHeader {
                header: header.clone(),
                hash,
                height: block::Height(parent.height.0 + offset),
                block_work: header
                    .difficulty_threshold
                    .to_work()
                    .expect("the fixture target has valid work"),
                validation: HeaderValidationState::Valid,
            });
            parent_hash = hash;
        }
        PreparedHeaderBatch::new(headers, lease, evidence).expect("the fixture batch is nonempty")
    }

    fn insertion(store: &TestStore, count: u32, evidence: EvidenceId) -> TransitionRequest {
        let batch = batch(
            store.lease.parent,
            count,
            store.lease.context_digest,
            evidence,
        );
        let target = batch.headers().last().expect("the batch is nonempty").hash;
        TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::InsertHeaders(crate::InsertHeaders {
                owner: WorkOwner {
                    state_version: store.metadata.state_version,
                    header_generation: store.metadata.header_generation,
                    verified_generation: None,
                    branch: BranchId::new(store.metadata.frontiers.finalized.hash, target),
                    session_id: 1,
                    request_id: NonZeroU64::new(1).expect("one is nonzero"),
                },
                source: SourceId::from_digest([3; 32]),
                parent_hash: store.lease.parent.hash,
                target_tip_hash: target,
                completion: TargetCompletion::V7AtomicRange,
                batch,
                aux: Vec::new(),
            }),
        }
    }

    #[test]
    fn headers_only_finalizes_exactly_tip_minus_one_thousand_before_publication() {
        let (store, config) = TestStore::new(EngineMode::HeadersOnly);
        let clock = ManualClock(Utc::now());
        let request = insertion(&store, 1_001, EvidenceId::from_digest([1; 32]));
        let plan = apply_transition(&store, request, &context(&config, &clock, None))
            .expect("the complete target is admitted atomically");

        assert_eq!(
            plan.change_set.metadata.frontiers.finalized.height,
            block::Height(1)
        );
        assert_eq!(
            plan.change_set.metadata.frontiers.header_best.height,
            block::Height(1_001)
        );
        assert_eq!(
            plan.change_set.metadata.frontiers.verified_best.height,
            block::Height(1)
        );
        assert_eq!(
            plan.change_set.metadata.finality_epoch,
            FinalityEpoch::new(1)
        );
        assert!(matches!(
            plan.change_set.finality_append.expect("depth finality is recorded").source,
            FinalitySource::HeadersOnlyDepth { selected_tip } if selected_tip.height == block::Height(1_001)
        ));
        assert_eq!(plan.cause(), TransitionCause::HeadersOnlyFinality);
    }

    #[test]
    fn integrated_finality_requires_authority_and_exact_verified_path() {
        let (mut store, config) = TestStore::new(EngineMode::Integrated);
        let clock = ManualClock(Utc::now());
        let authority = Authority;
        let insert = insertion(&store, 3, EvidenceId::from_digest([2; 32]));
        let insert_plan = apply_transition(&store, insert, &context(&config, &clock, None))
            .expect("network insertion itself needs no full-state authority");
        store.commit(&insert_plan);
        let new_path: Vec<_> = path(&store.graph, store.metadata.frontiers.header_best)
            .expect("the selected fixture path is continuous")
            .into_iter()
            .skip(1)
            .map(|frontier| crate::VerifiedHeaderRef {
                height: frontier.height,
                hash: frontier.hash,
                header: store
                    .graph
                    .node(frontier.hash)
                    .expect("path nodes exist")
                    .header
                    .clone(),
            })
            .collect();
        let verified_id = EvidenceId::from_digest([4; 32]);
        let verified = TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::VerifiedChainChanged(crate::VerifiedChainChanged {
                full_state_transition_id: verified_id,
                old_tip: store.metadata.frontiers.verified_best,
                new_path,
                cause: crate::VerifiedChangeCause::Grow,
            }),
        };
        assert!(matches!(
            apply_transition(&store, verified.clone(), &context(&config, &clock, None)),
            Err(TransitionFailure::Authority)
        ));
        let verified_plan = apply_transition(
            &store,
            verified,
            &context(&config, &clock, Some(&authority)),
        )
        .expect("the state writer authenticates its verified-path transition");
        store.commit(&verified_plan);
        let new_finalized = store.verified[1];
        let finality_id = EvidenceId::from_digest([5; 32]);
        let finalize = TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::FullStateFinalized(crate::FullStateFinalized {
                full_state_transition_id: finality_id,
                new_finalized,
                verified_path_proof: vec![store.verified[0].hash, new_finalized.hash],
            }),
        };
        let plan = apply_transition(
            &store,
            finalize,
            &context(&config, &clock, Some(&authority)),
        )
        .expect("exact verified full-state evidence advances finality");
        assert_eq!(plan.change_set.metadata.frontiers.finalized, new_finalized);
        assert!(matches!(
            plan.change_set.finality_append.expect("full-state finality is recorded").source,
            FinalitySource::FullState { evidence } if evidence == finality_id
        ));
    }

    #[test]
    fn stale_version_and_owner_fail_before_any_plan_effect() {
        let (store, config) = TestStore::new(EngineMode::HeadersOnly);
        let clock = ManualClock(Utc::now());
        let mut request = insertion(&store, 1, EvidenceId::from_digest([6; 32]));
        request.expected_version = StateVersion::new(9);
        assert!(matches!(
            apply_transition(&store, request, &context(&config, &clock, None)),
            Err(TransitionFailure::Stale {
                current
            }) if current == StateVersion::new(0)
        ));

        let startup = super::super::StartupCapability::new();
        assert_eq!(std::mem::size_of_val(&startup), 0);
    }

    #[test]
    fn apply_transition_is_the_only_public_dag_mutation_entry_point() {
        let graph_source = include_str!("../graph.rs");
        for old_entry in [
            "pub fn insert(",
            "pub fn add_reason(",
            "pub fn remove_operator_invalidation(",
            "pub fn set_consensus_body_invalid(",
            "pub fn set_body_state(",
            "pub fn set_validation(",
        ] {
            assert!(
                !graph_source.contains(old_entry),
                "raw mutation entry point escaped: {old_entry}"
            );
        }
        assert!(!include_str!("../../src/lib.rs").contains("pub use retention::enforce_retention"));
    }

    #[test]
    fn every_named_invariant_category_rejects_its_projected_corruption() {
        use std::num::NonZeroUsize;

        use crate::{
            AuxAuthentication, AuxDelivery, BodySizeHint, ChainScore, InvariantViolation,
            SuffixWork, WorkCoordinate,
        };
        use zakura_chain::work::difficulty::U256;

        let (store, config) = TestStore::new(EngineMode::HeadersOnly);
        let clock = ManualClock(Utc::now());
        let request = insertion(&store, 2, EvidenceId::from_digest([8; 32]));
        let owner = request
            .event
            .work_owner()
            .expect("insertion carries an owner");
        let plan = apply_transition(&store, request, &context(&config, &clock, None))
            .expect("the baseline plan satisfies every invariant");
        let tip = plan.change_set.metadata.frontiers.header_best;
        let first = plan
            .projected
            .ancestor(tip.hash, block::Height(1))
            .expect("the baseline ancestry is coherent")
            .expect("height one is retained");

        let mut corrupt = plan.clone();
        corrupt
            .projected
            .node_mut(tip.hash)
            .expect("tip exists")
            .hash = block::Hash([0; 32]);
        assert!(matches!(
            verify_plan(&store, &corrupt),
            Err(InvariantViolation::NodeHash(_))
        ));

        let mut corrupt = plan.clone();
        corrupt
            .projected
            .node_mut(tip.hash)
            .expect("tip exists")
            .parent_hash = block::Hash([0; 32]);
        assert!(matches!(
            verify_plan(&store, &corrupt),
            Err(InvariantViolation::Parent(_))
        ));

        let mut corrupt = plan.clone();
        corrupt.change_set.index_changes.inserted.clear();
        assert!(matches!(
            verify_plan(&store, &corrupt),
            Err(InvariantViolation::Index(_))
        ));

        let mut corrupt = plan.clone();
        corrupt
            .projected
            .node_mut(tip.hash)
            .expect("tip exists")
            .work_coordinate = WorkCoordinate::new(block::Hash([0; 32]), U256::zero());
        assert!(matches!(
            verify_plan(&store, &corrupt),
            Err(InvariantViolation::Work(_))
        ));

        let mut corrupt = plan.clone();
        corrupt
            .projected
            .node_mut(tip.hash)
            .expect("tip exists")
            .eligibility
            .inherited_from = Some(block::Hash([0; 32]));
        assert!(matches!(
            verify_plan(&store, &corrupt),
            Err(InvariantViolation::Eligibility(_))
        ));

        let mut corrupt = plan.clone();
        corrupt.change_set.selected_projection.put.clear();
        assert!(matches!(
            verify_plan(&store, &corrupt),
            Err(InvariantViolation::SelectedProjection(_))
        ));

        let mut corrupt = plan.clone();
        corrupt.change_set.metadata.header_best_score =
            ChainScore::new(SuffixWork::zero(), tip.hash);
        assert_eq!(
            verify_plan(&store, &corrupt),
            Err(InvariantViolation::Selection)
        );

        let mut corrupt = plan.clone();
        corrupt.change_set.verified_projection.put = vec![first, tip];
        corrupt.change_set.metadata.frontiers.verified_best = tip;
        assert!(matches!(
            verify_plan(&store, &corrupt),
            Err(InvariantViolation::VerifiedProjection(_))
        ));

        let mut corrupt = plan.clone();
        corrupt
            .trust_pins
            .push(Frontier::new(first.height, block::Hash([9; 32])));
        assert_eq!(
            verify_plan(&store, &corrupt),
            Err(InvariantViolation::TrustPin(first.height))
        );

        let mut corrupt = plan.clone();
        corrupt.change_set.delete_nodes.push(tip.hash);
        corrupt.change_set.index_changes.deleted.push(tip.hash);
        assert_eq!(
            verify_plan(&store, &corrupt),
            Err(InvariantViolation::Protected(tip.hash))
        );

        let mut corrupt = plan.clone();
        corrupt.limits.max_non_finalized_nodes = NonZeroUsize::new(1).expect("one is nonzero");
        assert_eq!(
            verify_plan(&store, &corrupt),
            Err(InvariantViolation::Limits)
        );

        let mut corrupt = plan.clone();
        corrupt.change_set.metadata.header_generation = plan.before.header_generation;
        assert_eq!(
            verify_plan(&store, &corrupt),
            Err(InvariantViolation::Generation)
        );

        let mut corrupt = plan;
        let missing = block::Hash([0xab; 32]);
        corrupt
            .change_set
            .aux_changes
            .push(crate::AuxDelta::Put(Box::new(AuxDelivery {
                delivery_id: EvidenceId::from_digest([0xac; 32]),
                header_hash: missing,
                source: SourceId::from_digest([0xad; 32]),
                owner,
                body_size: BodySizeHint::Unknown,
                payload_digest: None,
                authentication: AuxAuthentication::Unauthenticated,
            })));
        assert_eq!(
            verify_plan(&store, &corrupt),
            Err(InvariantViolation::Auxiliary(missing))
        );
    }

    #[test]
    fn checked_in_transition_seed_corpus_replays_green() {
        let seeds = include_str!("transition-seeds-v1.txt");
        for seed in seeds
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
        {
            let (store, config) = TestStore::new(EngineMode::HeadersOnly);
            let clock = ManualClock(Utc::now());
            match seed {
                "headers_only_insert_1" => {
                    apply_transition(
                        &store,
                        insertion(&store, 1, EvidenceId::from_digest([0x11; 32])),
                        &context(&config, &clock, None),
                    )
                    .expect("the one-header seed replays");
                }
                "headers_only_insert_1000" => {
                    let plan = apply_transition(
                        &store,
                        insertion(&store, 1_000, EvidenceId::from_digest([0x12; 32])),
                        &context(&config, &clock, None),
                    )
                    .expect("the exact-depth seed replays");
                    assert_eq!(
                        plan.change_set.metadata.frontiers.finalized.height,
                        block::Height(0)
                    );
                }
                "headers_only_insert_1001" => {
                    let plan = apply_transition(
                        &store,
                        insertion(&store, 1_001, EvidenceId::from_digest([0x13; 32])),
                        &context(&config, &clock, None),
                    )
                    .expect("the depth-plus-one seed replays");
                    assert_eq!(
                        plan.change_set.metadata.frontiers.finalized.height,
                        block::Height(1)
                    );
                }
                "stale_expected_version" => {
                    let mut request = insertion(&store, 1, EvidenceId::from_digest([0x14; 32]));
                    request.expected_version = StateVersion::new(1);
                    assert!(matches!(
                        apply_transition(&store, request, &context(&config, &clock, None)),
                        Err(TransitionFailure::Stale { .. })
                    ));
                }
                "idempotent_replay" => {
                    let mut store = store;
                    let request = insertion(&store, 1, EvidenceId::from_digest([0x15; 32]));
                    let plan =
                        apply_transition(&store, request.clone(), &context(&config, &clock, None))
                            .expect("the initial idempotency seed commits");
                    store.commit(&plan);
                    let replay = apply_transition(&store, request, &context(&config, &clock, None))
                        .expect("the committed evidence replay is a valid no-change");
                    assert!(replay.is_no_change());
                    assert_eq!(
                        replay.change_set.metadata.state_version,
                        store.metadata.state_version
                    );
                }
                unknown => panic!("unknown checked-in transition seed {unknown}"),
            }
        }
    }

    fn apply_projection(projection: &mut Vec<Frontier>, delta: &ProjectionDelta) {
        if let Some(height) = delta.remove_from {
            projection.retain(|frontier| frontier.height < height);
        }
        projection.extend(delta.put.iter().copied());
    }
}
