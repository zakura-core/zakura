//! The sole pure mutation algorithm for durable header-chain state.

use std::collections::{HashMap, HashSet, VecDeque};

use thiserror::Error;
use zakura_chain::block;

use crate::{
    BodyEvidence, BodyValidationState, ChangeSet, CounterExhausted, EligibilityDelta,
    EligibilityReason, EngineLimits, EngineMetadata, EngineMode, EngineSnapshot, EventAdmission,
    EvidenceId, FinalityRecord, FinalitySource, Frontier, FrontierSet, GraphError, HeaderNode,
    HeaderValidationState, IndexChanges, MemHeaderStore, ProjectionDelta, RetentionPlan,
    StateVersion, StoreError, StoreRead, TargetCompletion, TransitionCause, TransitionContext,
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

    /// Consume a successfully committed plan and create its ordered receipt.
    pub fn into_committed_receipt(self, durable_tx_id: u64) -> crate::CommittedTransition {
        let current = self.change_set.metadata.snapshot();
        let inserted = self
            .change_set
            .index_changes
            .inserted
            .iter()
            .map(|frontier| frontier.hash)
            .collect();
        let eligibility_changed = self
            .change_set
            .eligibility_changes
            .iter()
            .map(|delta| delta.hash)
            .collect();
        let evicted = self.change_set.delete_nodes.clone();
        crate::CommittedTransition {
            previous: self.before.clone(),
            current,
            cause: self.cause,
            inserted,
            eligibility_changed,
            evicted,
            retired_work: crate::RetiredWork {
                header_generation_changed: self.before.header_generation
                    != self.change_set.metadata.header_generation,
                verified_generation_changed: self.before.verified_generation
                    != self.change_set.metadata.verified_generation,
                owners: Vec::new(),
            },
            durable_tx_id,
        }
    }

    pub(crate) const fn projected(&self) -> &MemHeaderStore {
        &self.projected
    }

    /// Return the verified projected graph to the isolated structured fuzzer.
    #[cfg(any(test, feature = "fuzz-impl"))]
    pub fn fuzz_projected(&self) -> &MemHeaderStore {
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
    let mut metadata = store.metadata()?;
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
    let operator_reason_changed = operator_reason_will_change(&graph, &request.event)?;
    let migrated_pin_refuted = migrated_pin_refuted(store, &request.event)?;

    apply_event(
        store,
        &mut graph,
        &mut verified,
        &mut aux_changes,
        &request.event,
        context,
    )?;
    graph.recompute_all_eligibility()?;
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

fn operator_reason_will_change(
    graph: &MemHeaderStore,
    event: &TransitionEvent,
) -> Result<bool, GraphError> {
    let (target, reason, inserting) = match event {
        TransitionEvent::OperatorInvalidate(event) => (
            event.target,
            EligibilityReason::OperatorInvalid { id: event.id },
            true,
        ),
        TransitionEvent::OperatorReconsider(event) => (
            event.target,
            EligibilityReason::OperatorInvalid { id: event.id },
            false,
        ),
        _ => return Ok(false),
    };
    let present = graph
        .node(target)
        .ok_or(GraphError::UnknownNode(target))?
        .eligibility
        .direct_reasons
        .contains(&reason);
    Ok(if inserting { !present } else { present })
}

fn migrated_pin_refuted<S: StoreRead>(
    store: &S,
    event: &TransitionEvent,
) -> Result<Option<Frontier>, StoreError> {
    let TransitionEvent::MigratedPinRefutation(event) = event else {
        return Ok(None);
    };
    Ok(store
        .finality_history()?
        .into_iter()
        .find(|record| {
            record.current == event.pin
                && matches!(record.source, FinalitySource::MigratedHeadersOnly)
        })
        .map(|record| record.current))
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
            let common_ancestor = match event.completion {
                TargetCompletion::TargetComplete { common_ancestor }
                | TargetCompletion::SelectedAuxiliaryRepair {
                    common_ancestor, ..
                } => Some(common_ancestor),
                TargetCompletion::InternalFullState => None,
            };
            if common_ancestor.is_some_and(|common_ancestor| common_ancestor != lease.parent) {
                return Err(TransitionFailure::InvalidEvidence(
                    "target completion ancestor does not match the validation lease",
                ));
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
                            != store.snapshot()?.frontiers.header_best.hash
                        || store.selected_hash(selected_target.height)?
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
                TargetCompletion::TargetComplete { .. } | TargetCompletion::InternalFullState => {
                    if event.owner.branch.target_tip_hash != event.target_tip_hash {
                        return Err(TransitionFailure::InvalidEvidence(
                            "target completion does not end at the pursued hash",
                        ));
                    }
                }
            }
            for delivery in &event.aux {
                if delivery.owner != event.owner
                    || delivery.source != event.source
                    || delivery.authentication != crate::AuxAuthentication::Unauthenticated
                    || graph.node(delivery.header_hash).is_none()
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
                let stored = store
                    .aux_deliveries(delivery.header_hash)?
                    .into_iter()
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
        TransitionEvent::MigratedPinRefutation(event) => {
            if event.invalid_header.height > event.pin.height
                || migrated_pin_refuted(
                    store,
                    &TransitionEvent::MigratedPinRefutation(event.clone()),
                )?
                .is_none()
            {
                return Err(TransitionFailure::InvalidEvidence(
                    "full-state refutation does not name an imported pin ancestor",
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
                let existing = store
                    .aux_deliveries(event_delivery.header_hash)?
                    .into_iter()
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
    let mut candidate_tips: Vec<_> = graph
        .eligible_tips()
        .into_iter()
        .map(|tip| graph.score(tip.hash).map(|score| (score, tip.hash)))
        .collect::<Result<_, _>>()?;
    candidate_tips.sort_unstable_by_key(|(score, hash)| (*score, hash.0));
    let change_set = ChangeSet {
        put_nodes,
        delete_nodes: delete_nodes.clone(),
        index_changes: IndexChanges {
            inserted,
            deleted: delete_nodes,
        },
        candidate_tips,
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
            candidate_tips: store.candidate_tips()?,
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
        aux: Vec<crate::AuxDelivery>,
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
                    aux: Vec::new(),
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
            for change in &plan.change_set.aux_changes {
                match change {
                    crate::AuxDelta::Put(delivery) => {
                        self.aux
                            .retain(|existing| existing.delivery_id != delivery.delivery_id);
                        self.aux.push(**delivery);
                    }
                    crate::AuxDelta::Delete(delivery_id) => {
                        self.aux
                            .retain(|existing| existing.delivery_id != *delivery_id);
                    }
                }
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
        fn aux_deliveries(&self, hash: block::Hash) -> Result<Vec<crate::AuxDelivery>, StoreError> {
            Ok(self
                .aux
                .iter()
                .filter(|delivery| delivery.header_hash == hash)
                .copied()
                .collect())
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
            retention_references: &[],
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
            event: TransitionEvent::InsertHeaders(Box::new(crate::InsertHeaders {
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
                completion: TargetCompletion::TargetComplete {
                    common_ancestor: store.lease.parent,
                },
                batch,
                aux: Vec::new(),
            })),
        }
    }

    fn insert_verified_branch(
        graph: &mut MemHeaderStore,
        parent: Frontier,
        count: u32,
        difficulty: zakura_chain::work::difficulty::CompactDifficulty,
        nonce_seed: u8,
    ) -> Frontier {
        let mut parent = parent;
        for offset in 0..count {
            let mut header = *regtest_genesis_block().header;
            header.previous_block_hash = parent.hash;
            header.difficulty_threshold = difficulty;
            header.nonce.0[0] = nonce_seed;
            header.nonce.0[1..5].copy_from_slice(&offset.to_be_bytes());
            let header = Arc::new(header);
            let work = header
                .difficulty_threshold
                .to_work()
                .expect("the fixture target has valid work");
            parent = match graph
                .insert(
                    header,
                    work,
                    HeaderValidationState::Valid,
                    [],
                    BodyValidationState::Verified {
                        evidence: EvidenceId::from_digest([nonce_seed; 32]),
                    },
                )
                .expect("the verified fixture branch links to its parent")
            {
                crate::InsertResult::Inserted(frontier)
                | crate::InsertResult::AlreadyPresent(frontier) => frontier,
            };
        }
        parent
    }

    fn synchronize_fixture(store: &mut TestStore, verified_tip: Frontier) {
        store
            .graph
            .recompute_all_eligibility()
            .expect("the fixture eligibility cache recomputes");
        let header_best = store
            .graph
            .select_header_best()
            .expect("the fixture has an eligible tip")
            .0;
        store.selected = path(&store.graph, header_best).expect("the selected path is retained");
        store.verified = path(&store.graph, verified_tip).expect("the verified path is retained");
        store.metadata.frontiers.header_best = header_best;
        store.metadata.frontiers.verified_best = verified_tip;
        store.metadata.header_best_score = store
            .graph
            .score(header_best.hash)
            .expect("the selected score is exact");
    }

    fn operator_invalidate(
        store: &TestStore,
        target: block::Hash,
        id: crate::OperatorInvalidationId,
        evidence: u8,
    ) -> TransitionRequest {
        TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::OperatorInvalidate(crate::OperatorInvalidate {
                target,
                id,
                operator_reason_digest: [evidence.wrapping_add(1); 32],
                evidence: EvidenceId::from_digest([evidence; 32]),
            }),
        }
    }

    fn operator_reconsider(
        store: &TestStore,
        target: block::Hash,
        id: crate::OperatorInvalidationId,
        evidence: u8,
    ) -> TransitionRequest {
        TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::OperatorReconsider(crate::OperatorReconsider {
                target,
                id,
                evidence: EvidenceId::from_digest([evidence; 32]),
            }),
        }
    }

    #[test]
    fn aud_10_invalidation_promotes_alternate_and_aud_12_preserves_nested_reasons() {
        let (mut store, config) = TestStore::new(EngineMode::Integrated);
        let clock = ManualClock(Utc::now());
        let anchor = store.graph.finalized();
        let difficulty = store
            .graph
            .node(anchor.hash)
            .expect("the anchor exists")
            .header
            .difficulty_threshold;
        let left = insert_verified_branch(&mut store.graph, anchor, 1, difficulty, 0x11);
        let right = insert_verified_branch(&mut store.graph, anchor, 1, difficulty, 0x22);
        let (winner, loser) = if store.graph.score(left.hash).expect("left score exists")
            > store.graph.score(right.hash).expect("right score exists")
        {
            (left, right)
        } else {
            (right, left)
        };
        synchronize_fixture(&mut store, winner);
        let first_id = crate::OperatorInvalidationId::new([1; 16]);
        let second_id = crate::OperatorInvalidationId::new([2; 16]);

        let first = apply_transition(
            &store,
            operator_invalidate(&store, winner.hash, first_id, 0x31),
            &context(&config, &clock, None),
        )
        .expect("invalidating the winning verified fork reselects atomically");
        assert_eq!(first.change_set.metadata.frontiers.header_best, loser);
        assert_eq!(first.change_set.metadata.frontiers.verified_best, loser);
        assert_eq!(
            first.change_set.metadata.header_generation,
            HeaderGeneration::new(1)
        );
        assert_eq!(
            first.change_set.metadata.verified_generation,
            VerifiedGeneration::new(1)
        );
        store.commit(&first);

        let mut promoted = store.clone();
        let child_request = insertion(&promoted, 1, EvidenceId::from_digest([0x36; 32]));
        let child = match &child_request.event {
            TransitionEvent::InsertHeaders(event) => {
                assert_eq!(
                    event.parent_hash, loser.hash,
                    "the next request anchors to the exact promoted branch"
                );
                event.target_tip_hash
            }
            _ => unreachable!("the next-child fixture constructs one insertion"),
        };
        let child_plan =
            apply_transition(&promoted, child_request, &context(&config, &clock, None))
                .expect("the promoted branch accepts its next child");
        assert_eq!(
            child_plan.change_set.metadata.frontiers.header_best,
            Frontier::new(block::Height(2), child)
        );
        promoted.commit(&child_plan);
        assert_eq!(
            promoted
                .graph
                .node(child)
                .expect("the promoted child is retained")
                .parent_hash,
            loser.hash
        );

        let second = apply_transition(
            &store,
            operator_invalidate(&store, winner.hash, second_id, 0x32),
            &context(&config, &clock, None),
        )
        .expect("a nested operator reason is independently durable");
        store.commit(&second);
        let reconsider_first = apply_transition(
            &store,
            operator_reconsider(&store, winner.hash, first_id, 0x33),
            &context(&config, &clock, None),
        )
        .expect("reconsider removes only the named reason");
        assert_eq!(
            reconsider_first.change_set.metadata.frontiers.verified_best,
            loser
        );
        let winner_node = reconsider_first
            .projected()
            .node(winner.hash)
            .expect("the losing node remains retained");
        assert!(!winner_node
            .eligibility
            .direct_reasons
            .contains(&EligibilityReason::OperatorInvalid { id: first_id }));
        assert!(winner_node
            .eligibility
            .direct_reasons
            .contains(&EligibilityReason::OperatorInvalid { id: second_id }));
        store.commit(&reconsider_first);

        let reconsider_second = apply_transition(
            &store,
            operator_reconsider(&store, winner.hash, second_id, 0x34),
            &context(&config, &clock, None),
        )
        .expect("removing the final operator reason restores both frontiers");
        assert_eq!(
            reconsider_second.change_set.metadata.frontiers.header_best,
            winner
        );
        assert_eq!(
            reconsider_second
                .change_set
                .metadata
                .frontiers
                .verified_best,
            winner
        );
        store.commit(&reconsider_second);
        let absent = apply_transition(
            &store,
            operator_reconsider(&store, winner.hash, second_id, 0x35),
            &context(&config, &clock, None),
        )
        .expect("an absent operator ID is a valid no-change");
        assert!(absent.is_no_change());
    }

    #[test]
    fn aud_12_reconsider_restores_a_shorter_higher_work_verified_branch() {
        use zakura_chain::work::difficulty::{ExpandedDifficulty, U256};

        let (mut store, config) = TestStore::new(EngineMode::Integrated);
        let clock = ManualClock(Utc::now());
        let anchor = store.graph.finalized();
        let easy = store
            .graph
            .node(anchor.hash)
            .expect("the anchor exists")
            .header
            .difficulty_threshold;
        let easy_target: U256 = easy
            .to_expanded()
            .expect("the fixture target expands")
            .into();
        let hard = ExpandedDifficulty::from(easy_target >> 3).into();
        let longer = insert_verified_branch(&mut store.graph, anchor, 2, easy, 0x41);
        let shorter = insert_verified_branch(&mut store.graph, anchor, 1, hard, 0x42);
        assert!(
            store.graph.score(shorter.hash).expect("short score exists")
                > store.graph.score(longer.hash).expect("long score exists")
        );
        synchronize_fixture(&mut store, shorter);
        let id = crate::OperatorInvalidationId::new([3; 16]);

        let invalidate = apply_transition(
            &store,
            operator_invalidate(&store, shorter.hash, id, 0x43),
            &context(&config, &clock, None),
        )
        .expect("invalidating the shorter winner promotes the longer branch");
        assert_eq!(invalidate.change_set.metadata.frontiers.header_best, longer);
        assert_eq!(
            invalidate.change_set.metadata.frontiers.verified_best,
            longer
        );
        store.commit(&invalidate);

        let reconsider = apply_transition(
            &store,
            operator_reconsider(&store, shorter.hash, id, 0x44),
            &context(&config, &clock, None),
        )
        .expect("reconsider restores the shorter higher-work branch");
        assert_eq!(
            reconsider.change_set.metadata.frontiers.header_best,
            shorter
        );
        assert_eq!(
            reconsider.change_set.metadata.frontiers.verified_best,
            shorter
        );
    }

    #[test]
    fn aud_11_invalidating_only_verified_path_keeps_independent_header_best() {
        let (mut store, config) = TestStore::new(EngineMode::Integrated);
        let clock = ManualClock(Utc::now());
        let anchor = store.graph.finalized();
        let difficulty = store
            .graph
            .node(anchor.hash)
            .expect("the anchor exists")
            .header
            .difficulty_threshold;
        let verified_tip = insert_verified_branch(&mut store.graph, anchor, 2, difficulty, 0x51);
        let header_tip = insert_verified_branch(&mut store.graph, anchor, 3, difficulty, 0x53);
        for frontier in path(&store.graph, header_tip)
            .expect("the independent header path is retained")
            .into_iter()
            .skip(1)
        {
            store
                .graph
                .set_body_state(frontier.hash, BodyValidationState::Unknown)
                .expect("the independent candidate deliberately has no verified body");
        }
        synchronize_fixture(&mut store, verified_tip);
        assert_eq!(store.metadata.frontiers.header_best, header_tip);
        assert_eq!(store.metadata.frontiers.verified_best, verified_tip);

        let plan = apply_transition(
            &store,
            operator_invalidate(
                &store,
                store.verified[1].hash,
                crate::OperatorInvalidationId::new([4; 16]),
                0x52,
            ),
            &context(&config, &clock, None),
        )
        .expect("invalidating the only full-state branch falls back atomically");
        assert_eq!(
            plan.change_set.metadata.frontiers.header_best, header_tip,
            "the independently eligible header branch remains selected"
        );
        assert_eq!(plan.change_set.metadata.frontiers.verified_best, anchor);
        assert_eq!(
            plan.change_set.verified_projection,
            ProjectionDelta {
                remove_from: Some(block::Height(1)),
                put: Vec::new(),
            }
        );
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
    fn migrated_pin_refutation_requires_full_state_authority_and_exact_pin() {
        let (mut store, config) = TestStore::new(EngineMode::Integrated);
        let clock = ManualClock(Utc::now());
        let authority = Authority;
        let pin = store.graph.finalized();
        store.finality.push(FinalityRecord {
            previous: pin,
            current: pin,
            source: FinalitySource::MigratedHeadersOnly,
            epoch: FinalityEpoch::new(0),
        });
        let evidence = EvidenceId::from_digest([0x61; 32]);
        let request = |pin| TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::MigratedPinRefutation(crate::MigratedPinRefutation {
                full_state_transition_id: evidence,
                pin,
                invalid_header: Frontier::new(block::Height(0), block::Hash([0x62; 32])),
                rule: crate::BodyRuleId::new("body.imported-history"),
            }),
        };

        assert!(matches!(
            apply_transition(&store, request(pin), &context(&config, &clock, None)),
            Err(TransitionFailure::Authority)
        ));
        assert!(matches!(
            apply_transition(
                &store,
                request(Frontier::new(pin.height, block::Hash([0x63; 32]))),
                &context(&config, &clock, Some(&authority)),
            ),
            Err(TransitionFailure::InvalidEvidence(_))
        ));

        let plan = apply_transition(
            &store,
            request(pin),
            &context(&config, &clock, Some(&authority)),
        )
        .expect("full state can persist a refuted imported pin incident");
        assert_eq!(
            plan.change_set.metadata.alarms.migrated_pin_refuted,
            Some(pin)
        );
        assert_eq!(
            plan.change_set.metadata.state_version,
            store
                .metadata
                .state_version
                .checked_next()
                .expect("the fixture version has capacity")
        );
        assert!(plan.change_set.put_nodes.is_empty());
        assert!(plan.change_set.delete_nodes.is_empty());
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
    fn peer_target_completion_must_match_the_validation_lease_ancestor() {
        let (store, config) = TestStore::new(EngineMode::HeadersOnly);
        let clock = ManualClock(Utc::now());
        let mut request = insertion(&store, 1, EvidenceId::from_digest([0x64; 32]));
        let TransitionEvent::InsertHeaders(insert) = &mut request.event else {
            panic!("the fixture constructs a header insertion");
        };
        insert.completion = TargetCompletion::TargetComplete {
            common_ancestor: Frontier::new(store.lease.parent.height, block::Hash([0x65; 32])),
        };

        assert!(matches!(
            apply_transition(&store, request, &context(&config, &clock, None)),
            Err(TransitionFailure::InvalidEvidence(
                "target completion ancestor does not match the validation lease"
            ))
        ));
    }

    #[test]
    fn selected_auxiliary_repair_adds_only_one_exact_provenance_record() {
        let (mut store, config) = TestStore::new(EngineMode::Integrated);
        let clock = ManualClock(Utc::now());
        let anchor = store.metadata.frontiers.finalized;
        let insert = insertion(&store, 2, EvidenceId::from_digest([0x66; 32]));
        let TransitionEvent::InsertHeaders(initial) = &insert.event else {
            panic!("the fixture constructs a header insertion");
        };
        let repaired = initial.batch.headers()[0].clone();
        let selected_target = Frontier::new(repaired.height, repaired.hash);
        let inserted = apply_transition(&store, insert, &context(&config, &clock, None))
            .expect("the selected fixture branch inserts");
        store.commit(&inserted);

        store.lease.parent = anchor;
        store.lease.context_digest = [0x67; 32];
        let owner = crate::WorkScope::for_body_work(&store.snapshot())
            .bind(8, NonZeroU64::new(9).expect("nine is nonzero"));
        let source = SourceId::from_digest([0x68; 32]);
        let delivery = crate::AuxDelivery {
            delivery_id: EvidenceId::from_digest([0x69; 32]),
            header_hash: repaired.hash,
            source,
            owner,
            body_size: crate::BodySizeHint::Unknown,
            tree_aux: Some(crate::TreeAuxRecordV1 {
                height: repaired.height,
                sapling_root: zakura_chain::sapling::tree::Root::default(),
                orchard_root: zakura_chain::orchard::tree::Root::default(),
                ironwood_root: zakura_chain::ironwood::tree::Root::default(),
                sapling_tx_count: 1,
                orchard_tx_count: 2,
                ironwood_tx_count: 3,
                auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([0x6a; 32]),
            }),
            authentication: crate::AuxAuthentication::Unauthenticated,
        };
        let repair = TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::InsertHeaders(Box::new(crate::InsertHeaders {
                owner,
                source,
                parent_hash: anchor.hash,
                target_tip_hash: repaired.hash,
                completion: TargetCompletion::SelectedAuxiliaryRepair {
                    common_ancestor: anchor,
                    selected_target,
                },
                batch: PreparedHeaderBatch::new(
                    vec![repaired],
                    store.lease.context_digest,
                    EvidenceId::from_digest([0x6b; 32]),
                )
                .expect("the exact repair batch is nonempty"),
                aux: vec![delivery],
            })),
        };

        assert_eq!(
            repair.event.idempotency_key(),
            Some(delivery.delivery_id),
            "repair replay identity is the new provenance record, not the old header batch"
        );
        let repaired = apply_transition(&store, repair, &context(&config, &clock, None))
            .expect("one exact selected auxiliary repair is admitted");
        assert_eq!(repaired.change_set.put_nodes.len(), 1);
        assert_eq!(repaired.change_set.put_nodes[0].hash, selected_target.hash);
        assert_eq!(
            repaired.change_set.put_nodes[0].aux_delivery_ids,
            vec![delivery.delivery_id]
        );
        assert!(repaired.change_set.delete_nodes.is_empty());
        assert!(repaired.change_set.selected_projection.put.is_empty());
        assert!(repaired.change_set.verified_projection.put.is_empty());
        assert_eq!(
            repaired.change_set.metadata.header_generation,
            store.metadata.header_generation
        );
        assert_eq!(
            repaired.change_set.metadata.verified_generation,
            store.metadata.verified_generation
        );
        assert_eq!(
            repaired.change_set.aux_changes,
            vec![crate::AuxDelta::Put(Box::new(delivery))]
        );
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
                tree_aux: None,
                authentication: AuxAuthentication::Unauthenticated,
            })));
        assert_eq!(
            verify_plan(&store, &corrupt),
            Err(InvariantViolation::Auxiliary(missing))
        );
    }

    #[test]
    fn auxiliary_authentication_requires_exact_provenance_and_owned_next_header() {
        let (mut store, config) = TestStore::new(EngineMode::Integrated);
        let clock = ManualClock(Utc::now());
        let mut insert = insertion(&store, 2, EvidenceId::from_digest([0xb0; 32]));
        let TransitionEvent::InsertHeaders(insert_event) = &mut insert.event else {
            unreachable!("the insertion fixture contains header evidence")
        };
        let header_hash = insert_event.batch.headers()[0].hash;
        let boundary_hash = insert_event.batch.headers()[1].hash;
        let delivery = crate::AuxDelivery {
            delivery_id: EvidenceId::from_digest([0xb1; 32]),
            header_hash,
            source: SourceId::from_digest([0xb2; 32]),
            owner: insert_event.owner,
            body_size: crate::BodySizeHint::Unknown,
            tree_aux: Some(crate::TreeAuxRecordV1 {
                height: block::Height(1),
                sapling_root: zakura_chain::sapling::tree::Root::default(),
                orchard_root: zakura_chain::orchard::tree::Root::default(),
                ironwood_root: zakura_chain::ironwood::tree::Root::default(),
                sapling_tx_count: 3,
                orchard_tx_count: 4,
                ironwood_tx_count: 5,
                auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([0xb3; 32]),
            }),
            authentication: crate::AuxAuthentication::Unauthenticated,
        };
        insert_event.source = delivery.source;
        let second_delivery = crate::AuxDelivery {
            delivery_id: EvidenceId::from_digest([0xc1; 32]),
            ..delivery
        };
        let third_delivery = crate::AuxDelivery {
            delivery_id: EvidenceId::from_digest([0xc3; 32]),
            ..delivery
        };
        insert_event
            .aux
            .extend([delivery, second_delivery, third_delivery]);
        let inserted = apply_transition(&store, insert, &context(&config, &clock, None))
            .expect("the target and unauthenticated delivery insert atomically");
        store.commit(&inserted);

        let repair_owner = WorkOwner {
            state_version: store.metadata.state_version,
            header_generation: store.metadata.header_generation,
            verified_generation: Some(store.metadata.verified_generation),
            branch: BranchId::new(
                store.metadata.frontiers.finalized.hash,
                store.metadata.frontiers.header_best.hash,
            ),
            session_id: 2,
            request_id: NonZeroU64::new(2).expect("two is nonzero"),
        };
        let authentication = crate::AuxAuthentication::Authenticated {
            evidence: EvidenceId::from_digest([0xb4; 32]),
            boundary_hash,
        };
        let request = |delivery, authentication| TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::AuxEvidence(Box::new(crate::AuxEvidence {
                owner: repair_owner,
                deliveries: vec![delivery],
                authentication,
            })),
        };

        let mut changed_provenance = delivery;
        changed_provenance.source = SourceId::from_digest([0xb5; 32]);
        assert!(matches!(
            apply_transition(
                &store,
                request(changed_provenance, authentication),
                &context(&config, &clock, Some(&Authority)),
            ),
            Err(TransitionFailure::InvalidEvidence(
                "auxiliary evidence changes delivery provenance"
            ))
        ));
        let wrong_boundary = crate::AuxAuthentication::Authenticated {
            evidence: EvidenceId::from_digest([0xb6; 32]),
            boundary_hash: header_hash,
        };
        assert!(matches!(
            apply_transition(
                &store,
                request(delivery, wrong_boundary),
                &context(&config, &clock, Some(&Authority)),
            ),
            Err(TransitionFailure::InvalidEvidence(
                "auxiliary authentication is not the owned one-header-later boundary"
            ))
        ));

        let before = store.snapshot();
        let authenticated = apply_transition(
            &store,
            request(delivery, authentication),
            &context(&config, &clock, Some(&Authority)),
        )
        .expect("exact integrated evidence authenticates metadata only");
        assert!(authenticated.change_set.put_nodes.is_empty());
        assert!(authenticated.change_set.eligibility_changes.is_empty());
        assert_eq!(
            authenticated.change_set.metadata.frontiers,
            before.frontiers
        );
        assert_eq!(
            authenticated.change_set.metadata.header_generation,
            before.header_generation
        );
        assert_eq!(
            authenticated.change_set.metadata.verified_generation,
            before.verified_generation
        );
        assert_eq!(
            authenticated.change_set.aux_changes,
            vec![crate::AuxDelta::Put(Box::new(crate::AuxDelivery {
                authentication,
                ..delivery
            }))]
        );
        store.commit(&authenticated);

        let rejection = crate::AuxAuthentication::Rejected {
            evidence: EvidenceId::from_digest([0xc5; 32]),
        };
        let rejection_owner = WorkOwner {
            state_version: store.metadata.state_version,
            header_generation: store.metadata.header_generation,
            verified_generation: Some(store.metadata.verified_generation),
            ..repair_owner
        };
        let rejected = apply_transition(
            &store,
            TransitionRequest {
                expected_version: store.metadata.state_version,
                event: TransitionEvent::AuxEvidence(Box::new(crate::AuxEvidence {
                    owner: rejection_owner,
                    deliveries: vec![second_delivery, third_delivery],
                    authentication: rejection,
                })),
            },
            &context(&config, &clock, Some(&Authority)),
        )
        .expect("two exact metadata deliveries reject in one atomic transition");
        assert_eq!(
            rejected.change_set.aux_changes,
            vec![
                crate::AuxDelta::Put(Box::new(crate::AuxDelivery {
                    authentication: rejection,
                    ..second_delivery
                })),
                crate::AuxDelta::Put(Box::new(crate::AuxDelivery {
                    authentication: rejection,
                    ..third_delivery
                })),
            ],
        );
        assert_eq!(
            rejected.change_set.metadata.state_version,
            store
                .metadata
                .state_version
                .checked_next()
                .expect("the fixture state version can advance"),
            "the two-delivery rejection advances one atomic state version"
        );
        store.commit(&rejected);

        let replay = apply_transition(
            &store,
            TransitionRequest {
                expected_version: store.metadata.state_version,
                event: TransitionEvent::AuxEvidence(Box::new(crate::AuxEvidence {
                    owner: WorkOwner {
                        state_version: store.metadata.state_version,
                        header_generation: store.metadata.header_generation,
                        verified_generation: Some(store.metadata.verified_generation),
                        ..repair_owner
                    },
                    deliveries: vec![crate::AuxDelivery {
                        authentication,
                        ..delivery
                    }],
                    authentication,
                })),
            },
            &context(&config, &clock, Some(&Authority)),
        )
        .expect("authentication replay is idempotent");
        assert!(replay.is_no_change());
    }

    #[test]
    fn transient_body_evidence_cannot_regress_a_verified_body() {
        let (mut store, config) = TestStore::new(EngineMode::Integrated);
        let clock = ManualClock(Utc::now());
        store
            .graph
            .set_body_state(
                store.metadata.frontiers.verified_best.hash,
                BodyValidationState::Verified {
                    evidence: EvidenceId::from_digest([0xbf; 32]),
                },
            )
            .expect("the fixture body becomes verified");
        let request = TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::BodyEvidence(BodyEvidence::Transient(
                crate::TransientBodyFailure {
                    hash: store.metadata.frontiers.verified_best.hash,
                    evidence: EvidenceId::from_digest([0xc0; 32]),
                    kind: crate::TransientBodyFailureKind::Timeout,
                    availability: crate::BodyUnavailableSummary {
                        attempts: 1,
                        suppliers: 1,
                        alarmed: false,
                        ..Default::default()
                    },
                },
            )),
        };

        let result = apply_transition(&store, request, &context(&config, &clock, Some(&Authority)));
        assert!(
            matches!(
                result,
                Err(TransitionFailure::InvalidEvidence(
                    "body retry evidence cannot regress an already verified body"
                ))
            ),
            "unexpected transition result: {result:?}"
        );
    }

    #[test]
    fn new_body_supplier_restarts_only_the_selected_persistent_alarm() {
        let (mut store, config) = TestStore::new(EngineMode::Integrated);
        let now = Utc::now();
        let clock = ManualClock(now);
        let selected = store.metadata.frontiers.header_best;
        let old = crate::BodyUnavailableSummary {
            started_at: now - chrono::Duration::minutes(12),
            attempts: 10,
            suppliers: 2,
            supplier_set_digest: [0x11; 32],
            alarmed: true,
            next_probe_at: now + chrono::Duration::minutes(8),
        };
        store
            .graph
            .set_body_state(selected.hash, BodyValidationState::Unavailable(old))
            .expect("the selected fixture body exists");
        store.metadata.alarms.header_best_body_unavailable = Some(old);
        let fresh = crate::BodyUnavailableSummary {
            started_at: now,
            attempts: 0,
            suppliers: 2,
            supplier_set_digest: [0x22; 32],
            alarmed: false,
            next_probe_at: now,
        };
        let evidence = EvidenceId::from_digest([0xc1; 32]);
        let request = TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::BodySupplierDiscovered(crate::BodySupplierDiscovered {
                hash: selected.hash,
                evidence,
                availability: fresh,
            }),
        };

        let plan = apply_transition(
            &store,
            request.clone(),
            &context(&config, &clock, Some(&Authority)),
        )
        .expect("a changed supplier set starts a fresh availability episode");
        assert_eq!(plan.change_set.metadata.frontiers, store.metadata.frontiers);
        assert_eq!(
            plan.projected
                .node(selected.hash)
                .expect("the selected node remains retained")
                .body,
            BodyValidationState::Unavailable(fresh)
        );
        assert_eq!(
            plan.projected
                .node(selected.hash)
                .expect("the selected node remains retained")
                .eligibility,
            store
                .graph
                .node(selected.hash)
                .expect("the selected fixture node exists")
                .eligibility
        );
        assert_eq!(
            plan.change_set.metadata.alarms.header_best_body_unavailable,
            None
        );
        store.commit(&plan);
        let replay = apply_transition(
            &store,
            TransitionRequest {
                expected_version: store.metadata.state_version,
                ..request
            },
            &context(&config, &clock, Some(&Authority)),
        )
        .expect("the exact supplier-discovery evidence replays idempotently");
        assert!(replay.is_no_change());
    }

    #[test]
    fn body_supplier_restart_rejects_nonfresh_or_nonexpanding_evidence() {
        let (mut store, config) = TestStore::new(EngineMode::Integrated);
        let now = Utc::now();
        let clock = ManualClock(now);
        let selected = store.metadata.frontiers.header_best;
        let old = crate::BodyUnavailableSummary {
            started_at: now - chrono::Duration::minutes(12),
            attempts: 10,
            suppliers: 2,
            supplier_set_digest: [0x11; 32],
            alarmed: true,
            next_probe_at: now + chrono::Duration::minutes(8),
        };
        store
            .graph
            .set_body_state(selected.hash, BodyValidationState::Unavailable(old))
            .expect("the selected fixture body exists");
        store.metadata.alarms.header_best_body_unavailable = Some(old);
        let apply = |availability| {
            apply_transition(
                &store,
                TransitionRequest {
                    expected_version: store.metadata.state_version,
                    event: TransitionEvent::BodySupplierDiscovered(crate::BodySupplierDiscovered {
                        hash: selected.hash,
                        evidence: EvidenceId::from_digest([0xc2; 32]),
                        availability,
                    }),
                },
                &context(&config, &clock, Some(&Authority)),
            )
        };

        assert!(matches!(
            apply(crate::BodyUnavailableSummary {
                started_at: now,
                attempts: 0,
                suppliers: 2,
                supplier_set_digest: old.supplier_set_digest,
                alarmed: false,
                next_probe_at: now,
            }),
            Err(TransitionFailure::InvalidEvidence(
                "body supplier discovery does not add an eligible supplier"
            ))
        ));
        assert!(matches!(
            apply(crate::BodyUnavailableSummary {
                started_at: now,
                attempts: 1,
                suppliers: 3,
                supplier_set_digest: [0x22; 32],
                alarmed: false,
                next_probe_at: now,
            }),
            Err(TransitionFailure::InvalidEvidence(
                "body supplier discovery has an invalid fresh episode"
            ))
        ));
        assert!(matches!(
            apply(crate::BodyUnavailableSummary {
                started_at: now,
                attempts: 0,
                suppliers: 1,
                supplier_set_digest: [0x22; 32],
                alarmed: false,
                next_probe_at: now,
            }),
            Err(TransitionFailure::InvalidEvidence(
                "body supplier discovery does not add an eligible supplier"
            ))
        ));
    }

    #[test]
    fn operator_body_retry_restarts_the_selected_alarm_with_the_same_suppliers() {
        let (mut store, config) = TestStore::new(EngineMode::Integrated);
        let now = Utc::now();
        let clock = ManualClock(now);
        let selected = store.metadata.frontiers.header_best;
        let old = crate::BodyUnavailableSummary {
            started_at: now - chrono::Duration::minutes(12),
            attempts: 10,
            suppliers: 2,
            supplier_set_digest: [0x31; 32],
            alarmed: true,
            next_probe_at: now + chrono::Duration::minutes(8),
        };
        store
            .graph
            .set_body_state(selected.hash, BodyValidationState::Unavailable(old))
            .expect("the selected fixture body exists");
        store.metadata.alarms.header_best_body_unavailable = Some(old);
        let fresh = crate::BodyUnavailableSummary {
            started_at: now,
            attempts: 0,
            suppliers: old.suppliers,
            supplier_set_digest: old.supplier_set_digest,
            alarmed: false,
            next_probe_at: now,
        };
        let request = TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::OperatorBodyRetry(crate::OperatorBodyRetry {
                hash: selected.hash,
                evidence: EvidenceId::from_digest([0xc3; 32]),
                availability: fresh,
            }),
        };

        let plan = apply_transition(&store, request.clone(), &context(&config, &clock, None))
            .expect("an authenticated operator can restart the same supplier set");
        assert_eq!(plan.change_set.metadata.frontiers, store.metadata.frontiers);
        assert_eq!(
            plan.change_set.metadata.header_generation,
            store.metadata.header_generation
        );
        assert_eq!(
            plan.change_set.metadata.verified_generation,
            store.metadata.verified_generation
        );
        assert_eq!(
            plan.projected
                .node(selected.hash)
                .expect("the selected node remains retained")
                .body,
            BodyValidationState::Unavailable(fresh)
        );
        assert_eq!(
            plan.change_set.metadata.alarms.header_best_body_unavailable,
            None
        );
        store.commit(&plan);
        let replay = apply_transition(
            &store,
            TransitionRequest {
                expected_version: store.metadata.state_version,
                ..request
            },
            &context(&config, &clock, None),
        )
        .expect("the exact operator evidence replays idempotently");
        assert!(replay.is_no_change());
    }

    #[test]
    fn operator_body_retry_rejects_stale_or_malformed_requests() {
        let (mut store, config) = TestStore::new(EngineMode::Integrated);
        let now = Utc::now();
        let clock = ManualClock(now);
        let selected = store.metadata.frontiers.header_best;
        let old = crate::BodyUnavailableSummary {
            attempts: 10,
            suppliers: 2,
            supplier_set_digest: [0x41; 32],
            alarmed: true,
            ..Default::default()
        };
        store
            .graph
            .set_body_state(selected.hash, BodyValidationState::Unavailable(old))
            .expect("the selected fixture body exists");
        store.metadata.alarms.header_best_body_unavailable = Some(old);
        let fresh = crate::BodyUnavailableSummary {
            started_at: now,
            attempts: 0,
            suppliers: 2,
            supplier_set_digest: old.supplier_set_digest,
            alarmed: false,
            next_probe_at: now,
        };
        let apply = |hash, availability| {
            apply_transition(
                &store,
                TransitionRequest {
                    expected_version: store.metadata.state_version,
                    event: TransitionEvent::OperatorBodyRetry(crate::OperatorBodyRetry {
                        hash,
                        evidence: EvidenceId::from_digest([0xc4; 32]),
                        availability,
                    }),
                },
                &context(&config, &clock, None),
            )
        };

        assert!(matches!(
            apply(block::Hash([0x42; 32]), fresh),
            Err(TransitionFailure::InvalidEvidence(
                "operator body retry has an invalid fresh episode"
            ))
        ));
        assert!(matches!(
            apply(
                selected.hash,
                crate::BodyUnavailableSummary {
                    attempts: 1,
                    ..fresh
                }
            ),
            Err(TransitionFailure::InvalidEvidence(
                "operator body retry has an invalid fresh episode"
            ))
        ));
        store
            .graph
            .set_body_state(selected.hash, BodyValidationState::Unknown)
            .expect("the selected fixture body exists");
        let non_alarmed = apply_transition(
            &store,
            TransitionRequest {
                expected_version: store.metadata.state_version,
                event: TransitionEvent::OperatorBodyRetry(crate::OperatorBodyRetry {
                    hash: selected.hash,
                    evidence: EvidenceId::from_digest([0xc4; 32]),
                    availability: fresh,
                }),
            },
            &context(&config, &clock, None),
        );
        assert!(matches!(
            non_alarmed,
            Err(TransitionFailure::InvalidEvidence(
                "operator body retry requires the selected persistent alarm"
            ))
        ));
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
