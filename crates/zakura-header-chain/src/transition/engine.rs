//! Stateful ownership boundary for coherent header-chain planning.

use std::collections::{HashMap, HashSet};

use thiserror::Error;
use zakura_chain::block;

use crate::{
    AuxDelivery, AuxDelta, AuxEvidence, BodyEvidence, BodySupplierDiscovered, ChangeSet,
    EngineMetadata, EngineSnapshot, FinalityRecord, Frontier, FullStateFinalized, GraphError,
    InsertHeaders, MemHeaderStore, MigratedPinRefutation, OperatorBodyRetry, OperatorInvalidate,
    OperatorReconsider, ProjectionDelta, StateVersion, TransitionContext, TransitionDomain,
    TransitionEvent, TransitionFailure, TransitionPlan, ValidationLease, VerifiedBlockAccepted,
    VerifiedChainChanged,
};

use super::planner::derive_transition_plan;

/// Incoherent state supplied while hydrating an audited engine.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum EngineHydrationError {
    /// The supplied graph, projections, metadata, or auxiliary rows disagree.
    #[error("incoherent audited header-chain engine state: {0}")]
    Incoherent(&'static str),
    /// The supplied graph is internally incoherent.
    #[error(transparent)]
    Graph(#[from] GraphError),
}

/// The engine could not install a verified transition on its original in-memory source.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum CommittedTransitionError {
    /// Another transition changed the engine after the planner created this transition.
    #[error("committed header transition no longer matches its source snapshot")]
    StaleSource,
    /// The graph rejected the verified delta.
    #[error(transparent)]
    Graph(#[from] GraphError),
}

/// Durable predecessor leases used for contextual header validation.
#[derive(Clone, Debug, Default)]
pub struct HeaderValidationFacts {
    /// Exact predecessor leases available for missing retained parents.
    pub validation_leases: Vec<ValidationLease>,
}

/// Durable facts consumed by prepared header insertion, including finality rebase history.
#[derive(Clone, Debug, Default)]
pub struct HeaderInsertionFacts {
    /// Predecessor leases for the original and rebased parents.
    pub validation: HeaderValidationFacts,
    /// Contiguous finality records from the work's stable anchor to current finality.
    pub finality_rebase_history: Vec<FinalityRecord>,
}

/// Engine-boundary package that binds one [`TransitionEvent`] to the durable
/// facts that event may consume.
///
/// The state write adapter builds this from a [`crate::TransitionRequest`]: it
/// authenticates the event, loads only the store rows that variant needs, and
/// hands the result to [`HeaderChainEngine::plan_transition`]. The planner never
/// reads the durable store itself.
///
/// Exhaustiveness is the contract. Each variant carries exactly its allowed
/// facts (for example validation leases and finality rebase history for header
/// insertion, or a preserved migration pin for pin refutation). Unrelated store
/// facts are unrepresentable.
///
/// Freshness is also variant-specific: most inputs are version-qualified via
/// `expected_version`, while [`Self::InsertHeaders`] and [`Self::AuxEvidence`]
/// omit it and rely on work ownership instead.
#[derive(Clone, Debug)]
pub enum TransitionInput {
    /// Prepared header admission with contextual leases and optional rebase history.
    InsertHeaders {
        /// Authenticated prepared insertion.
        event: Box<InsertHeaders>,
        /// Durable validation and rebase facts for this insertion.
        facts: HeaderInsertionFacts,
    },
    /// Full-state selected-path replacement with contextual header leases.
    VerifiedChainChanged {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated verified-path change.
        event: VerifiedChainChanged,
        /// Durable predecessor leases for missing path headers.
        facts: HeaderValidationFacts,
    },
    /// Full-state side-path acceptance with contextual header leases.
    VerifiedBlockAccepted {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated side-path acceptance.
        event: VerifiedBlockAccepted,
        /// Durable predecessor leases for missing path headers.
        facts: HeaderValidationFacts,
    },
    /// Body delivery or verification evidence.
    BodyEvidence {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated body evidence.
        event: BodyEvidence,
    },
    /// Newly eligible body-supplier discovery.
    BodySupplierDiscovered {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated supplier discovery.
        event: BodySupplierDiscovered,
    },
    /// Authenticated operator body retry.
    OperatorBodyRetry {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated retry.
        event: OperatorBodyRetry,
    },
    /// Reversible operator invalidation.
    OperatorInvalidate {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated invalidation.
        event: OperatorInvalidate,
    },
    /// Reason-scoped operator reconsideration.
    OperatorReconsider {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated reconsideration.
        event: OperatorReconsider,
    },
    /// Integrated full-state finality advancement.
    FullStateFinalized {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated finality evidence.
        event: FullStateFinalized,
    },
    /// Migrated headers-only pin refutation with the preserved durable pin fact.
    MigratedPinRefutation {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
        /// Authenticated refutation.
        event: MigratedPinRefutation,
        /// The exact preserved migration pin when durable history contains it.
        preserved_pin: Option<Frontier>,
    },
    /// Hash-scoped auxiliary evidence; freshness is owner-qualified.
    AuxEvidence {
        /// Authenticated auxiliary update.
        event: Box<AuxEvidence>,
    },
    /// Reevaluate all locally due future-time deferrals.
    ReevaluateDeferred {
        /// Exact durable version observed by the caller.
        expected_version: StateVersion,
    },
}

impl TransitionInput {
    /// Return the submitted event domain.
    pub fn domain(&self) -> TransitionDomain {
        self.event().domain()
    }

    /// Return the typed event carried by this input.
    pub fn event(&self) -> TransitionEvent {
        match self {
            Self::InsertHeaders { event, .. } => TransitionEvent::InsertHeaders(event.clone()),
            Self::VerifiedChainChanged { event, .. } => {
                TransitionEvent::VerifiedChainChanged(event.clone())
            }
            Self::VerifiedBlockAccepted { event, .. } => {
                TransitionEvent::VerifiedBlockAccepted(event.clone())
            }
            Self::BodyEvidence { event, .. } => TransitionEvent::BodyEvidence(event.clone()),
            Self::BodySupplierDiscovered { event, .. } => {
                TransitionEvent::BodySupplierDiscovered(*event)
            }
            Self::OperatorBodyRetry { event, .. } => TransitionEvent::OperatorBodyRetry(*event),
            Self::OperatorInvalidate { event, .. } => TransitionEvent::OperatorInvalidate(*event),
            Self::OperatorReconsider { event, .. } => TransitionEvent::OperatorReconsider(*event),
            Self::FullStateFinalized { event, .. } => {
                TransitionEvent::FullStateFinalized(event.clone())
            }
            Self::MigratedPinRefutation { event, .. } => {
                TransitionEvent::MigratedPinRefutation(event.clone())
            }
            Self::AuxEvidence { event } => TransitionEvent::AuxEvidence(event.clone()),
            Self::ReevaluateDeferred { .. } => TransitionEvent::ReevaluateDeferred,
        }
    }

    /// Return the caller-observed durable version when the input is version-qualified.
    ///
    /// Owner-qualified insertion and auxiliary inputs return `None` because their
    /// freshness is enforced by work ownership rather than state version.
    pub const fn expected_version(&self) -> Option<StateVersion> {
        match self {
            Self::InsertHeaders { .. } | Self::AuxEvidence { .. } => None,
            Self::VerifiedChainChanged {
                expected_version, ..
            }
            | Self::VerifiedBlockAccepted {
                expected_version, ..
            }
            | Self::BodyEvidence {
                expected_version, ..
            }
            | Self::BodySupplierDiscovered {
                expected_version, ..
            }
            | Self::OperatorBodyRetry {
                expected_version, ..
            }
            | Self::OperatorInvalidate {
                expected_version, ..
            }
            | Self::OperatorReconsider {
                expected_version, ..
            }
            | Self::FullStateFinalized {
                expected_version, ..
            }
            | Self::MigratedPinRefutation {
                expected_version, ..
            }
            | Self::ReevaluateDeferred { expected_version } => Some(*expected_version),
        }
    }

    /// Return header-validation leases when this input carries them.
    pub fn header_validation_facts(&self) -> Option<&HeaderValidationFacts> {
        match self {
            Self::InsertHeaders { facts, .. } => Some(&facts.validation),
            Self::VerifiedChainChanged { facts, .. }
            | Self::VerifiedBlockAccepted { facts, .. } => Some(facts),
            _ => None,
        }
    }

    /// Return finality rebase history when this input is a header insertion.
    pub fn finality_rebase_history(&self) -> Option<&[FinalityRecord]> {
        match self {
            Self::InsertHeaders { facts, .. } => Some(&facts.finality_rebase_history),
            _ => None,
        }
    }

    /// Return the preserved migrated pin fact when this input is a pin refutation.
    pub const fn preserved_migrated_pin(&self) -> Option<Option<Frontier>> {
        match self {
            Self::MigratedPinRefutation { preserved_pin, .. } => Some(*preserved_pin),
            _ => None,
        }
    }
}

/// Owns coherent in-memory header-chain state and decides its valid transitions.
///
/// The engine determines whether evidence is admissible, stale, or replayed;
/// maintains header eligibility; selects the best fork; applies finality and
/// retention policy; and derives the exact graph, projection, alarm, and
/// metadata write set for each transition.
///
/// It performs no durable writes or publication; the runtime commits, installs,
/// and publishes each verified transition.
#[derive(Clone, Debug)]
pub struct HeaderChainEngine {
    /// Complete retained header graph.
    graph: MemHeaderStore,
    metadata: EngineMetadata,
    /// Selected header path from finality to the selected tip.
    selected_projection: Vec<Frontier>,
    /// Body-verified path from finality to the verified tip.
    verified_projection: Vec<Frontier>,
    /// Auxiliary deliveries keyed by retained header hash.
    aux_deliveries: HashMap<block::Hash, Vec<AuxDelivery>>,
}

impl HeaderChainEngine {
    /// The engine constructs coherent state from a successful durable recovery audit.
    ///
    /// Rechecks the coherence required for safe transition planning:
    /// - graph finality agrees with metadata;
    /// - selected and verified projections are contiguous finalized-rooted paths;
    /// - the selected tip and score agree with graph fork choice;
    /// - verified-path body state agrees with the engine mode; and
    /// - auxiliary deliveries agree with the graph's delivery index.
    ///
    /// Auxiliary deliveries are normalized into delivery-ID order. Returns an
    /// error if any supplied view disagrees with the graph or metadata.
    ///
    /// This constructor does not replace the exhaustive durable recovery audit.
    pub fn from_audited_state(
        graph: MemHeaderStore,
        metadata: EngineMetadata,
        selected: Vec<Frontier>,
        verified: Vec<Frontier>,
        deliveries: impl IntoIterator<Item = AuxDelivery>,
    ) -> Result<Self, EngineHydrationError> {
        if graph.finalized_frontier() != metadata.frontiers.finalized {
            return Err(EngineHydrationError::Incoherent(
                "graph finality disagrees with metadata",
            ));
        }
        verify_projection(&graph, &selected, metadata.frontiers.header_best, false)?;
        verify_projection(
            &graph,
            &verified,
            metadata.frontiers.verified_best,
            metadata.mode == crate::EngineMode::Integrated,
        )?;
        if metadata.mode == crate::EngineMode::HeadersOnly
            && verified.as_slice() != [metadata.frontiers.finalized]
        {
            return Err(EngineHydrationError::Incoherent(
                "headers-only verified projection extends past finality",
            ));
        }
        let (selected_frontier, score) = graph.select_best_header_chain()?;
        if selected_frontier != metadata.frontiers.header_best
            || score != metadata.header_best_score
        {
            return Err(EngineHydrationError::Incoherent(
                "selected frontier or score disagrees with graph",
            ));
        }

        let mut aux_deliveries: HashMap<_, Vec<_>> = HashMap::new();
        let mut delivery_ids = HashSet::new();
        for delivery in deliveries {
            let node =
                graph
                    .header_node(delivery.header_hash)
                    .ok_or(EngineHydrationError::Incoherent(
                        "auxiliary delivery has no retained header",
                    ))?;
            if !delivery_ids.insert(delivery.delivery_id)
                || !node.aux_delivery_ids.contains(&delivery.delivery_id)
            {
                return Err(EngineHydrationError::Incoherent(
                    "auxiliary delivery index disagrees with graph",
                ));
            }
            aux_deliveries
                .entry(delivery.header_hash)
                .or_default()
                .push(delivery);
        }
        for node in graph.header_nodes() {
            if node.aux_delivery_ids.iter().any(|delivery_id| {
                !aux_deliveries
                    .get(&node.hash)
                    .is_some_and(|rows| rows.iter().any(|row| row.delivery_id == *delivery_id))
            }) {
                return Err(EngineHydrationError::Incoherent(
                    "graph auxiliary index has no delivery",
                ));
            }
        }
        for rows in aux_deliveries.values_mut() {
            rows.sort_unstable_by_key(|delivery| delivery.delivery_id);
        }

        Ok(Self {
            graph,
            metadata,
            selected_projection: selected,
            verified_projection: verified,
            aux_deliveries,
        })
    }

    /// Plan one verified transition without mutating this engine.
    ///
    /// Returns a durable write set and the exact post-transition snapshot that
    /// the runtime may commit and then install. This method never writes durable
    /// state or publishes watches.
    pub fn plan_transition(
        &self,
        input: TransitionInput,
        context: &TransitionContext<'_>,
    ) -> Result<EngineTransition, TransitionFailure> {
        let plan = derive_transition_plan(self, input, context)?;
        Ok(EngineTransition { plan })
    }

    /// Install a verified transition after its durable batch has committed.
    ///
    /// The caller must have already persisted [`EngineTransition::change_set`].
    /// Returns [`CommittedTransitionError::StaleSource`] when another transition
    /// changed this engine after planning. On success, in-memory graph,
    /// projections, metadata, and auxiliary deliveries match the committed
    /// write set.
    pub fn install_committed_transition(
        &mut self,
        transition: EngineTransition,
    ) -> Result<(), CommittedTransitionError> {
        if self.snapshot() != *transition.before() {
            return Err(CommittedTransitionError::StaleSource);
        }
        self.apply_verified_plan(&transition.plan)?;
        Ok(())
    }

    fn apply_verified_plan(&mut self, plan: &TransitionPlan) -> Result<(), GraphError> {
        self.graph.apply_delta(plan.graph_delta())?;
        self.metadata = plan.change_set().metadata.clone();
        apply_delta_to_projection(
            &mut self.selected_projection,
            &plan.change_set().selected_projection,
        );
        apply_delta_to_projection(
            &mut self.verified_projection,
            &plan.change_set().verified_projection,
        );
        apply_aux_delta(&mut self.aux_deliveries, &plan.change_set().aux_changes);
        Ok(())
    }

    /// Return the atomic externally meaningful snapshot.
    pub fn snapshot(&self) -> EngineSnapshot {
        self.metadata.snapshot()
    }

    /// Return complete engine metadata.
    pub const fn metadata(&self) -> &EngineMetadata {
        &self.metadata
    }

    /// Return the engine-owned retained graph.
    pub const fn graph(&self) -> &MemHeaderStore {
        &self.graph
    }

    /// This method returns the frontier for each selected chain prefix from finality to the tip.
    pub fn selected_projection(&self) -> &[Frontier] {
        &self.selected_projection
    }

    /// This method returns the frontier for each verified chain prefix from finality to the tip.
    pub fn verified_projection(&self) -> &[Frontier] {
        &self.verified_projection
    }

    /// Return bounded auxiliary deliveries for one retained header.
    pub fn aux_deliveries(&self, hash: block::Hash) -> &[AuxDelivery] {
        self.aux_deliveries
            .get(&hash)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    pub(crate) fn aux_delivery_count(&self) -> usize {
        self.aux_deliveries.values().map(Vec::len).sum()
    }

    pub(crate) fn aux_delivery(&self, delivery_id: crate::EvidenceId) -> Option<&AuxDelivery> {
        self.aux_deliveries
            .values()
            .flatten()
            .find(|delivery| delivery.delivery_id == delivery_id)
    }
}

/// A verified durable write set ready for one post-commit in-memory installation.
#[derive(Clone, Debug)]
pub struct EngineTransition {
    plan: TransitionPlan,
}

impl EngineTransition {
    /// Return the coherent state observed before planning.
    pub const fn before(&self) -> &EngineSnapshot {
        self.plan.before()
    }

    /// Return the coherent state that exists after this transition commits.
    pub fn after(&self) -> EngineSnapshot {
        self.plan.change_set().metadata.snapshot()
    }

    /// Return the atomic durable write set.
    pub const fn change_set(&self) -> &ChangeSet {
        self.plan.change_set()
    }

    /// Return true when admission produced no durable effects.
    ///
    /// This covers exact adjacent replay, already-applied work, immediately
    /// evicted insertions, and other zero-effect admissions.
    pub fn is_no_change(&self) -> bool {
        self.plan.is_no_change()
    }

    /// Return the submitted transition domain.
    pub const fn domain(&self) -> crate::TransitionDomain {
        self.plan.domain()
    }

    /// Return the orthogonal effects produced by this transition.
    pub const fn effect(&self) -> crate::TransitionEffect {
        self.plan.effect()
    }

    #[cfg(any(test, feature = "fuzz-impl"))]
    pub(crate) fn into_plan(self) -> TransitionPlan {
        self.plan
    }
}

/// Verifies that a projection is a coherent, contiguous path through the graph.
///
/// The projection must begin at the graph's finalized frontier, end at `tip`,
/// reference graph nodes at matching heights, and advance one parent-linked
/// height at a time. When `require_verified_bodies` is set, every frontier
/// after finality must be recorded as accepted by full state.
///
/// Returns [`EngineHydrationError::Incoherent`] on the first violated
/// projection invariant.
fn verify_projection(
    graph: &MemHeaderStore,
    projection: &[Frontier],
    tip: Frontier,
    require_verified_bodies: bool,
) -> Result<(), EngineHydrationError> {
    if projection.first().copied() != Some(graph.finalized_frontier())
        || projection.last().copied() != Some(tip)
    {
        return Err(EngineHydrationError::Incoherent(
            "projection endpoints disagree with metadata",
        ));
    }

    for frontier in projection {
        let node = graph
            .header_node(frontier.hash)
            .filter(|node| node.height == frontier.height)
            .ok_or(EngineHydrationError::Incoherent(
                "projection frontier height disagrees with graph",
            ))?;
        if require_verified_bodies
            && *frontier != graph.finalized_frontier()
            && !matches!(
                node.body_validation_state,
                crate::BodyValidationState::Verified { .. }
            )
        {
            return Err(EngineHydrationError::Incoherent(
                "verified projection contains an unverified body",
            ));
        }
    }

    for pair in projection.windows(2) {
        if pair[1].height.0 != pair[0].height.0 + 1
            || graph
                .header_node(pair[1].hash)
                .is_none_or(|node| node.parent_hash != pair[0].hash)
        {
            return Err(EngineHydrationError::Incoherent(
                "projection is not a contiguous graph path",
            ));
        }
    }
    Ok(())
}

/// Applies a verified replacement to a height-ordered frontier projection.
///
/// Retires entries below `remove_before`, removes the old suffix beginning at
/// `remove_from`, then appends the replacement suffix from `put`.
///
/// Assumes transition planning has validated that `put` preserves ascending,
/// contiguous projection order.
fn apply_delta_to_projection(projection: &mut Vec<Frontier>, delta: &ProjectionDelta) {
    if let Some(height) = delta.remove_before {
        projection.retain(|frontier| frontier.height >= height);
    }
    if let Some(height) = delta.remove_from {
        projection.retain(|frontier| frontier.height < height);
    }
    projection.extend(delta.put.iter().copied());
}

/// Applies verified auxiliary-delivery changes to the in-memory index.
///
/// Changes are applied in order. A `Put` upserts by delivery ID within its
/// header bucket and keeps that bucket sorted. A `Delete` is a no-op when the
/// delivery is absent and removes the bucket when it becomes empty.
///
/// Assumes transition planning has validated retained headers and global
/// delivery-ID uniqueness.
fn apply_aux_delta(aux: &mut HashMap<block::Hash, Vec<AuxDelivery>>, changes: &[AuxDelta]) {
    for change in changes {
        match change {
            AuxDelta::Put(delivery) => {
                let rows = aux.entry(delivery.header_hash).or_default();
                rows.retain(|row| row.delivery_id != delivery.delivery_id);
                rows.push(**delivery);
                rows.sort_unstable_by_key(|row| row.delivery_id);
            }
            AuxDelta::Delete {
                header_hash,
                delivery_id,
            } => {
                if let Some(rows) = aux.get_mut(header_hash) {
                    rows.retain(|row| row.delivery_id != *delivery_id);
                    if rows.is_empty() {
                        aux.remove(header_hash);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU64, sync::Arc};

    use zakura_chain::block::genesis::regtest_genesis_block;

    use super::*;
    use crate::{
        AuxAuthentication, BodySizeHint, BodyValidationState, BranchId, HeaderGeneration,
        HeaderValidationState, HeaderWorkAuthority, InsertResult, SourceId,
    };

    fn graph_with_child() -> (MemHeaderStore, Frontier) {
        let genesis = regtest_genesis_block();
        let anchor = Frontier::new(block::Height(0), genesis.hash());
        let work = genesis
            .header
            .difficulty_threshold
            .to_work()
            .expect("the regtest target has valid work");
        let mut graph = MemHeaderStore::new(anchor, genesis.header.clone(), work, work.as_u256())
            .expect("the anchor is coherent");
        let mut header = *genesis.header;
        header.previous_block_hash = anchor.hash;
        header.nonce = [1; 32].into();
        let header = Arc::new(header);
        let child = match graph
            .insert(
                header,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the child inserts")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        };
        (graph, child)
    }

    fn delivery(
        delivery_id: crate::EvidenceId,
        header_hash: block::Hash,
        source: SourceId,
    ) -> AuxDelivery {
        let owner = HeaderWorkAuthority {
            header_generation: HeaderGeneration::new(0),
            branch: BranchId::new(header_hash, header_hash),
        }
        .bind(
            1,
            NonZeroU64::new(1).expect("the fixture request ID is nonzero"),
        );
        AuxDelivery {
            delivery_id,
            header_hash,
            source,
            owner: owner.into(),
            body_size: BodySizeHint::Unknown,
            tree_aux: None,
            authentication: AuxAuthentication::Unauthenticated,
        }
    }

    #[test]
    fn projection_validation_accepts_only_a_contiguous_graph_path() {
        let (graph, child) = graph_with_child();
        let anchor = graph.finalized_frontier();
        assert_eq!(
            verify_projection(&graph, &[anchor, child], child, false),
            Ok(())
        );
        assert_eq!(
            verify_projection(&graph, &[child], child, false),
            Err(EngineHydrationError::Incoherent(
                "projection endpoints disagree with metadata"
            ))
        );
        assert_eq!(
            verify_projection(&graph, &[anchor, child], anchor, false),
            Err(EngineHydrationError::Incoherent(
                "projection endpoints disagree with metadata"
            ))
        );
        assert_eq!(
            verify_projection(&graph, &[anchor, anchor, child], child, false),
            Err(EngineHydrationError::Incoherent(
                "projection is not a contiguous graph path"
            ))
        );
        assert_eq!(
            verify_projection(&graph, &[anchor, child], child, true),
            Err(EngineHydrationError::Incoherent(
                "verified projection contains an unverified body"
            ))
        );
    }

    #[test]
    fn projection_delta_retires_prefix_and_replaces_suffix() {
        let hashes = [
            block::Hash([1; 32]),
            block::Hash([2; 32]),
            block::Hash([3; 32]),
            block::Hash([4; 32]),
        ];
        let mut projection = vec![
            Frontier::new(block::Height(1), hashes[0]),
            Frontier::new(block::Height(2), hashes[1]),
            Frontier::new(block::Height(3), hashes[2]),
        ];
        apply_delta_to_projection(
            &mut projection,
            &ProjectionDelta {
                remove_before: Some(block::Height(2)),
                remove_from: Some(block::Height(3)),
                put: vec![Frontier::new(block::Height(3), hashes[3])],
            },
        );
        assert_eq!(
            projection,
            vec![
                Frontier::new(block::Height(2), hashes[1]),
                Frontier::new(block::Height(3), hashes[3]),
            ]
        );
    }

    #[test]
    fn auxiliary_delta_upserts_sorts_and_removes_empty_buckets() {
        let hash = block::Hash([0x11; 32]);
        let first_id = crate::EvidenceId::from_digest([1; 32]);
        let second_id = crate::EvidenceId::from_digest([2; 32]);
        let original = delivery(first_id, hash, SourceId::from_digest([3; 32]));
        let replacement = delivery(first_id, hash, SourceId::from_digest([4; 32]));
        let second = delivery(second_id, hash, SourceId::from_digest([5; 32]));
        let mut aux = HashMap::from([(hash, vec![original])]);

        apply_aux_delta(
            &mut aux,
            &[
                AuxDelta::Put(Box::new(second)),
                AuxDelta::Put(Box::new(replacement)),
            ],
        );
        assert_eq!(aux[&hash], vec![replacement, second]);

        apply_aux_delta(
            &mut aux,
            &[
                AuxDelta::Delete {
                    header_hash: hash,
                    delivery_id: first_id,
                },
                AuxDelta::Delete {
                    header_hash: hash,
                    delivery_id: second_id,
                },
            ],
        );
        assert!(!aux.contains_key(&hash));
    }
}
