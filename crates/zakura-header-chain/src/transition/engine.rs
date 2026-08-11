//! Stateful ownership boundary for coherent header-chain planning.

use std::collections::{HashMap, HashSet};

use thiserror::Error;
use zakura_chain::block;

use crate::{
    AuxDelivery, AuxDelta, ChangeSet, EngineMetadata, EngineSnapshot, Frontier, GraphError,
    MemHeaderStore, ProjectionDelta, TransitionContext, TransitionFailure, TransitionPlan,
    TransitionRequest, ValidationLease,
};

use super::planner::apply_transition_engine;

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

/// The narrow durable authority needed by one transition.
#[derive(Clone, Debug, Default)]
pub enum DurableTransitionFacts {
    /// This event needs no fact outside the coherent engine state.
    #[default]
    None,
    /// Durable context and finality provenance for one header insertion.
    HeaderInsertion {
        /// Exact predecessor leases available for the original and rebased parents.
        validation_contexts: Vec<ValidationLease>,
        /// Contiguous finality records from the work's stable anchor to current finality.
        finality_path: Vec<crate::FinalityRecord>,
    },
    /// The exact preserved migration pin, if durable finality history contains the requested pin.
    MigratedFinalityPin(Option<Frontier>),
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
    // Full header graph
    graph: MemHeaderStore,
    metadata: EngineMetadata,
    // Best header chain
    selected: Vec<Frontier>,
    // Header chain verified by block bodies
    verified: Vec<Frontier>,
    // Auxiliary deliveries for retained headers
    aux: HashMap<block::Hash, Vec<AuxDelivery>>,
}

impl HeaderChainEngine {
    /// Constructs an engine from state produced by a successful durable recovery audit.
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
        if graph.finalized() != metadata.frontiers.finalized {
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
        let (selected_frontier, score) = graph.select_header_best()?;
        if selected_frontier != metadata.frontiers.header_best
            || score != metadata.header_best_score
        {
            return Err(EngineHydrationError::Incoherent(
                "selected frontier or score disagrees with graph",
            ));
        }

        let mut aux: HashMap<_, Vec<_>> = HashMap::new();
        let mut delivery_ids = HashSet::new();
        for delivery in deliveries {
            let node = graph
                .node(delivery.header_hash)
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
            aux.entry(delivery.header_hash).or_default().push(delivery);
        }
        for node in graph.nodes() {
            if node.aux_delivery_ids.iter().any(|delivery_id| {
                !aux.get(&node.hash)
                    .is_some_and(|rows| rows.iter().any(|row| row.delivery_id == *delivery_id))
            }) {
                return Err(EngineHydrationError::Incoherent(
                    "graph auxiliary index has no delivery",
                ));
            }
        }
        for rows in aux.values_mut() {
            rows.sort_unstable_by_key(|delivery| delivery.delivery_id);
        }

        Ok(Self {
            graph,
            metadata,
            selected,
            verified,
            aux,
        })
    }

    /// Derive and verify one complete projected engine state without mutating this engine.
    pub fn apply(
        &self,
        request: TransitionRequest,
        context: &TransitionContext<'_>,
        durable: DurableTransitionFacts,
    ) -> Result<EngineTransition, TransitionFailure> {
        let plan = apply_transition_engine(self, &durable, request, context)?;
        #[cfg(test)]
        let projected = {
            let mut projected = self.clone();
            projected.apply_verified_plan(&plan)?;
            projected
        };
        Ok(EngineTransition {
            plan,
            #[cfg(test)]
            projected,
        })
    }

    /// Apply a verified graph delta after its durable batch has committed.
    pub fn apply_committed(
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
        apply_delta_to_projection(&mut self.selected, &plan.change_set().selected_projection);
        apply_delta_to_projection(&mut self.verified, &plan.change_set().verified_projection);
        apply_aux_delta(&mut self.aux, &plan.change_set().aux_changes);
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

    /// Return the complete selected projection.
    pub fn selected_projection(&self) -> &[Frontier] {
        &self.selected
    }

    /// Return the complete verified projection.
    pub fn verified_projection(&self) -> &[Frontier] {
        &self.verified
    }

    /// Return bounded auxiliary deliveries for one retained header.
    pub fn aux_deliveries(&self, hash: block::Hash) -> &[AuxDelivery] {
        self.aux.get(&hash).map(Vec::as_slice).unwrap_or_default()
    }

    pub(crate) fn aux_delivery_count(&self) -> usize {
        self.aux.values().map(Vec::len).sum()
    }

    pub(crate) fn aux_delivery(&self, delivery_id: crate::EvidenceId) -> Option<&AuxDelivery> {
        self.aux
            .values()
            .flatten()
            .find(|delivery| delivery.delivery_id == delivery_id)
    }
}

/// A verified durable write set ready for one post-commit in-memory application.
#[derive(Clone, Debug)]
pub struct EngineTransition {
    plan: TransitionPlan,
    #[cfg(test)]
    projected: HeaderChainEngine,
}

impl EngineTransition {
    /// Return the coherent state observed before planning.
    pub const fn before(&self) -> &EngineSnapshot {
        self.plan.before()
    }

    /// Return the atomic durable write set.
    pub const fn change_set(&self) -> &ChangeSet {
        self.plan.change_set()
    }

    /// Return true when the request is an idempotent replay.
    pub fn is_no_change(&self) -> bool {
        self.plan.is_no_change()
    }

    /// Return the classified transition cause.
    pub const fn cause(&self) -> crate::TransitionCause {
        self.plan.cause()
    }

    /// Consume the verified transition and return its complete projected engine state.
    #[cfg(test)]
    pub fn into_projected_engine(self) -> HeaderChainEngine {
        self.projected
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
    if projection.first().copied() != Some(graph.finalized())
        || projection.last().copied() != Some(tip)
    {
        return Err(EngineHydrationError::Incoherent(
            "projection endpoints disagree with metadata",
        ));
    }

    // Verify that each frontier is a valid node in the graph and that the projection is contiguous.
    for frontier in projection {
        let node = graph
            .node(frontier.hash)
            .filter(|node| node.height == frontier.height)
            .ok_or(EngineHydrationError::Incoherent(
                "projection frontier height disagrees with graph",
            ))?;
        if require_verified_bodies
            && *frontier != graph.finalized()
            && !matches!(node.body_validation_state, crate::BodyValidationState::Verified { .. })
        {
            return Err(EngineHydrationError::Incoherent(
                "verified projection contains an unverified body",
            ));
        }
    }

    // Verify that the projection is contiguous.
    for pair in projection.windows(2) {
        if pair[1].height.0 != pair[0].height.0 + 1
            || graph
                .node(pair[1].hash)
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
