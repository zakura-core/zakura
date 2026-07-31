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

/// The narrow durable authority needed by one transition.
#[derive(Clone, Debug, Default)]
pub enum DurableTransitionFacts {
    /// This event needs no fact outside the coherent engine state.
    #[default]
    None,
    /// Detached branch context retained until the R5 preparation-boundary decision.
    ValidationContext(ValidationLease),
    /// The exact preserved migration pin, when the requested pin is in durable finality history.
    MigratedFinalityPin(Option<Frontier>),
}

/// One coherent in-memory owner for transition planning state.
#[derive(Clone, Debug)]
pub struct HeaderChainEngine {
    graph: MemHeaderStore,
    metadata: EngineMetadata,
    selected: Vec<Frontier>,
    verified: Vec<Frontier>,
    aux: HashMap<block::Hash, Vec<AuxDelivery>>,
}

impl HeaderChainEngine {
    /// Hydrate state that has already passed the exhaustive durable recovery audit.
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
        verify_projection(&graph, &selected, metadata.frontiers.header_best)?;
        verify_projection(&graph, &verified, metadata.frontiers.verified_best)?;
        let (_, score) = graph.select_header_best()?;
        if score != metadata.header_best_score {
            return Err(EngineHydrationError::Incoherent(
                "selected score disagrees with graph",
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
        let mut projected = Self {
            graph: plan.projected_graph().clone(),
            metadata: plan.change_set().metadata.clone(),
            selected: self.selected.clone(),
            verified: self.verified.clone(),
            aux: self.aux.clone(),
        };
        apply_projection_delta(
            &mut projected.selected,
            &plan.change_set().selected_projection,
        );
        apply_projection_delta(
            &mut projected.verified,
            &plan.change_set().verified_projection,
        );
        apply_aux_delta(&mut projected.aux, &plan.change_set().aux_changes);
        Ok(EngineTransition { plan, projected })
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
}

/// A verified durable write set paired with its complete projected engine state.
#[derive(Clone, Debug)]
pub struct EngineTransition {
    plan: TransitionPlan,
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

    /// Consume the verified transition and return its complete projected engine state.
    pub fn into_projected_engine(self) -> HeaderChainEngine {
        self.projected
    }

    #[cfg(any(test, feature = "fuzz-impl"))]
    pub(crate) fn into_plan(self) -> TransitionPlan {
        self.plan
    }
}

fn verify_projection(
    graph: &MemHeaderStore,
    projection: &[Frontier],
    tip: Frontier,
) -> Result<(), EngineHydrationError> {
    if projection.first().copied() != Some(graph.finalized())
        || projection.last().copied() != Some(tip)
    {
        return Err(EngineHydrationError::Incoherent(
            "projection endpoints disagree with metadata",
        ));
    }
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

fn apply_projection_delta(projection: &mut Vec<Frontier>, delta: &ProjectionDelta) {
    if let Some(height) = delta.remove_before {
        projection.retain(|frontier| frontier.height >= height);
    }
    if let Some(height) = delta.remove_from {
        projection.retain(|frontier| frontier.height < height);
    }
    projection.extend(delta.put.iter().copied());
}

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
