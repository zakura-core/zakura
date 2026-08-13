//! Stateful ownership boundary for coherent header-chain planning.

mod input;
mod install;

use std::collections::{HashMap, HashSet};

use thiserror::Error;
use zakura_chain::block;

use crate::{
    AuxDelivery, EngineMetadata, EngineSnapshot, EngineTransition, GraphError, MemHeaderStore,
    TransitionContext, TransitionFailure,
};

use super::planner::derive_transition_plan;
use install::{merge_auxiliary_delivery_changes, merge_projection_delta, verify_projection};

pub use input::{HeaderInsertionFacts, HeaderValidationFacts, TransitionInput};

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
    #[error("committed header transition no longer matches its snapshot before commit")]
    StaleSource,
    /// The graph rejected the verified delta.
    #[error(transparent)]
    Graph(#[from] GraphError),
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
    selected_projection: Vec<crate::Frontier>,
    /// Body-verified path from finality to the verified tip.
    verified_projection: Vec<crate::Frontier>,
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
        selected: Vec<crate::Frontier>,
        verified: Vec<crate::Frontier>,
        deliveries: impl IntoIterator<Item = AuxDelivery>,
    ) -> Result<Self, EngineHydrationError> {
        if graph.finalized_frontier() != metadata.frontiers.finalized {
            return Err(EngineHydrationError::Incoherent(
                "graph finality disagrees with metadata",
            ));
        }
        if graph
            .header_nodes()
            .any(|node| node.work_coordinate().origin_hash() != metadata.work_origin.hash)
        {
            return Err(EngineHydrationError::Incoherent(
                "graph work origin disagrees with metadata",
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
    /// Callers own everything after this returns: persist
    /// [`EngineTransition::change_set`], then
    /// [`Self::install_committed_transition`], then publish
    /// [`EngineTransition::snapshot_after_commit`]. This method never writes durable state or
    /// publishes watches.
    ///
    /// # Returns
    ///
    /// - [`Ok`] — an invariant-verified [`EngineTransition`]. Inspect
    ///   [`EngineTransition::is_no_change`] and [`EngineTransition::effect`]
    ///   before assuming a mutation: valid evidence may produce no durable
    ///   change, or only a resource-stall alarm.
    /// - [`Err`] — [`TransitionFailure`]: reject with zero durable effects
    ///   (stale version/owner, invalid evidence, mode/authority refusal, etc.).
    ///   Do not confuse this with a verified no-change plan.
    ///
    /// # Install contract
    ///
    /// Install succeeds only if this engine's snapshot is still
    /// [`EngineTransition::snapshot_before_commit`]. Concurrent commits make the plan stale;
    /// re-plan against the current engine rather than forcing install.
    pub fn plan_transition(
        &self,
        input: TransitionInput,
        context: &TransitionContext<'_>,
    ) -> Result<EngineTransition, TransitionFailure> {
        derive_transition_plan(self, input, context)
    }

    /// Install a verified transition after its durable batch has committed.
    ///
    /// Callers must already have persisted [`EngineTransition::change_set`].
    /// This advances the in-memory engine only; it does not write durable state
    /// or publish watches—publish [`EngineTransition::snapshot_after_commit`] after success.
    ///
    /// # Returns
    ///
    /// - [`Ok`] — graph, projections, metadata, and auxiliary deliveries match
    ///   the committed write set and this engine is ready to publish.
    /// - [`Err`] — [`CommittedTransitionError::StaleSource`] if another
    ///   transition changed this engine after planning, or
    ///   [`CommittedTransitionError::Graph`] if the verified delta cannot apply.
    ///   On either error the engine is unchanged, but durable state may already
    ///   be ahead of memory; fail closed and recover from durable state.
    pub fn install_committed_transition(
        &mut self,
        transition: EngineTransition,
    ) -> Result<(), CommittedTransitionError> {
        if self.snapshot() != *transition.snapshot_before_commit() {
            return Err(CommittedTransitionError::StaleSource);
        }
        self.install_verified_plan(&transition)?;
        Ok(())
    }

    /// Install a verified plan's write set into this engine's in-memory state.
    ///
    /// Caller must have already checked the snapshot before commit; graph apply is the
    /// only fallible step and leaves the engine unchanged on error.
    fn install_verified_plan(&mut self, plan: &EngineTransition) -> Result<(), GraphError> {
        self.graph.apply_delta(plan.graph_delta())?;
        self.metadata = plan.change_set().metadata.clone();
        merge_projection_delta(
            &mut self.selected_projection,
            &plan.change_set().selected_projection,
        );
        merge_projection_delta(
            &mut self.verified_projection,
            &plan.change_set().verified_projection,
        );
        merge_auxiliary_delivery_changes(&mut self.aux_deliveries, &plan.change_set().aux_changes);
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
    pub fn selected_projection(&self) -> &[crate::Frontier] {
        &self.selected_projection
    }

    /// This method returns the frontier for each verified chain prefix from finality to the tip.
    pub fn verified_projection(&self) -> &[crate::Frontier] {
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
