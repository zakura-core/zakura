//! Cohesive mutable projection of graph, verified path, and auxiliary deltas.

use super::{
    InvalidTransitionEvidence, PlannerCoherenceViolation, ProjectionKind, TransitionFailure,
};
use std::{borrow::Cow, collections::HashSet, sync::Arc};

use zakura_chain::block;

use crate::graph::{GraphDelta, GraphOverlay, HeaderGraphEdit, HeaderGraphView};
use crate::{
    AuxDelta, BodyValidationState, EligibilityReason, EngineLimits, EvidenceId, Frontier,
    GraphError, HeaderChainEngine, HeaderValidationState, InsertResult, OperatorInvalidationId,
};

use super::retention::RetentionPlan;

/// Mutable projected state accumulated while applying one transition event.
pub(super) struct ProjectedTransitionState<'a> {
    graph: GraphOverlay<'a>,
    verified: Cow<'a, [Frontier]>,
    aux_changes: Vec<AuxDelta>,
    verified_selection_dirty: bool,
}

impl<'a> ProjectedTransitionState<'a> {
    /// Borrow the engine's graph and verified projection as the starting point.
    pub(super) fn new(engine: &'a HeaderChainEngine) -> Self {
        Self {
            graph: GraphOverlay::new(engine.graph()),
            verified: Cow::Borrowed(engine.verified_projection()),
            aux_changes: Vec::new(),
            verified_selection_dirty: false,
        }
    }

    /// Borrow the projected graph.
    pub(super) fn graph(&self) -> &GraphOverlay<'a> {
        &self.graph
    }

    /// Borrow the projected verified path.
    pub(super) fn verified(&self) -> &[Frontier] {
        &self.verified
    }

    /// Replace the verified path with one finalized-rooted frontier.
    pub(super) fn reset_verified(&mut self, frontier: Frontier) {
        self.verified = Cow::Owned(vec![frontier]);
    }

    /// Extend the verified path by one validated frontier.
    pub(super) fn push_verified(&mut self, frontier: Frontier) {
        self.verified.to_mut().push(frontier);
    }

    /// Insert one admitted header into the projected graph.
    pub(super) fn insert_header(
        &mut self,
        header: Arc<block::Header>,
        validation: HeaderValidationState,
        direct_reasons: Vec<EligibilityReason>,
        body_validation_state: BodyValidationState,
    ) -> Result<InsertResult, TransitionFailure> {
        Ok(self.graph.edit_insert_header(
            header,
            validation,
            direct_reasons,
            body_validation_state,
        )?)
    }

    /// Replace one retained header's body-validation state.
    pub(super) fn set_body_validation_state(
        &mut self,
        hash: block::Hash,
        body_validation_state: BodyValidationState,
    ) -> Result<(), TransitionFailure> {
        self.graph
            .edit_set_body_validation_state(hash, body_validation_state)?;
        Ok(())
    }

    /// Replace one retained header's time-dependent validation state.
    pub(super) fn set_header_validation_state(
        &mut self,
        hash: block::Hash,
        validation: HeaderValidationState,
    ) -> Result<(), TransitionFailure> {
        self.graph
            .edit_set_header_validation_state(hash, validation)?;
        Ok(())
    }

    /// Record one auxiliary delivery and stage its durable row together.
    pub(super) fn record_aux_delivery(
        &mut self,
        delivery: crate::AuxDelivery,
    ) -> Result<(), TransitionFailure> {
        self.graph
            .edit_record_auxiliary_evidence_delivery(delivery.header_hash, delivery.delivery_id)?;
        self.aux_changes.push(AuxDelta::Put(Box::new(delivery)));
        Ok(())
    }

    /// Stage an updated auxiliary delivery row.
    pub(super) fn update_aux_delivery(&mut self, delivery: crate::AuxDelivery) {
        self.aux_changes.push(AuxDelta::Put(Box::new(delivery)));
    }

    /// Add an operator invalidation and dirty verified selection when it changes state.
    pub(super) fn add_operator_invalidation(
        &mut self,
        target: block::Hash,
        reason: EligibilityReason,
    ) -> Result<(), TransitionFailure> {
        if self
            .graph
            .edit_add_header_eligibility_reason(target, reason)?
        {
            self.verified_selection_dirty = true;
        }
        Ok(())
    }

    /// Remove an operator invalidation and dirty verified selection when it changes state.
    pub(super) fn remove_operator_invalidation(
        &mut self,
        target: block::Hash,
        id: OperatorInvalidationId,
        evidence: Option<EvidenceId>,
    ) -> Result<(), TransitionFailure> {
        if self
            .graph
            .edit_remove_header_operator_invalidation(target, id, evidence)?
        {
            self.verified_selection_dirty = true;
        }
        Ok(())
    }

    /// Reselect the strongest fully verified eligible path when operator policy dirtied it.
    pub(super) fn refresh_verified_after_operator_change(
        &mut self,
    ) -> Result<(), TransitionFailure> {
        if self.verified_selection_dirty {
            self.verified = Cow::Owned(select_fully_verified_path(&self.graph)?);
            self.verified_selection_dirty = false;
        }
        Ok(())
    }

    /// Advance finality and trim the verified projection to the new anchor.
    pub(super) fn advance_finality(
        &mut self,
        new_finalized: Frontier,
    ) -> Result<(), TransitionFailure> {
        self.graph.edit_advance_finalized_frontier(new_finalized)?;
        let verified = self.verified.to_mut();
        verified.retain(|frontier| frontier.height >= new_finalized.height);
        if verified.first().copied() != Some(new_finalized) {
            verified.insert(0, new_finalized);
        }
        Ok(())
    }

    /// Collapse verified state to finality in headers-only mode.
    pub(super) fn force_headers_only_verified(&mut self) {
        self.verified = Cow::Owned(vec![self.graph.view_finalized_frontier()]);
    }

    /// Enforce retention against the projected graph.
    pub(super) fn enforce_retention(
        &mut self,
        header_best: Frontier,
        protect_all_verified_body_paths: bool,
        retention_references: impl IntoIterator<Item = zakura_chain::block::Hash>,
        limits: EngineLimits,
    ) -> Result<RetentionPlan, TransitionFailure> {
        let verified_best = self
            .verified
            .last()
            .copied()
            .unwrap_or_else(|| self.graph.view_finalized_frontier());
        Ok(super::retention::enforce_retention(
            &mut self.graph,
            header_best,
            verified_best,
            protect_all_verified_body_paths,
            retention_references,
            limits,
        )?)
    }

    /// Trim the verified projection against the retained graph and reconcile auxiliary rows.
    pub(super) fn finish_after_retention(
        mut self,
        engine: &HeaderChainEngine,
    ) -> Result<SettledProjectedState<'a>, TransitionFailure> {
        self.verified = trim_projection(&self.graph, self.verified)?;
        let graph_delta = self.graph.delta();
        let evicted: HashSet<_> = graph_delta
            .deleted_header_hashes()
            .iter()
            .copied()
            .collect();
        self.aux_changes.retain(|change| match change {
            AuxDelta::Put(delivery) => self.graph.view_header_node(delivery.header_hash).is_some(),
            AuxDelta::Delete { .. } => true,
        });
        let mut aux_deletes: Vec<_> = evicted
            .iter()
            .flat_map(|hash| {
                engine
                    .aux_deliveries(*hash)
                    .iter()
                    .map(|delivery| (*hash, delivery.delivery_id))
            })
            .collect();
        // HashSet iteration is nondeterministic; match adjacent ChangeSet ordering.
        aux_deletes.sort_unstable_by_key(|(hash, delivery_id)| (hash.0, *delivery_id));
        for (header_hash, delivery_id) in aux_deletes {
            self.aux_changes.push(AuxDelta::Delete {
                header_hash,
                delivery_id,
            });
        }
        Ok(SettledProjectedState {
            graph: self.graph,
            graph_delta,
            verified: self.verified,
            aux_changes: self.aux_changes,
        })
    }

    /// Return true when event application rebased work coordinates.
    pub(super) fn work_coordinates_rebased(&self) -> bool {
        self.graph.work_coordinates_rebased()
    }
}

/// Fully settled projected state ready for write-set assembly.
pub(super) struct SettledProjectedState<'a> {
    graph: GraphOverlay<'a>,
    graph_delta: GraphDelta,
    verified: Cow<'a, [Frontier]>,
    aux_changes: Vec<AuxDelta>,
}

impl<'a> SettledProjectedState<'a> {
    /// Atomically expose the graph and its matching final delta to write derivation.
    pub(super) fn into_write_parts(
        self,
    ) -> (
        GraphOverlay<'a>,
        GraphDelta,
        Cow<'a, [Frontier]>,
        Vec<AuxDelta>,
    ) {
        (
            self.graph,
            self.graph_delta,
            self.verified,
            self.aux_changes,
        )
    }
}

/// Reconstruct the finalized-rooted path ending at `tip`.
pub(super) fn path<G: HeaderGraphView>(
    graph: &G,
    tip: Frontier,
) -> Result<Vec<Frontier>, TransitionFailure> {
    let finalized = graph.view_finalized_frontier();
    let mut path = Vec::new();
    let mut current = tip;
    loop {
        path.push(current);
        if current == finalized {
            break;
        }
        let node = graph
            .view_header_node(current.hash)
            .ok_or(GraphError::UnknownHeaderNode(current.hash))?;
        current = Frontier::new(
            current
                .height
                .previous()
                .map_err(|_| GraphError::FinalizedFrontierNotDescendant {
                    current: finalized.hash,
                    candidate: tip.hash,
                })?,
            node.parent_hash,
        );
    }
    path.reverse();
    Ok(path)
}

/// Select the strongest fully verified eligible path.
pub(super) fn select_fully_verified_path<G: HeaderGraphView>(
    graph: &G,
) -> Result<Vec<Frontier>, TransitionFailure> {
    let finalized = graph.view_finalized_frontier();
    let mut connected = HashSet::from([finalized.hash]);
    let mut nodes = graph.view_header_nodes();
    nodes.sort_unstable_by_key(|node| (node.height, node.hash.0));
    for node in nodes {
        if node.hash != finalized.hash
            && node.is_eligible()
            && matches!(
                node.body_validation_state,
                BodyValidationState::Verified { .. }
            )
            && connected.contains(&node.parent_hash)
        {
            connected.insert(node.hash);
        }
    }
    let tip = connected
        .into_iter()
        .map(|hash| {
            let node = graph
                .view_header_node(hash)
                .expect("verified candidates are retained graph nodes");
            graph
                .view_header_chain_score(hash)
                .map(|score| (score, Frontier::new(node.height, hash)))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max_by_key(|(score, _)| *score)
        .map(|(_, frontier)| frontier)
        .ok_or(GraphError::UnknownHeaderNode(finalized.hash))?;
    path(graph, tip)
}

/// Trim a projection so it starts at finality and only names retained nodes.
pub(super) fn trim_projection<'a, G: HeaderGraphView>(
    graph: &G,
    projection: Cow<'a, [Frontier]>,
) -> Result<Cow<'a, [Frontier]>, TransitionFailure> {
    let requires_trim = projection.first().copied() != Some(graph.view_finalized_frontier())
        || projection.iter().any(|frontier| {
            frontier.height < graph.view_finalized_frontier().height
                || graph.view_header_node(frontier.hash).is_none()
        });
    if !requires_trim {
        return Ok(projection);
    }
    let mut result: Vec<_> = projection
        .iter()
        .copied()
        .filter(|frontier| {
            frontier.height >= graph.view_finalized_frontier().height
                && graph.view_header_node(frontier.hash).is_some()
        })
        .collect();
    if result.first().copied() != Some(graph.view_finalized_frontier()) {
        result.insert(0, graph.view_finalized_frontier());
    }
    for pair in result.windows(2) {
        if pair[1].height.0 != pair[0].height.0 + 1
            || graph
                .view_header_node(pair[1].hash)
                .is_none_or(|node| node.parent_hash != pair[0].hash)
        {
            return Err(InvalidTransitionEvidence::Planner(
                PlannerCoherenceViolation::DiscontinuousProjection(ProjectionKind::Verified),
            )
            .into());
        }
    }
    Ok(Cow::Owned(result))
}
