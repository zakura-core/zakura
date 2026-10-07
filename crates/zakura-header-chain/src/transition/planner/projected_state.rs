//! Cohesive mutable projection of graph, verified path, and auxiliary deltas.

use super::{
    InvalidTransitionEvidence, PlannerCoherenceViolation, ProjectionKind, TransitionFailure,
};
use std::{
    borrow::Cow,
    cmp::Reverse,
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
};

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
    repair_deliveries: HashSet<EvidenceId>,
    verified_selection_dirty: bool,
}

impl<'a> ProjectedTransitionState<'a> {
    /// Borrow the engine's graph and verified projection as the starting point.
    pub(super) fn new(engine: &'a HeaderChainEngine) -> Self {
        Self {
            graph: GraphOverlay::new(engine.graph()),
            verified: Cow::Borrowed(engine.verified_projection()),
            aux_changes: Vec::new(),
            repair_deliveries: HashSet::new(),
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

    /// Drop evicted bodies outside the surviving full-state verified path.
    pub(super) fn forget_evicted_bodies(
        &mut self,
        hashes: &[block::Hash],
    ) -> Result<(), TransitionFailure> {
        for &hash in hashes {
            if self.graph.view_header_node(hash).is_some_and(|node| {
                matches!(
                    node.body_validation_state,
                    BodyValidationState::Verified { .. }
                )
            }) {
                self.set_body_validation_state(hash, BodyValidationState::Unknown)?;
            }
        }
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
        selected_repair: bool,
    ) -> Result<usize, TransitionFailure> {
        self.graph
            .edit_record_auxiliary_evidence_delivery(delivery.header_hash, delivery.delivery_id)?;
        if selected_repair {
            self.repair_deliveries.insert(delivery.delivery_id);
        }
        Ok(self.update_aux_delivery(delivery))
    }

    /// Stage an updated auxiliary delivery row and return its batch-local index.
    pub(super) fn update_aux_delivery(&mut self, delivery: crate::AuxDelivery) -> usize {
        let index = self.aux_changes.len();
        self.aux_changes.push(AuxDelta::Put(Box::new(delivery)));
        index
    }

    /// Read a staged row while header admission coalesces semantic duplicates.
    pub(super) fn staged_aux_delivery(&self, index: usize) -> crate::AuxDelivery {
        let AuxDelta::Put(delivery) = &self.aux_changes[index] else {
            unreachable!("admission indices refer to staged delivery puts");
        };
        **delivery
    }

    /// Replace a staged correction without adding a second write for its evidence ID.
    pub(super) fn replace_staged_aux_delivery(
        &mut self,
        index: usize,
        delivery: crate::AuxDelivery,
    ) {
        self.aux_changes[index] = AuxDelta::Put(Box::new(delivery));
    }

    /// Replace non-authoritative input when its retained header's bucket is full.
    ///
    /// Returns false when the bucket stays full. The caller then drops the new input and still
    /// admits its header, because auxiliary input is advisory.
    pub(super) fn make_aux_delivery_room(
        &mut self,
        engine: &HeaderChainEngine,
        hash: block::Hash,
        limits: EngineLimits,
        selected_repair: bool,
        rooted: bool,
    ) -> Result<bool, TransitionFailure> {
        let node = self
            .graph
            .view_header_node(hash)
            .ok_or(GraphError::UnknownHeaderNode(hash))?;
        if node.aux_delivery_ids.len() < limits.max_aux_deliveries_per_header.get() {
            return Ok(true);
        }
        // Input retention grants no header or root authority. A selected repair may
        // replace an unchecked candidate, including recovered rows whose outcome claims
        // recovery discarded. Ordinary delivery cannot displace unchecked candidates.
        let Some(replaceable) = engine
            .aux_deliveries(hash)
            .iter()
            .filter(|delivery| {
                delivery.is_rejected()
                    || delivery.is_disputed()
                    || (selected_repair && delivery.is_unauthenticated())
            })
            // A size hint cannot replace a usable root candidate, including disputed roots.
            .filter(|delivery| rooted || delivery.tree_aux.is_none() || delivery.is_rejected())
            .filter(|delivery| node.aux_delivery_ids.contains(&delivery.delivery_id))
            .min_by_key(|delivery| {
                (
                    delivery.is_unauthenticated(),
                    !delivery.is_rejected(),
                    delivery.delivery_id,
                )
            })
        else {
            return Ok(false);
        };
        self.graph
            .remove_auxiliary_evidence_delivery(hash, replaceable.delivery_id)?;
        self.aux_changes.push(AuxDelta::Delete {
            header_hash: hash,
            delivery_id: replaceable.delivery_id,
        });
        Ok(true)
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

    /// Reselect after operator policy changes, with evicted bodies already removed.
    pub(super) fn refresh_verified_selection(&mut self) -> Result<(), TransitionFailure> {
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

    /// Enforce retention against the projected graph, then the aggregate auxiliary limit.
    pub(super) fn enforce_retention(
        &mut self,
        engine: &HeaderChainEngine,
        header_best: Frontier,
        retention_references: impl IntoIterator<Item = zakura_chain::block::Hash>,
        limits: EngineLimits,
    ) -> Result<RetentionPlan, TransitionFailure> {
        let verified_best = self
            .verified
            .last()
            .copied()
            .unwrap_or_else(|| self.graph.view_finalized_frontier());
        let plan = super::retention::enforce_retention(
            &mut self.graph,
            header_best,
            verified_best,
            retention_references,
            limits,
        )?;
        if !plan.admission_refused {
            self.evict_auxiliary_input(engine, header_best, limits)?;
        }
        Ok(plan)
    }

    /// Count auxiliary rows indexed by retained headers after this transition's edits.
    fn retained_aux_delivery_count(&self, engine: &HeaderChainEngine) -> usize {
        let delta = self.graph.delta();
        let removed_with_headers = delta
            .deleted_header_hashes()
            .iter()
            .map(|hash| engine.aux_deliveries(*hash).len())
            .sum::<usize>();
        let mut retained = engine
            .aux_delivery_count()
            .saturating_sub(removed_with_headers);
        for change in &self.aux_changes {
            match change {
                AuxDelta::Put(delivery)
                    if engine.aux_delivery(delivery.delivery_id).is_none()
                        && self
                            .graph
                            .view_header_node(delivery.header_hash)
                            .is_some_and(|node| {
                                node.aux_delivery_ids.contains(&delivery.delivery_id)
                            }) =>
                {
                    retained = retained.saturating_add(1);
                }
                AuxDelta::Delete { header_hash, .. }
                    if self.graph.view_header_node(*header_hash).is_some() =>
                {
                    retained = retained.saturating_sub(1);
                }
                _ => {}
            }
        }
        retained
    }

    /// Evict the lowest-priority auxiliary input until the aggregate limit holds.
    ///
    /// Auxiliary input is advisory, so aggregate pressure removes input rows and never headers.
    /// Eviction never removes authenticated input or input for the finalized header and its two
    /// selected successors, which the next commit needs. Among the remaining rows, it removes
    /// input off the selected path first. It then takes one row at a time from the fullest
    /// bucket, so many suppliers flooding a few headers cannot displace single honest rows.
    /// Ties go to the highest header, which the committer needs last. Within a bucket, rejected
    /// input goes first, then disputed, then input without roots, then unchecked roots.
    /// Rows admitted by this transition compete on the same terms, so low-priority new input is
    /// dropped rather than refusing the transition.
    fn evict_auxiliary_input(
        &mut self,
        engine: &HeaderChainEngine,
        header_best: Frontier,
        limits: EngineLimits,
    ) -> Result<(), TransitionFailure> {
        let mut retained = self.retained_aux_delivery_count(engine);
        if retained <= limits.max_aux_deliveries_total.get() {
            return Ok(());
        }

        let finalized = self.graph.view_finalized_frontier();
        let mut selected = HashSet::new();
        let mut commit_window = HashSet::new();
        let mut cursor = Some(header_best.hash);
        while let Some(hash) = cursor {
            let node = self
                .graph
                .view_header_node(hash)
                .ok_or(GraphError::UnknownHeaderNode(hash))?;
            selected.insert(hash);
            if node.height.0.saturating_sub(finalized.height.0) < 3 {
                commit_window.insert(hash);
            }
            cursor = (hash != finalized.hash).then_some(node.parent_hash);
        }

        let staged: HashMap<EvidenceId, crate::AuxDelivery> = self
            .aux_changes
            .iter()
            .filter_map(|change| match change {
                AuxDelta::Put(delivery) => Some((delivery.delivery_id, **delivery)),
                AuxDelta::Delete { .. } => None,
            })
            .collect();
        let mut holders: Vec<block::Hash> = engine.aux_delivery_header_hashes().collect();
        holders.extend(staged.values().map(|delivery| delivery.header_hash));
        holders.sort_unstable_by_key(|hash| hash.0);
        holders.dedup();

        // Each bucket lists its evictable rows in eviction order, last first for `pop`.
        let mut buckets: HashMap<block::Hash, (usize, Vec<(u8, EvidenceId)>)> = HashMap::new();
        let mut queue = BTreeSet::new();
        for hash in holders {
            if commit_window.contains(&hash) {
                continue;
            }
            let Some(node) = self.graph.view_header_node(hash) else {
                continue;
            };
            let mut evictable: Vec<(u8, EvidenceId)> = node
                .aux_delivery_ids
                .iter()
                .filter_map(|delivery_id| {
                    // A repair must retain its new input before it can report success.
                    if self.repair_deliveries.contains(delivery_id) {
                        return None;
                    }
                    let delivery = staged
                        .get(delivery_id)
                        .or_else(|| engine.aux_delivery(*delivery_id))?;
                    let rank = if delivery.is_authenticated() {
                        return None;
                    } else if delivery.is_rejected() {
                        0
                    } else if delivery.is_disputed() {
                        1
                    } else if delivery.tree_aux.is_none() {
                        2
                    } else {
                        3
                    };
                    Some((rank, *delivery_id))
                })
                .collect();
            if evictable.is_empty() {
                continue;
            }
            evictable.sort_unstable_by(|left, right| right.cmp(left));
            let occupancy = node.aux_delivery_ids.len();
            let key = (
                selected.contains(&hash),
                Reverse(occupancy),
                Reverse(node.height),
                hash.0,
            );
            queue.insert(key);
            buckets.insert(hash, (occupancy, evictable));
        }

        while retained > limits.max_aux_deliveries_total.get() {
            let Some((on_selected, _, height, raw_hash)) = queue.pop_first() else {
                // Protected input alone exceeds the limit; the final limit check refuses.
                return Ok(());
            };
            let hash = block::Hash(raw_hash);
            let (occupancy, evictable) = buckets
                .get_mut(&hash)
                .expect("every queued header has an evictable bucket");
            let (_, delivery_id) = evictable
                .pop()
                .expect("queued buckets hold at least one evictable row");
            self.graph
                .remove_auxiliary_evidence_delivery(hash, delivery_id)?;
            if engine.aux_delivery(delivery_id).is_some() {
                self.aux_changes.push(AuxDelta::Delete {
                    header_hash: hash,
                    delivery_id,
                });
            }
            retained = retained.saturating_sub(1);
            *occupancy = occupancy.saturating_sub(1);
            if !evictable.is_empty() {
                queue.insert((on_selected, Reverse(*occupancy), height, raw_hash));
            }
        }
        Ok(())
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
            // A later delivery can replace a row whose size hint this batch updated.
            AuxDelta::Put(delivery) => self
                .graph
                .view_header_node(delivery.header_hash)
                .is_some_and(|node| node.aux_delivery_ids.contains(&delivery.delivery_id)),
            AuxDelta::Delete { header_hash, .. } => !evicted.contains(header_hash),
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
    /// Validate auxiliary bounds against the exact post-retention projection.
    pub(super) fn validate_auxiliary_limits(
        &self,
        engine: &HeaderChainEngine,
        limits: EngineLimits,
    ) -> Result<(), TransitionFailure> {
        let deleted = self
            .aux_changes
            .iter()
            .filter(|change| matches!(change, AuxDelta::Delete { .. }))
            .count();
        let inserted = self
            .aux_changes
            .iter()
            .filter_map(|change| match change {
                AuxDelta::Put(delivery) => Some(delivery),
                AuxDelta::Delete { .. } => None,
            })
            .filter(|delivery| engine.aux_delivery(delivery.delivery_id).is_none())
            .count();
        let projected_total = engine
            .aux_delivery_count()
            .saturating_sub(deleted)
            .saturating_add(inserted);
        if projected_total > limits.max_aux_deliveries_total.get() {
            return Err(TransitionFailure::AuxiliaryLimitExceeded);
        }
        for delivery in self.aux_changes.iter().filter_map(|change| match change {
            AuxDelta::Put(delivery) => Some(delivery),
            AuxDelta::Delete { .. } => None,
        }) {
            let count = self
                .graph
                .view_header_node(delivery.header_hash)
                .map_or(0, |node| node.aux_delivery_ids.len());
            if count > limits.max_aux_deliveries_per_header.get() {
                return Err(TransitionFailure::AuxiliaryLimitExceeded);
            }
        }
        Ok(())
    }

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
