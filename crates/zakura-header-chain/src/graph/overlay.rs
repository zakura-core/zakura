use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use zakura_chain::block;

use super::{
    ConsensusInvalidBodyTombstone, GraphError, GraphRevision, HeaderGraphEdit, HeaderGraphView,
    InsertResult, MemHeaderStore,
};
use crate::{
    BodyValidationState, ChainScore, EligibilityReason, EligibilityState, EvidenceId, Frontier,
    HeaderNode, HeaderValidationState, OperatorInvalidationId, WorkCoordinate, WorkCoordinateError,
};

/// The work-coordinate semantics of an opaque graph transition.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(super) enum WorkCoordinateTransition {
    /// Preserve every retained header's work coordinate.
    #[default]
    PreserveCoordinates,
    /// Rebase every retained work coordinate to the finalized frontier.
    RebaseToFinalizedFrontier,
}

/// One revision-bound transition from a committed header graph.
///
/// `GraphOverlay` creates this value after it validates and stages a complete
/// transition. `MemHeaderStore` accepts the transition only when its base graph
/// revision matches the current graph revision. The private fields prevent
/// callers from constructing partial or internally inconsistent transitions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GraphDelta {
    pub(super) base_revision: GraphRevision,
    pub(super) work_coordinate_transition: WorkCoordinateTransition,
    pub(super) finalized_frontier: Option<Frontier>,
    pub(super) updated_header_nodes: Vec<HeaderNode>,
    pub(super) deleted_header_hashes: Vec<block::Hash>,
    pub(super) new_consensus_invalid_body_tombstones: Vec<ConsensusInvalidBodyTombstone>,
}

impl GraphDelta {
    /// Build a no-change transition bound to the graph's current revision.
    pub(crate) fn empty(base_graph: &MemHeaderStore) -> Self {
        Self {
            base_revision: base_graph.graph_revision,
            work_coordinate_transition: WorkCoordinateTransition::PreserveCoordinates,
            finalized_frontier: None,
            updated_header_nodes: Vec::new(),
            deleted_header_hashes: Vec::new(),
            new_consensus_invalid_body_tombstones: Vec::new(),
        }
    }

    /// Return whether the transition changes no graph-owned fact.
    pub(crate) fn is_empty(&self) -> bool {
        self.finalized_frontier.is_none()
            && self.updated_header_nodes.is_empty()
            && self.deleted_header_hashes.is_empty()
            && self.new_consensus_invalid_body_tombstones.is_empty()
    }

    /// Return the new finalized frontier when the transition advances finality.
    pub(crate) const fn finalized_frontier(&self) -> Option<Frontier> {
        self.finalized_frontier
    }

    /// Return new and replaced header nodes in deterministic order.
    pub(crate) fn updated_header_nodes(&self) -> &[HeaderNode] {
        &self.updated_header_nodes
    }

    /// Return removed header hashes in deterministic order.
    pub(crate) fn deleted_header_hashes(&self) -> &[block::Hash] {
        &self.deleted_header_hashes
    }

    /// Return newly created append-only consensus-invalid body tombstones.
    pub(crate) fn new_consensus_invalid_body_tombstones(&self) -> &[ConsensusInvalidBodyTombstone] {
        &self.new_consensus_invalid_body_tombstones
    }

    /// Return whether the transition rebases every retained work coordinate.
    pub(crate) const fn rebases_work_coordinates(&self) -> bool {
        matches!(
            self.work_coordinate_transition,
            WorkCoordinateTransition::RebaseToFinalizedFrontier
        )
    }
}

/// A mutable view that stages graph changes over a borrowed base graph.
///
/// Reads reflect the base graph and staged changes. Callers can extract the
/// changes as a [`GraphDelta`] without modifying the base graph.
///
/// A staged deletion hides a base-graph header node from every overlay read.
/// Deleting an inserted header node cancels its insertion. The final delta
/// contains only base-graph deletions and header nodes that differ from the
/// base graph.
pub(crate) struct GraphOverlay<'a> {
    base_graph: &'a MemHeaderStore,
    base_revision: GraphRevision,
    finalized_frontier: Frontier,
    updated_header_nodes_by_hash: HashMap<block::Hash, HeaderNode>,
    deleted_header_hashes: HashSet<block::Hash>,
    added_header_children: HashMap<block::Hash, HashSet<block::Hash>>,
    removed_header_children: HashMap<block::Hash, HashSet<block::Hash>>,
    eligible_header_tips: HashSet<block::Hash>,
    new_consensus_invalid_body_tombstones_by_hash:
        HashMap<block::Hash, ConsensusInvalidBodyTombstone>,
    work_coordinates_rebased: bool,
    header_node_count: usize,
}

impl<'a> GraphOverlay<'a> {
    pub(crate) fn new(base_graph: &'a MemHeaderStore) -> Self {
        Self {
            base_graph,
            base_revision: base_graph.graph_revision,
            finalized_frontier: base_graph.finalized_frontier,
            updated_header_nodes_by_hash: HashMap::new(),
            deleted_header_hashes: HashSet::new(),
            added_header_children: HashMap::new(),
            removed_header_children: HashMap::new(),
            eligible_header_tips: base_graph.eligible_header_tips.clone(),
            new_consensus_invalid_body_tombstones_by_hash: HashMap::new(),
            work_coordinates_rebased: false,
            header_node_count: base_graph.nodes.len(),
        }
    }

    /// Build the projected graph view that `delta` defines over `base_graph`.
    ///
    /// This method validates `delta` without modifying `base_graph`.
    pub(crate) fn from_delta(
        base_graph: &'a MemHeaderStore,
        delta: &GraphDelta,
    ) -> Result<Self, GraphError> {
        let indexes = base_graph.derive_index_changes(delta)?;
        let updated_header_nodes_by_hash = delta
            .updated_header_nodes
            .iter()
            .cloned()
            .map(|node| (node.hash, node))
            .collect();
        let deleted_header_hashes = delta.deleted_header_hashes.iter().copied().collect();
        let mut added_header_children: HashMap<_, HashSet<_>> = HashMap::new();
        let mut removed_header_children: HashMap<_, HashSet<_>> = HashMap::new();
        for (parent, child) in &indexes.added_header_children {
            added_header_children
                .entry(*parent)
                .or_default()
                .insert(*child);
        }
        for (parent, child) in &indexes.removed_header_children {
            removed_header_children
                .entry(*parent)
                .or_default()
                .insert(*child);
        }
        let mut eligible_header_tips = base_graph.eligible_header_tips.clone();
        for hash in &indexes.remove_eligible_header_tips {
            eligible_header_tips.remove(hash);
        }
        eligible_header_tips.extend(indexes.add_eligible_header_tips.iter().copied());
        Ok(Self {
            base_graph,
            base_revision: delta.base_revision,
            finalized_frontier: delta
                .finalized_frontier
                .unwrap_or(base_graph.finalized_frontier),
            updated_header_nodes_by_hash,
            deleted_header_hashes,
            added_header_children,
            removed_header_children,
            eligible_header_tips,
            new_consensus_invalid_body_tombstones_by_hash: delta
                .new_consensus_invalid_body_tombstones
                .iter()
                .cloned()
                .map(|tombstone| (tombstone.hash, tombstone))
                .collect(),
            work_coordinates_rebased: delta.work_coordinate_transition
                == WorkCoordinateTransition::RebaseToFinalizedFrontier,
            header_node_count: base_graph
                .nodes
                .len()
                .saturating_add(
                    delta
                        .updated_header_nodes
                        .iter()
                        .filter(|node| !base_graph.nodes.contains_key(&node.hash))
                        .count(),
                )
                .saturating_sub(delta.deleted_header_hashes.len()),
        })
    }

    pub(crate) const fn finalized_frontier(&self) -> Frontier {
        self.finalized_frontier
    }

    pub(crate) const fn header_node_count(&self) -> usize {
        self.header_node_count
    }

    /// Returns the node with the given hash, if it is retained.
    pub(crate) fn header_node(&self, hash: block::Hash) -> Option<&HeaderNode> {
        if self.deleted_header_hashes.contains(&hash) {
            return None;
        }
        self.updated_header_nodes_by_hash
            .get(&hash)
            .or_else(|| self.base_graph.nodes.get(&hash))
    }

    /// Returns a mutable reference to the node with the given hash, creating it if it is not present.
    fn stage_header_node(&mut self, hash: block::Hash) -> Result<&mut HeaderNode, GraphError> {
        if self.deleted_header_hashes.contains(&hash) {
            return Err(GraphError::UnknownHeaderNode(hash));
        }
        if !self.updated_header_nodes_by_hash.contains_key(&hash) {
            let node = self
                .base_graph
                .nodes
                .get(&hash)
                .cloned()
                .ok_or(GraphError::UnknownHeaderNode(hash))?;
            self.updated_header_nodes_by_hash.insert(hash, node);
        }
        Ok(self
            .updated_header_nodes_by_hash
            .get_mut(&hash)
            .expect("overlay node exists because it was staged above"))
    }

    /// Return every visible header node.
    ///
    /// The iterator excludes deleted header nodes. Staged header nodes replace
    /// their base-graph versions.
    pub(crate) fn header_nodes(&self) -> impl Iterator<Item = &HeaderNode> {
        self.base_graph
            .nodes
            .values()
            .filter(|node| {
                !self.deleted_header_hashes.contains(&node.hash)
                    && !self.updated_header_nodes_by_hash.contains_key(&node.hash)
            })
            .chain(
                self.updated_header_nodes_by_hash
                    .values()
                    .filter(|node| !self.deleted_header_hashes.contains(&node.hash)),
            )
    }

    /// Return every retained header hash.
    pub(crate) fn retained_header_hashes(&self) -> impl Iterator<Item = block::Hash> + '_ {
        self.header_nodes().map(|node| node.hash)
    }

    /// Return base-graph and staged header hashes at the exact height.
    pub(crate) fn header_hashes_at_height(&self, height: block::Height) -> Vec<block::Hash> {
        let mut hashes: HashSet<_> = self
            .base_graph
            .heights
            .get(&height)
            .into_iter()
            .flatten()
            .filter(|hash| {
                !self.deleted_header_hashes.contains(hash)
                    && !self.updated_header_nodes_by_hash.contains_key(hash)
            })
            .copied()
            .collect();
        hashes.extend(
            self.updated_header_nodes_by_hash
                .values()
                .filter(|node| {
                    node.height == height && !self.deleted_header_hashes.contains(&node.hash)
                })
                .map(|node| node.hash),
        );
        let mut hashes: Vec<_> = hashes.into_iter().collect();
        hashes.sort_unstable_by_key(|hash| hash.0);
        hashes
    }

    /// Return base-graph and staged children of the exact parent header.
    pub(crate) fn header_children(&self, parent_hash: block::Hash) -> Vec<block::Hash> {
        if self.header_node(parent_hash).is_none() {
            return Vec::new();
        }
        let removed = self.removed_header_children.get(&parent_hash);
        let mut children: HashSet<_> = self
            .base_graph
            .children
            .get(&parent_hash)
            .into_iter()
            .flatten()
            .filter(|child| {
                !self.deleted_header_hashes.contains(child)
                    && removed.is_none_or(|removed| !removed.contains(child))
            })
            .copied()
            .collect();
        children.extend(
            self.added_header_children
                .get(&parent_hash)
                .into_iter()
                .flatten()
                .filter(|child| !self.deleted_header_hashes.contains(child))
                .copied(),
        );
        let mut children: Vec<_> = children.into_iter().collect();
        children.sort_unstable_by_key(|hash| hash.0);
        children
    }

    /// Stages one admitted header whose parent is visible through the overlay.
    ///
    /// The overlay derives the height, cumulative work, and inherited eligibility
    /// from the parent. A consensus-invalid body-validation state makes the new
    /// header ineligible.
    ///
    /// This method returns [`InsertResult::AlreadyPresent`] for an identical
    /// existing header. It rejects conflicting duplicates, unknown parents,
    /// height or work overflow, and invalid graph structure.
    ///
    /// This method derives canonical block work from the header target. The
    /// caller must supply authoritative validation and body-validation states.
    pub(crate) fn insert(
        &mut self,
        header: Arc<block::Header>,
        validation: HeaderValidationState,
        direct_reasons: impl IntoIterator<Item = EligibilityReason>,
        mut body_validation_state: BodyValidationState,
    ) -> Result<InsertResult, GraphError> {
        let hash = header.hash();
        let block_work =
            header
                .difficulty_threshold
                .to_work()
                .ok_or(GraphError::InvalidHeaderNode {
                    header: hash,
                    invariant: crate::HeaderNodeInvariant::CanonicalBlockWork,
                })?;

        if let Some(existing) = self.header_node(hash) {
            if existing.header == header {
                return Ok(InsertResult::AlreadyPresent(Frontier::new(
                    existing.height,
                    hash,
                )));
            }
            return Err(GraphError::ConflictingDuplicate(hash));
        }

        let parent_hash = header.previous_block_hash;
        let parent = self
            .header_node(parent_hash)
            .ok_or(GraphError::UnknownParent {
                header: hash,
                parent: parent_hash,
            })?;
        let height = parent
            .height
            .next()
            .map_err(|_| GraphError::HeightOverflow {
                parent: parent_hash,
            })?;
        let inherited_from = (!parent.is_eligible()).then_some(parent_hash);
        let work_coordinate = match parent.work_coordinate().checked_add(block_work) {
            Ok(coordinate) => coordinate,
            Err(WorkCoordinateError::Overflow) if validation == HeaderValidationState::Valid => {
                self.rebase_work_coordinates_to_finalized_frontier()?;
                self.header_node(parent_hash)
                    .expect("the work rebase retains every graph node")
                    .work_coordinate()
                    .checked_add(block_work)?
            }
            Err(error) => return Err(error.into()),
        };
        if let Some(tombstone) = self
            .new_consensus_invalid_body_tombstones_by_hash
            .get(&hash)
            .or_else(|| self.base_graph.consensus_invalid_body_tombstones.get(&hash))
        {
            body_validation_state = BodyValidationState::ConsensusInvalid {
                evidence: tombstone.evidence,
                rule: tombstone.rule.clone(),
            };
        }

        let direct_reasons: BTreeSet<_> = direct_reasons.into_iter().collect();

        let node = HeaderNode {
            header,
            hash,
            parent_hash,
            height,
            block_work,
            work_coordinate,
            validation,
            eligibility: EligibilityState {
                direct_reasons,
                inherited_from,
            },
            body_validation_state,
            aux_delivery_ids: Vec::new(),
        };
        let eligible = node.is_eligible();

        self.updated_header_nodes_by_hash.insert(hash, node);

        self.deleted_header_hashes.remove(&hash);

        self.record_header_child_addition(parent_hash, hash);

        self.header_node_count = self.header_node_count.saturating_add(1);

        if eligible {
            self.eligible_header_tips.remove(&parent_hash);
            self.eligible_header_tips.insert(hash);
        }

        Ok(InsertResult::Inserted(Frontier::new(height, hash)))
    }

    /// This method adds an eligibility reason that directly excludes the header.
    ///
    /// A new eligibility reason triggers inherited-eligibility recomputation for
    /// the header and its descendants. The return value reports whether the
    /// method added the eligibility reason.
    ///
    /// An unknown header produces an error.
    pub(crate) fn add_eligibility_reason(
        &mut self,
        hash: block::Hash,
        reason: EligibilityReason,
    ) -> Result<bool, GraphError> {
        let changed = self
            .stage_header_node(hash)?
            .eligibility
            .direct_reasons
            .insert(reason);
        if changed {
            self.recompute_descendant_eligibility(hash)?;
        }
        Ok(changed)
    }

    /// Removes the operator invalidation matching the exact ID and evidence.
    ///
    /// Preserves all unrelated eligibility reasons. If a matching reason is
    /// removed, recomputes inherited eligibility for the target and its
    /// descendants. Returns whether the graph changed.
    ///
    /// Passing `None` makes an absent invalidation an idempotent no-op. Returns an
    /// error if the target header is unknown.
    pub(crate) fn remove_operator_invalidation(
        &mut self,
        hash: block::Hash,
        id: OperatorInvalidationId,
        evidence: Option<EvidenceId>,
    ) -> Result<bool, GraphError> {
        let reasons = &mut self.stage_header_node(hash)?.eligibility.direct_reasons;
        let before = reasons.len();
        reasons.retain(|reason| {
            !matches!(reason,
                EligibilityReason::OperatorInvalid { id: existing, evidence: existing_evidence, .. }
                if *existing == id && Some(*existing_evidence) == evidence)
        });
        let changed = reasons.len() != before;
        if changed {
            self.recompute_descendant_eligibility(hash)?;
        }
        Ok(changed)
    }

    /// This method replaces body-validation state while preserving permanent
    /// invalidity.
    pub(crate) fn set_body_validation_state(
        &mut self,
        hash: block::Hash,
        body_validation_state: BodyValidationState,
    ) -> Result<bool, GraphError> {
        let tombstone = match &body_validation_state {
            BodyValidationState::ConsensusInvalid { evidence, rule } => {
                Some(ConsensusInvalidBodyTombstone {
                    hash,
                    evidence: *evidence,
                    rule: rule.clone(),
                })
            }
            _ => None,
        };
        if let Some(existing) = self
            .new_consensus_invalid_body_tombstones_by_hash
            .get(&hash)
            .or_else(|| self.base_graph.consensus_invalid_body_tombstones.get(&hash))
        {
            if tombstone.as_ref() != Some(existing) {
                return Err(GraphError::PermanentBodyInvalidity(hash));
            }
        }
        let (changed, eligibility_changed) = {
            let node = self.stage_header_node(hash)?;
            if matches!(
                node.body_validation_state,
                BodyValidationState::ConsensusInvalid { .. }
            ) {
                return if node.body_validation_state == body_validation_state {
                    Ok(false)
                } else {
                    Err(GraphError::PermanentBodyInvalidity(hash))
                };
            }
            let was_eligible = node.is_eligible();
            let changed = node.body_validation_state != body_validation_state;
            node.body_validation_state = body_validation_state;
            (changed, was_eligible != node.is_eligible())
        };
        if eligibility_changed {
            self.recompute_descendant_eligibility(hash)?;
        }
        if let Some(tombstone) = tombstone {
            self.new_consensus_invalid_body_tombstones_by_hash
                .insert(hash, tombstone);
        }
        Ok(changed)
    }

    pub(crate) fn set_header_validation_state(
        &mut self,
        hash: block::Hash,
        validation: HeaderValidationState,
    ) -> Result<bool, GraphError> {
        let node = self.stage_header_node(hash)?;
        let changed = node.validation != validation;
        node.validation = validation;
        if changed {
            self.recompute_descendant_eligibility(hash)?;
        }
        Ok(changed)
    }

    pub(crate) fn record_auxiliary_evidence_delivery(
        &mut self,
        hash: block::Hash,
        delivery_id: EvidenceId,
    ) -> Result<bool, GraphError> {
        let ids = &mut self.stage_header_node(hash)?.aux_delivery_ids;
        if ids.contains(&delivery_id) {
            return Ok(false);
        }
        ids.push(delivery_id);
        Ok(true)
    }

    pub(crate) fn rebase_work_coordinates_to_finalized_frontier(
        &mut self,
    ) -> Result<(), GraphError> {
        let anchor = self
            .header_node(self.finalized_frontier.hash)
            .ok_or(GraphError::UnknownHeaderNode(self.finalized_frontier.hash))?
            .work_coordinate();
        let rebased: Vec<_> = self
            .header_nodes()
            .map(|node| {
                Ok((
                    node.hash,
                    WorkCoordinate::new(
                        self.finalized_frontier.hash,
                        node.work_coordinate().suffix_after(anchor)?.as_u256(),
                    ),
                ))
            })
            .collect::<Result<_, GraphError>>()?;
        for (hash, coordinate) in rebased {
            self.stage_header_node(hash)?.work_coordinate = coordinate;
        }
        self.work_coordinates_rebased = true;
        Ok(())
    }

    pub(crate) const fn work_coordinates_rebased(&self) -> bool {
        self.work_coordinates_rebased
    }

    pub(crate) fn header_ancestor(
        &self,
        descendant: block::Hash,
        height: block::Height,
    ) -> Result<Option<Frontier>, GraphError> {
        let mut node = self
            .header_node(descendant)
            .ok_or(GraphError::UnknownHeaderNode(descendant))?;
        if height > node.height {
            return Err(GraphError::InvalidAncestorHeight {
                ancestor: height,
                descendant: node.height,
            });
        }
        while node.height > height {
            let Some(parent) = self.header_node(node.parent_hash) else {
                return Ok(None);
            };
            node = parent;
        }
        Ok(Some(Frontier::new(node.height, node.hash)))
    }

    pub(crate) fn eligible_header_tips(&self) -> Vec<Frontier> {
        let mut tips: Vec<_> = self
            .eligible_header_tips
            .iter()
            .filter_map(|hash| self.header_node(*hash))
            .map(|node| Frontier::new(node.height, node.hash))
            .collect();
        tips.sort_unstable_by_key(|tip| tip.hash.0);
        tips
    }

    pub(crate) fn select_best_header_chain(&self) -> Result<(Frontier, ChainScore), GraphError> {
        let anchor = self
            .header_node(self.finalized_frontier.hash)
            .ok_or(GraphError::UnknownHeaderNode(self.finalized_frontier.hash))?;
        let mut best = None;
        for hash in &self.eligible_header_tips {
            let node = self
                .header_node(*hash)
                .expect("overlay eligible tips are retained nodes");
            let tip = Frontier::new(node.height, node.hash);
            let score = ChainScore::new(
                node.work_coordinate()
                    .suffix_after(anchor.work_coordinate())?,
                tip.hash,
            );
            if best.is_none_or(|(_, best_score)| score > best_score) {
                best = Some((tip, score));
            }
        }
        best.ok_or(GraphError::UnknownHeaderNode(self.finalized_frontier.hash))
    }

    /// Computes the chain score of the given header relative to the finalized header.
    pub(crate) fn header_chain_score(&self, hash: block::Hash) -> Result<ChainScore, GraphError> {
        let anchor = self
            .header_node(self.finalized_frontier.hash)
            .ok_or(GraphError::UnknownHeaderNode(self.finalized_frontier.hash))?;
        let node = self
            .header_node(hash)
            .ok_or(GraphError::UnknownHeaderNode(hash))?;
        Ok(ChainScore::new(
            node.work_coordinate()
                .suffix_after(anchor.work_coordinate())?,
            hash,
        ))
    }

    /// Stage a finalized-frontier advance to an eligible retained descendant.
    ///
    /// The overlay removes each ancestor and discarded sibling subtree. The
    /// overlay retains the new finalized frontier and every descendant. The
    /// method returns removed header hashes in deterministic order. Validation
    /// failures leave the overlay unchanged.
    ///
    /// The transition layer must authorize and durably commit this operation.
    pub(crate) fn advance_finalized_frontier(
        &mut self,
        finalized_frontier: Frontier,
    ) -> Result<Vec<block::Hash>, GraphError> {
        let node = self
            .header_node(finalized_frontier.hash)
            .ok_or(GraphError::UnknownHeaderNode(finalized_frontier.hash))?;
        if node.height != finalized_frontier.height {
            return Err(GraphError::UnknownHeaderNode(finalized_frontier.hash));
        }
        if !node.is_eligible() {
            return Err(GraphError::IneligibleFinalizedFrontier(
                finalized_frontier.hash,
            ));
        }

        let current = self.finalized_frontier;
        let mut finalized_path = Vec::new();
        let mut cursor = finalized_frontier;
        while cursor.height > current.height {
            let parent_hash = self
                .header_node(cursor.hash)
                .ok_or(GraphError::UnknownHeaderNode(cursor.hash))?
                .parent_hash;
            finalized_path.push((parent_hash, cursor.hash));
            let parent_height = cursor.height.previous().map_err(|_| {
                GraphError::FinalizedFrontierNotDescendant {
                    current: current.hash,
                    candidate: finalized_frontier.hash,
                }
            })?;
            cursor = Frontier::new(parent_height, parent_hash);
        }
        if cursor != current {
            return Err(GraphError::FinalizedFrontierNotDescendant {
                current: current.hash,
                candidate: finalized_frontier.hash,
            });
        }

        let mut deleted = HashSet::new();
        for (ancestor, retained_child) in finalized_path {
            deleted.insert(ancestor);
            for sibling in self.header_children(ancestor) {
                if sibling == retained_child {
                    continue;
                }
                let mut pending = vec![sibling];
                while let Some(hash) = pending.pop() {
                    if deleted.insert(hash) {
                        pending.extend(self.header_children(hash));
                    }
                }
            }
        }
        let mut deleted: Vec<_> = deleted.into_iter().collect();
        deleted.sort_unstable_by_key(|hash| hash.0);
        for hash in &deleted {
            self.delete_header_node(*hash)?;
        }
        self.finalized_frontier = finalized_frontier;
        self.stage_header_node(finalized_frontier.hash)?
            .eligibility
            .inherited_from = None;
        self.refresh_eligible_header_tip(finalized_frontier.hash);
        Ok(deleted)
    }

    /// Removes a leaf node from the overlay.
    pub(crate) fn remove_header_leaf(&mut self, hash: block::Hash) -> Result<(), GraphError> {
        if !self.header_children(hash).is_empty() {
            return Err(GraphError::HeaderNodeHasChildren(hash));
        }
        self.delete_header_node(hash)
    }

    /// Build the minimal transition that produces this projected graph.
    pub(crate) fn delta(&self) -> GraphDelta {
        let mut updated_header_nodes: Vec<_> = self
            .updated_header_nodes_by_hash
            .values()
            .filter(|node| {
                !self.deleted_header_hashes.contains(&node.hash)
                    && self.base_graph.nodes.get(&node.hash) != Some(*node)
            })
            .cloned()
            .collect();
        updated_header_nodes.sort_unstable_by_key(|node| (node.height, node.hash.0));
        let mut deleted_header_hashes: Vec<_> =
            self.deleted_header_hashes.iter().copied().collect();
        deleted_header_hashes.sort_unstable_by_key(|hash| hash.0);
        GraphDelta {
            base_revision: self.base_revision,
            work_coordinate_transition: if self.work_coordinates_rebased {
                WorkCoordinateTransition::RebaseToFinalizedFrontier
            } else {
                WorkCoordinateTransition::PreserveCoordinates
            },
            finalized_frontier: (self.finalized_frontier != self.base_graph.finalized_frontier)
                .then_some(self.finalized_frontier),
            updated_header_nodes,
            deleted_header_hashes,
            new_consensus_invalid_body_tombstones: {
                let mut new_consensus_invalid_body_tombstones_by_hash: Vec<_> = self
                    .new_consensus_invalid_body_tombstones_by_hash
                    .values()
                    .cloned()
                    .collect();
                new_consensus_invalid_body_tombstones_by_hash
                    .sort_unstable_by_key(|tombstone| tombstone.hash.0);
                new_consensus_invalid_body_tombstones_by_hash
            },
        }
    }

    /// Removes a node from the overlay.
    fn delete_header_node(&mut self, hash: block::Hash) -> Result<(), GraphError> {
        let node = self
            .header_node(hash)
            .cloned()
            .ok_or(GraphError::UnknownHeaderNode(hash))?;
        self.updated_header_nodes_by_hash.remove(&hash);
        if self.base_graph.nodes.contains_key(&hash) {
            self.deleted_header_hashes.insert(hash);
        }
        self.record_header_child_removal(node.parent_hash, hash);
        self.eligible_header_tips.remove(&hash);
        self.header_node_count = self.header_node_count.saturating_sub(1);
        self.refresh_eligible_header_tip(node.parent_hash);
        Ok(())
    }

    /// Propagates an eligibility change at `root` through its descendants.
    ///
    /// The root's own validation or direct reasons must already be updated. This
    /// recomputes each affected descendant's inherited ineligibility and refreshes
    /// eligible-tip membership for changed nodes and their parents.
    ///
    /// Stops traversing a branch once its inherited state is unchanged, because no
    /// descendant on that branch can be affected.
    fn recompute_descendant_eligibility(&mut self, root: block::Hash) -> Result<(), GraphError> {
        let mut affected = HashSet::from([root]);
        affected.insert(
            self.header_node(root)
                .ok_or(GraphError::UnknownHeaderNode(root))?
                .parent_hash,
        );
        let mut queue = VecDeque::from(self.header_children(root));
        while let Some(hash) = queue.pop_front() {
            let parent_hash = self
                .header_node(hash)
                .ok_or(GraphError::UnknownHeaderNode(hash))?
                .parent_hash;
            affected.insert(parent_hash);

            let inherited_from = (!self
                .header_node(parent_hash)
                .ok_or(GraphError::UnknownHeaderNode(parent_hash))?
                .is_eligible())
            .then_some(parent_hash);

            if self
                .header_node(hash)
                .is_some_and(|node| node.eligibility.inherited_from == inherited_from)
            {
                continue;
            }

            self.stage_header_node(hash)?.eligibility.inherited_from = inherited_from;
            affected.insert(hash);

            queue.extend(self.header_children(hash));
        }

        for hash in affected {
            self.refresh_eligible_header_tip(hash);
        }
        Ok(())
    }

    /// Checks if the given node has any eligible children.
    fn has_eligible_header_child(&self, hash: block::Hash) -> bool {
        self.header_children(hash)
            .into_iter()
            .any(|child| self.header_node(child).is_some_and(HeaderNode::is_eligible))
    }

    /// Refreshes the eligible tips for the given node.
    fn refresh_eligible_header_tip(&mut self, hash: block::Hash) {
        self.eligible_header_tips.remove(&hash);
        if self
            .header_node(hash)
            .is_some_and(|node| node.is_eligible() && !self.has_eligible_header_child(hash))
        {
            self.eligible_header_tips.insert(hash);
        }
    }

    /// Records a new child-parent edge.
    fn record_header_child_addition(&mut self, parent: block::Hash, child: block::Hash) {
        if self
            .removed_header_children
            .get_mut(&parent)
            .is_some_and(|removed| removed.remove(&child))
        {
            return;
        }
        self.added_header_children
            .entry(parent)
            .or_default()
            .insert(child);
    }

    /// Records a removed child-parent edge.
    fn record_header_child_removal(&mut self, parent: block::Hash, child: block::Hash) {
        if self
            .added_header_children
            .get_mut(&parent)
            .is_some_and(|added| added.remove(&child))
        {
            return;
        }
        if self
            .base_graph
            .children
            .get(&parent)
            .is_some_and(|children| children.contains(&child))
        {
            self.removed_header_children
                .entry(parent)
                .or_default()
                .insert(child);
        }
    }
}

impl HeaderGraphView for GraphOverlay<'_> {
    fn view_finalized_frontier(&self) -> Frontier {
        self.finalized_frontier()
    }

    fn view_header_node_count(&self) -> usize {
        self.header_node_count()
    }

    fn view_header_node(&self, hash: block::Hash) -> Option<&HeaderNode> {
        self.header_node(hash)
    }

    fn view_header_nodes(&self) -> Vec<&HeaderNode> {
        self.header_nodes().collect()
    }

    fn view_retained_header_hashes(&self) -> Vec<block::Hash> {
        self.retained_header_hashes().collect()
    }

    fn view_header_hashes_at_height(&self, height: block::Height) -> Vec<block::Hash> {
        self.header_hashes_at_height(height)
    }

    fn view_header_children(&self, parent: block::Hash) -> Vec<block::Hash> {
        self.header_children(parent)
    }

    fn view_eligible_header_tips(&self) -> Vec<Frontier> {
        self.eligible_header_tips()
    }

    fn view_select_best_header_chain(&self) -> Result<(Frontier, ChainScore), GraphError> {
        self.select_best_header_chain()
    }

    fn view_header_chain_score(&self, hash: block::Hash) -> Result<ChainScore, GraphError> {
        self.header_chain_score(hash)
    }

    fn view_header_ancestor(
        &self,
        descendant: block::Hash,
        height: block::Height,
    ) -> Result<Option<Frontier>, GraphError> {
        self.header_ancestor(descendant, height)
    }
}

impl HeaderGraphEdit for GraphOverlay<'_> {
    fn edit_insert_header(
        &mut self,
        header: Arc<block::Header>,
        validation: HeaderValidationState,
        direct_reasons: Vec<EligibilityReason>,
        body_validation_state: BodyValidationState,
    ) -> Result<InsertResult, GraphError> {
        self.insert(header, validation, direct_reasons, body_validation_state)
    }

    fn edit_add_header_eligibility_reason(
        &mut self,
        hash: block::Hash,
        reason: EligibilityReason,
    ) -> Result<bool, GraphError> {
        self.add_eligibility_reason(hash, reason)
    }

    fn edit_remove_header_operator_invalidation(
        &mut self,
        hash: block::Hash,
        id: OperatorInvalidationId,
        evidence: Option<EvidenceId>,
    ) -> Result<bool, GraphError> {
        self.remove_operator_invalidation(hash, id, evidence)
    }

    fn edit_set_body_validation_state(
        &mut self,
        hash: block::Hash,
        body_validation_state: BodyValidationState,
    ) -> Result<bool, GraphError> {
        self.set_body_validation_state(hash, body_validation_state)
    }

    fn edit_set_header_validation_state(
        &mut self,
        hash: block::Hash,
        validation: HeaderValidationState,
    ) -> Result<bool, GraphError> {
        self.set_header_validation_state(hash, validation)
    }

    fn edit_record_auxiliary_evidence_delivery(
        &mut self,
        hash: block::Hash,
        delivery_id: EvidenceId,
    ) -> Result<bool, GraphError> {
        self.record_auxiliary_evidence_delivery(hash, delivery_id)
    }

    fn edit_advance_finalized_frontier(
        &mut self,
        finalized: Frontier,
    ) -> Result<Vec<block::Hash>, GraphError> {
        self.advance_finalized_frontier(finalized)
    }

    fn edit_remove_header_leaf(&mut self, hash: block::Hash) -> Result<(), GraphError> {
        self.remove_header_leaf(hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::block::genesis::regtest_genesis_block;

    fn store() -> MemHeaderStore {
        let block = regtest_genesis_block();
        let work = block
            .header
            .difficulty_threshold
            .to_work()
            .expect("the regtest target has valid work");
        MemHeaderStore::new(
            Frontier::new(block::Height(0), block.hash()),
            block.header.clone(),
            work,
            work.as_u256(),
        )
        .expect("the anchor is coherent")
    }

    fn header(parent: block::Hash, marker: u8) -> Arc<block::Header> {
        let mut header = *regtest_genesis_block().header;
        header.previous_block_hash = parent;
        header.nonce = [marker; 32].into();
        Arc::new(header)
    }

    fn insert_child(store: &mut MemHeaderStore, parent: block::Hash, marker: u8) -> Frontier {
        let header = header(parent, marker);
        match store
            .insert(
                header,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the fixture child inserts")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        }
    }

    #[test]
    fn overlay_tracks_only_changed_nodes_edges_and_tips() {
        let base_graph = store();
        let anchor = base_graph.finalized_frontier();
        let mut overlay = GraphOverlay::new(&base_graph);
        let child_header = header(anchor.hash, 1);
        let child = match overlay
            .insert(
                child_header,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the overlay child inserts")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        };
        let delta = overlay.delta();
        assert_eq!(delta.updated_header_nodes.len(), 1);
        assert_eq!(delta.updated_header_nodes[0].hash, child.hash);
        assert_eq!(delta.deleted_header_hashes, Vec::<block::Hash>::new());
        assert_eq!(
            overlay.header_node_count(),
            base_graph.header_node_count().saturating_add(1)
        );
    }

    #[test]
    fn applying_delta_matches_the_complete_overlay_projection() {
        let mut base_graph = store();
        let anchor = base_graph.finalized_frontier();
        let first_header = header(anchor.hash, 1);
        let first = match base_graph
            .insert(
                first_header,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the first base_graph child inserts")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        };
        let second_header = header(first.hash, 2);
        let second = match base_graph
            .insert(
                second_header.clone(),
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the second base_graph child inserts")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        };
        let fork_header = header(anchor.hash, 3);
        let fork = match base_graph
            .insert(
                fork_header,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the base_graph fork inserts")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        };

        let mut overlay = GraphOverlay::new(&base_graph);
        let operator = OperatorInvalidationId::new([9; 16]);
        overlay
            .add_eligibility_reason(
                first.hash,
                EligibilityReason::operator_invalid(
                    first.hash,
                    operator,
                    EvidenceId::from_digest([9; 32]),
                ),
            )
            .expect("operator invalidation propagates");
        overlay
            .remove_operator_invalidation(
                first.hash,
                operator,
                Some(EvidenceId::from_digest([9; 32])),
            )
            .expect("operator reconsideration propagates");
        overlay
            .set_body_validation_state(second.hash, BodyValidationState::CommitmentMatched)
            .expect("body knowledge changes");
        let third_header = header(second.hash, 4);
        let third = match overlay
            .insert(
                third_header,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the overlay extension inserts")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        };
        overlay
            .remove_header_leaf(fork.hash)
            .expect("the unselected fork is removable");
        overlay
            .advance_finalized_frontier(first)
            .expect("the eligible descendant becomes the new anchor");

        let delta = overlay.delta();
        let projected = MemHeaderStore::reconstruct(crate::HeaderGraphReconstruction::new(
            first,
            overlay.header_nodes().cloned(),
            base_graph
                .consensus_invalid_body_tombstones()
                .chain(
                    overlay
                        .new_consensus_invalid_body_tombstones_by_hash
                        .values(),
                )
                .cloned(),
        ))
        .expect("the overlay materializes as a coherent graph");
        let mut applied = base_graph.clone();
        applied
            .apply_delta(&delta)
            .expect("the verified delta applies to its base_graph");

        assert_eq!(applied.finalized_frontier, projected.finalized_frontier);
        assert_eq!(applied.nodes, projected.nodes);
        assert_eq!(applied.children, projected.children);
        assert_eq!(applied.heights, projected.heights);
        assert_eq!(applied.eligible_header_tips, projected.eligible_header_tips);
        assert_eq!(
            applied.select_best_header_chain(),
            projected.select_best_header_chain()
        );
        assert_eq!(applied.finalized_frontier(), first);
        assert!(applied.header_node(anchor.hash).is_none());
        assert!(applied.header_node(fork.hash).is_none());
        assert_eq!(
            applied.header_node(third.hash).map(|node| node.parent_hash),
            Some(second.hash)
        );
    }

    #[test]
    fn canonical_deletion_removes_child_and_tip_indexes() {
        let mut base_graph = store();
        let anchor = base_graph.finalized_frontier();
        let child = insert_child(&mut base_graph, anchor.hash, 1);
        let mut delta = GraphDelta::empty(&base_graph);
        delta.deleted_header_hashes.push(child.hash);

        let projected = GraphOverlay::from_delta(&base_graph, &delta)
            .expect("canonical deletion reconstructs an overlay");
        assert!(projected.header_node(child.hash).is_none());
        assert!(projected.header_children(anchor.hash).is_empty());
        assert_eq!(projected.eligible_header_tips(), vec![anchor]);

        base_graph
            .apply_delta(&delta)
            .expect("canonical deletion applies to the store");
        assert!(base_graph.header_node(child.hash).is_none());
        assert!(base_graph.header_children(anchor.hash).is_empty());
        assert_eq!(base_graph.eligible_header_tips(), vec![anchor]);
    }

    #[test]
    fn conflicting_node_changes_fail_before_store_mutation() {
        let mut base_graph = store();
        let anchor = base_graph.finalized_frontier();
        let child = insert_child(&mut base_graph, anchor.hash, 1);
        let child_node = base_graph
            .header_node(child.hash)
            .expect("the fixture child is retained")
            .clone();
        let before_nodes = base_graph.nodes.clone();
        let before_children = base_graph.children.clone();
        let before_heights = base_graph.heights.clone();
        let before_tips = base_graph.eligible_header_tips.clone();
        let mut delta = GraphDelta::empty(&base_graph);
        delta.updated_header_nodes.push(child_node);
        delta.deleted_header_hashes.push(child.hash);

        assert_eq!(
            base_graph.apply_delta(&delta),
            Err(GraphError::DuplicateHeaderNode(child.hash))
        );
        assert_eq!(base_graph.nodes, before_nodes);
        assert_eq!(base_graph.children, before_children);
        assert_eq!(base_graph.heights, before_heights);
        assert_eq!(base_graph.eligible_header_tips, before_tips);
    }

    #[test]
    fn stale_delta_cannot_erase_newer_consensus_invalidity() {
        let mut base_graph = store();
        let anchor = base_graph.finalized_frontier();
        let child = insert_child(&mut base_graph, anchor.hash, 1);
        let mut overlay = GraphOverlay::new(&base_graph);
        overlay
            .set_header_validation_state(child.hash, HeaderValidationState::Valid)
            .expect("the staged validation is idempotent");
        overlay
            .record_auxiliary_evidence_delivery(child.hash, EvidenceId::from_digest([3; 32]))
            .expect("the overlay stages a node replacement");
        let delta = overlay.delta();
        drop(overlay);
        base_graph
            .set_body_validation_state(
                child.hash,
                BodyValidationState::ConsensusInvalid {
                    evidence: EvidenceId::from_digest([4; 32]),
                    rule: crate::BodyRuleId::new("test.stale-delta"),
                },
            )
            .expect("newer consensus invalidity advances the graph revision");
        let before = base_graph.clone();

        assert_eq!(
            base_graph.apply_delta(&delta),
            Err(GraphError::StaleDelta {
                current_revision: before.graph_revision,
                delta_base_revision: delta.base_revision,
            })
        );
        assert_eq!(base_graph.nodes, before.nodes);
        assert_eq!(base_graph.eligible_header_tips, before.eligible_header_tips);
    }

    #[test]
    fn finality_delta_keeps_the_new_root_after_deleting_its_parent() {
        let mut base_graph = store();
        let old_root = base_graph.finalized_frontier();
        let new_root = insert_child(&mut base_graph, old_root.hash, 1);
        let retained_tip = insert_child(&mut base_graph, new_root.hash, 2);
        let mut overlay = GraphOverlay::new(&base_graph);
        overlay
            .advance_finalized_frontier(new_root)
            .expect("the eligible child becomes finalized");
        let delta = overlay.delta();

        assert!(delta.deleted_header_hashes.contains(&old_root.hash));
        let projected = GraphOverlay::from_delta(&base_graph, &delta)
            .expect("the new root may outlive its deleted parent");
        assert_eq!(projected.finalized_frontier(), new_root);
        assert!(projected.header_node(new_root.hash).is_some());
        assert!(projected.header_node(retained_tip.hash).is_some());

        base_graph
            .apply_delta(&delta)
            .expect("the finality delta applies atomically");
        assert_eq!(base_graph.finalized_frontier(), new_root);
        assert!(base_graph.header_node(old_root.hash).is_none());
        assert!(base_graph.header_node(new_root.hash).is_some());
        assert!(base_graph.header_node(retained_tip.hash).is_some());
    }

    #[test]
    fn advancing_finality_retains_the_complete_descendant_subtree() {
        let mut base_graph = store();
        let anchor = base_graph.finalized_frontier();
        let finalized = insert_child(&mut base_graph, anchor.hash, 1);
        let mut retained_tip = finalized;
        for marker in 2..=64 {
            retained_tip = insert_child(&mut base_graph, retained_tip.hash, marker);
        }

        let mut overlay = GraphOverlay::new(&base_graph);
        let deleted = overlay
            .advance_finalized_frontier(finalized)
            .expect("the direct eligible descendant becomes finalized");

        assert_eq!(deleted, vec![anchor.hash]);
        assert_eq!(overlay.finalized_frontier(), finalized);
        assert!(overlay.header_node(retained_tip.hash).is_some());
        assert_eq!(overlay.header_node_count(), 64);
    }

    #[test]
    fn insertion_and_eligibility_changes_only_stage_affected_headers() {
        let mut base_graph = store();
        let anchor = base_graph.finalized_frontier();
        let left_header = header(anchor.hash, 1);
        let left = match base_graph
            .insert(
                left_header,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the left branch inserts")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        };
        let right_header = header(anchor.hash, 2);
        let right = match base_graph
            .insert(
                right_header,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the right branch inserts")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        };
        let right_child_header = header(right.hash, 3);
        let right_child = match base_graph
            .insert(
                right_child_header,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the unrelated descendant inserts")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        };
        let left_child_header = header(left.hash, 4);
        let left_child = match base_graph
            .insert(
                left_child_header,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the affected descendant inserts")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        };

        let mut insertion = GraphOverlay::new(&base_graph);
        insertion
            .insert(
                header(left_child.hash, 5),
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the overlay insertion succeeds");
        let insertion_delta = insertion.delta();
        assert_eq!(insertion_delta.updated_header_nodes.len(), 1);
        assert!(insertion_delta.deleted_header_hashes.is_empty());

        let mut eligibility = GraphOverlay::new(&base_graph);
        eligibility
            .add_eligibility_reason(
                left.hash,
                EligibilityReason::operator_invalid(
                    left.hash,
                    OperatorInvalidationId::new([5; 16]),
                    EvidenceId::from_digest([5; 32]),
                ),
            )
            .expect("the local invalidation propagates");
        let eligibility_delta = eligibility.delta();
        assert_eq!(eligibility_delta.updated_header_nodes.len(), 2);
        assert!(eligibility_delta.deleted_header_hashes.is_empty());
        assert!(!eligibility
            .header_node(left_child.hash)
            .is_some_and(HeaderNode::is_eligible));
        assert!(eligibility
            .header_node(right.hash)
            .is_some_and(HeaderNode::is_eligible));
        assert!(eligibility
            .header_node(right_child.hash)
            .is_some_and(HeaderNode::is_eligible));
    }
}
