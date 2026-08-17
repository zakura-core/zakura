use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use zakura_chain::block;

use super::{
    ConsensusInvalidBodyTombstone, GraphError, GraphRevision, HeaderGraphEdit, HeaderGraphView,
    HeaderNodeInvariant, InsertResult, MemHeaderStore,
};
use crate::{
    BodyValidationState, ChainScore, EligibilityReason, EligibilityState, EvidenceId, Frontier,
    HeaderNode, HeaderValidationState, OperatorInvalidationId, WorkCoordinate, WorkCoordinateError,
};

#[cfg(test)]
thread_local! {
    static OVERLAY_CONSTRUCTIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_overlay_construction_count() {
    OVERLAY_CONSTRUCTIONS.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn overlay_construction_count() -> usize {
    OVERLAY_CONSTRUCTIONS.with(std::cell::Cell::get)
}

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
    /// Finalized-path nodes this transition inserted and then retired.
    ///
    /// A finality advance deletes every header below the new finalized frontier, including
    /// headers the same transition inserted. Such a header cancels out: it reaches neither
    /// [`Self::updated_header_nodes`] nor [`Self::deleted_header_hashes`], because the live
    /// graph never stores it. The delta must still prove the new finalized frontier descends
    /// from the base frontier, so it carries exactly the retired nodes that proof reads.
    /// Delta application ignores this evidence.
    pub(super) retired_finalized_path_nodes: Vec<HeaderNode>,
    pub(super) new_consensus_invalid_body_tombstones: Vec<ConsensusInvalidBodyTombstone>,
}

/// A revision-bound sparse graph update that has passed live-delta validation.
///
/// Keeping this application separate from [`GraphDelta`] leaves room for a future transition to
/// carry the validated application across the durable commit boundary without changing the delta
/// or the public transition API.
pub(super) struct GraphDeltaApplication {
    pub(super) base_revision: GraphRevision,
    pub(super) finalized_frontier: Frontier,
    pub(super) updated_header_nodes_by_hash: HashMap<block::Hash, HeaderNode>,
    pub(super) deleted_header_hashes: HashSet<block::Hash>,
    pub(super) added_header_children: HashMap<block::Hash, HashSet<block::Hash>>,
    pub(super) removed_header_children: HashMap<block::Hash, HashSet<block::Hash>>,
    pub(super) eligible_header_tips: HashSet<block::Hash>,
    pub(super) new_consensus_invalid_body_tombstones_by_hash:
        HashMap<block::Hash, ConsensusInvalidBodyTombstone>,
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
            retired_finalized_path_nodes: Vec::new(),
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

    /// Return the retired finalized-path evidence in deterministic order.
    ///
    /// Delta validation reads the field directly. This accessor exists so tests outside this
    /// module can assert that the evidence stays out of the durable write set.
    #[cfg(test)]
    pub(crate) fn retired_finalized_path_nodes(&self) -> &[HeaderNode] {
        &self.retired_finalized_path_nodes
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
    /// Nodes this overlay inserted and then deleted, kept only as delta evidence.
    ///
    /// Every overlay read hides these, because the projected graph does not contain them.
    /// They survive here so [`GraphOverlay::delta`] can prove finalized descendancy across
    /// headers one transition both inserted and retired.
    retired_inserted_nodes: HashMap<block::Hash, HeaderNode>,
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
        #[cfg(test)]
        OVERLAY_CONSTRUCTIONS.with(|count| count.set(count.get().saturating_add(1)));
        Self {
            base_graph,
            base_revision: base_graph.graph_revision,
            finalized_frontier: base_graph.finalized_frontier,
            updated_header_nodes_by_hash: HashMap::new(),
            deleted_header_hashes: HashSet::new(),
            retired_inserted_nodes: HashMap::new(),
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
        // A delta is valid only for the exact graph revision from which it was derived.
        if delta.base_revision != base_graph.graph_revision {
            return Err(GraphError::StaleDelta {
                current_revision: base_graph.graph_revision,
                delta_base_revision: delta.base_revision,
            });
        }

        // Index the sparse changes for overlay lookups, rejecting input that would be silently
        // overwritten while collecting into maps or sets.
        let updated_header_nodes_by_hash = delta
            .updated_header_nodes
            .iter()
            .cloned()
            .map(|node| (node.hash, node))
            .collect::<HashMap<_, _>>();
        if updated_header_nodes_by_hash.len() != delta.updated_header_nodes.len() {
            let mut seen = HashSet::new();
            let duplicate = delta
                .updated_header_nodes
                .iter()
                .find_map(|node| (!seen.insert(node.hash)).then_some(node.hash))
                .expect("different map and sequence lengths imply a duplicate hash");
            return Err(GraphError::DuplicateHeaderNode(duplicate));
        }
        let deleted_header_hashes: HashSet<_> =
            delta.deleted_header_hashes.iter().copied().collect();
        if deleted_header_hashes.len() != delta.deleted_header_hashes.len() {
            let mut seen = HashSet::new();
            let duplicate = delta
                .deleted_header_hashes
                .iter()
                .find_map(|hash| (!seen.insert(*hash)).then_some(*hash))
                .expect("different set and sequence lengths imply a duplicate hash");
            return Err(GraphError::DuplicateHeaderNode(duplicate));
        }
        for hash in &deleted_header_hashes {
            // A node cannot be both updated and deleted, and deletions must refer to live nodes.
            if updated_header_nodes_by_hash.contains_key(hash) {
                return Err(GraphError::DuplicateHeaderNode(*hash));
            }
            if !base_graph.nodes.contains_key(hash) {
                return Err(GraphError::UnknownHeaderNode(*hash));
            }
        }
        let retired_inserted_nodes = delta
            .retired_finalized_path_nodes
            .iter()
            .cloned()
            .map(|node| (node.hash, node))
            .collect::<HashMap<_, _>>();
        if retired_inserted_nodes.len() != delta.retired_finalized_path_nodes.len() {
            let mut seen = HashSet::new();
            let duplicate = delta
                .retired_finalized_path_nodes
                .iter()
                .find_map(|node| (!seen.insert(node.hash)).then_some(node.hash))
                .expect("different map and sequence lengths imply a duplicate hash");
            return Err(GraphError::DuplicateHeaderNode(duplicate));
        }
        for hash in retired_inserted_nodes.keys() {
            // A retired node never reaches the live graph. A base-graph node is a real
            // deletion instead, and an updated node is a real insertion, so either overlap
            // means the delta describes one header two ways.
            if base_graph.nodes.contains_key(hash)
                || updated_header_nodes_by_hash.contains_key(hash)
            {
                return Err(GraphError::DuplicateHeaderNode(*hash));
            }
        }
        let new_consensus_invalid_body_tombstones_by_hash = delta
            .new_consensus_invalid_body_tombstones
            .iter()
            .cloned()
            .map(|tombstone| (tombstone.hash, tombstone))
            .collect::<HashMap<_, _>>();
        if new_consensus_invalid_body_tombstones_by_hash.len()
            != delta.new_consensus_invalid_body_tombstones.len()
        {
            let mut seen = HashSet::new();
            let duplicate = delta
                .new_consensus_invalid_body_tombstones
                .iter()
                .find_map(|tombstone| (!seen.insert(tombstone.hash)).then_some(tombstone.hash))
                .expect("different map and sequence lengths imply a duplicate hash");
            return Err(GraphError::DuplicateHeaderNode(duplicate));
        }

        // Derive only the child-index changes caused by added and deleted nodes. Existing updated
        // nodes retain their parent relationship from the audited base graph.
        let mut added_header_children: HashMap<_, HashSet<_>> = HashMap::new();
        let mut removed_header_children: HashMap<_, HashSet<_>> = HashMap::new();
        for node in updated_header_nodes_by_hash.values() {
            // A finality advance deletes the new frontier's parent, and a transition may
            // retire a parent it also inserted. Recording a child edge under a parent the
            // projected graph does not retain would strand that entry in the child index,
            // because nothing deletes the parent again.
            let parent_retained = updated_header_nodes_by_hash.contains_key(&node.parent_hash)
                || (base_graph.nodes.contains_key(&node.parent_hash)
                    && !deleted_header_hashes.contains(&node.parent_hash));
            if !base_graph.nodes.contains_key(&node.hash) && parent_retained {
                added_header_children
                    .entry(node.parent_hash)
                    .or_default()
                    .insert(node.hash);
            }
        }
        for hash in &deleted_header_hashes {
            let node = base_graph
                .nodes
                .get(hash)
                .expect("deleted hashes were checked against the base graph");
            if node.hash != base_graph.finalized_frontier.hash {
                removed_header_children
                    .entry(node.parent_hash)
                    .or_default()
                    .insert(node.hash);
            }
        }

        // Project optional scalar changes and the resulting node count without materializing the
        // complete graph.
        let finalized_frontier = delta
            .finalized_frontier
            .unwrap_or(base_graph.finalized_frontier);
        let header_node_count = base_graph
            .nodes
            .len()
            .saturating_add(
                updated_header_nodes_by_hash
                    .keys()
                    .filter(|hash| !base_graph.nodes.contains_key(*hash))
                    .count(),
            )
            .saturating_sub(deleted_header_hashes.len());
        let mut overlay = Self {
            base_graph,
            base_revision: delta.base_revision,
            finalized_frontier,
            updated_header_nodes_by_hash,
            deleted_header_hashes,
            retired_inserted_nodes,
            added_header_children,
            removed_header_children,
            eligible_header_tips: base_graph.eligible_header_tips.clone(),
            new_consensus_invalid_body_tombstones_by_hash,
            work_coordinates_rebased: delta.work_coordinate_transition
                == WorkCoordinateTransition::RebaseToFinalizedFrontier,
            header_node_count,
        };

        // Validate only invariants that can change at the sparse overlay boundary, then derive the
        // eligible-tip index for the projected graph.
        overlay.validate_delta_nodes()?;
        overlay.refresh_delta_eligible_header_tips();
        Ok(overlay)
    }

    /// Consume this validated projection into the exact owned mutations required by the live
    /// graph. The returned application remains bound to this overlay's base revision.
    pub(super) fn into_delta_application(self) -> GraphDeltaApplication {
        GraphDeltaApplication {
            base_revision: self.base_revision,
            finalized_frontier: self.finalized_frontier,
            updated_header_nodes_by_hash: self.updated_header_nodes_by_hash,
            deleted_header_hashes: self.deleted_header_hashes,
            added_header_children: self.added_header_children,
            removed_header_children: self.removed_header_children,
            eligible_header_tips: self.eligible_header_tips,
            new_consensus_invalid_body_tombstones_by_hash: self
                .new_consensus_invalid_body_tombstones_by_hash,
        }
    }

    /// Validate every graph invariant whose truth can change in this sparse delta.
    ///
    /// Unchanged nodes inherit the base graph's audit; validation follows only changed boundaries.
    fn validate_delta_nodes(&self) -> Result<(), GraphError> {
        let finalized_node = self
            .header_node(self.finalized_frontier.hash)
            .ok_or(GraphError::UnknownHeaderNode(self.finalized_frontier.hash))?;
        if finalized_node.height != self.finalized_frontier.height {
            return Err(GraphError::InvalidHeaderNode {
                header: self.finalized_frontier.hash,
                invariant: HeaderNodeInvariant::FinalizedFrontierHeight,
            });
        }
        if !finalized_node.is_eligible() {
            return Err(GraphError::IneligibleFinalizedFrontier(
                self.finalized_frontier.hash,
            ));
        }
        self.validate_finalized_descendant()?;
        self.validate_updated_nodes()?;
        self.validate_deleted_nodes()?;
        self.validate_tombstones()?;
        self.validate_updated_eligibility()?;
        Ok(())
    }

    /// Require the projected finalized frontier to be an exact descendant of the base frontier.
    ///
    /// The ancestry walk may cross nodes deleted by the same finality transition. A node the
    /// same transition also inserted survives in neither the base graph nor the updated set,
    /// so the walk reads it from the retired finalized-path evidence the delta carries.
    fn validate_finalized_descendant(&self) -> Result<(), GraphError> {
        if self.finalized_frontier == self.base_graph.finalized_frontier {
            return Ok(());
        }
        let mut cursor = self.finalized_frontier;
        while cursor.height > self.base_graph.finalized_frontier.height {
            let node = self
                .updated_header_nodes_by_hash
                .get(&cursor.hash)
                .or_else(|| self.base_graph.nodes.get(&cursor.hash))
                .or_else(|| self.retired_inserted_nodes.get(&cursor.hash))
                .ok_or(GraphError::UnknownHeaderNode(cursor.hash))?;
            if node.height != cursor.height {
                return Err(GraphError::UnknownHeaderNode(cursor.hash));
            }
            cursor = Frontier::new(block::Height(cursor.height.0 - 1), node.parent_hash);
        }
        if cursor != self.base_graph.finalized_frontier {
            return Err(GraphError::FinalizedFrontierNotDescendant {
                current: self.base_graph.finalized_frontier.hash,
                candidate: self.finalized_frontier.hash,
            });
        }
        Ok(())
    }

    /// Validate canonical fields, immutable replacements, parent continuity, and work coordinates
    /// for updated nodes. A graph-wide scan occurs only for an explicitly global work rebase.
    fn validate_updated_nodes(&self) -> Result<(), GraphError> {
        let current_anchor = self
            .base_graph
            .nodes
            .get(&self.base_graph.finalized_frontier.hash)
            .expect("the live graph retains its finalized frontier")
            .work_coordinate();
        for node in self.updated_header_nodes_by_hash.values() {
            if node.header.previous_block_hash != node.parent_hash {
                return Err(GraphError::InvalidHeaderNode {
                    header: node.hash,
                    invariant: HeaderNodeInvariant::CanonicalParentHash,
                });
            }
            if node.header.difficulty_threshold.to_work() != Some(node.block_work) {
                return Err(GraphError::InvalidHeaderNode {
                    header: node.hash,
                    invariant: HeaderNodeInvariant::CanonicalBlockWork,
                });
            }
            if let Some(old) = self.base_graph.nodes.get(&node.hash) {
                let immutable_changed = old.header != node.header
                    || old.hash != node.hash
                    || old.parent_hash != node.parent_hash
                    || old.height != node.height
                    || old.block_work != node.block_work;
                let coordinate_changed = old.work_coordinate() != node.work_coordinate();
                let invalidity_changed = matches!(
                    old.body_validation_state,
                    BodyValidationState::ConsensusInvalid { .. }
                ) && old.body_validation_state
                    != node.body_validation_state;
                if immutable_changed
                    || invalidity_changed
                    || (!self.work_coordinates_rebased && coordinate_changed)
                {
                    return Err(GraphError::InvalidHeaderNode {
                        header: node.hash,
                        invariant: HeaderNodeInvariant::ImmutableFields,
                    });
                }
            }
            if node.hash == self.finalized_frontier.hash {
                if node.eligibility.inherited_from.is_some() {
                    return Err(GraphError::InvalidHeaderNode {
                        header: node.hash,
                        invariant: HeaderNodeInvariant::DerivedHeaderState,
                    });
                }
                continue;
            }
            let parent = self
                .header_node(node.parent_hash)
                .ok_or(GraphError::UnknownParent {
                    header: node.hash,
                    parent: node.parent_hash,
                })?;
            if parent.height.next().ok() != Some(node.height) {
                return Err(GraphError::InvalidHeaderNode {
                    header: node.hash,
                    invariant: HeaderNodeInvariant::ParentHeight,
                });
            }
            if parent.work_coordinate().checked_add(node.block_work)? != node.work_coordinate() {
                return Err(GraphError::InvalidHeaderNode {
                    header: node.hash,
                    invariant: HeaderNodeInvariant::CumulativeWork,
                });
            }
        }
        if self.work_coordinates_rebased {
            for node in self.header_nodes() {
                if node.work_coordinate().origin_hash() != self.base_graph.finalized_frontier.hash {
                    return Err(GraphError::InvalidHeaderNode {
                        header: node.hash,
                        invariant: HeaderNodeInvariant::WorkRebaseOrigin,
                    });
                }
                if let Some(old) = self.base_graph.nodes.get(&node.hash) {
                    let expected = WorkCoordinate::new(
                        self.base_graph.finalized_frontier.hash,
                        old.work_coordinate()
                            .suffix_after(current_anchor)?
                            .as_u256(),
                    );
                    if node.work_coordinate() != expected {
                        return Err(GraphError::InvalidHeaderNode {
                            header: node.hash,
                            invariant: HeaderNodeInvariant::WorkRebaseCoordinate,
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Reject a deletion that leaves a retained child without its parent.
    ///
    /// The projected finalized root is the sole valid retained child of a deleted parent.
    fn validate_deleted_nodes(&self) -> Result<(), GraphError> {
        for hash in &self.deleted_header_hashes {
            if let Some(children) = self.base_graph.children.get(hash) {
                for child in children {
                    if self.header_node(*child).is_some() && *child != self.finalized_frontier.hash
                    {
                        return Err(GraphError::UnknownParent {
                            header: *child,
                            parent: *hash,
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Enforce append-only tombstones and exact agreement with retained consensus-invalid nodes.
    fn validate_tombstones(&self) -> Result<(), GraphError> {
        for (hash, tombstone) in &self.new_consensus_invalid_body_tombstones_by_hash {
            if let Some(existing) = self.base_graph.consensus_invalid_body_tombstones.get(hash) {
                if existing != tombstone {
                    return Err(GraphError::PermanentBodyInvalidity(*hash));
                }
            }
            if let Some(node) = self.header_node(*hash) {
                if !matches!(
                    &node.body_validation_state,
                    BodyValidationState::ConsensusInvalid { evidence, rule }
                        if *evidence == tombstone.evidence && *rule == tombstone.rule
                ) {
                    return Err(GraphError::InvalidHeaderNode {
                        header: *hash,
                        invariant: HeaderNodeInvariant::DerivedHeaderState,
                    });
                }
            }
        }
        for node in self.updated_header_nodes_by_hash.values() {
            let expected = match &node.body_validation_state {
                BodyValidationState::ConsensusInvalid { evidence, rule } => Some((*evidence, rule)),
                _ => None,
            };
            let tombstone = self
                .new_consensus_invalid_body_tombstones_by_hash
                .get(&node.hash)
                .or_else(|| {
                    self.base_graph
                        .consensus_invalid_body_tombstones
                        .get(&node.hash)
                });
            match (expected, tombstone) {
                (Some((evidence, rule)), Some(tombstone))
                    if evidence == tombstone.evidence && *rule == tombstone.rule => {}
                (Some(_), _) => return Err(GraphError::PermanentBodyInvalidity(node.hash)),
                (None, Some(_)) => return Err(GraphError::PermanentBodyInvalidity(node.hash)),
                (None, None) => {}
            }
        }
        Ok(())
    }

    /// Require updated nodes and their immediate children to carry the exact inherited
    /// eligibility derived from their projected parents.
    fn validate_updated_eligibility(&self) -> Result<(), GraphError> {
        for node in self.updated_header_nodes_by_hash.values() {
            if node.hash != self.finalized_frontier.hash {
                let parent =
                    self.header_node(node.parent_hash)
                        .ok_or(GraphError::UnknownParent {
                            header: node.hash,
                            parent: node.parent_hash,
                        })?;
                let expected = (!parent.is_eligible()).then_some(parent.hash);
                if node.eligibility.inherited_from != expected {
                    return Err(GraphError::InvalidHeaderNode {
                        header: node.hash,
                        invariant: HeaderNodeInvariant::DerivedHeaderState,
                    });
                }
            }
            for child in self.header_children(node.hash) {
                let child_node = self
                    .header_node(child)
                    .expect("projected children are retained projected nodes");
                let expected = (!node.is_eligible()).then_some(node.hash);
                if child_node.eligibility.inherited_from != expected {
                    return Err(GraphError::InvalidHeaderNode {
                        header: child,
                        invariant: HeaderNodeInvariant::DerivedHeaderState,
                    });
                }
            }
        }
        Ok(())
    }

    /// Recompute eligible-tip membership only for changed nodes and their affected parents.
    fn refresh_delta_eligible_header_tips(&mut self) {
        let mut affected = HashSet::new();
        for node in self.updated_header_nodes_by_hash.values() {
            affected.insert(node.hash);
            affected.insert(node.parent_hash);
        }
        for hash in &self.deleted_header_hashes {
            affected.insert(*hash);
            affected.insert(
                self.base_graph
                    .nodes
                    .get(hash)
                    .expect("deleted hashes were checked against the base graph")
                    .parent_hash,
            );
        }
        affected.insert(self.finalized_frontier.hash);
        for hash in affected {
            self.refresh_eligible_header_tip(hash);
        }
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
        if direct_reasons.len() > super::MAX_DIRECT_ELIGIBILITY_REASONS_V1 {
            return Err(GraphError::DirectEligibilityReasonLimit {
                header: hash,
                limit: super::MAX_DIRECT_ELIGIBILITY_REASONS_V1,
            });
        }

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

        self.retired_inserted_nodes.remove(&hash);

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
            .try_insert_direct_reason(hash, reason)?;
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
        let height = self
            .header_node(hash)
            .ok_or(GraphError::UnknownHeaderNode(hash))?
            .height;
        let tombstone = match &body_validation_state {
            BodyValidationState::ConsensusInvalid { evidence, rule } => {
                Some(ConsensusInvalidBodyTombstone {
                    hash,
                    height,
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
            retired_finalized_path_nodes: self.retired_finalized_path_nodes(),
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

    /// Collect the finalized-path nodes this transition inserted and then retired.
    ///
    /// The walk mirrors [`Self::validate_finalized_descendant`] and emits only the hashes that
    /// walk cannot otherwise resolve, so the evidence stays as small as the proof requires. It
    /// spans the heights the finality advance already traversed, so it adds no new bound.
    ///
    /// An unresolvable or inconsistent hash stops the walk and leaves the evidence short.
    /// The staged frontier is then incoherent, and delta validation reports it.
    fn retired_finalized_path_nodes(&self) -> Vec<HeaderNode> {
        let mut retired = Vec::new();
        if self.finalized_frontier == self.base_graph.finalized_frontier {
            return retired;
        }
        let mut cursor = self.finalized_frontier;
        while cursor.height > self.base_graph.finalized_frontier.height {
            let node = match self
                .updated_header_nodes_by_hash
                .get(&cursor.hash)
                .or_else(|| self.base_graph.nodes.get(&cursor.hash))
            {
                Some(node) => node,
                None => {
                    let Some(node) = self.retired_inserted_nodes.get(&cursor.hash) else {
                        break;
                    };
                    retired.push(node.clone());
                    node
                }
            };
            if node.height != cursor.height {
                break;
            }
            cursor = Frontier::new(block::Height(cursor.height.0 - 1), node.parent_hash);
        }
        retired.sort_unstable_by_key(|node| (node.height, node.hash.0));
        retired
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
        } else {
            // This deletion cancels an insertion from the same transition, so the delta
            // records neither. Keep the node as evidence for the finalized-descendant proof.
            self.retired_inserted_nodes.insert(hash, node.clone());
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

    fn visit_header_nodes(&self, visitor: &mut dyn FnMut(&HeaderNode)) {
        self.header_nodes().for_each(visitor);
    }

    fn view_header_hashes_at_height(&self, height: block::Height) -> Vec<block::Hash> {
        self.header_hashes_at_height(height)
    }

    fn view_header_children(&self, parent: block::Hash) -> Vec<block::Hash> {
        self.header_children(parent)
    }

    fn view_header_has_children(&self, parent: block::Hash) -> bool {
        self.base_graph
            .children
            .get(&parent)
            .into_iter()
            .flatten()
            .any(|child| {
                !self.deleted_header_hashes.contains(child)
                    && self
                        .removed_header_children
                        .get(&parent)
                        .is_none_or(|removed| !removed.contains(child))
            })
            || self
                .added_header_children
                .get(&parent)
                .is_some_and(|children| {
                    children
                        .iter()
                        .any(|child| !self.deleted_header_hashes.contains(child))
                })
    }

    fn view_eligible_header_tips(&self) -> Vec<Frontier> {
        self.eligible_header_tips()
    }

    fn view_eligible_header_tip_count(&self) -> usize {
        self.eligible_header_tips.len()
    }

    fn view_is_eligible_header_tip(&self, hash: block::Hash) -> bool {
        self.eligible_header_tips.contains(&hash)
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
    fn validated_application_contains_exact_sparse_node_edge_and_tip_changes() {
        let base_graph = store();
        let anchor = base_graph.finalized_frontier();
        let mut overlay = GraphOverlay::new(&base_graph);
        let child = overlay
            .insert(
                header(anchor.hash, 1),
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the sparse child inserts");
        let child = match child {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        };

        let application = GraphOverlay::from_delta(&base_graph, &overlay.delta())
            .expect("the sparse delta validates")
            .into_delta_application();

        assert_eq!(application.base_revision, base_graph.graph_revision);
        assert_eq!(application.finalized_frontier, anchor);
        assert_eq!(
            application
                .updated_header_nodes_by_hash
                .get(&child.hash)
                .map(|node| node.height),
            Some(child.height)
        );
        assert!(application
            .added_header_children
            .get(&anchor.hash)
            .is_some_and(|children| children.contains(&child.hash)));
        assert!(!application.eligible_header_tips.contains(&anchor.hash));
        assert!(application.eligible_header_tips.contains(&child.hash));
    }

    #[test]
    fn sparse_deletion_rejects_a_retained_child() {
        let mut base_graph = store();
        let anchor = base_graph.finalized_frontier();
        let parent = insert_child(&mut base_graph, anchor.hash, 1);
        let child = insert_child(&mut base_graph, parent.hash, 2);
        let mut delta = GraphDelta::empty(&base_graph);
        delta.deleted_header_hashes.push(parent.hash);

        assert_eq!(
            GraphOverlay::from_delta(&base_graph, &delta).map(drop),
            Err(GraphError::UnknownParent {
                header: child.hash,
                parent: parent.hash,
            })
        );
    }

    #[test]
    fn sparse_tombstone_requires_matching_retained_body_state() {
        let mut base_graph = store();
        let anchor = base_graph.finalized_frontier();
        let child = insert_child(&mut base_graph, anchor.hash, 1);
        let mut delta = GraphDelta::empty(&base_graph);
        delta
            .new_consensus_invalid_body_tombstones
            .push(ConsensusInvalidBodyTombstone {
                hash: child.hash,
                height: child.height,
                evidence: EvidenceId::from_digest([9; 32]),
                rule: crate::BodyRuleId::new("test.sparse-tombstone"),
            });

        assert!(matches!(
            GraphOverlay::from_delta(&base_graph, &delta),
            Err(GraphError::InvalidHeaderNode {
                header,
                invariant: HeaderNodeInvariant::DerivedHeaderState,
            }) if header == child.hash
        ));
    }

    #[test]
    fn sparse_eligibility_rejects_an_omitted_descendant_update() {
        let mut base_graph = store();
        let anchor = base_graph.finalized_frontier();
        let parent = insert_child(&mut base_graph, anchor.hash, 1);
        let child = insert_child(&mut base_graph, parent.hash, 2);
        let mut overlay = GraphOverlay::new(&base_graph);
        overlay
            .add_eligibility_reason(
                parent.hash,
                EligibilityReason::OperatorInvalid {
                    id: OperatorInvalidationId::new([7; 16]),
                    reason_digest: [8; 32],
                    evidence: EvidenceId::from_digest([9; 32]),
                },
            )
            .expect("the parent invalidation propagates to its child");
        let mut incomplete = overlay.delta();
        incomplete
            .updated_header_nodes
            .retain(|node| node.hash != child.hash);

        assert!(matches!(
            GraphOverlay::from_delta(&base_graph, &incomplete),
            Err(GraphError::InvalidHeaderNode {
                header,
                invariant: HeaderNodeInvariant::DerivedHeaderState,
            }) if header == child.hash
        ));
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

    /// Insert a chain of `length` children onto `parent` in one overlay.
    fn insert_overlay_chain(
        overlay: &mut GraphOverlay<'_>,
        parent: Frontier,
        length: u8,
    ) -> Vec<Frontier> {
        let mut chain = vec![parent];
        let mut cursor = parent;
        for marker in 1..=length {
            cursor = match overlay
                .insert(
                    header(cursor.hash, marker),
                    HeaderValidationState::Valid,
                    [],
                    BodyValidationState::Unknown,
                )
                .expect("the fixture child inserts")
            {
                InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => {
                    frontier
                }
            };
            chain.push(cursor);
        }
        chain
    }

    #[test]
    fn finality_across_same_transition_headers_applies_and_carries_its_evidence() {
        // Finalizing two above the base anchor retires a header this same transition inserted.
        // That header cancels out of both delta sets, so only the carried evidence proves the
        // new frontier descends from the base frontier.
        for advance in 1..=3usize {
            let base_graph = store();
            let anchor = base_graph.finalized_frontier();
            let mut overlay = GraphOverlay::new(&base_graph);
            let chain = insert_overlay_chain(&mut overlay, anchor, 4);
            let new_anchor = chain[advance];
            overlay
                .advance_finalized_frontier(new_anchor)
                .expect("the eligible descendant becomes the new anchor");

            let delta = overlay.delta();
            let retired: Vec<_> = delta
                .retired_finalized_path_nodes
                .iter()
                .map(|node| Frontier::new(node.height, node.hash))
                .collect();
            assert_eq!(
                retired,
                chain[1..advance].to_vec(),
                "the evidence is exactly the inserted headers below the new anchor"
            );
            for node in &delta.retired_finalized_path_nodes {
                assert!(
                    !delta
                        .updated_header_nodes
                        .iter()
                        .any(|updated| updated.hash == node.hash),
                    "retired evidence never doubles as a durable write"
                );
                assert!(
                    !delta.deleted_header_hashes.contains(&node.hash),
                    "retired evidence never doubles as a durable deletion"
                );
            }

            let projected = MemHeaderStore::reconstruct(crate::HeaderGraphReconstruction::new(
                new_anchor,
                overlay.header_nodes().cloned(),
                base_graph.consensus_invalid_body_tombstones().cloned(),
            ))
            .expect("the overlay materializes as a coherent graph");
            let mut applied = base_graph.clone();
            applied
                .apply_delta(&delta)
                .expect("the delta proves descendancy across the retired headers");

            assert_eq!(applied.finalized_frontier, projected.finalized_frontier);
            assert_eq!(applied.nodes, projected.nodes);
            assert_eq!(applied.heights, projected.heights);
            assert_eq!(applied.eligible_header_tips, projected.eligible_header_tips);
            // The retired anchor keeps no child entry: nothing would ever delete it again.
            assert_eq!(applied.children, projected.children);
        }
    }

    #[test]
    fn retired_evidence_conflicting_with_a_live_node_is_rejected() {
        let base_graph = store();
        let anchor = base_graph.finalized_frontier();
        let mut overlay = GraphOverlay::new(&base_graph);
        let chain = insert_overlay_chain(&mut overlay, anchor, 3);
        overlay
            .advance_finalized_frontier(chain[2])
            .expect("the eligible descendant becomes the new anchor");
        let delta = overlay.delta();
        let retired = delta
            .retired_finalized_path_nodes
            .first()
            .expect("finalizing two above the anchor retires one inserted header")
            .clone();

        let mut duplicated = delta.clone();
        duplicated
            .retired_finalized_path_nodes
            .push(retired.clone());
        assert!(matches!(
            GraphOverlay::from_delta(&base_graph, &duplicated),
            Err(GraphError::DuplicateHeaderNode(hash)) if hash == retired.hash
        ));

        let mut also_updated = delta.clone();
        also_updated.updated_header_nodes.push(retired.clone());
        assert!(matches!(
            GraphOverlay::from_delta(&base_graph, &also_updated),
            Err(GraphError::DuplicateHeaderNode(hash)) if hash == retired.hash
        ));

        let mut claims_a_base_node = delta.clone();
        claims_a_base_node.retired_finalized_path_nodes = vec![base_graph
            .nodes
            .get(&anchor.hash)
            .expect("the live graph retains its finalized frontier")
            .clone()];
        assert!(matches!(
            GraphOverlay::from_delta(&base_graph, &claims_a_base_node),
            Err(GraphError::DuplicateHeaderNode(hash)) if hash == anchor.hash
        ));
    }

    #[test]
    fn missing_retired_evidence_fails_the_finalized_descendant_proof() {
        let base_graph = store();
        let anchor = base_graph.finalized_frontier();
        let mut overlay = GraphOverlay::new(&base_graph);
        let chain = insert_overlay_chain(&mut overlay, anchor, 3);
        overlay
            .advance_finalized_frontier(chain[2])
            .expect("the eligible descendant becomes the new anchor");
        let mut delta = overlay.delta();
        let retired = delta
            .retired_finalized_path_nodes
            .first()
            .expect("finalizing two above the anchor retires one inserted header")
            .hash;
        delta.retired_finalized_path_nodes.clear();

        assert!(matches!(
            GraphOverlay::from_delta(&base_graph, &delta),
            Err(GraphError::UnknownHeaderNode(hash)) if hash == retired
        ));
    }
}
