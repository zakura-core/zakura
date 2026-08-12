#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use zakura_chain::{block, work::difficulty::Work};

use super::{GraphError, HeaderGraphEdit, HeaderGraphView, InsertResult, MemHeaderStore};
use crate::{
    BodyValidationState, ChainScore, EligibilityReason, EligibilityState, EvidenceId, Frontier,
    HeaderNode, HeaderValidationState, OperatorInvalidationId,
};

#[cfg(test)]
#[derive(Default)]
struct OverlayTestStatistics {
    base_nodes_cloned: Cell<usize>,
    eligibility_nodes_visited: Cell<usize>,
    finality_nodes_visited: Cell<usize>,
}

#[cfg(test)]
impl OverlayTestStatistics {
    fn increment(counter: &Cell<usize>) {
        counter.set(counter.get().saturating_add(1));
    }

    fn operation_counts(&self, changed_nodes: usize) -> (usize, usize, usize) {
        (
            self.base_nodes_cloned.get(),
            changed_nodes,
            self.eligibility_nodes_visited.get(),
        )
    }
}

// Changes made by an overlay relative to the base graph.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct GraphDelta {
    // If None, the finalized frontier is unchanged.
    pub(crate) finalized: Option<Frontier>,
    pub(crate) put_nodes: Vec<HeaderNode>,
    pub(crate) delete_nodes: Vec<block::Hash>,
}

/// A mutable view that stages graph changes over a borrowed base store.
///
/// Reads reflect both the base and staged changes. The changes can later be
/// extracted as a [`GraphDelta`] without modifying the base.
///
/// A staged deletion hides a base node from every overlay read. When the overlay
/// deletes a node that it inserted, the deletion cancels the insertion. The final
/// delta contains only base-node deletions and node values that differ from the
/// base graph.
pub(crate) struct GraphOverlay<'a> {
    base: &'a MemHeaderStore,
    finalized: Frontier,
    puts: HashMap<block::Hash, HeaderNode>,
    deletes: HashSet<block::Hash>,
    add_children: HashMap<block::Hash, HashSet<block::Hash>>,
    remove_children: HashMap<block::Hash, HashSet<block::Hash>>,
    eligible_tips: HashSet<block::Hash>,
    node_count: usize,
    #[cfg(test)]
    test_statistics: OverlayTestStatistics,
}

impl<'a> GraphOverlay<'a> {
    pub(crate) fn new(base: &'a MemHeaderStore) -> Self {
        Self {
            base,
            finalized: base.finalized,
            puts: HashMap::new(),
            deletes: HashSet::new(),
            add_children: HashMap::new(),
            remove_children: HashMap::new(),
            eligible_tips: base.eligible_tips.clone(),
            node_count: base.nodes.len(),
            #[cfg(test)]
            test_statistics: OverlayTestStatistics::default(),
        }
    }

    /// Reconstructs the projected graph view represented by `delta` over `base`
    /// without modifying the base store.
    pub(crate) fn from_delta(
        base: &'a MemHeaderStore,
        delta: &GraphDelta,
    ) -> Result<Self, GraphError> {
        let indexes = base.derive_index_changes(delta)?;
        let puts = delta
            .put_nodes
            .iter()
            .cloned()
            .map(|node| (node.hash, node))
            .collect();
        let deletes = delta.delete_nodes.iter().copied().collect();
        let mut add_children: HashMap<_, HashSet<_>> = HashMap::new();
        let mut remove_children: HashMap<_, HashSet<_>> = HashMap::new();
        for (parent, child) in &indexes.add_children {
            add_children.entry(*parent).or_default().insert(*child);
        }
        for (parent, child) in &indexes.remove_children {
            remove_children.entry(*parent).or_default().insert(*child);
        }
        let mut eligible_tips = base.eligible_tips.clone();
        for hash in &indexes.remove_eligible_tips {
            eligible_tips.remove(hash);
        }
        eligible_tips.extend(indexes.add_eligible_tips.iter().copied());
        Ok(Self {
            base,
            finalized: delta.finalized.unwrap_or(base.finalized),
            puts,
            deletes,
            add_children,
            remove_children,
            eligible_tips,
            node_count: base
                .nodes
                .len()
                .saturating_add(
                    delta
                        .put_nodes
                        .iter()
                        .filter(|node| !base.nodes.contains_key(&node.hash))
                        .count(),
                )
                .saturating_sub(delta.delete_nodes.len()),
            #[cfg(test)]
            test_statistics: OverlayTestStatistics::default(),
        })
    }

    pub(crate) const fn finalized(&self) -> Frontier {
        self.finalized
    }

    pub(crate) const fn node_count(&self) -> usize {
        self.node_count
    }

    /// Returns the node with the given hash, if it is retained.
    pub(crate) fn node(&self, hash: block::Hash) -> Option<&HeaderNode> {
        if self.deletes.contains(&hash) {
            return None;
        }
        self.puts.get(&hash).or_else(|| self.base.nodes.get(&hash))
    }

    /// Returns a mutable reference to the node with the given hash, creating it if it is not present.
    pub(crate) fn node_mut(&mut self, hash: block::Hash) -> Result<&mut HeaderNode, GraphError> {
        if self.deletes.contains(&hash) {
            return Err(GraphError::UnknownNode(hash));
        }
        if !self.puts.contains_key(&hash) {
            let node = self
                .base
                .nodes
                .get(&hash)
                .cloned()
                .ok_or(GraphError::UnknownNode(hash))?;
            #[cfg(test)]
            OverlayTestStatistics::increment(&self.test_statistics.base_nodes_cloned);
            self.puts.insert(hash, node);
        }
        Ok(self
            .puts
            .get_mut(&hash)
            .expect("overlay node exists because it was staged above"))
    }

    /// Iterates over all nodes visible through the overlay, excluding deleted
    /// nodes and replacing base nodes with their staged versions.
    pub(crate) fn nodes(&self) -> impl Iterator<Item = &HeaderNode> {
        self.base
            .nodes
            .values()
            .filter(|node| {
                !self.deletes.contains(&node.hash) && !self.puts.contains_key(&node.hash)
            })
            .chain(
                self.puts
                    .values()
                    .filter(|node| !self.deletes.contains(&node.hash)),
            )
    }

    /// Returns an iterator over all hashes that are currently retained.
    pub(crate) fn retained_hashes(&self) -> impl Iterator<Item = block::Hash> + '_ {
        self.nodes().map(|node| node.hash)
    }

    /// Returns all hashes at the given height, including both base and staged nodes.
    pub(crate) fn hashes_at_height(&self, height: block::Height) -> Vec<block::Hash> {
        let mut hashes: HashSet<_> = self
            .base
            .heights
            .get(&height)
            .into_iter()
            .flatten()
            .filter(|hash| !self.deletes.contains(hash) && !self.puts.contains_key(hash))
            .copied()
            .collect();
        hashes.extend(
            self.puts
                .values()
                .filter(|node| node.height == height && !self.deletes.contains(&node.hash))
                .map(|node| node.hash),
        );
        let mut hashes: Vec<_> = hashes.into_iter().collect();
        hashes.sort_unstable_by_key(|hash| hash.0);
        hashes
    }

    /// Returns all children of the given parent, including both base and staged nodes.
    pub(crate) fn children(&self, parent: block::Hash) -> Vec<block::Hash> {
        if self.node(parent).is_none() {
            return Vec::new();
        }
        let removed = self.remove_children.get(&parent);
        let mut children: HashSet<_> = self
            .base
            .children
            .get(&parent)
            .into_iter()
            .flatten()
            .filter(|child| {
                !self.deletes.contains(child)
                    && removed.is_none_or(|removed| !removed.contains(child))
            })
            .copied()
            .collect();
        children.extend(
            self.add_children
                .get(&parent)
                .into_iter()
                .flatten()
                .filter(|child| !self.deletes.contains(child))
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
    /// This method does not perform header consensus or proof-of-work validation;
    /// it records the caller-supplied validation result and work.
    pub(crate) fn insert(
        &mut self,
        header: Arc<block::Header>,
        block_work: Work,
        validation: HeaderValidationState,
        direct_reasons: impl IntoIterator<Item = EligibilityReason>,
        body: BodyValidationState,
    ) -> Result<InsertResult, GraphError> {
        let hash = header.hash();

        // If already present, return the existing frontier.
        if let Some(existing) = self.node(hash) {
            if existing.header == header {
                return Ok(InsertResult::AlreadyPresent(Frontier::new(
                    existing.height,
                    hash,
                )));
            }
            return Err(GraphError::ConflictingDuplicate(hash));
        }

        // Ensure the parent is visible through the overlay.
        let parent_hash = header.previous_block_hash;
        let parent = self.node(parent_hash).ok_or(GraphError::UnknownParent {
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
        let work_coordinate = parent.work_coordinate().checked_add(block_work)?;

        // Collect the direct reasons into a set.
        let direct_reasons: BTreeSet<_> = direct_reasons.into_iter().collect();

        // If the header is valid, has no direct reasons, and no inherited eligibility, it is eligible.
        let eligible = validation == HeaderValidationState::Valid
            && direct_reasons.is_empty()
            && inherited_from.is_none()
            && !matches!(body, BodyValidationState::ConsensusInvalid { .. });

        // Insert the node into the overlay.
        self.puts.insert(
            hash,
            HeaderNode {
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
                body_validation_state: body,
                aux_delivery_ids: Vec::new(),
            },
        );

        // Remove the parent from the eligible tips if it is no longer eligible.
        self.deletes.remove(&hash);

        // Record the new child-parent edge.
        self.record_child_add(parent_hash, hash);

        // Update the node count.
        self.node_count = self.node_count.saturating_add(1);

        // If the header is eligible, add it to the eligible tips.
        if eligible {
            self.eligible_tips.remove(&parent_hash);
            self.eligible_tips.insert(hash);
        }

        // Return the new frontier.
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
            .node_mut(hash)?
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
        let reasons = &mut self.node_mut(hash)?.eligibility.direct_reasons;
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
    pub(crate) fn set_body_state(
        &mut self,
        hash: block::Hash,
        body_validation_state: BodyValidationState,
    ) -> Result<bool, GraphError> {
        let (changed, eligibility_changed) = {
            let node = self.node_mut(hash)?;
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
        Ok(changed)
    }

    pub(crate) fn set_validation(
        &mut self,
        hash: block::Hash,
        validation: HeaderValidationState,
    ) -> Result<bool, GraphError> {
        let node = self.node_mut(hash)?;
        let changed = node.validation != validation;
        node.validation = validation;
        if changed {
            self.recompute_descendant_eligibility(hash)?;
        }
        Ok(changed)
    }

    pub(crate) fn ancestor(
        &self,
        descendant: block::Hash,
        height: block::Height,
    ) -> Result<Option<Frontier>, GraphError> {
        let mut node = self
            .node(descendant)
            .ok_or(GraphError::UnknownNode(descendant))?;
        if height > node.height {
            return Err(GraphError::InvalidAncestorHeight {
                ancestor: height,
                descendant: node.height,
            });
        }
        while node.height > height {
            let Some(parent) = self.node(node.parent_hash) else {
                return Ok(None);
            };
            node = parent;
        }
        Ok(Some(Frontier::new(node.height, node.hash)))
    }

    pub(crate) fn eligible_tips(&self) -> Vec<Frontier> {
        let mut tips: Vec<_> = self
            .eligible_tips
            .iter()
            .filter_map(|hash| self.node(*hash))
            .map(|node| Frontier::new(node.height, node.hash))
            .collect();
        tips.sort_unstable_by_key(|tip| tip.hash.0);
        tips
    }

    pub(crate) fn select_header_best(&self) -> Result<(Frontier, ChainScore), GraphError> {
        let anchor = self
            .node(self.finalized.hash)
            .ok_or(GraphError::UnknownNode(self.finalized.hash))?;
        let mut best = None;
        for hash in &self.eligible_tips {
            let node = self
                .node(*hash)
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
        best.ok_or(GraphError::UnknownNode(self.finalized.hash))
    }

    /// Computes the chain score of the given header relative to the finalized header.
    pub(crate) fn score(&self, hash: block::Hash) -> Result<ChainScore, GraphError> {
        let anchor = self
            .node(self.finalized.hash)
            .ok_or(GraphError::UnknownNode(self.finalized.hash))?;
        let node = self.node(hash).ok_or(GraphError::UnknownNode(hash))?;
        Ok(ChainScore::new(
            node.work_coordinate()
                .suffix_after(anchor.work_coordinate())?,
            hash,
        ))
    }

    /// Advances the overlay’s finality anchor to an eligible retained descendant.
    ///
    /// This method walks the parent path from the new anchor to the current
    /// anchor. At each path node, it traverses and removes discarded sibling
    /// subtrees. The traversal does not assume that the retained graph is a linked
    /// list. The graph retains the new anchor and its entire descendant subtree.
    /// The method sorts the returned hashes by raw bytes for deterministic output.
    ///
    /// This method rejects an unknown or height-mismatched frontier. It also
    /// rejects an ineligible node or a node outside the current anchor's
    /// descendants. These validation failures leave the overlay unchanged.
    ///
    /// This method only stages the graph changes. The transition layer authorizes
    /// and durably commits the finality advance.
    ///
    /// The method does not traverse the retained descendant subtree.
    pub(crate) fn advance_finalized(
        &mut self,
        finalized: Frontier,
    ) -> Result<Vec<block::Hash>, GraphError> {
        let node = self
            .node(finalized.hash)
            .ok_or(GraphError::UnknownNode(finalized.hash))?;
        if node.height != finalized.height {
            return Err(GraphError::UnknownNode(finalized.hash));
        }
        if !node.is_eligible() {
            return Err(GraphError::IneligibleFinalized(finalized.hash));
        }

        let current = self.finalized;
        let mut finalized_path = Vec::new();
        let mut cursor = finalized;
        while cursor.height > current.height {
            #[cfg(test)]
            OverlayTestStatistics::increment(&self.test_statistics.finality_nodes_visited);
            let parent_hash = self
                .node(cursor.hash)
                .ok_or(GraphError::UnknownNode(cursor.hash))?
                .parent_hash;
            finalized_path.push((parent_hash, cursor.hash));
            cursor = Frontier::new(block::Height(cursor.height.0 - 1), parent_hash);
        }
        if cursor != current {
            return Err(GraphError::FinalizedNotDescendant {
                current: current.hash,
                candidate: finalized.hash,
            });
        }

        // Collect the removed hashes.
        let mut deleted = HashSet::new();
        for (ancestor, retained_child) in finalized_path {
            deleted.insert(ancestor);
            for sibling in self.children(ancestor) {
                if sibling == retained_child {
                    continue;
                }
                let mut pending = vec![sibling];
                while let Some(hash) = pending.pop() {
                    if deleted.insert(hash) {
                        #[cfg(test)]
                        OverlayTestStatistics::increment(
                            &self.test_statistics.finality_nodes_visited,
                        );
                        pending.extend(self.children(hash));
                    }
                }
            }
        }
        let mut deleted: Vec<_> = deleted.into_iter().collect();
        deleted.sort_unstable_by_key(|hash| hash.0);
        for hash in &deleted {
            self.delete_node(*hash)?;
        }
        self.finalized = finalized;
        self.node_mut(finalized.hash)?.eligibility.inherited_from = None;
        self.refresh_eligible_tip(finalized.hash);
        Ok(deleted)
    }

    /// Removes a leaf node from the overlay.
    pub(crate) fn remove_leaf(&mut self, hash: block::Hash) -> Result<(), GraphError> {
        if !self.children(hash).is_empty() {
            return Err(GraphError::NodeHasChildren(hash));
        }
        self.delete_node(hash)
    }

    // Include only nodes whose final projected value differs from the base.
    pub(crate) fn delta(&self) -> GraphDelta {
        let mut put_nodes: Vec<_> = self
            .puts
            .values()
            .filter(|node| {
                !self.deletes.contains(&node.hash) && self.base.nodes.get(&node.hash) != Some(*node)
            })
            .cloned()
            .collect();
        put_nodes.sort_unstable_by_key(|node| (node.height, node.hash.0));
        let mut delete_nodes: Vec<_> = self.deletes.iter().copied().collect();
        delete_nodes.sort_unstable_by_key(|hash| hash.0);
        GraphDelta {
            finalized: (self.finalized != self.base.finalized).then_some(self.finalized),
            put_nodes,
            delete_nodes,
        }
    }

    /// Removes a node from the overlay.
    fn delete_node(&mut self, hash: block::Hash) -> Result<(), GraphError> {
        let node = self
            .node(hash)
            .cloned()
            .ok_or(GraphError::UnknownNode(hash))?;
        self.puts.remove(&hash);
        if self.base.nodes.contains_key(&hash) {
            self.deletes.insert(hash);
        }
        self.record_child_remove(node.parent_hash, hash);
        self.eligible_tips.remove(&hash);
        self.node_count = self.node_count.saturating_sub(1);
        self.refresh_eligible_tip(node.parent_hash);
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
            self.node(root)
                .ok_or(GraphError::UnknownNode(root))?
                .parent_hash,
        );
        let mut queue = VecDeque::from(self.children(root));
        while let Some(hash) = queue.pop_front() {
            #[cfg(test)]
            OverlayTestStatistics::increment(&self.test_statistics.eligibility_nodes_visited);

            // Get the parent hash.
            let parent_hash = self
                .node(hash)
                .ok_or(GraphError::UnknownNode(hash))?
                .parent_hash;
            affected.insert(parent_hash);

            // Check if the parent is eligible.
            let inherited_from = (!self
                .node(parent_hash)
                .ok_or(GraphError::UnknownNode(parent_hash))?
                .is_eligible())
            .then_some(parent_hash);

            // Check if the node is already eligible.
            if self
                .node(hash)
                .is_some_and(|node| node.eligibility.inherited_from == inherited_from)
            {
                continue;
            }

            // Update the node's inherited eligibility.
            self.node_mut(hash)?.eligibility.inherited_from = inherited_from;
            affected.insert(hash);

            // Add the children to the queue.
            queue.extend(self.children(hash));
        }

        // Refresh the eligible tips for the affected nodes.
        for hash in affected {
            self.refresh_eligible_tip(hash);
        }
        Ok(())
    }

    /// Checks if the given node has any eligible children.
    fn has_eligible_child(&self, hash: block::Hash) -> bool {
        self.children(hash)
            .into_iter()
            .any(|child| self.node(child).is_some_and(HeaderNode::is_eligible))
    }

    /// Refreshes the eligible tips for the given node.
    fn refresh_eligible_tip(&mut self, hash: block::Hash) {
        self.eligible_tips.remove(&hash);
        if self
            .node(hash)
            .is_some_and(|node| node.is_eligible() && !self.has_eligible_child(hash))
        {
            self.eligible_tips.insert(hash);
        }
    }

    /// Records a new child-parent edge.
    fn record_child_add(&mut self, parent: block::Hash, child: block::Hash) {
        if self
            .remove_children
            .get_mut(&parent)
            .is_some_and(|removed| removed.remove(&child))
        {
            return;
        }
        self.add_children.entry(parent).or_default().insert(child);
    }

    /// Records a removed child-parent edge.
    fn record_child_remove(&mut self, parent: block::Hash, child: block::Hash) {
        if self
            .add_children
            .get_mut(&parent)
            .is_some_and(|added| added.remove(&child))
        {
            return;
        }
        if self
            .base
            .children
            .get(&parent)
            .is_some_and(|children| children.contains(&child))
        {
            self.remove_children
                .entry(parent)
                .or_default()
                .insert(child);
        }
    }

    #[cfg(test)]
    fn operation_counts(&self) -> (usize, usize, usize) {
        self.test_statistics
            .operation_counts(self.puts.len().saturating_add(self.deletes.len()))
    }

    #[cfg(test)]
    fn finality_nodes_visited(&self) -> usize {
        self.test_statistics.finality_nodes_visited.get()
    }
}

impl HeaderGraphView for GraphOverlay<'_> {
    fn view_finalized(&self) -> Frontier {
        self.finalized()
    }

    fn view_node_count(&self) -> usize {
        self.node_count()
    }

    fn view_node(&self, hash: block::Hash) -> Option<&HeaderNode> {
        self.node(hash)
    }

    fn view_nodes(&self) -> Vec<&HeaderNode> {
        self.nodes().collect()
    }

    fn view_retained_hashes(&self) -> Vec<block::Hash> {
        self.retained_hashes().collect()
    }

    fn view_hashes_at_height(&self, height: block::Height) -> Vec<block::Hash> {
        self.hashes_at_height(height)
    }

    fn view_children(&self, parent: block::Hash) -> Vec<block::Hash> {
        self.children(parent)
    }

    fn view_eligible_tips(&self) -> Vec<Frontier> {
        self.eligible_tips()
    }

    fn view_select_header_best(&self) -> Result<(Frontier, ChainScore), GraphError> {
        self.select_header_best()
    }

    fn view_score(&self, hash: block::Hash) -> Result<ChainScore, GraphError> {
        self.score(hash)
    }

    fn view_ancestor(
        &self,
        descendant: block::Hash,
        height: block::Height,
    ) -> Result<Option<Frontier>, GraphError> {
        self.ancestor(descendant, height)
    }
}

impl HeaderGraphEdit for GraphOverlay<'_> {
    fn edit_node_mut(&mut self, hash: block::Hash) -> Result<&mut HeaderNode, GraphError> {
        self.node_mut(hash)
    }

    fn edit_insert(
        &mut self,
        header: Arc<block::Header>,
        block_work: Work,
        validation: HeaderValidationState,
        direct_reasons: Vec<EligibilityReason>,
        body: BodyValidationState,
    ) -> Result<InsertResult, GraphError> {
        self.insert(header, block_work, validation, direct_reasons, body)
    }

    fn edit_add_eligibility_reason(
        &mut self,
        hash: block::Hash,
        reason: EligibilityReason,
    ) -> Result<bool, GraphError> {
        self.add_eligibility_reason(hash, reason)
    }

    fn edit_remove_operator_invalidation(
        &mut self,
        hash: block::Hash,
        id: OperatorInvalidationId,
        evidence: Option<EvidenceId>,
    ) -> Result<bool, GraphError> {
        self.remove_operator_invalidation(hash, id, evidence)
    }

    fn edit_set_body_state(
        &mut self,
        hash: block::Hash,
        body: BodyValidationState,
    ) -> Result<bool, GraphError> {
        self.set_body_state(hash, body)
    }

    fn edit_set_validation(
        &mut self,
        hash: block::Hash,
        validation: HeaderValidationState,
    ) -> Result<bool, GraphError> {
        self.set_validation(hash, validation)
    }

    fn edit_advance_finalized(
        &mut self,
        finalized: Frontier,
    ) -> Result<Vec<block::Hash>, GraphError> {
        self.advance_finalized(finalized)
    }

    fn edit_remove_leaf(&mut self, hash: block::Hash) -> Result<(), GraphError> {
        self.remove_leaf(hash)
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
        let work = header
            .difficulty_threshold
            .to_work()
            .expect("the fixture target has valid work");
        match store
            .insert(
                header,
                work,
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
        let base = store();
        let anchor = base.finalized();
        let mut overlay = GraphOverlay::new(&base);
        let child_header = header(anchor.hash, 1);
        let work = child_header
            .difficulty_threshold
            .to_work()
            .expect("the child target has valid work");
        let child = match overlay
            .insert(
                child_header,
                work,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the overlay child inserts")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        };
        let delta = overlay.delta();
        assert_eq!(delta.put_nodes.len(), 1);
        assert_eq!(delta.put_nodes[0].hash, child.hash);
        assert_eq!(delta.delete_nodes, Vec::<block::Hash>::new());
        assert_eq!(overlay.node_count(), base.node_count().saturating_add(1));
    }

    #[test]
    fn applying_delta_matches_the_complete_overlay_projection() {
        let mut base = store();
        let anchor = base.finalized();
        let first_header = header(anchor.hash, 1);
        let work = first_header
            .difficulty_threshold
            .to_work()
            .expect("the fixture target has valid work");
        let first = match base
            .insert(
                first_header,
                work,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the first base child inserts")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        };
        let second_header = header(first.hash, 2);
        let second = match base
            .insert(
                second_header.clone(),
                work,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the second base child inserts")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        };
        let fork_header = header(anchor.hash, 3);
        let fork = match base
            .insert(
                fork_header,
                work,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the base fork inserts")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        };

        let mut overlay = GraphOverlay::new(&base);
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
            .set_body_state(second.hash, BodyValidationState::CommitmentMatched)
            .expect("body knowledge changes");
        let third_header = header(second.hash, 4);
        let third = match overlay
            .insert(
                third_header,
                work,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the overlay extension inserts")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        };
        overlay
            .remove_leaf(fork.hash)
            .expect("the unselected fork is removable");
        overlay
            .advance_finalized(first)
            .expect("the eligible descendant becomes the new anchor");

        let delta = overlay.delta();
        let projected = MemHeaderStore::from_nodes(first, overlay.nodes().cloned())
            .expect("the overlay materializes as a coherent graph");
        let mut applied = base.clone();
        applied
            .apply_delta(&delta)
            .expect("the verified delta applies to its base");

        assert_eq!(applied.finalized, projected.finalized);
        assert_eq!(applied.nodes, projected.nodes);
        assert_eq!(applied.children, projected.children);
        assert_eq!(applied.heights, projected.heights);
        assert_eq!(applied.eligible_tips, projected.eligible_tips);
        assert_eq!(applied.select_header_best(), projected.select_header_best());
        assert_eq!(applied.finalized(), first);
        assert!(applied.node(anchor.hash).is_none());
        assert!(applied.node(fork.hash).is_none());
        assert_eq!(
            applied.node(third.hash).map(|node| node.parent_hash),
            Some(second.hash)
        );
    }

    #[test]
    fn canonical_deletion_removes_child_and_tip_indexes() {
        let mut base = store();
        let anchor = base.finalized();
        let child = insert_child(&mut base, anchor.hash, 1);
        let delta = GraphDelta {
            finalized: None,
            put_nodes: Vec::new(),
            delete_nodes: vec![child.hash],
        };

        let projected = GraphOverlay::from_delta(&base, &delta)
            .expect("canonical deletion reconstructs an overlay");
        assert!(projected.node(child.hash).is_none());
        assert!(projected.children(anchor.hash).is_empty());
        assert_eq!(projected.eligible_tips(), vec![anchor]);

        base.apply_delta(&delta)
            .expect("canonical deletion applies to the store");
        assert!(base.node(child.hash).is_none());
        assert!(base.children(anchor.hash).is_empty());
        assert_eq!(base.eligible_tips(), vec![anchor]);
    }

    #[test]
    fn conflicting_node_changes_fail_before_store_mutation() {
        let mut base = store();
        let anchor = base.finalized();
        let child = insert_child(&mut base, anchor.hash, 1);
        let child_node = base
            .node(child.hash)
            .expect("the fixture child is retained")
            .clone();
        let before_nodes = base.nodes.clone();
        let before_children = base.children.clone();
        let before_heights = base.heights.clone();
        let before_tips = base.eligible_tips.clone();
        let delta = GraphDelta {
            finalized: None,
            put_nodes: vec![child_node],
            delete_nodes: vec![child.hash],
        };

        assert_eq!(
            base.apply_delta(&delta),
            Err(GraphError::ConflictingDuplicate(child.hash))
        );
        assert_eq!(base.nodes, before_nodes);
        assert_eq!(base.children, before_children);
        assert_eq!(base.heights, before_heights);
        assert_eq!(base.eligible_tips, before_tips);
    }

    #[test]
    fn finality_delta_keeps_the_new_root_after_deleting_its_parent() {
        let mut base = store();
        let old_root = base.finalized();
        let new_root = insert_child(&mut base, old_root.hash, 1);
        let retained_tip = insert_child(&mut base, new_root.hash, 2);
        let mut overlay = GraphOverlay::new(&base);
        overlay
            .advance_finalized(new_root)
            .expect("the eligible child becomes finalized");
        let delta = overlay.delta();

        assert!(delta.delete_nodes.contains(&old_root.hash));
        let projected = GraphOverlay::from_delta(&base, &delta)
            .expect("the new root may outlive its deleted parent");
        assert_eq!(projected.finalized(), new_root);
        assert!(projected.node(new_root.hash).is_some());
        assert!(projected.node(retained_tip.hash).is_some());

        base.apply_delta(&delta)
            .expect("the finality delta applies atomically");
        assert_eq!(base.finalized(), new_root);
        assert!(base.node(old_root.hash).is_none());
        assert!(base.node(new_root.hash).is_some());
        assert!(base.node(retained_tip.hash).is_some());
    }

    #[test]
    fn advancing_finality_does_not_walk_the_retained_descendant_subtree() {
        let mut base = store();
        let anchor = base.finalized();
        let finalized = insert_child(&mut base, anchor.hash, 1);
        let mut retained_tip = finalized;
        for marker in 2..=64 {
            retained_tip = insert_child(&mut base, retained_tip.hash, marker);
        }

        let mut overlay = GraphOverlay::new(&base);
        let deleted = overlay
            .advance_finalized(finalized)
            .expect("the direct eligible descendant becomes finalized");

        assert_eq!(deleted, vec![anchor.hash]);
        assert_eq!(overlay.finality_nodes_visited(), 1);
        assert_eq!(overlay.finalized(), finalized);
        assert!(overlay.node(retained_tip.hash).is_some());
        assert_eq!(overlay.node_count(), 64);
    }

    #[test]
    fn insertion_clones_no_base_nodes_and_eligibility_skips_unaffected_branches() {
        let mut base = store();
        let anchor = base.finalized();
        let work = base
            .node(anchor.hash)
            .expect("the anchor is retained")
            .block_work;
        let left_header = header(anchor.hash, 1);
        let left = match base
            .insert(
                left_header,
                work,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the left branch inserts")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        };
        let right_header = header(anchor.hash, 2);
        let right = match base
            .insert(
                right_header,
                work,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the right branch inserts")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        };
        let right_child_header = header(right.hash, 3);
        let right_child = match base
            .insert(
                right_child_header,
                work,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the unrelated descendant inserts")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        };
        let left_child_header = header(left.hash, 4);
        let left_child = match base
            .insert(
                left_child_header,
                work,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the affected descendant inserts")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        };

        let mut insertion = GraphOverlay::new(&base);
        insertion
            .insert(
                header(left_child.hash, 5),
                work,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the overlay insertion succeeds");
        assert_eq!(insertion.operation_counts(), (0, 1, 0));

        let mut eligibility = GraphOverlay::new(&base);
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
        let (base_clones, changed_nodes, eligibility_visits) = eligibility.operation_counts();
        assert_eq!(base_clones, 2);
        assert_eq!(changed_nodes, 2);
        assert_eq!(eligibility_visits, 1);
        assert!(!eligibility
            .node(left_child.hash)
            .is_some_and(HeaderNode::is_eligible));
        assert!(eligibility
            .node(right.hash)
            .is_some_and(HeaderNode::is_eligible));
        assert!(eligibility
            .node(right_child.hash)
            .is_some_and(HeaderNode::is_eligible));
    }

    #[test]
    fn production_planner_has_no_full_graph_clone_or_node_map_diff() {
        let planner = include_str!("../transition/planner.rs");
        assert!(!planner.contains("let mut graph = engine.graph().clone()"));
        assert!(!planner.contains("fn node_map"));
        assert!(!planner.contains("old_nodes"));
        assert!(!planner.contains("new_nodes"));
    }
}
