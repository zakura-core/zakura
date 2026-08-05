#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use zakura_chain::{block, work::difficulty::Work};

use super::{GraphError, InsertResult, MemHeaderStore};
use crate::{
    BodyRuleId, BodyValidationState, ChainScore, EligibilityReason, EligibilityState, EvidenceId,
    Frontier, HeaderNode, HeaderValidationState, OperatorInvalidationId,
};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct GraphDelta {
    pub(crate) finalized: Option<Frontier>,
    pub(crate) put_nodes: Vec<HeaderNode>,
    pub(crate) delete_nodes: Vec<block::Hash>,
    pub(crate) add_children: Vec<(block::Hash, block::Hash)>,
    pub(crate) remove_children: Vec<(block::Hash, block::Hash)>,
    pub(crate) add_eligible_tips: Vec<block::Hash>,
    pub(crate) remove_eligible_tips: Vec<block::Hash>,
}

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
    base_nodes_cloned: Cell<usize>,
    #[cfg(test)]
    eligibility_nodes_visited: Cell<usize>,
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
            base_nodes_cloned: Cell::new(0),
            #[cfg(test)]
            eligibility_nodes_visited: Cell::new(0),
        }
    }

    pub(crate) fn from_delta(base: &'a MemHeaderStore, delta: &GraphDelta) -> Self {
        let puts = delta
            .put_nodes
            .iter()
            .cloned()
            .map(|node| (node.hash, node))
            .collect();
        let deletes = delta.delete_nodes.iter().copied().collect();
        let mut add_children: HashMap<_, HashSet<_>> = HashMap::new();
        let mut remove_children: HashMap<_, HashSet<_>> = HashMap::new();
        for (parent, child) in &delta.add_children {
            add_children.entry(*parent).or_default().insert(*child);
        }
        for (parent, child) in &delta.remove_children {
            remove_children.entry(*parent).or_default().insert(*child);
        }
        let mut eligible_tips = base.eligible_tips.clone();
        for hash in &delta.remove_eligible_tips {
            eligible_tips.remove(hash);
        }
        eligible_tips.extend(delta.add_eligible_tips.iter().copied());
        Self {
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
            base_nodes_cloned: Cell::new(0),
            #[cfg(test)]
            eligibility_nodes_visited: Cell::new(0),
        }
    }

    pub(crate) const fn finalized(&self) -> Frontier {
        self.finalized
    }

    pub(crate) const fn node_count(&self) -> usize {
        self.node_count
    }

    pub(crate) fn node(&self, hash: block::Hash) -> Option<&HeaderNode> {
        if self.deletes.contains(&hash) {
            return None;
        }
        self.puts.get(&hash).or_else(|| self.base.nodes.get(&hash))
    }

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
            self.base_nodes_cloned
                .set(self.base_nodes_cloned.get().saturating_add(1));
            self.puts.insert(hash, node);
        }
        self.puts
            .get_mut(&hash)
            .ok_or(GraphError::UnknownNode(hash))
    }

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

    pub(crate) fn retained_hashes(&self) -> impl Iterator<Item = block::Hash> + '_ {
        self.nodes().map(|node| node.hash)
    }

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

    pub(crate) fn insert(
        &mut self,
        header: Arc<block::Header>,
        block_work: Work,
        validation: HeaderValidationState,
        direct_reasons: impl IntoIterator<Item = EligibilityReason>,
        body: BodyValidationState,
    ) -> Result<InsertResult, GraphError> {
        let hash = header.hash();
        if let Some(existing) = self.node(hash) {
            if existing.header == header {
                return Ok(InsertResult::AlreadyPresent(Frontier::new(
                    existing.height,
                    hash,
                )));
            }
            return Err(GraphError::ConflictingDuplicate(hash));
        }
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
        let direct_reasons: BTreeSet<_> = direct_reasons.into_iter().collect();
        let body_reason = match &body {
            BodyValidationState::ConsensusInvalid { evidence, rule } => {
                Some(EligibilityReason::ConsensusBodyInvalid {
                    evidence: *evidence,
                    rule: rule.clone(),
                })
            }
            _ => None,
        };
        let recorded_body_reasons = direct_reasons
            .iter()
            .filter(|reason| matches!(reason, EligibilityReason::ConsensusBodyInvalid { .. }))
            .count();
        if body_reason
            .as_ref()
            .is_some_and(|reason| !direct_reasons.contains(reason))
            || (body_reason.is_none() && recorded_body_reasons != 0)
            || recorded_body_reasons > 1
        {
            return Err(GraphError::BodyEligibilityMismatch(hash));
        }
        let eligible = validation == HeaderValidationState::Valid
            && direct_reasons.is_empty()
            && inherited_from.is_none();
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
                body,
                aux_delivery_ids: Vec::new(),
            },
        );
        self.deletes.remove(&hash);
        self.record_child_add(parent_hash, hash);
        self.node_count = self.node_count.saturating_add(1);
        if eligible {
            self.eligible_tips.remove(&parent_hash);
            self.eligible_tips.insert(hash);
        }
        Ok(InsertResult::Inserted(Frontier::new(height, hash)))
    }

    pub(crate) fn add_reason(
        &mut self,
        hash: block::Hash,
        reason: EligibilityReason,
    ) -> Result<bool, GraphError> {
        if matches!(reason, EligibilityReason::ConsensusBodyInvalid { .. }) {
            return Err(GraphError::BodyEligibilityMismatch(hash));
        }
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

    pub(crate) fn remove_operator_invalidation(
        &mut self,
        hash: block::Hash,
        id: OperatorInvalidationId,
    ) -> Result<bool, GraphError> {
        let changed = self
            .node_mut(hash)?
            .eligibility
            .direct_reasons
            .remove(&EligibilityReason::OperatorInvalid { id });
        if changed {
            self.recompute_descendant_eligibility(hash)?;
        }
        Ok(changed)
    }

    pub(crate) fn set_consensus_body_invalid(
        &mut self,
        hash: block::Hash,
        evidence: EvidenceId,
        rule: BodyRuleId,
    ) -> Result<bool, GraphError> {
        let node = self.node_mut(hash)?;
        let body = BodyValidationState::ConsensusInvalid {
            evidence,
            rule: rule.clone(),
        };
        let reason = EligibilityReason::ConsensusBodyInvalid { evidence, rule };
        if node.eligibility.direct_reasons.iter().any(|existing| {
            matches!(existing, EligibilityReason::ConsensusBodyInvalid { .. })
                && *existing != reason
        }) || matches!(node.body, BodyValidationState::ConsensusInvalid { .. })
            && node.body != body
        {
            return Err(GraphError::BodyEligibilityMismatch(hash));
        }
        let changed = node.body != body || !node.eligibility.direct_reasons.contains(&reason);
        node.body = body;
        node.eligibility.direct_reasons.insert(reason);
        if changed {
            self.recompute_descendant_eligibility(hash)?;
        }
        Ok(changed)
    }

    pub(crate) fn set_body_state(
        &mut self,
        hash: block::Hash,
        body: BodyValidationState,
    ) -> Result<bool, GraphError> {
        if matches!(body, BodyValidationState::ConsensusInvalid { .. }) {
            return Err(GraphError::BodyEligibilityMismatch(hash));
        }
        let node = self.node_mut(hash)?;
        if node
            .eligibility
            .direct_reasons
            .iter()
            .any(|reason| matches!(reason, EligibilityReason::ConsensusBodyInvalid { .. }))
        {
            return Err(GraphError::BodyEligibilityMismatch(hash));
        }
        let changed = node.body != body;
        node.body = body;
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
        let mut retained = HashSet::new();
        let mut pending = vec![finalized.hash];
        while let Some(hash) = pending.pop() {
            if retained.insert(hash) {
                pending.extend(self.children(hash));
            }
        }
        let mut deleted: Vec<_> = self
            .retained_hashes()
            .filter(|hash| !retained.contains(hash))
            .collect();
        deleted.sort_unstable_by_key(|hash| hash.0);
        for hash in &deleted {
            self.delete_node(*hash)?;
        }
        self.finalized = finalized;
        self.node_mut(finalized.hash)?.eligibility.inherited_from = None;
        self.refresh_eligible_tip(finalized.hash);
        Ok(deleted)
    }

    pub(crate) fn remove_leaf(&mut self, hash: block::Hash) -> Result<(), GraphError> {
        if !self.children(hash).is_empty() {
            return Err(GraphError::NodeHasChildren(hash));
        }
        self.delete_node(hash)
    }

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
        let mut add_children = flatten_edges(&self.add_children);
        let mut remove_children = flatten_edges(&self.remove_children);
        add_children.sort_unstable_by_key(|(parent, child)| (parent.0, child.0));
        remove_children.sort_unstable_by_key(|(parent, child)| (parent.0, child.0));
        let mut add_eligible_tips: Vec<_> = self
            .eligible_tips
            .difference(&self.base.eligible_tips)
            .copied()
            .collect();
        let mut remove_eligible_tips: Vec<_> = self
            .base
            .eligible_tips
            .difference(&self.eligible_tips)
            .copied()
            .collect();
        add_eligible_tips.sort_unstable_by_key(|hash| hash.0);
        remove_eligible_tips.sort_unstable_by_key(|hash| hash.0);
        GraphDelta {
            finalized: (self.finalized != self.base.finalized).then_some(self.finalized),
            put_nodes,
            delete_nodes,
            add_children,
            remove_children,
            add_eligible_tips,
            remove_eligible_tips,
        }
    }

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

    fn recompute_descendant_eligibility(&mut self, root: block::Hash) -> Result<(), GraphError> {
        let mut affected = HashSet::from([root]);
        let mut queue = VecDeque::from(self.children(root));
        while let Some(hash) = queue.pop_front() {
            #[cfg(test)]
            self.eligibility_nodes_visited
                .set(self.eligibility_nodes_visited.get().saturating_add(1));
            let parent_hash = self
                .node(hash)
                .ok_or(GraphError::UnknownNode(hash))?
                .parent_hash;
            let inherited_from = (!self
                .node(parent_hash)
                .ok_or(GraphError::UnknownNode(parent_hash))?
                .is_eligible())
            .then_some(parent_hash);
            if self
                .node(hash)
                .is_some_and(|node| node.eligibility.inherited_from == inherited_from)
            {
                continue;
            }
            self.node_mut(hash)?.eligibility.inherited_from = inherited_from;
            affected.insert(hash);
            queue.extend(self.children(hash));
        }
        let parents: Vec<_> = affected
            .iter()
            .filter_map(|hash| self.node(*hash).map(|node| node.parent_hash))
            .collect();
        affected.extend(parents);
        for hash in affected {
            self.refresh_eligible_tip(hash);
        }
        Ok(())
    }

    fn has_eligible_child(&self, hash: block::Hash) -> bool {
        self.children(hash)
            .into_iter()
            .any(|child| self.node(child).is_some_and(HeaderNode::is_eligible))
    }

    fn refresh_eligible_tip(&mut self, hash: block::Hash) {
        self.eligible_tips.remove(&hash);
        if self
            .node(hash)
            .is_some_and(|node| node.is_eligible() && !self.has_eligible_child(hash))
        {
            self.eligible_tips.insert(hash);
        }
    }

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
        (
            self.base_nodes_cloned.get(),
            self.puts.len().saturating_add(self.deletes.len()),
            self.eligibility_nodes_visited.get(),
        )
    }
}

fn flatten_edges(
    edges: &HashMap<block::Hash, HashSet<block::Hash>>,
) -> Vec<(block::Hash, block::Hash)> {
    edges
        .iter()
        .flat_map(|(parent, children)| children.iter().map(|child| (*parent, *child)))
        .collect()
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
        assert_eq!(delta.add_children, vec![(anchor.hash, child.hash)]);
        assert_eq!(delta.remove_eligible_tips, vec![anchor.hash]);
        assert_eq!(delta.add_eligible_tips, vec![child.hash]);
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
            .add_reason(
                first.hash,
                EligibilityReason::OperatorInvalid { id: operator },
            )
            .expect("operator invalidation propagates");
        overlay
            .remove_operator_invalidation(first.hash, operator)
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
            .add_reason(
                left.hash,
                EligibilityReason::OperatorInvalid {
                    id: OperatorInvalidationId::new([5; 16]),
                },
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
