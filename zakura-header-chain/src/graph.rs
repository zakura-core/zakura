//! Pure in-memory header DAG queries, eligibility propagation, and selection.

use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    sync::Arc,
};

use thiserror::Error;
use zakura_chain::{
    block,
    work::difficulty::{Work, U256},
};

use crate::{
    BodyRuleId, BodyValidationState, ChainScore, EligibilityReason, EligibilityState, EvidenceId,
    Frontier, HeaderNode, HeaderValidationState, OperatorInvalidationId, WorkCoordinate,
    WorkCoordinateError,
};

/// Failure to construct or query a coherent in-memory header DAG.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum GraphError {
    /// The supplied trusted anchor header does not hash to its frontier.
    #[error("trusted anchor header hashes to {actual:?}, expected {expected:?}")]
    AnchorHashMismatch {
        /// Expected configured anchor hash.
        expected: block::Hash,
        /// Locally computed header hash.
        actual: block::Hash,
    },
    /// A durable insertion attempted to reference an unknown parent.
    #[error("header {header:?} has unknown parent {parent:?}")]
    UnknownParent {
        /// Candidate header hash.
        header: block::Hash,
        /// Missing parent hash.
        parent: block::Hash,
    },
    /// The inferred child height crossed the supported range.
    #[error("child of {parent:?} exceeds the supported height range")]
    HeightOverflow {
        /// Parent hash at maximum height.
        parent: block::Hash,
    },
    /// The exact header hash is already retained with different contents.
    #[error("conflicting duplicate header {0:?}")]
    ConflictingDuplicate(block::Hash),
    /// A requested retained node does not exist.
    #[error("unknown retained header {0:?}")]
    UnknownNode(block::Hash),
    /// Retention attempted to remove a node that still has retained children.
    #[error("cannot remove non-leaf header {0:?}")]
    NodeHasChildren(block::Hash),
    /// Body-invalid state and its exact durable eligibility reason disagreed.
    #[error("body-invalid state and eligibility evidence disagree for {0:?}")]
    BodyEligibilityMismatch(block::Hash),
    /// A requested ancestor height is above its descendant.
    #[error("ancestor height {ancestor:?} exceeds descendant height {descendant:?}")]
    InvalidAncestorHeight {
        /// Requested ancestor height.
        ancestor: block::Height,
        /// Descendant height.
        descendant: block::Height,
    },
    /// Exact work accumulation or rebasing failed closed.
    #[error(transparent)]
    Work(#[from] WorkCoordinateError),
}

/// Result of an idempotent DAG insertion.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum InsertResult {
    /// A new node and all reconstructible indexes were inserted.
    Inserted(Frontier),
    /// The exact same node was already retained.
    AlreadyPresent(Frontier),
}

/// Pure hash-keyed in-memory implementation of the header DAG contract.
#[derive(Clone, Debug)]
pub struct MemHeaderStore {
    finalized: Frontier,
    nodes: HashMap<block::Hash, HeaderNode>,
    children: HashMap<block::Hash, HashSet<block::Hash>>,
    heights: HashMap<block::Height, HashSet<block::Hash>>,
}

impl MemHeaderStore {
    /// Construct a store rooted at one trusted, already-validated work origin.
    pub fn new(
        finalized: Frontier,
        header: Arc<block::Header>,
        block_work: Work,
        cumulative_work: U256,
    ) -> Result<Self, GraphError> {
        let actual = header.hash();
        if actual != finalized.hash {
            return Err(GraphError::AnchorHashMismatch {
                expected: finalized.hash,
                actual,
            });
        }
        let anchor = HeaderNode {
            parent_hash: header.previous_block_hash,
            header,
            hash: finalized.hash,
            height: finalized.height,
            block_work,
            work_coordinate: WorkCoordinate::new(finalized.hash, cumulative_work),
            validation: HeaderValidationState::Valid,
            eligibility: EligibilityState::default(),
            body: BodyValidationState::Unknown,
            aux_delivery_ids: Vec::new(),
        };
        let mut nodes = HashMap::new();
        nodes.insert(finalized.hash, anchor);
        let mut heights = HashMap::new();
        heights.insert(finalized.height, HashSet::from([finalized.hash]));
        Ok(Self {
            finalized,
            nodes,
            children: HashMap::new(),
            heights,
        })
    }

    /// Return the immutable finalized root of every eligible path.
    pub const fn finalized(&self) -> Frontier {
        self.finalized
    }

    /// Return the number of retained nodes, including the finalized anchor.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Return true when only no nodes are retained.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Read one retained node by exact consensus hash.
    pub fn node(&self, hash: block::Hash) -> Option<&HeaderNode> {
        self.nodes.get(&hash)
    }

    /// Return every retained hash at a height, ordered by raw internal bytes.
    pub fn hashes_at_height(&self, height: block::Height) -> Vec<block::Hash> {
        let mut hashes: Vec<_> = self
            .heights
            .get(&height)
            .into_iter()
            .flatten()
            .copied()
            .collect();
        hashes.sort_unstable_by_key(|hash| hash.0);
        hashes
    }

    /// Return direct children ordered by raw internal bytes.
    pub fn children(&self, parent: block::Hash) -> Vec<block::Hash> {
        let mut children: Vec<_> = self
            .children
            .get(&parent)
            .into_iter()
            .flatten()
            .copied()
            .collect();
        children.sort_unstable_by_key(|hash| hash.0);
        children
    }

    /// Insert one admitted header after its exact parent is retained.
    pub(crate) fn insert(
        &mut self,
        header: Arc<block::Header>,
        block_work: Work,
        validation: HeaderValidationState,
        direct_reasons: impl IntoIterator<Item = EligibilityReason>,
        body: BodyValidationState,
    ) -> Result<InsertResult, GraphError> {
        let hash = header.hash();
        if let Some(existing) = self.nodes.get(&hash) {
            if existing.header == header {
                return Ok(InsertResult::AlreadyPresent(Frontier::new(
                    existing.height,
                    hash,
                )));
            }
            return Err(GraphError::NodeHasChildren(hash));
        }
        let parent_hash = header.previous_block_hash;
        let parent = self
            .nodes
            .get(&parent_hash)
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
        let direct_reasons: BTreeSet<EligibilityReason> = direct_reasons.into_iter().collect();
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
        let node = HeaderNode {
            header,
            hash,
            parent_hash,
            height,
            block_work,
            work_coordinate: parent.work_coordinate().checked_add(block_work)?,
            validation,
            eligibility: EligibilityState {
                direct_reasons,
                inherited_from,
            },
            body,
            aux_delivery_ids: Vec::new(),
        };
        self.nodes.insert(hash, node);
        self.children.entry(parent_hash).or_default().insert(hash);
        self.heights.entry(height).or_default().insert(hash);
        Ok(InsertResult::Inserted(Frontier::new(height, hash)))
    }

    /// Add one independent direct reason, then recompute the affected subtree cache.
    pub(crate) fn add_reason(
        &mut self,
        hash: block::Hash,
        reason: EligibilityReason,
    ) -> Result<bool, GraphError> {
        if matches!(reason, EligibilityReason::ConsensusBodyInvalid { .. }) {
            return Err(GraphError::BodyEligibilityMismatch(hash));
        }
        let changed = self
            .nodes
            .get_mut(&hash)
            .ok_or(GraphError::UnknownNode(hash))?
            .eligibility
            .direct_reasons
            .insert(reason);
        if changed {
            self.recompute_descendant_eligibility(hash)?;
        }
        Ok(changed)
    }

    /// Remove exactly one operator invalidation, preserving every unrelated reason.
    pub(crate) fn remove_operator_invalidation(
        &mut self,
        hash: block::Hash,
        id: OperatorInvalidationId,
    ) -> Result<bool, GraphError> {
        let reason = EligibilityReason::OperatorInvalid { id };
        let changed = self
            .nodes
            .get_mut(&hash)
            .ok_or(GraphError::UnknownNode(hash))?
            .eligibility
            .direct_reasons
            .remove(&reason);
        if changed {
            self.recompute_descendant_eligibility(hash)?;
        }
        Ok(changed)
    }

    /// Atomically record one commitment-matching deterministic body failure.
    pub(crate) fn set_consensus_body_invalid(
        &mut self,
        hash: block::Hash,
        evidence: EvidenceId,
        rule: BodyRuleId,
    ) -> Result<bool, GraphError> {
        let node = self
            .nodes
            .get_mut(&hash)
            .ok_or(GraphError::UnknownNode(hash))?;
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

    /// Update body availability or verification without changing fork choice eligibility.
    pub(crate) fn set_body_state(
        &mut self,
        hash: block::Hash,
        body: BodyValidationState,
    ) -> Result<bool, GraphError> {
        if matches!(body, BodyValidationState::ConsensusInvalid { .. }) {
            return Err(GraphError::BodyEligibilityMismatch(hash));
        }
        let node = self
            .nodes
            .get_mut(&hash)
            .ok_or(GraphError::UnknownNode(hash))?;
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

    /// Update local-time validation state and recompute descendant eligibility.
    pub(crate) fn set_validation(
        &mut self,
        hash: block::Hash,
        validation: HeaderValidationState,
    ) -> Result<bool, GraphError> {
        let node = self
            .nodes
            .get_mut(&hash)
            .ok_or(GraphError::UnknownNode(hash))?;
        let changed = node.validation != validation;
        node.validation = validation;
        if changed {
            self.recompute_descendant_eligibility(hash)?;
        }
        Ok(changed)
    }

    /// Return the exact ancestor at `height`, if the retained path reaches it.
    pub fn ancestor(
        &self,
        descendant: block::Hash,
        height: block::Height,
    ) -> Result<Option<Frontier>, GraphError> {
        let mut node = self
            .nodes
            .get(&descendant)
            .ok_or(GraphError::UnknownNode(descendant))?;
        if height > node.height {
            return Err(GraphError::InvalidAncestorHeight {
                ancestor: height,
                descendant: node.height,
            });
        }
        while node.height > height {
            let Some(parent) = self.nodes.get(&node.parent_hash) else {
                return Ok(None);
            };
            node = parent;
        }
        Ok(Some(Frontier::new(node.height, node.hash)))
    }

    /// Return all currently maximal eligible nodes in deterministic hash order.
    pub fn eligible_tips(&self) -> Vec<Frontier> {
        let mut tips: Vec<_> =
            self.nodes
                .values()
                .filter(|node| {
                    node.is_eligible()
                        && !self.children(node.hash).into_iter().any(|child| {
                            self.nodes.get(&child).is_some_and(HeaderNode::is_eligible)
                        })
                })
                .map(|node| Frontier::new(node.height, node.hash))
                .collect();
        tips.sort_unstable_by_key(|tip| tip.hash.0);
        tips
    }

    /// Select the deterministic greatest-work eligible tip after the finalized anchor.
    pub fn select_header_best(&self) -> Result<(Frontier, ChainScore), GraphError> {
        let anchor = self
            .nodes
            .get(&self.finalized.hash)
            .ok_or(GraphError::UnknownNode(self.finalized.hash))?;
        self.eligible_tips()
            .into_iter()
            .map(|tip| {
                let node = self
                    .nodes
                    .get(&tip.hash)
                    .expect("eligible tips are derived from retained nodes");
                let score = ChainScore::new(
                    node.work_coordinate()
                        .suffix_after(anchor.work_coordinate())?,
                    tip.hash,
                );
                Ok((tip, score))
            })
            .collect::<Result<Vec<_>, GraphError>>()?
            .into_iter()
            .max_by_key(|(_, score)| *score)
            .ok_or(GraphError::UnknownNode(self.finalized.hash))
    }

    /// Return the selection score of one retained descendant of the finalized anchor.
    pub fn score(&self, hash: block::Hash) -> Result<ChainScore, GraphError> {
        let anchor = self
            .nodes
            .get(&self.finalized.hash)
            .ok_or(GraphError::UnknownNode(self.finalized.hash))?;
        let node = self.nodes.get(&hash).ok_or(GraphError::UnknownNode(hash))?;
        Ok(ChainScore::new(
            node.work_coordinate()
                .suffix_after(anchor.work_coordinate())?,
            hash,
        ))
    }

    pub(crate) fn from_nodes(
        finalized: Frontier,
        nodes: impl IntoIterator<Item = HeaderNode>,
    ) -> Result<Self, GraphError> {
        let mut node_map = HashMap::new();
        let mut children: HashMap<_, HashSet<_>> = HashMap::new();
        let mut heights: HashMap<_, HashSet<_>> = HashMap::new();
        for node in nodes {
            heights.entry(node.height).or_default().insert(node.hash);
            children
                .entry(node.parent_hash)
                .or_default()
                .insert(node.hash);
            node_map.insert(node.hash, node);
        }
        if !node_map.contains_key(&finalized.hash) {
            return Err(GraphError::UnknownNode(finalized.hash));
        }
        children.remove(
            &node_map
                .get(&finalized.hash)
                .expect("the finalized node was checked above")
                .parent_hash,
        );
        Ok(Self {
            finalized,
            nodes: node_map,
            children,
            heights,
        })
    }

    pub(crate) fn nodes(&self) -> impl Iterator<Item = &HeaderNode> {
        self.nodes.values()
    }

    pub(crate) fn node_mut(&mut self, hash: block::Hash) -> Result<&mut HeaderNode, GraphError> {
        self.nodes
            .get_mut(&hash)
            .ok_or(GraphError::UnknownNode(hash))
    }

    pub(crate) fn recompute_all_eligibility(&mut self) -> Result<(), GraphError> {
        let mut frontiers: Vec<_> = self
            .nodes
            .values()
            .map(|node| Frontier::new(node.height, node.hash))
            .collect();
        frontiers.sort_unstable_by_key(|frontier| (frontier.height, frontier.hash.0));
        for frontier in frontiers {
            if frontier == self.finalized {
                self.node_mut(frontier.hash)?.eligibility.inherited_from = None;
                continue;
            }
            let parent_hash = self
                .node(frontier.hash)
                .expect("frontier came from nodes")
                .parent_hash;
            let parent = self.node(parent_hash).ok_or(GraphError::UnknownParent {
                header: frontier.hash,
                parent: parent_hash,
            })?;
            let inherited_from = (!parent.is_eligible()).then_some(parent_hash);
            self.node_mut(frontier.hash)?.eligibility.inherited_from = inherited_from;
        }
        Ok(())
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
        let retained: HashSet<_> = self
            .nodes
            .keys()
            .copied()
            .filter(|hash| {
                self.ancestor(*hash, finalized.height)
                    .ok()
                    .flatten()
                    .is_some_and(|ancestor| ancestor == finalized)
            })
            .collect();
        let mut deleted: Vec<_> = self
            .nodes
            .keys()
            .copied()
            .filter(|hash| !retained.contains(hash))
            .collect();
        deleted.sort_unstable_by_key(|hash| hash.0);
        let nodes: Vec<_> = self
            .nodes
            .values()
            .filter(|node| retained.contains(&node.hash))
            .cloned()
            .collect();
        *self = Self::from_nodes(finalized, nodes)?;
        self.recompute_all_eligibility()?;
        Ok(deleted)
    }

    pub(crate) fn retained_hashes(&self) -> impl Iterator<Item = block::Hash> + '_ {
        self.nodes.keys().copied()
    }

    pub(crate) fn remove_leaf(&mut self, hash: block::Hash) -> Result<(), GraphError> {
        let node = self.nodes.get(&hash).ok_or(GraphError::UnknownNode(hash))?;
        if self
            .children
            .get(&hash)
            .is_some_and(|children| !children.is_empty())
        {
            return Err(GraphError::ConflictingDuplicate(hash));
        }
        let parent_hash = node.parent_hash;
        let height = node.height;
        self.nodes.remove(&hash);
        self.children.remove(&hash);
        if let Some(children) = self.children.get_mut(&parent_hash) {
            children.remove(&hash);
            if children.is_empty() {
                self.children.remove(&parent_hash);
            }
        }
        if let Some(hashes) = self.heights.get_mut(&height) {
            hashes.remove(&hash);
            if hashes.is_empty() {
                self.heights.remove(&height);
            }
        }
        Ok(())
    }

    fn recompute_descendant_eligibility(&mut self, root: block::Hash) -> Result<(), GraphError> {
        let mut queue = VecDeque::from(self.children(root));
        while let Some(hash) = queue.pop_front() {
            let parent_hash = self
                .nodes
                .get(&hash)
                .ok_or(GraphError::UnknownNode(hash))?
                .parent_hash;
            let parent = self
                .nodes
                .get(&parent_hash)
                .ok_or(GraphError::UnknownNode(parent_hash))?;
            let inherited_from = (!parent.is_eligible()).then_some(parent_hash);
            self.nodes
                .get_mut(&hash)
                .expect("the queued child was read from the retained node map")
                .eligibility
                .inherited_from = inherited_from;
            queue.extend(self.children(hash));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeSet;
    use zakura_chain::block::genesis::regtest_genesis_block;

    fn anchor_store() -> MemHeaderStore {
        let block = regtest_genesis_block();
        let hash = block.hash();
        let work = block
            .header
            .difficulty_threshold
            .to_work()
            .expect("the regtest genesis target has valid work");
        MemHeaderStore::new(
            Frontier::new(block::Height(0), hash),
            block.header.clone(),
            work,
            work.as_u256(),
        )
        .expect("the trusted fixture header matches its hash")
    }

    fn child(parent: block::Hash, seed: u8) -> Arc<block::Header> {
        let mut header = *regtest_genesis_block().header;
        header.previous_block_hash = parent;
        header.nonce = [seed; 32].into();
        Arc::new(header)
    }

    fn insert_child(store: &mut MemHeaderStore, parent: block::Hash, seed: u8) -> Frontier {
        let header = child(parent, seed);
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
            .expect("the fixture parent is retained")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        }
    }

    #[derive(Clone)]
    struct ReferenceNode {
        hash: block::Hash,
        parent: Option<block::Hash>,
        cumulative_work: U256,
        validation: HeaderValidationState,
        direct_reasons: BTreeSet<EligibilityReason>,
    }

    struct ReferenceDag {
        anchor: block::Hash,
        anchor_work: U256,
        nodes: HashMap<block::Hash, ReferenceNode>,
        insertion_order: Vec<block::Hash>,
    }

    impl ReferenceDag {
        fn new(anchor: &HeaderNode) -> Self {
            let node = ReferenceNode {
                hash: anchor.hash,
                parent: None,
                cumulative_work: U256::zero(),
                validation: HeaderValidationState::Valid,
                direct_reasons: BTreeSet::new(),
            };
            Self {
                anchor: anchor.hash,
                anchor_work: U256::zero(),
                nodes: HashMap::from([(anchor.hash, node)]),
                insertion_order: vec![anchor.hash],
            }
        }

        fn insert(&mut self, hash: block::Hash, parent: block::Hash, work: Work) {
            let cumulative_work = self.nodes[&parent]
                .cumulative_work
                .checked_add(work.as_u256())
                .expect("generated reference work does not overflow");
            self.nodes.insert(
                hash,
                ReferenceNode {
                    hash,
                    parent: Some(parent),
                    cumulative_work,
                    validation: HeaderValidationState::Valid,
                    direct_reasons: BTreeSet::new(),
                },
            );
            self.insertion_order.push(hash);
        }

        fn is_eligible(&self, mut hash: block::Hash) -> bool {
            loop {
                let node = &self.nodes[&hash];
                if node.validation != HeaderValidationState::Valid
                    || !node.direct_reasons.is_empty()
                {
                    return false;
                }
                let Some(parent) = node.parent else {
                    return hash == self.anchor;
                };
                hash = parent;
            }
        }

        fn selected(&self) -> block::Hash {
            self.nodes
                .values()
                .filter(|node| self.is_eligible(node.hash))
                .max_by(|left, right| {
                    let left_work = left
                        .cumulative_work
                        .checked_sub(self.anchor_work)
                        .expect("reference descendants have anchor work");
                    let right_work = right
                        .cumulative_work
                        .checked_sub(self.anchor_work)
                        .expect("reference descendants have anchor work");
                    left_work
                        .cmp(&right_work)
                        .then_with(|| left.hash.0.cmp(&right.hash.0))
                })
                .expect("the reference anchor is always eligible")
                .hash
        }
    }

    fn operation_header(parent: block::Hash, operation: usize) -> Arc<block::Header> {
        let mut header = *regtest_genesis_block().header;
        header.previous_block_hash = parent;
        let operation = u64::try_from(operation).expect("test operation index fits in u64");
        let mut nonce = [0; 32];
        nonce[..8].copy_from_slice(&operation.to_le_bytes());
        header.nonce = nonce.into();
        Arc::new(header)
    }

    #[test]
    fn fork_indexes_selection_and_inherited_reason_sets_are_exact() {
        let mut store = anchor_store();
        let anchor = store.finalized();
        let left = insert_child(&mut store, anchor.hash, 1);
        let right = insert_child(&mut store, anchor.hash, 2);
        assert_eq!(store.hashes_at_height(block::Height(1)).len(), 2);

        let left_tip = insert_child(&mut store, left.hash, 3);
        assert_eq!(
            store.select_header_best().expect("graph is coherent").0,
            left_tip
        );

        let first = EligibilityReason::OperatorInvalid {
            id: crate::OperatorInvalidationId::new([1; 16]),
        };
        let second = EligibilityReason::OperatorInvalid {
            id: crate::OperatorInvalidationId::new([2; 16]),
        };
        store
            .add_reason(left.hash, first)
            .expect("left is retained");
        store
            .add_reason(left.hash, second)
            .expect("left is retained");
        assert_eq!(
            store
                .node(left.hash)
                .expect("retained")
                .eligibility
                .direct_reasons
                .len(),
            2
        );
        assert_eq!(
            store
                .node(left_tip.hash)
                .expect("retained")
                .eligibility
                .inherited_from,
            Some(left.hash)
        );
        assert_eq!(
            store.select_header_best().expect("graph is coherent").0,
            right
        );

        store
            .remove_operator_invalidation(left.hash, crate::OperatorInvalidationId::new([1; 16]))
            .expect("left is retained");
        assert!(!store.node(left.hash).expect("retained").is_eligible());
        store
            .remove_operator_invalidation(left.hash, crate::OperatorInvalidationId::new([2; 16]))
            .expect("left is retained");
        assert!(store.node(left_tip.hash).expect("retained").is_eligible());
        assert_eq!(
            store.select_header_best().expect("graph is coherent").0,
            left_tip
        );
    }

    #[test]
    fn fixed_anchor_selection_has_no_incumbent_relative_rollback_horizon() {
        let mut store = anchor_store();
        let anchor = store.finalized();
        let mut incumbent = anchor;
        for offset in 0..1_000 {
            let seed = u8::try_from(offset % 251).expect("reduced nonce fits in u8");
            incumbent = insert_child(&mut store, incumbent.hash, seed);
        }
        assert_eq!(
            store.select_header_best().expect("graph is coherent").0,
            incumbent
        );

        let mut competitor = anchor;
        for offset in 0..1_001 {
            let seed = u8::try_from((offset + 127) % 251).expect("reduced nonce fits in u8");
            competitor = insert_child(&mut store, competitor.hash, seed);
        }
        assert_eq!(
            store.select_header_best().expect("graph is coherent").0,
            competitor,
            "selection is anchored at finalized and has no incumbent-relative depth rule"
        );
    }

    #[test]
    fn body_availability_does_not_override_header_work_or_mark_other_bodies() {
        let mut store = anchor_store();
        let anchor = store.finalized();
        let verified = child(anchor.hash, 41);
        let work = verified.difficulty_threshold.to_work().expect("valid work");
        let verified_hash = verified.hash();
        store
            .insert(
                verified,
                work,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Verified {
                    evidence: crate::EvidenceId::from_digest([4; 32]),
                },
            )
            .expect("verified fixture is inserted");
        let unknown_parent = insert_child(&mut store, anchor.hash, 51);
        let unknown_tip = insert_child(&mut store, unknown_parent.hash, 52);
        store
            .set_body_state(
                unknown_tip.hash,
                BodyValidationState::Unavailable(crate::BodyUnavailableSummary {
                    attempts: 10,
                    suppliers: 0,
                    alarmed: true,
                }),
            )
            .expect("the unavailable tip is retained");

        assert_eq!(
            store.select_header_best().expect("graph is coherent").0,
            unknown_tip
        );
        assert_eq!(
            store.node(verified_hash).expect("retained").body,
            BodyValidationState::Verified {
                evidence: crate::EvidenceId::from_digest([4; 32])
            }
        );
        assert_eq!(
            store.node(unknown_tip.hash).expect("retained").body,
            BodyValidationState::Unavailable(crate::BodyUnavailableSummary {
                attempts: 10,
                suppliers: 0,
                alarmed: true,
            })
        );
    }

    proptest! {
        #[test]
        fn insertion_permutations_match_an_independent_greatest_work_model(
            branch_lengths in prop::collection::vec(1_u8..8, 1..8),
            reverse in any::<bool>(),
        ) {
            let mut store = anchor_store();
            let anchor = store.finalized();
            let mut branches = Vec::new();
            for (branch, length) in branch_lengths.iter().copied().enumerate() {
                let mut parent = anchor.hash;
                let mut headers = Vec::new();
                for offset in 0..length {
                    let branch = u8::try_from(branch).expect("generated branch count fits in u8");
                    let seed = branch.wrapping_mul(17).wrapping_add(offset).wrapping_add(1);
                    let header = child(parent, seed);
                    parent = header.hash();
                    headers.push(header);
                }
                branches.push(headers);
            }
            if reverse {
                branches.reverse();
            }
            for headers in &branches {
                for header in headers {
                    let work = header.difficulty_threshold.to_work().expect("fixture target is valid");
                    store.insert(
                        header.clone(),
                        work,
                        HeaderValidationState::Valid,
                        [],
                        BodyValidationState::Unknown,
                    ).expect("each branch is inserted parent first");
                }
            }

            let expected = branches
                .iter()
                .map(|branch| {
                    let tip = branch.last().expect("generated branches are nonempty");
                    (branch.len(), tip.hash().0, tip.hash())
                })
                .max_by_key(|(length, hash_bytes, _)| (*length, *hash_bytes))
                .expect("at least one branch was generated")
                .2;
            prop_assert_eq!(store.select_header_best().expect("graph is coherent").0.hash, expected);
        }


        #[test]
        fn arbitrary_graph_operations_match_an_independent_uncached_model(
            operations in prop::collection::vec((0_u8..5, any::<usize>()), 1..100),
        ) {
            let mut store = anchor_store();
            let anchor = store.node(store.finalized().hash).expect("anchor is retained").clone();
            let mut model = ReferenceDag::new(&anchor);

            for (operation_index, (kind, target)) in operations.into_iter().enumerate() {
                let target_index = target % model.insertion_order.len();
                let target_hash = model.insertion_order[target_index];
                let mut id_bytes = [0; 16];
                let target_id = u64::try_from(target_index).expect("test node index fits in u64");
                id_bytes[..8].copy_from_slice(&target_id.to_le_bytes());
                let reason = EligibilityReason::OperatorInvalid {
                    id: crate::OperatorInvalidationId::new(id_bytes),
                };

                match kind {
                    0 => {
                        let header = operation_header(target_hash, operation_index + 1);
                        let hash = header.hash();
                        let work = header.difficulty_threshold.to_work().expect("fixture target is valid");
                        store.insert(
                            header,
                            work,
                            HeaderValidationState::Valid,
                            [],
                            BodyValidationState::Unknown,
                        ).expect("generated parent is retained");
                        model.insert(hash, target_hash, work);
                    }
                    1 => {
                        if target_hash != model.anchor {
                            store
                                .add_reason(target_hash, reason.clone())
                                .expect("target is retained");
                            model.nodes.get_mut(&target_hash).expect("target exists").direct_reasons.insert(reason);
                        }
                    }
                    2 => {
                        if target_hash != model.anchor {
                            let EligibilityReason::OperatorInvalid { id } = reason else {
                                unreachable!("the generated reason is operator-scoped")
                            };
                            store.remove_operator_invalidation(target_hash, id).expect("target is retained");
                            model.nodes.get_mut(&target_hash).expect("target exists").direct_reasons.remove(&reason);
                        }
                    }
                    3 => {
                        if target_hash != model.anchor {
                            let until = regtest_genesis_block().header.time + chrono::Duration::days(1);
                            store.set_validation(target_hash, HeaderValidationState::DeferredUntil(until)).expect("target is retained");
                            model.nodes.get_mut(&target_hash).expect("target exists").validation = HeaderValidationState::DeferredUntil(until);
                        }
                    }
                    4 => {
                        if target_hash != model.anchor {
                            store.set_validation(target_hash, HeaderValidationState::Valid).expect("target is retained");
                            model.nodes.get_mut(&target_hash).expect("target exists").validation = HeaderValidationState::Valid;
                        }
                    }
                    _ => unreachable!("the generated operation kind is bounded"),
                }

                prop_assert_eq!(
                    store.select_header_best().expect("graph is coherent").0.hash,
                    model.selected(),
                );
            }
        }
    }
}
