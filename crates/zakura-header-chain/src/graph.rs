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
    BodyValidationState, ChainScore, EligibilityReason, EligibilityState, EvidenceId, Frontier,
    HeaderNode, HeaderValidationState, OperatorInvalidationId, WorkCoordinate, WorkCoordinateError,
};

mod overlay;
pub(crate) use overlay::{GraphDelta, GraphOverlay};

/// Read-only access to one coherent retained header graph.
///
/// This abstraction allows fork-choice, planning, and invariant code to query
/// either the committed in-memory graph or a graph overlay containing proposed
/// changes. It exposes retained nodes, ancestry, eligibility, finality, and
/// derived fork-choice views without granting mutation or persistence access.
pub(crate) trait HeaderGraphView {
    fn view_finalized(&self) -> Frontier;
    fn view_node_count(&self) -> usize;
    fn view_node(&self, hash: block::Hash) -> Option<&HeaderNode>;
    fn view_nodes(&self) -> Vec<&HeaderNode>;
    fn view_retained_hashes(&self) -> Vec<block::Hash>;
    fn view_hashes_at_height(&self, height: block::Height) -> Vec<block::Hash>;
    fn view_children(&self, parent: block::Hash) -> Vec<block::Hash>;
    fn view_eligible_tips(&self) -> Vec<Frontier>;
    fn view_select_header_best(&self) -> Result<(Frontier, ChainScore), GraphError>;
    fn view_score(&self, hash: block::Hash) -> Result<ChainScore, GraphError>;
    fn view_ancestor(
        &self,
        descendant: block::Hash,
        height: block::Height,
    ) -> Result<Option<Frontier>, GraphError>;
}

/// In-memory mutation operations for a retained header-graph view.
///
/// Implementations apply node and eligibility changes while maintaining the
/// graph’s child, height, and eligible-tip indexes. Mutations may be provisional,
/// as with `GraphOverlay`; this trait does not durably commit or publish them.
///
/// Callers remain responsible for event authority and transition-level
/// invariant validation.
pub(crate) trait HeaderGraphEdit: HeaderGraphView {
    fn edit_node_mut(&mut self, hash: block::Hash) -> Result<&mut HeaderNode, GraphError>;
    fn edit_insert(
        &mut self,
        header: Arc<block::Header>,
        block_work: Work,
        validation: HeaderValidationState,
        direct_reasons: Vec<EligibilityReason>,
        body: BodyValidationState,
    ) -> Result<InsertResult, GraphError>;
    fn edit_add_eligibility_reason(
        &mut self,
        hash: block::Hash,
        reason: EligibilityReason,
    ) -> Result<bool, GraphError>;
    fn edit_remove_operator_invalidation(
        &mut self,
        hash: block::Hash,
        id: OperatorInvalidationId,
        evidence: Option<EvidenceId>,
    ) -> Result<bool, GraphError>;
    fn edit_set_body_state(
        &mut self,
        hash: block::Hash,
        body: BodyValidationState,
    ) -> Result<bool, GraphError>;
    fn edit_set_validation(
        &mut self,
        hash: block::Hash,
        validation: HeaderValidationState,
    ) -> Result<bool, GraphError>;
    fn edit_advance_finalized(
        &mut self,
        finalized: Frontier,
    ) -> Result<Vec<block::Hash>, GraphError>;
    fn edit_remove_leaf(&mut self, hash: block::Hash) -> Result<(), GraphError>;
}

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
    /// Finality attempted to root the graph at an ineligible header.
    #[error("cannot finalize ineligible retained header {0:?}")]
    IneligibleFinalized(block::Hash),
    /// Finality selected a retained header outside the current finalized subtree.
    #[error(
        "new finalized header {candidate:?} is not a descendant of current finalized header {current:?}"
    )]
    FinalizedNotDescendant {
        /// Current finalized root.
        current: block::Hash,
        /// Proposed finalized root.
        candidate: block::Hash,
    },
    /// Retention attempted to remove a node that still has retained children.
    #[error("cannot remove non-leaf header {0:?}")]
    NodeHasChildren(block::Hash),
    /// A caller tried to replace a permanent consensus-invalid body-validation state.
    #[error("consensus-invalid body state is permanent for {0:?}")]
    PermanentBodyInvalidity(block::Hash),
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
    /// The graph inserted a new node and all reconstructible indexes.
    Inserted(Frontier),
    /// The graph already retained the exact same node.
    AlreadyPresent(Frontier),
}

/// `MemHeaderStore` stores admitted headers in an in-memory domain graph.
///
/// `MemHeaderStore` owns canonical node state and reconstructible graph indexes.
/// It supports header selection, ancestry, finality, and retention policy.
/// It is not a durable store or a generic CRUD interface.
#[derive(Clone, Debug)]
pub struct MemHeaderStore {
    finalized: Frontier,
    // Nodes by hash.
    nodes: HashMap<block::Hash, HeaderNode>,
    // Children by parent hash.
    children: HashMap<block::Hash, HashSet<block::Hash>>,
    heights: HashMap<block::Height, HashSet<block::Hash>>,

    // A hash belongs to this when: a header is eligible (validated, no exclusion reasons, no ineligible ancestors)
    // and has no eligible children.
    eligible_tips: HashSet<block::Hash>,
}

struct DerivedIndexChanges {
    add_children: Vec<(block::Hash, block::Hash)>,
    remove_children: Vec<(block::Hash, block::Hash)>,
    add_eligible_tips: Vec<block::Hash>,
    remove_eligible_tips: Vec<block::Hash>,
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
            body_validation_state: BodyValidationState::Unknown,
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
            eligible_tips: HashSet::from([finalized.hash]),
        })
    }

    /// Return the immutable finalized root of every eligible path.
    pub const fn finalized(&self) -> Frontier {
        self.finalized
    }

    /// Return the number of retained nodes, including the finalized anchor.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
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

    /// Insert one admitted header after the graph retains its exact parent.
    /// Removes the parent from eligible tips as a side effect.
    pub(crate) fn insert(
        &mut self,
        header: Arc<block::Header>,
        block_work: Work,
        validation: HeaderValidationState,
        direct_reasons: impl IntoIterator<Item = EligibilityReason>,
        body: BodyValidationState,
    ) -> Result<InsertResult, GraphError> {
        // Check if the header already exists in the graph. If so, return the existing frontier.
        let hash = header.hash();
        if let Some(existing) = self.nodes.get(&hash) {
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
            body_validation_state: body,
            aux_delivery_ids: Vec::new(),
        };
        self.nodes.insert(hash, node);
        self.children.entry(parent_hash).or_default().insert(hash);
        self.heights.entry(height).or_default().insert(hash);
        if self
            .nodes
            .get(&hash)
            .expect("the inserted node is present")
            .is_eligible()
        {
            self.eligible_tips.remove(&parent_hash);
            self.eligible_tips.insert(hash);
        }
        Ok(InsertResult::Inserted(Frontier::new(height, hash)))
    }

    /// Marks an already-retained header as newly ineligible without deleting it.
    /// Its descendants also become ineligible, so the method recomputes ancestry
    /// eligibility and eligible-tip indexes.
    pub(crate) fn add_eligibility_reason(
        &mut self,
        hash: block::Hash,
        reason: EligibilityReason,
    ) -> Result<bool, GraphError> {
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

    /// Removes the operator eligibility reason matching `id` and
    /// `invalidation_evidence` from `hash`.
    ///
    /// Returns `true` if a reason was removed. On removal, recomputes inherited
    /// eligibility for the affected subtree. Other eligibility reasons are preserved.
    pub(crate) fn remove_operator_invalidation(
        &mut self,
        hash: block::Hash,
        id: OperatorInvalidationId,
        evidence: Option<EvidenceId>,
    ) -> Result<bool, GraphError> {
        let reasons = &mut self
            .nodes
            .get_mut(&hash)
            .ok_or(GraphError::UnknownNode(hash))?
            .eligibility
            .direct_reasons;
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

    /// This method replaces the body-validation state for retained header `hash`.
    ///
    /// Returns `true` when the state changes and `false` when it is unchanged.
    /// A consensus-invalid body-validation state makes the node and its
    /// descendants ineligible. Replaying the same consensus-invalid
    /// body-validation state leaves the node unchanged. This method rejects a
    /// different consensus-invalid body-validation state. It also rejects any
    /// later non-invalid body-validation state.
    /// Consensus invalidity is permanent.
    ///
    /// This method does not enforce transitions between non-invalid
    /// body-validation states. The caller must supply an authoritative
    /// body-validation state.
    pub(crate) fn set_body_state(
        &mut self,
        hash: block::Hash,
        body_validation_state: BodyValidationState,
    ) -> Result<bool, GraphError> {
        let (changed, eligibility_changed) = {
            let node = self
                .nodes
                .get_mut(&hash)
                .ok_or(GraphError::UnknownNode(hash))?;
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

    /// Replaces the local-time validation state of retained header `hash`.
    ///
    /// A deferred header (the one passing all deterministic checks but being far back in the future)
    /// is ineligible until changed to `Valid`. When the state
    /// changes, propagates the resulting eligibility change through its descendants
    /// and refreshes eligible fork tips.
    ///
    /// Returns `true` when the state changes and `false` when unchanged.
    /// Returns an error if `hash` is unknown or the retained graph is inconsistent.
    ///
    /// The caller must ensure the new state is justified by the authoritative clock.
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
        let mut tips: Vec<_> = self
            .eligible_tips
            .iter()
            .filter_map(|hash| self.nodes.get(hash))
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
        let mut best = None;
        for hash in &self.eligible_tips {
            let node = self
                .nodes
                .get(hash)
                .expect("eligible tips are derived from retained nodes");
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
        let mut store = Self {
            finalized,
            nodes: node_map,
            children,
            heights,
            eligible_tips: HashSet::new(),
        };
        store.rebuild_eligible_tips();
        Ok(store)
    }

    pub(crate) fn nodes(&self) -> impl Iterator<Item = &HeaderNode> {
        self.nodes.values()
    }

    pub(crate) fn node_mut(&mut self, hash: block::Hash) -> Result<&mut HeaderNode, GraphError> {
        self.nodes
            .get_mut(&hash)
            .ok_or(GraphError::UnknownNode(hash))
    }

    /// Propagates an eligibility change at `root` through its retained subtree.
    ///
    /// Assumes `root` already contains its updated direct eligibility state.
    /// For every descendant, recomputes `inherited_from` from its parent’s current
    /// eligibility. Then refreshes eligible-tip membership for the root, all
    /// descendants, and their parents.
    ///
    /// Returns `GraphError::UnknownNode` if a child edge references a missing node
    /// or a retained child references a missing parent.
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
        self.rebuild_eligible_tips();
        Ok(())
    }

    /// Moves the graph’s finality anchor to an eligible retained frontier.
    ///
    /// Retains the new anchor and all of its descendants, and removes every other
    /// node, including its ancestors and competing branches. Rebuilds affected
    /// height, child, and eligible-tip indexes, and clears inherited ineligibility
    /// from the new root.
    ///
    /// Returns the hashes of removed nodes in deterministic raw-byte order.
    ///
    /// Returns an error without modifying the graph if the frontier’s hash is
    /// unknown, its recorded height differs, or the node is ineligible.
    ///
    /// The caller must establish authority to advance finality before invoking this
    /// method. Durable commit and publication are handled by the transition layer.
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
        // Visited hashes
        let mut retained = HashSet::new();
        // Nodes to visit
        let mut pending = vec![finalized.hash];

        // Traverse the graph in depth-first order, starting from the new finalized frontier.
        while let Some(hash) = pending.pop() {
            if retained.insert(hash) {
                pending.extend(self.children(hash));
            }
        }

        // Remove all nodes that were left behind the frontier after it moved.
        let mut deleted: Vec<_> = self
            .nodes
            .keys()
            .copied()
            .filter(|hash| !retained.contains(hash))
            .collect();
        deleted.sort_unstable_by_key(|hash| hash.0);
        for hash in &deleted {
            let node = self
                .nodes
                .remove(hash)
                .expect("the deletion set came from retained graph nodes");
            self.children.remove(hash);
            self.eligible_tips.remove(hash);
            if let Some(hashes) = self.heights.get_mut(&node.height) {
                hashes.remove(hash);
                if hashes.is_empty() {
                    self.heights.remove(&node.height);
                }
            }
        }
        self.finalized = finalized;
        self.nodes
            .get_mut(&finalized.hash)
            .expect("the new finalized root is retained")
            .eligibility
            .inherited_from = None;
        self.refresh_eligible_tip(finalized.hash);
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
            return Err(GraphError::NodeHasChildren(hash));
        }
        let parent_hash = node.parent_hash;
        let height = node.height;
        self.eligible_tips.remove(&hash);
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
        self.refresh_eligible_tip(parent_hash);
        Ok(())
    }

    fn recompute_descendant_eligibility(&mut self, root: block::Hash) -> Result<(), GraphError> {
        let mut affected = HashSet::from([root]);
        affected.insert(
            self.nodes
                .get(&root)
                .ok_or(GraphError::UnknownNode(root))?
                .parent_hash,
        );
        let mut queue = VecDeque::from(self.children(root));
        while let Some(hash) = queue.pop_front() {
            affected.insert(hash);
            let parent_hash = self
                .nodes
                .get(&hash)
                .ok_or(GraphError::UnknownNode(hash))?
                .parent_hash;
            affected.insert(parent_hash);
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
        for hash in affected {
            self.refresh_eligible_tip(hash);
        }
        Ok(())
    }

    fn has_eligible_child(&self, hash: block::Hash) -> bool {
        self.children.get(&hash).is_some_and(|children| {
            children
                .iter()
                .any(|child| self.nodes.get(child).is_some_and(HeaderNode::is_eligible))
        })
    }

    fn refresh_eligible_tip(&mut self, hash: block::Hash) {
        self.eligible_tips.remove(&hash);
        if self
            .nodes
            .get(&hash)
            .is_some_and(|node| node.is_eligible() && !self.has_eligible_child(hash))
        {
            self.eligible_tips.insert(hash);
        }
    }

    fn rebuild_eligible_tips(&mut self) {
        self.eligible_tips = self
            .nodes
            .values()
            .filter(|node| node.is_eligible() && !self.has_eligible_child(node.hash))
            .map(|node| node.hash)
            .collect();
    }

    /// This method validates and installs a complete overlay-produced graph delta.
    ///
    /// It derives and applies height, child, and eligible-tip indexes from
    /// canonical node deletions and replacements. The caller must preserve
    /// immutable identity fields such as height when it replaces an existing
    /// node.
    ///
    /// A deletion that references an unknown node produces an error without
    /// modifying the store. A delta that deletes and replaces the same hash also
    /// produces an error without modifying the store.
    pub(crate) fn apply_delta(&mut self, delta: &GraphDelta) -> Result<(), GraphError> {
        let indexes = self.derive_index_changes(delta)?;

        // Apply deletions
        for hash in &delta.delete_nodes {
            let node = self
                .nodes
                .remove(hash)
                .expect("delta deletions were validated before mutation");
            self.children.remove(hash);
            if let Some(hashes) = self.heights.get_mut(&node.height) {
                hashes.remove(hash);
                if hashes.is_empty() {
                    self.heights.remove(&node.height);
                }
            }
        }

        // Apply replacements
        for node in &delta.put_nodes {
            let old = self.nodes.insert(node.hash, node.clone());
            if old.is_none() {
                self.heights
                    .entry(node.height)
                    .or_default()
                    .insert(node.hash);
            }
        }

        // Apply child-edge removals and additions
        for (parent, child) in &indexes.remove_children {
            if let Some(children) = self.children.get_mut(parent) {
                children.remove(child);
                if children.is_empty() {
                    self.children.remove(parent);
                }
            }
        }

        // Apply child-edge additions
        for (parent, child) in &indexes.add_children {
            self.children.entry(*parent).or_default().insert(*child);
        }

        // Apply eligible-tip removals and additions
        for hash in &indexes.remove_eligible_tips {
            self.eligible_tips.remove(hash);
        }
        self.eligible_tips
            .extend(indexes.add_eligible_tips.iter().copied());
        if let Some(finalized) = delta.finalized {
            self.finalized = finalized;
        }
        Ok(())
    }

    /// This method validates a complete overlay-produced graph delta.
    pub(crate) fn validate_delta(&self, delta: &GraphDelta) -> Result<(), GraphError> {
        self.derive_index_changes(delta).map(|_| ())
    }

    fn derive_index_changes(&self, delta: &GraphDelta) -> Result<DerivedIndexChanges, GraphError> {
        let mut puts = HashMap::new();
        for node in &delta.put_nodes {
            if puts.insert(node.hash, node).is_some() {
                return Err(GraphError::ConflictingDuplicate(node.hash));
            }
        }
        let mut deletes = HashSet::new();
        for hash in &delta.delete_nodes {
            if !deletes.insert(*hash) {
                return Err(GraphError::ConflictingDuplicate(*hash));
            }
            if !self.nodes.contains_key(hash) {
                return Err(GraphError::UnknownNode(*hash));
            }
            if puts.contains_key(hash) {
                return Err(GraphError::ConflictingDuplicate(*hash));
            }
        }

        let projected_node = |hash: block::Hash| {
            (!deletes.contains(&hash))
                .then(|| puts.get(&hash).copied().or_else(|| self.nodes.get(&hash)))
                .flatten()
        };
        let finalized = delta.finalized.unwrap_or(self.finalized);
        let finalized_node =
            projected_node(finalized.hash).ok_or(GraphError::UnknownNode(finalized.hash))?;
        if finalized_node.height != finalized.height {
            return Err(GraphError::UnknownNode(finalized.hash));
        }
        if !finalized_node.is_eligible() {
            return Err(GraphError::IneligibleFinalized(finalized.hash));
        }

        for node in &delta.put_nodes {
            if node.header.hash() != node.hash
                || node.header.previous_block_hash != node.parent_hash
                || node.header.difficulty_threshold.to_work() != Some(node.block_work)
            {
                return Err(GraphError::ConflictingDuplicate(node.hash));
            }
            if let Some(old) = self.nodes.get(&node.hash) {
                if old.header != node.header
                    || old.parent_hash != node.parent_hash
                    || old.height != node.height
                    || old.block_work != node.block_work
                {
                    return Err(GraphError::ConflictingDuplicate(node.hash));
                }
            } else if node.hash != finalized.hash {
                let parent = projected_node(node.parent_hash).ok_or(GraphError::UnknownParent {
                    header: node.hash,
                    parent: node.parent_hash,
                })?;
                if parent.height.next().ok() != Some(node.height) {
                    return Err(GraphError::UnknownParent {
                        header: node.hash,
                        parent: node.parent_hash,
                    });
                }
            }
        }

        for parent in &deletes {
            for child in self.children.get(parent).into_iter().flatten() {
                if !deletes.contains(child) && *child != finalized.hash {
                    return Err(GraphError::UnknownParent {
                        header: *child,
                        parent: *parent,
                    });
                }
            }
        }

        let mut add_children = HashSet::new();
        let mut remove_children = HashSet::new();
        for hash in &deletes {
            let node = self
                .nodes
                .get(hash)
                .expect("delta deletions were validated above");
            if self
                .children
                .get(&node.parent_hash)
                .is_some_and(|children| children.contains(hash))
            {
                remove_children.insert((node.parent_hash, *hash));
            }
            for child in self.children.get(hash).into_iter().flatten() {
                remove_children.insert((*hash, *child));
            }
        }
        if finalized != self.finalized {
            if let Some(node) = self.nodes.get(&finalized.hash) {
                if self
                    .children
                    .get(&node.parent_hash)
                    .is_some_and(|children| children.contains(&finalized.hash))
                {
                    remove_children.insert((node.parent_hash, finalized.hash));
                }
            }
        }
        for node in &delta.put_nodes {
            if !self.nodes.contains_key(&node.hash) && node.hash != finalized.hash {
                add_children.insert((node.parent_hash, node.hash));
            }
        }

        let mut affected = HashSet::from([self.finalized.hash, finalized.hash]);
        for node in &delta.put_nodes {
            affected.insert(node.hash);
            affected.insert(node.parent_hash);
        }
        for hash in &delta.delete_nodes {
            affected.insert(*hash);
            if let Some(node) = self.nodes.get(hash) {
                affected.insert(node.parent_hash);
            }
        }
        for (parent, child) in add_children.iter().chain(&remove_children) {
            affected.insert(*parent);
            affected.insert(*child);
        }

        let is_projected_tip = |hash: block::Hash| {
            let Some(node) = projected_node(hash) else {
                return false;
            };
            if !node.is_eligible() {
                return false;
            }
            let has_eligible_base_child = self
                .children
                .get(&hash)
                .into_iter()
                .flatten()
                .filter(|child| !remove_children.contains(&(hash, **child)))
                .any(|child| projected_node(*child).is_some_and(HeaderNode::is_eligible));
            let has_eligible_added_child = add_children
                .iter()
                .filter(|(parent, _)| *parent == hash)
                .any(|(_, child)| projected_node(*child).is_some_and(HeaderNode::is_eligible));
            !has_eligible_base_child && !has_eligible_added_child
        };

        let mut add_eligible_tips = Vec::new();
        let mut remove_eligible_tips = Vec::new();
        for hash in affected {
            let was_tip = self.eligible_tips.contains(&hash);
            let is_tip = is_projected_tip(hash);
            if is_tip && !was_tip {
                add_eligible_tips.push(hash);
            } else if was_tip && !is_tip {
                remove_eligible_tips.push(hash);
            }
        }

        let mut add_children: Vec<_> = add_children.into_iter().collect();
        let mut remove_children: Vec<_> = remove_children.into_iter().collect();
        add_children.sort_unstable_by_key(|(parent, child)| (parent.0, child.0));
        remove_children.sort_unstable_by_key(|(parent, child)| (parent.0, child.0));
        add_eligible_tips.sort_unstable_by_key(|hash| hash.0);
        remove_eligible_tips.sort_unstable_by_key(|hash| hash.0);
        Ok(DerivedIndexChanges {
            add_children,
            remove_children,
            add_eligible_tips,
            remove_eligible_tips,
        })
    }
}

impl HeaderGraphView for MemHeaderStore {
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

impl HeaderGraphEdit for MemHeaderStore {
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
    use crate::BodyRuleId;
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

    fn rebuild_finalized_reference(
        store: &mut MemHeaderStore,
        finalized: Frontier,
    ) -> Result<Vec<block::Hash>, GraphError> {
        let node = store
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
                pending.extend(store.children(hash));
            }
        }
        let mut deleted: Vec<_> = store
            .nodes
            .keys()
            .copied()
            .filter(|hash| !retained.contains(hash))
            .collect();
        deleted.sort_unstable_by_key(|hash| hash.0);
        let nodes: Vec<_> = store
            .nodes
            .values()
            .filter(|node| retained.contains(&node.hash))
            .cloned()
            .collect();
        *store = MemHeaderStore::from_nodes(finalized, nodes)?;
        store.recompute_all_eligibility()?;
        Ok(deleted)
    }

    #[test]
    fn conflicting_duplicate_reports_the_duplicate_hash() {
        let mut store = anchor_store();
        let anchor = store.finalized();
        let original = child(anchor.hash, 1);
        let original_hash = original.hash();
        let frontier = insert_child(&mut store, anchor.hash, 1);
        assert_eq!(frontier.hash, original_hash);

        store
            .nodes
            .get_mut(&original_hash)
            .expect("the inserted fixture node is retained")
            .header = child(anchor.hash, 2);
        let work = original
            .difficulty_threshold
            .to_work()
            .expect("the fixture target has valid work");

        assert_eq!(
            store.insert(
                original,
                work,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            ),
            Err(GraphError::ConflictingDuplicate(original_hash))
        );
    }

    #[test]
    fn removing_a_non_leaf_reports_that_its_children_are_retained() {
        let mut store = anchor_store();
        let anchor = store.finalized();
        let parent = insert_child(&mut store, anchor.hash, 1);
        let _child = insert_child(&mut store, parent.hash, 2);

        assert_eq!(
            store.remove_leaf(parent.hash),
            Err(GraphError::NodeHasChildren(parent.hash))
        );
        assert!(store.node(parent.hash).is_some());
    }

    #[test]
    fn advancing_finality_retains_exactly_the_new_finalized_subtree() {
        let mut store = anchor_store();
        let anchor = store.finalized();
        let selected_parent = insert_child(&mut store, anchor.hash, 1);
        let selected_child = insert_child(&mut store, selected_parent.hash, 2);
        let selected_tip = insert_child(&mut store, selected_child.hash, 3);
        let rejected_sibling = insert_child(&mut store, selected_parent.hash, 4);
        let rejected_descendant = insert_child(&mut store, rejected_sibling.hash, 5);

        let mut rebuilt = store.clone();
        let rebuilt_deleted = rebuild_finalized_reference(&mut rebuilt, selected_child)
            .expect("the rebuild oracle accepts the same finalized node");

        let deleted = store
            .advance_finalized(selected_child)
            .expect("the retained selected node can become finalized");

        assert_eq!(store.finalized(), selected_child);
        assert_eq!(deleted, rebuilt_deleted);
        assert_eq!(store.nodes, rebuilt.nodes);
        assert_eq!(store.children, rebuilt.children);
        assert_eq!(store.heights, rebuilt.heights);
        assert_eq!(store.eligible_tips, rebuilt.eligible_tips);
        assert!(store.node(selected_child.hash).is_some());
        assert!(store.node(selected_tip.hash).is_some());
        assert!(store.node(anchor.hash).is_none());
        assert!(store.node(selected_parent.hash).is_none());
        assert!(store.node(rejected_sibling.hash).is_none());
        assert!(store.node(rejected_descendant.hash).is_none());
        assert_eq!(
            deleted.into_iter().collect::<HashSet<_>>(),
            HashSet::from([
                anchor.hash,
                selected_parent.hash,
                rejected_sibling.hash,
                rejected_descendant.hash,
            ])
        );
    }

    #[test]
    fn advancing_finality_rejects_an_ineligible_root_without_mutation() {
        let mut store = anchor_store();
        let anchor = store.finalized();
        let candidate = insert_child(&mut store, anchor.hash, 1);
        store
            .add_eligibility_reason(
                candidate.hash,
                EligibilityReason::CheckpointConflict {
                    height: candidate.height,
                    expected: block::Hash([0xee; 32]),
                },
            )
            .expect("the fixture marks the candidate ineligible");
        let before = store.clone();

        assert_eq!(
            store.advance_finalized(candidate),
            Err(GraphError::IneligibleFinalized(candidate.hash))
        );
        assert_eq!(store.nodes, before.nodes);
        assert_eq!(store.children, before.children);
        assert_eq!(store.heights, before.heights);
        assert_eq!(store.eligible_tips, before.eligible_tips);
        assert_eq!(store.finalized, before.finalized);
    }

    fn uncached_eligible_tips(store: &MemHeaderStore) -> Vec<Frontier> {
        let mut tips: Vec<_> = store
            .nodes
            .values()
            .filter(|node| {
                node.is_eligible()
                    && !store.children.get(&node.hash).is_some_and(|children| {
                        children.iter().any(|child| {
                            store.nodes.get(child).is_some_and(HeaderNode::is_eligible)
                        })
                    })
            })
            .map(|node| Frontier::new(node.height, node.hash))
            .collect();
        tips.sort_unstable_by_key(|tip| tip.hash.0);
        tips
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

        let first = EligibilityReason::operator_invalid(
            left.hash,
            crate::OperatorInvalidationId::new([1; 16]),
            EvidenceId::from_digest([1; 32]),
        );
        let second = EligibilityReason::operator_invalid(
            left.hash,
            crate::OperatorInvalidationId::new([2; 16]),
            EvidenceId::from_digest([2; 32]),
        );
        store
            .add_eligibility_reason(left.hash, first)
            .expect("left is retained");
        store
            .add_eligibility_reason(left.hash, second)
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
            .remove_operator_invalidation(
                left.hash,
                crate::OperatorInvalidationId::new([1; 16]),
                Some(EvidenceId::from_digest([1; 32])),
            )
            .expect("left is retained");
        assert!(!store.node(left.hash).expect("retained").is_eligible());
        store
            .remove_operator_invalidation(
                left.hash,
                crate::OperatorInvalidationId::new([2; 16]),
                Some(EvidenceId::from_digest([2; 32])),
            )
            .expect("left is retained");
        assert!(store.node(left_tip.hash).expect("retained").is_eligible());
        assert_eq!(
            store.select_header_best().expect("graph is coherent").0,
            left_tip
        );
    }

    #[test]
    fn operator_reconsider_preserves_every_unnamed_reason() {
        let mut store = anchor_store();
        let anchor = store.finalized();
        let target = insert_child(&mut store, anchor.hash, 1);
        let descendant = insert_child(&mut store, target.hash, 2);
        let first_id = crate::OperatorInvalidationId::new([1; 16]);
        let second_id = crate::OperatorInvalidationId::new([2; 16]);
        let permanent_reasons = [
            EligibilityReason::SettledUpgradeConflict {
                height: target.height,
                expected: block::Hash([3; 32]),
            },
            EligibilityReason::CheckpointConflict {
                height: target.height,
                expected: block::Hash([4; 32]),
            },
            EligibilityReason::FinalityConflict { finalized: anchor },
        ];
        for reason in permanent_reasons.clone() {
            store
                .add_eligibility_reason(target.hash, reason)
                .expect("the operator target is retained");
        }
        for id in [first_id, second_id] {
            store
                .add_eligibility_reason(
                    target.hash,
                    EligibilityReason::operator_invalid(
                        target.hash,
                        id,
                        EvidenceId::from_digest([1; 32]),
                    ),
                )
                .expect("the operator target is retained");
        }
        let body_evidence = EvidenceId::from_digest([5; 32]);
        let body_rule = BodyRuleId::new("test.operator-reconsider");
        store
            .set_body_state(
                target.hash,
                BodyValidationState::ConsensusInvalid {
                    evidence: body_evidence,
                    rule: body_rule.clone(),
                },
            )
            .expect("intrinsic body invalidity is recorded independently");

        store
            .remove_operator_invalidation(
                target.hash,
                first_id,
                Some(EvidenceId::from_digest([1; 32])),
            )
            .expect("the operator target is retained");

        let target_node = store.node(target.hash).expect("the target is retained");
        assert!(!target_node
            .eligibility
            .direct_reasons
            .iter()
            .any(|reason| matches!(reason, EligibilityReason::OperatorInvalid { id, .. } if *id == first_id)));
        assert!(target_node
            .eligibility
            .direct_reasons
            .iter()
            .any(|reason| matches!(reason, EligibilityReason::OperatorInvalid { id, .. } if *id == second_id)));
        for reason in permanent_reasons {
            assert!(target_node.eligibility.direct_reasons.contains(&reason));
        }
        assert_eq!(
            target_node.body_validation_state,
            BodyValidationState::ConsensusInvalid {
                evidence: body_evidence,
                rule: body_rule,
            }
        );
        assert_eq!(
            store
                .node(descendant.hash)
                .expect("the descendant is retained")
                .eligibility
                .inherited_from,
            Some(target.hash)
        );
    }

    #[test]
    fn consensus_invalid_body_state_is_permanent_and_controls_eligibility() {
        let mut store = anchor_store();
        let anchor = store.finalized();
        let target = insert_child(&mut store, anchor.hash, 1);
        let descendant = insert_child(&mut store, target.hash, 2);
        let invalid = BodyValidationState::ConsensusInvalid {
            evidence: EvidenceId::from_digest([5; 32]),
            rule: BodyRuleId::new("test.consensus-invalid"),
        };

        assert!(store
            .set_body_state(target.hash, invalid.clone())
            .expect("the first consensus-invalid result is authoritative"));
        assert!(!store
            .set_body_state(target.hash, invalid)
            .expect("identical invalid evidence is idempotent"));
        let target_node = store
            .node(target.hash)
            .expect("the target remains retained");
        assert!(!target_node.is_eligible());
        assert!(target_node.eligibility.direct_reasons.is_empty());
        assert_eq!(
            store
                .node(descendant.hash)
                .expect("the descendant remains retained")
                .eligibility
                .inherited_from,
            Some(target.hash)
        );

        assert_eq!(
            store.set_body_state(
                target.hash,
                BodyValidationState::ConsensusInvalid {
                    evidence: EvidenceId::from_digest([6; 32]),
                    rule: BodyRuleId::new("test.conflicting-invalid"),
                },
            ),
            Err(GraphError::PermanentBodyInvalidity(target.hash))
        );
        assert_eq!(
            store.set_body_state(target.hash, BodyValidationState::Unknown),
            Err(GraphError::PermanentBodyInvalidity(target.hash))
        );
    }

    #[test]
    // DG-05: testing one below, at, and one above the retention boundary in
    // both insertion orders covers the fixed-anchor replacement edge.
    fn fixed_anchor_replacements_cover_finalization_boundary_in_both_orders() {
        fn insert_branch(
            store: &mut MemHeaderStore,
            anchor: Frontier,
            count: u32,
            seed_offset: u32,
        ) -> Frontier {
            let mut tip = anchor;
            for offset in 0..count {
                let seed =
                    u8::try_from((offset + seed_offset) % 251).expect("reduced nonce fits in u8");
                tip = insert_child(store, tip.hash, seed);
            }
            tip
        }

        for incumbent_depth in [999, 1_000, 1_001] {
            for competitor_first in [false, true] {
                let mut store = anchor_store();
                let anchor = store.finalized();
                let (incumbent, competitor) = if competitor_first {
                    let competitor = insert_branch(&mut store, anchor, incumbent_depth + 1, 127);
                    let incumbent = insert_branch(&mut store, anchor, incumbent_depth, 0);
                    (incumbent, competitor)
                } else {
                    let incumbent = insert_branch(&mut store, anchor, incumbent_depth, 0);
                    assert_eq!(
                        store.select_header_best().expect("graph is coherent").0,
                        incumbent
                    );
                    let competitor = insert_branch(&mut store, anchor, incumbent_depth + 1, 127);
                    (incumbent, competitor)
                };
                assert_ne!(incumbent.hash, competitor.hash);
                assert_eq!(
                    store.select_header_best().expect("graph is coherent").0,
                    competitor,
                    "selection is anchored at finalized and independent of depth or arrival order"
                );
            }
        }
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
                    ..Default::default()
                }),
            )
            .expect("the unavailable tip is retained");

        assert_eq!(
            store.select_header_best().expect("graph is coherent").0,
            unknown_tip
        );
        assert_eq!(
            store
                .node(verified_hash)
                .expect("retained")
                .body_validation_state,
            BodyValidationState::Verified {
                evidence: crate::EvidenceId::from_digest([4; 32])
            }
        );
        assert_eq!(
            store
                .node(unknown_tip.hash)
                .expect("retained")
                .body_validation_state,
            BodyValidationState::Unavailable(crate::BodyUnavailableSummary {
                attempts: 10,
                suppliers: 0,
                alarmed: true,
                ..Default::default()
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
                let mut evidence = [0; 32];
                evidence[..16].copy_from_slice(&id_bytes);
                evidence[16..].copy_from_slice(&id_bytes);
                let reason = EligibilityReason::operator_invalid(
                    target_hash,
                    crate::OperatorInvalidationId::new(id_bytes),
                    EvidenceId::from_digest(evidence),
                );

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
                                .add_eligibility_reason(target_hash, reason.clone())
                                .expect("target is retained");
                            model.nodes.get_mut(&target_hash).expect("target exists").direct_reasons.insert(reason);
                        }
                    }
                    2 => {
                        if target_hash != model.anchor {
                            let EligibilityReason::OperatorInvalid { id, evidence, .. } = reason else {
                                unreachable!("the generated reason is operator-scoped")
                            };
                            store
                                .remove_operator_invalidation(target_hash, id, Some(evidence))
                                .expect("target is retained");
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
                prop_assert_eq!(store.eligible_tips(), uncached_eligible_tips(&store));
            }
        }
    }
}
