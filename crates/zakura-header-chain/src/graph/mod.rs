//! Pure in-memory header DAG queries, eligibility propagation, and selection.

use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    fmt,
    sync::Arc,
};

use thiserror::Error;
use zakura_chain::{
    block,
    work::difficulty::{Work, U256},
};

use crate::{EvidenceId, OperatorInvalidationId};

mod frontier;
mod header_node;
mod overlay;

pub use frontier::{
    ChainScore, Frontier, FrontierSet, SuffixWork, WorkCoordinate, WorkCoordinateError,
};
pub use header_node::{
    BodyRuleId, BodyUnavailableSummary, BodyValidationState, DurableNodeError, EligibilityReason,
    EligibilityState, HeaderNode, HeaderValidationState,
};
pub(crate) use overlay::{GraphDelta, GraphOverlay};

#[cfg(test)]
pub(crate) mod test_support {
    use zakura_chain::block;

    use super::{GraphDelta, HeaderNode, MemHeaderStore};

    pub(crate) fn mutate_retained_header(
        graph: &mut MemHeaderStore,
        hash: block::Hash,
        mutation: impl FnOnce(&mut HeaderNode),
    ) {
        let header_node = graph
            .nodes
            .get_mut(&hash)
            .expect("the test mutation names a retained header");
        mutation(header_node);
    }

    pub(crate) fn mutate_updated_header(
        graph_delta: &mut GraphDelta,
        hash: block::Hash,
        mutation: impl FnOnce(&mut HeaderNode),
    ) {
        let header_node = graph_delta
            .updated_header_nodes
            .iter_mut()
            .find(|header_node| header_node.hash == hash)
            .expect("the test mutation names an updated header");
        mutation(header_node);
    }

    pub(crate) fn add_deleted_header(graph_delta: &mut GraphDelta, hash: block::Hash) {
        graph_delta.deleted_header_hashes.push(hash);
    }
}

/// Read-only access to one coherent retained header graph.
///
/// This abstraction allows fork-choice, planning, and invariant code to query
/// either the committed in-memory graph or a graph overlay containing proposed
/// changes. It exposes retained nodes, ancestry, eligibility, finality, and
/// derived fork-choice views without granting mutation or persistence access.
pub(crate) trait HeaderGraphView {
    /// Return the finalized frontier that roots every retained header path.
    fn view_finalized_frontier(&self) -> Frontier;
    /// Return the number of retained header nodes.
    fn view_header_node_count(&self) -> usize;
    /// Return the retained header node with the exact canonical hash.
    fn view_header_node(&self, hash: block::Hash) -> Option<&HeaderNode>;
    /// Return every retained header node.
    fn view_header_nodes(&self) -> Vec<&HeaderNode>;
    /// Return every retained canonical header hash.
    fn view_retained_header_hashes(&self) -> Vec<block::Hash>;
    /// Return retained header hashes at the exact height.
    fn view_header_hashes_at_height(&self, height: block::Height) -> Vec<block::Hash>;
    /// Return the retained direct children of the exact parent hash.
    fn view_header_children(&self, parent: block::Hash) -> Vec<block::Hash>;
    /// Return every eligible header without an eligible retained child.
    fn view_eligible_header_tips(&self) -> Vec<Frontier>;
    /// Select the eligible header chain with the greatest deterministic score.
    fn view_select_best_header_chain(&self) -> Result<(Frontier, ChainScore), GraphError>;
    /// Return one retained header's score relative to the finalized frontier.
    fn view_header_chain_score(&self, hash: block::Hash) -> Result<ChainScore, GraphError>;
    /// Return the descendant's retained ancestor at the exact height.
    fn view_header_ancestor(
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
    /// Insert one admitted header and derive its graph-owned fields.
    fn edit_insert_header(
        &mut self,
        header: Arc<block::Header>,
        validation: HeaderValidationState,
        direct_reasons: Vec<EligibilityReason>,
        body_validation_state: BodyValidationState,
    ) -> Result<InsertResult, GraphError>;
    /// Add one direct reason that excludes a retained header from selection.
    fn edit_add_header_eligibility_reason(
        &mut self,
        hash: block::Hash,
        reason: EligibilityReason,
    ) -> Result<bool, GraphError>;
    /// Remove the operator invalidation with the exact identity and evidence.
    fn edit_remove_header_operator_invalidation(
        &mut self,
        hash: block::Hash,
        id: OperatorInvalidationId,
        evidence: Option<EvidenceId>,
    ) -> Result<bool, GraphError>;
    /// Replace a retained header's body-validation state.
    fn edit_set_body_validation_state(
        &mut self,
        hash: block::Hash,
        body_validation_state: BodyValidationState,
    ) -> Result<bool, GraphError>;
    /// Replace a retained header's time-dependent validation state.
    fn edit_set_header_validation_state(
        &mut self,
        hash: block::Hash,
        validation: HeaderValidationState,
    ) -> Result<bool, GraphError>;
    /// Append one authenticated auxiliary evidence delivery identity.
    fn edit_record_auxiliary_evidence_delivery(
        &mut self,
        hash: block::Hash,
        delivery_id: EvidenceId,
    ) -> Result<bool, GraphError>;
    /// Move finality to an eligible descendant and remove every discarded path.
    fn edit_advance_finalized_frontier(
        &mut self,
        finalized_frontier: Frontier,
    ) -> Result<Vec<block::Hash>, GraphError>;
    /// Remove one retained header that has no retained children.
    fn edit_remove_header_leaf(&mut self, hash: block::Hash) -> Result<(), GraphError>;
}

/// Failure to construct or query a coherent in-memory header DAG.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum GraphError {
    /// A delta was produced from a different graph revision.
    #[error(
        "graph delta names base revision {delta_base_revision}, current revision is {current_revision}"
    )]
    StaleDelta {
        /// Current committed graph revision.
        current_revision: GraphRevision,
        /// Revision captured by the overlay.
        delta_base_revision: GraphRevision,
    },
    /// The graph revision cannot advance further.
    #[error("graph revision exhausted")]
    RevisionExhausted,
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
    /// A reconstructed graph contains more than one row for one hash.
    #[error("duplicate reconstructed header {0:?}")]
    DuplicateHeaderNode(block::Hash),
    /// A retained header row violates one structural invariant.
    #[error("retained header {header:?} violates the {invariant} invariant")]
    InvalidHeaderNode {
        /// Header whose durable row is invalid.
        header: block::Hash,
        /// Exact structural invariant that failed.
        invariant: HeaderNodeInvariant,
    },
    /// A requested retained node does not exist.
    #[error("unknown retained header {0:?}")]
    UnknownHeaderNode(block::Hash),
    /// Finality attempted to root the graph at an ineligible header.
    #[error("cannot finalize ineligible retained header {0:?}")]
    IneligibleFinalizedFrontier(block::Hash),
    /// Finality selected a retained header outside the current finalized subtree.
    #[error(
        "new finalized header {candidate:?} is not a descendant of current finalized header {current:?}"
    )]
    FinalizedFrontierNotDescendant {
        /// Current finalized root.
        current: block::Hash,
        /// Proposed finalized root.
        candidate: block::Hash,
    },
    /// Retention attempted to remove a node that still has retained children.
    #[error("cannot remove non-leaf header {0:?}")]
    HeaderNodeHasChildren(block::Hash),
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

/// Structural fact that authenticates one retained header row.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum HeaderNodeInvariant {
    /// The header difficulty target must encode valid canonical block work.
    #[error("canonical block work")]
    CanonicalBlockWork,
    /// The finalized row height must match the finalized frontier height.
    #[error("finalized frontier height")]
    FinalizedFrontierHeight,
    /// The stored hash must equal the canonical header hash.
    #[error("canonical header hash")]
    CanonicalHeaderHash,
    /// The stored parent hash must equal the canonical header parent hash.
    #[error("canonical parent hash")]
    CanonicalParentHash,
    /// Every retained coordinate must use the finalized row's work origin.
    #[error("work origin")]
    WorkOrigin,
    /// A child height must immediately follow its retained parent height.
    #[error("parent height")]
    ParentHeight,
    /// A child coordinate must equal its parent coordinate plus canonical block work.
    #[error("cumulative work")]
    CumulativeWork,
    /// An ordinary graph transition must preserve immutable header fields.
    #[error("immutable header fields")]
    ImmutableFields,
    /// A work rebase must use the current finalized frontier as its origin.
    #[error("work rebase origin")]
    WorkRebaseOrigin,
    /// A work rebase must preserve work relative to the finalized frontier.
    #[error("work rebase coordinate")]
    WorkRebaseCoordinate,
    /// Reconstruction must preserve the supplied direct header state.
    #[error("derived header state")]
    DerivedHeaderState,
}

/// Monotonic capability that identifies one committed in-memory graph state.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphRevision(u64);

impl GraphRevision {
    fn checked_next(self) -> Result<Self, GraphError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(GraphError::RevisionExhausted)
    }
}

impl fmt::Display for GraphRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Permanent consensus-invalid body evidence keyed by canonical header hash.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsensusInvalidBodyTombstone {
    /// Canonical header hash whose body failed consensus.
    pub hash: block::Hash,
    /// Exact authoritative evidence identity.
    pub evidence: EvidenceId,
    /// Exact failed consensus rule.
    pub rule: BodyRuleId,
}

/// Complete audited input for transactional header-graph reconstruction.
#[derive(Clone, Debug)]
pub struct HeaderGraphReconstruction {
    /// Finalized frontier that roots every retained header path.
    finalized_frontier: Frontier,
    /// Durable header rows to validate and index.
    header_nodes: Vec<HeaderNode>,
    /// Append-only consensus-invalid body evidence, including pruned headers.
    consensus_invalid_body_tombstones: Vec<ConsensusInvalidBodyTombstone>,
}

impl HeaderGraphReconstruction {
    /// Collect the durable rows that form one reconstruction transaction.
    pub fn new(
        finalized_frontier: Frontier,
        header_nodes: impl IntoIterator<Item = HeaderNode>,
        consensus_invalid_body_tombstones: impl IntoIterator<Item = ConsensusInvalidBodyTombstone>,
    ) -> Self {
        Self {
            finalized_frontier,
            header_nodes: header_nodes.into_iter().collect(),
            consensus_invalid_body_tombstones: consensus_invalid_body_tombstones
                .into_iter()
                .collect(),
        }
    }
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
    graph_revision: GraphRevision,
    finalized_frontier: Frontier,
    // Nodes by hash.
    nodes: HashMap<block::Hash, HeaderNode>,
    // Children by parent hash.
    children: HashMap<block::Hash, HashSet<block::Hash>>,
    heights: HashMap<block::Height, HashSet<block::Hash>>,

    // A hash belongs to this when: a header is eligible (validated, no exclusion reasons, no ineligible ancestors)
    // and has no eligible children.
    eligible_header_tips: HashSet<block::Hash>,
    consensus_invalid_body_tombstones: HashMap<block::Hash, ConsensusInvalidBodyTombstone>,
}

impl MemHeaderStore {
    /// Construct a store rooted at one trusted, already-validated work origin.
    pub fn new(
        finalized_frontier: Frontier,
        header: Arc<block::Header>,
        block_work: Work,
        cumulative_work: U256,
    ) -> Result<Self, GraphError> {
        let actual = header.hash();
        if actual != finalized_frontier.hash {
            return Err(GraphError::AnchorHashMismatch {
                expected: finalized_frontier.hash,
                actual,
            });
        }
        let anchor = HeaderNode {
            parent_hash: header.previous_block_hash,
            header,
            hash: finalized_frontier.hash,
            height: finalized_frontier.height,
            block_work,
            work_coordinate: WorkCoordinate::new(finalized_frontier.hash, cumulative_work),
            validation: HeaderValidationState::Valid,
            eligibility: EligibilityState::default(),
            body_validation_state: BodyValidationState::Unknown,
            aux_delivery_ids: Vec::new(),
        };
        let mut nodes = HashMap::new();
        nodes.insert(finalized_frontier.hash, anchor);
        let mut heights = HashMap::new();
        heights.insert(
            finalized_frontier.height,
            HashSet::from([finalized_frontier.hash]),
        );
        Ok(Self {
            graph_revision: GraphRevision::default(),
            finalized_frontier,
            nodes,
            children: HashMap::new(),
            heights,
            eligible_header_tips: HashSet::from([finalized_frontier.hash]),
            consensus_invalid_body_tombstones: HashMap::new(),
        })
    }

    /// Return the revision that binds newly created graph deltas.
    pub const fn graph_revision(&self) -> GraphRevision {
        self.graph_revision
    }

    /// Return permanent consensus-invalid evidence for `hash`.
    pub fn consensus_invalid_body_tombstone(
        &self,
        hash: block::Hash,
    ) -> Option<&ConsensusInvalidBodyTombstone> {
        self.consensus_invalid_body_tombstones.get(&hash)
    }

    /// Return the immutable finalized root of every eligible path.
    pub const fn finalized_frontier(&self) -> Frontier {
        self.finalized_frontier
    }

    /// Return the number of retained nodes, including the finalized anchor.
    pub fn header_node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Read one retained node by exact consensus hash.
    pub fn header_node(&self, hash: block::Hash) -> Option<&HeaderNode> {
        self.nodes.get(&hash)
    }

    /// Return every retained hash at a height, ordered by raw internal bytes.
    pub fn header_hashes_at_height(&self, height: block::Height) -> Vec<block::Hash> {
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
    pub fn header_children(&self, parent: block::Hash) -> Vec<block::Hash> {
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
        validation: HeaderValidationState,
        direct_reasons: impl IntoIterator<Item = EligibilityReason>,
        mut body_validation_state: BodyValidationState,
    ) -> Result<InsertResult, GraphError> {
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
        let block_work =
            header
                .difficulty_threshold
                .to_work()
                .ok_or(GraphError::InvalidHeaderNode {
                    header: hash,
                    invariant: HeaderNodeInvariant::CanonicalBlockWork,
                })?;
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
        let work_coordinate = match parent.work_coordinate().checked_add(block_work) {
            Ok(coordinate) => coordinate,
            Err(WorkCoordinateError::Overflow) if validation == HeaderValidationState::Valid => {
                self.rebase_work_coordinates_to_finalized_frontier()?;
                self.nodes
                    .get(&parent_hash)
                    .expect("the work rebase retains every graph node")
                    .work_coordinate()
                    .checked_add(block_work)?
            }
            Err(error) => return Err(error.into()),
        };
        if let Some(tombstone) = self.consensus_invalid_body_tombstones.get(&hash) {
            body_validation_state = BodyValidationState::ConsensusInvalid {
                evidence: tombstone.evidence,
                rule: tombstone.rule.clone(),
            };
        }
        let direct_reasons: BTreeSet<EligibilityReason> = direct_reasons.into_iter().collect();
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
        self.nodes.insert(hash, node);
        self.children.entry(parent_hash).or_default().insert(hash);
        self.heights.entry(height).or_default().insert(hash);
        if self
            .nodes
            .get(&hash)
            .expect("the inserted node is present")
            .is_eligible()
        {
            self.eligible_header_tips.remove(&parent_hash);
            self.eligible_header_tips.insert(hash);
        }
        self.advance_revision()?;
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
            .ok_or(GraphError::UnknownHeaderNode(hash))?
            .eligibility
            .direct_reasons
            .insert(reason);
        if changed {
            self.recompute_descendant_eligibility(hash)?;
            self.advance_revision()?;
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
            .ok_or(GraphError::UnknownHeaderNode(hash))?
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
            self.advance_revision()?;
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
        if let Some(existing) = self.consensus_invalid_body_tombstones.get(&hash) {
            if tombstone.as_ref() != Some(existing) {
                return Err(GraphError::PermanentBodyInvalidity(hash));
            }
        }
        let (changed, eligibility_changed) = {
            let node = self
                .nodes
                .get_mut(&hash)
                .ok_or(GraphError::UnknownHeaderNode(hash))?;
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
            self.consensus_invalid_body_tombstones
                .insert(hash, tombstone);
        }
        if changed {
            self.advance_revision()?;
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
    pub(crate) fn set_header_validation_state(
        &mut self,
        hash: block::Hash,
        validation: HeaderValidationState,
    ) -> Result<bool, GraphError> {
        let node = self
            .nodes
            .get_mut(&hash)
            .ok_or(GraphError::UnknownHeaderNode(hash))?;
        let changed = node.validation != validation;
        node.validation = validation;
        if changed {
            self.recompute_descendant_eligibility(hash)?;
            self.advance_revision()?;
        }
        Ok(changed)
    }

    /// Append one authenticated auxiliary evidence ID to a retained header.
    pub(crate) fn record_auxiliary_evidence_delivery(
        &mut self,
        hash: block::Hash,
        delivery_id: EvidenceId,
    ) -> Result<bool, GraphError> {
        let ids = &mut self
            .nodes
            .get_mut(&hash)
            .ok_or(GraphError::UnknownHeaderNode(hash))?
            .aux_delivery_ids;
        if ids.contains(&delivery_id) {
            return Ok(false);
        }
        ids.push(delivery_id);
        self.advance_revision()?;
        Ok(true)
    }

    /// Rebase every retained work coordinate to the current finalized frontier.
    pub(crate) fn rebase_work_coordinates_to_finalized_frontier(
        &mut self,
    ) -> Result<(), GraphError> {
        let anchor = self
            .header_node(self.finalized_frontier.hash)
            .ok_or(GraphError::UnknownHeaderNode(self.finalized_frontier.hash))?
            .work_coordinate();
        let mut rebased = HashMap::with_capacity(self.nodes.len());
        for node in self.nodes.values() {
            let suffix = node.work_coordinate().suffix_after(anchor)?.as_u256();
            rebased.insert(
                node.hash,
                WorkCoordinate::new(self.finalized_frontier.hash, suffix),
            );
        }
        for (hash, coordinate) in rebased {
            self.nodes
                .get_mut(&hash)
                .expect("rebased coordinates came from retained nodes")
                .work_coordinate = coordinate;
        }
        self.advance_revision()?;
        Ok(())
    }

    /// Return the exact ancestor at `height`, if the retained path reaches it.
    pub fn header_ancestor(
        &self,
        descendant: block::Hash,
        height: block::Height,
    ) -> Result<Option<Frontier>, GraphError> {
        let mut node = self
            .nodes
            .get(&descendant)
            .ok_or(GraphError::UnknownHeaderNode(descendant))?;
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
    pub fn eligible_header_tips(&self) -> Vec<Frontier> {
        let mut tips: Vec<_> = self
            .eligible_header_tips
            .iter()
            .filter_map(|hash| self.nodes.get(hash))
            .map(|node| Frontier::new(node.height, node.hash))
            .collect();
        tips.sort_unstable_by_key(|tip| tip.hash.0);
        tips
    }

    /// Select the deterministic greatest-work eligible tip after the finalized anchor.
    pub fn select_best_header_chain(&self) -> Result<(Frontier, ChainScore), GraphError> {
        let anchor = self
            .nodes
            .get(&self.finalized_frontier.hash)
            .ok_or(GraphError::UnknownHeaderNode(self.finalized_frontier.hash))?;
        let mut best = None;
        for hash in &self.eligible_header_tips {
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
        best.ok_or(GraphError::UnknownHeaderNode(self.finalized_frontier.hash))
    }

    /// Return the selection score of one retained descendant of the finalized anchor.
    pub fn header_chain_score(&self, hash: block::Hash) -> Result<ChainScore, GraphError> {
        let anchor = self
            .nodes
            .get(&self.finalized_frontier.hash)
            .ok_or(GraphError::UnknownHeaderNode(self.finalized_frontier.hash))?;
        let node = self
            .nodes
            .get(&hash)
            .ok_or(GraphError::UnknownHeaderNode(hash))?;
        Ok(ChainScore::new(
            node.work_coordinate()
                .suffix_after(anchor.work_coordinate())?,
            hash,
        ))
    }

    /// Validate durable rows and reconstruct every derived graph index.
    ///
    /// The constructor rejects duplicate hashes, non-canonical header hashes,
    /// invalid parent links, height gaps, forged block work, forged work
    /// coordinates, inconsistent permanent body invalidity, and an invalid
    /// finalized frontier. The constructor computes inherited eligibility and
    /// eligible header tips only after every source row passes validation.
    pub fn reconstruct(reconstruction: HeaderGraphReconstruction) -> Result<Self, GraphError> {
        let HeaderGraphReconstruction {
            finalized_frontier,
            header_nodes,
            consensus_invalid_body_tombstones: durable_consensus_invalid_body_tombstones,
        } = reconstruction;
        let mut node_map = HashMap::new();
        for node in header_nodes {
            let hash = node.hash;
            if node_map.insert(hash, node).is_some() {
                return Err(GraphError::DuplicateHeaderNode(hash));
            }
        }
        let mut consensus_invalid_body_tombstones = HashMap::new();
        for tombstone in durable_consensus_invalid_body_tombstones {
            let hash = tombstone.hash;
            if let Some(existing) =
                consensus_invalid_body_tombstones.insert(hash, tombstone.clone())
            {
                if existing != tombstone {
                    return Err(GraphError::PermanentBodyInvalidity(hash));
                }
                return Err(GraphError::DuplicateHeaderNode(hash));
            }
        }
        let finalized_node = node_map
            .get(&finalized_frontier.hash)
            .ok_or(GraphError::UnknownHeaderNode(finalized_frontier.hash))?;
        if finalized_node.height != finalized_frontier.height {
            return Err(GraphError::InvalidHeaderNode {
                header: finalized_frontier.hash,
                invariant: HeaderNodeInvariant::FinalizedFrontierHeight,
            });
        }
        let anchor_coordinate = finalized_node.work_coordinate();
        for node in node_map.values() {
            if node.header.hash() != node.hash {
                return Err(GraphError::InvalidHeaderNode {
                    header: node.hash,
                    invariant: HeaderNodeInvariant::CanonicalHeaderHash,
                });
            }
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
            if node.work_coordinate().origin_hash() != anchor_coordinate.origin_hash() {
                return Err(GraphError::InvalidHeaderNode {
                    header: node.hash,
                    invariant: HeaderNodeInvariant::WorkOrigin,
                });
            }
            if node.hash != finalized_frontier.hash {
                let parent = node_map
                    .get(&node.parent_hash)
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
                if parent.work_coordinate().checked_add(node.block_work)? != node.work_coordinate()
                {
                    return Err(GraphError::InvalidHeaderNode {
                        header: node.hash,
                        invariant: HeaderNodeInvariant::CumulativeWork,
                    });
                }
            }
            match (
                &node.body_validation_state,
                consensus_invalid_body_tombstones.get(&node.hash),
            ) {
                (BodyValidationState::ConsensusInvalid { evidence, rule }, Some(tombstone))
                    if *evidence == tombstone.evidence && *rule == tombstone.rule => {}
                (BodyValidationState::ConsensusInvalid { .. }, _) => {
                    return Err(GraphError::PermanentBodyInvalidity(node.hash));
                }
                (_, Some(_)) => {}
                (_, None) => {}
            }
        }
        for (hash, tombstone) in &consensus_invalid_body_tombstones {
            if let Some(node) = node_map.get_mut(hash) {
                node.body_validation_state = BodyValidationState::ConsensusInvalid {
                    evidence: tombstone.evidence,
                    rule: tombstone.rule.clone(),
                };
            }
        }
        let mut inherited = HashMap::with_capacity(node_map.len());
        let mut frontiers: Vec<_> = node_map
            .values()
            .map(|node| Frontier::new(node.height, node.hash))
            .collect();
        frontiers.sort_unstable_by_key(|frontier| (frontier.height, frontier.hash.0));
        for frontier in frontiers {
            if frontier == finalized_frontier {
                inherited.insert(frontier.hash, None);
                continue;
            }
            let node = &node_map[&frontier.hash];
            let parent = &node_map[&node.parent_hash];
            let parent_eligible = parent.validation == HeaderValidationState::Valid
                && parent.eligibility.direct_reasons.is_empty()
                && inherited.get(&parent.hash) == Some(&None)
                && !matches!(
                    parent.body_validation_state,
                    BodyValidationState::ConsensusInvalid { .. }
                );
            inherited.insert(frontier.hash, (!parent_eligible).then_some(parent.hash));
        }
        for (hash, inherited_from) in inherited {
            node_map
                .get_mut(&hash)
                .expect("eligibility changes came from reconstructed nodes")
                .eligibility
                .inherited_from = inherited_from;
        }
        if !node_map[&finalized_frontier.hash].is_eligible() {
            return Err(GraphError::IneligibleFinalizedFrontier(
                finalized_frontier.hash,
            ));
        }
        let mut children: HashMap<_, HashSet<_>> = HashMap::new();
        let mut heights: HashMap<_, HashSet<_>> = HashMap::new();
        for node in node_map.values() {
            heights.entry(node.height).or_default().insert(node.hash);
            if node.hash != finalized_frontier.hash {
                children
                    .entry(node.parent_hash)
                    .or_default()
                    .insert(node.hash);
            }
        }
        let mut store = Self {
            graph_revision: GraphRevision::default(),
            finalized_frontier,
            nodes: node_map,
            children,
            heights,
            eligible_header_tips: HashSet::new(),
            consensus_invalid_body_tombstones,
        };
        store.rebuild_eligible_header_tips();
        Ok(store)
    }

    pub(crate) fn header_nodes(&self) -> impl Iterator<Item = &HeaderNode> {
        self.nodes.values()
    }

    #[cfg(test)]
    pub(crate) fn consensus_invalid_body_tombstones(
        &self,
    ) -> impl Iterator<Item = &ConsensusInvalidBodyTombstone> {
        self.consensus_invalid_body_tombstones.values()
    }

    /// Propagates an eligibility change at `root` through its retained subtree.
    ///
    /// Assumes `root` already contains its updated direct eligibility state.
    /// For every descendant, recomputes `inherited_from` from its parent’s current
    /// eligibility. Then refreshes eligible-tip membership for the root, all
    /// descendants, and their parents.
    ///
    /// Returns `GraphError::UnknownHeaderNode` if a child edge references a missing node
    /// or a retained child references a missing parent.
    pub(crate) fn recompute_all_header_eligibility(&mut self) -> Result<(), GraphError> {
        let mut frontiers: Vec<_> = self
            .nodes
            .values()
            .map(|node| Frontier::new(node.height, node.hash))
            .collect();
        frontiers.sort_unstable_by_key(|frontier| (frontier.height, frontier.hash.0));
        let mut changes = Vec::with_capacity(frontiers.len());
        let mut eligible = HashMap::with_capacity(frontiers.len());
        for frontier in frontiers {
            if frontier == self.finalized_frontier {
                changes.push((frontier.hash, None));
                let node = self
                    .header_node(frontier.hash)
                    .ok_or(GraphError::UnknownHeaderNode(frontier.hash))?;
                eligible.insert(
                    frontier.hash,
                    node.validation == HeaderValidationState::Valid
                        && node.eligibility.direct_reasons.is_empty()
                        && !matches!(
                            node.body_validation_state,
                            BodyValidationState::ConsensusInvalid { .. }
                        ),
                );
                continue;
            }
            let parent_hash = self
                .header_node(frontier.hash)
                .expect("frontier came from nodes")
                .parent_hash;
            let _parent = self
                .header_node(parent_hash)
                .ok_or(GraphError::UnknownParent {
                    header: frontier.hash,
                    parent: parent_hash,
                })?;
            let parent_eligible =
                eligible
                    .get(&parent_hash)
                    .copied()
                    .ok_or(GraphError::UnknownParent {
                        header: frontier.hash,
                        parent: parent_hash,
                    })?;
            let inherited_from = (!parent_eligible).then_some(parent_hash);
            let node = self
                .header_node(frontier.hash)
                .ok_or(GraphError::UnknownHeaderNode(frontier.hash))?;
            eligible.insert(
                frontier.hash,
                node.validation == HeaderValidationState::Valid
                    && node.eligibility.direct_reasons.is_empty()
                    && inherited_from.is_none()
                    && !matches!(
                        node.body_validation_state,
                        BodyValidationState::ConsensusInvalid { .. }
                    ),
            );
            changes.push((frontier.hash, inherited_from));
        }
        for (hash, inherited_from) in changes {
            self.nodes
                .get_mut(&hash)
                .expect("eligibility changes came from retained nodes")
                .eligibility
                .inherited_from = inherited_from;
        }
        self.rebuild_eligible_header_tips();
        Ok(())
    }

    /// Move the finalized frontier to an eligible retained descendant.
    ///
    /// The graph retains the new finalized frontier and every descendant. The
    /// graph removes every ancestor and competing branch. The graph rebuilds
    /// each affected index. Validation failures leave the graph unchanged. The
    /// method returns removed header hashes in deterministic order.
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
        if self.header_ancestor(finalized_frontier.hash, self.finalized_frontier.height)?
            != Some(self.finalized_frontier)
        {
            return Err(GraphError::FinalizedFrontierNotDescendant {
                current: self.finalized_frontier.hash,
                candidate: finalized_frontier.hash,
            });
        }
        // Visited hashes
        let mut retained = HashSet::new();
        // Nodes to visit
        let mut pending = vec![finalized_frontier.hash];

        // Traverse the graph in depth-first order, starting from the new finalized frontier.
        while let Some(hash) = pending.pop() {
            if retained.insert(hash) {
                pending.extend(self.header_children(hash));
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
            self.eligible_header_tips.remove(hash);
            if let Some(hashes) = self.heights.get_mut(&node.height) {
                hashes.remove(hash);
                if hashes.is_empty() {
                    self.heights.remove(&node.height);
                }
            }
        }
        self.finalized_frontier = finalized_frontier;
        self.nodes
            .get_mut(&finalized_frontier.hash)
            .expect("the new finalized root is retained")
            .eligibility
            .inherited_from = None;
        self.refresh_eligible_header_tip(finalized_frontier.hash);
        self.advance_revision()?;
        Ok(deleted)
    }

    pub(crate) fn retained_header_hashes(&self) -> impl Iterator<Item = block::Hash> + '_ {
        self.nodes.keys().copied()
    }

    pub(crate) fn remove_header_leaf(&mut self, hash: block::Hash) -> Result<(), GraphError> {
        let node = self
            .nodes
            .get(&hash)
            .ok_or(GraphError::UnknownHeaderNode(hash))?;
        if self
            .children
            .get(&hash)
            .is_some_and(|children| !children.is_empty())
        {
            return Err(GraphError::HeaderNodeHasChildren(hash));
        }
        let parent_hash = node.parent_hash;
        let height = node.height;
        self.eligible_header_tips.remove(&hash);
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
        self.refresh_eligible_header_tip(parent_hash);
        self.advance_revision()?;
        Ok(())
    }

    fn advance_revision(&mut self) -> Result<(), GraphError> {
        self.graph_revision = self.graph_revision.checked_next()?;
        Ok(())
    }

    fn recompute_descendant_eligibility(&mut self, root: block::Hash) -> Result<(), GraphError> {
        let mut affected = HashSet::from([root]);
        affected.insert(
            self.nodes
                .get(&root)
                .ok_or(GraphError::UnknownHeaderNode(root))?
                .parent_hash,
        );
        let mut queue = VecDeque::from(self.header_children(root));
        while let Some(hash) = queue.pop_front() {
            affected.insert(hash);
            let parent_hash = self
                .nodes
                .get(&hash)
                .ok_or(GraphError::UnknownHeaderNode(hash))?
                .parent_hash;
            affected.insert(parent_hash);
            let parent = self
                .nodes
                .get(&parent_hash)
                .ok_or(GraphError::UnknownHeaderNode(parent_hash))?;
            let inherited_from = (!parent.is_eligible()).then_some(parent_hash);
            self.nodes
                .get_mut(&hash)
                .expect("the queued child was read from the retained node map")
                .eligibility
                .inherited_from = inherited_from;
            queue.extend(self.header_children(hash));
        }
        for hash in affected {
            self.refresh_eligible_header_tip(hash);
        }
        Ok(())
    }

    fn has_eligible_header_child(&self, hash: block::Hash) -> bool {
        self.children.get(&hash).is_some_and(|children| {
            children
                .iter()
                .any(|child| self.nodes.get(child).is_some_and(HeaderNode::is_eligible))
        })
    }

    fn refresh_eligible_header_tip(&mut self, hash: block::Hash) {
        self.eligible_header_tips.remove(&hash);
        if self
            .nodes
            .get(&hash)
            .is_some_and(|node| node.is_eligible() && !self.has_eligible_header_child(hash))
        {
            self.eligible_header_tips.insert(hash);
        }
    }

    fn rebuild_eligible_header_tips(&mut self) {
        self.eligible_header_tips = self
            .nodes
            .values()
            .filter(|node| node.is_eligible() && !self.has_eligible_header_child(node.hash))
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
        let application = GraphOverlay::from_delta(self, delta)?.into_delta_application();
        debug_assert_eq!(application.base_revision, self.graph_revision);
        if delta.is_empty() {
            return Ok(());
        }
        let next_revision = self.graph_revision.checked_next()?;

        for hash in &application.deleted_header_hashes {
            let node = self
                .nodes
                .remove(hash)
                .expect("validated deletions reference retained graph nodes");
            self.children.remove(hash);
            if let Some(hashes) = self.heights.get_mut(&node.height) {
                hashes.remove(hash);
                if hashes.is_empty() {
                    self.heights.remove(&node.height);
                }
            }
        }
        for (hash, node) in application.updated_header_nodes_by_hash {
            if !self.nodes.contains_key(&hash) {
                self.heights.entry(node.height).or_default().insert(hash);
            }
            self.nodes.insert(hash, node);
        }
        for (parent, removed) in application.removed_header_children {
            if let Some(children) = self.children.get_mut(&parent) {
                children.retain(|child| !removed.contains(child));
                if children.is_empty() {
                    self.children.remove(&parent);
                }
            }
        }
        for (parent, added) in application.added_header_children {
            self.children.entry(parent).or_default().extend(added);
        }
        self.consensus_invalid_body_tombstones
            .extend(application.new_consensus_invalid_body_tombstones_by_hash);
        self.finalized_frontier = application.finalized_frontier;
        self.eligible_header_tips = application.eligible_header_tips;
        self.graph_revision = next_revision;
        Ok(())
    }
}

impl HeaderGraphView for MemHeaderStore {
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

impl HeaderGraphEdit for MemHeaderStore {
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
        match store
            .insert(
                header,
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
            .header_node(finalized.hash)
            .ok_or(GraphError::UnknownHeaderNode(finalized.hash))?;
        if node.height != finalized.height {
            return Err(GraphError::UnknownHeaderNode(finalized.hash));
        }
        if !node.is_eligible() {
            return Err(GraphError::IneligibleFinalizedFrontier(finalized.hash));
        }
        let mut retained = HashSet::new();
        let mut pending = vec![finalized.hash];
        while let Some(hash) = pending.pop() {
            if retained.insert(hash) {
                pending.extend(store.header_children(hash));
            }
        }
        let mut deleted: Vec<_> = store
            .nodes
            .keys()
            .copied()
            .filter(|hash| !retained.contains(hash))
            .collect();
        deleted.sort_unstable_by_key(|hash| hash.0);
        let finalized_header_nodes: Vec<_> = store
            .nodes
            .values()
            .filter(|node| retained.contains(&node.hash))
            .cloned()
            .collect();
        let tombstones = store
            .consensus_invalid_body_tombstones()
            .cloned()
            .collect::<Vec<_>>();
        *store = MemHeaderStore::reconstruct(HeaderGraphReconstruction::new(
            finalized,
            finalized_header_nodes,
            tombstones,
        ))?;
        store.recompute_all_header_eligibility()?;
        Ok(deleted)
    }

    #[test]
    fn conflicting_duplicate_reports_the_duplicate_hash() {
        let mut store = anchor_store();
        let anchor = store.finalized_frontier();
        let original = child(anchor.hash, 1);
        let original_hash = original.hash();
        let frontier = insert_child(&mut store, anchor.hash, 1);
        assert_eq!(frontier.hash, original_hash);

        store
            .nodes
            .get_mut(&original_hash)
            .expect("the inserted fixture node is retained")
            .header = child(anchor.hash, 2);

        assert_eq!(
            store.insert(
                original,
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
        let anchor = store.finalized_frontier();
        let parent = insert_child(&mut store, anchor.hash, 1);
        let _child = insert_child(&mut store, parent.hash, 2);

        assert_eq!(
            store.remove_header_leaf(parent.hash),
            Err(GraphError::HeaderNodeHasChildren(parent.hash))
        );
        assert!(store.header_node(parent.hash).is_some());
    }

    #[test]
    fn advancing_finality_retains_exactly_the_new_finalized_subtree() {
        let mut store = anchor_store();
        let anchor = store.finalized_frontier();
        let selected_parent = insert_child(&mut store, anchor.hash, 1);
        let selected_child = insert_child(&mut store, selected_parent.hash, 2);
        let selected_tip = insert_child(&mut store, selected_child.hash, 3);
        let rejected_sibling = insert_child(&mut store, selected_parent.hash, 4);
        let rejected_descendant = insert_child(&mut store, rejected_sibling.hash, 5);

        let mut rebuilt = store.clone();
        let rebuilt_deleted = rebuild_finalized_reference(&mut rebuilt, selected_child)
            .expect("the rebuild oracle accepts the same finalized node");

        let deleted = store
            .advance_finalized_frontier(selected_child)
            .expect("the retained selected node can become finalized");

        assert_eq!(store.finalized_frontier(), selected_child);
        assert_eq!(deleted, rebuilt_deleted);
        assert_eq!(store.nodes, rebuilt.nodes);
        assert_eq!(store.children, rebuilt.children);
        assert_eq!(store.heights, rebuilt.heights);
        assert_eq!(store.eligible_header_tips, rebuilt.eligible_header_tips);
        assert!(store.header_node(selected_child.hash).is_some());
        assert!(store.header_node(selected_tip.hash).is_some());
        assert!(store.header_node(anchor.hash).is_none());
        assert!(store.header_node(selected_parent.hash).is_none());
        assert!(store.header_node(rejected_sibling.hash).is_none());
        assert!(store.header_node(rejected_descendant.hash).is_none());
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
        let anchor = store.finalized_frontier();
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
            store.advance_finalized_frontier(candidate),
            Err(GraphError::IneligibleFinalizedFrontier(candidate.hash))
        );
        assert_eq!(store.nodes, before.nodes);
        assert_eq!(store.children, before.children);
        assert_eq!(store.heights, before.heights);
        assert_eq!(store.eligible_header_tips, before.eligible_header_tips);
        assert_eq!(store.finalized_frontier, before.finalized_frontier);
    }

    fn uncached_eligible_header_tips(store: &MemHeaderStore) -> Vec<Frontier> {
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
        let anchor = store.finalized_frontier();
        let left = insert_child(&mut store, anchor.hash, 1);
        let right = insert_child(&mut store, anchor.hash, 2);
        assert_eq!(store.header_hashes_at_height(block::Height(1)).len(), 2);

        let left_tip = insert_child(&mut store, left.hash, 3);
        assert_eq!(
            store
                .select_best_header_chain()
                .expect("graph is coherent")
                .0,
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
                .header_node(left.hash)
                .expect("retained")
                .eligibility
                .direct_reasons
                .len(),
            2
        );
        assert_eq!(
            store
                .header_node(left_tip.hash)
                .expect("retained")
                .eligibility
                .inherited_from,
            Some(left.hash)
        );
        assert_eq!(
            store
                .select_best_header_chain()
                .expect("graph is coherent")
                .0,
            right
        );

        store
            .remove_operator_invalidation(
                left.hash,
                crate::OperatorInvalidationId::new([1; 16]),
                Some(EvidenceId::from_digest([1; 32])),
            )
            .expect("left is retained");
        assert!(!store
            .header_node(left.hash)
            .expect("retained")
            .is_eligible());
        store
            .remove_operator_invalidation(
                left.hash,
                crate::OperatorInvalidationId::new([2; 16]),
                Some(EvidenceId::from_digest([2; 32])),
            )
            .expect("left is retained");
        assert!(store
            .header_node(left_tip.hash)
            .expect("retained")
            .is_eligible());
        assert_eq!(
            store
                .select_best_header_chain()
                .expect("graph is coherent")
                .0,
            left_tip
        );
    }

    #[test]
    fn operator_reconsider_preserves_every_unnamed_reason() {
        let mut store = anchor_store();
        let anchor = store.finalized_frontier();
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
            .set_body_validation_state(
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

        let target_node = store
            .header_node(target.hash)
            .expect("the target is retained");
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
                .header_node(descendant.hash)
                .expect("the descendant is retained")
                .eligibility
                .inherited_from,
            Some(target.hash)
        );
    }

    #[test]
    fn consensus_invalid_body_state_is_permanent_and_controls_eligibility() {
        let mut store = anchor_store();
        let anchor = store.finalized_frontier();
        let target = insert_child(&mut store, anchor.hash, 1);
        let descendant = insert_child(&mut store, target.hash, 2);
        let invalid = BodyValidationState::ConsensusInvalid {
            evidence: EvidenceId::from_digest([5; 32]),
            rule: BodyRuleId::new("test.consensus-invalid"),
        };

        assert!(store
            .set_body_validation_state(target.hash, invalid.clone())
            .expect("the first consensus-invalid result is authoritative"));
        assert!(!store
            .set_body_validation_state(target.hash, invalid)
            .expect("identical invalid evidence is idempotent"));
        let target_node = store
            .header_node(target.hash)
            .expect("the target remains retained");
        assert!(!target_node.is_eligible());
        assert!(target_node.eligibility.direct_reasons.is_empty());
        assert_eq!(
            store
                .header_node(descendant.hash)
                .expect("the descendant remains retained")
                .eligibility
                .inherited_from,
            Some(target.hash)
        );

        assert_eq!(
            store.set_body_validation_state(
                target.hash,
                BodyValidationState::ConsensusInvalid {
                    evidence: EvidenceId::from_digest([6; 32]),
                    rule: BodyRuleId::new("test.conflicting-invalid"),
                },
            ),
            Err(GraphError::PermanentBodyInvalidity(target.hash))
        );
        assert_eq!(
            store.set_body_validation_state(target.hash, BodyValidationState::Unknown),
            Err(GraphError::PermanentBodyInvalidity(target.hash))
        );
    }

    #[test]
    fn reconstruction_rejects_duplicate_disconnected_and_forged_nodes() {
        let mut store = anchor_store();
        let anchor = store.finalized_frontier();
        let child = insert_child(&mut store, anchor.hash, 1);
        let nodes: Vec<_> = store.header_nodes().cloned().collect();

        let mut duplicate = nodes.clone();
        duplicate.push(
            store
                .header_node(child.hash)
                .expect("the child is retained")
                .clone(),
        );
        assert!(matches!(
            MemHeaderStore::reconstruct(HeaderGraphReconstruction::new(anchor, duplicate, [])),
            Err(GraphError::DuplicateHeaderNode(hash)) if hash == child.hash
        ));

        let mut disconnected = nodes.clone();
        disconnected
            .iter_mut()
            .find(|node| node.hash == child.hash)
            .expect("the child row is present")
            .parent_hash = block::Hash([9; 32]);
        assert!(matches!(
            MemHeaderStore::reconstruct(HeaderGraphReconstruction::new(anchor, disconnected, [])),
            Err(GraphError::InvalidHeaderNode { header, invariant: HeaderNodeInvariant::CanonicalParentHash })
                if header == child.hash
        ));

        let mut forged_work = nodes.clone();
        forged_work
            .iter_mut()
            .find(|node| node.hash == child.hash)
            .expect("the child row is present")
            .block_work = Work::zero();
        assert!(matches!(
            MemHeaderStore::reconstruct(HeaderGraphReconstruction::new(anchor, forged_work, [])),
            Err(GraphError::InvalidHeaderNode { header, invariant: HeaderNodeInvariant::CanonicalBlockWork })
                if header == child.hash
        ));

        let mut forged_coordinate = nodes;
        forged_coordinate
            .iter_mut()
            .find(|node| node.hash == child.hash)
            .expect("the child row is present")
            .work_coordinate = WorkCoordinate::new(anchor.hash, U256::MAX);
        assert!(matches!(
            MemHeaderStore::reconstruct(HeaderGraphReconstruction::new(
                anchor,
                forged_coordinate,
                [],
            )),
            Err(GraphError::InvalidHeaderNode { header, invariant: HeaderNodeInvariant::CumulativeWork })
                if header == child.hash
        ));
    }

    #[test]
    fn reconstruction_rejects_forged_canonical_header_hash() {
        let mut store = anchor_store();
        let anchor = store.finalized_frontier();
        let child = insert_child(&mut store, anchor.hash, 1);
        let mut nodes: Vec<_> = store.header_nodes().cloned().collect();
        let forged = nodes
            .iter_mut()
            .find(|node| node.hash == child.hash)
            .expect("the child row is present");

        Arc::make_mut(&mut forged.header).nonce = [9; 32].into();
        assert_ne!(forged.header.hash(), forged.hash);

        assert!(matches!(
            MemHeaderStore::reconstruct(HeaderGraphReconstruction::new(anchor, nodes, [])),
            Err(GraphError::InvalidHeaderNode {
                header,
                invariant: HeaderNodeInvariant::CanonicalHeaderHash,
            }) if header == child.hash
        ));
    }

    #[test]
    fn live_delta_application_trusts_established_header_hashes() {
        let mut store = anchor_store();
        let anchor = store.finalized_frontier();
        let child = insert_child(&mut store, anchor.hash, 1);
        let child_node = store
            .nodes
            .get_mut(&child.hash)
            .expect("the inserted child is retained");

        Arc::make_mut(&mut child_node.header).nonce = [9; 32].into();
        assert_ne!(child_node.header.hash(), child_node.hash);

        let mut updated_child = child_node.clone();
        updated_child.body_validation_state = BodyValidationState::CommitmentMatched;
        let mut delta = GraphDelta::empty(&store);
        delta.updated_header_nodes.push(updated_child);

        store
            .apply_delta(&delta)
            .expect("live graph projection trusts hashes established at admission");
        assert_eq!(
            store
                .header_node(child.hash)
                .expect("the projected child remains retained")
                .body_validation_state,
            BodyValidationState::CommitmentMatched
        );
    }

    #[test]
    fn sparse_delta_does_not_revalidate_unchanged_nodes() {
        let mut store = anchor_store();
        let anchor = store.finalized_frontier();
        let unchanged = insert_child(&mut store, anchor.hash, 1);
        let changed = insert_child(&mut store, anchor.hash, 2);

        store
            .nodes
            .get_mut(&unchanged.hash)
            .expect("the unchanged child is retained")
            .block_work = Work::zero();
        let mut updated_changed = store
            .nodes
            .get(&changed.hash)
            .expect("the changed child is retained")
            .clone();
        updated_changed.body_validation_state = BodyValidationState::CommitmentMatched;
        let mut delta = GraphDelta::empty(&store);
        delta.updated_header_nodes.push(updated_changed);

        store
            .apply_delta(&delta)
            .expect("live sparse application validates only delta-affected nodes");
        assert_eq!(
            store
                .header_node(changed.hash)
                .expect("the changed child remains retained")
                .body_validation_state,
            BodyValidationState::CommitmentMatched
        );
    }

    #[test]
    fn failed_eligibility_recomputation_is_transactional() {
        let mut store = anchor_store();
        let anchor = store.finalized_frontier();
        let first = insert_child(&mut store, anchor.hash, 1);
        let second = insert_child(&mut store, anchor.hash, 2);
        test_support::mutate_retained_header(&mut store, first.hash, |header_node| {
            header_node.eligibility.inherited_from = Some(anchor.hash);
        });
        test_support::mutate_retained_header(&mut store, second.hash, |header_node| {
            header_node.parent_hash = block::Hash([7; 32]);
        });
        let before_nodes = store.nodes.clone();
        let before_tips = store.eligible_header_tips.clone();

        assert!(matches!(
            store.recompute_all_header_eligibility(),
            Err(GraphError::UnknownParent { header, .. }) if header == second.hash
        ));
        assert_eq!(store.nodes, before_nodes);
        assert_eq!(store.eligible_header_tips, before_tips);
    }

    #[test]
    fn tombstoned_header_remains_invalid_after_retention_and_readmission() {
        let mut store = anchor_store();
        let anchor = store.finalized_frontier();
        let header = child(anchor.hash, 1);
        let hash = header.hash();
        let frontier = insert_child(&mut store, anchor.hash, 1);
        let evidence = EvidenceId::from_digest([8; 32]);
        let rule = BodyRuleId::new("test.permanent-tombstone");
        store
            .set_body_validation_state(
                hash,
                BodyValidationState::ConsensusInvalid {
                    evidence,
                    rule: rule.clone(),
                },
            )
            .expect("the consensus-invalid result is authoritative");
        store
            .remove_header_leaf(frontier.hash)
            .expect("retention removes the invalid leaf");

        store
            .insert(
                header,
                HeaderValidationState::Valid,
                [],
                BodyValidationState::Unknown,
            )
            .expect("the same canonical header is readmitted");
        let node = store
            .header_node(hash)
            .expect("the header is retained again");
        assert_eq!(
            node.body_validation_state,
            BodyValidationState::ConsensusInvalid { evidence, rule }
        );
        assert!(!node.is_eligible());
    }

    #[test]
    fn valid_insertion_lazily_rebases_after_coordinate_overflow() {
        let block = regtest_genesis_block();
        let work = block
            .header
            .difficulty_threshold
            .to_work()
            .expect("the fixture target has valid work");
        let anchor = Frontier::new(block::Height(0), block.hash());
        let cumulative = U256::MAX
            .checked_sub(work.as_u256())
            .expect("the fixture work is below the coordinate maximum");
        let mut store = MemHeaderStore::new(anchor, block.header.clone(), work, cumulative)
            .expect("the anchor coordinate is valid");
        let first = insert_child(&mut store, anchor.hash, 1);
        assert_eq!(
            store
                .header_node(first.hash)
                .expect("the first child is retained")
                .work_coordinate()
                .cumulative_work(),
            U256::MAX
        );

        let second = insert_child(&mut store, first.hash, 2);
        assert_eq!(
            store
                .header_node(anchor.hash)
                .expect("the anchor remains retained")
                .work_coordinate(),
            WorkCoordinate::new(anchor.hash, U256::zero())
        );
        assert_eq!(
            store
                .header_chain_score(second.hash)
                .expect("the score remains exact"),
            ChainScore::new(
                crate::SuffixWork::new(
                    work.as_u256()
                        .checked_add(work.as_u256())
                        .expect("two fixture work values fit"),
                ),
                second.hash,
            )
        );
    }

    #[test]
    fn sparse_rebase_rejects_an_omitted_surviving_node() {
        let block = regtest_genesis_block();
        let work = block
            .header
            .difficulty_threshold
            .to_work()
            .expect("the fixture target has valid work");
        let anchor = Frontier::new(block::Height(0), block.hash());
        let cumulative = U256::MAX
            .checked_sub(work.as_u256())
            .expect("the fixture work is below the coordinate maximum");
        let mut store = MemHeaderStore::new(anchor, block.header.clone(), work, cumulative)
            .expect("the anchor coordinate is valid");
        let child = insert_child(&mut store, anchor.hash, 1);
        let mut overlay = GraphOverlay::new(&store);
        overlay
            .rebase_work_coordinates_to_finalized_frontier()
            .expect("the complete sparse rebase is valid");
        let mut incomplete = overlay.delta();
        incomplete
            .updated_header_nodes
            .retain(|node| node.hash != child.hash);

        assert!(matches!(
            GraphOverlay::from_delta(&store, &incomplete),
            Err(GraphError::InvalidHeaderNode {
                header,
                invariant: HeaderNodeInvariant::WorkRebaseCoordinate,
            }) if header == child.hash
        ));
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
                let anchor = store.finalized_frontier();
                let (incumbent, competitor) = if competitor_first {
                    let competitor = insert_branch(&mut store, anchor, incumbent_depth + 1, 127);
                    let incumbent = insert_branch(&mut store, anchor, incumbent_depth, 0);
                    (incumbent, competitor)
                } else {
                    let incumbent = insert_branch(&mut store, anchor, incumbent_depth, 0);
                    assert_eq!(
                        store
                            .select_best_header_chain()
                            .expect("graph is coherent")
                            .0,
                        incumbent
                    );
                    let competitor = insert_branch(&mut store, anchor, incumbent_depth + 1, 127);
                    (incumbent, competitor)
                };
                assert_ne!(incumbent.hash, competitor.hash);
                assert_eq!(
                    store
                        .select_best_header_chain()
                        .expect("graph is coherent")
                        .0,
                    competitor,
                    "selection is anchored at finalized and independent of depth or arrival order"
                );
            }
        }
    }

    #[test]
    fn body_availability_does_not_override_header_work_or_mark_other_bodies() {
        let mut store = anchor_store();
        let anchor = store.finalized_frontier();
        let verified = child(anchor.hash, 41);
        let verified_hash = verified.hash();
        store
            .insert(
                verified,
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
            .set_body_validation_state(
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
            store
                .select_best_header_chain()
                .expect("graph is coherent")
                .0,
            unknown_tip
        );
        assert_eq!(
            store
                .header_node(verified_hash)
                .expect("retained")
                .body_validation_state,
            BodyValidationState::Verified {
                evidence: crate::EvidenceId::from_digest([4; 32])
            }
        );
        assert_eq!(
            store
                .header_node(unknown_tip.hash)
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
            let anchor = store.finalized_frontier();
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
                    store.insert(
                        header.clone(),
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
            prop_assert_eq!(store.select_best_header_chain().expect("graph is coherent").0.hash, expected);
        }


        #[test]
        fn arbitrary_graph_operations_match_an_independent_uncached_model(
            operations in prop::collection::vec((0_u8..5, any::<usize>()), 1..100),
        ) {
            let mut store = anchor_store();
            let anchor = store.header_node(store.finalized_frontier().hash).expect("anchor is retained").clone();
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
                            store.set_header_validation_state(target_hash, HeaderValidationState::DeferredUntil(until)).expect("target is retained");
                            model.nodes.get_mut(&target_hash).expect("target exists").validation = HeaderValidationState::DeferredUntil(until);
                        }
                    }
                    4 => {
                        if target_hash != model.anchor {
                            store.set_header_validation_state(target_hash, HeaderValidationState::Valid).expect("target is retained");
                            model.nodes.get_mut(&target_hash).expect("target exists").validation = HeaderValidationState::Valid;
                        }
                    }
                    _ => unreachable!("the generated operation kind is bounded"),
                }

                prop_assert_eq!(
                    store.select_best_header_chain().expect("graph is coherent").0.hash,
                    model.selected(),
                );
                prop_assert_eq!(store.eligible_header_tips(), uncached_eligible_header_tips(&store));
            }
        }
    }
}
