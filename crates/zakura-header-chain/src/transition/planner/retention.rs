//! Deterministic pure retention and resource-eviction planning.

use std::collections::{BTreeSet, HashSet};

use zakura_chain::block;

use crate::{
    graph::{HeaderGraphEdit, HeaderGraphView},
    BodyValidationState, EngineLimits, Frontier, GraphError,
};

/// Deterministic result of enforcing DAG resource bounds.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct RetentionPlan {
    /// True when protected paths prevented enforcement of the node bound.
    pub(super) admission_refused: bool,
    /// True when integrated verification/finality must advance before admission can resume.
    pub(super) resource_stalled: bool,
    /// Structural work performed while enforcing retention.
    pub(super) work: RetentionWork,
}

/// Exact structural work performed by one retention attempt.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct RetentionWork {
    /// Retained nodes visited while building the shared protected-path union.
    pub(super) protected_path_visits: usize,
    /// Retained nodes visited while building eviction candidate indexes.
    pub(super) candidate_nodes_scanned: usize,
    /// Retained nodes removed by deterministic eviction.
    pub(super) evicted_nodes: usize,
    /// Graph-sized workspaces allocated by retention.
    pub(super) graph_workspaces: usize,
}

/// Enforce deterministic retention while protecting selected, verified, and context paths.
pub(super) fn enforce_retention<G: HeaderGraphEdit>(
    store: &mut G,
    selected_header_tip: Frontier,
    verified_header_tip: Frontier,
    validation_context_references: impl IntoIterator<Item = block::Hash>,
    limits: EngineLimits,
) -> Result<RetentionPlan, GraphError> {
    let over_tip_limit = store.view_eligible_header_tip_count() > limits.max_candidate_tips.get();
    let over_node_limit =
        store.view_header_node_count().saturating_sub(1) > limits.max_non_finalized_nodes.get();
    if !over_tip_limit && !over_node_limit {
        return Ok(RetentionPlan::default());
    }

    let mut plan = RetentionPlan::default();
    let mut protected_header_hashes = HashSet::new();
    plan.work.graph_workspaces = 1;
    plan.work.protected_path_visits += add_protected_header_path(
        store,
        selected_header_tip.hash,
        &mut protected_header_hashes,
    )?;
    plan.work.protected_path_visits += add_protected_header_path(
        store,
        verified_header_tip.hash,
        &mut protected_header_hashes,
    )?;
    for reference in validation_context_references {
        if !protected_header_hashes.contains(&reference)
            && store.view_header_node(reference).is_some()
        {
            plan.work.protected_path_visits +=
                add_protected_header_path(store, reference, &mut protected_header_hashes)?;
        }
    }

    if protected_header_hashes.len().saturating_sub(1) > limits.max_non_finalized_nodes.get() {
        return Ok(resource_stalled_plan(plan));
    }

    let mut candidates = RetentionCandidates::build(store)?;
    plan.work.candidate_nodes_scanned = candidates.header_nodes_scanned;
    plan.work.graph_workspaces = plan.work.graph_workspaces.saturating_add(1);
    let permanently_evicted = evict_permanently_ineligible(
        store,
        &protected_header_hashes,
        &mut candidates,
        &mut plan.work,
    )?;
    if store.view_eligible_header_tip_count() <= limits.max_candidate_tips.get()
        && store.view_header_node_count().saturating_sub(1) <= limits.max_non_finalized_nodes.get()
    {
        return Ok(plan);
    }

    if permanently_evicted {
        candidates = RetentionCandidates::build(store)?;
        plan.work.candidate_nodes_scanned = plan
            .work
            .candidate_nodes_scanned
            .saturating_add(candidates.header_nodes_scanned);
        plan.work.graph_workspaces = plan.work.graph_workspaces.saturating_add(1);
    }

    if store.view_eligible_header_tip_count() > limits.max_candidate_tips.get() {
        while store.view_eligible_header_tip_count() > limits.max_candidate_tips.get() {
            let Some(score) = candidates.eligible_header_tips.pop_first() else {
                return Ok(resource_stalled_plan(plan));
            };
            if protected_header_hashes.contains(&score.tip_hash) {
                continue;
            }
            if store.view_header_node(score.tip_hash).is_some() {
                evict_tip_branch(
                    store,
                    score.tip_hash,
                    &protected_header_hashes,
                    &mut plan.work,
                )?;
            }
        }
    }

    if store.view_header_node_count().saturating_sub(1) > limits.max_non_finalized_nodes.get() {
        while store.view_header_node_count().saturating_sub(1)
            > limits.max_non_finalized_nodes.get()
        {
            let Some(score) = candidates.header_leaves.pop_first() else {
                return Ok(resource_stalled_plan(plan));
            };
            let hash = score.tip_hash;
            if protected_header_hashes.contains(&hash) {
                continue;
            }
            if store.view_header_node(hash).is_some() {
                evict_tip_branch(store, hash, &protected_header_hashes, &mut plan.work)?;
            }
        }
    }

    Ok(plan)
}

fn resource_stalled_plan(mut plan: RetentionPlan) -> RetentionPlan {
    plan.admission_refused = true;
    plan.resource_stalled = true;
    plan
}

struct RetentionCandidates {
    permanently_ineligible_roots: BTreeSet<(block::Height, [u8; 32])>,
    eligible_header_tips: BTreeSet<crate::ChainScore>,
    header_leaves: BTreeSet<crate::ChainScore>,
    header_nodes_scanned: usize,
}

impl RetentionCandidates {
    fn build<G: HeaderGraphView>(store: &G) -> Result<Self, GraphError> {
        let mut permanently_ineligible_roots = BTreeSet::new();
        let mut eligible_header_tips = BTreeSet::new();
        let mut header_leaves = BTreeSet::new();
        let mut header_nodes_scanned = 0usize;
        let mut error = None;
        store.visit_header_nodes(&mut |node| {
            if error.is_some() {
                return;
            }
            header_nodes_scanned = header_nodes_scanned.saturating_add(1);
            if node.eligibility.has_permanent_reason()
                || matches!(
                    node.body_validation_state,
                    BodyValidationState::ConsensusInvalid { .. }
                )
            {
                permanently_ineligible_roots.insert((node.height, node.hash.0));
            }
            let is_eligible_tip = store.view_is_eligible_header_tip(node.hash);
            let is_leaf = !store.view_header_has_children(node.hash);
            if is_eligible_tip || is_leaf {
                match store.view_header_chain_score(node.hash) {
                    Ok(score) => {
                        if is_eligible_tip {
                            eligible_header_tips.insert(score);
                        }
                        if is_leaf {
                            header_leaves.insert(score);
                        }
                    }
                    Err(graph_error) => error = Some(graph_error),
                }
            }
        });
        if let Some(error) = error {
            return Err(error);
        }
        Ok(Self {
            permanently_ineligible_roots,
            eligible_header_tips,
            header_leaves,
            header_nodes_scanned,
        })
    }
}

fn add_protected_header_path<G: HeaderGraphView>(
    store: &G,
    path_tip_hash: block::Hash,
    protected_header_hashes: &mut HashSet<block::Hash>,
) -> Result<usize, GraphError> {
    let mut header_hash = path_tip_hash;
    let mut visited_header_nodes = 0usize;
    loop {
        if protected_header_hashes.contains(&header_hash) {
            return Ok(visited_header_nodes);
        }
        visited_header_nodes = visited_header_nodes.saturating_add(1);
        let header_node = store
            .view_header_node(header_hash)
            .ok_or(GraphError::UnknownHeaderNode(header_hash))?;
        protected_header_hashes.insert(header_hash);
        if header_hash == store.view_finalized_frontier().hash {
            return Ok(visited_header_nodes);
        }
        header_hash = header_node.parent_hash;
    }
}

fn evict_permanently_ineligible<G: HeaderGraphEdit>(
    store: &mut G,
    protected_header_hashes: &HashSet<block::Hash>,
    candidates: &mut RetentionCandidates,
    retention_work: &mut RetentionWork,
) -> Result<bool, GraphError> {
    let mut evicted = false;
    while let Some((_, raw_hash)) = candidates.permanently_ineligible_roots.pop_first() {
        let root = block::Hash(raw_hash);
        if protected_header_hashes.contains(&root) {
            continue;
        }
        if store.view_header_node(root).is_none() {
            continue;
        }
        retention_work.graph_workspaces = retention_work.graph_workspaces.saturating_add(1);
        let mut descendants = subtree_postorder(store, root);
        for hash in descendants.drain(..) {
            store.edit_remove_header_leaf(hash)?;
            retention_work.evicted_nodes = retention_work.evicted_nodes.saturating_add(1);
            evicted = true;
        }
    }
    Ok(evicted)
}

fn subtree_postorder<G: HeaderGraphView>(store: &G, root: block::Hash) -> Vec<block::Hash> {
    let mut pending = vec![(root, false)];
    let mut result = Vec::new();
    while let Some((hash, visited)) = pending.pop() {
        if visited {
            result.push(hash);
        } else {
            pending.push((hash, true));
            pending.extend(
                store
                    .view_header_children(hash)
                    .into_iter()
                    .rev()
                    .map(|child| (child, false)),
            );
        }
    }
    result
}

fn evict_tip_branch<G: HeaderGraphEdit>(
    store: &mut G,
    branch_tip_hash: block::Hash,
    protected_header_hashes: &HashSet<block::Hash>,
    retention_work: &mut RetentionWork,
) -> Result<(), GraphError> {
    if protected_header_hashes.contains(&branch_tip_hash)
        || branch_tip_hash == store.view_finalized_frontier().hash
    {
        return Ok(());
    }
    let mut hash = store
        .view_header_node(branch_tip_hash)
        .ok_or(GraphError::UnknownHeaderNode(branch_tip_hash))?
        .parent_hash;
    retention_work.graph_workspaces = retention_work.graph_workspaces.saturating_add(1);
    for descendant in subtree_postorder(store, branch_tip_hash) {
        store.edit_remove_header_leaf(descendant)?;
        retention_work.evicted_nodes = retention_work.evicted_nodes.saturating_add(1);
    }

    loop {
        if protected_header_hashes.contains(&hash) || hash == store.view_finalized_frontier().hash {
            return Ok(());
        }
        let node = store
            .view_header_node(hash)
            .ok_or(GraphError::UnknownHeaderNode(hash))?;
        if !store.view_header_children(hash).is_empty() {
            return Ok(());
        }
        let parent = node.parent_hash;
        store.edit_remove_header_leaf(hash)?;
        retention_work.evicted_nodes = retention_work.evicted_nodes.saturating_add(1);
        if store.view_header_children(parent).is_empty() {
            hash = parent;
        } else {
            return Ok(());
        }
    }
}

#[cfg(feature = "test-support")]
#[path = "retention/benchmark_support.rs"]
mod benchmark_support;

#[cfg(feature = "test-support")]
pub use benchmark_support::{RetentionBenchmarkFixture, RetentionBenchmarkResult};

#[cfg(test)]
#[path = "retention/tests.rs"]
mod tests;
