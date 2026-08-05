//! Deterministic pure retention and resource-eviction planning.

use std::collections::HashSet;

use zakura_chain::block;

use crate::{
    graph::{HeaderGraphEdit, HeaderGraphView},
    EngineLimits, Frontier, GraphError,
};

/// Deterministic result of enforcing DAG resource bounds.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RetentionPlan {
    /// True when protected paths prevented enforcement of the node bound.
    pub admission_refused: bool,
    /// True when integrated verification/finality must advance before admission can resume.
    pub resource_stalled: bool,
}

/// Enforce deterministic retention while protecting selected, verified, and context paths.
pub(crate) fn enforce_retention<G: HeaderGraphEdit>(
    store: &mut G,
    header_best: Frontier,
    verified_best: Frontier,
    validation_context_references: impl IntoIterator<Item = block::Hash>,
    limits: EngineLimits,
) -> Result<RetentionPlan, GraphError> {
    let mut protected = HashSet::new();
    protect_path(store, header_best.hash, &mut protected)?;
    protect_path(store, verified_best.hash, &mut protected)?;
    for reference in validation_context_references {
        if !protected.contains(&reference) && store.view_node(reference).is_some() {
            protect_path(store, reference, &mut protected)?;
        }
    }

    let mut plan = RetentionPlan::default();
    let under_pressure = store.view_eligible_tips().len() > limits.max_candidate_tips.get()
        || store.view_node_count().saturating_sub(1) > limits.max_non_finalized_nodes.get();
    if under_pressure {
        evict_permanently_ineligible(store, &protected)?;
    }

    while store.view_eligible_tips().len() > limits.max_candidate_tips.get() {
        let Some(tip) = lowest_unprotected_eligible_tip(store, &protected)? else {
            plan.admission_refused = true;
            plan.resource_stalled = true;
            return Ok(plan);
        };
        let previous_node_count = store.view_node_count();
        evict_tip_branch(store, tip.hash, &protected)?;
        if store.view_node_count() == previous_node_count {
            plan.admission_refused = true;
            plan.resource_stalled = true;
            return Ok(plan);
        }
    }

    while store.view_node_count().saturating_sub(1) > limits.max_non_finalized_nodes.get() {
        let tip = match lowest_unprotected_eligible_tip(store, &protected)? {
            Some(tip) => Some(tip.hash),
            None => lowest_unprotected_leaf(store, &protected)?,
        };
        let Some(tip) = tip else {
            plan.admission_refused = true;
            plan.resource_stalled = true;
            return Ok(plan);
        };
        let previous_node_count = store.view_node_count();
        evict_tip_branch(store, tip, &protected)?;
        if store.view_node_count() == previous_node_count {
            plan.admission_refused = true;
            plan.resource_stalled = true;
            return Ok(plan);
        }
    }

    Ok(plan)
}

fn protect_path<G: HeaderGraphView>(
    store: &G,
    tip: block::Hash,
    protected: &mut HashSet<block::Hash>,
) -> Result<usize, GraphError> {
    let mut hash = tip;
    let mut visited = 0usize;
    loop {
        visited = visited.saturating_add(1);
        if protected.contains(&hash) {
            return Ok(visited);
        }
        let node = store.view_node(hash).ok_or(GraphError::UnknownNode(hash))?;
        protected.insert(hash);
        if hash == store.view_finalized().hash {
            return Ok(visited);
        }
        hash = node.parent_hash;
    }
}

fn evict_permanently_ineligible<G: HeaderGraphEdit>(
    store: &mut G,
    protected: &HashSet<block::Hash>,
) -> Result<(), GraphError> {
    let mut roots: Vec<_> = store
        .view_retained_hashes()
        .into_iter()
        .filter(|hash| {
            !protected.contains(hash)
                && store
                    .view_node(*hash)
                    .is_some_and(|node| node.eligibility.has_permanent_reason())
        })
        .collect();
    roots.sort_unstable_by_key(|hash| {
        let node = store
            .view_node(*hash)
            .expect("permanent roots were read from retained nodes");
        (node.height, hash.0)
    });
    for root in roots {
        if store.view_node(root).is_none() || subtree_contains_protected(store, root, protected) {
            continue;
        }
        let mut descendants = subtree_postorder(store, root);
        for hash in descendants.drain(..) {
            store.edit_remove_leaf(hash)?;
        }
    }
    Ok(())
}

fn subtree_contains_protected<G: HeaderGraphView>(
    store: &G,
    root: block::Hash,
    protected: &HashSet<block::Hash>,
) -> bool {
    let mut pending = vec![root];
    while let Some(hash) = pending.pop() {
        if protected.contains(&hash) {
            return true;
        }
        pending.extend(store.view_children(hash));
    }
    false
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
                    .view_children(hash)
                    .into_iter()
                    .rev()
                    .map(|child| (child, false)),
            );
        }
    }
    result
}

fn lowest_unprotected_eligible_tip<G: HeaderGraphView>(
    store: &G,
    protected: &HashSet<block::Hash>,
) -> Result<Option<Frontier>, GraphError> {
    let mut candidates: Vec<_> = store
        .view_eligible_tips()
        .into_iter()
        .filter(|tip| {
            !protected.contains(&tip.hash)
                && !subtree_contains_protected(store, tip.hash, protected)
        })
        .map(|tip| Ok((store.view_score(tip.hash)?, tip)))
        .collect::<Result<_, GraphError>>()?;
    candidates.sort_unstable_by_key(|(score, _)| *score);
    Ok(candidates.first().map(|(_, tip)| *tip))
}

fn lowest_unprotected_leaf<G: HeaderGraphView>(
    store: &G,
    protected: &HashSet<block::Hash>,
) -> Result<Option<block::Hash>, GraphError> {
    let mut candidates: Vec<_> = store
        .view_retained_hashes()
        .into_iter()
        .filter(|hash| !protected.contains(hash) && store.view_children(*hash).is_empty())
        .map(|hash| Ok((store.view_score(hash)?, hash)))
        .collect::<Result<_, GraphError>>()?;
    candidates.sort_unstable_by_key(|(score, _)| *score);
    Ok(candidates.first().map(|(_, hash)| *hash))
}

fn evict_tip_branch<G: HeaderGraphEdit>(
    store: &mut G,
    root: block::Hash,
    protected: &HashSet<block::Hash>,
) -> Result<(), GraphError> {
    if protected.contains(&root)
        || root == store.view_finalized().hash
        || subtree_contains_protected(store, root, protected)
    {
        return Ok(());
    }
    let mut hash = store
        .view_node(root)
        .ok_or(GraphError::UnknownNode(root))?
        .parent_hash;
    for descendant in subtree_postorder(store, root) {
        store.edit_remove_leaf(descendant)?;
    }

    loop {
        if protected.contains(&hash) || hash == store.view_finalized().hash {
            return Ok(());
        }
        let node = store.view_node(hash).ok_or(GraphError::UnknownNode(hash))?;
        if !store.view_children(hash).is_empty() {
            return Ok(());
        }
        let parent = node.parent_hash;
        store.edit_remove_leaf(hash)?;
        if store.view_children(parent).is_empty() {
            hash = parent;
        } else {
            return Ok(());
        }
    }
}
