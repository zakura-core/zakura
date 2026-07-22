//! Deterministic pure retention and resource-eviction planning.

use std::collections::HashSet;

use zakura_chain::block;

use crate::{EngineLimits, Frontier, GraphError, MemHeaderStore};

/// Deterministic result of enforcing DAG resource bounds.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RetentionPlan {
    /// Nodes removed only for resource retention, never marked invalid.
    pub evicted: Vec<block::Hash>,
    /// True when protected paths prevented enforcement of the node bound.
    pub admission_refused: bool,
    /// True when integrated verification/finality must advance before admission can resume.
    pub resource_stalled: bool,
}

/// Enforce deterministic retention while protecting selected, verified, and context paths.
pub(crate) fn enforce_retention(
    store: &mut MemHeaderStore,
    header_best: Frontier,
    verified_best: Frontier,
    validation_context_references: impl IntoIterator<Item = block::Hash>,
    limits: EngineLimits,
) -> Result<RetentionPlan, GraphError> {
    let mut protected = HashSet::new();
    protect_path(store, header_best.hash, &mut protected)?;
    protect_path(store, verified_best.hash, &mut protected)?;
    for reference in validation_context_references {
        protect_path(store, reference, &mut protected)?;
    }

    let mut plan = RetentionPlan::default();
    let under_pressure = store.eligible_tips().len() > limits.max_candidate_tips.get()
        || store.len().saturating_sub(1) > limits.max_non_finalized_nodes.get();
    if under_pressure {
        evict_permanently_ineligible(store, &protected, &mut plan)?;
    }

    while store.eligible_tips().len() > limits.max_candidate_tips.get() {
        let Some(tip) = lowest_unprotected_eligible_tip(store, &protected)? else {
            plan.admission_refused = true;
            plan.resource_stalled = true;
            return Ok(plan);
        };
        evict_tip_branch(store, tip.hash, &protected, &mut plan)?;
    }

    while store.len().saturating_sub(1) > limits.max_non_finalized_nodes.get() {
        let tip = match lowest_unprotected_eligible_tip(store, &protected)? {
            Some(tip) => Some(tip.hash),
            None => lowest_unprotected_leaf(store, &protected)?,
        };
        let Some(tip) = tip else {
            plan.admission_refused = true;
            plan.resource_stalled = true;
            return Ok(plan);
        };
        evict_tip_branch(store, tip, &protected, &mut plan)?;
    }

    Ok(plan)
}

fn protect_path(
    store: &MemHeaderStore,
    tip: block::Hash,
    protected: &mut HashSet<block::Hash>,
) -> Result<(), GraphError> {
    let mut hash = tip;
    loop {
        let node = store.node(hash).ok_or(GraphError::UnknownNode(hash))?;
        protected.insert(hash);
        if hash == store.finalized().hash {
            return Ok(());
        }
        hash = node.parent_hash;
    }
}

fn evict_permanently_ineligible(
    store: &mut MemHeaderStore,
    protected: &HashSet<block::Hash>,
    plan: &mut RetentionPlan,
) -> Result<(), GraphError> {
    let mut roots: Vec<_> = store
        .retained_hashes()
        .filter(|hash| {
            !protected.contains(hash)
                && store
                    .node(*hash)
                    .is_some_and(|node| node.eligibility.has_permanent_reason())
        })
        .collect();
    roots.sort_unstable_by_key(|hash| {
        let node = store
            .node(*hash)
            .expect("permanent roots were read from retained nodes");
        (node.height, hash.0)
    });
    for root in roots {
        if store.node(root).is_none() || subtree_contains_protected(store, root, protected) {
            continue;
        }
        let mut descendants = subtree_postorder(store, root);
        for hash in descendants.drain(..) {
            store.remove_leaf(hash)?;
            plan.evicted.push(hash);
        }
    }
    Ok(())
}

fn subtree_contains_protected(
    store: &MemHeaderStore,
    root: block::Hash,
    protected: &HashSet<block::Hash>,
) -> bool {
    let mut pending = vec![root];
    while let Some(hash) = pending.pop() {
        if protected.contains(&hash) {
            return true;
        }
        pending.extend(store.children(hash));
    }
    false
}

fn subtree_postorder(store: &MemHeaderStore, root: block::Hash) -> Vec<block::Hash> {
    let mut pending = vec![(root, false)];
    let mut result = Vec::new();
    while let Some((hash, visited)) = pending.pop() {
        if visited {
            result.push(hash);
        } else {
            pending.push((hash, true));
            pending.extend(
                store
                    .children(hash)
                    .into_iter()
                    .rev()
                    .map(|child| (child, false)),
            );
        }
    }
    result
}

fn lowest_unprotected_eligible_tip(
    store: &MemHeaderStore,
    protected: &HashSet<block::Hash>,
) -> Result<Option<Frontier>, GraphError> {
    let mut candidates: Vec<_> = store
        .eligible_tips()
        .into_iter()
        .filter(|tip| !protected.contains(&tip.hash))
        .map(|tip| Ok((store.score(tip.hash)?, tip)))
        .collect::<Result<_, GraphError>>()?;
    candidates.sort_unstable_by_key(|(score, _)| *score);
    Ok(candidates.first().map(|(_, tip)| *tip))
}

fn lowest_unprotected_leaf(
    store: &MemHeaderStore,
    protected: &HashSet<block::Hash>,
) -> Result<Option<block::Hash>, GraphError> {
    let mut candidates: Vec<_> = store
        .retained_hashes()
        .filter(|hash| !protected.contains(hash) && store.children(*hash).is_empty())
        .map(|hash| Ok((store.score(hash)?, hash)))
        .collect::<Result<_, GraphError>>()?;
    candidates.sort_unstable_by_key(|(score, _)| *score);
    Ok(candidates.first().map(|(_, hash)| *hash))
}

fn evict_tip_branch(
    store: &mut MemHeaderStore,
    mut hash: block::Hash,
    protected: &HashSet<block::Hash>,
    plan: &mut RetentionPlan,
) -> Result<(), GraphError> {
    loop {
        if protected.contains(&hash) || hash == store.finalized().hash {
            return Ok(());
        }
        let node = store.node(hash).ok_or(GraphError::UnknownNode(hash))?;
        if !store.children(hash).is_empty() {
            return Ok(());
        }
        let parent = node.parent_hash;
        store.remove_leaf(hash)?;
        plan.evicted.push(hash);
        if store.children(parent).is_empty() {
            hash = parent;
        } else {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, sync::Arc};

    use super::*;
    use crate::{BodyValidationState, EligibilityReason, HeaderValidationState, InsertResult};
    use zakura_chain::block::genesis::regtest_genesis_block;

    fn store() -> MemHeaderStore {
        let block = regtest_genesis_block();
        let hash = block.hash();
        let work = block
            .header
            .difficulty_threshold
            .to_work()
            .expect("valid work");
        MemHeaderStore::new(
            Frontier::new(block::Height(0), hash),
            block.header.clone(),
            work,
            work.as_u256(),
        )
        .expect("fixture anchor matches")
    }

    fn insert(
        store: &mut MemHeaderStore,
        parent: block::Hash,
        seed: u8,
        reasons: impl IntoIterator<Item = EligibilityReason>,
    ) -> Frontier {
        let mut header = *regtest_genesis_block().header;
        header.previous_block_hash = parent;
        header.nonce = [seed; 32].into();
        let header = Arc::new(header);
        let work = header.difficulty_threshold.to_work().expect("valid work");
        match store
            .insert(
                header,
                work,
                HeaderValidationState::Valid,
                reasons,
                BodyValidationState::Unknown,
            )
            .expect("fixture parent is retained")
        {
            InsertResult::Inserted(frontier) | InsertResult::AlreadyPresent(frontier) => frontier,
        }
    }

    fn limits(tips: usize, nodes: usize) -> EngineLimits {
        EngineLimits {
            max_candidate_tips: NonZeroUsize::new(tips).expect("test limit is nonzero"),
            max_non_finalized_nodes: NonZeroUsize::new(nodes).expect("test limit is nonzero"),
            ..EngineLimits::v1()
        }
    }

    #[test]
    fn candidate_tip_eviction_is_lowest_work_then_smallest_raw_hash() {
        let mut store = store();
        let anchor = store.finalized();
        let tips: Vec<_> = (1..=12)
            .map(|seed| insert(&mut store, anchor.hash, seed, []))
            .collect();
        let header_best = store.select_header_best().expect("graph is coherent").0;
        let mut expected: Vec<_> = tips
            .iter()
            .copied()
            .filter(|tip| *tip != header_best)
            .collect();
        expected.sort_unstable_by_key(|tip| store.score(tip.hash).expect("retained").tip_hash.0);

        let plan = enforce_retention(&mut store, header_best, anchor, [], limits(10, 100))
            .expect("retention succeeds");
        assert_eq!(plan.evicted, vec![expected[0].hash, expected[1].hash]);
        assert_eq!(store.eligible_tips().len(), 10);
        assert!(store.node(header_best.hash).is_some());
        assert!(!plan.admission_refused);

        let reacquired_seed = (1..=12)
            .find(|seed| {
                let mut header = *regtest_genesis_block().header;
                header.previous_block_hash = anchor.hash;
                header.nonce = [*seed; 32].into();
                header.hash() == plan.evicted[0]
            })
            .expect("the evicted fixture tip has a seed");
        let reacquired = insert(&mut store, anchor.hash, reacquired_seed, []);
        assert_eq!(reacquired.hash, plan.evicted[0]);
        assert!(store
            .node(reacquired.hash)
            .expect("reacquired")
            .is_eligible());
    }

    #[test]
    fn permanent_subtrees_are_evicted_first_only_under_pressure() {
        let mut store = store();
        let anchor = store.finalized();
        let permanent = insert(
            &mut store,
            anchor.hash,
            1,
            [EligibilityReason::CheckpointConflict {
                height: block::Height(1),
                expected: block::Hash([9; 32]),
            }],
        );
        let selected = insert(&mut store, anchor.hash, 2, []);
        let spare = insert(&mut store, anchor.hash, 3, []);

        let plan = enforce_retention(&mut store, selected, anchor, [], limits(10, 2))
            .expect("permanent subtree frees capacity");
        assert_eq!(plan.evicted, vec![permanent.hash]);
        assert!(store.node(selected.hash).is_some());
        assert!(store.node(spare.hash).is_some());
    }

    #[test]
    fn protected_paths_and_context_references_fail_closed_under_node_pressure() {
        let mut store = store();
        let anchor = store.finalized();
        let first = insert(&mut store, anchor.hash, 1, []);
        let selected = insert(&mut store, first.hash, 2, []);

        let plan = enforce_retention(&mut store, selected, anchor, [first.hash], limits(10, 1))
            .expect("retention returns a typed refusal");
        assert!(plan.admission_refused);
        assert!(plan.resource_stalled);
        assert!(plan.evicted.is_empty());
        assert!(store.node(selected.hash).is_some());
    }

    #[test]
    fn exact_v1_node_boundary_refuses_to_evict_the_selected_path() {
        let mut store = store();
        let anchor = store.finalized();
        let mut selected = anchor;
        for offset in 0..=crate::MAX_NON_FINALIZED_NODES_V1 {
            let seed = u8::try_from(offset % 251).expect("the reduced test nonce fits in u8");
            selected = insert(&mut store, selected.hash, seed, []);
        }
        assert_eq!(store.len() - 1, crate::MAX_NON_FINALIZED_NODES_V1 + 1);

        let plan = enforce_retention(&mut store, selected, anchor, [], EngineLimits::v1())
            .expect("the exact boundary produces a typed refusal");
        assert!(plan.admission_refused);
        assert!(plan.resource_stalled);
        assert!(plan.evicted.is_empty());
        assert!(store.node(selected.hash).is_some());
    }
}
