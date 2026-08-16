//! Deterministic pure retention and resource-eviction planning.

use std::collections::HashSet;

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
}

/// Enforce deterministic retention while protecting selected, verified, and context paths.
pub(super) fn enforce_retention<G: HeaderGraphEdit>(
    store: &mut G,
    header_best: Frontier,
    verified_best: Frontier,
    protect_all_verified_body_paths: bool,
    validation_context_references: impl IntoIterator<Item = block::Hash>,
    limits: EngineLimits,
) -> Result<RetentionPlan, GraphError> {
    let over_tip_limit = store.view_eligible_header_tips().len() > limits.max_candidate_tips.get();
    let over_node_limit =
        store.view_header_node_count().saturating_sub(1) > limits.max_non_finalized_nodes.get();
    if !over_tip_limit && !over_node_limit {
        return Ok(RetentionPlan::default());
    }

    let mut protected = HashSet::new();
    protect_path(store, header_best.hash, &mut protected)?;
    protect_path(store, verified_best.hash, &mut protected)?;
    if protect_all_verified_body_paths {
        let verified_body_hashes = store
            .view_header_nodes()
            .into_iter()
            .filter_map(|node| {
                matches!(
                    node.body_validation_state,
                    BodyValidationState::Verified { .. }
                )
                .then_some(node.hash)
            })
            .collect::<Vec<_>>();
        for hash in verified_body_hashes {
            protect_path(store, hash, &mut protected)?;
        }
    }
    for reference in validation_context_references {
        if !protected.contains(&reference) && store.view_header_node(reference).is_some() {
            protect_path(store, reference, &mut protected)?;
        }
    }

    let plan = RetentionPlan::default();
    evict_permanently_ineligible(store, &protected)?;
    if store.view_eligible_header_tips().len() <= limits.max_candidate_tips.get()
        && store.view_header_node_count().saturating_sub(1) <= limits.max_non_finalized_nodes.get()
    {
        return Ok(plan);
    }

    if store.view_eligible_header_tips().len() > limits.max_candidate_tips.get() {
        let mut eligible_candidates = unprotected_eligible_header_tips(store, &protected)?;
        while store.view_eligible_header_tips().len() > limits.max_candidate_tips.get() {
            let Some(tip) = eligible_candidates.pop() else {
                return Ok(stalled(plan));
            };
            if store.view_header_node(tip.hash).is_some() {
                evict_tip_branch(store, tip.hash, &protected)?;
            }
        }
    }

    if store.view_header_node_count().saturating_sub(1) > limits.max_non_finalized_nodes.get() {
        let mut leaf_candidates = unprotected_leaves(store, &protected)?;
        while store.view_header_node_count().saturating_sub(1)
            > limits.max_non_finalized_nodes.get()
        {
            let Some(hash) = leaf_candidates.pop() else {
                return Ok(stalled(plan));
            };
            if store.view_header_node(hash).is_some() {
                evict_tip_branch(store, hash, &protected)?;
            }
        }
    }

    Ok(plan)
}

fn stalled(mut plan: RetentionPlan) -> RetentionPlan {
    plan.admission_refused = true;
    plan.resource_stalled = true;
    plan
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
        let node = store
            .view_header_node(hash)
            .ok_or(GraphError::UnknownHeaderNode(hash))?;
        protected.insert(hash);
        if hash == store.view_finalized_frontier().hash {
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
        .view_retained_header_hashes()
        .into_iter()
        .filter(|hash| {
            !protected.contains(hash)
                && store.view_header_node(*hash).is_some_and(|node| {
                    node.eligibility.has_permanent_reason()
                        || matches!(
                            node.body_validation_state,
                            BodyValidationState::ConsensusInvalid { .. }
                        )
                })
        })
        .collect();
    roots.sort_unstable_by_key(|hash| {
        let node = store
            .view_header_node(*hash)
            .expect("permanent roots were read from retained nodes");
        (node.height, hash.0)
    });
    for root in roots {
        if store.view_header_node(root).is_none() {
            continue;
        }
        let mut descendants = subtree_postorder(store, root);
        for hash in descendants.drain(..) {
            store.edit_remove_header_leaf(hash)?;
        }
    }
    Ok(())
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

fn unprotected_eligible_header_tips<G: HeaderGraphView>(
    store: &G,
    protected: &HashSet<block::Hash>,
) -> Result<Vec<Frontier>, GraphError> {
    let mut candidates: Vec<_> = store
        .view_eligible_header_tips()
        .into_iter()
        .filter(|tip| !protected.contains(&tip.hash))
        .map(|tip| Ok((store.view_header_chain_score(tip.hash)?, tip)))
        .collect::<Result<_, GraphError>>()?;
    candidates.sort_unstable_by_key(|(score, _)| std::cmp::Reverse(*score));
    Ok(candidates.into_iter().map(|(_, tip)| tip).collect())
}

fn unprotected_leaves<G: HeaderGraphView>(
    store: &G,
    protected: &HashSet<block::Hash>,
) -> Result<Vec<block::Hash>, GraphError> {
    let mut candidates: Vec<_> = store
        .view_retained_header_hashes()
        .into_iter()
        .filter(|hash| !protected.contains(hash) && store.view_header_children(*hash).is_empty())
        .map(|hash| Ok((store.view_header_chain_score(hash)?, hash)))
        .collect::<Result<_, GraphError>>()?;
    candidates.sort_unstable_by_key(|(score, _)| std::cmp::Reverse(*score));
    Ok(candidates.into_iter().map(|(_, hash)| hash).collect())
}

fn evict_tip_branch<G: HeaderGraphEdit>(
    store: &mut G,
    root: block::Hash,
    protected: &HashSet<block::Hash>,
) -> Result<(), GraphError> {
    if protected.contains(&root) || root == store.view_finalized_frontier().hash {
        return Ok(());
    }
    let mut hash = store
        .view_header_node(root)
        .ok_or(GraphError::UnknownHeaderNode(root))?
        .parent_hash;
    for descendant in subtree_postorder(store, root) {
        store.edit_remove_header_leaf(descendant)?;
    }

    loop {
        if protected.contains(&hash) || hash == store.view_finalized_frontier().hash {
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
        if store.view_header_children(parent).is_empty() {
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
    use crate::{
        BodyValidationState, EligibilityReason, HeaderValidationState, InsertResult, MemHeaderStore,
    };
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
        match store
            .insert(
                header,
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
        let anchor = store.finalized_frontier();
        let tips: Vec<_> = (1..=12)
            .map(|seed| insert(&mut store, anchor.hash, seed, []))
            .collect();
        let header_best = store
            .select_best_header_chain()
            .expect("graph is coherent")
            .0;
        let mut expected: Vec<_> = tips
            .iter()
            .copied()
            .filter(|tip| *tip != header_best)
            .collect();
        expected.sort_unstable_by_key(|tip| {
            store
                .header_chain_score(tip.hash)
                .expect("retained")
                .tip_hash
                .0
        });

        let plan = enforce_retention(&mut store, header_best, anchor, true, [], limits(10, 100))
            .expect("retention succeeds");
        assert!(store.header_node(expected[0].hash).is_none());
        assert!(store.header_node(expected[1].hash).is_none());
        assert_eq!(store.eligible_header_tips().len(), 10);
        assert!(store.header_node(header_best.hash).is_some());
        assert!(!plan.admission_refused);

        let reacquired_seed = (1..=12)
            .find(|seed| {
                let mut header = *regtest_genesis_block().header;
                header.previous_block_hash = anchor.hash;
                header.nonce = [*seed; 32].into();
                header.hash() == expected[0].hash
            })
            .expect("the evicted fixture tip has a seed");
        let reacquired = insert(&mut store, anchor.hash, reacquired_seed, []);
        assert_eq!(reacquired.hash, expected[0].hash);
        assert!(store
            .header_node(reacquired.hash)
            .expect("reacquired")
            .is_eligible());
    }

    #[test]
    fn eligible_tip_with_deferred_descendant_is_evicted_without_stalling() {
        let mut store = store();
        let anchor = store.finalized_frontier();
        let tips: Vec<_> = (1..=12)
            .map(|seed| insert(&mut store, anchor.hash, seed, []))
            .collect();
        let header_best = store
            .select_best_header_chain()
            .expect("graph is coherent")
            .0;
        let victim = tips
            .iter()
            .copied()
            .filter(|tip| *tip != header_best)
            .min_by_key(|tip| store.header_chain_score(tip.hash).expect("retained"))
            .expect("one unprotected tip exists");
        let deferred = insert(&mut store, victim.hash, 13, []);
        store
            .set_header_validation_state(
                deferred.hash,
                HeaderValidationState::DeferredUntil(
                    regtest_genesis_block().header.time + chrono::Duration::hours(3),
                ),
            )
            .expect("the deferred descendant is retained");

        assert!(store
            .eligible_header_tips()
            .iter()
            .any(|tip| tip.hash == victim.hash));
        assert!(!store
            .header_node(deferred.hash)
            .expect("the deferred child is retained")
            .is_eligible());

        let plan = enforce_retention(&mut store, header_best, anchor, true, [], limits(10, 100))
            .expect("retention evicts the ineligible descendant subtree");

        assert!(store.header_node(deferred.hash).is_none());
        assert!(store.header_node(victim.hash).is_none());
        assert_eq!(store.eligible_header_tips().len(), 10);
        assert!(!plan.admission_refused);
    }

    #[test]
    fn permanent_subtrees_are_evicted_first_only_under_pressure() {
        let mut store = store();
        let anchor = store.finalized_frontier();
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

        enforce_retention(&mut store, selected, anchor, true, [], limits(10, 2))
            .expect("permanent subtree frees capacity");
        assert!(store.header_node(permanent.hash).is_none());
        assert!(store.header_node(selected.hash).is_some());
        assert!(store.header_node(spare.hash).is_some());
    }

    #[test]
    fn retention_below_both_limits_has_no_graph_work_or_effects() {
        let mut store = store();
        let anchor = store.finalized_frontier();
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
        let before_header_node_count = store.header_node_count();
        let before_tips = store.eligible_header_tips();
        let before_permanent = store.header_node(permanent.hash).cloned();
        let before_selected = store.header_node(selected.hash).cloned();

        let plan = enforce_retention(&mut store, selected, anchor, true, [], limits(10, 10))
            .expect("a graph below both limits needs no eviction planning");

        assert_eq!(plan, RetentionPlan::default());
        assert_eq!(store.header_node_count(), before_header_node_count);
        assert_eq!(store.eligible_header_tips(), before_tips);
        assert_eq!(store.header_node(permanent.hash), before_permanent.as_ref());
        assert_eq!(store.header_node(selected.hash), before_selected.as_ref());
    }

    #[test]
    fn protected_paths_and_context_references_fail_closed_under_node_pressure() {
        let mut store = store();
        let anchor = store.finalized_frontier();
        let first = insert(&mut store, anchor.hash, 1, []);
        let selected = insert(&mut store, first.hash, 2, []);

        let plan = enforce_retention(
            &mut store,
            selected,
            anchor,
            true,
            [first.hash],
            limits(10, 1),
        )
        .expect("retention returns a typed refusal");
        assert!(plan.admission_refused);
        assert!(plan.resource_stalled);
        assert!(store.header_node(selected.hash).is_some());
    }

    #[test]
    fn verified_body_branch_survives_independent_header_retention() {
        let mut store = store();
        let anchor = store.finalized_frontier();
        let verified_body = insert(&mut store, anchor.hash, 1, []);
        store
            .set_body_validation_state(
                verified_body.hash,
                BodyValidationState::Verified {
                    evidence: crate::EvidenceId::from_digest([0x51; 32]),
                },
            )
            .expect("the full-state side branch becomes verified");
        let selected_parent = insert(&mut store, anchor.hash, 2, []);
        let selected = insert(&mut store, selected_parent.hash, 3, []);
        let header_only = insert(&mut store, anchor.hash, 4, []);

        let plan = enforce_retention(&mut store, selected, selected, true, [], limits(2, 100))
            .expect("retention evicts an unowned header-only branch");

        assert!(!plan.admission_refused);
        assert!(store.header_node(selected.hash).is_some());
        assert!(store.header_node(verified_body.hash).is_some());
        assert!(store.header_node(header_only.hash).is_none());
        assert_eq!(store.eligible_header_tips().len(), 2);
    }

    #[test]
    fn verified_body_paths_fail_closed_when_they_fill_retention_capacity() {
        let mut store = store();
        let anchor = store.finalized_frontier();
        let first = insert(&mut store, anchor.hash, 1, []);
        let verified_body = insert(&mut store, first.hash, 2, []);
        store
            .set_body_validation_state(
                verified_body.hash,
                BodyValidationState::Verified {
                    evidence: crate::EvidenceId::from_digest([0x52; 32]),
                },
            )
            .expect("the full-state branch becomes verified");
        let selected = insert(&mut store, anchor.hash, 3, []);

        let plan = enforce_retention(&mut store, selected, selected, true, [], limits(1, 1))
            .expect("retention reports pressure without deleting full-state-owned nodes");

        assert!(plan.admission_refused);
        assert!(plan.resource_stalled);
        assert!(store.header_node(first.hash).is_some());
        assert!(store.header_node(verified_body.hash).is_some());
        assert!(store.header_node(selected.hash).is_some());
    }

    #[test]
    fn authoritative_full_state_replacement_evicts_the_dropped_verified_branch() {
        let mut store = store();
        let anchor = store.finalized_frontier();
        let retained_tips = (1..=10)
            .map(|seed| insert(&mut store, anchor.hash, seed, []))
            .collect::<Vec<_>>();
        for tip in &retained_tips {
            store
                .set_body_validation_state(
                    tip.hash,
                    BodyValidationState::Verified {
                        evidence: crate::EvidenceId::from_digest(tip.hash.0),
                    },
                )
                .expect("the full-state branch becomes verified");
        }

        let replacement = insert(&mut store, anchor.hash, 11, []);
        store
            .set_body_validation_state(
                replacement.hash,
                BodyValidationState::Verified {
                    evidence: crate::EvidenceId::from_digest(replacement.hash.0),
                },
            )
            .expect("the replacement full-state branch becomes verified");
        let dropped = retained_tips[0];
        let staged_tips = retained_tips
            .iter()
            .copied()
            .skip(1)
            .map(|tip| tip.hash)
            .chain(std::iter::once(replacement.hash))
            .collect::<Vec<_>>();

        let plan = enforce_retention(
            &mut store,
            replacement,
            replacement,
            false,
            staged_tips,
            limits(10, 100),
        )
        .expect("the authoritative full-state fork set permits replacement");

        assert!(!plan.admission_refused);
        assert!(store.header_node(dropped.hash).is_none());
        assert!(store.header_node(replacement.hash).is_some());
        assert_eq!(store.eligible_header_tips().len(), 10);
    }

    #[test]
    fn overlapping_protected_paths_stop_at_their_first_protected_ancestor() {
        let mut store = store();
        let anchor = store.finalized_frontier();
        let first = insert(&mut store, anchor.hash, 1, []);
        let selected = insert(&mut store, first.hash, 2, []);
        let branch = insert(&mut store, first.hash, 3, []);
        let mut protected = HashSet::new();

        assert_eq!(
            protect_path(&store, selected.hash, &mut protected)
                .expect("the selected path is coherent"),
            3
        );
        assert_eq!(
            protect_path(&store, branch.hash, &mut protected)
                .expect("the branch joins the selected path"),
            2
        );
        assert_eq!(
            protect_path(&store, first.hash, &mut protected)
                .expect("the context reference is already protected"),
            1
        );
        assert!(protected.contains(&anchor.hash));
        assert!(protected.contains(&first.hash));
        assert!(protected.contains(&selected.hash));
        assert!(protected.contains(&branch.hash));
    }

    #[test]
    fn reference_pruned_before_retention_is_no_longer_protectable() {
        let mut store = store();
        let anchor = store.finalized_frontier();

        let plan = enforce_retention(
            &mut store,
            anchor,
            anchor,
            true,
            [block::Hash([0x91; 32])],
            limits(10, 100),
        )
        .expect("a reference already pruned by finality is ignored");

        assert_eq!(plan, RetentionPlan::default());
    }

    #[test]
    fn exact_v1_node_boundary_refuses_to_evict_the_selected_path() {
        let mut store = store();
        let anchor = store.finalized_frontier();
        let mut selected = anchor;
        for offset in 0..=crate::MAX_NON_FINALIZED_NODES_V1 {
            let seed = u8::try_from(offset % 251).expect("the reduced test nonce fits in u8");
            selected = insert(&mut store, selected.hash, seed, []);
        }
        assert_eq!(
            store.header_node_count() - 1,
            crate::MAX_NON_FINALIZED_NODES_V1 + 1
        );

        let plan = enforce_retention(&mut store, selected, anchor, true, [], EngineLimits::v1())
            .expect("the exact boundary produces a typed refusal");
        assert!(plan.admission_refused);
        assert!(plan.resource_stalled);
        assert!(store.header_node(selected.hash).is_some());
    }
}
