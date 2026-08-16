//! Exact generation- and branch-keyed completed header targets.

use std::collections::HashSet;

use zakura_header_chain::{BranchId, Frontier, HeaderGeneration};

/// Exact selected-header generation and completed branch identity.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
struct CompletedTarget {
    generation: HeaderGeneration,
    branch: BranchId,
}

impl CompletedTarget {
    fn new(generation: HeaderGeneration, branch: BranchId) -> Self {
        Self { generation, branch }
    }
}

/// Complete atomic targets that can never alias across a generation or branch.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::zakura::header_sync) struct CompletedHeaderTargets(HashSet<CompletedTarget>);

impl CompletedHeaderTargets {
    pub(in crate::zakura::header_sync) fn mark(
        &mut self,
        generation: HeaderGeneration,
        branch: BranchId,
    ) {
        self.0.insert(CompletedTarget::new(generation, branch));
    }

    /// Whether this exact generation and branch was completely admitted.
    pub(in crate::zakura::header_sync) fn contains(
        &self,
        generation: HeaderGeneration,
        branch: BranchId,
    ) -> bool {
        self.0.contains(&CompletedTarget::new(generation, branch))
    }

    /// Retire completed targets not owned by the current generation and finalized anchor.
    pub(in crate::zakura::header_sync) fn retain_current(
        &mut self,
        generation: HeaderGeneration,
        finalized: Frontier,
    ) {
        self.0
            .retain(|key| key.generation == generation && key.branch.anchor_hash == finalized.hash);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.0.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::block;

    fn hash(byte: u8) -> block::Hash {
        block::Hash([byte; 32])
    }

    fn branch(anchor: u8, target: u8) -> BranchId {
        BranchId::new(hash(anchor), hash(target))
    }

    #[test]
    fn completion_is_keyed_only_by_generation_and_exact_branch() {
        let generation = HeaderGeneration::new(4);
        let branch_a = branch(1, 2);
        let branch_b = branch(1, 3);
        let mut completed = CompletedHeaderTargets::default();
        completed.mark(generation, branch_a);
        completed.mark(generation, branch_a);

        assert!(completed.contains(generation, branch_a));
        assert!(!completed.contains(generation, branch_b));
        assert!(!completed.contains(HeaderGeneration::new(5), branch_a));
        assert_eq!(completed.len(), 1);
    }

    #[test]
    // The completion key includes the generation and branch identity.
    // A changed value prevents reuse until the new target completes.
    fn completed_target_is_scoped_to_exact_generation_and_branch() {
        let old_generation = HeaderGeneration::new(4);
        let new_generation = HeaderGeneration::new(5);
        let old_branch = branch(1, 9);
        let mut completed = CompletedHeaderTargets::default();
        completed.mark(old_generation, old_branch);

        for target in [7, 8, 10] {
            let new_branch = branch(1, target);
            let mut reset_completed = completed.clone();
            reset_completed.retain_current(
                new_generation,
                Frontier::new(block::Height(10), new_branch.anchor_hash),
            );

            assert_eq!(reset_completed.len(), 0);
            assert!(!reset_completed.contains(new_generation, new_branch));

            reset_completed.mark(new_generation, old_branch);
            assert!(
                !reset_completed.contains(new_generation, new_branch),
                "completion on the old branch cannot alias the reset branch"
            );

            reset_completed.mark(new_generation, new_branch);
            assert!(
                reset_completed.contains(new_generation, new_branch),
                "completion starts only after the new exact branch is admitted"
            );
        }

        completed.retain_current(old_generation, Frontier::new(block::Height(12), hash(2)));
        assert_eq!(
            completed.len(),
            0,
            "an anchor change also retires completed targets"
        );
    }

    #[test]
    fn generation_change_conservatively_reuses_no_completed_targets() {
        let old_generation = HeaderGeneration::new(4);
        let new_generation = HeaderGeneration::new(5);
        let selected = branch(1, 9);
        let finalized = Frontier::new(block::Height(10), selected.anchor_hash);
        let mut completed = CompletedHeaderTargets::default();
        completed.mark(old_generation, selected);
        assert_eq!(completed.len(), 1);

        completed.retain_current(new_generation, finalized);

        assert_eq!(
            completed.len(),
            0,
            "without an authenticated reuse proof, generation retirement drops every target"
        );
        assert!(!completed.contains(new_generation, selected));
    }
}
