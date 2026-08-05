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
