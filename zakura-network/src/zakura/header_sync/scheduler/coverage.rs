//! Generation- and branch-keyed forward coverage.

use std::{cmp::Ordering, collections::BTreeMap};

use zakura_chain::block;
use zakura_header_chain::{BranchId, Frontier, HeaderGeneration};

/// One exact branch-qualified inclusive height range.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct BranchRange {
    /// Exact anchor and target branch identity.
    pub branch: BranchId,
    /// Inclusive first covered height.
    pub start: block::Height,
    /// Inclusive last covered height.
    pub end: block::Height,
}

impl BranchRange {
    /// Construct an ordered inclusive range, rejecting reversed bounds.
    pub fn new(branch: BranchId, start: block::Height, end: block::Height) -> Option<Self> {
        (start <= end).then_some(Self { branch, start, end })
    }
}

/// Exact selected-header generation and branch coverage identity.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct CoverageKey {
    generation: HeaderGeneration,
    branch: BranchId,
}

impl CoverageKey {
    fn new(generation: HeaderGeneration, branch: BranchId) -> Self {
        Self { generation, branch }
    }
}

impl Ord for CoverageKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.generation
            .cmp(&other.generation)
            .then_with(|| self.branch.anchor_hash.0.cmp(&other.branch.anchor_hash.0))
            .then_with(|| {
                self.branch
                    .target_tip_hash
                    .0
                    .cmp(&other.branch.target_tip_hash.0)
            })
    }
}

impl PartialOrd for CoverageKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct HeightInterval {
    start: block::Height,
    end: block::Height,
}

/// Sorted, non-overlapping inclusive intervals.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct IntervalSet(Vec<HeightInterval>);

impl IntervalSet {
    fn insert(&mut self, mut interval: HeightInterval) {
        let mut merged = Vec::with_capacity(self.0.len().saturating_add(1));
        let mut inserted = false;
        for current in self.0.drain(..) {
            if current.end.0.saturating_add(1) < interval.start.0 {
                merged.push(current);
            } else if interval.end.0.saturating_add(1) < current.start.0 {
                if !inserted {
                    merged.push(interval);
                    inserted = true;
                }
                merged.push(current);
            } else {
                interval.start = interval.start.min(current.start);
                interval.end = interval.end.max(current.end);
            }
        }
        if !inserted {
            merged.push(interval);
        }
        self.0 = merged;
    }

    fn ends_at(&self, height: block::Height) -> bool {
        self.0.last().is_some_and(|interval| interval.end == height)
    }
}

/// Forward coverage that can never alias across a generation or branch.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::zakura::header_sync) struct CoverageMap(BTreeMap<CoverageKey, IntervalSet>);

impl CoverageMap {
    pub(in crate::zakura::header_sync) fn mark(
        &mut self,
        generation: HeaderGeneration,
        range: BranchRange,
    ) {
        self.0
            .entry(CoverageKey::new(generation, range.branch))
            .or_default()
            .insert(HeightInterval {
                start: range.start,
                end: range.end,
            });
    }

    /// Whether this exact generation and branch has admitted the claimed target height.
    pub(in crate::zakura::header_sync) fn covers_tip(
        &self,
        generation: HeaderGeneration,
        branch: BranchId,
        tip_height: block::Height,
    ) -> bool {
        self.0
            .get(&CoverageKey::new(generation, branch))
            .is_some_and(|intervals| intervals.ends_at(tip_height))
    }

    /// Retire all coverage not owned by the exact current generation and finalized anchor.
    pub(in crate::zakura::header_sync) fn retain_current(
        &mut self,
        generation: HeaderGeneration,
        finalized: Frontier,
    ) {
        self.0.retain(|key, _| {
            key.generation == generation && key.branch.anchor_hash == finalized.hash
        });
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.0.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(byte: u8) -> block::Hash {
        block::Hash([byte; 32])
    }

    fn branch(anchor: u8, target: u8) -> BranchId {
        BranchId::new(hash(anchor), hash(target))
    }

    fn range(branch: BranchId, start: u32, end: u32) -> BranchRange {
        BranchRange::new(branch, block::Height(start), block::Height(end))
            .expect("the fixture range is ordered")
    }

    #[test]
    fn intervals_merge_only_within_one_generation_and_exact_branch() {
        let generation = HeaderGeneration::new(4);
        let branch_a = branch(1, 2);
        let branch_b = branch(1, 3);
        let mut coverage = CoverageMap::default();
        coverage.mark(generation, range(branch_a, 11, 12));
        coverage.mark(generation, range(branch_a, 13, 15));

        assert!(coverage.covers_tip(generation, branch_a, block::Height(15)));
        assert!(!coverage.covers_tip(generation, branch_b, block::Height(15)));
        assert!(!coverage.covers_tip(HeaderGeneration::new(5), branch_a, block::Height(15)));
        assert_eq!(coverage.len(), 1);
        assert!(BranchRange::new(branch_a, block::Height(16), block::Height(15)).is_none());
    }

    #[test]
    fn aud_08_old_coverage_misses_until_the_exact_reset_branch_completes() {
        let old_generation = HeaderGeneration::new(4);
        let new_generation = HeaderGeneration::new(5);
        let old_branch = branch(1, 9);
        let mut coverage = CoverageMap::default();
        coverage.mark(old_generation, range(old_branch, 11, 20));

        for (target, height) in [(7, 15), (8, 20), (10, 25)] {
            let new_branch = branch(1, target);
            let mut reset_coverage = coverage.clone();
            reset_coverage.retain_current(
                new_generation,
                Frontier::new(block::Height(10), new_branch.anchor_hash),
            );

            assert_eq!(reset_coverage.len(), 0);
            assert!(!reset_coverage.covers_tip(new_generation, new_branch, block::Height(height)));

            reset_coverage.mark(new_generation, range(old_branch, 11, height));
            assert!(
                !reset_coverage.covers_tip(new_generation, new_branch, block::Height(height)),
                "height coverage on the old branch cannot alias the reset branch"
            );

            reset_coverage.mark(new_generation, range(new_branch, 11, height));
            assert!(
                reset_coverage.covers_tip(new_generation, new_branch, block::Height(height)),
                "coverage starts only after the new exact branch completes"
            );
        }

        coverage.retain_current(old_generation, Frontier::new(block::Height(12), hash(2)));
        assert_eq!(coverage.len(), 0, "an anchor change also retires coverage");
    }
}
