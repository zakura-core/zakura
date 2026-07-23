//! Pure ownership registry and the sole asynchronous completion gate.

use std::{collections::HashMap, num::NonZeroU64};

use crate::{EngineSnapshot, SourceId, WorkOwner};

/// Exact reason an asynchronous completion has no remaining authority.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum StaleReason {
    /// Durable state changed after the work was scheduled.
    StateVersion,
    /// Selected-header work generation changed.
    HeaderGeneration,
    /// Verified-body generation changed for work that depends on it.
    VerifiedGeneration,
    /// Finality changed the immutable branch anchor.
    BranchAnchor,
    /// No pending entry exists for this source/request pair.
    MissingOwner,
    /// The pending entry belongs to another branch, session, generation, or target.
    OwnerMismatch,
}

/// Result of the centralized ownership check.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum CompletionDecision {
    /// The completion still has exact current authority.
    Current,
    /// The completion is terminally stale and must have no effects.
    Stale(StaleReason),
}

/// Exact pending asynchronous owners, keyed by supplier and request identity.
#[derive(Clone, Debug, Default)]
pub struct PendingOwners(HashMap<(SourceId, NonZeroU64), WorkOwner>);

impl PendingOwners {
    /// Register one newly published request, returning any contradictory prior owner.
    pub fn insert(&mut self, source: SourceId, owner: WorkOwner) -> Option<WorkOwner> {
        self.0.insert((source, owner.request_id), owner)
    }

    /// Retire one exact source/request owner.
    pub fn remove(&mut self, source: SourceId, request_id: NonZeroU64) -> Option<WorkOwner> {
        self.0.remove(&(source, request_id))
    }

    /// Retire every request owned by one source, returning exact retired owners.
    pub fn remove_source(&mut self, source: SourceId) -> Vec<WorkOwner> {
        let keys: Vec<_> = self
            .0
            .keys()
            .filter(|(candidate, _)| *candidate == source)
            .copied()
            .collect();
        keys.into_iter()
            .filter_map(|key| self.0.remove(&key))
            .collect()
    }

    /// Retire owners invalidated by a committed transition before new scheduling.
    pub fn apply_retirement(
        &mut self,
        retired: &crate::RetiredWork,
        current: &EngineSnapshot,
    ) -> Vec<WorkOwner> {
        let keys: Vec<_> = self
            .0
            .iter()
            .filter(|(_, owner)| {
                (retired.header_generation_changed
                    && owner.header_generation != current.header_generation)
                    || (retired.verified_generation_changed
                        && owner
                            .verified_generation
                            .is_some_and(|generation| generation != current.verified_generation))
                    || retired.owners.contains(owner)
                    || owner.state_version != current.state_version
                    || owner.branch.anchor_hash != current.frontiers.finalized.hash
            })
            .map(|(key, _)| *key)
            .collect();
        keys.into_iter()
            .filter_map(|key| self.0.remove(&key))
            .collect()
    }

    fn get(&self, source: SourceId, request_id: NonZeroU64) -> Option<WorkOwner> {
        self.0.get(&(source, request_id)).copied()
    }

    /// Number of exact pending owners.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether no asynchronous owner remains pending.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Sole pure decision point used before any completion effect.
#[derive(Copy, Clone, Debug, Default)]
pub struct CompletionGate;

impl CompletionGate {
    /// Compare every durable generation, branch anchor, source, session, request, and target fact.
    pub fn check(
        current: &EngineSnapshot,
        pending: &PendingOwners,
        source: SourceId,
        owner: &WorkOwner,
    ) -> CompletionDecision {
        if owner.state_version != current.state_version {
            return CompletionDecision::Stale(StaleReason::StateVersion);
        }
        if owner.header_generation != current.header_generation {
            return CompletionDecision::Stale(StaleReason::HeaderGeneration);
        }
        if owner
            .verified_generation
            .is_some_and(|generation| generation != current.verified_generation)
        {
            return CompletionDecision::Stale(StaleReason::VerifiedGeneration);
        }
        if owner.branch.anchor_hash != current.frontiers.finalized.hash {
            return CompletionDecision::Stale(StaleReason::BranchAnchor);
        }
        match pending.get(source, owner.request_id) {
            None => CompletionDecision::Stale(StaleReason::MissingOwner),
            Some(pending_owner) if pending_owner != *owner => {
                CompletionDecision::Stale(StaleReason::OwnerMismatch)
            }
            Some(_) => CompletionDecision::Current,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use zakura_chain::{block, work::difficulty::U256};

    use super::*;
    use crate::{
        AlarmSet, BranchId, ChainScore, EngineMode, Frontier, FrontierSet, HeaderGeneration,
        StateVersion, SuffixWork, VerifiedGeneration,
    };

    fn snapshot() -> EngineSnapshot {
        let anchor = Frontier::new(block::Height(10), block::Hash([1; 32]));
        EngineSnapshot {
            mode: EngineMode::Integrated,
            state_version: StateVersion::new(7),
            header_generation: HeaderGeneration::new(8),
            verified_generation: VerifiedGeneration::new(9),
            frontiers: FrontierSet {
                finalized: anchor,
                header_best: anchor,
                verified_best: anchor,
            },
            header_best_score: ChainScore::new(SuffixWork::new(U256::zero()), anchor.hash),
            oldest_retained_height: anchor.height,
            alarms: AlarmSet::default(),
        }
    }

    fn owner(snapshot: &EngineSnapshot) -> WorkOwner {
        WorkOwner {
            state_version: snapshot.state_version,
            header_generation: snapshot.header_generation,
            verified_generation: Some(snapshot.verified_generation),
            branch: BranchId::new(snapshot.frontiers.finalized.hash, block::Hash([2; 32])),
            session_id: 11,
            request_id: NonZeroU64::new(12).expect("twelve is nonzero"),
        }
    }

    #[derive(Debug, Default, Eq, PartialEq)]
    struct NoEffectsProbe {
        frontier_writes: usize,
        coverage_writes: usize,
        retry_writes: usize,
        repair_writes: usize,
        scheduler_writes: usize,
        publication_writes: usize,
        body_task_writes: usize,
        peer_score_writes: usize,
    }

    fn probe_completion(
        current: &EngineSnapshot,
        pending: &PendingOwners,
        source: SourceId,
        owner: &WorkOwner,
    ) -> (CompletionDecision, NoEffectsProbe) {
        let decision = CompletionGate::check(current, pending, source, owner);
        let mut probe = NoEffectsProbe::default();
        if decision == CompletionDecision::Current {
            probe.frontier_writes += 1;
            probe.coverage_writes += 1;
            probe.retry_writes += 1;
            probe.repair_writes += 1;
            probe.scheduler_writes += 1;
            probe.publication_writes += 1;
            probe.body_task_writes += 1;
            probe.peer_score_writes += 1;
        }
        (decision, probe)
    }

    #[test]
    fn every_generation_branch_session_request_and_pending_mismatch_is_stale() {
        let current = snapshot();
        let source = SourceId::from_digest([3; 32]);
        let expected = owner(&current);
        let mut pending = PendingOwners::default();
        assert_eq!(pending.insert(source, expected), None);
        assert_eq!(
            CompletionGate::check(&current, &pending, source, &expected),
            CompletionDecision::Current
        );

        let mut changed = current.clone();
        changed.state_version = StateVersion::new(10);
        assert_eq!(
            CompletionGate::check(&changed, &pending, source, &expected),
            CompletionDecision::Stale(StaleReason::StateVersion)
        );
        changed = current.clone();
        changed.header_generation = HeaderGeneration::new(10);
        assert_eq!(
            CompletionGate::check(&changed, &pending, source, &expected),
            CompletionDecision::Stale(StaleReason::HeaderGeneration)
        );
        changed = current.clone();
        changed.verified_generation = VerifiedGeneration::new(10);
        assert_eq!(
            CompletionGate::check(&changed, &pending, source, &expected),
            CompletionDecision::Stale(StaleReason::VerifiedGeneration)
        );
        changed = current.clone();
        changed.frontiers.finalized.hash = block::Hash([4; 32]);
        assert_eq!(
            CompletionGate::check(&changed, &pending, source, &expected),
            CompletionDecision::Stale(StaleReason::BranchAnchor)
        );

        let mut contradictory = expected;
        contradictory.session_id = 99;
        pending.insert(source, contradictory);
        assert_eq!(
            CompletionGate::check(&current, &pending, source, &expected),
            CompletionDecision::Stale(StaleReason::OwnerMismatch)
        );
        pending.remove(source, expected.request_id);
        assert_eq!(
            CompletionGate::check(&current, &pending, source, &expected),
            CompletionDecision::Stale(StaleReason::MissingOwner)
        );
    }

    #[test]
    fn centralized_retirement_removes_generation_and_exact_owner_work() {
        let mut current = snapshot();
        let source = SourceId::from_digest([3; 32]);
        let old = owner(&current);
        let mut exact = old;
        exact.request_id = NonZeroU64::new(13).expect("thirteen is nonzero");
        let mut pending = PendingOwners::default();
        pending.insert(source, old);
        pending.insert(source, exact);
        current.state_version = StateVersion::new(8);
        current.header_generation = HeaderGeneration::new(9);
        let retired = crate::RetiredWork {
            header_generation_changed: true,
            verified_generation_changed: false,
            owners: vec![exact],
        };
        let removed = pending.apply_retirement(&retired, &current);
        assert_eq!(removed.len(), 2);
        assert!(pending.is_empty());
    }

    #[test]
    fn all_owner_mismatches_have_zero_effects() {
        let current = snapshot();
        let source = SourceId::from_digest([3; 32]);
        let expected = owner(&current);
        let mut pending = PendingOwners::default();
        pending.insert(source, expected);
        let (decision, live_probe) = probe_completion(&current, &pending, source, &expected);
        assert_eq!(decision, CompletionDecision::Current);
        assert_ne!(live_probe, NoEffectsProbe::default());

        let mut cases = Vec::new();
        let mut changed_snapshot = current.clone();
        changed_snapshot.state_version = StateVersion::new(10);
        cases.push((changed_snapshot, source, expected, pending.clone()));
        let mut changed_snapshot = current.clone();
        changed_snapshot.header_generation = HeaderGeneration::new(10);
        cases.push((changed_snapshot, source, expected, pending.clone()));
        let mut changed_snapshot = current.clone();
        changed_snapshot.verified_generation = VerifiedGeneration::new(10);
        cases.push((changed_snapshot, source, expected, pending.clone()));
        let mut changed_snapshot = current.clone();
        changed_snapshot.frontiers.finalized.hash = block::Hash([4; 32]);
        cases.push((changed_snapshot, source, expected, pending.clone()));

        let mut changed_owner = expected;
        changed_owner.branch.anchor_hash = block::Hash([4; 32]);
        cases.push((current.clone(), source, changed_owner, pending.clone()));
        let mut changed_owner = expected;
        changed_owner.branch.target_tip_hash = block::Hash([4; 32]);
        cases.push((current.clone(), source, changed_owner, pending.clone()));
        let mut changed_owner = expected;
        changed_owner.session_id = 99;
        cases.push((current.clone(), source, changed_owner, pending.clone()));
        let mut changed_owner = expected;
        changed_owner.request_id = NonZeroU64::new(99).expect("ninety-nine is nonzero");
        cases.push((current.clone(), source, changed_owner, pending.clone()));
        cases.push((
            current.clone(),
            SourceId::from_digest([4; 32]),
            expected,
            pending.clone(),
        ));
        cases.push((current.clone(), source, expected, PendingOwners::default()));

        for (case_current, case_source, case_owner, case_pending) in cases {
            let (decision, probe) =
                probe_completion(&case_current, &case_pending, case_source, &case_owner);
            assert!(matches!(decision, CompletionDecision::Stale(_)));
            assert_eq!(probe, NoEffectsProbe::default());
        }
    }
}
