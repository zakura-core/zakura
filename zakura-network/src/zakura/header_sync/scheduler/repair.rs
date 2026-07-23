//! Generation- and branch-owned auxiliary VCT repair work.

use std::collections::HashMap;

use thiserror::Error;
use zakura_header_chain::{EngineSnapshot, EvidenceId, SourceId, VctRepairContext, WorkOwner};

use super::coverage::BranchRange;
use crate::zakura::header_sync::HeaderEntry;

/// Exact phase of one auxiliary repair task.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RepairPhase {
    /// Work exists but has not been assigned to a peer.
    Scheduled,
    /// An exact supplier is returning the repair range.
    OnWire {
        /// Supplying peer.
        peer: SourceId,
    },
    /// The complete repair response is buffered.
    Buffered,
    /// Validated repair evidence is waiting for bounded state capacity.
    WaitingForCapacity,
    /// One exact evidence transition was submitted to state.
    StateDispatched {
        /// Stable auxiliary evidence transition identity.
        transition: EvidenceId,
    },
}

/// Invalid construction or phase transition for VCT repair work.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum RepairTaskError {
    /// Range and owner name different exact branches.
    #[error("repair range and owner branch identities differ")]
    BranchMismatch,
    /// A wire owner changed the durable repair scope.
    #[error("wire assignment changed the VCT repair scope")]
    ScopeMismatch,
    /// The resolved target is outside the exact one-height repair range.
    #[error("resolved VCT repair target is outside its exact range")]
    TargetMismatch,
    /// The requested phase edge is not part of the repair state machine.
    #[error("illegal VCT repair phase transition")]
    IllegalPhase,
    /// The bounded attempt counter reached `u8::MAX`.
    #[error("VCT repair attempt counter is exhausted")]
    AttemptsExhausted,
}

/// One branch-owned auxiliary repair task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VctRepairTask {
    /// Exact asynchronous owner fixed when work was scheduled.
    pub owner: WorkOwner,
    /// Exact branch-qualified repair range.
    pub range: BranchRange,
    /// Current bounded phase.
    pub phase: RepairPhase,
    /// Failed or abandoned on-wire attempts.
    pub attempts: u8,
    /// State-resolved selected request context, once available.
    pub context: Option<VctRepairContext>,
    /// Whether the exact state context read is currently outstanding.
    pub context_requested: bool,
    /// Exact complete wire entry retained until off-reactor preparation succeeds.
    pub entry: Option<HeaderEntry>,
    /// Sealed metadata-only insertion retained while state action capacity is unavailable.
    pub prepared: Option<Box<zakura_header_chain::InsertHeaders>>,
    /// Authenticated supplier retained after the on-wire phase.
    pub source: Option<SourceId>,
}

impl VctRepairTask {
    /// Construct scheduled work only when its range and owner name the same branch.
    pub fn new(owner: WorkOwner, range: BranchRange) -> Result<Self, RepairTaskError> {
        if owner.branch != range.branch {
            return Err(RepairTaskError::BranchMismatch);
        }
        Ok(Self {
            owner,
            range,
            phase: RepairPhase::Scheduled,
            attempts: 0,
            context: None,
            context_requested: false,
            entry: None,
            prepared: None,
            source: None,
        })
    }

    /// Record that one bounded state context read is outstanding.
    pub fn mark_context_requested(&mut self) -> Result<(), RepairTaskError> {
        if self.phase != RepairPhase::Scheduled || self.context.is_some() || self.context_requested
        {
            return Err(RepairTaskError::IllegalPhase);
        }
        self.context_requested = true;
        Ok(())
    }

    /// Attach a still-current exact selected request context.
    pub fn resolve(&mut self, context: VctRepairContext) -> Result<(), RepairTaskError> {
        if self.phase != RepairPhase::Scheduled || !self.context_requested {
            return Err(RepairTaskError::IllegalPhase);
        }
        if self.range.start != self.range.end || context.target.height != self.range.start {
            return Err(RepairTaskError::TargetMismatch);
        }
        self.context = Some(context);
        self.context_requested = false;
        Ok(())
    }

    /// Release one failed local context read for a later retry.
    pub fn context_unavailable(&mut self) -> Result<(), RepairTaskError> {
        if self.phase != RepairPhase::Scheduled || !self.context_requested {
            return Err(RepairTaskError::IllegalPhase);
        }
        self.context_requested = false;
        Ok(())
    }

    /// Bind a resolved task to the actual canonical stream request.
    pub fn assign(&mut self, owner: WorkOwner, peer: SourceId) -> Result<(), RepairTaskError> {
        if self.phase != RepairPhase::Scheduled || self.context.is_none() {
            return Err(RepairTaskError::IllegalPhase);
        }
        if owner.scope() != self.owner.scope() {
            return Err(RepairTaskError::ScopeMismatch);
        }
        self.owner = owner;
        self.source = Some(peer);
        self.phase = RepairPhase::OnWire { peer };
        Ok(())
    }

    /// Retain one exact complete response before off-reactor preparation.
    pub fn buffer(&mut self, entry: HeaderEntry) -> Result<(), RepairTaskError> {
        let Some(context) = self.context.as_ref() else {
            return Err(RepairTaskError::IllegalPhase);
        };
        if !matches!(self.phase, RepairPhase::OnWire { .. }) {
            return Err(RepairTaskError::IllegalPhase);
        }
        if entry.header.hash() != context.target.hash || entry.tree_aux.is_none() {
            return Err(RepairTaskError::TargetMismatch);
        }
        self.entry = Some(entry);
        self.phase = RepairPhase::Buffered;
        Ok(())
    }

    /// Retain a sealed selected-auxiliary insertion after preparation.
    pub fn seal(
        &mut self,
        insert: Box<zakura_header_chain::InsertHeaders>,
    ) -> Result<(), RepairTaskError> {
        if !matches!(
            self.phase,
            RepairPhase::Buffered | RepairPhase::WaitingForCapacity
        ) || self.entry.is_none()
            || insert.owner != self.owner
        {
            return Err(RepairTaskError::IllegalPhase);
        }
        self.prepared = Some(insert);
        Ok(())
    }

    /// Advance along one legal monotonic repair phase edge.
    pub fn advance(&mut self, next: RepairPhase) -> Result<(), RepairTaskError> {
        let legal = matches!(
            (self.phase, next),
            (RepairPhase::Scheduled, RepairPhase::OnWire { .. })
                | (RepairPhase::OnWire { .. }, RepairPhase::Buffered)
                | (RepairPhase::Buffered, RepairPhase::WaitingForCapacity)
                | (RepairPhase::Buffered, RepairPhase::StateDispatched { .. })
                | (
                    RepairPhase::WaitingForCapacity,
                    RepairPhase::StateDispatched { .. }
                )
        );
        if !legal {
            return Err(RepairTaskError::IllegalPhase);
        }
        self.phase = next;
        Ok(())
    }

    /// Return any non-dispatched task to scheduling after one bounded failed attempt.
    pub fn retry(&mut self) -> Result<(), RepairTaskError> {
        if matches!(self.phase, RepairPhase::StateDispatched { .. }) {
            return Err(RepairTaskError::IllegalPhase);
        }
        self.attempts = self
            .attempts
            .checked_add(1)
            .ok_or(RepairTaskError::AttemptsExhausted)?;
        self.phase = RepairPhase::Scheduled;
        self.context = None;
        self.context_requested = false;
        self.entry = None;
        self.prepared = None;
        self.source = None;
        Ok(())
    }
}

/// Exact pending VCT repairs keyed by their complete owners.
#[derive(Clone, Debug, Default)]
pub struct VctRepairQueue(HashMap<WorkOwner, VctRepairTask>);

impl VctRepairQueue {
    /// Insert one exact task, returning a contradictory prior task for the same owner.
    pub fn insert(&mut self, task: VctRepairTask) -> Option<VctRepairTask> {
        self.0.insert(task.owner, task)
    }

    /// Return one exact task for phase handling.
    pub fn get_mut(&mut self, owner: WorkOwner) -> Option<&mut VctRepairTask> {
        self.0.get_mut(&owner)
    }

    /// Return one exact task without permitting mutation.
    pub fn get(&self, owner: WorkOwner) -> Option<&VctRepairTask> {
        self.0.get(&owner)
    }

    /// Return the sole scheduled task, when one exists.
    pub fn scheduled(&self) -> Option<&VctRepairTask> {
        self.0
            .values()
            .find(|task| task.phase == RepairPhase::Scheduled)
    }

    /// Return the sole task waiting for bounded action capacity.
    pub fn waiting(&self) -> Option<&VctRepairTask> {
        self.0
            .values()
            .find(|task| task.phase == RepairPhase::WaitingForCapacity)
    }

    /// Return the exact on-wire task matching one authenticated stream response.
    pub fn on_wire(
        &self,
        source: SourceId,
        session_id: u64,
        request_id: std::num::NonZeroU64,
    ) -> Option<&VctRepairTask> {
        self.0.values().find(|task| {
            task.phase == RepairPhase::OnWire { peer: source }
                && task.owner.session_id == session_id
                && task.owner.request_id == request_id
        })
    }

    /// Return repair work still owned by one authenticated stream session.
    pub fn for_session(&self, source: SourceId, session_id: u64) -> Option<&VctRepairTask> {
        self.0.values().find(|task| {
            task.source == Some(source)
                && task.owner.session_id == session_id
                && !matches!(task.phase, RepairPhase::StateDispatched { .. })
        })
    }

    /// Rekey a resolved task from its scheduling owner to its actual wire owner.
    pub fn assign(
        &mut self,
        scheduled_owner: WorkOwner,
        wire_owner: WorkOwner,
        peer: SourceId,
    ) -> Result<(), RepairTaskError> {
        let mut task = self
            .0
            .remove(&scheduled_owner)
            .ok_or(RepairTaskError::IllegalPhase)?;
        if let Err(error) = task.assign(wire_owner, peer) {
            self.0.insert(scheduled_owner, task);
            return Err(error);
        }
        self.0.insert(wire_owner, task);
        Ok(())
    }

    /// Retire one completed, stale, or canceled task.
    pub fn remove(&mut self, owner: WorkOwner) -> Option<VctRepairTask> {
        self.0.remove(&owner)
    }

    /// Retire every repair before replacing or withdrawing the current state need.
    pub fn drain(&mut self) -> Vec<VctRepairTask> {
        self.0.drain().map(|(_, task)| task).collect()
    }

    /// Retire every task whose version, generation, or finalized anchor is obsolete.
    pub fn retain_current(&mut self, current: &EngineSnapshot) -> Vec<VctRepairTask> {
        let obsolete: Vec<_> = self
            .0
            .iter()
            .filter_map(|(owner, task)| {
                (owner.state_version != current.state_version
                    || owner.header_generation != current.header_generation
                    || owner
                        .verified_generation
                        .is_some_and(|generation| generation != current.verified_generation)
                    || owner.branch.anchor_hash != current.frontiers.finalized.hash)
                    .then_some(task.clone())
            })
            .collect();
        self.0.retain(|owner, _| {
            owner.state_version == current.state_version
                && owner.header_generation == current.header_generation
                && owner
                    .verified_generation
                    .is_none_or(|generation| generation == current.verified_generation)
                && owner.branch.anchor_hash == current.frontiers.finalized.hash
        });
        obsolete
    }

    /// Number of exact pending repairs.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether no VCT repair remains pending.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use zakura_chain::{block, work::difficulty::U256};
    use zakura_header_chain::{
        AlarmSet, BranchId, ChainScore, EngineMode, Frontier, FrontierSet, HeaderGeneration,
        StateVersion, SuffixWork, VerifiedGeneration,
    };

    use super::*;

    fn hash(byte: u8) -> block::Hash {
        block::Hash([byte; 32])
    }

    fn snapshot() -> EngineSnapshot {
        let finalized = Frontier::new(block::Height(10), hash(1));
        let tip = Frontier::new(block::Height(20), hash(2));
        EngineSnapshot {
            mode: EngineMode::Integrated,
            state_version: StateVersion::new(3),
            header_generation: HeaderGeneration::new(4),
            verified_generation: VerifiedGeneration::new(5),
            frontiers: FrontierSet {
                finalized,
                header_best: tip,
                verified_best: finalized,
            },
            header_best_score: ChainScore::new(SuffixWork::new(U256::from(10_u8)), tip.hash),
            oldest_retained_height: finalized.height,
            alarms: AlarmSet::default(),
        }
    }

    fn owner(snapshot: &EngineSnapshot) -> WorkOwner {
        WorkOwner {
            state_version: snapshot.state_version,
            header_generation: snapshot.header_generation,
            verified_generation: Some(snapshot.verified_generation),
            branch: BranchId::new(
                snapshot.frontiers.finalized.hash,
                snapshot.frontiers.header_best.hash,
            ),
            session_id: 6,
            request_id: NonZeroU64::new(7).expect("seven is nonzero"),
        }
    }

    fn task(snapshot: &EngineSnapshot) -> VctRepairTask {
        let owner = owner(snapshot);
        VctRepairTask::new(
            owner,
            BranchRange::new(owner.branch, block::Height(11), block::Height(19))
                .expect("the fixture range is ordered"),
        )
        .expect("the fixture range and owner name the same branch")
    }

    #[test]
    fn phase_machine_is_monotonic_and_retries_only_before_state_dispatch() {
        let snapshot = snapshot();
        let mut task = task(&snapshot);
        let peer = SourceId::from_digest([8; 32]);
        task.advance(RepairPhase::OnWire { peer })
            .expect("scheduled work can go on wire");
        assert_eq!(task.retry(), Ok(()));
        assert_eq!(task.phase, RepairPhase::Scheduled);
        assert_eq!(task.attempts, 1);
        task.advance(RepairPhase::OnWire { peer })
            .expect("retried work can go on wire");
        task.advance(RepairPhase::Buffered)
            .expect("wire completion can buffer");
        task.advance(RepairPhase::WaitingForCapacity)
            .expect("buffered work can wait for capacity");
        task.advance(RepairPhase::StateDispatched {
            transition: EvidenceId::from_digest([9; 32]),
        })
        .expect("capacity admission can dispatch state");
        assert_eq!(task.retry(), Err(RepairTaskError::IllegalPhase));
        assert_eq!(
            task.advance(RepairPhase::Buffered),
            Err(RepairTaskError::IllegalPhase)
        );
    }

    #[test]
    fn range_branch_mismatch_is_unrepresentable() {
        let snapshot = snapshot();
        let owner = owner(&snapshot);
        let range = BranchRange::new(
            BranchId::new(owner.branch.anchor_hash, hash(3)),
            block::Height(11),
            block::Height(19),
        )
        .expect("the fixture range is ordered");
        assert_eq!(
            VctRepairTask::new(owner, range),
            Err(RepairTaskError::BranchMismatch)
        );
    }

    #[test]
    fn every_repair_phase_is_retired_before_new_generation_work() {
        let snapshot = snapshot();
        let peer = SourceId::from_digest([8; 32]);
        let transition = EvidenceId::from_digest([9; 32]);
        let phases = [
            RepairPhase::Scheduled,
            RepairPhase::OnWire { peer },
            RepairPhase::Buffered,
            RepairPhase::WaitingForCapacity,
            RepairPhase::StateDispatched { transition },
        ];
        for phase in phases {
            let mut task = task(&snapshot);
            task.phase = phase;
            let mut queue = VctRepairQueue::default();
            assert_eq!(queue.insert(task.clone()), None);
            let mut changed = snapshot.clone();
            changed.state_version = StateVersion::new(4);
            changed.header_generation = HeaderGeneration::new(5);
            assert_eq!(queue.retain_current(&changed), vec![task]);
            assert!(queue.is_empty(), "phase {phase:?} survived retirement");
        }
    }
}
