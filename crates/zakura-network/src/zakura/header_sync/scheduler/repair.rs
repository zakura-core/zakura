//! Generation- and branch-owned auxiliary VCT repair work.

use std::collections::HashSet;

use thiserror::Error;
use tokio::time::Instant;
use zakura_chain::block;
use zakura_header_chain::{BodyWorkOwner, EngineSnapshot, SourceId, VctRepairContext};

/// Maximum distinct suppliers retained and tried before one repair backoff cycle.
pub(in crate::zakura::header_sync) const MAX_SUPPLIERS_PER_CYCLE: usize = 3;

/// Structurally complete state of one auxiliary repair task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::header_sync) enum RepairPolicyState {
    /// Work needs an exact state context read.
    NeedsContext,
    /// The exact state context read is outstanding.
    QueryingContext {
        /// Latest time the context read may remain outstanding.
        deadline: Instant,
        /// Retry deadline used if the outstanding read never completes.
        retry_at: Instant,
    },
    /// A failed context read is waiting for its bounded retry deadline.
    ContextBackoff {
        /// Earliest time another context read may begin.
        retry_at: Instant,
    },
    /// A resolved repair is ready for supplier assignment.
    Ready {
        /// Exact selected request context.
        context: VctRepairContext,
    },
    /// A resolved repair exhausted one supplier cycle.
    SupplierBackoff {
        /// Exact selected request context.
        context: VctRepairContext,
        /// Earliest time another full supplier cycle may begin.
        retry_at: Instant,
    },
    /// A local failure paused the repair without completing its supplier cycle.
    LocalBackoff {
        /// Exact selected request context.
        context: VctRepairContext,
        /// Earliest time local scheduling may resume.
        retry_at: Instant,
    },
    /// A shared active target owns supplier, wire, preparation, and admission progress.
    Assigned {
        /// Exact selected request context.
        context: VctRepairContext,
    },
    /// State admitted the repair and the matching durable signal has not cleared yet.
    Completed,
}

/// Invalid correlation or state transition for VCT repair work.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub(in crate::zakura::header_sync) enum RepairPolicyError {
    /// A wire owner changed the durable repair scope.
    #[error("wire assignment changed the VCT repair scope")]
    ScopeMismatch,
    /// The resolved target is outside the exact one-height repair range.
    #[error("resolved VCT repair target is outside its exact range")]
    TargetMismatch,
    /// The requested edge is not part of the repair state machine.
    #[error("illegal VCT repair state transition")]
    IllegalState,
}

/// One branch-owned auxiliary repair task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::header_sync) struct RepairRequirement {
    /// Exact asynchronous owner fixed when work was scheduled.
    pub owner: BodyWorkOwner,
    /// Exact selected height whose auxiliary metadata must be replaced.
    pub height: block::Height,
    /// Durable repair-signal generation that owns this task.
    pub repair_generation: u64,
    /// Current structurally complete state.
    pub state: RepairPolicyState,
    /// Failed or abandoned on-wire attempts, saturated only for diagnostics.
    pub attempts: u64,
    /// Suppliers already tried in the current cycle.
    pub tried_sources: HashSet<SourceId>,
}

impl RepairRequirement {
    /// Construct one scheduled exact-height repair.
    pub fn new(owner: BodyWorkOwner, height: block::Height, repair_generation: u64) -> Self {
        Self {
            owner,
            height,
            repair_generation,
            state: RepairPolicyState::NeedsContext,
            attempts: 0,
            tried_sources: HashSet::new(),
        }
    }

    /// Record that one bounded state context read is outstanding.
    pub fn mark_context_requested(
        &mut self,
        deadline: Instant,
        retry_at: Instant,
    ) -> Result<(), RepairPolicyError> {
        if self.state != RepairPolicyState::NeedsContext {
            return Err(RepairPolicyError::IllegalState);
        }
        self.state = RepairPolicyState::QueryingContext { deadline, retry_at };
        Ok(())
    }

    /// Attach a still-current exact selected request context.
    pub fn resolve(&mut self, context: VctRepairContext) -> Result<(), RepairPolicyError> {
        if !matches!(self.state, RepairPolicyState::QueryingContext { .. }) {
            return Err(RepairPolicyError::IllegalState);
        }
        if context.target.height != self.height {
            return Err(RepairPolicyError::TargetMismatch);
        }
        self.state = RepairPolicyState::Ready { context };
        Ok(())
    }

    /// Release one failed local context read for a later retry.
    pub fn context_unavailable(&mut self, retry_at: Instant) -> Result<(), RepairPolicyError> {
        if !matches!(self.state, RepairPolicyState::QueryingContext { .. }) {
            return Err(RepairPolicyError::IllegalState);
        }
        self.state = RepairPolicyState::ContextBackoff { retry_at };
        Ok(())
    }

    /// Bind a resolved task to the actual canonical stream request.
    pub fn assign(&mut self, owner: BodyWorkOwner) -> Result<(), RepairPolicyError> {
        if owner.header_authority() != self.owner.header_authority() {
            return Err(RepairPolicyError::ScopeMismatch);
        }
        let RepairPolicyState::Ready { context } = &self.state else {
            return Err(RepairPolicyError::IllegalState);
        };
        self.owner = owner;
        self.state = RepairPolicyState::Assigned {
            context: context.clone(),
        };
        Ok(())
    }

    /// Mark a matching state admission as complete.
    pub fn complete(&mut self) -> Result<(), RepairPolicyError> {
        if !matches!(self.state, RepairPolicyState::Assigned { .. }) {
            return Err(RepairPolicyError::IllegalState);
        }
        self.state = RepairPolicyState::Completed;
        Ok(())
    }

    /// Return any non-dispatched task to scheduling after one failed supplier attempt.
    pub fn retry(&mut self, source: SourceId) -> Result<(), RepairPolicyError> {
        let context = match &self.state {
            RepairPolicyState::Assigned { context } => context.clone(),
            _ => return Err(RepairPolicyError::IllegalState),
        };
        self.attempts = self.attempts.saturating_add(1);
        if self.tried_sources.len() < MAX_SUPPLIERS_PER_CYCLE {
            self.tried_sources.insert(source);
        }
        self.state = RepairPolicyState::Ready { context };
        Ok(())
    }

    /// Record a supplier whose request could not be queued.
    pub fn record_failed_source(&mut self, source: SourceId) -> Result<(), RepairPolicyError> {
        if !matches!(self.state, RepairPolicyState::Ready { .. }) {
            return Err(RepairPolicyError::IllegalState);
        }
        self.attempts = self.attempts.saturating_add(1);
        if self.tried_sources.len() < MAX_SUPPLIERS_PER_CYCLE {
            self.tried_sources.insert(source);
        }
        Ok(())
    }

    /// Back off ready or assigned repair work after a local failure.
    pub fn defer_local_retry_until(&mut self, retry_at: Instant) -> Result<(), RepairPolicyError> {
        let context = match &self.state {
            RepairPolicyState::Ready { context } | RepairPolicyState::Assigned { context } => {
                context.clone()
            }
            _ => return Err(RepairPolicyError::IllegalState),
        };
        self.attempts = self.attempts.saturating_add(1);
        self.state = RepairPolicyState::LocalBackoff { context, retry_at };
        Ok(())
    }

    /// Whether this repair must pause before trying another supplier.
    pub fn supplier_cycle_exhausted(&self) -> bool {
        self.tried_sources.len() >= MAX_SUPPLIERS_PER_CYCLE
    }

    /// Pause after a complete or bounded supplier cycle, then make every supplier eligible again.
    pub fn defer_retry_until(&mut self, deadline: Instant) -> Result<(), RepairPolicyError> {
        let RepairPolicyState::Ready { context } = &self.state else {
            return Err(RepairPolicyError::IllegalState);
        };
        self.state = RepairPolicyState::SupplierBackoff {
            context: context.clone(),
            retry_at: deadline,
        };
        Ok(())
    }

    /// Resume a deferred context or supplier cycle once its backoff has elapsed.
    pub fn resume_retry_cycle(&mut self, now: Instant) {
        match &self.state {
            RepairPolicyState::QueryingContext { deadline, retry_at } if *deadline <= now => {
                self.state = RepairPolicyState::ContextBackoff {
                    retry_at: *retry_at,
                };
            }
            RepairPolicyState::ContextBackoff { retry_at } if *retry_at <= now => {
                self.state = RepairPolicyState::NeedsContext;
            }
            RepairPolicyState::SupplierBackoff { context, retry_at } if *retry_at <= now => {
                self.state = RepairPolicyState::Ready {
                    context: context.clone(),
                };
                self.tried_sources.clear();
            }
            RepairPolicyState::LocalBackoff { context, retry_at } if *retry_at <= now => {
                self.state = RepairPolicyState::Ready {
                    context: context.clone(),
                };
            }
            _ => {}
        }
    }

    /// Return the next task-owned maintenance deadline.
    pub fn next_deadline(&self) -> Option<Instant> {
        match self.state {
            RepairPolicyState::QueryingContext { deadline, .. } => Some(deadline),
            RepairPolicyState::ContextBackoff { retry_at }
            | RepairPolicyState::SupplierBackoff { retry_at, .. }
            | RepairPolicyState::LocalBackoff { retry_at, .. } => Some(retry_at),
            _ => None,
        }
    }
}

/// The sole optional VCT repair, when state requires one.
#[derive(Clone, Debug, Default)]
pub(in crate::zakura::header_sync) struct RepairRequirementSlot(Option<RepairRequirement>);

impl RepairRequirementSlot {
    /// Return the sole current task.
    pub fn current(&self) -> Option<&RepairRequirement> {
        self.0.as_ref()
    }

    /// Return the sole current task mutably.
    pub fn current_mut(&mut self) -> Option<&mut RepairRequirement> {
        self.0.as_mut()
    }

    /// Replace the current task and return the previous one.
    pub fn insert(&mut self, task: RepairRequirement) -> Option<RepairRequirement> {
        self.0.replace(task)
    }

    /// Return one exact task for phase handling.
    pub fn get_mut(&mut self, owner: BodyWorkOwner) -> Option<&mut RepairRequirement> {
        self.0.as_mut().filter(|task| task.owner == owner)
    }

    /// Return one exact task without permitting mutation.
    pub fn get(&self, owner: BodyWorkOwner) -> Option<&RepairRequirement> {
        self.0.as_ref().filter(|task| task.owner == owner)
    }

    /// Return the sole task that needs a context query.
    pub fn needs_context(&self) -> Option<&RepairRequirement> {
        self.0
            .as_ref()
            .filter(|task| task.state == RepairPolicyState::NeedsContext)
    }

    /// Return the sole task ready for supplier assignment.
    pub fn ready(&self) -> Option<&RepairRequirement> {
        self.0
            .as_ref()
            .filter(|task| matches!(task.state, RepairPolicyState::Ready { .. }))
    }

    /// Rekey a resolved task from its scheduling owner to its actual wire owner.
    pub fn assign(
        &mut self,
        scheduled_owner: BodyWorkOwner,
        wire_owner: BodyWorkOwner,
    ) -> Result<(), RepairPolicyError> {
        self.get_mut(scheduled_owner)
            .ok_or(RepairPolicyError::IllegalState)?
            .assign(wire_owner)
    }

    /// Retire one completed, stale, or canceled task.
    pub fn remove(&mut self, owner: BodyWorkOwner) -> Option<RepairRequirement> {
        if self.0.as_ref().is_some_and(|task| task.owner == owner) {
            self.0.take()
        } else {
            None
        }
    }

    /// Retire the current task before replacing or withdrawing the state need.
    pub fn take(&mut self) -> Option<RepairRequirement> {
        self.0.take()
    }

    /// Retire every task whose generation or finalized anchor is obsolete.
    pub fn retain_current(&mut self, current: &EngineSnapshot) -> Option<RepairRequirement> {
        let obsolete = self.0.as_ref().is_some_and(|task| {
            task.owner.header_generation != current.header_generation
                || task.owner.verified_generation != current.verified_generation
                || task.owner.header_authority().branch.anchor_hash
                    != current.frontiers.finalized.hash
        });
        if obsolete {
            self.0.take()
        } else {
            None
        }
    }

    /// Whether no VCT repair remains pending.
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use zakura_chain::{block, work::difficulty::U256};
    use zakura_header_chain::{
        AlarmSet, ChainScore, EngineMode, Frontier, FrontierSet, HeaderGeneration, StateVersion,
        SuffixWork, VerifiedGeneration,
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

    fn owner(snapshot: &EngineSnapshot) -> BodyWorkOwner {
        zakura_header_chain::BodyWorkAuthority::for_snapshot(snapshot)
            .bind(6, NonZeroU64::new(7).expect("seven is nonzero"))
    }

    fn task(snapshot: &EngineSnapshot) -> RepairRequirement {
        RepairRequirement::new(owner(snapshot), block::Height(19), 11)
    }

    fn mark_context_requested(task: &mut RepairRequirement) {
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        task.mark_context_requested(deadline, deadline + std::time::Duration::from_secs(1))
            .expect("needed context can be queried");
    }

    fn context() -> VctRepairContext {
        VctRepairContext {
            target: Frontier::new(block::Height(19), hash(5)),
            locator: zakura_header_chain::HeaderLocator::for_continuation(Frontier::new(
                block::Height(18),
                hash(4),
            )),
        }
    }

    #[test]
    fn state_machine_rotates_suppliers_and_rejects_illegal_transitions() {
        let snapshot = snapshot();
        let mut task = task(&snapshot);
        let source = SourceId::from_digest([8; 32]);
        let context = context();
        mark_context_requested(&mut task);
        task.resolve(context.clone())
            .expect("the exact context can resolve");
        task.assign(task.owner).expect("ready work can go on wire");
        assert_eq!(task.retry(source), Ok(()));
        assert_eq!(
            task.state,
            RepairPolicyState::Ready {
                context: context.clone()
            }
        );
        assert_eq!(task.attempts, 1);
        assert!(task.tried_sources.contains(&source));

        task.assign(task.owner)
            .expect("retried work can go on wire");
        task.complete()
            .expect("a matching state admission completes the task");
        let completed = task.clone();
        assert_eq!(task.retry(source), Err(RepairPolicyError::IllegalState));
        assert_eq!(task, completed, "completed work cannot transition again");
    }

    #[test]
    fn retry_cycle_rotates_sources_and_resumes_after_backoff() {
        let mut task = task(&snapshot());
        let first = SourceId::from_digest([8; 32]);
        let second = SourceId::from_digest([9; 32]);
        let context = context();
        mark_context_requested(&mut task);
        task.resolve(context.clone())
            .expect("the exact context resolves");
        task.assign(task.owner)
            .expect("the first supplier goes on wire");
        task.retry(first).expect("the first supplier can fail");
        task.assign(task.owner)
            .expect("the second supplier goes on wire");
        task.retry(second).expect("the second supplier can fail");
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        task.defer_retry_until(deadline)
            .expect("a complete supplier cycle backs off");

        task.resume_retry_cycle(deadline);

        assert!(task.tried_sources.is_empty());
        assert_eq!(task.state, RepairPolicyState::Ready { context });
        assert_eq!(task.attempts, 2);
    }

    #[test]
    fn supplier_cycle_bounds_identity_churn() {
        let mut task = task(&snapshot());
        let context = context();
        mark_context_requested(&mut task);
        task.resolve(context.clone())
            .expect("the exact context resolves");

        for byte in [1_u8, 2, 3] {
            task.assign(task.owner).expect("ready work can go on wire");
            task.retry(SourceId::from_digest([byte; 32]))
                .expect("each distinct supplier can fail");
        }
        assert!(task.supplier_cycle_exhausted());
        assert_eq!(task.tried_sources.len(), 3);
        assert_eq!(task.attempts, 3);

        for byte in 4_u8..=64 {
            task.record_failed_source(SourceId::from_digest([byte; 32]))
                .expect("late churn cannot enlarge an exhausted cycle");
        }
        assert!(task.supplier_cycle_exhausted());
        assert_eq!(task.tried_sources.len(), 3);
        assert!(!task.tried_sources.contains(&SourceId::from_digest([4; 32])));
        assert_eq!(task.attempts, 64);

        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        task.defer_retry_until(deadline)
            .expect("a bounded supplier cycle backs off");
        task.resume_retry_cycle(deadline);
        assert!(!task.supplier_cycle_exhausted());
        assert!(task.tried_sources.is_empty());
        assert_eq!(task.state, RepairPolicyState::Ready { context });
    }

    #[test]
    fn local_retry_preserves_supplier_eligibility_from_ready_and_assigned() {
        for assigned in [false, true] {
            let mut task = task(&snapshot());
            let context = context();
            mark_context_requested(&mut task);
            task.resolve(context.clone())
                .expect("the exact context resolves");
            let failed_source = SourceId::from_digest([1; 32]);
            if assigned {
                task.assign(task.owner).expect("ready work can go on wire");
                task.retry(failed_source)
                    .expect("one supplier failure starts the current cycle");
                task.assign(task.owner)
                    .expect("another supplier can own the current cycle");
            } else {
                task.tried_sources.insert(failed_source);
            }
            let attempts = task.attempts;
            let retry_at = Instant::now() + std::time::Duration::from_secs(1);

            task.defer_local_retry_until(retry_at)
                .expect("ready or assigned work can back off after a local failure");

            assert_eq!(
                task.tried_sources,
                [failed_source].into_iter().collect(),
                "assigned={assigned}"
            );
            assert_eq!(task.attempts, attempts + 1, "assigned={assigned}");
            assert_eq!(
                task.state,
                RepairPolicyState::LocalBackoff {
                    context: context.clone(),
                    retry_at,
                },
                "assigned={assigned}"
            );
            task.resume_retry_cycle(retry_at);
            assert_eq!(
                task.tried_sources,
                [failed_source].into_iter().collect(),
                "assigned={assigned}"
            );
            assert_eq!(
                task.state,
                RepairPolicyState::Ready { context },
                "assigned={assigned}"
            );
        }
    }

    #[test]
    fn context_backoff_wakes_at_its_deadline() {
        let mut task = task(&snapshot());
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        mark_context_requested(&mut task);
        task.context_unavailable(deadline)
            .expect("an unavailable query enters context backoff");

        task.resume_retry_cycle(deadline - std::time::Duration::from_millis(1));
        assert_eq!(
            task.state,
            RepairPolicyState::ContextBackoff { retry_at: deadline }
        );
        task.resume_retry_cycle(deadline);
        assert_eq!(task.state, RepairPolicyState::NeedsContext);
    }

    #[test]
    fn outstanding_context_query_times_out_before_retrying() {
        let mut task = task(&snapshot());
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        let retry_at = deadline + std::time::Duration::from_secs(2);
        task.mark_context_requested(deadline, retry_at)
            .expect("needed context can be queried");

        task.resume_retry_cycle(deadline);
        assert_eq!(task.state, RepairPolicyState::ContextBackoff { retry_at });
        task.resume_retry_cycle(retry_at);
        assert_eq!(task.state, RepairPolicyState::NeedsContext);
    }

    #[test]
    // The test enumerates each repair policy state.
    // A generation change must retire all prior work.
    fn generation_change_retires_every_repair_state() {
        let snapshot = snapshot();
        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        let context = context();
        let states = vec![
            RepairPolicyState::NeedsContext,
            RepairPolicyState::QueryingContext {
                deadline,
                retry_at: deadline + std::time::Duration::from_secs(1),
            },
            RepairPolicyState::ContextBackoff { retry_at: deadline },
            RepairPolicyState::Ready {
                context: context.clone(),
            },
            RepairPolicyState::SupplierBackoff {
                context: context.clone(),
                retry_at: deadline,
            },
            RepairPolicyState::LocalBackoff {
                context: context.clone(),
                retry_at: deadline,
            },
            RepairPolicyState::Assigned { context },
            RepairPolicyState::Completed,
        ];
        for state in states {
            let mut old_task = task(&snapshot);
            old_task.state = state.clone();
            let mut slot = RepairRequirementSlot::default();
            assert_eq!(slot.insert(old_task.clone()), None);
            let mut changed = snapshot.clone();
            changed.state_version = StateVersion::new(4);
            changed.header_generation = HeaderGeneration::new(5);
            changed.frontiers.header_best =
                Frontier::new(changed.frontiers.header_best.height, hash(3));
            assert_eq!(slot.retain_current(&changed), Some(old_task.clone()));
            assert!(slot.is_empty(), "state {state:?} survived retirement");

            let replacement = task(&changed);
            assert_eq!(slot.insert(replacement.clone()), None);
            assert_eq!(
                slot.needs_context(),
                Some(&replacement),
                "new exact-branch repair schedules only after old state retirement"
            );
            assert!(
                slot.get(old_task.owner).is_none(),
                "old state ownership cannot alias replacement work"
            );
        }
    }
}
