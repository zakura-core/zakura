//! Generation- and branch-owned auxiliary VCT repair work.

use std::collections::HashSet;

use thiserror::Error;
use tokio::time::Instant;
use zakura_chain::block;
use zakura_header_chain::{BodyWorkOwner, EngineSnapshot, SourceId, VctRepairContext};

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
        self.tried_sources.insert(source);
        self.state = RepairPolicyState::Ready { context };
        Ok(())
    }

    /// Record a supplier whose request could not be queued.
    pub fn record_failed_source(&mut self, source: SourceId) -> Result<(), RepairPolicyError> {
        if !matches!(self.state, RepairPolicyState::Ready { .. }) {
            return Err(RepairPolicyError::IllegalState);
        }
        self.attempts = self.attempts.saturating_add(1);
        self.tried_sources.insert(source);
        Ok(())
    }

    /// Pause after a complete supplier cycle, then make every supplier eligible again.
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
            _ => {}
        }
    }

    /// Return the next task-owned maintenance deadline.
    pub fn next_deadline(&self) -> Option<Instant> {
        match self.state {
            RepairPolicyState::QueryingContext { deadline, .. } => Some(deadline),
            RepairPolicyState::ContextBackoff { retry_at }
            | RepairPolicyState::SupplierBackoff { retry_at, .. } => Some(retry_at),
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
