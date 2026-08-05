//! Capabilities and authoritative clock inputs for pure transition planning.

use chrono::{DateTime, Utc};

use crate::{EngineConfig, InsertHeaders, OperatorBodyRetry, TransitionEvent, ValidationLease};

/// Consensus-local time source; transition events cannot supply their own time.
pub trait Clock: Send + Sync {
    /// Return the current consensus-local time.
    fn now(&self) -> DateTime<Utc>;
}

/// Production wall-clock implementation.
#[derive(Copy, Clone, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// State-writer capability that authenticates staged full-state transition IDs.
pub trait FullStateEvidenceAuthority: Send + Sync {
    /// Return true only when the complete event is the writer's staged mutation.
    fn authorizes_full_state(&self, event: &TransitionEvent) -> bool;

    /// Return true only when the serialized scheduler staged this exact retry action.
    fn authorizes_scheduler_retry(&self, _retry: &OperatorBodyRetry) -> bool {
        false
    }

    /// Return true only when the serialized authority boundary registered this exact completion.
    fn authorizes_header_completion(&self, _insert: &InsertHeaders) -> bool {
        false
    }

    /// Return true only when the serialized state adapter issued this exact durable lease.
    fn authorizes_validation_lease(&self, _lease: &ValidationLease) -> bool {
        false
    }
}

/// Trusted dependencies used while deriving a transition plan.
pub struct TransitionContext<'a> {
    /// Immutable mode, anchors, and resource limits.
    pub config: &'a EngineConfig,
    /// Authoritative local time.
    pub clock: &'a dyn Clock,
    /// Integrated full-state authority, available only inside the state writer.
    pub full_state_authority: Option<&'a dyn FullStateEvidenceAuthority>,
    /// Active retained-path targets that resource eviction must protect.
    pub retention_references: &'a [zakura_chain::block::Hash],
}
