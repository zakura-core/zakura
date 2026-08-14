//! Capabilities and authoritative clock inputs for pure transition planning.

use chrono::{DateTime, Utc};

use crate::{EngineConfig, InsertHeaders, OperatorBodyRetry, TransitionEvent, ValidationLease};

/// The consensus-local clock supplies time to transition events.
/// Transition events cannot supply time.
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
///
/// # Authority matrix by [`crate::EventAdmission`]
///
/// | [`crate::EventAdmission`] | Required capability | Failure |
/// | --- | --- | --- |
/// | `AnyMode` | none (mode-independent) | — |
/// | `IntegratedFullState` | integrated mode **and** [`Self::authorizes_full_state`] | [`crate::TransitionFailure::Mode`] / [`crate::TransitionFailure::Authority`] |
/// | `RegisteredScheduler` | [`Self::authorizes_scheduler_retry`] for the exact retry | [`crate::TransitionFailure::Authority`] |
/// | `RegisteredHeaderCompletion` | [`Self::authorizes_header_completion`] for the exact insert | [`crate::TransitionFailure::Authority`] |
///
/// Validation leases used while reconstructing predecessor context additionally
/// require [`Self::authorizes_validation_lease`]. Absent
/// [`TransitionContext::full_state_authority`] fails every gate that needs a
/// capability check.
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
///
/// Carries the config, clock, and optional [`FullStateEvidenceAuthority`] that
/// the planner consults for the [`crate::EventAdmission`] matrix documented on
/// that trait. The planner never loads durable rows itself; adapters must supply
/// event-specific facts on [`crate::TransitionInput`].
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
