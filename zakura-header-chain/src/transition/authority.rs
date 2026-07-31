//! Capabilities and authoritative clock inputs for pure transition planning.

use chrono::{DateTime, Utc};

use crate::{EngineConfig, EvidenceId};

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
    /// Return true only when `evidence` identifies the writer's staged mutation.
    fn authorizes(&self, evidence: EvidenceId) -> bool;
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
