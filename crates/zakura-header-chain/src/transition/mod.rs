//! Typed transition evidence, durable snapshots, and read-oriented store contracts.

mod authority;
mod engine;
mod invariants;
mod planner;
mod recovery;
mod store;
mod types;

pub use authority::{Clock, FullStateEvidenceAuthority, SystemClock, TransitionContext};
pub use engine::{
    CommittedTransitionError, DurableTransitionFacts, EngineHydrationError, EngineTransition,
    HeaderChainEngine,
};
pub(crate) use invariants::verify_candidate;
pub use invariants::InvariantViolation;
#[cfg(test)]
pub(crate) use invariants::{verify_plan, verify_plan_production};
pub use planner::TransitionFailure;
pub(crate) use planner::{PlanCandidate, TransitionPlan};
pub use recovery::{
    audit_store, audit_store_at, audit_store_for_trust_anchor_update, AuditViolation,
    RecoveryFailure, RecoveryPlan, RecoveryRepair, StoreAuditRead, ValidationContextRecord,
};
pub use store::StoreError;
pub use types::*;
