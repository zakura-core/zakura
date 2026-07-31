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
    DurableTransitionFacts, EngineHydrationError, EngineTransition, HeaderChainEngine,
};
pub(crate) use invariants::verify_plan;
pub use invariants::InvariantViolation;
pub use planner::{TransitionFailure, TransitionPlan};
pub use recovery::{
    audit_store, AuditViolation, RecoveryFailure, RecoveryPlan, RecoveryRepair, StoreAuditRead,
    ValidationContextRecord,
};
pub use store::StoreError;
pub use types::*;
