//! Typed transition evidence, durable snapshots, and read-oriented store contracts.

mod authority;
mod invariants;
mod planner;
mod recovery;
mod store;
mod types;

pub use authority::{
    Clock, FullStateEvidenceAuthority, StartupCapability, SystemClock, TransitionContext,
};
pub use invariants::{verify_plan, InvariantViolation};
pub use planner::{apply_transition, TransitionFailure, TransitionPlan};
pub use recovery::{
    audit_store, AuditViolation, RecoveryFailure, RecoveryPlan, RecoveryRepair, StoreAuditRead,
    ValidationContextRecord,
};
pub use store::{StoreError, StoreRead};
pub use types::*;
