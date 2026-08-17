//! Typed transition evidence, durable snapshots, and read-oriented store contracts.

mod authority;
mod engine;
mod invariants;
mod planner;
mod recovery;
mod types;

pub use authority::{Clock, FullStateEvidenceAuthority, SystemClock, TransitionContext};
pub use engine::{
    CommittedTransitionError, EngineHydrationError, HeaderChainEngine, HeaderInsertionFacts,
    HeaderValidationFacts, TransitionInput,
};
pub(crate) use invariants::verify_candidate;
pub use invariants::InvariantViolation;
#[cfg(test)]
pub(crate) use invariants::{verify_plan, verify_plan_production};
pub use planner::plan::EngineTransition;
pub(crate) use planner::plan::PlanCandidate;
#[cfg(feature = "test-support")]
pub use planner::retention::{RetentionBenchmarkFixture, RetentionBenchmarkResult};
pub use planner::{
    AuxiliaryViolation, BodyViolation, FinalityViolation, HeaderPathKind, HeaderPathProblem,
    HeaderValidationCheck, HeaderValidationSource, HeaderViolation, InvalidTransitionEvidence,
    LimitViolation, OperatorViolation, PlannerCoherenceViolation, ProjectionKind,
    TransitionFailure,
};
pub use recovery::{
    audit_store, audit_store_at, audit_store_for_trust_anchor_update, AuditViolation,
    RecoveryFailure, RecoveryPlan, RecoveryRepair, StoreAuditRead, StoreAuditSnapshot,
    ValidationContextRecord,
};
pub use types::*;
