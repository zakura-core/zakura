//! Typed transition evidence, durable snapshots, and read-oriented store contracts.

mod authority;
mod engine;
mod invariants;
mod planner;
mod recovery;
mod types;

pub use authority::{Clock, FullStateEvidenceAuthority, SystemClock, TransitionContext};
#[cfg(any(test, feature = "test-support", zakura_sim))]
pub use engine::DurableState;
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
    audit_store, audit_store_at, audit_store_for_trust_anchor_update,
    audit_store_for_trust_anchor_update_at, AuditViolation, RecoveryFailure, RecoveryPlan,
    RecoveryRepair, StoreAuditRead, StoreAuditSnapshot, ValidationContextRecord,
};
pub(crate) use types::semantic_payload_fingerprint;
pub use types::*;

/// Highest common frontier of the engine's contiguous paths from finality.
/// Equal hashes share all parents, so shared heights form a prefix.
fn common_body_frontier(
    selected: &[crate::Frontier],
    verified: &[crate::Frontier],
) -> crate::Frontier {
    let last = selected.len().min(verified.len()) - 1;
    if selected[last] == verified[last] {
        return selected[last];
    }
    debug_assert_eq!(
        selected[0], verified[0],
        "both projections start at finality"
    );
    let mut shared = 0;
    let mut different = last;
    while different - shared > 1 {
        let mid = shared + (different - shared) / 2;
        if selected[mid] == verified[mid] {
            shared = mid;
        } else {
            different = mid;
        }
    }
    selected[shared]
}
