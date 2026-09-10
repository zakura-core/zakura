//! Reversible operator invalidation and reconsideration evidence.

use zakura_chain::block;

use crate::{EvidenceId, OperatorInvalidationId};

/// Add one reversible operator reason.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct OperatorInvalidate {
    /// Exact retained target.
    pub target: block::Hash,
    /// Independently removable invalidation identity.
    pub id: OperatorInvalidationId,
    /// Stable authenticated operator-reason digest.
    pub operator_reason_digest: [u8; 32],
    /// Stable idempotency evidence for this authenticated operator action.
    pub evidence: EvidenceId,
}

/// Remove exactly one reversible operator reason.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct OperatorReconsider {
    /// Exact retained target.
    pub target: block::Hash,
    /// Exact invalidation identity to remove.
    pub id: OperatorInvalidationId,
    /// Exact currently installed invalidation evidence, or `None` if it is absent.
    pub invalidation_evidence: Option<EvidenceId>,
    /// Stable idempotency evidence for this authenticated operator action.
    pub evidence: EvidenceId,
}
