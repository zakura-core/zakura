//! Body verification classes, outcomes, and durable transition evidence.

use crate::{BodyRuleId, BodyUnavailableSummary, EvidenceId, SourceId};
use zakura_chain::block;

/// Exact body/header commitment mismatch kind.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BodyCommitmentKind {
    /// Delivered block header hash differs from the requested hash.
    HeaderHash,
    /// Transaction Merkle root mismatch.
    TransactionMerkleRoot,
    /// ZIP-244 authorization-data commitment mismatch.
    AuthDataRoot,
    /// Another height-applicable body-derived header commitment.
    Other(&'static str),
}

/// Supplier-attributed mismatched body payload.
/// A payload mismatch cannot affect eligibility.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct BodyPayloadMismatch {
    /// Stable delivery evidence.
    pub evidence: EvidenceId,
    /// Requested header hash.
    pub requested: block::Hash,
    /// Delivered header hash.
    pub delivered: block::Hash,
    /// Exact mismatched commitment.
    pub kind: BodyCommitmentKind,
    /// Body supplier, never a header-only supplier.
    pub source: SourceId,
}

/// Commitment-matching deterministic body consensus failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsensusBodyInvalid {
    /// Exact affected header.
    pub hash: block::Hash,
    /// Stable verifier evidence proving commitment matching and failure.
    pub evidence: EvidenceId,
    /// Exact full-state rule.
    pub rule: BodyRuleId,
    /// Proving body supplier, never inherited header suppliers.
    pub source: SourceId,
}

/// Retryable body failure category with no eligibility effect.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TransientBodyFailureKind {
    /// The verifier does not have the required state context yet.
    MissingContext,
    /// The coordinator canceled or superseded the work.
    Canceled,
    /// Local storage returned a transient failure.
    Storage,
    /// The verifier service became unavailable.
    VerifierUnavailable,
    /// External wait timed out.
    Timeout,
    /// The node temporarily exhausted local resources.
    ResourceExhausted,
}

/// Retryable body failure evidence.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TransientBodyFailure {
    /// Exact affected header.
    pub hash: block::Hash,
    /// Stable retry evidence.
    pub evidence: EvidenceId,
    /// Exact retry category.
    pub kind: TransientBodyFailureKind,
    /// Bounded persistent state of the owning retry episode.
    pub availability: BodyUnavailableSummary,
}

/// Authenticated discovery of a changed eligible body-supplier set.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct BodySupplierDiscovered {
    /// Exact selected header whose persistent retry episode gains a supplier.
    pub hash: block::Hash,
    /// Stable identity of the authenticated supplier-set observation.
    pub evidence: EvidenceId,
    /// Existing alarm episode with updated supplier evidence and a due probe.
    pub availability: BodyUnavailableSummary,
}

/// Authenticated operator request to restart one persistent body retry episode.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct OperatorBodyRetry {
    /// Exact selected header whose retry episode restarts.
    pub hash: block::Hash,
    /// Stable identity of the authenticated operator request.
    pub evidence: EvidenceId,
    /// Fresh zero-attempt episode summary.
    pub availability: BodyUnavailableSummary,
}

/// Full-state acceptance of one exact body/header pair.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct VerifiedBodyEvidence {
    /// Exact accepted header.
    pub hash: block::Hash,
    /// Stable verification evidence.
    pub evidence: EvidenceId,
}

/// Exhaustive body-result categories with intentionally distinct effects.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BodyVerificationOutcome {
    /// Full-state accepted the exact body/header pair.
    Verified(VerifiedBodyEvidence),
    /// The supplier delivered a payload that did not match the requested header.
    PayloadMismatch(BodyPayloadMismatch),
    /// Commitment-matching body data deterministically failed consensus.
    ConsensusInvalid(ConsensusBodyInvalid),
    /// Verification could not reach a durable consensus conclusion.
    Retryable(TransientBodyFailure),
}

/// Evidence-free classification that the verifier returns before the caller attaches supplier and evidence data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BodyVerificationClass {
    /// Full state already accepted the exact body.
    Duplicate,
    /// Delivered body data disagrees with a commitment in its admitted header.
    PayloadMismatch(BodyCommitmentKind),
    /// All applicable commitments matched before one deterministic consensus rule failed.
    ConsensusInvalid(BodyRuleId),
    /// Verification could not reach a durable consensus conclusion.
    Retryable(TransientBodyFailureKind),
}

impl From<BodyVerificationOutcome> for BodyEvidence {
    fn from(outcome: BodyVerificationOutcome) -> Self {
        match outcome {
            BodyVerificationOutcome::Verified(evidence) => Self::Verified(evidence),
            BodyVerificationOutcome::PayloadMismatch(evidence) => Self::PayloadMismatch(evidence),
            BodyVerificationOutcome::ConsensusInvalid(evidence) => Self::ConsensusInvalid(evidence),
            BodyVerificationOutcome::Retryable(evidence) => Self::Transient(evidence),
        }
    }
}

/// Durable transition evidence derived from one body-verification outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BodyEvidence {
    /// Bad delivery only.
    PayloadMismatch(BodyPayloadMismatch),
    /// Intrinsic deterministic body invalidity.
    ConsensusInvalid(ConsensusBodyInvalid),
    /// Retryable local/delivery failure.
    Transient(TransientBodyFailure),
    /// Full-state verified body.
    Verified(VerifiedBodyEvidence),
}
