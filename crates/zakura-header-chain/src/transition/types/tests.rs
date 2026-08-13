//! Semantic coverage for the typed transition surface.

use zakura_chain::block;

use crate::{
    BodyCommitmentKind, BodyEvidence, BodyPayloadMismatch, BodyRuleId, BodySizeHint,
    BodyUnavailableSummary, BodyVerificationOutcome, ConsensusBodyInvalid, EventAdmission,
    EvidenceId, Frontier, MigratedPinRefutation, OperatorInvalidationId, OperatorReconsider,
    SourceId, TransientBodyFailure, TransientBodyFailureKind, TransitionEvent, TransitionTypeError,
    VerifiedBodyEvidence,
};

#[test]
fn body_size_hints_enforce_zero_sentinel_and_canonical_limit() {
    assert_eq!(BodySizeHint::new(0), Ok(BodySizeHint::Unknown));
    assert!(matches!(BodySizeHint::new(1), Ok(BodySizeHint::Known(_))));
    let maximum =
        u32::try_from(block::MAX_BLOCK_BYTES).expect("the canonical block limit fits in u32");
    assert!(matches!(
        BodySizeHint::new(maximum),
        Ok(BodySizeHint::Known(_))
    ));
    assert_eq!(
        BodySizeHint::new(maximum + 1),
        Err(TransitionTypeError::InvalidBodySize(maximum + 1))
    );
}

#[test]
fn event_authority_and_evidence_policies_are_typed() {
    let evidence = EvidenceId::from_digest([7; 32]);
    let reconsider = TransitionEvent::OperatorReconsider(OperatorReconsider {
        target: block::Hash([1; 32]),
        id: OperatorInvalidationId::new([2; 16]),
        invalidation_evidence: Some(EvidenceId::from_digest([3; 32])),
        evidence,
    });
    assert_eq!(reconsider.admission(), EventAdmission::IntegratedFullState);
    assert_eq!(reconsider.idempotency_key(), Some(evidence));
    assert_eq!(reconsider.header_sync_owner(), None);
    assert_eq!(reconsider.body_owner(), None);

    let refutation = TransitionEvent::MigratedPinRefutation(MigratedPinRefutation {
        full_state_transition_id: evidence,
        pin: Frontier::new(block::Height(2), block::Hash([4; 32])),
        invalid_header: Frontier::new(block::Height(1), block::Hash([5; 32])),
        rule: BodyRuleId::new("body.rule"),
    });
    assert_eq!(refutation.admission(), EventAdmission::IntegratedFullState);
    assert_eq!(refutation.idempotency_key(), Some(evidence));
    assert_eq!(refutation.header_sync_owner(), None);
    assert_eq!(refutation.body_owner(), None);

    assert_eq!(
        TransitionEvent::ReevaluateDeferred.admission(),
        EventAdmission::AnyMode
    );
    assert_eq!(TransitionEvent::ReevaluateDeferred.idempotency_key(), None);
}

#[test]
fn body_verification_outcomes_preserve_distinct_transition_effects() {
    let evidence = EvidenceId::from_digest([9; 32]);
    let hash = block::Hash([8; 32]);
    assert!(matches!(
        BodyEvidence::from(BodyVerificationOutcome::Verified(VerifiedBodyEvidence {
            hash,
            evidence,
        })),
        BodyEvidence::Verified(VerifiedBodyEvidence { hash: actual, .. }) if actual == hash
    ));
    assert!(matches!(
        BodyEvidence::from(BodyVerificationOutcome::PayloadMismatch(
            BodyPayloadMismatch {
                evidence,
                requested: hash,
                delivered: block::Hash([7; 32]),
                kind: BodyCommitmentKind::HeaderHash,
                source: SourceId::from_digest([6; 32]),
            }
        )),
        BodyEvidence::PayloadMismatch(BodyPayloadMismatch { requested, .. }) if requested == hash
    ));
    assert!(matches!(
        BodyEvidence::from(BodyVerificationOutcome::ConsensusInvalid(
            ConsensusBodyInvalid {
                hash,
                evidence,
                rule: BodyRuleId::new("body.rule"),
                source: SourceId::from_digest([5; 32]),
            }
        )),
        BodyEvidence::ConsensusInvalid(ConsensusBodyInvalid { hash: actual, .. }) if actual == hash
    ));
    assert!(matches!(
        BodyEvidence::from(BodyVerificationOutcome::Retryable(TransientBodyFailure {
            hash,
            evidence,
            kind: TransientBodyFailureKind::MissingContext,
            availability: BodyUnavailableSummary {
                attempts: 1,
                suppliers: 1,
                alarmed: false,
                ..Default::default()
            },
        })),
        BodyEvidence::Transient(TransientBodyFailure { hash: actual, .. }) if actual == hash
    ));
}

#[test]
fn transition_domain_codes_are_stable_and_exhaustive() {
    use crate::TransitionDomain;

    let domains = [
        (TransitionDomain::InsertHeaders, 0),
        (TransitionDomain::VerifiedChainChanged, 1),
        (TransitionDomain::VerifiedBlockAccepted, 2),
        (TransitionDomain::BodyPayloadMismatch, 3),
        (TransitionDomain::ConsensusBodyInvalid, 4),
        (TransitionDomain::TransientBodyFailure, 5),
        (TransitionDomain::VerifiedBody, 6),
        (TransitionDomain::BodySupplierDiscovered, 7),
        (TransitionDomain::OperatorBodyRetry, 8),
        (TransitionDomain::OperatorInvalidate, 9),
        (TransitionDomain::OperatorReconsider, 10),
        (TransitionDomain::FullStateFinalized, 11),
        (TransitionDomain::MigratedPinRefutation, 12),
        (TransitionDomain::AuxEvidence, 13),
        (TransitionDomain::ReevaluateDeferred, 14),
    ];
    for (domain, code) in domains {
        assert_eq!(domain.code(), code);
        assert_eq!(TransitionDomain::from_code(code), Some(domain));
    }
    assert_eq!(TransitionDomain::from_code(15), None);
}
