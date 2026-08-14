//! Semantic coverage for the typed transition surface.

use std::{num::NonZeroU64, sync::Arc};

use zakura_chain::{
    block::{self, genesis::regtest_genesis_block},
    parameters::{testnet::RegtestParameters, Network},
};

use crate::{
    AuxDelivery, AuxEvidence, AuxObservationId, AuxObservationV1, AuxOutcome, AuxOutcomeStatus,
    AuxVerificationFactV1, BodyCommitmentKind, BodyEvidence, BodyPayloadMismatch, BodyRuleId,
    BodySizeHint, BodySupplierDiscovered, BodyUnavailableSummary, BodyVerificationOutcome,
    BodyWorkAuthority, BodyWorkOwner, BranchId, ConsensusBodyInvalid, EventAdmission, EvidenceId,
    Frontier, FullStateFinalized, HeaderGeneration, HeaderSyncWorkOwner, HeaderValidationState,
    HeaderWorkAuthority, InsertHeaders, MigratedPinRefutation, OperatorBodyRetry,
    OperatorInvalidate, OperatorInvalidationId, OperatorReconsider, PreparedHeader,
    PreparedHeaderBatch, SourceId, TargetCompletion, TransientBodyFailure,
    TransientBodyFailureKind, TransitionDomain, TransitionEvent, TransitionTypeError,
    VerifiedBlockAccepted, VerifiedBodyEvidence, VerifiedChainChanged, VerifiedChangeCause,
    VerifiedGeneration, VerifiedHeaderRef,
};

fn header_owner() -> HeaderSyncWorkOwner {
    HeaderWorkAuthority {
        header_generation: HeaderGeneration::new(2),
        branch: BranchId::new(block::Hash([1; 32]), block::Hash([2; 32])),
    }
    .bind(
        3,
        NonZeroU64::new(4).expect("the fixture request ID is nonzero"),
    )
    .into()
}

fn body_owner() -> BodyWorkOwner {
    BodyWorkAuthority {
        header: HeaderWorkAuthority {
            header_generation: HeaderGeneration::new(2),
            branch: BranchId::new(block::Hash([1; 32]), block::Hash([2; 32])),
        },
        verified_generation: VerifiedGeneration::new(5),
    }
    .bind(
        6,
        NonZeroU64::new(7).expect("the fixture request ID is nonzero"),
    )
}

fn prepared_batch(evidence: EvidenceId) -> PreparedHeaderBatch {
    let genesis = regtest_genesis_block();
    let parent = Frontier::new(block::Height(0), genesis.hash());
    let mut header = *genesis.header;
    header.previous_block_hash = parent.hash;
    header.nonce = [8; 32].into();
    let header = Arc::new(header);
    PreparedHeaderBatch::new(
        vec![PreparedHeader {
            hash: header.hash(),
            height: block::Height(1),
            block_work: header
                .difficulty_threshold
                .to_work()
                .expect("the regtest target has valid work"),
            validation: HeaderValidationState::Valid,
            header,
        }],
        parent,
        Network::new_regtest(RegtestParameters::default()),
        [9; 32],
        evidence,
    )
    .expect("the fixture batch is nonempty")
}

fn delivery(
    delivery_id: EvidenceId,
    header_hash: block::Hash,
    owner: HeaderSyncWorkOwner,
) -> AuxDelivery {
    AuxDelivery::new(
        delivery_id,
        header_hash,
        SourceId::from_digest([10; 32]),
        owner,
        BodySizeHint::Unknown,
        None,
    )
}

fn insert_event(
    evidence: EvidenceId,
    owner: HeaderSyncWorkOwner,
    completion: TargetCompletion,
    aux: Vec<AuxDelivery>,
) -> TransitionEvent {
    let batch = prepared_batch(evidence);
    let parent = batch.receipt().parent();
    let target = batch
        .headers()
        .last()
        .expect("the fixture batch is nonempty")
        .hash;
    TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
        owner,
        source: SourceId::from_digest([11; 32]),
        parent_hash: parent.hash,
        target_tip_hash: target,
        completion,
        batch,
        aux,
    }))
}

struct EventCase {
    name: &'static str,
    event: TransitionEvent,
    domain: TransitionDomain,
    admission: EventAdmission,
    idempotency: Option<EvidenceId>,
    header_owner: Option<HeaderSyncWorkOwner>,
    body_owner: Option<BodyWorkOwner>,
}

fn event_cases() -> Vec<EventCase> {
    let sync_owner = header_owner();
    let body_owner = body_owner();
    let parent = prepared_batch(EvidenceId::from_digest([12; 32]))
        .receipt()
        .parent();
    let batch_evidence = EvidenceId::from_digest([13; 32]);
    let repair_delivery_id = EvidenceId::from_digest([14; 32]);
    let repair_delivery = delivery(repair_delivery_id, block::Hash([15; 32]), body_owner.into());
    let full_state_evidence = EvidenceId::from_digest([16; 32]);
    let body_evidence = EvidenceId::from_digest([17; 32]);
    let operator_evidence = EvidenceId::from_digest([18; 32]);
    let aux_evidence = EvidenceId::from_digest([19; 32]);
    let availability = BodyUnavailableSummary::default();
    let aux_observation = AuxObservationV1::from_vct(
        body_owner,
        vec![delivery(
            aux_evidence,
            block::Hash([39; 32]),
            body_owner.into(),
        )],
        AuxVerificationFactV1::current_delivery_verified(),
        Some([40; 32].into()),
    )
    .expect("the auxiliary observation fixture is valid");
    let aux_observation_evidence =
        EvidenceId::from_digest(aux_observation.observation_id().digest());

    vec![
        EventCase {
            name: "complete header target",
            event: insert_event(
                batch_evidence,
                sync_owner,
                TargetCompletion::TargetComplete {
                    common_ancestor: parent,
                },
                Vec::new(),
            ),
            domain: TransitionDomain::InsertHeaders,
            admission: EventAdmission::RegisteredHeaderCompletion,
            idempotency: Some(batch_evidence),
            header_owner: Some(sync_owner),
            body_owner: None,
        },
        EventCase {
            name: "header target prefix",
            event: insert_event(
                batch_evidence,
                sync_owner,
                TargetCompletion::TargetPrefix {
                    common_ancestor: parent,
                },
                Vec::new(),
            ),
            domain: TransitionDomain::InsertHeaders,
            admission: EventAdmission::RegisteredHeaderCompletion,
            idempotency: Some(batch_evidence),
            header_owner: Some(sync_owner),
            body_owner: None,
        },
        EventCase {
            name: "selected auxiliary repair",
            event: insert_event(
                batch_evidence,
                body_owner.into(),
                TargetCompletion::SelectedAuxiliaryRepair {
                    common_ancestor: parent,
                    selected_target: Frontier::new(block::Height(1), block::Hash([15; 32])),
                },
                vec![repair_delivery],
            ),
            domain: TransitionDomain::InsertHeaders,
            admission: EventAdmission::RegisteredHeaderCompletion,
            idempotency: Some(repair_delivery_id),
            header_owner: Some(body_owner.into()),
            body_owner: None,
        },
        EventCase {
            name: "empty selected auxiliary repair",
            event: insert_event(
                batch_evidence,
                body_owner.into(),
                TargetCompletion::SelectedAuxiliaryRepair {
                    common_ancestor: parent,
                    selected_target: Frontier::new(block::Height(1), block::Hash([15; 32])),
                },
                Vec::new(),
            ),
            domain: TransitionDomain::InsertHeaders,
            admission: EventAdmission::RegisteredHeaderCompletion,
            idempotency: None,
            header_owner: Some(body_owner.into()),
            body_owner: None,
        },
        EventCase {
            name: "verified chain changed",
            event: TransitionEvent::VerifiedChainChanged(VerifiedChainChanged {
                full_state_transition_id: full_state_evidence,
                old_tip: parent,
                new_path: Vec::new(),
                cause: VerifiedChangeCause::Reset,
            }),
            domain: TransitionDomain::VerifiedChainChanged,
            admission: EventAdmission::IntegratedFullState,
            idempotency: Some(full_state_evidence),
            header_owner: None,
            body_owner: None,
        },
        EventCase {
            name: "verified side block",
            event: TransitionEvent::VerifiedBlockAccepted(VerifiedBlockAccepted {
                full_state_transition_id: full_state_evidence,
                path: Vec::new(),
            }),
            domain: TransitionDomain::VerifiedBlockAccepted,
            admission: EventAdmission::IntegratedFullState,
            idempotency: Some(full_state_evidence),
            header_owner: None,
            body_owner: None,
        },
        EventCase {
            name: "body payload mismatch",
            event: TransitionEvent::BodyEvidence(BodyEvidence::PayloadMismatch(
                BodyPayloadMismatch {
                    evidence: body_evidence,
                    requested: block::Hash([20; 32]),
                    delivered: block::Hash([21; 32]),
                    kind: BodyCommitmentKind::HeaderHash,
                    source: SourceId::from_digest([22; 32]),
                },
            )),
            domain: TransitionDomain::BodyPayloadMismatch,
            admission: EventAdmission::IntegratedFullState,
            idempotency: Some(body_evidence),
            header_owner: None,
            body_owner: None,
        },
        EventCase {
            name: "consensus-invalid body",
            event: TransitionEvent::BodyEvidence(BodyEvidence::ConsensusInvalid(
                ConsensusBodyInvalid {
                    hash: block::Hash([23; 32]),
                    evidence: body_evidence,
                    rule: BodyRuleId::new("body.rule"),
                    source: SourceId::from_digest([24; 32]),
                },
            )),
            domain: TransitionDomain::ConsensusBodyInvalid,
            admission: EventAdmission::IntegratedFullState,
            idempotency: Some(body_evidence),
            header_owner: None,
            body_owner: None,
        },
        EventCase {
            name: "transient body failure",
            event: TransitionEvent::BodyEvidence(BodyEvidence::Transient(TransientBodyFailure {
                hash: block::Hash([25; 32]),
                evidence: body_evidence,
                kind: TransientBodyFailureKind::Timeout,
                availability,
            })),
            domain: TransitionDomain::TransientBodyFailure,
            admission: EventAdmission::IntegratedFullState,
            idempotency: Some(body_evidence),
            header_owner: None,
            body_owner: None,
        },
        EventCase {
            name: "verified body",
            event: TransitionEvent::BodyEvidence(BodyEvidence::Verified(VerifiedBodyEvidence {
                hash: block::Hash([26; 32]),
                evidence: body_evidence,
            })),
            domain: TransitionDomain::VerifiedBody,
            admission: EventAdmission::IntegratedFullState,
            idempotency: Some(body_evidence),
            header_owner: None,
            body_owner: None,
        },
        EventCase {
            name: "body supplier discovered",
            event: TransitionEvent::BodySupplierDiscovered(BodySupplierDiscovered {
                hash: block::Hash([27; 32]),
                evidence: body_evidence,
                availability,
            }),
            domain: TransitionDomain::BodySupplierDiscovered,
            admission: EventAdmission::IntegratedFullState,
            idempotency: Some(body_evidence),
            header_owner: None,
            body_owner: None,
        },
        EventCase {
            name: "operator body retry",
            event: TransitionEvent::OperatorBodyRetry(OperatorBodyRetry {
                hash: block::Hash([28; 32]),
                evidence: operator_evidence,
                availability,
            }),
            domain: TransitionDomain::OperatorBodyRetry,
            admission: EventAdmission::RegisteredScheduler,
            idempotency: Some(operator_evidence),
            header_owner: None,
            body_owner: None,
        },
        EventCase {
            name: "operator invalidation",
            event: TransitionEvent::OperatorInvalidate(OperatorInvalidate {
                target: block::Hash([29; 32]),
                id: OperatorInvalidationId::new([30; 16]),
                operator_reason_digest: [31; 32],
                evidence: operator_evidence,
            }),
            domain: TransitionDomain::OperatorInvalidate,
            admission: EventAdmission::IntegratedFullState,
            idempotency: Some(operator_evidence),
            header_owner: None,
            body_owner: None,
        },
        EventCase {
            name: "operator reconsideration",
            event: TransitionEvent::OperatorReconsider(OperatorReconsider {
                target: block::Hash([32; 32]),
                id: OperatorInvalidationId::new([33; 16]),
                invalidation_evidence: Some(EvidenceId::from_digest([34; 32])),
                evidence: operator_evidence,
            }),
            domain: TransitionDomain::OperatorReconsider,
            admission: EventAdmission::IntegratedFullState,
            idempotency: Some(operator_evidence),
            header_owner: None,
            body_owner: None,
        },
        EventCase {
            name: "full-state finality",
            event: TransitionEvent::FullStateFinalized(FullStateFinalized {
                full_state_transition_id: full_state_evidence,
                new_finalized: Frontier::new(block::Height(2), block::Hash([35; 32])),
                verified_path_proof: vec![block::Hash([36; 32])],
            }),
            domain: TransitionDomain::FullStateFinalized,
            admission: EventAdmission::IntegratedFullState,
            idempotency: Some(full_state_evidence),
            header_owner: None,
            body_owner: None,
        },
        EventCase {
            name: "migrated pin refutation",
            event: TransitionEvent::MigratedPinRefutation(MigratedPinRefutation {
                full_state_transition_id: full_state_evidence,
                pin: Frontier::new(block::Height(3), block::Hash([37; 32])),
                invalid_header: Frontier::new(block::Height(2), block::Hash([38; 32])),
                rule: BodyRuleId::new("body.rule"),
            }),
            domain: TransitionDomain::MigratedPinRefutation,
            admission: EventAdmission::IntegratedFullState,
            idempotency: Some(full_state_evidence),
            header_owner: None,
            body_owner: None,
        },
        EventCase {
            name: "missing auxiliary observation",
            event: TransitionEvent::AuxEvidence(Box::new(AuxEvidence::missing())),
            domain: TransitionDomain::AuxEvidence,
            admission: EventAdmission::IntegratedFullState,
            idempotency: None,
            header_owner: None,
            body_owner: None,
        },
        EventCase {
            name: "authenticated auxiliary evidence",
            event: TransitionEvent::AuxEvidence(Box::new(AuxEvidence::observed(aux_observation))),
            domain: TransitionDomain::AuxEvidence,
            admission: EventAdmission::IntegratedFullState,
            idempotency: Some(aux_observation_evidence),
            header_owner: None,
            body_owner: Some(body_owner),
        },
        EventCase {
            name: "deferred reevaluation",
            event: TransitionEvent::ReevaluateDeferred,
            domain: TransitionDomain::ReevaluateDeferred,
            admission: EventAdmission::AnyMode,
            idempotency: None,
            header_owner: None,
            body_owner: None,
        },
    ]
}

#[test]
fn auxiliary_outcomes_only_refine_evidence() {
    let unauthenticated = AuxOutcome::unauthenticated();
    let disputed = AuxOutcome::derived(
        unauthenticated,
        AuxOutcomeStatus::Disputed,
        AuxObservationId::from_digest([1; 32]),
        block::Hash([3; 32]),
    );
    for next in [
        AuxOutcomeStatus::Disputed,
        AuxOutcomeStatus::Authenticated,
        AuxOutcomeStatus::Rejected,
    ] {
        assert!(unauthenticated.can_refine_to(next));
    }
    assert!(disputed.can_refine_to(AuxOutcomeStatus::Authenticated));
    assert!(disputed.can_refine_to(AuxOutcomeStatus::Rejected));
    assert!(!disputed.can_refine_to(AuxOutcomeStatus::Unauthenticated));
    assert!(!disputed.can_refine_to(AuxOutcomeStatus::Disputed));
}

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
fn transition_event_contract_matrix_is_exhaustive() {
    for case in event_cases() {
        assert_eq!(case.event.domain(), case.domain, "{} domain", case.name);
        assert_eq!(
            case.event.admission(),
            case.admission,
            "{} admission",
            case.name
        );
        assert_eq!(
            case.event.idempotency_key(),
            case.idempotency,
            "{} idempotency",
            case.name
        );
        assert_eq!(
            case.event.header_sync_owner(),
            case.header_owner,
            "{} header owner",
            case.name
        );
        assert_eq!(
            case.event.body_owner(),
            case.body_owner,
            "{} body owner",
            case.name
        );

        match (case.event.fingerprint(), case.idempotency) {
            (Some(fingerprint), Some(evidence)) => {
                assert_eq!(
                    fingerprint.domain(),
                    case.domain,
                    "{} fingerprint",
                    case.name
                );
                assert_eq!(
                    fingerprint.evidence(),
                    evidence,
                    "{} fingerprint",
                    case.name
                );
                assert_ne!(
                    fingerprint.payload_digest(),
                    [0; 32],
                    "{} fingerprint",
                    case.name
                );
                assert_eq!(
                    case.event.fingerprint(),
                    case.event.clone().fingerprint(),
                    "{} canonical fingerprint",
                    case.name
                );
            }
            (None, None) => {}
            (actual, expected) => panic!(
                "{} fingerprint presence disagrees: actual={actual:?}, idempotency={expected:?}",
                case.name
            ),
        }
    }
}

fn mutate_effect_bearing_payload(event: &mut TransitionEvent) {
    let mutate_hash = |hash: &mut block::Hash| hash.0[0] ^= 0xff;
    match event {
        TransitionEvent::InsertHeaders(event) => mutate_hash(&mut event.target_tip_hash),
        TransitionEvent::VerifiedChainChanged(event) => mutate_hash(&mut event.old_tip.hash),
        TransitionEvent::VerifiedBlockAccepted(event) => {
            let header = regtest_genesis_block().header.clone();
            event.path.push(VerifiedHeaderRef {
                height: block::Height(0),
                hash: header.hash(),
                header,
            });
        }
        TransitionEvent::BodyEvidence(BodyEvidence::PayloadMismatch(event)) => {
            mutate_hash(&mut event.delivered);
        }
        TransitionEvent::BodyEvidence(BodyEvidence::ConsensusInvalid(event)) => {
            mutate_hash(&mut event.hash);
        }
        TransitionEvent::BodyEvidence(BodyEvidence::Transient(event)) => {
            mutate_hash(&mut event.hash);
        }
        TransitionEvent::BodyEvidence(BodyEvidence::Verified(event)) => {
            mutate_hash(&mut event.hash);
        }
        TransitionEvent::BodySupplierDiscovered(event) => mutate_hash(&mut event.hash),
        TransitionEvent::OperatorBodyRetry(event) => mutate_hash(&mut event.hash),
        TransitionEvent::OperatorInvalidate(event) => mutate_hash(&mut event.target),
        TransitionEvent::OperatorReconsider(event) => mutate_hash(&mut event.target),
        TransitionEvent::FullStateFinalized(event) => mutate_hash(&mut event.new_finalized.hash),
        TransitionEvent::MigratedPinRefutation(event) => {
            mutate_hash(&mut event.invalid_header.hash);
        }
        TransitionEvent::AuxEvidence(event) => {
            let observation = event
                .observation()
                .expect("fingerprinted auxiliary events have an observation");
            let mut deliveries = observation.deliveries().to_vec();
            deliveries[0].delivery_id = EvidenceId::from_digest([40; 32]);
            **event = AuxEvidence::observed(
                AuxObservationV1::from_vct(
                    observation.owner(),
                    deliveries,
                    observation.verification(),
                    observation.boundary_witness(),
                )
                .expect("the mutated auxiliary observation is valid"),
            );
        }
        TransitionEvent::ReevaluateDeferred => {
            panic!("deferred reevaluation has no effect-bearing fingerprint")
        }
    }
}

#[test]
fn transition_fingerprint_binds_every_effect_bearing_domain() {
    let mut exercised = [false; 15];

    for case in event_cases() {
        let Some(original) = case.event.fingerprint() else {
            continue;
        };
        let code = usize::from(case.domain.code());
        if exercised[code] {
            continue;
        }

        let mut mutated_event = case.event.clone();
        mutate_effect_bearing_payload(&mut mutated_event);
        let mutated = mutated_event
            .fingerprint()
            .expect("a payload mutation preserves replay identity");
        if case.domain == TransitionDomain::AuxEvidence {
            assert_ne!(
                original.evidence(),
                mutated.evidence(),
                "{} evidence",
                case.name
            );
            assert_eq!(original.domain(), mutated.domain(), "{} domain", case.name);
            assert!(!original.conflicts_with(mutated));
            assert!(!original.conflicts_with(original), "{}", case.name);
            exercised[code] = true;
            continue;
        }
        assert_eq!(
            original.evidence(),
            mutated.evidence(),
            "{} evidence",
            case.name
        );
        assert_eq!(original.domain(), mutated.domain(), "{} domain", case.name);
        assert!(
            original.conflicts_with(mutated),
            "{} payload mutation must conflict",
            case.name
        );
        assert!(!original.conflicts_with(original), "{}", case.name);
        exercised[code] = true;
    }

    for domain in [
        TransitionDomain::InsertHeaders,
        TransitionDomain::VerifiedChainChanged,
        TransitionDomain::VerifiedBlockAccepted,
        TransitionDomain::BodyPayloadMismatch,
        TransitionDomain::ConsensusBodyInvalid,
        TransitionDomain::TransientBodyFailure,
        TransitionDomain::VerifiedBody,
        TransitionDomain::BodySupplierDiscovered,
        TransitionDomain::OperatorBodyRetry,
        TransitionDomain::OperatorInvalidate,
        TransitionDomain::OperatorReconsider,
        TransitionDomain::FullStateFinalized,
        TransitionDomain::MigratedPinRefutation,
        TransitionDomain::AuxEvidence,
    ] {
        assert!(
            exercised[usize::from(domain.code())],
            "{domain:?} must have a fingerprint conflict case"
        );
    }
    assert!(!exercised[usize::from(TransitionDomain::ReevaluateDeferred.code())]);
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
