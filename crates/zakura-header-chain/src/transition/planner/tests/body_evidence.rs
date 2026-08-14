use super::*;
use crate::BodyRuleId;

#[test]
fn transient_body_evidence_rejects_each_malformed_episode_shape() {
    let (store, config) = TestStore::new(EngineMode::Integrated);
    let now = Utc::now();
    let clock = ManualClock(now);
    let baseline = crate::BodyUnavailableSummary {
        started_at: now,
        attempts: 1,
        suppliers: 1,
        supplier_set_digest: [0x21; 32],
        alarmed: false,
        next_probe_at: now,
    };
    let malformed = [
        (
            "zero attempts",
            crate::BodyUnavailableSummary {
                attempts: 0,
                ..baseline
            },
        ),
        (
            "zero suppliers",
            crate::BodyUnavailableSummary {
                suppliers: 0,
                ..baseline
            },
        ),
        (
            "probe before episode start",
            crate::BodyUnavailableSummary {
                next_probe_at: now - chrono::Duration::seconds(1),
                ..baseline
            },
        ),
    ];

    for (label, availability) in malformed {
        let result = apply_transition(
            &store,
            TransitionRequest {
                expected_version: store.metadata.state_version,
                event: TransitionEvent::BodyEvidence(BodyEvidence::Transient(
                    crate::TransientBodyFailure {
                        hash: store.metadata.frontiers.header_best.hash,
                        evidence: EvidenceId::from_digest([0x22; 32]),
                        kind: crate::TransientBodyFailureKind::Timeout,
                        availability,
                    },
                )),
            },
            &context(&config, &clock, Some(&Authority)),
        );
        assert!(
            matches!(
                result,
                Err(TransitionFailure::InvalidEvidence(
                    InvalidTransitionEvidence::Body(BodyViolation::InvalidTransientEpisode)
                ))
            ),
            "{label}: {result:?}"
        );
    }
}

#[test]
fn invalidating_a_losing_body_advances_header_generation_without_a_reason_delta() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let authority = Authority;
    let anchor = store.graph.finalized_frontier();
    let difficulty = store
        .graph
        .header_node(anchor.hash)
        .expect("the anchor exists")
        .header
        .difficulty_threshold;
    let first = insert_verified_branch(&mut store.graph, anchor, 1, difficulty, 0x31);
    let second = insert_verified_branch(&mut store.graph, anchor, 1, difficulty, 0x32);
    let selected = store
        .graph
        .select_best_header_chain()
        .expect("the fixture has an eligible tip")
        .0;
    let losing = if selected == first { second } else { first };
    store
        .graph
        .set_body_validation_state(losing.hash, BodyValidationState::Unknown)
        .expect("the losing body remains unverified");
    synchronize_fixture(&mut store, anchor);
    let before = store.snapshot();
    let invalid_evidence = EvidenceId::from_digest([0x33; 32]);

    let plan = apply_transition(
        &store,
        TransitionRequest {
            expected_version: before.state_version,
            event: TransitionEvent::BodyEvidence(BodyEvidence::ConsensusInvalid(
                crate::ConsensusBodyInvalid {
                    hash: losing.hash,
                    evidence: invalid_evidence,
                    rule: BodyRuleId::new("test.losing-body-invalid"),
                    source: SourceId::from_digest([0x34; 32]),
                },
            )),
        },
        &context(&config, &clock, Some(&authority)),
    )
    .expect("authenticated invalidity excludes the losing branch");

    assert_eq!(plan.change_set.metadata.frontiers.header_best, selected);
    assert_eq!(
        plan.change_set.selected_projection,
        ProjectionDelta::default()
    );
    assert!(plan.change_set.eligibility_changes.is_empty());
    assert_eq!(
        plan.change_set.metadata.header_generation,
        before
            .header_generation
            .checked_next()
            .expect("the fixture generation has capacity")
    );
    let projected_graph = projected_graph(&store.graph, &plan);
    let changed = projected_graph
        .header_node(losing.hash)
        .expect("the invalid losing node remains retained");
    assert_eq!(
        changed.body_validation_state,
        BodyValidationState::ConsensusInvalid {
            evidence: invalid_evidence,
            rule: BodyRuleId::new("test.losing-body-invalid"),
        }
    );
    assert!(changed.eligibility.direct_reasons.is_empty());
}

#[test]
fn transient_body_evidence_cannot_regress_a_verified_body() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    store
        .graph
        .set_body_validation_state(
            store.metadata.frontiers.verified_best.hash,
            BodyValidationState::Verified {
                evidence: EvidenceId::from_digest([0xbf; 32]),
            },
        )
        .expect("the fixture body becomes verified");
    let request = TransitionRequest {
        expected_version: store.metadata.state_version,
        event: TransitionEvent::BodyEvidence(BodyEvidence::Transient(
            crate::TransientBodyFailure {
                hash: store.metadata.frontiers.verified_best.hash,
                evidence: EvidenceId::from_digest([0xc0; 32]),
                kind: crate::TransientBodyFailureKind::Timeout,
                availability: crate::BodyUnavailableSummary {
                    attempts: 1,
                    suppliers: 1,
                    alarmed: false,
                    ..Default::default()
                },
            },
        )),
    };

    let result = apply_transition(&store, request, &context(&config, &clock, Some(&Authority)));
    assert!(
        matches!(
            result,
            Err(TransitionFailure::InvalidEvidence(
                InvalidTransitionEvidence::Body(BodyViolation::RetryConflictsWithVerified)
            ))
        ),
        "unexpected transition result: {result:?}"
    );
}

#[test]
fn new_body_supplier_preserves_only_the_selected_persistent_alarm() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let now = Utc::now();
    let clock = ManualClock(now);
    let selected = store.metadata.frontiers.header_best;
    let old = crate::BodyUnavailableSummary {
        started_at: now - chrono::Duration::minutes(12),
        attempts: 10,
        suppliers: 2,
        supplier_set_digest: [0x11; 32],
        alarmed: true,
        next_probe_at: now + chrono::Duration::minutes(8),
    };
    store
        .graph
        .set_body_validation_state(selected.hash, BodyValidationState::Unavailable(old))
        .expect("the selected fixture body exists");
    store.metadata.alarms.header_best_body_unavailable = Some(old);
    let updated = crate::BodyUnavailableSummary {
        started_at: old.started_at,
        attempts: old.attempts,
        suppliers: 2,
        supplier_set_digest: [0x22; 32],
        alarmed: true,
        next_probe_at: now,
    };
    let evidence = EvidenceId::from_digest([0xc1; 32]);
    let request = TransitionRequest {
        expected_version: store.metadata.state_version,
        event: TransitionEvent::BodySupplierDiscovered(crate::BodySupplierDiscovered {
            hash: selected.hash,
            evidence,
            availability: updated,
        }),
    };

    let plan = apply_transition(
        &store,
        request.clone(),
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("a changed supplier set makes the persistent episode probeable");
    let projected_graph = projected_graph(&store.graph, &plan);
    assert_eq!(plan.change_set.metadata.frontiers, store.metadata.frontiers);
    assert_eq!(
        projected_graph
            .header_node(selected.hash)
            .expect("the selected node remains retained")
            .body_validation_state,
        BodyValidationState::Unavailable(updated)
    );
    assert_eq!(
        projected_graph
            .header_node(selected.hash)
            .expect("the selected node remains retained")
            .eligibility,
        store
            .graph
            .header_node(selected.hash)
            .expect("the selected fixture node exists")
            .eligibility
    );
    assert_eq!(
        plan.change_set.metadata.alarms.header_best_body_unavailable,
        Some(updated)
    );
    store.commit(&plan);
    let replay = apply_transition(
        &store,
        TransitionRequest {
            expected_version: store.metadata.state_version,
            ..request
        },
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("the exact supplier-discovery evidence replays idempotently");
    assert!(replay.is_no_change());
}

#[test]
fn body_supplier_discovery_rejects_reset_or_nonexpanding_evidence() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let now = Utc::now();
    let clock = ManualClock(now);
    let selected = store.metadata.frontiers.header_best;
    let old = crate::BodyUnavailableSummary {
        started_at: now - chrono::Duration::minutes(12),
        attempts: 10,
        suppliers: 2,
        supplier_set_digest: [0x11; 32],
        alarmed: true,
        next_probe_at: now + chrono::Duration::minutes(8),
    };
    store
        .graph
        .set_body_validation_state(selected.hash, BodyValidationState::Unavailable(old))
        .expect("the selected fixture body exists");
    store.metadata.alarms.header_best_body_unavailable = Some(old);
    let apply = |availability| {
        apply_transition(
            &store,
            TransitionRequest {
                expected_version: store.metadata.state_version,
                event: TransitionEvent::BodySupplierDiscovered(crate::BodySupplierDiscovered {
                    hash: selected.hash,
                    evidence: EvidenceId::from_digest([0xc2; 32]),
                    availability,
                }),
            },
            &context(&config, &clock, Some(&Authority)),
        )
    };

    assert!(matches!(
        apply(crate::BodyUnavailableSummary {
            started_at: old.started_at,
            attempts: old.attempts,
            suppliers: 2,
            supplier_set_digest: old.supplier_set_digest,
            alarmed: true,
            next_probe_at: now,
        }),
        Err(TransitionFailure::InvalidEvidence(
            InvalidTransitionEvidence::Body(BodyViolation::NoNewSupplier)
        ))
    ));
    assert!(matches!(
        apply(crate::BodyUnavailableSummary {
            started_at: now,
            attempts: old.attempts,
            suppliers: 3,
            supplier_set_digest: [0x22; 32],
            alarmed: true,
            next_probe_at: now,
        }),
        Err(TransitionFailure::InvalidEvidence(
            InvalidTransitionEvidence::Body(BodyViolation::SupplierEpisodeChanged)
        ))
    ));
    assert!(matches!(
        apply(crate::BodyUnavailableSummary {
            started_at: old.started_at,
            attempts: old.attempts,
            suppliers: 3,
            supplier_set_digest: [0x22; 32],
            alarmed: true,
            next_probe_at: now + chrono::Duration::minutes(1),
        }),
        Err(TransitionFailure::InvalidEvidence(
            InvalidTransitionEvidence::Body(BodyViolation::SupplierEpisodeChanged)
        ))
    ));
    assert!(matches!(
        apply(crate::BodyUnavailableSummary {
            started_at: old.started_at,
            attempts: old.attempts,
            suppliers: 1,
            supplier_set_digest: [0x22; 32],
            alarmed: true,
            next_probe_at: now,
        }),
        Err(TransitionFailure::InvalidEvidence(
            InvalidTransitionEvidence::Body(BodyViolation::NoNewSupplier)
        ))
    ));
}
