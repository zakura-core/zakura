use super::*;

#[test]
fn transient_body_evidence_cannot_regress_a_verified_body() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    store
        .graph
        .set_body_state(
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
                "body retry evidence cannot regress an already verified body"
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
        .set_body_state(selected.hash, BodyValidationState::Unavailable(old))
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
    assert_eq!(plan.change_set.metadata.frontiers, store.metadata.frontiers);
    assert_eq!(
        plan.projected
            .node(selected.hash)
            .expect("the selected node remains retained")
            .body_validation_state,
        BodyValidationState::Unavailable(updated)
    );
    assert_eq!(
        plan.projected
            .node(selected.hash)
            .expect("the selected node remains retained")
            .eligibility,
        store
            .graph
            .node(selected.hash)
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
        .set_body_state(selected.hash, BodyValidationState::Unavailable(old))
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
            "body supplier discovery does not add an eligible supplier"
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
            "body supplier discovery must preserve the persistent retry episode"
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
            "body supplier discovery must preserve the persistent retry episode"
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
            "body supplier discovery does not add an eligible supplier"
        ))
    ));
}
