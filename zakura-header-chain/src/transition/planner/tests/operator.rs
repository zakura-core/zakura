use super::*;

#[test]
// AUD-10/AUD-12: invalidation must reselect eligible work without erasing
// independent exclusion reasons; this fixture exercises both in one graph.
fn operator_invalidation_promotes_alternate_and_preserves_nested_reasons() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let anchor = store.graph.finalized();
    let difficulty = store
        .graph
        .node(anchor.hash)
        .expect("the anchor exists")
        .header
        .difficulty_threshold;
    let left = insert_verified_branch(&mut store.graph, anchor, 1, difficulty, 0x11);
    let right = insert_verified_branch(&mut store.graph, anchor, 1, difficulty, 0x22);
    let (winner, loser) = if store.graph.score(left.hash).expect("left score exists")
        > store.graph.score(right.hash).expect("right score exists")
    {
        (left, right)
    } else {
        (right, left)
    };
    synchronize_fixture(&mut store, winner);
    let first_id = crate::OperatorInvalidationId::new([1; 16]);
    let second_id = crate::OperatorInvalidationId::new([2; 16]);

    let first = apply_transition(
        &store,
        operator_invalidate(&store, winner.hash, first_id, 0x31),
        &context(&config, &clock, None),
    )
    .expect("invalidating the winning verified fork reselects atomically");
    assert_eq!(first.change_set.metadata.frontiers.header_best, loser);
    assert_eq!(first.change_set.metadata.frontiers.verified_best, loser);
    assert_eq!(
        first.change_set.metadata.header_generation,
        HeaderGeneration::new(1)
    );
    assert_eq!(
        first.change_set.metadata.verified_generation,
        VerifiedGeneration::new(1)
    );
    store.commit(&first);

    assert_next_child_commits(&store, &config, &clock, loser, 0x36);

    let second = apply_transition(
        &store,
        operator_invalidate(&store, winner.hash, second_id, 0x32),
        &context(&config, &clock, None),
    )
    .expect("a nested operator reason is independently durable");
    store.commit(&second);
    let reconsider_first = apply_transition(
        &store,
        operator_reconsider(&store, winner.hash, first_id, 0x33),
        &context(&config, &clock, None),
    )
    .expect("reconsider removes only the named reason");
    assert_eq!(
        reconsider_first.change_set.metadata.frontiers.verified_best,
        loser
    );
    let winner_node = reconsider_first
        .projected_graph()
        .node(winner.hash)
        .expect("the losing node remains retained");
    assert!(!winner_node
        .eligibility
        .direct_reasons
        .contains(&EligibilityReason::OperatorInvalid { id: first_id }));
    assert!(winner_node
        .eligibility
        .direct_reasons
        .contains(&EligibilityReason::OperatorInvalid { id: second_id }));
    store.commit(&reconsider_first);

    let reconsider_second = apply_transition(
        &store,
        operator_reconsider(&store, winner.hash, second_id, 0x34),
        &context(&config, &clock, None),
    )
    .expect("removing the final operator reason restores both frontiers");
    assert_eq!(
        reconsider_second.change_set.metadata.frontiers.header_best,
        winner
    );
    assert_eq!(
        reconsider_second
            .change_set
            .metadata
            .frontiers
            .verified_best,
        winner
    );
    store.commit(&reconsider_second);
    let absent = apply_transition(
        &store,
        operator_reconsider(&store, winner.hash, second_id, 0x35),
        &context(&config, &clock, None),
    )
    .expect("an absent operator ID is a valid no-change");
    assert!(absent.is_no_change());
}

#[test]
// AUD-12: reconsidering the exact reason is sufficient only when the
// restored branch is again eligible, after which cumulative work wins.
fn operator_reconsider_restores_shorter_higher_work_verified_branch() {
    use zakura_chain::work::difficulty::{ExpandedDifficulty, U256};

    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let anchor = store.graph.finalized();
    let easy = store
        .graph
        .node(anchor.hash)
        .expect("the anchor exists")
        .header
        .difficulty_threshold;
    let easy_target: U256 = easy
        .to_expanded()
        .expect("the fixture target expands")
        .into();
    let hard = ExpandedDifficulty::from(easy_target >> 3).into();
    let longer = insert_verified_branch(&mut store.graph, anchor, 2, easy, 0x41);
    let shorter = insert_verified_branch(&mut store.graph, anchor, 1, hard, 0x42);
    assert!(
        store.graph.score(shorter.hash).expect("short score exists")
            > store.graph.score(longer.hash).expect("long score exists")
    );
    synchronize_fixture(&mut store, shorter);
    let id = crate::OperatorInvalidationId::new([3; 16]);

    let invalidate = apply_transition(
        &store,
        operator_invalidate(&store, shorter.hash, id, 0x43),
        &context(&config, &clock, None),
    )
    .expect("invalidating the shorter winner promotes the longer branch");
    assert_eq!(invalidate.change_set.metadata.frontiers.header_best, longer);
    assert_eq!(
        invalidate.change_set.metadata.frontiers.verified_best,
        longer
    );
    store.commit(&invalidate);

    let reconsider = apply_transition(
        &store,
        operator_reconsider(&store, shorter.hash, id, 0x44),
        &context(&config, &clock, None),
    )
    .expect("reconsider restores the shorter higher-work branch");
    assert_eq!(
        reconsider.change_set.metadata.frontiers.header_best,
        shorter
    );
    assert_eq!(
        reconsider.change_set.metadata.frontiers.verified_best,
        shorter
    );
    let mut reconsidered = store.clone();
    reconsidered.commit(&reconsider);
    assert_next_child_commits(&reconsidered, &config, &clock, shorter, 0x45);
}

#[test]
// AUD-11: verified-body eligibility and header fork choice are independent,
// so invalidating the verified path must not discard a valid header winner.
fn invalidating_verified_path_preserves_independent_header_best() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let anchor = store.graph.finalized();
    let difficulty = store
        .graph
        .node(anchor.hash)
        .expect("the anchor exists")
        .header
        .difficulty_threshold;
    let verified_tip = insert_verified_branch(&mut store.graph, anchor, 2, difficulty, 0x51);
    let header_tip = insert_verified_branch(&mut store.graph, anchor, 3, difficulty, 0x53);
    for frontier in path(&store.graph, header_tip)
        .expect("the independent header path is retained")
        .into_iter()
        .skip(1)
    {
        store
            .graph
            .set_body_state(frontier.hash, BodyValidationState::Unknown)
            .expect("the independent candidate deliberately has no verified body");
    }
    synchronize_fixture(&mut store, verified_tip);
    assert_eq!(store.metadata.frontiers.header_best, header_tip);
    assert_eq!(store.metadata.frontiers.verified_best, verified_tip);

    let plan = apply_transition(
        &store,
        operator_invalidate(
            &store,
            store.verified[1].hash,
            crate::OperatorInvalidationId::new([4; 16]),
            0x52,
        ),
        &context(&config, &clock, None),
    )
    .expect("invalidating the only full-state branch falls back atomically");
    assert_eq!(
        plan.change_set.metadata.frontiers.header_best, header_tip,
        "the independently eligible header branch remains selected"
    );
    assert_eq!(plan.change_set.metadata.frontiers.verified_best, anchor);
    assert_eq!(
        plan.change_set.verified_projection,
        ProjectionDelta {
            remove_before: None,
            remove_from: Some(block::Height(1)),
            put: Vec::new(),
        }
    );
    let mut invalidated = store.clone();
    invalidated.commit(&plan);
    assert_next_child_commits(&invalidated, &config, &clock, header_tip, 0x54);
}

#[test]
fn operator_body_retry_restarts_the_selected_alarm_with_the_same_suppliers() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let now = Utc::now();
    let clock = ManualClock(now);
    let selected = store.metadata.frontiers.header_best;
    let old = crate::BodyUnavailableSummary {
        started_at: now - chrono::Duration::minutes(12),
        attempts: 10,
        suppliers: 2,
        supplier_set_digest: [0x31; 32],
        alarmed: true,
        next_probe_at: now + chrono::Duration::minutes(8),
    };
    store
        .graph
        .set_body_state(selected.hash, BodyValidationState::Unavailable(old))
        .expect("the selected fixture body exists");
    store.metadata.alarms.header_best_body_unavailable = Some(old);
    let fresh = crate::BodyUnavailableSummary {
        started_at: now,
        attempts: 0,
        suppliers: old.suppliers,
        supplier_set_digest: old.supplier_set_digest,
        alarmed: false,
        next_probe_at: now,
    };
    let request = TransitionRequest {
        expected_version: store.metadata.state_version,
        event: TransitionEvent::OperatorBodyRetry(crate::OperatorBodyRetry {
            hash: selected.hash,
            evidence: EvidenceId::from_digest([0xc3; 32]),
            availability: fresh,
        }),
    };

    let plan = apply_transition(&store, request.clone(), &context(&config, &clock, None))
        .expect("an authenticated operator can restart the same supplier set");
    assert_eq!(plan.change_set.metadata.frontiers, store.metadata.frontiers);
    assert_eq!(
        plan.change_set.metadata.header_generation,
        store.metadata.header_generation
    );
    assert_eq!(
        plan.change_set.metadata.verified_generation,
        store.metadata.verified_generation
    );
    assert_eq!(
        plan.projected
            .node(selected.hash)
            .expect("the selected node remains retained")
            .body,
        BodyValidationState::Unavailable(fresh)
    );
    assert_eq!(
        plan.change_set.metadata.alarms.header_best_body_unavailable,
        None
    );
    store.commit(&plan);
    let replay = apply_transition(
        &store,
        TransitionRequest {
            expected_version: store.metadata.state_version,
            ..request
        },
        &context(&config, &clock, None),
    )
    .expect("the exact operator evidence replays idempotently");
    assert!(replay.is_no_change());
}

#[test]
fn operator_body_retry_rejects_stale_or_malformed_requests() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let now = Utc::now();
    let clock = ManualClock(now);
    let selected = store.metadata.frontiers.header_best;
    let old = crate::BodyUnavailableSummary {
        attempts: 10,
        suppliers: 2,
        supplier_set_digest: [0x41; 32],
        alarmed: true,
        ..Default::default()
    };
    store
        .graph
        .set_body_state(selected.hash, BodyValidationState::Unavailable(old))
        .expect("the selected fixture body exists");
    store.metadata.alarms.header_best_body_unavailable = Some(old);
    let fresh = crate::BodyUnavailableSummary {
        started_at: now,
        attempts: 0,
        suppliers: 2,
        supplier_set_digest: old.supplier_set_digest,
        alarmed: false,
        next_probe_at: now,
    };
    let apply = |hash, availability| {
        apply_transition(
            &store,
            TransitionRequest {
                expected_version: store.metadata.state_version,
                event: TransitionEvent::OperatorBodyRetry(crate::OperatorBodyRetry {
                    hash,
                    evidence: EvidenceId::from_digest([0xc4; 32]),
                    availability,
                }),
            },
            &context(&config, &clock, None),
        )
    };

    assert!(matches!(
        apply(block::Hash([0x42; 32]), fresh),
        Err(TransitionFailure::InvalidEvidence(
            "operator body retry has an invalid fresh episode"
        ))
    ));
    assert!(matches!(
        apply(
            selected.hash,
            crate::BodyUnavailableSummary {
                attempts: 1,
                ..fresh
            }
        ),
        Err(TransitionFailure::InvalidEvidence(
            "operator body retry has an invalid fresh episode"
        ))
    ));
    store
        .graph
        .set_body_state(selected.hash, BodyValidationState::Unknown)
        .expect("the selected fixture body exists");
    let non_alarmed = apply_transition(
        &store,
        TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::OperatorBodyRetry(crate::OperatorBodyRetry {
                hash: selected.hash,
                evidence: EvidenceId::from_digest([0xc4; 32]),
                availability: fresh,
            }),
        },
        &context(&config, &clock, None),
    );
    assert!(matches!(
        non_alarmed,
        Err(TransitionFailure::InvalidEvidence(
            "operator body retry requires the selected persistent alarm"
        ))
    ));
}
