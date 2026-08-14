use super::super::projected_state::path;
use super::*;

#[test]
fn operator_invalidation_rejects_the_finalized_anchor() {
    let (store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let anchor = store.graph.finalized_frontier();

    let err = apply_transition(
        &store,
        operator_invalidate(
            &store,
            anchor.hash,
            crate::OperatorInvalidationId::new([0xa1; 16]),
            0xa2,
        ),
        &context(&config, &clock, Some(&Authority)),
    )
    .expect_err("invalidating the finalized anchor must fail before any graph edit");
    assert_eq!(
        err,
        TransitionFailure::InvalidEvidence(InvalidTransitionEvidence::Operator(
            OperatorViolation::FinalizedAnchorTarget
        ))
    );
}

#[test]
// AUD-10/AUD-12: invalidation must reselect eligible work without erasing
// independent exclusion reasons. This fixture exercises both rules in one graph.
fn operator_invalidation_promotes_alternate_and_preserves_nested_reasons() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let anchor = store.graph.finalized_frontier();
    let difficulty = store
        .graph
        .header_node(anchor.hash)
        .expect("the anchor exists")
        .header
        .difficulty_threshold;
    let left = insert_verified_branch(&mut store.graph, anchor, 1, difficulty, 0x11);
    let right = insert_verified_branch(&mut store.graph, anchor, 1, difficulty, 0x22);
    let (winner, loser) = if store
        .graph
        .header_chain_score(left.hash)
        .expect("left score exists")
        > store
            .graph
            .header_chain_score(right.hash)
            .expect("right score exists")
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
        &context(&config, &clock, Some(&Authority)),
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
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("a nested operator reason is independently durable");
    store.commit(&second);
    let reconsider_first = apply_transition(
        &store,
        operator_reconsider(&store, winner.hash, first_id, 0x33),
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("reconsider removes only the named reason");
    assert_eq!(
        reconsider_first.change_set.metadata.frontiers.verified_best,
        loser
    );
    let reconsidered_graph = projected_graph(&store.graph, &reconsider_first);
    let winner_node = reconsidered_graph
        .header_node(winner.hash)
        .expect("the losing node remains retained");
    assert!(!winner_node.eligibility.direct_reasons.iter().any(
        |reason| matches!(reason, EligibilityReason::OperatorInvalid { id, .. } if *id == first_id)
    ));
    assert!(winner_node.eligibility.direct_reasons.iter().any(
        |reason| matches!(reason, EligibilityReason::OperatorInvalid { id, .. } if *id == second_id)
    ));
    store.commit(&reconsider_first);

    let reconsider_second = apply_transition(
        &store,
        operator_reconsider(&store, winner.hash, second_id, 0x34),
        &context(&config, &clock, Some(&Authority)),
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
        &context(&config, &clock, Some(&Authority)),
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
    let anchor = store.graph.finalized_frontier();
    let easy = store
        .graph
        .header_node(anchor.hash)
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
        store
            .graph
            .header_chain_score(shorter.hash)
            .expect("short score exists")
            > store
                .graph
                .header_chain_score(longer.hash)
                .expect("long score exists")
    );
    synchronize_fixture(&mut store, shorter);
    let id = crate::OperatorInvalidationId::new([3; 16]);

    let invalidate = apply_transition(
        &store,
        operator_invalidate(&store, shorter.hash, id, 0x43),
        &context(&config, &clock, Some(&Authority)),
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
        &context(&config, &clock, Some(&Authority)),
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
fn replay_identity_is_domain_payload_and_authority_bound() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let anchor = store.graph.finalized_frontier();
    let difficulty = store
        .graph
        .header_node(anchor.hash)
        .expect("the anchor exists")
        .header
        .difficulty_threshold;
    let tip = insert_verified_branch(&mut store.graph, anchor, 1, difficulty, 0x60);
    synchronize_fixture(&mut store, tip);
    let target = tip.hash;
    let id = crate::OperatorInvalidationId::new([0x61; 16]);
    let request = operator_invalidate(&store, target, id, 0x62);
    let committed = apply_transition(
        &store,
        request.clone(),
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("the original authenticated action commits");
    store.commit(&committed);

    let exact = apply_transition(
        &store,
        request.clone(),
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("an exact stale-version replay is idempotent");
    assert!(exact.is_no_change());

    assert!(matches!(
        apply_transition(&store, request.clone(), &context(&config, &clock, None)),
        Err(TransitionFailure::Authority)
    ));

    let mut conflicting = request.clone();
    let TransitionEvent::OperatorInvalidate(event) = &mut conflicting.event else {
        unreachable!("the fixture is an operator invalidation");
    };
    event.operator_reason_digest[0] ^= 1;
    assert!(matches!(
        apply_transition(
            &store,
            conflicting,
            &context(&config, &clock, Some(&Authority)),
        ),
        Err(TransitionFailure::ConflictingReplay)
    ));

    let reconsider = TransitionRequest {
        expected_version: store.metadata.state_version,
        event: TransitionEvent::OperatorReconsider(crate::OperatorReconsider {
            target,
            id,
            invalidation_evidence: request.event.idempotency_key(),
            evidence: request
                .event
                .idempotency_key()
                .expect("the invalidation has replay evidence"),
        }),
    };
    let cross_domain = apply_transition(
        &store,
        reconsider,
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("the same raw evidence in another event domain is not a replay");
    assert!(!cross_domain.is_no_change());
}

#[test]
fn replay_protection_covers_only_the_adjacent_state_changing_transition() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let anchor = store.graph.finalized_frontier();
    let difficulty = store
        .graph
        .header_node(anchor.hash)
        .expect("the anchor exists")
        .header
        .difficulty_threshold;
    let tip = insert_verified_branch(&mut store.graph, anchor, 1, difficulty, 0x80);
    synchronize_fixture(&mut store, tip);
    let target = tip.hash;
    let id = crate::OperatorInvalidationId::new([0x81; 16]);
    let invalidate = operator_invalidate(&store, target, id, 0x82);
    let committed = apply_transition(
        &store,
        invalidate.clone(),
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("the original authenticated action commits");
    store.commit(&committed);

    let adjacent = apply_transition(
        &store,
        invalidate.clone(),
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("an exact adjacent replay is short-circuited");
    assert!(adjacent.is_no_change());

    let reconsider = operator_reconsider(&store, target, id, 0x83);
    let intervening = apply_transition(
        &store,
        reconsider,
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("an intervening state-changing transition commits");
    store.commit(&intervening);
    assert_eq!(
        store
            .metadata
            .last_transition
            .expect("the intervening transition is replay protected")
            .evidence(),
        EvidenceId::from_digest([0x83; 32]),
    );

    let mut older = invalidate;
    older.expected_version = store.metadata.state_version;
    let replayed = apply_transition(&store, older, &context(&config, &clock, Some(&Authority)))
        .expect("an older event replans after an intervening transition");
    assert!(
        !replayed.is_no_change(),
        "one-slot replay protection does not short-circuit older events"
    );
    store.commit(&replayed);
    assert_eq!(
        store
            .metadata
            .last_transition
            .expect("the replanned older event becomes the adjacent slot")
            .evidence(),
        EvidenceId::from_digest([0x82; 32]),
    );
}

#[test]
fn full_state_authority_is_bound_to_the_complete_event_payload() {
    struct ExactAuthority(crate::TransitionFingerprint);

    impl crate::FullStateEvidenceAuthority for ExactAuthority {
        fn authorizes_full_state(&self, event: &TransitionEvent) -> bool {
            event.fingerprint() == Some(self.0)
        }
    }

    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let anchor = store.graph.finalized_frontier();
    let difficulty = store
        .graph
        .header_node(anchor.hash)
        .expect("the anchor exists")
        .header
        .difficulty_threshold;
    let tip = insert_verified_branch(&mut store.graph, anchor, 1, difficulty, 0x70);
    synchronize_fixture(&mut store, tip);
    let request = operator_invalidate(
        &store,
        tip.hash,
        crate::OperatorInvalidationId::new([0x71; 16]),
        0x72,
    );
    let authority = ExactAuthority(
        request
            .event
            .fingerprint()
            .expect("operator invalidation carries stable evidence"),
    );
    let exact_context = TransitionContext {
        config: &config,
        clock: &clock,
        full_state_authority: Some(&authority),
        retention_references: &[],
    };
    apply_transition(&store, request.clone(), &exact_context)
        .expect("the exact staged payload is authorized");

    let evidence = request
        .event
        .idempotency_key()
        .expect("operator invalidation carries stable evidence");
    let mut substituted = request.clone();
    let TransitionEvent::OperatorInvalidate(event) = &mut substituted.event else {
        unreachable!("the fixture is an operator invalidation")
    };
    event.operator_reason_digest[0] ^= 1;
    assert!(matches!(
        apply_transition(&store, substituted, &exact_context),
        Err(TransitionFailure::Authority)
    ));

    let cross_variant = TransitionRequest {
        expected_version: request.expected_version,
        event: TransitionEvent::OperatorReconsider(crate::OperatorReconsider {
            target: tip.hash,
            id: crate::OperatorInvalidationId::new([0x71; 16]),
            invalidation_evidence: None,
            evidence,
        }),
    };
    assert!(matches!(
        apply_transition(&store, cross_variant, &exact_context),
        Err(TransitionFailure::Authority)
    ));
}

#[test]
fn old_reconsideration_cannot_remove_a_new_invalidation_episode() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let anchor = store.graph.finalized_frontier();
    let difficulty = store
        .graph
        .header_node(anchor.hash)
        .expect("the anchor exists")
        .header
        .difficulty_threshold;
    let tip = insert_verified_branch(&mut store.graph, anchor, 1, difficulty, 0x75);
    synchronize_fixture(&mut store, tip);
    let id = crate::OperatorInvalidationId::new([0x76; 16]);

    let first_invalidation = apply_transition(
        &store,
        operator_invalidate(&store, tip.hash, id, 0x77),
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("the first invalidation episode commits");
    store.commit(&first_invalidation);
    let mut old_reconsideration = operator_reconsider(&store, tip.hash, id, 0x78);
    let first_reconsideration = apply_transition(
        &store,
        old_reconsideration.clone(),
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("the first episode is reconsidered");
    store.commit(&first_reconsideration);

    let second_invalidation = apply_transition(
        &store,
        operator_invalidate(&store, tip.hash, id, 0x79),
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("a new invalidation episode may reuse the operator-facing ID");
    store.commit(&second_invalidation);
    old_reconsideration.expected_version = store.metadata.state_version;
    let replay = apply_transition(
        &store,
        old_reconsideration,
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("old reconsideration evidence has no effect on the new episode");

    assert!(replay.is_no_change());
    let replayed_graph = projected_graph(&store.graph, &replay);
    assert!(replayed_graph
        .header_node(tip.hash)
        .expect("the target remains retained")
        .eligibility
        .direct_reasons
        .iter()
        .any(|reason| matches!(
            reason,
            EligibilityReason::OperatorInvalid { id: current, evidence, .. }
                if *current == id && *evidence == EvidenceId::from_digest([0x79; 32])
        )));
}

#[test]
// AUD-11: verified-body eligibility and header fork choice are independent,
// so invalidating the verified path must not discard a valid header winner.
fn invalidating_verified_path_preserves_independent_header_best() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let anchor = store.graph.finalized_frontier();
    let difficulty = store
        .graph
        .header_node(anchor.hash)
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
            .set_body_validation_state(frontier.hash, BodyValidationState::Unknown)
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
        &context(&config, &clock, Some(&Authority)),
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
        .set_body_validation_state(selected.hash, BodyValidationState::Unavailable(old))
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

    let plan = apply_transition(
        &store,
        request.clone(),
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("an authenticated operator can restart the same supplier set");
    let projected_graph = projected_graph(&store.graph, &plan);
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
        projected_graph
            .header_node(selected.hash)
            .expect("the selected node remains retained")
            .body_validation_state,
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
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("the exact operator evidence replays idempotently");
    assert!(replay.is_no_change());
    assert!(replay.graph_delta.is_empty());
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
        .set_body_validation_state(selected.hash, BodyValidationState::Unavailable(old))
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
            &context(&config, &clock, Some(&Authority)),
        )
    };

    assert!(matches!(
        apply(block::Hash([0x42; 32]), fresh),
        Err(TransitionFailure::InvalidEvidence(
            InvalidTransitionEvidence::Body(BodyViolation::InvalidOperatorRetryEpisode)
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
            InvalidTransitionEvidence::Body(BodyViolation::InvalidOperatorRetryEpisode)
        ))
    ));
    store
        .graph
        .set_body_validation_state(selected.hash, BodyValidationState::Unknown)
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
        &context(&config, &clock, Some(&Authority)),
    );
    assert!(matches!(
        non_alarmed,
        Err(TransitionFailure::InvalidEvidence(
            InvalidTransitionEvidence::Body(BodyViolation::OperatorRetryRequiresPersistentAlarm)
        ))
    ));
}
