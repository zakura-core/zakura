use super::*;

#[test]
fn finalization_projection_delta_removes_only_the_retired_prefix() {
    let old: Vec<_> = (0..=3)
        .map(|height| {
            let byte = u8::try_from(height).expect("the fixture height is at most three");
            Frontier::new(block::Height(height), block::Hash([byte; 32]))
        })
        .collect();
    let new = old[1..].to_vec();

    assert_eq!(
        projection_delta(&old, &new),
        ProjectionDelta {
            remove_before: Some(block::Height(1)),
            remove_from: None,
            put: Vec::new(),
        }
    );
}

#[test]
// AUD-13: both operation orders must produce the same durable graph and
// projections, which covers every observable result of the transition.
fn finalization_and_replacement_match_serial_histories() {
    use zakura_chain::work::difficulty::{ExpandedDifficulty, U256};

    let (mut base, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let authority = Authority;
    let anchor = base.graph.finalized();
    let easy = base
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
    let incumbent = insert_verified_branch(&mut base.graph, anchor, 2, easy, 0x61);
    let incumbent_path = path(&base.graph, incumbent).expect("the incumbent path is retained");
    let shared_finalized = incumbent_path[1];
    let replacement = insert_verified_branch(&mut base.graph, shared_finalized, 1, hard, 0x62);
    base.graph
        .set_body_state(replacement.hash, BodyValidationState::Unknown)
        .expect("the replacement deliberately has no verified body");
    synchronize_fixture(&mut base, incumbent);
    assert_eq!(base.metadata.frontiers.header_best, replacement);

    let invalidation_id = crate::OperatorInvalidationId::new([0x63; 16]);
    let invalidate = apply_transition(
        &base,
        operator_invalidate(&base, replacement.hash, invalidation_id, 0x64),
        &context(&config, &clock, None),
    )
    .expect("the fixture starts with the incumbent selected");
    base.commit(&invalidate);
    assert_eq!(base.metadata.frontiers.header_best, incumbent);

    let reconsider =
        |store: &TestStore| operator_reconsider(store, replacement.hash, invalidation_id, 0x65);
    let finalize = |store: &TestStore| TransitionRequest {
        expected_version: store.metadata.state_version,
        event: TransitionEvent::FullStateFinalized(crate::FullStateFinalized {
            full_state_transition_id: EvidenceId::from_digest([0x66; 32]),
            new_finalized: shared_finalized,
            verified_path_proof: vec![anchor.hash, shared_finalized.hash],
        }),
    };

    let initially_planned_reconsider = reconsider(&base);
    let initially_planned_finalize = finalize(&base);
    let held_replacement_plan = apply_transition(
        &base,
        initially_planned_reconsider.clone(),
        &context(&config, &clock, None),
    )
    .expect("replacement can pause after planning");
    let held_finality_plan = apply_transition(
        &base,
        initially_planned_finalize.clone(),
        &context(&config, &clock, Some(&authority)),
    )
    .expect("finalization can pause after planning");
    assert_eq!(held_replacement_plan.before(), &base.snapshot());
    assert_eq!(held_finality_plan.before(), &base.snapshot());

    let mut replacement_then_finality = base.clone();
    let replacement_plan = apply_transition(
        &replacement_then_finality,
        reconsider(&replacement_then_finality),
        &context(&config, &clock, None),
    )
    .expect("replacement can win the first serialized position");
    replacement_then_finality.commit(&replacement_plan);
    assert_eq!(
        replacement_then_finality.metadata.frontiers.header_best,
        replacement
    );
    assert!(matches!(
        apply_transition(
            &replacement_then_finality,
            initially_planned_finalize,
            &context(&config, &clock, Some(&authority))
        ),
        Err(TransitionFailure::Stale { .. })
    ));
    let finality_plan = apply_transition(
        &replacement_then_finality,
        finalize(&replacement_then_finality),
        &context(&config, &clock, Some(&authority)),
    )
    .expect("finalization replans from the committed replacement snapshot");
    replacement_then_finality.commit(&finality_plan);

    let mut finality_then_replacement = base.clone();
    let finality_plan = apply_transition(
        &finality_then_replacement,
        finalize(&finality_then_replacement),
        &context(&config, &clock, Some(&authority)),
    )
    .expect("finalization can win the first serialized position");
    finality_then_replacement.commit(&finality_plan);
    assert_eq!(
        finality_then_replacement.metadata.frontiers.finalized,
        shared_finalized
    );
    assert!(matches!(
        apply_transition(
            &finality_then_replacement,
            initially_planned_reconsider,
            &context(&config, &clock, None)
        ),
        Err(TransitionFailure::Stale { .. })
    ));
    let replacement_plan = apply_transition(
        &finality_then_replacement,
        reconsider(&finality_then_replacement),
        &context(&config, &clock, None),
    )
    .expect("replacement replans from the committed finality snapshot");
    finality_then_replacement.commit(&replacement_plan);

    let logical_state = |store: &TestStore| {
        let mut nodes: Vec<_> = store.graph.nodes().cloned().collect();
        nodes.sort_by_key(|node| node.hash.0);
        (
            store.snapshot(),
            store.selected.clone(),
            store.verified.clone(),
            store.finality.clone(),
            nodes,
        )
    };
    assert_eq!(
        logical_state(&replacement_then_finality),
        logical_state(&finality_then_replacement),
        "both barrier orders converge to one complete serial history"
    );
    assert_eq!(
        replacement_then_finality.metadata.last_transition_id,
        EvidenceId::from_digest([0x66; 32]),
        "replacement-then-finality retains the actual last serialized event"
    );
    assert_eq!(
        finality_then_replacement.metadata.last_transition_id,
        EvidenceId::from_digest([0x65; 32]),
        "finality-then-replacement retains the actual last serialized event"
    );
    assert_eq!(
        replacement_then_finality.metadata.frontiers.finalized,
        shared_finalized
    );
    assert_eq!(
        replacement_then_finality.metadata.frontiers.header_best,
        replacement
    );
    assert_eq!(
        replacement_then_finality.metadata.frontiers.verified_best,
        incumbent
    );
    assert_eq!(
        replacement_then_finality.metadata.state_version,
        StateVersion::new(base.metadata.state_version.get().saturating_add(2))
    );
    assert_next_child_commits(
        &replacement_then_finality,
        &config,
        &clock,
        replacement,
        0x67,
    );
    assert_next_child_commits(
        &finality_then_replacement,
        &config,
        &clock,
        replacement,
        0x67,
    );
}

#[test]
fn finality_atomic_prune_rebase_projection_generation() {
    use zakura_chain::work::difficulty::{ExpandedDifficulty, U256};

    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let authority = Authority;
    let old_finalized = store.graph.finalized();
    let easy = store
        .graph
        .node(old_finalized.hash)
        .expect("the anchor exists")
        .header
        .difficulty_threshold;
    let easy_target: U256 = easy
        .to_expanded()
        .expect("the fixture target expands")
        .into();
    let hard = ExpandedDifficulty::from(easy_target >> 3).into();

    let verified_tip = insert_verified_branch(&mut store.graph, old_finalized, 3, easy, 0x71);
    let verified_path =
        path(&store.graph, verified_tip).expect("the verified fixture path is retained");
    let new_finalized = verified_path[1];
    let selected_tip = insert_verified_branch(&mut store.graph, old_finalized, 2, hard, 0x72);
    synchronize_fixture(&mut store, verified_tip);
    assert_eq!(store.metadata.frontiers.header_best, selected_tip);
    assert!(
        !store.selected.contains(&new_finalized),
        "the selected history deliberately conflicts with the full-state anchor"
    );
    let old_selected = store.selected.clone();
    let old_verified = store.verified.clone();
    let before = store.snapshot();
    let retained_tip_coordinate = store
        .graph
        .node(verified_tip.hash)
        .expect("the retained tip exists")
        .work_coordinate();
    let new_anchor_coordinate = store
        .graph
        .node(new_finalized.hash)
        .expect("the new anchor exists")
        .work_coordinate();

    let plan = apply_transition(
        &store,
        TransitionRequest {
            expected_version: before.state_version,
            event: TransitionEvent::FullStateFinalized(crate::FullStateFinalized {
                full_state_transition_id: EvidenceId::from_digest([0x73; 32]),
                new_finalized,
                verified_path_proof: vec![old_finalized.hash, new_finalized.hash],
            }),
        },
        &context(&config, &clock, Some(&authority)),
    )
    .expect("authenticated full-state finality transitions both histories atomically");

    let changes = plan.change_set();
    assert_eq!(changes.metadata.frontiers.finalized, new_finalized);
    assert_eq!(changes.metadata.frontiers.header_best, verified_tip);
    assert_eq!(changes.metadata.frontiers.verified_best, verified_tip);
    assert_eq!(
        changes.metadata.header_best_score.suffix_work,
        retained_tip_coordinate
            .suffix_after(new_anchor_coordinate)
            .expect("the retained tip descends from the new anchor"),
        "selection work is rebased to the new finalized anchor"
    );
    assert_eq!(
        changes.delete_nodes.len(),
        old_selected.len(),
        "the old anchor and every node on the conflicting selected history are pruned"
    );
    assert!(old_selected
        .iter()
        .all(|frontier| changes.delete_nodes.contains(&frontier.hash)));
    assert!(old_verified
        .iter()
        .skip(1)
        .all(|frontier| !changes.delete_nodes.contains(&frontier.hash)));
    assert_eq!(
        changes.selected_projection,
        ProjectionDelta {
            remove_before: Some(verified_path[1].height),
            remove_from: Some(verified_path[1].height),
            put: verified_path[1..].to_vec(),
        }
    );
    assert_eq!(
        changes.verified_projection,
        ProjectionDelta {
            remove_before: Some(verified_path[1].height),
            remove_from: None,
            put: Vec::new(),
        }
    );
    assert_eq!(
        changes.metadata.state_version,
        before
            .state_version
            .checked_next()
            .expect("the fixture version can advance")
    );
    assert_eq!(
        changes.metadata.header_generation,
        before
            .header_generation
            .checked_next()
            .expect("the fixture generation can advance")
    );
    assert_eq!(
        changes.metadata.verified_generation,
        before
            .verified_generation
            .checked_next()
            .expect("the fixture generation can advance")
    );
    assert_eq!(
        changes.finality_append,
        Some(FinalityRecord {
            previous: old_finalized,
            current: new_finalized,
            source: FinalitySource::FullState {
                evidence: EvidenceId::from_digest([0x73; 32]),
            },
            epoch: FinalityEpoch::new(1),
        })
    );
    verify_plan(&test_engine(&store), &plan)
        .expect("the complete atomic finality plan is coherent");
}

#[test]
fn headers_only_finalizes_exactly_tip_minus_one_thousand_before_publication() {
    let (store, config) = TestStore::new(EngineMode::HeadersOnly);
    let clock = ManualClock(Utc::now());
    let request = insertion(&store, 1_001, EvidenceId::from_digest([1; 32]));
    let plan = apply_transition(&store, request, &context(&config, &clock, None))
        .expect("the complete target is admitted atomically");

    assert_eq!(
        plan.change_set.metadata.frontiers.finalized.height,
        block::Height(1)
    );
    assert_eq!(
        plan.change_set.metadata.frontiers.header_best.height,
        block::Height(1_001)
    );
    assert_eq!(
        plan.change_set.metadata.frontiers.verified_best.height,
        block::Height(1)
    );
    assert_eq!(
        plan.change_set.metadata.finality_epoch,
        FinalityEpoch::new(1)
    );
    assert!(matches!(
        plan.change_set.finality_append.expect("depth finality is recorded").source,
        FinalitySource::HeadersOnlyDepth { selected_tip } if selected_tip.height == block::Height(1_001)
    ));
    assert_eq!(plan.cause(), TransitionCause::HeadersOnlyFinality);
}

#[test]
fn integrated_finality_requires_authority_and_exact_verified_path() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let authority = Authority;
    let insert = insertion(&store, 3, EvidenceId::from_digest([2; 32]));
    let insert_plan = apply_transition(&store, insert, &context(&config, &clock, None))
        .expect("network insertion itself needs no full-state authority");
    store.commit(&insert_plan);
    let new_path: Vec<_> = path(&store.graph, store.metadata.frontiers.header_best)
        .expect("the selected fixture path is continuous")
        .into_iter()
        .skip(1)
        .map(|frontier| crate::VerifiedHeaderRef {
            height: frontier.height,
            hash: frontier.hash,
            header: store
                .graph
                .node(frontier.hash)
                .expect("path nodes exist")
                .header
                .clone(),
        })
        .collect();
    let verified_id = EvidenceId::from_digest([4; 32]);
    let verified = TransitionRequest {
        expected_version: store.metadata.state_version,
        event: TransitionEvent::VerifiedChainChanged(crate::VerifiedChainChanged {
            full_state_transition_id: verified_id,
            old_tip: store.metadata.frontiers.verified_best,
            new_path,
            cause: crate::VerifiedChangeCause::Grow,
        }),
    };
    assert!(matches!(
        apply_transition(&store, verified.clone(), &context(&config, &clock, None)),
        Err(TransitionFailure::Authority)
    ));
    let verified_plan = apply_transition(
        &store,
        verified,
        &context(&config, &clock, Some(&authority)),
    )
    .expect("the state writer authenticates its verified-path transition");
    store.commit(&verified_plan);
    let new_finalized = store.verified[1];
    let finality_id = EvidenceId::from_digest([5; 32]);
    let finalize = TransitionRequest {
        expected_version: store.metadata.state_version,
        event: TransitionEvent::FullStateFinalized(crate::FullStateFinalized {
            full_state_transition_id: finality_id,
            new_finalized,
            verified_path_proof: vec![store.verified[0].hash, new_finalized.hash],
        }),
    };
    let plan = apply_transition(
        &store,
        finalize,
        &context(&config, &clock, Some(&authority)),
    )
    .expect("exact verified full-state evidence advances finality");
    assert_eq!(plan.change_set.metadata.frontiers.finalized, new_finalized);
    assert!(matches!(
        plan.change_set.finality_append.expect("full-state finality is recorded").source,
        FinalitySource::FullState { evidence } if evidence == finality_id
    ));
}
