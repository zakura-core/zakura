use super::*;

#[test]
fn full_commit_ensures_exact_node_body_and_independent_selection() {
    use zakura_chain::work::difficulty::{ExpandedDifficulty, U256};

    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let authority = Authority;
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
    let header_best = insert_verified_branch(&mut store.graph, anchor, 1, hard, 0x58);
    store
        .graph
        .set_body_state(header_best.hash, BodyValidationState::Unknown)
        .expect("the harder competitor remains header-only");
    synchronize_fixture(&mut store, anchor);
    assert_eq!(store.metadata.frontiers.header_best, header_best);
    assert_eq!(store.metadata.frontiers.verified_best, anchor);

    let mut accepted_header = *regtest_genesis_block().header;
    accepted_header.previous_block_hash = anchor.hash;
    accepted_header.difficulty_threshold = easy;
    accepted_header.nonce.0[0] = 0x59;
    let accepted_header = Arc::new(accepted_header);
    let accepted = Frontier::new(
        anchor
            .height
            .next()
            .expect("the fixture anchor has a next height"),
        accepted_header.hash(),
    );
    assert!(
        store.graph.node(accepted.hash).is_none(),
        "the full-state header is deliberately absent from the DAG"
    );
    let evidence = EvidenceId::from_digest([0x5a; 32]);
    let before = store.snapshot();
    let plan = apply_transition(
        &store,
        TransitionRequest {
            expected_version: before.state_version,
            event: TransitionEvent::VerifiedChainChanged(crate::VerifiedChainChanged {
                full_state_transition_id: evidence,
                old_tip: anchor,
                new_path: vec![crate::VerifiedHeaderRef {
                    height: accepted.height,
                    hash: accepted.hash,
                    header: accepted_header,
                }],
                cause: crate::VerifiedChangeCause::Grow,
            }),
        },
        &context(&config, &clock, Some(&authority)),
    )
    .expect("the exact full-state growth produces one coherent header transition");

    let accepted_node = plan
        .projected
        .node(accepted.hash)
        .expect("the full-state header is inserted into the projected DAG");
    assert_eq!(accepted_node.parent_hash, anchor.hash);
    assert_eq!(
        accepted_node.body,
        BodyValidationState::Verified { evidence }
    );
    assert_eq!(plan.change_set.metadata.frontiers.verified_best, accepted);
    assert_eq!(
        plan.change_set.metadata.frontiers.header_best, header_best,
        "full-state selection does not override the independently harder header path"
    );
    assert_eq!(
        plan.change_set.metadata.verified_generation,
        before
            .verified_generation
            .checked_next()
            .expect("the fixture generation can advance")
    );
    assert_eq!(
        plan.change_set.metadata.header_generation,
        before
            .header_generation
            .checked_next()
            .expect("the fixture generation can advance")
    );
    verify_plan(&test_engine(&store), &plan).expect("the full-state integration plan is coherent");
}

#[test]
fn accepted_side_path_does_not_replace_the_verified_winner() {
    let (store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let authority = Authority;
    let request = insertion(&store, 2, EvidenceId::from_digest([0x56; 32]));
    let TransitionEvent::InsertHeaders(insert) = request.event else {
        unreachable!("the fixture constructs a header insertion")
    };
    let path: Vec<_> = insert
        .batch
        .headers()
        .iter()
        .map(|header| crate::VerifiedHeaderRef {
            height: header.height,
            hash: header.hash,
            header: header.header.clone(),
        })
        .collect();
    let accepted = path.last().expect("the side path is nonempty").hash;
    let evidence = EvidenceId::from_digest([0x57; 32]);

    let plan = apply_transition(
        &store,
        TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::VerifiedBlockAccepted(crate::VerifiedBlockAccepted {
                full_state_transition_id: evidence,
                path,
            }),
        },
        &context(&config, &clock, Some(&authority)),
    )
    .expect("authenticated full state admits the absent side path");

    assert_eq!(
        plan.change_set.metadata.frontiers.verified_best,
        store.metadata.frontiers.verified_best
    );
    assert_eq!(
        plan.change_set.verified_projection,
        ProjectionDelta::default()
    );
    assert!(matches!(
        plan.projected.node(accepted).map(|node| &node.body),
        Some(BodyValidationState::Verified {
            evidence: actual
        }) if *actual == evidence
    ));
}

#[test]
fn apply_transition_is_the_only_public_dag_mutation_entry_point() {
    let graph_source = include_str!("../../../graph.rs");
    for old_entry in [
        "pub fn insert(",
        "pub fn add_reason(",
        "pub fn remove_operator_invalidation(",
        "pub fn set_consensus_body_invalid(",
        "pub fn set_body_state(",
        "pub fn set_validation(",
    ] {
        assert!(
            !graph_source.contains(old_entry),
            "raw mutation entry point escaped: {old_entry}"
        );
    }
    assert!(
        !include_str!("../../../../src/lib.rs").contains("pub use retention::enforce_retention")
    );
}

#[test]
fn stateful_engine_returns_its_complete_projected_state() {
    let (store, config) = TestStore::new(EngineMode::Integrated);
    let engine = test_engine(&store);
    let clock = ManualClock(Utc::now());
    let request = insertion(&store, 3, EvidenceId::from_digest([0x92; 32]));
    let before = engine.snapshot();
    let transition = engine
        .apply(
            request,
            &context(&config, &clock, None),
            crate::DurableTransitionFacts::ValidationContext(store.lease.clone()),
        )
        .expect("the stateful engine plans the insertion");

    assert_eq!(transition.before(), &before);
    let expected = transition.change_set().metadata.snapshot();
    let projected = transition.into_projected_engine();
    assert_eq!(projected.snapshot(), expected);
    assert_eq!(
        projected.selected_projection().last().copied(),
        Some(expected.frontiers.header_best)
    );
}
