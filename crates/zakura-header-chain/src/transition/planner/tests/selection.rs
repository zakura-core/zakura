use super::super::{
    projected_state::{select_fully_verified_path, trim_projection},
    write_set::projection_delta,
};
use super::*;

#[test]
fn malformed_full_state_paths_fail_with_exact_shape_errors() {
    let (store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let authority = Authority;
    let apply = |event| {
        apply_transition(
            &store,
            TransitionRequest {
                expected_version: store.metadata.state_version,
                event,
            },
            &context(&config, &clock, Some(&authority)),
        )
    };

    assert!(matches!(
        apply(TransitionEvent::VerifiedBlockAccepted(
            crate::VerifiedBlockAccepted {
                full_state_transition_id: EvidenceId::from_digest([0x60; 32]),
                path: Vec::new(),
            }
        )),
        Err(TransitionFailure::InvalidEvidence(
            InvalidTransitionEvidence::Header(HeaderViolation::Path {
                kind: HeaderPathKind::AcceptedSide,
                problem: HeaderPathProblem::Empty,
            })
        ))
    ));
    assert_eq!(
        apply(TransitionEvent::VerifiedChainChanged(
            crate::VerifiedChainChanged {
                full_state_transition_id: EvidenceId::from_digest([0x61; 32]),
                old_tip: Frontier::new(block::Height(0), block::Hash([0x62; 32])),
                new_path: Vec::new(),
                cause: crate::VerifiedChangeCause::Grow,
            }
        ))
        .expect_err("the old verified tip is an exact freshness guard"),
        TransitionFailure::StalePreparation
    );

    let request = insertion(&store, 1, EvidenceId::from_digest([0x63; 32]));
    let TransitionEvent::InsertHeaders(insert) = request.event else {
        unreachable!("the fixture constructs a header insertion")
    };
    let prepared = &insert.batch.headers()[0];
    assert!(matches!(
        apply(TransitionEvent::VerifiedBlockAccepted(
            crate::VerifiedBlockAccepted {
                full_state_transition_id: EvidenceId::from_digest([0x64; 32]),
                path: vec![crate::VerifiedHeaderRef {
                    height: block::Height(prepared.height.0 + 1),
                    hash: prepared.hash,
                    header: prepared.header.clone(),
                }],
            }
        )),
        Err(TransitionFailure::InvalidEvidence(
            InvalidTransitionEvidence::Header(HeaderViolation::Path {
                kind: HeaderPathKind::AcceptedSide,
                problem: HeaderPathProblem::Discontinuous,
            })
        ))
    ));
}

#[test]
fn projection_delta_and_verified_selection_cover_fork_boundaries() {
    let a0 = Frontier::new(block::Height(0), block::Hash([0x70; 32]));
    let a1 = Frontier::new(block::Height(1), block::Hash([0x71; 32]));
    let a2 = Frontier::new(block::Height(2), block::Hash([0x72; 32]));
    let b1 = Frontier::new(block::Height(1), block::Hash([0x81; 32]));
    let b2 = Frontier::new(block::Height(2), block::Hash([0x82; 32]));
    assert_eq!(
        projection_delta(&[a0, a1], &[a0, a1, a2]),
        ProjectionDelta {
            remove_before: None,
            remove_from: Some(block::Height(2)),
            put: vec![a2],
        }
    );
    assert_eq!(
        projection_delta(&[a0, a1, a2], &[a0, b1, b2]),
        ProjectionDelta {
            remove_before: None,
            remove_from: Some(block::Height(1)),
            put: vec![b1, b2],
        }
    );
    assert_eq!(
        projection_delta(&[a0, a1, a2], &[a1, a2]),
        ProjectionDelta {
            remove_before: Some(block::Height(1)),
            remove_from: None,
            put: Vec::new(),
        }
    );

    let (mut store, _) = TestStore::new(EngineMode::Integrated);
    let anchor = store.graph.finalized_frontier();
    let difficulty = regtest_genesis_block().header.difficulty_threshold;
    let tip = insert_verified_branch(&mut store.graph, anchor, 2, difficulty, 0x73);
    let first = store
        .graph
        .header_ancestor(tip.hash, block::Height(1))
        .expect("the test ancestry is coherent")
        .expect("the two-header branch contains height one");
    store
        .graph
        .set_body_validation_state(first.hash, BodyValidationState::Unknown)
        .expect("the intermediate body becomes unverified");
    assert_eq!(
        select_fully_verified_path(&store.graph)
            .expect("verified selection remains finalized-rooted"),
        vec![anchor],
        "a verified descendant cannot jump over an unverified parent"
    );
}

#[test]
fn complete_unchanged_projection_remains_borrowed() {
    let (store, _) = TestStore::new(EngineMode::Integrated);
    let projection = Cow::Borrowed(store.selected.as_slice());
    let trimmed = trim_projection(&store.graph, projection)
        .expect("the complete fixture projection remains valid");

    assert!(matches!(trimmed, Cow::Borrowed(_)));
}

#[test]
fn committed_transition_reports_a_stale_source_without_panicking() {
    let (store, config) = TestStore::new(EngineMode::HeadersOnly);
    let clock = ManualClock(Utc::now());
    let request = insertion(&store, 1, EvidenceId::from_digest([0xe0; 32]));
    let input = || fixture_transition_input(&store, request.clone());
    let mut engine = test_engine(&store);
    let first = engine
        .plan_transition(input(), &context(&config, &clock, None))
        .expect("the first transition plans from the snapshot before commit");
    let stale = engine
        .plan_transition(input(), &context(&config, &clock, None))
        .expect("the second transition plans from the same snapshot before commit");

    engine
        .install_committed_transition(first)
        .expect("the first transition still has its exact source");
    assert_eq!(
        engine.install_committed_transition(stale),
        Err(crate::CommittedTransitionError::StaleSource)
    );
}

#[test]
fn full_commit_ensures_exact_node_body_and_independent_selection() {
    use zakura_chain::work::difficulty::{ExpandedDifficulty, U256};

    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let authority = Authority;
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
    let header_best = insert_verified_branch(&mut store.graph, anchor, 1, hard, 0x58);
    store
        .graph
        .set_body_validation_state(header_best.hash, BodyValidationState::Unknown)
        .expect("the harder competitor remains header-only");
    synchronize_fixture(&mut store, anchor);
    assert_eq!(store.metadata.frontiers.header_best, header_best);
    assert_eq!(store.metadata.frontiers.verified_best, anchor);

    let mut accepted_header = *regtest_genesis_block().header;
    accepted_header.previous_block_hash = anchor.hash;
    accepted_header.difficulty_threshold = easy;
    accepted_header.time += chrono::Duration::seconds(1);
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
        store.graph.header_node(accepted.hash).is_none(),
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

    let projected_graph = projected_graph(&store.graph, &plan);
    let accepted_node = projected_graph
        .header_node(accepted.hash)
        .expect("the full-state header is inserted into the projected DAG");
    assert_eq!(accepted_node.parent_hash, anchor.hash);
    assert_eq!(
        accepted_node.body_validation_state,
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
fn full_state_insertion_rejects_a_contextually_invalid_header() {
    let (store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let authority = Authority;
    let anchor = store.graph.finalized_frontier();
    let parent = store
        .graph
        .header_node(anchor.hash)
        .expect("the finalized anchor exists");
    let mut header = *regtest_genesis_block().header;
    header.previous_block_hash = anchor.hash;
    header.difficulty_threshold = parent.header.difficulty_threshold;
    header.time = parent.header.time;
    header.nonce.0[0] = 0x5b;
    let header = Arc::new(header);
    let hash = header.hash();

    let request = TransitionRequest {
        expected_version: store.metadata.state_version,
        event: TransitionEvent::VerifiedChainChanged(crate::VerifiedChainChanged {
            full_state_transition_id: EvidenceId::from_digest([0x5c; 32]),
            old_tip: anchor,
            new_path: vec![crate::VerifiedHeaderRef {
                height: anchor
                    .height
                    .next()
                    .expect("the fixture anchor has a next height"),
                hash,
                header,
            }],
            cause: crate::VerifiedChangeCause::Grow,
        }),
    };

    assert!(matches!(
        apply_transition(&store, request, &context(&config, &clock, Some(&authority)),),
        Err(TransitionFailure::InvalidEvidence(
            InvalidTransitionEvidence::Header(crate::HeaderViolation::Validation {
                source: crate::HeaderValidationSource::FullState,
                check: HeaderValidationCheck::ContextualValidation,
            })
        ))
    ));
    assert!(
        store.graph.header_node(hash).is_none(),
        "rejected full-state evidence must not install the supplied header"
    );
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
    let projected_graph = projected_graph(&store.graph, &plan);

    assert_eq!(
        plan.change_set.metadata.frontiers.verified_best,
        store.metadata.frontiers.verified_best
    );
    assert_eq!(
        plan.change_set.verified_projection,
        ProjectionDelta::default()
    );
    assert!(matches!(
        projected_graph
            .header_node(accepted)
            .map(|node| &node.body_validation_state),
        Some(BodyValidationState::Verified {
            evidence: actual
        }) if *actual == evidence
    ));
}

#[test]
fn public_transition_api_plans_then_installs_one_verified_dag_change() {
    let (store, config) = TestStore::new(EngineMode::Integrated);
    let engine = test_engine(&store);
    let clock = ManualClock(Utc::now());
    let request = insertion(&store, 3, EvidenceId::from_digest([0x92; 32]));
    let before = engine.snapshot();
    let transition = engine
        .plan_transition(
            fixture_transition_input(&store, request),
            &context(&config, &clock, None),
        )
        .expect("the stateful engine plans the insertion");

    assert_eq!(transition.snapshot_before_commit(), &before);
    assert_ne!(
        transition.snapshot_after_commit().state_version,
        before.state_version,
        "a state-changing insertion advances the durable version"
    );
    assert_eq!(
        engine.snapshot(),
        before,
        "planning must leave the source engine unchanged"
    );
    let expected = transition.snapshot_after_commit();
    let mut projected = engine.clone();
    projected
        .install_committed_transition(transition)
        .expect("the transition still has its exact source");
    assert_eq!(projected.snapshot(), expected);
    assert_eq!(
        projected.selected_projection().last().copied(),
        Some(expected.frontiers.header_best)
    );
}
