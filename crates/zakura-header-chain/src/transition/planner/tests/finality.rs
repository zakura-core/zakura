use super::super::{
    admission::validate_finality_rebase_path, projected_state::path, write_set::projection_delta,
};
use super::*;
use crate::InvariantViolation;

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

fn validation_lease_for(store: &TestStore, parent: Frontier) -> ValidationLease {
    let mut facts = Vec::new();
    let mut current = parent;
    loop {
        let node = store
            .graph
            .header_node(current.hash)
            .expect("the validation fixture path is retained");
        facts.push(HeaderContextFact {
            frontier: current,
            header: node.header.clone(),
        });
        if current.height == store.graph.finalized_frontier().height {
            break;
        }
        let parent_node = store
            .graph
            .header_node(node.parent_hash)
            .expect("the validation fixture parent is retained");
        current = Frontier::new(parent_node.height, parent_node.hash);
    }
    ValidationLease::new(
        parent,
        facts,
        store.lease.network().clone(),
        store.lease.trust_anchor_digest(),
    )
}

fn apply_with_header_rebase_facts(
    store: &TestStore,
    request: TransitionRequest,
    config: &EngineConfig,
    clock: &ManualClock,
    validation: ValidationLease,
) -> Result<TransitionPlan, TransitionFailure> {
    test_engine(store)
        .plan_transition(
            request,
            &context(config, clock, None),
            DurableTransitionFacts::HeaderInsertion {
                validation_contexts: vec![validation],
                finality_path: store.finality.clone(),
            },
        )
        .map(crate::EngineTransition::into_plan)
}

#[test]
fn header_insert_rebases_and_trims_across_each_monotone_finality_position() {
    for finalized_count in 1..=3_u32 {
        let (mut store, config) = TestStore::new(EngineMode::Integrated);
        let clock = ManualClock(Utc::now());
        let authority = Authority;
        let original_anchor = store.graph.finalized_frontier();
        let prepared = batch(
            original_anchor,
            3,
            store.lease.trust_anchor_digest(),
            EvidenceId::from_digest([0x81; 32]),
        );
        let target = prepared
            .headers()
            .last()
            .expect("the prepared path is nonempty")
            .hash;
        let held = TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::InsertHeaders(Box::new(crate::InsertHeaders {
                owner: crate::HeaderWorkAuthority::for_target(&store.snapshot(), target)
                    .bind(9, NonZeroU64::new(7).expect("seven is nonzero"))
                    .into(),
                source: SourceId::from_digest([0x82; 32]),
                parent_hash: original_anchor.hash,
                target_tip_hash: target,
                completion: TargetCompletion::TargetComplete {
                    common_ancestor: original_anchor,
                },
                batch: prepared.clone(),
                aux: Vec::new(),
            })),
        };

        let mut inserted = Vec::new();
        for header in prepared
            .headers()
            .iter()
            .take(usize::try_from(finalized_count).expect("the bounded count fits usize"))
        {
            let frontier = match store
                .graph
                .insert(
                    header.header.clone(),
                    header.validation,
                    [],
                    BodyValidationState::Verified {
                        evidence: EvidenceId::from_digest([0x83; 32]),
                    },
                )
                .expect("the canonical fixture prefix inserts")
            {
                crate::InsertResult::Inserted(frontier)
                | crate::InsertResult::AlreadyPresent(frontier) => frontier,
            };
            inserted.push(frontier);
        }
        let verified_tip = *inserted.last().expect("at least one prefix header inserts");
        synchronize_fixture(&mut store, verified_tip);
        let rebased_validation = validation_lease_for(&store, verified_tip);

        let mut previous = original_anchor;
        for (index, next) in inserted.iter().copied().enumerate() {
            let evidence_byte =
                0x84_u8.saturating_add(u8::try_from(index).expect("the three-step index fits u8"));
            let plan = apply_transition(
                &store,
                TransitionRequest {
                    expected_version: store.metadata.state_version,
                    event: TransitionEvent::FullStateFinalized(crate::FullStateFinalized {
                        full_state_transition_id: EvidenceId::from_digest([evidence_byte; 32]),
                        new_finalized: next,
                        verified_path_proof: vec![previous.hash, next.hash],
                    }),
                },
                &context(&config, &clock, Some(&authority)),
            )
            .expect("each monotone finality step commits");
            store.commit(&plan);
            previous = next;
        }

        if finalized_count == 2 {
            let mut missing_proof = store.clone();
            missing_proof.finality.clear();
            assert!(matches!(
                apply_with_header_rebase_facts(
                    &missing_proof,
                    held.clone(),
                    &config,
                    &clock,
                    rebased_validation.clone(),
                ),
                Err(TransitionFailure::Stale { .. })
            ));
        }
        if finalized_count == 1 {
            let mut exhausted = store.clone();
            exhausted
                .finality
                .last_mut()
                .expect("one finality step produces history")
                .epoch = FinalityEpoch::new(u64::MAX);
            assert!(matches!(
                apply_with_header_rebase_facts(
                    &exhausted,
                    held.clone(),
                    &config,
                    &clock,
                    rebased_validation.clone(),
                ),
                Err(TransitionFailure::Counter(_))
            ));
        }

        let plan =
            apply_with_header_rebase_facts(&store, held, &config, &clock, rebased_validation)
                .expect("state-proven monotone finality preserves the prepared suffix");
        let remaining = 3usize.saturating_sub(
            usize::try_from(finalized_count).expect("the bounded count fits usize"),
        );
        assert_eq!(plan.change_set.put_nodes.len(), remaining);
        assert_eq!(plan.is_no_change(), remaining == 0);
        assert_eq!(
            plan.cause(),
            if remaining == 0 {
                TransitionCause::HeaderWorkAlreadyApplied
            } else {
                TransitionCause::HeaderWorkRebased
            }
        );
        assert_eq!(plan.change_set.metadata.frontiers.finalized, verified_tip);
    }
}

#[test]
fn header_insert_rebase_rejects_a_competing_finalized_branch() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let authority = Authority;
    let original_anchor = store.graph.finalized_frontier();
    let held = insertion(&store, 2, EvidenceId::from_digest([0x91; 32]));
    let difficulty = store
        .graph
        .header_node(original_anchor.hash)
        .expect("the anchor is retained")
        .header
        .difficulty_threshold;
    let competing = insert_verified_branch(&mut store.graph, original_anchor, 1, difficulty, 0x92);
    synchronize_fixture(&mut store, competing);
    let validation = validation_lease_for(&store, competing);
    let finality = apply_transition(
        &store,
        TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::FullStateFinalized(crate::FullStateFinalized {
                full_state_transition_id: EvidenceId::from_digest([0x93; 32]),
                new_finalized: competing,
                verified_path_proof: vec![original_anchor.hash, competing.hash],
            }),
        },
        &context(&config, &clock, Some(&authority)),
    )
    .expect("the competing branch becomes finalized");
    store.commit(&finality);

    assert!(matches!(
        apply_with_header_rebase_facts(&store, held, &config, &clock, validation),
        Err(TransitionFailure::Stale { .. })
    ));
}

#[test]
fn header_insert_rebase_preserves_a_retained_parent_after_new_finality() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let authority = Authority;
    let original_anchor = store.graph.finalized_frontier();
    let difficulty = store
        .graph
        .header_node(original_anchor.hash)
        .expect("the anchor is retained")
        .header
        .difficulty_threshold;
    let parent = insert_verified_branch(&mut store.graph, original_anchor, 2, difficulty, 0xa1);
    let selected = path(&store.graph, parent).expect("the verified parent path is retained");
    let new_finalized = selected[1];
    synchronize_fixture(&mut store, parent);
    let validation = validation_lease_for(&store, parent);
    let prepared = batch(
        parent,
        1,
        store.lease.trust_anchor_digest(),
        EvidenceId::from_digest([0xa2; 32]),
    );
    let target = prepared
        .headers()
        .last()
        .expect("the child batch is nonempty")
        .hash;
    let held = TransitionRequest {
        expected_version: store.metadata.state_version,
        event: TransitionEvent::InsertHeaders(Box::new(crate::InsertHeaders {
            owner: crate::HeaderWorkAuthority::for_target(&store.snapshot(), target)
                .bind(4, NonZeroU64::new(5).expect("five is nonzero"))
                .into(),
            source: SourceId::from_digest([0xa3; 32]),
            parent_hash: parent.hash,
            target_tip_hash: target,
            completion: TargetCompletion::TargetComplete {
                common_ancestor: parent,
            },
            batch: prepared,
            aux: Vec::new(),
        })),
    };
    let finality = apply_transition(
        &store,
        TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::FullStateFinalized(crate::FullStateFinalized {
                full_state_transition_id: EvidenceId::from_digest([0xa4; 32]),
                new_finalized,
                verified_path_proof: vec![original_anchor.hash, new_finalized.hash],
            }),
        },
        &context(&config, &clock, Some(&authority)),
    )
    .expect("the parent remains above new finality");
    store.commit(&finality);

    let plan = apply_with_header_rebase_facts(&store, held, &config, &clock, validation)
        .expect("the retained parent preserves all prepared child work");
    assert_eq!(plan.cause(), TransitionCause::HeaderWorkRebased);
    assert_eq!(plan.change_set.put_nodes.len(), 1);
    assert_eq!(plan.change_set.put_nodes[0].parent_hash, parent.hash);
}

#[test]
// AUD-13: both operation orders must produce the same durable graph and
// projections, which covers every observable result of the transition.
fn finalization_and_replacement_match_serial_histories() {
    use zakura_chain::work::difficulty::{ExpandedDifficulty, U256};

    let (mut base, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let authority = Authority;
    let anchor = base.graph.finalized_frontier();
    let easy = base
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
    let incumbent = insert_verified_branch(&mut base.graph, anchor, 2, easy, 0x61);
    let incumbent_path = path(&base.graph, incumbent).expect("the incumbent path is retained");
    let shared_finalized = incumbent_path[1];
    let replacement = insert_verified_branch(&mut base.graph, shared_finalized, 1, hard, 0x62);
    base.graph
        .set_body_validation_state(replacement.hash, BodyValidationState::Unknown)
        .expect("the replacement deliberately has no verified body");
    synchronize_fixture(&mut base, incumbent);
    assert_eq!(base.metadata.frontiers.header_best, replacement);

    let invalidation_id = crate::OperatorInvalidationId::new([0x63; 16]);
    let invalidate = apply_transition(
        &base,
        operator_invalidate(&base, replacement.hash, invalidation_id, 0x64),
        &context(&config, &clock, Some(&Authority)),
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
        &context(&config, &clock, Some(&Authority)),
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
        &context(&config, &clock, Some(&Authority)),
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
            &context(&config, &clock, Some(&Authority))
        ),
        Err(TransitionFailure::Stale { .. })
    ));
    let replacement_plan = apply_transition(
        &finality_then_replacement,
        reconsider(&finality_then_replacement),
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("replacement replans from the committed finality snapshot");
    finality_then_replacement.commit(&replacement_plan);

    let logical_state = |store: &TestStore| {
        let mut nodes: Vec<_> = store.graph.header_nodes().cloned().collect();
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
        replacement_then_finality
            .metadata
            .last_transition
            .expect("the finality transition is replay protected")
            .evidence(),
        EvidenceId::from_digest([0x66; 32]),
        "replacement-then-finality retains the actual last serialized event"
    );
    assert_eq!(
        finality_then_replacement
            .metadata
            .last_transition
            .expect("the replacement transition is replay protected")
            .evidence(),
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
    let old_finalized = store.graph.finalized_frontier();
    let easy = store
        .graph
        .header_node(old_finalized.hash)
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
        .header_node(verified_tip.hash)
        .expect("the retained tip exists")
        .work_coordinate();
    let new_anchor_coordinate = store
        .graph
        .header_node(new_finalized.hash)
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
                .header_node(frontier.hash)
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

#[test]
fn checkpoint_verified_growth_advances_verified_and_finalized_atomically() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let authority = Authority;
    let insert = insertion(&store, 8, EvidenceId::from_digest([0x91; 32]));
    let insert_plan = apply_transition(&store, insert, &context(&config, &clock, None))
        .expect("network insertion prepares the checkpoint header");
    store.commit(&insert_plan);

    let old_tip = store.metadata.frontiers.verified_best;
    let checkpoint = store.selected[1];
    let header = store
        .graph
        .header_node(checkpoint.hash)
        .expect("the checkpoint header was inserted")
        .header
        .clone();
    let evidence = EvidenceId::from_digest([0x92; 32]);
    let request = TransitionRequest {
        expected_version: store.metadata.state_version,
        event: TransitionEvent::VerifiedChainChanged(crate::VerifiedChainChanged {
            full_state_transition_id: evidence,
            old_tip,
            new_path: vec![crate::VerifiedHeaderRef {
                height: checkpoint.height,
                hash: checkpoint.hash,
                header,
            }],
            cause: crate::VerifiedChangeCause::CheckpointFinalizedGrow,
        }),
    };

    let plan = apply_transition(&store, request, &context(&config, &clock, Some(&authority)))
        .expect("checkpoint authority advances verification and finality together");
    assert_eq!(plan.change_set.metadata.frontiers.verified_best, checkpoint);
    assert_eq!(plan.change_set.metadata.frontiers.finalized, checkpoint);
    assert_eq!(plan.cause(), TransitionCause::CheckpointFinality);
    assert!(
        super::super::super::invariants::is_incremental_checkpoint_finality(
            &test_engine(&store),
            &plan
        )
    );
    assert!(matches!(
        plan.change_set
            .finality_append
            .expect("checkpoint finality is recorded")
            .source,
        FinalitySource::FullState { evidence: actual } if actual == evidence
    ));

    let mut unverified = plan.clone();
    unverified.change_set.put_nodes[0].body_validation_state = BodyValidationState::Unknown;
    crate::graph::test_support::mutate_updated_header(
        &mut unverified.graph_delta,
        checkpoint.hash,
        |header_node| header_node.body_validation_state = BodyValidationState::Unknown,
    );
    assert_eq!(
        verify_plan(&test_engine(&store), &unverified),
        Err(InvariantViolation::VerifiedProjection(checkpoint.hash))
    );

    let selected_tip = store.metadata.frontiers.header_best;
    let mut evicted_selected = plan.clone();
    evicted_selected
        .change_set
        .delete_nodes
        .push(selected_tip.hash);
    evicted_selected
        .change_set
        .index_changes
        .deleted
        .push(selected_tip.hash);
    crate::graph::test_support::add_deleted_header(
        &mut evicted_selected.graph_delta,
        selected_tip.hash,
    );
    assert_eq!(
        verify_plan(&test_engine(&store), &evicted_selected),
        Err(InvariantViolation::SelectedProjection(selected_tip.hash))
    );

    let mut pin_conflict = plan;
    pin_conflict.trust_pins =
        vec![Frontier::new(checkpoint.height, block::Hash([0xff; 32]))].into();
    assert_eq!(
        verify_plan(&test_engine(&store), &pin_conflict),
        Err(InvariantViolation::TrustPin(checkpoint.height))
    );
}

#[test]
fn validate_finality_rebase_path_table() {
    let a0 = Frontier::new(block::Height(0), block::Hash([0x10; 32]));
    let a1 = Frontier::new(block::Height(1), block::Hash([0x11; 32]));
    let a2 = Frontier::new(block::Height(2), block::Hash([0x12; 32]));
    let a3 = Frontier::new(block::Height(3), block::Hash([0x13; 32]));
    let competing = Frontier::new(block::Height(1), block::Hash([0x91; 32]));
    let evidence = EvidenceId::from_digest([0x44; 32]);
    let source = FinalitySource::FullState { evidence };

    struct Case {
        label: &'static str,
        original_anchor: block::Hash,
        current_finalized: Frontier,
        path: Vec<FinalityRecord>,
        expected: Result<(), TransitionFailure>,
    }

    let cases = [
        Case {
            label: "unchanged anchor with empty path",
            original_anchor: a1.hash,
            current_finalized: a1,
            path: Vec::new(),
            expected: Ok(()),
        },
        Case {
            label: "unchanged anchor with nonempty path",
            original_anchor: a1.hash,
            current_finalized: a1,
            path: vec![FinalityRecord {
                previous: a0,
                current: a1,
                source,
                epoch: FinalityEpoch::new(1),
            }],
            expected: Err(TransitionFailure::StalePreparation),
        },
        Case {
            label: "one valid advancement",
            original_anchor: a0.hash,
            current_finalized: a1,
            path: vec![FinalityRecord {
                previous: a0,
                current: a1,
                source,
                epoch: FinalityEpoch::new(1),
            }],
            expected: Ok(()),
        },
        Case {
            label: "multiple contiguous advancements",
            original_anchor: a0.hash,
            current_finalized: a3,
            path: vec![
                FinalityRecord {
                    previous: a0,
                    current: a1,
                    source,
                    epoch: FinalityEpoch::new(1),
                },
                FinalityRecord {
                    previous: a1,
                    current: a2,
                    source,
                    epoch: FinalityEpoch::new(2),
                },
                FinalityRecord {
                    previous: a2,
                    current: a3,
                    source,
                    epoch: FinalityEpoch::new(3),
                },
            ],
            expected: Ok(()),
        },
        Case {
            label: "first record may begin at any epoch",
            original_anchor: a0.hash,
            current_finalized: a1,
            path: vec![FinalityRecord {
                previous: a0,
                current: a1,
                source,
                epoch: FinalityEpoch::new(9),
            }],
            expected: Ok(()),
        },
        Case {
            label: "changed anchor with empty path",
            original_anchor: a0.hash,
            current_finalized: a1,
            path: Vec::new(),
            expected: Err(TransitionFailure::StalePreparation),
        },
        Case {
            label: "first predecessor hash mismatch",
            original_anchor: a0.hash,
            current_finalized: a1,
            path: vec![FinalityRecord {
                previous: competing,
                current: a1,
                source,
                epoch: FinalityEpoch::new(1),
            }],
            expected: Err(TransitionFailure::StalePreparation),
        },
        Case {
            label: "gap between current and next previous",
            original_anchor: a0.hash,
            current_finalized: a3,
            path: vec![
                FinalityRecord {
                    previous: a0,
                    current: a1,
                    source,
                    epoch: FinalityEpoch::new(1),
                },
                FinalityRecord {
                    previous: a2,
                    current: a3,
                    source,
                    epoch: FinalityEpoch::new(2),
                },
            ],
            expected: Err(TransitionFailure::StalePreparation),
        },
        Case {
            label: "duplicate epoch",
            original_anchor: a0.hash,
            current_finalized: a2,
            path: vec![
                FinalityRecord {
                    previous: a0,
                    current: a1,
                    source,
                    epoch: FinalityEpoch::new(1),
                },
                FinalityRecord {
                    previous: a1,
                    current: a2,
                    source,
                    epoch: FinalityEpoch::new(1),
                },
            ],
            expected: Err(TransitionFailure::StalePreparation),
        },
        Case {
            label: "skipped epoch",
            original_anchor: a0.hash,
            current_finalized: a2,
            path: vec![
                FinalityRecord {
                    previous: a0,
                    current: a1,
                    source,
                    epoch: FinalityEpoch::new(1),
                },
                FinalityRecord {
                    previous: a1,
                    current: a2,
                    source,
                    epoch: FinalityEpoch::new(3),
                },
            ],
            expected: Err(TransitionFailure::StalePreparation),
        },
        Case {
            label: "reversed epoch",
            original_anchor: a0.hash,
            current_finalized: a2,
            path: vec![
                FinalityRecord {
                    previous: a0,
                    current: a1,
                    source,
                    epoch: FinalityEpoch::new(2),
                },
                FinalityRecord {
                    previous: a1,
                    current: a2,
                    source,
                    epoch: FinalityEpoch::new(1),
                },
            ],
            expected: Err(TransitionFailure::StalePreparation),
        },
        Case {
            label: "height retreat",
            original_anchor: a0.hash,
            current_finalized: Frontier::new(block::Height(4), block::Hash([0x14; 32])),
            path: vec![FinalityRecord {
                previous: Frontier::new(block::Height(5), a0.hash),
                current: Frontier::new(block::Height(4), block::Hash([0x14; 32])),
                source,
                epoch: FinalityEpoch::new(1),
            }],
            expected: Err(TransitionFailure::StalePreparation),
        },
        Case {
            label: "wrong terminal frontier",
            original_anchor: a0.hash,
            current_finalized: a2,
            path: vec![FinalityRecord {
                previous: a0,
                current: a1,
                source,
                epoch: FinalityEpoch::new(1),
            }],
            expected: Err(TransitionFailure::StalePreparation),
        },
    ];

    for case in cases {
        assert_eq!(
            validate_finality_rebase_path(case.original_anchor, case.current_finalized, &case.path),
            case.expected,
            "{}",
            case.label
        );
    }

    assert_eq!(
        validate_finality_rebase_path(
            a0.hash,
            a1,
            &[FinalityRecord {
                previous: a0,
                current: a1,
                source,
                epoch: FinalityEpoch::new(u64::MAX),
            }],
        ),
        Err(TransitionFailure::Counter(
            FinalityEpoch::new(u64::MAX)
                .checked_next()
                .expect_err("u64::MAX has no next finality epoch"),
        )),
        "epoch exhaustion at u64::MAX"
    );

    // First predecessor compares only its hash because BranchId retains only the anchor hash.
    // Equal heights are allowed; only a height retreat is rejected.
    assert_eq!(
        validate_finality_rebase_path(
            a0.hash,
            a1,
            &[FinalityRecord {
                previous: Frontier::new(block::Height(1), a0.hash),
                current: a1,
                source,
                epoch: FinalityEpoch::new(1),
            }],
        ),
        Ok(()),
        "first-record predecessor height is intentionally ignored when it does not retreat"
    );
}
