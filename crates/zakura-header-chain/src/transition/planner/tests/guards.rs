use super::*;

#[test]
fn mode_and_capability_gates_precede_event_projection() {
    let (headers_only_store, headers_only_config) = TestStore::new(EngineMode::HeadersOnly);
    let clock = ManualClock(Utc::now());
    let body_event =
        TransitionEvent::BodyEvidence(BodyEvidence::PayloadMismatch(crate::BodyPayloadMismatch {
            evidence: EvidenceId::from_digest([0x50; 32]),
            requested: headers_only_store.metadata.frontiers.header_best.hash,
            delivered: block::Hash([0x51; 32]),
            kind: crate::BodyCommitmentKind::HeaderHash,
            source: SourceId::from_digest([0x52; 32]),
        }));
    assert_eq!(
        apply_transition(
            &headers_only_store,
            TransitionRequest {
                expected_version: headers_only_store.metadata.state_version,
                event: body_event.clone(),
            },
            &context(&headers_only_config, &clock, Some(&Authority)),
        )
        .expect_err("full-state evidence is unavailable in headers-only mode"),
        TransitionFailure::Mode
    );

    let (integrated_store, integrated_config) = TestStore::new(EngineMode::Integrated);
    assert_eq!(
        apply_transition(
            &integrated_store,
            TransitionRequest {
                expected_version: integrated_store.metadata.state_version,
                event: body_event,
            },
            &context(&integrated_config, &clock, None),
        )
        .expect_err("integrated evidence still requires exact authority"),
        TransitionFailure::Authority
    );

    let no_authority = TransitionContext {
        config: &integrated_config,
        clock: &clock,
        full_state_authority: None,
        retention_references: &[],
    };
    assert_eq!(
        apply_transition(
            &integrated_store,
            insertion(&integrated_store, 1, EvidenceId::from_digest([0x53; 32]),),
            &no_authority,
        )
        .expect_err("registered completion capability is mandatory"),
        TransitionFailure::Authority
    );
    assert_eq!(
        apply_transition(
            &integrated_store,
            TransitionRequest {
                expected_version: integrated_store.metadata.state_version,
                event: TransitionEvent::OperatorBodyRetry(crate::OperatorBodyRetry {
                    hash: integrated_store.metadata.frontiers.header_best.hash,
                    evidence: EvidenceId::from_digest([0x54; 32]),
                    availability: crate::BodyUnavailableSummary::default(),
                }),
            },
            &no_authority,
        )
        .expect_err("registered scheduler capability is mandatory"),
        TransitionFailure::Authority
    );
}

#[test]
fn active_prepared_header_limit_accepts_exactly_limit_and_rejects_one_more() {
    let (store, mut config) = TestStore::new(EngineMode::HeadersOnly);
    config.limits.max_headers_per_transition =
        std::num::NonZeroUsize::new(2).expect("two is nonzero");
    let clock = ManualClock(Utc::now());

    apply_transition(
        &store,
        insertion(&store, 2, EvidenceId::from_digest([0x55; 32])),
        &context(&config, &clock, None),
    )
    .expect("a batch at the active runtime limit is admitted");
    assert!(matches!(
        apply_transition(
            &store,
            insertion(&store, 3, EvidenceId::from_digest([0x56; 32])),
            &context(&config, &clock, None),
        ),
        Err(TransitionFailure::InvalidEvidence(
            InvalidTransitionEvidence::Limit(LimitViolation::PreparedHeadersExceeded)
        ))
    ));
}

#[test]
fn retention_references_admit_all_staged_targets_and_candidate_tips() {
    let (store, config) = TestStore::new(EngineMode::HeadersOnly);
    let clock = ManualClock(Utc::now());
    let references = vec![
        store.metadata.frontiers.finalized.hash;
        crate::MAX_STAGED_TARGETS_V1 + crate::MAX_CANDIDATE_TIPS_V1
    ];

    apply_transition(
        &store,
        TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::ReevaluateDeferred,
        },
        &TransitionContext {
            config: &config,
            clock: &clock,
            full_state_authority: None,
            retention_references: &references,
        },
    )
    .expect("one transition admits every active header target and full-state fork tip");
}

#[test]
fn retention_references_are_bounded_before_ancestry_walks() {
    let (store, config) = TestStore::new(EngineMode::HeadersOnly);
    let clock = ManualClock(Utc::now());
    let references = vec![
        store.metadata.frontiers.finalized.hash;
        config.limits.max_retention_references.get() + 1
    ];
    let result = apply_transition(
        &store,
        TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::ReevaluateDeferred,
        },
        &TransitionContext {
            config: &config,
            clock: &clock,
            full_state_authority: None,
            retention_references: &references,
        },
    );

    assert!(matches!(
        result,
        Err(TransitionFailure::InvalidEvidence(
            InvalidTransitionEvidence::Limit(LimitViolation::RetentionReferencesExceeded)
        ))
    ));
}

#[test]
fn typed_header_authority_ignores_global_version_but_binds_branch_freshness() {
    let (store, config) = TestStore::new(EngineMode::HeadersOnly);
    let clock = ManualClock(Utc::now());
    let mut request = insertion(&store, 1, EvidenceId::from_digest([6; 32]));
    request.expected_version = StateVersion::new(9);
    apply_transition(&store, request, &context(&config, &clock, None))
        .expect("global state versions do not authorize header work");

    let request = insertion(&store, 1, EvidenceId::from_digest([7; 32]));
    let TransitionEvent::InsertHeaders(insert) = &request.event else {
        panic!("the fixture constructs a header insertion");
    };
    let owner = insert
        .owner
        .header_owner()
        .expect("the fixture is ordinary header work");
    let stale_authorities = [
        crate::HeaderWorkAuthority {
            header_generation: HeaderGeneration::new(9),
            ..owner.authority
        },
        crate::HeaderWorkAuthority {
            branch: BranchId::new(
                block::Hash([0x58; 32]),
                owner.authority.branch.target_tip_hash,
            ),
            ..owner.authority
        },
        crate::HeaderWorkAuthority {
            branch: BranchId::new(owner.authority.branch.anchor_hash, block::Hash([0x59; 32])),
            ..owner.authority
        },
    ];
    for authority in stale_authorities {
        let mut stale = request.clone();
        let TransitionEvent::InsertHeaders(insert) = &mut stale.event else {
            unreachable!("the cloned fixture remains a header insertion");
        };
        insert.owner = crate::HeaderWorkOwner { authority, ..owner }.into();
        assert!(matches!(
            apply_transition(&store, stale, &context(&config, &clock, None)),
            Err(TransitionFailure::Stale { current }) if current == StateVersion::new(0)
        ));
    }
}

#[test]
fn typed_body_authority_ignores_global_version_but_rejects_stale_generation() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let mut insert = insertion(&store, 2, EvidenceId::from_digest([0xa0; 32]));
    let TransitionEvent::InsertHeaders(insert_event) = &mut insert.event else {
        panic!("the fixture constructs a header insertion");
    };
    let header_hash = insert_event.batch.headers()[0].hash;
    let boundary_hash = insert_event.batch.headers()[1].hash;
    let delivery = crate::AuxDelivery {
        delivery_id: EvidenceId::from_digest([0xa1; 32]),
        header_hash,
        source: SourceId::from_digest([0xa2; 32]),
        owner: insert_event.owner,
        body_size: crate::BodySizeHint::Unknown,
        tree_aux: Some(crate::TreeAuxRecordV1 {
            height: block::Height(1),
            sapling_root: zakura_chain::sapling::tree::Root::default(),
            orchard_root: zakura_chain::orchard::tree::Root::default(),
            ironwood_root: zakura_chain::ironwood::tree::Root::default(),
            sapling_tx_count: 1,
            orchard_tx_count: 1,
            ironwood_tx_count: 1,
            auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([0xa3; 32]),
        }),
        authentication: crate::AuxAuthentication::Unauthenticated,
    };
    insert_event.source = delivery.source;
    insert_event.aux.push(delivery);
    let inserted = apply_transition(&store, insert, &context(&config, &clock, None))
        .expect("the header and unauthenticated delivery insert");
    store.commit(&inserted);

    let owner = crate::BodyWorkAuthority::for_snapshot(&store.snapshot()).bind(
        1,
        NonZeroU64::new(1).expect("fixture request IDs are nonzero"),
    );
    let authentication = crate::AuxAuthentication::Authenticated {
        evidence: EvidenceId::from_digest([0xa4; 32]),
        boundary_hash,
    };
    let request = TransitionRequest {
        expected_version: StateVersion::new(9),
        event: TransitionEvent::AuxEvidence(Box::new(crate::AuxEvidence {
            owner,
            deliveries: vec![delivery],
            authentication,
        })),
    };
    apply_transition(
        &store,
        request.clone(),
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("global state versions do not authorize auxiliary evidence");

    let TransitionEvent::AuxEvidence(event) = &request.event else {
        panic!("the fixture constructs auxiliary evidence");
    };
    let owner = event.owner;
    let stale_authorities = [
        crate::BodyWorkAuthority {
            header: crate::HeaderWorkAuthority {
                header_generation: HeaderGeneration::new(9),
                ..owner.authority.header
            },
            ..owner.authority
        },
        crate::BodyWorkAuthority {
            verified_generation: VerifiedGeneration::new(9),
            ..owner.authority
        },
        crate::BodyWorkAuthority {
            header: crate::HeaderWorkAuthority {
                branch: BranchId::new(
                    block::Hash([0x5a; 32]),
                    owner.authority.header.branch.target_tip_hash,
                ),
                ..owner.authority.header
            },
            ..owner.authority
        },
    ];
    for authority in stale_authorities {
        let mut stale = request.clone();
        let TransitionEvent::AuxEvidence(event) = &mut stale.event else {
            unreachable!("the cloned fixture remains auxiliary evidence");
        };
        event.owner = crate::BodyWorkOwner { authority, ..owner };
        assert!(matches!(
            apply_transition(
                &store,
                stale,
                &context(&config, &clock, Some(&Authority)),
            ),
            Err(TransitionFailure::Stale { current }) if current == store.metadata.state_version
        ));
    }
}

#[test]
fn peer_target_completion_must_match_the_validation_lease_ancestor() {
    let (store, config) = TestStore::new(EngineMode::HeadersOnly);
    let clock = ManualClock(Utc::now());
    let mut request = insertion(&store, 1, EvidenceId::from_digest([0x64; 32]));
    let TransitionEvent::InsertHeaders(insert) = &mut request.event else {
        panic!("the fixture constructs a header insertion");
    };
    insert.completion = TargetCompletion::TargetComplete {
        common_ancestor: Frontier::new(store.lease.parent.height, block::Hash([0x65; 32])),
    };

    assert!(matches!(
        apply_transition(&store, request, &context(&config, &clock, None)),
        Err(TransitionFailure::InvalidEvidence(
            InvalidTransitionEvidence::Header(crate::HeaderViolation::Path {
                kind: HeaderPathKind::Completion,
                problem: HeaderPathProblem::AncestorMismatch
            })
        ))
    ));
}

#[test]
fn bounded_target_prefix_has_the_same_exact_owner_and_validation_guards() {
    let (store, config) = TestStore::new(EngineMode::HeadersOnly);
    let clock = ManualClock(Utc::now());
    let mut request = insertion(&store, 2, EvidenceId::from_digest([0x66; 32]));
    let TransitionEvent::InsertHeaders(insert) = &mut request.event else {
        panic!("the fixture constructs a header insertion");
    };
    insert.completion = TargetCompletion::TargetPrefix {
        common_ancestor: store.lease.parent,
    };
    let expected_target = insert.target_tip_hash;

    let plan = apply_transition(&store, request, &context(&config, &clock, None))
        .expect("a validated bounded prefix is admitted under its exact target owner");

    assert_eq!(
        plan.change_set.metadata.frontiers.header_best.hash,
        expected_target
    );
}

#[test]
fn header_acceptance_cannot_construct_body_or_state_validity() {
    let (store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let before = store.snapshot();
    let plan = apply_transition(
        &store,
        insertion(&store, 2, EvidenceId::from_digest([0xaf; 32])),
        &context(&config, &clock, None),
    )
    .expect("the prepared header-only target is accepted");

    assert!(
        plan.change_set
            .put_nodes
            .iter()
            .all(|node| node.body_validation_state == BodyValidationState::Unknown),
        "header acceptance creates no body-valid fact"
    );
    assert_eq!(
        plan.change_set.metadata.frontiers.verified_best, before.frontiers.verified_best,
        "header acceptance cannot advance full-state validity"
    );
    assert_eq!(
        plan.change_set.metadata.verified_generation, before.verified_generation,
        "header acceptance cannot publish a state-valid generation"
    );
    assert!(
        plan.change_set.verified_projection.put.is_empty()
            && plan.change_set.verified_projection.remove_from.is_none(),
        "header acceptance cannot mutate the verified projection"
    );
}

#[test]
fn graph_boundary_and_transition_invariants_reject_corruption() {
    use std::num::NonZeroUsize;

    use crate::{
        AuxAuthentication, AuxDelivery, BodySizeHint, ChainScore, InvariantViolation, SuffixWork,
        WorkCoordinate,
    };
    use zakura_chain::work::difficulty::U256;

    let (store, config) = TestStore::new(EngineMode::HeadersOnly);
    let clock = ManualClock(Utc::now());
    let request = insertion(&store, 2, EvidenceId::from_digest([8; 32]));
    let owner = request
        .event
        .header_sync_owner()
        .expect("insertion carries an owner");
    let plan = apply_transition(&store, request, &context(&config, &clock, None))
        .expect("the baseline plan satisfies every invariant");
    let tip = plan.change_set.metadata.frontiers.header_best;
    let baseline_graph = projected_graph(&store.graph, &plan);
    let first = baseline_graph
        .header_ancestor(tip.hash, block::Height(1))
        .expect("the baseline ancestry is coherent")
        .expect("height one is retained");

    let mut corrupt = plan.clone();
    crate::graph::test_support::mutate_updated_header(
        &mut corrupt.graph_delta,
        tip.hash,
        |header_node| header_node.hash = block::Hash([0; 32]),
    );
    assert!(matches!(
        verify_plan(&test_engine(&store), &corrupt),
        Err(InvariantViolation::NodeHash(_))
    ));

    let mut corrupt = plan.clone();
    crate::graph::test_support::mutate_updated_header(
        &mut corrupt.graph_delta,
        tip.hash,
        |header_node| header_node.parent_hash = block::Hash([0; 32]),
    );
    assert!(matches!(
        verify_plan(&test_engine(&store), &corrupt),
        Err(InvariantViolation::Index(_))
    ));

    let mut corrupt = plan.clone();
    let missing_index = corrupt
        .change_set
        .index_changes
        .inserted
        .iter()
        .min_by_key(|frontier| frontier.hash.0)
        .expect("the insertion plan adds indexed headers")
        .hash;
    corrupt.change_set.index_changes.inserted.clear();
    assert_eq!(
        verify_plan(&test_engine(&store), &corrupt),
        Err(InvariantViolation::Index(missing_index))
    );

    let mut corrupt = plan.clone();
    crate::graph::test_support::mutate_updated_header(
        &mut corrupt.graph_delta,
        tip.hash,
        |header_node| {
            header_node.work_coordinate = WorkCoordinate::new(block::Hash([0; 32]), U256::zero());
        },
    );
    assert!(matches!(
        verify_plan(&test_engine(&store), &corrupt),
        Err(InvariantViolation::Index(_))
    ));

    let mut corrupt = plan.clone();
    crate::graph::test_support::mutate_updated_header(
        &mut corrupt.graph_delta,
        tip.hash,
        |header_node| {
            header_node.eligibility.inherited_from = Some(block::Hash([0; 32]));
        },
    );
    assert!(matches!(
        verify_plan(&test_engine(&store), &corrupt),
        Err(InvariantViolation::Index(_))
    ));

    let mut corrupt = plan.clone();
    corrupt.change_set.selected_projection.put.clear();
    assert!(matches!(
        verify_plan(&test_engine(&store), &corrupt),
        Err(InvariantViolation::SelectedProjection(_))
    ));

    let mut corrupt = plan.clone();
    corrupt.change_set.metadata.header_best_score = ChainScore::new(SuffixWork::zero(), tip.hash);
    assert_eq!(
        verify_plan(&test_engine(&store), &corrupt),
        Err(InvariantViolation::Selection)
    );

    let mut corrupt = plan.clone();
    corrupt.change_set.verified_projection.put = vec![first, tip];
    corrupt.change_set.metadata.frontiers.verified_best = tip;
    assert!(matches!(
        verify_plan(&test_engine(&store), &corrupt),
        Err(InvariantViolation::VerifiedProjection(_))
    ));

    let mut corrupt = plan.clone();
    corrupt.trust_pins = corrupt
        .trust_pins
        .iter()
        .copied()
        .chain([Frontier::new(first.height, block::Hash([9; 32]))])
        .collect::<Vec<_>>()
        .into();
    assert_eq!(
        verify_plan(&test_engine(&store), &corrupt),
        Err(InvariantViolation::TrustPin(first.height))
    );

    let mut corrupt = plan.clone();
    let finalized = store.metadata.frontiers.finalized.hash;
    corrupt.change_set.delete_nodes.push(finalized);
    corrupt.change_set.index_changes.deleted.push(finalized);
    crate::graph::test_support::add_deleted_header(&mut corrupt.graph_delta, finalized);
    assert!(matches!(
        verify_plan(&test_engine(&store), &corrupt),
        Err(InvariantViolation::Index(_))
    ));

    let mut corrupt = plan.clone();
    corrupt.limits.max_non_finalized_nodes = NonZeroUsize::new(1).expect("one is nonzero");
    assert_eq!(
        verify_plan(&test_engine(&store), &corrupt),
        Err(InvariantViolation::Limits)
    );

    let mut corrupt = plan.clone();
    corrupt.change_set.metadata.header_generation = plan.snapshot_before_commit.header_generation;
    assert_eq!(
        verify_plan(&test_engine(&store), &corrupt),
        Err(InvariantViolation::Generation)
    );

    let mut corrupt = plan;
    let missing = block::Hash([0xab; 32]);
    corrupt
        .change_set
        .aux_changes
        .push(crate::AuxDelta::Put(Box::new(AuxDelivery {
            delivery_id: EvidenceId::from_digest([0xac; 32]),
            header_hash: missing,
            source: SourceId::from_digest([0xad; 32]),
            owner,
            body_size: BodySizeHint::Unknown,
            tree_aux: None,
            authentication: AuxAuthentication::Unauthenticated,
        })));
    assert_eq!(
        verify_plan(&test_engine(&store), &corrupt),
        Err(InvariantViolation::Auxiliary(missing))
    );
}
