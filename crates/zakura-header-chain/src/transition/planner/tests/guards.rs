use super::*;

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
            "retained-path references exceed the per-transition limit"
        ))
    ));
}

#[test]
fn typed_header_authority_ignores_global_version_but_rejects_stale_generation() {
    let (store, config) = TestStore::new(EngineMode::HeadersOnly);
    let clock = ManualClock(Utc::now());
    let mut request = insertion(&store, 1, EvidenceId::from_digest([6; 32]));
    request.expected_version = StateVersion::new(9);
    apply_transition(&store, request, &context(&config, &clock, None))
        .expect("global state versions do not authorize header work");

    let mut request = insertion(&store, 1, EvidenceId::from_digest([7; 32]));
    let TransitionEvent::InsertHeaders(insert) = &mut request.event else {
        panic!("the fixture constructs a header insertion");
    };
    let owner = insert
        .owner
        .header_owner()
        .expect("the fixture is ordinary header work");
    insert.owner = crate::HeaderWorkOwner {
        authority: crate::HeaderWorkAuthority {
            header_generation: HeaderGeneration::new(9),
            ..owner.authority
        },
        ..owner
    }
    .into();
    assert!(matches!(
        apply_transition(&store, request, &context(&config, &clock, None)),
        Err(TransitionFailure::Stale { current }) if current == StateVersion::new(0)
    ));
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
            "target completion ancestor does not match the retained parent"
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
            .all(|node| node.body == BodyValidationState::Unknown),
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
fn every_named_invariant_category_rejects_its_projected_corruption() {
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
    let first = plan
        .projected
        .ancestor(tip.hash, block::Height(1))
        .expect("the baseline ancestry is coherent")
        .expect("height one is retained");

    let mut corrupt = plan.clone();
    corrupt
        .projected
        .node_mut(tip.hash)
        .expect("tip exists")
        .hash = block::Hash([0; 32]);
    assert!(matches!(
        verify_plan(&test_engine(&store), &corrupt),
        Err(InvariantViolation::NodeHash(_))
    ));

    let mut corrupt = plan.clone();
    corrupt
        .projected
        .node_mut(tip.hash)
        .expect("tip exists")
        .parent_hash = block::Hash([0; 32]);
    assert!(matches!(
        verify_plan(&test_engine(&store), &corrupt),
        Err(InvariantViolation::Parent(_))
    ));

    let mut corrupt = plan.clone();
    corrupt.change_set.index_changes.inserted.clear();
    assert!(matches!(
        verify_plan(&test_engine(&store), &corrupt),
        Err(InvariantViolation::Index(_))
    ));

    let mut corrupt = plan.clone();
    corrupt
        .projected
        .node_mut(tip.hash)
        .expect("tip exists")
        .work_coordinate = WorkCoordinate::new(block::Hash([0; 32]), U256::zero());
    assert!(matches!(
        verify_plan(&test_engine(&store), &corrupt),
        Err(InvariantViolation::Work(_))
    ));

    let mut corrupt = plan.clone();
    corrupt
        .projected
        .node_mut(tip.hash)
        .expect("tip exists")
        .eligibility
        .inherited_from = Some(block::Hash([0; 32]));
    assert!(matches!(
        verify_plan(&test_engine(&store), &corrupt),
        Err(InvariantViolation::Eligibility(_))
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
    corrupt
        .trust_pins
        .push(Frontier::new(first.height, block::Hash([9; 32])));
    assert_eq!(
        verify_plan(&test_engine(&store), &corrupt),
        Err(InvariantViolation::TrustPin(first.height))
    );

    let mut corrupt = plan.clone();
    let finalized = store.metadata.frontiers.finalized.hash;
    corrupt.change_set.delete_nodes.push(finalized);
    corrupt.change_set.index_changes.deleted.push(finalized);
    corrupt.graph_delta.delete_nodes.push(finalized);
    assert_eq!(
        verify_plan(&test_engine(&store), &corrupt),
        Err(InvariantViolation::Protected(finalized))
    );

    let mut corrupt = plan.clone();
    corrupt.limits.max_non_finalized_nodes = NonZeroUsize::new(1).expect("one is nonzero");
    assert_eq!(
        verify_plan(&test_engine(&store), &corrupt),
        Err(InvariantViolation::Limits)
    );

    let mut corrupt = plan.clone();
    corrupt.change_set.metadata.header_generation = plan.before.header_generation;
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
