use super::super::projected_state::path;
use super::*;
use crate::{AuxDelta, InvariantViolation};

fn unauthenticated_delivery(
    insert: &crate::InsertHeaders,
    delivery_id: EvidenceId,
) -> crate::AuxDelivery {
    let header = insert
        .batch
        .headers()
        .first()
        .expect("the insertion fixture is nonempty");
    crate::AuxDelivery::new(
        delivery_id,
        header.hash,
        insert.source,
        insert.owner,
        crate::BodySizeHint::Unknown,
        None,
    )
}

#[test]
fn auxiliary_delivery_ids_are_globally_unique_across_headers() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let delivery_id = EvidenceId::from_digest([0xd0; 32]);

    let mut first = insertion(&store, 1, EvidenceId::from_digest([0xd1; 32]));
    let TransitionEvent::InsertHeaders(insert) = &mut first.event else {
        unreachable!("the fixture constructs a header insertion")
    };
    insert
        .aux
        .push(unauthenticated_delivery(insert, delivery_id));
    let first = apply_transition(&store, first, &context(&config, &clock, None))
        .expect("the first globally unique delivery commits");
    store.commit(&first);

    let mut second = insertion(&store, 1, EvidenceId::from_digest([0xd2; 32]));
    let TransitionEvent::InsertHeaders(insert) = &mut second.event else {
        unreachable!("the fixture constructs a header insertion")
    };
    insert
        .aux
        .push(unauthenticated_delivery(insert, delivery_id));
    assert!(matches!(
        apply_transition(&store, second, &context(&config, &clock, None)),
        Err(TransitionFailure::InvalidEvidence(
            InvalidTransitionEvidence::Auxiliary(AuxiliaryViolation::ReplayConflict)
        ))
    ));
}

#[test]
fn auxiliary_evidence_rejects_invalid_counts_and_duplicate_identity() {
    let (store, _) = TestStore::new(EngineMode::Integrated);
    let mut insert = insertion(&store, 1, EvidenceId::from_digest([0xd6; 32]));
    let TransitionEvent::InsertHeaders(insert_event) = &mut insert.event else {
        unreachable!("the fixture constructs a header insertion")
    };
    let delivery = unauthenticated_delivery(insert_event, EvidenceId::from_digest([0xd7; 32]));
    let owner = body_owner(&store.snapshot(), 7, 1);
    assert!(crate::AuxObservationV1::from_vct(
        owner,
        Vec::new(),
        crate::AuxVerificationFactV1::current_delivery_verified(),
        None,
    )
    .is_none());
    assert!(crate::AuxObservationV1::from_vct(
        owner,
        vec![delivery, delivery],
        crate::AuxVerificationFactV1::ambiguous_deliveries_failed(1),
        None,
    )
    .is_none());
}

#[test]
fn same_transition_auxiliary_eviction_has_no_generation_effect() {
    use zakura_chain::work::difficulty::{ExpandedDifficulty, U256};

    let (mut store, mut config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let anchor = store.graph.finalized_frontier();
    let easy = store
        .graph
        .header_node(anchor.hash)
        .expect("the anchor exists")
        .header
        .difficulty_threshold;
    let easy_target: U256 = easy.to_expanded().expect("the target expands").into();
    let hard = ExpandedDifficulty::from(easy_target >> 3).into();
    let incumbent = insert_verified_branch(&mut store.graph, anchor, 1, hard, 0xd3);
    synchronize_fixture(&mut store, incumbent);
    store.lease = ValidationLease::new(
        anchor,
        vec![HeaderContextFact {
            frontier: anchor,
            header: store
                .graph
                .header_node(anchor.hash)
                .expect("the anchor remains retained")
                .header
                .clone(),
        }],
        config.network.clone(),
        config.trust_anchor_digest(),
    );
    config.limits.max_non_finalized_nodes = std::num::NonZeroUsize::new(1).expect("one is nonzero");

    let mut request = insertion(&store, 1, EvidenceId::from_digest([0xd4; 32]));
    let TransitionEvent::InsertHeaders(insert) = &mut request.event else {
        unreachable!("the fixture constructs a header insertion")
    };
    insert.aux.push(unauthenticated_delivery(
        insert,
        EvidenceId::from_digest([0xd5; 32]),
    ));
    let plan = apply_transition(&store, request, &context(&config, &clock, None))
        .expect("the immediately evicted branch is a coherent no-op");

    assert!(plan.is_no_change());
    assert_eq!(plan.change_set.metadata, store.metadata);
    assert!(plan.change_set.aux_changes.is_empty());
}

#[test]
fn auxiliary_deletes_for_evicted_headers_are_sorted_by_hash_and_delivery_id() {
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

    let verified_tip = insert_verified_branch(&mut store.graph, old_finalized, 3, easy, 0xe1);
    let verified_path =
        path(&store.graph, verified_tip).expect("the verified fixture path is retained");
    let new_finalized = verified_path[1];
    let selected_tip = insert_verified_branch(&mut store.graph, old_finalized, 2, hard, 0xe2);
    synchronize_fixture(&mut store, verified_tip);
    assert_eq!(store.metadata.frontiers.header_best, selected_tip);

    let owner = crate::HeaderWorkOwner {
        authority: crate::HeaderWorkAuthority {
            header_generation: store.metadata.header_generation,
            branch: BranchId::new(old_finalized.hash, selected_tip.hash),
        },
        session_id: 1,
        request_id: NonZeroU64::new(1).expect("one is nonzero"),
    }
    .into();
    let source = SourceId::from_digest([0xe3; 32]);
    let competing = store.selected.iter().copied().skip(1).collect::<Vec<_>>();
    assert_eq!(
        competing.len(),
        2,
        "the competing branch has two retained headers"
    );
    // Attach deliveries out of sorted order so the plan must impose (hash, id) order.
    let deliveries = [
        crate::AuxDelivery::new(
            EvidenceId::from_digest([0xf2; 32]),
            competing[1].hash,
            source,
            owner,
            crate::BodySizeHint::Unknown,
            None,
        ),
        crate::AuxDelivery::new(
            EvidenceId::from_digest([0xf0; 32]),
            competing[0].hash,
            source,
            owner,
            crate::BodySizeHint::Unknown,
            None,
        ),
        crate::AuxDelivery::new(
            EvidenceId::from_digest([0xf1; 32]),
            competing[0].hash,
            source,
            owner,
            crate::BodySizeHint::Unknown,
            None,
        ),
    ];
    for delivery in &deliveries {
        store
            .graph
            .record_auxiliary_evidence_delivery(delivery.header_hash, delivery.delivery_id)
            .expect("the competing header remains retained");
        store.aux.push(*delivery);
    }

    let plan = apply_transition(
        &store,
        TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::FullStateFinalized(crate::FullStateFinalized {
                full_state_transition_id: EvidenceId::from_digest([0xe4; 32]),
                new_finalized,
                verified_path_proof: vec![old_finalized.hash, new_finalized.hash],
            }),
        },
        &context(&config, &clock, Some(&authority)),
    )
    .expect("authenticated full-state finality prunes the competing branch");

    let mut expected: Vec<_> = deliveries
        .iter()
        .map(|delivery| AuxDelta::Delete {
            header_hash: delivery.header_hash,
            delivery_id: delivery.delivery_id,
        })
        .collect();
    expected.sort_unstable_by_key(|change| match change {
        AuxDelta::Delete {
            header_hash,
            delivery_id,
        } => (header_hash.0, *delivery_id),
        AuxDelta::Put(_) => unreachable!("the fixture constructs only deletes"),
    });
    assert_eq!(plan.change_set.aux_changes, expected);
    assert!(competing
        .iter()
        .all(|frontier| plan.change_set.delete_nodes.contains(&frontier.hash)));
}

fn body_owner(snapshot: &EngineSnapshot, session_id: u64, request_id: u64) -> crate::BodyWorkOwner {
    crate::BodyWorkAuthority::for_snapshot(snapshot).bind(
        session_id,
        NonZeroU64::new(request_id).expect("fixture request IDs are nonzero"),
    )
}

#[test]
fn selected_auxiliary_repair_adds_only_one_exact_provenance_record() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let anchor = store.metadata.frontiers.finalized;
    let insert = insertion(&store, 2, EvidenceId::from_digest([0x66; 32]));
    let TransitionEvent::InsertHeaders(initial) = &insert.event else {
        panic!("the fixture constructs a header insertion");
    };
    let repaired = initial.batch.headers()[0].clone();
    let selected_target = Frontier::new(repaired.height, repaired.hash);
    let inserted = apply_transition(&store, insert, &context(&config, &clock, None))
        .expect("the selected fixture branch inserts");
    store.commit(&inserted);

    store.lease.parent = anchor;
    store.lease.context_digest = [0x67; 32];
    let owner = body_owner(&store.snapshot(), 8, 9);
    let source = SourceId::from_digest([0x68; 32]);
    let delivery = crate::AuxDelivery::new(
        EvidenceId::from_digest([0x69; 32]),
        repaired.hash,
        source,
        owner.into(),
        crate::BodySizeHint::Unknown,
        Some(crate::TreeAuxRecordV1 {
            height: repaired.height,
            sapling_root: zakura_chain::sapling::tree::Root::default(),
            orchard_root: zakura_chain::orchard::tree::Root::default(),
            ironwood_root: zakura_chain::ironwood::tree::Root::default(),
            sapling_tx_count: 1,
            orchard_tx_count: 2,
            ironwood_tx_count: 3,
            auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([0x6a; 32]),
        }),
    );
    let repair = TransitionRequest {
        expected_version: store.metadata.state_version,
        event: TransitionEvent::InsertHeaders(Box::new(crate::InsertHeaders {
            owner: owner.into(),
            source,
            parent_hash: anchor.hash,
            target_tip_hash: repaired.hash,
            completion: TargetCompletion::SelectedAuxiliaryRepair {
                common_ancestor: anchor,
                selected_target,
            },
            batch: PreparedHeaderBatch::new(
                vec![repaired],
                anchor,
                store.lease.network().clone(),
                store.lease.trust_anchor_digest,
                EvidenceId::from_digest([0x6b; 32]),
            )
            .expect("the exact repair batch is nonempty"),
            aux: vec![delivery],
        })),
    };

    assert_eq!(
        repair.event.idempotency_key(),
        Some(delivery.delivery_id),
        "repair replay identity is the new provenance record, not the old header batch"
    );
    let mut wrongly_header_authorized = repair.clone();
    let TransitionEvent::InsertHeaders(wrong) = &mut wrongly_header_authorized.event else {
        panic!("the fixture constructs a header insertion");
    };
    let body_owner = wrong
        .owner
        .body_owner()
        .expect("the fixture repair has body authority");
    let header_owner = crate::HeaderWorkOwner {
        authority: body_owner.authority.header,
        session_id: body_owner.session_id,
        request_id: body_owner.request_id,
    };
    wrong.owner = header_owner.into();
    wrong.aux[0].owner = header_owner.into();
    assert!(matches!(
        apply_transition(
            &store,
            wrongly_header_authorized,
            &context(&config, &clock, None)
        ),
        Err(TransitionFailure::InvalidEvidence(
            InvalidTransitionEvidence::Header(HeaderViolation::RepairOwnerRoleMismatch)
        ))
    ));
    let repaired = apply_transition(&store, repair, &context(&config, &clock, None))
        .expect("one exact selected auxiliary repair is admitted");
    assert_eq!(repaired.change_set.put_nodes.len(), 1);
    assert_eq!(repaired.change_set.put_nodes[0].hash, selected_target.hash);
    assert_eq!(
        repaired.change_set.put_nodes[0].aux_delivery_ids,
        vec![delivery.delivery_id]
    );
    assert!(repaired.change_set.delete_nodes.is_empty());
    assert!(repaired.change_set.selected_projection.put.is_empty());
    assert!(repaired.change_set.verified_projection.put.is_empty());
    assert_eq!(
        repaired.change_set.metadata.header_generation,
        store.metadata.header_generation
    );
    assert_eq!(
        repaired.change_set.metadata.verified_generation,
        store.metadata.verified_generation
    );
    assert_eq!(
        repaired.change_set.aux_changes,
        vec![crate::AuxDelta::Put(Box::new(delivery))]
    );
}

#[test]
fn auxiliary_delivery_is_batch_hash_scoped_and_selection_neutral() {
    let (store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let without_aux = insertion(&store, 2, EvidenceId::from_digest([0x70; 32]));
    let TransitionEvent::InsertHeaders(insert) = &without_aux.event else {
        panic!("the fixture constructs a header insertion");
    };
    let prepared = insert.batch.headers()[0].clone();
    let delivery = crate::AuxDelivery::new(
        EvidenceId::from_digest([0x71; 32]),
        prepared.hash,
        insert.source,
        insert.owner,
        crate::BodySizeHint::Unknown,
        Some(crate::TreeAuxRecordV1 {
            height: prepared.height,
            sapling_root: zakura_chain::sapling::tree::Root::default(),
            orchard_root: zakura_chain::orchard::tree::Root::default(),
            ironwood_root: zakura_chain::ironwood::tree::Root::default(),
            sapling_tx_count: 1,
            orchard_tx_count: 2,
            ironwood_tx_count: 3,
            auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([0x72; 32]),
        }),
    );
    let without_plan =
        apply_transition(&store, without_aux.clone(), &context(&config, &clock, None))
            .expect("the control target inserts without advisory metadata");
    let mut with_aux = without_aux.clone();
    let TransitionEvent::InsertHeaders(insert) = &mut with_aux.event else {
        unreachable!("the cloned fixture remains a header insertion")
    };
    insert.aux.push(delivery);
    let with_plan = apply_transition(&store, with_aux, &context(&config, &clock, None))
        .expect("one exact hash-scoped auxiliary delivery is admitted");

    let mut with_metadata = with_plan.change_set.metadata.clone();
    let mut without_metadata = without_plan.change_set.metadata.clone();
    with_metadata.last_transition = None;
    without_metadata.last_transition = None;
    assert_eq!(with_metadata, without_metadata);
    assert_eq!(
        with_plan.change_set.selected_projection,
        without_plan.change_set.selected_projection
    );
    assert_eq!(
        with_plan.change_set.verified_projection,
        without_plan.change_set.verified_projection
    );
    assert_eq!(
        with_plan.change_set.eligibility_changes,
        without_plan.change_set.eligibility_changes
    );
    assert_eq!(
        with_plan.change_set.aux_changes,
        vec![crate::AuxDelta::Put(Box::new(delivery))]
    );

    let mut unretained_aux = vec![crate::AuxDelta::Put(Box::new(delivery))];
    unretained_aux.retain(|change| match change {
        crate::AuxDelta::Put(delivery) => store.graph.header_node(delivery.header_hash).is_some(),
        crate::AuxDelta::Delete { .. } => true,
    });
    assert!(
        unretained_aux.is_empty(),
        "auxiliary metadata for a node removed in the same plan is not persisted"
    );

    let mut unrelated = delivery;
    unrelated.header_hash = store.metadata.frontiers.finalized.hash;
    unrelated.tree_aux = None;
    let mut wrong_height = delivery;
    wrong_height
        .tree_aux
        .as_mut()
        .expect("the fixture has tree auxiliary data")
        .height = prepared
        .height
        .next()
        .expect("the bounded fixture height advances");
    let mut wrong_owner = delivery;
    wrong_owner.owner = match wrong_owner.owner {
        crate::HeaderSyncWorkOwner::Header(owner) => crate::HeaderWorkOwner {
            session_id: owner.session_id.saturating_add(1),
            ..owner
        }
        .into(),
        crate::HeaderSyncWorkOwner::BodyRepair(owner) => crate::BodyWorkOwner {
            session_id: owner.session_id.saturating_add(1),
            ..owner
        }
        .into(),
    };
    let mut wrong_source = delivery;
    wrong_source.source = SourceId::from_digest([0x73; 32]);
    let preauthenticated = delivery
        .promote_recovered_outcome(1, [Some([0x74; 32]), None], Some(block::Hash([0x75; 32])))
        .expect("the test outcome is coherent");
    for (label, deliveries) in [
        ("header outside the admitted batch", vec![unrelated]),
        ("tree-aux height mismatch", vec![wrong_height]),
        ("duplicate delivery identity", vec![delivery, delivery]),
        ("different work owner", vec![wrong_owner]),
        ("different source", vec![wrong_source]),
        ("premature authentication", vec![preauthenticated]),
    ] {
        let mut request = without_aux.clone();
        let TransitionEvent::InsertHeaders(insert) = &mut request.event else {
            unreachable!("the cloned fixture remains a header insertion")
        };
        insert.aux = deliveries;
        assert!(
            matches!(
                apply_transition(&store, request, &context(&config, &clock, None)),
                Err(TransitionFailure::InvalidEvidence(
                    InvalidTransitionEvidence::Auxiliary(
                        AuxiliaryViolation::AdmittedTargetMismatch
                    )
                ))
            ),
            "{label}"
        );
        assert_eq!(
            store.snapshot(),
            without_plan.snapshot_before_commit,
            "{label} changed the snapshot before commit"
        );
    }
}

#[test]
fn auxiliary_outcomes_derive_from_exact_owned_observations() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let mut insert = insertion(&store, 2, EvidenceId::from_digest([0xb0; 32]));
    let TransitionEvent::InsertHeaders(insert_event) = &mut insert.event else {
        unreachable!("the insertion fixture contains header evidence")
    };
    let header_hash = insert_event.batch.headers()[0].hash;
    let boundary_hash = insert_event.batch.headers()[1].hash;
    let tree_aux = |height, marker| crate::TreeAuxRecordV1 {
        height: block::Height(height),
        sapling_root: zakura_chain::sapling::tree::Root::default(),
        orchard_root: zakura_chain::orchard::tree::Root::default(),
        ironwood_root: zakura_chain::ironwood::tree::Root::default(),
        sapling_tx_count: 3,
        orchard_tx_count: 4,
        ironwood_tx_count: 5,
        auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([marker; 32]),
    };
    let delivery = crate::AuxDelivery::new(
        EvidenceId::from_digest([0xb1; 32]),
        header_hash,
        SourceId::from_digest([0xb2; 32]),
        insert_event.owner,
        crate::BodySizeHint::Unknown,
        Some(tree_aux(1, 0xb3)),
    );
    let second_delivery = crate::AuxDelivery::new(
        EvidenceId::from_digest([0xc1; 32]),
        boundary_hash,
        delivery.source,
        delivery.owner,
        crate::BodySizeHint::Unknown,
        Some(tree_aux(2, 0xc2)),
    );
    let mut third_delivery = delivery;
    third_delivery.delivery_id = EvidenceId::from_digest([0xc3; 32]);
    insert_event.source = delivery.source;
    insert_event
        .aux
        .extend([delivery, second_delivery, third_delivery]);
    let inserted = apply_transition(&store, insert, &context(&config, &clock, None))
        .expect("the target and unauthenticated delivery insert atomically");
    store.commit(&inserted);

    let repair_owner = body_owner(&store.snapshot(), 2, 2);
    let observed = |deliveries: Vec<crate::AuxDelivery>,
                    verification: crate::AuxVerificationFactV1,
                    witness: Option<zakura_chain::block::merkle::AuthDataRoot>| {
        TransitionEvent::AuxEvidence(Box::new(crate::AuxEvidence::observed(
            crate::AuxObservationV1::from_vct(repair_owner, deliveries, verification, witness)
                .expect("the observation fixture is valid"),
        )))
    };

    let mut changed_provenance = delivery;
    changed_provenance.source = SourceId::from_digest([0xb5; 32]);
    assert!(matches!(
        apply_transition(
            &store,
            TransitionRequest {
                expected_version: store.metadata.state_version,
                event: observed(
                    vec![changed_provenance],
                    crate::AuxVerificationFactV1::current_delivery_verified(),
                    Some([0xb4; 32].into()),
                ),
            },
            &context(&config, &clock, Some(&Authority)),
        ),
        Err(TransitionFailure::InvalidEvidence(
            InvalidTransitionEvidence::Auxiliary(AuxiliaryViolation::ProvenanceMismatch)
        ))
    ));
    let missing = apply_transition(
        &store,
        TransitionRequest {
            expected_version: store.metadata.state_version,
            event: observed(
                vec![delivery],
                crate::AuxVerificationFactV1::current_delivery_verified(),
                None,
            ),
        },
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("missing boundary evidence is a verified no-change");
    assert!(missing.is_no_change());

    let disputed = apply_transition(
        &store,
        TransitionRequest {
            expected_version: store.metadata.state_version,
            event: observed(
                vec![third_delivery, second_delivery],
                crate::AuxVerificationFactV1::ambiguous_deliveries_failed(2),
                Some([0xb7; 32].into()),
            ),
        },
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("ambiguous observations dispute both deliveries");
    assert_eq!(disputed.change_set.aux_changes.len(), 2);
    assert!(disputed.change_set.aux_changes.iter().all(|change| {
        matches!(
            change,
            AuxDelta::Put(delivery)
                if delivery.outcome().status() == crate::AuxOutcomeStatus::Disputed
                    && delivery.outcome().boundary_hash() == Some(boundary_hash)
        )
    }));
    store.commit(&disputed);

    let before = store.snapshot();
    let authentication_event = observed(
        vec![delivery],
        crate::AuxVerificationFactV1::current_delivery_verified(),
        Some([0xb4; 32].into()),
    );
    let authenticated = apply_transition(
        &store,
        TransitionRequest {
            expected_version: store.metadata.state_version,
            event: authentication_event.clone(),
        },
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("exact integrated evidence authenticates metadata only");
    assert!(authenticated.change_set.put_nodes.is_empty());
    assert!(authenticated.change_set.eligibility_changes.is_empty());
    assert_eq!(
        authenticated.change_set.metadata.frontiers,
        before.frontiers
    );
    assert_eq!(
        authenticated.change_set.metadata.header_generation,
        before.header_generation
    );
    assert_eq!(
        authenticated.change_set.metadata.verified_generation,
        before.verified_generation
    );
    assert!(authenticated.effect().is_aux_authentication());
    assert_eq!(authenticated.domain(), TransitionDomain::AuxEvidence);
    assert!(
        super::super::super::invariants::is_incremental_aux_authentication(
            &test_engine(&store),
            &authenticated
        )
    );
    let AuxDelta::Put(authenticated_delivery) = &authenticated.change_set.aux_changes[0] else {
        unreachable!("authentication puts one delivery")
    };
    assert_eq!(
        authenticated_delivery.outcome().status(),
        crate::AuxOutcomeStatus::Authenticated
    );
    assert_eq!(
        authenticated_delivery.outcome().boundary_hash(),
        Some(boundary_hash)
    );

    let mut corrupt = authenticated.clone();
    let AuxDelta::Put(corrupt_delivery) = &mut corrupt.change_set.aux_changes[0] else {
        unreachable!("the authenticated plan replaces one delivery");
    };
    corrupt_delivery.source = SourceId::from_digest([0xee; 32]);
    assert_eq!(
        verify_plan(&test_engine(&store), &corrupt),
        Err(InvariantViolation::Auxiliary(header_hash))
    );
    store.commit(&authenticated);

    let rejected = apply_transition(
        &store,
        TransitionRequest {
            expected_version: store.metadata.state_version,
            event: observed(
                vec![second_delivery],
                crate::AuxVerificationFactV1::successor_delivery_failed(2),
                Some([0xc5; 32].into()),
            ),
        },
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("two exact metadata deliveries reject in one atomic transition");
    assert!(rejected.effect().is_aux_authentication());
    assert_eq!(rejected.domain(), TransitionDomain::AuxEvidence);
    let AuxDelta::Put(rejected_delivery) = &rejected.change_set.aux_changes[0] else {
        unreachable!("rejection puts one delivery")
    };
    assert_eq!(
        rejected_delivery.outcome().status(),
        crate::AuxOutcomeStatus::Rejected
    );
    assert_eq!(
        rejected_delivery.outcome().boundary_hash(),
        Some(boundary_hash)
    );
    assert_eq!(
        rejected.change_set.metadata.state_version,
        store
            .metadata
            .state_version
            .checked_next()
            .expect("the fixture state version can advance"),
        "the rejection advances one atomic state version"
    );
    store.commit(&rejected);

    let replay = apply_transition(
        &store,
        TransitionRequest {
            expected_version: store.metadata.state_version,
            event: authentication_event,
        },
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("authentication replay is idempotent");
    assert!(replay.is_no_change());
}

#[test]
fn auxiliary_resource_limits_reject_equal_plus_one_without_effects() {
    let (store, mut config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    config.limits.max_aux_deliveries_per_header =
        std::num::NonZeroUsize::new(1).expect("one is nonzero");
    config.limits.max_aux_deliveries_total =
        std::num::NonZeroUsize::new(2).expect("two is nonzero");

    let mut exact = insertion(&store, 1, EvidenceId::from_digest([0xd1; 32]));
    let TransitionEvent::InsertHeaders(insert) = &mut exact.event else {
        unreachable!("the fixture is a header insertion");
    };
    let header_hash = insert.batch.headers()[0].hash;
    insert.aux.push(crate::AuxDelivery::new(
        EvidenceId::from_digest([0xd2; 32]),
        header_hash,
        insert.source,
        insert.owner,
        crate::BodySizeHint::Unknown,
        None,
    ));
    apply_transition(&store, exact.clone(), &context(&config, &clock, None))
        .expect("the exact per-header auxiliary limit is admitted");

    let mut oversized = exact;
    let TransitionEvent::InsertHeaders(insert) = &mut oversized.event else {
        unreachable!("the fixture is a header insertion");
    };
    let mut duplicate = insert.aux[0];
    duplicate.delivery_id = EvidenceId::from_digest([0xd3; 32]);
    insert.aux.push(duplicate);
    assert!(matches!(
        apply_transition(&store, oversized.clone(), &context(&config, &clock, None)),
        Err(TransitionFailure::AuxiliaryLimitExceeded)
    ));

    config.limits.max_aux_deliveries_per_header =
        std::num::NonZeroUsize::new(2).expect("two is nonzero");
    config.limits.max_aux_deliveries_total =
        std::num::NonZeroUsize::new(1).expect("one is nonzero");
    assert!(matches!(
        apply_transition(&store, oversized, &context(&config, &clock, None)),
        Err(TransitionFailure::AuxiliaryLimitExceeded)
    ));
    assert_eq!(store.metadata.state_version, StateVersion::new(0));
    assert!(!store.metadata.alarms.resource_stalled);
    assert!(store.aux.is_empty());
}
