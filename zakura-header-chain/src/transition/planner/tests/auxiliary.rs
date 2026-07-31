use super::*;

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
    let owner = crate::WorkScope::for_body_work(&store.snapshot())
        .bind(8, NonZeroU64::new(9).expect("nine is nonzero"));
    let source = SourceId::from_digest([0x68; 32]);
    let delivery = crate::AuxDelivery {
        delivery_id: EvidenceId::from_digest([0x69; 32]),
        header_hash: repaired.hash,
        source,
        owner,
        body_size: crate::BodySizeHint::Unknown,
        tree_aux: Some(crate::TreeAuxRecordV1 {
            height: repaired.height,
            sapling_root: zakura_chain::sapling::tree::Root::default(),
            orchard_root: zakura_chain::orchard::tree::Root::default(),
            ironwood_root: zakura_chain::ironwood::tree::Root::default(),
            sapling_tx_count: 1,
            orchard_tx_count: 2,
            ironwood_tx_count: 3,
            auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([0x6a; 32]),
        }),
        authentication: crate::AuxAuthentication::Unauthenticated,
    };
    let repair = TransitionRequest {
        expected_version: store.metadata.state_version,
        event: TransitionEvent::InsertHeaders(Box::new(crate::InsertHeaders {
            owner,
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
    let delivery = crate::AuxDelivery {
        delivery_id: EvidenceId::from_digest([0x71; 32]),
        header_hash: prepared.hash,
        source: insert.source,
        owner: insert.owner,
        body_size: crate::BodySizeHint::Unknown,
        tree_aux: Some(crate::TreeAuxRecordV1 {
            height: prepared.height,
            sapling_root: zakura_chain::sapling::tree::Root::default(),
            orchard_root: zakura_chain::orchard::tree::Root::default(),
            ironwood_root: zakura_chain::ironwood::tree::Root::default(),
            sapling_tx_count: 1,
            orchard_tx_count: 2,
            ironwood_tx_count: 3,
            auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([0x72; 32]),
        }),
        authentication: crate::AuxAuthentication::Unauthenticated,
    };
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

    assert_eq!(
        with_plan.change_set.metadata,
        without_plan.change_set.metadata
    );
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
        crate::AuxDelta::Put(delivery) => store.graph.node(delivery.header_hash).is_some(),
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
    wrong_owner.owner.session_id = wrong_owner.owner.session_id.saturating_add(1);
    let mut wrong_source = delivery;
    wrong_source.source = SourceId::from_digest([0x73; 32]);
    let mut preauthenticated = delivery;
    preauthenticated.authentication = crate::AuxAuthentication::Authenticated {
        evidence: EvidenceId::from_digest([0x74; 32]),
        boundary_hash: block::Hash([0x75; 32]),
    };
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
                    "auxiliary delivery does not match the admitted target"
                ))
            ),
            "{label}"
        );
        assert_eq!(
            store.snapshot(),
            without_plan.before,
            "{label} changed the source snapshot"
        );
    }
}

#[test]
fn auxiliary_authentication_requires_exact_provenance_and_owned_next_header() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let mut insert = insertion(&store, 2, EvidenceId::from_digest([0xb0; 32]));
    let TransitionEvent::InsertHeaders(insert_event) = &mut insert.event else {
        unreachable!("the insertion fixture contains header evidence")
    };
    let header_hash = insert_event.batch.headers()[0].hash;
    let boundary_hash = insert_event.batch.headers()[1].hash;
    let delivery = crate::AuxDelivery {
        delivery_id: EvidenceId::from_digest([0xb1; 32]),
        header_hash,
        source: SourceId::from_digest([0xb2; 32]),
        owner: insert_event.owner,
        body_size: crate::BodySizeHint::Unknown,
        tree_aux: Some(crate::TreeAuxRecordV1 {
            height: block::Height(1),
            sapling_root: zakura_chain::sapling::tree::Root::default(),
            orchard_root: zakura_chain::orchard::tree::Root::default(),
            ironwood_root: zakura_chain::ironwood::tree::Root::default(),
            sapling_tx_count: 3,
            orchard_tx_count: 4,
            ironwood_tx_count: 5,
            auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([0xb3; 32]),
        }),
        authentication: crate::AuxAuthentication::Unauthenticated,
    };
    insert_event.source = delivery.source;
    let second_delivery = crate::AuxDelivery {
        delivery_id: EvidenceId::from_digest([0xc1; 32]),
        ..delivery
    };
    let third_delivery = crate::AuxDelivery {
        delivery_id: EvidenceId::from_digest([0xc3; 32]),
        ..delivery
    };
    insert_event
        .aux
        .extend([delivery, second_delivery, third_delivery]);
    let inserted = apply_transition(&store, insert, &context(&config, &clock, None))
        .expect("the target and unauthenticated delivery insert atomically");
    store.commit(&inserted);

    let repair_owner = WorkOwner {
        state_version: store.metadata.state_version,
        header_generation: store.metadata.header_generation,
        verified_generation: Some(store.metadata.verified_generation),
        branch: BranchId::new(
            store.metadata.frontiers.finalized.hash,
            store.metadata.frontiers.header_best.hash,
        ),
        session_id: 2,
        request_id: NonZeroU64::new(2).expect("two is nonzero"),
    };
    let authentication = crate::AuxAuthentication::Authenticated {
        evidence: EvidenceId::from_digest([0xb4; 32]),
        boundary_hash,
    };
    let request = |delivery, authentication| TransitionRequest {
        expected_version: store.metadata.state_version,
        event: TransitionEvent::AuxEvidence(Box::new(crate::AuxEvidence {
            owner: repair_owner,
            deliveries: vec![delivery],
            authentication,
        })),
    };

    let mut changed_provenance = delivery;
    changed_provenance.source = SourceId::from_digest([0xb5; 32]);
    assert!(matches!(
        apply_transition(
            &store,
            request(changed_provenance, authentication),
            &context(&config, &clock, Some(&Authority)),
        ),
        Err(TransitionFailure::InvalidEvidence(
            "auxiliary evidence changes delivery provenance"
        ))
    ));
    let wrong_boundary = crate::AuxAuthentication::Authenticated {
        evidence: EvidenceId::from_digest([0xb6; 32]),
        boundary_hash: header_hash,
    };
    assert!(matches!(
        apply_transition(
            &store,
            request(delivery, wrong_boundary),
            &context(&config, &clock, Some(&Authority)),
        ),
        Err(TransitionFailure::InvalidEvidence(
            "auxiliary authentication is not the owned one-header-later boundary"
        ))
    ));

    let before = store.snapshot();
    let authenticated = apply_transition(
        &store,
        request(delivery, authentication),
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
    assert_eq!(
        authenticated.change_set.aux_changes,
        vec![crate::AuxDelta::Put(Box::new(crate::AuxDelivery {
            authentication,
            ..delivery
        }))]
    );
    store.commit(&authenticated);

    let rejection = crate::AuxAuthentication::Rejected {
        evidence: EvidenceId::from_digest([0xc5; 32]),
    };
    let rejection_owner = WorkOwner {
        state_version: store.metadata.state_version,
        header_generation: store.metadata.header_generation,
        verified_generation: Some(store.metadata.verified_generation),
        ..repair_owner
    };
    let rejected = apply_transition(
        &store,
        TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::AuxEvidence(Box::new(crate::AuxEvidence {
                owner: rejection_owner,
                deliveries: vec![second_delivery, third_delivery],
                authentication: rejection,
            })),
        },
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("two exact metadata deliveries reject in one atomic transition");
    assert_eq!(
        rejected.change_set.aux_changes,
        vec![
            crate::AuxDelta::Put(Box::new(crate::AuxDelivery {
                authentication: rejection,
                ..second_delivery
            })),
            crate::AuxDelta::Put(Box::new(crate::AuxDelivery {
                authentication: rejection,
                ..third_delivery
            })),
        ],
    );
    assert_eq!(
        rejected.change_set.metadata.state_version,
        store
            .metadata
            .state_version
            .checked_next()
            .expect("the fixture state version can advance"),
        "the two-delivery rejection advances one atomic state version"
    );
    store.commit(&rejected);

    let replay = apply_transition(
        &store,
        TransitionRequest {
            expected_version: store.metadata.state_version,
            event: TransitionEvent::AuxEvidence(Box::new(crate::AuxEvidence {
                owner: WorkOwner {
                    state_version: store.metadata.state_version,
                    header_generation: store.metadata.header_generation,
                    verified_generation: Some(store.metadata.verified_generation),
                    ..repair_owner
                },
                deliveries: vec![crate::AuxDelivery {
                    authentication,
                    ..delivery
                }],
                authentication,
            })),
        },
        &context(&config, &clock, Some(&Authority)),
    )
    .expect("authentication replay is idempotent");
    assert!(replay.is_no_change());
}
