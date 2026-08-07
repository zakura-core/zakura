use super::*;

#[test]
fn ordinary_header_insertion_rejects_body_repair_authority() {
    let (store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let mut request = insertion(&store, 1, EvidenceId::from_digest([0x2f; 32]));
    let TransitionEvent::InsertHeaders(insert) = &mut request.event else {
        panic!("the fixture request inserts headers");
    };
    let header_owner = insert
        .owner
        .header_owner()
        .expect("the fixture is ordinary header work");
    insert.owner = crate::BodyWorkOwner {
        authority: crate::BodyWorkAuthority {
            header: header_owner.authority,
            verified_generation: store.metadata.verified_generation,
        },
        session_id: header_owner.session_id,
        request_id: header_owner.request_id,
    }
    .into();

    assert!(matches!(
        apply_transition(&store, request, &context(&config, &clock, None)),
        Err(TransitionFailure::InvalidEvidence(
            "ordinary header insertion does not have pure header authority"
        ))
    ));
}

#[test]
fn resource_bound_refusal_commits_only_the_alarm_and_recovers() {
    let (mut store, mut config) = TestStore::new(EngineMode::Integrated);
    config.limits.max_non_finalized_nodes = std::num::NonZeroUsize::new(1).expect("one is nonzero");
    let clock = ManualClock(Utc::now());

    let refused = apply_transition(
        &store,
        insertion(&store, 2, EvidenceId::from_digest([0x30; 32])),
        &context(&config, &clock, None),
    )
    .expect("resource refusal produces an alarm-only plan");
    assert_eq!(refused.cause(), TransitionCause::ResourceStalled);
    assert!(refused.change_set.metadata.alarms.resource_stalled);
    assert_eq!(refused.graph_delta, crate::graph::GraphDelta::default());
    assert_eq!(
        refused.change_set.metadata.state_version,
        StateVersion::new(1)
    );
    assert!(refused.change_set.put_nodes.is_empty());
    assert!(refused.change_set.delete_nodes.is_empty());
    assert_eq!(refused.projected.node_count(), 1);
    assert_eq!(
        refused.change_set.metadata.frontiers,
        store.metadata.frontiers
    );
    store.commit(&refused);

    let recovered = apply_transition(
        &store,
        insertion(&store, 1, EvidenceId::from_digest([0x31; 32])),
        &context(&config, &clock, None),
    )
    .expect("an insertion within the bound clears the resource alarm");
    assert_eq!(recovered.cause(), TransitionCause::Event);
    assert!(!recovered.change_set.metadata.alarms.resource_stalled);
    assert_eq!(
        recovered.change_set.metadata.state_version,
        StateVersion::new(2)
    );
    assert_eq!(recovered.projected.node_count(), 2);
}

#[test]
fn engine_rejects_context_free_batch_with_invalid_retained_time() {
    let (store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(regtest_genesis_block().header.time + chrono::Duration::hours(1));
    let mut request = insertion(&store, 1, EvidenceId::from_digest([0x31; 32]));
    let TransitionEvent::InsertHeaders(insert) = &mut request.event else {
        panic!("the fixture request inserts headers");
    };
    let original = &insert.batch.headers()[0];
    let mut header = *original.header;
    header.time = store
        .graph
        .node(insert.parent_hash)
        .expect("the exact parent is retained")
        .header
        .time;
    let header = Arc::new(header);
    let hash = header.hash();
    let prepared = PreparedHeader {
        header: header.clone(),
        hash,
        height: original.height,
        block_work: header
            .difficulty_threshold
            .to_work()
            .expect("the fixture target has valid work"),
        validation: HeaderValidationState::Valid,
    };
    insert.target_tip_hash = hash;
    let owner = insert
        .owner
        .header_owner()
        .expect("the fixture is ordinary header work");
    insert.owner = crate::HeaderWorkOwner {
        authority: crate::HeaderWorkAuthority {
            branch: BranchId::new(insert.parent_hash, hash),
            ..owner.authority
        },
        ..owner
    }
    .into();
    insert.batch = PreparedHeaderBatch::new(
        vec![prepared],
        store.lease.parent,
        store.lease.network().clone(),
        store.lease.trust_anchor_digest,
        EvidenceId::from_digest([0x31; 32]),
    )
    .expect("the context-free fixture is nonempty");

    assert!(matches!(
        apply_transition(&store, request, &context(&config, &clock, None)),
        Err(TransitionFailure::InvalidEvidence(
            "prepared header failed retained contextual validation"
        ))
    ));
}

#[test]
fn insertion_enforces_every_immutable_configured_checkpoint() {
    let (store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let request = insertion(&store, 1, EvidenceId::from_digest([0x44; 32]));
    let child = match &request.event {
        TransitionEvent::InsertHeaders(event) => Frontier::new(
            store
                .lease
                .parent
                .height
                .next()
                .expect("the fixture anchor has a next height"),
            event.target_tip_hash,
        ),
        _ => unreachable!("the insertion fixture constructs one header event"),
    };

    let mut matching_store = store.clone();
    let mut matching_config = config.clone();
    matching_config.local_checkpoints =
        CheckpointSet::new([child]).expect("the matching checkpoint fixture is unique");
    matching_store.metadata.anchor_manifest_digest = matching_config.trust_anchor_digest();
    matching_store.lease = ValidationLease::new(
        matching_store.lease.parent,
        matching_store.lease.predecessors.clone(),
        matching_config.network.clone(),
        matching_config.trust_anchor_digest(),
    );
    let matching_request = insertion(&matching_store, 1, EvidenceId::from_digest([0x44; 32]));
    let matching = apply_transition(
        &matching_store,
        matching_request,
        &context(&matching_config, &clock, None),
    )
    .expect("an insertion matching the immutable configured checkpoint commits");
    assert_eq!(matching.change_set.metadata.frontiers.header_best, child);
    assert!(matching
        .projected
        .node(child.hash)
        .expect("the matching checkpoint child is retained")
        .eligibility
        .direct_reasons
        .is_empty());

    let expected = Frontier::new(child.height, block::Hash([0x45; 32]));
    let mut conflicting_store = store;
    let mut conflicting_config = config;
    conflicting_config.local_checkpoints =
        CheckpointSet::new([expected]).expect("the conflicting checkpoint fixture is unique");
    conflicting_store.metadata.anchor_manifest_digest = conflicting_config.trust_anchor_digest();
    conflicting_store.lease = ValidationLease::new(
        conflicting_store.lease.parent,
        conflicting_store.lease.predecessors.clone(),
        conflicting_config.network.clone(),
        conflicting_config.trust_anchor_digest(),
    );
    let conflicting_request = insertion(&conflicting_store, 1, EvidenceId::from_digest([0x44; 32]));
    let conflicting = apply_transition(
        &conflicting_store,
        conflicting_request,
        &context(&conflicting_config, &clock, Some(&Authority)),
    )
    .expect("a conflicting header is retained only as checkpoint evidence");
    assert_eq!(
        conflicting.change_set.metadata.frontiers.header_best,
        conflicting_store.metadata.frontiers.header_best
    );
    assert!(conflicting
        .projected
        .node(child.hash)
        .expect("the conflicting checkpoint child is retained")
        .eligibility
        .direct_reasons
        .contains(&EligibilityReason::CheckpointConflict {
            height: child.height,
            expected: expected.hash,
        }));

    let conflicting_child = conflicting
        .projected
        .node(child.hash)
        .expect("the conflicting checkpoint child is retained");
    let mut descendant_header = *conflicting_child.header;
    descendant_header.previous_block_hash = conflicting_child.hash;
    descendant_header.nonce.0[0] ^= 1;
    let descendant_header = Arc::new(descendant_header);
    let descendant_work = descendant_header
        .difficulty_threshold
        .to_work()
        .expect("the fixture target has exact work");
    let mut descendant_graph = conflicting.projected.clone();
    let descendant = descendant_graph
        .insert(
            descendant_header,
            descendant_work,
            HeaderValidationState::Valid,
            [],
            BodyValidationState::Unknown,
        )
        .expect("the checkpoint-conflicting descendant links");
    let descendant = match descendant {
        crate::InsertResult::Inserted(frontier) => descendant_graph
            .node(frontier.hash)
            .expect("the inserted descendant is retained"),
        crate::InsertResult::AlreadyPresent(_) => {
            unreachable!("the descendant nonce is unique in this fixture")
        }
    };
    assert_eq!(descendant.eligibility.inherited_from, Some(child.hash));
    assert!(!descendant.is_eligible());
    let mut committed_conflict = conflicting_store.clone();
    committed_conflict.commit(&conflicting);
    let reconsider = apply_transition(
        &committed_conflict,
        operator_reconsider(
            &committed_conflict,
            child.hash,
            crate::OperatorInvalidationId::new([0x46; 16]),
            0x47,
        ),
        &context(&conflicting_config, &clock, Some(&Authority)),
    )
    .expect("operator reconsider can remove only a matching operator reason");
    assert!(reconsider
        .projected
        .node(child.hash)
        .expect("the checkpoint conflict remains retained")
        .eligibility
        .direct_reasons
        .contains(&EligibilityReason::CheckpointConflict {
            height: child.height,
            expected: expected.hash,
        }));
    assert_eq!(
        reconsider.change_set.metadata.frontiers.header_best,
        committed_conflict.metadata.frontiers.header_best
    );
}

#[test]
fn production_settled_pins_create_exact_permanent_conflict_reasons() {
    let clock = ManualClock(Utc::now());
    for (network, bytes) in [
        (
            Network::Mainnet,
            zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES.as_slice(),
        ),
        (
            Network::new_default_testnet(),
            zakura_test::vectors::BLOCK_TESTNET_GENESIS_BYTES.as_slice(),
        ),
    ] {
        let genesis = Arc::<Block>::zcash_deserialize(bytes)
            .expect("the production genesis vector is canonical");
        let frontier = Frontier::new(block::Height(0), genesis.hash());
        let config = EngineConfig::new(
            EngineMode::Integrated,
            network.clone(),
            TrustedAnchor {
                frontier,
                header: genesis.header.clone(),
            },
            CheckpointSet::default(),
        )
        .expect("the production configuration installs its settled pin");
        let pin = config
            .settled_manifest
            .pin_for_network(&network)
            .expect("the release manifest has a production pin");
        assert!(anchor_reasons(
            &context(&config, &clock, None),
            pin.activation.height,
            pin.activation.hash,
        )
        .is_empty());
        let conflicting_hash = block::Hash([0x55; 32]);
        assert_eq!(
            anchor_reasons(
                &context(&config, &clock, None),
                pin.activation.height,
                conflicting_hash,
            ),
            vec![EligibilityReason::SettledUpgradeConflict {
                height: pin.activation.height,
                expected: pin.activation.hash,
            }]
        );
    }
}

#[test]
fn migrated_pin_refutation_requires_full_state_authority_and_exact_pin() {
    let (mut store, config) = TestStore::new(EngineMode::Integrated);
    let clock = ManualClock(Utc::now());
    let authority = Authority;
    let pin = store.graph.finalized();
    store.finality.push(FinalityRecord {
        previous: pin,
        current: pin,
        source: FinalitySource::MigratedHeadersOnly,
        epoch: FinalityEpoch::new(0),
    });
    let evidence = EvidenceId::from_digest([0x61; 32]);
    let request = |pin| TransitionRequest {
        expected_version: store.metadata.state_version,
        event: TransitionEvent::MigratedPinRefutation(crate::MigratedPinRefutation {
            full_state_transition_id: evidence,
            pin,
            invalid_header: Frontier::new(block::Height(0), block::Hash([0x62; 32])),
            rule: crate::BodyRuleId::new("body.imported-history"),
        }),
    };

    assert!(matches!(
        apply_transition(&store, request(pin), &context(&config, &clock, None)),
        Err(TransitionFailure::Authority)
    ));
    assert!(matches!(
        apply_transition(
            &store,
            request(Frontier::new(pin.height, block::Hash([0x63; 32]))),
            &context(&config, &clock, Some(&authority)),
        ),
        Err(TransitionFailure::InvalidEvidence(_))
    ));

    let plan = apply_transition(
        &store,
        request(pin),
        &context(&config, &clock, Some(&authority)),
    )
    .expect("full state can persist a refuted imported pin incident");
    assert_eq!(
        plan.change_set.metadata.alarms.migrated_pin_refuted,
        Some(pin)
    );
    assert_eq!(
        plan.change_set.metadata.state_version,
        store
            .metadata
            .state_version
            .checked_next()
            .expect("the fixture version has capacity")
    );
    assert!(plan.change_set.put_nodes.is_empty());
    assert!(plan.change_set.delete_nodes.is_empty());
}
