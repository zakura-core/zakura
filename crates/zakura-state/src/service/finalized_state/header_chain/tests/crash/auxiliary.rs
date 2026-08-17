use super::*;

pub(super) fn crash_fixture_selected_auxiliary_repair_reopens_complete_before_or_after() {
    for (index, target) in FaultPoint::ALL.into_iter().enumerate() {
        let cache = tempfile::tempdir().expect("the test cache directory is created");
        let db_config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: false,
            debug_skip_non_finalized_state_backup_task: true,
            ..Config::default()
        };
        let (engine_config, anchor, metadata) = fixture();
        let network = engine_config.network().clone();
        let db = open(&db_config, &network);
        let store = HeaderChainStore::new(db.clone());
        store
            .initialize(metadata, anchor.clone())
            .expect("the empty schema initializes");
        let (runtime, _) = store
            .startup(&engine_config)
            .expect("the initial store audits");
        let initial = runtime.publisher().snapshot();
        let anchor_frontier = Frontier::new(anchor.height, anchor.hash);
        let lease = runtime
            .reader()
            .validation_context(anchor.hash)
            .expect("the anchor validation context is coherent")
            .expect("the initialized anchor is retained");
        let rules = HeaderRules::for_validation_lease(&lease)
            .expect("the authenticated regtest policy is valid");
        let marker = u8::try_from(index + 0x10).expect("the fault-point list fits in u8");
        let mut child_header = *anchor.header;
        child_header.previous_block_hash = anchor.hash;
        child_header.time += chrono::Duration::seconds(1);
        child_header.nonce.0[0] = marker;
        let child_header = Arc::new(child_header);
        let headers = [child_header.clone()];
        let insertion_batch = zakura_header_chain::prepare_headers(
            HeaderBatchInput::new(&headers),
            lease.parent(),
            &rules,
            &SystemClock,
        )
        .expect("the selected repair fixture passes production validation");
        let child = Frontier::new(
            anchor
                .height
                .next()
                .expect("the genesis anchor has a next height"),
            child_header.hash(),
        );
        let insertion_owner = header_owner(&initial, child.hash, 51, 52);
        let insertion_context = TransitionContext {
            config: &engine_config,
            clock: &SystemClock,
            full_state_authority: None,
            retention_references: &[],
        };
        runtime
            .apply(
                TransitionRequest {
                    expected_version: initial.state_version,
                    event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                        owner: insertion_owner,
                        source: SourceId::from_digest([marker.wrapping_add(1); 32]),
                        parent_hash: anchor.hash,
                        target_tip_hash: child.hash,
                        completion: TargetCompletion::TargetComplete {
                            common_ancestor: anchor_frontier,
                        },
                        batch: insertion_batch,
                        aux: Vec::new(),
                    })),
                },
                &insertion_context,
            )
            .expect("the selected repair target inserts without auxiliary metadata");
        let before = runtime.publisher().snapshot();
        assert_eq!(before.frontiers.header_best, child);
        assert!(runtime
            .store
            .aux_deliveries(child.hash)
            .expect("the initial auxiliary index is readable")
            .is_empty());

        let repair_lease = runtime
            .reader()
            .validation_context(anchor.hash)
            .expect("the repair validation context is coherent")
            .expect("the repair parent remains retained");
        let repair_rules = HeaderRules::for_validation_lease(&repair_lease)
            .expect("the authenticated repair policy is valid");
        let repair_batch = zakura_header_chain::prepare_headers(
            HeaderBatchInput::new(&headers),
            repair_lease.parent(),
            &repair_rules,
            &SystemClock,
        )
        .expect("the selected header redelivery passes production validation");
        let repair_owner = zakura_header_chain::BodyWorkAuthority::for_snapshot(&before)
            .bind(53, NonZeroU64::new(54).expect("fifty-four is nonzero"));
        let source = SourceId::from_digest([marker.wrapping_add(2); 32]);
        let delivery = AuxDelivery::new(
            EvidenceId::from_digest([marker.wrapping_add(3); 32]),
            child.hash,
            source,
            repair_owner.into(),
            zakura_header_chain::BodySizeHint::Unknown,
            Some(zakura_header_chain::TreeAuxRecordV1 {
                height: child.height,
                sapling_root: Default::default(),
                orchard_root: Default::default(),
                ironwood_root: Default::default(),
                sapling_tx_count: 4,
                orchard_tx_count: 5,
                ironwood_tx_count: 6,
                auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from(
                    [marker.wrapping_add(4); 32],
                ),
            }),
        );
        let context = TransitionContext {
            config: &engine_config,
            clock: &SystemClock,
            full_state_authority: None,
            retention_references: &[],
        };
        let marker_key = [marker; 4];
        let mut full_state_batch = DiskWriteBatch::new();
        runtime
            .store
            .put_raw(
                &mut full_state_batch,
                ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT,
                marker_key,
                [marker],
            )
            .expect("the paired selected-repair marker can be staged");
        let memory_swapped = Arc::new(AtomicBool::new(false));
        let swap_probe = memory_swapped.clone();
        let result = runtime.apply_combined_with_fault(
            TransitionRequest {
                expected_version: before.state_version,
                event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                    owner: repair_owner.into(),
                    source,
                    parent_hash: anchor.hash,
                    target_tip_hash: child.hash,
                    completion: TargetCompletion::SelectedAuxiliaryRepair {
                        common_ancestor: anchor_frontier,
                        selected_target: child,
                    },
                    batch: repair_batch,
                    aux: vec![delivery],
                })),
            },
            &context,
            full_state_batch,
            move || swap_probe.store(true, Ordering::SeqCst),
            |point| {
                if point == target {
                    Err(HeaderChainStoreError::InjectedCrash(point))
                } else {
                    Ok(())
                }
            },
        );
        assert!(matches!(
            result,
            Err(HeaderChainStoreError::InjectedCrash(point)) if point == target
        ));

        let observation = observe_transition_crash(
            target,
            runtime,
            db,
            &db_config,
            &network,
            &engine_config,
            &before,
            &memory_swapped,
            Some(marker_key),
        );
        let committed = target.commit_completed();
        let committed_version = before
            .state_version
            .checked_next()
            .expect("the short fixture state version can advance");
        let durable = &observation.durable;
        assert_eq!(
            durable.state_version,
            if committed {
                committed_version
            } else {
                before.state_version
            },
            "{target:?}"
        );
        assert_eq!(durable.frontiers, before.frontiers, "{target:?}");
        assert_eq!(
            durable.header_generation, before.header_generation,
            "{target:?}"
        );
        assert_eq!(
            durable.verified_generation, before.verified_generation,
            "{target:?}"
        );
        let child_node = observation
            .reopened
            .store
            .header_node(child.hash)
            .expect("the selected repair node read succeeds")
            .expect("the selected repair target remains retained");
        assert_eq!(
            child_node.aux_delivery_ids,
            if committed {
                vec![delivery.delivery_id]
            } else {
                Vec::new()
            },
            "{target:?}"
        );
        let stored_deliveries = observation
            .reopened
            .store
            .aux_deliveries(child.hash)
            .expect("the selected repair auxiliary index is readable");
        assert_eq!(
            stored_deliveries,
            if committed {
                vec![delivery]
            } else {
                Vec::new()
            },
            "{target:?}"
        );
        assert_eq!(
            observation.startup.current.state_version,
            if committed {
                committed_version
            } else {
                before.state_version
            },
            "{target:?}"
        );
        assert_eq!(
            observation.startup.current.frontiers, before.frontiers,
            "{target:?}"
        );
        let reopened_child = observation
            .reopened
            .store
            .header_node(child.hash)
            .expect("the reopened selected repair node read succeeds")
            .expect("the reopened selected repair target remains retained");
        assert_eq!(
            reopened_child.aux_delivery_ids,
            if committed {
                vec![delivery.delivery_id]
            } else {
                Vec::new()
            },
            "{target:?}"
        );
        assert_eq!(
            observation
                .reopened
                .store
                .aux_deliveries(child.hash)
                .expect("the reopened selected repair auxiliary index is readable"),
            if committed {
                vec![delivery]
            } else {
                Vec::new()
            },
            "{target:?}"
        );
    }
}

pub(super) fn crash_fixture_aux_authentication_reopens_complete_before_or_after() {
    const AUX_FAULT_POINTS: [FaultPoint; 4] = FaultPoint::ALL;

    for (index, target) in AUX_FAULT_POINTS.into_iter().enumerate() {
        let cache = tempfile::tempdir().expect("the test cache directory is created");
        let db_config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: false,
            debug_skip_non_finalized_state_backup_task: true,
            ..Config::default()
        };
        let (engine_config, anchor, metadata) = fixture();
        let network = engine_config.network().clone();
        let db = open(&db_config, &network);
        let store = HeaderChainStore::new(db.clone());
        store
            .initialize(metadata, anchor.clone())
            .expect("the empty schema initializes");
        let (runtime, _) = store
            .startup(&engine_config)
            .expect("the initial store audits");
        let initial = runtime.publisher().snapshot();
        let anchor_frontier = Frontier::new(anchor.height, anchor.hash);
        let lease = runtime
            .reader()
            .validation_context(anchor.hash)
            .expect("the anchor validation context is coherent")
            .expect("the initialized anchor is retained");
        let rules = HeaderRules::for_validation_lease(&lease)
            .expect("the authenticated regtest policy is valid");
        let marker = u8::try_from(index + 0xe0).expect("the fault-point list fits in u8");

        let mut current_header = *anchor.header;
        current_header.previous_block_hash = anchor.hash;
        current_header.time += chrono::Duration::seconds(1);
        current_header.nonce.0[0] = marker;
        let current_header = Arc::new(current_header);
        let mut boundary_header = *current_header;
        boundary_header.previous_block_hash = current_header.hash();
        boundary_header.time += chrono::Duration::seconds(1);
        boundary_header.nonce.0[0] = marker.wrapping_add(1);
        let boundary_header = Arc::new(boundary_header);
        let headers = [current_header.clone(), boundary_header.clone()];
        let batch = zakura_header_chain::prepare_headers(
            HeaderBatchInput::new(&headers),
            lease.parent(),
            &rules,
            &SystemClock,
        )
        .expect("the auxiliary fixture headers pass production validation");
        let current_height = anchor
            .height
            .next()
            .expect("the genesis anchor has a next height");
        let boundary_height = current_height
            .next()
            .expect("the first child has a next height");
        let current = Frontier::new(current_height, current_header.hash());
        let boundary = Frontier::new(boundary_height, boundary_header.hash());
        let insertion_owner = header_owner(&initial, boundary.hash, 21, 22);
        let source = SourceId::from_digest([marker.wrapping_add(2); 32]);
        let delivery = AuxDelivery::new(
            EvidenceId::from_digest([marker.wrapping_add(3); 32]),
            current.hash,
            source,
            insertion_owner,
            zakura_header_chain::BodySizeHint::Unknown,
            Some(zakura_header_chain::TreeAuxRecordV1 {
                height: current.height,
                sapling_root: Default::default(),
                orchard_root: Default::default(),
                ironwood_root: Default::default(),
                sapling_tx_count: 1,
                orchard_tx_count: 2,
                ironwood_tx_count: 3,
                auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from(
                    [marker.wrapping_add(4); 32],
                ),
            }),
        );
        let insertion_context = TransitionContext {
            config: &engine_config,
            clock: &SystemClock,
            full_state_authority: None,
            retention_references: &[],
        };
        runtime
            .apply(
                TransitionRequest {
                    expected_version: initial.state_version,
                    event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                        owner: insertion_owner,
                        source,
                        parent_hash: anchor.hash,
                        target_tip_hash: boundary.hash,
                        completion: TargetCompletion::TargetComplete {
                            common_ancestor: anchor_frontier,
                        },
                        batch,
                        aux: vec![delivery],
                    })),
                },
                &insertion_context,
            )
            .expect("the unauthenticated delivery inserts with its exact headers");

        let before = runtime.publisher().snapshot();
        let observation = zakura_header_chain::AuxObservationV1::from_vct(
            body_owner(
                &before,
                insertion_owner.session_id(),
                insertion_owner.request_id().get(),
            ),
            vec![delivery],
            zakura_header_chain::AuxVerificationFactV1::current_delivery_verified(),
            Some([marker.wrapping_add(5); 32].into()),
        )
        .expect("the authentication observation is valid");
        let evidence = EvidenceId::from_digest(observation.observation_id().digest());
        let authority = Authority(evidence);
        let context = TransitionContext {
            config: &engine_config,
            clock: &SystemClock,
            full_state_authority: Some(&authority),
            retention_references: &[],
        };
        let request = TransitionRequest {
            expected_version: before.state_version,
            event: TransitionEvent::AuxEvidence(Box::new(
                zakura_header_chain::AuxEvidence::observed(observation),
            )),
        };
        let marker_key = [marker; 4];
        let mut full_state_batch = DiskWriteBatch::new();
        runtime
            .store
            .put_raw(
                &mut full_state_batch,
                ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT,
                marker_key,
                [marker],
            )
            .expect("the paired auxiliary marker can be staged");
        let memory_swapped = Arc::new(AtomicBool::new(false));
        let swap_probe = memory_swapped.clone();
        let result = runtime.apply_combined_with_fault(
            request,
            &context,
            full_state_batch,
            move || swap_probe.store(true, Ordering::SeqCst),
            |point| {
                if point == target {
                    Err(HeaderChainStoreError::InjectedCrash(point))
                } else {
                    Ok(())
                }
            },
        );
        assert!(matches!(
            result,
            Err(HeaderChainStoreError::InjectedCrash(point)) if point == target
        ));

        let observation = observe_transition_crash(
            target,
            runtime,
            db,
            &db_config,
            &network,
            &engine_config,
            &before,
            &memory_swapped,
            Some(marker_key),
        );
        let committed = target.commit_completed();
        let committed_version = before
            .state_version
            .checked_next()
            .expect("the short fixture state version can advance");
        let durable = &observation.durable;
        assert_eq!(
            durable.state_version,
            if committed {
                committed_version
            } else {
                before.state_version
            },
            "{target:?}"
        );
        assert_eq!(durable.frontiers, before.frontiers, "{target:?}");
        assert_eq!(
            durable.header_generation, before.header_generation,
            "{target:?}"
        );
        assert_eq!(
            durable.verified_generation, before.verified_generation,
            "{target:?}"
        );
        let stored_delivery = observation
            .reopened
            .store
            .aux_deliveries(current.hash)
            .expect("the auxiliary row read succeeds");
        assert_eq!(stored_delivery.len(), 1, "{target:?}");
        assert_eq!(
            stored_delivery[0].is_authenticated(),
            committed,
            "{target:?}"
        );
        let current_node = observation
            .reopened
            .store
            .header_node(current.hash)
            .expect("the auxiliary header node read succeeds")
            .expect("the auxiliary header remains retained");
        assert_eq!(
            current_node.aux_delivery_ids,
            vec![delivery.delivery_id],
            "{target:?}"
        );
        assert_eq!(
            observation.startup.current.state_version,
            if committed {
                committed_version
            } else {
                before.state_version
            },
            "{target:?}"
        );
        assert_eq!(
            observation.startup.current.frontiers, before.frontiers,
            "{target:?}"
        );
        let reopened_delivery = observation
            .reopened
            .store
            .aux_deliveries(current.hash)
            .expect("the reopened auxiliary row is readable");
        assert_eq!(reopened_delivery.len(), 1, "{target:?}");
        assert_eq!(
            reopened_delivery[0].is_authenticated(),
            committed,
            "{target:?}"
        );
    }
}

pub(super) fn crash_fixture_two_delivery_aux_rejection_never_partially_commits() {
    const REJECTION_FAULT_POINTS: [FaultPoint; 4] = FaultPoint::ALL;

    for (index, target) in REJECTION_FAULT_POINTS.into_iter().enumerate() {
        let cache = tempfile::tempdir().expect("the test cache directory is created");
        let db_config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: false,
            debug_skip_non_finalized_state_backup_task: true,
            ..Config::default()
        };
        let (engine_config, mut anchor, metadata) = fixture();
        let network = engine_config.network().clone();
        let db = open(&db_config, &network);
        let store = HeaderChainStore::new(db.clone());
        store
            .initialize(metadata.clone(), anchor.clone())
            .expect("the empty schema initializes");
        let marker = u8::try_from(index + 0x80).expect("the rejection cases fit in u8");
        let delivery_owner =
            zakura_header_chain::BodyWorkAuthority::for_snapshot(&metadata.snapshot())
                .bind(61, NonZeroU64::new(62).expect("sixty-two is nonzero"));
        let first = AuxDelivery::new(
            EvidenceId::from_digest([marker.wrapping_add(1); 32]),
            anchor.hash,
            SourceId::from_digest([marker.wrapping_add(2); 32]),
            delivery_owner.into(),
            zakura_header_chain::BodySizeHint::Unknown,
            None,
        );
        let second = AuxDelivery::new(
            EvidenceId::from_digest([marker.wrapping_add(3); 32]),
            first.header_hash,
            SourceId::from_digest([marker.wrapping_add(4); 32]),
            first.owner,
            first.body_size,
            first.tree_aux,
        );
        anchor
            .aux_delivery_ids
            .extend([first.delivery_id, second.delivery_id]);
        let mut seed = DiskWriteBatch::new();
        store
            .put_value(
                &mut seed,
                HEADER_NODE_BY_HASH,
                anchor.hash.0,
                &HeaderNodeDisk::from_domain(&anchor),
            )
            .expect("the two-delivery anchor node encodes");
        for delivery in [first, second] {
            store
                .put_value(
                    &mut seed,
                    HEADER_AUX_DELIVERY,
                    HeaderAuxDeliveryKey {
                        header: delivery.header_hash,
                        delivery: delivery.delivery_id,
                    }
                    .as_bytes(),
                    &delivery,
                )
                .expect("the unauthenticated auxiliary delivery encodes");
        }
        db.write(seed)
            .expect("the coherent two-delivery fixture commits");
        let (runtime, _) = store
            .startup(&engine_config)
            .expect("the two-delivery fixture audits");
        let before = runtime.publisher().snapshot();
        let observation = zakura_header_chain::AuxObservationV1::from_vct(
            zakura_header_chain::BodyWorkAuthority::for_snapshot(&before)
                .bind(delivery_owner.session_id, delivery_owner.request_id),
            vec![first],
            zakura_header_chain::AuxVerificationFactV1::successor_delivery_failed(1),
            Some([marker.wrapping_add(5); 32].into()),
        )
        .expect("the rejection observation is valid");
        let evidence = EvidenceId::from_digest(observation.observation_id().digest());
        let authority = Authority(evidence);
        let context = TransitionContext {
            config: &engine_config,
            clock: &SystemClock,
            full_state_authority: Some(&authority),
            retention_references: &[],
        };
        let marker_key = [marker; 4];
        let mut full_state_batch = DiskWriteBatch::new();
        runtime
            .store
            .put_raw(
                &mut full_state_batch,
                ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT,
                marker_key,
                [marker],
            )
            .expect("the paired rejection marker can be staged");
        let memory_swapped = Arc::new(AtomicBool::new(false));
        let swap_probe = memory_swapped.clone();
        let result = runtime.apply_combined_with_fault(
            TransitionRequest {
                expected_version: before.state_version,
                event: TransitionEvent::AuxEvidence(Box::new(
                    zakura_header_chain::AuxEvidence::observed(observation),
                )),
            },
            &context,
            full_state_batch,
            move || swap_probe.store(true, Ordering::SeqCst),
            |point| {
                if point == target {
                    Err(HeaderChainStoreError::InjectedCrash(point))
                } else {
                    Ok(())
                }
            },
        );
        assert!(matches!(
            result,
            Err(HeaderChainStoreError::InjectedCrash(point)) if point == target
        ));

        let observation = observe_transition_crash(
            target,
            runtime,
            db,
            &db_config,
            &network,
            &engine_config,
            &before,
            &memory_swapped,
            Some(marker_key),
        );
        let committed = target.commit_completed();
        let committed_version = before
            .state_version
            .checked_next()
            .expect("the short fixture state version can advance");
        let durable = &observation.durable;
        assert_eq!(
            durable.state_version,
            if committed {
                committed_version
            } else {
                before.state_version
            },
            "{target:?}"
        );
        assert_eq!(durable.frontiers, before.frontiers, "{target:?}");
        let stored = observation
            .reopened
            .store
            .aux_deliveries(anchor.hash)
            .expect("the rejected delivery rows are readable");
        assert_eq!(stored.len(), 2);
        assert_eq!(stored[0].is_rejected(), committed);
        assert!(stored[1].is_unauthenticated());
        assert_eq!(
            observation
                .reopened
                .store
                .header_node(anchor.hash)
                .expect("the rejection anchor node read succeeds")
                .expect("the rejection anchor remains retained")
                .aux_delivery_ids,
            vec![first.delivery_id, second.delivery_id]
        );
        assert_eq!(
            observation.startup.current.state_version,
            if committed {
                committed_version
            } else {
                before.state_version
            },
            "{target:?}"
        );
        let reopened_deliveries = observation
            .reopened
            .store
            .aux_deliveries(anchor.hash)
            .expect("the reopened rejected delivery rows are readable");
        assert_eq!(reopened_deliveries.len(), 2);
        assert_eq!(reopened_deliveries[0].is_rejected(), committed);
        assert!(reopened_deliveries[1].is_unauthenticated());
    }
}
