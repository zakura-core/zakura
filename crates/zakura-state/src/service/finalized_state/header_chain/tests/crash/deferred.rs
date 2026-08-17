use super::*;

pub(super) fn crash_fixture_deferred_header_reevaluation_reopens_complete_before_or_after() {
    use chrono::Timelike as _;

    #[derive(Copy, Clone)]
    struct FixedClock(chrono::DateTime<Utc>);

    impl zakura_header_chain::Clock for FixedClock {
        fn now(&self) -> chrono::DateTime<Utc> {
            self.0
        }
    }

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
        let marker = u8::try_from(index + 0xa0).expect("the fault-point list fits in u8");
        let preparation_clock = FixedClock(
            (Utc::now() - chrono::Duration::hours(3))
                .with_nanosecond(0)
                .expect("the fixture time has a valid zero nanosecond field"),
        );
        let mut future_header = *anchor.header;
        future_header.previous_block_hash = anchor.hash;
        future_header.time = preparation_clock.0 + chrono::Duration::hours(3);
        future_header.nonce.0[0] = marker;
        let future_header = Arc::new(future_header);
        let headers = [future_header.clone()];
        let batch = zakura_header_chain::prepare_headers(
            HeaderBatchInput::new(&headers),
            lease.parent(),
            &rules,
            &preparation_clock,
        )
        .expect("the locally future header is admitted as deferred");
        let deferred_until = future_header.time - chrono::Duration::hours(2);
        assert_eq!(
            batch.headers()[0].validation,
            HeaderValidationState::DeferredUntil(deferred_until)
        );
        let future = Frontier::new(
            anchor
                .height
                .next()
                .expect("the genesis anchor has a next height"),
            future_header.hash(),
        );
        let owner = header_owner(&initial, future.hash, 31, 32);
        let insertion_context = TransitionContext {
            config: &engine_config,
            clock: &preparation_clock,
            full_state_authority: None,
            retention_references: &[],
        };
        runtime
            .apply(
                TransitionRequest {
                    expected_version: initial.state_version,
                    event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                        owner,
                        source: SourceId::from_digest([marker.wrapping_add(1); 32]),
                        parent_hash: anchor.hash,
                        target_tip_hash: future.hash,
                        completion: TargetCompletion::TargetComplete {
                            common_ancestor: anchor_frontier,
                        },
                        batch,
                        aux: Vec::new(),
                    })),
                },
                &insertion_context,
            )
            .expect("the deferred header insertion commits");
        let before = runtime.publisher().snapshot();
        assert_eq!(before.frontiers.header_best, anchor_frontier);
        assert_eq!(
            runtime
                .store
                .header_node(future.hash)
                .expect("the deferred node read succeeds")
                .expect("the deferred node is retained")
                .validation,
            HeaderValidationState::DeferredUntil(deferred_until)
        );
        assert_eq!(
            runtime
                .store
                .deferred_entries()
                .expect("the deferred index is readable"),
            vec![(deferred_until, future.hash)]
        );
        assert_eq!(
            runtime
                .earliest_deferred()
                .expect("the earliest deferred deadline is readable"),
            Some(deferred_until)
        );

        let reevaluation_clock = FixedClock(deferred_until);
        let context = TransitionContext {
            config: &engine_config,
            clock: &reevaluation_clock,
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
            .expect("the paired reevaluation marker can be staged");
        let memory_swapped = Arc::new(AtomicBool::new(false));
        let swap_probe = memory_swapped.clone();
        let result = runtime.apply_combined_with_fault(
            TransitionRequest {
                expected_version: before.state_version,
                event: TransitionEvent::ReevaluateDeferred,
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

        let allowed_startup_repairs = BTreeSet::from([
            RecoveryRepair::DeferredIndex,
            RecoveryRepair::SelectedProjection,
        ]);
        let observation = observe_transition_crash_with_allowed_startup_repairs(
            target,
            runtime,
            db,
            &db_config,
            &network,
            &engine_config,
            &before,
            &memory_swapped,
            Some(marker_key),
            &allowed_startup_repairs,
        );
        let committed_version = before
            .state_version
            .checked_next()
            .expect("the short fixture state version can advance");
        let committed_header_generation = before
            .header_generation
            .checked_next()
            .expect("the short fixture header generation can advance");
        let durable = &observation.durable;
        assert_eq!(durable.state_version, committed_version, "{target:?}");
        assert_eq!(durable.frontiers.header_best, future, "{target:?}");
        assert_eq!(
            durable.header_generation, committed_header_generation,
            "{target:?}"
        );
        assert_eq!(
            durable.frontiers.verified_best, before.frontiers.verified_best,
            "{target:?}"
        );
        assert_eq!(
            durable.verified_generation, before.verified_generation,
            "{target:?}"
        );
        assert_eq!(
            observation
                .reopened
                .store
                .header_node(future.hash)
                .expect("the future node read succeeds")
                .expect("the future node remains retained")
                .validation,
            HeaderValidationState::Valid,
            "{target:?}"
        );
        assert_eq!(
            observation
                .reopened
                .store
                .deferred_entries()
                .expect("the deferred index is readable"),
            Vec::new(),
            "{target:?}"
        );
        assert_eq!(
            observation
                .reopened
                .earliest_deferred()
                .expect("the post-reevaluation deferred deadline is readable"),
            None,
            "{target:?}"
        );
        assert_eq!(
            observation.startup.current.frontiers.header_best, future,
            "{target:?}"
        );
        assert_eq!(
            observation.startup.current.state_version, committed_version,
            "{target:?}"
        );
        assert_eq!(
            observation
                .reopened
                .store
                .header_node(future.hash)
                .expect("the reopened future node read succeeds")
                .expect("the reopened future node remains retained")
                .validation,
            HeaderValidationState::Valid,
            "{target:?}"
        );
        assert_eq!(
            observation
                .reopened
                .store
                .deferred_entries()
                .expect("the reopened deferred index is readable"),
            Vec::new(),
            "{target:?}"
        );
    }
}

pub(super) fn crash_fixture_migrated_pin_refutation_fails_closed_at_every_reachable_boundary() {
    const REFUTATION_FAULT_POINTS: [FaultPoint; 2] =
        [FaultPoint::BeforeCommit, FaultPoint::AfterCommit];

    for (index, target) in REFUTATION_FAULT_POINTS.into_iter().enumerate() {
        let cache = tempfile::tempdir().expect("the test cache directory is created");
        let db_config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: false,
            debug_skip_non_finalized_state_backup_task: true,
            ..Config::default()
        };
        let (integrated_config, anchor, mut metadata) = fixture();
        let mut headers_only_config = integrated_config.clone();
        headers_only_config.mode = EngineMode::HeadersOnly;
        metadata.mode = EngineMode::HeadersOnly;
        let anchor_frontier = Frontier::new(anchor.height, anchor.hash);
        let network = integrated_config.network().clone();
        let db = open(&db_config, &network);
        let store = HeaderChainStore::new(db.clone());
        store
            .initialize(metadata, anchor)
            .expect("the headers-only schema initializes");
        let migrated_record = FinalityRecord {
            previous: anchor_frontier,
            current: anchor_frontier,
            source: FinalitySource::MigratedHeadersOnly,
            epoch: FinalityEpoch::new(0),
        };
        let mut migration_batch = DiskWriteBatch::new();
        store
            .put_value(
                &mut migration_batch,
                HEADER_FINALITY_HISTORY,
                HeaderFinalityKey(migrated_record.epoch).as_bytes(),
                &migrated_record,
            )
            .expect("the migrated finality record encodes");
        db.write(migration_batch)
            .expect("the migrated finality record commits");
        audit_store(&store, &headers_only_config)
            .expect("the headers-only source store is coherent");
        let (runtime, _) = store
            .migrate_headers_only_to_integrated(&integrated_config, anchor_frontier)
            .expect("the explicit mode migration succeeds before publication");
        let before = runtime.publisher().snapshot();
        assert_eq!(before.mode, EngineMode::Integrated);
        assert_eq!(before.alarms.migrated_pin_refuted, None);

        let marker = u8::try_from(index + 0xf0).expect("the fault-point list fits in u8");
        let evidence = EvidenceId::from_digest([marker; 32]);
        let authority = Authority(evidence);
        let context = TransitionContext {
            config: &integrated_config,
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
            .expect("the paired refutation marker can be staged");
        let memory_swapped = Arc::new(AtomicBool::new(false));
        let swap_probe = memory_swapped.clone();
        let result = runtime.apply_combined_with_fault(
            TransitionRequest {
                expected_version: before.state_version,
                event: TransitionEvent::MigratedPinRefutation(
                    zakura_header_chain::MigratedPinRefutation {
                        full_state_transition_id: evidence,
                        pin: anchor_frontier,
                        invalid_header: anchor_frontier,
                        rule: BodyRuleId::new("aud14.migrated_pin_refutation"),
                    },
                ),
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

        let committed = target == FaultPoint::AfterCommit;
        let committed_version = before
            .state_version
            .checked_next()
            .expect("the short fixture state version can advance");
        let durable = runtime
            .store
            .snapshot()
            .expect("the refutation snapshot read succeeds");
        assert_eq!(
            durable.state_version,
            if committed {
                committed_version
            } else {
                before.state_version
            },
            "{target:?}"
        );
        assert_eq!(
            durable.alarms.migrated_pin_refuted,
            committed.then_some(anchor_frontier),
            "{target:?}"
        );
        assert_eq!(durable.frontiers, before.frontiers, "{target:?}");
        let marker_cf = runtime
            .store
            .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
            .expect("the marker column family is open");
        assert_eq!(
            runtime
                .store
                .db
                .raw_get_cf(&marker_cf, &marker_key)
                .expect("the paired marker read succeeds")
                .is_some(),
            committed,
            "{target:?}"
        );
        assert!(!memory_swapped.load(Ordering::SeqCst), "{target:?}");
        assert_eq!(runtime.publisher().snapshot(), before, "{target:?}");
        drop(runtime);
        drop(db);

        let reopened_store = HeaderChainStore::new(open(&db_config, &network));
        let reopened_metadata = reopened_store
            .metadata()
            .expect("the refutation metadata reopens");
        assert_eq!(
            reopened_metadata.alarms.migrated_pin_refuted,
            committed.then_some(anchor_frontier),
            "{target:?}"
        );
        let reopened_marker_cf = reopened_store
            .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
            .expect("the reopened marker column family is open");
        assert_eq!(
            reopened_store
                .db
                .raw_get_cf(&reopened_marker_cf, &marker_key)
                .expect("the reopened paired marker read succeeds")
                .is_some(),
            committed,
            "{target:?}"
        );
        if committed {
            assert!(matches!(
                reopened_store.startup(&integrated_config),
                Err(HeaderChainStoreError::MigratedPinRefuted { pin })
                    if pin == anchor_frontier
            ));
        } else {
            let (reopened, report) = reopened_store
                .startup(&integrated_config)
                .expect("the uncommitted refutation reopens normally");
            assert_eq!(report.current, before, "{target:?}");
            assert_eq!(reopened.publisher().snapshot(), before, "{target:?}");
        }
    }
}

pub(super) fn crash_fixture_no_change_crash_points_preserve_the_paired_full_state_transaction() {
    for (index, target) in FaultPoint::NO_CHANGE.into_iter().enumerate() {
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
            .initialize(metadata.clone(), anchor.clone())
            .expect("the empty schema initializes");
        let (runtime, _) = store
            .startup(&engine_config)
            .expect("the initial store audits");
        let before = runtime.publisher().snapshot();
        let marker = u8::try_from(index + 0x40).expect("the fault-point list fits in u8");
        let evidence = EvidenceId::from_digest([marker; 32]);
        let authority = Authority(evidence);
        let context = TransitionContext {
            config: &engine_config,
            clock: &SystemClock,
            full_state_authority: Some(&authority),
            retention_references: &[],
        };
        let request = TransitionRequest {
            expected_version: metadata.state_version,
            event: TransitionEvent::BodyEvidence(BodyEvidence::PayloadMismatch(
                BodyPayloadMismatch {
                    evidence,
                    requested: anchor.hash,
                    delivered: block::Hash([marker; 32]),
                    kind: BodyCommitmentKind::HeaderHash,
                    source: SourceId::from_digest([marker.wrapping_add(1); 32]),
                },
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
            .expect("the paired full-state marker can be staged");
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
        assert_eq!(
            observation.durable,
            metadata.snapshot(),
            "a no-change transition never changes metadata at {target:?}"
        );
        assert_eq!(
            observation.startup.current,
            metadata.snapshot(),
            "{target:?}"
        );
    }
}
