use super::*;

pub(super) fn crash_fixture_startup_recovery_reopens_complete_before_or_after_without_publication()
{
    const STARTUP_FAULT_POINTS: [FaultPoint; 3] = [
        FaultPoint::BeforeCommit,
        FaultPoint::AfterCommit,
        FaultPoint::AfterPublish,
    ];

    for target in STARTUP_FAULT_POINTS {
        let cache = tempfile::tempdir().expect("the test cache directory is created");
        let db_config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: false,
            debug_skip_non_finalized_state_backup_task: true,
            ..Config::default()
        };
        let (engine_config, anchor, metadata) = fixture();
        let network = engine_config.network.clone();
        let db = open(&db_config, &network);
        let store = HeaderChainStore::new(db.clone());
        store
            .initialize(metadata.clone(), anchor.clone())
            .expect("the empty schema initializes");
        let mut corrupt = DiskWriteBatch::new();
        store
            .delete_raw(
                &mut corrupt,
                HEADER_SELECTED,
                HeaderHeightKey(anchor.height).as_bytes(),
            )
            .expect("the selected projection row is addressable");
        db.write(corrupt)
            .expect("the reconstructible selected-index corruption is durable");
        assert_eq!(store.selected_hash(anchor.height), Ok(None));

        let observer = store.clone();
        let result = store.startup_with_fault(&engine_config, |point| {
            if point == target {
                Err(HeaderChainStoreError::InjectedCrash(point))
            } else {
                Ok(())
            }
        });
        assert!(matches!(
            result,
            Err(HeaderChainStoreError::InjectedCrash(point)) if point == target
        ));

        let committed = target.commit_completed();
        assert_eq!(
            observer.selected_hash(anchor.height),
            if committed {
                Ok(Some(anchor.hash))
            } else {
                Ok(None)
            },
            "{target:?}"
        );
        let durable = observer
            .metadata()
            .expect("the startup-recovery metadata is readable");
        assert_eq!(
            durable.state_version,
            if committed {
                StateVersion::new(2)
            } else {
                metadata.state_version
            },
            "{target:?}"
        );
        assert_eq!(
            durable.header_generation,
            if committed {
                HeaderGeneration::new(2)
            } else {
                metadata.header_generation
            },
            "{target:?}"
        );
        assert_eq!(
            durable.verified_generation, metadata.verified_generation,
            "{target:?}"
        );

        drop(db);
        let (reopened, report) = observer
            .startup(&engine_config)
            .expect("the interrupted startup recovery completes before publication");
        assert_eq!(
            report.repairs,
            if committed {
                BTreeSet::new()
            } else {
                BTreeSet::from([RecoveryRepair::SelectedProjection])
            },
            "{target:?}"
        );
        assert_eq!(
            report.current.state_version,
            StateVersion::new(2),
            "{target:?}"
        );
        assert_eq!(
            report.current.header_generation,
            HeaderGeneration::new(2),
            "{target:?}"
        );
        assert_eq!(
            report.current.verified_generation, metadata.verified_generation,
            "{target:?}"
        );
        assert_eq!(
            reopened.store.selected_hash(anchor.height),
            Ok(Some(anchor.hash)),
            "{target:?}"
        );
        assert_eq!(
            reopened.publisher().snapshot(),
            report.current,
            "{target:?}"
        );
    }
}

pub(super) fn crash_fixture_every_state_writer_crash_point_reopens_complete_before_or_after() {
    for (index, target) in FaultPoint::ALL.into_iter().enumerate() {
        let cache = tempfile::tempdir().expect("the test cache directory is created");
        let db_config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: false,
            debug_skip_non_finalized_state_backup_task: true,
            ..Config::default()
        };
        let (engine_config, anchor, metadata) = fixture();
        let network = engine_config.network.clone();
        let db = open(&db_config, &network);
        let store = HeaderChainStore::new(db.clone());
        store
            .initialize(metadata.clone(), anchor.clone())
            .expect("the empty schema initializes");
        let (runtime, _) = store
            .startup(&engine_config)
            .expect("the initial store audits");
        let before = runtime.publisher().snapshot();
        let marker = u8::try_from(index + 1).expect("the fault-point list fits in u8");
        let evidence = EvidenceId::from_digest([marker; 32]);
        let authority = Authority(evidence);
        let context = TransitionContext {
            config: &engine_config,
            clock: &SystemClock,
            full_state_authority: Some(&authority),
            retention_references: &[],
        };
        let request = TransitionRequest {
            expected_version: StateVersion::new(1),
            event: TransitionEvent::BodyEvidence(BodyEvidence::Transient(TransientBodyFailure {
                hash: anchor.hash,
                evidence,
                kind: TransientBodyFailureKind::Storage,
                availability: BodyUnavailableSummary {
                    attempts: 1,
                    suppliers: 1,
                    alarmed: false,
                    ..Default::default()
                },
            })),
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
            .expect("the combined full-state marker can be staged");
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
            observation.durable.state_version,
            if target.commit_completed() {
                StateVersion::new(2)
            } else {
                StateVersion::new(1)
            },
            "{target:?}"
        );
        assert_eq!(observation.startup.current, observation.durable);
        assert_eq!(
            observation.reopened.publisher().snapshot(),
            observation.durable
        );
    }
}

pub(super) fn crash_fixture_requester_insertion_reopens_complete_before_or_after() {
    for (index, target) in FaultPoint::ALL.into_iter().enumerate() {
        let cache = tempfile::tempdir().expect("the test cache directory is created");
        let db_config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: false,
            debug_skip_non_finalized_state_backup_task: true,
            ..Config::default()
        };
        let (engine_config, anchor, metadata) = fixture();
        let network = engine_config.network.clone();
        let db = open(&db_config, &network);
        let store = HeaderChainStore::new(db.clone());
        store
            .initialize(metadata.clone(), anchor.clone())
            .expect("the empty schema initializes");
        let (runtime, _) = store
            .startup(&engine_config)
            .expect("the initial store audits");
        let before = runtime.publisher().snapshot();
        let anchor_frontier = metadata.frontiers.finalized;
        let lease = runtime
            .reader()
            .validation_context(anchor.hash)
            .expect("the anchor validation context is coherent")
            .expect("the initialized anchor is retained");
        let rules = HeaderRules::for_validation_lease(&lease)
            .expect("the authenticated regtest policy is valid");
        let marker = u8::try_from(index + 0x20).expect("the fault-point list fits in u8");
        let mut child_header = *anchor.header;
        child_header.previous_block_hash = anchor.hash;
        child_header.time += chrono::Duration::seconds(1);
        child_header.nonce.0[0] = marker;
        let child_header = Arc::new(child_header);
        let headers = [child_header.clone()];
        let batch = zakura_header_chain::prepare_headers(
            HeaderBatchInput::new(&headers),
            lease.parent(),
            &rules,
            &SystemClock,
        )
        .expect("the exact next child prepares through production validation");
        let child = Frontier::new(
            anchor_frontier
                .height
                .next()
                .expect("the genesis anchor has a next height"),
            child_header.hash(),
        );
        let owner = header_owner(&metadata.snapshot(), child.hash, 1, 1);
        let request = TransitionRequest {
            expected_version: metadata.state_version,
            event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                owner,
                source: SourceId::from_digest([marker.wrapping_add(1); 32]),
                parent_hash: anchor.hash,
                target_tip_hash: child.hash,
                completion: TargetCompletion::TargetComplete {
                    common_ancestor: anchor_frontier,
                },
                batch,
                aux: Vec::new(),
            })),
        };
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
        let committed = target.commit_completed();
        assert_eq!(
            observation.durable.state_version,
            if committed {
                StateVersion::new(2)
            } else {
                StateVersion::new(1)
            },
            "{target:?}"
        );
        assert_eq!(
            observation.durable.frontiers.header_best,
            if committed { child } else { anchor_frontier },
            "{target:?}"
        );
        assert_eq!(
            observation
                .reopened
                .store
                .header_node(child.hash)
                .expect("the reopened child row read succeeds")
                .is_some(),
            committed,
            "{target:?}"
        );
        assert_eq!(
            observation
                .reopened
                .store
                .selected_hash(child.height)
                .expect("the reopened selected projection is readable"),
            committed.then_some(child.hash),
            "{target:?}"
        );
    }
}
