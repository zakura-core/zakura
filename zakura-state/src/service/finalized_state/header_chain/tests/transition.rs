use super::*;

#[test]
fn migrated_headers_only_pin_refutation_is_durable_and_fail_closed() {
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
    let db = open(&db_config, &integrated_config.network);
    let store = HeaderChainStore::new(db.clone());
    store
        .initialize(metadata, anchor.clone())
        .expect("the headers-only schema initializes");
    let record = FinalityRecord {
        previous: anchor_frontier,
        current: anchor_frontier,
        source: FinalitySource::MigratedHeadersOnly,
        epoch: FinalityEpoch::new(0),
    };
    let mut batch = DiskWriteBatch::new();
    store
        .put_value(
            &mut batch,
            HEADER_FINALITY_HISTORY,
            HeaderFinalityKey(record.epoch).as_bytes(),
            &record,
        )
        .expect("the finality record encodes");
    db.write(batch).expect("the headers-only record commits");
    audit_store(&store, &headers_only_config).expect("the source store is coherent");
    assert!(matches!(
        preserve_headers_only_pin(FinalityRecord {
            previous: anchor_frontier,
            current: Frontier::new(block::Height(1), block::Hash([89; 32])),
            source: FinalitySource::HeadersOnlyDepth {
                selected_tip: Frontier::new(block::Height(1_001), block::Hash([90; 32])),
            },
            epoch: FinalityEpoch::new(1),
        })
        .source,
        FinalitySource::MigratedHeadersOnly
    ));
    assert!(matches!(
        store.clone().migrate_headers_only_to_integrated(
            &integrated_config,
            Frontier::new(anchor.height, block::Hash([99; 32])),
        ),
        Err(HeaderChainStoreError::Incoherent(
            "integrated migration requires full-state verification through the preserved pin"
        ))
    ));

    let (runtime, report) = store
        .migrate_headers_only_to_integrated(&integrated_config, anchor_frontier)
        .expect("the explicit mode migration succeeds before publication");
    assert_eq!(report.current.mode, EngineMode::Integrated);
    assert!(matches!(
        runtime.store.finality_history().as_deref(),
        Ok([FinalityRecord {
            source: FinalitySource::MigratedHeadersOnly,
            ..
        }])
    ));

    let evidence = EvidenceId::from_digest([77; 32]);
    let authority = Authority(evidence);
    let snapshot = runtime.publisher().snapshot();
    let context = TransitionContext {
        config: &integrated_config,
        clock: &SystemClock,
        full_state_authority: Some(&authority),
        retention_references: &[],
    };
    let result = runtime.apply(
        TransitionRequest {
            expected_version: snapshot.state_version,
            event: TransitionEvent::MigratedPinRefutation(
                zakura_header_chain::MigratedPinRefutation {
                    full_state_transition_id: evidence,
                    pin: anchor_frontier,
                    invalid_header: anchor_frontier,
                    rule: BodyRuleId::new("migrated-pin-refutation"),
                },
            ),
        },
        &context,
    );
    assert!(matches!(
        result,
        Err(HeaderChainStoreError::MigratedPinRefuted { pin }) if pin == anchor_frontier
    ));
    assert_eq!(runtime.publisher().snapshot(), snapshot);
    assert_eq!(
        runtime
            .store
            .metadata()
            .expect("incident metadata is readable")
            .alarms
            .migrated_pin_refuted,
        Some(anchor_frontier)
    );

    drop(runtime);
    assert!(matches!(
        HeaderChainStore::new(db).startup(&integrated_config),
        Err(HeaderChainStoreError::MigratedPinRefuted { pin }) if pin == anchor_frontier
    ));
}

#[test]
fn serialized_apply_commits_before_receipt_and_reopens_exactly() {
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
        .expect("an empty header schema initializes atomically");
    assert_eq!(store.node(anchor.hash), Ok(Some(anchor.clone())));
    assert_eq!(store.selected_hash(anchor.height), Ok(Some(anchor.hash)));
    assert_eq!(store.verified_hash(anchor.height), Ok(Some(anchor.hash)));
    let (runtime, startup) = store
        .startup(&engine_config)
        .expect("the coherent store audits before publication");
    assert!(startup.repairs.is_empty());
    assert_eq!(runtime.publisher().snapshot(), metadata.snapshot());
    assert_transition_engine_matches_store(&runtime);
    let mut subscriber = runtime.publisher().subscribe();

    let evidence = EvidenceId::from_digest([7; 32]);
    let authority = Authority(evidence);
    let availability = BodyUnavailableSummary {
        started_at: Utc
            .timestamp_opt(1_000, 0)
            .single()
            .expect("valid fixture time"),
        attempts: 10,
        suppliers: 2,
        supplier_set_digest: [0x22; 32],
        alarmed: true,
        next_probe_at: Utc
            .timestamp_opt(1_600, 0)
            .single()
            .expect("valid fixture time"),
    };
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
            availability,
        })),
    };
    let result = runtime
        .apply(request.clone(), &context)
        .expect("the transition commits");
    assert_eq!(result, ApplyResult::Committed);
    let committed = runtime.publisher().snapshot();
    assert_eq!(committed.state_version, StateVersion::new(2));
    assert!(subscriber
        .has_changed()
        .expect("the publisher remains open"));
    assert_eq!(*subscriber.borrow_and_update(), committed);
    assert_eq!(
        committed.alarms.header_best_body_unavailable,
        Some(availability)
    );
    assert!(matches!(
        runtime.store.node(anchor.hash).expect("the node row decodes").expect("the anchor remains").body,
        BodyValidationState::Unavailable(summary)
            if summary == availability
    ));
    assert!(matches!(
        runtime.apply(request, &context).expect("idempotent replay succeeds"),
        ApplyResult::NoChange(receipt) if receipt.state_version == StateVersion::new(2)
    ));
    assert_transition_engine_matches_store(&runtime);
    assert!(matches!(
        runtime
            .apply(
                TransitionRequest {
                    expected_version: StateVersion::new(1),
                    event: TransitionEvent::ReevaluateDeferred,
                },
                &context,
            )
            .expect("a stale CAS is a typed zero-effect result"),
        ApplyResult::Stale(receipt) if receipt.current_version == StateVersion::new(2)
    ));

    drop(runtime);
    drop(db);
    let reopened = HeaderChainStore::new(open(&db_config, &network));
    let (reopened, report) = reopened
        .startup(&engine_config)
        .expect("the committed store reopens through exhaustive audit");
    assert_eq!(report.current, committed);
    assert_eq!(reopened.publisher().snapshot(), committed);
    assert_transition_engine_matches_store(&reopened);
    assert!(matches!(
        reopened
            .store
            .node(anchor.hash)
            .expect("the reopened node row decodes")
            .expect("the reopened anchor exists")
            .body,
        BodyValidationState::Unavailable(summary)
            if summary == availability
    ));
    let verified_evidence = EvidenceId::from_digest([8; 32]);
    let verified_authority = Authority(verified_evidence);
    let verified_context = TransitionContext {
        config: &engine_config,
        clock: &SystemClock,
        full_state_authority: Some(&verified_authority),
        retention_references: &[],
    };
    let result = reopened
        .apply(
            TransitionRequest {
                expected_version: StateVersion::new(2),
                event: TransitionEvent::BodyEvidence(BodyEvidence::Verified(
                    VerifiedBodyEvidence {
                        hash: anchor.hash,
                        evidence: verified_evidence,
                    },
                )),
            },
            &verified_context,
        )
        .expect("verified body evidence clears persistent unavailability");
    assert_eq!(result, ApplyResult::Committed);
    let verified = reopened.publisher().snapshot();
    assert_eq!(
        verified.frontiers.header_best,
        committed.frontiers.header_best
    );
    assert_eq!(verified.alarms.header_best_body_unavailable, None);
}

#[test]
fn failed_batch_encoding_has_zero_durable_effects() {
    let cache = tempfile::tempdir().expect("the test cache directory is created");
    let db_config = Config {
        cache_dir: cache.path().to_owned(),
        ephemeral: true,
        debug_skip_non_finalized_state_backup_task: true,
        ..Config::default()
    };
    let (engine_config, mut anchor, metadata) = fixture();
    let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
    store
        .initialize(metadata.clone(), anchor.clone())
        .expect("the empty schema initializes");

    let evidence = EvidenceId::from_digest([9; 32]);
    let rule = BodyRuleId::new("x".repeat(129));
    anchor.body = BodyValidationState::ConsensusInvalid {
        evidence,
        rule: rule.clone(),
    };
    anchor
        .eligibility
        .direct_reasons
        .insert(EligibilityReason::ConsensusBodyInvalid { evidence, rule });
    let mut next_metadata = metadata.clone();
    next_metadata.state_version = StateVersion::new(2);
    let changes = ChangeSet {
        put_nodes: vec![anchor],
        delete_nodes: Vec::new(),
        index_changes: zakura_header_chain::IndexChanges::default(),
        selected_projection: zakura_header_chain::ProjectionDelta::default(),
        verified_projection: zakura_header_chain::ProjectionDelta::default(),
        eligibility_changes: Vec::new(),
        aux_changes: Vec::new(),
        finality_append: None,
        metadata: next_metadata,
    };

    assert!(matches!(
        store.batch_for(&changes),
        Err(HeaderChainStoreError::Codec(
            HeaderChainValueError::Oversized {
                field: "body_rule",
                length: 129
            }
        ))
    ));
    assert_eq!(
        store
            .metadata()
            .expect("the original metadata remains readable")
            .state_version,
        StateVersion::new(1)
    );
}

#[test]
fn prepared_full_state_swaps_only_after_combined_commit() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor, metadata) = fixture();
    let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
    store
        .initialize(metadata.clone(), anchor.clone())
        .expect("the empty schema initializes");
    let (runtime, _) = store
        .startup(&engine_config)
        .expect("the initial store audits");
    let evidence = EvidenceId::from_digest([0x44; 32]);
    let request = TransitionRequest {
        expected_version: metadata.state_version,
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
    assert!(matches!(
        PreparedFullStateTransition::new(
            EvidenceId::from_digest([0x45; 32]),
            metadata.frontiers.verified_best,
            Vec::new(),
            NonFinalizedState::new(&engine_config.network),
            None,
            request.clone(),
        ),
        Err(PreparedFullStateTransitionError::IdentityMismatch)
    ));
    let verified_request = TransitionRequest {
        expected_version: metadata.state_version,
        event: TransitionEvent::VerifiedChainChanged(VerifiedChainChanged {
            full_state_transition_id: evidence,
            old_tip: metadata.frontiers.verified_best,
            new_path: Vec::new(),
            cause: VerifiedChangeCause::Reset,
        }),
    };
    assert!(matches!(
        PreparedFullStateTransition::new(
            evidence,
            Frontier::new(block::Height(1), block::Hash([0x55; 32])),
            Vec::new(),
            NonFinalizedState::new(&engine_config.network),
            None,
            verified_request,
        ),
        Err(PreparedFullStateTransitionError::VerifiedPathMismatch)
    ));

    let staged = NonFinalizedState::new(&engine_config.network);
    let mut live = NonFinalizedState::new(&Network::Mainnet);
    let prepared = PreparedFullStateTransition::new(
        evidence,
        metadata.frontiers.verified_best,
        Vec::new(),
        staged,
        None,
        request,
    )
    .expect("the duplicated staged facts agree");
    let context = TransitionContext {
        config: &engine_config,
        clock: &SystemClock,
        full_state_authority: None,
        retention_references: &[],
    };
    let result = prepared
        .commit(&runtime, &mut live, &context)
        .expect("the staged mutation commits");
    assert_eq!(result, ApplyResult::Committed);
    let committed = runtime.publisher().snapshot();
    assert_eq!(live.network, engine_config.network);
    assert_eq!(
        runtime
            .store
            .snapshot()
            .expect("the combined commit is durable"),
        committed
    );
}

#[test]
fn stale_prepared_full_state_transition_is_a_hard_error() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor, metadata) = fixture();
    let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
    store
        .initialize(metadata.clone(), anchor.clone())
        .expect("the empty schema initializes");
    let (runtime, _) = store
        .startup(&engine_config)
        .expect("the initial store audits");
    let evidence = EvidenceId::from_digest([0x51; 32]);
    let marker_key = [0x52; 4];
    let mut full_state_batch = DiskWriteBatch::new();
    runtime
        .store
        .put_raw(
            &mut full_state_batch,
            ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT,
            marker_key,
            [0x53],
        )
        .expect("the full-state marker stages");
    let prepared = PreparedFullStateTransition::new(
        evidence,
        metadata.frontiers.verified_best,
        Vec::new(),
        NonFinalizedState::new(&engine_config.network),
        Some(full_state_batch),
        TransitionRequest {
            expected_version: StateVersion::new(0),
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
        },
    )
    .expect("the duplicated staged facts agree");
    let mut live = NonFinalizedState::new(&Network::Mainnet);
    let error = prepared
        .commit(
            &runtime,
            &mut live,
            &TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: None,
                retention_references: &[],
            },
        )
        .expect_err("stale prepared mutations must not look committed");

    assert!(matches!(
        error,
        HeaderChainStoreError::StaleFullStateTransition {
            current_version
        } if current_version == metadata.state_version
    ));
    assert_eq!(live.network, Network::Mainnet);
    let marker_cf = runtime
        .store
        .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
        .expect("the marker column is open");
    assert_eq!(
        runtime
            .store
            .db
            .raw_get_cf(&marker_cf, &marker_key)
            .expect("the absent marker reads"),
        None
    );
}

#[test]
fn no_change_header_plan_still_commits_full_state_then_swaps_without_publication() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor, metadata) = fixture();
    let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
    store
        .initialize(metadata.clone(), anchor.clone())
        .expect("the empty schema initializes");
    let (runtime, _) = store
        .startup(&engine_config)
        .expect("the initial store audits");
    let evidence = EvidenceId::from_digest([0x61; 32]);
    let request = TransitionRequest {
        expected_version: metadata.state_version,
        event: TransitionEvent::OperatorReconsider(zakura_header_chain::OperatorReconsider {
            target: anchor.hash,
            id: zakura_header_chain::OperatorInvalidationId::new([0x62; 16]),
            evidence,
        }),
    };
    let marker_key = [0x63; 4];
    let mut full_state_batch = DiskWriteBatch::new();
    runtime
        .store
        .put_raw(
            &mut full_state_batch,
            ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT,
            marker_key,
            [0x64],
        )
        .expect("the full-state marker stages");
    let mut live = NonFinalizedState::new(&Network::Mainnet);
    let prepared = PreparedFullStateTransition::new(
        evidence,
        metadata.frontiers.verified_best,
        Vec::new(),
        NonFinalizedState::new(&engine_config.network),
        Some(full_state_batch),
        request,
    )
    .expect("the no-change header evidence is coherent");
    let result = prepared
        .commit(
            &runtime,
            &mut live,
            &TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: None,
                retention_references: &[],
            },
        )
        .expect("the full-state-only mutation commits");

    assert!(matches!(result, ApplyResult::NoChange(_)));
    assert_eq!(live.network, engine_config.network);
    assert_eq!(runtime.publisher().snapshot(), metadata.snapshot());
    let marker_cf = runtime
        .store
        .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
        .expect("the marker column is open");
    assert_eq!(
        runtime
            .store
            .db
            .raw_get_cf(&marker_cf, &marker_key)
            .expect("the committed marker reads"),
        Some(vec![0x64])
    );
}

#[test]
fn mismatched_staged_frontier_writes_and_swaps_nothing() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor, metadata) = fixture();
    let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
    store
        .initialize(metadata.clone(), anchor.clone())
        .expect("the empty schema initializes");
    let (runtime, _) = store
        .startup(&engine_config)
        .expect("the initial store audits");
    let marker_key = [0x71; 4];
    let mut full_state_batch = DiskWriteBatch::new();
    runtime
        .store
        .put_raw(
            &mut full_state_batch,
            ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT,
            marker_key,
            [0x72],
        )
        .expect("the full-state marker stages");
    let swapped = AtomicBool::new(false);
    let expected = Frontier::new(block::Height(1), anchor.hash);
    let error = runtime
        .apply_combined_expected(
            TransitionRequest {
                expected_version: metadata.state_version,
                event: TransitionEvent::OperatorReconsider(
                    zakura_header_chain::OperatorReconsider {
                        target: anchor.hash,
                        id: zakura_header_chain::OperatorInvalidationId::new([0x73; 16]),
                        evidence: EvidenceId::from_digest([0x74; 32]),
                    },
                ),
            },
            &TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: None,
                retention_references: &[],
            },
            full_state_batch,
            expected,
            || swapped.store(true, Ordering::SeqCst),
        )
        .expect_err("a mismatched full-state frontier fails before mutation");

    assert!(matches!(
        error,
        HeaderChainStoreError::VerifiedFrontierMismatch {
            expected: error_expected,
            actual,
        } if error_expected == expected && actual == metadata.frontiers.verified_best
    ));
    assert!(!swapped.load(Ordering::SeqCst));
    assert_eq!(runtime.publisher().snapshot(), metadata.snapshot());
    let marker_cf = runtime
        .store
        .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
        .expect("the marker column is open");
    assert_eq!(
        runtime
            .store
            .db
            .raw_get_cf(&marker_cf, &marker_key)
            .expect("the absent marker reads"),
        None
    );
}
