use super::*;

#[test]
fn state_adapter_rejects_tampered_full_state_finality_provenance() {
    let (_, anchor, metadata) = fixture();
    let snapshot = metadata.snapshot();
    let current = Frontier::new(
        anchor
            .height
            .next()
            .expect("the anchor has a successor height"),
        block::Hash([0x91; 32]),
    );
    let proof = vec![anchor.hash, current.hash];
    let mut event = FullStateFinalized {
        full_state_transition_id: zakura_header_chain::full_state_finality_evidence(
            snapshot.state_version,
            current,
            &proof,
        ),
        new_finalized: current,
        verified_path_proof: proof,
    };
    assert!(validate_full_state_finality_provenance(
        &TransitionEvent::FullStateFinalized(event.clone()),
        &snapshot,
    )
    .is_ok());

    event.full_state_transition_id = EvidenceId::from_digest([0x92; 32]);
    assert!(matches!(
        validate_full_state_finality_provenance(
            &TransitionEvent::FullStateFinalized(event),
            &snapshot,
        ),
        Err(HeaderChainStoreError::Incoherent(
            "full-state finality provenance does not match the authorized transition"
        ))
    ));
}

fn full_state_source(evidence: EvidenceId) -> FinalitySource {
    FinalitySource::FullState {
        provenance: zakura_header_chain::FullStateFinalityProvenance {
            evidence,
            state_version: StateVersion::new(1),
            kind: zakura_header_chain::FullStateFinalityKind::Finalized,
        },
    }
}

#[test]
fn authenticated_full_state_retention_uses_only_the_staged_fork_set() {
    let staged = [block::Hash([0x21; 32]), block::Hash([0x22; 32])];
    let stale_lease = [block::Hash([0x20; 32])];

    assert_eq!(
        combined_retention_references(&staged, None).as_ref(),
        staged,
        "authenticated full state must be able to retire stale header leases"
    );
    assert_eq!(
        combined_retention_references(&staged, Some(&stale_lease)).as_ref(),
        [stale_lease[0], staged[0], staged[1]],
        "ordinary transitions must preserve active header leases"
    );
}

#[test]
fn finality_rebase_reads_only_the_generation_bounded_recent_suffix() {
    let cache = tempfile::tempdir().expect("the test cache directory is created");
    let db_config = Config {
        cache_dir: cache.path().to_owned(),
        ephemeral: false,
        debug_skip_non_finalized_state_backup_task: true,
        ..Config::default()
    };
    let (engine_config, anchor, mut metadata) = fixture();
    let anchor_frontier = Frontier::new(anchor.height, anchor.hash);
    let db = open(&db_config, engine_config.network());
    let store = HeaderChainStore::new(db.clone());
    store
        .initialize(metadata.clone(), anchor)
        .expect("the finality suffix fixture initializes");

    let second = Frontier::new(block::Height(10), block::Hash([0x21; 32]));
    let third = Frontier::new(block::Height(20), block::Hash([0x31; 32]));
    let fourth = Frontier::new(block::Height(30), block::Hash([0x41; 32]));
    let record_two = FinalityRecord {
        previous: anchor_frontier,
        current: second,
        source: full_state_source(EvidenceId::from_digest([0x22; 32])),
        epoch: FinalityEpoch::new(2),
    };
    let record_three = FinalityRecord {
        previous: second,
        current: third,
        source: full_state_source(EvidenceId::from_digest([0x32; 32])),
        epoch: FinalityEpoch::new(3),
    };
    let record_four = FinalityRecord {
        previous: third,
        current: fourth,
        source: full_state_source(EvidenceId::from_digest([0x42; 32])),
        epoch: FinalityEpoch::new(4),
    };
    metadata.finality_epoch = FinalityEpoch::new(4);
    metadata.frontiers.finalized = fourth;
    let mut batch = DiskWriteBatch::new();
    store
        .put_raw(
            &mut batch,
            HEADER_FINALITY_HISTORY,
            HeaderFinalityKey(FinalityEpoch::new(1)).as_bytes(),
            [0xff],
        )
        .expect("the deliberately corrupt old epoch is staged");
    for record in [record_two, record_three, record_four] {
        store
            .put_value(
                &mut batch,
                HEADER_FINALITY_HISTORY,
                HeaderFinalityKey(record.epoch).as_bytes(),
                &record,
            )
            .expect("the recent finality record encodes");
    }
    store
        .put_value(&mut batch, HEADER_ENGINE_META, METADATA_KEY, &metadata)
        .expect("the current finality metadata encodes");
    db.write(batch)
        .expect("the finality suffix fixture commits");

    assert_eq!(
        store
            .finality_rebase_history(third.hash, fourth, 1)
            .expect("one recent epoch is sufficient"),
        vec![record_four]
    );
    assert_eq!(
        store
            .finality_rebase_history(second.hash, fourth, 2)
            .expect("two recent epochs are sufficient"),
        vec![record_three, record_four]
    );
    assert!(store
        .finality_rebase_history(anchor_frontier.hash, fourth, 2)
        .expect("an insufficient generation bound is a stale path")
        .is_empty());
}

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
    let db = open(&db_config, integrated_config.network());
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
    assert_eq!(
        runtime
            .store
            .metadata()
            .expect("the migrated metadata is readable")
            .headers_only_migration_epoch,
        Some(FinalityEpoch::new(0))
    );
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
    let network = engine_config.network().clone();
    let db = open(&db_config, &network);
    let store = HeaderChainStore::new(db.clone());
    store
        .initialize(metadata.clone(), anchor.clone())
        .expect("an empty header schema initializes atomically");
    assert_eq!(store.header_node(anchor.hash), Ok(Some(anchor.clone())));
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
        runtime.store.header_node(anchor.hash).expect("the node row decodes").expect("the anchor remains").body_validation_state,
        BodyValidationState::Unavailable(summary)
            if summary == availability
    ));
    let replay = runtime
        .apply(request, &context)
        .expect("idempotent replay succeeds");
    assert!(
        matches!(replay, ApplyResult::NoChange(receipt) if receipt.state_version == StateVersion::new(2)),
        "unexpected replay result: {replay:?}"
    );
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
            .header_node(anchor.hash)
            .expect("the reopened node row decodes")
            .expect("the reopened anchor exists")
            .body_validation_state,
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
    let store = HeaderChainStore::new(open(&db_config, engine_config.network()));
    store
        .initialize(metadata.clone(), anchor.clone())
        .expect("the empty schema initializes");

    let evidence = EvidenceId::from_digest([9; 32]);
    let rule = BodyRuleId::new("x".repeat(129));
    anchor.body_validation_state = BodyValidationState::ConsensusInvalid {
        evidence,
        rule: rule.clone(),
    };
    let mut next_metadata = metadata.clone();
    next_metadata.state_version = StateVersion::new(2);
    let changes = ChangeSet {
        put_nodes: vec![anchor],
        delete_nodes: Vec::new(),
        put_consensus_invalid_body_tombstones: Vec::new(),
        index_changes: zakura_header_chain::IndexChanges::default(),
        selected_projection: zakura_header_chain::ProjectionDelta::default(),
        verified_projection: zakura_header_chain::ProjectionDelta::default(),
        eligibility_changes: Vec::new(),
        aux_changes: Vec::new(),
        finality_append: None,
        finality_ancestry: zakura_header_chain::FinalityWitnessProof::default(),
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
fn finality_history_creates_an_authenticated_checkpoint_at_the_retained_bound() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor_node, metadata) = fixture();
    let anchor = metadata.frontiers.finalized;
    let store = HeaderChainStore::new(open(&db_config, engine_config.network()));
    store
        .initialize(metadata.clone(), anchor_node)
        .expect("the bounded history fixture initializes");

    let mut seed = DiskWriteBatch::new();
    stage_full_state_canonical_hash(&store, &mut seed, anchor);
    for epoch in 1..u64::try_from(FINALITY_HISTORY_LIMIT).expect("the limit fits u64") {
        let record = FinalityRecord {
            previous: anchor,
            current: anchor,
            source: full_state_source(EvidenceId::from_digest([0x90; 32])),
            epoch: FinalityEpoch::new(epoch),
        };
        store
            .put_value(
                &mut seed,
                HEADER_FINALITY_HISTORY,
                HeaderFinalityKey(record.epoch).as_bytes(),
                &record,
            )
            .expect("the retained history row encodes");
    }
    store
        .put_value(
            &mut seed,
            HEADER_ENGINE_META,
            FINALITY_HISTORY_COUNT_KEY,
            &HeaderRowCountDisk(u64::try_from(FINALITY_HISTORY_LIMIT).expect("the limit fits u64")),
        )
        .expect("the retained history count encodes");
    store.db.write(seed).expect("the bounded history seeds");

    let appended = FinalityRecord {
        previous: anchor,
        current: anchor,
        source: full_state_source(EvidenceId::from_digest([0x91; 32])),
        epoch: FinalityEpoch::new(
            u64::try_from(FINALITY_HISTORY_LIMIT).expect("the limit fits u64"),
        ),
    };
    let mut next_metadata = metadata;
    next_metadata.finality_epoch = appended.epoch;
    let changes = ChangeSet {
        put_nodes: Vec::new(),
        delete_nodes: Vec::new(),
        put_consensus_invalid_body_tombstones: Vec::new(),
        index_changes: zakura_header_chain::IndexChanges::default(),
        selected_projection: zakura_header_chain::ProjectionDelta::default(),
        verified_projection: zakura_header_chain::ProjectionDelta::default(),
        eligibility_changes: Vec::new(),
        aux_changes: Vec::new(),
        finality_append: Some(appended),
        finality_ancestry: zakura_header_chain::FinalityWitnessProof::default(),
        metadata: next_metadata,
    };
    let batch = store
        .batch_for(&changes)
        .expect("the bounded append creates a checkpoint");
    store
        .db
        .write(batch)
        .expect("the checkpoint commits atomically");

    let audit = store
        .audit_snapshot()
        .expect("the checkpoint snapshot opens");
    assert_eq!(
        audit
            .finality_history_checkpoint()
            .expect("the checkpoint decodes"),
        Some(FinalityHistoryCheckpoint {
            epoch: FinalityEpoch::new(0),
            frontier: anchor,
        })
    );
    assert_eq!(
        audit
            .finality_history_count()
            .expect("the retained count decodes"),
        FINALITY_HISTORY_LIMIT
    );
    let mut epochs = Vec::with_capacity(FINALITY_HISTORY_LIMIT);
    audit
        .visit_finality_history(RowLimit::new(FINALITY_HISTORY_LIMIT), &mut |record| {
            epochs.push(record.epoch);
            Ok(())
        })
        .expect("the retained history remains bounded");
    assert_eq!(epochs.first(), Some(&FinalityEpoch::new(1)));
    assert_eq!(epochs.last(), Some(&appended.epoch));
}

#[test]
fn finality_history_eviction_does_not_walk_earlier_tombstones() {
    // Every eviction deletes the lowest key in the retained window, so it leaves a tombstone
    // exactly where a from-the-start seek begins. The window is a few MB, far too small to
    // trigger the compaction that would collect those tombstones, so locating the oldest
    // record by walking from the start costs one skipped delete per eviction ever performed
    // and eventually dominates every block commit. Eviction must seek past the published
    // checkpoint instead, which keeps its cost flat.
    const ROUNDS: u64 = 64;
    const ALLOWED_GROWTH: u64 = 8;

    let db_config = Config::ephemeral();
    let (engine_config, anchor_node, metadata) = fixture();
    let anchor = metadata.frontiers.finalized;
    let store = HeaderChainStore::new(open(&db_config, engine_config.network()));
    store
        .initialize(metadata.clone(), anchor_node)
        .expect("the bounded history fixture initializes");

    let limit = u64::try_from(FINALITY_HISTORY_LIMIT).expect("the limit fits u64");
    let mut seed = DiskWriteBatch::new();
    stage_full_state_canonical_hash(&store, &mut seed, anchor);
    for epoch in 1..limit {
        let record = FinalityRecord {
            previous: anchor,
            current: anchor,
            source: full_state_source(EvidenceId::from_digest([0x90; 32])),
            epoch: FinalityEpoch::new(epoch),
        };
        store
            .put_value(
                &mut seed,
                HEADER_FINALITY_HISTORY,
                HeaderFinalityKey(record.epoch).as_bytes(),
                &record,
            )
            .expect("the retained history row encodes");
    }
    store
        .put_value(
            &mut seed,
            HEADER_ENGINE_META,
            FINALITY_HISTORY_COUNT_KEY,
            &HeaderRowCountDisk(limit),
        )
        .expect("the retained history count encodes");
    store.db.write(seed).expect("the bounded history seeds");

    let mut next_metadata = metadata;
    rocksdb::perf::set_perf_stats(rocksdb::PerfStatsLevel::EnableCount);
    let mut context = rocksdb::PerfContext::default();
    let mut skipped_deletes =
        Vec::with_capacity(usize::try_from(ROUNDS).expect("the round count fits usize"));

    for round in 0..ROUNDS {
        let appended = FinalityRecord {
            previous: anchor,
            current: anchor,
            source: full_state_source(EvidenceId::from_digest([0x91; 32])),
            epoch: FinalityEpoch::new(limit + round),
        };
        next_metadata.finality_epoch = appended.epoch;
        let changes = ChangeSet {
            put_nodes: Vec::new(),
            delete_nodes: Vec::new(),
            put_consensus_invalid_body_tombstones: Vec::new(),
            index_changes: zakura_header_chain::IndexChanges::default(),
            selected_projection: zakura_header_chain::ProjectionDelta::default(),
            verified_projection: zakura_header_chain::ProjectionDelta::default(),
            eligibility_changes: Vec::new(),
            aux_changes: Vec::new(),
            finality_append: Some(appended),
            finality_ancestry: zakura_header_chain::FinalityWitnessProof::default(),
            metadata: next_metadata.clone(),
        };

        context.reset();
        let batch = store
            .batch_for(&changes)
            .expect("the bounded append evicts the oldest record");
        skipped_deletes.push(context.metric(rocksdb::PerfMetric::InternalDeleteSkippedCount));

        store
            .db
            .write(batch)
            .expect("the eviction commits atomically");
    }
    rocksdb::perf::set_perf_stats(rocksdb::PerfStatsLevel::Disable);

    let first = skipped_deletes.first().copied().expect("a round ran");
    let last = skipped_deletes.last().copied().expect("a round ran");
    assert!(
        last <= first + ALLOWED_GROWTH,
        "eviction cost grows with the number of prior evictions: the first eviction skipped \
         {first} deleted keys and eviction {ROUNDS} skipped {last}. Eviction is seeking from \
         the start of the retained window instead of from the published checkpoint. \
         Per-round counts: {skipped_deletes:?}"
    );

    // The window still holds exactly `limit` records, ending at the last epoch appended.
    let audit = store
        .audit_snapshot()
        .expect("the checkpoint snapshot opens");
    assert_eq!(
        audit
            .finality_history_count()
            .expect("the retained count decodes"),
        FINALITY_HISTORY_LIMIT
    );
    assert_eq!(
        audit
            .finality_history_checkpoint()
            .expect("the checkpoint decodes")
            .map(|checkpoint| checkpoint.epoch),
        Some(FinalityEpoch::new(ROUNDS - 1)),
        "each round must evict exactly one record, advancing the checkpoint by one"
    );
    let mut epochs = Vec::with_capacity(FINALITY_HISTORY_LIMIT);
    audit
        .visit_finality_history(RowLimit::new(FINALITY_HISTORY_LIMIT), &mut |record| {
            epochs.push(record.epoch);
            Ok(())
        })
        .expect("the retained history remains bounded");
    assert_eq!(epochs.first(), Some(&FinalityEpoch::new(ROUNDS)));
    assert_eq!(epochs.last(), Some(&FinalityEpoch::new(limit + ROUNDS - 1)));
}

#[test]
fn prepared_full_state_swaps_only_after_combined_commit() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor, metadata) = fixture();
    let store = HeaderChainStore::new(open(&db_config, engine_config.network()));
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
            NonFinalizedState::new(engine_config.network()),
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
            NonFinalizedState::new(engine_config.network()),
            None,
            verified_request,
        ),
        Err(PreparedFullStateTransitionError::VerifiedPathMismatch)
    ));

    let staged = NonFinalizedState::new(engine_config.network());
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
    assert_eq!(&live.network, engine_config.network());
    assert_eq!(
        runtime
            .store
            .snapshot()
            .expect("the combined commit is durable"),
        committed
    );
}

#[test]
fn unrelated_body_commit_cannot_stale_current_header_generation_work() {
    let cache = tempfile::tempdir().expect("the test cache directory is created");
    let db_config = Config {
        cache_dir: cache.path().to_owned(),
        ephemeral: false,
        debug_skip_non_finalized_state_backup_task: true,
        ..Config::default()
    };
    let (engine_config, anchor, metadata) = fixture();
    let store = HeaderChainStore::new(open(&db_config, engine_config.network()));
    store
        .initialize(metadata, anchor.clone())
        .expect("the header schema initializes");
    let (runtime, _) = store
        .startup(&engine_config)
        .expect("the coherent store starts");
    let initial = runtime.publisher().snapshot();
    let anchor_frontier = initial.frontiers.finalized;
    let lease = runtime
        .reader()
        .validation_context(anchor.hash)
        .expect("the anchor validation context is coherent")
        .expect("the initialized anchor is retained");
    let rules = HeaderRules::for_validation_lease(&lease)
        .expect("the authenticated regtest policy is valid");
    let mut child_header = *anchor.header;
    child_header.previous_block_hash = anchor.hash;
    child_header.time += chrono::Duration::seconds(1);
    let child_header = Arc::new(child_header);
    let batch = zakura_header_chain::prepare_headers(
        HeaderBatchInput::new(std::slice::from_ref(&child_header)),
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
    let evidence = EvidenceId::from_digest([0x68; 32]);
    let authority = Authority(evidence);
    let context = TransitionContext {
        config: &engine_config,
        clock: &SystemClock,
        full_state_authority: Some(&authority),
        retention_references: &[],
    };
    assert_eq!(
        runtime
            .apply(
                TransitionRequest {
                    expected_version: initial.state_version,
                    event: TransitionEvent::BodyEvidence(BodyEvidence::Transient(
                        TransientBodyFailure {
                            hash: anchor.hash,
                            evidence,
                            kind: TransientBodyFailureKind::Storage,
                            availability: BodyUnavailableSummary {
                                attempts: 1,
                                suppliers: 1,
                                ..Default::default()
                            },
                        },
                    )),
                },
                &context,
            )
            .expect("the unrelated body transition commits"),
        ApplyResult::Committed
    );
    let after_body = runtime.publisher().snapshot();
    assert_ne!(after_body.state_version, initial.state_version);
    assert_eq!(after_body.header_generation, initial.header_generation);

    let result = runtime
        .apply(
            TransitionRequest {
                expected_version: initial.state_version,
                event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                    owner: header_owner(&initial, child.hash, 1, 1),
                    source: SourceId::from_digest([0x69; 32]),
                    parent_hash: anchor_frontier.hash,
                    target_tip_hash: child.hash,
                    completion: TargetCompletion::TargetComplete {
                        common_ancestor: anchor_frontier,
                    },
                    batch,
                    aux: Vec::new(),
                })),
            },
            &context,
        )
        .expect("generation-current header work reaches the transition planner");

    assert_eq!(result, ApplyResult::Committed);
    assert_eq!(runtime.publisher().snapshot().frontiers.header_best, child);
}

#[test]
fn lazy_work_rebase_commits_coordinates_and_reopens() {
    let cache = tempfile::tempdir().expect("the test cache directory is created");
    let db_config = Config {
        cache_dir: cache.path().to_owned(),
        ephemeral: false,
        debug_skip_non_finalized_state_backup_task: true,
        ..Config::default()
    };
    let (engine_config, anchor, metadata) = fixture();
    let anchor = HeaderNode::from_durable_parts(
        anchor.header.clone(),
        anchor.hash,
        anchor.parent_hash,
        anchor.height,
        anchor.block_work,
        WorkCoordinate::new(anchor.hash, zakura_chain::work::difficulty::U256::MAX),
        anchor.validation,
        anchor.eligibility.clone(),
        anchor.body_validation_state.clone(),
        anchor.aux_delivery_ids.clone(),
    )
    .expect("the near-overflow anchor retains its canonical identity");
    let db = open(&db_config, engine_config.network());
    let store = HeaderChainStore::new(db.clone());
    store
        .initialize(metadata, anchor.clone())
        .expect("the near-overflow coordinate fixture initializes");
    let (runtime, _) = store
        .startup(&engine_config)
        .expect("the near-overflow coordinate fixture starts");
    let initial = runtime.publisher().snapshot();
    let lease = runtime
        .reader()
        .validation_context(anchor.hash)
        .expect("the anchor validation context is coherent")
        .expect("the initialized anchor is retained");
    let rules = HeaderRules::for_validation_lease(&lease)
        .expect("the authenticated regtest policy is valid");
    let mut child_header = *anchor.header;
    child_header.previous_block_hash = anchor.hash;
    child_header.time += chrono::Duration::seconds(1);
    let child_header = Arc::new(child_header);
    let batch = zakura_header_chain::prepare_headers(
        HeaderBatchInput::new(std::slice::from_ref(&child_header)),
        lease.parent(),
        &rules,
        &SystemClock,
    )
    .expect("the overflow-triggering child prepares through production validation");
    let child = Frontier::new(block::Height(1), child_header.hash());
    let context = TransitionContext {
        config: &engine_config,
        clock: &SystemClock,
        full_state_authority: None,
        retention_references: &[],
    };

    assert_eq!(
        runtime
            .apply(
                TransitionRequest {
                    expected_version: initial.state_version,
                    event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                        owner: header_owner(&initial, child.hash, 1, 1),
                        source: SourceId::from_digest([0x6a; 32]),
                        parent_hash: anchor.hash,
                        target_tip_hash: child.hash,
                        completion: TargetCompletion::TargetComplete {
                            common_ancestor: initial.frontiers.finalized,
                        },
                        batch,
                        aux: Vec::new(),
                    })),
                },
                &context,
            )
            .expect("the valid insertion lazily rebases before it commits"),
        ApplyResult::Committed
    );
    let committed = runtime.publisher().snapshot();
    assert_eq!(committed.frontiers.header_best, child);
    assert_eq!(
        runtime
            .store
            .metadata()
            .expect("the rebased metadata row decodes")
            .work_origin,
        initial.frontiers.finalized
    );
    assert_eq!(
        runtime
            .store
            .header_node(anchor.hash)
            .expect("the rebased anchor row decodes")
            .expect("the rebased anchor remains")
            .work_coordinate(),
        WorkCoordinate::new(anchor.hash, Default::default())
    );

    drop(runtime);
    drop(db);
    let (reopened, report) = HeaderChainStore::new(open(&db_config, engine_config.network()))
        .startup(&engine_config)
        .expect("recovery authenticates the durable rebased coordinates");
    assert_eq!(report.current, committed);
    assert_eq!(reopened.publisher().snapshot(), committed);
    assert_eq!(
        reopened
            .store
            .header_node(child.hash)
            .expect("the rebased child row decodes")
            .expect("the rebased child remains")
            .work_coordinate()
            .origin_hash(),
        anchor.hash
    );
}

#[test]
fn resource_stall_alarm_is_published_and_durable_before_refusal() {
    let cache = tempfile::tempdir().expect("the test cache directory is created");
    let db_config = Config {
        cache_dir: cache.path().to_owned(),
        ephemeral: false,
        debug_skip_non_finalized_state_backup_task: true,
        ..Config::default()
    };
    let (mut engine_config, anchor, metadata) = fixture();
    engine_config.limits.max_non_finalized_nodes = NonZeroUsize::new(1).expect("one is nonzero");
    let db = open(&db_config, engine_config.network());
    let store = HeaderChainStore::new(db.clone());
    store
        .initialize(metadata, anchor.clone())
        .expect("the header schema initializes");
    let (runtime, _) = store
        .startup(&engine_config)
        .expect("the coherent store starts");
    let initial = runtime.publisher().snapshot();
    let anchor_frontier = initial.frontiers.finalized;
    let lease = runtime
        .reader()
        .validation_context(anchor.hash)
        .expect("the anchor validation context is coherent")
        .expect("the initialized anchor is retained");
    let rules = HeaderRules::for_validation_lease(&lease)
        .expect("the authenticated regtest policy is valid");
    let mut first_header = *anchor.header;
    first_header.previous_block_hash = anchor.hash;
    first_header.time += chrono::Duration::seconds(1);
    let first_header = Arc::new(first_header);
    let mut second_header = *first_header;
    second_header.previous_block_hash = first_header.hash();
    second_header.time += chrono::Duration::seconds(1);
    let second_header = Arc::new(second_header);
    let batch = zakura_header_chain::prepare_headers(
        HeaderBatchInput::new(&[first_header, second_header.clone()]),
        lease.parent(),
        &rules,
        &SystemClock,
    )
    .expect("the two-child batch prepares through production validation");
    let target = Frontier::new(block::Height(2), second_header.hash());
    let owner = header_owner(&initial, target.hash, 1, 1);
    let attempted_branch = owner.header_authority().branch;
    let request = TransitionRequest {
        expected_version: initial.state_version,
        event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
            owner,
            source: SourceId::from_digest([0x70; 32]),
            parent_hash: anchor.hash,
            target_tip_hash: target.hash,
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
    let marker_key = b"resource-stall-caller-batch";
    let marker_cf = runtime
        .store
        .cf(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
        .expect("the caller-batch marker column family exists");
    let mut caller_batch = DiskWriteBatch::new();
    caller_batch.zs_insert(
        &marker_cf,
        RawBytes::new_raw_bytes(marker_key.to_vec()),
        RawBytes::new_raw_bytes(vec![1]),
    );
    let swapped = AtomicBool::new(false);
    let result = runtime.apply_combined(request.clone(), &context, caller_batch, || {
        swapped.store(true, Ordering::SeqCst)
    });
    assert!(matches!(
        result,
        Ok(ApplyResult::ResourceStalled(CommittedStallReceipt {
            state_version,
            alarm_changed: true,
            attempted_branch: Some(branch),
        })) if state_version == StateVersion::new(2) && branch == attempted_branch
    ));
    assert!(!swapped.load(Ordering::SeqCst));
    assert_eq!(
        runtime
            .store
            .db
            .raw_get_cf(&marker_cf, marker_key)
            .expect("the caller marker read succeeds"),
        None,
        "a resource stall commits only its alarm metadata"
    );

    let repeated = runtime.apply(request, &context);
    assert!(matches!(
        repeated,
        Ok(ApplyResult::ResourceStalled(CommittedStallReceipt {
            state_version,
            alarm_changed: false,
            attempted_branch: Some(branch),
        })) if state_version == StateVersion::new(2) && branch == attempted_branch
    ));

    let published = runtime.publisher().snapshot();
    assert!(published.alarms.resource_stalled);
    assert_eq!(published.state_version, StateVersion::new(2));
    assert_eq!(published.frontiers, initial.frontiers);
    assert_eq!(
        runtime
            .store
            .metadata()
            .expect("the resource alarm metadata is readable")
            .snapshot(),
        published
    );
    assert_eq!(
        runtime
            .store
            .load_header_nodes()
            .expect("the retained graph is readable"),
        vec![anchor]
    );

    drop(runtime);
    let (reopened, report) = HeaderChainStore::new(db)
        .startup(&engine_config)
        .expect("the resource-stalled store reopens coherently");
    assert!(report.current.alarms.resource_stalled);
    assert_eq!(reopened.publisher().snapshot(), published);
}

#[test]
fn stale_prepared_full_state_transition_is_a_hard_error() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor, metadata) = fixture();
    let store = HeaderChainStore::new(open(&db_config, engine_config.network()));
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
        NonFinalizedState::new(engine_config.network()),
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
    let store = HeaderChainStore::new(open(&db_config, engine_config.network()));
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
            invalidation_evidence: None,
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
        NonFinalizedState::new(engine_config.network()),
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
    assert_eq!(&live.network, engine_config.network());
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
    let store = HeaderChainStore::new(open(&db_config, engine_config.network()));
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
    let authority = Authority(EvidenceId::from_digest([0x74; 32]));
    let error = runtime
        .apply_combined_expected(
            TransitionRequest {
                expected_version: metadata.state_version,
                event: TransitionEvent::OperatorReconsider(
                    zakura_header_chain::OperatorReconsider {
                        target: anchor.hash,
                        id: zakura_header_chain::OperatorInvalidationId::new([0x73; 16]),
                        invalidation_evidence: None,
                        evidence: EvidenceId::from_digest([0x74; 32]),
                    },
                ),
            },
            &TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: Some(&authority),
                retention_references: &[],
            },
            full_state_batch,
            expected,
            &[],
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

#[test]
fn checkpoint_auxiliary_staging_does_not_clone_the_retained_engine() {
    let source = include_str!("../../header_chain.rs");
    let start = source
        .find("pub(in crate::service) fn apply_aux_then_checkpoint_combined")
        .expect("the combined checkpoint implementation exists");
    let end = source[start..]
        .find("fn apply_combined_with_fault")
        .map(|offset| start.saturating_add(offset))
        .expect("the next runtime method bounds the combined checkpoint implementation");
    let implementation = &source[start..end];

    assert!(
        !implementation.contains("transition_engine.clone()"),
        "per-block VCT staging must not clone the retained header engine"
    );
    assert!(
        implementation.contains("restore_transition_engine_after_staging_error"),
        "pre-commit staging errors must restore the unchanged durable engine"
    );
    assert!(
        implementation.contains("checkpoint_headers_are_retained"),
        "already admitted checkpoint headers must not rebuild predecessor leases per block"
    );
    assert!(
        implementation.contains("transition_engine.graph().header_node(header.hash).is_some()"),
        "the predecessor-lease fast path must be justified by the coherent retained graph"
    );
    let validated = implementation
        .find("validate_full_state_finality_provenance")
        .expect("the combined checkpoint path validates full-state finality provenance");
    let staged = implementation
        .find("install_committed_transition")
        .expect("the combined checkpoint path stages the auxiliary transition");
    assert!(
        validated < staged,
        "checkpoint provenance must be validated against the pre-auxiliary snapshot, \
         because staging the auxiliary transition advances the state version the \
         state writer bound its evidence to"
    );
}

#[test]
fn coalesced_replacement_cannot_hide_body_work_epoch() {
    let (_, _, metadata) = fixture();
    let initial = metadata.snapshot();
    let publisher = Publisher::new(initial.clone());
    let subscriber = publisher.subscribe_views();
    assert_eq!(
        subscriber.borrow().body_work_epoch,
        zakura_header_chain::BodyWorkEpoch::default()
    );

    let mut compatible = initial.clone();
    compatible.state_version = StateVersion::new(initial.state_version.get() + 1);
    publisher.publish(compatible.clone(), TransitionEffect::none());
    assert_eq!(
        publisher.view().body_work_epoch,
        zakura_header_chain::BodyWorkEpoch::default()
    );

    let mut invalidated = compatible.clone();
    invalidated.state_version = StateVersion::new(compatible.state_version.get() + 1);
    let mut invalidating_effect = TransitionEffect::none();
    invalidating_effect.body_work = zakura_header_chain::BodyWorkEffect::Invalidated;
    publisher.publish(invalidated.clone(), invalidating_effect);

    let mut later_extension = invalidated;
    later_extension.state_version = StateVersion::new(later_extension.state_version.get() + 1);
    publisher.publish(later_extension, TransitionEffect::none());
    assert_eq!(
        subscriber.borrow().body_work_epoch,
        zakura_header_chain::BodyWorkEpoch::new(1),
        "a coalesced replacement and extension must retain the cumulative epoch change"
    );

    let before_resource_stall = publisher.view().body_work_epoch;
    publisher.publish(publisher.snapshot(), TransitionEffect::resource_stalled());
    assert_eq!(publisher.view().body_work_epoch, before_resource_stall);
}

#[test]
fn repeated_compatible_finality_publications_preserve_body_work_epoch() {
    let (_, _, metadata) = fixture();
    let initial = metadata.snapshot();
    let publisher = Publisher::new(initial.clone());

    let mut first_checkpoint = initial;
    first_checkpoint.state_version = StateVersion::new(first_checkpoint.state_version.get() + 1);
    publisher.publish(first_checkpoint.clone(), TransitionEffect::none());

    let mut second_checkpoint = first_checkpoint;
    second_checkpoint.state_version = StateVersion::new(second_checkpoint.state_version.get() + 1);
    publisher.publish(second_checkpoint, TransitionEffect::none());

    assert_eq!(
        publisher.view().body_work_epoch,
        zakura_header_chain::BodyWorkEpoch::default(),
        "compatible checkpoint publications must not advance the cumulative epoch"
    );
}

#[test]
fn combined_checkpoint_rejects_stale_version_and_accepts_pre_auxiliary_evidence() {
    let cache = tempfile::tempdir().expect("the test cache directory is created");
    let db_config = Config {
        cache_dir: cache.path().to_owned(),
        ephemeral: false,
        debug_skip_non_finalized_state_backup_task: true,
        ..Config::default()
    };
    let (engine_config, anchor, metadata) = fixture();
    let store = HeaderChainStore::new(open(&db_config, engine_config.network()));
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
    let mut child_header = *anchor.header;
    child_header.previous_block_hash = anchor.hash;
    child_header.time += chrono::Duration::seconds(1);
    child_header.nonce.0[0] = 0x81;
    let child_header = Arc::new(child_header);
    // Authenticating a VCT delivery reads the delivery's direct successor on the owned
    // branch, so the fixture admits one header past the checkpoint target.
    let mut successor_header = *child_header;
    successor_header.previous_block_hash = child_header.hash();
    successor_header.time += chrono::Duration::seconds(1);
    successor_header.nonce.0[0] = 0x85;
    let successor_header = Arc::new(successor_header);
    let headers = [child_header.clone(), successor_header.clone()];
    let insertion_batch = zakura_header_chain::prepare_headers(
        HeaderBatchInput::new(&headers),
        lease.parent(),
        &rules,
        &SystemClock,
    )
    .expect("the checkpoint fixture passes production validation");
    let child = Frontier::new(
        anchor
            .height
            .next()
            .expect("the genesis anchor has a next height"),
        child_header.hash(),
    );
    let successor = Frontier::new(
        child.height.next().expect("the child has a next height"),
        successor_header.hash(),
    );

    // The unauthenticated delivery gives the auxiliary transition real durable work, which is
    // what advances the state version between the writer's read and the checkpoint plan.
    let source = SourceId::from_digest([0x82; 32]);
    // The admission rule requires the delivery and its insertion to share one owner.
    let insertion_owner = header_owner(&initial, successor.hash, 63, 64);
    let delivery = zakura_header_chain::AuxDelivery::new(
        EvidenceId::from_digest([0x83; 32]),
        child.hash,
        source,
        insertion_owner,
        zakura_header_chain::BodySizeHint::Unknown,
        Some(zakura_header_chain::TreeAuxRecordV1 {
            height: child.height,
            sapling_root: Default::default(),
            orchard_root: Default::default(),
            ironwood_root: Default::default(),
            sapling_tx_count: 4,
            orchard_tx_count: 5,
            ironwood_tx_count: 6,
            auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([0x84; 32]),
        }),
    );
    runtime
        .apply(
            TransitionRequest {
                expected_version: initial.state_version,
                event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                    owner: insertion_owner,
                    source,
                    parent_hash: anchor.hash,
                    target_tip_hash: successor.hash,
                    completion: TargetCompletion::TargetComplete {
                        common_ancestor: anchor_frontier,
                    },
                    batch: insertion_batch,
                    aux: vec![delivery],
                })),
            },
            &TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: None,
                retention_references: &[],
            },
        )
        .expect("the checkpoint header inserts with its unauthenticated delivery");

    let before = runtime.publisher().snapshot();
    assert_eq!(before.frontiers.verified_best, anchor_frontier);

    // The state writer binds checkpoint finality evidence to the version it read.
    let checkpoint_evidence =
        zakura_header_chain::checkpoint_finality_evidence(before.state_version, child);
    let checkpoint_event = TransitionEvent::VerifiedChainChanged(VerifiedChainChanged {
        full_state_transition_id: checkpoint_evidence,
        old_tip: before.frontiers.verified_best,
        new_path: vec![zakura_header_chain::VerifiedHeaderRef {
            height: child.height,
            hash: child.hash,
            header: child_header,
        }],
        cause: VerifiedChangeCause::CheckpointFinalizedGrow,
    });

    let observation = zakura_header_chain::AuxObservationV1::from_vct(
        body_owner(&before, 63, 64),
        vec![delivery],
        zakura_header_chain::AuxVerificationFactV1::current_delivery_verified(),
        Some(zakura_chain::block::merkle::AuthDataRoot::from([0x84; 32])),
    )
    .expect("the VCT authentication observation is well formed");
    let aux_event = TransitionEvent::AuxEvidence(Box::new(
        zakura_header_chain::AuxEvidence::observed(observation),
    ));
    let aux_authority = Authority(
        aux_event
            .idempotency_key()
            .expect("the auxiliary observation has an identity"),
    );
    let checkpoint_authority = Authority(checkpoint_evidence);

    let stale_version = StateVersion::new(
        before
            .state_version
            .get()
            .checked_sub(1)
            .expect("header insertion advanced the initial version"),
    );
    let mut stale_full_state_batch = DiskWriteBatch::new();
    stage_full_state_canonical_hash(&runtime.store, &mut stale_full_state_batch, child);
    let mut stale_memory_swapped = false;
    let stale_result = runtime
        .apply_aux_then_checkpoint_combined(
            TransitionRequest {
                expected_version: before.state_version,
                event: aux_event.clone(),
            },
            &TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: Some(&aux_authority),
                retention_references: &[],
            },
            TransitionRequest {
                expected_version: stale_version,
                event: checkpoint_event.clone(),
            },
            &TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: Some(&checkpoint_authority),
                retention_references: &[],
            },
            stale_full_state_batch,
            || stale_memory_swapped = true,
        )
        .expect("a stale checkpoint request returns a typed result");

    assert_eq!(
        stale_result,
        ApplyResult::Stale(StaleReceipt {
            current_version: before.state_version,
            branch: None,
        })
    );
    assert!(!stale_memory_swapped);
    assert_eq!(runtime.publisher().snapshot(), before);

    let mut full_state_batch = DiskWriteBatch::new();
    stage_full_state_canonical_hash(&runtime.store, &mut full_state_batch, child);

    let result = runtime
        .apply_aux_then_checkpoint_combined(
            TransitionRequest {
                expected_version: before.state_version,
                event: aux_event,
            },
            &TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: Some(&aux_authority),
                retention_references: &[],
            },
            TransitionRequest {
                expected_version: before.state_version,
                event: checkpoint_event,
            },
            &TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: Some(&checkpoint_authority),
                retention_references: &[],
            },
            full_state_batch,
            || {},
        )
        .expect("the checkpoint commits against evidence bound to the pre-auxiliary version");

    assert!(matches!(result, ApplyResult::Committed));
    let after = runtime.publisher().snapshot();
    assert_eq!(after.frontiers.verified_best, child);
    assert_eq!(
        after.state_version.get(),
        before.state_version.get() + 2,
        "the auxiliary transition must advance the version the checkpoint evidence was bound to"
    );

    drop(runtime);
    let (reopened, report) = HeaderChainStore::new(open(&db_config, engine_config.network()))
        .startup(&engine_config)
        .expect("recovery authenticates the pre-auxiliary checkpoint provenance");
    assert_eq!(report.current, after);
    assert_eq!(reopened.publisher().snapshot(), after);
}

#[test]
fn checkpoint_grow_provenance_is_bound_to_one_state_version() {
    let (_, anchor, metadata) = fixture();
    let snapshot = metadata.snapshot();
    let current = Frontier::new(
        anchor
            .height
            .next()
            .expect("the anchor has a successor height"),
        block::Hash([0x95; 32]),
    );
    let event = TransitionEvent::VerifiedChainChanged(VerifiedChainChanged {
        full_state_transition_id: zakura_header_chain::checkpoint_finality_evidence(
            snapshot.state_version,
            current,
        ),
        old_tip: metadata.frontiers.verified_best,
        new_path: vec![zakura_header_chain::VerifiedHeaderRef {
            height: current.height,
            hash: current.hash,
            header: anchor.header.clone(),
        }],
        cause: VerifiedChangeCause::CheckpointFinalizedGrow,
    });
    assert!(validate_full_state_finality_provenance(&event, &snapshot).is_ok());

    let mut advanced = snapshot.clone();
    advanced.state_version = StateVersion::new(snapshot.state_version.get() + 1);
    assert!(
        matches!(
            validate_full_state_finality_provenance(&event, &advanced),
            Err(HeaderChainStoreError::Incoherent(
                "full-state finality provenance does not match the authorized transition"
            ))
        ),
        "checkpoint evidence must not validate against a version the state writer never read"
    );
}
