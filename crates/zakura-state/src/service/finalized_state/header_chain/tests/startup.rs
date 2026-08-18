use super::*;

/// Commit one deferred header at `insertion_time`, then return the closed database.
fn commit_deferral(
    header_generation: HeaderGeneration,
    insertion_time: DateTime<Utc>,
) -> (DiskDb, EngineConfig, Frontier) {
    #[derive(Copy, Clone)]
    struct FixedClock(DateTime<Utc>);

    impl zakura_header_chain::Clock for FixedClock {
        fn now(&self) -> DateTime<Utc> {
            self.0
        }
    }

    let db_config = Config::ephemeral();
    let (engine_config, anchor, mut metadata) = fixture();
    metadata.header_generation = header_generation;
    let db = open(&db_config, engine_config.network());
    let store = HeaderChainStore::new(db.clone());
    store
        .initialize(metadata, anchor.clone())
        .expect("the valid anchor initializes the fixture");
    let (runtime, _) = store
        .startup(&engine_config)
        .expect("the initial store audits");
    let before = runtime.publisher().snapshot();
    let insertion_clock = FixedClock(insertion_time);
    let mut child_header = *anchor.header;
    child_header.previous_block_hash = anchor.hash;
    child_header.time = insertion_clock.0 + chrono::Duration::hours(3);
    child_header.nonce.0[0] = 0x51;
    let child_header = Arc::new(child_header);
    let child = Frontier::new(
        anchor
            .height
            .next()
            .expect("the anchor has a successor height"),
        child_header.hash(),
    );
    let lease = runtime
        .reader()
        .validation_context(anchor.hash)
        .expect("the anchor validation context is coherent")
        .expect("the anchor is retained");
    let rules = HeaderRules::for_validation_lease(&lease)
        .expect("the authenticated network policy is valid");
    let batch = zakura_header_chain::prepare_headers(
        HeaderBatchInput::new(std::slice::from_ref(&child_header)),
        lease.parent(),
        &rules,
        &insertion_clock,
    )
    .expect("the future header prepares as deferred");
    assert!(matches!(
        batch.headers()[0].validation,
        HeaderValidationState::DeferredUntil(_)
    ));
    runtime
        .apply(
            TransitionRequest {
                expected_version: before.state_version,
                event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                    owner: header_owner(&before, child.hash, 0x51, 0x52),
                    source: SourceId::from_digest([0x51; 32]),
                    parent_hash: anchor.hash,
                    target_tip_hash: child.hash,
                    completion: TargetCompletion::TargetComplete {
                        common_ancestor: Frontier::new(anchor.height, anchor.hash),
                    },
                    batch,
                    aux: Vec::new(),
                })),
            },
            &TransitionContext {
                config: &engine_config,
                clock: &insertion_clock,
                full_state_authority: None,
                retention_references: &[],
            },
        )
        .expect("the deferred header insertion commits");
    assert_eq!(
        runtime.publisher().snapshot().frontiers.header_best,
        Frontier::new(anchor.height, anchor.hash)
    );
    drop(runtime);
    (db, engine_config, child)
}

#[test]
fn startup_commits_deferred_reevaluation_before_publication() {
    let (db, engine_config, child) = commit_deferral(
        HeaderGeneration::new(1),
        Utc::now() - chrono::Duration::hours(3),
    );

    let (reopened, report) = HeaderChainStore::new(db)
        .startup(&engine_config)
        .expect("startup reevaluates the elapsed deferral before publication");
    assert!(report.repairs.is_empty());
    assert_eq!(report.current.frontiers.header_best, child);
    assert_eq!(reopened.publisher().snapshot(), report.current);
    assert_eq!(
        reopened
            .store
            .header_node(child.hash)
            .expect("the child row is readable")
            .expect("the child remains retained")
            .validation,
        HeaderValidationState::Valid
    );
    assert_eq!(
        reopened
            .store
            .deferred_entries()
            .expect("the deferred index is readable"),
        Vec::new()
    );
}

#[test]
fn startup_rejects_a_due_deferral_when_settlement_cannot_plan() {
    // The insertion consumes the last header generation. Startup must reject the database because
    // the runtime would repeat the same exhausted transition as soon as its writer starts.
    let (db, engine_config, _) = commit_deferral(
        HeaderGeneration::new(u64::MAX.saturating_sub(1)),
        Utc::now() - chrono::Duration::hours(3),
    );

    assert!(matches!(
        HeaderChainStore::new(db).startup(&engine_config),
        Err(HeaderChainStoreError::Transition(
            TransitionFailure::Counter(_)
        ))
    ));
}

#[test]
fn startup_preserves_a_future_deferral_without_settlement() {
    let (db, engine_config, child) = commit_deferral(HeaderGeneration::new(1), Utc::now());

    let (reopened, report) = HeaderChainStore::new(db)
        .startup(&engine_config)
        .expect("a future deferral does not require startup settlement");
    assert!(report.repairs.is_empty());
    assert_eq!(
        report.current.frontiers.header_best,
        engine_config.bootstrap_anchor().frontier
    );
    assert!(matches!(
        reopened
            .store
            .header_node(child.hash)
            .expect("the child row is readable")
            .expect("the child remains retained")
            .validation,
        HeaderValidationState::DeferredUntil(until) if until > Utc::now()
    ));
    assert_eq!(
        reopened
            .store
            .deferred_entries()
            .expect("the deferred index is readable")
            .len(),
        1
    );
}

#[test]
fn startup_rejects_an_ineligible_verified_projection_before_publication() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor, metadata) = fixture();
    let anchor_frontier = Frontier::new(anchor.height, anchor.hash);
    let db = open(&db_config, engine_config.network());
    let store = HeaderChainStore::new(db.clone());
    store
        .initialize(metadata, anchor.clone())
        .expect("the header schema initializes");

    let mut child_header = *anchor.header;
    child_header.previous_block_hash = anchor.hash;
    child_header.time += chrono::Duration::seconds(1);
    child_header.nonce.0[0] = 0x71;
    let child_header = Arc::new(child_header);
    let child = VerifiedHeaderRef {
        height: anchor
            .height
            .next()
            .expect("the genesis anchor has a successor"),
        hash: child_header.hash(),
        header: child_header,
    };
    let (runtime, _) = store
        .startup_reconciled(
            &engine_config,
            anchor_frontier,
            Vec::new(),
            vec![child.clone()],
        )
        .expect("the verified child reconciles before the corruption");

    let evidence = EvidenceId::from_digest([0x72; 32]);
    let id = OperatorInvalidationId::new([0x73; 16]);
    let mut hasher = sha2::Sha256::new();
    use sha2::Digest as _;
    hasher.update(b"zakura-operator-invalidation-v1");
    hasher.update(child.hash.0);
    hasher.update(id.bytes());
    let before = runtime.publisher().snapshot();
    runtime
        .apply(
            TransitionRequest {
                expected_version: before.state_version,
                event: TransitionEvent::OperatorInvalidate(OperatorInvalidate {
                    target: child.hash,
                    id,
                    operator_reason_digest: hasher.finalize().into(),
                    evidence,
                }),
            },
            &TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: Some(&Authority(evidence)),
                retention_references: &[],
            },
        )
        .expect("the operator invalidation removes the child from active projections");
    assert_eq!(
        runtime.publisher().snapshot().frontiers.verified_best,
        anchor_frontier
    );

    let child_frontier = Frontier::new(child.height, child.hash);
    let mut corrupt_metadata = runtime
        .store
        .metadata()
        .expect("the post-invalidation metadata is readable");
    corrupt_metadata.frontiers.verified_best = child_frontier;
    let mut corrupt = DiskWriteBatch::new();
    runtime
        .store
        .put_value(
            &mut corrupt,
            HEADER_ENGINE_META,
            METADATA_KEY,
            &corrupt_metadata,
        )
        .expect("the corrupt metadata encodes");
    runtime
        .store
        .put_raw(
            &mut corrupt,
            HEADER_VERIFIED,
            HeaderHeightKey(child.height).as_bytes(),
            child.hash.0,
        )
        .expect("the corrupt verified projection row encodes");
    db.write(corrupt)
        .expect("the ineligible verified projection reaches RocksDB");
    drop(runtime);

    assert!(matches!(
        HeaderChainStore::new(db).startup(&engine_config),
        Err(HeaderChainStoreError::Recovery(RecoveryFailure::Source { violations }))
            if violations == vec![zakura_header_chain::AuditViolation::ProtectedPath(child.hash)]
    ));
}

#[test]
fn rocksdb_recovery_rejects_a_forged_headers_only_witness() {
    let db_config = Config::ephemeral();
    let (mut engine_config, anchor, mut metadata) = fixture();
    engine_config.mode = EngineMode::HeadersOnly;
    engine_config.limits.local_finality_depth =
        std::num::NonZeroU32::new(1).expect("one is nonzero");
    metadata.mode = EngineMode::HeadersOnly;
    let db = open(&db_config, engine_config.network());
    let store = HeaderChainStore::new(db.clone());
    store
        .initialize(metadata, anchor.clone())
        .expect("the headers-only anchor initializes");
    let (runtime, _) = store
        .startup(&engine_config)
        .expect("the initial headers-only store audits");
    let before = runtime.publisher().snapshot();
    let lease = runtime
        .reader()
        .validation_context(anchor.hash)
        .expect("the anchor validation context is coherent")
        .expect("the anchor is retained");
    let rules = HeaderRules::for_validation_lease(&lease)
        .expect("the authenticated network policy is valid");
    let mut headers = Vec::new();
    let mut parent = anchor.hash;
    let mut parent_header = anchor.header;
    for nonce in [0x61, 0x62] {
        let mut header = *parent_header;
        header.previous_block_hash = parent;
        header.time += chrono::Duration::seconds(1);
        header.nonce.0[0] = nonce;
        let header = Arc::new(header);
        parent = header.hash();
        parent_header = header.clone();
        headers.push(header);
    }
    let batch = zakura_header_chain::prepare_headers(
        HeaderBatchInput::new(&headers),
        lease.parent(),
        &rules,
        &SystemClock,
    )
    .expect("the two-header branch prepares");
    let selected_tip = Frontier::new(block::Height(2), headers[1].hash());
    runtime
        .apply(
            TransitionRequest {
                expected_version: before.state_version,
                event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                    owner: header_owner(&before, selected_tip.hash, 0x61, 0x62),
                    source: SourceId::from_digest([0x61; 32]),
                    parent_hash: anchor.hash,
                    target_tip_hash: selected_tip.hash,
                    completion: TargetCompletion::TargetComplete {
                        common_ancestor: Frontier::new(anchor.height, anchor.hash),
                    },
                    batch,
                    aux: Vec::new(),
                })),
            },
            &TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: None,
                retention_references: &[],
            },
        )
        .expect("the headers-only finality transition commits");
    let finalized = runtime.publisher().snapshot().frontiers.finalized;
    assert_eq!(finalized.height, block::Height(1));

    // The selected tip sits above the finalized frontier, where no canonical index reaches, so
    // only the finalized frontier needs an independent row.
    let mut canonical = DiskWriteBatch::new();
    runtime
        .store
        .put_raw(
            &mut canonical,
            "zakura_header_hash_by_height",
            HeaderHeightKey(finalized.height).as_bytes(),
            finalized.hash.0,
        )
        .expect("the independent canonical finalized row encodes");
    db.write(canonical)
        .expect("the independent canonical finalized row commits");
    audit_store(&runtime.store, &engine_config)
        .expect("the exact headers-only selected-tip witness recovers");

    let witness_key = HeaderFinalityWitnessKey {
        height: selected_tip.height,
        hash: selected_tip.hash,
    }
    .as_bytes();
    let witness_cf = runtime
        .store
        .cf(HEADER_FINALITY_WITNESS)
        .expect("the witness column family exists");
    let witness_value = db
        .raw_get_cf(&witness_cf, &witness_key)
        .expect("the witness row is readable")
        .expect("the selected tip has a witness row");

    let mut bad_count = DiskWriteBatch::new();
    runtime
        .store
        .put_value(
            &mut bad_count,
            HEADER_ENGINE_META,
            FINALITY_WITNESS_COUNT_KEY,
            &HeaderRowCountDisk(2),
        )
        .expect("the forged witness count encodes");
    db.write(bad_count)
        .expect("the forged witness count reaches RocksDB");
    assert!(HeaderChainStore::new(db.clone())
        .startup(&engine_config)
        .is_err());
    let mut restore_count = DiskWriteBatch::new();
    runtime
        .store
        .put_value(
            &mut restore_count,
            HEADER_ENGINE_META,
            FINALITY_WITNESS_COUNT_KEY,
            &HeaderRowCountDisk(1),
        )
        .expect("the exact witness count encodes");
    db.write(restore_count)
        .expect("the exact witness count is restored");

    let mut zero_references = witness_value.clone();
    zero_references[1..9].fill(0);
    let mut bad_references = DiskWriteBatch::new();
    runtime
        .store
        .put_raw(
            &mut bad_references,
            HEADER_FINALITY_WITNESS,
            witness_key,
            zero_references,
        )
        .expect("the forged witness references stage");
    db.write(bad_references)
        .expect("the forged witness references reach RocksDB");
    assert!(HeaderChainStore::new(db.clone())
        .startup(&engine_config)
        .is_err());
    let mut restore_witness = DiskWriteBatch::new();
    runtime
        .store
        .put_raw(
            &mut restore_witness,
            HEADER_FINALITY_WITNESS,
            witness_key,
            &witness_value,
        )
        .expect("the exact witness row stages");
    db.write(restore_witness)
        .expect("the exact witness row is restored");

    let forged_key = HeaderFinalityWitnessKey {
        height: selected_tip.height,
        hash: block::Hash([0x64; 32]),
    }
    .as_bytes();
    let mut bad_key = DiskWriteBatch::new();
    runtime
        .store
        .delete_raw(&mut bad_key, HEADER_FINALITY_WITNESS, witness_key)
        .expect("the exact witness deletion stages");
    runtime
        .store
        .put_raw(
            &mut bad_key,
            HEADER_FINALITY_WITNESS,
            forged_key,
            &witness_value,
        )
        .expect("the forged witness key stages");
    db.write(bad_key)
        .expect("the forged witness key reaches RocksDB");
    assert!(HeaderChainStore::new(db.clone())
        .startup(&engine_config)
        .is_err());
    let mut restore_key = DiskWriteBatch::new();
    runtime
        .store
        .delete_raw(&mut restore_key, HEADER_FINALITY_WITNESS, forged_key)
        .expect("the forged witness deletion stages");
    runtime
        .store
        .put_raw(
            &mut restore_key,
            HEADER_FINALITY_WITNESS,
            witness_key,
            &witness_value,
        )
        .expect("the exact witness key stages");
    db.write(restore_key)
        .expect("the exact witness key is restored");

    let mut forged = runtime
        .store
        .finality_history()
        .expect("the finality history is readable")
        .last()
        .copied()
        .expect("the depth transition appended a finality record");
    forged.source = FinalitySource::HeadersOnlyDepth {
        selected_tip: Frontier::new(selected_tip.height, block::Hash([0x63; 32])),
    };
    let mut corruption = DiskWriteBatch::new();
    runtime
        .store
        .put_value(
            &mut corruption,
            HEADER_FINALITY_HISTORY,
            HeaderFinalityKey(forged.epoch).as_bytes(),
            &forged,
        )
        .expect("the forged finality row encodes");
    db.write(corruption)
        .expect("the forged finality row reaches RocksDB");
    drop(runtime);

    assert!(matches!(
        HeaderChainStore::new(db).startup(&engine_config),
        Err(HeaderChainStoreError::Recovery(RecoveryFailure::Source { violations }))
            if violations.contains(&zakura_header_chain::AuditViolation::Finality)
    ));
}

fn legacy_rejected_aux_bytes(delivery: AuxDelivery, evidence: [u8; 32]) -> Vec<u8> {
    let mut bytes = delivery
        .encode()
        .expect("the base auxiliary delivery encodes");
    *bytes
        .last_mut()
        .expect("the encoded delivery ends in its status code") = 2;
    bytes.extend(evidence);
    bytes
}

fn mark_metadata_as_v1(metadata: &EngineMetadata) -> Vec<u8> {
    mark_metadata_as_legacy_without_policy(metadata, 1)
}

fn mark_metadata_as_v2(metadata: &EngineMetadata) -> Vec<u8> {
    mark_metadata_as_legacy_without_policy(metadata, 2)
}

fn mark_metadata_as_v3(metadata: &EngineMetadata) -> Vec<u8> {
    let mut bytes = metadata.encode().expect("the metadata fixture encodes");
    bytes[..4].copy_from_slice(&3_u32.to_be_bytes());
    bytes
}

fn mark_metadata_as_legacy_without_policy(metadata: &EngineMetadata, version: u32) -> Vec<u8> {
    let mut bytes = metadata.encode().expect("the metadata fixture encodes");
    bytes[..4].copy_from_slice(&version.to_be_bytes());
    // Version one and two wrote the network kind but no network policy digest.
    bytes.drain(6..38);
    bytes
}

fn v1_bounded_rule(rule: &str) -> Vec<u8> {
    let rule_bytes = rule.as_bytes();
    let mut bytes = Vec::with_capacity(4 + rule_bytes.len());
    bytes.extend(
        u32::try_from(rule_bytes.len())
            .expect("fixture rule IDs fit u32")
            .to_be_bytes(),
    );
    bytes.extend(rule_bytes);
    bytes
}

fn v1_consensus_invalid_tombstone_bytes(
    hash: block::Hash,
    evidence: EvidenceId,
    rule: &str,
) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.push(1);
    bytes.extend(hash.0);
    bytes.extend(evidence.digest());
    bytes.extend(v1_bounded_rule(rule));
    bytes
}

fn v1_consensus_invalid_authority_bytes(
    hash: block::Hash,
    evidence: EvidenceId,
    rule: &str,
) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.push(1);
    bytes.push(1);
    bytes.extend(hash.0);
    bytes.extend(evidence.digest());
    bytes.extend(v1_bounded_rule(rule));
    bytes
}

#[test]
fn rocksdb_snapshot_stops_at_the_first_extra_row_without_decoding() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor, metadata) = fixture();
    let store = HeaderChainStore::new(open(&db_config, engine_config.network()));
    store
        .initialize(metadata, anchor)
        .expect("the valid anchor initializes the fixture");
    let cf = store
        .cf(HEADER_NODE_BY_HASH)
        .expect("the node column family exists");
    store
        .db
        .put_cf(&cf, [0xff; 32], b"intentionally malformed")
        .expect("the malformed extra row writes");

    let audit = store.audit_snapshot().expect("the audit snapshot opens");
    let mut decoded = 0;
    assert_eq!(
        audit.visit_header_nodes(RowLimit::new(1), &mut |_| {
            decoded += 1;
            Ok(())
        }),
        Err(StoreError::LimitExceeded {
            collection: StoreCollection::HeaderNodes,
            limit: RowLimit::new(1),
        })
    );
    assert_eq!(decoded, 1);
}

#[test]
fn version_one_migration_downgrades_legacy_verdicts_atomically() {
    let db_config = Config::ephemeral();
    let (engine_config, mut anchor, mut metadata) = mainnet_fixture();
    metadata.last_transition = Some(zakura_header_chain::TransitionFingerprint::from_parts(
        zakura_header_chain::TransitionDomain::AuxEvidence,
        EvidenceId::from_digest([0x30; 32]),
        [0x30; 32],
    ));
    let previous_state_version = metadata.state_version;
    let delivery = AuxDelivery::new(
        EvidenceId::from_digest([0x31; 32]),
        anchor.hash,
        SourceId::from_digest([0x32; 32]),
        header_owner(&metadata.snapshot(), anchor.hash, 1, 1),
        zakura_header_chain::BodySizeHint::Unknown,
        None,
    );
    anchor.body_validation_state = BodyValidationState::Verified {
        evidence: EvidenceId::from_digest([0x34; 32]),
    };
    anchor.aux_delivery_ids.push(delivery.delivery_id);
    let db = open(&db_config, engine_config.network());
    let store = HeaderChainStore::new(db.clone());
    store
        .initialize(metadata.clone(), anchor.clone())
        .expect("the current fixture initializes");

    let delivery_key = HeaderAuxDeliveryKey {
        header: delivery.header_hash,
        delivery: delivery.delivery_id,
    }
    .as_bytes();
    let delivery_value = legacy_rejected_aux_bytes(delivery, [0x33; 32]);
    let mut batch = DiskWriteBatch::new();
    store
        .put_raw(
            &mut batch,
            HEADER_AUX_DELIVERY,
            delivery_key,
            &delivery_value,
        )
        .expect("the legacy auxiliary row stages");
    store
        .put_raw(
            &mut batch,
            HEADER_ENGINE_META,
            METADATA_KEY,
            mark_metadata_as_v1(&metadata),
        )
        .expect("the version-one metadata stages");
    let authority_cf = store
        .cf(HEADER_BODY_EVIDENCE_AUTHORITY)
        .expect("the body-evidence authority column family exists");
    let mut authority_value = store
        .db
        .raw_get_cf(&authority_cf, &anchor.hash.0)
        .expect("the body-evidence authority is readable")
        .expect("verified full state writes body-evidence authority");
    authority_value[0] = 1;
    store
        .put_raw(
            &mut batch,
            HEADER_BODY_EVIDENCE_AUTHORITY,
            anchor.hash.0,
            authority_value,
        )
        .expect("the version-one body-evidence authority stages");
    store
        .delete_raw(&mut batch, HEADER_ENGINE_META, TOMBSTONE_COUNT_KEY)
        .expect("the version-one fixture omits the current tombstone count");
    store
        .delete_raw(&mut batch, HEADER_ENGINE_META, FINALITY_HISTORY_COUNT_KEY)
        .expect("the version-one fixture omits the current finality count");
    stage_full_state_canonical_hash(&store, &mut batch, metadata.frontiers.finalized);
    store.db.write(batch).expect("the legacy fixture commits");

    assert!(store
        .is_initialized()
        .expect("released version-one metadata identifies an initialized store"));
    assert!(store
        .migrate_to_current(&engine_config)
        .expect("the version-one store migrates"));
    let migrated_metadata = store.metadata().expect("the metadata remains readable");
    assert_eq!(
        migrated_metadata.disk_format,
        HeaderChainDiskVersion::CURRENT
    );
    assert_eq!(
        migrated_metadata.state_version,
        previous_state_version
            .checked_next()
            .expect("the fixture state version can advance")
    );
    assert_eq!(migrated_metadata.last_transition, None);
    assert_eq!(
        store
            .get_value::<HeaderRowCountDisk>(HEADER_ENGINE_META, TOMBSTONE_COUNT_KEY)
            .expect("the migrated tombstone count is readable"),
        Some(HeaderRowCountDisk(0))
    );
    assert_eq!(
        store
            .get_value::<HeaderRowCountDisk>(HEADER_ENGINE_META, FINALITY_HISTORY_COUNT_KEY)
            .expect("the migrated finality count is readable"),
        Some(HeaderRowCountDisk(1))
    );
    assert!(!store
        .migrate_to_current(&engine_config)
        .expect("a repeated migration is a no-op"));
    assert_eq!(
        store
            .load_aux_deliveries()
            .expect("the legacy delivery is readable"),
        vec![UntrustedAuxDeliveryRow::new(
            delivery,
            0,
            [None, None],
            None,
        )]
    );
    let (_, report) = store
        .clone()
        .startup(&engine_config)
        .expect("startup audits the migrated store");
    assert!(report.publication_allowed);
    let aux_cf = store
        .cf(HEADER_AUX_DELIVERY)
        .expect("the auxiliary column family exists");
    let migrated_value = store
        .db
        .raw_get_cf(&aux_cf, &delivery_key)
        .expect("the migrated row is readable")
        .expect("the migrated row exists");
    assert_eq!(AuxDelivery::decode(&migrated_value), Ok(delivery));
    assert_ne!(migrated_value, delivery_value);
    assert!(!HeaderChainStore::new(db)
        .migrate_to_current(&engine_config)
        .expect("reopening the migrated store is a no-op"));
}

#[test]
fn version_one_migration_drops_pruned_consensus_invalid_rows() {
    let db_config = Config::ephemeral();
    let (mut engine_config, anchor, metadata) = mainnet_fixture();
    engine_config.limits.max_non_finalized_nodes = NonZeroUsize::new(1).expect("one is nonzero");
    let store = HeaderChainStore::new(open(&db_config, engine_config.network()));
    store
        .initialize(metadata.clone(), anchor)
        .expect("the current fixture initializes");

    let pruned = block::Hash([0xab; 32]);
    let evidence = EvidenceId::from_digest([0xcd; 32]);
    let rule = "body-consensus-invalid";
    let mut batch = DiskWriteBatch::new();
    store
        .put_raw(
            &mut batch,
            HEADER_ENGINE_META,
            METADATA_KEY,
            mark_metadata_as_v1(&metadata),
        )
        .expect("the version-one metadata stages");
    store
        .put_raw(
            &mut batch,
            HEADER_CONSENSUS_INVALID_BODY_TOMBSTONE,
            pruned.0,
            v1_consensus_invalid_tombstone_bytes(pruned, evidence, rule),
        )
        .expect("the version-one pruned tombstone stages");
    store
        .put_raw(
            &mut batch,
            HEADER_BODY_EVIDENCE_AUTHORITY,
            pruned.0,
            v1_consensus_invalid_authority_bytes(pruned, evidence, rule),
        )
        .expect("the version-one pruned authority stages");
    store
        .delete_raw(&mut batch, HEADER_ENGINE_META, TOMBSTONE_COUNT_KEY)
        .expect("the version-one fixture omits the current tombstone count");
    store
        .delete_raw(&mut batch, HEADER_ENGINE_META, FINALITY_HISTORY_COUNT_KEY)
        .expect("the version-one fixture omits the current finality count");
    stage_full_state_canonical_hash(&store, &mut batch, metadata.frontiers.finalized);
    store
        .db
        .write(batch)
        .expect("the pruned v1 fixture commits");

    assert!(store
        .header_node(pruned)
        .expect("a missing pruned header is readable")
        .is_none());
    assert!(store
        .migrate_to_current(&engine_config)
        .expect("the version-one store migrates without the pruned header node"));

    let tombstone_cf = store
        .cf(HEADER_CONSENSUS_INVALID_BODY_TOMBSTONE)
        .expect("the tombstone column family exists");
    assert!(store
        .db
        .raw_get_cf(&tombstone_cf, &pruned.0)
        .expect("the pruned tombstone family is readable")
        .is_none());
    let authority_cf = store
        .cf(HEADER_BODY_EVIDENCE_AUTHORITY)
        .expect("the body-evidence authority column family exists");
    assert!(store
        .db
        .raw_get_cf(&authority_cf, &pruned.0)
        .expect("the pruned authority family is readable")
        .is_none());
    assert_eq!(
        store
            .get_value::<HeaderRowCountDisk>(HEADER_ENGINE_META, TOMBSTONE_COUNT_KEY)
            .expect("the migrated tombstone count is readable"),
        Some(HeaderRowCountDisk(0))
    );

    let (_, report) = store
        .startup(&engine_config)
        .expect("startup audits the store after dropping pruned v1 invalid-body rows");
    assert!(report.publication_allowed);
}

#[test]
fn version_one_migration_limit_leaves_every_row_unchanged() {
    let db_config = Config::ephemeral();
    let (mut engine_config, mut anchor, metadata) = mainnet_fixture();
    engine_config.limits.max_aux_deliveries_total = NonZeroUsize::new(1).expect("one is nonzero");
    let deliveries = [0x41, 0x42].map(|marker| {
        AuxDelivery::new(
            EvidenceId::from_digest([marker; 32]),
            anchor.hash,
            SourceId::from_digest([marker.wrapping_add(1); 32]),
            header_owner(&metadata.snapshot(), anchor.hash, u64::from(marker), 1),
            zakura_header_chain::BodySizeHint::Unknown,
            None,
        )
    });
    anchor
        .aux_delivery_ids
        .extend(deliveries.iter().map(|delivery| delivery.delivery_id));
    let db = open(&db_config, engine_config.network());
    let store = HeaderChainStore::new(db);
    store
        .initialize(metadata.clone(), anchor)
        .expect("the current fixture initializes");

    let first_key = HeaderAuxDeliveryKey {
        header: deliveries[0].header_hash,
        delivery: deliveries[0].delivery_id,
    }
    .as_bytes();
    let second_key = HeaderAuxDeliveryKey {
        header: deliveries[1].header_hash,
        delivery: deliveries[1].delivery_id,
    }
    .as_bytes();
    let first_value = legacy_rejected_aux_bytes(deliveries[0], [0x51; 32]);
    let second_value = legacy_rejected_aux_bytes(deliveries[1], [0x52; 32]);
    let metadata_value = mark_metadata_as_v1(&metadata);
    let mut batch = DiskWriteBatch::new();
    for (key, value) in [
        (first_key, first_value.clone()),
        (second_key, second_value.clone()),
    ] {
        store
            .put_raw(&mut batch, HEADER_AUX_DELIVERY, key, value)
            .expect("the legacy auxiliary row stages");
    }
    store
        .put_raw(
            &mut batch,
            HEADER_ENGINE_META,
            METADATA_KEY,
            &metadata_value,
        )
        .expect("the legacy metadata row stages");
    stage_full_state_canonical_hash(&store, &mut batch, metadata.frontiers.finalized);
    store.db.write(batch).expect("the legacy fixture commits");

    assert!(matches!(
        store.migrate_to_current(&engine_config),
        Err(HeaderChainStoreError::Store(StoreError::LimitExceeded {
            collection: StoreCollection::AuxiliaryDeliveries,
            limit,
        })) if limit == RowLimit::new(1)
    ));
    let aux_cf = store
        .cf(HEADER_AUX_DELIVERY)
        .expect("the auxiliary column family exists");
    let metadata_cf = store
        .cf(HEADER_ENGINE_META)
        .expect("the metadata column family exists");
    assert_eq!(
        store
            .db
            .raw_get_cf(&aux_cf, &first_key)
            .expect("the first row is readable"),
        Some(first_value)
    );
    assert_eq!(
        store
            .db
            .raw_get_cf(&aux_cf, &second_key)
            .expect("the second row is readable"),
        Some(second_value)
    );
    assert_eq!(
        store
            .db
            .raw_get_cf(&metadata_cf, METADATA_KEY)
            .expect("the metadata row is readable"),
        Some(metadata_value)
    );
}

#[test]
fn version_one_migration_rejects_an_ambiguous_network_policy_without_writing() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor, metadata) = fixture();
    let changed_network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            canopy: Some(10),
            ..Default::default()
        },
        ..Default::default()
    });
    let changed_config = EngineConfig::new(
        engine_config.mode,
        changed_network,
        engine_config.bootstrap_anchor().clone(),
        CheckpointSet::default(),
    )
    .expect("the changed policy accepts the same bootstrap anchor");
    assert_eq!(
        engine_config.network().kind(),
        changed_config.network().kind()
    );
    assert_eq!(
        engine_config.trust_anchor_digest(),
        changed_config.trust_anchor_digest()
    );
    assert_ne!(
        engine_config.network_policy_digest(),
        changed_config.network_policy_digest()
    );
    let metadata_value = mark_metadata_as_v1(&metadata);
    let db = open(&db_config, engine_config.network());
    let store = HeaderChainStore::new(db);
    store
        .initialize(metadata, anchor)
        .expect("the current fixture initializes");
    let mut batch = DiskWriteBatch::new();
    store
        .put_raw(
            &mut batch,
            HEADER_ENGINE_META,
            METADATA_KEY,
            &metadata_value,
        )
        .expect("the version-one metadata stages");
    store.db.write(batch).expect("the legacy fixture commits");

    assert!(matches!(
        store.migrate_to_current(&changed_config),
        Err(HeaderChainStoreError::Incoherent(
            "version-one network policy is ambiguous; rebuild the header-chain database"
        ))
    ));
    let metadata_cf = store
        .cf(HEADER_ENGINE_META)
        .expect("the metadata column family exists");
    assert_eq!(
        store
            .db
            .raw_get_cf(&metadata_cf, METADATA_KEY)
            .expect("the metadata row is readable"),
        Some(metadata_value)
    );
}

#[test]
fn version_two_migration_injects_network_policy_and_leaves_current_aux_unchanged() {
    let db_config = Config::ephemeral();
    let (engine_config, mut anchor, metadata) = mainnet_fixture();
    let previous_state_version = metadata.state_version;
    let delivery = AuxDelivery::new(
        EvidenceId::from_digest([0x21; 32]),
        anchor.hash,
        SourceId::from_digest([0x22; 32]),
        header_owner(&metadata.snapshot(), anchor.hash, 3, 1),
        zakura_header_chain::BodySizeHint::Unknown,
        None,
    );
    anchor.aux_delivery_ids.push(delivery.delivery_id);
    let db = open(&db_config, engine_config.network());
    let store = HeaderChainStore::new(db.clone());
    store
        .initialize(metadata.clone(), anchor.clone())
        .expect("the current fixture initializes");

    let delivery_key = HeaderAuxDeliveryKey {
        header: delivery.header_hash,
        delivery: delivery.delivery_id,
    }
    .as_bytes();
    let current_aux = delivery
        .encode()
        .expect("the current auxiliary fixture encodes");
    let mut batch = DiskWriteBatch::new();
    store
        .put_raw(&mut batch, HEADER_AUX_DELIVERY, delivery_key, &current_aux)
        .expect("the current auxiliary row stages");
    store
        .put_raw(
            &mut batch,
            HEADER_ENGINE_META,
            METADATA_KEY,
            mark_metadata_as_v2(&metadata),
        )
        .expect("the version-two metadata stages");
    stage_full_state_canonical_hash(&store, &mut batch, metadata.frontiers.finalized);
    store
        .db
        .write(batch)
        .expect("the version-two fixture commits");
    let aux_cf = store
        .cf(HEADER_AUX_DELIVERY)
        .expect("the auxiliary column family exists");

    assert!(store
        .is_initialized()
        .expect("released version-two metadata identifies an initialized store"));
    assert!(store
        .migrate_to_current(&engine_config)
        .expect("the version-two store migrates"));
    let migrated_metadata = store.metadata().expect("the metadata remains readable");
    assert_eq!(
        migrated_metadata.disk_format,
        HeaderChainDiskVersion::CURRENT
    );
    assert_eq!(
        migrated_metadata.network_policy_digest,
        engine_config.network_policy_digest()
    );
    assert_eq!(
        migrated_metadata.state_version,
        previous_state_version
            .checked_next()
            .expect("the fixture state version can advance")
    );
    assert_eq!(migrated_metadata.last_transition, None);
    assert_eq!(
        store
            .db
            .raw_get_cf(&aux_cf, &delivery_key)
            .expect("the auxiliary row stays readable"),
        Some(current_aux)
    );
    let (_, report) = store
        .clone()
        .startup(&engine_config)
        .expect("startup audits the migrated version-two store");
    assert!(report.publication_allowed);
    assert!(!HeaderChainStore::new(db)
        .migrate_to_current(&engine_config)
        .expect("reopening the migrated store is a no-op"));
}

#[test]
fn version_two_migration_rejects_an_ambiguous_network_policy_without_writing() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor, metadata) = fixture();
    let changed_network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            canopy: Some(10),
            ..Default::default()
        },
        ..Default::default()
    });
    let changed_config = EngineConfig::new(
        engine_config.mode,
        changed_network,
        engine_config.bootstrap_anchor().clone(),
        CheckpointSet::default(),
    )
    .expect("the changed policy accepts the same bootstrap anchor");
    let metadata_value = mark_metadata_as_v2(&metadata);
    let db = open(&db_config, engine_config.network());
    let store = HeaderChainStore::new(db);
    store
        .initialize(metadata, anchor)
        .expect("the current fixture initializes");
    let mut batch = DiskWriteBatch::new();
    store
        .put_raw(
            &mut batch,
            HEADER_ENGINE_META,
            METADATA_KEY,
            &metadata_value,
        )
        .expect("the version-two metadata stages");
    store.db.write(batch).expect("the legacy fixture commits");

    assert!(matches!(
        store.migrate_to_current(&changed_config),
        Err(HeaderChainStoreError::Incoherent(
            "version-two network policy is ambiguous; rebuild the header-chain database"
        ))
    ));
    let metadata_cf = store
        .cf(HEADER_ENGINE_META)
        .expect("the metadata column family exists");
    assert_eq!(
        store
            .db
            .raw_get_cf(&metadata_cf, METADATA_KEY)
            .expect("the metadata row is readable"),
        Some(metadata_value)
    );
}

#[test]
fn version_three_integrated_migration_authenticates_the_full_state_frontier() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor, metadata) = mainnet_fixture();
    let frontier = metadata.frontiers.finalized;
    let previous_state_version = metadata.state_version;
    let db = open(&db_config, engine_config.network());
    let store = HeaderChainStore::new(db.clone());
    store
        .initialize(metadata.clone(), anchor)
        .expect("the current fixture initializes");
    let mut batch = DiskWriteBatch::new();
    store
        .put_raw(
            &mut batch,
            HEADER_ENGINE_META,
            METADATA_KEY,
            mark_metadata_as_v3(&metadata),
        )
        .expect("the version-three metadata stages");
    stage_full_state_canonical_hash(&store, &mut batch, frontier);
    store.db.write(batch).expect("the legacy fixture commits");

    assert!(store
        .migrate_to_current(&engine_config)
        .expect("the version-three store migrates"));
    let migrated = store.metadata().expect("the migrated metadata is readable");
    assert_eq!(migrated.disk_format, HeaderChainDiskVersion::CURRENT);
    assert_eq!(
        migrated.state_version,
        previous_state_version
            .checked_next()
            .expect("the fixture state version advances")
    );
    assert_eq!(
        store.finality_history().expect("history is readable"),
        vec![FinalityRecord {
            previous: frontier,
            current: frontier,
            source: FinalitySource::DiskMigration {
                from_version: HeaderChainDiskVersion(3),
                network_policy_digest: engine_config.network_policy_digest(),
                authentication: zakura_header_chain::DiskMigrationAuthentication::FullState,
            },
            epoch: metadata.finality_epoch,
        }]
    );
    let (_, report) = HeaderChainStore::new(db)
        .startup(&engine_config)
        .expect("startup audits the migrated store");
    assert!(report.publication_allowed);
}

#[test]
fn version_three_migration_rejects_a_network_policy_mismatch_atomically() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor, mut metadata) = mainnet_fixture();
    metadata.network_policy_digest = [0x73; 32];
    let metadata_value = mark_metadata_as_v3(&metadata);
    let db = open(&db_config, engine_config.network());
    let store = HeaderChainStore::new(db);
    store
        .initialize(
            EngineMetadata {
                disk_format: HeaderChainDiskVersion::CURRENT,
                network_policy_digest: engine_config.network_policy_digest(),
                ..metadata.clone()
            },
            anchor,
        )
        .expect("the current fixture initializes");
    let mut batch = DiskWriteBatch::new();
    store
        .put_raw(
            &mut batch,
            HEADER_ENGINE_META,
            METADATA_KEY,
            &metadata_value,
        )
        .expect("the mismatched version-three metadata stages");
    store.db.write(batch).expect("the legacy fixture commits");

    assert!(matches!(
        store.migrate_to_current(&engine_config),
        Err(HeaderChainStoreError::Incoherent(
            "legacy network policy does not match the configured policy"
        ))
    ));
    let metadata_cf = store
        .cf(HEADER_ENGINE_META)
        .expect("the metadata column family exists");
    assert_eq!(
        store
            .db
            .raw_get_cf(&metadata_cf, METADATA_KEY)
            .expect("the metadata remains readable"),
        Some(metadata_value)
    );
}

#[test]
fn every_legacy_headers_only_version_migrates_with_a_complete_depth_proof() {
    for version in 1_u32..=3 {
        let db_config = Config::ephemeral();
        let (mut engine_config, anchor, mut metadata) = mainnet_fixture();
        engine_config.mode = EngineMode::HeadersOnly;
        engine_config.limits.local_finality_depth =
            std::num::NonZeroU32::new(1).expect("one is nonzero");
        metadata.mode = EngineMode::HeadersOnly;
        let db = open(&db_config, engine_config.network());
        let store = HeaderChainStore::new(db.clone());
        store
            .initialize(metadata, anchor.clone())
            .expect("the headers-only fixture initializes");
        let (runtime, _) = store
            .startup(&engine_config)
            .expect("the headers-only fixture audits");
        let before = runtime.publisher().snapshot();
        let lease = runtime
            .reader()
            .validation_context(anchor.hash)
            .expect("the anchor context is coherent")
            .expect("the anchor is retained");
        let rules =
            HeaderRules::for_validation_lease(&lease).expect("the Mainnet rules are coherent");
        let blocks = [
            zakura_test::vectors::BLOCK_MAINNET_1_BYTES.as_slice(),
            zakura_test::vectors::BLOCK_MAINNET_2_BYTES.as_slice(),
        ]
        .map(|bytes| {
            bytes
                .zcash_deserialize_into::<Arc<block::Block>>()
                .expect("the Mainnet block fixture deserializes")
        });
        let headers = blocks.map(|block| block.header.clone());
        let selected_tip = Frontier::new(block::Height(2), headers[1].hash());
        let prepared = zakura_header_chain::prepare_headers(
            HeaderBatchInput::new(&headers),
            lease.parent(),
            &rules,
            &SystemClock,
        )
        .expect("the Mainnet depth proof prepares");
        runtime
            .apply(
                TransitionRequest {
                    expected_version: before.state_version,
                    event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                        owner: header_owner(&before, selected_tip.hash, 0x81, 0x82),
                        source: SourceId::from_digest([0x83; 32]),
                        parent_hash: anchor.hash,
                        target_tip_hash: selected_tip.hash,
                        completion: TargetCompletion::TargetComplete {
                            common_ancestor: Frontier::new(anchor.height, anchor.hash),
                        },
                        batch: prepared,
                        aux: Vec::new(),
                    })),
                },
                &TransitionContext {
                    config: &engine_config,
                    clock: &SystemClock,
                    full_state_authority: None,
                    retention_references: &[],
                },
            )
            .expect("the headers-only depth transition commits");
        let legacy_metadata = runtime.store.metadata().expect("metadata is readable");
        let finalized = legacy_metadata.frontiers.finalized;
        assert_eq!(finalized.height, block::Height(1));
        let metadata_value = match version {
            1 => mark_metadata_as_v1(&legacy_metadata),
            2 => mark_metadata_as_v2(&legacy_metadata),
            3 => mark_metadata_as_v3(&legacy_metadata),
            _ => unreachable!("the loop covers released legacy versions"),
        };
        let mut downgrade = DiskWriteBatch::new();
        runtime
            .store
            .put_raw(
                &mut downgrade,
                HEADER_ENGINE_META,
                METADATA_KEY,
                metadata_value,
            )
            .expect("the legacy metadata stages");
        stage_full_state_canonical_hash(&runtime.store, &mut downgrade, finalized);
        runtime
            .store
            .db
            .write(downgrade)
            .expect("the legacy fixture commits");
        let migrated_store = runtime.store.clone();
        drop(runtime);

        assert!(migrated_store
            .migrate_to_current(&engine_config)
            .expect("the headers-only store migrates"));
        let history = migrated_store
            .finality_history()
            .expect("the migration history is readable");
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].previous, finalized);
        assert_eq!(history[0].current, finalized);
        assert!(matches!(
            history[0].source,
            FinalitySource::DiskMigration {
                from_version,
                authentication:
                    zakura_header_chain::DiskMigrationAuthentication::HeadersOnlyDepth {
                        selected_tip: tip,
                    },
                ..
            } if from_version == HeaderChainDiskVersion(version) && tip == selected_tip
        ));
        let (reopened, report) = HeaderChainStore::new(db)
            .startup(&engine_config)
            .expect("startup audits the migrated headers-only store");
        assert!(report.publication_allowed);

        let before = reopened.publisher().snapshot();
        let lease = reopened
            .reader()
            .validation_context(selected_tip.hash)
            .expect("the selected-tip context is coherent")
            .expect("the selected tip is retained");
        let rules =
            HeaderRules::for_validation_lease(&lease).expect("the Mainnet rules remain coherent");
        let block_three = zakura_test::vectors::BLOCK_MAINNET_3_BYTES
            .as_slice()
            .zcash_deserialize_into::<Arc<block::Block>>()
            .expect("Mainnet block three deserializes");
        let next_tip = Frontier::new(block::Height(3), block_three.hash());
        let prepared = zakura_header_chain::prepare_headers(
            HeaderBatchInput::new(std::slice::from_ref(&block_three.header)),
            lease.parent(),
            &rules,
            &SystemClock,
        )
        .expect("the ordinary extension prepares");
        reopened
            .apply(
                TransitionRequest {
                    expected_version: before.state_version,
                    event: TransitionEvent::InsertHeaders(Box::new(InsertHeaders {
                        owner: header_owner(&before, next_tip.hash, 0x84, 0x85),
                        source: SourceId::from_digest([0x86; 32]),
                        parent_hash: selected_tip.hash,
                        target_tip_hash: next_tip.hash,
                        completion: TargetCompletion::TargetComplete {
                            common_ancestor: selected_tip,
                        },
                        batch: prepared,
                        aux: Vec::new(),
                    })),
                },
                &TransitionContext {
                    config: &engine_config,
                    clock: &SystemClock,
                    full_state_authority: None,
                    retention_references: &[],
                },
            )
            .expect("the ordinary extension commits");
        assert_eq!(
            reopened
                .store
                .get_value::<HeaderRowCountDisk>(HEADER_ENGINE_META, FINALITY_WITNESS_COUNT_KEY,)
                .expect("the witness count is readable"),
            Some(HeaderRowCountDisk(2)),
            "one ordinary extension adds one witness node"
        );
    }
}

#[test]
fn startup_atomically_rebinds_an_extended_checkpoint_manifest() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor, metadata) = fixture();
    let previous_state_version = metadata.state_version;
    let updated_config = EngineConfig::new(
        engine_config.mode,
        engine_config.network().clone(),
        engine_config.bootstrap_anchor().clone(),
        CheckpointSet::new([Frontier::new(block::Height(10), block::Hash([0x93; 32]))])
            .expect("the extension checkpoint is unique"),
    )
    .expect("the updated engine configuration is coherent");
    let db = open(&db_config, engine_config.network());
    let store = HeaderChainStore::new(db.clone());
    store
        .initialize(metadata, anchor)
        .expect("the old manifest initializes the fixture");

    let (runtime, report) = store
        .startup(&updated_config)
        .expect("startup rebinds a fully audited checkpoint extension");
    assert_eq!(
        report.repairs,
        BTreeSet::from([RecoveryRepair::TrustAnchorConfiguration])
    );
    assert_eq!(
        report.current.state_version,
        previous_state_version
            .checked_next()
            .expect("the fixture state version can advance")
    );
    drop(runtime);

    let (_, reopened) = HeaderChainStore::new(db)
        .startup(&updated_config)
        .expect("the rebound manifest persists atomically");
    assert!(reopened.repairs.is_empty());
}

#[test]
fn startup_reconciles_restored_full_state_before_first_publication() {
    let cache = tempfile::tempdir().expect("the test cache directory is created");
    let db_config = Config {
        cache_dir: cache.path().to_owned(),
        ephemeral: false,
        debug_skip_non_finalized_state_backup_task: true,
        ..Config::default()
    };
    let (engine_config, anchor, metadata) = fixture();
    let anchor_frontier = Frontier::new(anchor.height, anchor.hash);
    let db = open(&db_config, engine_config.network());
    let store = HeaderChainStore::new(db.clone());
    store
        .initialize(metadata, anchor.clone())
        .expect("the header schema initializes");

    let mut child_header = *anchor.header;
    child_header.previous_block_hash = anchor.hash;
    child_header.time += chrono::Duration::seconds(1);
    let child_header = Arc::new(child_header);
    let child = VerifiedHeaderRef {
        height: anchor
            .height
            .next()
            .expect("genesis has a successor height"),
        hash: child_header.hash(),
        header: child_header,
    };
    let (runtime, report) = store
        .startup_reconciled(
            &engine_config,
            anchor_frontier,
            Vec::new(),
            vec![child.clone()],
        )
        .expect("restored full state reconciles before publication");

    assert!(report.publication_allowed);
    assert_eq!(
        runtime.publisher().snapshot().frontiers.verified_best,
        Frontier::new(child.height, child.hash)
    );
    assert_eq!(
        runtime
            .verified_projection()
            .expect("projection is readable"),
        vec![anchor_frontier, Frontier::new(child.height, child.hash)]
    );
    assert!(matches!(
        runtime.store.header_node(child.hash),
        Ok(Some(HeaderNode {
            body_validation_state: BodyValidationState::Verified { .. },
            ..
        }))
    ));

    drop(runtime);
    let reopened = HeaderChainStore::new(db.clone())
        .startup_reconciled(&engine_config, anchor_frontier, Vec::new(), Vec::new())
        .expect("a restart resets a committed-but-unrestored verified suffix")
        .0;
    assert_eq!(
        reopened.publisher().snapshot().frontiers.verified_best,
        anchor_frontier
    );
    assert_eq!(
        reopened
            .verified_projection()
            .expect("reset projection is readable"),
        vec![anchor_frontier]
    );

    drop(reopened);
    let finalized_child = Frontier::new(child.height, child.hash);
    let advanced_store = HeaderChainStore::new(db);
    let mut full_state_batch = DiskWriteBatch::new();
    stage_full_state_canonical_hash(&advanced_store, &mut full_state_batch, anchor_frontier);
    stage_full_state_canonical_hash(&advanced_store, &mut full_state_batch, finalized_child);
    advanced_store
        .db
        .write(full_state_batch)
        .expect("the full-state canonical child commits");
    let advanced = advanced_store
        .startup_reconciled(&engine_config, finalized_child, vec![child], Vec::new())
        .expect("a dark checkpoint gap is reconciled and finalized before publication")
        .0;
    let snapshot = advanced.publisher().snapshot();
    assert_eq!(snapshot.frontiers.finalized, finalized_child);
    assert_eq!(snapshot.frontiers.verified_best, finalized_child);
    assert_eq!(
        advanced
            .verified_projection()
            .expect("advanced projection is readable"),
        vec![finalized_child]
    );
}

#[test]
fn startup_reconciliation_chunks_finalized_gaps_at_the_node_limit() {
    let db_config = Config::ephemeral();
    let (mut engine_config, anchor, metadata) = fixture();
    engine_config.limits.max_non_finalized_nodes = NonZeroUsize::new(2).expect("two is nonzero");
    let store = HeaderChainStore::new(open(&db_config, engine_config.network()));
    store
        .initialize(metadata, anchor.clone())
        .expect("the header schema initializes");

    let mut path = Vec::new();
    let mut parent = Frontier::new(anchor.height, anchor.hash);
    let mut parent_header = anchor.header;
    for _ in 0..5 {
        let mut header = *parent_header;
        header.previous_block_hash = parent.hash;
        header.time += chrono::Duration::seconds(1);
        let header = Arc::new(header);
        let height = parent
            .height
            .next()
            .expect("the five-header fixture stays in range");
        let hash = header.hash();
        path.push(VerifiedHeaderRef {
            height,
            hash,
            header: header.clone(),
        });
        parent = Frontier::new(height, hash);
        parent_header = header;
    }

    let mut full_state_batch = DiskWriteBatch::new();
    stage_full_state_canonical_hash(
        &store,
        &mut full_state_batch,
        Frontier::new(anchor.height, anchor.hash),
    );
    for header in &path {
        stage_full_state_canonical_hash(
            &store,
            &mut full_state_batch,
            Frontier::new(header.height, header.hash),
        );
    }
    store
        .db
        .write(full_state_batch)
        .expect("the canonical full-state path commits");

    let (runtime, report) = store
        .startup_reconciled(&engine_config, parent, path, Vec::new())
        .expect("an oversized finalized gap reconciles in bounded chunks");

    assert!(report.publication_allowed);
    assert_eq!(runtime.publisher().snapshot().frontiers.finalized, parent);
    assert_eq!(
        runtime
            .verified_projection()
            .expect("the final projection is readable"),
        vec![parent]
    );
    assert_eq!(
        runtime
            .store
            .load_header_nodes()
            .expect("the retained nodes are readable")
            .len(),
        1,
        "each bounded chunk is finalized before admitting the next"
    );
    assert_eq!(
        runtime
            .store
            .finality_history()
            .expect("chunk finality history is readable")
            .len(),
        4
    );
}

#[test]
fn streaming_reconstruction_resumes_from_the_last_atomic_chunk() {
    let db_config = Config::ephemeral();
    let (mut engine_config, anchor, metadata) = fixture();
    engine_config.limits.max_non_finalized_nodes = NonZeroUsize::new(2).expect("two is nonzero");
    let store = HeaderChainStore::new(open(&db_config, engine_config.network()));
    store
        .initialize(metadata, anchor.clone())
        .expect("the header schema initializes");

    let mut path = Vec::new();
    let mut parent = Frontier::new(anchor.height, anchor.hash);
    let mut parent_header = anchor.header;
    for _ in 0..5 {
        let mut header = *parent_header;
        header.previous_block_hash = parent.hash;
        header.time += chrono::Duration::seconds(1);
        let header = Arc::new(header);
        let height = parent
            .height
            .next()
            .expect("the bounded fixture height has a successor");
        let hash = header.hash();
        path.push(VerifiedHeaderRef {
            height,
            hash,
            header: header.clone(),
        });
        parent = Frontier::new(height, hash);
        parent_header = header;
    }

    let mut full_state_batch = DiskWriteBatch::new();
    stage_full_state_canonical_hash(
        &store,
        &mut full_state_batch,
        Frontier::new(anchor.height, anchor.hash),
    );
    for header in &path {
        stage_full_state_canonical_hash(
            &store,
            &mut full_state_batch,
            Frontier::new(header.height, header.hash),
        );
    }
    store
        .db
        .write(full_state_batch)
        .expect("the canonical full-state path commits");

    let first_attempt = store.clone().startup_reconciled_streaming(
        &engine_config,
        parent,
        Vec::new(),
        |height| {
            if height == block::Height(3) {
                return Err(HeaderChainStoreError::MissingCanonicalHeader(height));
            }
            Ok(path[usize::try_from(height.0 - 1).expect("fixture index fits")].clone())
        },
        |_| {},
    );
    assert!(matches!(
        first_attempt,
        Err(HeaderChainStoreError::MissingCanonicalHeader(
            block::Height(3)
        ))
    ));
    assert_eq!(
        store
            .snapshot()
            .expect("the first chunk snapshot is readable")
            .frontiers
            .finalized,
        Frontier::new(path[1].height, path[1].hash)
    );
    let durable_progress = store
        .reconstruction_progress()
        .expect("the restart marker is readable")
        .expect("the interrupted attempt retains a marker");
    assert_eq!(
        durable_progress.last_committed,
        Frontier::new(path[1].height, path[1].hash)
    );
    assert_eq!(durable_progress.next_height, block::Height(3));

    let requested = std::cell::RefCell::new(Vec::new());
    let (runtime, report) = store
        .startup_reconciled_streaming(
            &engine_config,
            parent,
            Vec::new(),
            |height| {
                requested.borrow_mut().push(height);
                Ok(path[usize::try_from(height.0 - 1).expect("fixture index fits")].clone())
            },
            |_| {},
        )
        .expect("the second attempt resumes from its committed marker");
    assert!(report.publication_allowed);
    assert_eq!(
        requested.into_inner(),
        [block::Height(3), block::Height(4), block::Height(5)]
    );
    assert_eq!(runtime.publisher().snapshot().frontiers.finalized, parent);
    assert_eq!(
        runtime
            .store
            .reconstruction_progress()
            .expect("the completed marker lookup is readable"),
        None,
        "publication is enabled only after the restart marker is removed"
    );
}

#[test]
fn malformed_reconstruction_progress_fails_closed() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor, metadata) = fixture();
    let anchor_frontier = Frontier::new(anchor.height, anchor.hash);
    let store = HeaderChainStore::new(open(&db_config, engine_config.network()));
    store
        .initialize(metadata, anchor)
        .expect("the header schema initializes");
    let mut corrupt = DiskWriteBatch::new();
    store
        .put_raw(
            &mut corrupt,
            HEADER_ENGINE_META,
            RECONSTRUCTION_PROGRESS_KEY,
            [99],
        )
        .expect("the progress row is addressable");
    store
        .db
        .write(corrupt)
        .expect("the malformed marker is durable");

    assert!(matches!(
        store.clone().startup_reconciled_streaming(
            &engine_config,
            anchor_frontier,
            Vec::new(),
            |_| unreachable!("the malformed marker fails before canonical reads"),
            |_| {},
        ),
        Err(HeaderChainStoreError::Codec(
            HeaderChainValueError::UnknownDiscriminant {
                field: "header_reconstruction_version",
                value: 99,
            }
        ))
    ));

    let target = Frontier::new(block::Height(1), block::Hash([0x91; 32]));
    let contradictory = HeaderReconstructionProgressDisk {
        network: engine_config.network().kind(),
        target,
        next_height: block::Height(1),
        phase: HeaderReconstructionPhaseDisk::FinalAudit,
        last_committed: anchor_frontier,
    };
    store
        .write_reconstruction_progress(&contradictory)
        .expect("the semantically contradictory marker encodes");
    assert!(matches!(
        store.startup_reconciled_streaming(
            &engine_config,
            target,
            Vec::new(),
            |_| unreachable!("a terminal phase before its target fails before canonical reads"),
            |_| {},
        ),
        Err(HeaderChainStoreError::Incoherent(
            "terminal header reconstruction phase precedes its target"
        ))
    ));
}

#[test]
fn startup_repairs_every_reconstructible_index_atomically_before_publication() {
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
    let mut corrupt = DiskWriteBatch::new();
    let bogus_parent = block::Hash([0x11; 32]);
    let bogus_child = block::Hash([0x22; 32]);
    let mut child_header = *anchor.header;
    child_header.previous_block_hash = anchor.hash;
    child_header.time += chrono::Duration::seconds(1);
    let child_hash = child_header.hash();
    let child_eligibility = zakura_header_chain::EligibilityState {
        inherited_from: Some(bogus_parent),
        ..Default::default()
    };
    let child = HeaderNode::from_durable_parts(
        Arc::new(child_header),
        child_hash,
        anchor.hash,
        block::Height(1),
        anchor.block_work,
        anchor
            .work_coordinate()
            .checked_add(anchor.block_work)
            .expect("the fixture work coordinate does not overflow"),
        HeaderValidationState::Valid,
        child_eligibility,
        BodyValidationState::Unknown,
        Vec::new(),
    )
    .expect("the child fixture is internally coherent");
    store
        .put_value(
            &mut corrupt,
            HEADER_NODE_BY_HASH,
            child.hash.0,
            &HeaderNodeDisk::from_domain(&child),
        )
        .expect("the child source row encodes");
    store
        .put_empty(
            &mut corrupt,
            HEADER_CHILD,
            HeaderChildKey {
                parent: bogus_parent,
                child: bogus_child,
            }
            .as_bytes(),
        )
        .expect("the child cache accepts the fixture row");
    store
        .delete_raw(
            &mut corrupt,
            HEADER_SELECTED,
            HeaderHeightKey(anchor.height).as_bytes(),
        )
        .expect("the selected cache row is addressable");
    store
        .delete_raw(
            &mut corrupt,
            HEADER_VERIFIED,
            HeaderHeightKey(anchor.height).as_bytes(),
        )
        .expect("the verified cache row is addressable");
    store
        .put_empty(
            &mut corrupt,
            HEADER_DEFERRED,
            HeaderDeferredKey::new(1, 0, bogus_child)
                .expect("the fixture timestamp is valid")
                .as_bytes(),
        )
        .expect("the deferred cache accepts the fixture row");
    let mut corrupt_metadata = metadata.clone();
    corrupt_metadata.oldest_retained_height = block::Height(1);
    store
        .put_value(
            &mut corrupt,
            HEADER_ENGINE_META,
            METADATA_KEY,
            &corrupt_metadata,
        )
        .expect("the fixture metadata encodes");
    db.write(corrupt)
        .expect("the fixture cache corruption is durable");

    let (runtime, report) = store
        .startup(&engine_config)
        .expect("a reconstructible cache is repaired");
    assert_eq!(
        report.repairs,
        BTreeSet::from([
            RecoveryRepair::ChildIndex,
            RecoveryRepair::DeferredIndex,
            RecoveryRepair::SelectedProjection,
            RecoveryRepair::VerifiedProjection,
            RecoveryRepair::InheritedEligibility,
            RecoveryRepair::RetentionMetadata,
        ])
    );
    assert_eq!(report.previous.state_version, StateVersion::new(1));
    assert_eq!(report.current.state_version, StateVersion::new(2));
    assert_eq!(report.current.header_generation, HeaderGeneration::new(2));
    assert_eq!(
        report.current.verified_generation,
        VerifiedGeneration::new(2)
    );
    assert_eq!(report.current.oldest_retained_height, anchor.height);
    assert!(report.publication_allowed);
    assert_eq!(runtime.publisher().snapshot(), report.current);
    assert_eq!(
        runtime.store.selected_hash(anchor.height),
        Ok(Some(anchor.hash))
    );
    assert_eq!(
        runtime.store.selected_hash(child.height),
        Ok(Some(child.hash))
    );
    assert_eq!(
        runtime.store.verified_hash(anchor.height),
        Ok(Some(anchor.hash))
    );
    assert_eq!(
        runtime.store.header_child_edges(),
        Ok(vec![(anchor.hash, child.hash)])
    );
    assert_eq!(runtime.store.deferred_entries(), Ok(Vec::new()));
    assert_eq!(
        runtime
            .store
            .header_node(child.hash)
            .expect("the repaired child decodes")
            .expect("the repaired child remains")
            .eligibility
            .inherited_from,
        None
    );

    drop(runtime);
    drop(db);
    let (reopened, reopened_report) = HeaderChainStore::new(open(&db_config, &network))
        .startup(&engine_config)
        .expect("the atomic repair reopens coherently");
    assert!(reopened_report.repairs.is_empty());
    assert_eq!(reopened.publisher().snapshot(), report.current);
}

#[test]
fn authoritative_corruption_fails_before_publisher_construction() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor, metadata) = fixture();
    let store = HeaderChainStore::new(open(&db_config, engine_config.network()));
    store
        .initialize(metadata, anchor.clone())
        .expect("the empty schema initializes");
    let mut corrupt = DiskWriteBatch::new();
    store
        .delete_raw(&mut corrupt, HEADER_NODE_BY_HASH, anchor.hash.0)
        .expect("the anchor row is addressable");
    store
        .db
        .write(corrupt)
        .expect("the fixture source corruption is durable");

    assert!(matches!(
        store.startup(&engine_config),
        Err(HeaderChainStoreError::Recovery(
            RecoveryFailure::Source { .. }
        ))
    ));
}

#[test]
fn startup_rejects_verified_projection_without_exact_body_authority() {
    let db_config = Config::ephemeral();
    let (engine_config, mut anchor, metadata) = fixture();
    let evidence = EvidenceId::from_digest([0xa5; 32]);
    anchor.body_validation_state = BodyValidationState::Verified { evidence };
    let store = HeaderChainStore::new(open(&db_config, engine_config.network()));
    store
        .initialize(metadata, anchor.clone())
        .expect("the verified fixture initializes with body authority");

    let mut corrupt = DiskWriteBatch::new();
    store
        .delete_raw(&mut corrupt, HEADER_BODY_EVIDENCE_AUTHORITY, anchor.hash.0)
        .expect("the body authority row is addressable");
    store
        .db
        .write(corrupt)
        .expect("the missing authority row is durable");

    assert!(matches!(
        store.startup(&engine_config),
        Err(HeaderChainStoreError::Recovery(RecoveryFailure::Source {
            violations,
        })) if violations.contains(&zakura_header_chain::AuditViolation::BodyValidationEvidenceAuthority(anchor.hash))
    ));
}
