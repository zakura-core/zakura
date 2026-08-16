use super::*;

#[test]
fn startup_atomically_rebinds_an_extended_checkpoint_manifest() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor, metadata) = fixture();
    let previous_state_version = metadata.state_version;
    let updated_config = EngineConfig::new(
        engine_config.mode,
        engine_config.network.clone(),
        engine_config.bootstrap_anchor().clone(),
        CheckpointSet::new([Frontier::new(block::Height(10), block::Hash([0x93; 32]))])
            .expect("the extension checkpoint is unique"),
    )
    .expect("the updated engine configuration is coherent");
    let db = open(&db_config, &engine_config.network);
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
    let db = open(&db_config, &engine_config.network);
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
    let advanced = HeaderChainStore::new(db)
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
    let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
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
            .all_header_nodes()
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
    let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
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
    let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
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
        network: engine_config.network.kind(),
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
    let network = engine_config.network.clone();
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
    let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
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
    let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
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
