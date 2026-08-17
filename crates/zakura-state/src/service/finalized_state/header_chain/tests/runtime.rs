use super::*;

#[test]
fn atomic_finality_context_can_use_a_newly_staged_anchor_path() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor, metadata) = fixture();
    let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
    store
        .initialize(metadata, anchor.clone())
        .expect("the empty schema initializes");

    let mut nodes = Vec::new();
    let mut parent = anchor;
    for height in 1..=28 {
        let mut header = *parent.header;
        header.previous_block_hash = parent.hash;
        header.time += chrono::Duration::seconds(1);
        header.nonce.0[0] = u8::try_from(height).expect("the staged test path is shorter than 256");
        let header = Arc::new(header);
        let hash = header.hash();
        let node = HeaderNode::from_durable_parts(
            header,
            hash,
            parent.hash,
            block::Height(height),
            parent.block_work,
            parent
                .work_coordinate()
                .checked_add(parent.block_work)
                .expect("the short staged path cannot exhaust cumulative work"),
            HeaderValidationState::Valid,
            Default::default(),
            BodyValidationState::Unknown,
            Vec::new(),
        )
        .expect("the staged node fields are coherent");
        parent = node.clone();
        nodes.push(node);
    }
    let staged: HashMap<_, _> = nodes.iter().map(|node| (node.hash, node)).collect();
    let contexts = authenticated_context_headers(&store, parent.hash, Some(&staged))
        .expect("the atomic batch can authenticate context from its staged node overlay");
    assert_eq!(contexts.len(), 27);
    assert_eq!(
        contexts.first().map(|context| context.height),
        Some(block::Height(1))
    );
    assert_eq!(
        contexts.last().map(|context| context.height),
        Some(block::Height(27))
    );
    assert_eq!(
        parent.header.previous_block_hash,
        contexts
            .last()
            .expect("the context is nonempty")
            .header
            .hash()
    );
}

#[test]
fn publisher_mirror_stays_absent_until_attachment_then_tracks_commits() {
    let (_, _, metadata) = fixture();
    let initial = metadata.snapshot();
    let publisher = Publisher::new(initial.clone());
    let (mirror_sender, mirror_receiver) = watch::channel(None);

    assert_eq!(*mirror_receiver.borrow(), None);

    publisher.mirror_to(mirror_sender);
    assert_eq!(*mirror_receiver.borrow(), Some(initial.clone()));

    let mut committed = initial;
    committed.state_version = StateVersion::new(2);
    publisher.publish(committed.clone(), TransitionEffect::none());
    assert_eq!(*mirror_receiver.borrow(), Some(committed));
}

#[test]
fn coherent_reader_builds_locator_from_the_durable_selected_projection() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor, metadata) = fixture();
    let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
    store
        .initialize(metadata, anchor.clone())
        .expect("the empty schema initializes");
    let (runtime, _) = store
        .startup(&engine_config)
        .expect("the initialized store audits");

    let reader = runtime.reader();
    let durable = reader
        .selected_locator()
        .expect("the durable selected projection is coherent");
    let committed = reader
        .committed_selected_locator()
        .expect("the committed selected projection is coherent");
    assert_eq!(committed, durable);
    assert_eq!(
        durable.entries(),
        &[Frontier::new(anchor.height, anchor.hash)]
    );
}

#[test]
fn body_refill_snapshot_holds_the_complete_transition_barrier() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor, metadata) = fixture();
    let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
    store
        .initialize(metadata, anchor.clone())
        .expect("the empty schema initializes");
    let (runtime, _) = store
        .startup(&engine_config)
        .expect("the initialized store audits");
    let reader = runtime.reader();
    let cloned_reader = reader.clone();

    assert!(Arc::ptr_eq(&reader.config, &cloned_reader.config));

    let (full_state, selected_projection) = reader
        .with_selected_projection(|| {
            assert!(reader.store.writer.try_lock().is_err());
            assert!(reader.transition_engine.try_lock().is_err());
            Frontier::new(anchor.height, anchor.hash)
        })
        .expect("the body refill snapshot is coherent");

    assert_eq!(full_state, Frontier::new(anchor.height, anchor.hash));
    assert_eq!(selected_projection, vec![full_state]);
}

#[test]
fn selected_body_window_reads_four_thousand_hashes_in_one_coherent_range() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor, metadata) = fixture();
    let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
    store
        .initialize(metadata, anchor.clone())
        .expect("the empty schema initializes");

    let genesis = VerifiedHeaderRef {
        height: anchor.height,
        hash: anchor.hash,
        header: anchor.header.clone(),
    };
    let mut parent = genesis.clone();
    let mut restored = Vec::new();
    for height in 1_u32..=4_000 {
        let mut header = *parent.header;
        header.previous_block_hash = parent.hash;
        header.time += chrono::Duration::seconds(1);
        header.nonce.0[..4].copy_from_slice(&height.to_le_bytes());
        let header = Arc::new(header);
        let child = VerifiedHeaderRef {
            height: block::Height(height),
            hash: header.hash(),
            header,
        };
        parent = child.clone();
        restored.push(child);
    }

    let (runtime, _) = store
        .startup_reconciled(
            &engine_config,
            Frontier::new(genesis.height, genesis.hash),
            Vec::new(),
            restored.clone(),
        )
        .expect("the genesis-finalized scratch path reconciles");
    let selected = runtime
        .reader()
        .selected_hashes(block::Height(1), 4_000)
        .expect("the full block-sync window is one coherent projection read");

    assert_eq!(selected.len(), 4_000);
    assert_eq!(
        selected.first().copied(),
        Some(Frontier::new(restored[0].height, restored[0].hash))
    );
    assert_eq!(
        selected.last().copied(),
        restored
            .last()
            .map(|header| Frontier::new(header.height, header.hash))
    );
}

#[tokio::test(start_paused = true)]
async fn retained_path_serves_a_locator_before_the_header_retention_window() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor, metadata) = fixture();
    let db = open(&db_config, &engine_config.network);
    let store = HeaderChainStore::new(db.clone());
    store
        .initialize(metadata, anchor.clone())
        .expect("the empty schema initializes");

    let genesis = VerifiedHeaderRef {
        height: anchor.height,
        hash: anchor.hash,
        header: anchor.header.clone(),
    };
    let mut path = Vec::new();
    let mut parent = genesis.clone();
    for marker in 1..=3 {
        let mut header = *parent.header;
        header.previous_block_hash = parent.hash;
        header.time += chrono::Duration::seconds(1);
        header.nonce.0[0] = marker;
        let header = Arc::new(header);
        let height = parent
            .height
            .next()
            .expect("the three-header fixture stays in range");
        let hash = header.hash();
        let child = VerifiedHeaderRef {
            height,
            hash,
            header,
        };
        path.push(child.clone());
        parent = child;
    }

    let hash_by_height = db
        .cf_handle("hash_by_height")
        .expect("the finalized hash index exists");
    let height_by_hash = db
        .cf_handle("height_by_hash")
        .expect("the finalized height index exists");
    let block_header_by_height = db
        .cf_handle("block_header_by_height")
        .expect("the finalized header column exists");
    let mut batch = DiskWriteBatch::new();
    for header in std::iter::once(&genesis).chain(path[..2].iter()) {
        batch.zs_insert(&hash_by_height, header.height, header.hash);
        batch.zs_insert(&height_by_hash, header.hash, header.height);
        batch.zs_insert(
            &block_header_by_height,
            header.height,
            header.header.as_ref(),
        );
    }
    db.write(batch)
        .expect("the canonical finalized header fixture commits");

    let finalized = Frontier::new(path[1].height, path[1].hash);
    let (runtime, _) = store
        .startup_reconciled(
            &engine_config,
            finalized,
            path[..2].to_vec(),
            path[2..].to_vec(),
        )
        .expect("the finalized prefix and retained suffix reconcile");
    let reader = runtime.reader();
    let target = Frontier::new(path[2].height, path[2].hash);
    let scope = zakura_header_chain::HeaderWorkAuthority::for_target(
        &runtime.publisher().snapshot(),
        target.hash,
    );
    let RetainedPathLeaseOutcome::Acquired(lease) = reader
        .acquire_retained_path(
            SourceId::from_digest([0x71; 32]),
            9,
            target.hash,
            &[genesis.hash],
            scope,
        )
        .expect("the finalized locator is a coherent retained path")
    else {
        panic!("the finalized locator should acquire a lease");
    };
    assert_eq!(
        lease.common_ancestor,
        Frontier::new(genesis.height, genesis.hash)
    );

    let owner = SourceId::from_digest([0x71; 32]);
    let mut after = genesis.hash;
    for (expected, complete) in path.iter().zip([false, false, true]) {
        let RetainedPathReadOutcome::Page(page) = reader
            .read_retained_path(owner, 9, lease.lease_id, scope, after, 1)
            .expect("the historical path page is coherent")
        else {
            panic!("the historical path lease should remain available");
        };
        assert_eq!(
            page.headers.as_slice(),
            std::slice::from_ref(&expected.header)
        );
        assert_eq!(page.aux_deliveries, vec![Vec::new()]);
        assert_eq!(page.complete, complete);
        after = expected.hash;
    }
    assert!(reader
        .release_retained_path(owner, 9, lease.lease_id, scope)
        .expect("the one-header-page cursor releases"));

    for (marker, page_count) in [(0x72, 2), (0x73, 3)] {
        let page_owner = SourceId::from_digest([marker; 32]);
        let RetainedPathLeaseOutcome::Acquired(lease) = reader
            .acquire_retained_path(page_owner, 9, target.hash, &[genesis.hash], scope)
            .expect("the tier-boundary page cursor acquires")
        else {
            panic!("the tier-boundary cursor should be retained");
        };
        let mut after = genesis.hash;
        let mut served = Vec::new();
        loop {
            let RetainedPathReadOutcome::Page(page) = reader
                .read_retained_path(page_owner, 9, lease.lease_id, scope, after, page_count)
                .expect("the page spanning the storage-tier boundary is coherent")
            else {
                panic!("the tier-boundary cursor should remain available");
            };
            served.extend(page.headers.iter().map(|header| header.hash()));
            if page.complete {
                break;
            }
            after = page
                .headers
                .last()
                .expect("an incomplete page contains at least one header")
                .hash();
        }
        assert_eq!(
            served,
            path.iter().map(|header| header.hash).collect::<Vec<_>>(),
            "page counts ending at and after the tier boundary serve one canonical sequence",
        );
    }

    let retry_owner = SourceId::from_digest([0x74; 32]);
    let RetainedPathLeaseOutcome::Acquired(retry_lease) = reader
        .acquire_retained_path(retry_owner, 9, target.hash, &[genesis.hash], scope)
        .expect("the corruption-retry cursor acquires")
    else {
        panic!("the corruption-retry cursor should be retained");
    };
    let mut corrupt = DiskWriteBatch::new();
    corrupt.zs_delete(&hash_by_height, path[0].height);
    db.write(corrupt)
        .expect("the test removes one finalized path hash");
    assert!(reader
        .read_retained_path(retry_owner, 9, retry_lease.lease_id, scope, genesis.hash, 1,)
        .is_err());
    let mut restore = DiskWriteBatch::new();
    restore.zs_insert(&hash_by_height, path[0].height, path[0].hash);
    db.write(restore)
        .expect("the test restores the finalized path hash");
    let RetainedPathReadOutcome::Page(retried) = reader
        .read_retained_path(retry_owner, 9, retry_lease.lease_id, scope, genesis.hash, 1)
        .expect("a repaired local row can retry the same cursor position")
    else {
        panic!("the failed page did not advance the cursor");
    };
    assert_eq!(retried.headers[0].hash(), path[0].hash);

    let expiry_owner = SourceId::from_digest([0x75; 32]);
    let RetainedPathLeaseOutcome::Acquired(expiry_lease) = reader
        .acquire_retained_path(expiry_owner, 9, target.hash, &[genesis.hash], scope)
        .expect("the failed-read expiry cursor acquires")
    else {
        panic!("the failed-read expiry cursor should be retained");
    };
    tokio::time::advance(RETAINED_PATH_LEASE_IDLE.saturating_sub(Duration::from_secs(1))).await;
    let mut corrupt = DiskWriteBatch::new();
    corrupt.zs_delete(&hash_by_height, path[0].height);
    db.write(corrupt)
        .expect("the test removes the expiring cursor's next hash");
    assert!(reader
        .read_retained_path(
            expiry_owner,
            9,
            expiry_lease.lease_id,
            scope,
            genesis.hash,
            1,
        )
        .is_err());
    tokio::time::advance(Duration::from_secs(2)).await;
    let mut restore = DiskWriteBatch::new();
    restore.zs_insert(&hash_by_height, path[0].height, path[0].hash);
    db.write(restore)
        .expect("the test restores the expiring cursor's next hash");
    assert_eq!(
        reader
            .read_retained_path(
                expiry_owner,
                9,
                expiry_lease.lease_id,
                scope,
                genesis.hash,
                1,
            )
            .expect("an expired cursor is a normal unavailable outcome"),
        RetainedPathReadOutcome::Unavailable,
        "a failed page must not renew its cursor deadline",
    );
}

#[tokio::test(start_paused = true)]
async fn retained_path_leases_are_exact_bounded_session_scoped_and_expiring() {
    let db_config = Config::ephemeral();
    let (engine_config, anchor, metadata) = fixture();
    let anchor_frontier = Frontier::new(anchor.height, anchor.hash);
    let store = HeaderChainStore::new(open(&db_config, &engine_config.network));
    store
        .initialize(metadata, anchor.clone())
        .expect("the empty schema initializes");
    let mut child_header = *anchor.header;
    child_header.previous_block_hash = anchor.hash;
    child_header.time += chrono::Duration::seconds(1);
    let child_header = Arc::new(child_header);
    let child = VerifiedHeaderRef {
        height: anchor.height.next().expect("genesis has a successor"),
        hash: child_header.hash(),
        header: child_header,
    };
    let mut grandchild_header = *anchor.header;
    grandchild_header.previous_block_hash = child.hash;
    grandchild_header.time += chrono::Duration::seconds(2);
    let grandchild_header = Arc::new(grandchild_header);
    let grandchild = VerifiedHeaderRef {
        height: child.height.next().expect("the child has a successor"),
        hash: grandchild_header.hash(),
        header: grandchild_header,
    };
    let (runtime, _) = store
        .startup_reconciled(
            &engine_config,
            anchor_frontier,
            Vec::new(),
            vec![child.clone(), grandchild.clone()],
        )
        .expect("the selected two-header path reconciles");
    let reader = runtime.reader();
    let validation_lease = reader
        .validation_context(anchor.hash)
        .expect("the retained parent context is coherent")
        .expect("the retained anchor has validation context");
    assert_eq!(validation_lease.parent(), anchor_frontier);
    assert_eq!(
        validation_lease.trust_anchor_digest(),
        engine_config.trust_anchor_digest()
    );
    assert_eq!(
        reader
            .validation_context(block::Hash([0xff; 32]))
            .expect("an absent parent is a normal stale read"),
        None
    );
    let durable_window = reader
        .selected_auxiliary_window(child.height, child.hash)
        .expect("the exact selected auxiliary window is coherent")
        .expect("the selected child is retained");
    let window = runtime
        .selected_auxiliary_window(child.height, child.hash)
        .expect("the in-memory selected auxiliary window is coherent")
        .expect("the selected child is retained in the committed engine");
    assert_eq!(window, durable_window);
    let captured_projection = runtime
        .capture_selected_projection()
        .expect("the in-memory selected projection is coherent");
    let child_index = captured_projection
        .frontiers
        .binary_search_by_key(&child.height, |frontier| frontier.height)
        .expect("the selected projection contains the child");
    assert_eq!(
        runtime
            .selected_auxiliary_window_at_projection_index(
                child_index,
                Frontier::new(child.height, child.hash),
            )
            .expect("the captured projection index is coherent"),
        Some(window.clone())
    );
    assert_eq!(
        runtime
            .selected_auxiliary_window_at_projection_index(
                child_index + 1,
                Frontier::new(child.height, child.hash),
            )
            .expect("a stale projection index is a normal read outcome"),
        None
    );
    assert_eq!(
        window.engine_snapshot,
        runtime.publisher().snapshot(),
        "the auxiliary window carries the snapshot read under the same transition lock"
    );
    assert_eq!(window.delivery_header.header_node.hash, child.hash);
    assert!(window.delivery_header.auxiliary_deliveries.is_empty());
    let successor_header = window
        .successor_header
        .expect("the selected grandchild follows");
    assert_eq!(successor_header.header_node.hash, grandchild.hash);
    assert!(successor_header.auxiliary_deliveries.is_empty());
    assert_eq!(
        reader
            .selected_auxiliary_window(child.height, block::Hash([0xfe; 32]))
            .expect("a stale branch hash is a normal read outcome"),
        None
    );
    let snapshot = runtime.publisher().snapshot();
    let owner = zakura_header_chain::BodyWorkAuthority::for_snapshot(&snapshot)
        .bind(7, NonZeroU64::new(8).expect("eight is nonzero"));
    let repair = reader
        .vct_repair_context(owner, child.height)
        .expect("the selected repair context is coherent")
        .expect("the current owner resolves its selected header");
    assert_eq!(repair.target, Frontier::new(child.height, child.hash));
    assert_eq!(repair.locator.entries(), &[anchor_frontier]);

    let mut stale_owner = owner;
    stale_owner.authority.verified_generation = VerifiedGeneration::new(
        owner
            .verified_generation
            .get()
            .checked_add(1)
            .expect("the fixture state version can advance"),
    );
    assert_eq!(
        reader
            .vct_repair_context(stale_owner, child.height)
            .expect("a stale repair owner is a normal read outcome"),
        None
    );
    assert_eq!(
        reader
            .vct_repair_context(owner, anchor.height)
            .expect("a finalized repair height is a normal stale outcome"),
        None
    );

    let aux = zakura_header_chain::TreeAuxRecordV1 {
        height: child.height,
        sapling_root: Default::default(),
        orchard_root: Default::default(),
        ironwood_root: Default::default(),
        sapling_tx_count: 13,
        orchard_tx_count: 14,
        ironwood_tx_count: 15,
        auth_data_root: zakura_chain::block::merkle::AuthDataRoot::from([16; 32]),
    };
    let delivery = AuxDelivery::new(
        EvidenceId::from_digest([0x91; 32]),
        child.hash,
        SourceId::from_digest([0x92; 32]),
        owner.into(),
        zakura_header_chain::BodySizeHint::Unknown,
        Some(aux),
    );
    let mut child_node = runtime
        .store
        .header_node(child.hash)
        .expect("the selected child row decodes")
        .expect("the selected child is retained");
    child_node.aux_delivery_ids.push(delivery.delivery_id);
    let mut aux_batch = DiskWriteBatch::new();
    runtime
        .store
        .put_value(
            &mut aux_batch,
            HEADER_NODE_BY_HASH,
            child.hash.0,
            &HeaderNodeDisk::from_domain(&child_node),
        )
        .expect("the selected child with auxiliary evidence encodes");
    runtime
        .store
        .put_value(
            &mut aux_batch,
            HEADER_AUX_DELIVERY,
            HeaderAuxDeliveryKey {
                header: child.hash,
                delivery: delivery.delivery_id,
            }
            .as_bytes(),
            &delivery,
        )
        .expect("the selected auxiliary delivery encodes");
    runtime
        .store
        .db
        .write(aux_batch)
        .expect("the coherent selected auxiliary fixture commits");
    *runtime
        .transition_engine
        .lock()
        .expect("the transition engine mutex is not poisoned") =
        load_transition_engine(&runtime.store)
            .expect("the direct durable test fixture refreshes the runtime mirror");
    let roots = reader
        .selected_block_roots(child.height, 2)
        .expect("selected auxiliary roots are coherent");
    assert_eq!(roots.len(), 1, "the read stops at the first missing height");
    assert_eq!(roots[0].height, child.height);
    assert_eq!(roots[0].sapling_tx, aux.sapling_tx_count);
    assert_eq!(roots[0].orchard_tx, aux.orchard_tx_count);
    assert_eq!(roots[0].ironwood_tx, aux.ironwood_tx_count);
    assert_eq!(roots[0].auth_data_root, aux.auth_data_root);
    let crate::service::write::VctAuxiliaryWindowRead::Ready(window) =
        crate::service::write::HeaderChainWriter::new(runtime.clone(), engine_config.clone())
            .vct_auxiliary_window(child.height, child.hash)
            .expect("the selected auxiliary window is coherent")
    else {
        panic!("the current delivery remains usable without successor auxiliary data");
    };
    assert_eq!(window.successor_height, Some(grandchild.height));
    assert!(window.successor.is_none());

    let owner = SourceId::from_digest([1; 32]);
    let lease_scope = zakura_header_chain::HeaderWorkAuthority::for_target(
        &runtime.publisher().snapshot(),
        grandchild.hash,
    );
    let acquired = reader
        .acquire_retained_path(owner, 7, grandchild.hash, &[anchor.hash], lease_scope)
        .expect("the coherent target path is readable");
    let RetainedPathLeaseOutcome::Acquired(lease) = acquired else {
        panic!("the exact retained target should acquire a lease");
    };
    assert_eq!(
        lease.target,
        Frontier::new(grandchild.height, grandchild.hash)
    );
    assert_eq!(lease.common_ancestor, anchor_frontier);
    assert_eq!(lease.scope, lease_scope);
    let mut wrong_scope = lease_scope;
    wrong_scope.header_generation = wrong_scope
        .header_generation
        .checked_next()
        .expect("the fixture generation has a successor");
    assert_eq!(
        reader
            .acquire_retained_path(
                SourceId::from_digest([0xee; 32]),
                7,
                grandchild.hash,
                &[anchor.hash],
                wrong_scope,
            )
            .expect("a stale acquisition scope is a normal refusal"),
        RetainedPathLeaseOutcome::Busy
    );
    assert_eq!(
        reader
            .acquire_retained_path(owner, 7, grandchild.hash, &[anchor.hash], lease_scope,)
            .expect("the lease bound is a normal outcome"),
        RetainedPathLeaseOutcome::Busy
    );
    assert_eq!(
        reader
            .acquire_retained_path(owner, 8, grandchild.hash, &[anchor.hash], lease_scope)
            .expect("a new session cannot replace a live lease"),
        RetainedPathLeaseOutcome::Busy,
        "same-peer replacement requires exact release or expiry"
    );
    assert_eq!(
        reader
            .read_retained_path(owner, 8, lease.lease_id, lease_scope, anchor.hash, 1)
            .expect("a mismatched session is non-fatal"),
        RetainedPathReadOutcome::Unavailable
    );
    assert_eq!(
        reader
            .read_retained_path(owner, 7, lease.lease_id, wrong_scope, anchor.hash, 1)
            .expect("a mismatched branch scope is non-fatal"),
        RetainedPathReadOutcome::Unavailable
    );
    assert!(!reader
        .release_retained_path(owner, 7, lease.lease_id, wrong_scope)
        .expect("a mismatched release scope is non-fatal"));
    let RetainedPathReadOutcome::Page(page) = reader
        .read_retained_path(owner, 7, lease.lease_id, lease_scope, anchor.hash, 1)
        .expect("a lease page read validates against the serialized publication gate")
    else {
        panic!("the current owner should read its lease");
    };
    assert_eq!(page.headers.len(), 1);
    assert_eq!(page.headers[0].hash(), child.hash);
    assert_eq!(page.common_ancestor, anchor_frontier);
    assert_eq!(page.scope, lease_scope);
    assert_eq!(page.aux_deliveries, vec![vec![delivery]]);
    assert!(!page.complete);
    assert_eq!(
        reader
            .read_retained_path(owner, 7, lease.lease_id, lease_scope, anchor.hash, 1)
            .expect("a replayed cursor position is a normal refusal"),
        RetainedPathReadOutcome::Unavailable,
        "the opaque cursor advances exactly once and cannot be rewound",
    );

    let before = runtime.publisher().snapshot();
    let evidence = EvidenceId::from_digest([3; 32]);
    let id = zakura_header_chain::OperatorInvalidationId::new([3; 16]);
    let mut hasher = sha2::Sha256::new();
    use sha2::Digest as _;
    hasher.update(b"zakura-operator-invalidation-v1");
    hasher.update(child.hash.0);
    hasher.update(id.bytes());
    let authority = Authority(evidence);
    runtime
        .apply(
            TransitionRequest {
                expected_version: before.state_version,
                event: TransitionEvent::OperatorInvalidate(
                    zakura_header_chain::OperatorInvalidate {
                        target: child.hash,
                        id,
                        operator_reason_digest: hasher.finalize().into(),
                        evidence,
                    },
                ),
            },
            &TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: Some(&authority),
                retention_references: &[],
            },
        )
        .expect("the selected path can change while the lease is active");
    assert_eq!(
        runtime.publisher().snapshot().frontiers.header_best,
        anchor_frontier
    );

    let RetainedPathReadOutcome::Page(continuation) = reader
        .read_retained_path(owner, 7, lease.lease_id, lease_scope, child.hash, 1)
        .expect("the immutable cursor continues after reselection")
    else {
        panic!("the current owner should read its continuation");
    };
    assert_eq!(
        continuation.common_ancestor,
        Frontier::new(child.height, child.hash)
    );
    assert_eq!(continuation.headers[0].hash(), grandchild.hash);
    assert!(continuation.complete);

    assert_eq!(
        reader
            .acquire_retained_path(
                SourceId::from_digest([2; 32]),
                7,
                block::Hash([0xfe; 32]),
                &[anchor.hash],
                zakura_header_chain::HeaderWorkAuthority::for_target(
                    &runtime.publisher().snapshot(),
                    block::Hash([0xfe; 32]),
                ),
            )
            .expect("an absent target is a normal outcome"),
        RetainedPathLeaseOutcome::TargetNotRetained
    );
    assert_eq!(
        reader
            .acquire_retained_path(
                SourceId::from_digest([2; 32]),
                7,
                child.hash,
                &[block::Hash([0xfd; 32])],
                zakura_header_chain::HeaderWorkAuthority::for_target(
                    &runtime.publisher().snapshot(),
                    child.hash,
                ),
            )
            .expect("a disjoint locator is a normal outcome"),
        RetainedPathLeaseOutcome::NoLocatorIntersection
    );
    let RetainedPathLeaseOutcome::Acquired(target_intersection) = reader
        .acquire_retained_path(
            SourceId::from_digest([2; 32]),
            7,
            child.hash,
            &[child.hash, anchor.hash],
            zakura_header_chain::HeaderWorkAuthority::for_target(
                &runtime.publisher().snapshot(),
                child.hash,
            ),
        )
        .expect("the first requester-order intersection is selected")
    else {
        panic!("the target itself intersects the locator");
    };
    assert_eq!(target_intersection.common_ancestor.hash, child.hash);
    let RetainedPathReadOutcome::Page(completed) = reader
        .read_retained_path(
            SourceId::from_digest([2; 32]),
            7,
            target_intersection.lease_id,
            target_intersection.scope,
            child.hash,
            1,
        )
        .expect("a cursor acquired at its target is readable")
    else {
        panic!("the target-intersection cursor remains available");
    };
    assert!(completed.headers.is_empty());
    assert!(completed.complete);
    assert!(reader
        .release_retained_path(
            SourceId::from_digest([2; 32]),
            7,
            target_intersection.lease_id,
            target_intersection.scope,
        )
        .expect("the requester-order test lease releases"));

    assert!(reader
        .release_retained_path(owner, 7, lease.lease_id, lease_scope)
        .expect("the exact owner can release its lease"));
    for marker in 1..=MAX_RETAINED_PATH_LEASES {
        let marker = u8::try_from(marker).expect("the lease cap fits in one byte");
        assert!(matches!(
            reader
                .acquire_retained_path(
                    SourceId::from_digest([marker; 32]),
                    9,
                    child.hash,
                    &[anchor.hash],
                    zakura_header_chain::HeaderWorkAuthority::for_target(
                        &runtime.publisher().snapshot(),
                        child.hash,
                    ),
                )
                .expect("bounded acquisition returns an outcome"),
            RetainedPathLeaseOutcome::Acquired(_)
        ));
    }
    assert_eq!(
        reader
            .acquire_retained_path(
                SourceId::from_digest([0xff; 32]),
                9,
                child.hash,
                &[anchor.hash],
                zakura_header_chain::HeaderWorkAuthority::for_target(
                    &runtime.publisher().snapshot(),
                    child.hash,
                ),
            )
            .expect("capacity refusal is a normal outcome"),
        RetainedPathLeaseOutcome::Busy
    );
    let active_references = {
        let mut leases = runtime
            .leases
            .lock()
            .expect("the lease registry mutex is not poisoned");
        let active_references = leases.active_references(Instant::now());
        let cached_references = leases.active_references(Instant::now());
        assert!(Arc::ptr_eq(&active_references, &cached_references));
        active_references
    };
    assert_eq!(
        active_references.as_ref(),
        [child.hash],
        "each lease contributes only its target; retaining that target protects its whole ancestry"
    );

    tokio::time::advance(RETAINED_PATH_LEASE_IDLE + Duration::from_secs(1)).await;
    assert!(runtime
        .leases
        .lock()
        .expect("the lease registry mutex is not poisoned")
        .active_references(Instant::now())
        .is_empty());
    assert!(matches!(
        reader
            .acquire_retained_path(
                SourceId::from_digest([0xff; 32]),
                10,
                child.hash,
                &[anchor.hash],
                zakura_header_chain::HeaderWorkAuthority::for_target(
                    &runtime.publisher().snapshot(),
                    child.hash,
                ),
            )
            .expect("expired slots are reclaimed"),
        RetainedPathLeaseOutcome::Acquired(_)
    ));

    let snapshot = runtime.publisher().snapshot();
    let delivery = AuxDelivery::new(
        EvidenceId::from_digest([0xa1; 32]),
        anchor.hash,
        SourceId::from_digest([0xa2; 32]),
        body_owner(&snapshot, 11, 12).into(),
        zakura_header_chain::BodySizeHint::Unknown,
        None,
    );
    let mut corrupt = DiskWriteBatch::new();
    runtime
        .store
        .put_value(
            &mut corrupt,
            HEADER_AUX_DELIVERY,
            HeaderAuxDeliveryKey {
                header: anchor.hash,
                delivery: delivery.delivery_id,
            }
            .as_bytes(),
            &delivery,
        )
        .expect("the contradictory auxiliary row encodes");
    runtime
        .store
        .db
        .write(corrupt)
        .expect("the contradictory auxiliary row commits");
    assert!(matches!(
        reader.selected_auxiliary_window(anchor.height, anchor.hash),
        Err(HeaderChainStoreError::Store(StoreError::Incoherent(
            "retained node and auxiliary delivery index disagree"
        )))
    ));
}
