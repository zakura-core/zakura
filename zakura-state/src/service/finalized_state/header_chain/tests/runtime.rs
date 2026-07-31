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
    publisher.publish(committed.clone());
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

    assert_eq!(
        runtime
            .reader()
            .selected_locator()
            .expect("the selected projection is coherent")
            .entries(),
        &[Frontier::new(anchor.height, anchor.hash)]
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
    let child_header = Arc::new(child_header);
    let child = VerifiedHeaderRef {
        height: anchor.height.next().expect("genesis has a successor"),
        hash: child_header.hash(),
        header: child_header,
    };
    let mut grandchild_header = *anchor.header;
    grandchild_header.previous_block_hash = child.hash;
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
    let window = reader
        .selected_aux_window(child.height, child.hash)
        .expect("the exact selected auxiliary window is coherent")
        .expect("the selected child is retained");
    assert_eq!(
        window.snapshot,
        runtime.publisher().snapshot(),
        "the auxiliary window carries the snapshot read under the same transition lock"
    );
    assert_eq!(window.current.hash, child.hash);
    assert!(window.current_deliveries.is_empty());
    let (window_successor, successor_deliveries) =
        window.successor.expect("the selected grandchild follows");
    assert_eq!(window_successor.hash, grandchild.hash);
    assert!(successor_deliveries.is_empty());
    assert_eq!(
        reader
            .selected_aux_window(child.height, block::Hash([0xfe; 32]))
            .expect("a stale branch hash is a normal read outcome"),
        None
    );
    let snapshot = runtime.publisher().snapshot();
    let owner =
        WorkScope::for_body_work(&snapshot).bind(7, NonZeroU64::new(8).expect("eight is nonzero"));
    let repair = reader
        .vct_repair_context(owner, child.height)
        .expect("the selected repair context is coherent")
        .expect("the current owner resolves its selected header");
    assert_eq!(repair.target, Frontier::new(child.height, child.hash));
    assert_eq!(repair.locator.entries(), &[anchor_frontier]);

    let mut stale_owner = owner;
    stale_owner.state_version = StateVersion::new(
        owner
            .state_version
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
    let delivery = AuxDelivery {
        delivery_id: EvidenceId::from_digest([0x91; 32]),
        header_hash: child.hash,
        source: SourceId::from_digest([0x92; 32]),
        owner,
        body_size: zakura_header_chain::BodySizeHint::Unknown,
        tree_aux: Some(aux),
        authentication: zakura_header_chain::AuxAuthentication::Unauthenticated,
    };
    let mut child_node = runtime
        .store
        .node(child.hash)
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
    assert!(matches!(
        crate::service::write::HeaderChainWriter::new(
            runtime.clone(),
            engine_config.clone()
        )
        .vct_aux_window(child.height, child.hash)
        .expect("the selected auxiliary window is coherent"),
        crate::service::write::VctAuxWindowRead::Missing { height }
            if height == grandchild.height
    ));

    let owner = SourceId::from_digest([1; 32]);
    let lease_scope = zakura_header_chain::WorkScope::for_header_target(
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
    assert_eq!(lease.path.as_ref(), &[child.hash, grandchild.hash]);
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
        .expect("the lease page is readable")
    else {
        panic!("the current owner should read its lease");
    };
    assert_eq!(page.nodes.len(), 1);
    assert_eq!(page.nodes[0].hash, child.hash);
    assert_eq!(page.common_ancestor, anchor_frontier);
    assert_eq!(page.scope, lease_scope);
    assert_eq!(page.aux_deliveries, vec![vec![delivery]]);
    assert!(!page.complete);
    let RetainedPathReadOutcome::Page(continuation) = reader
        .read_retained_path(owner, 7, lease.lease_id, lease_scope, child.hash, 1)
        .expect("the continuation page is readable")
    else {
        panic!("the current owner should read its continuation");
    };
    assert_eq!(
        continuation.common_ancestor,
        Frontier::new(child.height, child.hash)
    );
    assert_eq!(continuation.nodes[0].hash, grandchild.hash);
    assert!(continuation.complete);

    let before = runtime.publisher().snapshot();
    runtime
        .apply(
            TransitionRequest {
                expected_version: before.state_version,
                event: TransitionEvent::OperatorInvalidate(
                    zakura_header_chain::OperatorInvalidate {
                        target: child.hash,
                        id: zakura_header_chain::OperatorInvalidationId::new([3; 16]),
                        operator_reason_digest: [4; 32],
                        evidence: EvidenceId::from_digest([3; 32]),
                    },
                ),
            },
            &TransitionContext {
                config: &engine_config,
                clock: &SystemClock,
                full_state_authority: None,
                retention_references: &[],
            },
        )
        .expect("the selected path can change while the lease is active");
    assert_eq!(
        runtime.publisher().snapshot().frontiers.header_best,
        anchor_frontier
    );
    let RetainedPathReadOutcome::Page(page_after_reselection) = reader
        .read_retained_path(owner, 7, lease.lease_id, lease_scope, anchor.hash, 1)
        .expect("the immutable lease survives reselection")
    else {
        panic!("the lease remains available after reselection");
    };
    assert_eq!(page_after_reselection.nodes[0].hash, child.hash);

    assert_eq!(
        reader
            .acquire_retained_path(
                SourceId::from_digest([2; 32]),
                7,
                block::Hash([0xfe; 32]),
                &[anchor.hash],
                zakura_header_chain::WorkScope::for_header_target(
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
                zakura_header_chain::WorkScope::for_header_target(
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
            zakura_header_chain::WorkScope::for_header_target(
                &runtime.publisher().snapshot(),
                child.hash,
            ),
        )
        .expect("the first requester-order intersection is selected")
    else {
        panic!("the target itself intersects the locator");
    };
    assert_eq!(target_intersection.common_ancestor.hash, child.hash);
    assert!(target_intersection.path.is_empty());
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
                    zakura_header_chain::WorkScope::for_header_target(
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
                zakura_header_chain::WorkScope::for_header_target(
                    &runtime.publisher().snapshot(),
                    child.hash,
                ),
            )
            .expect("capacity refusal is a normal outcome"),
        RetainedPathLeaseOutcome::Busy
    );
    let active_references = runtime
        .leases
        .lock()
        .expect("the lease registry mutex is not poisoned")
        .active_references(Instant::now());
    assert!(active_references.contains(&anchor.hash));
    assert!(active_references.contains(&child.hash));

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
                zakura_header_chain::WorkScope::for_header_target(
                    &runtime.publisher().snapshot(),
                    child.hash,
                ),
            )
            .expect("expired slots are reclaimed"),
        RetainedPathLeaseOutcome::Acquired(_)
    ));

    let snapshot = runtime.publisher().snapshot();
    let delivery = AuxDelivery {
        delivery_id: EvidenceId::from_digest([0xa1; 32]),
        header_hash: anchor.hash,
        source: SourceId::from_digest([0xa2; 32]),
        owner: zakura_header_chain::WorkOwner {
            state_version: snapshot.state_version,
            header_generation: snapshot.header_generation,
            verified_generation: Some(snapshot.verified_generation),
            branch: zakura_header_chain::BranchId::new(anchor.hash, anchor.hash),
            session_id: 11,
            request_id: std::num::NonZeroU64::new(12).expect("twelve is nonzero"),
        },
        body_size: zakura_header_chain::BodySizeHint::Unknown,
        tree_aux: None,
        authentication: zakura_header_chain::AuxAuthentication::Unauthenticated,
    };
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
        reader.selected_aux_window(anchor.height, anchor.hash),
        Err(HeaderChainStoreError::Store(StoreError::Incoherent(
            "retained node and auxiliary delivery index disagree"
        )))
    ));
}
