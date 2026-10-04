use super::*;

#[test]
fn port_page_serves_finalized_tree_aux_without_peer_delivery_provenance() {
    let header = regtest_genesis_block().header.clone();
    let hash = header.hash();
    let frontier = zakura_header_chain::Frontier::new(block::Height(0), hash);
    let tree_aux = TreeAuxRecordV1 {
        height: block::Height(0),
        sapling_root: Default::default(),
        orchard_root: zakura_chain::orchard::tree::NoteCommitmentTree::default().root(),
        ironwood_root: zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
        sapling_tx_count: 0,
        orchard_tx_count: 0,
        ironwood_tx_count: 0,
        auth_data_root: [0; 32].into(),
    };
    let page = zakura_node_services::header_chain::RetainedHeaderPathPage {
        common_ancestor: frontier,
        target: frontier,
        scope: zakura_header_chain::HeaderWorkAuthority {
            header_generation: zakura_header_chain::HeaderGeneration::new(1),
            branch: zakura_header_chain::BranchId::new(hash, hash),
        },
        headers: vec![header],
        aux_deliveries: vec![Vec::new()],
        finalized_tree_aux: vec![Some(tree_aux)],
        finalized_body_sizes: vec![None],
        complete: true,
    };

    let served = assemble_port_header_path_page(1, page, AuxSchema::V1)
        .expect("the finalized-state page is coherent");

    assert_eq!(served.tree_aux_schema, AuxSchema::V1);
    assert_eq!(served.entries[0].tree_aux, Some(tree_aux));
    assert_eq!(served.entries[0].body_size, 0);
}

#[test]
fn port_page_prefers_committed_body_size_over_delivery_hints() {
    let header = regtest_genesis_block().header.clone();
    let hash = header.hash();
    let frontier = zakura_header_chain::Frontier::new(block::Height(0), hash);
    let scope = || zakura_header_chain::HeaderWorkAuthority {
        header_generation: zakura_header_chain::HeaderGeneration::new(1),
        branch: zakura_header_chain::BranchId::new(hash, hash),
    };
    let owner: zakura_header_chain::HeaderSyncWorkOwner = scope()
        .bind(3, std::num::NonZeroU64::new(4).expect("four is nonzero"))
        .into();
    let advertised = zakura_header_chain::AuxDelivery::new(
        zakura_header_chain::EvidenceId::from_digest([1; 32]),
        hash,
        zakura_header_chain::SourceId::from_digest([2; 32]),
        owner,
        zakura_header_chain::BodySizeHint::Known(
            std::num::NonZeroU32::new(777).expect("the hint is nonzero"),
        ),
        None,
    );
    let page = |finalized_body_size: Option<std::num::NonZeroU32>| {
        zakura_node_services::header_chain::RetainedHeaderPathPage {
            common_ancestor: frontier,
            target: frontier,
            scope: scope(),
            headers: vec![header.clone()],
            aux_deliveries: vec![vec![advertised]],
            finalized_tree_aux: vec![None],
            finalized_body_sizes: vec![finalized_body_size],
            complete: true,
        }
    };

    let served =
        assemble_port_header_path_page(1, page(std::num::NonZeroU32::new(1_234)), AuxSchema::None)
            .expect("the page is coherent");
    assert_eq!(
        served.entries[0].body_size, 1_234,
        "the committed size wins"
    );

    let served = assemble_port_header_path_page(1, page(None), AuxSchema::None)
        .expect("the page is coherent");
    assert_eq!(
        served.entries[0].body_size, 777,
        "the delivery hint is the fallback"
    );
}

#[test]
fn port_page_rejects_misaligned_body_sizes() {
    let header = regtest_genesis_block().header.clone();
    let hash = header.hash();
    let frontier = zakura_header_chain::Frontier::new(block::Height(0), hash);
    let page = zakura_node_services::header_chain::RetainedHeaderPathPage {
        common_ancestor: frontier,
        target: frontier,
        scope: zakura_header_chain::HeaderWorkAuthority {
            header_generation: zakura_header_chain::HeaderGeneration::new(1),
            branch: zakura_header_chain::BranchId::new(hash, hash),
        },
        headers: vec![header],
        aux_deliveries: vec![Vec::new()],
        finalized_tree_aux: vec![None],
        finalized_body_sizes: Vec::new(),
        complete: true,
    };

    assert!(assemble_port_header_path_page(1, page, AuxSchema::None).is_none());
}

#[test]
fn serving_count_reserves_bytes_for_the_requested_aux_schema() {
    let mut startup = startup(CancellationToken::new());
    startup.max_frame_bytes = 1_000;
    let (_, _, reactor) = build_header_sync_reactor(startup).expect("the serving fixture builds");
    let without_aux = reactor.served_page_count(u32::MAX, AuxSchema::None);
    let with_aux = reactor.served_page_count(u32::MAX, AuxSchema::V1);
    let response_bytes = |count| {
        headers_response_bytes(
            &reactor.startup.network,
            AuxSchema::V1,
            usize::try_from(count).expect("the response count fits usize"),
        )
        .expect("the bounded response size fits usize")
    };
    let limit = usize::try_from(reactor.serving_limits.max_message_bytes())
        .expect("the configured message limit fits usize");

    assert!(with_aux < without_aux);
    assert!(response_bytes(with_aux) <= limit);
    assert!(response_bytes(with_aux.saturating_add(1)) > limit);
}

#[test]
fn same_peer_session_replaces_at_the_full_direction_limit() {
    let mut startup = startup(CancellationToken::new());
    startup.config.peer_limits.max_inbound_peers = 1;
    let (_handle, _actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the replacement fixture builds");
    let peer = peer();
    let old_cancel = CancellationToken::new();
    let (old_send, _old_outbound) = framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts_with_session_id(
        peer.clone(),
        7,
        old_send,
        old_cancel.clone(),
    ));
    let replacement_cancel = CancellationToken::new();
    let (replacement_send, _replacement_outbound) = framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts_with_session_id(
        peer.clone(),
        8,
        replacement_send,
        replacement_cancel.clone(),
    ));

    assert!(old_cancel.is_cancelled());
    assert!(!replacement_cancel.is_cancelled());
    assert_eq!(reactor.admitted_count(ServicePeerDirection::Inbound), 1);
    assert_eq!(
        reactor
            .peer_state
            .get(&peer)
            .expect("the replacement session is retained")
            .session
            .session_id(),
        8
    );
}

#[test]
fn incomplete_page_releases_capacity_before_the_peer_continues() {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_handle, mut actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the serving fixture builds");
    let peer = peer();
    let (send, mut outbound) = framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts_with_session_id(
        peer.clone(),
        7,
        send,
        CancellationToken::new(),
    ));
    outbound.try_recv().expect("the initial status is sent");
    let mut header = *regtest_genesis_block().header;
    header.previous_block_hash = anchor.hash;
    let header = Arc::new(header);
    let first = header.hash();
    let target = zakura_header_chain::Frontier::new(block::Height(2), block::Hash([0x31; 32]));
    let scope = zakura_header_chain::HeaderWorkAuthority::for_target(&snapshot, target.hash);
    let request_id = HeaderSyncRequestId::new(1).unwrap();
    reactor.served_paths.insert(
        peer.clone(),
        ServedPathState::Active {
            session_id: 7,
            lease_id: 9,
            target,
            scope,
            next_after: anchor,
            pending_request: PendingServedRequest {
                request_id,
                max_header_count: 1,
                tree_aux_schema: AuxSchema::None,
            },
        },
    );
    reactor.served_path_deadlines.insert(
        peer.clone(),
        Instant::now() + std::time::Duration::from_secs(30),
    );

    reactor.handle_header_path_page_ready(
        peer.clone(),
        7,
        scope,
        request_id,
        target.hash,
        HeaderPathPageResult::Page(Box::new(HeaderPathPage {
            lease_id: 9,
            common_ancestor: anchor,
            target,
            scope,
            tree_aux_schema: AuxSchema::None,
            entries: vec![HeaderEntry {
                header,
                body_size: 0,
                tree_aux: None,
            }],
            complete: false,
        })),
    );
    outbound.try_recv().expect("the incomplete page is sent");
    assert!(
        matches!(
            actions.try_recv(),
            Ok(HeaderPortOperation::ReleaseHeaderPath { lease_id: 9, .. })
        ),
        "the peer must not hold state capacity while deciding whether to continue"
    );
    assert!(!reactor.served_paths.contains_key(&peer));
    assert!(!reactor.served_path_deadlines.contains_key(&peer));

    reactor.handle_get_headers(peer, 7, request(2, target.hash, first));
    assert!(matches!(actions.try_recv(),
        Ok(HeaderPortOperation::AcquirePath { request: GetHeaders {
            request_id: 2, target_tip_hash, locator_hashes, ..
        }, .. }) if target_tip_hash == target.hash && locator_hashes == vec![first]
    ));
}

#[test]
fn failed_path_acquisition_dispatch_removes_state_and_deadline() {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_handle, _actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the serving fixture builds");
    let peer = peer();
    for _ in 0..128 {
        reactor
            .actions
            .try_send(HeaderPortOperation::Misbehavior {
                peer: peer.clone(),
                reason: HeaderSyncMisbehavior::MalformedMessage,
            })
            .expect("the bounded action queue has exactly 128 slots");
    }

    reactor.handle_get_headers(
        peer.clone(),
        7,
        request(10, block::Hash([0x32; 32]), anchor.hash),
    );

    assert!(!reactor.served_paths.contains_key(&peer));
    assert!(!reactor.served_path_deadlines.contains_key(&peer));
}

#[tokio::test]
async fn retained_path_pages_keep_one_target_and_release_each_page() {
    let shutdown = CancellationToken::new();
    let mut startup = startup(shutdown.clone());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(committed_snapshot(anchor)));
    startup.committed_snapshots = Some(snapshots_rx);
    let (handle, mut actions, task) =
        spawn_header_sync_reactor(startup).expect("the fixture starts");
    let (send, mut outbound) = framed_channel(8);
    let peer = peer();
    handle
        .send(Event::PeerConnected(PeerSession::from_parts(
            peer.clone(),
            send,
            CancellationToken::new(),
        )))
        .await
        .expect("the reactor remains available");
    let _initial_status = outbound.recv().await.expect("initial status is sent");

    let mut first_header = *regtest_genesis_block().header;
    let common =
        zakura_header_chain::Frontier::new(block::Height(0), first_header.previous_block_hash);
    first_header.previous_block_hash = common.hash;
    let first_header = Arc::new(first_header);
    let first = first_header.hash();
    let mut second_header = *regtest_genesis_block().header;
    second_header.previous_block_hash = first;
    let second_header = Arc::new(second_header);
    let target = zakura_header_chain::Frontier::new(block::Height(2), second_header.hash());
    let first_request = request(1, target.hash, common.hash);

    handle
        .send(Event::WireMessage {
            peer: peer.clone(),
            session_id: 0,
            msg: HeaderSyncMessage::GetHeaders(first_request.clone()),
        })
        .await
        .expect("the request reaches the reactor");
    let scope = match next_action(&mut actions).await {
        HeaderPortOperation::AcquirePath {
            request: actual,
            scope,
            ..
        } if actual == first_request => scope,
        other => panic!("expected retained-path acquisition, got {other:?}"),
    };

    let stale_request = request(99, target.hash, common.hash);
    handle
        .send(Event::PathLeaseReady {
            peer: peer.clone(),
            session_id: 0,
            scope,
            request: stale_request,
            result: HeaderPathLeaseResult::Acquired(HeaderPathLease {
                lease_id: 99,
                common_ancestor: common,
                target,
                scope,
            }),
        })
        .await
        .expect("the stale lease result reaches the reactor");
    assert!(matches!(
        next_action(&mut actions).await,
        HeaderPortOperation::ReleaseHeaderPath { lease_id: 99, .. }
    ));

    handle
        .send(Event::PathLeaseReady {
            peer: peer.clone(),
            session_id: 0,
            scope,
            request: first_request,
            result: HeaderPathLeaseResult::Acquired(HeaderPathLease {
                lease_id: 9,
                common_ancestor: common,
                target,
                scope,
            }),
        })
        .await
        .expect("the lease result reaches the reactor");
    assert!(matches!(
        next_action(&mut actions).await,
        HeaderPortOperation::ReadPath {
            lease_id: 9,
            request_id,
            after_hash,
            max_header_count: 1,
            tree_aux_schema: AuxSchema::V1,
            ..
        } if request_id.get() == 1 && after_hash == common.hash
    ));

    handle
        .send(Event::HeaderPathPageReady {
            peer: peer.clone(),
            session_id: 0,
            scope,
            request_id: HeaderSyncRequestId::new(99).expect("99 is nonzero"),
            target_tip_hash: target.hash,
            result: HeaderPathPageResult::Unavailable,
        })
        .await
        .expect("the stale page result reaches the reactor");

    handle
        .send(Event::HeaderPathPageReady {
            peer: peer.clone(),
            session_id: 0,
            scope,
            request_id: HeaderSyncRequestId::new(1).expect("one is nonzero"),
            target_tip_hash: target.hash,
            result: HeaderPathPageResult::Page(Box::new(HeaderPathPage {
                lease_id: 9,
                common_ancestor: common,
                target,
                scope,
                tree_aux_schema: AuxSchema::None,
                entries: vec![HeaderEntry {
                    header: first_header,
                    body_size: 0,
                    tree_aux: None,
                }],
                complete: false,
            })),
        })
        .await
        .expect("the first page reaches the reactor");
    let first_frame = outbound.recv().await.expect("the first page is queued");
    let first_response = handle
        .codec()
        .decode_frame(
            first_frame,
            Some(HeaderSyncDecodeContext {
                max_header_count: 1,
                requested_tree_aux_schema: AuxSchema::V1,
            }),
        )
        .expect("schema-zero fallback decodes");
    assert!(matches!(
        first_response,
        HeaderSyncMessage::Headers(Headers {
            request_id: 1,
            target_tip_hash,
            common_ancestor_hash,
            complete: false,
            tree_aux_schema: AuxSchema::None,
            ..
        }) if target_tip_hash == target.hash && common_ancestor_hash == common.hash
    ));

    assert!(matches!(
        next_action(&mut actions).await,
        HeaderPortOperation::ReleaseHeaderPath { lease_id: 9, .. }
    ));
    let continuation = request(2, target.hash, first);
    handle
        .send(Event::WireMessage {
            peer: peer.clone(),
            session_id: 0,
            msg: HeaderSyncMessage::GetHeaders(continuation.clone()),
        })
        .await
        .expect("the continuation reaches the reactor");
    let scope = match next_action(&mut actions).await {
        HeaderPortOperation::AcquirePath {
            request: actual,
            scope,
            ..
        } if actual == continuation => scope,
        other => panic!("the continuation must acquire a fresh lease, got {other:?}"),
    };
    let continuation_ancestor = zakura_header_chain::Frontier::new(block::Height(1), first);
    handle
        .send(Event::PathLeaseReady {
            peer: peer.clone(),
            session_id: 0,
            scope,
            request: continuation,
            result: HeaderPathLeaseResult::Acquired(HeaderPathLease {
                lease_id: 10,
                common_ancestor: continuation_ancestor,
                target,
                scope,
            }),
        })
        .await
        .expect("the continuation lease reaches the reactor");
    assert!(matches!(
        next_action(&mut actions).await,
        HeaderPortOperation::ReadPath {
            lease_id: 10, request_id, after_hash,
            tree_aux_schema: AuxSchema::V1, ..
        } if request_id.get() == 2 && after_hash == first
    ));

    let tree_aux = TreeAuxRecordV1 {
        height: block::Height(2),
        sapling_root: Default::default(),
        orchard_root: zakura_chain::orchard::tree::NoteCommitmentTree::default().root(),
        ironwood_root: zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
        sapling_tx_count: 0,
        orchard_tx_count: 0,
        ironwood_tx_count: 0,
        auth_data_root: [0; 32].into(),
    };
    handle
        .send(Event::HeaderPathPageReady {
            peer: peer.clone(),
            session_id: 0,
            scope,
            request_id: HeaderSyncRequestId::new(2).expect("two is nonzero"),
            target_tip_hash: target.hash,
            result: HeaderPathPageResult::Page(Box::new(HeaderPathPage {
                lease_id: 10,
                common_ancestor: continuation_ancestor,
                target,
                scope,
                tree_aux_schema: AuxSchema::V1,
                entries: vec![HeaderEntry {
                    header: second_header,
                    body_size: 123,
                    tree_aux: Some(tree_aux),
                }],
                complete: true,
            })),
        })
        .await
        .expect("the completion reaches the reactor");
    let completion_frame = outbound.recv().await.expect("the completion is queued");
    let completion = handle
        .codec()
        .decode_frame(
            completion_frame,
            Some(HeaderSyncDecodeContext {
                max_header_count: 1,
                requested_tree_aux_schema: AuxSchema::V1,
            }),
        )
        .expect("the completion decodes");
    assert!(matches!(
        completion,
        HeaderSyncMessage::Headers(Headers {
            request_id: 2,
            target_tip_hash,
            common_ancestor_hash,
            complete: true,
            tree_aux_schema: AuxSchema::V1,
            entries,
            ..
        }) if target_tip_hash == target.hash
            && common_ancestor_hash == first
            && entries.len() == 1
            && entries[0].body_size == 123
            && entries[0].tree_aux == Some(tree_aux)
    ));
    assert!(matches!(
        next_action(&mut actions).await,
        HeaderPortOperation::ReleaseHeaderPath { lease_id: 10, .. }
    ));

    shutdown.cancel();
    task.await.expect("the reactor exits cleanly");
}

#[test]
fn serving_finishes_through_repeated_head_and_finality_updates() {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let mut snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_handle, mut actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the serving fixture builds");
    let (send, mut outbound) = framed_channel(8);
    let peer = peer();
    reactor.handle_peer_connected(PeerSession::from_parts_with_session_id(
        peer.clone(),
        7,
        send,
        CancellationToken::new(),
    ));
    outbound.try_recv().expect("the initial status is sent");
    let mut header = *regtest_genesis_block().header;
    header.previous_block_hash = anchor.hash;
    let header = Arc::new(header);
    let target = zakura_header_chain::Frontier::new(block::Height(1), header.hash());
    let request = request(1, target.hash, anchor.hash);
    reactor.handle_get_headers(peer.clone(), 7, request.clone());
    let scope = match actions.try_recv().expect("acquisition is dispatched") {
        HeaderPortOperation::AcquirePath { scope, .. } => scope,
        other => panic!("expected acquisition, got {other:?}"),
    };
    let mut advance = |reactor: &mut HeaderSyncReactor| {
        for height in 2..=101 {
            snapshot.state_version = snapshot.state_version.checked_next().unwrap();
            snapshot.header_generation = snapshot.header_generation.checked_next().unwrap();
            snapshot.frontiers.header_best =
                zakura_header_chain::Frontier::new(block::Height(height), block::Hash([0x91; 32]));
            snapshot.frontiers.finalized = target;
            reactor.observe_latest_committed_snapshot(snapshot.clone());
        }
    };
    advance(&mut reactor);
    assert!(matches!(
        reactor.served_paths.get(&peer),
        Some(ServedPathState::Acquiring { .. })
    ));
    assert!(
        actions.try_recv().is_err(),
        "head updates do not cancel acquisition"
    );
    reactor.handle_header_path_lease_ready(
        peer.clone(),
        7,
        scope,
        request,
        HeaderPathLeaseResult::Acquired(HeaderPathLease {
            lease_id: 17,
            common_ancestor: anchor,
            target,
            scope,
        }),
    );
    assert!(matches!(
        actions.try_recv(),
        Ok(HeaderPortOperation::ReadPath { lease_id: 17, .. })
    ));
    advance(&mut reactor);
    assert!(matches!(
        reactor.served_paths.get(&peer),
        Some(ServedPathState::Active { .. })
    ));
    assert!(
        actions.try_recv().is_err(),
        "head updates do not cancel a page read"
    );
    reactor.handle_header_path_page_ready(
        peer.clone(),
        7,
        scope,
        HeaderSyncRequestId::new(1).expect("one is nonzero"),
        target.hash,
        HeaderPathPageResult::Page(Box::new(HeaderPathPage {
            lease_id: 17,
            common_ancestor: anchor,
            target,
            scope,
            tree_aux_schema: AuxSchema::None,
            entries: vec![HeaderEntry {
                header,
                body_size: 0,
                tree_aux: None,
            }],
            complete: true,
        })),
    );
    let response = outbound
        .try_recv()
        .expect("the original request receives a response");
    assert!(
        matches!(reactor.codec.decode_frame(response, Some(HeaderSyncDecodeContext { max_header_count: 1, requested_tree_aux_schema: AuxSchema::None })).expect("the response decodes"),
        HeaderSyncMessage::Headers(Headers { request_id: 1, target_tip_hash, complete: true, .. })
            if target_tip_hash == target.hash)
    );
    assert!(
        outbound.try_recv().is_err(),
        "no update produces a Busy refusal"
    );
    assert!(matches!(
        actions.try_recv(),
        Ok(HeaderPortOperation::ReleaseHeaderPath { lease_id: 17, .. })
    ));
    assert!(!reactor.served_paths.contains_key(&peer));
}

#[tokio::test]
async fn every_unservable_path_result_is_a_correlated_explicit_outcome() {
    let shutdown = CancellationToken::new();
    let mut startup = startup(shutdown.clone());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(committed_snapshot(anchor)));
    startup.committed_snapshots = Some(snapshots_rx);
    let (handle, mut actions, task) =
        spawn_header_sync_reactor(startup).expect("the fixture starts");
    let (send, mut outbound) = framed_channel(8);
    let peer = peer();
    handle
        .send(Event::PeerConnected(PeerSession::from_parts(
            peer.clone(),
            send,
            CancellationToken::new(),
        )))
        .await
        .expect("the reactor remains available");
    let _initial_status = outbound.recv().await.expect("initial status is sent");

    for (offset, outcome) in [
        HeadersOutcomeCode::TargetNotRetained,
        HeadersOutcomeCode::NoLocatorIntersection,
        HeadersOutcomeCode::HistoryPruned,
        HeadersOutcomeCode::Busy,
    ]
    .into_iter()
    .enumerate()
    {
        let request_id = u64::try_from(offset + 1).expect("the fixture IDs fit in u64");
        let target = block::Hash([u8::try_from(offset + 1).expect("small marker"); 32]);
        let request = request(request_id, target, block::Hash([0x41; 32]));
        handle
            .send(Event::WireMessage {
                peer: peer.clone(),
                session_id: 0,
                msg: HeaderSyncMessage::GetHeaders(request.clone()),
            })
            .await
            .expect("the request reaches the reactor");
        let scope = match next_action(&mut actions).await {
            HeaderPortOperation::AcquirePath {
                request: actual,
                scope,
                ..
            } if actual == request => scope,
            other => panic!("expected retained-path acquisition, got {other:?}"),
        };
        handle
            .send(Event::PathLeaseReady {
                peer: peer.clone(),
                session_id: 0,
                scope,
                request,
                result: HeaderPathLeaseResult::Outcome(outcome),
            })
            .await
            .expect("the state outcome reaches the reactor");
        let frame = outbound.recv().await.expect("the outcome is queued");
        assert_eq!(
            handle
                .codec()
                .decode_frame(frame, None)
                .expect("the outcome decodes"),
            HeaderSyncMessage::HeadersOutcome(HeadersOutcome {
                request_id,
                target_tip_hash: target,
                outcome,
            })
        );
    }

    let request_id = 9_u64;
    let target = zakura_header_chain::Frontier::new(block::Height(1), block::Hash([9; 32]));
    let request = request(request_id, target.hash, anchor.hash);
    handle
        .send(Event::WireMessage {
            peer: peer.clone(),
            session_id: 0,
            msg: HeaderSyncMessage::GetHeaders(request.clone()),
        })
        .await
        .expect("the request reaches the reactor");
    let scope = match next_action(&mut actions).await {
        HeaderPortOperation::AcquirePath {
            request: actual,
            scope,
            ..
        } if actual == request => scope,
        other => panic!("expected retained-path acquisition, got {other:?}"),
    };
    handle
        .send(Event::PathLeaseReady {
            peer: peer.clone(),
            session_id: 0,
            scope,
            request,
            result: HeaderPathLeaseResult::Acquired(HeaderPathLease {
                lease_id: 17,
                common_ancestor: anchor,
                target,
                scope,
            }),
        })
        .await
        .expect("the lease reaches the reactor");
    assert!(matches!(
        next_action(&mut actions).await,
        HeaderPortOperation::ReadPath { lease_id: 17, .. }
    ));
    handle
        .send(Event::HeaderPathPageReady {
            peer,
            session_id: 0,
            scope,
            request_id: HeaderSyncRequestId::new(request_id).expect("nine is nonzero"),
            target_tip_hash: target.hash,
            result: HeaderPathPageResult::Page(Box::new(HeaderPathPage {
                lease_id: 18,
                common_ancestor: anchor,
                target,
                scope,
                tree_aux_schema: AuxSchema::None,
                entries: Vec::new(),
                complete: true,
            })),
        })
        .await
        .expect("the incoherent page reaches the reactor");
    let frame = outbound
        .recv()
        .await
        .expect("the failure outcome is queued");
    assert_eq!(
        handle
            .codec()
            .decode_frame(frame, None)
            .expect("the failure outcome decodes"),
        HeaderSyncMessage::HeadersOutcome(HeadersOutcome {
            request_id,
            target_tip_hash: target.hash,
            outcome: HeadersOutcomeCode::Busy,
        })
    );
    assert!(matches!(
        next_action(&mut actions).await,
        HeaderPortOperation::ReleaseHeaderPath { lease_id: 17, .. }
    ));

    shutdown.cancel();
    task.await.expect("the reactor exits cleanly");
}

#[test]
fn served_aux_selection_is_deterministic_and_excludes_rejected_evidence() {
    let owner: zakura_header_chain::HeaderSyncWorkOwner =
        zakura_header_chain::HeaderWorkAuthority {
            header_generation: zakura_header_chain::HeaderGeneration::new(2),
            branch: zakura_header_chain::BranchId::new(block::Hash([1; 32]), block::Hash([2; 32])),
        }
        .bind(3, std::num::NonZeroU64::new(4).expect("four is nonzero"))
        .into();
    let source = zakura_header_chain::SourceId::from_digest([5; 32]);
    let header_hash = block::Hash([6; 32]);
    let tree_aux = TreeAuxRecordV1 {
        height: block::Height(1),
        sapling_root: Default::default(),
        orchard_root: Default::default(),
        ironwood_root: Default::default(),
        sapling_tx_count: 0,
        orchard_tx_count: 0,
        ironwood_tx_count: 0,
        auth_data_root: [0; 32].into(),
    };
    // status_code: 0 = fresh (unauthenticated), 1 = authenticated, 2 = rejected.
    let delivery = |marker: u8, size: u32, status_code: u8| {
        let delivery = zakura_header_chain::AuxDelivery::new(
            zakura_header_chain::EvidenceId::from_digest([marker; 32]),
            header_hash,
            source,
            owner,
            zakura_header_chain::BodySizeHint::Known(
                std::num::NonZeroU32::new(size).expect("test sizes are nonzero"),
            ),
            Some(tree_aux),
        );
        if status_code == 0 {
            delivery
        } else {
            delivery
                .test_only_with_outcome(
                    status_code,
                    [Some([marker.wrapping_add(6); 32]), None],
                    Some(block::Hash([9; 32])),
                )
                .expect("the test outcome is coherent")
        }
    };
    let rejected = delivery(1, 10, 2);
    let unauthenticated = delivery(2, 20, 0);
    let authenticated = delivery(3, 30, 1);
    let deliveries = [rejected, unauthenticated, authenticated];

    assert_eq!(
        selected_port_aux_delivery(&deliveries, AuxSchema::V1),
        Some(authenticated)
    );
    assert_eq!(
        selected_port_aux_delivery(&deliveries, AuxSchema::None),
        Some(authenticated)
    );
    assert_eq!(selected_port_aux_delivery(&[rejected], AuxSchema::V1), None);
}

#[test]
fn retained_page_uses_v1_only_when_every_record_is_available() {
    let header = regtest_genesis_block().header.clone();
    let hash = header.hash();
    let frontier = zakura_header_chain::Frontier::new(block::Height(0), hash);
    let scope = || zakura_header_chain::HeaderWorkAuthority {
        header_generation: zakura_header_chain::HeaderGeneration::new(2),
        branch: zakura_header_chain::BranchId::new(block::Hash([1; 32]), block::Hash([2; 32])),
    };
    let owner: zakura_header_chain::HeaderSyncWorkOwner = scope()
        .bind(3, std::num::NonZeroU64::new(4).expect("four is nonzero"))
        .into();
    let tree_aux = TreeAuxRecordV1 {
        height: block::Height(0),
        sapling_root: Default::default(),
        orchard_root: Default::default(),
        ironwood_root: Default::default(),
        sapling_tx_count: 0,
        orchard_tx_count: 0,
        ironwood_tx_count: 0,
        auth_data_root: [0; 32].into(),
    };
    let mut page = zakura_node_services::header_chain::RetainedHeaderPathPage {
        common_ancestor: frontier,
        target: frontier,
        scope: scope(),
        headers: vec![header],
        aux_deliveries: vec![Vec::new()],
        finalized_tree_aux: vec![None],
        finalized_body_sizes: vec![None],
        complete: true,
    };

    let fallback = assemble_port_header_path_page(1, page.clone(), AuxSchema::V1)
        .expect("the coherent parallel page assembles");
    assert_eq!(fallback.tree_aux_schema, AuxSchema::None);
    assert_eq!(fallback.entries[0].body_size, 0);
    assert_eq!(fallback.entries[0].tree_aux, None);

    page.finalized_tree_aux[0] = Some(tree_aux);
    let served_from_finalized_state =
        assemble_port_header_path_page(1, page.clone(), AuxSchema::V1)
            .expect("the coherent finalized-state page assembles");
    assert_eq!(served_from_finalized_state.tree_aux_schema, AuxSchema::V1);
    assert_eq!(served_from_finalized_state.entries[0].body_size, 0);
    assert_eq!(
        served_from_finalized_state.entries[0].tree_aux,
        Some(tree_aux)
    );
    page.finalized_tree_aux[0] = None;

    page.aux_deliveries[0].push(zakura_header_chain::AuxDelivery::new(
        zakura_header_chain::EvidenceId::from_digest([10; 32]),
        hash,
        zakura_header_chain::SourceId::from_digest([11; 32]),
        owner,
        zakura_header_chain::BodySizeHint::Known(
            std::num::NonZeroU32::new(321).expect("the hint is nonzero"),
        ),
        Some(tree_aux),
    ));
    let no_aux = assemble_port_header_path_page(1, page.clone(), AuxSchema::None)
        .expect("the coherent parallel page assembles");
    assert_eq!(no_aux.tree_aux_schema, AuxSchema::None);
    assert_eq!(no_aux.entries[0].body_size, 321);
    assert_eq!(no_aux.entries[0].tree_aux, None);

    let served = assemble_port_header_path_page(1, page, AuxSchema::V1)
        .expect("the coherent parallel page assembles");
    assert_eq!(served.tree_aux_schema, AuxSchema::V1);
    assert_eq!(served.entries[0].body_size, 321);
    assert_eq!(served.entries[0].tree_aux, Some(tree_aux));
}

#[test]
fn port_page_takes_the_size_from_any_retained_delivery_under_v1() {
    let header = regtest_genesis_block().header.clone();
    let hash = header.hash();
    let frontier = zakura_header_chain::Frontier::new(block::Height(0), hash);
    let scope = || zakura_header_chain::HeaderWorkAuthority {
        header_generation: zakura_header_chain::HeaderGeneration::new(1),
        branch: zakura_header_chain::BranchId::new(hash, hash),
    };
    let owner: zakura_header_chain::HeaderSyncWorkOwner = scope()
        .bind(3, std::num::NonZeroU64::new(4).expect("four is nonzero"))
        .into();
    let tree_aux = TreeAuxRecordV1 {
        height: block::Height(0),
        sapling_root: Default::default(),
        orchard_root: Default::default(),
        ironwood_root: Default::default(),
        sapling_tx_count: 0,
        orchard_tx_count: 0,
        ironwood_tx_count: 0,
        auth_data_root: [0; 32].into(),
    };
    let rooted_without_size = zakura_header_chain::AuxDelivery::new(
        zakura_header_chain::EvidenceId::from_digest([1; 32]),
        hash,
        zakura_header_chain::SourceId::from_digest([2; 32]),
        owner,
        zakura_header_chain::BodySizeHint::Unknown,
        Some(tree_aux),
    );
    let sized_without_root = zakura_header_chain::AuxDelivery::new(
        zakura_header_chain::EvidenceId::from_digest([3; 32]),
        hash,
        zakura_header_chain::SourceId::from_digest([4; 32]),
        owner,
        zakura_header_chain::BodySizeHint::Known(
            std::num::NonZeroU32::new(555).expect("the hint is nonzero"),
        ),
        None,
    );
    let page = zakura_node_services::header_chain::RetainedHeaderPathPage {
        common_ancestor: frontier,
        target: frontier,
        scope: scope(),
        headers: vec![header],
        aux_deliveries: vec![vec![rooted_without_size, sized_without_root]],
        finalized_tree_aux: vec![None],
        finalized_body_sizes: vec![None],
        complete: true,
    };

    let served =
        assemble_port_header_path_page(1, page, AuxSchema::V1).expect("the page is coherent");

    assert_eq!(served.tree_aux_schema, AuxSchema::V1);
    assert_eq!(served.entries[0].tree_aux, Some(tree_aux));
    assert_eq!(served.entries[0].body_size, 555);
}
