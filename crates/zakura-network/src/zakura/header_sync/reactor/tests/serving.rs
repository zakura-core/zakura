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
        complete: true,
    };

    let served = assemble_port_header_path_page(1, page, AuxSchema::V1)
        .expect("the finalized-state page is coherent");

    assert_eq!(served.tree_aux_schema, AuxSchema::V1);
    assert_eq!(served.entries[0].tree_aux, Some(tree_aux));
    assert_eq!(served.entries[0].body_size, 0);
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
fn new_request_replaces_idle_served_path_and_releases_its_lease() {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (_handle, mut actions, mut reactor) =
        build_header_sync_reactor(startup).expect("the serving fixture builds");
    let peer = peer();
    let old_target = zakura_header_chain::Frontier::new(block::Height(1), block::Hash([0x31; 32]));
    let old_scope =
        zakura_header_chain::HeaderWorkAuthority::for_target(&snapshot, old_target.hash);
    reactor.served_paths.insert(
        peer.clone(),
        ServedPathState::Active {
            session_id: 7,
            lease_id: 9,
            target: old_target,
            scope: old_scope,
            next_after: anchor,
            pending_request: None,
        },
    );
    reactor.served_path_deadlines.insert(
        peer.clone(),
        Instant::now() + std::time::Duration::from_secs(30),
    );
    let new_target = block::Hash([0x32; 32]);

    reactor.handle_get_headers(peer.clone(), 7, request(10, new_target, anchor.hash));

    assert!(matches!(
        actions.try_recv(),
        Ok(HeaderPortOperation::ReleaseHeaderPath { lease_id: 9, .. })
    ));
    assert!(matches!(
        actions.try_recv(),
        Ok(HeaderPortOperation::AcquirePath {
            request: GetHeaders {
                request_id: 10,
                target_tip_hash,
                ..
            },
            ..
        }) if target_tip_hash == new_target
    ));
    assert!(matches!(
        reactor.served_paths.get(&peer),
        Some(ServedPathState::Acquiring {
            request_id,
            target_tip_hash,
            ..
        }) if request_id.get() == 10 && *target_tip_hash == new_target
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
async fn retained_path_pages_keep_one_target_and_release_after_completion() {
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

    let continuation = request(2, target.hash, first);
    handle
        .send(Event::WireMessage {
            peer: peer.clone(),
            session_id: 0,
            msg: HeaderSyncMessage::GetHeaders(continuation),
        })
        .await
        .expect("the continuation reaches the reactor");
    assert!(matches!(
        next_action(&mut actions).await,
        HeaderPortOperation::ReadPath {
            lease_id: 9,
            request_id,
            after_hash,
            tree_aux_schema: AuxSchema::V1,
            ..
        } if request_id.get() == 2 && after_hash == first
    ));

    let continuation_ancestor = zakura_header_chain::Frontier::new(block::Height(1), first);
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
                lease_id: 9,
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
        HeaderPortOperation::ReleaseHeaderPath { lease_id: 9, .. }
    ));

    shutdown.cancel();
    task.await.expect("the reactor exits cleanly");
}

#[tokio::test]
async fn generation_change_retires_served_path_before_late_page_completion() {
    let shutdown = CancellationToken::new();
    let mut startup = startup(shutdown.clone());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let initial = committed_snapshot(anchor);
    let (snapshots_tx, snapshots_rx) = watch::channel(Some(initial.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (handle, mut actions, task) =
        spawn_header_sync_reactor(startup).expect("the serving fixture starts");

    let (send, mut outbound) = framed_channel(8);
    let peer = peer();
    handle
        .send(Event::PeerConnected(PeerSession::from_parts(
            peer.clone(),
            send,
            CancellationToken::new(),
        )))
        .await
        .expect("the peer connects");
    let _initial_status = outbound.recv().await.expect("initial status is sent");

    let target = zakura_header_chain::Frontier::new(block::Height(1), block::Hash([0x61; 32]));
    let request = request(1, target.hash, anchor.hash);
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
        HeaderPortOperation::ReadPath {
            lease_id: 17,
            scope: action_scope,
            ..
        } if action_scope == scope
    ));

    let mut advanced = initial;
    advanced.state_version = advanced
        .state_version
        .checked_next()
        .expect("the fixture state version has a successor");
    advanced.header_generation = advanced
        .header_generation
        .checked_next()
        .expect("the fixture header generation has a successor");
    snapshots_tx
        .send(Some(advanced))
        .expect("the snapshot receiver remains live");
    assert!(matches!(
        next_action(&mut actions).await,
        HeaderPortOperation::ReleaseHeaderPath {
            lease_id: 17,
            scope: action_scope,
            ..
        } if action_scope == scope
    ));
    let mut retired = outbound.recv().await.expect("retirement sends an outcome");
    if matches!(
        handle
            .codec()
            .decode_frame(retired.clone(), None)
            .expect("the concurrent frame decodes"),
        HeaderSyncMessage::Status(_)
    ) {
        retired = outbound
            .recv()
            .await
            .expect("the outcome follows the refreshed status");
    }
    assert_eq!(
        handle
            .codec()
            .decode_frame(retired, None)
            .expect("the retirement outcome decodes"),
        HeaderSyncMessage::HeadersOutcome(HeadersOutcome {
            request_id: 1,
            target_tip_hash: target.hash,
            outcome: HeadersOutcomeCode::Busy,
        })
    );

    handle
        .send(Event::HeaderPathPageReady {
            peer,
            session_id: 0,
            scope,
            request_id: HeaderSyncRequestId::new(1).expect("one is nonzero"),
            target_tip_hash: target.hash,
            result: HeaderPathPageResult::Page(Box::new(HeaderPathPage {
                lease_id: 17,
                common_ancestor: anchor,
                target,
                scope,
                tree_aux_schema: AuxSchema::None,
                entries: Vec::new(),
                complete: true,
            })),
        })
        .await
        .expect("the late page reaches the reactor");
    assert!(
        time::timeout(std::time::Duration::from_millis(20), outbound.recv())
            .await
            .is_err(),
        "a retired page cannot produce a wire response"
    );
    assert!(
        time::timeout(std::time::Duration::from_millis(20), actions.recv())
            .await
            .is_err(),
        "a retired page has no release, punishment, or follow-on action"
    );

    shutdown.cancel();
    task.await.expect("the reactor exits cleanly");
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
