use super::*;

#[test]
fn empty_complete_response_at_target_is_benign() {
    let (mut reactor, mut actions, _snapshot, peer, _source, owner) = peer_violation_fixture();
    reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the fixture has active work")
        .phase = HeaderTargetPhase::Receiving;
    let target_height = reactor
        .peer_work_queue
        .active(&peer)
        .expect("the fixture has active work")
        .target
        .status
        .selected_tip_height;

    reactor.handle_headers(
        peer.clone(),
        owner.session_id,
        owner.scope(),
        Headers {
            request_id: owner.request_id.get(),
            target_tip_hash: owner.branch.target_tip_hash,
            common_ancestor_height: target_height,
            common_ancestor_hash: owner.branch.target_tip_hash,
            complete: true,
            tree_aux_schema: AuxSchema::None,
            entries: Vec::new(),
        },
    );

    assert!(reactor.peer_work_queue.active(&peer).is_none());
    assert!(
        actions.try_recv().is_err(),
        "an already-known target neither scores the peer nor retries malformed work"
    );
}

#[test]
fn empty_complete_response_requires_the_exact_height_qualified_ancestor() {
    let (mut reactor, mut actions, _snapshot, peer, _source, owner) = peer_violation_fixture();
    reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the fixture has active work")
        .phase = HeaderTargetPhase::Receiving;
    let target_height = reactor
        .peer_work_queue
        .active(&peer)
        .expect("the fixture has active work")
        .target
        .status
        .selected_tip_height;

    reactor.handle_headers(
        peer.clone(),
        owner.session_id,
        owner.scope(),
        Headers {
            request_id: owner.request_id.get(),
            target_tip_hash: owner.branch.target_tip_hash,
            common_ancestor_height: block::Height(target_height.0.saturating_add(1)),
            common_ancestor_hash: owner.branch.target_tip_hash,
            complete: true,
            tree_aux_schema: AuxSchema::None,
            entries: Vec::new(),
        },
    );

    assert!(reactor.peer_work_queue.active(&peer).is_none());
    assert!(matches!(
        actions.try_recv(),
        Ok(HeaderPortOperation::Misbehavior {
            peer: reported_peer,
            reason: HeaderSyncMisbehavior::MalformedMessage,
        }) if reported_peer == peer
    ));
}

#[tokio::test]
async fn stale_locator_completion_cannot_rebase_onto_a_new_generation() {
    let shutdown = CancellationToken::new();
    let mut startup = startup(shutdown.clone());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let initial = committed_snapshot(anchor);
    let (snapshots_tx, snapshots_rx) = watch::channel(Some(initial.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (handle, mut actions, task) =
        spawn_header_sync_reactor(startup).expect("the requester fixture starts");

    let (send, mut outbound) = framed_channel(8);
    let peer = peer();
    handle
        .send(HeaderSyncEvent::PeerConnected(
            HeaderSyncPeerSession::from_parts(peer.clone(), send, CancellationToken::new()),
        ))
        .await
        .expect("the peer connects");
    let _initial_status = outbound.recv().await.expect("initial status is sent");

    let target = block::Hash([0x52; 32]);
    let remote_status = Status {
        work_anchor_height: anchor.height,
        work_anchor_hash: anchor.hash,
        selected_tip_height: block::Height(2),
        selected_tip_hash: target,
        suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(2_u8),
        oldest_retained_height: anchor.height,
        max_headers_per_response: 1,
        max_inflight_requests: 1,
        max_message_bytes: 2_000_000,
        tree_aux_schema_mask: 0,
    };
    handle
        .send(HeaderSyncEvent::SessionWireMessage {
            peer: peer.clone(),
            session_id: 0,
            msg: HeaderSyncMessage::Status(remote_status.clone()),
        })
        .await
        .expect("the target status reaches the reactor");
    let stale_scope = match next_action(&mut actions).await {
        HeaderPortOperation::QueryHeaderLocator {
            target_tip_hash,
            scope,
            ..
        } if target_tip_hash == target => scope,
        other => panic!("expected locator query for target, got {other:?}"),
    };

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

    let fresh_scope = loop {
        handle
            .send(HeaderSyncEvent::SessionWireMessage {
                peer: peer.clone(),
                session_id: 0,
                msg: HeaderSyncMessage::Status(remote_status.clone()),
            })
            .await
            .expect("a refreshed target status reaches the reactor");
        let observed_scope = match next_action(&mut actions).await {
            HeaderPortOperation::QueryHeaderLocator {
                target_tip_hash,
                scope,
                ..
            } if target_tip_hash == target => scope,
            other => panic!("expected refreshed locator query for target, got {other:?}"),
        };
        if observed_scope != stale_scope {
            break observed_scope;
        }
        tokio::task::yield_now().await;
    };

    handle
        .send(HeaderSyncEvent::HeaderLocatorReady {
            peer: peer.clone(),
            session_id: 0,
            target_tip_hash: target,
            scope: stale_scope,
            locator: Some(zakura_header_chain::HeaderLocator::for_continuation(anchor)),
        })
        .await
        .expect("the delayed locator reaches the reactor");
    assert!(
        time::timeout(std::time::Duration::from_millis(20), outbound.recv())
            .await
            .is_err(),
        "a stale locator cannot send GetHeaders under the new generation"
    );
    assert!(
        time::timeout(std::time::Duration::from_millis(20), actions.recv())
            .await
            .is_err(),
        "retiring a stale locator has no punishment or follow-on action"
    );

    assert_ne!(fresh_scope, stale_scope);
    handle
        .send(HeaderSyncEvent::HeaderLocatorReady {
            peer,
            session_id: 0,
            target_tip_hash: target,
            scope: fresh_scope,
            locator: Some(zakura_header_chain::HeaderLocator::for_continuation(anchor)),
        })
        .await
        .expect("the current locator reaches the reactor");
    assert!(matches!(
        handle
            .codec()
            .decode_frame(outbound.recv().await.expect("GetHeaders is sent"), None)
            .expect("GetHeaders decodes"),
        HeaderSyncMessage::GetHeaders(_)
    ));

    shutdown.cancel();
    task.await.expect("the reactor exits cleanly");
}

#[tokio::test]
async fn requester_stages_all_pages_before_one_exact_admission() {
    let shutdown = CancellationToken::new();
    let mut startup = startup(shutdown.clone());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let (snapshots_tx, snapshots_rx) = watch::channel(Some(committed_snapshot(anchor)));
    startup.committed_snapshots = Some(snapshots_rx);
    let (handle, mut actions, task) =
        spawn_header_sync_reactor(startup).expect("the requester fixture starts");
    let (send, mut outbound) = framed_channel(8);
    let peer = peer();
    handle
        .send(HeaderSyncEvent::PeerConnected(
            HeaderSyncPeerSession::from_parts(peer.clone(), send, CancellationToken::new()),
        ))
        .await
        .expect("the peer connects");
    let status_frame = outbound.recv().await.expect("initial status is sent");
    assert!(matches!(
        handle
            .codec()
            .decode_frame(status_frame, None)
            .expect("status decodes"),
        HeaderSyncMessage::Status(_)
    ));

    let mut first_header = *regtest_genesis_block().header;
    first_header.previous_block_hash = anchor.hash;
    first_header.time += chrono::Duration::seconds(1);
    let first_header = Arc::new(first_header);
    let first = zakura_header_chain::Frontier::new(block::Height(1), first_header.hash());
    let mut second_header = *regtest_genesis_block().header;
    second_header.previous_block_hash = first.hash;
    second_header.time += chrono::Duration::seconds(2);
    let second_header = Arc::new(second_header);
    let target = zakura_header_chain::Frontier::new(block::Height(2), second_header.hash());
    let remote_status = Status {
        work_anchor_height: anchor.height,
        work_anchor_hash: anchor.hash,
        selected_tip_height: target.height,
        selected_tip_hash: target.hash,
        suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(2_u8),
        oldest_retained_height: anchor.height,
        max_headers_per_response: 1,
        max_inflight_requests: 1,
        max_message_bytes: 2_000_000,
        tree_aux_schema_mask: 0,
    };
    handle
        .send(HeaderSyncEvent::SessionWireMessage {
            peer: peer.clone(),
            session_id: 0,
            msg: HeaderSyncMessage::Status(remote_status.clone()),
        })
        .await
        .expect("the target status reaches the reactor");
    let scope = match next_action(&mut actions).await {
        HeaderPortOperation::QueryHeaderLocator {
            target_tip_hash,
            scope,
            ..
        } if target_tip_hash == target.hash => scope,
        other => panic!("expected locator query for target, got {other:?}"),
    };
    handle
        .send(HeaderSyncEvent::HeaderLocatorReady {
            peer: peer.clone(),
            session_id: 0,
            target_tip_hash: target.hash,
            scope,
            locator: Some(zakura_header_chain::HeaderLocator::for_continuation(anchor)),
        })
        .await
        .expect("the locator reaches the reactor");
    let first_request = match handle
        .codec()
        .decode_frame(outbound.recv().await.expect("first request is sent"), None)
        .expect("first request decodes")
    {
        HeaderSyncMessage::GetHeaders(request) => request,
        other => panic!("expected GetHeaders, got {other:?}"),
    };
    handle
        .send(HeaderSyncEvent::SessionResponse {
            peer: peer.clone(),
            session_id: 0,
            scope,
            msg: HeaderSyncMessage::Headers(Headers {
                request_id: first_request.request_id,
                target_tip_hash: target.hash,
                common_ancestor_height: anchor.height,
                common_ancestor_hash: anchor.hash,
                complete: false,
                tree_aux_schema: AuxSchema::None,
                entries: vec![HeaderEntry {
                    header: first_header.clone(),
                    body_size: 0,
                    tree_aux: None,
                }],
            }),
        })
        .await
        .expect("the first response page reaches the reactor");
    let continuation = match handle
        .codec()
        .decode_frame(outbound.recv().await.expect("continuation is sent"), None)
        .expect("continuation decodes")
    {
        HeaderSyncMessage::GetHeaders(request) => request,
        other => panic!("expected continuation GetHeaders, got {other:?}"),
    };
    assert_eq!(continuation.locator_hashes, vec![first.hash]);
    handle
        .send(HeaderSyncEvent::SessionResponse {
            peer: peer.clone(),
            session_id: 0,
            scope,
            msg: HeaderSyncMessage::Headers(Headers {
                request_id: continuation.request_id,
                target_tip_hash: target.hash,
                common_ancestor_height: first.height,
                common_ancestor_hash: first.hash,
                complete: true,
                tree_aux_schema: AuxSchema::None,
                entries: vec![HeaderEntry {
                    header: second_header,
                    body_size: 0,
                    tree_aux: None,
                }],
            }),
        })
        .await
        .expect("the completion page reaches the reactor");
    let HeaderPortOperation::PrepareHeaderTarget {
        source,
        network,
        owner,
        common_ancestor,
        target: admitted_target,
        entries,
        ..
    } = next_action(&mut actions).await
    else {
        panic!("the complete target must produce one admission action");
    };
    assert_eq!(common_ancestor, anchor);
    assert_eq!(admitted_target, target);
    assert_eq!(entries.len(), 2);
    assert_eq!(owner.request_id.get(), first_request.request_id);
    let anchor_header = regtest_genesis_block().header.clone();
    let lease = zakura_header_chain::ValidationLease::new(
        anchor,
        vec![zakura_header_chain::HeaderContextFact {
            frontier: anchor,
            difficulty_threshold: anchor_header.difficulty_threshold,
            time: anchor_header.time,
        }],
        [9; 32],
    );
    let rules = zakura_header_chain::HeaderRules::for_validation_lease(network, &lease)
        .expect("the authenticated regtest policy is valid");
    let headers: Vec<_> = entries.iter().map(|entry| entry.header.clone()).collect();
    let batch = zakura_header_chain::prepare_headers(
        zakura_header_chain::HeaderBatchInput::new(&headers),
        &lease,
        &rules,
        &zakura_header_chain::SystemClock,
    )
    .expect("the requester fixture headers prepare");
    let insert = zakura_header_chain::InsertHeaders {
        owner,
        source,
        parent_hash: anchor.hash,
        target_tip_hash: target.hash,
        completion: zakura_header_chain::TargetCompletion::TargetComplete {
            common_ancestor: anchor,
        },
        batch,
        aux: Vec::new(),
    };
    let mut stale_owner = owner;
    stale_owner.session_id = stale_owner.session_id.saturating_add(1);
    let mut stale_insert = insert.clone();
    stale_insert.owner = stale_owner;
    handle
        .send(HeaderSyncEvent::HeaderTargetPrepared {
            peer: peer.clone(),
            source,
            owner: stale_owner,
            result: HeaderTargetPreparationResult::Prepared(Box::new(stale_insert)),
        })
        .await
        .expect("the stale preparation reaches the completion gate");
    assert!(
        time::timeout(std::time::Duration::from_millis(20), actions.recv())
            .await
            .is_err(),
        "a stale preparation has no state-call or peer-score action"
    );
    let mut mismatched_insert = insert.clone();
    mismatched_insert.source = zakura_header_chain::SourceId::from_digest([7; 32]);
    handle
        .send(HeaderSyncEvent::HeaderTargetPrepared {
            peer: peer.clone(),
            source,
            owner,
            result: HeaderTargetPreparationResult::Prepared(Box::new(mismatched_insert)),
        })
        .await
        .expect("the contradictory sealed evidence reaches the reactor");
    assert!(
        time::timeout(std::time::Duration::from_millis(20), actions.recv())
            .await
            .is_err(),
        "contradictory sealed evidence has no state-call or peer-score action"
    );
    handle
        .send(HeaderSyncEvent::HeaderTargetPrepared {
            peer: peer.clone(),
            source,
            owner,
            result: HeaderTargetPreparationResult::Prepared(Box::new(insert.clone())),
        })
        .await
        .expect("the preparation result reaches the reactor");
    assert!(matches!(
        next_action(&mut actions).await,
        HeaderPortOperation::ApplyHeaderTarget {
            owner: actual_owner,
            insert: actual_insert,
            ..
        } if actual_owner == owner && *actual_insert == insert
    ));
    handle
        .send(HeaderSyncEvent::HeaderTargetPrepared {
            peer: peer.clone(),
            source,
            owner,
            result: HeaderTargetPreparationResult::Prepared(Box::new(insert.clone())),
        })
        .await
        .expect("the duplicate preparation reaches the reactor");
    assert!(
        time::timeout(std::time::Duration::from_millis(20), actions.recv())
            .await
            .is_err(),
        "a duplicate preparation cannot submit a second state call"
    );
    handle
        .send(HeaderSyncEvent::HeaderTargetAdmissionReady {
            peer: peer.clone(),
            source: zakura_header_chain::SourceId::from_digest([8; 32]),
            owner,
            result: HeaderTargetAdmissionResult::Failed(invalid_header_failure(source, owner)),
        })
        .await
        .expect("the wrong-source state result reaches the reactor");
    assert!(
        time::timeout(std::time::Duration::from_millis(20), actions.recv())
            .await
            .is_err(),
        "a wrong-source state result cannot score or retire current work"
    );
    handle
        .send(HeaderSyncEvent::HeaderTargetAdmissionReady {
            peer: peer.clone(),
            source,
            owner,
            result: HeaderTargetAdmissionResult::Applied,
        })
        .await
        .expect("the admission result reaches the reactor");
    let mut advisory_height_changed = remote_status;
    advisory_height_changed.selected_tip_height = block::Height(200);
    handle
        .send(HeaderSyncEvent::SessionWireMessage {
            peer,
            session_id: 0,
            msg: HeaderSyncMessage::Status(advisory_height_changed),
        })
        .await
        .expect("the duplicate target with a changed advisory height reaches the reactor");
    assert!(
        time::timeout(std::time::Duration::from_millis(20), actions.recv())
            .await
            .is_err(),
        "exact target completion ignores a peer's changed advisory height"
    );

    drop(snapshots_tx);
    shutdown.cancel();
    task.await.expect("the reactor exits cleanly");
}

#[tokio::test]
async fn explicit_outcomes_are_nonpunitive_and_reschedule_after_status_refresh() {
    let shutdown = CancellationToken::new();
    let mut startup = startup(shutdown.clone());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(committed_snapshot(anchor)));
    startup.committed_snapshots = Some(snapshots_rx);
    let (handle, mut actions, task) =
        spawn_header_sync_reactor(startup).expect("the requester fixture starts");
    let (send, mut outbound) = framed_channel(16);
    let peer = peer();
    handle
        .send(HeaderSyncEvent::PeerConnected(
            HeaderSyncPeerSession::from_parts(peer.clone(), send, CancellationToken::new()),
        ))
        .await
        .expect("the peer connects");
    let _initial_status = outbound.recv().await.expect("initial status is sent");

    let target = block::Hash([0x42; 32]);
    let remote_status = Status {
        work_anchor_height: anchor.height,
        work_anchor_hash: anchor.hash,
        selected_tip_height: block::Height(2),
        selected_tip_hash: target,
        suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(2_u8),
        oldest_retained_height: anchor.height,
        max_headers_per_response: 1,
        max_inflight_requests: 1,
        max_message_bytes: 2_000_000,
        tree_aux_schema_mask: 0,
    };

    for outcome in [
        HeadersOutcomeCode::TargetNotRetained,
        HeadersOutcomeCode::HistoryPruned,
        HeadersOutcomeCode::Busy,
        HeadersOutcomeCode::NoLocatorIntersection,
    ] {
        handle
            .send(HeaderSyncEvent::SessionWireMessage {
                peer: peer.clone(),
                session_id: 0,
                msg: HeaderSyncMessage::Status(remote_status.clone()),
            })
            .await
            .expect("the refreshed status reaches the reactor");
        let scope = match next_action(&mut actions).await {
            HeaderPortOperation::QueryHeaderLocator {
                target_tip_hash,
                scope,
                ..
            } if target_tip_hash == target => scope,
            other => panic!("expected locator query for target, got {other:?}"),
        };
        handle
            .send(HeaderSyncEvent::HeaderLocatorReady {
                peer: peer.clone(),
                session_id: 0,
                target_tip_hash: target,
                scope,
                locator: Some(zakura_header_chain::HeaderLocator::for_continuation(anchor)),
            })
            .await
            .expect("the locator reaches the reactor");
        let request = match handle
            .codec()
            .decode_frame(outbound.recv().await.expect("the request is sent"), None)
            .expect("the request decodes")
        {
            HeaderSyncMessage::GetHeaders(request) => request,
            other => panic!("expected GetHeaders, got {other:?}"),
        };
        handle
            .send(HeaderSyncEvent::SessionResponse {
                peer: peer.clone(),
                session_id: 0,
                scope,
                msg: HeaderSyncMessage::HeadersOutcome(HeadersOutcome {
                    request_id: request.request_id,
                    target_tip_hash: target,
                    outcome,
                }),
            })
            .await
            .expect("the explicit outcome reaches the reactor");
    }

    handle
        .send(HeaderSyncEvent::SessionWireMessage {
            peer,
            session_id: 0,
            msg: HeaderSyncMessage::Status(remote_status),
        })
        .await
        .expect("the next bounded status refresh reaches the reactor");
    assert!(matches!(
        next_action(&mut actions).await,
        HeaderPortOperation::QueryHeaderLocator {
            target_tip_hash,
            ..
        } if target_tip_hash == target
    ));

    shutdown.cancel();
    task.await.expect("the reactor exits cleanly");
}
