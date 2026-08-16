use super::*;
use crate::zakura::header_sync::scheduler::peer_work::{
    HEADER_CHUNK_BUDGET_CAPACITY_V1, MAX_HEADER_CHUNK_RESERVATION_V1,
};

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
        owner.session_id(),
        owner.header_authority(),
        Headers {
            request_id: owner.request_id().get(),
            target_tip_hash: owner.header_authority().branch.target_tip_hash,
            common_ancestor_height: target_height,
            common_ancestor_hash: owner.header_authority().branch.target_tip_hash,
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
        owner.session_id(),
        owner.header_authority(),
        Headers {
            request_id: owner.request_id().get(),
            target_tip_hash: owner.header_authority().branch.target_tip_hash,
            common_ancestor_height: block::Height(target_height.0.saturating_add(1)),
            common_ancestor_hash: owner.header_authority().branch.target_tip_hash,
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

#[test]
fn requester_admits_its_owned_prefix_when_the_chunk_budget_is_exhausted() {
    let (mut reactor, mut actions, snapshot, peer, _source, owner) = peer_violation_fixture();
    let active = reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the fixture has active work");
    let entry = active.entries[0].clone();
    active.phase = HeaderTargetPhase::Receiving;
    active.common_ancestor = Some(snapshot.frontiers.finalized);
    active.entries = vec![entry; HEADER_CHUNK_BUDGET_CAPACITY_V1 - 1];
    active.max_header_count = 1;
    let staged_tip = active
        .staged_tip()
        .expect("the bounded staged fixture has an inferred tip");
    let mut next_header = *regtest_genesis_block().header;
    next_header.previous_block_hash = staged_tip.hash;
    let next_entry = HeaderEntry {
        header: Arc::new(next_header),
        body_size: 0,
        tree_aux: None,
    };
    let _ = active;
    reactor
        .peer_work_queue
        .set_capacity_for_test(&peer, HEADER_CHUNK_BUDGET_CAPACITY_V1 - 1, 1);

    reactor.handle_headers(
        peer.clone(),
        owner.session_id(),
        owner.header_authority(),
        Headers {
            request_id: owner.request_id().get(),
            target_tip_hash: owner.header_authority().branch.target_tip_hash,
            common_ancestor_height: staged_tip.height,
            common_ancestor_hash: staged_tip.hash,
            complete: false,
            tree_aux_schema: AuxSchema::None,
            entries: vec![next_entry],
        },
    );

    let HeaderPortOperation::PrepareHeaderTarget {
        owner: prefix_owner,
        common_ancestor,
        target,
        completion,
        entries,
        ..
    } = actions
        .try_recv()
        .expect("the exhausted chunk budget prepares its owned prefix")
    else {
        panic!("the exhausted chunk budget must prepare a header target");
    };
    assert_eq!(common_ancestor, snapshot.frontiers.finalized);
    assert_eq!(entries.len(), HEADER_CHUNK_BUDGET_CAPACITY_V1);
    assert_eq!(
        target.height,
        block::Height(
            u32::try_from(HEADER_CHUNK_BUDGET_CAPACITY_V1)
                .expect("the owned header budget fits in a height")
        )
    );
    assert_eq!(
        prefix_owner.header_authority().branch.target_tip_hash,
        target.hash
    );
    assert_ne!(target.hash, owner.header_authority().branch.target_tip_hash);
    assert_eq!(
        completion,
        zakura_header_chain::TargetCompletion::TargetPrefix { common_ancestor }
    );
    assert!(matches!(
        reactor
            .peer_work_queue
            .active(&peer)
            .map(|active| active.phase),
        Some(HeaderTargetPhase::Preparing)
    ));
    assert!(
        reactor
            .peer_work_queue
            .active(&peer)
            .expect("preparation retains the active capacity owner")
            .entries
            .is_empty(),
        "staged entries move into preparation instead of remaining cloned"
    );
    assert_eq!(
        reactor.peer_work_queue.owned_header_count(&peer),
        HEADER_CHUNK_BUDGET_CAPACITY_V1,
        "moving entries does not release their RAII capacity before admission"
    );
    assert!(
        actions.try_recv().is_err(),
        "prefix preparation replaces the overflowing continuation request"
    );
}

#[test]
fn requester_admits_prefix_at_durable_graph_headroom_before_chunk_budget_is_full() {
    let (mut reactor, mut actions, snapshot, peer, _source, owner) = peer_violation_fixture();
    let durable_prefix_count = 2usize;
    let max_nodes = u32::try_from(zakura_header_chain::MAX_NON_FINALIZED_NODES_V1)
        .expect("the v1 retained-node limit fits a block height");
    reactor
        .committed_snapshot
        .as_mut()
        .expect("the fixture has a committed snapshot")
        .frontiers
        .header_best
        .height = block::Height(
        snapshot
            .frontiers
            .finalized
            .height
            .0
            .checked_add(max_nodes)
            .and_then(|height| {
                height.checked_sub(
                    u32::try_from(durable_prefix_count)
                        .expect("the fixture prefix count fits a block height"),
                )
            })
            .expect("the fixture divergence fits a block height"),
    );

    let active = reactor
        .peer_work_queue
        .active_mut(&peer)
        .expect("the fixture has active work");
    let entry = active.entries[0].clone();
    active.phase = HeaderTargetPhase::Receiving;
    active.common_ancestor = Some(snapshot.frontiers.finalized);
    active.entries = vec![entry];
    active.max_header_count = MAX_HS_RANGE;
    let staged_tip = active
        .staged_tip()
        .expect("the staged fixture has an inferred tip");
    let mut next_header = *regtest_genesis_block().header;
    next_header.previous_block_hash = staged_tip.hash;
    let next_entry = HeaderEntry {
        header: Arc::new(next_header),
        body_size: 0,
        tree_aux: None,
    };
    let _ = active;
    reactor.peer_work_queue.set_capacity_for_test(&peer, 1, 1);

    reactor.handle_headers(
        peer.clone(),
        owner.session_id(),
        owner.header_authority(),
        Headers {
            request_id: owner.request_id().get(),
            target_tip_hash: owner.header_authority().branch.target_tip_hash,
            common_ancestor_height: staged_tip.height,
            common_ancestor_hash: staged_tip.hash,
            complete: false,
            tree_aux_schema: AuxSchema::None,
            entries: vec![next_entry],
        },
    );

    let HeaderPortOperation::PrepareHeaderTarget {
        completion,
        entries,
        ..
    } = actions
        .try_recv()
        .expect("durable graph headroom seals the admissible prefix")
    else {
        panic!("durable graph headroom must prepare a header target");
    };
    assert_eq!(entries.len(), durable_prefix_count);
    assert!(matches!(
        completion,
        zakura_header_chain::TargetCompletion::TargetPrefix { .. }
    ));
    assert_eq!(
        reactor.peer_work_queue.chunk_budget_usage(),
        (0, durable_prefix_count),
        "the prefix seals before exhausting the independent in-memory chunk budget"
    );
    assert!(actions.try_recv().is_err());
}

#[test]
fn durable_prefix_headroom_accounts_for_staged_headers_at_exact_limits() {
    let anchor =
        zakura_header_chain::Frontier::new(block::Height(10), regtest_genesis_block().hash());
    let mut snapshot = committed_snapshot(anchor);
    let max_nodes = u32::try_from(zakura_header_chain::MAX_NON_FINALIZED_NODES_V1)
        .expect("the v1 retained-node limit fits a block height");

    assert_eq!(
        HeaderSyncReactor::durable_header_prefix_remaining(&snapshot, 0),
        max_nodes
    );
    snapshot.frontiers.header_best.height = block::Height(
        anchor
            .height
            .0
            .checked_add(max_nodes)
            .and_then(|height| height.checked_sub(2))
            .expect("the exact-bound fixture fits a block height"),
    );
    assert_eq!(
        HeaderSyncReactor::durable_header_prefix_remaining(&snapshot, 0),
        2
    );
    assert_eq!(
        HeaderSyncReactor::durable_header_prefix_remaining(&snapshot, 1),
        1
    );
    assert_eq!(
        HeaderSyncReactor::durable_header_prefix_remaining(&snapshot, 2),
        0
    );
    assert_eq!(
        HeaderSyncReactor::durable_header_prefix_remaining(&snapshot, 3),
        0,
        "overfull fixtures fail closed instead of wrapping headroom"
    );
}

#[test]
fn integrated_request_headroom_refills_at_half_window_and_admits_the_final_prefix() {
    let anchor =
        zakura_header_chain::Frontier::new(block::Height(10), regtest_genesis_block().hash());
    let mut snapshot = committed_snapshot(anchor);
    let remote_tip = block::Height(20_000);

    assert_eq!(
        HeaderSyncReactor::request_header_prefix_remaining(&snapshot, 0, remote_tip),
        MAX_HS_RANGE
    );

    snapshot.frontiers.header_best.height = block::Height(anchor.height.0 + MAX_HS_RANGE - 1);
    assert_eq!(
        HeaderSyncReactor::request_header_prefix_remaining(&snapshot, 0, remote_tip),
        0,
        "a nearly full window does not cause one-header durable transitions"
    );
    assert_eq!(
        HeaderSyncReactor::request_header_prefix_remaining(
            &snapshot,
            0,
            block::Height(snapshot.frontiers.header_best.height.0 + 1),
        ),
        1,
        "the exact final partial target remains reachable"
    );

    snapshot.frontiers.header_best.height = block::Height(anchor.height.0 + 750);
    snapshot.frontiers.verified_best.height = block::Height(anchor.height.0 + 400);
    assert_eq!(
        HeaderSyncReactor::request_header_prefix_remaining(&snapshot, 0, remote_tip),
        MAX_HS_RANGE - 350,
        "a partial native admission cannot starve the next checkpoint range"
    );
    assert_eq!(
        HeaderSyncReactor::request_header_prefix_remaining(&snapshot, 1, remote_tip),
        MAX_HS_RANGE - 351,
        "staged entries consume the already-open low-water refill"
    );

    snapshot.frontiers.finalized.height = block::Height(anchor.height.0 + 400);
    snapshot.frontiers.header_best.height =
        block::Height(anchor.height.0 + INTEGRATED_HEADER_REFILL_LOW_WATER_V1 + 400);
    snapshot.frontiers.verified_best.height = block::Height(anchor.height.0 + 400);
    assert_eq!(
        HeaderSyncReactor::request_header_prefix_remaining(&snapshot, 0, remote_tip),
        MAX_HS_RANGE - INTEGRATED_HEADER_REFILL_LOW_WATER_V1,
        "the half-window boundary opens enough headroom to refill the durable prefix"
    );

    snapshot.frontiers.header_best.height =
        block::Height(anchor.height.0 + INTEGRATED_HEADER_REFILL_LOW_WATER_V1 + 400 + 1);
    assert_eq!(
        HeaderSyncReactor::request_header_prefix_remaining(&snapshot, 0, remote_tip),
        0,
        "one block above half-window low water remains closed"
    );
}

#[tokio::test]
async fn requester_shares_durable_graph_headroom_across_wire_requests() {
    let shutdown = CancellationToken::new();
    let mut startup = startup(shutdown.clone());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let mut snapshot = committed_snapshot(anchor);
    let durable_prefix_count = 2_u32;
    let max_nodes = u32::try_from(zakura_header_chain::MAX_NON_FINALIZED_NODES_V1)
        .expect("the v1 retained-node limit fits a block height");
    snapshot.frontiers.header_best = zakura_header_chain::Frontier::new(
        block::Height(
            anchor
                .height
                .0
                .checked_add(max_nodes)
                .and_then(|height| height.checked_sub(durable_prefix_count))
                .expect("the wire fixture divergence fits a block height"),
        ),
        block::Hash([0x51; 32]),
    );
    snapshot.frontiers.verified_best = snapshot.frontiers.header_best;
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    let (handle, mut actions, task) =
        spawn_header_sync_reactor(startup).expect("the requester fixture starts");

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

    let target = block::Hash([0x52; 32]);
    let remote_status = Status {
        work_anchor_height: anchor.height,
        work_anchor_hash: anchor.hash,
        selected_tip_height: block::Height(
            snapshot
                .frontiers
                .header_best
                .height
                .0
                .checked_add(100)
                .expect("the remote target height fits a block height"),
        ),
        selected_tip_hash: target,
        suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(2_u8),
        oldest_retained_height: anchor.height,
        max_headers_per_response: u32::MAX,
        max_inflight_requests: 1,
        max_message_bytes: 2_000_000,
        tree_aux_schema_mask: 0,
    };
    handle
        .send(Event::WireMessage {
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
        } if target_tip_hash == target => scope,
        other => panic!("expected locator query for target, got {other:?}"),
    };
    handle
        .send(Event::HeaderLocatorReady {
            peer: peer.clone(),
            session_id: 0,
            target_tip_hash: target,
            scope,
            locator: Some(zakura_header_chain::HeaderLocator::for_continuation(
                snapshot.frontiers.header_best,
            )),
        })
        .await
        .expect("the locator reaches the reactor");
    let request = match handle
        .codec()
        .decode_frame(outbound.recv().await.expect("GetHeaders is sent"), None)
        .expect("GetHeaders decodes")
    {
        HeaderSyncMessage::GetHeaders(request) => request,
        other => panic!("expected GetHeaders, got {other:?}"),
    };
    assert_eq!(request.max_header_count, durable_prefix_count);

    let (second_send, mut second_outbound) = framed_channel(8);
    let second_peer =
        ZakuraPeerId::new(vec![0x72; 32]).expect("the second peer ID has the required length");
    handle
        .send(Event::PeerConnected(PeerSession::from_parts(
            second_peer.clone(),
            second_send,
            CancellationToken::new(),
        )))
        .await
        .expect("the second peer connects");
    let _second_status = second_outbound
        .recv()
        .await
        .expect("the second peer receives initial status");
    let second_target = block::Hash([0x53; 32]);
    let mut second_remote_status = remote_status;
    second_remote_status.selected_tip_hash = second_target;
    handle
        .send(Event::WireMessage {
            peer: second_peer.clone(),
            session_id: 0,
            msg: HeaderSyncMessage::Status(second_remote_status),
        })
        .await
        .expect("the second target status reaches the reactor");
    let second_scope = match next_action(&mut actions).await {
        HeaderPortOperation::QueryHeaderLocator {
            target_tip_hash,
            scope,
            ..
        } if target_tip_hash == second_target => scope,
        other => panic!("expected second locator query, got {other:?}"),
    };
    handle
        .send(Event::HeaderLocatorReady {
            peer: second_peer,
            session_id: 0,
            target_tip_hash: second_target,
            scope: second_scope,
            locator: Some(zakura_header_chain::HeaderLocator::for_continuation(
                snapshot.frontiers.header_best,
            )),
        })
        .await
        .expect("the second locator reaches the reactor");
    assert!(
        time::timeout(std::time::Duration::from_millis(20), second_outbound.recv())
            .await
            .is_err(),
        "the first request's durable reservation prevents a duplicate wire request"
    );

    let mut first_header = *regtest_genesis_block().header;
    first_header.previous_block_hash = snapshot.frontiers.header_best.hash;
    first_header.time += chrono::Duration::seconds(1);
    let first_header = Arc::new(first_header);
    handle
        .send(Event::SessionResponse {
            peer,
            session_id: 0,
            scope,
            msg: HeaderSyncMessage::Headers(Headers {
                request_id: request.request_id,
                target_tip_hash: target,
                common_ancestor_height: snapshot.frontiers.header_best.height,
                common_ancestor_hash: snapshot.frontiers.header_best.hash,
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
        .expect("the partial response reaches the reactor");
    let continuation = match handle
        .codec()
        .decode_frame(
            outbound.recv().await.expect("the continuation is sent"),
            None,
        )
        .expect("the continuation decodes")
    {
        HeaderSyncMessage::GetHeaders(request) => request,
        other => panic!("expected continuation GetHeaders, got {other:?}"),
    };
    assert_eq!(continuation.max_header_count, 1);
    assert_eq!(continuation.locator_hashes, vec![first_header.hash()]);

    shutdown.cancel();
    task.await.expect("the reactor exits cleanly");
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
        .send(Event::PeerConnected(PeerSession::from_parts(
            peer.clone(),
            send,
            CancellationToken::new(),
        )))
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
        .send(Event::WireMessage {
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

    let fresh_scope = match next_action(&mut actions).await {
        HeaderPortOperation::QueryHeaderLocator {
            target_tip_hash,
            scope,
            ..
        } if target_tip_hash == target => scope,
        other => panic!("expected refreshed locator query for target, got {other:?}"),
    };

    handle
        .send(Event::HeaderLocatorReady {
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
        .send(Event::HeaderLocatorReady {
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
    let network = startup.network.clone();
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let (snapshots_tx, snapshots_rx) = watch::channel(Some(committed_snapshot(anchor)));
    startup.committed_snapshots = Some(snapshots_rx);
    let (handle, mut actions, task) =
        spawn_header_sync_reactor(startup).expect("the requester fixture starts");
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
        max_headers_per_response: u32::MAX,
        max_inflight_requests: 1,
        max_message_bytes: 2_000_000,
        tree_aux_schema_mask: 0,
    };
    handle
        .send(Event::WireMessage {
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
        .send(Event::HeaderLocatorReady {
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
    assert_eq!(
        first_request.max_header_count,
        u32::try_from(MAX_HEADER_CHUNK_RESERVATION_V1)
            .expect("the fair reservation fits on the wire")
    );
    handle
        .send(Event::SessionResponse {
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
        .send(Event::SessionResponse {
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
    assert_eq!(owner.request_id().get(), first_request.request_id);
    let anchor_header = regtest_genesis_block().header.clone();
    let lease = zakura_header_chain::ValidationLease::new(
        anchor,
        vec![zakura_header_chain::HeaderContextFact {
            frontier: anchor,
            header: anchor_header,
        }],
        network.clone(),
        [9; 32],
    );
    let rules = zakura_header_chain::HeaderRules::for_validation_lease(&lease)
        .expect("the authenticated regtest policy is valid");
    let headers: Vec<_> = entries.iter().map(|entry| entry.header.clone()).collect();
    let batch = zakura_header_chain::prepare_headers(
        zakura_header_chain::HeaderBatchInput::new(&headers),
        lease.parent(),
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
    let adapter_key = zakura_node_services::header_chain::AdapterKey::new();
    let foreign_key = zakura_node_services::header_chain::AdapterKey::new();
    let sealed = zakura_node_services::header_chain::PreparedHeaderTarget::from_insert(
        &adapter_key,
        Box::new(insert.clone()),
    );
    assert!(
        sealed.into_insert(&foreign_key).is_err(),
        "a different port instance cannot unseal and rebind a prepared target"
    );
    let exact_owner = owner
        .header_owner()
        .expect("the fixture is ordinary header work");
    let stale_owner: zakura_header_chain::HeaderSyncWorkOwner =
        zakura_header_chain::HeaderWorkOwner {
            session_id: exact_owner.session_id.saturating_add(1),
            ..exact_owner
        }
        .into();
    let mut stale_insert = insert.clone();
    stale_insert.owner = stale_owner;
    handle
        .send(Event::HeaderTargetPrepared {
            peer: peer.clone(),
            source,
            owner: stale_owner,
            result: HeaderTargetPreparationResult::Prepared(
                zakura_node_services::header_chain::PreparedHeaderTarget::from_insert(
                    &adapter_key,
                    Box::new(stale_insert),
                ),
            ),
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
        .send(Event::HeaderTargetPrepared {
            peer: peer.clone(),
            source,
            owner,
            result: HeaderTargetPreparationResult::Prepared(
                zakura_node_services::header_chain::PreparedHeaderTarget::from_insert(
                    &adapter_key,
                    Box::new(mismatched_insert),
                ),
            ),
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
        .send(Event::HeaderTargetPrepared {
            peer: peer.clone(),
            source,
            owner,
            result: HeaderTargetPreparationResult::Prepared(
                zakura_node_services::header_chain::PreparedHeaderTarget::from_insert(
                    &adapter_key,
                    Box::new(insert.clone()),
                ),
            ),
        })
        .await
        .expect("the preparation result reaches the reactor");
    let HeaderPortOperation::ApplyHeaderTarget {
        owner: actual_owner,
        target: actual_target,
        ..
    } = next_action(&mut actions).await
    else {
        panic!("the prepared target must produce one apply operation");
    };
    assert_eq!(actual_owner, owner);
    assert_eq!(
        *actual_target
            .into_insert(&adapter_key)
            .expect("the fixture adapter opens its own sealed target"),
        insert
    );
    handle
        .send(Event::HeaderTargetPrepared {
            peer: peer.clone(),
            source,
            owner,
            result: HeaderTargetPreparationResult::Prepared(
                zakura_node_services::header_chain::PreparedHeaderTarget::from_insert(
                    &adapter_key,
                    Box::new(insert.clone()),
                ),
            ),
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
        .send(Event::HeaderTargetAdmissionReady {
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
        .send(Event::HeaderTargetAdmissionReady {
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
        .send(Event::WireMessage {
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
        .send(Event::PeerConnected(PeerSession::from_parts(
            peer.clone(),
            send,
            CancellationToken::new(),
        )))
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
            .send(Event::WireMessage {
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
            .send(Event::HeaderLocatorReady {
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
            .send(Event::SessionResponse {
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
        .send(Event::WireMessage {
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
