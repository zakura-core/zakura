use super::*;
use zakura_node_services::header_chain as port;

pub(super) struct CapacityPort(pub(super) port::ServingCapacitySignal);

impl port::Port for CapacityPort {
    fn continuation_locator(
        &self,
    ) -> port::HeaderChainFuture<
        '_,
        Result<Option<zakura_header_chain::HeaderLocator>, port::PortError>,
    > {
        Box::pin(async { Ok(None) })
    }
    fn vct_repair_context(
        &self,
        _: zakura_header_chain::BodyWorkOwner,
        _: block::Height,
    ) -> port::HeaderChainFuture<'_, Result<port::VctRepairContextReply, port::PortError>> {
        Box::pin(async { Ok(port::VctRepairContextReply::Stale) })
    }
    fn acquire_header_path(
        &self,
        _: port::AcquirePath,
    ) -> port::HeaderChainFuture<'_, Result<port::AcquirePathReply, port::PortError>> {
        Box::pin(async { Ok(port::AcquirePathReply::CapacityBusy(self.0.clone())) })
    }
    fn read_header_path(
        &self,
        _: port::RetainedHeaderPath,
        _: port::ReadPath,
    ) -> port::HeaderChainFuture<'_, Result<port::ReadPathReply, port::PortError>> {
        panic!("the capacity fixture never acquires a path")
    }
    fn release_header_path(
        &self,
        _: port::RetainedHeaderPath,
    ) -> port::HeaderChainFuture<'_, Result<(), port::PortError>> {
        panic!("the capacity fixture never acquires a path")
    }
    fn prepare_header_target(
        &self,
        _: port::PrepareHeaderTarget,
    ) -> port::HeaderChainFuture<'_, port::PrepareHeaderTargetReply> {
        panic!("the capacity fixture only serves headers")
    }
    fn apply_header_target(
        &self,
        _: port::PreparedHeaderTarget,
    ) -> port::HeaderChainFuture<'_, port::ApplyHeaderTargetReply> {
        panic!("the capacity fixture only serves headers")
    }
}

#[tokio::test(start_paused = true)]
async fn port_capacity_release_preserves_busy_order_and_pending_status() {
    for release_before_reply in [false, true] {
        let signal = port::ServingCapacitySignal::default();
        let mut startup = startup(CancellationToken::new());
        let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
        let snapshot = committed_snapshot(anchor);
        let (_tx, rx) = watch::channel(Some(snapshot));
        startup.committed_snapshots = Some(rx);
        startup.header_chain_port = Arc::new(CapacityPort(signal.clone()));
        startup.port_dispatch = PortDispatch::Direct;
        let (_handle, _actions, mut reactor) = build_header_sync_reactor(startup).unwrap();
        let peer = peer();
        let (send, mut outbound) = framed_channel(1);
        reactor.handle_peer_connected(PeerSession::from_parts_with_session_id(
            peer.clone(),
            7,
            send,
            CancellationToken::new(),
        ));
        let initial = reactor
            .codec
            .decode_frame(outbound.try_recv().unwrap(), None)
            .unwrap();
        reactor.handle_get_headers(peer.clone(), 7, request(1, anchor.hash, anchor.hash));
        let completion = reactor.pending_port_operations.next().await.unwrap();
        if release_before_reply {
            signal.release();
        }
        reactor.handle_port_completion(completion);
        if !release_before_reply {
            assert!(HeaderSyncReactor::wait_for_capacity(&reactor.peer_state)
                .now_or_never()
                .is_none());
            signal.release();
        }
        HeaderSyncReactor::wait_for_capacity(&reactor.peer_state).await;
        time::advance(std::time::Duration::from_secs(1)).await;
        reactor.refresh_statuses();
        // Busy still fills the queue, so the failed status publication must remain pending.
        let busy = reactor
            .codec
            .decode_frame(outbound.try_recv().unwrap(), None)
            .unwrap();
        assert!(matches!(
            busy,
            HeaderSyncMessage::HeadersOutcome(HeadersOutcome {
                request_id: 1,
                outcome: HeadersOutcomeCode::Busy,
                ..
            })
        ));
        time::advance(std::time::Duration::from_millis(100)).await;
        reactor.refresh_statuses();
        let refreshed = reactor
            .codec
            .decode_frame(outbound.try_recv().unwrap(), None)
            .unwrap();
        assert_eq!(refreshed, initial);
        assert!(outbound.try_recv().is_err());
        assert!(reactor.peer_state[&peer].capacity_signal.is_none());
    }
}

#[tokio::test(start_paused = true)]
async fn replaced_session_discards_capacity_notification() {
    let signal = port::ServingCapacitySignal::default();
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let (_tx, rx) = watch::channel(Some(committed_snapshot(anchor)));
    startup.committed_snapshots = Some(rx);
    let (_handle, _actions, mut reactor) = build_header_sync_reactor(startup).unwrap();
    let peer = peer();
    let (send, _outbound) = framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts_with_session_id(
        peer.clone(),
        7,
        send,
        CancellationToken::new(),
    ));
    reactor.peer_state.get_mut(&peer).unwrap().capacity_signal = Some(signal.clone());
    reactor
        .peer_state
        .get_mut(&peer)
        .unwrap()
        .waiting_for_serving_slot = true;
    let (send, mut outbound) = framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts_with_session_id(
        peer.clone(),
        8,
        send,
        CancellationToken::new(),
    ));
    outbound.try_recv().unwrap();
    signal.release();
    time::advance(std::time::Duration::from_secs(1)).await;
    reactor.refresh_statuses();
    assert!(outbound.try_recv().is_err());
    assert!(reactor.peer_state[&peer].capacity_signal.is_none());
    assert!(!reactor.peer_state[&peer].waiting_for_serving_slot);
}

#[tokio::test(start_paused = true)]
async fn serving_slot_release_wakes_only_its_waiting_peer() {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let scope = zakura_header_chain::HeaderWorkAuthority::for_target(&snapshot, anchor.hash);
    let (_tx, rx) = watch::channel(Some(snapshot));
    startup.committed_snapshots = Some(rx);
    let (_handle, _actions, mut reactor) = build_header_sync_reactor(startup).unwrap();
    let mut outbounds = Vec::new();
    let peers: Vec<_> = [0x71, 0x72]
        .into_iter()
        .map(|marker| ZakuraPeerId::new(vec![marker; 32]).unwrap())
        .collect();
    for peer in &peers {
        let (send, mut outbound) = framed_channel(8);
        reactor.handle_peer_connected(PeerSession::from_parts_with_session_id(
            peer.clone(),
            7,
            send,
            CancellationToken::new(),
        ));
        outbound.try_recv().unwrap();
        reactor.handle_get_headers(peer.clone(), 7, request(1, anchor.hash, anchor.hash));
        reactor.handle_get_headers(peer.clone(), 7, request(2, anchor.hash, anchor.hash));
        let busy = reactor
            .codec
            .decode_frame(outbound.try_recv().unwrap(), None)
            .unwrap();
        assert!(matches!(
            busy,
            HeaderSyncMessage::HeadersOutcome(HeadersOutcome {
                request_id: 2,
                outcome: HeadersOutcomeCode::Busy,
                ..
            })
        ));
        outbounds.push(outbound);
    }
    reactor.handle_header_path_lease_ready(
        peers[0].clone(),
        7,
        scope,
        request(1, anchor.hash, anchor.hash),
        HeaderPathLeaseResult::Outcome(HeadersOutcomeCode::TargetNotRetained),
    );
    outbounds[0].try_recv().unwrap();
    time::advance(std::time::Duration::from_secs(1)).await;
    reactor.refresh_statuses();
    assert!(matches!(
        reactor
            .codec
            .decode_frame(outbounds[0].try_recv().unwrap(), None)
            .unwrap(),
        HeaderSyncMessage::Status(_)
    ));
    assert!(outbounds[1].try_recv().is_err());
    assert!(!reactor.peer_state[&peers[0]].waiting_for_serving_slot);
    assert!(reactor.peer_state[&peers[1]].waiting_for_serving_slot);
}

#[tokio::test(start_paused = true)]
async fn reactor_wakes_on_capacity_release_without_an_input_event() {
    let signal = port::ServingCapacitySignal::default();
    let shutdown = CancellationToken::new();
    let mut startup = startup(shutdown.clone());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let (_tx, rx) = watch::channel(Some(committed_snapshot(anchor)));
    startup.committed_snapshots = Some(rx);
    startup.header_chain_port = Arc::new(CapacityPort(signal.clone()));
    startup.port_dispatch = PortDispatch::Direct;
    let (_handle, _actions, mut reactor) = build_header_sync_reactor(startup).unwrap();
    let peer = peer();
    let (send, mut outbound) = framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts_with_session_id(
        peer.clone(),
        7,
        send,
        CancellationToken::new(),
    ));
    let codec = reactor.codec.clone();
    let initial = codec
        .decode_frame(outbound.try_recv().unwrap(), None)
        .unwrap();
    reactor.handle_get_headers(peer, 7, request(1, anchor.hash, anchor.hash));
    let task = tokio::spawn(reactor.run());
    let busy = time::timeout(std::time::Duration::from_millis(100), outbound.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        codec.decode_frame(busy, None).unwrap(),
        HeaderSyncMessage::HeadersOutcome(HeadersOutcome {
            outcome: HeadersOutcomeCode::Busy,
            ..
        })
    ));
    signal.release();
    let refreshed = time::timeout(std::time::Duration::from_millis(1100), outbound.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(codec.decode_frame(refreshed, None).unwrap(), initial);
    shutdown.cancel();
    task.await.unwrap();
}
