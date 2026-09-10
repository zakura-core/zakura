use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::Notify;

use super::*;
use crate::zakura::testkit::{TraceCapture, TraceValue};
use crate::zakura::CloseCause;
use zakura_node_services::header_chain as port;

#[derive(Debug)]
struct PanickingPort {
    release: Option<Arc<Notify>>,
    vct_release: Option<Arc<Notify>>,
}

impl port::Port for PanickingPort {
    fn continuation_locator(
        &self,
    ) -> port::HeaderChainFuture<
        '_,
        Result<Option<zakura_header_chain::HeaderLocator>, port::PortError>,
    > {
        let release = self.release.clone();
        Box::pin(async move {
            if let Some(release) = release {
                release.notified().await;
            }
            panic!("unbounded panic payload must remain inside the port boundary")
        })
    }

    fn vct_repair_context(
        &self,
        _owner: zakura_header_chain::BodyWorkOwner,
        _height: block::Height,
    ) -> port::HeaderChainFuture<'_, Result<port::VctRepairContextReply, port::PortError>> {
        let release = self.vct_release.clone();
        Box::pin(async move {
            if let Some(release) = release {
                release.notified().await;
            }
            panic!("internal repair port panic")
        })
    }

    fn acquire_header_path(
        &self,
        _request: port::AcquirePath,
    ) -> port::HeaderChainFuture<'_, Result<port::AcquirePathReply, port::PortError>> {
        Box::pin(async { Ok(port::AcquirePathReply::TargetNotRetained) })
    }

    fn read_header_path(
        &self,
        _path: port::RetainedHeaderPath,
        _request: port::ReadPath,
    ) -> port::HeaderChainFuture<'_, Result<port::ReadPathReply, port::PortError>> {
        Box::pin(async { Ok(port::ReadPathReply::Unavailable) })
    }

    fn release_header_path(
        &self,
        _path: port::RetainedHeaderPath,
    ) -> port::HeaderChainFuture<'_, Result<(), port::PortError>> {
        Box::pin(async { Ok(()) })
    }

    fn prepare_header_target(
        &self,
        request: port::PrepareHeaderTarget,
    ) -> port::HeaderChainFuture<'_, port::PrepareHeaderTargetReply> {
        Box::pin(async move {
            Err(Arc::new(
                zakura_header_chain::HeaderChainError::local_resource(
                    zakura_header_chain::ErrorSubject::Branch(
                        request.owner.header_authority().branch,
                    ),
                    None,
                ),
            ))
        })
    }

    fn apply_header_target(
        &self,
        target: port::PreparedHeaderTarget,
    ) -> port::HeaderChainFuture<'_, port::ApplyHeaderTargetReply> {
        let owner = target.owner();
        Box::pin(async move {
            Err(Arc::new(
                zakura_header_chain::HeaderChainError::local_resource(
                    zakura_header_chain::ErrorSubject::Branch(owner.header_authority().branch),
                    None,
                ),
            ))
        })
    }
}

fn direct_reactor(port: Arc<dyn port::Port>) -> HeaderSyncReactor {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot));
    startup.committed_snapshots = Some(snapshots_rx);
    startup.header_chain_port = port;
    startup.use_direct_port();
    let (_, _, reactor) =
        build_header_sync_reactor(startup).expect("the direct-port fixture builds");
    reactor
}

fn connected_session(
    reactor: &mut HeaderSyncReactor,
    peer: ZakuraPeerId,
    session_id: u64,
) -> (CancellationToken, CloseCause) {
    let (send, _outbound) = crate::zakura::framed_channel(8);
    let service_cancel = CancellationToken::new();
    let connection_cancel = CancellationToken::new();
    let close_cause = CloseCause::new();
    reactor.handle_peer_connected(PeerSession::from_parts_with_connection(
        peer,
        session_id,
        send,
        service_cancel,
        connection_cancel.clone(),
        close_cause.clone(),
    ));
    (connection_cancel, close_cause)
}

#[tokio::test]
async fn port_future_panic_disconnects_exact_session_and_reactor_survives() {
    let mut reactor = direct_reactor(Arc::new(PanickingPort {
        release: None,
        vct_release: None,
    }));
    let peer = peer();
    let session_id = 7;
    let (connection_cancel, close_cause) =
        connected_session(&mut reactor, peer.clone(), session_id);
    let snapshot = reactor
        .committed_snapshot
        .clone()
        .expect("the fixture has a committed snapshot");
    let target = block::Hash([0x81; 32]);
    let scope = zakura_header_chain::HeaderWorkAuthority::for_target(&snapshot, target);

    assert!(
        reactor.dispatch_action(HeaderPortOperation::QueryHeaderLocator {
            peer: peer.clone(),
            session_id,
            target_tip_hash: target,
            scope,
        })
    );
    let completion = reactor
        .pending_port_operations
        .next()
        .await
        .expect("the panicking operation completes at its unwind boundary");
    reactor.handle_port_completion(completion);

    assert!(connection_cancel.is_cancelled());
    assert_eq!(close_cause.get_or("missing"), "header_port_panic");
    assert!(!reactor.request_deadlines.contains_key(&peer));

    let second_peer =
        ZakuraPeerId::new(vec![0x82; 32]).expect("the second peer ID has the required length");
    let (second_cancel, _) = connected_session(&mut reactor, second_peer.clone(), 8);
    assert!(!second_cancel.is_cancelled());
    assert!(reactor.peer_state.contains_key(&second_peer));
}

#[tokio::test]
async fn stale_port_panic_does_not_disconnect_replacement_session() {
    let release = Arc::new(Notify::new());
    let mut reactor = direct_reactor(Arc::new(PanickingPort {
        release: Some(release.clone()),
        vct_release: None,
    }));
    let peer = peer();
    let (old_cancel, _) = connected_session(&mut reactor, peer.clone(), 10);
    let snapshot = reactor
        .committed_snapshot
        .clone()
        .expect("the fixture has a committed snapshot");
    let target = block::Hash([0x83; 32]);
    let scope = zakura_header_chain::HeaderWorkAuthority::for_target(&snapshot, target);
    assert!(
        reactor.dispatch_action(HeaderPortOperation::QueryHeaderLocator {
            peer: peer.clone(),
            session_id: 10,
            target_tip_hash: target,
            scope,
        })
    );

    let (replacement_cancel, _) = connected_session(&mut reactor, peer.clone(), 11);
    release.notify_one();
    let completion = reactor
        .pending_port_operations
        .next()
        .await
        .expect("the stale operation reaches its panic boundary");
    reactor.handle_port_completion(completion);

    assert!(old_cancel.is_cancelled());
    assert!(!replacement_cancel.is_cancelled());
    assert_eq!(
        reactor
            .peer_state
            .get(&peer)
            .expect("the replacement remains admitted")
            .session
            .session_id(),
        11
    );
}

#[tokio::test]
async fn internal_vct_port_panic_is_contained_without_peer_disconnect() {
    let mut reactor = direct_reactor(Arc::new(PanickingPort {
        release: None,
        vct_release: None,
    }));
    let peer = peer();
    let (connection_cancel, _) = connected_session(&mut reactor, peer.clone(), 7);
    let snapshot = reactor
        .committed_snapshot
        .clone()
        .expect("the fixture has a committed snapshot");
    let owner = zakura_header_chain::BodyWorkAuthority::for_snapshot(&snapshot).bind(
        INTERNAL_VCT_REPAIR_SESSION_ID,
        std::num::NonZeroU64::new(1).expect("one is nonzero"),
    );

    assert!(
        reactor.dispatch_action(HeaderPortOperation::QueryVctRepairContext {
            owner,
            height: block::Height(1),
        })
    );
    let completion = reactor
        .pending_port_operations
        .next()
        .await
        .expect("the repair panic reaches its unwind boundary");
    reactor.handle_port_completion(completion);

    assert!(!connection_cancel.is_cancelled());
    assert!(reactor.peer_state.contains_key(&peer));
}

#[tokio::test(start_paused = true)]
async fn hanging_vct_port_future_is_removed_at_the_request_deadline() {
    let mut reactor = direct_reactor(Arc::new(PanickingPort {
        release: None,
        vct_release: Some(Arc::new(Notify::new())),
    }));
    let snapshot = reactor
        .committed_snapshot
        .clone()
        .expect("the fixture has a committed snapshot");
    let owner = zakura_header_chain::BodyWorkAuthority::for_snapshot(&snapshot).bind(
        INTERNAL_VCT_REPAIR_SESSION_ID,
        std::num::NonZeroU64::new(1).expect("one is nonzero"),
    );
    let started = Instant::now();

    assert!(
        reactor.dispatch_action(HeaderPortOperation::QueryVctRepairContext {
            owner,
            height: block::Height(1),
        })
    );
    let completion = reactor
        .pending_port_operations
        .next()
        .await
        .expect("the direct port timeout completes the hung operation");
    reactor.handle_port_completion(completion);

    assert!(Instant::now().duration_since(started) >= reactor.startup.request_timeout);
    assert!(reactor.pending_port_operations.is_empty());
}

#[tokio::test]
async fn port_panic_trace_is_bounded_and_request_scope_serializes_null_generation() {
    let mut capture =
        TraceCapture::for_test("port_panic_trace_is_bounded").expect("trace capture starts");
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    startup.header_chain_port = Arc::new(PanickingPort {
        release: None,
        vct_release: None,
    });
    startup.trace = crate::zakura::ZakuraTrace::new(capture.tracer(), "panic-test");
    startup.use_direct_port();
    let (_, _, mut reactor) =
        build_header_sync_reactor(startup).expect("the traced reactor builds");
    let peer = peer();
    let (connection_cancel, _) = connected_session(&mut reactor, peer.clone(), 7);
    let target = block::Hash([0x91; 32]);
    let scope = zakura_header_chain::HeaderWorkAuthority::for_target(&snapshot, target);
    reactor.emit_header_request(
        &peer,
        7,
        scope,
        HeaderSyncRequestId::new(1).expect("one is nonzero"),
        target,
        &zakura_header_chain::HeaderLocator::for_continuation(anchor),
        1,
        AuxSchema::V1,
    );
    assert!(
        reactor.dispatch_action(HeaderPortOperation::QueryHeaderLocator {
            peer,
            session_id: 7,
            target_tip_hash: target,
            scope,
        })
    );
    let completion = reactor
        .pending_port_operations
        .next()
        .await
        .expect("the panic is contained");
    reactor.handle_port_completion(completion);
    assert!(connection_cancel.is_cancelled());

    capture.flush().await;
    let reader = capture.reader().expect("the trace reloads");
    let header_trace = reader.table(HEADER_SYNC_TABLE.table());
    header_trace.assert_row(
        hs_trace::HEADER_REQUEST_SENT,
        &[
            (hs_trace::VERIFIED_GENERATION, TraceValue::Null),
            (
                hs_trace::STREAM_VERSION,
                TraceValue::U64(u64::from(ZAKURA_HEADER_SYNC_STREAM_VERSION)),
            ),
        ],
    );
    header_trace.assert_row(
        hs_trace::HEADER_PEER_VIOLATION,
        &[
            (hs_trace::BOUNDARY, TraceValue::Str("port")),
            (hs_trace::DISPOSITION, TraceValue::Str("disconnect")),
            (hs_trace::OPERATION, TraceValue::Str("query_header_locator")),
        ],
    );
    let encoded = serde_json::to_string(&header_trace.rows()).expect("trace rows serialize");
    assert!(!encoded.contains("unbounded panic payload"));
    assert!(!encoded.contains("\"entries\""));
    assert!(!encoded.contains("\"locator_hashes\""));
    let _ = capture.finish().await.expect("trace capture finishes");
}

#[derive(Debug, Default)]
struct LocatorCounters {
    started: std::sync::atomic::AtomicUsize,
}

#[derive(Clone, Debug)]
struct StalledLocatorPort {
    counters: Arc<LocatorCounters>,
    release: Arc<Notify>,
    locator: zakura_header_chain::HeaderLocator,
}

impl port::Port for StalledLocatorPort {
    fn continuation_locator(
        &self,
    ) -> port::HeaderChainFuture<
        '_,
        Result<Option<zakura_header_chain::HeaderLocator>, port::PortError>,
    > {
        let counters = self.counters.clone();
        let release = self.release.clone();
        let locator = self.locator.clone();
        Box::pin(async move {
            counters
                .started
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            release.notified().await;
            Ok(Some(locator))
        })
    }

    fn vct_repair_context(
        &self,
        _owner: zakura_header_chain::BodyWorkOwner,
        _height: block::Height,
    ) -> port::HeaderChainFuture<'_, Result<port::VctRepairContextReply, port::PortError>> {
        Box::pin(async { Err(port::PortError::Unavailable { source: None }) })
    }

    fn acquire_header_path(
        &self,
        _request: port::AcquirePath,
    ) -> port::HeaderChainFuture<'_, Result<port::AcquirePathReply, port::PortError>> {
        Box::pin(async { Ok(port::AcquirePathReply::TargetNotRetained) })
    }

    fn read_header_path(
        &self,
        _path: port::RetainedHeaderPath,
        _request: port::ReadPath,
    ) -> port::HeaderChainFuture<'_, Result<port::ReadPathReply, port::PortError>> {
        Box::pin(async { Ok(port::ReadPathReply::Unavailable) })
    }

    fn release_header_path(
        &self,
        _path: port::RetainedHeaderPath,
    ) -> port::HeaderChainFuture<'_, Result<(), port::PortError>> {
        Box::pin(async { Ok(()) })
    }

    fn prepare_header_target(
        &self,
        _request: port::PrepareHeaderTarget,
    ) -> port::HeaderChainFuture<'_, port::PrepareHeaderTargetReply> {
        Box::pin(async { panic!("the stalled-locator fixture never reaches target preparation") })
    }

    fn apply_header_target(
        &self,
        _target: port::PreparedHeaderTarget,
    ) -> port::HeaderChainFuture<'_, port::ApplyHeaderTargetReply> {
        Box::pin(async { panic!("the stalled-locator fixture never reaches target application") })
    }
}

fn churn_status(anchor: zakura_header_chain::Frontier, marker: u8) -> Status {
    let mut target_hash = [0u8; 32];
    target_hash[0] = 0xA5;
    target_hash[1] = marker;
    Status {
        work_anchor_height: anchor.height,
        work_anchor_hash: anchor.hash,
        selected_tip_height: block::Height(u32::from(marker) + 1),
        selected_tip_hash: block::Hash(target_hash),
        suffix_cumulative_work: zakura_chain::work::difficulty::U256::from(1_u8),
        oldest_retained_height: anchor.height,
        max_headers_per_response: 16,
        max_inflight_requests: 1,
        max_message_bytes: 1_000_000,
        tree_aux_schema_mask: 0,
    }
}

#[tokio::test]
async fn status_tip_churn_coalesces_to_one_in_flight_locator_query() {
    let counters = Arc::new(LocatorCounters::default());
    let release = Arc::new(Notify::new());
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot));
    startup.committed_snapshots = Some(snapshots_rx);
    startup.header_chain_port = Arc::new(StalledLocatorPort {
        counters: counters.clone(),
        release: release.clone(),
        locator: zakura_header_chain::HeaderLocator::for_continuation(anchor),
    });
    startup.use_direct_port();
    let (_, _, mut reactor) =
        build_header_sync_reactor(startup).expect("the direct-port fixture builds");

    let peer = peer();
    let session_id = 7;
    let (send, mut outbound) = crate::zakura::framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts_with_connection(
        peer.clone(),
        session_id,
        send,
        CancellationToken::new(),
        CancellationToken::new(),
        crate::zakura::CloseCause::new(),
    ));
    let _ = outbound.recv().await;

    const STATUS_CHURN: usize = zakura_header_chain::MAX_STAGED_TARGETS_V1 + 8;
    for marker in 0..STATUS_CHURN {
        reactor.handle_wire_message(
            peer.clone(),
            session_id,
            HeaderSyncMessage::Status(churn_status(anchor, marker as u8)),
        );
    }

    assert_eq!(reactor.pending_port_operations.len(), 1);
    assert_eq!(reactor.pending_locator_queries.len(), 1);

    let latest = churn_status(anchor, (STATUS_CHURN - 1) as u8);
    assert_eq!(
        reactor
            .peer_work_queue
            .awaiting_target(&peer)
            .map(|target| target.status.selected_tip_hash),
        Some(latest.selected_tip_hash)
    );

    release.notify_one();
    let completion = reactor
        .pending_port_operations
        .next()
        .await
        .expect("the coalesced locator completes");
    reactor.handle_port_completion(completion);

    assert!(reactor.pending_locator_queries.is_empty());
    assert_eq!(
        counters.started.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "status tip churn must not spawn unbounded locator futures"
    );
    let frame = outbound
        .recv()
        .await
        .expect("GetHeaders uses the latest tip");
    let HeaderSyncMessage::GetHeaders(request) = reactor
        .codec
        .decode_frame(frame, None)
        .expect("GetHeaders decodes")
    else {
        panic!("expected GetHeaders for the latest staged tip");
    };
    assert_eq!(request.target_tip_hash, latest.selected_tip_hash);
}

#[tokio::test]
async fn generation_change_does_not_reuse_a_stale_locator_under_coalescing() {
    let counters = Arc::new(LocatorCounters::default());
    let release = Arc::new(Notify::new());
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot.clone()));
    startup.committed_snapshots = Some(snapshots_rx);
    startup.header_chain_port = Arc::new(StalledLocatorPort {
        counters: counters.clone(),
        release: release.clone(),
        locator: zakura_header_chain::HeaderLocator::for_continuation(anchor),
    });
    startup.use_direct_port();
    let (_, _, mut reactor) =
        build_header_sync_reactor(startup).expect("the direct-port fixture builds");

    let peer = peer();
    let session_id = 7;
    let (send, mut outbound) = crate::zakura::framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts_with_connection(
        peer.clone(),
        session_id,
        send,
        CancellationToken::new(),
        CancellationToken::new(),
        crate::zakura::CloseCause::new(),
    ));
    let _ = outbound.recv().await;

    let status = churn_status(anchor, 1);
    reactor.handle_wire_message(
        peer.clone(),
        session_id,
        HeaderSyncMessage::Status(status.clone()),
    );
    assert_eq!(reactor.pending_port_operations.len(), 1);
    assert_eq!(reactor.pending_locator_queries.len(), 1);

    let mut advanced = snapshot;
    advanced.state_version = advanced
        .state_version
        .checked_next()
        .expect("the fixture state version advances");
    advanced.header_generation = advanced
        .header_generation
        .checked_next()
        .expect("the fixture header generation advances");
    reactor
        .peer_state
        .get_mut(&peer)
        .expect("the peer remains admitted")
        .last_status = Some(status.clone());
    reactor.observe_latest_committed_snapshot(advanced.clone());

    assert_eq!(
        reactor.pending_port_operations.len(),
        1,
        "reconsider must coalesce onto the in-flight locator read"
    );
    assert_eq!(
        reactor
            .peer_work_queue
            .awaiting_target(&peer)
            .map(|target| target.scope.header_generation),
        Some(advanced.header_generation)
    );

    release.notify_one();
    let completion = reactor
        .pending_port_operations
        .next()
        .await
        .expect("the stale-generation locator completes");
    reactor.handle_port_completion(completion);

    assert_eq!(
        counters.started.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the first locator ran once under the old generation"
    );
    assert_eq!(
        reactor.pending_port_operations.len(),
        1,
        "a stale locator must re-dispatch under the new header generation"
    );
    assert_eq!(reactor.pending_locator_queries.len(), 1);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), outbound.recv())
            .await
            .is_err(),
        "a stale locator cannot send GetHeaders under the new generation"
    );
}

#[tokio::test(start_paused = true)]
async fn hanging_locator_port_future_is_removed_at_the_request_deadline() {
    let mut reactor = direct_reactor(Arc::new(StalledLocatorPort {
        counters: Arc::new(LocatorCounters::default()),
        release: Arc::new(Notify::new()),
        locator: zakura_header_chain::HeaderLocator::for_continuation(
            zakura_header_chain::Frontier::new(block::Height(0), block::Hash([0u8; 32])),
        ),
    }));
    let peer = peer();
    connected_session(&mut reactor, peer.clone(), 7);
    let snapshot = reactor
        .committed_snapshot
        .clone()
        .expect("the fixture has a committed snapshot");
    let target = block::Hash([0x81; 32]);
    let scope = zakura_header_chain::HeaderWorkAuthority::for_target(&snapshot, target);
    let started = Instant::now();

    assert!(
        reactor.dispatch_action(HeaderPortOperation::QueryHeaderLocator {
            peer: peer.clone(),
            session_id: 7,
            target_tip_hash: target,
            scope,
        })
    );
    let completion = reactor
        .pending_port_operations
        .next()
        .await
        .expect("the direct port timeout completes the hung locator");
    reactor.handle_port_completion(completion);

    assert!(Instant::now().duration_since(started) >= reactor.startup.request_timeout);
    assert!(reactor.pending_port_operations.is_empty());
    assert!(reactor.pending_locator_queries.is_empty());
}

#[tokio::test(start_paused = true)]
async fn locator_timeout_after_tip_churn_rediscovers_the_latest_target() {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot));
    startup.committed_snapshots = Some(snapshots_rx);
    startup.header_chain_port = Arc::new(StalledLocatorPort {
        counters: Arc::new(LocatorCounters::default()),
        release: Arc::new(Notify::new()),
        locator: zakura_header_chain::HeaderLocator::for_continuation(anchor),
    });
    startup.use_direct_port();
    let (_, _, mut reactor) =
        build_header_sync_reactor(startup).expect("the direct-port fixture builds");

    let peer = peer();
    let session_id = 7;
    let (send, mut outbound) = crate::zakura::framed_channel(8);
    reactor.handle_peer_connected(PeerSession::from_parts_with_connection(
        peer.clone(),
        session_id,
        send,
        CancellationToken::new(),
        CancellationToken::new(),
        crate::zakura::CloseCause::new(),
    ));
    let _ = outbound.recv().await;

    reactor.handle_wire_message(
        peer.clone(),
        session_id,
        HeaderSyncMessage::Status(churn_status(anchor, 1)),
    );
    reactor.handle_wire_message(
        peer.clone(),
        session_id,
        HeaderSyncMessage::Status(churn_status(anchor, 2)),
    );
    assert_eq!(reactor.pending_port_operations.len(), 1);

    let completion = reactor
        .pending_port_operations
        .next()
        .await
        .expect("the superseded tip's locator times out");
    reactor.handle_port_completion(completion);

    let latest = churn_status(anchor, 2);
    assert_eq!(
        reactor
            .peer_work_queue
            .awaiting_target(&peer)
            .map(|target| target.status.selected_tip_hash),
        Some(latest.selected_tip_hash),
        "a timed-out locator for a replaced tip must not drop the latest staged target"
    );
    assert_eq!(
        reactor.pending_port_operations.len(),
        1,
        "the reactor must rediscover a locator for the latest tip"
    );
    assert_eq!(reactor.pending_locator_queries.len(), 1);
}
