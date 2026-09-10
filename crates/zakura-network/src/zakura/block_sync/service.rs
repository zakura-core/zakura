use super::{config::*, peer_registry::SessionAdmission, wire::*, *};
use crate::zakura::{
    handle_pipe_exit, spawn_supervised_pipe, FramedRecv, FramedSend, OrderedSendError,
    OrderedSessionDemand, OrderedStreamOpening, OrderedStreamPair, OrderedStreamPolicy, Peer,
    PeerStreamSession, Service, ServicePeerSnapshot, SinkReject, Stream, StreamMode,
    ZakuraBlockSyncCandidateState, ZakuraConnId, ZakuraPeerId, FRAME_HEADER_BYTES,
};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};
use tokio::sync::Notify;

mod sessions;
pub(super) use sessions::CurrentSessions;
use sessions::SessionCapacity;

#[cfg(test)]
mod tests;

/// Maximum frame bytes for one stream-6 body frame plus protocol framing.
///
/// A block body is still decoded and validated against Zebra's
/// `MAX_BLOCK_BYTES`; this frame cap has extra slack so stream-6 can classify
/// oversized or incompatible block-sync payloads in the codec instead of
/// dropping them at the raw transport gate.
pub const MAX_BS_FRAME_BYTES: u32 = {
    // This cast is safe: MAX_BS_MESSAGE_BYTES is asserted below 4 MiB.
    (MAX_BS_MESSAGE_BYTES + FRAME_HEADER_BYTES) as u32
};

const BLOCK_SYNC_PAIR: OrderedStreamPair = OrderedStreamPair {
    data: Stream {
        kind: ZAKURA_STREAM_BLOCK_SYNC,
        version: ZAKURA_BLOCK_SYNC_STREAM_VERSION,
        capability: ZAKURA_CAP_BLOCK_SYNC,
        frame_cap: MAX_BS_FRAME_BYTES,
        mode: StreamMode::Ordered,
    },
    requests: Stream {
        kind: ZAKURA_STREAM_BLOCK_REQUESTS,
        version: 1,
        // Nine payload bytes plus the fixed eight-byte frame header.
        capability: ZAKURA_CAP_BLOCK_SYNC,
        frame_cap: 17,
        mode: StreamMode::Ordered,
    },
};
const BLOCK_SYNC_PAIR_STREAMS: [Stream; 2] = [BLOCK_SYNC_PAIR.data, BLOCK_SYNC_PAIR.requests];

/// Service-declared streams for native block sync.
pub(crate) fn block_sync_streams() -> &'static [Stream] {
    &BLOCK_SYNC_PAIR_STREAMS
}

/// Cloneable typed stream-6 sender.
#[derive(Clone, Debug)]
pub struct BlockSyncPeerSession {
    peer_id: ZakuraPeerId,
    session_id: u64,
    direction: ServicePeerDirection,
    send: FramedSend,
    requests: FramedSend,
    remote_status: watch::Sender<bool>,
    cancel_token: CancellationToken,
    /// One stored wake released after the reactor installs this serving handle.
    reactor_ready: Arc<Notify>,
}

impl BlockSyncPeerSession {
    pub(crate) fn new(
        session: &PeerStreamSession,
        session_id: u64,
        direction: ServicePeerDirection,
        requests: FramedSend,
    ) -> Self {
        Self {
            peer_id: session.peer_id().clone(),
            session_id,
            direction,
            send: session.sender(),
            requests,
            remote_status: watch::channel(false).0,
            cancel_token: session.cancel_token(),
            reactor_ready: Arc::new(Notify::new()),
        }
    }

    /// Build a session directly from a `FramedSend` for routine-level unit tests,
    /// bypassing a full `PeerStreamSession`. The `send` half feeds a `framed_channel`
    /// the test reads, and `cancel_token` lets the test tear the routine down.
    #[cfg(test)]
    pub(super) fn for_test(
        peer_id: ZakuraPeerId,
        send: FramedSend,
        cancel_token: CancellationToken,
    ) -> Self {
        Self::for_test_with_session_id(peer_id, 0, send, cancel_token)
    }

    /// Build a test session with an explicit generation for ordering tests.
    #[cfg(test)]
    pub(super) fn for_test_with_session_id(
        peer_id: ZakuraPeerId,
        session_id: u64,
        send: FramedSend,
        cancel_token: CancellationToken,
    ) -> Self {
        Self {
            peer_id,
            session_id,
            direction: ServicePeerDirection::Outbound,
            requests: send.clone(),
            send,
            remote_status: watch::channel(false).0,
            cancel_token,
            reactor_ready: Arc::new(Notify::new()),
        }
    }

    /// Authenticated peer identity for this block-sync session.
    pub fn peer_id(&self) -> &ZakuraPeerId {
        &self.peer_id
    }

    /// Reactor generation that owns this stream session.
    pub(super) fn session_id(&self) -> u64 {
        self.session_id
    }

    /// Direction of the underlying Zakura connection.
    pub fn direction(&self) -> ServicePeerDirection {
        self.direction
    }

    /// Peer disconnect/local shutdown cancellation token.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    /// Wait until the reactor has installed or rejected this exact session.
    pub(super) async fn wait_until_reactor_ready(&self) {
        self.reactor_ready.notified().await;
    }

    /// Release the peer routine after reactor-side admission finishes.
    pub(super) fn mark_reactor_ready(&self) {
        self.reactor_ready.notify_one();
    }

    /// Current free slots in this peer's bounded outbound stream queue.
    pub fn outbound_capacity(&self) -> usize {
        self.request_sender_ref().capacity()
    }

    /// Total slots in this peer's bounded outbound stream queue.
    pub fn outbound_max_capacity(&self) -> usize {
        self.request_sender_ref().max_capacity()
    }

    /// Queue used by the download routine, which reserves space before taking work.
    pub(super) fn request_sender(&self) -> FramedSend {
        self.request_sender_ref().clone()
    }

    fn request_sender_ref(&self) -> &FramedSend {
        &self.requests
    }

    pub(super) fn mark_status_received(&self) {
        self.remote_status.send_replace(true);
    }

    pub(super) fn subscribe_remote_status(&self) -> watch::Receiver<bool> {
        self.remote_status.subscribe()
    }

    pub(super) fn data_sender(&self) -> FramedSend {
        self.send.clone()
    }

    /// Send a typed status advertisement.
    pub fn try_send_status(&self, status: BlockSyncStatus) -> Result<(), OrderedSendError> {
        let frame = BlockSyncMessage::Status(status)
            .encode_frame()
            .map_err(|error| OrderedSendError::Encode(Box::new(error)))?;
        match self.send.try_send(frame) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_frame)) => Err(OrderedSendError::Full),
            Err(mpsc::error::TrySendError::Closed(_frame)) => Err(OrderedSendError::Closed),
        }
    }

    /// Send a typed status advertisement, waiting for transport queue capacity.
    pub async fn send_status(&self, status: BlockSyncStatus) -> Result<(), OrderedSendError> {
        let frame = BlockSyncMessage::Status(status)
            .encode_frame()
            .map_err(|error| OrderedSendError::Encode(Box::new(error)))?;
        self.send
            .send(frame)
            .await
            .map_err(|_error| OrderedSendError::Closed)
    }
}

/// Native stream-6 block-sync service scaffold.
#[derive(Debug)]
pub(crate) struct BlockSyncService {
    inner: Arc<BlockSyncServiceInner>,
    range_source: Option<Arc<dyn BlockRangeSource>>,
    local_status: Option<watch::Receiver<BlockSyncStatus>>,
    service_demand:
        Option<watch::Receiver<zakura_node_services::sync_lifecycle::SyncServiceDemand>>,
    _reactor_task: Option<JoinHandle<()>>,
}

#[derive(Debug)]
struct BlockSyncServiceInner {
    capacity: SessionCapacity,
    config: ZakuraBlockSyncConfig,
    sessions: Arc<CurrentSessions>,
    /// Shared download primitives wired into each peer routine by `add_peer`.
    /// Tests without a reactor use `None` and drain incoming frames.
    routine_wiring: Option<super::state::RoutineWiring>,
    peer_snapshot: watch::Receiver<ServicePeerSnapshot>,
    candidates: watch::Receiver<ZakuraBlockSyncCandidateState>,
    /// Connections whose block-sync session exited while the connection stayed
    /// up. A claim bridges the transport's reopen backoff so a discovery
    /// ownership sample cannot close a healthy connection mid-gap; it is only
    /// honored while this service would re-admit the peer immediately (see
    /// `owns_connection_for_peer`).
    session_gap_claims: StdMutex<HashMap<ZakuraPeerId, SessionGapClaim>>,
    next_session_id: AtomicU64,
}

#[derive(Debug)]
struct BlockSyncPeerRecord {
    session: BlockSyncPeerSession,
    conn_id: ZakuraConnId,
    session_id: u64,
    direction: ServicePeerDirection,
    cancel_token: CancellationToken,
}

#[derive(Debug)]
struct SessionGapClaim {
    conn_id: ZakuraConnId,
    direction: ServicePeerDirection,
}

impl BlockSyncServiceInner {
    fn finish_session(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId, session_id: u64) -> bool {
        let Ok(mut active_peers) = self.sessions.active.lock() else {
            return false;
        };
        let owns_session = active_peers
            .get(peer)
            .is_some_and(|record| record.conn_id == conn_id && record.session_id == session_id);
        if !owns_session {
            return false;
        }

        let removed = active_peers
            .remove(peer)
            .expect("record exists because the ownership check just matched it");

        // The connection may outlive this session while the transport backs off
        // before reopening the stream; remember the claim so ownership checks
        // bridge the gap. The claim is written while still holding the peer-map
        // lock so a concurrent `remove_peer` for the closing connection cannot
        // clear claims between the removal above and this insert, which would
        // leak a claim for a dead connection.
        if let Ok(mut claims) = self.session_gap_claims.lock() {
            claims.insert(
                peer.clone(),
                SessionGapClaim {
                    conn_id,
                    direction: removed.direction,
                },
            );
        }
        self.sessions.notify();
        true
    }
}

impl BlockSyncService {
    pub(crate) fn new(config: ZakuraBlockSyncConfig) -> Self {
        Self::new_with_startup(BlockSyncStartup::inert(config))
    }

    pub(crate) fn new_with_handle(config: ZakuraBlockSyncConfig, handle: BlockSyncHandle) -> Self {
        Self {
            range_source: handle.range_source.clone(),
            local_status: Some(handle.subscribe_status()),
            inner: Arc::new(BlockSyncServiceInner {
                capacity: SessionCapacity::new(config.peer_limits),
                config,
                sessions: handle.current_sessions.clone(),
                routine_wiring: handle.routine_wiring.clone(),
                peer_snapshot: handle.subscribe_peer_snapshot(),
                candidates: handle.subscribe_candidate_state(),
                session_gap_claims: StdMutex::new(HashMap::new()),
                next_session_id: AtomicU64::new(1),
            }),
            service_demand: None,
            _reactor_task: None,
        }
    }

    pub(crate) fn new_with_header_tip(
        config: ZakuraBlockSyncConfig,
        header_tip: watch::Receiver<(block::Height, block::Hash)>,
    ) -> Self {
        let best_header_tip = *header_tip.borrow();
        let startup = BlockSyncStartup::new(
            BlockSyncFrontiers {
                finalized_height: block::Height::MIN,
                verified_block_tip: block::Height::MIN,
                verified_block_hash: block::Hash([0; 32]),
            },
            best_header_tip,
            header_tip,
            config,
        );
        Self::new_with_startup(startup)
    }

    fn new_with_startup(startup: BlockSyncStartup) -> Self {
        let config = startup.config.clone();
        let (handle, _actions, reactor_task) = spawn_block_sync_reactor(startup);
        Self {
            range_source: None,
            local_status: Some(handle.subscribe_status()),
            inner: Arc::new(BlockSyncServiceInner {
                capacity: SessionCapacity::new(config.peer_limits),
                config,
                sessions: handle.current_sessions.clone(),
                routine_wiring: handle.routine_wiring.clone(),
                peer_snapshot: handle.subscribe_peer_snapshot(),
                candidates: handle.subscribe_candidate_state(),
                session_gap_claims: StdMutex::new(HashMap::new()),
                next_session_id: AtomicU64::new(1),
            }),
            service_demand: None,
            _reactor_task: Some(reactor_task),
        }
    }

    pub(crate) fn with_service_demand(
        mut self,
        service_demand: Option<
            watch::Receiver<zakura_node_services::sync_lifecycle::SyncServiceDemand>,
        >,
    ) -> Self {
        self.service_demand = service_demand;
        self
    }

    #[cfg(test)]
    pub(crate) fn peer_count(&self) -> usize {
        self.inner
            .sessions
            .active
            .lock()
            .expect("block-sync peer map mutex is never poisoned")
            .len()
    }

    fn peer_slots_free(&self, direction: ServicePeerDirection) -> bool {
        let peers = self
            .inner
            .sessions
            .active
            .lock()
            .expect("block-sync peer map mutex is never poisoned");
        let count = peers
            .values()
            .filter(|record| record.direction == direction)
            .count();
        let cap = match direction {
            ServicePeerDirection::Inbound => self.inner.config.peer_limits.max_inbound_peers,
            ServicePeerDirection::Outbound => self.inner.config.peer_limits.max_outbound_peers,
        };
        count < cap
    }

    fn session_needs_body_work(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) -> bool {
        self.inner.routine_wiring.as_ref().is_some_and(|wiring| {
            wiring
                .registry
                .has_expired_session_park(peer, conn_id, Instant::now())
        })
    }

    fn peer_is_parked(&self, peer_id: &ZakuraPeerId) -> bool {
        self.inner
            .routine_wiring
            .as_ref()
            .is_some_and(|wiring| wiring.registry.is_peer_parked(peer_id, Instant::now()))
    }

    fn peer_park_deadline(&self, peer_id: &ZakuraPeerId) -> Option<Instant> {
        let now = Instant::now();
        self.inner
            .routine_wiring
            .as_ref()
            .and_then(|wiring| wiring.registry.peer_park_deadline(peer_id, now))
    }
}

impl Service for BlockSyncService {
    fn name(&self) -> &'static str {
        "block-sync"
    }

    fn streams(&self) -> &[Stream] {
        block_sync_streams()
    }

    fn ordered_stream_pair(&self, stream: Stream) -> Option<OrderedStreamPair> {
        BLOCK_SYNC_PAIR_STREAMS
            .contains(&stream)
            .then_some(BLOCK_SYNC_PAIR)
    }

    fn reserve_ordered_session(
        &self,
        direction: ServicePeerDirection,
    ) -> Result<
        Option<Arc<dyn crate::zakura::OrderedSessionResources>>,
        crate::zakura::OrderedSessionFull,
    > {
        self.inner.capacity.reserve(direction).map(Some)
    }

    fn stream_queue_depths(&self, stream: Stream) -> Option<(usize, usize)> {
        if stream == BLOCK_SYNC_PAIR.requests {
            Some((1, 1))
        } else {
            let limits = self.inner.config.peer_limits;
            Some((
                limits.inbound_queue_depth.max(1),
                limits.outbound_queue_depth.max(1),
            ))
        }
    }

    fn message_payload_limits(&self, stream: Stream) -> &'static [(u16, usize)] {
        if stream == BLOCK_SYNC_PAIR.requests {
            serving_regulation::message_payload_limits()
        } else if stream == BLOCK_SYNC_PAIR.data {
            &[(1, 53), (4, 9), (5, 9)]
        } else {
            &[]
        }
    }

    fn message_types(&self, stream: Stream) -> Option<&'static [u16]> {
        Some(if stream == BLOCK_SYNC_PAIR.requests {
            &[2]
        } else {
            &[1, 3, 4, 5]
        })
    }

    fn ordered_stream_policy(&self, _kind: u16) -> OrderedStreamPolicy {
        OrderedStreamPolicy {
            opening: OrderedStreamOpening::EitherSide,
            reopen: true,
        }
    }

    fn ordered_session_demand(
        &self,
        conn_id: ZakuraConnId,
        peer: &ZakuraPeerId,
        _negotiated: u64,
        direction: ServicePeerDirection,
    ) -> OrderedSessionDemand {
        let mut capacity = self.inner.capacity.subscribe();
        if !self.inner.capacity.available(direction) {
            return OrderedSessionDemand::WaitForChange(Box::pin(async move {
                let _ = capacity.changed().await;
            }));
        }
        self.reserved_ordered_session_demand(conn_id, peer, _negotiated, direction)
    }

    fn reserved_ordered_session_demand(
        &self,
        conn_id: ZakuraConnId,
        peer: &ZakuraPeerId,
        _negotiated: u64,
        direction: ServicePeerDirection,
    ) -> OrderedSessionDemand {
        if let Some(deadline) = self.peer_park_deadline(peer) {
            return OrderedSessionDemand::RetryAt(deadline);
        }
        let mut peer_snapshot = self.inner.peer_snapshot.clone();
        peer_snapshot.borrow_and_update();
        if !self.peer_slots_free(direction) {
            return OrderedSessionDemand::WaitForChange(Box::pin(async move {
                if peer_snapshot.changed().await.is_err() {
                    std::future::pending::<()>().await;
                }
            }));
        }

        // A newly negotiated peer is still admitted at the tip so it can
        // exchange status and serve the remote. This gate applies only after a
        // local park: if another peer filled the body gap during the cooldown,
        // keep this session absent until block sync publishes useful work again.
        let native_applying = self
            .service_demand
            .as_ref()
            .is_none_or(|demand| demand.borrow().block.is_applying());
        if native_applying && self.session_needs_body_work(peer, conn_id) {
            let mut candidates = self.inner.candidates.clone();
            if candidates
                .borrow_and_update()
                .missing_block_bodies
                .is_empty()
            {
                let mut service_demand = self.service_demand.clone();
                return OrderedSessionDemand::WaitForChange(Box::pin(async move {
                    if let Some(demand) = service_demand.as_mut() {
                        tokio::select! {
                            changed = candidates.changed() => {
                                if changed.is_err() {
                                    std::future::pending::<()>().await;
                                }
                            }
                            changed = demand.changed() => {
                                if changed.is_err() {
                                    std::future::pending::<()>().await;
                                }
                            }
                        }
                    } else if candidates.changed().await.is_err() {
                        std::future::pending::<()>().await;
                    }
                }));
            }
        }

        OrderedSessionDemand::OpenNow
    }

    fn wants_peer(
        &self,
        peer: &ZakuraPeerId,
        _negotiated: u64,
        direction: ServicePeerDirection,
    ) -> bool {
        !self.peer_is_parked(peer) && self.peer_slots_free(direction)
    }

    fn add_peer(&self, mut peer: Peer) {
        if self.peer_is_parked(&peer.id) {
            peer.service_cancel_token().cancel();
            return;
        }

        let Some((data_session_id, version, recv, send)) =
            peer.take_versioned_stream_with_session_id(ZAKURA_STREAM_BLOCK_SYNC)
        else {
            return;
        };
        let (incoming_requests, request_sender) = {
            let Some((request_session_id, request_version, recv, send)) =
                peer.take_versioned_stream_with_session_id(ZAKURA_STREAM_BLOCK_REQUESTS)
            else {
                peer.service_cancel_token().cancel();
                return;
            };
            if version != BLOCK_SYNC_PAIR.data.version
                || request_version != BLOCK_SYNC_PAIR.requests.version
                || request_session_id != data_session_id
            {
                peer.service_cancel_token().cancel();
                return;
            }
            (recv, send)
        };

        let peer_id = peer.id.clone();
        let session = PeerStreamSession::new(
            peer_id.clone(),
            ZAKURA_STREAM_BLOCK_SYNC,
            version,
            recv,
            send,
            peer.service_cancel_token(),
        );
        let service_cancel_token = session.cancel_token();
        let connection_cancel_token = peer.cancel_token();
        let close_cause = peer.close_cause();
        let conn_id = peer.conn_id;

        let (
            old_record,
            re_admitted_after_no_progress,
            routine_generation,
            session_id,
            block_sync_session,
        ) = {
            let mut active_peers = self
                .inner
                .sessions
                .active
                .lock()
                .expect("block-sync peer map mutex is never poisoned");
            if active_peers
                .get(&peer_id)
                .is_some_and(|record| record.conn_id > conn_id)
            {
                service_cancel_token.cancel();
                return;
            }

            // A peer registered for this direction may replace its session.
            // A connection-symmetry collision replaces the losing session with the winning stream.
            // Apply the per-direction cap only to a new peer.
            let already_counted = active_peers
                .get(&peer_id)
                .is_some_and(|record| record.direction == peer.direction);
            if !already_counted {
                let count = active_peers
                    .values()
                    .filter(|record| record.direction == peer.direction)
                    .count();
                let cap = match peer.direction {
                    ServicePeerDirection::Inbound => {
                        self.inner.config.peer_limits.max_inbound_peers
                    }
                    ServicePeerDirection::Outbound => {
                        self.inner.config.peer_limits.max_outbound_peers
                    }
                };
                if count >= cap {
                    service_cancel_token.cancel();
                    return;
                }
            }

            // Admission is atomic with the park state: a park recorded by the
            // predecessor routine after the entry-point `peer_is_parked` check
            // is honored here instead of being silently bypassed.
            let (routine_generation, re_admitted_after_no_progress) =
                if let Some(wiring) = &self.inner.routine_wiring {
                    match wiring.registry.admit_session(
                        &peer_id,
                        peer.direction,
                        &wiring.config,
                        conn_id,
                        Instant::now(),
                    ) {
                        SessionAdmission::Parked => {
                            service_cancel_token.cancel();
                            return;
                        }
                        SessionAdmission::Readmitted { generation } => (Some(generation), true),
                        SessionAdmission::Fresh { generation } => (Some(generation), false),
                    }
                } else {
                    (None, false)
                };
            // Production uses the registry's globally unique routine generation.
            // Handle-less tests use the service-local fallback.
            let session_id = routine_generation
                .unwrap_or_else(|| self.inner.next_session_id.fetch_add(1, Ordering::Relaxed));
            let block_sync_session =
                BlockSyncPeerSession::new(&session, session_id, peer.direction, request_sender);
            let old_record = active_peers.insert(
                peer_id.clone(),
                BlockSyncPeerRecord {
                    session: block_sync_session.clone(),
                    conn_id,
                    session_id,
                    direction: peer.direction,
                    cancel_token: service_cancel_token.clone(),
                },
            );
            (
                old_record,
                re_admitted_after_no_progress,
                routine_generation,
                session_id,
                block_sync_session,
            )
        };
        let (_session_peer, _stream_kind, _stream_version, recv, send, _session_cancel) =
            session.into_parts();

        // Production outbound frames go through `BlockSyncPeerSession`; its
        // sender clone keeps the stream alive after this redundant half drops.
        drop(send);
        if let Some(old_record) = old_record {
            old_record.cancel_token.cancel();
        }
        // The admitted session supersedes any gap claim left by its predecessor.
        self.inner
            .session_gap_claims
            .lock()
            .expect("block-sync gap-claim mutex is never poisoned")
            .remove(&peer_id);

        let run_cancel = service_cancel_token.clone();
        let on_teardown = {
            let peer_id = peer_id.clone();
            let inner = self.inner.clone();
            move || {
                inner.finish_session(&peer_id, conn_id, session_id);
            }
        };
        let on_panic = {
            let connection_cancel_token = connection_cancel_token.clone();
            let close_cause = close_cause.clone();
            move || {
                close_cause.record("service_panic");
                connection_cancel_token.cancel();
            }
        };
        // Publish the table before spawning the reader. The reactor marks the
        // exact session ready after reconciling its snapshot.
        self.inner.sessions.notify();

        // the per-peer pipe-routine is spawned HERE (the pipe spawn point), so
        // a protocol reject still cancels the whole connection via
        // `handle_pipe_exit`. The routine owns `recv` (the transport read), decodes
        // each frame, and runs the download/serving dispatch in its own task —
        // there is no reactor inbound demux. When the service has no reactor wiring
        // (inert/handle-less test constructors) there is no routine to run; drain
        // the stream so frames are not silently mishandled and the lifecycle still
        // flows.
        let pipe = {
            let source = self.range_source.clone();
            let local_status = self.local_status.clone();
            let connection_cancel_token = connection_cancel_token.clone();
            let close_cause = close_cause.clone();
            let routine_wiring = self.inner.routine_wiring.clone();
            let block_sync_session = block_sync_session.clone();
            let peer_id = peer_id.clone();
            async move {
                let reactor_ready = if routine_wiring.is_some() {
                    tokio::select! {
                        () = block_sync_session.wait_until_reactor_ready() => true,
                        () = run_cancel.cancelled() => false,
                    }
                } else {
                    true
                };
                let result = if !reactor_ready || run_cancel.is_cancelled() {
                    Ok(())
                } else {
                    match routine_wiring {
                        Some(wiring) => {
                            let generation = routine_generation.expect(
                            "production block-sync wiring allocates a routine generation before spawn",
                        );
                            let serving = wiring.serving_regulator.session(peer_id.clone());
                            let routine = super::peer_routine::PeerRoutine::new(
                                peer_id,
                                conn_id,
                                block_sync_session.clone(),
                                recv,
                                wiring.config,
                                !re_admitted_after_no_progress,
                                generation,
                                wiring.budget,
                                wiring.work,
                                wiring.registry.clone(),
                                wiring.received_throughput,
                                wiring.sequencer_input,
                                wiring.sequencer_input_bytes,
                                wiring.sequencer_input_decoded_attributed_memory_bytes,
                                wiring.routine_to_reactor,
                                wiring.view,
                                run_cancel.clone(),
                                wiring.trace.clone(),
                            );
                            let download = routine.run();
                            tokio::pin!(download);
                            let result = tokio::select! {
                                result = &mut download => result,
                                result = super::serving::serve_requests(
                                    block_sync_session, incoming_requests, serving, wiring.registry,
                                    local_status.expect("paired serving has a local status watch"),
                                    source, wiring.trace,
                                ) => {
                                    // Do not drop unanswered downloads before their
                                    // stream-failure policy has been applied.
                                    run_cancel.cancel();
                                    match (result, download.await) {
                                        (Err(error @ SinkReject::Protocol(_)), _)
                                        | (_, Err(error @ SinkReject::Protocol(_))) => Err(error),
                                        (serving, download) => serving.and(download),
                                    }
                                },
                            };
                            run_cancel.cancel();
                            result
                        }
                        None => {
                            let result = tokio::select! {
                                result = drain_inbound(recv, run_cancel.clone()) => result,
                                result = drain_inbound(incoming_requests, run_cancel.clone()) => result,
                            };
                            run_cancel.cancel();
                            result
                        }
                    }
                };
                handle_pipe_exit("block-sync", &connection_cancel_token, &close_cause, result);
            }
        };
        // Let the returned handle drop to detach the supervised task (like
        // `tokio::spawn`); the `PipeTeardown` still runs on every exit path.
        spawn_supervised_pipe(
            peer_id.clone(),
            service_cancel_token.clone(),
            on_teardown,
            on_panic,
            pipe,
        );
    }

    fn owns_connection_for_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) -> bool {
        let session_is_active = self
            .inner
            .sessions
            .active
            .lock()
            .expect("block-sync peer map mutex is never poisoned")
            .get(peer)
            .is_some_and(|record| record.conn_id == conn_id);
        if session_is_active {
            return true;
        }

        // A transient session exit leaves a gap claim during transport backoff.
        // Honor the claim only while this service would admit the peer.
        // A park, full slots, or useless-work gate releases the connection like a rejected stream.
        let Some(direction) = self
            .inner
            .session_gap_claims
            .lock()
            .expect("block-sync gap-claim mutex is never poisoned")
            .get(peer)
            .and_then(|claim| (claim.conn_id == conn_id).then_some(claim.direction))
        else {
            return false;
        };
        matches!(
            self.ordered_session_demand(conn_id, peer, ZAKURA_CAP_BLOCK_SYNC, direction),
            OrderedSessionDemand::OpenNow
        )
    }

    fn remove_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) {
        let removed_record = {
            let mut active_peers = self
                .inner
                .sessions
                .active
                .lock()
                .expect("block-sync peer map mutex is never poisoned");
            let removed = match active_peers.get(peer) {
                Some(record) if record.conn_id == conn_id => active_peers.remove(peer),
                Some(_) | None => None,
            };
            // The claim is cleared while still holding the peer-map lock so it
            // stays ordered with `finish_session`'s remove-then-claim sequence
            // for the same connection; clearing outside the lock could leave a
            // late claim behind for this closed connection.
            let mut claims = self
                .inner
                .session_gap_claims
                .lock()
                .expect("block-sync gap-claim mutex is never poisoned");
            if claims
                .get(peer)
                .is_some_and(|claim| claim.conn_id == conn_id)
            {
                claims.remove(peer);
            }
            removed
        };
        if let Some(wiring) = &self.inner.routine_wiring {
            wiring
                .registry
                .connection_closed(peer, conn_id, Instant::now());
        }
        let Some(record) = removed_record else {
            return;
        };

        record.cancel_token.cancel();
        self.inner.sessions.notify();
    }

    fn deliver_frame(
        &self,
        _peer_id: ZakuraPeerId,
        _stream_kind: u16,
        _frame: Frame,
    ) -> Result<(), SinkReject> {
        // The inbound data flow is inverted: block sync is an `Ordered` stream
        // whose `FramedRecv` is taken by `add_peer` and owned by the per-peer
        // pipe-routine ([`PeerRoutine`](super::peer_routine)), which decodes and
        // dispatches every frame in its own task. The `Service::deliver_frame`
        // entry point (driven only by the testkit recorder / `registry.deliver`,
        // never the production ordered-stream reader) therefore has no routine to
        // route into and no reactor inbound path to emit to. It is not the
        // block-sync inbound path; accept-and-ignore rather than constructing a
        // detached one-shot decode that could never reach the owning routine. No
        // production frame reaches here (the routine consumes the stream), so this
        // drops nothing that the routine would otherwise handle.
        Ok(())
    }
}

/// Drain a peer's inbound block-sync stream when the service has no reactor
/// wiring to spawn a pipe-routine (the inert / handle-less test constructors).
/// Frames are read and discarded until cancellation or stream close, so the
/// transport reader makes progress and the lifecycle still fires; no routine
/// exists to act on them.
async fn drain_inbound(mut recv: FramedRecv, cancel: CancellationToken) -> Result<(), SinkReject> {
    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            frame = recv.recv() => {
                if frame.is_none() {
                    return Ok(());
                }
            }
        }
    }
}
