use super::{config::*, events::*, wire::*, *};
use crate::zakura::{
    handle_pipe_exit, spawn_supervised_pipe, FramedRecv, FramedSend, OrderedSendError,
    OrderedSessionDemand, OrderedStreamOpening, OrderedStreamPolicy, Peer, PeerStreamSession,
    Service, ServicePeerSnapshot, SinkReject, Stream, StreamMode, ZakuraBlockSyncCandidateState,
    ZakuraConnId, ZakuraPeerId, FRAME_HEADER_BYTES,
};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

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

const BLOCK_SYNC_SERVICE_STREAMS: [Stream; 1] = [Stream {
    kind: ZAKURA_STREAM_BLOCK_SYNC,
    version: ZAKURA_BLOCK_SYNC_STREAM_VERSION,
    frame_cap: MAX_BS_FRAME_BYTES,
    capability: ZAKURA_CAP_BLOCK_SYNC,
    mode: StreamMode::Ordered,
}];

/// Service-declared streams for native block sync.
pub(crate) fn block_sync_streams() -> &'static [Stream] {
    &BLOCK_SYNC_SERVICE_STREAMS
}

/// Cloneable typed stream-6 sender.
#[derive(Clone, Debug)]
pub struct BlockSyncPeerSession {
    peer_id: ZakuraPeerId,
    direction: ServicePeerDirection,
    send: FramedSend,
    cancel_token: CancellationToken,
}

impl BlockSyncPeerSession {
    pub(crate) fn new(session: &PeerStreamSession, direction: ServicePeerDirection) -> Self {
        Self {
            peer_id: session.peer_id().clone(),
            direction,
            send: session.sender(),
            cancel_token: session.cancel_token(),
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
        Self {
            peer_id,
            direction: ServicePeerDirection::Outbound,
            send,
            cancel_token,
        }
    }

    /// Authenticated peer identity for this block-sync session.
    pub fn peer_id(&self) -> &ZakuraPeerId {
        &self.peer_id
    }

    /// Direction of the underlying Zakura connection.
    pub fn direction(&self) -> ServicePeerDirection {
        self.direction
    }

    /// Peer disconnect/local shutdown cancellation token.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    /// Current free slots in this peer's bounded outbound stream queue.
    pub fn outbound_capacity(&self) -> usize {
        self.send.capacity()
    }

    /// Total slots in this peer's bounded outbound stream queue.
    pub fn outbound_max_capacity(&self) -> usize {
        self.send.max_capacity()
    }

    /// Send a typed status advertisement.
    pub fn try_send_status(&self, status: BlockSyncStatus) -> Result<(), OrderedSendError> {
        self.try_send_message(BlockSyncMessage::Status(status))
    }

    /// Send a typed status advertisement, waiting for transport queue capacity.
    pub async fn send_status(&self, status: BlockSyncStatus) -> Result<(), OrderedSendError> {
        self.send_message(BlockSyncMessage::Status(status)).await
    }

    /// Send a typed block range request.
    pub fn try_send_get_blocks(
        &self,
        start_height: block::Height,
        count: u32,
    ) -> Result<(), OrderedSendError> {
        self.try_send_message(BlockSyncMessage::GetBlocks {
            start_height,
            count,
        })
    }

    /// Send one typed block body frame.
    pub fn try_send_block(&self, block: Arc<block::Block>) -> Result<(), OrderedSendError> {
        self.try_send_message(BlockSyncMessage::Block(block))
    }

    /// Send one typed block body frame, waiting for transport queue capacity.
    pub async fn send_block(&self, block: Arc<block::Block>) -> Result<(), OrderedSendError> {
        self.send_message(BlockSyncMessage::Block(block)).await
    }

    /// Send a typed response terminator.
    pub fn try_send_blocks_done(
        &self,
        start_height: block::Height,
        returned: u32,
    ) -> Result<(), OrderedSendError> {
        self.try_send_message(BlockSyncMessage::BlocksDone {
            start_height,
            returned,
        })
    }

    /// Send a typed response terminator, waiting for transport queue capacity.
    pub async fn send_blocks_done(
        &self,
        start_height: block::Height,
        returned: u32,
    ) -> Result<(), OrderedSendError> {
        self.send_message(BlockSyncMessage::BlocksDone {
            start_height,
            returned,
        })
        .await
    }

    /// Send a typed unavailable-range response.
    pub fn try_send_range_unavailable(
        &self,
        start_height: block::Height,
        count: u32,
    ) -> Result<(), OrderedSendError> {
        self.try_send_message(BlockSyncMessage::RangeUnavailable {
            start_height,
            count,
        })
    }

    /// Send a typed unavailable-range response, waiting for transport queue capacity.
    pub async fn send_range_unavailable(
        &self,
        start_height: block::Height,
        count: u32,
    ) -> Result<(), OrderedSendError> {
        self.send_message(BlockSyncMessage::RangeUnavailable {
            start_height,
            count,
        })
        .await
    }

    fn try_send_message(&self, msg: BlockSyncMessage) -> Result<(), OrderedSendError> {
        let frame = msg
            .encode_frame()
            .map_err(|error| OrderedSendError::Encode(Box::new(error)))?;
        match self.send.try_send(frame) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_frame)) => Err(OrderedSendError::Full),
            Err(mpsc::error::TrySendError::Closed(_frame)) => Err(OrderedSendError::Closed),
        }
    }

    async fn send_message(&self, msg: BlockSyncMessage) -> Result<(), OrderedSendError> {
        let frame = msg
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
    _held_events: Option<Arc<StdMutex<mpsc::Receiver<BlockSyncEvent>>>>,
    _reactor_task: Option<JoinHandle<()>>,
}

#[derive(Debug)]
struct BlockSyncServiceInner {
    config: ZakuraBlockSyncConfig,
    lifecycle: mpsc::UnboundedSender<BlockSyncEvent>,
    /// Shared download primitives every per-peer pipe-routine is wired with at
    /// `add_peer` (per-peer routines). `None` for the inert/handle-less constructors that never
    /// spawn routines (they only observe `events`/`lifecycle`).
    routine_wiring: Option<super::state::RoutineWiring>,
    /// Reactor notification used to wake demand waiting on an active-session slot.
    peer_snapshot: watch::Receiver<ServicePeerSnapshot>,
    /// Reactor-owned body work used to wake a parked session once it is useful again.
    candidates: watch::Receiver<ZakuraBlockSyncCandidateState>,
    /// Authoritative active session for each peer's current transport connection.
    active_peers: StdMutex<HashMap<ZakuraPeerId, BlockSyncPeerRecord>>,
    next_session_id: AtomicU64,
}

#[derive(Debug)]
struct BlockSyncPeerRecord {
    conn_id: ZakuraConnId,
    session_id: u64,
    direction: ServicePeerDirection,
    cancel_token: CancellationToken,
}

impl BlockSyncServiceInner {
    fn finish_session(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId, session_id: u64) -> bool {
        let Ok(mut active_peers) = self.active_peers.lock() else {
            return false;
        };
        let owns_session = active_peers
            .get(peer)
            .is_some_and(|record| record.conn_id == conn_id && record.session_id == session_id);
        if !owns_session {
            return false;
        }

        active_peers.remove(peer);
        true
    }
}

impl BlockSyncService {
    pub(crate) fn new(config: ZakuraBlockSyncConfig) -> Self {
        Self::new_with_startup(BlockSyncStartup::inert(config))
    }

    pub(crate) fn new_with_handle(config: ZakuraBlockSyncConfig, handle: BlockSyncHandle) -> Self {
        Self {
            inner: Arc::new(BlockSyncServiceInner {
                config,
                lifecycle: handle.lifecycle.clone(),
                routine_wiring: handle.routine_wiring.clone(),
                peer_snapshot: handle.subscribe_peer_snapshot(),
                candidates: handle.subscribe_candidate_state(),
                active_peers: StdMutex::new(HashMap::new()),
                next_session_id: AtomicU64::new(1),
            }),
            _held_events: None,
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
            inner: Arc::new(BlockSyncServiceInner {
                config,
                lifecycle: handle.lifecycle.clone(),
                routine_wiring: handle.routine_wiring.clone(),
                peer_snapshot: handle.subscribe_peer_snapshot(),
                candidates: handle.subscribe_candidate_state(),
                active_peers: StdMutex::new(HashMap::new()),
                next_session_id: AtomicU64::new(1),
            }),
            _held_events: None,
            _reactor_task: Some(reactor_task),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        config: ZakuraBlockSyncConfig,
    ) -> (Self, mpsc::Receiver<BlockSyncEvent>) {
        let (events, event_rx) = mpsc::channel(config.peer_limits.inbound_queue_depth.max(1));
        let (lifecycle, mut lifecycle_rx) = mpsc::unbounded_channel();
        let (_peer_snapshot_tx, peer_snapshot) =
            watch::channel(ServicePeerSnapshot::new(0, 0, config.peer_limits));
        let (_candidates_tx, candidates) = watch::channel(ZakuraBlockSyncCandidateState::default());
        let events_for_lifecycle = events.clone();
        tokio::spawn(async move {
            while let Some(event) = lifecycle_rx.recv().await {
                let _ = events_for_lifecycle.send(event).await;
            }
        });
        (
            Self {
                inner: Arc::new(BlockSyncServiceInner {
                    config,
                    lifecycle,
                    routine_wiring: None,
                    peer_snapshot,
                    candidates,
                    active_peers: StdMutex::new(HashMap::new()),
                    next_session_id: AtomicU64::new(1),
                }),
                _held_events: None,
                _reactor_task: None,
            },
            event_rx,
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_handle_for_test(
        config: ZakuraBlockSyncConfig,
        handle: BlockSyncHandle,
    ) -> Self {
        Self::new_with_handle(config, handle)
    }

    #[cfg(test)]
    pub(crate) fn peer_count(&self) -> usize {
        self.inner
            .active_peers
            .lock()
            .expect("block-sync peer-state mutex is never poisoned")
            .len()
    }

    fn peer_slots_free(&self, direction: ServicePeerDirection) -> bool {
        let active_peers = self
            .inner
            .active_peers
            .lock()
            .expect("block-sync peer-state mutex is never poisoned");
        let count = active_peers
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
                .session_park_expired(peer, conn_id, Instant::now())
        })
    }

    fn peer_is_parked(&self, peer_id: &ZakuraPeerId) -> bool {
        self.inner
            .routine_wiring
            .as_ref()
            .is_some_and(|wiring| wiring.registry.is_peer_parked(peer_id, Instant::now()))
    }

    fn peer_parked_until(&self, peer_id: &ZakuraPeerId) -> Option<Instant> {
        let now = Instant::now();
        self.inner
            .routine_wiring
            .as_ref()
            .and_then(|wiring| wiring.registry.peer_parked_until(peer_id, now))
    }
}

impl Service for BlockSyncService {
    fn name(&self) -> &'static str {
        "block-sync"
    }

    fn streams(&self) -> &[Stream] {
        block_sync_streams()
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
        if let Some(until) = self.peer_parked_until(peer) {
            return OrderedSessionDemand::RetryAt(until);
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
        if self.session_needs_body_work(peer, conn_id) {
            let mut candidates = self.inner.candidates.clone();
            if candidates
                .borrow_and_update()
                .missing_block_bodies
                .is_empty()
            {
                return OrderedSessionDemand::WaitForChange(Box::pin(async move {
                    if candidates.changed().await.is_err() {
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

        let Some((recv, send)) = peer.take_stream(ZAKURA_STREAM_BLOCK_SYNC) else {
            return;
        };

        let peer_id = peer.id.clone();
        let session = PeerStreamSession::new(
            peer_id.clone(),
            ZAKURA_STREAM_BLOCK_SYNC,
            recv,
            send,
            peer.service_cancel_token(),
        );
        let service_cancel_token = session.cancel_token();
        let connection_cancel_token = peer.cancel_token();
        let close_cause = peer.close_cause();
        let block_sync_session = BlockSyncPeerSession::new(&session, peer.direction);
        let session_id = self.inner.next_session_id.fetch_add(1, Ordering::Relaxed);
        let conn_id = peer.conn_id;
        let (_session_peer, _stream_kind, recv, send, _session_cancel) = session.into_parts();

        // Production outbound block-sync frames go directly through
        // `BlockSyncPeerSession` (the per-peer routine's `try_send_get_blocks` /
        // the reactor's `try_send_status`/serving sends), so the raw transport
        // sender taken from the stream here is redundant. The outbound stream stays
        // alive through the `BlockSyncPeerSession` clone the reactor holds, so
        // nothing is lost by dropping it.
        drop(send);

        let (old_record, re_admitted_after_no_progress, routine_generation) = {
            let mut active_peers = self
                .inner
                .active_peers
                .lock()
                .expect("block-sync peer-state mutex is never poisoned");
            if active_peers
                .get(&peer_id)
                .is_some_and(|record| record.conn_id > conn_id)
            {
                service_cancel_token.cancel();
                return;
            }

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

            let (routine_generation, re_admitted_after_no_progress) = if let Some(wiring) =
                &self.inner.routine_wiring
            {
                let generation = wiring
                    .registry
                    .admit(&peer_id, peer.direction, &wiring.config);
                let re_admitted =
                    wiring
                        .registry
                        .take_parked_session(&peer_id, conn_id, Instant::now());
                (Some(generation), re_admitted)
            } else {
                (None, false)
            };
            let old_record = active_peers.insert(
                peer_id.clone(),
                BlockSyncPeerRecord {
                    conn_id: peer.conn_id,
                    session_id,
                    direction: peer.direction,
                    cancel_token: service_cancel_token.clone(),
                },
            );
            (
                old_record,
                re_admitted_after_no_progress,
                routine_generation,
            )
        };
        if let Some(old_record) = old_record {
            old_record.cancel_token.cancel();
        }

        let run_cancel = service_cancel_token.clone();
        let on_teardown = {
            let lifecycle = self.inner.lifecycle.clone();
            let peer_id = peer_id.clone();
            let inner = self.inner.clone();
            move || {
                let should_notify = inner.finish_session(&peer_id, conn_id, session_id);

                if should_notify {
                    let _ = lifecycle.send(BlockSyncEvent::PeerDisconnected(peer_id));
                }
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
        // the per-peer pipe-routine is spawned HERE (the pipe spawn point), so
        // a protocol reject still cancels the whole connection via
        // `handle_pipe_exit`. The routine owns `recv` (the transport read), decodes
        // each frame, and runs the download/serving dispatch in its own task —
        // there is no reactor inbound demux. When the service has no reactor wiring
        // (inert/handle-less test constructors) there is no routine to run; drain
        // the stream so frames are not silently mishandled and the lifecycle still
        // flows.
        let pipe = {
            let connection_cancel_token = connection_cancel_token.clone();
            let close_cause = close_cause.clone();
            let routine_wiring = self.inner.routine_wiring.clone();
            let block_sync_session = block_sync_session.clone();
            let peer_id = peer_id.clone();
            async move {
                let result = match routine_wiring {
                    Some(wiring) => {
                        let generation = routine_generation.expect(
                            "production block-sync wiring allocates a routine generation before spawn",
                        );
                        let routine = super::peer_routine::PeerRoutine::new(
                            peer_id,
                            conn_id,
                            block_sync_session,
                            recv,
                            wiring.config,
                            !re_admitted_after_no_progress,
                            generation,
                            wiring.budget,
                            wiring.work,
                            wiring.registry,
                            wiring.received_throughput,
                            wiring.sequencer_input,
                            wiring.sequencer_input_bytes,
                            wiring.sequencer_input_decoded_attributed_memory_bytes,
                            wiring.actions,
                            wiring.routine_to_reactor,
                            wiring.view,
                            run_cancel,
                            wiring.trace,
                        );
                        routine.run().await
                    }
                    None => drain_inbound(recv, run_cancel).await,
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

        let _ = self
            .inner
            .lifecycle
            .send(BlockSyncEvent::PeerConnected(block_sync_session));
    }

    fn remove_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) {
        let removed_record = {
            let mut active_peers = self
                .inner
                .active_peers
                .lock()
                .expect("block-sync peer-state mutex is never poisoned");
            match active_peers.get(peer) {
                Some(record) if record.conn_id == conn_id => active_peers.remove(peer),
                Some(_) | None => None,
            }
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
        let _ = self
            .inner
            .lifecycle
            .send(BlockSyncEvent::PeerDisconnected(peer.clone()));
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
