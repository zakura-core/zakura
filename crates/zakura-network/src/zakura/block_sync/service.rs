use super::{config::*, events::*, peer_registry::SessionAdmission, wire::*, *};
use crate::zakura::{
    handle_pipe_exit, spawn_supervised_pipe, FramedRecv, FramedSend, OrderedSendError, Peer,
    PeerStreamSession, Service, ServicePeerSnapshot, SessionDemand, SessionOpening, SessionPolicy,
    SinkReject, Stream, ZakuraBlockSyncCandidateState, ZakuraConnId, ZakuraPeerId,
    FRAME_HEADER_BYTES,
};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};
use zakura_chain::serialization::ZcashDecoder;

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
    messages: Some(super::regulated::wire::RULES),
    ..Stream::PERSISTENT
}];

/// Service-declared streams for native block sync.
pub(crate) fn block_sync_streams() -> &'static [Stream] {
    &BLOCK_SYNC_SERVICE_STREAMS
}

/// Cloneable typed stream-6 sender.
#[derive(Clone, Debug)]
pub struct BlockSyncPeerSession {
    status_sender: Option<Arc<super::regulated::status_sender::StatusSender>>,
    peer_id: ZakuraPeerId,
    direction: ServicePeerDirection,
    send: FramedSend,
    cancel_token: CancellationToken,
}

impl BlockSyncPeerSession {
    pub(crate) fn new(session: &PeerStreamSession, direction: ServicePeerDirection) -> Self {
        Self {
            status_sender: None,
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
            status_sender: None,
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

    pub(super) fn request_sender(&self) -> FramedSend {
        self.send.clone()
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
        if let Some(sender) = &self.status_sender {
            return sender.try_send(status, &self.send);
        }
        self.try_send_message(BlockSyncMessage::Status(status))
    }

    /// Send a typed status advertisement, waiting for transport queue capacity.
    pub async fn send_status(&self, status: BlockSyncStatus) -> Result<(), OrderedSendError> {
        if self.status_sender.is_none() {
            return self.send_message(BlockSyncMessage::Status(status)).await;
        }
        loop {
            match self.try_send_status(status) {
                Err(OrderedSendError::Full) => {
                    let next = self
                        .status_next_due()
                        .unwrap_or_else(Instant::now)
                        .max(Instant::now() + Duration::from_millis(100));
                    tokio::select! {
                        () = self.cancel_token.cancelled() => return Err(OrderedSendError::Closed),
                        () = tokio::time::sleep_until(next.into()) => {},
                    }
                }
                result => return result,
            }
        }
    }

    pub(super) fn status_next_due(&self) -> Option<Instant> {
        self.status_sender
            .as_ref()
            .and_then(|sender| sender.next_due())
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
    decoder: ZcashDecoder,
    inner: Arc<BlockSyncServiceInner>,
    service_demand:
        Option<watch::Receiver<zakura_node_services::sync_lifecycle::SyncServiceDemand>>,
    _held_events: Option<Arc<StdMutex<mpsc::Receiver<BlockSyncEvent>>>>,
    _reactor_task: Option<JoinHandle<()>>,
}

#[derive(Debug)]
struct BlockSyncServiceInner {
    // Connection lifetime, not session lifetime: replacement cannot reset cadence.
    status_senders: StdMutex<
        HashMap<(ZakuraPeerId, ZakuraConnId), Arc<super::regulated::status_sender::StatusSender>>,
    >,
    requester_sessions: crate::zakura::regulation::SessionTable<()>,
    sessions: crate::zakura::regulation::SessionCapacity,
    config: ZakuraBlockSyncConfig,
    lifecycle: mpsc::UnboundedSender<BlockSyncEvent>,
    /// Shared download primitives every per-peer pipe-routine is wired with at
    /// `add_peer` (per-peer routines). `None` for the inert/handle-less constructors that never
    /// spawn routines (they only observe `events`/`lifecycle`).
    routine_wiring: Option<super::state::RoutineWiring>,
    peer_snapshot: watch::Receiver<ServicePeerSnapshot>,
    candidates: watch::Receiver<ZakuraBlockSyncCandidateState>,
    active_peers: StdMutex<HashMap<ZakuraPeerId, BlockSyncPeerRecord>>,
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
        let Ok(mut active_peers) = self.active_peers.lock() else {
            return false;
        };
        let owns_session = active_peers
            .get(peer)
            .is_some_and(|record| record.conn_id == conn_id && record.session_id == session_id);
        if !owns_session {
            return false;
        }

        self.requester_sessions.remove(
            peer,
            crate::zakura::regulation::SessionKey {
                conn_id,
                session_id,
            },
        );
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
        true
    }
}

impl BlockSyncService {
    pub(crate) fn new(config: ZakuraBlockSyncConfig, decoder: ZcashDecoder) -> Self {
        Self::new_with_startup(BlockSyncStartup::inert(config), decoder)
    }

    pub(crate) fn new_with_handle(
        config: ZakuraBlockSyncConfig,
        handle: BlockSyncHandle,
        decoder: ZcashDecoder,
    ) -> Self {
        Self {
            decoder,
            inner: Arc::new(BlockSyncServiceInner {
                status_senders: Default::default(),
                requester_sessions: Default::default(),
                sessions: crate::zakura::regulation::SessionCapacity::new(
                    "block-sync",
                    &config.peer_limits,
                ),
                config,
                lifecycle: handle.lifecycle.clone(),
                routine_wiring: handle.routine_wiring.clone(),
                peer_snapshot: handle.subscribe_peer_snapshot(),
                candidates: handle.subscribe_candidate_state(),
                active_peers: StdMutex::new(HashMap::new()),
                session_gap_claims: StdMutex::new(HashMap::new()),
                next_session_id: AtomicU64::new(1),
            }),
            service_demand: None,
            _held_events: None,
            _reactor_task: None,
        }
    }

    pub(crate) fn new_with_header_tip(
        config: ZakuraBlockSyncConfig,
        header_tip: watch::Receiver<(block::Height, block::Hash)>,
        decoder: ZcashDecoder,
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
        Self::new_with_startup(startup, decoder)
    }

    fn new_with_startup(startup: BlockSyncStartup, decoder: ZcashDecoder) -> Self {
        let config = startup.config.clone();
        let (handle, _actions, reactor_task) = spawn_block_sync_reactor(startup);
        Self {
            decoder,
            inner: Arc::new(BlockSyncServiceInner {
                status_senders: Default::default(),
                requester_sessions: Default::default(),
                sessions: crate::zakura::regulation::SessionCapacity::new(
                    "block-sync",
                    &config.peer_limits,
                ),
                config,
                lifecycle: handle.lifecycle.clone(),
                routine_wiring: handle.routine_wiring.clone(),
                peer_snapshot: handle.subscribe_peer_snapshot(),
                candidates: handle.subscribe_candidate_state(),
                active_peers: StdMutex::new(HashMap::new()),
                session_gap_claims: StdMutex::new(HashMap::new()),
                next_session_id: AtomicU64::new(1),
            }),
            service_demand: None,
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
                decoder: ZcashDecoder::for_network(&zakura_chain::parameters::Network::Mainnet),
                inner: Arc::new(BlockSyncServiceInner {
                    status_senders: Default::default(),
                    requester_sessions: Default::default(),
                    sessions: crate::zakura::regulation::SessionCapacity::new(
                        "block-sync",
                        &config.peer_limits,
                    ),
                    config,
                    lifecycle,
                    routine_wiring: None,
                    peer_snapshot,
                    candidates,
                    active_peers: StdMutex::new(HashMap::new()),
                    session_gap_claims: StdMutex::new(HashMap::new()),
                    next_session_id: AtomicU64::new(1),
                }),
                service_demand: None,
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
        Self::new_with_handle(
            config,
            handle,
            ZcashDecoder::for_network(&zakura_chain::parameters::Network::Mainnet),
        )
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
            .active_peers
            .lock()
            .expect("block-sync peer map mutex is never poisoned")
            .len()
    }

    fn peer_slots_free(&self, direction: ServicePeerDirection) -> bool {
        let peers = self
            .inner
            .active_peers
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

    fn reserve_session(
        &self,
        direction: ServicePeerDirection,
    ) -> Result<Option<Arc<dyn crate::zakura::SessionResources>>, crate::zakura::SessionFull> {
        if self
            .inner
            .routine_wiring
            .as_ref()
            .is_some_and(|wiring| wiring.serving.is_some())
        {
            self.inner.sessions.reserve(direction).map(Some)
        } else {
            Ok(None)
        }
    }

    fn session_policy(&self) -> SessionPolicy {
        SessionPolicy {
            opening: SessionOpening::EitherSide,
            reopen: true,
        }
    }

    fn session_demand(
        &self,
        conn_id: ZakuraConnId,
        peer: &ZakuraPeerId,
        _negotiated: u64,
        direction: ServicePeerDirection,
    ) -> SessionDemand {
        if let Some(deadline) = self.peer_park_deadline(peer) {
            return SessionDemand::RetryAt(deadline);
        }

        if self
            .inner
            .routine_wiring
            .as_ref()
            .is_some_and(|wiring| wiring.serving.is_some())
        {
            let demand = self.inner.sessions.demand(direction);
            if !matches!(demand, SessionDemand::OpenNow) {
                return demand;
            }
        }

        let mut peer_snapshot = self.inner.peer_snapshot.clone();
        peer_snapshot.borrow_and_update();
        if !self.peer_slots_free(direction) {
            return SessionDemand::WaitForChange(Box::pin(async move {
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
                return SessionDemand::WaitForChange(Box::pin(async move {
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

        SessionDemand::OpenNow
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
            ZAKURA_BLOCK_SYNC_STREAM_VERSION,
            recv,
            send,
            peer.service_cancel_token(),
        );
        let service_cancel_token = session.cancel_token();
        let connection_cancel_token = peer.cancel_token();
        let close_cause = peer.close_cause();
        let mut block_sync_session = BlockSyncPeerSession::new(&session, peer.direction);
        let session_id = self.inner.next_session_id.fetch_add(1, Ordering::Relaxed);
        let conn_id = peer.conn_id;
        let (_session_peer, _stream_kind, _stream_version, recv, send, _session_cancel) =
            session.into_parts();

        // Production outbound block-sync frames go directly through
        // `BlockSyncPeerSession` (the per-peer routine's `try_send_get_blocks` /
        // the reactor's `try_send_status`/serving sends), so the raw transport
        // sender taken from the stream here is redundant. The outbound stream stays
        // alive through the `BlockSyncPeerSession` clone the reactor holds, so
        // nothing is lost by dropping it.
        drop(send);

        let requester_fence = self
            .inner
            .routine_wiring
            .as_ref()
            .filter(|wiring| wiring.serving.is_some())
            .map(|_| {
                crate::zakura::regulation::WriterFence::new(
                    connection_cancel_token.clone(),
                    close_cause.clone(),
                )
            });
        let (old_record, re_admitted_after_no_progress, routine_generation) = {
            let mut active_peers = self
                .inner
                .active_peers
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
            if let Some(fence) = &requester_fence {
                use crate::zakura::regulation::{Current, Replacement, SessionKey};
                if matches!(
                    self.inner.requester_sessions.replace(
                        peer_id.clone(),
                        Current {
                            key: SessionKey {
                                conn_id,
                                session_id
                            },
                            cancel: service_cancel_token.clone(),
                            fence: fence.clone(),
                            session: (),
                        }
                    ),
                    Replacement::Refused
                ) {
                    return;
                }
            }
            if requester_fence.is_some() {
                block_sync_session.status_sender = Some(
                    self.inner
                        .status_senders
                        .lock()
                        .expect("status sender map is not poisoned")
                        .entry((peer_id.clone(), conn_id))
                        .or_default()
                        .clone(),
                );
            }
            let old_record = active_peers.insert(
                peer_id.clone(),
                BlockSyncPeerRecord {
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
            )
        };
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
            let decoder = self.decoder;
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
                        let serving = wiring.serving.as_ref().map(|serving| {
                            serving.session(
                                &peer_id,
                                block_sync_session.request_sender(),
                                run_cancel.clone(),
                            )
                        });
                        let requester = requester_fence.map(|fence| {
                            super::regulated::live_requester::LiveRequester::new(
                                wiring.request_pool.clone(),
                                fence,
                                &recv,
                                block_sync_session.request_sender(),
                            )
                        });
                        let routine = super::peer_routine::PeerRoutine::new(
                            decoder,
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
                            wiring.routine_to_reactor,
                            wiring.view,
                            run_cancel,
                            wiring.trace,
                        )
                        .with_serving(serving)
                        .with_requester(requester);
                        tokio::select! {
                            biased;
                            () = connection_cancel_token.cancelled() => Ok(()),
                            result = routine.run() => result,
                        }
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

    fn owns_connection_for_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) -> bool {
        let session_is_active = self
            .inner
            .active_peers
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
            self.session_demand(conn_id, peer, ZAKURA_CAP_BLOCK_SYNC, direction),
            SessionDemand::OpenNow
        )
    }

    fn remove_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) {
        let removed_record = {
            let mut active_peers = self
                .inner
                .active_peers
                .lock()
                .expect("block-sync peer map mutex is never poisoned");
            self.inner
                .status_senders
                .lock()
                .expect("status sender map is not poisoned")
                .remove(&(peer.clone(), conn_id));
            let removed = match active_peers.get(peer) {
                Some(record) if record.conn_id == conn_id => {
                    self.inner.requester_sessions.remove(
                        peer,
                        crate::zakura::regulation::SessionKey {
                            conn_id,
                            session_id: record.session_id,
                        },
                    );
                    active_peers.remove(peer)
                }
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

#[cfg(test)]
mod requester_session_tests {
    use super::*;
    use crate::zakura::{
        regulation::{Current, SessionKey, WriterFence},
        CloseCause,
    };

    #[tokio::test]
    async fn connection_removal_retires_and_removes_its_requester_fence() {
        let service = BlockSyncService::new(
            ZakuraBlockSyncConfig::default(),
            ZcashDecoder::for_network(&zakura_chain::parameters::Network::Mainnet),
        );
        let peer = ZakuraPeerId::new(vec![41; 32]).unwrap();
        let connection = CancellationToken::new();
        let cancel = CancellationToken::new();
        let fence = WriterFence::new(connection.clone(), CloseCause::default());
        let exchange = fence.open().unwrap();
        let writer = exchange.writer();
        assert!(writer.publish(|| {}));
        assert!(writer.try_start(|| true));
        service.inner.requester_sessions.replace(
            peer.clone(),
            Current {
                key: SessionKey {
                    conn_id: 7,
                    session_id: 3,
                },
                cancel: cancel.clone(),
                fence: fence.clone(),
                session: (),
            },
        );
        service.inner.active_peers.lock().unwrap().insert(
            peer.clone(),
            BlockSyncPeerRecord {
                conn_id: 7,
                session_id: 3,
                direction: ServicePeerDirection::Outbound,
                cancel_token: cancel.clone(),
            },
        );
        service.remove_peer(&peer, 6);
        assert!(service.inner.requester_sessions.get(&peer).is_some());
        assert!(
            !connection.is_cancelled(),
            "stale removal leaves the current fence open"
        );
        service.remove_peer(&peer, 7);
        assert!(service.inner.requester_sessions.get(&peer).is_none());
        assert!(
            connection.is_cancelled(),
            "removal fences the unanswered written exchange"
        );
        assert!(cancel.is_cancelled());
        assert!(fence.open().is_none());
    }
}

#[cfg(test)]
mod regulated_frame_tests {
    use super::*;

    #[derive(Debug)]
    struct EmptySource;

    impl crate::zakura::BlockRangeSource for EmptySource {
        fn read(
            &self,
            request: crate::zakura::BlockRangeRead,
        ) -> futures::future::BoxFuture<
            'static,
            Result<crate::zakura::BlockRangeReadResult, crate::BoxError>,
        > {
            Box::pin(async move {
                Ok(crate::zakura::BlockRangeReadResult {
                    blocks: vec![],
                    lease: request.lease,
                })
            })
        }
    }

    #[tokio::test]
    async fn status_cadence_survives_session_replacement_and_ends_with_its_connection() {
        let service = BlockSyncService::new_with_startup(
            BlockSyncStartup::inert(ZakuraBlockSyncConfig::default())
                .with_range_source(Arc::new(EmptySource)),
            ZcashDecoder::for_network(&zakura_chain::parameters::Network::Mainnet),
        );
        let peer = ZakuraPeerId::new(vec![42; 32]).unwrap();
        let mut held_channels = Vec::new();
        let mut attach = |conn_id| {
            let (in_tx, in_rx) = crate::zakura::framed_channel(4);
            let (out_tx, out_rx) = crate::zakura::framed_channel(4);
            service.add_peer(Peer::new_with_conn_id_and_direction(
                conn_id,
                peer.clone(),
                None,
                ZAKURA_CAP_BLOCK_SYNC,
                ServicePeerDirection::Outbound,
                HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (in_rx, out_tx))]),
                CancellationToken::new(),
            ));
            held_channels.push((in_tx, out_rx));
            service
                .inner
                .status_senders
                .lock()
                .unwrap()
                .get(&(peer.clone(), conn_id))
                .unwrap()
                .clone()
        };
        let first = attach(1);
        let replacement = attach(1);
        assert!(Arc::ptr_eq(&first, &replacement));
        let new_connection = attach(2);
        assert!(!Arc::ptr_eq(&first, &new_connection));
        service.remove_peer(&peer, 1);
        assert!(!service
            .inner
            .status_senders
            .lock()
            .unwrap()
            .contains_key(&(peer.clone(), 1)));
        assert!(service
            .inner
            .status_senders
            .lock()
            .unwrap()
            .contains_key(&(peer.clone(), 2)));
        service.remove_peer(&peer, 2);
        assert!(service.inner.status_senders.lock().unwrap().is_empty());
    }

    use crate::zakura::{
        transport::{FrameFilter, InboundReader},
        MessageRole,
    };

    #[test]
    fn production_getblocks_layout_declares_exact_control_and_body_limits() {
        Stream::check_layout(block_sync_streams()).unwrap();
        let stream = &block_sync_streams()[0];
        let filter = FrameFilter::new(stream.messages, InboundReader::Persistent);
        for (tag, bytes) in [(1, 53), (2, 9), (3, 2_000_001), (4, 9), (5, 9)] {
            assert_eq!(
                filter
                    .check_header(tag, 0, bytes, usize::try_from(stream.frame_cap).unwrap())
                    .unwrap(),
                bytes + FRAME_HEADER_BYTES
            );
            assert!(filter
                .check_header(tag, 1, bytes, usize::try_from(stream.frame_cap).unwrap())
                .is_err());
        }
        assert!(filter
            .check_header(6, 0, 1, usize::try_from(stream.frame_cap).unwrap())
            .is_err());
        assert!(filter
            .check_header(4, 0, 8, usize::try_from(stream.frame_cap).unwrap())
            .is_err());
        let MessageRole::Announcement { cadence } = stream.messages.unwrap()[0].role else {
            panic!("Status is an announcement")
        };
        assert_eq!(cadence.capacity, 22);
        assert_eq!(cadence.send_interval, Duration::from_secs(30));
    }
}
