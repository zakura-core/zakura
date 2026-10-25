use zakura_rt::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use super::{
    config::*, events::*, peer_registry::SessionAdmission, regulated::status_sender::StatusSend,
    wire::*, *,
};
use crate::zakura::{
    handle_pipe_exit, spawn_supervised_pipe, FramedRecv, FramedSend, OrderedSendError, Peer,
    PeerStreamSession, Service, ServicePeerSnapshot, SessionDemand, SessionOpening, SessionPolicy,
    SinkReject, Stream, StreamQueueDepths, ZakuraBlockSyncCandidateState, ZakuraConnId,
    ZakuraPeerId, FRAME_HEADER_BYTES,
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

/// Stream-6 frames the transport reader may queue for the peer routine.
///
/// The live reservations' owed bound grows with the request window, at up to
/// one 2 MB body per live range. This depth caps the queue at a constant
/// instead. A session holds these frames, the one the reader waits to forward
/// and the one the routine is working on: 10 frames, about 20 MB. Later
/// frames wait in the QUIC stream receive window, so
/// the cap only slows a peer when the routine is already the bottleneck.
/// Requests and control share the queue, so it keeps the two slots that
/// regulation needs (`minimum_ordered_queue_capacity`).
const BLOCK_SYNC_INBOUND_QUEUE_DEPTH: usize = 8;

const BLOCK_SYNC_SERVICE_STREAMS: [Stream; 1] = [Stream {
    kind: ZAKURA_STREAM_BLOCK_SYNC,
    version: ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
    frame_cap: MAX_BS_FRAME_BYTES,
    capability: ZAKURA_CAP_BLOCK_SYNC_REGULATED,
    messages: Some(super::regulated::wire::RULES),
    queue_depths: Some(StreamQueueDepths {
        inbound: BLOCK_SYNC_INBOUND_QUEUE_DEPTH,
        // Serving output has its own byte bound, so writes keep the
        // transport's depth.
        outbound: usize::MAX,
    }),
    ..Stream::PERSISTENT
}];

/// The block-sync stream layout.
#[cfg(test)]
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

    #[cfg(test)]
    pub(super) fn with_status_for_test(mut self, status: BlockSyncStatus) -> Self {
        let state = Arc::new(super::regulated::status_sender::StatusSender::new(
            u32::MAX,
            Duration::ZERO,
        ));
        state.record_received(status);
        self.status_sender = Some(state);
        self
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

    /// `status` as this session sends it: regulated sessions apply their serving limit.
    pub(super) fn wire_status(&self, status: BlockSyncStatus) -> BlockSyncStatus {
        self.status_sender
            .as_ref()
            .map_or(status, |sender| sender.advertised(status))
    }

    /// Send a typed status advertisement.
    ///
    /// Returns `Full` when the connection's cadence defers the Status, when the
    /// previous Status is still unwritten, or when the outbound queue is full.
    pub fn try_send_status(&self, status: BlockSyncStatus) -> Result<(), OrderedSendError> {
        match self.offer_status(status)? {
            StatusSend::Queued => Ok(()),
            StatusSend::Deferred(_) | StatusSend::Unwritten | StatusSend::QueueFull => {
                Err(OrderedSendError::Full)
            }
        }
    }

    /// Offer the latest local status to this connection's Status cadence.
    pub(super) fn offer_status(
        &self,
        status: BlockSyncStatus,
    ) -> Result<StatusSend, OrderedSendError> {
        if let Some(sender) = &self.status_sender {
            return sender.try_send(status, &self.send);
        }
        match self.try_send_message(BlockSyncMessage::Status(status)) {
            Ok(()) => Ok(StatusSend::Queued),
            Err(OrderedSendError::Full) => Ok(StatusSend::QueueFull),
            Err(error) => Err(error),
        }
    }

    /// Send a typed status advertisement, waiting for its cadence and for
    /// transport queue capacity.
    pub async fn send_status(&self, status: BlockSyncStatus) -> Result<(), OrderedSendError> {
        loop {
            let retry_at = match self.offer_status(status)? {
                StatusSend::Queued => return Ok(()),
                StatusSend::Deferred(at) => at,
                StatusSend::Unwritten | StatusSend::QueueFull => {
                    Instant::now() + Duration::from_millis(100)
                }
            };
            zakura_rt::select! {
                () = self.cancel_token.cancelled() => return Err(OrderedSendError::Closed),
                () = zakura_rt::time::sleep_until(retry_at) => {},
            }
        }
    }

    pub(super) fn received_status(&self) -> Option<BlockSyncStatus> {
        self.status_sender
            .as_ref()
            .and_then(|state| state.received())
    }

    pub(super) fn record_received_status(&self, status: BlockSyncStatus) {
        if let Some(state) = &self.status_sender {
            state.record_received(status);
        }
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
}

/// Native stream-6 block-sync service scaffold.
#[derive(Debug)]
pub(crate) struct BlockSyncService {
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
    /// The registry generation admitted for this session, when routines are wired.
    generation: Option<u64>,
}

#[derive(Debug)]
struct SessionGapClaim {
    conn_id: ZakuraConnId,
    direction: ServicePeerDirection,
}

impl BlockSyncServiceInner {
    /// End this session if it still owns the peer's record: close its registry
    /// generation, leave a gap claim for a stream reopen, and queue its
    /// disconnect. A replaced session does nothing.
    ///
    /// The disconnect is queued under the peer-map lock, so the reactor sees it
    /// before the connect of any session admitted after this one.
    fn finish_session(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId, session_id: u64) {
        let Ok(mut active_peers) = self.active_peers.lock() else {
            return;
        };
        let owns_session = active_peers
            .get(peer)
            .is_some_and(|record| record.conn_id == conn_id && record.session_id == session_id);
        if !owns_session {
            return;
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
        // Close before queueing the disconnect below, so the reactor removes
        // this generation's entry and not a later admission's.
        if let (Some(wiring), Some(generation)) = (&self.routine_wiring, removed.generation) {
            wiring.registry.close_session(peer, generation);
        }

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
        let _ = self
            .lifecycle
            .send(BlockSyncEvent::PeerDisconnected(peer.clone()));
    }
}

impl BlockSyncService {
    pub(crate) fn new(config: ZakuraBlockSyncConfig) -> Result<Self, BlockSyncStartError> {
        Self::new_with_startup(BlockSyncStartup::inert(config))
    }

    /// Serve with a reactor that already validated its config.
    pub(crate) fn new_with_handle(handle: BlockSyncHandle) -> Self {
        let config = handle
            .routine_wiring
            .as_ref()
            .expect("a handle from spawn_block_sync_reactor always carries routine wiring")
            .config
            .clone();
        Self::from_handle(config, handle)
    }

    fn from_handle(config: ZakuraBlockSyncConfig, handle: BlockSyncHandle) -> Self {
        Self {
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
    ) -> Result<Self, BlockSyncStartError> {
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

    fn new_with_startup(startup: BlockSyncStartup) -> Result<Self, BlockSyncStartError> {
        let config = startup.config.clone();
        let (handle, _actions, reactor_task) = spawn_block_sync_reactor(startup)?;
        Ok(Self {
            _reactor_task: Some(reactor_task),
            ..Self::from_handle(config, handle)
        })
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
        Self::from_handle(config, handle)
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

    /// Recheck local policy, skipping only capacity already owned by the session.
    fn demand_for_session(
        &self,
        conn_id: ZakuraConnId,
        peer: &ZakuraPeerId,
        direction: ServicePeerDirection,
        reserved: bool,
    ) -> SessionDemand {
        if let Some(deadline) = self.peer_park_deadline(peer) {
            // Block sync still runs on std's clock.
            return SessionDemand::RetryAt(deadline);
        }

        if !reserved && self.inner.routine_wiring.is_some() {
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
                        zakura_rt::select! {
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
        &BLOCK_SYNC_SERVICE_STREAMS
    }

    fn reserve_session(
        &self,
        direction: ServicePeerDirection,
    ) -> Result<Option<Arc<dyn crate::zakura::SessionResources>>, crate::zakura::SessionFull> {
        if self.inner.routine_wiring.is_some() {
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
        self.demand_for_session(conn_id, peer, direction, false)
    }

    fn reserved_session_demand(
        &self,
        conn_id: ZakuraConnId,
        peer: &ZakuraPeerId,
        _negotiated: u64,
        direction: ServicePeerDirection,
    ) -> SessionDemand {
        self.demand_for_session(conn_id, peer, direction, true)
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

        let Some((_, version, recv, send)) =
            peer.take_versioned_stream_with_session_id(ZAKURA_STREAM_BLOCK_SYNC)
        else {
            return;
        };
        // Public in-process Peer constructors predate stream version metadata.
        let version = if version == 0 {
            ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION
        } else {
            version
        };
        // Serving needs room for a response and for requests or control.
        if version != ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION
            || (self.inner.routine_wiring.is_some()
                && (peer.negotiated & ZAKURA_CAP_BLOCK_SYNC_REGULATED == 0
                    || send.max_capacity() < 2))
        {
            peer.close_cause().record("block_sync_regulation_contract");
            peer.cancel_token().cancel();
            return;
        }

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
        let mut block_sync_session = BlockSyncPeerSession::new(&session, peer.direction);
        let session_id = self.inner.next_session_id.fetch_add(1, Ordering::Relaxed);
        let conn_id = peer.conn_id;
        let (_session_peer, _stream_kind, _stream_version, recv, send, _session_cancel) =
            session.into_parts();

        // Production outbound block-sync frames go directly through
        // `BlockSyncPeerSession` (the per-peer routine's `GetBlocks` send /
        // the reactor's `offer_status`, the serving session), so the raw transport
        // sender taken from the stream here is redundant. The outbound stream stays
        // alive through the `BlockSyncPeerSession` clone the reactor holds, so
        // nothing is lost by dropping it.
        drop(send);

        let routine_wiring = self.inner.routine_wiring.clone();
        let (old_record, re_admitted_after_no_progress, routine) = {
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
            // A newer connection in its reopen gap also supersedes this one.
            // Claims only change under the peer-map lock, so this check and
            // the removal below see the same claim.
            if self
                .inner
                .session_gap_claims
                .lock()
                .expect("block-sync gap-claim mutex is never poisoned")
                .get(&peer_id)
                .is_some_and(|claim| claim.conn_id > conn_id)
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
            // A spawned routine holds its generation and the exact fence
            // registered in `requester_sessions`.
            let (routine, re_admitted_after_no_progress) = match routine_wiring {
                Some(wiring) => {
                    let (generation, readmitted) = match wiring.registry.admit_session(
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
                        SessionAdmission::Readmitted { generation } => (generation, true),
                        SessionAdmission::Fresh { generation } => (generation, false),
                    };
                    let fence = crate::zakura::regulation::WriterFence::new(
                        connection_cancel_token.clone(),
                        close_cause.clone(),
                    );
                    (Some((wiring, generation, fence)), readmitted)
                }
                None => (None, false),
            };
            if let Some((wiring, generation, fence)) = &routine {
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
                    // No session owns this admission. The kept session's
                    // disconnect removes the closed entry.
                    wiring.registry.close_session(&peer_id, *generation);
                    return;
                }
                block_sync_session.status_sender = Some(
                    self.inner
                        .status_senders
                        .lock()
                        .expect("status sender map is not poisoned")
                        .entry((peer_id.clone(), conn_id))
                        .or_insert_with(|| {
                            Arc::new(super::regulated::status_sender::StatusSender::new(
                                super::regulated::session::serving_max_inflight_requests(
                                    &self.inner.config,
                                ),
                                self.inner.config.status_refresh_interval,
                            ))
                        })
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
                    generation: routine.as_ref().map(|(_, generation, _)| *generation),
                },
            );
            // The admitted session supersedes any gap claim left by its
            // predecessor. Clearing it under the peer-map lock keeps a later
            // session's `finish_session` claim from being removed here.
            self.inner
                .session_gap_claims
                .lock()
                .expect("block-sync gap-claim mutex is never poisoned")
                .remove(&peer_id);
            // Queue the connect under the peer-map lock and before spawning. The
            // reactor then sees it after the disconnect of any session that ended
            // before this admission, and before this session's own disconnect.
            let _ = self
                .inner
                .lifecycle
                .send(BlockSyncEvent::PeerConnected(block_sync_session.clone()));
            (old_record, re_admitted_after_no_progress, routine)
        };
        if let Some(old_record) = old_record {
            old_record.cancel_token.cancel();
        }

        let run_cancel = service_cancel_token.clone();
        let on_teardown = {
            let peer_id = peer_id.clone();
            let inner = self.inner.clone();
            move || inner.finish_session(&peer_id, conn_id, session_id)
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
            let block_sync_session = block_sync_session.clone();
            let peer_id = peer_id.clone();
            async move {
                let result = match routine {
                    Some((wiring, generation, fence)) => {
                        let serving = wiring.serving.session(
                            &peer_id,
                            block_sync_session.request_sender(),
                            run_cancel.clone(),
                            connection_cancel_token.clone(),
                            close_cause.clone(),
                        );
                        let requester = super::regulated::live_requester::LiveRequester::new(
                            wiring.request_pool,
                            fence,
                            &recv,
                            block_sync_session.request_sender(),
                        );
                        let routine = super::peer_routine::PeerRoutine::new(
                            peer_id,
                            conn_id,
                            block_sync_session,
                            recv,
                            requester,
                            serving,
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
                        );
                        zakura_rt::select! {
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
            "block_sync_pipe",
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
        // The exiting session can still hold its reservation until its disconnect
        // is processed, so the session-capacity gate would count it against itself.
        matches!(
            self.demand_for_session(conn_id, peer, direction, true),
            SessionDemand::OpenNow
        )
    }

    fn remove_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) {
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
        drop(claims);
        // Close the registry generation and queue the disconnect under the
        // peer-map lock, as in `finish_session`.
        if let Some(wiring) = &self.inner.routine_wiring {
            if let Some(generation) = removed.as_ref().and_then(|record| record.generation) {
                wiring.registry.close_session(peer, generation);
            }
            wiring
                .registry
                .connection_closed(peer, conn_id, Instant::now());
        }
        if let Some(record) = removed {
            record.cancel_token.cancel();
            let _ = self
                .inner
                .lifecycle
                .send(BlockSyncEvent::PeerDisconnected(peer.clone()));
        }
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
        zakura_rt::select! {
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

    fn with_session_slots(inbound: usize, outbound: usize) -> ZakuraBlockSyncConfig {
        let mut config = ZakuraBlockSyncConfig::default();
        config.peer_limits.max_inbound_peers = inbound;
        config.peer_limits.max_outbound_peers = outbound;
        config
    }

    /// 3,633 slots fund one advertised request each; 3,634 cannot, so neither the
    /// reactor nor the service starts and nothing is advertised.
    #[tokio::test]
    async fn an_unfundable_config_fails_before_the_reactor_or_service_starts() {
        for config in [
            with_session_slots(3_000, 634),
            with_session_slots(usize::MAX, usize::MAX),
        ] {
            assert!(matches!(
                spawn_block_sync_reactor(BlockSyncStartup::inert(config.clone())),
                Err(BlockSyncStartError::InvalidConfig(_))
            ));
            assert!(matches!(
                BlockSyncService::new(config),
                Err(BlockSyncStartError::InvalidConfig(_))
            ));
        }

        let service = BlockSyncService::new(with_session_slots(3_000, 633))
            .expect("3,633 slots fund one request each");
        let wiring = service.inner.routine_wiring.as_ref().unwrap();
        assert_eq!(
            super::super::regulated::session::serving_max_inflight_requests(&wiring.config),
            1
        );
    }

    /// A served handle carries the config its reactor validated.
    #[tokio::test]
    async fn a_handle_serves_with_the_config_its_reactor_validated() {
        let config = with_session_slots(7, 9);
        let (handle, _actions, _task) =
            spawn_block_sync_reactor(BlockSyncStartup::inert(config.clone()))
                .expect("valid block-sync config");
        let service = BlockSyncService::new_with_handle(handle);
        assert_eq!(service.inner.config.peer_limits, config.peer_limits);
    }

    /// Zero serving slots is a valid config that admits no session in either
    /// direction.
    #[tokio::test]
    async fn zero_session_slots_admit_no_serving_session() {
        let service =
            BlockSyncService::new(with_session_slots(0, 0)).expect("zero slots are valid");
        let peer = ZakuraPeerId::new(vec![42; 32]).unwrap();
        for direction in [
            ServicePeerDirection::Inbound,
            ServicePeerDirection::Outbound,
        ] {
            assert!(!service.wants_peer(&peer, 0, direction));
            assert!(service.reserve_session(direction).is_err());
        }
    }

    #[tokio::test]
    async fn connection_removal_retires_and_removes_its_requester_fence() {
        let service = BlockSyncService::new(ZakuraBlockSyncConfig::default())
            .expect("valid block-sync config");
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
                generation: None,
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

    /// `finish_session` itself queues the disconnect, so a teardown worker that
    /// stalls after it returns cannot let a replacement's connect reach the
    /// reactor first.
    #[tokio::test]
    async fn a_finished_session_queues_its_disconnect_before_a_replacement_connects() {
        let (service, mut events) =
            BlockSyncService::new_for_test(ZakuraBlockSyncConfig::default());
        let peer = ZakuraPeerId::new(vec![0x53; 32]).unwrap();
        let conn_id = 1;
        let mut held_channels = Vec::new();
        let mut attach = || {
            let (in_tx, in_rx) = crate::zakura::framed_channel(4);
            let (out_tx, out_rx) = crate::zakura::framed_channel(4);
            service.add_peer(Peer::new_with_conn_id_and_direction(
                conn_id,
                peer.clone(),
                None,
                ZAKURA_CAP_BLOCK_SYNC_REGULATED,
                ServicePeerDirection::Outbound,
                HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (in_rx, out_tx))]),
                CancellationToken::new(),
            ));
            held_channels.push((in_tx, out_rx));
        };
        /// Wait for the next lifecycle event the service queued.
        async fn next_event(events: &mut mpsc::Receiver<BlockSyncEvent>) -> BlockSyncEvent {
            tokio::time::timeout(Duration::from_secs(1), events.recv())
                .await
                .expect("lifecycle event arrives")
                .expect("lifecycle channel stays open")
        }

        attach();
        assert!(matches!(
            next_event(&mut events).await,
            BlockSyncEvent::PeerConnected(session) if session.peer_id() == &peer
        ));
        let session_id = service.inner.active_peers.lock().unwrap()[&peer].session_id;

        // Run the first session's teardown step directly, as if its pipe worker
        // stalled right after it. The stream then reopens on the same connection.
        service.inner.finish_session(&peer, conn_id, session_id);
        attach();

        assert!(matches!(
            next_event(&mut events).await,
            BlockSyncEvent::PeerDisconnected(gone) if gone == peer
        ));
        assert!(matches!(
            next_event(&mut events).await,
            BlockSyncEvent::PeerConnected(session) if session.peer_id() == &peer
        ));
    }

    /// A same-connection replacement refused because the old session has a
    /// started exchange closes the connection and keeps the old record. The
    /// replacement's unused registry generation must not outlive the old
    /// session's disconnect.
    #[tokio::test]
    async fn a_refused_replacement_leaves_no_registry_entry_after_the_old_session_ends() {
        let mut relayed =
            super::super::tests::RelayedBlockSync::spawn(ZakuraBlockSyncConfig::default());
        let service = &relayed.service;
        let wiring = service.inner.routine_wiring.as_ref().unwrap();
        let peer = ZakuraPeerId::new(vec![44; 32]).unwrap();
        let (conn_id, session_id) = (7, 3);
        let generation = wiring
            .registry
            .admit_session(
                &peer,
                ServicePeerDirection::Outbound,
                &wiring.config,
                conn_id,
                Instant::now(),
            )
            .generation();
        let connection = CancellationToken::new();
        let fence = WriterFence::new(connection.clone(), CloseCause::default());
        let exchange = fence.open().unwrap();
        let writer = exchange.writer();
        assert!(writer.publish(|| {}));
        assert!(writer.try_start(|| true));
        let cancel = CancellationToken::new();
        service.inner.requester_sessions.replace(
            peer.clone(),
            Current {
                key: SessionKey {
                    conn_id,
                    session_id,
                },
                cancel: cancel.clone(),
                fence,
                session: (),
            },
        );
        service.inner.active_peers.lock().unwrap().insert(
            peer.clone(),
            BlockSyncPeerRecord {
                conn_id,
                session_id,
                direction: ServicePeerDirection::Outbound,
                cancel_token: cancel,
                generation: Some(generation),
            },
        );

        let _stream = relayed.add_outbound(conn_id, &peer);
        assert!(
            connection.is_cancelled(),
            "the started exchange closes the connection"
        );
        assert_eq!(
            service.inner.active_peers.lock().unwrap()[&peer].session_id,
            session_id,
            "the old record stays until its teardown",
        );

        // The old session's teardown queues the first lifecycle event, since the
        // refused replacement queued no connect.
        service.inner.finish_session(&peer, conn_id, session_id);
        let disconnect = relayed.next_event().await;
        assert!(matches!(&disconnect, BlockSyncEvent::PeerDisconnected(gone) if gone == &peer));
        assert_eq!(relayed.relay(disconnect).await, 0);
        assert!(
            !relayed.has_entry(&peer),
            "the refused replacement's generation must not outlive the old session",
        );
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
    async fn an_older_connection_cannot_clear_a_newer_gap_claim() {
        let service = BlockSyncService::new_with_startup(
            BlockSyncStartup::inert(ZakuraBlockSyncConfig::default())
                .with_range_source(Arc::new(EmptySource)),
        )
        .expect("valid block-sync config");
        let peer = ZakuraPeerId::new(vec![44; 32]).unwrap();
        let mut held_channels = Vec::new();
        let mut attach = |conn_id| {
            let (in_tx, in_rx) = crate::zakura::framed_channel(4);
            let (out_tx, out_rx) = crate::zakura::framed_channel(4);
            service.add_peer(Peer::new_with_service_streams(
                conn_id,
                peer.clone(),
                None,
                ZAKURA_CAP_BLOCK_SYNC_REGULATED,
                ServicePeerDirection::Outbound,
                HashMap::from([(
                    ZAKURA_STREAM_BLOCK_SYNC,
                    crate::zakura::transport::ServiceStream::new(
                        0,
                        ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
                        in_rx,
                        out_tx,
                        CancellationToken::new(),
                    ),
                )]),
                CancellationToken::new(),
                crate::zakura::CloseCause::default(),
            ));
            held_channels.push((in_tx, out_rx));
        };
        let claim_conn = |service: &BlockSyncService| {
            service
                .inner
                .session_gap_claims
                .lock()
                .unwrap()
                .get(&peer)
                .map(|claim| claim.conn_id)
        };
        // Connection 2's session exited and left a claim for its reopen.
        service.inner.session_gap_claims.lock().unwrap().insert(
            peer.clone(),
            SessionGapClaim {
                conn_id: 2,
                direction: ServicePeerDirection::Outbound,
            },
        );

        // A delayed admission for connection 1 arrives during that gap.
        attach(1);
        assert_eq!(service.peer_count(), 0);
        assert_eq!(claim_conn(&service), Some(2));

        attach(2);
        assert_eq!(service.peer_count(), 1);
        assert_eq!(claim_conn(&service), None);
    }

    #[tokio::test]
    async fn status_cadence_survives_session_replacement_and_ends_with_its_connection() {
        let service = BlockSyncService::new_with_startup(
            BlockSyncStartup::inert(ZakuraBlockSyncConfig::default())
                .with_range_source(Arc::new(EmptySource)),
        )
        .expect("valid block-sync config");
        let peer = ZakuraPeerId::new(vec![42; 32]).unwrap();
        let mut held_channels = Vec::new();
        let mut attach = |conn_id| {
            let (in_tx, in_rx) = crate::zakura::framed_channel(4);
            let (out_tx, out_rx) = crate::zakura::framed_channel(4);
            service.add_peer(Peer::new_with_service_streams(
                conn_id,
                peer.clone(),
                None,
                ZAKURA_CAP_BLOCK_SYNC_REGULATED,
                ServicePeerDirection::Outbound,
                HashMap::from([(
                    ZAKURA_STREAM_BLOCK_SYNC,
                    crate::zakura::transport::ServiceStream::new(
                        0,
                        ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
                        in_rx,
                        out_tx,
                        CancellationToken::new(),
                    ),
                )]),
                CancellationToken::new(),
                crate::zakura::CloseCause::default(),
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
        let received = BlockSyncStatus {
            servable_high: block::Height(20),
            ..Default::default()
        };
        first.record_received(received);
        let replacement = attach(1);
        assert_eq!(replacement.received(), Some(received));
        assert!(Arc::ptr_eq(&first, &replacement));
        // Regulated connections advertise the budgeted serving limit, not the local one.
        let local = ZakuraBlockSyncConfig::default().initial_status();
        assert_eq!(
            replacement.advertised(local).max_inflight_requests,
            super::super::regulated::session::serving_max_inflight_requests(
                &ZakuraBlockSyncConfig::default()
            )
        );
        assert!(replacement.advertised(local).max_inflight_requests < local.max_inflight_requests);
        let new_connection = attach(2);
        assert!(!Arc::ptr_eq(&first, &new_connection));
        assert_eq!(new_connection.received(), None);
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

    /// Session replacement, session exit, and connection removal leave no
    /// per-peer or per-connection entry behind, however often a peer reconnects.
    #[tokio::test]
    async fn reconnect_churn_returns_block_sync_maps_to_baseline() {
        let service = BlockSyncService::new_with_startup(
            BlockSyncStartup::inert(ZakuraBlockSyncConfig::default())
                .with_range_source(Arc::new(EmptySource)),
        )
        .expect("valid block-sync config");
        let peer = ZakuraPeerId::new(vec![43; 32]).unwrap();
        let mut held_channels = Vec::new();
        let mut attach = |conn_id, connection: &CancellationToken| {
            let session = CancellationToken::new();
            let (in_tx, in_rx) = crate::zakura::framed_channel(4);
            let (out_tx, out_rx) = crate::zakura::framed_channel(4);
            service.add_peer(Peer::new_with_service_streams(
                conn_id,
                peer.clone(),
                None,
                ZAKURA_CAP_BLOCK_SYNC_REGULATED,
                ServicePeerDirection::Outbound,
                HashMap::from([(
                    ZAKURA_STREAM_BLOCK_SYNC,
                    crate::zakura::transport::ServiceStream::new(
                        0,
                        ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
                        in_rx,
                        out_tx,
                        session.clone(),
                    ),
                )]),
                connection.clone(),
                crate::zakura::CloseCause::default(),
            ));
            held_channels.push((in_tx, out_rx));
            session
        };
        let in_maps = |conn_id| {
            (
                service
                    .inner
                    .active_peers
                    .lock()
                    .unwrap()
                    .contains_key(&peer),
                service.inner.requester_sessions.get(&peer).is_some(),
                service
                    .inner
                    .status_senders
                    .lock()
                    .unwrap()
                    .contains_key(&(peer.clone(), conn_id)),
                service
                    .inner
                    .session_gap_claims
                    .lock()
                    .unwrap()
                    .get(&peer)
                    .is_some_and(|claim| claim.conn_id == conn_id),
            )
        };
        for conn_id in 1..=16 {
            let connection = CancellationToken::new();
            attach(conn_id, &connection);
            // A same-connection replacement keeps one entry per map.
            let session = attach(conn_id, &connection);
            assert_eq!(in_maps(conn_id), (true, true, true, false));
            if conn_id % 2 == 0 {
                // The session exits first and leaves a gap claim for its connection.
                session.cancel();
                tokio::time::timeout(Duration::from_secs(5), async {
                    while in_maps(conn_id) != (false, false, true, true) {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("the exiting session leaves only connection-scoped entries");
            }
            service.remove_peer(&peer, conn_id);
            connection.cancel();
            assert_eq!(in_maps(conn_id), (false, false, false, false));
        }
        assert!(service.inner.active_peers.lock().unwrap().is_empty());
        assert!(service.inner.status_senders.lock().unwrap().is_empty());
        assert!(service.inner.session_gap_claims.lock().unwrap().is_empty());
        let mut seen = service.inner.requester_sessions.subscribe();
        assert!(service
            .inner
            .requester_sessions
            .snapshot(&mut seen)
            .is_empty());
    }

    use crate::zakura::{
        transport::{FrameFilter, InboundReader},
        MessageRole,
    };

    #[tokio::test]
    async fn negotiation_requires_the_regulated_capability() {
        let service = Arc::new(
            BlockSyncService::new_with_startup(
                BlockSyncStartup::inert(ZakuraBlockSyncConfig::default())
                    .with_range_source(Arc::new(EmptySource)),
            )
            .expect("valid block-sync config"),
        );
        let registry = crate::zakura::ServiceRegistry::new(vec![service]).unwrap();
        // The retired version 2 bit and the retired header-sync bit select nothing.
        for caps in [1 << 3, 1 << 4] {
            assert!(registry.persistent_streams_for_negotiated(caps).is_empty());
        }
        for caps in [
            ZAKURA_CAP_BLOCK_SYNC_REGULATED,
            ZAKURA_CAP_BLOCK_SYNC_REGULATED | (1 << 3),
        ] {
            let streams = registry.persistent_streams_for_negotiated(caps);
            assert_eq!(streams.len(), 1);
            assert_eq!(
                streams[0].version,
                ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION
            );
            assert!(streams[0].messages.is_some());
        }
        let default = BlockSyncService::new(ZakuraBlockSyncConfig::default())
            .expect("valid block-sync config");
        assert_eq!(default.streams(), &BLOCK_SYNC_SERVICE_STREAMS);
    }

    #[tokio::test]
    async fn sessions_install_fences_and_reject_version_two_or_one_slot() {
        for (version, depth) in [(2, 2), (3, 1), (3, 2)] {
            let service = BlockSyncService::new_with_startup(
                BlockSyncStartup::inert(ZakuraBlockSyncConfig::default())
                    .with_range_source(Arc::new(EmptySource)),
            )
            .expect("valid block-sync config");
            let peer = ZakuraPeerId::new(vec![45; 32]).unwrap();
            let connection = CancellationToken::new();
            let (_input, recv) = crate::zakura::framed_channel(depth);
            let (send, _output) = crate::zakura::framed_channel(depth);
            service.add_peer(Peer::new_with_service_streams(
                1,
                peer.clone(),
                None,
                ZAKURA_CAP_BLOCK_SYNC_REGULATED,
                ServicePeerDirection::Outbound,
                HashMap::from([(
                    ZAKURA_STREAM_BLOCK_SYNC,
                    crate::zakura::transport::ServiceStream::new(
                        1,
                        version,
                        recv,
                        send,
                        connection.child_token(),
                    ),
                )]),
                connection.clone(),
                crate::zakura::CloseCause::default(),
            ));
            let accepted = version == 3 && depth >= 2;
            assert_eq!(connection.is_cancelled(), !accepted);
            assert_eq!(
                service.inner.requester_sessions.get(&peer).is_some(),
                accepted
            );
            assert_eq!(
                service
                    .inner
                    .status_senders
                    .lock()
                    .unwrap()
                    .contains_key(&(peer, 1)),
                accepted
            );
            connection.cancel();
        }
    }

    /// A reserved remote offer must retain policy checks without buying a second slot.
    #[tokio::test]
    async fn reserved_regulated_session_uses_the_last_slot_in_either_direction() {
        let mut config = ZakuraBlockSyncConfig::default();
        config.peer_limits.max_inbound_peers = 1;
        config.peer_limits.max_outbound_peers = 1;
        config.peer_limits.max_pending_escalations = 2;
        let service = BlockSyncService::new_with_startup(
            BlockSyncStartup::inert(config).with_range_source(Arc::new(EmptySource)),
        )
        .expect("valid block-sync config");
        let peer = ZakuraPeerId::new(vec![47; 32]).unwrap();
        for direction in [
            ServicePeerDirection::Inbound,
            ServicePeerDirection::Outbound,
        ] {
            let reserved = service.reserve_session(direction).unwrap().unwrap();
            assert!(matches!(
                service.session_demand(1, &peer, ZAKURA_CAP_BLOCK_SYNC_REGULATED, direction,),
                SessionDemand::WaitForChange(_)
            ));
            assert!(matches!(
                service.reserved_session_demand(
                    1,
                    &peer,
                    ZAKURA_CAP_BLOCK_SYNC_REGULATED,
                    direction,
                ),
                SessionDemand::OpenNow
            ));
            drop(reserved);
            assert!(matches!(
                service.session_demand(1, &peer, ZAKURA_CAP_BLOCK_SYNC_REGULATED, direction,),
                SessionDemand::OpenNow
            ));
        }
    }

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
