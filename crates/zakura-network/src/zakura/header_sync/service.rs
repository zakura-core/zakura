use std::sync::Arc;

use zakura_header_chain::Frontier;
use zakura_node_services::task::spawn_erased_named;
#[cfg(any(test, feature = "zakura-testkit"))]
use zakura_rt::sync::mpsc;
use zakura_rt::{
    collections::HashMap,
    sync::{watch, CancellationToken, Mutex as StdMutex, MutexGuard, PoisonError},
    task::JoinHandle,
    time::Instant,
};

use super::{
    config::{header_sync_header_bytes_for_network, COMMON_HEADER_BYTES, REGTEST_HEADER_BYTES},
    events::*,
    pipe::run_peer,
    v9::{
        CommonMessage, Cursor, LeaseOwner, Publisher, RegtestMessage, SubscribeError,
        SubscribeHeaders, Subscriber,
    },
    wire::*,
    Status,
};
#[cfg(test)]
use crate::zakura::framed_channel;
#[cfg(any(test, feature = "zakura-testkit"))]
use crate::zakura::ZakuraSupervisorHandle;
use crate::zakura::{
    handle_pipe_exit,
    regulation::{
        CadenceSendError, CadenceSender, Current, ExchangeWriter, Replacement, SessionKey,
        SessionTable, WriterFence,
    },
    spawn_supervised_pipe,
    transport::{GuardedFrameSlot, GuardedReserveError},
    wire_codec::encode_frame,
    BoxRunFuture, CloseCause, Frame, FramedRecv, FramedSend, OrderedSendError, Peer, Service,
    ServicePeerDirection, SessionDemand, SessionOpening, SessionPolicy, Sink, SinkReject, Stream,
    ZakuraConnId, ZakuraPeerId, ZAKURA_CAP_HEADER_SYNC,
};

/// Conservative re-offer delay when header sync publishes no peer backoff deadline.
const HEADER_SYNC_ADVISORY_BACKOFF: std::time::Duration = std::time::Duration::from_secs(60);

#[cfg(test)]
mod stream_tests {
    use super::*;
    use crate::zakura::{header_sync::v9, StreamQueueDepths};
    use zakura_chain::parameters::Network;

    /// Both header sizes declare stream 5 at version 9 under bit 10 alone,
    /// with at least two queue slots each way.
    #[test]
    fn declares_only_capability_bit_ten_at_stream_version_nine() {
        assert_eq!(ZAKURA_CAP_HEADER_SYNC, 1 << 10);
        for network in [Network::Mainnet, Network::new_regtest(Default::default())] {
            let layout = v9::layout(&network);
            assert_eq!(layout.len(), 1);
            let stream = layout[0];
            assert_eq!(
                (stream.kind, stream.version, stream.capability),
                (ZAKURA_STREAM_HEADER_SYNC, 9, 1 << 10)
            );
            assert!(stream.messages.is_some());
            assert!(matches!(
                stream.queue_depths,
                Some(StreamQueueDepths { inbound, outbound }) if inbound >= 2 && outbound >= 2
            ));
        }
    }
}

#[cfg(test)]
mod session_tests {
    use super::*;
    use crate::zakura::{framed_channel, header_sync::v9, wire_codec::decode_frame};
    use zakura_chain::block;

    /// A `Close` queued behind an `Open` follows it: retiring the session's
    /// fence before the `Open`'s first byte skips both, and otherwise both go
    /// out in order.
    #[tokio::test]
    async fn updates_queued_behind_a_skipped_open_are_skipped_too() {
        let genesis = Frontier::new(block::Height(0), block::Hash([1; 32]));
        let target = Frontier::new(block::Height(5), block::Hash([2; 32]));
        let status = v9::test_support::status::<REGTEST_HEADER_BYTES>(target);
        for retire in [false, true] {
            let connection = CancellationToken::new();
            let close_cause = CloseCause::new();
            let fence = WriterFence::new(connection.clone(), close_cause.clone());
            let (send, mut outbound) = framed_channel(8);
            let subscriber =
                Subscriber::new(REGTEST_HEADER_BYTES, fence.clone(), &framed_channel(1).1);
            let session = PeerSession::new(
                ZakuraPeerId::new(vec![0x71; 32]).expect("the peer ID has 32 bytes"),
                1,
                ServicePeerDirection::Outbound,
                send,
                CancellationToken::new(),
                connection.clone(),
                close_cause,
                subscriber,
                Arc::new(StdMutex::new(CadenceSender::new())),
            );
            session
                .try_open(&status, &[genesis], AuxSchema::None, 4)
                .expect("the Open is queued");
            assert!(
                session.try_close().expect("the queue has room"),
                "the Close is queued"
            );

            if retire {
                assert!(fence.retire());
                drop(session);
                assert_eq!(outbound.recv().await, None, "both updates are skipped");
            } else {
                for operation in [v9::Operation::Open, v9::Operation::Close] {
                    let frame = outbound.recv().await.expect("the update is written");
                    let Ok(v9::Message::SubscribeHeaders(update)) =
                        decode_frame::<v9::RegtestMessage>(&frame)
                    else {
                        panic!("the frame is a subscription update");
                    };
                    assert_eq!(update.operation, operation);
                }
            }
            assert!(!connection.is_cancelled());
        }
    }
}

/// Cloneable typed sender for one canonical header-sync stream.
#[derive(Clone, Debug)]
pub struct PeerSession {
    peer_id: ZakuraPeerId,
    session_id: u64,
    direction: ServicePeerDirection,
    inner: Arc<PeerSessionInner>,
}

#[derive(Debug)]
struct PeerSessionInner {
    send: FramedSend,
    cancel_token: CancellationToken,
    connection_cancel_token: CancellationToken,
    close_cause: CloseCause,
    /// The subscription this session holds on the peer, fenced so that
    /// retiring the session cannot orphan a subscription the peer still
    /// serves.
    subscriber: StdMutex<Subscriber>,
    /// The writer of the last `Open` this session queued. Each later `Grant`
    /// and `Close` follows it, so retirement that skips the `Open` skips them.
    open_writer: StdMutex<Option<ExchangeWriter>>,
    /// The connection's `Status` cadence.
    status: StatusCadence,
}

/// One connection's `Status` cadence, which every session of the connection
/// shares, so a reopened stream cannot reset it. `Status` rows match on every
/// network.
type StatusCadence = Arc<StdMutex<CadenceSender<CommonMessage>>>;

impl PeerSession {
    /// A session for `peer_id` that sends through `send`, subscribes with
    /// `subscriber`, and shares its connection's `status` cadence.
    #[allow(clippy::too_many_arguments)]
    fn new(
        peer_id: ZakuraPeerId,
        session_id: u64,
        direction: ServicePeerDirection,
        send: FramedSend,
        cancel_token: CancellationToken,
        connection_cancel_token: CancellationToken,
        close_cause: CloseCause,
        subscriber: Subscriber,
        status: StatusCadence,
    ) -> Self {
        Self {
            peer_id,
            session_id,
            direction,
            inner: Arc::new(PeerSessionInner {
                send,
                cancel_token,
                connection_cancel_token,
                close_cause,
                subscriber: StdMutex::new(subscriber),
                open_writer: StdMutex::new(None),
                status,
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn from_parts(
        peer_id: ZakuraPeerId,
        send: FramedSend,
        cancel_token: CancellationToken,
    ) -> Self {
        Self::from_parts_with_direction(peer_id, ServicePeerDirection::Inbound, send, cancel_token)
    }

    #[cfg(test)]
    pub(crate) fn from_parts_with_session_id(
        peer_id: ZakuraPeerId,
        session_id: u64,
        send: FramedSend,
        cancel_token: CancellationToken,
    ) -> Self {
        Self::for_test(
            peer_id,
            session_id,
            ServicePeerDirection::Inbound,
            send,
            cancel_token.clone(),
            cancel_token,
            CloseCause::new(),
        )
    }

    #[cfg(test)]
    pub(crate) fn from_parts_with_connection(
        peer_id: ZakuraPeerId,
        session_id: u64,
        send: FramedSend,
        service_cancel_token: CancellationToken,
        connection_cancel_token: CancellationToken,
        close_cause: CloseCause,
    ) -> Self {
        Self::for_test(
            peer_id,
            session_id,
            ServicePeerDirection::Inbound,
            send,
            service_cancel_token,
            connection_cancel_token,
            close_cause,
        )
    }

    #[cfg(test)]
    pub(crate) fn from_parts_with_direction(
        peer_id: ZakuraPeerId,
        direction: ServicePeerDirection,
        send: FramedSend,
        cancel_token: CancellationToken,
    ) -> Self {
        Self::for_test(
            peer_id,
            0,
            direction,
            send,
            cancel_token.clone(),
            cancel_token,
            CloseCause::new(),
        )
    }

    /// A Regtest session with its own fence and `Status` cadence.
    #[cfg(test)]
    fn for_test(
        peer_id: ZakuraPeerId,
        session_id: u64,
        direction: ServicePeerDirection,
        send: FramedSend,
        cancel_token: CancellationToken,
        connection_cancel_token: CancellationToken,
        close_cause: CloseCause,
    ) -> Self {
        let fence = WriterFence::new(connection_cancel_token.clone(), close_cause.clone());
        // An in-process reader with no frame header check.
        let subscriber = Subscriber::new(REGTEST_HEADER_BYTES, fence, &framed_channel(1).1);
        Self::new(
            peer_id,
            session_id,
            direction,
            send,
            cancel_token,
            connection_cancel_token,
            close_cause,
            subscriber,
            Arc::new(StdMutex::new(CadenceSender::new())),
        )
    }

    /// Authenticated peer identity.
    pub fn peer_id(&self) -> &ZakuraPeerId {
        &self.peer_id
    }

    /// Unique ordered-stream generation.
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    /// Direction of the underlying connection.
    pub fn direction(&self) -> ServicePeerDirection {
        self.direction
    }

    /// Peer disconnect/local shutdown token.
    pub fn cancel_token(&self) -> CancellationToken {
        self.inner.cancel_token.clone()
    }

    pub(super) fn disconnect_for_port_panic(&self) {
        self.inner.close_cause.record("header_port_panic");
        self.inner.connection_cancel_token.cancel();
    }

    /// Close the connection because a page of subscription `id` failed
    /// validation. The subscription acknowledges no further cursor.
    pub(super) fn disconnect_for_header_fault(&self, id: u64) {
        self.subscriber().validation_failed(id);
        self.inner.close_cause.record("header_peer_fault");
        self.inner.connection_cancel_token.cancel();
    }

    /// Current free slots in the bounded outbound queue.
    pub fn outbound_capacity(&self) -> usize {
        self.inner.send.capacity()
    }

    /// Total outbound queue slots.
    pub fn outbound_max_capacity(&self) -> usize {
        self.inner.send.max_capacity()
    }

    /// Lock this session's subscriber. No holder panics, so the lock is never
    /// poisoned.
    pub(super) fn subscriber(&self) -> MutexGuard<'_, Subscriber> {
        self.inner
            .subscriber
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Queue `status` under the connection's cadence. `Full` means the cadence
    /// or the queue holds it back, and it waits as the latest value.
    pub(super) fn try_send_status(&self, status: Status) -> Result<(), OrderedSendError> {
        let mut cadence = self
            .inner
            .status
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        cadence
            .update(CommonMessage::Status(status))
            .map_err(|error| OrderedSendError::Encode(error.into()))?;
        match cadence.send_due(&self.inner.send) {
            Ok(0) => Err(OrderedSendError::Full),
            Ok(_) => Ok(()),
            Err(CadenceSendError::Closed) => Err(OrderedSendError::Closed),
            Err(error) => Err(OrderedSendError::Encode(error.into())),
        }
    }

    /// Record an `Open` toward the selected tip of `status`, granting the
    /// window for `staging` reserved headers, and queue it under the session's
    /// fence. Returns the subscription ID.
    ///
    /// Nothing is recorded unless the queue has a slot.
    pub(super) fn try_open(
        &self,
        status: &Status,
        locators: &[Frontier],
        schema: AuxSchema,
        staging: u32,
    ) -> Result<HeaderSyncRequestId, OrderedSendError> {
        let slot = self.reserve()?;
        let mut subscriber = self.subscriber();
        let (update, writer) =
            subscriber
                .open(status, locators, schema, staging)
                .map_err(|error| match error {
                    SubscribeError::Fenced => OrderedSendError::Closed,
                    error => OrderedSendError::Encode(error.into()),
                })?;
        let id = update.subscription_id;
        if slot
            .send_fenced(update_frame(subscriber.header_bytes(), update), &writer)
            .is_err()
        {
            // The fence skipped the frame before its first byte.
            subscriber.retract(id);
            return Err(OrderedSendError::Closed);
        }
        *self.open_writer() = Some(writer);
        Ok(HeaderSyncRequestId::new(id).expect("subscription IDs are nonzero"))
    }

    /// Record a `Grant` that refills the window for `staging` reserved
    /// headers, and queue it, if one is due. Returns whether it queued one.
    pub(super) fn try_grant(&self, staging: u32) -> Result<bool, OrderedSendError> {
        let slot = self.reserve()?;
        let Some(writer) = self.open_writer().clone() else {
            return Ok(false);
        };
        let mut subscriber = self.subscriber();
        let Some(update) = subscriber.grant_due(staging) else {
            return Ok(false);
        };
        slot.send_following(update_frame(subscriber.header_bytes(), update), &writer)
            .map_err(|_| OrderedSendError::Closed)?;
        Ok(true)
    }

    /// Record a `Close` of the live subscription and queue it, unless it is
    /// already closing. Returns whether it queued one. Pages within the
    /// granted credit still arrive until the terminal outcome.
    pub(super) fn try_close(&self) -> Result<bool, OrderedSendError> {
        let slot = self.reserve()?;
        let Some(writer) = self.open_writer().clone() else {
            return Ok(false);
        };
        let mut subscriber = self.subscriber();
        let Ok(update) = subscriber.close() else {
            return Ok(false);
        };
        slot.send_following(update_frame(subscriber.header_bytes(), update), &writer)
            .map_err(|_| OrderedSendError::Closed)?;
        Ok(true)
    }

    /// The writer of the last `Open` this session queued, if any.
    fn open_writer(&self) -> MutexGuard<'_, Option<ExchangeWriter>> {
        self.inner
            .open_writer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Record that every page of subscription `id` through `cursor` passed
    /// validation. The next update acknowledges it.
    pub(super) fn accept(&self, id: u64, cursor: &Cursor) {
        // A zero staging budget grants nothing.
        let _ = self.subscriber().accept_through(id, cursor, 0);
    }

    /// Reserve a queue slot before recording an update.
    fn reserve(&self) -> Result<GuardedFrameSlot<'_>, OrderedSendError> {
        self.inner
            .send
            .try_reserve_guarded()
            .map_err(|error| match error {
                GuardedReserveError::Full => OrderedSendError::Full,
                GuardedReserveError::Closed | GuardedReserveError::Unsupported => {
                    OrderedSendError::Closed
                }
            })
    }
}

/// Encode an update the subscriber recorded, checking its credit against the
/// smallest page of a network whose headers take `header_bytes`.
fn update_frame(header_bytes: usize, update: SubscribeHeaders) -> Frame {
    if header_bytes == REGTEST_HEADER_BYTES {
        encode_frame(&RegtestMessage::SubscribeHeaders(update))
    } else {
        encode_frame(&CommonMessage::SubscribeHeaders(update))
    }
    .expect("the subscriber records only updates that encode")
}

/// Process actions that do not require the production state driver.
#[cfg(any(test, feature = "zakura-testkit"))]
pub(crate) async fn drive_header_sync_actions(
    mut actions: mpsc::Receiver<HeaderSyncAction>,
    handle: HeaderSyncHandle,
    _supervisor: ZakuraSupervisorHandle,
    shutdown: CancellationToken,
) {
    loop {
        let action = zakura_rt::select! {
            _ = shutdown.cancelled() => return,
            action = actions.recv() => match action {
                Some(action) => action,
                None => return,
            },
        };
        match action {
            HeaderSyncAction::Misbehavior { peer, reason } => {
                tracing::debug!(?peer, ?reason, "recorded Zakura header-sync peer violation");
            }
            HeaderSyncAction::DropPeer {
                peer,
                session_id,
                reason,
            } => {
                tracing::debug!(
                    ?peer,
                    session_id,
                    reason,
                    "dropped an unproductive Zakura header-sync peer"
                );
            }
            HeaderSyncAction::QueryHeaderLocator {
                peer,
                session_id,
                target_tip_hash,
                scope,
            } => {
                let _ = handle
                    .send(Event::HeaderLocatorReady {
                        peer,
                        session_id,
                        target_tip_hash,
                        scope,
                        locator: None,
                    })
                    .await;
            }
            HeaderSyncAction::QueryVctRepairContext { owner, .. } => {
                let _ = handle
                    .send(Event::VctRepairContextReady {
                        owner,
                        result: VctRepairContextResult::Unavailable,
                    })
                    .await;
            }
            HeaderSyncAction::PrepareHeaderTarget { .. }
            | HeaderSyncAction::ApplyHeaderTarget { .. } => {}
        }
    }
}

/// Native header-sync service.
#[derive(Debug)]
pub(crate) struct HeaderSyncService {
    header_sync: HeaderSyncHandle,
    /// Each peer's current session, with its connection direction.
    sessions: Arc<SessionTable<ServicePeerDirection>>,
    /// Orders admission checks with the table, so an older connection never
    /// replaces a newer one's session.
    admission: StdMutex<()>,
    /// Each live connection's `Status` cadence.
    status_cadences: StdMutex<HashMap<(ZakuraPeerId, ZakuraConnId), StatusCadence>>,
    service_demand:
        Option<watch::Receiver<zakura_node_services::sync_lifecycle::SyncServiceDemand>>,
}

impl HeaderSyncService {
    pub(crate) fn new(header_sync: HeaderSyncHandle) -> Self {
        Self {
            header_sync,
            sessions: Arc::default(),
            admission: StdMutex::new(()),
            status_cadences: StdMutex::new(HashMap::new()),
            service_demand: None,
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

    fn coordinator_demand(&self) -> Option<SessionDemand> {
        let mut service_demand = self.service_demand.clone()?;
        if service_demand.borrow().header.is_enabled() {
            return None;
        }
        Some(SessionDemand::WaitForChange(Box::pin(async move {
            loop {
                if service_demand.changed().await.is_err() {
                    std::future::pending::<()>().await;
                }
                if service_demand.borrow().header.is_enabled() {
                    return;
                }
            }
        })))
    }
}

impl Service for HeaderSyncService {
    fn name(&self) -> &'static str {
        "header-sync"
    }

    fn streams(&self) -> &[Stream] {
        self.header_sync.layout
    }

    fn session_policy(&self) -> SessionPolicy {
        SessionPolicy {
            opening: SessionOpening::InitiatorOnly,
            reopen: true,
        }
    }

    fn session_demand(
        &self,
        _conn_id: ZakuraConnId,
        peer: &ZakuraPeerId,
        _negotiated: u64,
        direction: ServicePeerDirection,
    ) -> SessionDemand {
        if let Some(demand) = self.coordinator_demand() {
            return demand;
        }
        let mut peers = self.header_sync.subscribe_peer_snapshot();
        let snapshot = *peers.borrow_and_update();
        let slots_free = match direction {
            ServicePeerDirection::Inbound => snapshot.inbound_slots_free,
            ServicePeerDirection::Outbound => snapshot.outbound_slots_free,
        };
        if slots_free == 0 {
            return SessionDemand::WaitForChange(Box::pin(async move {
                if peers.changed().await.is_err() {
                    std::future::pending::<()>().await;
                }
            }));
        }

        let Some(node_id) = header_peer_node_id(peer) else {
            return SessionDemand::Retire;
        };
        if self
            .header_sync
            .candidate_state()
            .backed_off_node_ids
            .contains(&node_id)
        {
            // The reactor publishes the backed-off set without per-node deadlines.
            // The service re-offers each target after one conservative backoff window.
            return SessionDemand::RetryAt(Instant::now() + HEADER_SYNC_ADVISORY_BACKOFF);
        }

        SessionDemand::OpenNow
    }

    fn wants_peer(
        &self,
        peer: &ZakuraPeerId,
        _negotiated: u64,
        direction: ServicePeerDirection,
    ) -> bool {
        if self
            .service_demand
            .as_ref()
            .is_some_and(|demand| !demand.borrow().header.is_enabled())
        {
            return false;
        }
        let replaces_same_direction = self
            .sessions
            .get(peer)
            .is_some_and(|(_, current)| current == direction);
        if replaces_same_direction {
            return true;
        }
        let snapshot = self.header_sync.peer_snapshot();
        match direction {
            ServicePeerDirection::Inbound => snapshot.inbound_slots_free > 0,
            ServicePeerDirection::Outbound => snapshot.outbound_slots_free > 0,
        }
    }

    fn add_peer(&self, mut peer: Peer) {
        let Some((session_id, stream_version, recv, send)) =
            peer.take_versioned_stream_with_session_id(ZAKURA_STREAM_HEADER_SYNC)
        else {
            return;
        };
        // The publisher needs room for a page and for control.
        if stream_version != ZAKURA_HEADER_SYNC_STREAM_VERSION
            || peer.negotiated & ZAKURA_CAP_HEADER_SYNC == 0
            || send.max_capacity() < 2
        {
            peer.close_cause().record("header_sync_regulation_contract");
            peer.cancel_token().cancel();
            return;
        }

        let peer_id = peer.id.clone();
        let conn_id = peer.conn_id;
        let key = SessionKey {
            conn_id,
            session_id,
        };
        let service_cancel_token = peer.service_cancel_token();
        let connection_cancel_token = peer.cancel_token();
        let close_cause = peer.close_cause();
        let fence = WriterFence::new(connection_cancel_token.clone(), close_cause.clone());
        {
            let _admission = self
                .admission
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if self
                .sessions
                .get(&peer_id)
                .is_some_and(|(current, _)| current.conn_id > conn_id)
            {
                service_cancel_token.cancel();
                return;
            }
            let current = Current {
                key,
                cancel: service_cancel_token.clone(),
                fence: fence.clone(),
                session: peer.direction,
            };
            if matches!(
                self.sessions.replace(peer_id.clone(), current),
                Replacement::Refused
            ) {
                return;
            }
        }

        let status = self
            .status_cadences
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry((peer_id.clone(), conn_id))
            .or_insert_with(|| Arc::new(StdMutex::new(CadenceSender::new())))
            .clone();
        let header_bytes = header_sync_header_bytes_for_network(&self.header_sync.network);
        // The reader checks each page and outcome header against the
        // subscription before it reads the payload.
        let subscriber = Subscriber::new(header_bytes, fence, &recv);
        let session = PeerSession::new(
            peer_id.clone(),
            session_id,
            peer.direction,
            send.clone(),
            service_cancel_token.clone(),
            connection_cancel_token.clone(),
            close_cause.clone(),
            subscriber,
            status,
        );
        let (publisher, publishing) = spawn_publisher(
            &self.header_sync,
            header_bytes,
            &peer_id,
            session_id,
            send,
            service_cancel_token.clone(),
        );

        let admitted = self.header_sync.connect_session(session.clone());
        let handle = self.header_sync.clone();
        let pipe_cancel = service_cancel_token.clone();
        let protocol_connection_cancel = connection_cancel_token.clone();
        let protocol_close_cause = close_cause.clone();
        let pipe: BoxRunFuture<'static, ()> = Box::pin(async move {
            let Ok(admitted) = admitted else {
                return;
            };
            zakura_rt::select! {
                biased;
                _ = pipe_cancel.cancelled() => return,
                result = admitted => if result.is_err() { return; },
            }
            let result = if header_bytes == REGTEST_HEADER_BYTES {
                run_peer::<REGTEST_HEADER_BYTES>(
                    handle,
                    session,
                    publisher,
                    publishing,
                    recv,
                    pipe_cancel,
                )
                .await
            } else {
                run_peer::<COMMON_HEADER_BYTES>(
                    handle,
                    session,
                    publisher,
                    publishing,
                    recv,
                    pipe_cancel,
                )
                .await
            };
            handle_pipe_exit(
                "header-sync",
                &protocol_connection_cancel,
                &protocol_close_cause,
                result,
            );
        });

        let teardown_handle = self.header_sync.clone();
        let teardown_sessions = self.sessions.clone();
        let teardown_peer = peer_id.clone();
        let teardown_close_cause = close_cause.clone();
        let on_teardown = move || {
            teardown_sessions.remove(&teardown_peer, key);
            // Replaced sessions still own a reserved teardown slot. The reactor checks generations.
            let _ = teardown_handle.send_lifecycle(Event::PeerDisconnected {
                peer: teardown_peer,
                session_id,
                reason: teardown_close_cause.get_or("stream_closed"),
            });
        };
        let panic_connection_cancel = connection_cancel_token.clone();
        let panic_close_cause = close_cause.clone();
        let on_panic = move || {
            panic_close_cause.record("service_panic");
            panic_connection_cancel.cancel();
        };
        spawn_supervised_pipe(
            "header_sync_pipe",
            peer_id,
            service_cancel_token,
            on_teardown,
            on_panic,
            pipe,
        );
    }

    fn owns_connection_for_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) -> bool {
        let owns_stream = self
            .sessions
            .get(peer)
            .is_some_and(|(key, _)| key.conn_id == conn_id);
        if !owns_stream {
            return false;
        }
        let Ok(bytes) = <[u8; 32]>::try_from(peer.as_bytes()) else {
            return false;
        };
        let Ok(node_id) = zakura_quic::NodeId::from_bytes(&bytes) else {
            return false;
        };
        self.header_sync
            .candidate_state()
            .admitted_node_ids
            .contains(&node_id)
    }

    fn remove_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) {
        self.status_cadences
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&(peer.clone(), conn_id));
        let Some((key, _)) = self.sessions.get(peer) else {
            return;
        };
        if key.conn_id != conn_id || self.sessions.remove(peer, key).is_none() {
            return;
        }
        let _ = self.header_sync.send_lifecycle(Event::PeerDisconnected {
            peer: peer.clone(),
            session_id: key.session_id,
            reason: "service_removed",
        });
    }

    fn deliver_frame(
        &self,
        _peer: ZakuraPeerId,
        _stream_kind: u16,
        _frame: Frame,
    ) -> Result<(), SinkReject> {
        // Only the testkit recorder delivers frames here. Every header-sync
        // frame needs the session reader that owns its stream, so a frame
        // without one is ignored.
        Ok(())
    }
}

/// Spawn the publisher for one session of `peer`, on a network whose headers
/// take `header_bytes`. Returns its handle and its task.
fn spawn_publisher(
    header_sync: &HeaderSyncHandle,
    header_bytes: usize,
    peer: &ZakuraPeerId,
    session_id: u64,
    send: FramedSend,
    cancel: CancellationToken,
) -> (Publisher, JoinHandle<()>) {
    let port = header_sync.port.clone();
    let push = header_sync.serving.push(peer);
    let owner = LeaseOwner {
        peer: zakura_header_chain::SourceId::from_digest(peer.digest()),
        session_id,
    };
    let limits = header_sync.page_limits;
    if header_bytes == REGTEST_HEADER_BYTES {
        Publisher::spawn::<REGTEST_HEADER_BYTES>(port, push, send, owner, limits, cancel)
    } else {
        Publisher::spawn::<COMMON_HEADER_BYTES>(port, push, send, owner, limits, cancel)
    }
}

/// Stream 5 as a recorder declares it: the version 9 stream without a message
/// table, so a recorder sees every frame a test sends.
const PASSTHROUGH_STREAMS: [Stream; 1] = [Stream {
    messages: None,
    queue_depths: None,
    ..CommonMessage::LAYOUT[0]
}];

/// Testkit/no-reactor mode records inbound frames without decoding them.
#[derive(Debug)]
pub(crate) struct HeaderSyncPassthroughService {
    inner: Arc<dyn Service>,
}

impl HeaderSyncPassthroughService {
    pub(crate) fn new(inner: Arc<dyn Service>) -> Self {
        Self { inner }
    }
}

impl Service for HeaderSyncPassthroughService {
    fn name(&self) -> &'static str {
        "header-sync-passthrough"
    }

    fn streams(&self) -> &[Stream] {
        &PASSTHROUGH_STREAMS
    }

    fn session_policy(&self) -> SessionPolicy {
        self.inner.session_policy()
    }

    fn session_demand(
        &self,
        conn_id: ZakuraConnId,
        peer: &ZakuraPeerId,
        negotiated: u64,
        direction: ServicePeerDirection,
    ) -> SessionDemand {
        self.inner
            .session_demand(conn_id, peer, negotiated, direction)
    }

    fn wants_peer(
        &self,
        peer: &ZakuraPeerId,
        negotiated: u64,
        direction: ServicePeerDirection,
    ) -> bool {
        self.inner.wants_peer(peer, negotiated, direction)
    }

    fn add_peer(&self, mut peer: Peer) {
        let Some((recv, _)) = peer.take_stream(ZAKURA_STREAM_HEADER_SYNC) else {
            return;
        };
        let sink = HeaderSyncPassthroughSink {
            peer_id: peer.id.clone(),
            inner: self.inner.clone(),
            cancel_token: peer.cancel_token(),
        };
        spawn_erased_named("header_sync_passthrough", async move {
            if let Err(error) = Box::new(sink).run(recv).await {
                tracing::debug!(?error, "header-sync passthrough stopped");
            }
        });
    }

    fn remove_peer(&self, _peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {}

    fn deliver_frame(
        &self,
        peer: ZakuraPeerId,
        stream_kind: u16,
        frame: Frame,
    ) -> Result<(), SinkReject> {
        self.inner.deliver_frame(peer, stream_kind, frame)
    }
}

#[derive(Debug)]
struct HeaderSyncPassthroughSink {
    peer_id: ZakuraPeerId,
    inner: Arc<dyn Service>,
    cancel_token: CancellationToken,
}

impl Sink for HeaderSyncPassthroughSink {
    fn run(self: Box<Self>, mut recv: FramedRecv) -> BoxRunFuture<'static, Result<(), SinkReject>> {
        Box::pin(async move {
            loop {
                let frame = zakura_rt::select! {
                    _ = self.cancel_token.cancelled() => return Ok(()),
                    frame = recv.recv() => match frame {
                        Some(frame) => frame,
                        None => return Ok(()),
                    },
                };
                self.inner
                    .deliver_frame(self.peer_id.clone(), ZAKURA_STREAM_HEADER_SYNC, frame)?;
            }
        })
    }
}

/// Return the node identity for a valid header-sync peer ID.
fn header_peer_node_id(peer: &ZakuraPeerId) -> Option<zakura_quic::NodeId> {
    let bytes = <[u8; 32]>::try_from(peer.as_bytes()).ok()?;
    zakura_quic::NodeId::from_bytes(&bytes).ok()
}
