use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex as StdMutex,
    },
};

use tokio::{sync::mpsc, task};
use tokio_util::sync::CancellationToken;
use zakura_chain::block;

use super::{events::*, pipe::run_peer, wire::*, FRAME_HEADER_BYTES};
use crate::zakura::{
    handle_pipe_exit, spawn_supervised_pipe, BoxRunFuture, Frame, FramedRecv, FramedSend,
    OrderedSendError, Peer, PeerStreamSession, Service, ServicePeerDirection, Sink, SinkReject,
    Stream, StreamMode, ZakuraConnId, ZakuraPeerId, ZakuraSupervisorHandle, ZAKURA_CAP_HEADER_SYNC,
};

const HEADER_SYNC_FRAME_CAP: u32 = (MAX_HS_MESSAGE_BYTES + FRAME_HEADER_BYTES) as u32;

const HEADER_SYNC_SERVICE_STREAMS: [Stream; 1] = [Stream {
    kind: ZAKURA_STREAM_HEADER_SYNC,
    version: ZAKURA_HEADER_SYNC_STREAM_VERSION,
    frame_cap: HEADER_SYNC_FRAME_CAP,
    capability: ZAKURA_CAP_HEADER_SYNC,
    mode: StreamMode::Ordered,
}];

/// The sole stream declaration for native header sync.
pub(crate) fn header_sync_streams() -> &'static [Stream] {
    &HEADER_SYNC_SERVICE_STREAMS
}

#[cfg(test)]
mod stream_tests {
    use super::*;

    #[test]
    fn declares_only_capability_bit_five_at_stream_version_eight() {
        assert_eq!(ZAKURA_CAP_HEADER_SYNC, 1 << 5);
        assert_eq!(header_sync_streams().len(), 1);
        assert_eq!(
            header_sync_streams()[0],
            Stream {
                kind: ZAKURA_STREAM_HEADER_SYNC,
                version: 8,
                frame_cap: HEADER_SYNC_FRAME_CAP,
                capability: 1 << 5,
                mode: StreamMode::Ordered,
            }
        );
    }
}

/// Cloneable typed sender for one canonical header-sync stream.
#[derive(Clone, Debug)]
pub struct HeaderSyncPeerSession {
    peer_id: ZakuraPeerId,
    session_id: u64,
    direction: ServicePeerDirection,
    inner: Arc<HeaderSyncPeerSessionInner>,
}

#[derive(Debug)]
struct HeaderSyncPeerSessionInner {
    send: FramedSend,
    cancel_token: CancellationToken,
    commands: Option<mpsc::UnboundedSender<HeaderSyncPeerCommand>>,
    next_request_id: AtomicU64,
}

impl HeaderSyncPeerSession {
    fn new_with_commands(
        session: &PeerStreamSession,
        direction: ServicePeerDirection,
        commands: mpsc::UnboundedSender<HeaderSyncPeerCommand>,
        session_id: u64,
    ) -> Self {
        debug_assert_eq!(
            session.stream_version(),
            ZAKURA_HEADER_SYNC_STREAM_VERSION,
            "transport admits only the canonical header-sync stream version"
        );
        Self::from_parts_with_direction_and_commands(
            session.peer_id().clone(),
            session_id,
            direction,
            session.sender(),
            session.cancel_token(),
            Some(commands),
        )
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
    pub(crate) fn from_parts_with_direction(
        peer_id: ZakuraPeerId,
        direction: ServicePeerDirection,
        send: FramedSend,
        cancel_token: CancellationToken,
    ) -> Self {
        Self::from_parts_with_direction_and_commands(
            peer_id,
            0,
            direction,
            send,
            cancel_token,
            None,
        )
    }

    fn from_parts_with_direction_and_commands(
        peer_id: ZakuraPeerId,
        session_id: u64,
        direction: ServicePeerDirection,
        send: FramedSend,
        cancel_token: CancellationToken,
        commands: Option<mpsc::UnboundedSender<HeaderSyncPeerCommand>>,
    ) -> Self {
        Self {
            peer_id,
            session_id,
            direction,
            inner: Arc::new(HeaderSyncPeerSessionInner {
                send,
                cancel_token,
                commands,
                next_request_id: AtomicU64::new(1),
            }),
        }
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

    /// Current free slots in the bounded outbound queue.
    pub fn outbound_capacity(&self) -> usize {
        self.inner.send.capacity()
    }

    /// Total outbound queue slots.
    pub fn outbound_max_capacity(&self) -> usize {
        self.inner.send.max_capacity()
    }

    fn next_request_id(&self) -> Result<HeaderSyncRequestId, OrderedSendError> {
        let mut id = self.inner.next_request_id.load(Ordering::Relaxed);
        loop {
            let next_id = id.checked_add(1).ok_or_else(|| {
                OrderedSendError::Encode("header-sync request ID counter exhausted".into())
            })?;
            match self.inner.next_request_id.compare_exchange_weak(
                id,
                next_id,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => id = current,
            }
        }
        HeaderSyncRequestId::new(id).ok_or_else(|| {
            OrderedSendError::Encode("header-sync request ID counter exhausted".into())
        })
    }

    pub(super) fn try_send_status(
        &self,
        codec: &HeaderSyncCodec,
        status: Status,
    ) -> Result<(), OrderedSendError> {
        self.try_send(codec, HeaderSyncMessage::Status(status))
    }

    pub(super) fn try_send_get_headers(
        &self,
        codec: &HeaderSyncCodec,
        target_tip_hash: block::Hash,
        locator: &zakura_header_chain::HeaderLocator,
        max_header_count: u32,
        tree_aux_schema: AuxSchema,
    ) -> Result<HeaderSyncRequestId, OrderedSendError> {
        let request_id = self.next_request_id()?;
        let message = HeaderSyncMessage::GetHeaders(GetHeaders {
            request_id: request_id.get(),
            target_tip_hash,
            locator_hashes: locator.hashes(),
            max_header_count,
            tree_aux_schema,
        });
        let frame = codec
            .encode_frame(&message)
            .map_err(|error| OrderedSendError::Encode(Box::new(error)))?;
        let expected = ExpectedHeadersResponse {
            request_id,
            context: HeaderSyncDecodeContext {
                max_header_count,
                requested_tree_aux_schema: tree_aux_schema,
            },
        };
        if let Some(commands) = &self.inner.commands {
            commands
                .send(HeaderSyncPeerCommand::Reserve(expected))
                .map_err(|_| OrderedSendError::Closed)?;
        }
        let result = match self.inner.send.try_send(frame) {
            Ok(()) => Ok(request_id),
            Err(mpsc::error::TrySendError::Full(_)) => Err(OrderedSendError::Full),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(OrderedSendError::Closed),
        };
        if result.is_err() {
            if let Some(commands) = &self.inner.commands {
                let _ = commands.send(HeaderSyncPeerCommand::Cancel(request_id));
            }
        }
        result
    }

    pub(super) fn try_send_headers(
        &self,
        codec: &HeaderSyncCodec,
        headers: Headers,
    ) -> Result<(), OrderedSendError> {
        self.try_send(codec, HeaderSyncMessage::Headers(headers))
    }

    pub(super) fn try_send_headers_outcome(
        &self,
        codec: &HeaderSyncCodec,
        outcome: HeadersOutcome,
    ) -> Result<(), OrderedSendError> {
        self.try_send(codec, HeaderSyncMessage::HeadersOutcome(outcome))
    }

    fn try_send(
        &self,
        codec: &HeaderSyncCodec,
        message: HeaderSyncMessage,
    ) -> Result<(), OrderedSendError> {
        let frame = codec
            .encode_frame(&message)
            .map_err(|error| OrderedSendError::Encode(Box::new(error)))?;
        match self.inner.send.try_send(frame) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(OrderedSendError::Full),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(OrderedSendError::Closed),
        }
    }
}

#[derive(Debug)]
pub(super) enum HeaderSyncPeerCommand {
    Reserve(ExpectedHeadersResponse),
    Cancel(HeaderSyncRequestId),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct ExpectedHeadersResponse {
    pub(super) request_id: HeaderSyncRequestId,
    pub(super) context: HeaderSyncDecodeContext,
}

/// Pump actions that can be satisfied without the production state driver.
pub(crate) async fn drive_header_sync_actions(
    mut actions: mpsc::Receiver<HeaderSyncAction>,
    handle: HeaderSyncHandle,
    _supervisor: ZakuraSupervisorHandle,
    shutdown: CancellationToken,
) {
    loop {
        let action = tokio::select! {
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
            HeaderSyncAction::QueryHeaderLocator {
                peer,
                session_id,
                target_tip_hash,
            } => {
                let _ = handle
                    .send(HeaderSyncEvent::HeaderLocatorReady {
                        peer,
                        session_id,
                        target_tip_hash,
                        locator: None,
                    })
                    .await;
            }
            HeaderSyncAction::AcquireHeaderPath {
                peer,
                session_id,
                request,
            } => {
                let _ = handle
                    .send(HeaderSyncEvent::HeaderPathLeaseReady {
                        peer,
                        session_id,
                        request,
                        result: HeaderPathLeaseResult::Outcome(
                            HeadersOutcomeCode::TargetNotRetained,
                        ),
                    })
                    .await;
            }
            HeaderSyncAction::QueryMissingBlockBodies { .. }
            | HeaderSyncAction::ReadHeaderPath { .. }
            | HeaderSyncAction::ReleaseHeaderPath { .. }
            | HeaderSyncAction::BodyGaps { .. }
            | HeaderSyncAction::HeaderAdvanced { .. }
            | HeaderSyncAction::HeaderReanchored { .. } => {}
        }
    }
}

/// Native header-sync service.
#[derive(Debug)]
pub(crate) struct HeaderSyncService {
    header_sync: HeaderSyncHandle,
    peers: Arc<StdMutex<HashMap<ZakuraPeerId, HeaderSyncPeerRecord>>>,
}

#[derive(Debug)]
struct HeaderSyncPeerRecord {
    conn_id: ZakuraConnId,
    session_id: u64,
    cancel_token: CancellationToken,
}

impl HeaderSyncService {
    pub(crate) fn new(header_sync: HeaderSyncHandle) -> Self {
        Self {
            header_sync,
            peers: Arc::new(StdMutex::new(HashMap::new())),
        }
    }
}

impl Service for HeaderSyncService {
    fn name(&self) -> &'static str {
        "header-sync"
    }

    fn streams(&self) -> &[Stream] {
        header_sync_streams()
    }

    fn wants_peer(
        &self,
        _peer: &ZakuraPeerId,
        _negotiated: u64,
        direction: ServicePeerDirection,
    ) -> bool {
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
        if stream_version != ZAKURA_HEADER_SYNC_STREAM_VERSION {
            return;
        }

        let peer_id = peer.id.clone();
        let session = PeerStreamSession::new(
            peer_id.clone(),
            ZAKURA_STREAM_HEADER_SYNC,
            ZAKURA_HEADER_SYNC_STREAM_VERSION,
            recv,
            send,
            peer.service_cancel_token(),
        );
        let service_cancel_token = session.cancel_token();
        let connection_cancel_token = peer.cancel_token();
        let close_cause = peer.close_cause();
        let conn_id = peer.conn_id;
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        let header_sync_session = HeaderSyncPeerSession::new_with_commands(
            &session,
            peer.direction,
            commands_tx,
            session_id,
        );

        {
            let mut peers = self
                .peers
                .lock()
                .expect("header-sync peer map mutex is never poisoned");
            if peers
                .get(&peer_id)
                .is_some_and(|record| record.conn_id > conn_id)
            {
                service_cancel_token.cancel();
                return;
            }
            if let Some(old) = peers.insert(
                peer_id.clone(),
                HeaderSyncPeerRecord {
                    conn_id,
                    session_id,
                    cancel_token: header_sync_session.cancel_token(),
                },
            ) {
                old.cancel_token.cancel();
            }
        }

        let _ = self
            .header_sync
            .send_lifecycle(HeaderSyncEvent::PeerConnected(header_sync_session));
        let codec = self.header_sync.codec();
        let (_, _, _, recv, _, _) = session.into_parts();
        let pipe_peer = peer_id.clone();
        let pipe_cancel = service_cancel_token.clone();
        let protocol_connection_cancel = connection_cancel_token.clone();
        let protocol_close_cause = close_cause.clone();
        let handle = self.header_sync.clone();
        let pipe: BoxRunFuture<'static, ()> = Box::pin(async move {
            handle_pipe_exit(
                "header-sync",
                &protocol_connection_cancel,
                &protocol_close_cause,
                run_peer(
                    handle,
                    codec,
                    pipe_peer,
                    session_id,
                    commands_rx,
                    recv,
                    pipe_cancel,
                )
                .await,
            );
        });

        let teardown_handle = self.header_sync.clone();
        let teardown_peers = self.peers.clone();
        let teardown_peer = peer_id.clone();
        let on_teardown = move || {
            let should_notify = {
                let mut peers = teardown_peers
                    .lock()
                    .expect("header-sync peer map mutex is never poisoned");
                if peers.get(&teardown_peer).is_some_and(|record| {
                    record.conn_id == conn_id && record.session_id == session_id
                }) {
                    peers.remove(&teardown_peer);
                    true
                } else {
                    false
                }
            };
            if should_notify {
                let _ = teardown_handle
                    .send_lifecycle(HeaderSyncEvent::PeerDisconnected(teardown_peer));
            }
        };
        let panic_connection_cancel = connection_cancel_token.clone();
        let panic_close_cause = close_cause.clone();
        let on_panic = move || {
            panic_close_cause.record("service_panic");
            panic_connection_cancel.cancel();
        };
        spawn_supervised_pipe(peer_id, service_cancel_token, on_teardown, on_panic, pipe);
    }

    fn remove_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) {
        let removed = {
            let mut peers = self
                .peers
                .lock()
                .expect("header-sync peer map mutex is never poisoned");
            if peers
                .get(peer)
                .is_some_and(|record| record.conn_id == conn_id)
            {
                peers.remove(peer)
            } else {
                None
            }
        };
        if let Some(record) = removed {
            record.cancel_token.cancel();
            let _ = self
                .header_sync
                .send_lifecycle(HeaderSyncEvent::PeerDisconnected(peer.clone()));
        }
    }

    fn deliver_frame(
        &self,
        peer: ZakuraPeerId,
        stream_kind: u16,
        frame: Frame,
    ) -> Result<(), SinkReject> {
        if stream_kind != ZAKURA_STREAM_HEADER_SYNC {
            return Ok(());
        }
        let message = self
            .header_sync
            .codec()
            .decode_frame(frame, None)
            .map_err(|error| SinkReject::protocol(std::io::Error::other(error.to_string())))?;
        self.header_sync
            .try_send(HeaderSyncEvent::SessionWireMessage {
                peer,
                session_id: 0,
                msg: message,
            })
            .map_err(|error| SinkReject::local(error.to_string()))
    }
}

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
        header_sync_streams()
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
        task::spawn(async move {
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
                let frame = tokio::select! {
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
