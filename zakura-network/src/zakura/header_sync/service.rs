use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex as StdMutex,
    },
};

use tokio::{sync::mpsc, task};
use tokio_util::sync::CancellationToken;

use super::{events::*, pipe::*, wire::*, *};
use crate::zakura::{
    handle_pipe_exit, spawn_supervised_pipe, BoxRunFuture, Flow, Frame, FramedRecv, FramedSend,
    OrderedSendError, Peer, PeerStreamSession, Pipe, Service, ServicePeerDirection, SessionGuard,
    Sink, SinkReject, Stream, StreamMode, ZakuraConnId, ZakuraPeerId, ZakuraSupervisorHandle,
    ZAKURA_CAP_HEADER_SYNC,
};

const HEADER_SYNC_SERVICE_STREAMS: [Stream; 1] = [Stream {
    kind: ZAKURA_STREAM_HEADER_SYNC,
    version: ZAKURA_HEADER_SYNC_STREAM_VERSION,
    // Advisory until the transport wires Stream::frame_cap end-to-end; the
    // authoritative inbound cap is app_frame_cap_for_stream_kind. The cast is
    // safe because both terms are small protocol constants checked against the
    // local message cap in header_sync::wire.
    frame_cap: (MAX_HS_MESSAGE_BYTES + FRAME_HEADER_BYTES) as u32,
    capability: ZAKURA_CAP_HEADER_SYNC,
    mode: StreamMode::Ordered,
}];

/// Service-declared streams for native header sync.
pub(crate) fn header_sync_streams() -> &'static [Stream] {
    &HEADER_SYNC_SERVICE_STREAMS
}

/// Cloneable typed header-sync sender and peer-local response expectations.
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

    #[cfg(test)]
    pub(crate) fn from_parts_with_direction_and_session_id(
        peer_id: ZakuraPeerId,
        direction: ServicePeerDirection,
        send: FramedSend,
        cancel_token: CancellationToken,
        session_id: u64,
    ) -> Self {
        Self::from_parts_with_direction_and_commands(
            peer_id,
            session_id,
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

    /// Authenticated peer identity for this header-sync session.
    pub fn peer_id(&self) -> &ZakuraPeerId {
        &self.peer_id
    }

    /// Unique ordered-stream generation that owns this session.
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    /// Direction of the underlying Zakura connection.
    pub fn direction(&self) -> ServicePeerDirection {
        self.direction
    }

    /// Peer disconnect/local shutdown cancellation token.
    pub fn cancel_token(&self) -> CancellationToken {
        self.inner.cancel_token.clone()
    }

    /// Current free slots in this peer's bounded outbound stream queue.
    pub fn outbound_capacity(&self) -> usize {
        self.inner.send.capacity()
    }

    /// Total slots in this peer's bounded outbound stream queue.
    pub fn outbound_max_capacity(&self) -> usize {
        self.inner.send.max_capacity()
    }

    /// Retire a request ID so any late response is dropped without scoring.
    pub fn retire_expected_headers(
        &self,
        request_id: HeaderSyncRequestId,
    ) -> Result<(), OrderedSendError> {
        let Some(commands) = &self.inner.commands else {
            return Ok(());
        };
        commands
            .send(HeaderSyncPeerCommand::Retire(request_id))
            .map_err(|_| OrderedSendError::Closed)
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
                Err(current_id) => id = current_id,
            }
        }
        HeaderSyncRequestId::new(id).ok_or_else(|| {
            OrderedSendError::Encode("header-sync request ID counter exhausted".into())
        })
    }

    /// Send a typed status advertisement.
    pub fn try_send_status(&self, status: HeaderSyncStatus) -> Result<(), OrderedSendError> {
        self.try_send_message(HeaderSyncMessage::Status(status), None)
    }

    /// Reserve response correlation, then send a typed header range request.
    pub fn try_send_get_headers(
        &self,
        start_height: block::Height,
        count: u32,
        want_tree_aux_roots: bool,
    ) -> Result<HeaderSyncRequestId, OrderedSendError> {
        let request_id = self.next_request_id()?;
        let expected =
            ExpectedHeadersResponse::new(request_id, start_height, count, want_tree_aux_roots)
                .map_err(|error| OrderedSendError::Encode(Box::new(error)))?;
        if let Some(commands) = &self.inner.commands {
            commands
                .send(HeaderSyncPeerCommand::Reserve(expected))
                .map_err(|_| OrderedSendError::Closed)?;
            if let Err(error) = self.try_send_message(
                HeaderSyncMessage::GetHeaders {
                    start_height,
                    count,
                    want_tree_aux_roots,
                },
                Some(request_id),
            ) {
                let _ = commands.send(HeaderSyncPeerCommand::Cancel(expected));
                return Err(error);
            }
            return Ok(request_id);
        }

        self.try_send_message(
            HeaderSyncMessage::GetHeaders {
                start_height,
                count,
                want_tree_aux_roots,
            },
            Some(request_id),
        )
        .map(|()| request_id)
    }

    /// Prepare a correlated header request without making its frame visible to the peer.
    pub(super) fn prepare_get_headers(
        &self,
        start_height: block::Height,
        count: u32,
        want_tree_aux_roots: bool,
    ) -> Result<PreparedGetHeaders, OrderedSendError> {
        let request_id = self.next_request_id()?;
        let expected =
            ExpectedHeadersResponse::new(request_id, start_height, count, want_tree_aux_roots)
                .map_err(|error| OrderedSendError::Encode(Box::new(error)))?;
        let frame = HeaderSyncMessage::GetHeaders {
            start_height,
            count,
            want_tree_aux_roots,
        }
        .encode_frame(Some(request_id))
        .map_err(|error| OrderedSendError::Encode(Box::new(error)))?;

        let reservation = ExpectedHeadersReservation::new(self.inner.commands.clone(), expected)?;
        Ok(PreparedGetHeaders {
            request_id,
            frame,
            send: self.inner.send.clone(),
            reservation,
        })
    }

    /// Send a typed header range response with one advisory body-size hint and
    /// tree-aux root payload per header.
    pub fn try_send_headers_with_sizes_and_roots(
        &self,
        request_id: HeaderSyncRequestId,
        headers: Vec<Arc<block::Header>>,
        body_sizes: Vec<u32>,
        tree_aux_roots: Vec<BlockCommitmentRoots>,
    ) -> Result<(), OrderedSendError> {
        self.try_send_message(
            HeaderSyncMessage::Headers {
                headers,
                body_sizes,
                tree_aux_roots,
            },
            Some(request_id),
        )
    }

    /// Send a typed full tip block announcement.
    pub fn try_send_new_block(&self, block: Arc<block::Block>) -> Result<(), OrderedSendError> {
        self.try_send_message(HeaderSyncMessage::NewBlock(block), None)
    }

    fn try_send_message(
        &self,
        msg: HeaderSyncMessage,
        request_id: Option<HeaderSyncRequestId>,
    ) -> Result<(), OrderedSendError> {
        let frame = msg
            .encode_frame(request_id)
            .map_err(|error| OrderedSendError::Encode(Box::new(error)))?;
        match self.inner.send.try_send(frame) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_frame)) => Err(OrderedSendError::Full),
            Err(mpsc::error::TrySendError::Closed(_frame)) => Err(OrderedSendError::Closed),
        }
    }
}

pub(super) struct PreparedGetHeaders {
    request_id: HeaderSyncRequestId,
    frame: Frame,
    send: FramedSend,
    reservation: ExpectedHeadersReservation,
}

impl PreparedGetHeaders {
    pub(super) fn request_id(&self) -> HeaderSyncRequestId {
        self.request_id
    }

    /// Wait for outbound capacity and publish the prepared frame.
    ///
    /// Dropping this future before publication synchronously cancels the pipe's
    /// response reservation.
    pub(super) async fn send(self) -> Result<HeaderSyncRequestId, OrderedSendError> {
        let Self {
            request_id,
            frame,
            send,
            mut reservation,
        } = self;
        send.send(frame)
            .await
            .map_err(|_| OrderedSendError::Closed)?;
        reservation.disarm();
        Ok(request_id)
    }
}

struct ExpectedHeadersReservation {
    commands: Option<mpsc::UnboundedSender<HeaderSyncPeerCommand>>,
    expected: ExpectedHeadersResponse,
    armed: bool,
}

impl ExpectedHeadersReservation {
    fn new(
        commands: Option<mpsc::UnboundedSender<HeaderSyncPeerCommand>>,
        expected: ExpectedHeadersResponse,
    ) -> Result<Self, OrderedSendError> {
        if let Some(commands) = &commands {
            commands
                .send(HeaderSyncPeerCommand::Reserve(expected))
                .map_err(|_| OrderedSendError::Closed)?;
        }
        Ok(Self {
            commands,
            expected,
            armed: true,
        })
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ExpectedHeadersReservation {
    fn drop(&mut self) {
        if self.armed {
            if let Some(commands) = &self.commands {
                let _ = commands.send(HeaderSyncPeerCommand::Cancel(self.expected));
            }
        }
    }
}

/// Commands from shared scheduling state into one peer-owned header-sync pipe.
#[derive(Debug)]
pub(super) enum HeaderSyncPeerCommand {
    /// Reserve an expected `Headers` response before `GetHeaders` is queued.
    Reserve(ExpectedHeadersResponse),
    /// Roll back an expectation when `GetHeaders` could not be queued.
    Cancel(ExpectedHeadersResponse),
    /// Retire an expected `Headers` response after timeout or cancellation.
    Retire(HeaderSyncRequestId),
}

/// Pump actor actions that can be satisfied at the transport/service seam.
pub(crate) async fn drive_header_sync_actions(
    mut actions: mpsc::Receiver<HeaderSyncAction>,
    handle: HeaderSyncHandle,
    // Retained so the disconnect capability stays wired into the driver, even
    // though peer scoring no longer drives disconnects (misbehavior is record-only).
    _supervisor: ZakuraSupervisorHandle,
    shutdown: CancellationToken,
) {
    loop {
        let action = tokio::select! {
            _ = shutdown.cancelled() => return,
            action = actions.recv() => {
                let Some(action) = action else {
                    return;
                };
                action
            }
        };

        match action {
            #[cfg(test)]
            HeaderSyncAction::SendMessage { .. } | HeaderSyncAction::ForwardNewBlock { .. } => {}
            HeaderSyncAction::Misbehavior { peer, reason } => {
                // Record-only: peer scoring no longer drives disconnects.
                tracing::debug!(?peer, ?reason, "recorded Zakura header-sync peer violation");
            }
            HeaderSyncAction::NewBlockReceived { peer, hash, .. } => {
                tracing::debug!(
                    ?peer,
                    ?hash,
                    "Zakura header-sync NewBlock body arrived before block-acceptance hook is wired"
                );
            }
            HeaderSyncAction::QueryHeadersByHeightRange {
                peer,
                session_id,
                request_id,
                start,
                count,
                ..
            } => {
                let _ = handle
                    .send(HeaderSyncEvent::HeaderRangeResponseFinished {
                        peer,
                        session_id,
                        request_id,
                        start_height: start,
                        requested_count: count,
                        returned_count: 0,
                    })
                    .await;
            }
            HeaderSyncAction::CommitHeaderRange {
                peer,
                start_height,
                headers,
                ..
            } => {
                tracing::debug!(
                    ?peer,
                    ?start_height,
                    count = headers.len(),
                    "suppressing Zakura header range commit until state driver is wired"
                );
            }
            HeaderSyncAction::QueryBestHeaderTip
            | HeaderSyncAction::QueryMissingBlockBodies { .. }
            | HeaderSyncAction::BodyGaps { .. }
            | HeaderSyncAction::HeaderAdvanced { .. }
            | HeaderSyncAction::HeaderReanchored { .. } => {}
        }
    }
}

/// Native versioned header-sync service.
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
        // Escalation is a local-room check. First-party summary usefulness is
        // advisory and is applied by header-sync candidate selection upstream.
        let snapshot = self.header_sync.peer_snapshot();
        match direction {
            ServicePeerDirection::Inbound => snapshot.inbound_slots_free > 0,
            ServicePeerDirection::Outbound => snapshot.outbound_slots_free > 0,
        }
    }

    fn add_peer(&self, mut peer: Peer) {
        let Some((session_id, recv, send)) =
            peer.take_stream_with_session_id(ZAKURA_STREAM_HEADER_SYNC)
        else {
            return;
        };

        let peer_id = peer.id.clone();
        let session = PeerStreamSession::new(
            peer_id.clone(),
            ZAKURA_STREAM_HEADER_SYNC,
            recv,
            send,
            peer.service_cancel_token(),
        );
        // The sink loop parks on the service token (a child of the connection
        // token) exactly as the old `HeaderSyncSink::run` select did. The
        // connection token is cancelled only on a protocol reject below, never on
        // a normal/parked exit — parking one service must not tear down the
        // shared connection that other services (discovery, block-sync) ride on.
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
            if let Some(old_record) = peers.insert(
                peer_id.clone(),
                HeaderSyncPeerRecord {
                    conn_id,
                    session_id,
                    cancel_token: header_sync_session.cancel_token(),
                },
            ) {
                old_record.cancel_token.cancel();
            }
        }

        let _ = self
            .header_sync
            .send_lifecycle(HeaderSyncEvent::PeerConnected(header_sync_session.clone()));

        let (_session_peer, _stream_kind, recv, _send, _session_cancel) = session.into_parts();

        // Phase 2 keeps request/response correlation in `HsLocal`: after the
        // session queues an outbound `GetHeaders`, the peer-owned pipe records
        // the expected `Headers` response in plain local state.
        let pipe = Pipe::new(
            peer_id.clone(),
            HsLocal::new(commands_rx, DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL),
            HsEnv::new_with_session_id(self.header_sync.clone(), session_id),
            SessionGuard::oversize_only(header_sync_guard_max_bytes()),
            run_inbound,
            &PIPE_SHAPE,
        );
        // The pipe future reproduces the old sink's connection handling: a
        // protocol reject (the only way `run_peer` returns `Err`, since
        // `run_inbound` maps a closed-queue `Local` to a benign continue)
        // cancels the *connection*, matching the old
        // `connection_cancel_token.cancel()` on `SinkReject::Protocol`. A normal
        // or parked exit leaves the connection alone.
        let pipe_cancel_token = service_cancel_token.clone();
        let protocol_connection_cancel_token = connection_cancel_token.clone();
        let protocol_close_cause = close_cause.clone();
        let pipe = async move {
            handle_pipe_exit(
                "header-sync",
                &protocol_connection_cancel_token,
                &protocol_close_cause,
                run_peer(pipe, recv, pipe_cancel_token).await,
            );
        };

        // The supervised teardown runs on every exit path — normal return,
        // protocol reject, or panic. It cancels this peer's *service* token
        // (idempotent; already cancelled on a park/protocol exit) and sends
        // `PeerDisconnected`. Sending it from teardown is the latent-bug fix: the
        // old sink only sent `PeerDisconnected` on the normal return path, so a
        // panicking task leaked the peer's reactor state.
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
        let panic_connection_cancel_token = connection_cancel_token.clone();
        let panic_close_cause = close_cause.clone();
        let on_panic = move || {
            panic_close_cause.record("service_panic");
            panic_connection_cancel_token.cancel();
        };

        // Reuse the single supervised launcher; let the returned handle drop to
        // detach the task (the `PipeTeardown` still runs on every exit path).
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
        peer_id: ZakuraPeerId,
        stream_kind: u16,
        frame: Frame,
    ) -> Result<(), SinkReject> {
        if stream_kind != ZAKURA_STREAM_HEADER_SYNC {
            return Ok(());
        }

        // The test/recorder path has no peer session, so a `Headers` response
        // with no outstanding request is rejected as `UnsolicitedHeaders`. A
        // `Local` reject (closed reactor queue) is surfaced to the registry
        // exactly as the old `deliver_header_sync_frame` returned it.
        match deliver(&self.header_sync, 0, None, peer_id, frame) {
            Flow::Continue(()) | Flow::Done => Ok(()),
            Flow::Reject(reject) => Err(reject),
        }
    }
}

/// Service-level oversize cap for the header-sync guard.
///
/// Matches the decode stage's `MAX_HS_MESSAGE_BYTES` threshold so the guard
/// rejects nothing the decode stage would have admitted; the transport already
/// caps frames at this payload size before they reach the service, so this is a
/// defense-in-depth bound that never changes which events fire.
fn header_sync_guard_max_bytes() -> u32 {
    // `MAX_HS_MESSAGE_BYTES` is a 2 MiB protocol constant that fits in `u32`;
    // the `const` assertion in `wire.rs` keeps it below the local message cap.
    u32::try_from(MAX_HS_MESSAGE_BYTES)
        .expect("MAX_HS_MESSAGE_BYTES is a 2 MiB constant that fits in u32")
}

/// Testkit/no-reactor mode records header-sync inbound frames without running header sync.
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
        let Some((recv, _send)) = peer.take_stream(ZAKURA_STREAM_HEADER_SYNC) else {
            return;
        };

        let inner = self.inner.clone();
        let peer_id = peer.id.clone();
        let cancel_token = peer.cancel_token();

        task::spawn(async move {
            let sink = Box::new(HeaderSyncPassthroughSink {
                peer_id: peer_id.clone(),
                inner,
                cancel_token: cancel_token.clone(),
            });

            match sink.run(recv).await {
                Ok(()) => {}
                Err(SinkReject::Protocol(error)) => {
                    tracing::debug!(
                        ?error,
                        ?peer_id,
                        "header-sync passthrough rejected protocol-invalid frame"
                    );
                    cancel_token.cancel();
                }
                Err(SinkReject::Local(error)) => {
                    tracing::debug!(
                        ?error,
                        ?peer_id,
                        "header-sync passthrough could not deliver frame locally"
                    );
                }
            }
        });
    }

    fn remove_peer(&self, _peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {}

    fn deliver_frame(
        &self,
        peer_id: ZakuraPeerId,
        stream_kind: u16,
        frame: Frame,
    ) -> Result<(), SinkReject> {
        self.inner.deliver_frame(peer_id, stream_kind, frame)
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
                    frame = recv.recv() => {
                        let Some(frame) = frame else {
                            return Ok(());
                        };
                        frame
                    }
                };

                match self.inner.deliver_frame(
                    self.peer_id.clone(),
                    ZAKURA_STREAM_HEADER_SYNC,
                    frame,
                ) {
                    Ok(()) => {}
                    Err(SinkReject::Protocol(error)) => return Err(SinkReject::Protocol(error)),
                    Err(SinkReject::Local(error)) => {
                        tracing::debug!(
                            ?error,
                            peer_id = ?self.peer_id,
                            "header-sync passthrough could not deliver frame locally"
                        );
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod request_id_tests {
    use super::*;

    #[test]
    fn request_id_exhaustion_remains_fail_closed() {
        let (send, _recv) = crate::zakura::framed_channel(1);
        let peer_id = ZakuraPeerId::new(vec![1; 32]).expect("test peer id is valid");
        let session = HeaderSyncPeerSession::from_parts_with_direction(
            peer_id,
            ServicePeerDirection::Outbound,
            send,
            CancellationToken::new(),
        );
        session
            .inner
            .next_request_id
            .store(u64::MAX, Ordering::Relaxed);

        assert!(session.next_request_id().is_err());
        assert!(session.next_request_id().is_err());
    }

    #[tokio::test]
    async fn expectation_channel_failure_prevents_get_headers_publication() {
        let (send, mut recv) = crate::zakura::framed_channel(1);
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        drop(commands_rx);
        let peer_id = ZakuraPeerId::new(vec![2; 32]).expect("test peer id is valid");
        let session = HeaderSyncPeerSession::from_parts_with_direction_and_commands(
            peer_id,
            1,
            ServicePeerDirection::Outbound,
            send,
            CancellationToken::new(),
            Some(commands_tx),
        );

        assert!(matches!(
            session.try_send_get_headers(block::Height(1), 1, true),
            Err(OrderedSendError::Closed)
        ));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), recv.recv())
                .await
                .is_err(),
            "failed expectation reservation must publish no wire request"
        );
        assert_eq!(
            session.inner.next_request_id.load(Ordering::Relaxed),
            2,
            "a failed reservation burns its request ID instead of reusing it ambiguously"
        );
    }

    #[tokio::test]
    async fn full_transport_queue_rolls_back_and_does_not_reuse_request_id() {
        let (send, mut recv) = crate::zakura::framed_channel(1);
        send.try_send(Frame {
            message_type: 0,
            flags: 0,
            payload: Vec::new(),
        })
        .expect("test transport queue starts empty");
        let (commands_tx, mut commands_rx) = mpsc::unbounded_channel();
        let peer_id = ZakuraPeerId::new(vec![3; 32]).expect("test peer id is valid");
        let session = HeaderSyncPeerSession::from_parts_with_direction_and_commands(
            peer_id,
            1,
            ServicePeerDirection::Outbound,
            send,
            CancellationToken::new(),
            Some(commands_tx),
        );

        assert!(matches!(
            session.try_send_get_headers(block::Height(1), 1, true),
            Err(OrderedSendError::Full)
        ));
        let reserved = match commands_rx.try_recv().expect("reservation is published") {
            HeaderSyncPeerCommand::Reserve(expected) => expected,
            command => panic!("expected reservation command, got {command:?}"),
        };
        let cancelled = match commands_rx.try_recv().expect("rollback is published") {
            HeaderSyncPeerCommand::Cancel(expected) => expected,
            command => panic!("expected rollback command, got {command:?}"),
        };
        assert_eq!(cancelled, reserved);
        assert_eq!(
            reserved.request_id,
            HeaderSyncRequestId::new(1).expect("non-zero id")
        );

        recv.recv().await.expect("dummy frame remains queued");
        let next_id = session
            .try_send_get_headers(block::Height(2), 1, true)
            .expect("queue has capacity after draining");
        assert_eq!(
            next_id,
            HeaderSyncRequestId::new(2).expect("non-zero id"),
            "the rolled-back ID must not be reused"
        );
        let next_reserved = match commands_rx
            .try_recv()
            .expect("next reservation is published")
        {
            HeaderSyncPeerCommand::Reserve(expected) => expected,
            command => panic!("expected reservation command, got {command:?}"),
        };
        assert_eq!(next_reserved.request_id, next_id);
        let frame = recv.recv().await.expect("second request is published");
        let (message, wire_request_id) =
            HeaderSyncMessage::decode_frame(frame, HeaderSyncDecodeContext::control())
                .expect("published request decodes");
        assert!(matches!(message, HeaderSyncMessage::GetHeaders { .. }));
        assert_eq!(wire_request_id, Some(next_id));
    }

    #[test]
    fn closed_transport_queue_rolls_back_reserved_expectation() {
        let (send, recv) = crate::zakura::framed_channel(1);
        drop(recv);
        let (commands_tx, mut commands_rx) = mpsc::unbounded_channel();
        let peer_id = ZakuraPeerId::new(vec![4; 32]).expect("test peer id is valid");
        let session = HeaderSyncPeerSession::from_parts_with_direction_and_commands(
            peer_id,
            1,
            ServicePeerDirection::Outbound,
            send,
            CancellationToken::new(),
            Some(commands_tx),
        );

        assert!(matches!(
            session.try_send_get_headers(block::Height(1), 1, true),
            Err(OrderedSendError::Closed)
        ));
        let reserved = match commands_rx.try_recv().expect("reservation is published") {
            HeaderSyncPeerCommand::Reserve(expected) => expected,
            command => panic!("expected reservation command, got {command:?}"),
        };
        let cancelled = match commands_rx.try_recv().expect("rollback is published") {
            HeaderSyncPeerCommand::Cancel(expected) => expected,
            command => panic!("expected rollback command, got {command:?}"),
        };
        assert_eq!(cancelled, reserved);
        assert_eq!(
            reserved.request_id,
            HeaderSyncRequestId::new(1).expect("non-zero id")
        );
        assert_eq!(session.inner.next_request_id.load(Ordering::Relaxed), 2);
    }
}
