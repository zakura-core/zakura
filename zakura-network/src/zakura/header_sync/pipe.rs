//! header_sync/pipe.rs - the per-peer header-sync pipe (v6).
//!
//! THE PHASE-2 DAG SLICE IS THIS DIAGRAM. The code below is a mechanical
//! transcription; the [`PIPE_SHAPE`] const is the inspectable, drift-checked
//! copy of it.
//!
//!  command(reserve expected) ─▶ expected_headers.push_back ─▶ queued(GetHeaders)
//!  queue failure ─────────────▶ command(cancel expected) ───▶ expected_headers.remove
//!  recv ─▶ guard ─┬─ Headers ─▶ expected_headers.pop_front ─▶ decode ─▶ forward(WireMessage)
//!                 └─ Control ───────────────────────────────▶ decode ─▶ forward(WireMessage)
//!
//! Phase 2 moves request/response correlation out of
//! [`HeaderSyncPeerSession`] and into [`HsLocal`]. The shared scheduler still
//! decides when to ask a peer for headers, but reserves the expectation in this
//! peer-owned pipe before queueing outbound `GetHeaders`, rolling it back if the
//! transport queue rejects the frame. The pipe prioritizes and drains commands
//! before inbound frames so a response cannot beat its local expectation. This
//! retires the session mutex without changing the reactor's synthetic
//! `WireMessage` test path.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{events::*, scheduler::*, service::HeaderSyncPeerCommand, wire::*, *};
use crate::zakura::{
    Edge, Flow, FramedRecv, Node, NodeKind, Pipe, PipeCx, PipeShape, SinkReject, ZakuraPeerId,
};

const MAX_RETIRED_HEADER_REQUEST_IDS: usize = 4096;

pub(super) struct HsLocal {
    /// Plain peer-local response expectations, owned by this pipe task.
    expected_headers: VecDeque<ExpectedHeadersResponse>,
    /// Request-id keyed response expectations for header-sync v7.
    expected_headers_by_id: HashMap<HeaderSyncRequestId, ExpectedHeadersResponse>,
    /// Retired request IDs whose late responses should be dropped without scoring.
    retired_headers: HashSet<HeaderSyncRequestId>,
    /// Insertion order used to keep retired request IDs bounded.
    retired_header_order: VecDeque<HeaderSyncRequestId>,
    /// Highest locally reserved v7 request ID in this stream session.
    highest_reserved_request_id: Option<HeaderSyncRequestId>,
    /// Commands from shared scheduling state into this peer-local pipe.
    commands: mpsc::UnboundedReceiver<HeaderSyncPeerCommand>,
    /// Negotiated header-sync stream version.
    stream_version: u16,
    /// Pre-decode rate gate for inbound `NewBlock` floods.
    ///
    /// `NewBlock` is the only header-sync v6 message that deserializes a full
    /// `Arc<Block>` (up to `MAX_HS_MESSAGE_BYTES`) directly from the wire. The
    /// reactor's semantic `inbound_new_block` meter only fires *after* that
    /// decode, so an authenticated peer could otherwise force one full-block
    /// deserialization per frame before being metered. This gate enforces the
    /// same minimum interval *before* decode so excess `NewBlock` frames are
    /// dropped without ever reaching `Block::zcash_deserialize`.
    new_block_meter: RateMeter,
}

impl HsLocal {
    /// Build per-peer local state around this peer's header-sync v6 session.
    pub(super) fn new(
        commands: mpsc::UnboundedReceiver<HeaderSyncPeerCommand>,
        new_block_min_interval: Duration,
        stream_version: u16,
    ) -> Self {
        Self {
            expected_headers: VecDeque::new(),
            expected_headers_by_id: HashMap::new(),
            retired_headers: HashSet::new(),
            retired_header_order: VecDeque::new(),
            highest_reserved_request_id: None,
            commands,
            stream_version,
            new_block_meter: RateMeter::new(new_block_min_interval),
        }
    }

    /// Take one pre-decode `NewBlock` token. `false` means this frame arrived
    /// faster than the minimum interval and must be dropped before decode.
    fn admit_new_block(&mut self) -> bool {
        self.new_block_meter.try_take(Instant::now())
    }

    fn pop_expected_headers_response(&mut self) -> Option<ExpectedHeadersResponse> {
        self.expected_headers.pop_front()
    }

    fn pop_expected_headers_response_by_id(
        &mut self,
        request_id: HeaderSyncRequestId,
    ) -> Result<Option<ExpectedHeadersResponse>, HeaderSyncWireError> {
        if let Some(expected) = self.expected_headers_by_id.remove(&request_id) {
            self.remember_consumed_request_id(request_id);
            return Ok(Some(expected));
        }
        if self.retired_headers.contains(&request_id) {
            metrics::counter!("sync.header.response.retired").increment(1);
            return Ok(None);
        }
        if self
            .highest_reserved_request_id
            .is_some_and(|highest| request_id.get() <= highest.get())
        {
            metrics::counter!("sync.header.response.evicted_retired").increment(1);
            return Ok(None);
        }
        Err(HeaderSyncWireError::UnsolicitedHeaders)
    }

    fn remember_consumed_request_id(&mut self, request_id: HeaderSyncRequestId) {
        if self.retired_headers.insert(request_id) {
            self.retired_header_order.push_back(request_id);
        }
        while self.retired_headers.len() > MAX_RETIRED_HEADER_REQUEST_IDS {
            let Some(oldest) = self.retired_header_order.pop_front() else {
                break;
            };
            self.retired_headers.remove(&oldest);
        }
    }

    fn reactivate_request_id(&mut self, request_id: HeaderSyncRequestId) {
        self.retired_headers.remove(&request_id);
        if let Some(index) = self
            .retired_header_order
            .iter()
            .position(|retired| *retired == request_id)
        {
            self.retired_header_order.remove(index);
        }
    }

    /// Restore a solicited-response expectation that was popped for decode but
    /// whose decoded `Headers` event could not be handed to the reactor (the
    /// bounded `events` queue was full or closed). It goes back to the *front* so
    /// FIFO order is preserved and the reactor's still-outstanding range stays
    /// correlated, instead of leaving the expectation silently consumed.
    fn restore_expected_headers(&mut self, expected: ExpectedHeadersResponse) {
        if let Some(request_id) = expected.request_id {
            self.reactivate_request_id(request_id);
            self.expected_headers_by_id.insert(request_id, expected);
        } else {
            self.expected_headers.push_front(expected);
        }
    }

    fn handle_command(&mut self, command: HeaderSyncPeerCommand) {
        match command {
            HeaderSyncPeerCommand::Reserve(expected) => {
                if let Some(request_id) = expected.request_id {
                    self.highest_reserved_request_id = Some(
                        self.highest_reserved_request_id
                            .filter(|highest| highest.get() >= request_id.get())
                            .unwrap_or(request_id),
                    );
                    self.reactivate_request_id(request_id);
                    self.expected_headers_by_id.insert(request_id, expected);
                } else {
                    self.expected_headers.push_back(expected);
                }
            }
            HeaderSyncPeerCommand::Cancel(expected) => {
                if let Some(request_id) = expected.request_id {
                    self.expected_headers_by_id.remove(&request_id);
                    self.reactivate_request_id(request_id);
                } else if let Some(index) = self
                    .expected_headers
                    .iter()
                    .rposition(|candidate| *candidate == expected)
                {
                    self.expected_headers.remove(index);
                }
            }
            HeaderSyncPeerCommand::Retire(request_id) => {
                self.expected_headers_by_id.remove(&request_id);
                self.remember_consumed_request_id(request_id);
            }
        }
    }

    fn drain_ready_commands(&mut self) {
        while let Ok(command) = self.commands.try_recv() {
            self.handle_command(command);
        }
    }
}

/// Shared environment handed to every header-sync pipe.
///
/// Phase 1's environment is just the cloneable reactor handle: the decode stage
/// forwards each decoded message (or decode failure) to the unchanged reactor
/// over this handle. Cross-peer shared core state arrives in Phase 2.
#[derive(Clone)]
pub(super) struct HsEnv {
    /// Handle used to forward inbound wire events to the header-sync reactor.
    handle: HeaderSyncHandle,
    /// Unique ordered-stream generation that owns this pipe.
    session_id: u64,
}

impl HsEnv {
    /// Wrap a cloneable reactor handle as the pipe's shared environment.
    #[cfg(test)]
    pub(super) fn new(handle: HeaderSyncHandle) -> Self {
        Self {
            handle,
            session_id: 0,
        }
    }

    /// Wrap a reactor handle and its owning ordered-stream generation.
    pub(super) fn new_with_session_id(handle: HeaderSyncHandle, session_id: u64) -> Self {
        Self { handle, session_id }
    }
}

/// The Phase-2 header-sync pipe DAG slice, as checked documentation.
pub(super) const PIPE_SHAPE: PipeShape = PipeShape {
    service: "header-sync",
    nodes: &[
        Node {
            id: "guard",
            kind: NodeKind::Guard,
        },
        Node {
            id: "decode",
            kind: NodeKind::Decode,
        },
        Node {
            id: "correlate",
            kind: NodeKind::Mutate,
        },
        Node {
            id: "emit",
            kind: NodeKind::Emit,
        },
    ],
    edges: &[
        Edge {
            from: "guard",
            to: "correlate",
            on: "Headers",
        },
        Edge {
            from: "guard",
            to: "decode",
            on: "Control",
        },
        Edge {
            from: "correlate",
            to: "decode",
            on: "Expected",
        },
        Edge {
            from: "decode",
            to: "emit",
            on: "Ok",
        },
    ],
};

/// Executable transcription of [`PIPE_SHAPE`] — the production entry function.
///
/// The guard already admitted this frame (oversize-only) before `run_inbound`
/// is reached, so this is the `Headers|Control → correlate → decode → emit` tail. It
/// delegates to the single [`deliver`] implementation with the peer-owned
/// expected-response value, so the production pipe and the test/recorder
/// `deliver_frame` path can never diverge on *what* they decode or emit.
///
/// The two callers differ only in how they treat a closed reactor queue, which
/// reproduces the old per-caller handling exactly: the production sink logged
/// the `SinkReject::Local` and continued the loop, so `run_inbound` maps that
/// one case to a debug log plus [`Flow::Done`] (which [`run_peer`] treats as
/// "continue"). Protocol rejects pass straight through and tear the peer down.
///
/// One addition over the old per-caller handling: when a *solicited* `Headers`
/// response hits that local-reject path, the expectation popped before decode is
/// restored to [`HsLocal`] so reactor queue saturation cannot silently consume it
/// and strand the still-outstanding range.
pub(super) fn run_inbound(cx: &mut PipeCx<'_, HsLocal, HsEnv>, frame: Frame) -> Flow<()> {
    // Pre-decode `NewBlock` rate gate: a `NewBlock` frame that arrives inside the
    // per-peer minimum interval is dropped *before* the full `Arc<Block>` is
    // deserialized, so a flood cannot force repeated full-block decode ahead of
    // the reactor's semantic meter. Throttling (drop, keep the peer) matches the
    // session guard's back-pressure outcome and the reactor's cheap
    // dedup-without-scoring policy, so honest re-floods are not penalized; the
    // first frame in each window still reaches the reactor, preserving
    // first-offense malformed/spam disconnects.
    if u8::try_from(frame.message_type).ok() == Some(MSG_HS_NEW_BLOCK)
        && !cx.local.admit_new_block()
    {
        metrics::counter!("sync.header.tip.new_block.predecode_throttled").increment(1);
        return Flow::Done;
    }

    let expected = if u8::try_from(frame.message_type).ok() == Some(MSG_HS_HEADERS) {
        if cx.local.stream_version >= ZAKURA_HEADER_SYNC_STREAM_VERSION_V7 {
            match HeaderSyncMessage::peek_headers_request_id(&frame.payload)
                .and_then(|request_id| cx.local.pop_expected_headers_response_by_id(request_id))
            {
                Ok(Some(expected)) => Some(expected),
                Ok(None) => return Flow::Done,
                Err(error) => {
                    let error = Arc::new(error);
                    let _ = cx
                        .env
                        .handle
                        .try_send(HeaderSyncEvent::WireProtocolFailure {
                            peer: cx.peer_id.clone(),
                            reason: HeaderSyncMisbehavior::UnsolicitedHeaders,
                            error: error.clone(),
                        });
                    return Flow::Reject(SinkReject::protocol(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        error.to_string(),
                    )));
                }
            }
        } else {
            cx.local.pop_expected_headers_response()
        }
    } else {
        None
    };
    match deliver(
        &cx.env.handle,
        cx.local.stream_version,
        cx.env.session_id,
        expected,
        cx.peer_id.clone(),
        frame,
    ) {
        Flow::Reject(SinkReject::Local(error)) => {
            // The reactor `events` queue was full or closed, so this decoded frame
            // could not be delivered locally. For a *solicited* `Headers` response
            // the expectation was already popped before decode, so restore it: the
            // reactor's matching range is still outstanding, and a consumed-but-
            // undelivered expectation would otherwise lose the response entirely
            // (recoverable only by the request timeout) and desynchronize the
            // peer-local FIFO from that outstanding range. Restoring keeps the pipe
            // in the same state as a request still awaiting its response, which the
            // timeout/retry machinery already handles correctly.
            if let Some(expected) = expected {
                cx.local.restore_expected_headers(expected);
            }
            tracing::debug!(
                ?error,
                peer_id = ?cx.peer_id,
                "header-sync stream could not deliver frame locally"
            );
            Flow::Done
        }
        other => other,
    }
}

/// The single inbound decode/branch/forward stage, shared by both paths.
///
/// This is the one decode implementation reachable from:
///
/// - the production pipe's [`run_inbound`] (with `Some(session)` so a `Headers`
///   response is correlated against the peer's outstanding `GetHeaders`), and
/// - [`HeaderSyncService::deliver_frame`](super::service::HeaderSyncService) (the
///   test/recorder path, which passes `None` so a `Headers` response with no
///   outstanding request is rejected as `UnsolicitedHeaders`).
///
/// It is a faithful port of the old `deliver_header_sync_frame`: the same events
/// fire on the same conditions, mapped onto [`Flow`]:
///
/// - a successful forward to the reactor ⇒ [`Flow::Continue`],
/// - the old `SinkReject::Protocol` cases ⇒ [`Flow::Reject`] with a `Protocol`
///   reason (fatal — disconnect the peer), and
/// - the old `SinkReject::Local` "queue closed" case ⇒ [`Flow::Reject`] with a
///   `Local` reason. Each caller then maps `Local` to its old behavior:
///   `run_inbound` logs and continues, while `deliver_frame` returns it to the
///   registry as `Err(SinkReject::Local(_))`.
pub(super) fn deliver(
    handle: &HeaderSyncHandle,
    stream_version: u16,
    session_id: u64,
    expected: Option<ExpectedHeadersResponse>,
    peer_id: ZakuraPeerId,
    frame: Frame,
) -> Flow<()> {
    if u8::try_from(frame.message_type).ok() == Some(MSG_HS_HEADERS) {
        let Some(expected) = expected else {
            let error = Arc::new(HeaderSyncWireError::UnsolicitedHeaders);
            let _ = handle.try_send(HeaderSyncEvent::WireProtocolFailure {
                peer: peer_id.clone(),
                reason: HeaderSyncMisbehavior::UnsolicitedHeaders,
                error: error.clone(),
            });
            let protocol_error =
                std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string());
            return Flow::Reject(SinkReject::protocol(protocol_error));
        };

        let (msg, request_id) = match HeaderSyncMessage::decode_frame_with_request_id(
            frame,
            HeaderSyncDecodeContext::for_headers_response(expected, expected.count),
        ) {
            Ok(msg) => msg,
            Err(error) => {
                let protocol_error =
                    std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string());
                let _ = handle.try_send(HeaderSyncEvent::WireProtocolFailure {
                    peer: peer_id.clone(),
                    reason: HeaderSyncMisbehavior::MalformedMessage,
                    error: Arc::new(error),
                });
                return Flow::Reject(SinkReject::protocol(protocol_error));
            }
        };

        if let HeaderSyncMessage::Headers {
            headers,
            body_sizes,
            tree_aux_roots,
        } = msg
        {
            return forward(
                handle,
                HeaderSyncEvent::WireHeaders {
                    peer: peer_id,
                    session_id,
                    request_id,
                    headers,
                    body_sizes,
                    tree_aux_roots,
                },
            );
        }

        return forward(handle, HeaderSyncEvent::WireMessage { peer: peer_id, msg });
    }

    let (msg, request_id) = match decode_control_frame(frame, stream_version) {
        Ok(msg) => msg,
        Err(error) => {
            let protocol_error =
                std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string());
            let _ = handle.try_send(HeaderSyncEvent::WireDecodeFailed {
                peer: peer_id,
                error: Arc::new(error),
            });
            return Flow::Reject(SinkReject::protocol(protocol_error));
        }
    };

    match msg {
        HeaderSyncMessage::GetHeaders {
            start_height,
            count,
            want_tree_aux_roots,
        } => forward(
            handle,
            HeaderSyncEvent::WireGetHeaders {
                peer: peer_id,
                session_id,
                request_id,
                start_height,
                count,
                want_tree_aux_roots,
            },
        ),
        msg => forward(
            handle,
            HeaderSyncEvent::SessionWireMessage {
                peer: peer_id,
                session_id,
                msg,
            },
        ),
    }
}

/// Run one peer-owned header-sync pipe until stream close, cancellation, or reject.
///
/// Correlation-ordering invariant: a `Reserve` command is enqueued
/// before outbound `GetHeaders` is queued. If transport queueing fails, a matching
/// `Cancel` command follows. The loop drains ready commands before
/// every inbound frame, and the `biased` select prefers the command channel over
/// `recv`, so a wire response can never arrive before its reservation is visible
/// in `HsLocal`.
pub(super) async fn run_peer(
    mut pipe: Pipe<HsLocal, HsEnv>,
    mut recv: FramedRecv,
    cancel: CancellationToken,
) -> Result<(), SinkReject> {
    enum Input {
        Frame(Frame),
        Command(HeaderSyncPeerCommand),
        Done,
    }

    loop {
        pipe.local_mut().drain_ready_commands();

        let input = {
            let local = pipe.local_mut();
            tokio::select! {
                biased;
                () = cancel.cancelled() => Input::Done,
                command = local.commands.recv() => match command {
                    Some(command) => Input::Command(command),
                    None => Input::Done,
                },
                frame = recv.recv() => match frame {
                    Some(frame) => Input::Frame(frame),
                    None => Input::Done,
                },
            }
        };

        match input {
            Input::Done => return Ok(()),
            Input::Frame(frame) => {
                pipe.local_mut().drain_ready_commands();
                match pipe.run_one(frame) {
                    Flow::Continue(()) | Flow::Done => {}
                    Flow::Reject(reject) => return Err(reject),
                }
            }
            Input::Command(command) => pipe.local_mut().handle_command(command),
        }
    }
}

/// Forward a successfully decoded inbound event to the reactor.
///
/// A closed reactor queue is a local, non-fatal condition for the peer: the old
/// `deliver_header_sync_frame` returned `SinkReject::local` here, so this returns
/// [`Flow::Reject`] with a `Local` reason. Callers decide whether to continue or
/// surface it (see [`deliver`]).
fn forward(handle: &HeaderSyncHandle, event: HeaderSyncEvent) -> Flow<()> {
    match handle.try_send(event) {
        Ok(()) => Flow::Continue(()),
        Err(error) => Flow::Reject(SinkReject::local(format!(
            "header-sync queue closed: {error}"
        ))),
    }
}

/// Decode a non-`Headers` (control) frame.
///
/// `Headers` frames need the peer's outstanding-request context and are handled
/// in [`deliver`]; a `Headers` frame reaching this path has no correlated
/// request, so it is rejected as `UnsolicitedHeaders` exactly as the old
/// `decode_header_sync_frame` did.
fn decode_control_frame(
    frame: Frame,
    stream_version: u16,
) -> Result<(HeaderSyncMessage, Option<HeaderSyncRequestId>), HeaderSyncWireError> {
    if u8::try_from(frame.message_type).ok() == Some(MSG_HS_HEADERS) {
        return Err(HeaderSyncWireError::UnsolicitedHeaders);
    }

    HeaderSyncMessage::decode_frame_with_request_id(
        frame,
        HeaderSyncDecodeContext::control_for_version(stream_version),
    )
}

#[cfg(test)]
mod tests {
    use tokio::sync::watch;

    use super::*;
    use crate::zakura::{ServicePeerSnapshot, ZakuraHeaderSyncCandidateState};

    const FRAME_FORKS: [&str; 2] = ["Headers", "Control"];

    fn peer() -> ZakuraPeerId {
        ZakuraPeerId::new(vec![5; 32]).expect("test peer id is within bounds")
    }

    /// Build a `HeaderSyncHandle` whose bounded `events` queue the test can drain.
    /// The watch frontiers are never read on the inbound decode path, so dummy
    /// values suffice.
    fn test_handle() -> (HeaderSyncHandle, mpsc::Receiver<HeaderSyncEvent>) {
        let (events, events_rx) = mpsc::channel(16);
        let (lifecycle, _lifecycle_rx) = mpsc::unbounded_channel();
        let (_tip_tx, tip) = watch::channel((block::Height(0), block::Hash([0; 32])));
        let (_peers_tx, peers) = watch::channel(ServicePeerSnapshot::default());
        let (_candidates_tx, candidates) =
            watch::channel(ZakuraHeaderSyncCandidateState::default());
        (
            HeaderSyncHandle {
                events,
                lifecycle,
                tip,
                peers,
                candidates,
            },
            events_rx,
        )
    }

    /// Build a `HeaderSyncHandle` whose bounded `events` queue is already full,
    /// so the next `try_send` from the pipe fails with `Full`. The receiver is
    /// returned (and must be kept alive) so the failure is `Full`, not `Closed`.
    fn saturated_events_handle() -> (HeaderSyncHandle, mpsc::Receiver<HeaderSyncEvent>) {
        let (events, events_rx) = mpsc::channel(1);
        events
            .try_send(HeaderSyncEvent::PeerDisconnected(peer()))
            .expect("the single events slot is free");
        let (lifecycle, _lifecycle_rx) = mpsc::unbounded_channel();
        let (_tip_tx, tip) = watch::channel((block::Height(0), block::Hash([0; 32])));
        let (_peers_tx, peers) = watch::channel(ServicePeerSnapshot::default());
        let (_candidates_tx, candidates) =
            watch::channel(ZakuraHeaderSyncCandidateState::default());
        (
            HeaderSyncHandle {
                events,
                lifecycle,
                tip,
                peers,
                candidates,
            },
            events_rx,
        )
    }

    fn headers_frame(payload: Vec<u8>) -> Frame {
        Frame {
            message_type: u16::from(MSG_HS_HEADERS),
            flags: 0,
            payload,
        }
    }

    /// A `Headers` frame with no recorded expectation is unsolicited: it reports
    /// `UnsolicitedHeaders` misbehavior and rejects the peer, before any decode.
    #[test]
    fn deliver_unsolicited_headers_rejects_without_expectation() {
        let (handle, mut events) = test_handle();

        let flow = deliver(
            &handle,
            ZAKURA_HEADER_SYNC_STREAM_VERSION,
            0,
            None,
            peer(),
            headers_frame(Vec::new()),
        );

        assert!(matches!(flow, Flow::Reject(SinkReject::Protocol(_))));
        match events.try_recv() {
            Ok(HeaderSyncEvent::WireProtocolFailure { reason, .. }) => {
                assert!(matches!(reason, HeaderSyncMisbehavior::UnsolicitedHeaders));
            }
            other => panic!("expected WireProtocolFailure(UnsolicitedHeaders), got {other:?}"),
        }
    }

    /// With a recorded expectation, the same `Headers` frame is *correlated* and
    /// decoded: a malformed payload now reports `MalformedMessage`, not
    /// `UnsolicitedHeaders`, proving the expectation was consumed before decode.
    #[test]
    fn deliver_correlated_headers_decodes_against_expectation() {
        let (handle, mut events) = test_handle();
        let expected =
            ExpectedHeadersResponse::new(block::Height(1), 1, true).expect("count is valid");

        let flow = deliver(
            &handle,
            ZAKURA_HEADER_SYNC_STREAM_VERSION,
            0,
            Some(expected),
            peer(),
            headers_frame(Vec::new()),
        );

        assert!(matches!(flow, Flow::Reject(SinkReject::Protocol(_))));
        match events.try_recv() {
            Ok(HeaderSyncEvent::WireProtocolFailure { reason, .. }) => {
                assert!(matches!(reason, HeaderSyncMisbehavior::MalformedMessage));
            }
            other => panic!("expected WireProtocolFailure(MalformedMessage), got {other:?}"),
        }
    }

    /// The peer-local correlation queue is FIFO and is filled by draining ready
    /// commands. This is the invariant `run_peer` relies on: an expectation
    /// recorded by a `Reserve` command is drained and available to
    /// pop before the matching `Headers` response is processed.
    #[test]
    fn local_correlation_queue_drains_commands_in_fifo_order() {
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        let mut local = HsLocal::new(
            commands_rx,
            DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
            ZAKURA_HEADER_SYNC_STREAM_VERSION,
        );

        let first =
            ExpectedHeadersResponse::new(block::Height(1), 1, false).expect("count is valid");
        let second =
            ExpectedHeadersResponse::new(block::Height(2), 2, false).expect("count is valid");
        commands_tx
            .send(HeaderSyncPeerCommand::Reserve(first))
            .expect("pipe is alive");
        commands_tx
            .send(HeaderSyncPeerCommand::Reserve(second))
            .expect("pipe is alive");

        // Nothing is available until the pipe drains its ready commands.
        assert_eq!(local.pop_expected_headers_response(), None);
        local.drain_ready_commands();

        assert_eq!(local.pop_expected_headers_response(), Some(first));
        assert_eq!(local.pop_expected_headers_response(), Some(second));
        assert_eq!(local.pop_expected_headers_response(), None);
    }

    #[test]
    fn cancelled_v7_reservation_leaves_no_active_or_retired_expectation() {
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        let request_id = HeaderSyncRequestId::new(10).expect("non-zero id");
        let expected = ExpectedHeadersResponse::new(block::Height(1), 1, true)
            .expect("count is valid")
            .with_request_id(request_id);
        commands_tx
            .send(HeaderSyncPeerCommand::Reserve(expected))
            .expect("pipe is alive");
        commands_tx
            .send(HeaderSyncPeerCommand::Cancel(expected))
            .expect("pipe is alive");
        let mut local = HsLocal::new(
            commands_rx,
            DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
            ZAKURA_HEADER_SYNC_STREAM_VERSION_V7,
        );

        local.drain_ready_commands();

        assert!(!local.expected_headers_by_id.contains_key(&request_id));
        assert!(!local.retired_headers.contains(&request_id));
        assert!(local.retired_header_order.is_empty());
    }

    #[test]
    fn v7_headers_responses_match_by_request_id_not_fifo_order() {
        let (handle, mut events) = test_handle();
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        let first_id = HeaderSyncRequestId::new(1).expect("non-zero id");
        let second_id = HeaderSyncRequestId::new(2).expect("non-zero id");
        let first = ExpectedHeadersResponse::new(block::Height(1), 1, true)
            .expect("count is valid")
            .with_request_id(first_id);
        let second = ExpectedHeadersResponse::new(block::Height(2), 1, true)
            .expect("count is valid")
            .with_request_id(second_id);
        commands_tx
            .send(HeaderSyncPeerCommand::Reserve(first))
            .expect("pipe is alive");
        commands_tx
            .send(HeaderSyncPeerCommand::Reserve(second))
            .expect("pipe is alive");

        let mut local = HsLocal::new(
            commands_rx,
            DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
            ZAKURA_HEADER_SYNC_STREAM_VERSION_V7,
        );
        local.drain_ready_commands();
        let mut pipe = Pipe::new(
            peer(),
            local,
            HsEnv::new(handle),
            crate::zakura::SessionGuard::oversize_only(MAX_HS_MESSAGE_BYTES as u32),
            run_inbound,
            &PIPE_SHAPE,
        );
        let empty_headers = HeaderSyncMessage::Headers {
            headers: Vec::new(),
            body_sizes: Vec::new(),
            tree_aux_roots: Vec::new(),
        };
        let second_frame = empty_headers
            .encode_frame_for_version(ZAKURA_HEADER_SYNC_STREAM_VERSION_V7, Some(second_id))
            .expect("v7 response encodes");
        let first_frame = empty_headers
            .encode_frame_for_version(ZAKURA_HEADER_SYNC_STREAM_VERSION_V7, Some(first_id))
            .expect("v7 response encodes");

        assert!(matches!(pipe.run_one(second_frame), Flow::Continue(())));
        match events.try_recv() {
            Ok(HeaderSyncEvent::WireHeaders { request_id, .. }) => {
                assert_eq!(request_id, Some(second_id));
            }
            other => panic!("expected second response to be forwarded by id, got {other:?}"),
        }

        assert!(matches!(pipe.run_one(first_frame), Flow::Continue(())));
        match events.try_recv() {
            Ok(HeaderSyncEvent::WireHeaders { request_id, .. }) => {
                assert_eq!(request_id, Some(first_id));
            }
            other => panic!("expected first response to be forwarded by id, got {other:?}"),
        }

        let duplicate = empty_headers
            .encode_frame_for_version(ZAKURA_HEADER_SYNC_STREAM_VERSION_V7, Some(second_id))
            .expect("duplicate v7 response encodes");
        assert!(matches!(pipe.run_one(duplicate), Flow::Done));
        assert!(matches!(
            events.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn v7_retired_headers_response_is_dropped_without_scoring() {
        let (handle, mut events) = test_handle();
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();
        let request_id = HeaderSyncRequestId::new(7).expect("non-zero id");
        commands_tx
            .send(HeaderSyncPeerCommand::Retire(request_id))
            .expect("pipe is alive");
        let mut local = HsLocal::new(
            commands_rx,
            DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
            ZAKURA_HEADER_SYNC_STREAM_VERSION_V7,
        );
        local.drain_ready_commands();
        let mut pipe = Pipe::new(
            peer(),
            local,
            HsEnv::new(handle),
            crate::zakura::SessionGuard::oversize_only(MAX_HS_MESSAGE_BYTES as u32),
            run_inbound,
            &PIPE_SHAPE,
        );
        for _ in 0..2 {
            let frame = HeaderSyncMessage::Headers {
                headers: Vec::new(),
                body_sizes: Vec::new(),
                tree_aux_roots: Vec::new(),
            }
            .encode_frame_for_version(ZAKURA_HEADER_SYNC_STREAM_VERSION_V7, Some(request_id))
            .expect("v7 response encodes");

            assert!(matches!(pipe.run_one(frame), Flow::Done));
            assert!(matches!(
                events.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
        }
    }

    #[test]
    fn v7_evicted_retired_id_is_still_stale_but_future_id_is_unknown() {
        let (_commands_tx, commands_rx) = mpsc::unbounded_channel();
        let mut local = HsLocal::new(
            commands_rx,
            DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
            ZAKURA_HEADER_SYNC_STREAM_VERSION_V7,
        );
        for id in 1..=u64::try_from(MAX_RETIRED_HEADER_REQUEST_IDS + 1)
            .expect("retired-id test bound fits in u64")
        {
            let request_id = HeaderSyncRequestId::new(id).expect("positive id");
            let expected = ExpectedHeadersResponse::new(block::Height(1), 1, true)
                .expect("count is valid")
                .with_request_id(request_id);
            local.handle_command(HeaderSyncPeerCommand::Reserve(expected));
            local.handle_command(HeaderSyncPeerCommand::Retire(request_id));
        }
        let evicted = HeaderSyncRequestId::new(1).expect("non-zero id");
        let never_issued = HeaderSyncRequestId::new(
            u64::try_from(MAX_RETIRED_HEADER_REQUEST_IDS + 2)
                .expect("retired-id test bound fits in u64"),
        )
        .expect("non-zero id");

        assert!(!local.retired_headers.contains(&evicted));
        assert_eq!(
            local
                .pop_expected_headers_response_by_id(evicted)
                .expect("evicted issued id remains stale"),
            None
        );
        assert!(matches!(
            local.pop_expected_headers_response_by_id(never_issued),
            Err(HeaderSyncWireError::UnsolicitedHeaders)
        ));
    }

    #[test]
    fn v7_restored_expectation_reactivates_consumed_request_id() {
        let (_commands_tx, commands_rx) = mpsc::unbounded_channel();
        let request_id = HeaderSyncRequestId::new(9).expect("non-zero id");
        let expected = ExpectedHeadersResponse::new(block::Height(1), 1, true)
            .expect("count is valid")
            .with_request_id(request_id);
        let mut local = HsLocal::new(
            commands_rx,
            DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
            ZAKURA_HEADER_SYNC_STREAM_VERSION_V7,
        );
        local.expected_headers_by_id.insert(request_id, expected);

        assert_eq!(
            local
                .pop_expected_headers_response_by_id(request_id)
                .expect("active id is known"),
            Some(expected)
        );
        local.restore_expected_headers(expected);
        assert_eq!(
            local
                .pop_expected_headers_response_by_id(request_id)
                .expect("restored id is active"),
            Some(expected)
        );
    }

    #[test]
    fn v7_unknown_headers_response_id_is_protocol_failure() {
        let (handle, mut events) = test_handle();
        let (_commands_tx, commands_rx) = mpsc::unbounded_channel();
        let request_id = HeaderSyncRequestId::new(8).expect("non-zero id");
        let mut pipe = Pipe::new(
            peer(),
            HsLocal::new(
                commands_rx,
                DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
                ZAKURA_HEADER_SYNC_STREAM_VERSION_V7,
            ),
            HsEnv::new(handle),
            crate::zakura::SessionGuard::oversize_only(MAX_HS_MESSAGE_BYTES as u32),
            run_inbound,
            &PIPE_SHAPE,
        );
        let frame = HeaderSyncMessage::Headers {
            headers: Vec::new(),
            body_sizes: Vec::new(),
            tree_aux_roots: Vec::new(),
        }
        .encode_frame_for_version(ZAKURA_HEADER_SYNC_STREAM_VERSION_V7, Some(request_id))
        .expect("v7 response encodes");

        assert!(matches!(pipe.run_one(frame), Flow::Reject(_)));
        match events.try_recv() {
            Ok(HeaderSyncEvent::WireProtocolFailure { reason, .. }) => {
                assert_eq!(reason, HeaderSyncMisbehavior::UnsolicitedHeaders);
            }
            other => panic!("expected unknown id protocol failure, got {other:?}"),
        }
    }

    /// A `NewBlock` flood is throttled *before* full-block decode: the first
    /// frame in a window is decoded and forwarded to the reactor, but a second
    /// distinct frame inside the per-peer minimum interval is dropped before
    /// `Block::zcash_deserialize` runs, so nothing reaches the reactor and the
    /// peer is kept (`Flow::Done`). This proves the amplification gap is closed —
    /// without the pre-decode gate the second full block is deserialized and
    /// forwarded too.
    #[test]
    fn new_block_flood_is_throttled_before_decode() {
        use zakura_chain::serialization::ZcashDeserializeInto;
        use zakura_test::vectors::{BLOCK_MAINNET_1_BYTES, BLOCK_MAINNET_2_BYTES};

        let (handle, mut events) = test_handle();
        let (_commands_tx, commands_rx) = mpsc::unbounded_channel();

        let block_one: Arc<block::Block> = Arc::new(
            BLOCK_MAINNET_1_BYTES
                .zcash_deserialize_into()
                .expect("block 1 vector parses"),
        );
        let block_two: Arc<block::Block> = Arc::new(
            BLOCK_MAINNET_2_BYTES
                .zcash_deserialize_into()
                .expect("block 2 vector parses"),
        );
        let frame_one = HeaderSyncMessage::NewBlock(block_one.clone())
            .encode_frame()
            .expect("new block frame encodes");
        let frame_two = HeaderSyncMessage::NewBlock(block_two.clone())
            .encode_frame()
            .expect("new block frame encodes");

        let mut pipe = Pipe::new(
            peer(),
            HsLocal::new(
                commands_rx,
                DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
                ZAKURA_HEADER_SYNC_STREAM_VERSION,
            ),
            HsEnv::new(handle),
            crate::zakura::SessionGuard::oversize_only(MAX_HS_MESSAGE_BYTES as u32),
            run_inbound,
            &PIPE_SHAPE,
        );

        // First flood frame: admitted, decoded, and forwarded to the reactor.
        assert!(matches!(pipe.run_one(frame_one), Flow::Continue(())));
        match events.try_recv() {
            Ok(HeaderSyncEvent::WireMessage {
                msg: HeaderSyncMessage::NewBlock(block),
                ..
            }) => assert_eq!(block.hash(), block_one.hash()),
            other => panic!("expected first NewBlock to be forwarded, got {other:?}"),
        }

        // Second distinct flood frame inside the interval is dropped before
        // decode: the peer is kept and nothing reaches the reactor.
        assert!(matches!(pipe.run_one(frame_two), Flow::Done));
        assert!(
            matches!(events.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "second NewBlock must be throttled before decode, not forwarded"
        );
    }

    /// Under reactor `events`-queue saturation, a valid *solicited* `Headers`
    /// response must not silently consume its peer-local expectation. The pipe
    /// pops the expectation before decode; when the decoded response cannot be
    /// delivered to the full reactor queue, the pipe logs and continues
    /// (`Flow::Done`) — but the popped expectation is restored to the FIFO so the
    /// reactor's still-outstanding range stays correlated. Without the fix the
    /// expectation is consumed and lost, stranding the range until the request
    /// timeout and desynchronizing the peer-local FIFO from the outstanding range.
    #[test]
    fn saturated_events_queue_restores_solicited_expectation() {
        use zakura_chain::{orchard, sapling, serialization::ZcashDeserializeInto};
        use zakura_test::vectors::BLOCK_MAINNET_1_BYTES;

        // Keep `_events_rx` alive so the saturated queue rejects with `Full`
        // (a live receiver), not `Closed`.
        let (handle, _events_rx) = saturated_events_handle();
        let (commands_tx, commands_rx) = mpsc::unbounded_channel();

        let expected =
            ExpectedHeadersResponse::new(block::Height(1), 1, true).expect("count is valid");
        commands_tx
            .send(HeaderSyncPeerCommand::Reserve(expected))
            .expect("pipe is alive");

        // A syntactically valid one-header solicited response: it decodes against
        // the expectation and reaches the reactor forward, where the full queue
        // turns it into a local reject.
        let block_one: Arc<block::Block> = Arc::new(
            BLOCK_MAINNET_1_BYTES
                .zcash_deserialize_into()
                .expect("block 1 vector parses"),
        );
        let solicited_headers = HeaderSyncMessage::Headers {
            headers: vec![block_one.header.clone()],
            body_sizes: vec![0],
            tree_aux_roots: vec![BlockCommitmentRoots {
                height: block::Height(1),
                sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
                orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
                ironwood_root: zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
                sapling_tx: 0,
                orchard_tx: 0,
                ironwood_tx: 0,
                auth_data_root: block::merkle::AuthDataRoot::from([0u8; 32]),
            }],
        }
        .encode_frame()
        .expect("headers frame encodes");

        let mut pipe = Pipe::new(
            peer(),
            HsLocal::new(
                commands_rx,
                DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL,
                ZAKURA_HEADER_SYNC_STREAM_VERSION,
            ),
            HsEnv::new(handle),
            crate::zakura::SessionGuard::oversize_only(MAX_HS_MESSAGE_BYTES as u32),
            run_inbound,
            &PIPE_SHAPE,
        );
        // Drain the recorded expectation into `HsLocal`, mirroring `run_peer`'s
        // pre-frame command drain so the `Headers` frame is correlated.
        pipe.local_mut().drain_ready_commands();
        assert_eq!(
            pipe.local_mut().pop_expected_headers_response(),
            Some(expected),
            "the solicited response expectation should be available after draining commands"
        );
        pipe.local_mut().restore_expected_headers(expected);
        HeaderSyncMessage::decode_frame(
            solicited_headers.clone(),
            HeaderSyncDecodeContext::for_headers_response(expected, expected.count),
        )
        .expect("test Headers frame decodes against its expectation");

        // The decoded response cannot be delivered (events queue is full); the
        // pipe logs and continues, exactly as production does.
        let flow = pipe.run_one(solicited_headers);
        match flow {
            Flow::Done => {}
            Flow::Continue(()) => panic!("unexpected successful forward"),
            Flow::Reject(SinkReject::Protocol(_)) => panic!("unexpected protocol reject"),
            Flow::Reject(SinkReject::Local(_)) => panic!("unexpected local reject"),
        }

        // The popped expectation must be restored so the still-outstanding range
        // stays correlated. Without the fix the expectation is gone (returns None).
        assert_eq!(
            pipe.local_mut().pop_expected_headers_response(),
            Some(expected),
            "a solicited Headers response dropped on reactor queue saturation must restore its expectation"
        );
    }

    #[test]
    fn pipe_shape_matches_runtime() {
        // (a) The declared shape is internally consistent.
        PIPE_SHAPE
            .validate()
            .expect("header-sync PIPE_SHAPE edges name only real nodes");

        // (b) Phase 2's real runtime fork is still frame-shape based:
        // `Headers` needs peer-local request correlation, while all other
        // Header-sync v6 messages decode as `Control` and are forwarded to the
        // compatibility reactor for semantic dispatch.
        let frame_forks: Vec<&str> = PIPE_SHAPE
            .edges
            .iter()
            .filter(|edge| edge.from == "guard")
            .map(|edge| edge.on)
            .collect();

        assert_eq!(
            frame_forks.len(),
            FRAME_FORKS.len(),
            "guard has exactly the runtime frame-shape forks"
        );
        for fork in FRAME_FORKS {
            assert!(
                frame_forks.contains(&fork),
                "guard edge missing for runtime fork {fork}"
            );
        }

        // (c) `Headers` responses correlate before decode; all decoded messages
        // terminate at the single forward/emit stage.
        assert!(
            PIPE_SHAPE
                .edges
                .iter()
                .any(|edge| edge.from == "correlate" && edge.to == "decode"),
            "headers responses correlate before decode"
        );
        assert!(
            PIPE_SHAPE
                .nodes
                .iter()
                .any(|node| node.id == "emit" && matches!(node.kind, NodeKind::Emit)),
            "the pipe terminates at a single `emit` node"
        );
    }
}
