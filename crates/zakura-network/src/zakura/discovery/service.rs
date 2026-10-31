//! Native discovery service (stream kind 4) on the Zakura transport.
//!
//! Discovery is a single long-lived ordered stream per peer. Each side runs a
//! [`DiscoverySink`] (the reader, which imports peer records and answers
//! `GetPeers`) and a [`PeerIO`] (the writer, which periodically gossips the
//! local self-record and asks for more peers). Each [`DiscoveryMessage`]
//! travels in one transport frame whose type is the message's discriminator;
//! `wire` defines the payloads and their bounds.

use std::sync::Arc;

use zakura_node_services::task::spawn_erased_named;
use zakura_quic::NodeId;
use zakura_rt::{
    collections::HashMap,
    sync::{CancellationToken, Mutex as StdMutex, MutexGuard},
};

use crate::zakura::{
    handle_pipe_exit, spawn_supervised_peer_task, spawn_supervised_pipe, BlockSyncHandle,
    CloseCause, Flow, Frame, FramedRecv, FramedSend, HeaderSyncHandle, Peer, PeerStreamSession,
    Pipe, Service, ServiceAdmissionDecision, ServicePeerDirection, SessionDemand, SessionOpening,
    SessionPolicy, SinkReject, Stream, ZakuraConnId, ZakuraPeerId, ZakuraTrace,
    DEFAULT_ZAKURA_MAX_CONNECTIONS, FRAME_HEADER_BYTES, ZAKURA_CAP_DISCOVERY,
};
use crate::BoxError;

use super::peer::{PeerIO, SharedPeerState};
use super::pipe::{discovery_pipe, DsEnv, DsLocal};
use super::protocol::{
    DiscoveryMessage, RecordVerdict, Services, ZakuraDiscoveryHandle, ZakuraNodeRecord,
    ZAKURA_DISCOVERY_STREAM_VERSION, ZAKURA_STREAM_DISCOVERY,
};
use super::requester::{reservation_pool, ClaimedAnswer, Queries};
use super::serving::{PeerSampler, ServiceReporter, Serving, ServingCapacity};
use super::state::DiscoverySessionKey;
use super::trace::{AnswerPhase, DiscoveryAnswerEvent, DiscoveryViolationEvent, ViolationStage};
use super::wire::{PEERS_MAX_BYTES, RULES};
use crate::zakura::regulation::{CadenceSender, ReservationPool, WriterFence};

const DISCOVERY_SERVICE_STREAMS: [Stream; 1] = [Stream {
    kind: ZAKURA_STREAM_DISCOVERY,
    version: ZAKURA_DISCOVERY_STREAM_VERSION,
    // The largest payload plus its header is far below u32::MAX.
    frame_cap: (PEERS_MAX_BYTES + FRAME_HEADER_BYTES) as u32,
    capability: ZAKURA_CAP_DISCOVERY,
    messages: Some(RULES),
    ..Stream::PERSISTENT
}];

const _: () = Stream::validate_layout(&DISCOVERY_SERVICE_STREAMS);

/// Service-declared streams for native discovery.
pub(crate) fn discovery_streams() -> &'static [Stream] {
    &DISCOVERY_SERVICE_STREAMS
}

/// Cloneable typed sender for one native discovery ordered stream.
#[derive(Clone, Debug)]
pub struct DiscoveryPeerSession {
    peer_id: ZakuraPeerId,
    direction: ServicePeerDirection,
    pub(super) send: FramedSend,
    cancel: CancellationToken,
}

impl DiscoveryPeerSession {
    fn new(session: &PeerStreamSession, direction: ServicePeerDirection) -> Self {
        Self {
            peer_id: session.peer_id().clone(),
            direction,
            send: session.sender(),
            cancel: session.cancel_token(),
        }
    }

    /// Authenticated peer identity for this discovery stream.
    pub fn peer_id(&self) -> &ZakuraPeerId {
        &self.peer_id
    }

    /// Direction of the underlying Zakura connection.
    pub fn direction(&self) -> ServicePeerDirection {
        self.direction
    }

    /// Peer disconnect/local shutdown cancellation token.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }
}

/// Native discovery service backed by a [`ZakuraDiscoveryHandle`] runtime.
#[derive(Clone, Debug)]
pub struct DiscoveryService {
    handle: ZakuraDiscoveryHandle,
    header_sync: Option<HeaderSyncHandle>,
    block_sync: Option<BlockSyncHandle>,
    session_states: Arc<StdMutex<DiscoverySessionStates>>,
    connection_owners: Arc<StdMutex<Vec<Arc<dyn Service>>>>,
    /// Node-wide capacity of the request rows.
    serving: ServingCapacity,
    /// Node-wide reservations for this node's queries.
    query_pool: ReservationPool,
    trace: ZakuraTrace,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DiscoverySessionState {
    Active,
    Retired,
}

/// The session-state slot for one `(peer, connection)`, tagged with the stream
/// session that owns it so a finished exchange cannot retire a newer session
/// admitted on the same connection.
#[derive(Clone, Copy, Debug)]
struct DiscoverySessionRecord {
    session_id: u64,
    state: DiscoverySessionState,
    /// Consecutive sessions on this connection that ended without a successful
    /// exchange. Reaching `MAX_DISCOVERY_SESSION_FAILURES` retires the record so
    /// the transport stops reopening a stream the peer keeps breaking.
    failed_attempts: u32,
}

/// Consecutive failed sessions on one `(peer, connection)` before discovery
/// retires it. Generous: an honest peer completes the exchange on the first
/// session, and a fresh connection always starts a fresh record.
const MAX_DISCOVERY_SESSION_FAILURES: u32 = 3;

/// Discovery's session records, one per `(peer, connection)`.
///
/// Each transition names the exact session it acts for, so a finished or
/// failed session cannot change the record of a newer session admitted on the
/// same connection. The service holds the records under their own lock,
/// separate from the discovery handle's.
#[derive(Debug, Default)]
struct DiscoverySessionStates {
    records: HashMap<(ZakuraPeerId, ZakuraConnId), DiscoverySessionRecord>,
}

/// What charging one failed session did to its record.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum SessionFailure {
    /// The session owns no active record, so nothing was charged.
    Stale,
    /// The record counted the failure and stays active.
    Counted,
    /// The failure reached the bound and retired the record.
    Retired {
        /// Consecutive failed sessions, including this one.
        attempts: u32,
    },
}

impl DiscoverySessionStates {
    /// Whether the record for `peer` on `conn_id` is retired.
    fn is_retired(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) -> bool {
        self.records
            .get(&(peer.clone(), conn_id))
            .is_some_and(|record| record.state == DiscoverySessionState::Retired)
    }

    /// Makes `session` the active owner of its connection's record.
    ///
    /// The failure count carries over from the previous session, so
    /// consecutive broken sessions on one connection stay bounded. Returns
    /// `false`, changing nothing, when the record is retired: the remote
    /// initiates streams on inbound connections and could otherwise churn
    /// sessions at its own pace.
    fn open(&mut self, session: &DiscoverySessionKey) -> bool {
        let key = (session.peer_id.clone(), session.conn_id);
        let previous = self.records.get(&key);
        if previous.is_some_and(|record| record.state == DiscoverySessionState::Retired) {
            return false;
        }
        let failed_attempts = previous.map_or(0, |record| record.failed_attempts);
        self.records.insert(
            key,
            DiscoverySessionRecord {
                session_id: session.session_id,
                state: DiscoverySessionState::Active,
                failed_attempts,
            },
        );
        true
    }

    /// Retires the record `session` owns. Returns whether it did.
    fn retire(&mut self, session: &DiscoverySessionKey) -> bool {
        let Some(record) = self.owned_by(session) else {
            return false;
        };
        record.state = DiscoverySessionState::Retired;
        true
    }

    /// Charges one failed session to the active record `session` owns, and
    /// retires the record at `MAX_DISCOVERY_SESSION_FAILURES`.
    fn record_failure(&mut self, session: &DiscoverySessionKey) -> SessionFailure {
        let Some(record) = self
            .owned_by(session)
            .filter(|record| record.state == DiscoverySessionState::Active)
        else {
            return SessionFailure::Stale;
        };
        record.failed_attempts = record.failed_attempts.saturating_add(1);
        if record.failed_attempts >= MAX_DISCOVERY_SESSION_FAILURES {
            record.state = DiscoverySessionState::Retired;
            return SessionFailure::Retired {
                attempts: record.failed_attempts,
            };
        }
        SessionFailure::Counted
    }

    /// Resets the failure count of the record `session` owns, so the intended
    /// shared-connection refresh loop never accumulates failures.
    fn record_success(&mut self, session: &DiscoverySessionKey) {
        if let Some(record) = self.owned_by(session) {
            record.failed_attempts = 0;
        }
    }

    /// Drops the record for `peer` on `conn_id` once the connection ends.
    fn remove_connection(&mut self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) {
        self.records.remove(&(peer.clone(), conn_id));
    }

    fn owned_by(&mut self, session: &DiscoverySessionKey) -> Option<&mut DiscoverySessionRecord> {
        self.records
            .get_mut(&(session.peer_id.clone(), session.conn_id))
            .filter(|record| record.session_id == session.session_id)
    }
}

/// One session's record in the shared session-state map.
pub(super) struct SessionSlot {
    states: Arc<StdMutex<DiscoverySessionStates>>,
    pub(super) key: DiscoverySessionKey,
}

impl SessionSlot {
    pub(super) fn succeed(&self) {
        self.lock().record_success(&self.key);
    }

    /// Charges this session's failure. A parked session also charges one,
    /// so reopen-then-park cycles stay bounded on its connection.
    pub(super) fn fail(&self) {
        if let SessionFailure::Retired { attempts } = self.lock().record_failure(&self.key) {
            tracing::info!(
                peer = ?self.key.peer_id,
                conn_id = self.key.conn_id,
                attempts,
                "retiring Zakura discovery sessions on this connection after repeated failed exchanges"
            );
        }
    }

    pub(super) fn retire(&self) {
        self.lock().retire(&self.key);
    }

    fn lock(&self) -> MutexGuard<'_, DiscoverySessionStates> {
        self.states
            .lock()
            .expect("discovery session-state mutex is never poisoned")
    }
}

impl DiscoveryService {
    /// Builds a discovery service driven by `handle`, with serving pools sized
    /// for the default connection cap.
    pub fn new(handle: ZakuraDiscoveryHandle) -> Self {
        Self::with_max_connections(handle, DEFAULT_ZAKURA_MAX_CONNECTIONS)
    }

    /// Builds a discovery service driven by `handle`, with serving pools sized
    /// for a node that admits `max_connections`.
    pub fn with_max_connections(handle: ZakuraDiscoveryHandle, max_connections: usize) -> Self {
        Self {
            handle,
            header_sync: None,
            block_sync: None,
            session_states: Arc::default(),
            connection_owners: Arc::new(StdMutex::new(Vec::new())),
            serving: ServingCapacity::new(max_connections),
            query_pool: reservation_pool(),
            trace: ZakuraTrace::default(),
        }
    }

    /// Builds a discovery service with header-sync and block-sync summary
    /// providers, which writes its violations to `trace` and sizes its serving
    /// pools for `max_connections`.
    pub(crate) fn with_sync_services(
        handle: ZakuraDiscoveryHandle,
        header_sync: HeaderSyncHandle,
        block_sync: Option<BlockSyncHandle>,
        trace: ZakuraTrace,
        max_connections: usize,
    ) -> Self {
        Self {
            handle,
            header_sync: Some(header_sync),
            block_sync,
            session_states: Arc::default(),
            connection_owners: Arc::new(StdMutex::new(Vec::new())),
            serving: ServingCapacity::new(max_connections),
            query_pool: reservation_pool(),
            trace,
        }
    }

    pub(crate) fn set_connection_owners(&self, owners: Vec<Arc<dyn Service>>) {
        *self
            .connection_owners
            .lock()
            .expect("discovery connection-owner mutex is never poisoned") = owners;
    }

    /// Returns the underlying discovery runtime handle.
    pub fn handle(&self) -> &ZakuraDiscoveryHandle {
        &self.handle
    }
}

impl Service for DiscoveryService {
    fn name(&self) -> &'static str {
        "discovery"
    }

    fn streams(&self) -> &[Stream] {
        discovery_streams()
    }

    fn session_policy(&self) -> SessionPolicy {
        SessionPolicy {
            opening: SessionOpening::InitiatorOnly,
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
        if self
            .session_states
            .lock()
            .expect("discovery session-state mutex is never poisoned")
            .is_retired(peer, conn_id)
        {
            return SessionDemand::Retire;
        }

        let mut peers = self.handle.subscribe_peer_snapshot();
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

        SessionDemand::OpenNow
    }

    fn wants_peer(
        &self,
        _peer: &ZakuraPeerId,
        _negotiated: u64,
        direction: ServicePeerDirection,
    ) -> bool {
        // Discovery escalation only checks this reactor's local room; live
        // summaries are first-party advisory data imported by the runtime.
        let snapshot = self.handle.peer_snapshot();
        match direction {
            ServicePeerDirection::Inbound => snapshot.inbound_slots_free > 0,
            ServicePeerDirection::Outbound => snapshot.outbound_slots_free > 0,
        }
    }

    fn add_peer(&self, mut peer: Peer) {
        let Some((session_id, recv, send)) =
            peer.take_stream_with_session_id(ZAKURA_STREAM_DISCOVERY)
        else {
            return;
        };
        let Some(peer_node_id) = node_id_from_peer_id(&peer.id) else {
            // A peer id that is not a 32-byte node id cannot be a discovery
            // author; drop the stream without registering an exchange.
            return;
        };
        let session = PeerStreamSession::new(
            peer.id.clone(),
            ZAKURA_STREAM_DISCOVERY,
            ZAKURA_DISCOVERY_STREAM_VERSION,
            recv,
            send,
            peer.service_cancel_token(),
        );
        let discovery_session = DiscoveryPeerSession::new(&session, peer.direction);
        let slot = SessionSlot {
            states: self.session_states.clone(),
            key: DiscoverySessionKey::new(peer.id.clone(), peer.conn_id, session_id),
        };
        if !slot.lock().open(&slot.key) {
            return;
        }
        let service_cancel = discovery_session.cancel_token();
        let connection_cancel = peer.cancel_token();
        let close_cause = peer.close_cause();
        let (_peer_id, _stream_kind, _stream_version, recv, _send, _session_cancel) =
            session.into_parts();

        let handle = self.handle.clone();
        let header_sync = self.header_sync.clone();
        let block_sync = self.block_sync.clone();
        let serving = self.serving.clone();
        let query_pool = self.query_pool.clone();
        let trace = self.trace.clone();
        let connection_owners = self
            .connection_owners
            .lock()
            .expect("discovery connection-owner mutex is never poisoned")
            .clone();
        // SR-1: a panic in the admission task (before it hands off to the
        // exchange) must still disconnect this one peer and cancel its discovery
        // session instead of leaving admitted state behind a half-live
        // connection. Normal/parked exits cancel `service_cancel` inline below;
        // `on_panic` covers the unwind path only.
        let admit_peer_id = discovery_session.peer_id().clone();
        let panic_service_cancel = service_cancel.clone();
        let panic_connection_cancel = connection_cancel.clone();
        let panic_close_cause = close_cause.clone();
        spawn_supervised_peer_task(
            "discovery_admission",
            admit_peer_id,
            || {},
            move || {
                panic_close_cause.record("service_panic");
                panic_service_cancel.cancel();
                panic_connection_cancel.cancel();
            },
            async move {
                let decision = handle
                    .admit_peer_session(slot.key.clone(), discovery_session.direction())
                    .await;
                if decision != ServiceAdmissionDecision::Admit {
                    metrics::counter!("zakura.discovery.peer.parked").increment(1);
                    tracing::info!(
                        peer = ?discovery_session.peer_id(),
                        direction = ?discovery_session.direction(),
                        ?decision,
                        "locally parking Zakura discovery service session"
                    );
                    // A parked session ends without an exchange, so it is charged.
                    slot.fail();
                    service_cancel.cancel();
                    return;
                }

                spawn_discovery_exchange(DiscoveryExchangeStart {
                    handle,
                    header_sync,
                    block_sync,
                    serving,
                    query_pool,
                    connection_owners,
                    peer_node_id,
                    discovery_session,
                    slot,
                    recv,
                    service_cancel,
                    connection_cancel,
                    close_cause,
                    trace,
                });
            },
        );
    }

    fn remove_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) {
        self.session_states
            .lock()
            .expect("discovery session-state mutex is never poisoned")
            .remove_connection(peer, conn_id);
        let handle = self.handle.clone();
        let peer = peer.clone();
        spawn_erased_named("discovery_remove_peer", async move {
            handle.remove_peer(&peer, conn_id).await;
        });
    }
}

struct DiscoveryExchangeStart {
    handle: ZakuraDiscoveryHandle,
    header_sync: Option<HeaderSyncHandle>,
    block_sync: Option<BlockSyncHandle>,
    serving: ServingCapacity,
    query_pool: ReservationPool,
    connection_owners: Vec<Arc<dyn Service>>,
    peer_node_id: NodeId,
    discovery_session: DiscoveryPeerSession,
    /// The session's record, which also names the session.
    slot: SessionSlot,
    recv: FramedRecv,
    service_cancel: CancellationToken,
    connection_cancel: CancellationToken,
    close_cause: CloseCause,
    trace: ZakuraTrace,
}

fn spawn_discovery_exchange(start: DiscoveryExchangeStart) {
    let DiscoveryExchangeStart {
        handle,
        header_sync,
        block_sync,
        serving,
        query_pool,
        connection_owners,
        peer_node_id,
        discovery_session,
        slot,
        recv,
        service_cancel,
        connection_cancel,
        close_cause,
        trace,
    } = start;
    let peer_id = discovery_session.peer_id().clone();
    let state = SharedPeerState::default();
    let serving = serving.session(
        PeerSampler {
            handle: handle.clone(),
            requester: peer_node_id,
        },
        ServiceReporter {
            handle: handle.clone(),
            header_sync,
            block_sync,
        },
        &peer_id,
        discovery_session.send.clone(),
        service_cancel.clone(),
        connection_cancel.clone(),
        close_cause.clone(),
    );
    let answers = serving.answers_written();
    let queries = Queries::new(
        query_pool,
        WriterFence::new(connection_cancel.clone(), close_cause.clone()),
        &recv,
        discovery_session.send.clone(),
        service_cancel.clone(),
    );
    let sink = DiscoverySink {
        handle: handle.clone(),
        serving,
        queries: queries.clone(),
        peer_node_id,
        session: discovery_session.clone(),
        key: slot.key.clone(),
        state: state.clone(),
        trace,
    };
    let sink_service_cancel = service_cancel.clone();
    let reject_connection_cancel = connection_cancel.clone();
    let panic_connection_cancel = connection_cancel.clone();
    let reject_close_cause = close_cause.clone();
    let panic_close_cause = close_cause.clone();
    let sink_peer_id = peer_id.clone();
    // A protocol reject is fatal to the connection; normal/parked exits leave it
    // for the writer's `PeerIO` to tear down once it knows no other service
    // owns the peer (below). Panic teardown is in `on_panic`.
    let pipe = async move {
        let mut pipe = discovery_pipe(sink_peer_id);
        handle_pipe_exit(
            "discovery",
            &reject_connection_cancel,
            &reject_close_cause,
            run_discovery_pipe(&mut pipe, recv, sink).await,
        );
    };
    let on_panic = move || {
        panic_close_cause.record("service_panic");
        panic_connection_cancel.cancel();
    };
    // Let the returned handle drop to detach the supervised reader task; the
    // `PipeTeardown` still runs on every exit path.
    spawn_supervised_pipe(
        "discovery_pipe",
        peer_id.clone(),
        sink_service_cancel,
        || {},
        on_panic,
        pipe,
    );

    let io = PeerIO {
        handle,
        session: discovery_session,
        slot,
        state,
        hello: CadenceSender::new(),
        queries,
        answers,
        connection_owners,
        peer_node_id,
        connection_cancel: connection_cancel.clone(),
        close_cause: close_cause.clone(),
    };
    // SR-1: a panic in the writer task skips its `service_cancel.cancel()`,
    // `handle.remove_session()`, and discovery-only connection cancellation,
    // leaving admitted discovery state behind a half-live connection. On the
    // unwind path, disconnect this one peer; the connection teardown then drives
    // the async session removal through the registry. Normal exits end the
    // session in `PeerIO::run`, so `on_panic` is the panic-only path.
    spawn_supervised_peer_task(
        "discovery_source",
        peer_id,
        || {},
        move || {
            close_cause.record("service_panic");
            service_cancel.cancel();
            connection_cancel.cancel();
        },
        io.run(),
    );
}

/// Reader half of the discovery stream: imports peer records and answers queries.
struct DiscoverySink {
    handle: ZakuraDiscoveryHandle,
    /// Serves this session's requests.
    serving: Serving,
    /// This session's queries, whose answers the reader claims.
    queries: Queries,
    peer_node_id: NodeId,
    session: DiscoveryPeerSession,
    /// The session this reader serves.
    key: DiscoverySessionKey,
    /// Receives the exchange progress this reader processes.
    state: SharedPeerState,
    /// Records each violation before the connection closes.
    trace: ZakuraTrace,
}

async fn run_discovery_pipe(
    pipe: &mut Pipe<DsLocal, DsEnv>,
    recv: FramedRecv,
    sink: DiscoverySink,
) -> Result<(), SinkReject> {
    let cancel = sink.session.cancel_token();
    // A cancelled session drops its pending work, including a wait for a
    // verification slot, so it neither holds the slot queue nor imports after
    // its session ends.
    zakura_rt::select! {
        biased;
        _ = cancel.cancelled() => Ok(()),
        read = read_discovery_messages(pipe, recv, &sink) => read,
    }
}

/// Reads frames until the stream ends, and traces the violation that ends it
/// early.
async fn read_discovery_messages(
    pipe: &mut Pipe<DsLocal, DsEnv>,
    mut recv: FramedRecv,
    sink: &DiscoverySink,
) -> Result<(), SinkReject> {
    while let Some(frame) = recv.recv().await {
        let message_type = frame.message_type;
        if let Err((stage, reject)) = read_frame(pipe, frame, sink).await {
            if let SinkReject::Protocol(error) = &reject {
                sink.trace_violation(message_type, stage, error);
            }
            return Err(reject);
        }
    }
    Ok(())
}

/// Runs one frame through the pipe and handles its decoded message. An error
/// names the stage that rejected the frame.
async fn read_frame(
    pipe: &mut Pipe<DsLocal, DsEnv>,
    frame: Frame,
    sink: &DiscoverySink,
) -> Result<(), (ViolationStage, SinkReject)> {
    // An answer claims its query before its payload is decoded.
    let answer = sink
        .queries
        .claim(frame.message_type, frame.payload.len())
        .map_err(|error| (ViolationStage::Claim, SinkReject::protocol(error)))?
        .map(|claimed| TracedAnswer::claimed(claimed, sink));
    if let Flow::Reject(reject) = pipe.run_one(frame) {
        return Err((ViolationStage::Decode, reject));
    }
    let Some(message) = pipe.local_mut().take_decoded() else {
        return Ok(());
    };
    if let Some(answer) = &answer {
        answer
            .claimed
            .admitted()
            .check(&message)
            .map_err(|error| (ViolationStage::Answer, SinkReject::protocol(error)))?;
    }
    sink.handle_message(message, answer)
        .await
        .map_err(|reject| (ViolationStage::Handle, reject))
}

/// A claimed answer that traces how it settles, on every path out of the
/// reader.
struct TracedAnswer<'a> {
    claimed: ClaimedAnswer,
    sink: &'a DiscoverySink,
    imported: bool,
}

impl<'a> TracedAnswer<'a> {
    fn claimed(claimed: ClaimedAnswer, sink: &'a DiscoverySink) -> Self {
        let answer = Self {
            claimed,
            sink,
            imported: false,
        };
        answer.trace(AnswerPhase::Claimed);
        answer
    }

    fn trace(&self, phase: AnswerPhase) {
        self.sink.trace.emit_event(|| {
            DiscoveryAnswerEvent::new(
                self.sink.session.peer_id(),
                self.sink.key.session_id,
                self.claimed.admitted().kind(),
                phase,
            )
        });
    }

    /// Counts the answer toward the session's exchange.
    fn record_imported(&mut self) {
        self.sink.state.record_answer(self.claimed.admitted());
        self.imported = true;
    }
}

impl Drop for TracedAnswer<'_> {
    fn drop(&mut self) {
        self.trace(if self.imported {
            AnswerPhase::Imported
        } else {
            AnswerPhase::Dropped
        });
    }
}

impl DiscoverySink {
    fn trace_violation(&self, message_type: u16, stage: ViolationStage, error: &BoxError) {
        self.trace.emit_event(|| {
            DiscoveryViolationEvent::new(
                self.session.peer_id(),
                self.key.session_id,
                message_type,
                stage,
                error,
            )
        });
    }

    /// Handles one decoded message. An `answer` already passed its query's
    /// checks, and its import completes its part of the exchange. It settles
    /// once this returns or the reader drops this future.
    async fn handle_message(
        &self,
        message: DiscoveryMessage,
        answer: Option<TracedAnswer<'_>>,
    ) -> Result<(), SinkReject> {
        if !self.handle.is_current_session(&self.key).await {
            return Ok(());
        }

        match message {
            DiscoveryMessage::Hello { record } => self.handle_hello(record).await?,
            DiscoveryMessage::GetPeers(query) => self
                .serving
                .get_peers
                .admit(query)
                .map_err(SinkReject::protocol)?,
            DiscoveryMessage::Peers { records } => {
                self.handle
                    .import_session_peer_records(&self.key, records, self.peer_node_id)
                    .await
                    .map_err(SinkReject::protocol)?;
            }
            DiscoveryMessage::GetServices(query) => self
                .serving
                .get_services
                .admit(query)
                .map_err(SinkReject::protocol)?,
            DiscoveryMessage::Services(services) => self.handle_services(services).await?,
        }
        if let Some(mut answer) = answer {
            answer.record_imported();
        }
        Ok(())
    }

    /// Imports the peer's own summaries.
    async fn handle_services(&self, services: Services) -> Result<(), SinkReject> {
        if services.node_id != self.peer_node_id {
            return Err(SinkReject::protocol(
                "Zakura discovery SERVICES authored by a different node id",
            ));
        }
        self.handle
            .import_session_services(&self.key, services, self.peer_node_id)
            .await
            .map_err(SinkReject::protocol)?;
        Ok(())
    }

    /// Imports the peer's own record. An authentic record counts as initial
    /// progress even when local policy drops it.
    async fn handle_hello(&self, record: ZakuraNodeRecord) -> Result<(), SinkReject> {
        if let Err(error) = self
            .handle
            .import_session_record(&self.key, record, self.peer_node_id)
            .await
        {
            match error.verdict() {
                RecordVerdict::Drop => {
                    tracing::debug!(?error, "dropped Zakura discovery hello");
                }
                RecordVerdict::Disconnect => return Err(SinkReject::protocol(error)),
            }
        }
        self.state.record_hello();
        Ok(())
    }
}

/// Returns the node id encoded by a discovery peer id, if it is a 32-byte
/// node id.
fn node_id_from_peer_id(peer_id: &ZakuraPeerId) -> Option<NodeId> {
    let bytes: [u8; 32] = peer_id.as_bytes().try_into().ok()?;
    NodeId::from_bytes(&bytes).ok()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use tokio::{sync::watch, task::JoinHandle};
    use zakura_quic::NodeSecretKey;

    use super::*;
    use crate::zakura::block_sync::BlockSyncService;
    use crate::zakura::discovery::peer::{
        peer_has_other_service_owner, DISCOVERY_ANSWER_WRITE_DEADLINE,
        DISCOVERY_INITIAL_EXCHANGE_TIMEOUT,
    };
    use crate::zakura::discovery::protocol::{
        DiscoveryServiceSummary, GetPeers, GetServices, ServiceSummaryEnvelope,
        ZakuraLiveServiceSummary, ZakuraNodeRecordBody, ZakuraServiceId,
        MAX_DISCOVERY_RECORDS_PER_RESPONSE, MSG_DISCOVERY_PEERS, MSG_DISCOVERY_SERVICES,
    };
    use crate::zakura::discovery::wire::SEND_INTERVAL;
    use crate::zakura::header_sync::HeaderSyncService;
    use crate::zakura::wire_codec::encode_frame;
    use crate::zakura::Event;
    use crate::zakura::{
        framed_channel, spawn_block_sync_reactor, spawn_header_sync_reactor, BlockSyncFrontiers,
        BlockSyncStartup, FullStateFrontiers, HeaderSyncAction, HeaderSyncStartup, PeerSession,
        ServicePeerLimits, ZakuraBlockSyncConfig, ZakuraDiscoveryConfig,
        ZakuraDiscoveryLocalConfig, ZakuraHandshakeConfig, ZakuraHeaderSyncConfig,
        LOCAL_MAX_MESSAGE_BYTES, MAX_BS_RESPONSE_BYTES, ZAKURA_CAP_BLOCK_SYNC_REGULATED,
        ZAKURA_CAP_DISCOVERY, ZAKURA_CAP_HEADER_SYNC, ZAKURA_CAP_LEGACY_GOSSIP,
        ZAKURA_HEADER_SYNC_STREAM_VERSION, ZAKURA_STREAM_HEADER_SYNC,
    };
    use crate::zakura::{wire_codec::decode_frame, Frame};
    use zakura_chain::{block, parameters::Network};

    #[test]
    fn connection_ownership_is_scoped_to_the_exact_connection() {
        let peer = ZakuraPeerId::new(vec![37; 32]).expect("test peer id is within bounds");
        let owners: Vec<Arc<dyn Service>> = vec![Arc::new(TestConnectionOwner {
            peer: peer.clone(),
            conn_id: 2,
        })];

        assert!(!peer_has_other_service_owner(&owners, &peer, 1));
        assert!(peer_has_other_service_owner(&owners, &peer, 2));
    }

    #[derive(Debug)]
    struct TestConnectionOwner {
        peer: ZakuraPeerId,
        conn_id: ZakuraConnId,
    }

    impl Service for TestConnectionOwner {
        fn name(&self) -> &'static str {
            "test-connection-owner"
        }

        fn streams(&self) -> &[Stream] {
            &[]
        }

        fn owns_connection_for_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) -> bool {
            peer == &self.peer && conn_id == self.conn_id
        }

        fn add_peer(&self, _peer: Peer) {}

        fn remove_peer(&self, _peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {}
    }

    fn current_test_unix_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_secs()
    }

    fn spawn_test_header_sync() -> Result<
        (
            HeaderSyncHandle,
            tokio::sync::mpsc::Receiver<HeaderSyncAction>,
            JoinHandle<()>,
        ),
        crate::BoxError,
    > {
        let network = Network::new_regtest(Default::default());
        let anchor = (block::Height(0), network.genesis_hash());
        let startup = HeaderSyncStartup::new(
            network,
            anchor,
            FullStateFrontiers {
                finalized_height: anchor.0,
                verified_block_tip: anchor.0,
                verified_block_hash: anchor.1,
            },
            Some(anchor),
            ZakuraHeaderSyncConfig::default(),
            LOCAL_MAX_MESSAGE_BYTES,
        );
        spawn_header_sync_reactor(startup).map_err(Into::into)
    }

    fn discovery_frame(message: DiscoveryMessage) -> Result<Frame, crate::BoxError> {
        Ok(encode_frame(&message)?)
    }

    fn signed_discovery_record(
        secret_key: &NodeSecretKey,
        handshake: &ZakuraHandshakeConfig,
    ) -> Result<ZakuraNodeRecord, crate::BoxError> {
        let body = ZakuraNodeRecordBody {
            node_id: secret_key.public(),
            direct_addrs: vec![SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(45, 33, 30, 45)),
                8233,
            )],
            services: vec![ZakuraServiceId::discovery()],
            zakura_protocol_min: handshake.zakura_protocol_min,
            zakura_protocol_max: handshake.zakura_protocol_max,
            network_id: handshake.network_id,
            chain_id: handshake.chain_id,
            sequence: 1,
            expires_at_unix_secs: current_test_unix_secs().saturating_add(60),
            extensions: Vec::new(),
        };
        Ok(ZakuraNodeRecord::sign(body, secret_key)?)
    }

    async fn complete_peer_side_discovery_exchange(
        peer_send: &FramedSend,
        peer_recv: &mut FramedRecv,
        peer_secret: &NodeSecretKey,
        handshake: &ZakuraHandshakeConfig,
    ) -> Result<(), crate::BoxError> {
        let mut saw_hello = false;
        let mut saw_get_peers = false;
        let mut saw_get_services = false;
        while !(saw_hello && saw_get_peers && saw_get_services) {
            let frame = tokio::time::timeout(Duration::from_secs(2), peer_recv.recv())
                .await?
                .expect("discovery source sends exchange frames");
            match decode_frame::<DiscoveryMessage>(&frame)? {
                DiscoveryMessage::Hello { .. } => saw_hello = true,
                DiscoveryMessage::GetPeers(_) => saw_get_peers = true,
                DiscoveryMessage::GetServices(_) => saw_get_services = true,
                DiscoveryMessage::Peers { .. } | DiscoveryMessage::Services(_) => {}
            }
        }
        answer_discovery_exchange(peer_send, peer_secret, handshake).await
    }

    /// Send the peer's `Hello`, `Peers` and `Services`, which complete the
    /// local side's exchange.
    async fn answer_discovery_exchange(
        peer_send: &FramedSend,
        peer_secret: &NodeSecretKey,
        handshake: &ZakuraHandshakeConfig,
    ) -> Result<(), crate::BoxError> {
        peer_send
            .send(discovery_frame(DiscoveryMessage::Hello {
                record: signed_discovery_record(peer_secret, handshake)?,
            })?)
            .await?;
        peer_send
            .send(discovery_frame(DiscoveryMessage::Peers {
                records: Vec::new(),
            })?)
            .await?;
        let summary = DiscoveryServiceSummary {
            peer_exchange_slots_free: 1,
            max_records_per_response: 1,
            expected_disconnect_after_exchange: true,
        };
        peer_send
            .send(discovery_frame(DiscoveryMessage::Services(Services {
                node_id: peer_secret.public(),
                expires_at_unix_secs: u64::MAX,
                summaries: vec![ServiceSummaryEnvelope::discovery(&summary)?],
            }))?)
            .await?;

        Ok(())
    }

    async fn wait_for_discovery_inbound_peers(handle: &ZakuraDiscoveryHandle, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if handle.peer_snapshot().inbound_peers == expected {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("discovery peer snapshot reaches expected inbound count");
    }

    #[tokio::test]
    async fn get_services_returns_local_first_party_discovery_summary(
    ) -> Result<(), crate::BoxError> {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let local_secret = NodeSecretKey::from_bytes(&[21u8; 32]);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: local_secret.clone(),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig {
                peer_limits: ServicePeerLimits {
                    max_inbound_peers: 4,
                    ..ServicePeerLimits::default()
                },
                ..ZakuraDiscoveryConfig::default()
            },
            connected_rx,
        )?;
        let service = DiscoveryService::new(handle.clone());
        let peer_node_id = NodeSecretKey::from_bytes(&[22u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        let (peer_send, service_recv) = framed_channel(8);
        let (service_send, mut peer_recv) = framed_channel(8);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);

        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY,
            streams,
            CancellationToken::new(),
        ));

        peer_send
            .send(encode_frame(&DiscoveryMessage::GetServices(GetServices {
                wanted_services: vec![ZakuraServiceId::discovery()],
            }))?)
            .await?;

        let services = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let frame = peer_recv.recv().await.expect("discovery stream stays open");
                let message =
                    decode_frame::<DiscoveryMessage>(&frame).expect("outbound frame decodes");
                if let DiscoveryMessage::Services(services) = message {
                    return services;
                }
            }
        })
        .await
        .expect("service response is sent");

        assert_eq!(services.node_id, local_secret.public());
        assert_eq!(services.summaries.len(), 1);
        assert_eq!(
            services.summaries[0].service_id,
            ZakuraServiceId::discovery()
        );
        let summary = services.summaries[0]
            .decode_discovery()?
            .expect("discovery summary tag decodes");
        assert_eq!(summary.peer_exchange_slots_free, 3);
        assert!(summary.expected_disconnect_after_exchange);
        assert_eq!(
            summary.max_records_per_response,
            u16::try_from(MAX_DISCOVERY_RECORDS_PER_RESPONSE)
                .expect("record response cap fits in u16")
        );

        Ok(())
    }

    #[tokio::test]
    async fn get_services_returns_local_first_party_block_sync_summary(
    ) -> Result<(), crate::BoxError> {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let local_secret = NodeSecretKey::from_bytes(&[24u8; 32]);
        let discovery_handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: local_secret.clone(),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery(), ZakuraServiceId::block_sync()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let (header_sync, _header_actions, header_task) = spawn_test_header_sync()?;
        let (tip_tx, tip_rx) = watch::channel((block::Height(5), block::Hash([5; 32])));
        drop(tip_tx);
        let (block_sync, _block_actions, block_task) =
            spawn_block_sync_reactor(BlockSyncStartup::new(
                BlockSyncFrontiers {
                    finalized_height: block::Height(0),
                    verified_block_tip: block::Height(5),
                    verified_block_hash: block::Hash([5; 32]),
                },
                (block::Height(5), block::Hash([5; 32])),
                tip_rx,
                ZakuraBlockSyncConfig::default()
                    .decoded_block_pool(10_000)
                    .expect("fixture decoded allowance fits"),
                ZakuraBlockSyncConfig::default(),
            ))
            .expect("valid block-sync config");
        let service = DiscoveryService::with_sync_services(
            discovery_handle,
            header_sync,
            Some(block_sync.clone()),
            ZakuraTrace::default(),
            DEFAULT_ZAKURA_MAX_CONNECTIONS,
        );
        let peer_node_id = NodeSecretKey::from_bytes(&[25u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        let (peer_send, service_recv) = framed_channel(8);
        let (service_send, mut peer_recv) = framed_channel(8);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);

        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY | ZAKURA_CAP_BLOCK_SYNC_REGULATED,
            streams,
            CancellationToken::new(),
        ));

        peer_send
            .send(encode_frame(&DiscoveryMessage::GetServices(GetServices {
                wanted_services: vec![ZakuraServiceId::block_sync()],
            }))?)
            .await?;

        let services = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let frame = peer_recv.recv().await.expect("discovery stream stays open");
                let message =
                    decode_frame::<DiscoveryMessage>(&frame).expect("outbound frame decodes");
                if let DiscoveryMessage::Services(services) = message {
                    return services;
                }
            }
        })
        .await
        .expect("service response is sent");

        assert_eq!(services.node_id, local_secret.public());
        assert_eq!(services.summaries.len(), 1);
        assert_eq!(
            services.summaries[0].service_id,
            ZakuraServiceId::block_sync()
        );
        let summary = services.summaries[0]
            .decode_block_sync()?
            .expect("block summary tag decodes");
        assert_eq!(summary.servable_low, block::Height(0));
        assert_eq!(summary.servable_high, block::Height(5));
        assert_eq!(summary.tip_hash, block::Hash([5; 32]));
        assert_eq!(
            usize::from(summary.free_slots),
            block_sync.peer_snapshot().inbound_slots_free
        );
        assert_eq!(
            summary.max_blocks_per_response,
            ZakuraBlockSyncConfig::default().advertised_max_blocks_per_response()
        );
        assert_eq!(summary.max_response_bytes, MAX_BS_RESPONSE_BYTES);

        // A query for header sync alone leaves the block-sync summary out.
        peer_send
            .send(encode_frame(&DiscoveryMessage::GetServices(GetServices {
                wanted_services: vec![ZakuraServiceId::header_sync()],
            }))?)
            .await?;
        let services = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let frame = peer_recv.recv().await.expect("discovery stream stays open");
                let message =
                    decode_frame::<DiscoveryMessage>(&frame).expect("outbound frame decodes");
                if let DiscoveryMessage::Services(services) = message {
                    return services;
                }
            }
        })
        .await
        .expect("service response is sent");
        let service_ids: Vec<_> = services
            .summaries
            .iter()
            .map(|summary| summary.service_id.clone())
            .collect();
        assert_eq!(service_ids, vec![ZakuraServiceId::header_sync()]);

        header_task.abort();
        block_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn inbound_services_updates_first_party_live_summary_cache() -> Result<(), crate::BoxError>
    {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let local_secret = NodeSecretKey::from_bytes(&[23u8; 32]);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: local_secret,
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let service = DiscoveryService::new(handle.clone());
        let peer_node_id = NodeSecretKey::from_bytes(&[24u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        connected_tx.send_replace(vec![peer_id.clone()]);

        let (peer_send, service_recv) = framed_channel(8);
        let (service_send, mut peer_recv) = framed_channel(8);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);

        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY,
            streams,
            CancellationToken::new(),
        ));

        let summary = DiscoveryServiceSummary {
            peer_exchange_slots_free: 7,
            max_records_per_response: 11,
            expected_disconnect_after_exchange: false,
        };
        // A Services answers the node's own GetServices.
        tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(frame) = peer_recv.recv().await {
                if matches!(
                    decode_frame::<DiscoveryMessage>(&frame),
                    Ok(DiscoveryMessage::GetServices(_))
                ) {
                    return;
                }
            }
            panic!("the node sends GetServices before its stream ends");
        })
        .await?;
        peer_send
            .send(encode_frame(&DiscoveryMessage::Services(Services {
                node_id: peer_node_id,
                expires_at_unix_secs: u64::MAX,
                summaries: vec![ServiceSummaryEnvelope::discovery(&summary)?],
            }))?)
            .await?;

        let cached = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(cached) = handle.live_service_summaries(peer_node_id).await {
                    if !cached.is_empty() {
                        return cached;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("inbound SERVICES is imported");

        assert_eq!(cached.len(), 1);
        assert_eq!(
            cached[0].summary,
            ZakuraLiveServiceSummary::Discovery(summary)
        );

        Ok(())
    }

    #[tokio::test]
    async fn discovery_only_short_lived_exchange_closes_connection_and_backs_off(
    ) -> Result<(), crate::BoxError> {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let local_secret = NodeSecretKey::from_bytes(&[40u8; 32]);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: local_secret,
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let service = DiscoveryService::new(handle.clone());
        let peer_secret = NodeSecretKey::from_bytes(&[41u8; 32]);
        let peer_node_id = peer_secret.public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        connected_tx.send_replace(vec![peer_id.clone()]);

        let connection_cancel = CancellationToken::new();
        let (peer_send, service_recv) = framed_channel(16);
        let (service_send, mut peer_recv) = framed_channel(16);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);

        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY,
            streams,
            connection_cancel.clone(),
        ));

        wait_for_discovery_inbound_peers(&handle, 1).await;
        complete_peer_side_discovery_exchange(&peer_send, &mut peer_recv, &peer_secret, &handshake)
            .await?;
        tokio::time::timeout(Duration::from_secs(2), connection_cancel.cancelled())
            .await
            .expect("discovery-only exchange closes the shared connection");
        wait_for_discovery_inbound_peers(&handle, 0).await;

        connected_tx.send_replace(Vec::new());
        assert!(handle
            .dial_candidates(&[ZakuraServiceId::discovery()], &[])
            .await
            .is_empty());

        Ok(())
    }

    /// A discovery-only peer that asked for services and completed the local
    /// exchange, while the local stream holds `capacity` unread frames.
    /// Returns the connection's token and the peer's halves of the stream.
    async fn discovery_only_peer_awaiting_services(
        seed: u8,
        capacity: usize,
    ) -> Result<(CancellationToken, FramedSend, FramedRecv), crate::BoxError> {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: NodeSecretKey::from_bytes(&[seed; 32]),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let service = DiscoveryService::new(handle.clone());
        let peer_secret = NodeSecretKey::from_bytes(&[seed + 1; 32]);
        let peer_id = ZakuraPeerId::new(peer_secret.public().as_bytes().to_vec())?;
        connected_tx.send_replace(vec![peer_id.clone()]);

        let connection_cancel = CancellationToken::new();
        let (peer_send, service_recv) = framed_channel(16);
        let (service_send, peer_recv) = framed_channel(capacity);
        let local_output = service_send.clone();
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);
        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY,
            streams,
            connection_cancel.clone(),
        ));
        wait_for_discovery_inbound_peers(&handle, 1).await;
        // The local Hello, GetPeers and GetServices fill the stream, so the
        // answer below cannot be written until the peer reads.
        tokio::time::timeout(Duration::from_secs(2), async {
            while local_output.capacity() > 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the local exchange fills the stream");
        peer_send
            .send(discovery_frame(DiscoveryMessage::GetServices(
                GetServices {
                    wanted_services: Vec::new(),
                },
            ))?)
            .await?;
        answer_discovery_exchange(&peer_send, &peer_secret, &handshake).await?;
        Ok((connection_cancel, peer_send, peer_recv))
    }

    /// A completed exchange closes its discovery-only connection only after
    /// the peer's admitted `Services` answer is written.
    #[tokio::test]
    async fn discovery_only_close_waits_for_an_unwritten_answer() -> Result<(), crate::BoxError> {
        let (connection_cancel, _peer_send, mut peer_recv) =
            discovery_only_peer_awaiting_services(44, 3).await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(200), connection_cancel.cancelled())
                .await
                .is_err(),
            "the connection stays open while the Services answer is unwritten"
        );
        loop {
            let frame = tokio::time::timeout(Duration::from_secs(2), peer_recv.recv())
                .await?
                .expect("the local side writes its answer before closing");
            if matches!(
                decode_frame::<DiscoveryMessage>(&frame)?,
                DiscoveryMessage::Services(_)
            ) {
                break;
            }
        }
        tokio::time::timeout(Duration::from_secs(2), connection_cancel.cancelled())
            .await
            .expect("the written answer lets the connection close");
        Ok(())
    }

    /// A peer that never reads cannot hold a discovery-only connection open
    /// past the answer deadline.
    #[tokio::test(start_paused = true)]
    async fn discovery_only_close_waits_at_most_the_answer_deadline() -> Result<(), crate::BoxError>
    {
        let (connection_cancel, _peer_send, _peer_recv) =
            discovery_only_peer_awaiting_services(46, 3).await?;
        let waiting = tokio::time::Instant::now();
        tokio::time::timeout(
            DISCOVERY_ANSWER_WRITE_DEADLINE + Duration::from_secs(1),
            connection_cancel.cancelled(),
        )
        .await
        .expect("the deadline closes the connection");
        assert!(waiting.elapsed() >= DISCOVERY_ANSWER_WRITE_DEADLINE - Duration::from_millis(100));
        Ok(())
    }

    #[tokio::test]
    async fn discovery_short_lived_exchange_keeps_header_sync_connection(
    ) -> Result<(), crate::BoxError> {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let local_secret = NodeSecretKey::from_bytes(&[42u8; 32]);
        let discovery_handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: local_secret,
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let (header_sync, _header_actions, header_task) = spawn_test_header_sync()?;
        let service = DiscoveryService::with_sync_services(
            discovery_handle.clone(),
            header_sync.clone(),
            None,
            ZakuraTrace::default(),
            DEFAULT_ZAKURA_MAX_CONNECTIONS,
        );
        let peer_secret = NodeSecretKey::from_bytes(&[43u8; 32]);
        let peer_node_id = peer_secret.public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        connected_tx.send_replace(vec![peer_id.clone()]);

        let (header_send, _header_recv) = framed_channel(8);
        let header_session = PeerSession::from_parts_with_direction(
            peer_id.clone(),
            ServicePeerDirection::Inbound,
            header_send,
            CancellationToken::new(),
        );
        header_sync
            .send(Event::PeerConnected(header_session))
            .await?;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if header_sync
                    .candidate_state()
                    .admitted_node_ids
                    .contains(&peer_node_id)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("header sync admits the peer");

        let connection_cancel = CancellationToken::new();
        let (peer_send, service_recv) = framed_channel(16);
        let (service_send, mut peer_recv) = framed_channel(16);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);
        // The owning service claims the connection directly.
        // This test owner represents the production header-sync service.
        service.set_connection_owners(vec![Arc::new(TestConnectionOwner {
            peer: peer_id.clone(),
            conn_id: 0,
        })]);
        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY | ZAKURA_CAP_HEADER_SYNC,
            streams,
            connection_cancel.clone(),
        ));

        wait_for_discovery_inbound_peers(&discovery_handle, 1).await;
        complete_peer_side_discovery_exchange(&peer_send, &mut peer_recv, &peer_secret, &handshake)
            .await?;
        // Another service owns the connection.
        // Discovery therefore refreshes the exchange instead of releasing its session.
        wait_for_discovery_inbound_peers(&discovery_handle, 1).await;
        assert_eq!(header_sync.peer_snapshot().inbound_peers, 1);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), connection_cancel.cancelled())
                .await
                .is_err(),
            "discovery releases only its own session while header sync owns the connection"
        );

        header_task.abort();
        Ok(())
    }

    /// Queries written to `send` until `cancel`, on an in-process channel.
    fn test_queries(send: &FramedSend, cancel: &CancellationToken) -> Queries {
        let (_unused, recv) = framed_channel(1);
        Queries::new(
            reservation_pool(),
            WriterFence::new(CancellationToken::new(), CloseCause::new()),
            &recv,
            send.clone(),
            cancel.clone(),
        )
    }

    /// The writer of inbound session 1 on connection 2, writing to `send`, and
    /// the session states that hold its open record.
    fn test_peer_io(
        handle: ZakuraDiscoveryHandle,
        peer_id: ZakuraPeerId,
        send: FramedSend,
    ) -> (PeerIO, Arc<StdMutex<DiscoverySessionStates>>) {
        let cancel = CancellationToken::new();
        let connection_cancel = CancellationToken::new();
        let close_cause = CloseCause::new();
        let peer_node_id = node_id_from_peer_id(&peer_id).expect("test peer ids are node ids");
        let states = Arc::new(StdMutex::new(DiscoverySessionStates::default()));
        let slot = SessionSlot {
            states: states.clone(),
            key: DiscoverySessionKey::new(peer_id.clone(), 2, 1),
        };
        assert!(slot.lock().open(&slot.key));
        let serving = ServingCapacity::new(DEFAULT_ZAKURA_MAX_CONNECTIONS).session(
            PeerSampler {
                handle: handle.clone(),
                requester: peer_node_id,
            },
            ServiceReporter {
                handle: handle.clone(),
                header_sync: None,
                block_sync: None,
            },
            &peer_id,
            send.clone(),
            cancel.clone(),
            connection_cancel.clone(),
            close_cause.clone(),
        );
        let io = PeerIO {
            handle,
            queries: test_queries(&send, &cancel),
            session: DiscoveryPeerSession {
                peer_id,
                direction: ServicePeerDirection::Inbound,
                send,
                cancel,
            },
            slot,
            state: SharedPeerState::default(),
            hello: CadenceSender::new(),
            answers: serving.answers_written(),
            connection_owners: Vec::new(),
            peer_node_id,
            connection_cancel,
            close_cause,
        };
        (io, states)
    }

    /// Build a sink bound to one exact stream generation on `conn_id`.
    fn discovery_sink_for_session(
        handle: &ZakuraDiscoveryHandle,
        peer_id: &ZakuraPeerId,
        peer_node_id: NodeId,
        conn_id: ZakuraConnId,
        session_id: u64,
    ) -> (DiscoverySink, FramedRecv) {
        discovery_sink_with_send_capacity(handle, peer_id, peer_node_id, conn_id, session_id, 8)
    }

    /// Build a sink as [`discovery_sink_for_session`] does, whose stream
    /// holds `capacity` unread frames.
    fn discovery_sink_with_send_capacity(
        handle: &ZakuraDiscoveryHandle,
        peer_id: &ZakuraPeerId,
        peer_node_id: NodeId,
        conn_id: ZakuraConnId,
        session_id: u64,
        capacity: usize,
    ) -> (DiscoverySink, FramedRecv) {
        let (send, recv) = framed_channel(capacity);
        let cancel = CancellationToken::new();
        let serving = ServingCapacity::new(DEFAULT_ZAKURA_MAX_CONNECTIONS).session(
            PeerSampler {
                handle: handle.clone(),
                requester: peer_node_id,
            },
            ServiceReporter {
                handle: handle.clone(),
                header_sync: None,
                block_sync: None,
            },
            peer_id,
            send.clone(),
            cancel.clone(),
            CancellationToken::new(),
            CloseCause::new(),
        );
        let sink = DiscoverySink {
            handle: handle.clone(),
            serving,
            queries: test_queries(&send, &cancel),
            peer_node_id,
            session: DiscoveryPeerSession {
                peer_id: peer_id.clone(),
                direction: ServicePeerDirection::Inbound,
                send,
                cancel,
            },
            key: DiscoverySessionKey::new(peer_id.clone(), conn_id, session_id),
            state: SharedPeerState::default(),
            trace: ZakuraTrace::default(),
        };
        (sink, recv)
    }

    /// An authentic expired `Hello` counts as initial progress. The same
    /// record with a bad signature disconnects, although it is also expired.
    #[tokio::test]
    async fn expired_hello_counts_as_progress_unless_its_signature_fails(
    ) -> Result<(), crate::BoxError> {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: NodeSecretKey::from_bytes(&[48u8; 32]),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let peer_secret = NodeSecretKey::from_bytes(&[49u8; 32]);
        let peer_node_id = peer_secret.public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        let (sink, _recv) = discovery_sink_for_session(&handle, &peer_id, peer_node_id, 0, 1);

        let mut body = signed_discovery_record(&peer_secret, &handshake)?.body;
        body.expires_at_unix_secs = current_test_unix_secs() - 3_600;
        let expired = ZakuraNodeRecord::sign(body, &peer_secret)?;
        let mut forged = expired.clone();
        forged.body.sequence += 1;

        assert!(matches!(
            sink.handle_hello(forged).await,
            Err(SinkReject::Protocol(_))
        ));
        assert!(!sink.state.progress().hello);

        sink.handle_hello(expired)
            .await
            .expect("an authentic expired hello is dropped, not rejected");
        assert!(sink.state.progress().hello);
        Ok(())
    }

    /// Each `GetPeers` gets exactly one `Peers` from the row's `Serve`. While
    /// answers wait, a second open request is served and a third breaks the
    /// margin.
    #[tokio::test]
    async fn get_peers_is_served_once_per_request_within_the_serve_margin(
    ) -> Result<(), crate::BoxError> {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: NodeSecretKey::from_bytes(&[52u8; 32]),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let peer_node_id = NodeSecretKey::from_bytes(&[53u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        assert_eq!(
            handle
                .admit_peer_session(
                    DiscoverySessionKey::new(peer_id.clone(), 0, 1),
                    ServicePeerDirection::Inbound
                )
                .await,
            ServiceAdmissionDecision::Admit
        );
        let (sink, mut recv) = discovery_sink_for_session(&handle, &peer_id, peer_node_id, 0, 1);
        let query = || GetPeers {
            limit: 1,
            wanted_services: Vec::new(),
            exclude_node_ids: Vec::new(),
        };
        async fn next_peers(recv: &mut FramedRecv) -> Result<(), crate::BoxError> {
            let frame = tokio::time::timeout(Duration::from_secs(2), recv.recv())
                .await?
                .expect("the session stays open");
            assert!(matches!(
                decode_frame::<DiscoveryMessage>(&frame)?,
                DiscoveryMessage::Peers { records } if records.is_empty()
            ));
            Ok(())
        }

        sink.handle_message(
            DiscoveryMessage::GetPeers(GetPeers {
                limit: 1,
                wanted_services: Vec::new(),
                exclude_node_ids: Vec::new(),
            }),
            None,
        )
        .await?;
        next_peers(&mut recv).await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), recv.recv())
                .await
                .is_err(),
            "one request gets one response"
        );

        // Holding the state lock keeps sampling, so both requests stay open.
        let state = handle.lock_for_test().await;
        sink.serving.get_peers.admit(query())?;
        sink.serving.get_peers.admit(query())?;
        assert!(sink.serving.get_peers.admit(query()).is_err());
        drop(state);
        next_peers(&mut recv).await?;
        next_peers(&mut recv).await?;
        Ok(())
    }

    /// Each `GetServices` gets exactly one `Services` from the row's `Serve`,
    /// filtered to the wanted services. While generation waits, the session
    /// admits the row's open request and one more, and refuses a third.
    #[tokio::test]
    async fn get_services_is_served_once_per_request_within_the_serve_margin(
    ) -> Result<(), crate::BoxError> {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: NodeSecretKey::from_bytes(&[60u8; 32]),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let peer_node_id = NodeSecretKey::from_bytes(&[61u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        assert_eq!(
            handle
                .admit_peer_session(
                    DiscoverySessionKey::new(peer_id.clone(), 0, 1),
                    ServicePeerDirection::Inbound
                )
                .await,
            ServiceAdmissionDecision::Admit
        );
        let (sink, mut recv) = discovery_sink_for_session(&handle, &peer_id, peer_node_id, 0, 1);
        let query = |wanted_services| GetServices { wanted_services };
        async fn next_services(
            recv: &mut FramedRecv,
        ) -> Result<Vec<ZakuraServiceId>, crate::BoxError> {
            let frame = tokio::time::timeout(Duration::from_secs(2), recv.recv())
                .await?
                .expect("the session stays open");
            let DiscoveryMessage::Services(services) = decode_frame::<DiscoveryMessage>(&frame)?
            else {
                panic!("a GetServices is answered with Services");
            };
            Ok(services
                .summaries
                .into_iter()
                .map(|summary| summary.service_id)
                .collect())
        }

        // The sink has no header-sync or block-sync service, so an empty
        // wanted list and a list naming discovery both get the discovery
        // summary, and a list naming only header sync gets no summary.
        for (wanted, expected) in [
            (Vec::new(), vec![ZakuraServiceId::discovery()]),
            (
                vec![ZakuraServiceId::discovery(), ZakuraServiceId::block_sync()],
                vec![ZakuraServiceId::discovery()],
            ),
            (vec![ZakuraServiceId::header_sync()], Vec::new()),
        ] {
            sink.handle_message(DiscoveryMessage::GetServices(query(wanted)), None)
                .await?;
            assert_eq!(next_services(&mut recv).await?, expected);
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), recv.recv())
                .await
                .is_err(),
            "one request gets one response"
        );

        // Holding the state lock keeps the discovery summary waiting, so both
        // requests stay open.
        let state = handle.lock_for_test().await;
        sink.serving.get_services.admit(query(Vec::new()))?;
        sink.serving.get_services.admit(query(Vec::new()))?;
        assert!(sink.serving.get_services.admit(query(Vec::new())).is_err());
        drop(state);
        next_services(&mut recv).await?;
        next_services(&mut recv).await?;
        Ok(())
    }

    /// The reader claims each `Peers` before decoding it: a `Peers` with no
    /// unanswered `GetPeers` disconnects, and so does one with more records
    /// than its `GetPeers` asked for.
    #[tokio::test]
    async fn peers_needs_its_get_peers_and_stays_within_its_limit() -> Result<(), crate::BoxError> {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: NodeSecretKey::from_bytes(&[56u8; 32]),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let peer_node_id = NodeSecretKey::from_bytes(&[57u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        assert_eq!(
            handle
                .admit_peer_session(
                    DiscoverySessionKey::new(peer_id.clone(), 0, 1),
                    ServicePeerDirection::Inbound
                )
                .await,
            ServiceAdmissionDecision::Admit
        );
        let empty_peers = || {
            discovery_frame(DiscoveryMessage::Peers {
                records: Vec::new(),
            })
        };

        let (sink, _sent) = discovery_sink_for_session(&handle, &peer_id, peer_node_id, 0, 1);
        let (to_sink, recv) = framed_channel(4);
        let reader = tokio::spawn(async move {
            let mut pipe = discovery_pipe(peer_id);
            run_discovery_pipe(&mut pipe, recv, sink).await
        });
        to_sink.send(empty_peers()?).await?;
        let rejected = tokio::time::timeout(Duration::from_secs(2), reader).await??;
        assert!(matches!(rejected, Err(SinkReject::Protocol(_))));

        // The answer to a `GetPeers` with limit 1 carries two records.
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        let (sink, mut sent) = discovery_sink_for_session(&handle, &peer_id, peer_node_id, 0, 1);
        sink.queries
            .send_get_peers(GetPeers {
                limit: 1,
                wanted_services: Vec::new(),
                exclude_node_ids: Vec::new(),
            })
            .await
            .map_err(|unsent| format!("{unsent:?}"))?;
        sent.recv().await.expect("the query is written");
        let records = vec![
            signed_discovery_record(&NodeSecretKey::from_bytes(&[58u8; 32]), &handshake)?,
            signed_discovery_record(&NodeSecretKey::from_bytes(&[59u8; 32]), &handshake)?,
        ];
        let (to_sink, recv) = framed_channel(4);
        let reader = tokio::spawn(async move {
            let mut pipe = discovery_pipe(peer_id);
            run_discovery_pipe(&mut pipe, recv, sink).await
        });
        to_sink
            .send(discovery_frame(DiscoveryMessage::Peers { records })?)
            .await?;
        let rejected = tokio::time::timeout(Duration::from_secs(2), reader).await??;
        assert!(matches!(rejected, Err(SinkReject::Protocol(_))));
        for seed in [58u8, 59] {
            let node_id = NodeSecretKey::from_bytes(&[seed; 32]).public();
            assert_eq!(handle.record_for(node_id).await, None, "nothing imports");
        }
        Ok(())
    }

    /// A violation writes one `discovery_violation` row that names the
    /// rejecting stage and the frame's message type.
    #[tokio::test]
    async fn violation_is_traced_with_its_stage() -> Result<(), crate::BoxError> {
        use crate::zakura::{
            discovery::wire::PEERS,
            testkit::{TraceCapture, TraceValue},
            trace::{discovery_trace as d_trace, peer_label, DISCOVERY_TABLE},
        };

        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: NodeSecretKey::from_bytes(&[60u8; 32]),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let peer_node_id = NodeSecretKey::from_bytes(&[61u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        handle
            .admit_peer_session(
                DiscoverySessionKey::new(peer_id.clone(), 0, 1),
                ServicePeerDirection::Inbound,
            )
            .await;
        let mut capture = TraceCapture::for_test("discovery_violation_is_traced")?;
        let (mut sink, _sent) = discovery_sink_for_session(&handle, &peer_id, peer_node_id, 0, 1);
        sink.trace = ZakuraTrace::new(capture.tracer(), "violation-test");

        // A `Peers` that answers no `GetPeers` fails its claim.
        let (to_sink, recv) = framed_channel(4);
        let reader = tokio::spawn({
            let peer_id = peer_id.clone();
            async move {
                let mut pipe = discovery_pipe(peer_id);
                run_discovery_pipe(&mut pipe, recv, sink).await
            }
        });
        to_sink
            .send(discovery_frame(DiscoveryMessage::Peers {
                records: Vec::new(),
            })?)
            .await?;
        let rejected = tokio::time::timeout(Duration::from_secs(2), reader).await??;
        assert!(matches!(rejected, Err(SinkReject::Protocol(_))));

        capture.flush().await;
        let reader = capture.reader()?;
        reader.table(DISCOVERY_TABLE.table()).assert_row(
            d_trace::DISCOVERY_VIOLATION,
            &[
                (d_trace::PEER, TraceValue::Str(&peer_label(&peer_id))),
                (d_trace::SESSION_ID, TraceValue::U64(1)),
                (
                    d_trace::MESSAGE_TYPE,
                    TraceValue::U64(u64::from(PEERS.message_type)),
                ),
                (d_trace::STAGE, TraceValue::Str("claim")),
            ],
        );
        capture.finish().await?;
        Ok(())
    }

    /// The reader claims each `Services` before decoding it: a `Services`
    /// with no unanswered `GetServices` disconnects, and so does a second
    /// answer. A summary outside a nonempty wanted list disconnects.
    #[tokio::test]
    async fn services_needs_its_get_services_and_stays_within_its_wanted_list(
    ) -> Result<(), crate::BoxError> {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: NodeSecretKey::from_bytes(&[62u8; 32]),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let peer_node_id = NodeSecretKey::from_bytes(&[63u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        assert_eq!(
            handle
                .admit_peer_session(
                    DiscoverySessionKey::new(peer_id.clone(), 0, 1),
                    ServicePeerDirection::Inbound
                )
                .await,
            ServiceAdmissionDecision::Admit
        );
        let discovery_summary = ServiceSummaryEnvelope::discovery(&DiscoveryServiceSummary {
            peer_exchange_slots_free: 1,
            max_records_per_response: 32,
            expected_disconnect_after_exchange: false,
        })?;
        let services = |summaries| {
            DiscoveryMessage::Services(Services {
                node_id: peer_node_id,
                expires_at_unix_secs: u64::MAX,
                summaries,
            })
        };
        let spawn_reader = |sink: DiscoverySink| {
            let peer_id = peer_id.clone();
            let (to_sink, recv) = framed_channel(4);
            let reader = tokio::spawn(async move {
                let mut pipe = discovery_pipe(peer_id);
                run_discovery_pipe(&mut pipe, recv, sink).await
            });
            (to_sink, reader)
        };

        // Unsolicited.
        let (sink, _sent) = discovery_sink_for_session(&handle, &peer_id, peer_node_id, 0, 1);
        let (to_sink, reader) = spawn_reader(sink);
        to_sink.send(discovery_frame(services(Vec::new()))?).await?;
        let rejected = tokio::time::timeout(Duration::from_secs(2), reader).await??;
        assert!(matches!(rejected, Err(SinkReject::Protocol(_))));

        // Answered once, then a second answer.
        let (sink, mut sent) = discovery_sink_for_session(&handle, &peer_id, peer_node_id, 0, 1);
        sink.queries
            .send_get_services(GetServices {
                wanted_services: vec![ZakuraServiceId::discovery()],
            })
            .await
            .map_err(|unsent| format!("{unsent:?}"))?;
        sent.recv().await.expect("the query is written");
        let (to_sink, reader) = spawn_reader(sink);
        to_sink
            .send(discovery_frame(services(vec![discovery_summary.clone()]))?)
            .await?;
        to_sink.send(discovery_frame(services(Vec::new()))?).await?;
        let rejected = tokio::time::timeout(Duration::from_secs(2), reader).await??;
        assert!(matches!(rejected, Err(SinkReject::Protocol(_))));

        // A summary the wanted list does not name.
        let (sink, mut sent) = discovery_sink_for_session(&handle, &peer_id, peer_node_id, 0, 1);
        sink.queries
            .send_get_services(GetServices {
                wanted_services: vec![ZakuraServiceId::header_sync()],
            })
            .await
            .map_err(|unsent| format!("{unsent:?}"))?;
        sent.recv().await.expect("the query is written");
        let (to_sink, reader) = spawn_reader(sink);
        to_sink
            .send(discovery_frame(services(vec![discovery_summary]))?)
            .await?;
        let rejected = tokio::time::timeout(Duration::from_secs(2), reader).await??;
        assert!(matches!(rejected, Err(SinkReject::Protocol(_))));
        Ok(())
    }

    /// A peer that stops reading holds at most two unsent answers. Its next
    /// requests wait, and a request past the commitment margin soon breaks
    /// it.
    #[tokio::test]
    async fn a_peer_that_stops_reading_breaks_the_margin_after_two_unsent_answers(
    ) -> Result<(), crate::BoxError> {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: NodeSecretKey::from_bytes(&[54u8; 32]),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let peer_node_id = NodeSecretKey::from_bytes(&[55u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        // The stream holds one unread answer; the peer never reads it.
        let (sink, _recv) =
            discovery_sink_with_send_capacity(&handle, &peer_id, peer_node_id, 0, 1, 1);
        let query = || GetPeers {
            limit: 1,
            wanted_services: Vec::new(),
            exclude_node_ids: Vec::new(),
        };

        // At most: the stream takes one answer, two wait unsent, and two
        // more requests wait within the margin of one open request.
        let mut admitted = 0;
        while sink.serving.get_peers.admit(query()).is_ok() {
            admitted += 1;
            assert!(admitted <= 5, "a peer that stops reading is never cut off");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    }

    /// A cancelled session's reader drops a `Hello` that waits for a
    /// verification slot. It leaves the slot queue and never imports the
    /// record.
    #[tokio::test]
    async fn cancelled_session_drops_a_hello_waiting_for_a_verification_slot(
    ) -> Result<(), crate::BoxError> {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: NodeSecretKey::from_bytes(&[50u8; 32]),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let peer_secret = NodeSecretKey::from_bytes(&[51u8; 32]);
        let peer_node_id = peer_secret.public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        assert_eq!(
            handle
                .admit_peer_session(
                    DiscoverySessionKey::new(peer_id.clone(), 0, 1),
                    ServicePeerDirection::Inbound
                )
                .await,
            ServiceAdmissionDecision::Admit,
        );
        let (sink, _sink_recv) = discovery_sink_for_session(&handle, &peer_id, peer_node_id, 0, 1);
        let cancel = sink.session.cancel_token();
        let (peer_send, recv) = framed_channel(8);
        peer_send
            .send(discovery_frame(DiscoveryMessage::Hello {
                record: signed_discovery_record(&peer_secret, &handshake)?,
            })?)
            .await?;

        let held = handle.hold_verify_slots();
        let mut pipe = discovery_pipe(peer_id.clone());
        let reader = tokio::spawn(async move { run_discovery_pipe(&mut pipe, recv, sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!reader.is_finished(), "the hello waits for a slot");

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), reader)
            .await
            .expect("the cancelled reader stops while the slots stay held")?
            .expect("a cancelled session ends without a rejection");
        let slots = held.len();
        drop(held);
        assert_eq!(
            handle.hold_verify_slots().len(),
            slots,
            "the cancelled hello took no verification slot"
        );
        assert_eq!(handle.record_for(peer_node_id).await, None);
        Ok(())
    }

    /// The `DiscoverySink` session guard must drop messages from a superseded stream generation.
    ///
    /// A replacement discovery stream re-admits the peer under a higher `session_id`.
    /// The previous sink can still hold a frame that it decoded before replacement.
    /// The guard prevents that sink from changing the peer record or rejecting the connection.
    ///
    /// This test drives the guard directly because the previous fixture uses a retired API.
    #[tokio::test]
    async fn superseded_discovery_session_cannot_act_on_the_connection(
    ) -> Result<(), crate::BoxError> {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let local_secret = NodeSecretKey::from_bytes(&[44u8; 32]);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: local_secret,
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;

        let peer_secret = NodeSecretKey::from_bytes(&[45u8; 32]);
        let peer_node_id = peer_secret.public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        let conn_id: ZakuraConnId = 0;

        // Admit session 1 and then replace it with session 2 on the same connection.
        assert_eq!(
            handle
                .admit_peer_session(
                    DiscoverySessionKey::new(peer_id.clone(), conn_id, 1),
                    ServicePeerDirection::Inbound
                )
                .await,
            ServiceAdmissionDecision::Admit,
        );
        let (stale_sink, _stale_recv) =
            discovery_sink_for_session(&handle, &peer_id, peer_node_id, conn_id, 1);
        assert_eq!(
            handle
                .admit_peer_session(
                    DiscoverySessionKey::new(peer_id.clone(), conn_id, 2),
                    ServicePeerDirection::Inbound
                )
                .await,
            ServiceAdmissionDecision::Admit,
        );
        let (live_sink, _live_recv) =
            discovery_sink_for_session(&handle, &peer_id, peer_node_id, conn_id, 2);

        // A hello from another peer violates the protocol.
        // The sink exposes that rejection directly.
        let imposter_secret = NodeSecretKey::from_bytes(&[46u8; 32]);
        let imposter_record = signed_discovery_record(&imposter_secret, &handshake)?;

        // Positive control: the current session does reject it. Without this the
        // test would pass for the wrong reason if `handle_message` stopped
        // rejecting at all.
        assert!(
            live_sink
                .handle_message(
                    DiscoveryMessage::Hello {
                        record: imposter_record.clone(),
                    },
                    None,
                )
                .await
                .is_err(),
            "the current session must still reject a hello authored by another node",
        );

        assert!(
            stale_sink
                .handle_message(
                    DiscoveryMessage::Hello {
                        record: imposter_record,
                    },
                    None,
                )
                .await
                .is_ok(),
            "a superseded session must not reject on the connection's behalf: the guard is what \
             stops a retired stream from tearing down the live one",
        );

        Ok(())
    }

    /// A session-state map holding one active record for `session`.
    fn states_with_active(session: &DiscoverySessionKey) -> DiscoverySessionStates {
        let mut states = DiscoverySessionStates::default();
        assert!(states.open(session));
        states
    }

    fn record_for(
        states: &DiscoverySessionStates,
        peer: &ZakuraPeerId,
        conn_id: ZakuraConnId,
    ) -> DiscoverySessionRecord {
        *states
            .records
            .get(&(peer.clone(), conn_id))
            .expect("test session record exists")
    }

    #[test]
    fn teardown_before_retirement_does_not_recreate_discovery_session_state() {
        let peer = ZakuraPeerId::new(vec![71; 32]).expect("test peer id is within bounds");
        let conn_id = 9;
        let session = DiscoverySessionKey::new(peer.clone(), conn_id, 1);
        let mut states = states_with_active(&session);

        states.remove_connection(&peer, conn_id);

        assert!(!states.retire(&session));
        assert!(states.records.is_empty());
    }

    #[test]
    fn stale_exchange_cannot_retire_a_newer_session_on_the_same_connection() {
        let peer = ZakuraPeerId::new(vec![72; 32]).expect("test peer id is within bounds");
        let conn_id = 9;
        let first = DiscoverySessionKey::new(peer.clone(), conn_id, 1);
        let mut states = states_with_active(&first);

        // A reopened stream admits session 2 on the same connection while
        // session 1's exchange is still finishing.
        let second = DiscoverySessionKey::new(peer.clone(), conn_id, 2);
        assert!(states.open(&second));

        assert!(
            !states.retire(&first),
            "session 1's finished exchange must not retire session 2's slot"
        );
        assert_eq!(
            record_for(&states, &peer, conn_id).state,
            DiscoverySessionState::Active,
        );

        assert!(states.retire(&second));
        assert_eq!(
            record_for(&states, &peer, conn_id).state,
            DiscoverySessionState::Retired,
        );
    }

    #[test]
    fn failed_sessions_retire_discovery_after_bounded_attempts() {
        let peer = ZakuraPeerId::new(vec![73; 32]).expect("test peer id is within bounds");
        let conn_id = 9;
        let session = DiscoverySessionKey::new(peer.clone(), conn_id, 1);
        let mut states = states_with_active(&session);

        for attempt in 1..MAX_DISCOVERY_SESSION_FAILURES {
            assert_eq!(
                states.record_failure(&session),
                SessionFailure::Counted,
                "attempt {attempt} must not yet retire the record"
            );
            let record = record_for(&states, &peer, conn_id);
            assert_eq!(record.state, DiscoverySessionState::Active);
            assert_eq!(record.failed_attempts, attempt);
        }

        assert_eq!(
            states.record_failure(&session),
            SessionFailure::Retired {
                attempts: MAX_DISCOVERY_SESSION_FAILURES
            },
            "the failure bound must retire the record"
        );
        assert_eq!(
            record_for(&states, &peer, conn_id).state,
            DiscoverySessionState::Retired,
        );
        assert!(
            !states.open(&DiscoverySessionKey::new(peer.clone(), conn_id, 2)),
            "a retired record must not be resurrected by a new stream"
        );
    }

    #[test]
    fn successful_exchange_resets_discovery_failure_count() {
        let peer = ZakuraPeerId::new(vec![74; 32]).expect("test peer id is within bounds");
        let conn_id = 9;
        let session = DiscoverySessionKey::new(peer.clone(), conn_id, 1);
        let mut states = states_with_active(&session);

        for _ in 1..MAX_DISCOVERY_SESSION_FAILURES {
            assert_eq!(states.record_failure(&session), SessionFailure::Counted);
        }
        states.record_success(&session);
        assert_eq!(record_for(&states, &peer, conn_id).failed_attempts, 0);

        for _ in 1..MAX_DISCOVERY_SESSION_FAILURES {
            assert_eq!(
                states.record_failure(&session),
                SessionFailure::Counted,
                "the reset counter must grant a full fresh failure budget"
            );
        }
        assert_eq!(
            record_for(&states, &peer, conn_id).state,
            DiscoverySessionState::Active,
        );
    }

    #[test]
    fn stale_session_failure_cannot_charge_a_newer_session() {
        let peer = ZakuraPeerId::new(vec![75; 32]).expect("test peer id is within bounds");
        let conn_id = 9;
        let mut states = states_with_active(&DiscoverySessionKey::new(peer.clone(), conn_id, 2));

        assert_eq!(
            states.record_failure(&DiscoverySessionKey::new(peer.clone(), conn_id, 1)),
            SessionFailure::Stale,
            "session 1's failure must not charge session 2's slot"
        );
        assert_eq!(record_for(&states, &peer, conn_id).failed_attempts, 0);
    }

    /// One step of a session-record operation sequence.
    #[derive(Clone, Debug)]
    enum SessionOp {
        Open(usize, ZakuraConnId, u64),
        Fail(usize, ZakuraConnId, u64),
        Succeed(usize, ZakuraConnId, u64),
        Retire(usize, ZakuraConnId, u64),
        RemoveConnection(usize, ZakuraConnId),
    }

    fn session_op() -> impl proptest::strategy::Strategy<Value = SessionOp> {
        use proptest::prelude::*;
        let key = || (0usize..2, 0u64..2, 0u64..4);
        prop_oneof![
            2 => key().prop_map(|(peer, conn, session)| SessionOp::Open(peer, conn, session)),
            3 => key().prop_map(|(peer, conn, session)| SessionOp::Fail(peer, conn, session)),
            1 => key().prop_map(|(peer, conn, session)| SessionOp::Succeed(peer, conn, session)),
            1 => key().prop_map(|(peer, conn, session)| SessionOp::Retire(peer, conn, session)),
            1 => (0usize..2, 0u64..2).prop_map(|(peer, conn)| SessionOp::RemoveConnection(peer, conn)),
        ]
    }

    /// The model's view of one connection's record.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct SessionModel {
        session_id: u64,
        retired: bool,
        failures: u32,
    }

    proptest::proptest! {
        /// After every step the session records match an independent model: a
        /// session acts only on the record it owns, a retired record never
        /// reopens before its connection ends, and the failure count survives
        /// reopens until a success resets it or the bound retires the record.
        #[test]
        fn session_records_match_independent_model(
            ops in proptest::collection::vec(session_op(), 1..96),
        ) {
            let peers = [
                ZakuraPeerId::new(vec![80; 32]).expect("test peer id is within bounds"),
                ZakuraPeerId::new(vec![81; 32]).expect("test peer id is within bounds"),
            ];
            let mut states = DiscoverySessionStates::default();
            let mut model: HashMap<(usize, ZakuraConnId), SessionModel> = HashMap::new();
            for op in ops {
                match op {
                    SessionOp::Open(peer, conn_id, session_id) => {
                        let key = DiscoverySessionKey::new(peers[peer].clone(), conn_id, session_id);
                        let previous = model.get(&(peer, conn_id)).copied();
                        let expected = !previous.is_some_and(|record| record.retired);
                        proptest::prop_assert_eq!(states.open(&key), expected);
                        if expected {
                            model.insert((peer, conn_id), SessionModel {
                                session_id,
                                retired: false,
                                failures: previous.map_or(0, |record| record.failures),
                            });
                        }
                    }
                    SessionOp::Fail(peer, conn_id, session_id) => {
                        let key = DiscoverySessionKey::new(peers[peer].clone(), conn_id, session_id);
                        let expected = match model.get_mut(&(peer, conn_id)) {
                            Some(record) if record.session_id == session_id && !record.retired => {
                                record.failures += 1;
                                if record.failures >= MAX_DISCOVERY_SESSION_FAILURES {
                                    record.retired = true;
                                    SessionFailure::Retired { attempts: record.failures }
                                } else {
                                    SessionFailure::Counted
                                }
                            }
                            _ => SessionFailure::Stale,
                        };
                        proptest::prop_assert_eq!(states.record_failure(&key), expected);
                    }
                    SessionOp::Succeed(peer, conn_id, session_id) => {
                        let key = DiscoverySessionKey::new(peers[peer].clone(), conn_id, session_id);
                        states.record_success(&key);
                        if let Some(record) = model
                            .get_mut(&(peer, conn_id))
                            .filter(|record| record.session_id == session_id)
                        {
                            record.failures = 0;
                        }
                    }
                    SessionOp::Retire(peer, conn_id, session_id) => {
                        let key = DiscoverySessionKey::new(peers[peer].clone(), conn_id, session_id);
                        let owned = model
                            .get_mut(&(peer, conn_id))
                            .filter(|record| record.session_id == session_id);
                        let expected = owned.is_some();
                        if let Some(record) = owned {
                            record.retired = true;
                        }
                        proptest::prop_assert_eq!(states.retire(&key), expected);
                    }
                    SessionOp::RemoveConnection(peer, conn_id) => {
                        states.remove_connection(&peers[peer], conn_id);
                        model.remove(&(peer, conn_id));
                    }
                }

                proptest::prop_assert_eq!(states.records.len(), model.len());
                for ((peer, conn_id), expected) in &model {
                    let record = record_for(&states, &peers[*peer], *conn_id);
                    proptest::prop_assert_eq!(record.session_id, expected.session_id);
                    proptest::prop_assert_eq!(
                        record.state == DiscoverySessionState::Retired,
                        expected.retired
                    );
                    proptest::prop_assert_eq!(record.failed_attempts, expected.failures);
                    proptest::prop_assert_eq!(
                        states.is_retired(&peers[*peer], *conn_id),
                        expected.retired
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn add_peer_refuses_a_retired_discovery_record() -> Result<(), crate::BoxError> {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: NodeSecretKey::from_bytes(&[48u8; 32]),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let service = DiscoveryService::new(handle.clone());
        let peer_secret = NodeSecretKey::from_bytes(&[49u8; 32]);
        let peer_id = ZakuraPeerId::new(peer_secret.public().as_bytes().to_vec())?;
        connected_tx.send_replace(vec![peer_id.clone()]);

        service
            .session_states
            .lock()
            .expect("discovery session-state mutex is never poisoned")
            .records
            .insert(
                (peer_id.clone(), 0),
                DiscoverySessionRecord {
                    session_id: 7,
                    state: DiscoverySessionState::Retired,
                    failed_attempts: MAX_DISCOVERY_SESSION_FAILURES,
                },
            );

        let connection_cancel = CancellationToken::new();
        let (_peer_send, service_recv) = framed_channel(16);
        let (service_send, _peer_recv) = framed_channel(16);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);
        service.add_peer(Peer::new(
            peer_id.clone(),
            None,
            ZAKURA_CAP_DISCOVERY,
            streams,
            connection_cancel.clone(),
        ));

        // Asserted synchronously (current-thread runtime): a refused add_peer
        // must not have replaced the retired record or spawned an exchange.
        let record = record_for(
            &service
                .session_states
                .lock()
                .expect("discovery session-state mutex is never poisoned"),
            &peer_id,
            0,
        );
        assert_eq!(
            record.session_id, 7,
            "a retired record must not be resurrected by a new stream"
        );
        assert_eq!(record.state, DiscoverySessionState::Retired);
        assert!(matches!(
            service.session_demand(
                0,
                &peer_id,
                ZAKURA_CAP_DISCOVERY,
                ServicePeerDirection::Inbound,
            ),
            SessionDemand::Retire,
        ));
        Ok(())
    }

    #[tokio::test]
    async fn add_peer_preserves_failure_count_across_reopens() -> Result<(), crate::BoxError> {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: NodeSecretKey::from_bytes(&[50u8; 32]),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let service = DiscoveryService::new(handle.clone());
        let peer_secret = NodeSecretKey::from_bytes(&[51u8; 32]);
        let peer_id = ZakuraPeerId::new(peer_secret.public().as_bytes().to_vec())?;
        connected_tx.send_replace(vec![peer_id.clone()]);

        service
            .session_states
            .lock()
            .expect("discovery session-state mutex is never poisoned")
            .records
            .insert(
                (peer_id.clone(), 0),
                DiscoverySessionRecord {
                    session_id: 7,
                    state: DiscoverySessionState::Active,
                    failed_attempts: MAX_DISCOVERY_SESSION_FAILURES - 1,
                },
            );

        let connection_cancel = CancellationToken::new();
        let (_peer_send, service_recv) = framed_channel(16);
        let (service_send, _peer_recv) = framed_channel(16);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);
        service.add_peer(Peer::new(
            peer_id.clone(),
            None,
            ZAKURA_CAP_DISCOVERY,
            streams,
            connection_cancel.clone(),
        ));

        let record = record_for(
            &service
                .session_states
                .lock()
                .expect("discovery session-state mutex is never poisoned"),
            &peer_id,
            0,
        );
        assert_ne!(
            record.session_id, 7,
            "a live reopen must install the new stream session"
        );
        assert_eq!(record.state, DiscoverySessionState::Active);
        assert_eq!(
            record.failed_attempts,
            MAX_DISCOVERY_SESSION_FAILURES - 1,
            "consecutive failures must stay counted across reopens"
        );
        Ok(())
    }

    /// Returns the authors of the `Hello` frames queued on `recv`, draining
    /// every queued frame.
    fn drain_hello_authors(recv: &mut FramedRecv) -> Vec<NodeId> {
        use futures::FutureExt;

        let mut authors = Vec::new();
        while let Some(Some(frame)) = recv.recv().now_or_never() {
            if let DiscoveryMessage::Hello { record } =
                decode_frame::<DiscoveryMessage>(&frame).expect("the source encodes valid frames")
            {
                authors.push(record.body.node_id);
            }
        }
        authors
    }

    /// The source sends `Hello` once per send interval: an exchange inside the
    /// interval sends none, and the first exchange after it sends the current
    /// self-record.
    #[tokio::test(start_paused = true)]
    async fn exchange_resends_hello_once_its_interval_passes() -> Result<(), crate::BoxError> {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let local_secret = NodeSecretKey::from_bytes(&[52u8; 32]);
        let local_node_id = local_secret.public();
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: local_secret,
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let peer_node_id = NodeSecretKey::from_bytes(&[53u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        assert_eq!(
            handle
                .admit_peer_session(
                    DiscoverySessionKey::new(peer_id.clone(), 2, 1),
                    ServicePeerDirection::Inbound
                )
                .await,
            ServiceAdmissionDecision::Admit
        );
        let (send, mut recv) = framed_channel(8);
        let (mut source, _states) = test_peer_io(handle.clone(), peer_id, send);

        source.exchange().await.expect("the session is current");
        assert_eq!(drain_hello_authors(&mut recv), vec![local_node_id]);

        source.exchange().await.expect("the session is current");
        assert_eq!(
            drain_hello_authors(&mut recv),
            Vec::new(),
            "an exchange inside the interval sends no Hello"
        );

        tokio::time::advance(SEND_INTERVAL).await;
        source.exchange().await.expect("the session is current");
        assert_eq!(drain_hello_authors(&mut recv), vec![local_node_id]);
        Ok(())
    }

    #[tokio::test]
    async fn stale_discovery_source_cannot_send_after_stream_replacement(
    ) -> Result<(), crate::BoxError> {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: NodeSecretKey::from_bytes(&[38u8; 32]),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let peer_node_id = NodeSecretKey::from_bytes(&[39u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        assert_eq!(
            handle
                .admit_peer_session(
                    DiscoverySessionKey::new(peer_id.clone(), 2, 1),
                    ServicePeerDirection::Inbound
                )
                .await,
            ServiceAdmissionDecision::Admit
        );

        let (send, mut recv) = framed_channel(4);
        let (mut source, _states) = test_peer_io(handle.clone(), peer_id.clone(), send);
        assert_eq!(
            handle
                .admit_peer_session(
                    DiscoverySessionKey::new(peer_id, 2, 2),
                    ServicePeerDirection::Inbound
                )
                .await,
            ServiceAdmissionDecision::Admit
        );

        assert_eq!(source.exchange().await, Err(()));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), recv.recv())
                .await
                .is_err(),
            "a replaced discovery stream must not emit another exchange"
        );
        Ok(())
    }

    #[tokio::test]
    async fn discovery_source_replaced_during_settle_does_not_complete_exchange(
    ) -> Result<(), crate::BoxError> {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: NodeSecretKey::from_bytes(&[48u8; 32]),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let peer_node_id = NodeSecretKey::from_bytes(&[49u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        assert_eq!(
            handle
                .admit_peer_session(
                    DiscoverySessionKey::new(peer_id.clone(), 2, 1),
                    ServicePeerDirection::Inbound
                )
                .await,
            ServiceAdmissionDecision::Admit
        );

        let (send, mut recv) = framed_channel(4);
        let (source, states) = test_peer_io(handle.clone(), peer_id.clone(), send);
        let (state, queries) = (source.state.clone(), source.queries.clone());
        let connection_cancel = source.connection_cancel.clone();
        let source_task = tokio::spawn(source.run());
        for _ in 0..3 {
            recv.recv()
                .await
                .expect("initial discovery request is sent");
        }

        assert_eq!(
            handle
                .admit_peer_session(
                    DiscoverySessionKey::new(peer_id.clone(), 2, 2),
                    ServicePeerDirection::Inbound
                )
                .await,
            ServiceAdmissionDecision::Admit
        );
        state.record_hello();
        for message_type in [MSG_DISCOVERY_PEERS, MSG_DISCOVERY_SERVICES] {
            let answer = queries
                .claim(u16::from(message_type), 0)?
                .expect("the frame answers a live query");
            state.record_answer(answer.admitted());
        }

        source_task.await?;
        let record = record_for(
            &states
                .lock()
                .expect("discovery session-state mutex is never poisoned"),
            &peer_id,
            2,
        );
        assert_eq!(
            record.failed_attempts, 1,
            "the replaced session's exchange ends uncompleted and is charged"
        );
        assert_eq!(record.state, DiscoverySessionState::Active);
        assert!(!connection_cancel.is_cancelled());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn silent_discovery_source_does_not_complete_exchange() -> Result<(), crate::BoxError> {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: NodeSecretKey::from_bytes(&[50u8; 32]),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let peer_node_id = NodeSecretKey::from_bytes(&[51u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        assert_eq!(
            handle
                .admit_peer_session(
                    DiscoverySessionKey::new(peer_id.clone(), 2, 1),
                    ServicePeerDirection::Inbound
                )
                .await,
            ServiceAdmissionDecision::Admit
        );

        let (send, _recv) = framed_channel(4);
        let (source, states) = test_peer_io(handle, peer_id.clone(), send);
        let close_cause = source.close_cause.clone();
        source.run().await;

        assert_eq!(
            close_cause.get_or("none"),
            "discovery_exchange_timed_out",
            "a peer that sends no discovery responses never completes the initial exchange"
        );
        assert_eq!(
            record_for(
                &states
                    .lock()
                    .expect("discovery session-state mutex is never poisoned"),
                &peer_id,
                2,
            )
            .state,
            DiscoverySessionState::Retired
        );
        Ok(())
    }

    /// Build a peer whose stream carries a service's negotiated stream version.
    ///
    /// `Peer::new` defaults every stream to version 0, but `HeaderSyncService`
    /// only admits a stream tagged `ZAKURA_HEADER_SYNC_STREAM_VERSION`. Tests
    /// that hand a real header-sync service a peer must therefore build the
    /// stream with its version rather than through the plain constructor.
    fn versioned_peer(
        peer_id: ZakuraPeerId,
        negotiated: u64,
        kind: u16,
        version: u16,
        stream: (FramedRecv, FramedSend),
        cancel_token: CancellationToken,
    ) -> Peer {
        let (recv, send) = stream;
        Peer::new_with_service_streams(
            0,
            peer_id,
            None,
            negotiated,
            ServicePeerDirection::Inbound,
            HashMap::from([(
                kind,
                crate::zakura::transport::ServiceStream::new(
                    0,
                    version,
                    recv,
                    send,
                    cancel_token.child_token(),
                ),
            )]),
            cancel_token,
            CloseCause::new(),
        )
    }

    /// Waits for the source's next refresh, a `GetPeers`, and checks that it
    /// comes no sooner than the rows' send interval after `since`.
    async fn wait_for_next_discovery_refresh(
        peer_recv: &mut FramedRecv,
        since: tokio::time::Instant,
    ) -> Result<(), crate::BoxError> {
        tokio::time::timeout(2 * SEND_INTERVAL, async {
            loop {
                let frame = peer_recv
                    .recv()
                    .await
                    .expect("discovery refresh stream remains open");
                if let DiscoveryMessage::GetPeers(_) = decode_frame::<DiscoveryMessage>(&frame)? {
                    assert!(
                        since.elapsed() >= SEND_INTERVAL,
                        "a refresh waits for the send interval"
                    );
                    return Ok(());
                }
            }
        })
        .await?
    }

    #[tokio::test]
    async fn discovery_exchange_closes_connection_when_block_sync_is_not_admitted(
    ) -> Result<(), crate::BoxError> {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let local_secret = NodeSecretKey::from_bytes(&[40u8; 32]);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: local_secret,
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let service = DiscoveryService::new(handle.clone());
        let peer_secret = NodeSecretKey::from_bytes(&[41u8; 32]);
        let peer_node_id = peer_secret.public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        connected_tx.send_replace(vec![peer_id.clone()]);

        let connection_cancel = CancellationToken::new();
        let (peer_send, service_recv) = framed_channel(16);
        let (service_send, mut peer_recv) = framed_channel(16);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);

        service.add_peer(Peer::new(
            peer_id.clone(),
            None,
            ZAKURA_CAP_DISCOVERY | ZAKURA_CAP_BLOCK_SYNC_REGULATED,
            streams,
            connection_cancel.clone(),
        ));

        wait_for_discovery_inbound_peers(&handle, 1).await;
        complete_peer_side_discovery_exchange(&peer_send, &mut peer_recv, &peer_secret, &handshake)
            .await?;
        tokio::time::timeout(Duration::from_secs(2), connection_cancel.cancelled())
            .await
            .expect("unadmitted block sync does not keep the shared connection alive");
        // A finished discovery-only exchange retires the session so the handler
        // cannot reopen the stream while the connection tears down.
        assert!(matches!(
            service.session_demand(
                0,
                &peer_id,
                ZAKURA_CAP_DISCOVERY | ZAKURA_CAP_BLOCK_SYNC_REGULATED,
                ServicePeerDirection::Inbound,
            ),
            SessionDemand::Retire,
        ));
        wait_for_discovery_inbound_peers(&handle, 0).await;

        connected_tx.send_replace(Vec::new());
        assert!(handle
            .dial_candidates(&[ZakuraServiceId::discovery()], &[])
            .await
            .is_empty());

        Ok(())
    }

    /// A peer whose answers miss the initial-exchange timeout is not charged a
    /// failed session: the answers can wait behind local verification and the
    /// peer's serving queue. A discovery-only connection still closes.
    #[tokio::test(start_paused = true)]
    async fn timed_out_initial_exchange_closes_without_a_failure_charge(
    ) -> Result<(), crate::BoxError> {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: NodeSecretKey::from_bytes(&[54u8; 32]),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let service = DiscoveryService::new(handle.clone());
        let peer_id = ZakuraPeerId::new(
            NodeSecretKey::from_bytes(&[55u8; 32])
                .public()
                .as_bytes()
                .to_vec(),
        )?;
        connected_tx.send_replace(vec![peer_id.clone()]);

        let connection_cancel = CancellationToken::new();
        let (_peer_send, service_recv) = framed_channel(16);
        let (service_send, _peer_recv) = framed_channel(16);
        service.add_peer(Peer::new(
            peer_id.clone(),
            None,
            ZAKURA_CAP_DISCOVERY,
            HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]),
            connection_cancel.clone(),
        ));

        tokio::time::timeout(
            2 * DISCOVERY_INITIAL_EXCHANGE_TIMEOUT,
            connection_cancel.cancelled(),
        )
        .await
        .expect("a discovery-only connection closes after the timeout");
        let record = service
            .session_states
            .lock()
            .expect("discovery session-state mutex is never poisoned")
            .records
            .get(&(peer_id, 0))
            .copied()
            .expect("the session record outlives the exchange");
        assert_eq!(record.failed_attempts, 0);
        assert_eq!(record.state, DiscoverySessionState::Retired);
        Ok(())
    }

    /// A timed-out session on a shared connection is settled when it ends: it
    /// is charged if the exchange never completed, and resets the count if
    /// the exchange completed late.
    #[tokio::test(start_paused = true)]
    async fn timed_out_shared_session_is_settled_when_it_ends() -> Result<(), crate::BoxError> {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: NodeSecretKey::from_bytes(&[56u8; 32]),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let service = DiscoveryService::new(handle.clone());
        let peer_secret = NodeSecretKey::from_bytes(&[57u8; 32]);
        let peer_id = ZakuraPeerId::new(peer_secret.public().as_bytes().to_vec())?;
        service.set_connection_owners(vec![Arc::new(TestConnectionOwner {
            peer: peer_id.clone(),
            conn_id: 0,
        })]);
        connected_tx.send_replace(vec![peer_id.clone()]);
        let connection_cancel = CancellationToken::new();
        // The source sees the ended stream at its next send, a send interval
        // later.
        async fn wait_for_session_end(handle: &ZakuraDiscoveryHandle) {
            tokio::time::timeout(2 * SEND_INTERVAL, async {
                while handle.peer_snapshot().inbound_peers != 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("the session ends once its stream ends");
        }
        let failed_attempts = || {
            service
                .session_states
                .lock()
                .expect("discovery session-state mutex is never poisoned")
                .records
                .get(&(peer_id.clone(), 0))
                .map(|record| record.failed_attempts)
        };

        // The peer stays silent past the timeout, then ends the stream.
        let (peer_send, service_recv) = framed_channel(16);
        let (service_send, peer_recv) = framed_channel(16);
        service.add_peer(Peer::new(
            peer_id.clone(),
            None,
            ZAKURA_CAP_DISCOVERY | ZAKURA_CAP_LEGACY_GOSSIP,
            HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]),
            connection_cancel.clone(),
        ));
        wait_for_discovery_inbound_peers(&handle, 1).await;
        tokio::time::sleep(2 * DISCOVERY_INITIAL_EXCHANGE_TIMEOUT).await;
        assert_eq!(failed_attempts(), Some(0), "a slow exchange is not charged");
        drop((peer_send, peer_recv));
        wait_for_session_end(&handle).await;
        assert_eq!(failed_attempts(), Some(1));

        // The peer answers after the timeout, then ends the stream.
        let (peer_send, service_recv) = framed_channel(16);
        let (service_send, mut peer_recv) = framed_channel(16);
        service.add_peer(Peer::new(
            peer_id.clone(),
            None,
            ZAKURA_CAP_DISCOVERY | ZAKURA_CAP_LEGACY_GOSSIP,
            HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]),
            connection_cancel.clone(),
        ));
        wait_for_discovery_inbound_peers(&handle, 1).await;
        tokio::time::sleep(2 * DISCOVERY_INITIAL_EXCHANGE_TIMEOUT).await;
        complete_peer_side_discovery_exchange(&peer_send, &mut peer_recv, &peer_secret, &handshake)
            .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop((peer_send, peer_recv));
        wait_for_session_end(&handle).await;
        assert_eq!(failed_attempts(), Some(0));
        assert!(!connection_cancel.is_cancelled());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn admitted_legacy_gossip_keeps_discovery_connection_alive() -> Result<(), crate::BoxError>
    {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: NodeSecretKey::from_bytes(&[46u8; 32]),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let service = DiscoveryService::new(handle.clone());
        let peer_secret = NodeSecretKey::from_bytes(&[47u8; 32]);
        let peer_node_id = peer_secret.public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        service.set_connection_owners(vec![Arc::new(TestConnectionOwner {
            peer: peer_id.clone(),
            conn_id: 0,
        })]);
        connected_tx.send_replace(vec![peer_id.clone()]);

        let connection_cancel = CancellationToken::new();
        let (peer_send, service_recv) = framed_channel(16);
        let (service_send, mut peer_recv) = framed_channel(16);
        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY | ZAKURA_CAP_LEGACY_GOSSIP,
            HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]),
            connection_cancel.clone(),
        ));

        wait_for_discovery_inbound_peers(&handle, 1).await;
        let exchanged = tokio::time::Instant::now();
        complete_peer_side_discovery_exchange(&peer_send, &mut peer_recv, &peer_secret, &handshake)
            .await?;
        wait_for_next_discovery_refresh(&mut peer_recv, exchanged).await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), connection_cancel.cancelled())
                .await
                .is_err(),
            "an admitted legacy-gossip service owns the shared connection"
        );
        assert_eq!(handle.peer_snapshot().inbound_peers, 1);

        connection_cancel.cancel();
        wait_for_discovery_inbound_peers(&handle, 0).await;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn admitted_block_sync_keeps_discovery_connection_alive() -> Result<(), crate::BoxError> {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: NodeSecretKey::from_bytes(&[44u8; 32]),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery(), ZakuraServiceId::block_sync()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let service = DiscoveryService::new(handle.clone());
        let (block_sync, _block_events) =
            BlockSyncService::new_for_test(ZakuraBlockSyncConfig::default());
        let block_sync = Arc::new(block_sync);
        service.set_connection_owners(vec![block_sync.clone()]);

        let peer_secret = NodeSecretKey::from_bytes(&[45u8; 32]);
        let peer_node_id = peer_secret.public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        connected_tx.send_replace(vec![peer_id.clone()]);
        let connection_cancel = CancellationToken::new();

        let (_peer_block_send, service_block_recv) = framed_channel(4);
        let (service_block_send, _peer_block_recv) = framed_channel(4);
        block_sync.add_peer(Peer::new(
            peer_id.clone(),
            None,
            ZAKURA_CAP_BLOCK_SYNC_REGULATED,
            HashMap::from([(
                crate::zakura::ZAKURA_STREAM_BLOCK_SYNC,
                (service_block_recv, service_block_send),
            )]),
            connection_cancel.clone(),
        ));

        let (peer_send, service_recv) = framed_channel(16);
        let (service_send, mut peer_recv) = framed_channel(16);
        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY | ZAKURA_CAP_BLOCK_SYNC_REGULATED,
            HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]),
            connection_cancel.clone(),
        ));

        wait_for_discovery_inbound_peers(&handle, 1).await;
        let exchanged = tokio::time::Instant::now();
        complete_peer_side_discovery_exchange(&peer_send, &mut peer_recv, &peer_secret, &handshake)
            .await?;
        wait_for_next_discovery_refresh(&mut peer_recv, exchanged).await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), connection_cancel.cancelled())
                .await
                .is_err(),
            "an admitted block-sync service owns the shared connection"
        );
        assert_eq!(handle.peer_snapshot().inbound_peers, 1);

        connection_cancel.cancel();
        wait_for_discovery_inbound_peers(&handle, 0).await;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn discovery_refreshes_while_header_sync_owns_connection() -> Result<(), crate::BoxError>
    {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let local_secret = NodeSecretKey::from_bytes(&[42u8; 32]);
        let discovery_handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: local_secret,
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let (header_sync, _header_actions, header_task) = spawn_test_header_sync()?;
        let service = DiscoveryService::with_sync_services(
            discovery_handle.clone(),
            header_sync.clone(),
            None,
            ZakuraTrace::default(),
            DEFAULT_ZAKURA_MAX_CONNECTIONS,
        );
        let header_service = Arc::new(HeaderSyncService::new(header_sync.clone()));
        service.set_connection_owners(vec![header_service.clone()]);
        let peer_secret = NodeSecretKey::from_bytes(&[43u8; 32]);
        let peer_node_id = peer_secret.public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        connected_tx.send_replace(vec![peer_id.clone()]);

        let connection_cancel = CancellationToken::new();
        let (_peer_header_send, service_header_recv) = framed_channel(8);
        let (service_header_send, _peer_header_recv) = framed_channel(8);
        header_service.add_peer(versioned_peer(
            peer_id.clone(),
            ZAKURA_CAP_HEADER_SYNC,
            ZAKURA_STREAM_HEADER_SYNC,
            ZAKURA_HEADER_SYNC_STREAM_VERSION,
            (service_header_recv, service_header_send),
            connection_cancel.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if header_sync
                    .candidate_state()
                    .admitted_node_ids
                    .contains(&peer_node_id)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("header sync admits the peer");

        let (peer_send, service_recv) = framed_channel(16);
        let (service_send, mut peer_recv) = framed_channel(16);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);
        service.add_peer(Peer::new(
            peer_id.clone(),
            None,
            ZAKURA_CAP_DISCOVERY | ZAKURA_CAP_HEADER_SYNC,
            streams,
            connection_cancel.clone(),
        ));

        wait_for_discovery_inbound_peers(&discovery_handle, 1).await;
        let exchanged = tokio::time::Instant::now();
        complete_peer_side_discovery_exchange(&peer_send, &mut peer_recv, &peer_secret, &handshake)
            .await?;
        wait_for_next_discovery_refresh(&mut peer_recv, exchanged).await?;
        assert_eq!(discovery_handle.peer_snapshot().inbound_peers, 1);
        assert_eq!(header_sync.peer_snapshot().inbound_peers, 1);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), connection_cancel.cancelled())
                .await
                .is_err(),
            "discovery releases only its own session while header sync owns the connection"
        );
        // The refresh loop still owns this session.
        // The handler must keep the stream eligible.
        assert!(!matches!(
            service.session_demand(
                0,
                &peer_id,
                ZAKURA_CAP_DISCOVERY | ZAKURA_CAP_HEADER_SYNC,
                ServicePeerDirection::Inbound,
            ),
            SessionDemand::Retire,
        ));

        connection_cancel.cancel();
        wait_for_discovery_inbound_peers(&discovery_handle, 0).await;
        header_task.abort();
        Ok(())
    }

    /// A handle with the admitted inbound session 1 on connection `conn_id`,
    /// and the peer's id and secret.
    async fn handle_with_session(
        seed: u8,
        conn_id: ZakuraConnId,
    ) -> Result<(ZakuraDiscoveryHandle, ZakuraPeerId, NodeSecretKey), crate::BoxError> {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: NodeSecretKey::from_bytes(&[seed; 32]),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let peer_secret = NodeSecretKey::from_bytes(&[seed + 1; 32]);
        let peer_id = ZakuraPeerId::new(peer_secret.public().as_bytes().to_vec())?;
        assert_eq!(
            handle
                .admit_peer_session(
                    DiscoverySessionKey::new(peer_id.clone(), conn_id, 1),
                    ServicePeerDirection::Inbound
                )
                .await,
            ServiceAdmissionDecision::Admit
        );
        Ok((handle, peer_id, peer_secret))
    }

    /// Claims the answer to a `GetPeers` that `queries` sends.
    async fn claimed_peers_answer(queries: &Queries) -> ClaimedAnswer {
        let query = GetPeers {
            limit: 1,
            wanted_services: Vec::new(),
            exclude_node_ids: Vec::new(),
        };
        queries
            .send_get_peers(query)
            .await
            .expect("the session's first query is sent");
        queries
            .claim(u16::from(MSG_DISCOVERY_PEERS), 2)
            .expect("an empty answer fits its query")
            .expect("the frame answers a query")
    }

    /// Past the initial timeout, a delivered answer that waits for verification
    /// holds the discovery-only close. Its settlement releases the close.
    #[tokio::test(start_paused = true)]
    async fn timed_out_close_waits_for_a_delivered_answer() -> Result<(), crate::BoxError> {
        let (handle, peer_id, _) = handle_with_session(60, 2).await?;
        let (send, _peer_recv) = framed_channel(16);
        let (io, _states) = test_peer_io(handle, peer_id, send);
        let connection = io.connection_cancel.clone();
        let answer = claimed_peers_answer(&io.queries).await;
        let started = tokio::time::Instant::now();
        let session = tokio::spawn(io.run());

        tokio::time::sleep(
            DISCOVERY_INITIAL_EXCHANGE_TIMEOUT + DISCOVERY_ANSWER_WRITE_DEADLINE / 2,
        )
        .await;
        assert!(
            !connection.is_cancelled(),
            "the timeout does not revoke the delivered answer"
        );

        drop(answer);
        tokio::time::timeout(Duration::from_millis(100), connection.cancelled())
            .await
            .expect("the settled answer lets the connection close");
        assert!(
            started.elapsed()
                < DISCOVERY_INITIAL_EXCHANGE_TIMEOUT + DISCOVERY_ANSWER_WRITE_DEADLINE
        );
        session.await?;
        Ok(())
    }

    /// An answer stuck behind verification cannot hold the connection open past
    /// the answer deadline.
    #[tokio::test(start_paused = true)]
    async fn timed_out_close_waits_at_most_the_deadline_for_a_delivered_answer(
    ) -> Result<(), crate::BoxError> {
        let (handle, peer_id, _) = handle_with_session(62, 2).await?;
        let (send, _peer_recv) = framed_channel(16);
        let (io, _states) = test_peer_io(handle, peer_id, send);
        let connection = io.connection_cancel.clone();
        let _answer = claimed_peers_answer(&io.queries).await;
        let session = tokio::spawn(io.run());

        tokio::time::timeout(
            DISCOVERY_INITIAL_EXCHANGE_TIMEOUT
                + DISCOVERY_ANSWER_WRITE_DEADLINE
                + Duration::from_secs(1),
            connection.cancelled(),
        )
        .await
        .expect("the deadline closes the connection");
        session.await?;
        Ok(())
    }

    /// An owner that the test or the call count turns on.
    #[derive(Debug)]
    struct ChangingOwner {
        owns: AtomicBool,
        calls: AtomicUsize,
        /// The call from which this service owns every connection.
        owns_from_call: usize,
    }

    impl ChangingOwner {
        fn new(owns_from_call: usize) -> Arc<Self> {
            Arc::new(Self {
                owns: AtomicBool::new(false),
                calls: AtomicUsize::new(0),
                owns_from_call,
            })
        }
    }

    impl Service for ChangingOwner {
        fn name(&self) -> &'static str {
            "test-changing-owner"
        }

        fn streams(&self) -> &[Stream] {
            &[]
        }

        fn owns_connection_for_peer(&self, _: &ZakuraPeerId, _: ZakuraConnId) -> bool {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.owns.load(Ordering::SeqCst) || call >= self.owns_from_call
        }

        fn add_peer(&self, _peer: Peer) {}

        fn remove_peer(&self, _peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {}
    }

    /// A service admitted during the answer wait keeps the connection, and the
    /// session refreshes until it is cancelled.
    #[tokio::test(start_paused = true)]
    async fn a_service_admitted_during_the_answer_wait_keeps_the_connection(
    ) -> Result<(), crate::BoxError> {
        let (handle, peer_id, _) = handle_with_session(64, 2).await?;
        let (send, _peer_recv) = framed_channel(16);
        let (mut io, _states) = test_peer_io(handle, peer_id, send);
        let owner = ChangingOwner::new(usize::MAX);
        io.connection_owners = vec![owner.clone()];
        let connection = io.connection_cancel.clone();
        let cancel = io.session.cancel_token();
        let answer = claimed_peers_answer(&io.queries).await;
        let session = tokio::spawn(io.run());

        tokio::time::sleep(
            DISCOVERY_INITIAL_EXCHANGE_TIMEOUT + DISCOVERY_ANSWER_WRITE_DEADLINE / 2,
        )
        .await;
        owner.owns.store(true, Ordering::SeqCst);
        drop(answer);
        tokio::time::sleep(DISCOVERY_ANSWER_WRITE_DEADLINE).await;
        assert!(!connection.is_cancelled(), "the new owner keeps it open");
        assert!(
            !session.is_finished(),
            "the session refreshes for its owner"
        );

        cancel.cancel();
        session.await?;
        assert!(!connection.is_cancelled(), "the session leaves it open");
        Ok(())
    }

    /// A service admitted while the session's removal waits keeps the
    /// connection: the last read of the owners comes right before the cancel.
    #[tokio::test(start_paused = true)]
    async fn a_service_admitted_during_session_removal_keeps_the_connection(
    ) -> Result<(), crate::BoxError> {
        let (handle, peer_id, _) = handle_with_session(66, 2).await?;
        let key = DiscoverySessionKey::new(peer_id.clone(), 2, 1);
        let (send, _peer_recv) = framed_channel(16);
        let (mut io, _states) = test_peer_io(handle.clone(), peer_id, send);
        // The first check after the timeout, the check after the answer wait,
        // and the check that starts `end` find no owner. The fourth follows
        // the session's removal.
        let owner = ChangingOwner::new(4);
        io.connection_owners = vec![owner.clone()];
        let connection = io.connection_cancel.clone();

        tokio::spawn(io.run()).await?;

        assert_eq!(owner.calls.load(Ordering::SeqCst), 4);
        assert!(!handle.is_current_session(&key).await, "the session ended");
        assert!(!connection.is_cancelled(), "the new owner keeps it open");
        Ok(())
    }

    /// A `Peers` answer that outlives its session in verification installs
    /// nothing, and it still settles.
    #[tokio::test]
    async fn a_replaced_session_installs_no_answer_from_verification() -> Result<(), crate::BoxError>
    {
        let (handle, peer_id, peer_secret) = handle_with_session(68, 0).await?;
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let relayed = NodeSecretKey::from_bytes(&[70u8; 32]);
        let (sink, _sent) =
            discovery_sink_for_session(&handle, &peer_id, peer_secret.public(), 0, 1);
        let queries = sink.queries.clone();
        queries
            .send_get_peers(GetPeers {
                limit: 1,
                wanted_services: Vec::new(),
                exclude_node_ids: Vec::new(),
            })
            .await
            .expect("the session's first query is sent");

        let held = handle.hold_verify_slots();
        let (to_sink, recv) = framed_channel(4);
        let reader_peer = peer_id.clone();
        let reader = tokio::spawn(async move {
            let mut pipe = discovery_pipe(reader_peer);
            run_discovery_pipe(&mut pipe, recv, sink).await
        });
        to_sink
            .send(discovery_frame(DiscoveryMessage::Peers {
                records: vec![signed_discovery_record(&relayed, &handshake)?],
            })?)
            .await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(50), queries.answers_settled())
                .await
                .is_err(),
            "the answer waits for a verification slot"
        );

        assert_eq!(
            handle
                .admit_peer_session(
                    DiscoverySessionKey::new(peer_id, 0, 2),
                    ServicePeerDirection::Inbound
                )
                .await,
            ServiceAdmissionDecision::Admit
        );
        drop(held);
        tokio::time::timeout(Duration::from_secs(5), queries.answers_settled())
            .await
            .expect("the answer settles after its verification");
        drop(to_sink);
        tokio::time::timeout(Duration::from_secs(5), reader)
            .await??
            .expect("a replaced session's answer rejects nothing");
        assert_eq!(handle.record_for(relayed.public()).await, None);
        Ok(())
    }
}
