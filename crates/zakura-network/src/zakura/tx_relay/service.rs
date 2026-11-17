//! `tx-relay`'s service and IO: the v2 [`Service`] that takes each session's
//! two streams, and the actor that runs the pure machine against the pool.
//!
//! One actor task owns the machine, the pool handle and every reservation.
//! Each session has a reader per stream, which decodes frames and hands them
//! to the actor, and a writer per stream, which the actor feeds. The actor
//! performs the machine's effects: frames to send, calls to the pool, and
//! connection closes. The pool's answers, verdicts, cancellations and
//! publications come back to it as inputs. So all the rules live in the
//! machine, and this file only moves data.

use std::{fmt, future::Future, pin::Pin, sync::Arc, time::Duration};

use futures::{stream::FuturesUnordered, StreamExt};
use rand_10::Rng;
use zakura_chain::{
    block,
    transaction::{Transaction, UnminedTx, UnminedTxId, VerifiedUnminedTx},
};
use zakura_node_services::mempool::port::{
    ChainTip, Change, ChangeEntry, ChangeSeq, ChangesSince, PoolHandle, PoolReader, PoolSnapshot,
    PoolView, Reservation, ReservationId, Unreserved as PortUnreserved, Verdict as PortVerdict,
};
use zakura_rt::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc, CancellationToken, Mutex, OwnedSemaphorePermit, Semaphore,
    },
    task,
    time::{self, Instant},
};

use super::{
    machine::{
        peer::Hello, CallId, CloseReason, Granted, PeerKey, RelayConfig, RelayEffect, RelayMachine,
        SessionId, SnapshotInfo, Unreserved, Verdict, WaiterId, MAX_SERVES,
    },
    sessions::{LiveSession, TxRelaySessions},
    wire::{
        inv_payload_bound, tx_frame, TxRelayMessage, INV_RATE, MAX_INV_ENTRIES, MAX_TX_BYTES,
        TX_RELAY_DATA_KIND, TX_RELAY_STATE_KIND, TX_RELAY_STREAMS,
    },
    ZakuraTxRelayConfig,
};
use crate::{
    config::UnconditionalPeers,
    zakura::{
        regulation::ByteBucket,
        wire_codec::{decode_frame, encode_frame},
        Frame, FrameGuard, FramedRecv, FramedSend, Peer, Service, SessionOpening, SessionPolicy,
        Stream, ZakuraConnId, ZakuraPeerId,
    },
};

/// Whether a verdict decided at a height may start a cooldown: the node is
/// current and its best tip is still there. `zakurad` supplies it.
pub type MayCool = Arc<dyn Fn(zakura_chain::block::Height) -> bool + Send + Sync>;

/// How long a session waits to look again while its last `Inv` is
/// unwritten or its data writer has no room. A write frees both, and wakes
/// nothing.
const INV_RETRY: Duration = Duration::from_millis(50);

/// Jobs a session's data writer queues: a serve job for every request our
/// `Hello` lets a peer have unanswered, each only an id until the writer
/// builds its body, and room for the one unwritten `Inv`, which goes out
/// only while the jobs' room is free. A conformant peer never fills it.
const DATA_QUEUE: usize = MAX_SERVES + 4;

/// What a session's writer sends.
enum Outgoing {
    /// A frame, with what it holds until the transport has written it.
    Frame(Frame, Option<FrameGuard>),
    /// A `Tx` answering the peer's request for `id`, built only when the
    /// writer reaches it. `request` keeps the request outstanding until the
    /// answer is queued for the transport.
    Serve { id: UnminedTxId, request: Request },
}

/// One of the peer's requests to us, held until its answer is queued: its
/// place among those our `Hello` serves, and the marker that keeps its id
/// counted as outstanding.
struct Request {
    _place: OwnedSemaphorePermit,
    _outstanding: Arc<()>,
}

/// Where one of the peer's outstanding requests stands.
enum Answer {
    /// The machine has yet to answer it.
    Unanswered(OwnedSemaphorePermit),
    /// Its serve job waits on the data writer while the marker lives.
    Serving(zakura_rt::sync::Weak<()>),
}

impl Answer {
    /// Whether the request is still outstanding.
    fn outstanding(&self) -> bool {
        match self {
            Self::Unanswered(_) => true,
            Self::Serving(marker) => marker.strong_count() > 0,
        }
    }
}

/// Where a data writer builds the bodies it serves, and answers `NotFound`
/// for those the pool no longer holds.
struct Bodies {
    pool: PoolReader,
    state: mpsc::Sender<Outgoing>,
    close: Closer,
}

/// Frames a session's state writer queues. Requests, `NotFound` answers and
/// our `Hello` are bounded by the machine's per-peer limits, far below this.
const STATE_QUEUE: usize = 64;

/// Received messages one stream's reader may hand the actor before the
/// actor has handled them. The reader takes its next frame only once the
/// actor drops the last one, and taking a frame is what gives the last
/// frame's receive grant back to the transport, so a message waiting for
/// the actor always sits inside its stream's receive share. A peer that
/// sends faster than the actor handles its frames is held back by the
/// transport.
const QUEUED_PER_STREAM: usize = 1;

/// The `tx-relay` service.
pub struct TxRelayService {
    inputs: mpsc::UnboundedSender<Input>,
    next_session: AtomicU64,
    /// Keys the hash that names peers to the machine, so no peer can choose
    /// an identity that collides with another's.
    peer_key: blake2b_simd::Params,
    /// The connections with a session, which discovery leaves open.
    owned: Owned,
    /// Where data writers build the bodies they serve.
    pool: PoolReader,
    /// The addresses whose sessions are never charged for a body.
    unconditional: UnconditionalPeers,
}

/// The connections tx-relay owns, until the transport removes them. The
/// service adds a session when it takes its streams, and the actor removes
/// it when the session ends.
type Owned = Arc<Mutex<HashMap<(ZakuraPeerId, ZakuraConnId), Ownership>>>;

/// How long a connection whose last session ended stays owned, so the
/// transport can reopen the session before discovery retires the
/// connection: past the transport's 8 second cap on its reopen backoff.
const REOPEN_GAP: Duration = Duration::from_secs(10);

/// One connection's claim: its live sessions, counted since its next
/// session can open before its last one has ended, and the end of the
/// reopen gap after the last one ended.
#[derive(Debug, Default)]
struct Ownership {
    sessions: usize,
    gap_until: Option<Instant>,
}

impl Ownership {
    /// Whether the connection is owned at `now`.
    fn claims(&self, now: Instant) -> bool {
        self.sessions > 0 || self.gap_until.is_some_and(|until| now < until)
    }
}

/// Removes one session of `connection` from `owned` at `now`. The last one
/// leaves a claim for the reopen gap.
fn disown(owned: &Owned, connection: &(ZakuraPeerId, ZakuraConnId), now: Instant) {
    let mut owned = owned.lock().expect("the ownership map is never poisoned");
    if let Some(ownership) = owned.get_mut(connection) {
        ownership.sessions = ownership.sessions.saturating_sub(1);
        if ownership.sessions == 0 {
            ownership.gap_until = Some(now + REOPEN_GAP);
        }
    }
}

impl fmt::Debug for TxRelayService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TxRelayService")
            .finish_non_exhaustive()
    }
}

impl TxRelayService {
    /// Starts the actor on `pool` with `config`'s fan-out and returns the
    /// service to register. A session whose address is in `unconditional`
    /// is never charged for the bodies it supplies. Each session is
    /// published in `sessions` from when the actor holds a view of the pool
    /// until the session ends or the pool stops.
    pub fn spawn(
        pool: PoolHandle,
        may_cool: MayCool,
        config: &ZakuraTxRelayConfig,
        unconditional: UnconditionalPeers,
        sessions: TxRelaySessions,
    ) -> Arc<Self> {
        let (inputs, receiver) = mpsc::unbounded_channel();
        let owned = Owned::default();
        let reader = pool.reader();
        let actor = Actor::new(pool, may_cool, config, sessions, owned.clone());
        task::spawn_named("tx-relay-actor", actor.run(receiver));
        let mut key = [0; 32];
        zakura_rt::rand::rng().fill_bytes(&mut key);
        let mut peer_key = blake2b_simd::Params::new();
        peer_key
            .hash_length(8)
            .key(&key)
            .personal(b"Zakura_TxPeerKey");
        Arc::new(Self {
            inputs,
            next_session: AtomicU64::new(0),
            peer_key,
            owned,
            pool: reader,
            unconditional,
        })
    }

    /// The machine's name for `peer`, the same for every connection to it: a
    /// keyed hash of its identity, so the service keeps no table of peers.
    fn peer_key(&self, peer: &ZakuraPeerId) -> PeerKey {
        let hash = self.peer_key.hash(peer.as_bytes());
        let bytes: [u8; 8] = hash
            .as_bytes()
            .try_into()
            .expect("the hash is configured to 8 bytes");
        PeerKey(u64::from_le_bytes(bytes))
    }

    /// A fresh session key.
    fn session_key(&self) -> u64 {
        self.next_session.fetch_add(1, Ordering::Relaxed)
    }
}

impl Service for TxRelayService {
    fn name(&self) -> &'static str {
        "tx-relay"
    }

    fn streams(&self) -> &[Stream] {
        &TX_RELAY_STREAMS
    }

    /// A session that ends while its connection stays up opens again, as
    /// the plan's reopening rule says. The new session starts clean on the
    /// machine's side and pays the connection's `Inv` bucket, so reopening
    /// buys no allowance.
    fn session_policy(&self) -> SessionPolicy {
        SessionPolicy {
            opening: SessionOpening::InitiatorOnly,
            reopen: true,
        }
    }

    fn owns_connection_for_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) -> bool {
        self.owned
            .lock()
            .expect("the ownership map is never poisoned")
            .get(&(peer.clone(), conn_id))
            .is_some_and(|ownership| ownership.claims(Instant::now()))
    }

    fn add_peer(&self, mut peer: Peer) {
        let (Some((data_recv, data_send)), Some((state_recv, state_send))) = (
            peer.take_stream(TX_RELAY_DATA_KIND),
            peer.take_stream(TX_RELAY_STATE_KIND),
        ) else {
            return;
        };
        let connection = (peer.id.clone(), peer.conn_id);
        {
            let mut owned = self
                .owned
                .lock()
                .expect("the ownership map is never poisoned");
            let ownership = owned.entry(connection.clone()).or_default();
            ownership.sessions += 1;
            ownership.gap_until = None;
        }
        let key = self.session_key();
        let peer_key = self.peer_key(&peer.id);
        let conn_cancel = peer.cancel_token();
        let session_cancel = peer.service_cancel_token();
        let close_cause = peer.close_cause();
        let exempt = peer
            .remote_ip
            .is_some_and(|ip| self.unconditional.contains(ip));
        let close = Closer {
            conn_cancel: conn_cancel.clone(),
            close_cause,
        };
        let state = spawn_writer(state_send, STATE_QUEUE, session_cancel.clone(), None);
        let bodies = Bodies {
            pool: self.pool.clone(),
            state: state.clone(),
            close: close.clone(),
        };
        let data = spawn_writer(data_send, DATA_QUEUE, session_cancel.clone(), Some(bodies));
        if self
            .inputs
            .send(Input::Open {
                key,
                peer: peer_key,
                exempt,
                data,
                state,
                close: close.clone(),
                connection: connection.clone(),
            })
            .is_err()
        {
            disown(&self.owned, &connection, Instant::now());
            return;
        }
        spawn_reader(
            key,
            data_recv,
            self.inputs.clone(),
            session_cancel.clone(),
            close.clone(),
        );
        spawn_reader(key, state_recv, self.inputs.clone(), session_cancel, close);
    }

    fn remove_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) {
        self.owned
            .lock()
            .expect("the ownership map is never poisoned")
            .remove(&(peer.clone(), conn_id));
    }
}

/// Ends a connection with a recorded cause.
#[derive(Clone)]
struct Closer {
    conn_cancel: CancellationToken,
    close_cause: crate::zakura::transport::CloseCause,
}

impl Closer {
    /// Records `reason`, if nothing came first, and closes the connection.
    fn close(&self, reason: &'static str) {
        self.close_cause.record(reason);
        self.conn_cancel.cancel();
    }
}

/// Starts a writer that sends what it is given, in order, and queues at most
/// `depth` of them. A frame with a guard keeps it through the transport
/// write. A data writer builds each served body from `bodies` only when it
/// reaches it, so the transport's output gate, which every body waits for,
/// bounds the bytes queued to the peer, plus the one body being built.
fn spawn_writer(
    send: FramedSend,
    depth: usize,
    cancel: CancellationToken,
    bodies: Option<Bodies>,
) -> mpsc::Sender<Outgoing> {
    let (frames, mut queue) = mpsc::channel::<Outgoing>(depth);
    task::spawn_named("tx-relay-writer", async move {
        loop {
            let frame = zakura_rt::select! {
                () = cancel.cancelled() => return,
                frame = queue.recv() => match frame {
                    Some(frame) => frame,
                    None => return,
                },
            };
            match frame {
                Outgoing::Frame(frame, None) => {
                    if send.send(frame).await.is_err() {
                        return;
                    }
                }
                Outgoing::Frame(frame, Some(guard)) => {
                    let Ok(slot) = send.reserve_guarded().await else {
                        return;
                    };
                    // A closed queue drops the frame and its guard, and the
                    // next reserve returns.
                    slot.send(frame, guard);
                }
                Outgoing::Serve { id, request } => {
                    let Some(bodies) = &bodies else {
                        continue;
                    };
                    if !serve_body(&send, bodies, id).await {
                        return;
                    }
                    drop(request);
                }
            }
        }
    });
    frames
}

/// Sends the body of `id` from the pool's latest snapshot, or `NotFound` on
/// the state stream if the snapshot no longer holds it. Returns `false` once
/// the data stream has closed.
///
/// The check is against the latest snapshot, not the one the request was
/// accepted against, so a job waiting behind a peer that stopped reading
/// keeps no evicted body alive: it holds only the id. An id the pool removed
/// is answered `NotFound`, which is true by then, and frees the request on
/// both sides at once. Answering nothing would leave the request
/// outstanding at the peer, holding one of its slots to us until its own
/// deadline, and the peer could not tell the id was gone. The machine
/// withdraws its advert when it reads the removal.
async fn serve_body(send: &FramedSend, bodies: &Bodies, id: UnminedTxId) -> bool {
    // The body is serialized once, into the frame, and the snapshot is
    // dropped before the frame waits for the transport.
    let frame = bodies
        .pool
        .snapshot()
        .and_then(|snapshot| exact(&snapshot, id).and_then(|tx| tx_frame(id, &tx.transaction)));
    let Some(frame) = frame else {
        let message = TxRelayMessage::NotFound(id);
        let Ok(frame) = encode_frame(&message) else {
            return true;
        };
        match bodies.state.try_send(Outgoing::Frame(frame, None)) {
            Ok(()) => count(&message, "sent"),
            Err(mpsc::error::TrySendError::Full(_)) => {
                bodies.close.close("tx-relay: peer stopped reading");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
        return true;
    };
    if send.send(frame).await.is_err() {
        return false;
    }
    metrics::counter!("zakura.p2p.tx_relay.tx", "direction" => "sent").increment(1);
    true
}

/// Starts a reader that decodes frames and hands them to the actor. At most
/// [`QUEUED_PER_STREAM`] of its messages wait for the actor, each holding a
/// permit until the actor has handled it, and the reader waits for a permit
/// before it takes the next frame. A frame that does not decode closes the
/// connection. When the stream ends, the session ends.
fn spawn_reader(
    key: u64,
    mut recv: FramedRecv,
    inputs: mpsc::UnboundedSender<Input>,
    cancel: CancellationToken,
    close: Closer,
) {
    let queued = Arc::new(Semaphore::new(QUEUED_PER_STREAM));
    task::spawn_named("tx-relay-reader", async move {
        loop {
            // The permit comes first: the next `recv` returns the last
            // frame's receive grant, so it waits until the actor is done
            // with that frame.
            let permit = zakura_rt::select! {
                () = cancel.cancelled() => break,
                permit = queued.clone().acquire_owned() => match permit {
                    Ok(permit) => permit,
                    Err(_) => break,
                },
            };
            let frame = zakura_rt::select! {
                () = cancel.cancelled() => break,
                frame = recv.recv() => match frame {
                    Some(frame) => frame,
                    None => break,
                },
            };
            match decode_frame::<TxRelayMessage>(&frame) {
                Ok(message) => {
                    let input = Input::Message {
                        key,
                        message,
                        _permit: permit,
                    };
                    if inputs.send(input).is_err() {
                        return;
                    }
                }
                Err(_) => {
                    close.close("tx-relay: malformed frame");
                    break;
                }
            }
        }
        let _ = inputs.send(Input::Ended { key });
    });
}

/// What reaches the actor from sessions.
enum Input {
    /// A session's streams are taken.
    Open {
        key: u64,
        peer: PeerKey,
        /// Whether its address is an unconditional peer's.
        exempt: bool,
        data: mpsc::Sender<Outgoing>,
        state: mpsc::Sender<Outgoing>,
        close: Closer,
        connection: (ZakuraPeerId, ZakuraConnId),
    },
    /// A decoded message from a session, holding its stream's queue permit
    /// until the actor drops it, so its reader takes no next frame meanwhile.
    /// See [`QUEUED_PER_STREAM`].
    Message {
        key: u64,
        message: TxRelayMessage,
        _permit: OwnedSemaphorePermit,
    },
    /// One of a session's streams ended.
    Ended { key: u64 },
}

/// What a pool call returns to the actor.
enum PoolResult {
    Reserved {
        call: CallId,
        answer: Result<Reservation, PortUnreserved>,
    },
    Verdict {
        waiter: WaiterId,
        verdict: PortVerdict,
    },
    Cancelled {
        reservation: ReservationId,
    },
    Done,
}

/// A pool call in flight.
type PoolCall = Pin<Box<dyn Future<Output = PoolResult> + Send>>;

/// One session's writers and its closer.
struct SessionIo {
    data: mpsc::Sender<Outgoing>,
    state: mpsc::Sender<Outgoing>,
    close: Closer,
    /// The peer's requests to us not yet answered: at most what our `Hello`
    /// serves, [`MAX_SERVES`]. A request past it breaks the peer's limit.
    /// Every `GetTxs` takes a place, whatever its answer.
    requests: Arc<Semaphore>,
    /// The peer's outstanding requests, by id, until each answer is queued.
    /// A peer may not have one id outstanding twice.
    answering: HashMap<UnminedTxId, Answer>,
    /// Alive while this session's last `Inv` waits for its transport write.
    /// One unwritten `Inv` of at most about 70 KB, with the transport's
    /// 64 KiB of write-ahead, stays within the row's 512 KiB sender
    /// allowance.
    inv_unwritten: zakura_rt::sync::Weak<()>,
    /// When the next `Inv` may go out, if the last try had to wait.
    inv_due: Option<Instant>,
    /// The machine may hold announcements for this session that the pool
    /// shows. Set by an advert, and cleared when the machine has none to
    /// take.
    inv_ready: bool,
    /// The peer and connection the session runs on.
    connection: (ZakuraPeerId, ZakuraConnId),
    /// The session's entry in the live-session record, once the actor holds
    /// a view of the pool. It is removed when this drops.
    live: Option<LiveSession>,
}

/// One connection's `Inv` byte rate, and how many of its sessions are live.
struct ConnectionInv {
    bucket: ByteBucket,
    sessions: usize,
}

/// The task that owns the machine.
struct Actor {
    machine: RelayMachine,
    pool: PoolHandle,
    may_cool: MayCool,
    base: Instant,
    sessions: HashMap<SessionId, SessionIo>,
    by_key: HashMap<u64, SessionId>,
    reservations: HashMap<ReservationId, Reservation>,
    /// Ends the wait on each held reservation's cancellation once the
    /// reservation leaves the actor.
    fetch_waits: HashMap<ReservationId, CancellationToken>,
    calls: FuturesUnordered<PoolCall>,
    snapshot: Option<Arc<PoolSnapshot>>,
    /// Each connection's `Inv` byte rate, which every session on it pays
    /// before it queues a frame, so overlapping or replacing sessions share
    /// one opening allowance.
    inv_buckets: HashMap<(ZakuraPeerId, ZakuraConnId), ConnectionInv>,
    /// The newest change the machine has, or `None` before the first
    /// snapshot.
    cursor: Option<ChangeSeq>,
    /// Whether the core behind the pool is still running.
    pool_open: bool,
    /// The live-session record the legacy adapter reads.
    record: TxRelaySessions,
    owned: Owned,
}

impl Actor {
    /// A new actor on `pool`, fanning out as `config` says.
    fn new(
        pool: PoolHandle,
        may_cool: MayCool,
        config: &ZakuraTxRelayConfig,
        record: TxRelaySessions,
        owned: Owned,
    ) -> Self {
        let mut key = [0; 8];
        zakura_rt::rand::rng().fill_bytes(&mut key);
        let short_id_key = u64::from_le_bytes(key);
        // The pool is off until the first snapshot, which brings its tip.
        let empty = SnapshotInfo {
            active: false,
            tip: ChainTip {
                hash: block::Hash([0; 32]),
                height: block::Height(0),
            },
            sizes: Vec::new(),
        };
        Self {
            machine: RelayMachine::new(
                RelayConfig {
                    fan_out: config.fan_out(),
                    short_id_key,
                },
                empty,
                Vec::new(),
            ),
            pool,
            may_cool,
            base: Instant::now(),
            sessions: HashMap::new(),
            by_key: HashMap::new(),
            reservations: HashMap::new(),
            fetch_waits: HashMap::new(),
            calls: FuturesUnordered::new(),
            snapshot: None,
            inv_buckets: HashMap::default(),
            cursor: None,
            pool_open: true,
            record,
            owned,
        }
    }

    /// The machine's clock: time since the actor started.
    fn now(&self) -> Duration {
        Instant::now().saturating_duration_since(self.base)
    }

    /// Runs until every session and the service are gone.
    async fn run(mut self, mut inputs: mpsc::UnboundedReceiver<Input>) {
        let mut reader = self.pool.reader();
        self.read_pool();
        loop {
            let deadline = self.next_wake();
            zakura_rt::select! {
                input = inputs.recv() => match input {
                    Some(input) => self.input(input),
                    None => return,
                },
                Some(result) = self.calls.next(), if !self.calls.is_empty() => {
                    self.pool_result(result);
                }
                changed = reader.snapshot_changed(), if self.pool_open => {
                    if changed {
                        self.read_pool();
                    } else {
                        // The core is gone. Its receiver stays ready, so
                        // stop watching it, and hand every peer's
                        // transactions back to the legacy adapter.
                        self.pool_open = false;
                        for io in self.sessions.values_mut() {
                            io.live = None;
                        }
                        let now = self.now();
                        self.machine.pool_stopped(now);
                    }
                }
                () = time::sleep_until(deadline) => {
                    let now = self.now();
                    self.machine.tick(now);
                }
            }
            self.apply_effects();
            self.flush_adverts();
            self.publish_live();
        }
    }

    /// Publishes each session that is not yet in the live-session record,
    /// once the actor holds a view of the pool. Until then the legacy
    /// adapter keeps the peer's transactions.
    fn publish_live(&mut self) {
        if !self.pool_open || self.snapshot.is_none() {
            return;
        }
        for (session, io) in &mut self.sessions {
            if io.live.is_none() {
                let (peer, conn_id) = io.connection.clone();
                io.live = Some(self.record.open(peer, conn_id, session.0));
            }
        }
    }

    /// When the actor next has something due: the machine's deadline or the
    /// next `Inv` a session may send, at once unless its last try waited.
    fn next_wake(&self) -> Instant {
        let far = Instant::now() + Duration::from_secs(3600);
        let machine = self
            .machine
            .next_deadline()
            .map(|at| self.base + at)
            .unwrap_or(far);
        let now = Instant::now();
        let advert = self
            .sessions
            .values()
            .filter(|session| session.inv_ready)
            .map(|session| session.inv_due.unwrap_or(now))
            .min()
            .unwrap_or(far);
        machine.min(advert)
    }

    /// Hands one session input to the machine.
    fn input(&mut self, input: Input) {
        let now = self.now();
        match input {
            Input::Open {
                key,
                peer,
                exempt,
                data,
                state,
                close,
                connection,
            } => {
                let session = self.machine.connect(peer, exempt, now);
                let hello = TxRelayMessage::Hello {
                    // `MAX_SERVES` is a small constant that fits a `u16`.
                    serves: u16::try_from(MAX_SERVES).unwrap_or(u16::MAX),
                    max_tx_bytes: MAX_TX_BYTES,
                };
                if let Ok(frame) = encode_frame(&hello) {
                    let _ = state.try_send(Outgoing::Frame(frame, None));
                }
                self.by_key.insert(key, session);
                let opened = Instant::now();
                // A bucket no session uses that has refilled is the same as
                // a new one, so it goes.
                // Widening u32 to u64 is lossless.
                let full = u64::from(INV_RATE.opening_allowance);
                self.inv_buckets.retain(|_, inv| {
                    inv.sessions > 0 || inv.bucket.ready_at(full, opened) != Some(opened)
                });
                self.inv_buckets
                    .entry(connection.clone())
                    .or_insert_with(|| ConnectionInv {
                        bucket: ByteBucket::sender(INV_RATE, opened),
                        sessions: 0,
                    })
                    .sessions += 1;
                self.sessions.insert(
                    session,
                    SessionIo {
                        data,
                        state,
                        close,
                        requests: Arc::new(Semaphore::new(MAX_SERVES)),
                        answering: HashMap::new(),
                        inv_unwritten: zakura_rt::sync::Weak::new(),
                        inv_due: None,
                        inv_ready: false,
                        connection,
                        live: None,
                    },
                );
                self.count_sessions();
            }
            Input::Message { key, message, .. } => {
                let Some(session) = self.by_key.get(&key).copied() else {
                    return;
                };
                self.message(session, message, now);
            }
            Input::Ended { key } => {
                // The first of the two readers to stop ends the session.
                let Some(session) = self.by_key.remove(&key) else {
                    return;
                };
                // Dropping its writers' queues stops them, and its live
                // entry leaves the record.
                if let Some(io) = self.sessions.remove(&session) {
                    disown(&self.owned, &io.connection, Instant::now());
                    if let Some(inv) = self.inv_buckets.get_mut(&io.connection) {
                        inv.sessions = inv.sessions.saturating_sub(1);
                    }
                }
                self.count_sessions();
                self.machine.end(session, now);
            }
        }
    }

    /// Hands one decoded message to the machine.
    fn message(&mut self, session: SessionId, message: TxRelayMessage, now: Duration) {
        count(&message, "received");
        match message {
            TxRelayMessage::Hello {
                serves,
                max_tx_bytes,
            } => self.machine.hello(
                session,
                Hello {
                    serves: usize::from(serves),
                    max_tx_bytes,
                },
                now,
            ),
            TxRelayMessage::Inv(entries) => self.machine.inv(session, &entries, now),
            TxRelayMessage::GetTxs(id) => {
                // The place is taken before the machine picks the answer, so
                // a request answered `NotFound` counts too.
                let Some(io) = self.sessions.get_mut(&session) else {
                    return;
                };
                // At most `MAX_SERVES` entries are outstanding, so this
                // bounds the map.
                io.answering.retain(|_, answer| answer.outstanding());
                if io.answering.contains_key(&id) {
                    io.close.close("tx-relay: id requested twice");
                    return;
                }
                let Ok(request) = io.requests.clone().try_acquire_owned() else {
                    io.close.close("tx-relay: too many requests");
                    return;
                };
                io.answering.insert(id, Answer::Unanswered(request));
                self.machine.get_txs(session, id, now);
            }
            TxRelayMessage::NotFound(id) => self.machine.not_found(session, id, now),
            TxRelayMessage::Tx { id, body } => {
                // A frame's body is at most the data stream's cap.
                let size = u32::try_from(body.len()).unwrap_or(u32::MAX);
                if !self.machine.tx_arrived(session, id, size, now) {
                    return;
                }
                let mut bytes = body.as_slice();
                match Transaction::zcash_deserialize_bounded(&mut bytes) {
                    Ok(transaction) if bytes.is_empty() => {
                        let body = UnminedTx::from(transaction);
                        self.machine.tx(session, id, body, now);
                    }
                    _ => {
                        if let Some(io) = self.sessions.get(&session) {
                            io.close.close("tx-relay: undecodable transaction");
                        }
                    }
                }
            }
        }
    }

    /// Hands one pool result to the machine.
    fn pool_result(&mut self, result: PoolResult) {
        let now = self.now();
        match result {
            PoolResult::Reserved { call, answer } => {
                let answer = match answer {
                    Ok(reservation) => {
                        let id = reservation.id();
                        let token = reservation.cancellation();
                        let cancelled = token.is_cancelled();
                        self.reservations.insert(id, reservation);
                        if !cancelled {
                            let done = CancellationToken::new();
                            self.fetch_waits.insert(id, done.clone());
                            self.calls.push(Box::pin(async move {
                                zakura_rt::select! {
                                    () = token.cancelled() => PoolResult::Cancelled { reservation: id },
                                    () = done.cancelled() => PoolResult::Done,
                                }
                            }));
                        }
                        Ok(Granted {
                            reservation: id,
                            cancelled,
                        })
                    }
                    Err(refused) => Err(unreserved(refused)),
                };
                self.machine.reserve_answer(call, answer, now);
            }
            PoolResult::Verdict { waiter, verdict } => {
                let verdict = verdict_of(verdict);
                if verdict == Verdict::Admitted {
                    metrics::counter!("zakura.p2p.tx_relay.tx.admitted").increment(1);
                }
                if let Verdict::Rejected { tip, .. } = verdict {
                    // The machine starts a cooldown only when the node is
                    // current and its best tip is the verdict's: exactly
                    // what `may_cool` answers.
                    let may_cool = (self.may_cool)(tip);
                    self.machine.set_current(may_cool);
                    if may_cool {
                        self.machine.set_best_tip(tip);
                    }
                }
                self.machine.verdict(waiter, verdict, now);
            }
            PoolResult::Cancelled { reservation } => {
                if self.reservations.contains_key(&reservation) {
                    self.machine.fetch_cancelled(reservation, now);
                }
            }
            PoolResult::Done => {}
        }
    }

    /// Reads the pool's newest publication and hands it to the machine:
    /// each batch after the machine's place, or the whole pool when the
    /// machine has no place in the log.
    fn read_pool(&mut self) {
        let view = match self.cursor {
            Some(cursor) => self.pool.view(cursor),
            None => self.pool.snapshot().map(|snapshot| PoolView {
                snapshot,
                changes: ChangesSince::Resync,
            }),
        };
        let Some(view) = view else {
            return;
        };
        let now = self.now();
        let snapshot = view.snapshot;
        self.cursor = Some(snapshot.last_change);
        match view.changes {
            ChangesSince::Entries(entries) => {
                let mut batches = batches(entries);
                // A publication with no entries still carries the pool's
                // state, such as turning on with nothing in it.
                if batches.is_empty() {
                    batches.push(Vec::new());
                }
                for batch in batches {
                    let sizes = batch
                        .iter()
                        .filter_map(|change| match change {
                            Change::Added(id) => Some(*id),
                            _ => None,
                        })
                        .filter_map(|id| size_in(&snapshot, id).map(|size| (id, size)))
                        .collect();
                    let info = SnapshotInfo {
                        active: snapshot.active,
                        tip: snapshot.tip,
                        sizes,
                    };
                    self.machine.pool_changed(batch, info, now);
                }
            }
            ChangesSince::Resync => {
                let visible = snapshot
                    .transactions
                    .iter()
                    .map(|tx| (tx.transaction.id(), transaction_size(tx.transaction.size())))
                    .collect();
                let info = SnapshotInfo {
                    active: snapshot.active,
                    tip: snapshot.tip,
                    sizes: Vec::new(),
                };
                self.machine.pool_resynced(visible, info, now);
            }
        }
        self.snapshot = Some(snapshot);
    }

    /// Performs every effect the machine produced.
    fn apply_effects(&mut self) {
        loop {
            let effects = self.machine.take_effects();
            if effects.is_empty() {
                break;
            }
            for effect in effects {
                self.effect(effect);
            }
        }
    }

    /// Performs one effect.
    fn effect(&mut self, effect: RelayEffect) {
        match effect {
            RelayEffect::Ask { session, id } => {
                self.send(
                    session,
                    TX_RELAY_STATE_KIND,
                    &TxRelayMessage::GetTxs(id),
                    None,
                );
            }
            // The machine queued the id. `flush_adverts` takes it.
            RelayEffect::Advert { session, .. } => {
                if let Some(io) = self.sessions.get_mut(&session) {
                    io.inv_ready = true;
                }
            }
            RelayEffect::Serve { session, id } => self.serve(session, id),
            RelayEffect::NotFound { session, id } => {
                self.send(
                    session,
                    TX_RELAY_STATE_KIND,
                    &TxRelayMessage::NotFound(id),
                    None,
                );
                // The answer is queued, so the request's place is free.
                if let Some(io) = self.sessions.get_mut(&session) {
                    io.answering.remove(&id);
                }
            }
            RelayEffect::Close { session, reason } => {
                if let Some(io) = self.sessions.get(&session) {
                    io.close.close(close_reason(reason));
                }
            }
            RelayEffect::Reserve { call, id } => {
                let pool = self.pool.clone();
                self.calls.push(Box::pin(async move {
                    let answer = pool.reserve(id).await;
                    PoolResult::Reserved { call, answer }
                }));
            }
            RelayEffect::Submit {
                waiter,
                reservation,
                body,
            } => {
                let Some(reservation) = self.take_reservation(reservation) else {
                    return;
                };
                // The body goes into the core's queue now or is dropped
                // with a `NoRoom` verdict, so the actor never holds a decoded
                // body while it reads the next frame.
                let verdict = self.pool.submit_now(reservation, body);
                self.calls.push(Box::pin(async move {
                    PoolResult::Verdict {
                        waiter,
                        verdict: verdict.await,
                    }
                }));
            }
            RelayEffect::Release { reservation } => {
                // Dropping the reservation gives its slot back.
                self.take_reservation(reservation);
            }
            RelayEffect::FetchFailed { reservation } => {
                let Some(reservation) = self.take_reservation(reservation) else {
                    return;
                };
                let pool = self.pool.clone();
                self.calls.push(Box::pin(async move {
                    pool.fetch_failed(reservation).await;
                    PoolResult::Done
                }));
            }
        }
    }

    /// Takes `id` out of the actor and ends the wait on its cancellation.
    fn take_reservation(&mut self, id: ReservationId) -> Option<Reservation> {
        if let Some(done) = self.fetch_waits.remove(&id) {
            done.cancel();
        }
        self.reservations.remove(&id)
    }

    /// Queues `session`'s request for `id`, which the machine found
    /// servable, for the data writer to build when it reaches it. The job
    /// keeps the request's place until its answer is queued. A conformant
    /// peer counts at least as many requests outstanding as we do, so it
    /// never runs out of places.
    fn serve(&mut self, session: SessionId, id: UnminedTxId) {
        let Some(io) = self.sessions.get_mut(&session) else {
            return;
        };
        let Some(Answer::Unanswered(place)) = io.answering.remove(&id) else {
            return;
        };
        let outstanding = Arc::new(());
        io.answering.insert(
            id,
            Answer::Serving(zakura_rt::sync::downgrade(&outstanding)),
        );
        let request = Request {
            _place: place,
            _outstanding: outstanding,
        };
        match io.data.try_send(Outgoing::Serve { id, request }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                io.close.close("tx-relay: peer stopped reading");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }

    /// Encodes `message` and queues it on `session`'s stream of `kind`, with
    /// `guard` held until it is written.
    fn send(
        &self,
        session: SessionId,
        kind: u16,
        message: &TxRelayMessage,
        guard: Option<FrameGuard>,
    ) {
        let Some(io) = self.sessions.get(&session) else {
            return;
        };
        let Ok(frame) = encode_frame(message) else {
            return;
        };
        let writer = if kind == TX_RELAY_DATA_KIND {
            &io.data
        } else {
            &io.state
        };
        match writer.try_send(Outgoing::Frame(frame, guard)) {
            Ok(()) => count(message, "sent"),
            Err(mpsc::error::TrySendError::Full(_)) => {
                io.close.close("tx-relay: peer stopped reading");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }

    /// Publishes how many sessions are live.
    fn count_sessions(&self) {
        // A session count is far below `f64`'s exact integer range.
        metrics::gauge!("zakura.p2p.tx_relay.sessions").set(self.sessions.len() as f64);
    }

    /// Sends each session's queued announcements as one `Inv`, at once,
    /// once the session's `Inv` byte rate pays for it. While it waits, new
    /// announcements join the frame, so a session batches only under load.
    fn flush_adverts(&mut self) {
        let now = Instant::now();
        let sessions: Vec<SessionId> = self
            .sessions
            .iter()
            .filter(|(_, io)| io.inv_ready && !io.inv_due.is_some_and(|due| now < due))
            .map(|(session, _)| *session)
            .collect();
        for session in sessions {
            let queued = self.machine.queued_inv(session).min(MAX_INV_ENTRIES);
            let Some(io) = self.sessions.get_mut(&session) else {
                continue;
            };
            io.inv_due = None;
            if queued == 0 {
                io.inv_ready = false;
                continue;
            }
            // Keep room for every body the peer may have requested, and
            // keep one `Inv` unwritten at most.
            if io.data.capacity() <= MAX_SERVES || io.inv_unwritten.strong_count() > 0 {
                io.inv_due = Some(now + INV_RETRY);
                continue;
            }
            // Wait until the rate pays for every queued id, so the frame
            // taken below is always paid for.
            // Widening usize to u64 is lossless on supported targets.
            let bound = inv_payload_bound(queued) as u64;
            let Some(inv) = self.inv_buckets.get_mut(&io.connection) else {
                continue;
            };
            match inv.bucket.ready_at(bound, now) {
                Some(at) if at > now => {
                    io.inv_due = Some(at);
                    continue;
                }
                // The row's layout check keeps the largest frame within the
                // bucket's depth, so it is always ready at some time.
                _ => {}
            }
            let entries = self.machine.take_inv(session, MAX_INV_ENTRIES);
            let Some(io) = self.sessions.get_mut(&session) else {
                continue;
            };
            if entries.is_empty() {
                // An `Inv` holds at least one id.
                io.inv_ready = false;
                continue;
            }
            let message = TxRelayMessage::Inv(entries);
            let Ok(frame) = encode_frame(&message) else {
                continue;
            };
            let Some(inv) = self.inv_buckets.get_mut(&io.connection) else {
                continue;
            };
            // Widening usize to u64 is lossless on supported targets.
            let paid = inv.bucket.try_pay(frame.payload.len() as u64, now);
            debug_assert!(paid, "the frame is within the bound the bucket held");
            let written = Arc::new(());
            io.inv_unwritten = zakura_rt::sync::downgrade(&written);
            // The capacity check leaves room, and the actor is the only
            // sender, so the frame is queued.
            if io
                .data
                .try_send(Outgoing::Frame(frame, Some(FrameGuard::new(written))))
                .is_ok()
            {
                count(&message, "sent");
            }
        }
    }
}

/// Counts one message `direction` ("sent" or "received"): ids for `Inv`, one
/// for each other message. `Hello` is not counted.
fn count(message: &TxRelayMessage, direction: &'static str) {
    let (name, amount) = match message {
        TxRelayMessage::Hello { .. } => return,
        // An `Inv` holds at most 1,024 ids.
        TxRelayMessage::Inv(entries) => ("zakura.p2p.tx_relay.inv_ids", entries.len() as u64),
        TxRelayMessage::GetTxs(_) => ("zakura.p2p.tx_relay.get_txs", 1),
        TxRelayMessage::Tx { .. } => ("zakura.p2p.tx_relay.tx", 1),
        TxRelayMessage::NotFound(_) => ("zakura.p2p.tx_relay.not_found", 1),
    };
    metrics::counter!(name, "direction" => direction).increment(amount);
}

/// The machine's view of a refused reservation.
fn unreserved(refused: PortUnreserved) -> Unreserved {
    match refused {
        PortUnreserved::Inactive => Unreserved::Inactive,
        PortUnreserved::Pooled => Unreserved::Pooled,
        PortUnreserved::Pending => Unreserved::Pending,
        PortUnreserved::Rejected(rejection) => Unreserved::Rejected {
            scope: rejection.scope,
        },
        PortUnreserved::NoRoom => Unreserved::NoRoom,
        PortUnreserved::Stopped => Unreserved::Stopped,
    }
}

/// The machine's view of a verdict.
fn verdict_of(verdict: PortVerdict) -> Verdict {
    match verdict {
        PortVerdict::Admitted => Verdict::Admitted,
        PortVerdict::Rejected(rejection) => Verdict::Rejected {
            scope: rejection.scope,
            charge: rejection.charge,
            tip: rejection.tip,
        },
        PortVerdict::NoRoom => Verdict::NoRoom,
        PortVerdict::LocalFault(fault) => Verdict::LocalFault(fault),
    }
}

/// The recorded cause for a close the machine asked for.
fn close_reason(reason: CloseReason) -> &'static str {
    match reason {
        CloseReason::Violation(_) => "tx-relay: protocol violation",
        CloseReason::Ban => "tx-relay: banned transaction",
        CloseReason::RepeatedCooldown => "tx-relay: repeated cooldown",
        CloseReason::CooldownsFull => "tx-relay: cooldown table full",
        CloseReason::Unresponsive => "tx-relay: unanswered request",
    }
}

/// The snapshot's transaction with exactly `id`.
fn exact(snapshot: &PoolSnapshot, id: UnminedTxId) -> Option<&VerifiedUnminedTx> {
    snapshot
        .transaction(&id.mined_id())
        .filter(|tx| tx.transaction.id() == id)
}

/// The serialized size of the snapshot's transaction with exactly `id`.
fn size_in(snapshot: &PoolSnapshot, id: UnminedTxId) -> Option<u32> {
    exact(snapshot, id).map(|tx| transaction_size(tx.transaction.size()))
}

/// A transaction's serialized size on the wire. It fits a `u32`: the pool
/// holds nothing over the protocol's 2,000,000 bytes.
fn transaction_size(size: usize) -> u32 {
    u32::try_from(size).unwrap_or(u32::MAX)
}

/// Splits log entries into the batches they were published in, in order.
fn batches(entries: Vec<ChangeEntry>) -> Vec<Vec<Change>> {
    let mut batches: Vec<(u64, Vec<Change>)> = Vec::new();
    for entry in entries {
        match batches.last_mut() {
            Some((batch, changes)) if *batch == entry.batch => changes.push(entry.change),
            _ => batches.push((entry.batch, vec![entry.change])),
        }
    }
    batches.into_iter().map(|(_, changes)| changes).collect()
}

#[cfg(test)]
mod tests;
