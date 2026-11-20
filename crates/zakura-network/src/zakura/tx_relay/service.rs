//! `tx-relay`'s service and IO: the v2 [`Service`] that takes each session's
//! two streams and runs one routine for it.
//!
//! Each session's routine owns its streams, its [`PeerMachine`], its calls
//! to the pool and the reservations they bring, and its `Inv` pacing. It
//! reads and decodes its own frames, so peers' bodies decode in parallel.
//! What the routines share, the wanted table and the reactor, is a
//! [`Shared`] behind one brief lock, which no routine holds across an await.
//! After each step the routine wakes the others the machine's rules name:
//! those an effect reaches, which read their mail first at their next step,
//! and all of them once the wanted table's ring moved or claims opened up.
//! See [`Marks`] and [`NodeEffect::reach`](super::machine::NodeEffect::reach).
//! A session that ends hands its unanswered reserve calls and verdicts to
//! the reactor task, which applies them to the shared state. So all the
//! rules live in the machines, and this file only moves data.

use std::{fmt, future::Future, net::IpAddr, pin::Pin, sync::Arc, time::Duration};

use futures::{stream::FuturesUnordered, StreamExt};
use rand_10::Rng;
use zakura_chain::{
    block,
    transaction::{self, Transaction, UnminedTx, UnminedTxId, VerifiedUnminedTx},
};
use zakura_node_services::mempool::port::{
    ChainTip, ChangeSeq, ChangesSince, LocalFault, PoolHandle, PoolReader, PoolSnapshot,
    PoolView as PortView, Reservation, ReservationId, Unreserved as PortUnreserved,
    Verdict as PortVerdict,
};
use zakura_rt::{
    collections::HashMap,
    sync::{mpsc, CancellationToken, Mutex, Notify},
    task,
    time::{self, Instant},
};

use super::{
    machine::{
        peer::Hello, CallId, CloseReason, Granted, InvRoom, Mail, Marks, Paid, PeerKey,
        PeerMachine, PoolView, RelayConfig, RelayEffect, SessionId, Shared, Time, Unreserved,
        Verdict, Violation, WaiterId, MAX_SERVES, RANK_INTERVAL,
    },
    sessions::{LiveSession, TxRelaySessions},
    wire::{
        tx_frame, TxRelayMessage, INV_RATE, MAX_TX_BYTES, TX_RELAY_DATA_KIND, TX_RELAY_STATE_KIND,
        TX_RELAY_STREAMS,
    },
    ZakuraTxRelayConfig,
};
use crate::{
    config::UnconditionalPeers,
    zakura::{
        regulation::{ByteBucket, MeteredReady, MeteredSender},
        resume_task_panic,
        transport::OrderedStreamFailure,
        wire_codec::{decode_frame, encode_frame},
        Frame, FrameGuard, FramedRecv, FramedSend, Peer, Service, SessionOpening, SessionPolicy,
        Stream, ZakuraConnId, ZakuraPeerId,
    },
};

/// Whether a verdict decided at a height may start a cooldown: the node is
/// current and its best tip is still there. `zakurad` supplies it.
pub type MayCool = Arc<dyn Fn(zakura_chain::block::Height) -> bool + Send + Sync>;

/// Jobs a session's data writer queues: a serve job for every request our
/// `Hello` lets a peer have unanswered, each only an id until the writer
/// builds its body, and room for the one unwritten `Inv`, which goes out
/// only while the jobs' room is free. A conformant peer never fills it.
const DATA_QUEUE: usize = MAX_SERVES + 4;

/// Frames a session's state writer queues. Requests, `NotFound` answers and
/// our `Hello` are bounded by the machine's per-peer limits, far below this.
const STATE_QUEUE: usize = 64;

/// What a session's writer sends.
enum Outgoing {
    /// A frame, with what it holds until the transport has written it.
    Frame(Frame, Option<FrameGuard>),
    /// A `Tx` answering the peer's request for `id`, built only when the
    /// writer reaches it.
    Serve(UnminedTxId),
}

/// Where a data writer builds the bodies it serves, and answers `NotFound`
/// for those the pool no longer holds.
struct Bodies {
    pool: PoolReader,
    state: mpsc::Sender<Outgoing>,
    /// Where the writer reports to the routine.
    reports: mpsc::UnboundedSender<WriterReport>,
}

/// What a data writer tells its routine.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum WriterReport {
    /// It started answering the peer's request for this id. It says so
    /// before the answer can reach the peer, so the request's place is free
    /// before the peer can count it free.
    Started(UnminedTxId),
    /// Its `NotFound` found the state writer's queue full: the peer stopped
    /// reading.
    Stalled,
}

/// The `tx-relay` service.
pub struct TxRelayService {
    /// What each session's routine is wired to.
    wiring: Wiring,
    /// Keys the hash that names peers to the machine, so no peer can choose
    /// an identity that collides with another's.
    peer_key: blake2b_simd::Params,
}

/// What every routine shares, behind one lock that no routine holds across
/// an await.
struct Node {
    shared: Shared,
    /// Each live session's handle.
    sessions: HashMap<SessionId, Handle>,
}

/// What another routine needs of a session: its peer, its closer, and its
/// mail.
struct Handle {
    peer: PeerKey,
    close: Closer,
    /// Wakes the session's routine to look again. A wake before the routine
    /// waits is kept, so none is lost.
    wake: Arc<Notify>,
    /// What other sessions' steps left for it.
    mail: Vec<Mail>,
}

/// Pool calls in flight.
type Calls = FuturesUnordered<PoolCall>;

/// Each connection's `Inv` byte rate, which every session on it pays before
/// it queues a frame, so overlapping or replacing sessions share one
/// opening allowance.
type InvBuckets = Arc<Mutex<HashMap<(ZakuraPeerId, ZakuraConnId), ConnectionInv>>>;

/// The connections tx-relay owns, until the transport removes them. The
/// service adds a session when it takes its streams, and the routine removes
/// it when the session ends.
type Owned = Arc<Mutex<HashMap<(ZakuraPeerId, ZakuraConnId), Ownership>>>;

/// How long a connection whose last session ended stays owned, so the
/// transport can reopen the session before discovery retires the
/// connection: past the transport's 8 second cap on its reopen backoff.
const REOPEN_GAP: Duration = Duration::from_secs(10);

/// How long a routine waits on the pool core for each answer: the 73 second
/// deadline the node's own mempool gives each transaction.
const POOL_ANSWER: Duration = Duration::from_secs(73);

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
    /// Starts the reactor task on `pool` with `config`'s fan-out and returns
    /// the service to register. A body whose supplier's address is in
    /// `unconditional` when it arrives is never charged. Each session is
    /// published in `sessions` from when its routine holds a view of the
    /// pool until the session ends or the pool stops.
    pub fn spawn(
        pool: PoolHandle,
        may_cool: MayCool,
        config: &ZakuraTxRelayConfig,
        unconditional: UnconditionalPeers,
        sessions: TxRelaySessions,
    ) -> Arc<Self> {
        let mut key = [0; 8];
        zakura_rt::rand::rng().fill_bytes(&mut key);
        let node = Node {
            shared: Shared::new(RelayConfig {
                fan_out: config.fan_out(),
                short_id_key: u64::from_le_bytes(key),
            }),
            sessions: HashMap::default(),
        };
        let (handoff, handed) = mpsc::unbounded_channel();
        let wiring = Wiring {
            node: Arc::new(Mutex::new(node)),
            pool,
            may_cool,
            handoff,
            owned: Owned::default(),
            record: sessions,
            inv_buckets: InvBuckets::default(),
            base: Instant::now(),
            unconditional,
        };
        // The reactor task holds a reader, not a handle, so it does not keep
        // the core running, and it stops once the service, every routine
        // and its last call are gone.
        task::spawn_named(
            "tx-relay-reactor",
            run_reactor(
                wiring.node.clone(),
                wiring.pool.reader(),
                wiring.may_cool.clone(),
                wiring.base,
                handed,
            ),
        );
        let mut key = [0; 32];
        zakura_rt::rand::rng().fill_bytes(&mut key);
        let mut peer_key = blake2b_simd::Params::new();
        peer_key
            .hash_length(8)
            .key(&key)
            .personal(b"Zakura_TxPeerKey");
        Arc::new(Self { wiring, peer_key })
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
        self.wiring
            .owned
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
                .wiring
                .owned
                .lock()
                .expect("the ownership map is never poisoned");
            let ownership = owned.entry(connection.clone()).or_default();
            ownership.sessions += 1;
            ownership.gap_until = None;
        }
        let peer_key = self.peer_key(&peer.id);
        let rtt = peer.rtt_reader();
        let conn_cancel = peer.cancel_token();
        let session_cancel = peer.service_cancel_token();
        let close_cause = peer.close_cause();
        let close = Closer {
            conn_cancel,
            close_cause,
        };
        let state = spawn_writer(state_send, STATE_QUEUE, session_cancel.clone(), None);
        let (reports, reports_rx) = mpsc::unbounded_channel();
        let bodies = Bodies {
            pool: self.wiring.pool.reader(),
            state: state.clone(),
            reports,
        };
        let data = spawn_writer(data_send, DATA_QUEUE, session_cancel.clone(), Some(bodies));
        let inv = MeteredSender::new(open_inv_bucket(&self.wiring.inv_buckets, &connection));
        let routine = Routine::open(
            self.wiring.clone(),
            peer_key,
            peer.remote_ip,
            SessionIo::new(data, state, close, reports_rx, inv, connection),
            session_cancel,
            rtt,
        );
        task::spawn_named("tx-relay-routine", routine.run(data_recv, state_recv));
    }

    fn remove_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) {
        self.wiring
            .owned
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
                Outgoing::Serve(id) => {
                    let Some(bodies) = &bodies else {
                        continue;
                    };
                    let _ = bodies.reports.send(WriterReport::Started(id));
                    if !serve_body(&send, bodies, id).await {
                        return;
                    }
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
                let _ = bodies.reports.send(WriterReport::Stalled);
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

/// What a pool call returns to its routine, or to the reactor task once its
/// session ended.
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

/// One session's streams and regulation tools: its writers, its closer, and
/// its `Inv` sender.
struct SessionIo {
    data: mpsc::Sender<Outgoing>,
    state: mpsc::Sender<Outgoing>,
    close: Closer,
    /// What the data writer reports.
    reports: mpsc::UnboundedReceiver<WriterReport>,
    /// A writer's queue was full at this step: the peer stopped reading.
    stalled: bool,
    /// Pays the connection's `Inv` byte rate and keeps one `Inv` unwritten
    /// at most. One unwritten `Inv` of at most about 70 KB, with the
    /// transport's 64 KiB of write-ahead, stays within the row's 512 KiB
    /// sender allowance.
    inv: MeteredSender,
    /// The peer and connection the session runs on.
    connection: (ZakuraPeerId, ZakuraConnId),
    /// The session's entry in the live-session record, once the routine
    /// holds a view of the pool. It is removed when this drops.
    live: Option<LiveSession>,
}

impl SessionIo {
    /// A session's IO on these writers, closing with `close`, sending `Inv`
    /// through `inv`, running on `connection`. `reports` hears from the
    /// data writer.
    fn new(
        data: mpsc::Sender<Outgoing>,
        state: mpsc::Sender<Outgoing>,
        close: Closer,
        reports: mpsc::UnboundedReceiver<WriterReport>,
        inv: MeteredSender,
        connection: (ZakuraPeerId, ZakuraConnId),
    ) -> Self {
        Self {
            data,
            state,
            close,
            reports,
            stalled: false,
            inv,
            connection,
            live: None,
        }
    }
}

/// One connection's `Inv` byte rate, and how many of its sessions are live.
struct ConnectionInv {
    bucket: Arc<Mutex<ByteBucket>>,
    sessions: usize,
}

/// Counts a new session on `connection` in its `Inv` bucket, making the
/// bucket if the connection has none, and returns the bucket. A bucket no
/// session uses that has refilled is the same as a new one, so it goes.
fn open_inv_bucket(
    buckets: &InvBuckets,
    connection: &(ZakuraPeerId, ZakuraConnId),
) -> Arc<Mutex<ByteBucket>> {
    let opened = Instant::now();
    let mut buckets = buckets.lock().expect("the bucket map is never poisoned");
    // Widening u32 to u64 is lossless.
    let full = u64::from(INV_RATE.opening_allowance);
    buckets.retain(|_, inv| {
        let mut bucket = inv.bucket.lock().expect("a bucket is never poisoned");
        inv.sessions > 0 || bucket.ready_at(full, opened) != Some(opened)
    });
    let inv = buckets
        .entry(connection.clone())
        .or_insert_with(|| ConnectionInv {
            bucket: Arc::new(Mutex::new(ByteBucket::sender(INV_RATE, opened))),
            sessions: 0,
        });
    inv.sessions += 1;
    inv.bucket.clone()
}

/// The pool as a routine read it: the snapshot it last read.
#[derive(Clone, Debug)]
struct SnapshotView(Arc<PoolSnapshot>);

impl SnapshotView {
    /// The view before any snapshot: a pool that is off.
    fn empty() -> Self {
        Self(Arc::new(PoolSnapshot::inactive(
            ChainTip {
                hash: block::Hash([0; 32]),
                height: block::Height(0),
            },
            ChangeSeq(0),
        )))
    }
}

impl PoolView for SnapshotView {
    fn live(&self) -> bool {
        self.0.active
    }

    fn tip(&self) -> ChainTip {
        self.0.tip
    }

    fn last_seq(&self) -> u64 {
        self.0.last_change.0
    }

    fn holds(&self, id: &UnminedTxId) -> bool {
        exact(&self.0, *id).is_some()
    }

    fn holds_mined(&self, id: &transaction::Hash) -> bool {
        self.0.transaction(id).is_some()
    }

    fn size(&self, id: &UnminedTxId) -> Option<u32> {
        size_in(&self.0, *id)
    }

    fn ids(&self) -> Vec<UnminedTxId> {
        self.0
            .transactions()
            .map(|tx| tx.transaction.id())
            .collect()
    }

    fn parents(&self, id: &UnminedTxId) -> Vec<UnminedTxId> {
        let snapshot = &self.0;
        let Some(parents) = snapshot.dependencies.dependencies().get(&id.mined_id()) else {
            return Vec::new();
        };
        parents
            .iter()
            .filter_map(|parent| snapshot.transaction(parent))
            .map(|parent| parent.transaction.id())
            .collect()
    }
}

/// What a routine is wired to: the shared state, the pool, the reactor's
/// handoff, and the service's maps.
#[derive(Clone)]
struct Wiring {
    node: Arc<Mutex<Node>>,
    pool: PoolHandle,
    may_cool: MayCool,
    handoff: mpsc::UnboundedSender<Calls>,
    owned: Owned,
    record: TxRelaySessions,
    inv_buckets: InvBuckets,
    base: Instant,
    /// Bodies from these addresses are never charged. See
    /// [`Routine::exempt`].
    unconditional: UnconditionalPeers,
}

/// One session's routine. See the module docs.
struct Routine {
    machine: PeerMachine,
    wiring: Wiring,
    /// The peer's address, which [`Self::exempt`] looks up.
    remote_ip: Option<IpAddr>,
    /// Wakes this routine to look again: for its mail, a moved ring, or
    /// claims that opened up.
    wake: Arc<Notify>,
    io: SessionIo,
    /// The session's cancellation, which stops its writers too.
    cancel: CancellationToken,
    /// Reads the connection's smoothed round-trip time, which the routine
    /// reports to the ranking once a [`RANK_INTERVAL`], from `rtt_due` on.
    rtt: Option<zakura_quic::RttReader>,
    rtt_due: Instant,
    /// The pool as this routine last read it, and its place in the log, or
    /// `None` before the first snapshot.
    view: SnapshotView,
    cursor: Option<ChangeSeq>,
    /// Whether the core behind the pool is still running.
    pool_open: bool,
    calls: Calls,
    reservations: HashMap<ReservationId, Reservation>,
    /// Ends the wait on each held reservation's cancellation once the
    /// reservation leaves the routine.
    fetch_waits: HashMap<ReservationId, CancellationToken>,
}

impl Routine {
    /// Opens a session of `peer` on `io`, sends our `Hello`, and returns its
    /// routine, which reads the pool before its first step and reports the
    /// round-trip time `rtt` reads on its first tick.
    fn open(
        wiring: Wiring,
        peer: PeerKey,
        remote_ip: Option<IpAddr>,
        io: SessionIo,
        cancel: CancellationToken,
        rtt: Option<zakura_quic::RttReader>,
    ) -> Self {
        let wake = Arc::new(Notify::new());
        let view = SnapshotView::empty();
        let now = Instant::now().saturating_duration_since(wiring.base);
        let (machine, effects, wakes) = {
            let mut node = wiring.node.lock().expect("the node is never poisoned");
            let node = &mut *node;
            let marks = Marks::of(&node.shared);
            let machine = PeerMachine::connect(&mut node.shared, &view, peer, now);
            node.sessions.insert(
                machine.session(),
                Handle {
                    peer,
                    close: io.close.clone(),
                    wake: wake.clone(),
                    mail: Vec::new(),
                },
            );
            count_sessions(node);
            let effects = std::mem::take(&mut node.shared.effects);
            let wakes = route(node, marks, Some(machine.session()));
            (machine, effects, wakes)
        };
        for wake in wakes {
            wake.notify_one();
        }
        let hello = TxRelayMessage::Hello {
            // `MAX_SERVES` is a small constant that fits a `u16`.
            serves: u16::try_from(MAX_SERVES).unwrap_or(u16::MAX),
            max_tx_bytes: MAX_TX_BYTES,
        };
        if let Ok(frame) = encode_frame(&hello) {
            let _ = io.state.try_send(Outgoing::Frame(frame, None));
        }
        let mut routine = Self {
            machine,
            wiring,
            remote_ip,
            wake,
            io,
            cancel,
            rtt,
            rtt_due: Instant::now(),
            view,
            cursor: None,
            pool_open: true,
            calls: FuturesUnordered::new(),
            reservations: HashMap::default(),
            fetch_waits: HashMap::default(),
        };
        for effect in effects {
            routine.effect(effect);
        }
        routine
    }

    /// The machine's clock: time since the service started.
    fn now(&self) -> Time {
        Instant::now().saturating_duration_since(self.wiring.base)
    }

    /// Whether the peer's address is one of `network.unconditional_peers`
    /// now. A pinned name's address joins the list only once a connection
    /// proves the name's node ID, which can be after the session opened.
    fn exempt(&self) -> bool {
        self.remote_ip
            .is_some_and(|ip| self.wiring.unconditional.contains(ip))
    }

    /// Runs the session until it ends: reads its frames, the pool's answers
    /// and publications, its wakes, and its deadlines.
    async fn run(mut self, mut data: FramedRecv, mut state: FramedRecv) {
        let mut reader = self.wiring.pool.reader();
        self.read_pool();
        self.after_step();
        while !self.machine.is_ended() {
            let wake = self.wake.clone();
            let deadline = self.next_wake();
            zakura_rt::select! {
                () = self.cancel.cancelled() => break,
                frame = data.recv() => {
                    let Some(frame) = frame else { break };
                    if !self.frame(&frame).await {
                        break;
                    }
                }
                frame = state.recv() => {
                    let Some(frame) = frame else { break };
                    if !self.frame(&frame).await {
                        break;
                    }
                }
                Some(result) = self.calls.next(), if !self.calls.is_empty() => {
                    self.pool_result(result);
                }
                changed = reader.snapshot_changed(), if self.pool_open => {
                    if changed {
                        self.read_pool();
                    } else {
                        self.pool_stopped();
                    }
                }
                Some(report) = self.io.reports.recv() => self.writer_report(report),
                () = wake.notified() => self.tick(),
                () = time::sleep_until(deadline) => self.tick(),
            }
            self.after_step();
        }
        // The transport retires a session it cannot write to by its write
        // deadline. That peer stopped reading, a stall as a full writer
        // queue is. The session's streams share one failure record.
        if data.failure() == Some(OrderedStreamFailure::WriteTimeout) {
            self.step(stopped_reading);
        }
        self.finish();
    }

    /// Hands what the data writer reported to the machine.
    fn writer_report(&mut self, report: WriterReport) {
        match report {
            WriterReport::Started(id) => {
                self.step(|machine, shared, pool, now| machine.served(shared, pool, id, now));
            }
            WriterReport::Stalled => {
                self.step(stopped_reading);
            }
        }
    }

    /// Hands one input to the machine under the shared lock, after the mail
    /// other sessions left. Routes the step's effects on other sessions,
    /// wakes the routines that should look, and performs this session's
    /// effects once the lock is gone.
    fn step<R>(
        &mut self,
        input: impl FnOnce(&mut PeerMachine, &mut Shared, &dyn PoolView, Time) -> R,
    ) -> R {
        let now = self.now();
        let view = self.view.clone();
        let session = self.machine.session();
        let (result, effects, closes, wakes) = {
            let mut node = self.wiring.node.lock().expect("the node is never poisoned");
            let node = &mut *node;
            let marks = Marks::of(&node.shared);
            let mail = node
                .sessions
                .get_mut(&session)
                .map(|handle| std::mem::take(&mut handle.mail))
                .unwrap_or_default();
            for mail in mail {
                self.machine.read_mail(&mut node.shared, &view, mail);
            }
            let result = input(&mut self.machine, &mut node.shared, &view, now);
            let effects = std::mem::take(&mut node.shared.effects);
            let closes = closers(node, &effects, Some(session));
            let wakes = route(node, marks, Some(session));
            (result, effects, closes, wakes)
        };
        for wake in wakes {
            wake.notify_one();
        }
        for (close, reason) in closes {
            close.close(close_reason(reason));
        }
        for effect in effects {
            self.effect(effect);
        }
        self.stopped_reading();
        result
    }

    /// A writer's queue was full in the last step: the peer stopped
    /// reading, which the machine decides on in a step of its own.
    fn stopped_reading(&mut self) {
        if std::mem::take(&mut self.io.stalled) {
            self.step(stopped_reading);
        }
    }

    /// Time passed, or another step woke this one. A round-trip time due
    /// for the ranking goes in this step.
    fn tick(&mut self) {
        match self.due_rtt() {
            Some(rtt) => self.step(|machine, shared, pool, now| {
                machine.observe_rtt(shared, pool, rtt, now);
            }),
            None => self.step(|machine, shared, pool, now| machine.tick(shared, pool, now)),
        }
    }

    /// The connection's round-trip time, if a report is due, and when the
    /// next one is. A connection with no sample yet reports nothing.
    fn due_rtt(&mut self) -> Option<Duration> {
        let reader = self.rtt.as_ref()?;
        let now = Instant::now();
        if now < self.rtt_due {
            return None;
        }
        self.rtt_due = now + RANK_INTERVAL;
        reader.rtt().filter(|rtt| !rtt.is_zero())
    }

    /// What this routine does after each input: sends announcements and
    /// joins the live-session record once it holds a view of the pool.
    fn after_step(&mut self) {
        self.flush_adverts();
        let open = !self.machine.is_ended() && self.pool_open;
        if open && self.cursor.is_some() && self.io.live.is_none() {
            let (peer, conn_id) = self.io.connection.clone();
            self.io.live = Some(
                self.wiring
                    .record
                    .open(peer, conn_id, self.machine.session().0),
            );
        }
    }

    /// When this routine next has something due: the machine's deadline,
    /// which covers its next `Inv` try, or its next round-trip time report.
    fn next_wake(&self) -> Instant {
        let far = Instant::now() + Duration::from_secs(3600);
        let machine = {
            let node = self.wiring.node.lock().expect("the node is never poisoned");
            self.machine.next_deadline(&node.shared)
        };
        let machine = machine.map_or(far, |at| self.wiring.base + at);
        match self.rtt {
            Some(_) => machine.min(self.rtt_due),
            None => machine,
        }
    }

    /// Handles one frame from the peer. Returns `false` for a frame that
    /// does not decode, which closes the connection and ends the session.
    async fn frame(&mut self, frame: &Frame) -> bool {
        match decode_frame::<TxRelayMessage>(frame) {
            Ok(message) => {
                self.message(message).await;
                true
            }
            Err(_) => {
                // Row 3 of the close table.
                self.io.close.close("tx-relay: malformed frame");
                false
            }
        }
    }

    /// Hands one decoded message to the machine. A `Tx` body waits for its
    /// decode on the blocking pool first.
    async fn message(&mut self, message: TxRelayMessage) {
        count(&message, "received");
        match message {
            TxRelayMessage::Hello {
                serves,
                max_tx_bytes,
            } => {
                let hello = Hello {
                    serves: usize::from(serves),
                    max_tx_bytes,
                };
                self.step(|machine, shared, pool, now| machine.hello(shared, pool, hello, now));
            }
            TxRelayMessage::Inv(entries) => {
                self.step(|machine, shared, pool, now| machine.inv(shared, pool, &entries, now));
            }
            TxRelayMessage::GetTxs(id) => {
                // The writer reports an answer it started before handing it
                // to the transport, so each answer the peer has seen is read
                // here before the peer's next request.
                while let Ok(report) = self.io.reports.try_recv() {
                    self.writer_report(report);
                }
                self.step(|machine, shared, pool, now| machine.get_txs(shared, pool, id, now));
            }
            TxRelayMessage::NotFound(id) => {
                self.step(|machine, shared, pool, now| machine.not_found(shared, pool, id, now));
            }
            TxRelayMessage::Tx { id, body } => {
                // A frame's body is at most the data stream's cap.
                let size = u32::try_from(body.len()).unwrap_or(u32::MAX);
                // The body is judged as of when it arrived, before and after
                // it decodes. A body on time when it came is not late because
                // its decode took time, and its supplier keeps the exemption
                // it had then.
                let arrived = self.now();
                let exempt = self.exempt();
                let read = self.step(|machine, shared, pool, _| {
                    machine.tx_arrived(shared, pool, id, size, arrived)
                });
                if !read {
                    return;
                }
                // Decoding checks the body's curve points and `UnminedTx`
                // hashes it, so both run on the blocking pool, outside the
                // shared lock, and this routine waits for its peer's one body.
                let decoded = task::spawn_blocking_named("tx-relay-decode", move || {
                    let mut bytes = body.as_slice();
                    match Transaction::zcash_deserialize_bounded(&mut bytes) {
                        Ok(transaction) if bytes.is_empty() => Some(UnminedTx::from(transaction)),
                        _ => None,
                    }
                })
                .await;
                // A decode is cancelled only when the runtime shuts down.
                let Ok(decoded) = resume_task_panic(decoded) else {
                    return;
                };
                match decoded {
                    Some(body) => {
                        self.step(|machine, shared, pool, _| {
                            machine.tx(shared, pool, id, body, exempt, arrived);
                        });
                    }
                    None => {
                        self.step(|machine, shared, pool, _| {
                            machine.undecodable(shared, pool, arrived);
                        });
                    }
                }
            }
        }
    }

    /// Hands one of this session's pool results to the machine.
    fn pool_result(&mut self, result: PoolResult) {
        match result {
            PoolResult::Reserved { call, answer } => {
                let answer = match answer {
                    Ok(reservation) => Ok(self.hold(reservation)),
                    Err(refused) => Err(unreserved(refused)),
                };
                self.step(|machine, shared, pool, now| {
                    machine.reserve_answer(shared, pool, call, answer, now);
                });
            }
            PoolResult::Verdict { waiter, verdict } => {
                let verdict = verdict_of(verdict);
                let may_cool = read_verdict(&self.wiring.may_cool, verdict);
                self.step(|machine, shared, pool, now| {
                    set_currentness(shared, verdict, may_cool);
                    machine.verdict(shared, pool, waiter, verdict, now);
                });
            }
            PoolResult::Cancelled { reservation } => {
                if self.reservations.contains_key(&reservation) {
                    self.step(|machine, shared, pool, now| {
                        machine.fetch_cancelled(shared, pool, reservation, now);
                    });
                }
            }
            PoolResult::Done => {}
        }
    }

    /// Holds a granted reservation, waiting on its cancellation unless its
    /// token already fired. Returns the machine's view of the grant.
    fn hold(&mut self, reservation: Reservation) -> Granted {
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
        Granted {
            reservation: id,
            cancelled,
        }
    }

    /// Reads the pool's newest publication and hands it to the machine:
    /// every change after this routine's place, or the whole pool when it
    /// has no place in the log.
    fn read_pool(&mut self) {
        let view = match self.cursor {
            Some(cursor) => self.wiring.pool.view(cursor),
            None => self.wiring.pool.snapshot().map(|snapshot| PortView {
                snapshot,
                changes: ChangesSince::Resync,
            }),
        };
        let Some(view) = view else {
            return;
        };
        let activated = !self.view.0.active && view.snapshot.active;
        self.cursor = Some(view.snapshot.last_change);
        self.view = SnapshotView(view.snapshot);
        match view.changes {
            ChangesSince::Entries(entries) => {
                let changes: Vec<(u64, _)> = entries
                    .into_iter()
                    .map(|entry| (entry.seq.0, entry.change))
                    .collect();
                self.step(|machine, shared, pool, now| {
                    let read = shared.log(changes, now);
                    machine.pool_changed(shared, pool, &read, activated, now);
                });
            }
            ChangesSince::Resync => {
                self.step(|machine, shared, pool, now| {
                    machine.pool_resynced(shared, pool, activated, now);
                });
            }
        }
    }

    /// The core is gone. Its receiver stays ready, so this routine stops
    /// watching it, and hands its peer's transactions back to the legacy
    /// adapter.
    fn pool_stopped(&mut self) {
        self.pool_open = false;
        self.io.live = None;
        self.step(|machine, shared, pool, now| {
            shared.stopped = true;
            machine.tick(shared, pool, now);
        });
    }

    /// Ends the session: hands what it held to the shared state, gives back
    /// its reservations, and hands its unanswered pool calls to the reactor
    /// task.
    fn finish(mut self) {
        self.step(|machine, shared, pool, now| machine.end(shared, pool, now));
        {
            let mut node = self.wiring.node.lock().expect("the node is never poisoned");
            node.sessions.remove(&self.machine.session());
            count_sessions(&node);
        }
        for (_, done) in self.fetch_waits.drain() {
            done.cancel();
        }
        self.reservations.clear();
        if !self.calls.is_empty() {
            let calls = std::mem::take(&mut self.calls);
            let _ = self.wiring.handoff.send(calls);
        }
        disown(&self.wiring.owned, &self.io.connection, Instant::now());
        if let Some(inv) = self
            .wiring
            .inv_buckets
            .lock()
            .expect("the bucket map is never poisoned")
            .get_mut(&self.io.connection)
        {
            inv.sessions = inv.sessions.saturating_sub(1);
        }
        self.io.live = None;
    }

    /// Performs one of this session's effects.
    fn effect(&mut self, effect: RelayEffect) {
        match effect {
            RelayEffect::Ask { id, .. } => {
                self.send(TX_RELAY_STATE_KIND, &TxRelayMessage::GetTxs(id));
            }
            // The machine queued the id, and `flush_adverts` asks it for an
            // `Inv` after every step.
            RelayEffect::Advert { .. } => {}
            RelayEffect::Serve { id, .. } => self.serve(id),
            RelayEffect::NotFound { id, .. } => {
                self.send(TX_RELAY_STATE_KIND, &TxRelayMessage::NotFound(id));
            }
            RelayEffect::Close { session, reason } => {
                if session == self.machine.session() {
                    self.io.close.close(close_reason(reason));
                }
            }
            // Ending the session leaves the connection, and its other
            // services, alone.
            RelayEffect::Park { session } => {
                if session == self.machine.session() {
                    metrics::counter!("zakura.p2p.tx_relay.parked").increment(1);
                    self.cancel.cancel();
                }
            }
            RelayEffect::Reserve { call, id } => {
                let pool = self.wiring.pool.clone();
                self.calls.push(Box::pin(async move {
                    // A core that does not answer in time is busy, so the
                    // machine tries again later.
                    let answer = pool
                        .reserve(id, Instant::now() + POOL_ANSWER)
                        .await
                        .unwrap_or(Err(PortUnreserved::NoRoom));
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
                // with a `NoRoom` verdict, so the routine never holds a
                // decoded body while it reads the next frame.
                let verdict =
                    self.wiring
                        .pool
                        .submit_now(reservation, body, Instant::now() + POOL_ANSWER);
                self.calls.push(Box::pin(async move {
                    PoolResult::Verdict {
                        waiter,
                        verdict: verdict
                            .await
                            .unwrap_or(PortVerdict::LocalFault(LocalFault::VerifyTimeout)),
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
                let pool = self.wiring.pool.clone();
                self.calls.push(Box::pin(async move {
                    pool.fetch_failed(reservation, Instant::now() + POOL_ANSWER)
                        .await;
                    PoolResult::Done
                }));
            }
        }
    }

    /// Takes `id` out of the routine and ends the wait on its cancellation.
    fn take_reservation(&mut self, id: ReservationId) -> Option<Reservation> {
        if let Some(done) = self.fetch_waits.remove(&id) {
            done.cancel();
        }
        self.reservations.remove(&id)
    }

    /// Queues the request for `id`, which the machine found servable, for
    /// the data writer to build when it reaches it. The machine counts the
    /// request until the writer starts its answer.
    fn serve(&mut self, id: UnminedTxId) {
        match self.io.data.try_send(Outgoing::Serve(id)) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => self.io.stalled = true,
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }

    /// Encodes `message` and queues it on the stream of `kind`. A full queue
    /// says the peer stopped reading.
    fn send(&mut self, kind: u16, message: &TxRelayMessage) {
        let Ok(frame) = encode_frame(message) else {
            return;
        };
        let writer = if kind == TX_RELAY_DATA_KIND {
            &self.io.data
        } else {
            &self.io.state
        };
        match writer.try_send(Outgoing::Frame(frame, None)) {
            Ok(()) => count(message, "sent"),
            Err(mpsc::error::TrySendError::Full(_)) => self.io.stalled = true,
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }

    /// Sends the `Inv` the machine lets go now, if any, and asks again until
    /// it lets none go, so with more queued than one frame takes, it sets
    /// its own retry for after this frame is written. The machine decides
    /// when one may go from what the data writer and the `Inv` sender say,
    /// so while it waits, new announcements join the frame, and a session
    /// batches only under load.
    fn flush_adverts(&mut self) {
        loop {
            // One instant for the check, the machine's decision and the
            // payment, so a refill between two samples cannot let the
            // machine take ids the rate then refuses to pay for.
            let now = Instant::now();
            let (inv, base) = (&self.io.inv, self.wiring.base);
            let at = now.saturating_duration_since(base);
            let room = InvRoom {
                free: self.io.data.capacity(),
                paid: |bytes| match inv.ready(bytes, now) {
                    MeteredReady::Now => Paid::Now,
                    MeteredReady::At(paid) => Paid::At(paid.saturating_duration_since(base)),
                    MeteredReady::Unwritten => Paid::Unwritten,
                },
            };
            let entries = self.machine.next_inv(&self.view, room, at);
            if entries.is_empty() {
                return;
            }
            let message = TxRelayMessage::Inv(entries);
            let Ok(frame) = encode_frame(&message) else {
                return;
            };
            // The frame is within the bound the sender said the rate pays
            // for. Widening usize to u64 is lossless on supported targets.
            let Some(guard) = self.io.inv.pay(frame.payload.len() as u64, now) else {
                debug_assert!(false, "the frame is within the bound the rate paid for");
                return;
            };
            // The machine checked the writer had room, and the routine is the
            // only sender besides the writer's own serve jobs, so the frame
            // is queued.
            if self
                .io
                .data
                .try_send(Outgoing::Frame(frame, Some(guard)))
                .is_ok()
            {
                count(&message, "sent");
            }
        }
    }
}

/// The machine's input for a peer that stopped reading. See
/// [`PeerMachine::stopped_reading`].
fn stopped_reading(machine: &mut PeerMachine, shared: &mut Shared, pool: &dyn PoolView, now: Time) {
    machine.stopped_reading(shared, pool, now);
}

/// Counts an admission, and for a rejection returns whether `may_cool` lets
/// it start a cooldown.
fn read_verdict(may_cool: &MayCool, verdict: Verdict) -> Option<bool> {
    match verdict {
        Verdict::Admitted => {
            metrics::counter!("zakura.p2p.tx_relay.tx.admitted").increment(1);
            None
        }
        Verdict::Rejected { tip, .. } => Some(may_cool(tip)),
        _ => None,
    }
}

/// Sets the node's currentness for a rejection's charge: the machine starts
/// a cooldown only when the node is current and its best tip is the
/// verdict's, exactly what `may_cool` answers.
fn set_currentness(shared: &mut Shared, verdict: Verdict, may_cool: Option<bool>) {
    if let (Verdict::Rejected { tip, .. }, Some(may_cool)) = (verdict, may_cool) {
        shared.set_current(may_cool);
        if may_cool {
            shared.set_best_tip(tip);
        }
    }
}

/// The closers of the sessions other than `stepped` that `effects` close,
/// with why. `stepped` performs its own close.
fn closers(
    node: &Node,
    effects: &[RelayEffect],
    stepped: Option<SessionId>,
) -> Vec<(Closer, CloseReason)> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            RelayEffect::Close { session, reason } if Some(*session) != stepped => node
                .sessions
                .get(session)
                .map(|handle| (handle.close.clone(), *reason)),
            _ => None,
        })
        .collect()
}

/// Leaves each effect on another session as mail for the sessions it
/// reaches, and returns the wakes the step owes, by the machine's rules:
/// each session an effect reaches, and every session but `stepped` once
/// [`Marks::wake_all`] says so. `stepped` read the ring in its own step.
fn route(node: &mut Node, marks: Marks, stepped: Option<SessionId>) -> Vec<Arc<Notify>> {
    let mut wakes = Vec::new();
    for effect in std::mem::take(&mut node.shared.node) {
        let live = node.sessions.iter();
        let (to, mail) = effect.reach(live.map(|(session, handle)| (*session, handle.peer)));
        for session in to {
            if let Some(handle) = node.sessions.get_mut(&session) {
                handle.mail.extend(mail);
                wakes.push(handle.wake.clone());
            }
        }
    }
    if marks.wake_all(&node.shared) {
        let others = node
            .sessions
            .iter()
            .filter(|(session, _)| Some(**session) != stepped);
        wakes.extend(others.map(|(_, handle)| handle.wake.clone()));
    }
    wakes
}

/// Publishes how many sessions are live.
fn count_sessions(node: &Node) {
    // A session count is far below `f64`'s exact integer range.
    metrics::gauge!("zakura.p2p.tx_relay.sessions").set(node.sessions.len() as f64);
}

/// The reactor task: answers the reserve calls and verdicts of sessions
/// that ended, as they arrive. A grant goes straight back, and a verdict's
/// charge reaches its peer's live sessions.
async fn run_reactor(
    node: Arc<Mutex<Node>>,
    pool: PoolReader,
    may_cool: MayCool,
    base: Instant,
    mut handed: mpsc::UnboundedReceiver<Calls>,
) {
    let mut calls: Calls = FuturesUnordered::new();
    let mut open = true;
    loop {
        zakura_rt::select! {
            batch = handed.recv(), if open => match batch {
                Some(batch) => calls.extend(batch),
                None => open = false,
            },
            Some(result) = calls.next(), if !calls.is_empty() => {
                let now = Instant::now().saturating_duration_since(base);
                ended_result(&node, &pool, &may_cool, result, now);
            }
            else => return,
        }
    }
}

/// Applies one pool result of a session that ended.
fn ended_result(
    node: &Arc<Mutex<Node>>,
    pool: &PoolReader,
    may_cool: &MayCool,
    result: PoolResult,
    now: Time,
) {
    let mut grant = None;
    let (effects, closes, wakes) = {
        let mut node = node.lock().expect("the node is never poisoned");
        let node = &mut *node;
        let marks = Marks::of(&node.shared);
        match result {
            PoolResult::Reserved { call, answer } => {
                let answer = match answer {
                    Ok(reservation) => {
                        let granted = Granted {
                            reservation: reservation.id(),
                            cancelled: reservation.cancellation().is_cancelled(),
                        };
                        grant = Some(reservation);
                        Ok(granted)
                    }
                    Err(refused) => Err(unreserved(refused)),
                };
                if !node.shared.ended_call_answer(call, answer) {
                    node.shared.answer_unknown(answer);
                }
            }
            PoolResult::Verdict { waiter, verdict } => {
                let verdict = verdict_of(verdict);
                let cools = read_verdict(may_cool, verdict);
                set_currentness(&mut node.shared, verdict, cools);
                let view = pool
                    .snapshot()
                    .map_or_else(SnapshotView::empty, SnapshotView);
                node.shared.ended_verdict(&view, waiter, verdict, now);
            }
            PoolResult::Cancelled { .. } | PoolResult::Done => {}
        }
        let effects = std::mem::take(&mut node.shared.effects);
        let closes = closers(node, &effects, None);
        let wakes = route(node, marks, None);
        (effects, closes, wakes)
    };
    // The only other effects here give a grant back, which dropping it does.
    drop((grant, effects));
    for wake in wakes {
        wake.notify_one();
    }
    for (close, reason) in closes {
        close.close(close_reason(reason));
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

/// The recorded cause for a close the machine asked for, as the close
/// table's row for it gives.
fn close_reason(reason: CloseReason) -> &'static str {
    match reason {
        CloseReason::Violation(Violation::Undecodable) => "tx-relay: undecodable transaction",
        CloseReason::Violation(Violation::RequestedTwice) => "tx-relay: id requested twice",
        CloseReason::Violation(Violation::TooManyRequests) => "tx-relay: too many requests",
        CloseReason::Violation(_) => "tx-relay: protocol violation",
        CloseReason::Ban => "tx-relay: banned transaction",
        CloseReason::RepeatedCooldown => "tx-relay: repeated cooldown",
        CloseReason::CooldownsFull => "tx-relay: cooldown table full",
        CloseReason::Unresponsive => "tx-relay: unanswered request",
        CloseReason::StoppedReading => "tx-relay: peer stopped reading",
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

#[cfg(test)]
mod tests;
