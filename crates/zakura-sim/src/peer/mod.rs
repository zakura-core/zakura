//! Scripted peers for `header-sync/wire` (M5-C): real QUIC endpoints on the
//! run's `SimNet` that run the Zakura handshake from the public wire types.
//!
//! A peer either dials the node from a fresh address per dial or listens
//! for the node's dial. It offers or accepts the capability mask its
//! [`PeerSpec`] names, keeps the header stream it negotiated open and read,
//! and carries out its scripted [`PeerAction`]s.
//!
//! Actions follow M4's `Control` pattern. [`PeerControl`] offers a peer's
//! next action as `peer:<name>:<action>` only while the peer is settled and
//! idle, so the `Plan` picks when it happens, `execute` only queues it, and
//! the peer's task carries it out. So at most one action is queued per
//! peer.

pub(crate) mod handshake;

use std::{
    cell::Cell,
    future::Future,
    net::{Ipv4Addr, SocketAddr},
    pin::Pin,
    rc::Rc,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use zakura_network::zakura::{
    v9::RegtestMessage, P2P_V2_ALPN, ZAKURA_CAP_HEADER_SYNC, ZAKURA_HEADER_SYNC_STREAM_VERSION,
    ZAKURA_STREAM_HEADER_SYNC,
};
use zakura_quic::{
    sim::{SeededInputs, SimNet},
    Acceptor, Admit, Conn, IncomingInfo, NodeAddr, QuicBindConfig, QuicConfig, QuicEndpoint,
    RecvStream, SendStream, VarInt,
};
use zakura_rt::sync::{mpsc, watch};
use zakura_scheduler::SimInstant;

use self::handshake::{Echo, Local};
use crate::{ActionSource, Boundary, Identity, ObservationLog};

/// The node's address on the run's network.
pub const NODE_ADDR: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 4000);

/// How long a peer waits after its connection ends before it dials again:
/// the production handshake timeout, so the node has torn the old connection
/// down and doesn't refuse the new one as a duplicate.
const REDIAL_SETTLE: Duration = Duration::from_secs(10);

/// How long an [`PeerAction::Open`] waits for the node to refuse the stream.
const REFUSAL_WAIT: Duration = Duration::from_secs(10);

/// The address peer `index` binds for its `dial`-th connection, from 0:
/// `10.<index + 1>.0.<dial + 1>:4000`. A listening peer binds its first.
/// So every connection incarnation has its own 4-tuple.
///
/// # Panics
///
/// If `index` or `dial` is past 253, which no scenario reaches.
pub fn peer_addr(index: usize, dial: usize) -> SocketAddr {
    let octet = |n: usize| {
        n.checked_add(1)
            .and_then(|n| u8::try_from(n).ok())
            .filter(|octet| *octet < 255)
            .expect("a scenario has fewer than 254 peers and dials")
    };
    SocketAddr::from((Ipv4Addr::new(10, octet(index), 0, octet(dial)), 4000))
}

/// The peer that binds `addr`, by index, if [`peer_addr`] gives it.
pub fn peer_of(addr: SocketAddr) -> Option<usize> {
    let SocketAddr::V4(v4) = addr else {
        return None;
    };
    let [ten, index, zero, dial] = v4.ip().octets();
    let given = |octet: u8| (1..255).contains(&octet);
    (ten == 10 && zero == 0 && given(index) && given(dial) && v4.port() == 4000)
        .then(|| usize::from(index - 1))
}

/// The run's index for peer `index`'s keys: 0 is the node's.
pub(crate) fn key_index(index: usize) -> u32 {
    u32::try_from(index + 1).expect("a scenario has few peers")
}

/// Which end of a peer's connection dials.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Dialer {
    /// The peer dials the node, and opens the header stream.
    Peer,
    /// The node dials the peer, and opens the header stream.
    Node,
}

/// One scripted peer, named `peer-<i>` by its index in the scenario.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerSpec {
    /// Which end dials.
    pub dialer: Dialer,
    /// The capability mask it offers as the dialer, or intersects with the
    /// node's hello as the responder.
    pub capabilities: u64,
    /// Whether, as the responder, it echoes the node's nonce wrongly.
    #[serde(default)]
    pub bad_nonce_echo: bool,
    /// Its actions, in order.
    #[serde(default)]
    pub script: Vec<PeerAction>,
}

impl PeerSpec {
    /// A peer that dials and offers header sync alone.
    pub fn dialing() -> Self {
        PeerSpec {
            dialer: Dialer::Peer,
            capabilities: ZAKURA_CAP_HEADER_SYNC,
            bad_nonce_echo: false,
            script: Vec::new(),
        }
    }

    /// A peer that the node dials, accepting header sync alone.
    pub fn listening() -> Self {
        PeerSpec {
            dialer: Dialer::Node,
            ..PeerSpec::dialing()
        }
    }
}

/// Something a scripted peer does on its connection when the `Plan` picks
/// it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case", deny_unknown_fields)]
pub enum PeerAction {
    /// Opens a stream with a prelude for `kind` at `version`, and waits for
    /// the node to reset it or close the connection, for at most 10 s.
    Open {
        /// The stream kind.
        kind: u16,
        /// The stream version.
        version: u16,
    },
    /// Finishes the peer's side of its header stream. As the responder it
    /// then waits for the node to open the next one.
    FinishHeader,
    /// Closes the connection with application code 0, as `Conn::close`.
    Close,
    /// After a [`PeerAction::Close`], dials again from a fresh address, once
    /// the production handshake timeout has passed. Only a dialing peer can.
    Redial,
}

impl PeerAction {
    /// The action's name in its identity.
    pub fn name(self) -> &'static str {
        match self {
            PeerAction::Open { .. } => "open",
            PeerAction::FinishHeader => "finish-header",
            PeerAction::Close => "close",
            PeerAction::Redial => "redial",
        }
    }
}

/// Why a peer couldn't go on, which ends the run as a harness failure: the
/// scenario asked for something its peer can't do, or the node opened a
/// header stream at a version the peer doesn't speak. The first failure
/// recorded stands. Stream tasks record into it directly, so a failure
/// still counts after its connection closes.
#[derive(Clone, Debug, Default)]
pub struct PeerFailure(Arc<Mutex<Option<String>>>);

impl PeerFailure {
    /// Records `why`, unless a failure is already recorded. The lock is
    /// never held across another call.
    fn record(&self, why: String) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert(why);
    }

    /// The failure recorded, if any.
    fn get(&self) -> Option<String> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// Whether a peer may be offered its next action, shared by its task and
/// [`PeerControl`]. A queued action keeps the peer unready until its task
/// takes it, so the task running in between can't make it ready again.
#[derive(Clone, Debug, Default)]
pub(crate) struct Readiness(Rc<Cell<Phase>>);

/// A peer's [`Readiness`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Phase {
    /// Doing something, or waiting to settle.
    #[default]
    Busy,
    /// Settled and idle.
    Ready,
    /// An action is queued for the task.
    Queued,
}

impl Readiness {
    /// Whether the peer may be offered its next action.
    fn ready(&self) -> bool {
        self.0.get() == Phase::Ready
    }

    /// The task's view: ready while `settled`, unless an action is queued.
    fn set(&self, settled: bool) {
        if self.0.get() != Phase::Queued {
            self.0.set(if settled { Phase::Ready } else { Phase::Busy });
        }
    }

    /// An action was queued for the task.
    fn queued(&self) {
        self.0.set(Phase::Queued);
    }

    /// The task took its queued action.
    fn taken(&self) {
        self.0.set(Phase::Busy);
    }
}

/// One peer's script, as [`PeerControl`] offers it.
struct Slot {
    name: String,
    script: Vec<PeerAction>,
    /// The next action to offer.
    next: usize,
    readiness: Readiness,
    to_task: mpsc::Sender<PeerAction>,
}

/// Offers each peer's next scripted action, as `peer:<name>:<action>`,
/// while that peer is ready for it.
pub struct PeerControl {
    slots: Vec<Slot>,
    failure: PeerFailure,
}

impl PeerControl {
    /// No peers yet. Failures land in `failure`.
    pub(crate) fn new(failure: PeerFailure) -> Self {
        PeerControl {
            slots: Vec::new(),
            failure,
        }
    }

    /// Adds peer `name`'s script. Returns the readiness its task keeps, and
    /// the receiver it takes actions from, which holds one.
    pub(crate) fn add(
        &mut self,
        name: &str,
        script: Vec<PeerAction>,
    ) -> (Readiness, mpsc::Receiver<PeerAction>) {
        let (to_task, commands) = mpsc::channel(1);
        let readiness = Readiness::default();
        self.slots.push(Slot {
            name: name.to_owned(),
            script,
            next: 0,
            readiness: readiness.clone(),
            to_task,
        });
        (readiness, commands)
    }
}

impl ActionSource for PeerControl {
    /// Each ready peer's next action.
    fn enabled(&self, _: SimInstant, out: &mut Vec<(Identity, u64)>) {
        for (index, slot) in (0..).zip(&self.slots) {
            if let Some(action) = slot
                .script
                .get(slot.next)
                .filter(|_| slot.readiness.ready())
            {
                out.push((Identity::peer(&slot.name, action.name()), index));
            }
        }
    }

    /// None: a peer's readiness changes only when its task runs.
    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    /// Queues the peer's next action and marks it busy until its task is
    /// done with it.
    fn execute(&mut self, local: u64, _: &mut ObservationLog) {
        let slot = usize::try_from(local)
            .ok()
            .and_then(|index| self.slots.get_mut(index))
            .expect("`enabled` offered this peer at this step");
        slot.readiness.queued();
        slot.to_task
            .try_send(slot.script[slot.next])
            .expect("a peer is offered an action only while its last one is done");
        slot.next += 1;
    }

    /// Fails the run once a peer couldn't carry out an action.
    fn observe(&mut self, _: &mut ObservationLog) -> Result<(), String> {
        match self.failure.get() {
            Some(failure) => Err(format!("PeerActionFailed: {failure}")),
            None => Ok(()),
        }
    }
}

/// What a peer's task needs.
pub(crate) struct PeerTask {
    /// Its name, `peer-<i>`.
    pub(crate) name: String,
    /// Its index in the scenario.
    pub(crate) index: usize,
    pub(crate) spec: PeerSpec,
    /// The seed of its keys and endpoint inputs.
    pub(crate) seed: [u8; 32],
    pub(crate) net: SimNet,
    /// The node's identity and address.
    pub(crate) node: NodeAddr,
    pub(crate) commands: mpsc::Receiver<PeerAction>,
    pub(crate) readiness: Readiness,
    pub(crate) failure: PeerFailure,
    /// Set once a listening peer serves, so the node's dial finds it.
    pub(crate) listening: watch::Sender<bool>,
}

/// How a connection's service ended.
enum Ended {
    /// The connection closed.
    Closed,
    /// The peer can't go on, and has recorded why.
    Failed,
}

/// What a stream task tells its peer.
enum StreamEvent {
    /// The node opened a header stream: the peer's half to write on.
    Header { id: u64, send: SendStream },
    /// The node's first whole frame arrived on header stream `id`.
    FirstFrame { id: u64 },
}

/// One socket at `addr`, with room for the streams a scenario opens.
fn bind_config(addr: SocketAddr) -> QuicBindConfig {
    QuicBindConfig {
        addrs: vec![addr],
        max_bidi_streams: 16,
        max_connections: 16,
        dial_reserve: 0,
        max_closing_connections: 16,
    }
}

/// Hands each connection the node opens to the peer's task, and holds it
/// until it closes.
struct Forward(mpsc::Sender<Conn>);

impl Acceptor for Forward {
    /// Accepts every attempt.
    fn admit(&self, _: &IncomingInfo) -> Admit {
        Admit::Accept
    }

    /// Zakura's ALPN.
    fn alpns(&self) -> Vec<Vec<u8>> {
        vec![P2P_V2_ALPN.to_vec()]
    }

    /// Forwards the connection, then holds it until it closes.
    fn handle(&self, conn: Conn) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let forward = self.0.clone();
        Box::pin(async move {
            if forward.send(conn.clone()).await.is_ok() {
                conn.closed().await;
            }
        })
    }
}

/// Reads frames from the node on header stream `id` until it ends, and
/// reports the first whole one. Every read is drained, so the node's writer
/// never stalls on this peer.
async fn read_frames(id: u64, mut recv: RecvStream, events: mpsc::UnboundedSender<StreamEvent>) {
    let cap = usize::try_from(RegtestMessage::LAYOUT[0].frame_cap).unwrap_or(usize::MAX);
    let mut first = true;
    loop {
        let mut header = [0; 8];
        if recv.read_exact(&mut header).await.is_err() {
            return;
        }
        let len = usize::try_from(u32::from_le_bytes([
            header[4], header[5], header[6], header[7],
        ]))
        .unwrap_or(usize::MAX);
        if len > cap {
            return;
        }
        let mut payload = vec![0; len];
        if recv.read_exact(&mut payload).await.is_err() {
            return;
        }
        if std::mem::take(&mut first) && events.send(StreamEvent::FirstFrame { id }).is_err() {
            return;
        }
    }
}

/// Serves one stream the node opened on peer `name`'s connection: a header
/// stream goes to the peer and is read, and any other is held and drained
/// until it ends. A header stream at a version the peer doesn't speak fails
/// the run through `failure` instead, since the node only opens the version
/// it negotiated.
async fn serve_stream(
    send: SendStream,
    mut recv: RecvStream,
    events: mpsc::UnboundedSender<StreamEvent>,
    name: String,
    failure: PeerFailure,
) {
    let Ok(prelude) = handshake::read_prelude(&mut recv).await else {
        return;
    };
    if prelude.stream_kind == ZAKURA_STREAM_HEADER_SYNC {
        let id = recv.id();
        if prelude.stream_version != ZAKURA_HEADER_SYNC_STREAM_VERSION {
            failure.record(format!(
                "{name}: the node opened header stream {id} at version {}, not \
                 {ZAKURA_HEADER_SYNC_STREAM_VERSION}",
                prelude.stream_version
            ));
            return;
        }
        if events.send(StreamEvent::Header { id, send }).is_ok() {
            read_frames(id, recv, events).await;
        }
    } else {
        let _held = send;
        while matches!(recv.read_chunk(64 * 1024).await, Ok(Some(_))) {}
    }
}

/// Accepts every stream the node opens on peer `name`'s `conn`, each in its
/// own task, with failures going to `failure`.
async fn accept_streams(
    conn: Conn,
    events: mpsc::UnboundedSender<StreamEvent>,
    name: String,
    failure: PeerFailure,
) {
    while let Ok((send, recv)) = conn.accept_bi().await {
        let stream = serve_stream(send, recv, events.clone(), name.clone(), failure.clone());
        zakura_rt::task::spawn_named("peer_stream", stream);
    }
}

impl PeerTask {
    /// Runs the peer until the run ends.
    pub(crate) async fn run(mut self) {
        let local = Local::regtest();
        match self.spec.dialer {
            Dialer::Peer => self.dial(&local).await,
            Dialer::Node if self.spec.script.contains(&PeerAction::Redial) => {
                self.fail("only a dialing peer can redial");
            }
            Dialer::Node => self.listen(&local).await,
        }
        // The endpoint stays up, so nothing the node holds closes under it.
        std::future::pending::<()>().await;
    }

    /// Records why the peer can't go on, as [`PeerFailure`] describes.
    fn fail(&self, why: impl std::fmt::Display) -> Ended {
        self.failure.record(format!("{}: {why}", self.name));
        Ended::Failed
    }

    /// Binds the endpoint for the `dial`-th connection.
    fn bind(&self, dial: usize) -> QuicEndpoint {
        let inputs = u32::try_from(dial)
            .ok()
            .and_then(|dial| dial.checked_mul(256))
            .and_then(|base| base.checked_add(key_index(self.index)))
            .expect("a peer dials fewer than 2^24 times");
        QuicEndpoint::bind_with(
            SeededInputs::node_key(self.seed, key_index(self.index)),
            &bind_config(peer_addr(self.index, dial)),
            &QuicConfig::default(),
            self.net.clone(),
            SeededInputs::new(self.seed, inputs).endpoint_inputs(),
        )
        .expect("a peer binds a fresh address")
    }

    /// Dials the node, from a fresh address each time, until a connection
    /// ends without a [`PeerAction::Redial`] after it.
    async fn dial(&mut self, local: &Local) {
        let mut dial = 0;
        loop {
            let quic = self.bind(dial);
            let Ok(conn) = quic.connect(self.node.clone(), &[P2P_V2_ALPN]).await else {
                return;
            };
            let Ok(negotiated) =
                handshake::initiate(&conn, quic.local_id(), self.spec.capabilities, local).await
            else {
                return;
            };
            if let Ended::Failed = self.serve(&conn, negotiated).await {
                return;
            }
            // Only a redial may follow a closed connection.
            self.readiness.set(true);
            let next = self.commands.recv().await;
            self.readiness.taken();
            match next {
                Some(PeerAction::Redial) => {}
                Some(action) => {
                    self.fail(format!(
                        "can't {} after its connection closed",
                        action.name()
                    ));
                    return;
                }
                None => return,
            }
            quic.shutdown().await;
            zakura_rt::time::sleep(REDIAL_SETTLE).await;
            dial += 1;
        }
    }

    /// Listens for the node's dials, and answers each one's handshake.
    async fn listen(&mut self, local: &Local) {
        let quic = self.bind(0);
        let (forward, mut incoming) = mpsc::channel(4);
        quic.serve(Forward(forward))
            .expect("a peer's endpoint serves");
        self.listening.send_replace(true);
        let echo = if self.spec.bad_nonce_echo {
            Echo::Wrong
        } else {
            Echo::Faithful
        };
        while let Some(conn) = incoming.recv().await {
            let answered =
                handshake::respond(&conn, conn.remote_id(), self.spec.capabilities, echo, local)
                    .await;
            let ended = match answered {
                Ok(negotiated) => self.serve(&conn, negotiated).await,
                Err(_) => {
                    conn.closed().await;
                    Ended::Closed
                }
            };
            if let Ended::Failed = ended {
                return;
            }
        }
    }

    /// Serves one connection whose handshake negotiated `negotiated`, until
    /// it closes. The peer is ready for an action while it's idle and
    /// settled: no header stream is expected, or the node's first frame has
    /// arrived on it, which the node sends once it admits the session.
    async fn serve(&mut self, conn: &Conn, negotiated: u64) -> Ended {
        let header_sync = negotiated & ZAKURA_CAP_HEADER_SYNC != 0;
        let (events, mut stream_events) = mpsc::unbounded_channel();
        let accept = accept_streams(
            conn.clone(),
            events.clone(),
            self.name.clone(),
            self.failure.clone(),
        );
        zakura_rt::task::spawn_named("peer_accept", accept);
        // The header stream the peer writes on, and whether it's settled.
        let mut header: Option<(u64, SendStream)> = None;
        let mut settled = !header_sync;
        if header_sync && self.spec.dialer == Dialer::Peer {
            let Ok((mut send, recv)) = conn.open_bi().await else {
                return Ended::Closed;
            };
            let opened = handshake::write_prelude(
                &mut send,
                ZAKURA_STREAM_HEADER_SYNC,
                ZAKURA_HEADER_SYNC_STREAM_VERSION,
            )
            .await;
            if opened.is_err() {
                return Ended::Closed;
            }
            let id = send.id();
            zakura_rt::task::spawn_named("peer_header", read_frames(id, recv, events.clone()));
            header = Some((id, send));
        }
        loop {
            self.readiness.set(settled);
            zakura_rt::select! {
                Some(event) = stream_events.recv() => match event {
                    StreamEvent::Header { id, send } => header = Some((id, send)),
                    StreamEvent::FirstFrame { id } => {
                        settled |= header.as_ref().is_some_and(|(current, _)| *current == id);
                    }
                },
                Some(action) = self.commands.recv() => {
                    self.readiness.taken();
                    match action {
                        PeerAction::Open { kind, version } => open(conn, kind, version).await,
                        PeerAction::FinishHeader => {
                            let Some((_, mut send)) = header.take() else {
                                return self.fail("can't finish-header without a header stream");
                            };
                            let _ = send.finish();
                            // A responder waits for the node to reopen it.
                            settled = self.spec.dialer == Dialer::Peer;
                        }
                        PeerAction::Close => {
                            conn.close(VarInt::from_u32(0), b"close");
                            conn.closed().await;
                            return Ended::Closed;
                        }
                        PeerAction::Redial => return self.fail("can't redial a live connection"),
                    }
                }
                _ = conn.closed() => {
                    self.readiness.set(false);
                    return Ended::Closed;
                }
            }
        }
    }
}

/// Opens a `kind` stream at `version` on `conn` and waits, for at most
/// [`REFUSAL_WAIT`], for the node to reset it, finish it, or close the
/// connection.
async fn open(conn: &Conn, kind: u16, version: u16) {
    let Ok((mut send, mut recv)) = conn.open_bi().await else {
        return;
    };
    if handshake::write_prelude(&mut send, kind, version)
        .await
        .is_err()
    {
        return;
    }
    let refused = async { while matches!(recv.read_chunk(64 * 1024).await, Ok(Some(_))) {} };
    let _ = zakura_rt::time::timeout(REFUSAL_WAIT, refused).await;
}
