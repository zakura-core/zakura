//! Scripted `tx-relay` peers: real QUIC endpoints on the run's `SimNet`
//! that dial the node, open `tx-relay`'s data and state streams, and run a
//! composed script of [`RelayStep`]s on them.
//!
//! A peer sends its `Hello` as its [`HelloMode`] says, answers each of the
//! node's `GetTxs` as its [`Answer`] says, and runs its steps one after
//! another. Its `Inv`s pay its own byte bucket, as a conformant sender's
//! do. No step floods them past the node's bucket: the node refills that
//! bucket while it reads a flood, by an amount the run's schedule sets, so
//! no peer can tell which frame overran it.
//!
//! Every frame a peer writes is labelled before the write, as the header
//! and discovery peers' are: the frame model over the stream's rows for the
//! header, a model of the node's `Inv` byte bucket for the metered row, and
//! `tx-relay`'s own rules for the content. A content fault also names the
//! close-table row it breaks, in the run's [`RelayEvidence`] ledger. The
//! labels and the writes go to [`PeerRecords`], which the evidence places
//! on the node's connection. A request past the node's `Hello`, or for an
//! id already outstanding, breaks a rule whose close depends on when the
//! node's writer frees an earlier request's place, which no peer sees, so
//! those frames are labelled conformant and their closes are neutral.
//!
//! [`RelayEvidence`]: crate::domain::tx_relay::RelayEvidence

use std::{
    collections::{BTreeSet, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};
use zakura_chain::transaction::{Hash, UnminedTxId};
use zakura_network::zakura::{
    tx_relay_sim::{
        TxRelayMessage, INV_RATE, KNOWN_CAP, MAX_INV_ENTRIES, MAX_SERVES, MAX_TX_BYTES,
        PENDING_PER_PEER, TX_RELAY_DATA_KIND, TX_RELAY_STATE_KIND, TX_RELAY_STREAMS,
        ZAKURA_CAP_TX_RELAY,
    },
    wire_codec::{decode_frame, encode_frame},
    CloseRow, Frame, MessageRule, Stream, P2P_V2_ALPN,
};
use zakura_quic::{
    sim::{SeededInputs, SimNet},
    Conn, NodeAddr, QuicConfig, QuicEndpoint, RecvStream, SendStream,
};
use zakura_rt::sync::mpsc;

use super::{
    bind_config,
    evidence::{PeerRecord, PeerRecords, RecordBody, Remote},
    frame_bytes, handshake,
    handshake::Local,
    key_index, peer_addr, PeerFailure,
};
use crate::{
    domain::tx_relay::{lock, SharedRelay, TxKey},
    model::{FrameHeader, FrameModel, Label, Labels, ReaderEnd},
    net::WireOrder,
    observe::StreamId,
};

/// How a peer sends its `Hello`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HelloMode {
    /// First on the state stream, as each session opens.
    #[default]
    First,
    /// Only when a [`RelayStep::Hello`] step runs.
    Scripted,
}

/// How a peer answers the node's `GetTxs`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Answer {
    /// With the body it holds, or `NotFound` for an id it doesn't.
    #[default]
    Serve,
    /// With `NotFound`, whatever it announced.
    NotFound,
    /// Never, so each request runs past its deadline: a stall.
    Never,
    /// With the body, after announcing it a byte larger: row 10.
    WrongSize,
    /// With another held body of the same size under the requested id:
    /// row 11.
    WrongId,
    /// With bytes of the announced size that don't decode: row 12.
    Undecodable,
    /// With the body, then a `NotFound` for the same request: row 9.
    Twice,
}

impl Answer {
    /// Every answer, in the order a composed script's byte picks them. A
    /// new one goes at the end, with a bump of the harness's generator.
    pub const ALL: [Answer; 7] = [
        Answer::Serve,
        Answer::NotFound,
        Answer::Never,
        Answer::WrongSize,
        Answer::WrongId,
        Answer::Undecodable,
        Answer::Twice,
    ];

    /// Whether the answer keeps every wire rule.
    pub fn conformant(self) -> bool {
        matches!(self, Answer::Serve | Answer::NotFound | Answer::Never)
    }
}

/// One of `tx-relay`'s rows, as a header fault names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RelayRow {
    /// `Hello`, on the state stream.
    Hello,
    /// `GetTxs`, on the state stream.
    GetTxs,
    /// `NotFound`, on the state stream.
    NotFound,
    /// `Inv`, on the data stream.
    Inv,
    /// `Tx`, on the data stream.
    Tx,
}

impl RelayRow {
    /// Every row, state stream first.
    pub const ALL: [RelayRow; 5] = [
        RelayRow::Hello,
        RelayRow::GetTxs,
        RelayRow::NotFound,
        RelayRow::Inv,
        RelayRow::Tx,
    ];

    /// Whether it travels on the data stream.
    pub fn on_data(self) -> bool {
        matches!(self, RelayRow::Inv | RelayRow::Tx)
    }
}

/// How a frame's header breaks its row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HeaderFault {
    /// A reserved flag is set.
    ReservedFlags,
    /// The payload is shorter than the row's smallest.
    ShortPayload,
    /// The payload is one byte past the row's largest.
    LongPayload,
}

/// One step of a composed script. A step that can't run yet, such as a
/// fetch with nothing announced, is skipped.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RelayStep {
    /// Announces its next held body.
    Announce,
    /// Announces every held body it hasn't.
    AnnounceAll,
    /// Announces `count` fresh ids no one holds, paced by its bucket.
    AnnounceFake {
        /// How many.
        count: u16,
    },
    /// Announces [`PENDING_PER_PEER`] and 64 more fresh ids, paced, past
    /// the node's pending set.
    FillPending,
    /// Announces [`KNOWN_CAP`] and 64 more fresh ids, paced, past the
    /// node's skip set.
    FillKnown,
    /// Asks for the oldest id the node announced that it hasn't fetched.
    Fetch,
    /// Asks for up to [`MAX_SERVES`] ids the node announced, in one write.
    FetchBurst,
    /// Asks for an id no one holds.
    AskUnknown,
    /// Asks for universe body `tx`, which the node may hold and never
    /// announced to it.
    Probe {
        /// The body, by index in the universe.
        tx: usize,
    },
    /// Sends its `Hello`. A second one in a session breaks row 4.
    Hello,
    /// Ends its session and opens the next on the same connection.
    Reopen,
    /// Waits `secs` seconds.
    Wait {
        /// How long.
        secs: u16,
    },
    /// A first `Hello` that serves no requests: row 5.
    BadHello,
    /// A `Tx` for universe body `tx`, which the node never asked for: row 9.
    UnrequestedTx {
        /// The body, by index in the universe.
        tx: usize,
    },
    /// A `NotFound` for an id the node never asked for: row 9.
    UnrequestedNotFound,
    /// An `Inv` whose count promises more entries than it carries: row 3.
    MalformedInv,
    /// A frame of `row` whose header breaks the table.
    HeaderFault {
        /// The row.
        row: RelayRow,
        /// How its header breaks it.
        fault: HeaderFault,
    },
    /// A frame of a type the stream doesn't declare: the data stream if
    /// `data`, the state stream otherwise.
    UndeclaredType {
        /// Which stream.
        data: bool,
    },
    /// `GetTxs` for ids the node announced, the last twice, in one write,
    /// while the peer stops reading its data stream: row 7, neutral.
    RequestTwice,
    /// More `GetTxs` than the node's `Hello` serves and its writer starts,
    /// in one write, for ids it announced, while the peer stops reading its
    /// data stream: row 8, neutral.
    TooManyRequests,
}

impl RelayStep {
    /// The steps that keep every wire rule. A new one goes at the end, with
    /// a bump of the harness's generator.
    pub const CONFORMANT: [RelayStep; 14] = [
        RelayStep::Announce,
        RelayStep::AnnounceAll,
        RelayStep::AnnounceFake { count: 1 },
        RelayStep::AnnounceFake { count: 64 },
        RelayStep::AnnounceFake { count: 1_024 },
        RelayStep::Fetch,
        RelayStep::FetchBurst,
        RelayStep::AskUnknown,
        RelayStep::Probe { tx: 0 },
        RelayStep::Probe { tx: 8 },
        RelayStep::Hello,
        RelayStep::Reopen,
        RelayStep::Wait { secs: 1 },
        RelayStep::Wait { secs: 10 },
    ];

    /// The steps that break a rule, after [`Self::CONFORMANT`] in a
    /// composed script's alphabet. A new one goes at the end, with a bump of
    /// the harness's generator. The bound fillers come last: they announce
    /// tens of thousands of ids. The header faults close at once, one per
    /// row and fault.
    pub const FAULTS: [RelayStep; 25] = [
        RelayStep::BadHello,
        RelayStep::UnrequestedTx { tx: 0 },
        RelayStep::UnrequestedNotFound,
        RelayStep::MalformedInv,
        RelayStep::UndeclaredType { data: true },
        RelayStep::UndeclaredType { data: false },
        RelayStep::RequestTwice,
        RelayStep::TooManyRequests,
        RelayStep::FillPending,
        RelayStep::FillKnown,
        header_fault(RelayRow::Hello, HeaderFault::ReservedFlags),
        header_fault(RelayRow::Hello, HeaderFault::ShortPayload),
        header_fault(RelayRow::Hello, HeaderFault::LongPayload),
        header_fault(RelayRow::GetTxs, HeaderFault::ReservedFlags),
        header_fault(RelayRow::GetTxs, HeaderFault::ShortPayload),
        header_fault(RelayRow::GetTxs, HeaderFault::LongPayload),
        header_fault(RelayRow::NotFound, HeaderFault::ReservedFlags),
        header_fault(RelayRow::NotFound, HeaderFault::ShortPayload),
        header_fault(RelayRow::NotFound, HeaderFault::LongPayload),
        header_fault(RelayRow::Inv, HeaderFault::ReservedFlags),
        header_fault(RelayRow::Inv, HeaderFault::ShortPayload),
        header_fault(RelayRow::Inv, HeaderFault::LongPayload),
        header_fault(RelayRow::Tx, HeaderFault::ReservedFlags),
        header_fault(RelayRow::Tx, HeaderFault::ShortPayload),
        header_fault(RelayRow::Tx, HeaderFault::LongPayload),
    ];
}

/// A [`RelayStep::HeaderFault`] of `row` by `fault`.
pub const fn header_fault(row: RelayRow, fault: HeaderFault) -> RelayStep {
    RelayStep::HeaderFault { row, fault }
}

/// One scripted peer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelayPeerSpec {
    /// When it dials, in milliseconds past the run's start.
    pub dial_at_ms: u64,
    /// The bodies it holds, by index in the universe, in the order it
    /// announces them.
    pub holds: Vec<usize>,
    /// How it sends its `Hello`.
    pub hello: HelloMode,
    /// How it answers the node's requests.
    pub answer: Answer,
    /// The gap between two of its answers, in milliseconds.
    pub answer_gap_ms: u64,
    /// Fresh ids it announces as each session opens, after its `Hello`.
    pub announce_on_open: u16,
    /// Whether its script may break rules. A step that would break one
    /// runs only if it may, and is skipped otherwise.
    pub faults: bool,
    /// Its steps, in order.
    pub steps: Vec<RelayStep>,
}

impl RelayPeerSpec {
    /// An honest peer that holds `holds`, announces them all and serves
    /// them.
    pub fn honest(holds: Vec<usize>) -> Self {
        RelayPeerSpec {
            dial_at_ms: 0,
            holds,
            hello: HelloMode::First,
            answer: Answer::Serve,
            answer_gap_ms: 0,
            announce_on_open: 0,
            faults: false,
            steps: vec![RelayStep::AnnounceAll],
        }
    }
}

/// The universe's bodies, as the peers serve them: each id with its
/// serialized bytes.
pub type Bodies = Arc<Vec<(UnminedTxId, Vec<u8>)>>;

/// What a scripted `tx-relay` peer's task needs.
pub(crate) struct RelayPeer {
    /// Its index in the scenario.
    pub(crate) index: usize,
    /// Its name, `peer-<i>`.
    pub(crate) name: String,
    /// The seed of its keys and endpoint inputs.
    pub(crate) seed: [u8; 32],
    pub(crate) net: SimNet,
    /// The node's identity and address.
    pub(crate) node: NodeAddr,
    pub(crate) spec: RelayPeerSpec,
    pub(crate) bodies: Bodies,
    /// Where it records its frames.
    pub(crate) records: PeerRecords,
    /// The run's wire-event stamps.
    pub(crate) order: WireOrder,
    pub(crate) failure: PeerFailure,
    /// The run's ledger of broken rows and node frames.
    pub(crate) evidence: SharedRelay,
    /// The node's `Inv` byte bucket depth on a connection.
    pub(crate) inv_depth: u64,
}

/// The QUIC receive windows a peer grants, the smallest QUIC allows, so a
/// peer that stops reading its data stream blocks the node's data writer
/// within one of the node's own bodies.
const RECEIVE_WINDOW: u32 = 64 * 1024;

/// The QUIC idle timeout of a peer and of the node: past a park's three
/// minutes, since a parked peer's connection carries no other service's
/// frames here.
pub const IDLE_TIMEOUT_SECS: u32 = 600;

/// The most answers the node starts while its peer reads nothing: the body
/// the peer's reader was reading, the one the node's writer blocks on, the
/// four the data stream's outbound queue holds, and one to spare.
const STARTED_WHILE_PAUSED: usize = 7;

/// How long a peer stops reading its data stream while it asks for a burst
/// of bodies, so the node's answers queue unstarted.
const READ_PAUSE: Duration = Duration::from_secs(2);

/// The time since the run's start.
fn now() -> Duration {
    zakura_scheduler::hooks::now()
        .expect("a peer runs inside a run")
        .since_start()
}

/// Sleeps until `at` since the run's start.
async fn sleep_until(at: Duration) {
    let wait = at.saturating_sub(now());
    if !wait.is_zero() {
        zakura_rt::time::sleep(wait).await;
    }
}

/// The `n`-th fresh id of peer `peer`: a legacy id no transaction has.
pub fn fake_id(peer: usize, n: u32) -> UnminedTxId {
    let mut hash = [0xEE; 32];
    hash[1] = u8::try_from(peer).unwrap_or(u8::MAX);
    hash[2..6].copy_from_slice(&n.to_le_bytes());
    UnminedTxId::from_legacy_id(Hash(hash))
}

/// The size a fresh id is announced with.
const FAKE_SIZE: u32 = 250;

/// The gap between two steps.
const STEP_GAP: Duration = Duration::from_millis(250);

/// How long a peer waits after its session ended before it opens the
/// next: well inside the node's stream open rate, and long enough that a
/// park's refusals stay few.
const REOPEN_WAIT: Duration = Duration::from_secs(10);

/// How long a scripted reopen waits between ending a session and opening
/// the next.
const REOPEN_PAUSE: Duration = Duration::from_millis(100);

/// A byte bucket: depth, rate in bytes a second, tokens, and when it last
/// refilled. The node's own buckets refill the same way.
#[derive(Clone, Copy, Debug)]
struct Bucket {
    depth: u64,
    rate: u64,
    tokens: u64,
    last: Duration,
}

impl Bucket {
    /// A full bucket of `depth` refilling at `rate` from `now`.
    fn new(depth: u64, rate: u64, now: Duration) -> Self {
        Bucket {
            depth,
            rate,
            tokens: depth,
            last: now,
        }
    }

    /// Adds what the time since the last refill earned, up to the depth.
    fn refill(&mut self, now: Duration) {
        let elapsed = now.saturating_sub(self.last).as_nanos();
        let earned = elapsed * u128::from(self.rate) / 1_000_000_000;
        if earned == 0 {
            return;
        }
        let room = u128::from(self.depth - self.tokens);
        if earned >= room {
            self.tokens = self.depth;
            self.last = now;
            return;
        }
        self.tokens += u64::try_from(earned).expect("below the room left in a u64");
        let took = (earned * 1_000_000_000).div_ceil(u128::from(self.rate));
        self.last += Duration::from_nanos(u64::try_from(took).unwrap_or(u64::MAX));
    }

    /// Pays `bytes` at `now` if the bucket holds them.
    fn try_pay(&mut self, bytes: u64, now: Duration) -> bool {
        self.refill(now);
        if self.tokens < bytes {
            return false;
        }
        self.tokens -= bytes;
        true
    }

    /// When the bucket holds `bytes`, from `now` on.
    fn ready_at(&mut self, bytes: u64, now: Duration) -> Duration {
        self.refill(now);
        let missing = u128::from(bytes.saturating_sub(self.tokens));
        if missing == 0 {
            return now;
        }
        let wait = (missing * 1_000_000_000).div_ceil(u128::from(self.rate.max(1)));
        self.last + Duration::from_nanos(u64::try_from(wait).unwrap_or(u64::MAX))
    }
}

/// One of a session's two streams, as the peer writes it.
struct Writer {
    stream: StreamId,
    rules: &'static [MessageRule],
    id: u64,
    send: SendStream,
    /// Frames written so far, the next one's `seq`.
    seq: u64,
}

/// What a stream reader hands the peer's task.
enum Event {
    /// A whole frame from the node on session `session`'s `kind` stream.
    Frame {
        session: u32,
        kind: u16,
        frame: Frame,
    },
    /// Session `session`'s stream ended.
    Ended { session: u32 },
}

/// One session: its two streams and its own counts.
struct Session {
    number: u32,
    data: Writer,
    state: Writer,
    /// `Hello`s it sent.
    hellos: u32,
    /// State frames it read from the node.
    node_state_frames: u64,
    /// `Hello`s it read from the node.
    node_hellos: u32,
    /// Ids the node announced on it, oldest first, that it hasn't fetched.
    announced: VecDeque<UnminedTxId>,
    /// Every id the node announced on it.
    told: BTreeSet<TxKey>,
    /// Until when, in milliseconds since the run's start, its data reader
    /// reads nothing.
    data_paused: Arc<AtomicU64>,
}

/// One frame to write: its header, payload, label and the close-table row
/// its content breaks, if any.
struct Out {
    header: FrameHeader,
    payload: Vec<u8>,
    family: Labels,
    row: Option<CloseRow>,
}

impl Out {
    /// `message`, encoded, labelled `family`, breaking `row`.
    fn of(message: &TxRelayMessage, family: Labels, row: Option<CloseRow>) -> Out {
        let frame = encode_frame(message).expect("a scripted message encodes");
        Out {
            header: FrameHeader {
                message_type: frame.message_type,
                flags: frame.flags,
                payload_len: frame.payload.len(),
            },
            payload: frame.payload,
            family,
            row,
        }
    }

    /// A conformant `message`.
    fn conformant(message: &TxRelayMessage) -> Out {
        Out::of(message, Labels::NONE, None)
    }

    /// `message`, breaking content row `row`.
    fn breaking(message: &TxRelayMessage, label: Label, row: CloseRow) -> Out {
        Out::of(message, Labels::of(label), Some(row))
    }
}

/// What one connection keeps across its sessions.
struct Connection {
    remote: Remote,
    /// The peer's own `Inv` bucket, which every session on the connection
    /// pays, as the node's does.
    sender: Bucket,
    /// A model of the node's receiving bucket for the peer's `Inv`s.
    receiver: Bucket,
    /// Requests the node made that the peer hasn't answered, oldest first.
    asked: VecDeque<UnminedTxId>,
    /// When the next answer may go.
    next_answer: Duration,
    /// Ids it announced, with the size it announced.
    announced: Vec<(UnminedTxId, u32)>,
    /// Held bodies announced so far.
    held_announced: usize,
    /// Fresh ids waiting to be announced, oldest first.
    to_announce: VecDeque<(UnminedTxId, u32)>,
    /// The next fresh id's number.
    next_fake: u32,
    /// The next session's number.
    next_session: u32,
}

impl RelayPeer {
    /// The rows of `tx-relay`'s stream of `kind`.
    fn rules(kind: u16) -> &'static [MessageRule] {
        Self::stream(kind)
            .messages
            .expect("tx-relay tables both its streams")
    }

    /// The stream of `kind`.
    fn stream(kind: u16) -> Stream {
        *TX_RELAY_STREAMS
            .iter()
            .find(|stream| stream.kind == kind)
            .expect("tx-relay has both kinds")
    }

    /// Records why the peer can't go on.
    fn fail(&self, why: impl std::fmt::Display) {
        self.failure.record(format!("{}: {why}", self.name));
    }

    /// Binds the peer's endpoint.
    fn bind(&self) -> QuicEndpoint {
        QuicEndpoint::bind_with(
            SeededInputs::node_key(self.seed, key_index(self.index)),
            &bind_config(peer_addr(self.index, 0)),
            &QuicConfig {
                stream_receive_window_bytes: RECEIVE_WINDOW,
                receive_window_bytes: RECEIVE_WINDOW,
                idle_timeout_secs: IDLE_TIMEOUT_SECS,
                ..QuicConfig::default()
            },
            self.net.clone(),
            SeededInputs::new(self.seed, key_index(self.index)).endpoint_inputs(),
        )
        .expect("a peer binds a fresh address")
    }

    /// Runs the peer until the run ends: it dials once, runs its script on
    /// the connection, and then idles.
    pub(crate) async fn run(self) {
        sleep_until(Duration::from_millis(self.spec.dial_at_ms)).await;
        let quic = self.bind();
        if let Ok(conn) = quic.connect(self.node.clone(), &[P2P_V2_ALPN]).await {
            let remote = Remote {
                addr: peer_addr(self.index, 0),
                shared_id: conn.shared_id(),
            };
            // The node takes the lower of both sides' idle timeouts, so the
            // peer offers the one its QUIC config has.
            let local = Local::regtest(None, self.records.clone())
                .with_idle_timeout_secs(IDLE_TIMEOUT_SECS);
            match handshake::initiate(&conn, quic.local_id(), ZAKURA_CAP_TX_RELAY, &local, remote)
                .await
            {
                Ok(negotiated) if negotiated & ZAKURA_CAP_TX_RELAY != 0 => {
                    self.connection(&conn, remote).await;
                }
                Ok(_) => self.fail("the node did not accept tx-relay"),
                Err(_) => {}
            }
            // Holding the connection keeps the node's side of it.
            std::future::pending::<()>().await;
        }
        std::future::pending::<()>().await;
    }

    /// Opens session `number` on `conn`: both streams, each with its
    /// prelude and the session's number, and a reader for each.
    async fn open_session(
        &self,
        conn: &Conn,
        number: u32,
        events: &mpsc::UnboundedSender<Event>,
    ) -> Option<Session> {
        let mut writers = Vec::new();
        let data_paused = Arc::new(AtomicU64::new(0));
        for kind in [TX_RELAY_DATA_KIND, TX_RELAY_STATE_KIND] {
            let layout = Self::stream(kind);
            let (mut send, recv) = conn.open_bi().await.ok()?;
            handshake::write_prelude_capped(
                &mut send,
                layout.kind,
                layout.version,
                layout.frame_cap,
                Some(u64::from(number) + 1),
            )
            .await
            .ok()?;
            zakura_rt::task::spawn_named(
                "peer_tx_relay",
                read(
                    number,
                    kind,
                    recv,
                    layout.frame_cap,
                    events.clone(),
                    if kind == TX_RELAY_DATA_KIND {
                        Arc::clone(&data_paused)
                    } else {
                        Arc::new(AtomicU64::new(0))
                    },
                ),
            );
            writers.push(Writer {
                stream: StreamId {
                    kind: layout.kind,
                    version: layout.version,
                },
                rules: Self::rules(kind),
                id: send.id(),
                send,
                seq: 0,
            });
        }
        let state = writers.pop().expect("two writers");
        let data = writers.pop().expect("two writers");
        Some(Session {
            number,
            data,
            state,
            hellos: 0,
            node_state_frames: 0,
            node_hellos: 0,
            announced: VecDeque::new(),
            told: BTreeSet::new(),
            data_paused,
        })
    }

    /// Writes `frames` on `writer` in one write, each recorded with its
    /// label first, and its broken row in the ledger. False once the write
    /// fails.
    async fn write_all(&self, remote: Remote, writer: &mut Writer, frames: Vec<Out>) -> bool {
        let model = FrameModel::new(writer.rules, ReaderEnd::Persistent);
        let mut bytes = Vec::new();
        let mut written = Vec::new();
        for out in frames {
            let labels = model.labels(&out.header);
            let label = labels.union(out.family).first();
            let row = model
                .row(out.header.message_type)
                .map(|row| row.name.to_owned());
            // A header fault decides the frame before its content does.
            if let (Some(broken), true) = (out.row, labels == Labels::NONE) {
                lock(&self.evidence).broke(remote.addr, broken);
            }
            let seq = writer.seq;
            writer.seq += 1;
            written.push(seq);
            self.record(
                remote,
                writer.id,
                RecordBody::Frame {
                    seq,
                    row,
                    rule_stream: writer.stream,
                    label,
                    exhausted: false,
                    exchange: None,
                    payload: u64::try_from(out.payload.len()).unwrap_or(u64::MAX),
                    prepares: None,
                    asks: None,
                },
            );
            bytes.extend(frame_bytes(&out.header, &out.payload));
        }
        if writer.send.write_all(&bytes).await.is_err() {
            return false;
        }
        let order = self.order.next();
        for seq in written {
            self.record(remote, writer.id, RecordBody::Written { seq, order });
        }
        true
    }

    /// Records `body` on `remote`'s QUIC stream `quic_stream`.
    fn record(&self, remote: Remote, quic_stream: u64, body: RecordBody) {
        self.records.borrow_mut().push_back(PeerRecord {
            remote,
            quic_stream,
            body,
        });
    }

    /// Serves one connection until it closes.
    async fn connection(&self, conn: &Conn, remote: Remote) {
        let opened = now();
        let (events, mut incoming) = mpsc::unbounded_channel();
        let Some(mut session) = self.open_session(conn, 0, &events).await else {
            return;
        };
        let rate = u64::from(INV_RATE.bytes_per_second);
        let mut connection = Connection {
            remote,
            sender: Bucket::new(u64::from(INV_RATE.opening_allowance), rate, opened),
            receiver: Bucket::new(self.inv_depth, INV_RATE.receiver_bytes_per_second(), opened),
            asked: VecDeque::new(),
            next_answer: Duration::ZERO,
            announced: Vec::new(),
            held_announced: 0,
            to_announce: VecDeque::new(),
            next_fake: 0,
            next_session: 1,
        };
        let mut steps = self.spec.steps.iter().copied();
        let mut next_step = Duration::ZERO;
        // When the session ended and the next one may open.
        let mut reopen_at: Option<Duration> = None;
        loop {
            let at = now();
            if let Some(due) = reopen_at {
                if at >= due {
                    reopen_at = None;
                    let number = connection.next_session;
                    connection.next_session += 1;
                    let Some(next) = self.open_session(conn, number, &events).await else {
                        return;
                    };
                    session = next;
                }
            }
            // A session writes nothing until the node's `Hello` shows the
            // node admitted it, so no frame lands on a stream the node may
            // never admit.
            let admitted = reopen_at.is_none() && session.node_hellos > 0;
            if admitted {
                if !self.answer(&mut session, &mut connection, at).await
                    || !self.pace_inv(&mut session, &mut connection, at).await
                {
                    return;
                }
                if connection.to_announce.is_empty() && at >= next_step {
                    match steps.next() {
                        Some(step) => {
                            match self.step(conn, step, &mut session, &mut connection).await {
                                Some(true) => {}
                                Some(false) => return,
                                // A scripted reopen ended the session.
                                None => reopen_at = Some(now() + REOPEN_PAUSE),
                            }
                            next_step = now() + STEP_GAP;
                            if let RelayStep::Wait { secs } = step {
                                next_step += Duration::from_secs(u64::from(secs));
                            }
                        }
                        None => next_step = Duration::MAX,
                    }
                }
            }
            let wake = self.next_wake(admitted, &mut connection, next_step, reopen_at);
            let event = zakura_rt::select! {
                event = incoming.recv() => event,
                () = sleep_until(wake) => continue,
            };
            match event {
                Some(Event::Frame {
                    session: number,
                    kind,
                    frame,
                }) if number == session.number && reopen_at.is_none() => {
                    let first = session.node_hellos == 0;
                    self.node_frame(&mut session, &mut connection, kind, &frame);
                    if first
                        && session.node_hellos > 0
                        && !self.start(&mut session, &mut connection).await
                    {
                        return;
                    }
                }
                Some(Event::Frame { .. }) => {}
                Some(Event::Ended { session: number })
                    if number == session.number && reopen_at.is_none() =>
                {
                    if conn.close_reason().is_some() {
                        return;
                    }
                    // The node ended the session and kept the connection:
                    // a park.
                    connection.asked.clear();
                    reopen_at = Some(now() + REOPEN_WAIT);
                }
                Some(Event::Ended { .. }) => {}
                None => return,
            }
        }
    }

    /// When the task next has something due.
    fn next_wake(
        &self,
        admitted: bool,
        connection: &mut Connection,
        next_step: Duration,
        reopen_at: Option<Duration>,
    ) -> Duration {
        let far = now() + Duration::from_secs(3_600);
        let mut wake = reopen_at.unwrap_or(far);
        if admitted {
            if !connection.asked.is_empty() && self.spec.answer != Answer::Never {
                wake = wake.min(connection.next_answer);
            }
            if let Some(bytes) = self.next_inv_bytes(connection) {
                let at = now();
                wake = wake.min(connection.sender.ready_at(bytes, at));
            } else {
                wake = wake.min(next_step);
            }
        }
        wake.max(now())
    }

    /// A session the node admitted starts: its `Hello` and its opening
    /// announcements, as the spec says.
    async fn start(&self, session: &mut Session, connection: &mut Connection) -> bool {
        if self.spec.hello == HelloMode::First && !self.hello(session, connection).await {
            return false;
        }
        for _ in 0..self.spec.announce_on_open {
            let id = self.fresh(connection);
            connection.to_announce.push_back((id, FAKE_SIZE));
        }
        true
    }

    /// The next fresh id.
    fn fresh(&self, connection: &mut Connection) -> UnminedTxId {
        let id = fake_id(self.index, connection.next_fake);
        connection.next_fake += 1;
        id
    }

    /// Sends a valid `Hello`. A second one in the session breaks row 4.
    async fn hello(&self, session: &mut Session, connection: &mut Connection) -> bool {
        let hello = TxRelayMessage::Hello {
            serves: u16::try_from(MAX_SERVES).unwrap_or(u16::MAX),
            max_tx_bytes: MAX_TX_BYTES,
        };
        let out = if session.hellos > 0 {
            Out::breaking(&hello, Label::BadContent, CloseRow::RELAY_CLOSE_4)
        } else {
            Out::conformant(&hello)
        };
        session.hellos += 1;
        self.write_all(connection.remote, &mut session.state, vec![out])
            .await
    }

    /// Reads one frame from the node: its `Hello`, its requests, which the
    /// peer answers in turn, and its announcements, which a fetch reads.
    /// Records an out-of-order node `Hello` in the ledger.
    fn node_frame(
        &self,
        session: &mut Session,
        connection: &mut Connection,
        kind: u16,
        frame: &Frame,
    ) {
        let Ok(message) = decode_frame::<TxRelayMessage>(frame) else {
            return;
        };
        if kind == TX_RELAY_STATE_KIND {
            session.node_state_frames += 1;
            let hello = matches!(message, TxRelayMessage::Hello { .. });
            if (hello && session.node_hellos > 0) || (session.node_state_frames == 1 && !hello) {
                lock(&self.evidence).node_hello_out_of_order(format!(
                    "{}: the node's state frame {} on session {} was {message:?}",
                    self.name, session.node_state_frames, session.number
                ));
            }
            if hello {
                session.node_hellos += 1;
            }
        }
        match message {
            TxRelayMessage::GetTxs(id) => {
                if connection.asked.is_empty() {
                    connection.next_answer = connection.next_answer.max(now());
                }
                connection.asked.push_back(id);
            }
            TxRelayMessage::Inv(entries) => {
                for (id, _) in entries {
                    if session.told.insert(TxKey(id)) {
                        session.announced.push_back(id);
                    }
                }
            }
            TxRelayMessage::Hello { .. }
            | TxRelayMessage::Tx { .. }
            | TxRelayMessage::NotFound(_) => {}
        }
    }

    /// The body of `id` it holds, by index in the universe.
    fn held(&self, id: UnminedTxId) -> Option<usize> {
        self.spec
            .holds
            .iter()
            .copied()
            .find(|tx| self.bodies.get(*tx).is_some_and(|(held, _)| *held == id))
    }

    /// Answers the oldest request once its gap passed, as the spec says.
    async fn answer(
        &self,
        session: &mut Session,
        connection: &mut Connection,
        at: Duration,
    ) -> bool {
        if self.spec.answer == Answer::Never || at < connection.next_answer {
            return true;
        }
        let Some(id) = connection.asked.pop_front() else {
            return true;
        };
        connection.next_answer = at + Duration::from_millis(self.spec.answer_gap_ms);
        let not_found = TxRelayMessage::NotFound(id);
        let held = self.held(id);
        let (data, state): (Vec<Out>, Vec<Out>) = match (self.spec.answer, held) {
            (Answer::NotFound, _) | (_, None) => (Vec::new(), vec![Out::conformant(&not_found)]),
            (Answer::Serve | Answer::WrongSize | Answer::Never, Some(tx)) => {
                let body = self.bodies[tx].1.clone();
                let tx = TxRelayMessage::Tx { id, body };
                let out = if self.spec.answer == Answer::WrongSize {
                    Out::breaking(&tx, Label::BadContent, CloseRow::RELAY_CLOSE_10)
                } else {
                    Out::conformant(&tx)
                };
                (vec![out], Vec::new())
            }
            (Answer::WrongId, Some(tx)) => {
                let other = self
                    .spec
                    .holds
                    .iter()
                    .copied()
                    .find(|other| *other != tx)
                    .unwrap_or(tx);
                let body = self.bodies[other].1.clone();
                let tx_message = TxRelayMessage::Tx { id, body };
                // A peer that holds one body serves it under its own id.
                let out = if other == tx {
                    Out::conformant(&tx_message)
                } else {
                    Out::breaking(&tx_message, Label::BadContent, CloseRow::RELAY_CLOSE_11)
                };
                (vec![out], Vec::new())
            }
            (Answer::Undecodable, Some(tx)) => {
                let body = vec![0xFF; self.bodies[tx].1.len()];
                let tx = TxRelayMessage::Tx { id, body };
                (
                    vec![Out::breaking(
                        &tx,
                        Label::BadContent,
                        CloseRow::RELAY_CLOSE_12,
                    )],
                    Vec::new(),
                )
            }
            (Answer::Twice, Some(tx)) => {
                let body = self.bodies[tx].1.clone();
                let tx = TxRelayMessage::Tx { id, body };
                (
                    vec![Out::conformant(&tx)],
                    vec![Out::breaking(
                        &not_found,
                        Label::Unsolicited,
                        CloseRow::RELAY_CLOSE_9,
                    )],
                )
            }
        };
        let remote = connection.remote;
        (data.is_empty() || self.write_all(remote, &mut session.data, data).await)
            && (state.is_empty() || self.write_all(remote, &mut session.state, state).await)
    }

    /// The payload bytes of the next paced `Inv`, if one waits.
    fn next_inv_bytes(&self, connection: &Connection) -> Option<u64> {
        let entries = connection.to_announce.len().min(MAX_INV_ENTRIES);
        (entries > 0).then(|| inv_bytes(entries))
    }

    /// Sends the next paced `Inv` if its bucket pays for it.
    async fn pace_inv(
        &self,
        session: &mut Session,
        connection: &mut Connection,
        at: Duration,
    ) -> bool {
        let Some(bytes) = self.next_inv_bytes(connection) else {
            return true;
        };
        if !connection.sender.try_pay(bytes, at) {
            return true;
        }
        // The node's bucket is at least as deep and refills faster, so a
        // paced frame always fits it.
        connection.receiver.try_pay(bytes, at);
        let take = connection.to_announce.len().min(MAX_INV_ENTRIES);
        let entries: Vec<(UnminedTxId, u32)> = connection.to_announce.drain(..take).collect();
        connection.announced.extend(entries.iter().copied());
        let inv = TxRelayMessage::Inv(entries);
        self.write_all(
            connection.remote,
            &mut session.data,
            vec![Out::conformant(&inv)],
        )
        .await
    }

    /// Runs one step. Returns `None` once a scripted reopen ended the
    /// session, and `Some(false)` once a write fails.
    async fn step(
        &self,
        conn: &Conn,
        step: RelayStep,
        session: &mut Session,
        connection: &mut Connection,
    ) -> Option<bool> {
        let remote = connection.remote;
        let hello = session.hellos > 0;
        let ask = |id| {
            let message = TxRelayMessage::GetTxs(id);
            if hello {
                Out::conformant(&message)
            } else {
                Out::breaking(&message, Label::BadContent, CloseRow::RELAY_CLOSE_6)
            }
        };
        let (data, state): (Vec<Out>, Vec<Out>) = match step {
            RelayStep::Announce | RelayStep::AnnounceAll => {
                let count = if step == RelayStep::Announce {
                    1
                } else {
                    usize::MAX
                };
                let next = connection.held_announced;
                for tx in self.spec.holds.iter().skip(next).take(count).copied() {
                    let (id, body) = &self.bodies[tx];
                    let size = u32::try_from(body.len()).unwrap_or(u32::MAX);
                    let size = if self.spec.answer == Answer::WrongSize {
                        size + 1
                    } else {
                        size
                    };
                    connection.to_announce.push_back((*id, size));
                    connection.held_announced += 1;
                }
                (Vec::new(), Vec::new())
            }
            RelayStep::AnnounceFake { count } => {
                self.queue_fresh(connection, usize::from(count));
                (Vec::new(), Vec::new())
            }
            RelayStep::FillPending => {
                self.queue_fresh(connection, PENDING_PER_PEER + 64);
                (Vec::new(), Vec::new())
            }
            RelayStep::FillKnown => {
                self.queue_fresh(connection, KNOWN_CAP + 64);
                (Vec::new(), Vec::new())
            }
            RelayStep::Fetch => match session.announced.pop_front() {
                Some(id) if hello => (Vec::new(), vec![ask(id)]),
                _ => (Vec::new(), Vec::new()),
            },
            RelayStep::FetchBurst => {
                let take = session.announced.len().min(MAX_SERVES);
                let ids: Vec<UnminedTxId> = session.announced.drain(..take).collect();
                if hello {
                    (Vec::new(), ids.into_iter().map(ask).collect())
                } else {
                    (Vec::new(), Vec::new())
                }
            }
            RelayStep::AskUnknown => {
                let id = self.fresh(connection);
                if hello || self.faults_allowed() {
                    (Vec::new(), vec![ask(id)])
                } else {
                    (Vec::new(), Vec::new())
                }
            }
            RelayStep::Probe { tx } => match self.bodies.get(tx) {
                Some((id, _)) if hello || self.faults_allowed() => (Vec::new(), vec![ask(*id)]),
                _ => (Vec::new(), Vec::new()),
            },
            RelayStep::Hello => {
                if hello && !self.faults_allowed() {
                    return Some(true);
                }
                return Some(self.hello(session, connection).await);
            }
            RelayStep::Reopen => {
                // Finishing both streams ends the session on the node.
                let _ = session.data.send.finish();
                let _ = session.state.send.finish();
                let _ = conn;
                connection.asked.clear();
                return None;
            }
            RelayStep::Wait { .. } => (Vec::new(), Vec::new()),
            RelayStep::BadHello => {
                let message = TxRelayMessage::Hello {
                    serves: 0,
                    max_tx_bytes: MAX_TX_BYTES,
                };
                let row = if hello {
                    CloseRow::RELAY_CLOSE_4
                } else {
                    CloseRow::RELAY_CLOSE_5
                };
                session.hellos += 1;
                (
                    Vec::new(),
                    vec![Out::breaking(&message, Label::BadContent, row)],
                )
            }
            RelayStep::UnrequestedTx { tx } => {
                let tx = self.spec.holds.get(tx).copied().unwrap_or(0);
                let (id, body) = self.bodies[tx].clone();
                let asked = connection.asked.contains(&id);
                let message = TxRelayMessage::Tx { id, body };
                let out = if asked {
                    connection.asked.retain(|other| *other != id);
                    Out::conformant(&message)
                } else {
                    Out::breaking(&message, Label::Unsolicited, CloseRow::RELAY_CLOSE_9)
                };
                (vec![out], Vec::new())
            }
            RelayStep::UnrequestedNotFound => {
                let id = self.fresh(connection);
                let message = TxRelayMessage::NotFound(id);
                (
                    Vec::new(),
                    vec![Out::breaking(
                        &message,
                        Label::Unsolicited,
                        CloseRow::RELAY_CLOSE_9,
                    )],
                )
            }
            RelayStep::MalformedInv => {
                let ids = [self.fresh(connection), self.fresh(connection)];
                let mut out = Out::breaking(
                    &TxRelayMessage::Inv(ids.iter().map(|id| (*id, FAKE_SIZE)).collect()),
                    Label::BadContent,
                    CloseRow::RELAY_CLOSE_3,
                );
                // Drops the last entry, which the count still promises.
                let entry = inv_bytes(2) - inv_bytes(1);
                let keep = out.payload.len() - usize::try_from(entry).unwrap_or(0);
                out.payload.truncate(keep);
                out.header.payload_len = out.payload.len();
                if !connection.receiver.try_pay(out.payload.len() as u64, now()) {
                    out.family = out.family.with(Label::OverRate);
                }
                (vec![out], Vec::new())
            }
            RelayStep::HeaderFault { row, fault } => {
                let id = self.fresh(connection);
                let message = match row {
                    RelayRow::Hello => TxRelayMessage::Hello {
                        serves: u16::try_from(MAX_SERVES).unwrap_or(u16::MAX),
                        max_tx_bytes: MAX_TX_BYTES,
                    },
                    RelayRow::GetTxs => TxRelayMessage::GetTxs(id),
                    RelayRow::NotFound => TxRelayMessage::NotFound(id),
                    RelayRow::Inv => TxRelayMessage::Inv(vec![(id, FAKE_SIZE)]),
                    RelayRow::Tx => TxRelayMessage::Tx {
                        id: self.bodies[0].0,
                        body: self.bodies[0].1.clone(),
                    },
                };
                let mut out = Out::conformant(&message);
                let rule = Self::rules(stream_of(row))
                    .iter()
                    .find(|rule| rule.message_type == out.header.message_type)
                    .expect("the row is tabled");
                match fault {
                    HeaderFault::ReservedFlags => out.header.flags = 0x8000,
                    HeaderFault::ShortPayload => {
                        out.payload.truncate(rule.payload.min().saturating_sub(1));
                        out.header.payload_len = out.payload.len();
                    }
                    HeaderFault::LongPayload => {
                        // Only the header goes: the node judges the length
                        // there, and a body past the largest row would be
                        // megabytes.
                        out.payload.clear();
                        out.header.payload_len = rule.payload.max() + 1;
                    }
                }
                if row.on_data() {
                    (vec![out], Vec::new())
                } else {
                    (Vec::new(), vec![out])
                }
            }
            RelayStep::UndeclaredType { data } => {
                let out = Out {
                    header: FrameHeader {
                        message_type: 0x7777,
                        flags: 0,
                        payload_len: 4,
                    },
                    payload: vec![0; 4],
                    family: Labels::NONE,
                    row: None,
                };
                if data {
                    (vec![out], Vec::new())
                } else {
                    (Vec::new(), vec![out])
                }
            }
            RelayStep::RequestTwice => {
                if !hello {
                    return Some(true);
                }
                // Ids the node announced, the last asked for twice while
                // the peer reads nothing. The node starts the answers it
                // can write or queue on the stream, at most
                // [`STARTED_WHILE_PAUSED`], and the last one's request is
                // still unanswered when it comes again.
                if !hello || session.announced.len() < STARTED_WHILE_PAUSED + 1 {
                    return Some(true);
                }
                let mut ids: Vec<UnminedTxId> =
                    session.announced.drain(..=STARTED_WHILE_PAUSED).collect();
                ids.push(*ids.last().expect("a few ids"));
                pause_reads(session);
                (Vec::new(), ids.into_iter().map(ask).collect())
            }
            RelayStep::TooManyRequests => {
                // While the peer reads nothing, the node starts at most
                // [`STARTED_WHILE_PAUSED`] answers, and the rest queue
                // unstarted: the last request is past what the node's
                // `Hello` serves.
                let count = MAX_SERVES + STARTED_WHILE_PAUSED + 1;
                if !hello || session.announced.len() < count {
                    return Some(true);
                }
                let ids: Vec<UnminedTxId> = session.announced.drain(..count).collect();
                pause_reads(session);
                (Vec::new(), ids.into_iter().map(ask).collect())
            }
        };
        let wrote = (data.is_empty() || self.write_all(remote, &mut session.data, data).await)
            && (state.is_empty() || self.write_all(remote, &mut session.state, state).await);
        Some(wrote)
    }

    /// Whether its script may break rules.
    fn faults_allowed(&self) -> bool {
        self.spec.faults
    }

    /// Queues `count` fresh ids to announce, paced.
    fn queue_fresh(&self, connection: &mut Connection, count: usize) {
        for _ in 0..count {
            let id = self.fresh(connection);
            connection.to_announce.push_back((id, FAKE_SIZE));
        }
    }
}

/// The stream kind `row` travels on.
fn stream_of(row: RelayRow) -> u16 {
    if row.on_data() {
        TX_RELAY_DATA_KIND
    } else {
        TX_RELAY_STATE_KIND
    }
}

/// The payload bytes of an `Inv` of `entries` entries.
fn inv_bytes(entries: usize) -> u64 {
    let entry = (UnminedTxId::from_legacy_id(Hash([0; 32])), FAKE_SIZE);
    let frame = encode_frame(&TxRelayMessage::Inv(vec![entry; entries]))
        .expect("an Inv within its row encodes");
    frame.payload.len() as u64
}

/// Stops `session`'s data reader for [`READ_PAUSE`].
fn pause_reads(session: &Session) {
    let until = now().saturating_add(READ_PAUSE);
    let until = u64::try_from(until.as_millis()).unwrap_or(u64::MAX);
    session.data_paused.store(until, Ordering::SeqCst);
}

/// Reads frames from the node on session `session`'s `kind` stream, and
/// hands each to the peer's task, then the stream's end. It reads nothing
/// until `paused`, in milliseconds since the run's start, has passed.
async fn read(
    session: u32,
    kind: u16,
    mut recv: RecvStream,
    cap: u32,
    events: mpsc::UnboundedSender<Event>,
    paused: Arc<AtomicU64>,
) {
    let cap = usize::try_from(cap).unwrap_or(usize::MAX);
    loop {
        loop {
            let until = Duration::from_millis(paused.load(Ordering::SeqCst));
            if until <= now() {
                break;
            }
            sleep_until(until).await;
        }
        let mut header = [0; 8];
        if recv.read_exact(&mut header).await.is_err() {
            break;
        }
        let len = usize::try_from(u32::from_le_bytes([
            header[4], header[5], header[6], header[7],
        ]))
        .unwrap_or(usize::MAX);
        if len > cap {
            break;
        }
        let mut payload = vec![0; len];
        if recv.read_exact(&mut payload).await.is_err() {
            break;
        }
        let frame = Frame {
            message_type: u16::from_le_bytes([header[0], header[1]]),
            flags: u16::from_le_bytes([header[2], header[3]]),
            payload,
        };
        if events
            .send(Event::Frame {
                session,
                kind,
                frame,
            })
            .is_err()
        {
            return;
        }
    }
    let _ = events.send(Event::Ended { session });
}
