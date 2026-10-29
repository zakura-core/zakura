//! Scripted peers for `header-sync/wire` (M5-C): real QUIC endpoints on the
//! run's `SimNet` that run the Zakura handshake from the public wire types.
//!
//! A peer either dials the node from a fresh address per dial or listens
//! for the node's dial. It offers or accepts the capability mask its
//! [`PeerSpec`] names, keeps the header stream it negotiated open and read,
//! and carries out its scripted [`PeerAction`]s.
//!
//! A peer with an [`HonestSpec`] runs the [`Honest`] script on its header
//! streams (M5-D). Every frame a peer sends there is labelled by its
//! [`StreamModels`] before the write and logged as a `PeerFrame`, then as a
//! `PeerWritten` once the write returns, and every frame it reads from the
//! node is logged as a `PeerReceived`. Each record names its connection and
//! QUIC stream from the identity map, never from draw order, so it waits in
//! [`PeerControl`] until the node's transport has admitted its stream.
//!
//! A peer with an [`AdversarySpec`] also takes [`PeerAction::Act`]s, the
//! receiver-direction boundaries of M5-E1 ([`adversary`]). Its frames are
//! labelled by the same models and by the content labeller, and the node's
//! evidence judges each at the reader stage that decides it ([`stage`]).
//!
//! Actions follow M4's `Control` pattern. [`PeerControl`] offers a peer's
//! next action as `peer:<name>:<action>` only while the peer is settled and
//! idle, so the `Plan` picks when it happens, `execute` only queues it, and
//! the peer's task carries it out. So at most one action is queued per
//! peer, and an adversary's actions stay within the rate [`limit`] sets.

pub mod adversary;
mod content;
mod evidence;
pub(crate) mod handshake;
pub mod honest;
pub mod limit;
mod stage;

use std::{
    cell::Cell,
    collections::BTreeMap,
    future::Future,
    net::{Ipv4Addr, SocketAddr},
    pin::Pin,
    rc::Rc,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use zakura_network::zakura::{
    v9::{Cursor, Headers, Message, Operation, RegtestMessage},
    wire_codec::{decode_frame, encode_frame},
    Frame, P2P_V2_ALPN, ZAKURA_CAP_HEADER_SYNC, ZAKURA_HEADER_SYNC_STREAM_VERSION,
    ZAKURA_STREAM_HEADER_SYNC,
};
use zakura_quic::{
    sim::{SeededInputs, SimNet},
    Acceptor, Admit, Conn, IncomingInfo, NodeAddr, QuicBindConfig, QuicConfig, QuicEndpoint,
    RecvStream, SendStream, VarInt,
};
use zakura_rt::{
    sync::{mpsc, watch},
    time::Instant,
};
use zakura_scheduler::SimInstant;

pub(crate) use self::evidence::{PeerRecord, PeerRecords, RecordBody};
use self::{
    adversary::{Act, Adversary, AdversarySpec},
    content::ContentLabeller,
    evidence::WireEvidence,
    handshake::{Echo, Local},
    honest::{Honest, HonestChain, HonestSpec},
    limit::{RateLimit, ADVERSARY_RATE},
};
use crate::{
    model::{
        CadenceModel, Charge, FrameHeader, FrameModel, Label, Labels, ReaderEnd, SubscriptionModel,
    },
    observe::{PeerCredit, StreamId},
    table::rule_row,
    ActionSource, Boundary, Identity, ObservationLog,
};

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
    /// If set, it runs the honest script on its header streams.
    #[serde(default)]
    pub honest: Option<HonestSpec>,
    /// If set, it's an adversary: its script may hold [`PeerAction::Act`]s,
    /// and between them it runs the honest script on `honest`'s chain as
    /// this says. Its actions are limited to [`ADVERSARY_RATE`] per virtual
    /// second.
    #[serde(default)]
    pub adversary: Option<AdversarySpec>,
}

impl PeerSpec {
    /// A peer that dials and offers header sync alone.
    pub fn dialing() -> Self {
        PeerSpec {
            dialer: Dialer::Peer,
            capabilities: ZAKURA_CAP_HEADER_SYNC,
            bad_nonce_echo: false,
            script: Vec::new(),
            honest: None,
            adversary: None,
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
    /// Releases one held grant of an honest subscriber, offered once its
    /// quiet time after the last page has passed.
    Grant,
    /// An adversary's act, offered once its setup history is done.
    Act {
        /// The act.
        act: Act,
    },
}

impl PeerAction {
    /// The action's name in its identity.
    pub fn name(self) -> &'static str {
        match self {
            PeerAction::Open { .. } => "open",
            PeerAction::FinishHeader => "finish-header",
            PeerAction::Close => "close",
            PeerAction::Redial => "redial",
            PeerAction::Grant => "grant",
            PeerAction::Act { act } => act.name(),
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
    /// An adversary's budget of actions per virtual second.
    limit: Option<RateLimit>,
}

impl Slot {
    /// The next action, if the peer is ready for it and its budget allows
    /// it at `now`.
    fn offered(&self, now: SimInstant) -> Option<PeerAction> {
        let allowed = self.limit.is_none_or(|limit| limit.allows(now));
        self.script
            .get(self.next)
            .copied()
            .filter(|_| allowed && self.readiness.ready())
    }
}

/// Offers each peer's next scripted action, as `peer:<name>:<action>`,
/// while that peer is ready for it.
pub struct PeerControl {
    slots: Vec<Slot>,
    failure: PeerFailure,
    /// What the peers' tasks recorded, until the identity map places it.
    records: PeerRecords,
    evidence: WireEvidence,
}

impl PeerControl {
    /// No peers yet. Failures land in `failure`, and the peers' tasks
    /// record into `records`.
    pub(crate) fn new(failure: PeerFailure, records: PeerRecords) -> Self {
        PeerControl {
            slots: Vec::new(),
            failure,
            records,
            evidence: WireEvidence::default(),
        }
    }

    /// Adds peer `name`'s script, limited to [`ADVERSARY_RATE`] actions
    /// per virtual second if it's an `adversary`. Returns the readiness its
    /// task keeps, and the receiver it takes actions from, which holds one.
    pub(crate) fn add(
        &mut self,
        name: &str,
        script: Vec<PeerAction>,
        adversary: bool,
    ) -> (Readiness, mpsc::Receiver<PeerAction>) {
        let (to_task, commands) = mpsc::channel(1);
        let readiness = Readiness::default();
        self.slots.push(Slot {
            name: name.to_owned(),
            script,
            next: 0,
            readiness: readiness.clone(),
            to_task,
            limit: adversary.then(|| RateLimit::new(ADVERSARY_RATE)),
        });
        (readiness, commands)
    }
}

impl ActionSource for PeerControl {
    /// Each ready peer's next action, if its budget allows it.
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        for (index, slot) in (0..).zip(&self.slots) {
            if let Some(action) = slot.offered(now) {
                out.push((Identity::peer(&slot.name, action.name()), index));
            }
        }
    }

    /// The next whole second, for each ready adversary whose budget is
    /// spent. Otherwise a peer's readiness changes only when its task runs.
    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        for slot in &self.slots {
            let waiting = slot.script.get(slot.next).is_some() && slot.readiness.ready();
            if let Some(limit) = slot.limit.filter(|limit| waiting && !limit.allows(now)) {
                out.push(Boundary::Opens(limit.opens()));
            }
        }
    }

    /// Queues the peer's next action, charges an adversary's budget, and
    /// marks the peer busy until its task is done with the action.
    fn execute(&mut self, local: u64, _: &mut ObservationLog) {
        let slot = usize::try_from(local)
            .ok()
            .and_then(|index| self.slots.get_mut(index))
            .expect("`enabled` offered this peer at this step");
        if let Some(limit) = slot.limit.as_mut() {
            limit.charge(zakura_scheduler::hooks::now().expect("a peer acts inside a run"));
        }
        slot.readiness.queued();
        slot.to_task
            .try_send(slot.script[slot.next])
            .expect("a peer is offered an action only while its last one is done");
        slot.next += 1;
    }

    /// Fails the run once a peer couldn't carry out an action. Otherwise
    /// logs the peers' records the identity map can place, and what the
    /// node's evidence means for P3b's checks ([`WireEvidence`]). This
    /// source is added after the transport tap, so the identity rows it
    /// reads are logged first.
    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        if let Some(failure) = self.failure.get() {
            return Err(format!("PeerActionFailed: {failure}"));
        }
        self.evidence.observe(&self.records, log)
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
    /// Its chain, if it runs the honest script.
    pub(crate) chain: Option<Arc<HonestChain>>,
    /// Where it records its frames.
    pub(crate) records: PeerRecords,
    /// How many scripted actions it has taken, so its next is
    /// `spec.script[taken]`.
    pub(crate) taken: usize,
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
    /// A whole frame from the node arrived on header stream `id`.
    Frame { id: u64, frame: Frame },
}

/// The bytes of a plain `Headers` page's fixed fields: the subscription ID,
/// the target, the parent's height and hash, the count, the marker and the
/// schema.
pub(crate) const PAGE_FIXED_BYTES: usize = 8 + 32 + 4 + 32 + 4 + 1 + 1;

/// The bytes of one plain Regtest page entry: a 177-byte header and its
/// 4-byte body-size hint.
pub(crate) const ENTRY_BYTES: usize = 177 + 4;

/// Labelled frames, encoded, for a peer to write at once: one frame, or a
/// burst.
pub(crate) struct Outgoing {
    /// The frames' bytes, headers included. A frame whose payload is
    /// withheld has only its header here.
    bytes: Vec<u8>,
    /// What each frame's `PeerFrame` records, in order.
    records: Vec<RecordBody>,
}

impl Outgoing {
    /// Withholds a lone frame's payload: only its header is written.
    pub(crate) fn withhold(&mut self) {
        assert_eq!(self.records.len(), 1, "only a lone frame withholds");
        self.bytes.truncate(8);
    }

    /// Whether every frame here is labelled `Conformant`.
    pub(crate) fn conformant(&self) -> bool {
        self.records.iter().all(|record| {
            matches!(
                record,
                RecordBody::Frame {
                    label: Label::Conformant,
                    ..
                }
            )
        })
    }

    /// Whether any frame here found its cadence bucket empty when charged.
    pub(crate) fn exhausted(&self) -> bool {
        self.records.iter().any(|record| {
            matches!(
                record,
                RecordBody::Frame {
                    exhausted: true,
                    ..
                }
            )
        })
    }

    /// `frames` in order, written with one call, so a burst's writes don't
    /// each take their own steps.
    pub(crate) fn burst(frames: Vec<Outgoing>) -> Outgoing {
        let mut burst = Outgoing {
            bytes: Vec::new(),
            records: Vec::new(),
        };
        for frame in frames {
            burst.bytes.extend(frame.bytes);
            burst.records.extend(frame.records);
        }
        burst
    }
}

/// A frame a peer writes: the message its role and content labels read,
/// and the header and payload it actually writes. An honest frame is its
/// message encoded. A crafted one may declare another type, flags or
/// length, carry bytes the encoder refuses, or withhold its payload.
#[derive(Clone, Debug)]
pub(crate) struct Crafted {
    /// What the frame means to the role and content labels.
    pub(crate) message: RegtestMessage,
    /// The header written.
    pub(crate) header: FrameHeader,
    /// The bytes written after the header, which may stop short of
    /// `header.payload_len`.
    pub(crate) payload: Vec<u8>,
}

impl Crafted {
    /// `message`, encoded as it is.
    pub(crate) fn of(message: RegtestMessage) -> Self {
        let frame = encode_frame(&message).expect("a crafted frame starts from a valid message");
        Crafted {
            header: FrameHeader {
                message_type: frame.message_type,
                flags: frame.flags,
                payload_len: frame.payload.len(),
            },
            payload: frame.payload,
            message,
        }
    }

    /// The header and payload bytes, as written.
    fn bytes(&self) -> Vec<u8> {
        // Crafted lengths stay far below 4 GiB.
        let len = u32::try_from(self.header.payload_len).expect("a crafted payload fits u32");
        let mut bytes = Vec::with_capacity(8 + self.payload.len());
        bytes.extend(self.header.message_type.to_le_bytes());
        bytes.extend(self.header.flags.to_le_bytes());
        bytes.extend(len.to_le_bytes());
        bytes.extend(&self.payload);
        bytes
    }
}

/// One header stream's role models, which label each frame a peer sends
/// before the write and follow each frame it reads from the node (P3a),
/// and its content labeller (M5-Q14).
#[derive(Clone)]
pub(crate) struct StreamModels {
    /// When the stream started, the cadence model's origin.
    start: Instant,
    frames: FrameModel,
    /// The header-sync subscription row, in both directions.
    pub(crate) subscriptions: SubscriptionModel,
    cadence: CadenceModel,
    content: ContentLabeller,
    /// Wire cursors as the subscription model's keys.
    cursors: Cursors,
    /// Frames labelled so far, the next one's `seq`.
    sent: u64,
}

/// Wire cursors as keys for the subscription model, which compares cursors
/// by equality alone, as the publisher does: each distinct (height, hash,
/// headers, bytes) gets its own key, so a cursor differing in any field is
/// another cursor.
#[derive(Clone, Debug, Default)]
struct Cursors {
    keys: BTreeMap<(u32, [u8; 32], u64, u64), u64>,
    /// The headers and bytes the pages of each subscription consumed, by
    /// who sent the pages and the subscription's ID.
    consumed: BTreeMap<(Pages, u64), (u64, u64)>,
}

/// Whose pages a subscription carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Pages {
    /// The peer's, on a subscription the node opened.
    Peer,
    /// The node's, on one the peer opened.
    Node,
}

impl Cursors {
    /// `cursor`'s key.
    fn key(&mut self, cursor: &Cursor) -> u64 {
        let next = u64::try_from(self.keys.len()).expect("a run sees fewer than 2^64 cursors");
        *self
            .keys
            .entry((cursor.height.0, cursor.hash.0, cursor.headers, cursor.bytes))
            .or_insert(next)
    }

    /// The key of `page`'s cursor, `payload_len` bytes long, from `pages`,
    /// with the totals it consumes through it. Nothing is recorded.
    fn page(&mut self, pages: Pages, page: &Headers, payload_len: usize) -> (u64, (u64, u64)) {
        let count = u32::try_from(page.entries.len()).unwrap_or(u32::MAX);
        let (headers, bytes) = self
            .consumed
            .get(&(pages, page.subscription_id))
            .copied()
            .unwrap_or_default();
        let totals = (
            headers.saturating_add(u64::from(count)),
            bytes.saturating_add(u64::try_from(payload_len).unwrap_or(u64::MAX)),
        );
        let last = page
            .entries
            .last()
            .map_or(page.common_ancestor_hash, |entry| entry.header.hash());
        let key = self.key(&Cursor {
            height: zakura_chain::block::Height(
                page.common_ancestor_height.0.saturating_add(count),
            ),
            hash: last,
            headers: totals.0,
            bytes: totals.1,
        });
        (key, totals)
    }
}

/// The model session of a header stream: each stream has models of its own.
const SESSION: u64 = 0;

/// The subscription row the models follow.
fn subscription_row() -> u16 {
    RegtestMessage::ROWS
        .iter()
        .find(|row| {
            matches!(
                row.role,
                zakura_network::zakura::MessageRole::Subscription { .. }
            )
        })
        .expect("header sync has a subscription row")
        .message_type
}

impl StreamModels {
    /// Fresh models for a stream that starts at `start`, for a peer
    /// holding `chain`.
    pub(crate) fn new(start: Instant, chain: Arc<HonestChain>) -> Self {
        StreamModels {
            start,
            frames: FrameModel::new(&RegtestMessage::ROWS, ReaderEnd::Persistent),
            subscriptions: SubscriptionModel::new(&RegtestMessage::ROWS, subscription_row())
                .expect("the row is a subscription"),
            cadence: CadenceModel::new(),
            content: ContentLabeller::new(chain),
            cursors: Cursors::default(),
            sent: 0,
        }
    }

    /// Every label `frame` would get, without sending it.
    pub(crate) fn preview(&self, frame: &Crafted) -> Labels {
        self.clone().labels(frame)
    }

    /// Labels `frame` with the models, as [`Self::labels`] does, and
    /// returns its bytes and record. A conformant frame changes the models
    /// as its sending does.
    pub(crate) fn send_crafted(&mut self, frame: Crafted) -> Outgoing {
        let header = frame.header;
        let label = self.labels(&frame).first();
        let stream = &RegtestMessage::LAYOUT[0];
        let id = |stream: &zakura_network::zakura::Stream| StreamId {
            kind: stream.kind,
            version: stream.version,
        };
        let (row, rule_stream) = match self.frames.row(header.message_type) {
            Some(row) => {
                let (rule_stream, rule) = rule_row(&RegtestMessage::LAYOUT, stream, row, label);
                (Some(rule.name.to_owned()), id(rule_stream))
            }
            None => (None, id(stream)),
        };
        let exhausted = self.frames.row(header.message_type).is_some_and(|row| {
            let now = Instant::now().saturating_duration_since(self.start);
            self.cadence.charge(SESSION, stream.version, row, now) == Charge::Exhausted
        });
        let seq = self.sent;
        self.sent += 1;
        Outgoing {
            bytes: frame.bytes(),
            records: vec![RecordBody::Frame {
                seq,
                row,
                rule_stream,
                label,
                exhausted,
                exchange: exchange(&frame.message),
                // A frame's declared length is under 4 GiB.
                payload: header.payload_len as u64,
            }],
        }
    }

    /// Every label of `frame`: its header's faults, its role's under the
    /// subscription model, and its content's. The role reads the header's
    /// declared length, and only a frame whose type has a row and is its
    /// message's has one. A frame with any fault is labelled on a copy of
    /// the subscription model, so it changes no model state (P3a, Scope).
    /// A conformant one changes the models as its sending does.
    fn labels(&mut self, frame: &Crafted) -> Labels {
        let header = frame.header;
        let message = &frame.message;
        let header_labels = self.frames.labels(&header);
        let content = self.content.label(message, header.payload_len);
        let mut copy;
        let subscriptions = if header_labels.is_conformant() && content.is_conformant() {
            &mut self.subscriptions
        } else {
            copy = self.subscriptions.clone();
            &mut copy
        };
        let own_row = self.frames.row(header.message_type).is_some()
            && message_type(message) == header.message_type;
        let (role, page) = if own_row {
            role_labels(
                subscriptions,
                &mut self.cursors,
                message,
                header.payload_len,
            )
        } else {
            (Labels::NONE, None)
        };
        let labels = header_labels.union(role).union(content);
        if labels.is_conformant() {
            self.content.sent(message);
            if let (Message::Headers(sent), Some(totals)) = (message, page) {
                self.cursors
                    .consumed
                    .insert((Pages::Peer, sent.subscription_id), totals);
            }
        }
        labels
    }

    /// Follows the node's `message`, `payload_len` bytes long, as the
    /// models' node side: its own subscription updates, and its pages and
    /// outcomes on this peer's subscription.
    pub(crate) fn receive(&mut self, message: &RegtestMessage, payload_len: usize) {
        self.content.received(message);
        let models = &mut self.subscriptions;
        match message {
            Message::Status(_) => {}
            Message::SubscribeHeaders(update) => {
                let id = update.subscription_id;
                let acknowledged = self.cursors.key(&update.acknowledged);
                match update.operation {
                    Operation::Open => {
                        models.subscribe(SESSION, id, update.added_credit, acknowledged);
                    }
                    Operation::Grant => {
                        // The node grants once it accepted the pages through
                        // the cursor it acknowledges.
                        models.accept(SESSION, id, acknowledged);
                        models.grant(SESSION, id, update.added_credit);
                    }
                    Operation::Close => {
                        models.accept(SESSION, id, acknowledged);
                        models.close(SESSION, id);
                    }
                }
            }
            Message::Headers(page) => {
                let count = u32::try_from(page.entries.len()).unwrap_or(u32::MAX);
                let (last, totals) = self.cursors.page(Pages::Node, page, payload_len);
                if models.send_page(SESSION, page.subscription_id, count, payload_len, last) {
                    self.cursors
                        .consumed
                        .insert((Pages::Node, page.subscription_id), totals);
                }
            }
            Message::HeadersOutcome(outcome) => {
                models.end(SESSION, outcome.subscription_id);
                models.terminal_written(SESSION, outcome.subscription_id);
            }
        }
    }
}

/// `message`'s type.
fn message_type(message: &RegtestMessage) -> u16 {
    use zakura_network::zakura::wire_codec::WireMessage;
    message.message_type()
}

/// The exchange or subscription `message` names, if any.
fn exchange(message: &RegtestMessage) -> Option<u64> {
    match message {
        Message::Status(_) => None,
        Message::SubscribeHeaders(update) => Some(update.subscription_id),
        Message::Headers(page) => Some(page.subscription_id),
        Message::HeadersOutcome(outcome) => Some(outcome.subscription_id),
    }
}

/// The subscription model's labels for the peer sending `message`, a page
/// read as `payload_len` bytes, with cursors keyed by `cursors`. It applies
/// a conformant one. For a page, also the totals it consumes, which the
/// caller records once the whole frame is conformant.
fn role_labels(
    subscriptions: &mut SubscriptionModel,
    cursors: &mut Cursors,
    message: &RegtestMessage,
    payload_len: usize,
) -> (Labels, Option<(u64, u64)>) {
    let labels = match message {
        Message::Status(_) => Labels::NONE,
        Message::SubscribeHeaders(update) => {
            let (id, sequence) = (update.subscription_id, update.update_sequence);
            let acknowledged = cursors.key(&update.acknowledged);
            match update.operation {
                Operation::Open => subscriptions.open_frame(
                    SESSION,
                    id,
                    sequence,
                    update.added_credit,
                    acknowledged,
                ),
                Operation::Grant => subscriptions.grant_frame(
                    SESSION,
                    id,
                    sequence,
                    acknowledged,
                    update.added_credit,
                ),
                Operation::Close => subscriptions.close_frame(SESSION, id, sequence, acknowledged),
            }
        }
        Message::Headers(page) => {
            let count = u32::try_from(page.entries.len()).unwrap_or(u32::MAX);
            let (last, totals) = cursors.page(Pages::Peer, page, payload_len);
            let labels =
                subscriptions.page_frame(SESSION, page.subscription_id, count, payload_len, last);
            return (labels, Some(totals));
        }
        Message::HeadersOutcome(outcome) => {
            subscriptions.ending_frame(SESSION, outcome.subscription_id)
        }
    };
    (labels, None)
}

/// The credit `message` adds, if it's an `Open` or a `Grant`.
fn credit(message: &RegtestMessage) -> Option<PeerCredit> {
    match message {
        Message::SubscribeHeaders(update) if update.operation != Operation::Close => {
            Some(PeerCredit {
                objects: update.added_credit.objects,
                bytes: update.added_credit.bytes,
            })
        }
        _ => None,
    }
}

/// The header stream a peer writes on.
struct HeaderStream {
    id: u64,
    send: SendStream,
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
/// forwards each whole one. Every read is drained, so the node's writer
/// never stalls on this peer.
async fn read_frames(id: u64, mut recv: RecvStream, events: mpsc::UnboundedSender<StreamEvent>) {
    let cap = usize::try_from(RegtestMessage::LAYOUT[0].frame_cap).unwrap_or(usize::MAX);
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
        let frame = Frame {
            message_type: u16::from_le_bytes([header[0], header[1]]),
            flags: u16::from_le_bytes([header[2], header[3]]),
            payload,
        };
        if events.send(StreamEvent::Frame { id, frame }).is_err() {
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

/// The scripts a peer runs on its header streams.
struct Scripts {
    honest: Option<Honest>,
    adversary: Option<Adversary>,
}

impl Scripts {
    /// When the peer may next become ready for `next`, its next scripted
    /// action, if it runs a script: the adversary's setup history for an
    /// act, and the honest script's settling for any other action.
    fn wake(&self, next: Option<PeerAction>) -> Option<Instant> {
        let honest = self.honest.as_ref()?;
        match (next, &self.adversary) {
            (Some(PeerAction::Act { act }), Some(adversary)) => adversary.wake(act, honest),
            _ => honest.wake(next),
        }
    }
}

impl PeerTask {
    /// Runs the peer until the run ends.
    pub(crate) async fn run(mut self) {
        let local = Local::regtest();
        let adversary = self.spec.adversary.clone().map(Adversary::new);
        let (announce, serve) = adversary.as_ref().map_or((true, true), |adversary| {
            (adversary.announces(), adversary.serves())
        });
        let honest = match (self.spec.honest.clone(), self.chain.clone()) {
            (Some(spec), Some(chain)) => Some(Honest::with(spec, chain, announce, serve)),
            (None, None) => None,
            _ => unreachable!("the harness gives a chain to exactly the honest peers"),
        };
        let acts = self
            .spec
            .script
            .iter()
            .any(|action| matches!(action, PeerAction::Act { .. }));
        let mut scripts = Scripts { honest, adversary };
        match self.spec.dialer {
            _ if scripts.adversary.is_some() && scripts.honest.is_none() => {
                self.fail("an adversary needs an honest chain");
            }
            _ if acts && scripts.adversary.is_none() => {
                self.fail("only an adversary can act");
            }
            Dialer::Peer => self.dial(&local, &mut scripts).await,
            Dialer::Node if self.spec.script.contains(&PeerAction::Redial) => {
                self.fail("only a dialing peer can redial");
            }
            Dialer::Node => self.listen(&local, &mut scripts).await,
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
    async fn dial(&mut self, local: &Local, scripts: &mut Scripts) {
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
            let remote = peer_addr(self.index, dial);
            if let Ended::Failed = self.serve(&conn, negotiated, remote, scripts).await {
                return;
            }
            // Only a redial may follow a closed connection.
            self.readiness.set(true);
            let next = self.commands.recv().await;
            self.readiness.taken();
            self.taken += 1;
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
    async fn listen(&mut self, local: &Local, scripts: &mut Scripts) {
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
                Ok(negotiated) => {
                    self.serve(&conn, negotiated, peer_addr(self.index, 0), scripts)
                        .await
                }
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

    /// Records `body` on `remote`'s QUIC stream `quic_stream`, for
    /// [`PeerControl`] to place.
    fn record(&self, remote: SocketAddr, quic_stream: u64, body: RecordBody) {
        self.records.borrow_mut().push_back(PeerRecord {
            remote,
            quic_stream,
            body,
        });
    }

    /// Writes `frames` on `stream` in order, recording the `PeerFrame` of
    /// each frame of a write before it and their `PeerWritten`s after.
    /// False once a write fails, which leaves the rest unwritten.
    async fn write(
        &self,
        remote: SocketAddr,
        stream: &mut HeaderStream,
        frames: Vec<Outgoing>,
    ) -> bool {
        for frame in frames {
            let mut written = Vec::with_capacity(frame.records.len());
            for record in frame.records {
                let RecordBody::Frame { seq, .. } = record else {
                    unreachable!("an outgoing frame records a PeerFrame");
                };
                written.push(seq);
                self.record(remote, stream.id, record);
            }
            if stream.send.write_all(&frame.bytes).await.is_err() {
                return false;
            }
            for seq in written {
                self.record(remote, stream.id, RecordBody::Written { seq });
            }
        }
        true
    }

    /// Serves one connection from `remote` whose handshake negotiated
    /// `negotiated`, until it closes. The peer is ready for an action while
    /// it's idle and settled: no header stream is expected, or the node's
    /// first frame has arrived on it, which the node sends once it admits
    /// the session, and the honest script, if it runs one, is settled too.
    async fn serve(
        &mut self,
        conn: &Conn,
        negotiated: u64,
        remote: SocketAddr,
        scripts: &mut Scripts,
    ) -> Ended {
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
        let mut header: Option<HeaderStream> = None;
        // Frames read so far on each header stream, the next one's `seq`.
        let mut reads = BTreeMap::new();
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
            reads.insert(id, 0);
            let mut stream = HeaderStream { id, send };
            if let Some(honest) = scripts.honest.as_mut() {
                let frames = honest.start_stream();
                if !self.write(remote, &mut stream, frames).await {
                    return Ended::Closed;
                }
            }
            header = Some(stream);
        }
        loop {
            let now = Instant::now();
            let next = self.spec.script.get(self.taken).copied();
            let wake = scripts.wake(next);
            let ready =
                settled && (scripts.honest.is_none() || wake.is_some_and(|wake| now >= wake));
            self.readiness.set(ready);
            // The honest script's next timer: its refresh, or when it may
            // become settled, which makes the peer ready.
            let deadline = scripts.honest.as_ref().map(|honest| {
                wake.filter(|wake| *wake > now)
                    .map_or(honest.deadline(), |wake| wake.min(honest.deadline()))
            });
            let mut frames = Vec::new();
            zakura_rt::select! {
                Some(event) = stream_events.recv() => match event {
                    StreamEvent::Header { id, send } => {
                        reads.insert(id, 0);
                        header = Some(HeaderStream { id, send });
                        if let Some(honest) = scripts.honest.as_mut() {
                            frames = honest.start_stream();
                        }
                    }
                    StreamEvent::Frame { id, frame } => {
                        // Every header stream's reads are recorded, a
                        // finished or replaced one's included, since the
                        // node can still write on it.
                        let read = reads.get_mut(&id).expect("a header stream's reader is known");
                        let seq = *read;
                        *read += 1;
                        let current = header.as_ref().is_some_and(|stream| stream.id == id);
                        settled |= current;
                        let Ok(message) = decode_frame::<RegtestMessage>(&frame) else {
                            return self.fail(format!(
                                "the node's frame {seq} on stream {id} doesn't decode"
                            ));
                        };
                        self.record(remote, id, RecordBody::Received {
                            seq,
                            message_type: frame.message_type,
                            // A frame's payload is under its 2 MiB cap.
                            payload: frame.payload.len() as u64,
                            credit: credit(&message),
                        });
                        if let Some(adversary) = scripts.adversary.as_mut().filter(|_| current) {
                            adversary.before_receive(&message);
                        }
                        if let Some(honest) = scripts.honest.as_mut().filter(|_| current) {
                            frames = honest.receive(&message, frame.payload.len());
                        }
                    }
                },
                Some(action) = self.commands.recv() => {
                    self.readiness.taken();
                    self.taken += 1;
                    match action {
                        PeerAction::Open { kind, version } => open(conn, kind, version).await,
                        PeerAction::FinishHeader => {
                            let Some(mut stream) = header.take() else {
                                return self.fail("can't finish-header without a header stream");
                            };
                            let _ = stream.send.finish();
                            // A responder waits for the node to reopen it.
                            settled = self.spec.dialer == Dialer::Peer;
                        }
                        PeerAction::Close => {
                            conn.close(VarInt::from_u32(0), b"close");
                            conn.closed().await;
                            return Ended::Closed;
                        }
                        PeerAction::Redial => return self.fail("can't redial a live connection"),
                        PeerAction::Grant => {
                            match scripts.honest.as_mut().map(Honest::release_grant) {
                                Some(Ok(released)) => frames = released,
                                Some(Err(why)) => return self.fail(why),
                                None => return self.fail("can't grant without the honest script"),
                            }
                        }
                        PeerAction::Act { act } => {
                            let (Some(adversary), Some(honest)) =
                                (scripts.adversary.as_mut(), scripts.honest.as_mut())
                            else {
                                return self.fail("only an adversary can act");
                            };
                            match adversary.act(act, honest) {
                                Ok(acted) => frames = acted,
                                Err(why) => return self.fail(why),
                            }
                        }
                    }
                }
                () = zakura_rt::time::sleep_until(deadline.unwrap_or(now)), if deadline.is_some() => {
                    if let Some(honest) = scripts.honest.as_mut() {
                        frames = honest.tick();
                    }
                }
                _ = conn.closed() => {
                    self.readiness.set(false);
                    return Ended::Closed;
                }
            }
            if let Some(stream) = header.as_mut() {
                if !frames.is_empty() && !self.write(remote, stream, frames).await {
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
