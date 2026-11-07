//! The discovery script (disc-2.2): what a scripted peer does on a
//! discovery stream, and the fixture's signed records and answers.
//!
//! A peer with a [`DiscoveryScript`] runs it on stream 4 once its handshake
//! negotiated discovery. As the dialer it opens the stream; as the listener
//! it takes the one the node opens. It sends its own `Hello`, answers each
//! `GetPeers` with `Peers` and each `GetServices` with `Services`, and holds
//! an answer until the virtual time its script names. [`PeerAction::Ask`]
//! sends its own requests.
//!
//! Every frame the peer sends is labelled before the write by reg-5's
//! [`FrameModel`], [`ExchangeModel`] and [`CadenceModel`] over discovery's
//! rows, which come from `DiscoveryService::streams()`, so the labels follow
//! the table the node enforces without a copy of it, and by the content
//! labeller ([`content`]). A `Peers` also owes no more than its `GetPeers`
//! limit's cap, which the node's claim checks: [`ExchangeModel`] owes every
//! answer the row's largest ending, as the node's reservation does.
//!
//! A peer also takes [`DiscoveryAct`]s, the receiver-direction boundaries of
//! disc-3 ([`adversary`]).
//!
//! [`PeerAction::Ask`]: super::PeerAction::Ask

use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::{Duration, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use zakura_chain::parameters::Network;
use zakura_network::zakura::{
    wire_codec::{encode_frame, WireMessage},
    DiscoveryMessage, DiscoveryService, DiscoveryServiceSummary, GetPeers, GetServices,
    MessageRole, Service, ServiceSummaryEnvelope, Services, Stream, ZakuraDiscoveryConfig,
    ZakuraDiscoveryHandle, ZakuraDiscoveryLocalConfig, ZakuraHandshakeConfig, ZakuraNodeRecord,
    ZakuraNodeRecordBody, ZakuraServiceId, MAX_DISCOVERY_RECORDS_PER_RESPONSE,
    MAX_NODE_RECORD_BYTES, MSG_DISCOVERY_PEERS,
};
use zakura_quic::{NodeId, NodeSecretKey};
use zakura_rt::{
    sync::watch,
    time::{utc_now, Instant},
};

mod adversary;
mod content;

pub use self::adversary::{DiscoveryAct, DiscoveryRow, DiscoveryRule};
use self::content::DiscoveryContent;
use super::{evidence::RecordBody, frame_bytes, Outgoing};
use crate::{
    model::{
        CadenceModel, Charge, ExchangeModel, FrameHeader, FrameModel, Label, Labels, Owed,
        ReaderEnd,
    },
    observe::StreamId,
    table::rule_row,
};

/// The public address the fixture's relayed records name.
pub(crate) const FIXTURE_ADDR: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(45, 33, 30, 45)), 8233);

/// A signed record for `secret` at `direct_addrs`, valid for ten minutes
/// past `now`, in the run's UTC, which is the clock discovery validates it
/// against.
pub(crate) fn record(
    secret: &NodeSecretKey,
    handshake: &ZakuraHandshakeConfig,
    now: u64,
    direct_addrs: Vec<SocketAddr>,
) -> ZakuraNodeRecord {
    ZakuraNodeRecord::sign(body(secret, handshake, now, direct_addrs), secret)
        .expect("a fixture record signs")
}

/// The body of [`record`].
fn body(
    secret: &NodeSecretKey,
    handshake: &ZakuraHandshakeConfig,
    now: u64,
    direct_addrs: Vec<SocketAddr>,
) -> ZakuraNodeRecordBody {
    ZakuraNodeRecordBody {
        node_id: secret.public(),
        direct_addrs,
        services: vec![ZakuraServiceId::discovery()],
        zakura_protocol_min: handshake.zakura_protocol_min,
        zakura_protocol_max: handshake.zakura_protocol_max,
        network_id: handshake.network_id,
        chain_id: handshake.chain_id,
        sequence: 1,
        expires_at_unix_secs: now.saturating_add(600),
        extensions: Vec::new(),
    }
}

/// The key of the author of a script's `index`th relayed record.
fn relay_author(index: u8) -> NodeSecretKey {
    NodeSecretKey::from_bytes(&[0x40 + index; 32])
}

/// The node ID of the author of a script's `index`th relayed record, the
/// same for every peer's script.
pub fn relayed_node(index: u8) -> NodeId {
    relay_author(index).public()
}

/// The run's UTC, in Unix seconds.
pub(crate) fn unix_now() -> u64 {
    utc_now()
        .duration_since(UNIX_EPOCH)
        .expect("the run clock is after the epoch")
        .as_secs()
}

/// `node_id`'s `Services` answer: one discovery summary that never goes
/// stale.
pub(crate) fn services(node_id: NodeId) -> Services {
    let summary = DiscoveryServiceSummary {
        peer_exchange_slots_free: 1,
        max_records_per_response: 1,
        expected_disconnect_after_exchange: true,
    };
    Services {
        node_id,
        expires_at_unix_secs: u64::MAX,
        summaries: vec![
            ServiceSummaryEnvelope::discovery(&summary).expect("the fixture summary encodes")
        ],
    }
}

/// Discovery's stream, as `DiscoveryService::streams()` declares it, read
/// from a service on a fixture handle. Building the handle draws its
/// disclosure key from the run's RNG, so a harness calls this once, before
/// its tasks run.
pub fn discovery_stream() -> Stream {
    let handshake = ZakuraHandshakeConfig::for_network(&Network::new_regtest(Default::default()));
    let (_connected, connected) = watch::channel(Vec::new());
    let handle = ZakuraDiscoveryHandle::new(
        ZakuraDiscoveryLocalConfig {
            secret_key: NodeSecretKey::from_bytes(&[21; 32]),
            direct_addrs: Vec::new(),
            services: vec![ZakuraServiceId::discovery()],
            zakura_protocol_min: handshake.zakura_protocol_min,
            zakura_protocol_max: handshake.zakura_protocol_max,
            network_id: handshake.network_id,
            chain_id: handshake.chain_id,
            last_authored_sequence: None,
        },
        ZakuraDiscoveryConfig::default(),
        connected,
    )
    .expect("the fixture config is valid");
    let streams = DiscoveryService::new(handle).streams().to_vec();
    let [stream] = streams[..] else {
        panic!("discovery declares one stream, not {}", streams.len());
    };
    stream
}

/// A scripted peer's discovery script.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryScript {
    /// How many records it relays in each `Peers`, at most 32 and at most
    /// the node's limit. Each is signed by its own fixture author and names
    /// [`FIXTURE_ADDR`].
    #[serde(default)]
    pub relays: u8,
    /// The answers it holds, each until a virtual time.
    #[serde(default)]
    pub holds: Vec<Hold>,
}

/// An answer the peer holds until `until_ms` milliseconds past the run's
/// start. One due earlier waits until then, and one due later goes when
/// due.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hold {
    /// The answer.
    pub answer: Answer,
    /// When it may go, in milliseconds past the run's start.
    pub until_ms: u64,
}

/// A frame the peer sends because the node's session expects it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Answer {
    /// Its own `Hello`, once its stream starts.
    Hello,
    /// `Peers`, for each `GetPeers`.
    Peers,
    /// `Services`, for each `GetServices`.
    Services,
}

/// A request the peer sends on its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Request {
    /// `GetPeers` for the most records a response may carry.
    GetPeers,
    /// `GetServices` for every service.
    GetServices,
}

impl Request {
    /// The request's name in its action's identity.
    pub fn name(self) -> &'static str {
        match self {
            Request::GetPeers => "get-peers",
            Request::GetServices => "get-services",
        }
    }
}

/// The model session of a discovery stream: each stream has models of its
/// own.
const SESSION: u64 = 0;

/// An answer waiting for its hold.
struct Held {
    due: Instant,
    answer: Answer,
    message: DiscoveryMessage,
    /// The node's request it answers.
    exchange: Option<u64>,
}

/// A frame a peer writes on a discovery stream: the message its labels
/// read, the header and payload it actually writes, and the records whose
/// signature the fixture forged. An honest frame is its message encoded. A
/// crafted one may declare another type, flags or length, or carry bytes
/// the encoder refuses.
#[derive(Clone, Debug)]
pub(crate) struct DiscoveryFrame {
    /// What the frame means to the role and content labels.
    message: DiscoveryMessage,
    /// The header written.
    header: FrameHeader,
    /// The bytes written after the header.
    payload: Vec<u8>,
    /// The message's records whose signature the fixture forged, by place.
    forged: Vec<usize>,
}

impl DiscoveryFrame {
    /// `message`, encoded as it is.
    fn of(message: DiscoveryMessage) -> Self {
        let frame = encode_frame(&message).expect("a fixture frame starts from a valid message");
        DiscoveryFrame {
            header: FrameHeader {
                message_type: frame.message_type,
                flags: frame.flags,
                payload_len: frame.payload.len(),
            },
            payload: frame.payload,
            message,
            forged: Vec::new(),
        }
    }

    /// Writes `len` payload bytes: the payload cut short, or padded with
    /// zeros.
    fn resize(&mut self, len: usize) {
        self.payload.resize(len, 0);
        self.header.payload_len = len;
    }
}

/// The most a `Peers` answer to a `GetPeers` with `limit` may carry, as the
/// node's claim checks it: the record count, then at most `limit` of the
/// largest records. The `peers` row's minimum is an empty `Peers`, its
/// count alone.
fn peers_cap(peers: &zakura_network::zakura::MessageRule, limit: u16) -> u64 {
    let bytes = peers.payload.min() + usize::from(limit) * MAX_NODE_RECORD_BYTES;
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

/// One discovery stream's role models and the answers it holds.
struct Models {
    /// When the stream started, the cadence model's origin.
    start: Instant,
    frames: FrameModel,
    /// The node's requests, which the peer answers, by request type.
    node_asks: BTreeMap<u16, ExchangeModel>,
    /// The peer's requests, which the node answers, by request type.
    peer_asks: BTreeMap<u16, ExchangeModel>,
    cadence: CadenceModel,
    /// The node's `GetPeers` limits, by the exchange key its answer takes.
    limits: BTreeMap<u64, u16>,
    /// The next exchange key, shared by both ends' requests.
    next_key: u64,
    /// Frames labelled so far, the next one's `seq`.
    sent: u64,
    held: Vec<Held>,
    /// The answers sent at least once.
    answered: Vec<Answer>,
    /// The request row of a [`DiscoveryAct::RequestRun`] under way.
    run: Option<Request>,
    /// The requests that run sent.
    run_sent: usize,
}

impl Models {
    /// Fresh models for a stream of `stream` that starts now.
    fn new(stream: &Stream) -> Self {
        let rules = stream.messages.expect("discovery's stream has a table");
        let requests = || {
            rules
                .iter()
                .filter(|row| matches!(row.role, MessageRole::Request { .. }))
                .map(|row| {
                    let model = ExchangeModel::new(rules, row.message_type)
                        .expect("a request row has an exchange model");
                    (row.message_type, model)
                })
                .collect::<BTreeMap<_, _>>()
        };
        let mut peer_asks = requests();
        for model in peer_asks.values_mut() {
            // Discovery advertises no request limit, so each row's
            // `max_in_flight` holds.
            model.advertise(SESSION, u32::MAX);
        }
        Models {
            start: Instant::now(),
            frames: FrameModel::new(rules, ReaderEnd::Persistent),
            node_asks: requests(),
            peer_asks,
            cadence: CadenceModel::new(),
            limits: BTreeMap::new(),
            next_key: 0,
            sent: 0,
            held: Vec::new(),
            answered: Vec::new(),
            run: None,
            run_sent: 0,
        }
    }

    /// A new exchange key.
    fn key(&mut self) -> u64 {
        let key = self.next_key;
        self.next_key += 1;
        key
    }
}

/// What a peer needs to run its [`DiscoveryScript`], and the models of its
/// current discovery stream.
pub(crate) struct DiscoveryPeer {
    stream: Stream,
    script: DiscoveryScript,
    secret: NodeSecretKey,
    /// The address its `Hello` names.
    addr: SocketAddr,
    handshake: ZakuraHandshakeConfig,
    /// The authors of the records it relays.
    relays: Vec<NodeSecretKey>,
    content: DiscoveryContent,
    models: Option<Models>,
}

impl DiscoveryPeer {
    /// The peer that binds `secret` and listens at `addr`, running `script`
    /// on discovery's `stream`. An error if the script relays more records
    /// than a response may carry.
    pub(crate) fn new(
        stream: Stream,
        script: DiscoveryScript,
        secret: NodeSecretKey,
        addr: SocketAddr,
    ) -> Result<Self, String> {
        if usize::from(script.relays) > MAX_DISCOVERY_RECORDS_PER_RESPONSE {
            return Err(format!(
                "a discovery script relays at most {MAX_DISCOVERY_RECORDS_PER_RESPONSE} records, \
                 not {}",
                script.relays
            ));
        }
        let relays = (0..script.relays).map(relay_author).collect();
        let handshake =
            ZakuraHandshakeConfig::for_network(&Network::new_regtest(Default::default()));
        // The node runs on the peer's network.
        let content = DiscoveryContent::new(handshake, secret.public());
        Ok(DiscoveryPeer {
            stream,
            script,
            secret,
            addr,
            handshake,
            relays,
            content,
            models: None,
        })
    }

    /// Discovery's stream, the one the peer runs its script on.
    pub(crate) fn stream(&self) -> Stream {
        self.stream
    }

    /// A discovery stream started: fresh models, and the peer's `Hello`,
    /// unless it's held.
    pub(crate) fn start(&mut self) -> Vec<Outgoing> {
        self.models = Some(Models::new(&self.stream));
        let hello = self.hello();
        self.queue(Answer::Hello, hello, None);
        self.tick()
    }

    /// The peer's own `Hello`, valid for ten minutes from now.
    fn hello(&self) -> DiscoveryMessage {
        DiscoveryMessage::Hello {
            record: record(&self.secret, &self.handshake, unix_now(), vec![self.addr]),
        }
    }

    /// Follows the node's `message` and queues the answer to a request.
    /// Returns the answers due now.
    pub(crate) fn receive(&mut self, message: &DiscoveryMessage) -> Vec<Outgoing> {
        let message_type = message.message_type();
        let models = self.models.as_mut().expect("a discovery stream started");
        let row = models.frames.row(message_type);
        let mut limit = None;
        let mut run = Vec::new();
        let answer = match message {
            DiscoveryMessage::Hello { .. } => None,
            DiscoveryMessage::GetPeers(query) => {
                limit = Some(query.limit);
                let records = self
                    .relays
                    .iter()
                    .filter(|author| !query.exclude_node_ids.contains(author.public().as_bytes()))
                    .take(usize::from(query.limit))
                    .map(|author| record(author, &self.handshake, unix_now(), vec![FIXTURE_ADDR]))
                    .collect();
                Some((Answer::Peers, DiscoveryMessage::Peers { records }))
            }
            DiscoveryMessage::GetServices(_) => Some((
                Answer::Services,
                DiscoveryMessage::Services(services(self.secret.public())),
            )),
            DiscoveryMessage::Peers { .. } | DiscoveryMessage::Services(_) => {
                if let Some(MessageRole::Response { request, .. }) = row.map(|row| row.role) {
                    if let Some(model) = models.peer_asks.get_mut(&request) {
                        model.answered(SESSION);
                    }
                    // A run asks again once the node answered the last.
                    if let Some(next) = models.run.filter(|ran| request == ran.message_type()) {
                        run.push(next);
                    }
                }
                None
            }
        };
        if let Some((answer, reply)) = answer {
            let key = models.key();
            if let Some(limit) = limit {
                models.limits.insert(key, limit);
            }
            // A zero owes the answer its row's largest ending, the most
            // `ExchangeModel` lets any request owe.
            let owed = Owed {
                frames: 0,
                bytes: 0,
            };
            if let Some(model) = models.node_asks.get_mut(&message_type) {
                model.sent(SESSION, key, owed);
            }
            self.queue(answer, reply, Some(key));
        }
        let mut out = self.tick();
        for request in run {
            out.extend(self.run_step(request));
        }
        out
    }

    /// The peer's own `request` on its current discovery stream, labelled.
    pub(crate) fn ask(&mut self, request: Request) -> Outgoing {
        let message = request.message();
        self.send_new(DiscoveryFrame::of(message))
    }

    /// Labels `frame`, a request, under a new exchange key, as
    /// [`Self::send`] does.
    fn send_new(&mut self, frame: DiscoveryFrame) -> Outgoing {
        let key = self
            .models
            .as_mut()
            .expect("a discovery stream started")
            .key();
        self.send(frame, Some(key))
    }

    /// When the next held answer is due, if one waits.
    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.models.as_ref()?.held.iter().map(|held| held.due).min()
    }

    /// The held answers due now, in the order they were queued.
    pub(crate) fn tick(&mut self) -> Vec<Outgoing> {
        let now = Instant::now();
        let Some(models) = self.models.as_mut() else {
            return Vec::new();
        };
        let (due, waiting) = std::mem::take(&mut models.held)
            .into_iter()
            .partition::<Vec<_>, _>(|held| held.due <= now);
        models.held = waiting;
        due.into_iter()
            .map(|held| self.send(DiscoveryFrame::of(held.message), held.exchange))
            .collect()
    }

    /// Takes the held `answer`, and returns its message and the exchange it
    /// answers. `None` if none is held.
    fn take_held(&mut self, answer: Answer) -> Option<(DiscoveryMessage, Option<u64>)> {
        let held = &mut self.models.as_mut()?.held;
        let at = held.iter().position(|held| held.answer == answer)?;
        let held = held.remove(at);
        Some((held.message, held.exchange))
    }

    /// The held `answer`'s message, if one is held.
    fn held(&self, answer: Answer) -> Option<&DiscoveryMessage> {
        self.models
            .as_ref()?
            .held
            .iter()
            .find(|held| held.answer == answer)
            .map(|held| &held.message)
    }

    /// Queues `message`, the peer's `answer` to exchange `exchange`, until
    /// its hold passes.
    fn queue(&mut self, answer: Answer, message: DiscoveryMessage, exchange: Option<u64>) {
        let now = Instant::now();
        let due = self
            .script
            .holds
            .iter()
            .filter(|hold| hold.answer == answer)
            .map(|hold| {
                let elapsed = zakura_scheduler::hooks::now()
                    .expect("a peer runs inside a run")
                    .since_start();
                now + Duration::from_millis(hold.until_ms).saturating_sub(elapsed)
            })
            .max()
            .unwrap_or(now);
        let models = self.models.as_mut().expect("a discovery stream started");
        models.held.push(Held {
            due,
            answer,
            message,
            exchange,
        });
    }

    /// Labels `frame` with the stream's models and its content, and returns
    /// its bytes and record. A request opens exchange `exchange`, and a
    /// response answers the node's request `exchange`: with none, it's
    /// unsolicited. A conformant frame changes the models as its sending
    /// does, and a faulty one is labelled on a copy of them.
    fn send(&mut self, frame: DiscoveryFrame, exchange: Option<u64>) -> Outgoing {
        let header = frame.header;
        let stream = &self.stream;
        let content = &self.content;
        let models = self.models.as_mut().expect("a discovery stream started");
        let row = models.frames.row(header.message_type);
        let own_row = row.is_some() && frame.message.message_type() == header.message_type;
        let limit = exchange.and_then(|key| models.limits.get(&key).copied());
        let faults = models.frames.labels(&header).union(content.label(
            &frame.message,
            &frame.forged,
            limit,
        ));
        let role = match row.map(|row| row.role).filter(|_| own_row) {
            Some(MessageRole::Request { .. }) => {
                let model = models
                    .peer_asks
                    .get_mut(&header.message_type)
                    .expect("every request row has a model");
                if faults.is_conformant() {
                    model.request_frame(SESSION)
                } else {
                    model.clone().request_frame(SESSION)
                }
            }
            Some(MessageRole::Response { request, .. }) => {
                let model = models
                    .node_asks
                    .get_mut(&request)
                    .expect("every request row has a model");
                let peers = row.filter(|row| row.message_type == u16::from(MSG_DISCOVERY_PEERS));
                match exchange {
                    None => Labels::of(Label::Unsolicited),
                    // The node's claim refuses a `Peers` above its own
                    // `GetPeers` limit's cap, below the row's largest.
                    Some(key)
                        if model.owed(SESSION, key).is_some()
                            && peers.zip(limit).is_some_and(|(peers, limit)| {
                                header.payload_len as u64 > peers_cap(peers, limit)
                            }) =>
                    {
                        Labels::of(Label::OverOwed)
                    }
                    Some(key) if faults.is_conformant() => {
                        model.response_frame(SESSION, key, &header)
                    }
                    Some(key) => model.clone().response_frame(SESSION, key, &header),
                }
            }
            _ => Labels::NONE,
        };
        let labels = faults.union(role);
        if labels.is_conformant() {
            if let Some(answer) = answer_of(&frame.message) {
                if !models.answered.contains(&answer) {
                    models.answered.push(answer);
                }
            }
        }
        let label = labels.first();
        let id = |stream: &Stream| StreamId {
            kind: stream.kind,
            version: stream.version,
        };
        let layout = std::slice::from_ref(stream);
        let (row_name, rule_stream) = match row {
            Some(row) => {
                let (rule_stream, rule) = rule_row(layout, stream, row, label);
                (Some(rule.name.to_owned()), id(rule_stream))
            }
            None => (None, id(stream)),
        };
        let exhausted = row.is_some_and(|row| {
            let now = Instant::now().saturating_duration_since(models.start);
            models.cadence.charge(SESSION, stream.version, row, now) == Charge::Exhausted
        });
        let seq = models.sent;
        models.sent += 1;
        Outgoing {
            bytes: frame_bytes(&header, &frame.payload),
            records: vec![RecordBody::Frame {
                seq,
                row: row_name,
                rule_stream,
                label,
                exhausted,
                exchange,
                // A frame's payload is under its stream's frame cap.
                payload: header.payload_len as u64,
                prepares: None,
            }],
        }
    }
}

/// The answer `message` is, if it's one.
fn answer_of(message: &DiscoveryMessage) -> Option<Answer> {
    match message {
        DiscoveryMessage::Hello { .. } => Some(Answer::Hello),
        DiscoveryMessage::Peers { .. } => Some(Answer::Peers),
        DiscoveryMessage::Services(_) => Some(Answer::Services),
        DiscoveryMessage::GetPeers(_) | DiscoveryMessage::GetServices(_) => None,
    }
}

impl Request {
    /// The request's message: `GetPeers` for the most records a response
    /// may carry, or `GetServices` for every service.
    fn message(self) -> DiscoveryMessage {
        match self {
            Request::GetPeers => DiscoveryMessage::GetPeers(GetPeers {
                limit: u16::try_from(MAX_DISCOVERY_RECORDS_PER_RESPONSE)
                    .expect("the wire limit fits u16"),
                wanted_services: Vec::new(),
                exclude_node_ids: Vec::new(),
            }),
            Request::GetServices => DiscoveryMessage::GetServices(GetServices {
                wanted_services: Vec::new(),
            }),
        }
    }

    /// The request's message type.
    fn message_type(self) -> u16 {
        self.message().message_type()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A peer on discovery's stream, with that stream's models started.
    fn started() -> DiscoveryPeer {
        let mut peer = DiscoveryPeer::new(
            discovery_stream(),
            DiscoveryScript::default(),
            NodeSecretKey::from_bytes(&[7; 32]),
            FIXTURE_ADDR,
        )
        .expect("the default script is valid");
        peer.models = Some(Models::new(&peer.stream));
        peer
    }

    /// The label of `sent`'s one frame.
    fn label(sent: &Outgoing) -> Label {
        match sent.records[..] {
            [RecordBody::Frame { label, .. }] => label,
            ref records => panic!("one frame record, not {records:?}"),
        }
    }

    /// The peer's frames take reg-5's labels over discovery's rows. A third
    /// unanswered `GetPeers` passes twice the row's `max_in_flight` of 1, and
    /// a `Peers` for an exchange the node never opened is unsolicited.
    #[test]
    fn discovery_frames_take_the_models_labels() {
        let mut peer = started();
        let asks: Vec<_> = (0..3)
            .map(|_| label(&peer.ask(Request::GetPeers)))
            .collect();
        assert_eq!(
            asks,
            [Label::Conformant, Label::Conformant, Label::OverMargin]
        );
        let peers = DiscoveryMessage::Peers {
            records: Vec::new(),
        };
        assert_eq!(
            label(&peer.send(DiscoveryFrame::of(peers.clone()), Some(9))),
            Label::Unsolicited
        );
        assert_eq!(
            label(&peer.send(DiscoveryFrame::of(peers), None)),
            Label::Unsolicited,
            "a response with no exchange answers nothing"
        );
    }
}
