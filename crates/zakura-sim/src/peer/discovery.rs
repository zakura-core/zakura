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
//! the table the node enforces without a copy of it.
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
};
use zakura_quic::{NodeId, NodeSecretKey};
use zakura_rt::{
    sync::watch,
    time::{utc_now, Instant},
};

use super::{evidence::RecordBody, frame_bytes, Outgoing};
use crate::{
    model::{CadenceModel, Charge, ExchangeModel, FrameHeader, FrameModel, Owed, ReaderEnd},
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
    let body = ZakuraNodeRecordBody {
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
    };
    ZakuraNodeRecord::sign(body, secret).expect("a fixture record signs")
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
    message: DiscoveryMessage,
    /// The node's request it answers.
    exchange: Option<u64>,
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
    /// The next exchange key, shared by both ends' requests.
    next_key: u64,
    /// Frames labelled so far, the next one's `seq`.
    sent: u64,
    held: Vec<Held>,
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
            next_key: 0,
            sent: 0,
            held: Vec::new(),
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
        let relays = (0..script.relays)
            .map(|author| NodeSecretKey::from_bytes(&[0x40 + author; 32]))
            .collect();
        Ok(DiscoveryPeer {
            stream,
            script,
            secret,
            addr,
            handshake: ZakuraHandshakeConfig::for_network(
                &Network::new_regtest(Default::default()),
            ),
            relays,
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
        let hello = DiscoveryMessage::Hello {
            record: record(&self.secret, &self.handshake, unix_now(), vec![self.addr]),
        };
        self.queue(Answer::Hello, hello, None);
        self.tick()
    }

    /// Follows the node's `message` and queues the answer to a request.
    /// Returns the answers due now.
    pub(crate) fn receive(&mut self, message: &DiscoveryMessage) -> Vec<Outgoing> {
        let message_type = message.message_type();
        let models = self.models.as_mut().expect("a discovery stream started");
        let row = models.frames.row(message_type);
        let answer = match message {
            DiscoveryMessage::Hello { .. } => None,
            DiscoveryMessage::GetPeers(query) => {
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
                }
                None
            }
        };
        if let Some((answer, reply)) = answer {
            let key = models.key();
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
        self.tick()
    }

    /// The peer's own `request` on its current discovery stream, labelled.
    pub(crate) fn ask(&mut self, request: Request) -> Outgoing {
        let message = match request {
            Request::GetPeers => DiscoveryMessage::GetPeers(GetPeers {
                limit: u16::try_from(MAX_DISCOVERY_RECORDS_PER_RESPONSE)
                    .expect("the wire limit fits u16"),
                wanted_services: Vec::new(),
                exclude_node_ids: Vec::new(),
            }),
            Request::GetServices => DiscoveryMessage::GetServices(GetServices {
                wanted_services: Vec::new(),
            }),
        };
        let key = self
            .models
            .as_mut()
            .expect("a discovery stream started")
            .key();
        self.send(&message, Some(key))
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
            .map(|held| self.send(&held.message, held.exchange))
            .collect()
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
            message,
            exchange,
        });
    }

    /// Labels `message` with the stream's models and returns its bytes and
    /// record. A request opens exchange `exchange`, and a response answers
    /// the node's request `exchange`. A conformant frame changes the models
    /// as its sending does.
    fn send(&mut self, message: &DiscoveryMessage, exchange: Option<u64>) -> Outgoing {
        let frame = encode_frame(message).expect("the fixture's frames encode");
        let header = FrameHeader {
            message_type: frame.message_type,
            flags: frame.flags,
            payload_len: frame.payload.len(),
        };
        let stream = &self.stream;
        let models = self.models.as_mut().expect("a discovery stream started");
        let row = models.frames.row(header.message_type);
        let mut labels = models.frames.labels(&header);
        if labels.is_conformant() {
            labels = match row.map(|row| row.role) {
                Some(MessageRole::Request { .. }) => models
                    .peer_asks
                    .get_mut(&header.message_type)
                    .map_or(labels, |model| model.request_frame(SESSION)),
                Some(MessageRole::Response { request, .. }) => {
                    match (exchange, models.node_asks.get_mut(&request)) {
                        (Some(key), Some(model)) => model.response_frame(SESSION, key, &header),
                        _ => labels,
                    }
                }
                _ => labels,
            };
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Label;

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
        assert_eq!(label(&peer.send(&peers, Some(9))), Label::Unsolicited);
    }
}
