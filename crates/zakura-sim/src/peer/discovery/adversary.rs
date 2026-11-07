//! Discovery acts (disc-3.1): a scripted peer breaks discovery's rules on
//! purpose, one `Plan`-picked [`DiscoveryAct`] at a time, as sim-5's
//! [`Act`](crate::peer::adversary::Act)s do on header sync's rows.
//!
//! An act is offered only once the peer's discovery stream holds the state
//! it needs ([`DiscoveryPeer::ready`]): a held answer to fault, the node's
//! exchange already answered, or no request of the peer's own open. When it
//! runs, it builds its frames from the models' state at that moment, and
//! checks that the models give the act's frame exactly the act's label
//! before sending it. A frame with a fault is labelled on a copy of the
//! models, so it changes no model state.
//!
//! An act on a response row takes the answer the peer holds for the node's
//! open exchange and sends it changed, so the frame breaks only the act's
//! rule: a node that skips that rule's check admits it as the answer.

use serde::{Deserialize, Serialize};
use zakura_network::zakura::{
    wire_codec::WireMessage, DiscoveryMessage, MessageRole, MessageRule, ZakuraNodeRecord,
    ZakuraServiceId,
};
use zakura_quic::NodeSecretKey;

use super::{
    body, peers_cap, services, unix_now, Answer, DiscoveryFrame, DiscoveryPeer, Request, SESSION,
};
use crate::{model::Label, peer::Outgoing};

/// A row of discovery's table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DiscoveryRow {
    /// `hello`.
    Hello,
    /// `get-peers`.
    GetPeers,
    /// `peers`.
    Peers,
    /// `get-services`.
    GetServices,
    /// `services`.
    Services,
}

impl DiscoveryRow {
    /// Every row, in table order.
    pub const ALL: [DiscoveryRow; 5] = [
        DiscoveryRow::Hello,
        DiscoveryRow::GetPeers,
        DiscoveryRow::Peers,
        DiscoveryRow::GetServices,
        DiscoveryRow::Services,
    ];

    /// The row's name in the table.
    pub fn name(self) -> &'static str {
        match self {
            DiscoveryRow::Hello => "hello",
            DiscoveryRow::GetPeers => "get-peers",
            DiscoveryRow::Peers => "peers",
            DiscoveryRow::GetServices => "get-services",
            DiscoveryRow::Services => "services",
        }
    }

    /// The row's rule.
    pub fn rule(self) -> &'static MessageRule {
        <DiscoveryMessage as WireMessage>::RULES
            .iter()
            .find(|rule| rule.name == self.name())
            .expect("discovery has every row")
    }

    /// The request a row's frame is, if it's a request row.
    fn request(self) -> Option<Request> {
        match self {
            DiscoveryRow::GetPeers => Some(Request::GetPeers),
            DiscoveryRow::GetServices => Some(Request::GetServices),
            _ => None,
        }
    }

    /// The answer a row's frame is, if it's a response row.
    fn answer(self) -> Option<Answer> {
        match self {
            DiscoveryRow::Peers => Some(Answer::Peers),
            DiscoveryRow::Services => Some(Answer::Services),
            _ => None,
        }
    }
}

/// A content rule the content labeller checks ([`super::content`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DiscoveryRule {
    /// A `Hello` whose record carries another body's signature.
    HelloSignature,
    /// A `GetPeers` whose limit is 0.
    GetPeersLimit,
    /// A `Peers` whose first record carries another body's signature.
    PeersSignature,
    /// A `GetServices` that wants one service twice.
    GetServicesWanted,
    /// A `Services` authored by another node ID.
    ServicesNodeId,
}

impl DiscoveryRule {
    /// Every rule, one per row, in table order.
    pub const ALL: [DiscoveryRule; 5] = [
        DiscoveryRule::HelloSignature,
        DiscoveryRule::GetPeersLimit,
        DiscoveryRule::PeersSignature,
        DiscoveryRule::GetServicesWanted,
        DiscoveryRule::ServicesNodeId,
    ];

    /// The row the rule's frame is on.
    pub fn row(self) -> DiscoveryRow {
        DiscoveryRow::ALL[DiscoveryRule::ALL
            .iter()
            .position(|rule| *rule == self)
            .expect("every rule is listed")]
    }
}

/// One discovery act, the `peer:<name>:<act>` a `Plan` picks. Each fault act
/// reaches one `<label>-closes` ID, and each conformant one a control.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "act", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DiscoveryAct {
    /// A `Hello` sent as a type the table doesn't declare.
    UndeclaredType {
        /// The type.
        message_type: u16,
    },
    /// A frame of `row` with flag bit `bit` set.
    ReservedFlags {
        /// The row.
        row: DiscoveryRow,
        /// The flag bit, 0 to 15.
        bit: u8,
    },
    /// A frame of `row` one byte under its minimum.
    ShortPayload {
        /// The row.
        row: DiscoveryRow,
    },
    /// A frame of `row` one byte over its maximum.
    LongPayload {
        /// The row.
        row: DiscoveryRow,
    },
    /// Requests of `row`, written at once, until one is past twice the
    /// row's `max_in_flight`.
    OverMargin {
        /// The request row.
        row: DiscoveryRow,
    },
    /// A response of `row` once the node's exchange has its answer.
    Unsolicited {
        /// The response row.
        row: DiscoveryRow,
    },
    /// The held answer of `row`, one byte over its request's own cap.
    OverOwed {
        /// The response row.
        row: DiscoveryRow,
    },
    /// A frame that breaks `rule`.
    BadContent {
        /// The rule.
        rule: DiscoveryRule,
    },
    /// A `Hello` whose record expired: conformant, since expiry is local
    /// policy.
    ExpiredHello,
    /// The held `Peers`, its first record expired: conformant, since a
    /// record can expire in transit.
    ExpiredRelay,
    /// Up to [`BURST`] `Hello`s, written at once, stopping after the first
    /// that finds the cadence bucket empty: conformant, that one
    /// `exhausted`.
    HelloBurst,
    /// Requests of `row`, each once the node answered the last, stopping
    /// after the first that finds the cadence bucket empty, or after
    /// [`BURST`]: conformant, that one `exhausted`.
    RequestRun {
        /// The request row.
        row: DiscoveryRow,
    },
}

/// The most frames one burst or run sends: past every discovery row's
/// cadence capacity.
pub const BURST: usize = 64;

impl DiscoveryAct {
    /// The act's name in its identity.
    pub fn name(self) -> &'static str {
        match self {
            DiscoveryAct::UndeclaredType { .. } => "undeclared-type",
            DiscoveryAct::ReservedFlags { .. } => "reserved-flags",
            DiscoveryAct::ShortPayload { .. } => "short-payload",
            DiscoveryAct::LongPayload { .. } => "long-payload",
            DiscoveryAct::OverMargin { .. } => "over-margin",
            DiscoveryAct::Unsolicited { .. } => "unsolicited",
            DiscoveryAct::OverOwed { .. } => "over-owed",
            DiscoveryAct::BadContent { .. } => "bad-content",
            DiscoveryAct::ExpiredHello => "expired-hello",
            DiscoveryAct::ExpiredRelay => "expired-relay",
            DiscoveryAct::HelloBurst => "hello-burst",
            DiscoveryAct::RequestRun { .. } => "request-run",
        }
    }

    /// The label of the act's frame.
    pub fn label(self) -> Label {
        match self {
            DiscoveryAct::UndeclaredType { .. } => Label::UndeclaredType,
            DiscoveryAct::ReservedFlags { .. } => Label::ReservedFlags,
            DiscoveryAct::ShortPayload { .. } => Label::ShortPayload,
            DiscoveryAct::LongPayload { .. } => Label::LongPayload,
            DiscoveryAct::OverMargin { .. } => Label::OverMargin,
            DiscoveryAct::Unsolicited { .. } => Label::Unsolicited,
            DiscoveryAct::OverOwed { .. } => Label::OverOwed,
            DiscoveryAct::BadContent { .. } => Label::BadContent,
            DiscoveryAct::ExpiredHello
            | DiscoveryAct::ExpiredRelay
            | DiscoveryAct::HelloBurst
            | DiscoveryAct::RequestRun { .. } => Label::Conformant,
        }
    }

    /// The row of the act's frame.
    pub fn row(self) -> DiscoveryRow {
        match self {
            DiscoveryAct::ReservedFlags { row, .. }
            | DiscoveryAct::ShortPayload { row }
            | DiscoveryAct::LongPayload { row }
            | DiscoveryAct::OverMargin { row }
            | DiscoveryAct::Unsolicited { row }
            | DiscoveryAct::OverOwed { row }
            | DiscoveryAct::RequestRun { row } => row,
            DiscoveryAct::BadContent { rule } => rule.row(),
            DiscoveryAct::UndeclaredType { .. }
            | DiscoveryAct::ExpiredHello
            | DiscoveryAct::HelloBurst => DiscoveryRow::Hello,
            DiscoveryAct::ExpiredRelay => DiscoveryRow::Peers,
        }
    }

    /// The held answer the act changes, if it changes one: any act on a
    /// response row but an unsolicited one.
    pub fn takes(self) -> Option<Answer> {
        match self {
            DiscoveryAct::Unsolicited { .. } => None,
            act => act.row().answer(),
        }
    }
}

/// `record` with another body's signature: its own body with the next
/// sequence, signed by `secret`, the record's author.
fn forge(record: &mut ZakuraNodeRecord, secret: &NodeSecretKey) {
    let mut other = record.body.clone();
    other.sequence = other.sequence.wrapping_add(1);
    record.signature = ZakuraNodeRecord::sign(other, secret)
        .expect("a fixture record signs")
        .signature;
}

impl DiscoveryPeer {
    /// Whether `act` may run now: the peer's discovery stream started, and
    /// it holds the state the act needs.
    pub(crate) fn ready(&self, act: DiscoveryAct) -> bool {
        let Some(models) = self.models.as_ref() else {
            return false;
        };
        if let Some(answer) = act.takes() {
            let relays = |message: &DiscoveryMessage| matches!(message, DiscoveryMessage::Peers { records } if !records.is_empty());
            return match self.held(answer) {
                None => false,
                Some(message) => match act {
                    DiscoveryAct::BadContent {
                        rule: DiscoveryRule::PeersSignature,
                    }
                    | DiscoveryAct::ExpiredRelay => relays(message),
                    _ => true,
                },
            };
        }
        match act {
            DiscoveryAct::Unsolicited { row } => row.answer().is_some_and(|answer| {
                let request = row.rule().role;
                let MessageRole::Response { request, .. } = request else {
                    return false;
                };
                models.answered.contains(&answer)
                    && self.held(answer).is_none()
                    && models.node_asks[&request].open().next().is_none()
            }),
            DiscoveryAct::OverMargin { row } | DiscoveryAct::RequestRun { row } => {
                row.request().is_some_and(|request| {
                    models.run.is_none()
                        && models.peer_asks[&request.message_type()].serving(SESSION) == 0
                })
            }
            _ => true,
        }
    }

    /// Carries out `act`: checks it's ready, builds its frames from the
    /// models' state now, and labels each, failing unless the models give
    /// the act's frame the act's label.
    pub(crate) fn act(&mut self, act: DiscoveryAct) -> Result<Vec<Outgoing>, String> {
        if !self.ready(act) {
            return Err(format!("{}'s precondition doesn't hold", act.name()));
        }
        let row = act.row();
        let sent = match act {
            DiscoveryAct::UndeclaredType { message_type } => {
                if self
                    .stream_rules()
                    .iter()
                    .any(|rule| rule.message_type == message_type)
                {
                    return Err(format!("type {message_type} has a row"));
                }
                let mut frame = DiscoveryFrame::of(self.hello());
                frame.header.message_type = message_type;
                vec![self.send(frame, None)]
            }
            DiscoveryAct::ReservedFlags { bit, .. } => {
                let Some(flag) = 1u16.checked_shl(u32::from(bit)) else {
                    return Err(format!("flag bit {bit} is past 15"));
                };
                let (mut frame, exchange) = self.row_frame(row)?;
                frame.header.flags = flag;
                vec![self.send(frame, exchange)]
            }
            DiscoveryAct::ShortPayload { .. } => {
                let (mut frame, exchange) = self.row_frame(row)?;
                frame.resize(row.rule().payload.min() - 1);
                vec![self.send(frame, exchange)]
            }
            DiscoveryAct::LongPayload { .. } => {
                let (mut frame, exchange) = self.row_frame(row)?;
                frame.resize(row.rule().payload.max() + 1);
                vec![self.send(frame, exchange)]
            }
            DiscoveryAct::OverMargin { .. } => {
                let request = row.request().ok_or("over-margin needs a request row")?;
                let mut burst = Vec::new();
                while burst.len() < BURST {
                    let sent = self.ask(request);
                    let over = record_label(&sent) == Label::OverMargin;
                    burst.push(sent);
                    if over {
                        break;
                    }
                }
                vec![Outgoing::burst(burst)]
            }
            DiscoveryAct::Unsolicited { .. } => {
                // The node's exchange has its answer, so nothing is held: a
                // new one, a `Peers` relaying nothing or the peer's
                // `Services`.
                let message = match row.answer() {
                    Some(Answer::Peers) => DiscoveryMessage::Peers {
                        records: Vec::new(),
                    },
                    Some(Answer::Services) => {
                        DiscoveryMessage::Services(services(self.secret.public()))
                    }
                    _ => return Err("unsolicited needs a response row".to_owned()),
                };
                vec![self.send(DiscoveryFrame::of(message), None)]
            }
            DiscoveryAct::OverOwed { .. } => {
                let (mut frame, exchange) = self.row_frame(row)?;
                let limit = exchange
                    .and_then(|key| self.models.as_ref()?.limits.get(&key).copied())
                    .ok_or("over-owed needs a Peers for the node's GetPeers")?;
                let cap = usize::try_from(peers_cap(row.rule(), limit)).unwrap_or(usize::MAX);
                if cap >= row.rule().payload.max() {
                    return Err(format!(
                        "the node's GetPeers limit {limit} owes the row's largest Peers"
                    ));
                }
                frame.resize(cap + 1);
                vec![self.send(frame, exchange)]
            }
            DiscoveryAct::BadContent { rule } => {
                let (frame, exchange) = self.bad_frame(rule)?;
                vec![self.send(frame, exchange)]
            }
            DiscoveryAct::ExpiredHello => {
                let mut record = match self.hello() {
                    DiscoveryMessage::Hello { record } => record,
                    _ => unreachable!("the peer's own Hello"),
                };
                record = self.expired(record, &self.secret.clone());
                vec![self.send(DiscoveryFrame::of(DiscoveryMessage::Hello { record }), None)]
            }
            DiscoveryAct::ExpiredRelay => {
                let (message, exchange) = self.take_held(Answer::Peers).ok_or("no held Peers")?;
                let DiscoveryMessage::Peers { mut records } = message else {
                    unreachable!("a held Peers answer is a Peers");
                };
                let author = self
                    .relays
                    .iter()
                    .find(|author| author.public() == records[0].body.node_id)
                    .cloned()
                    .expect("the peer relays its own fixture authors' records");
                records[0] = self.expired(records[0].clone(), &author);
                let frame = DiscoveryFrame::of(DiscoveryMessage::Peers { records });
                vec![self.send(frame, exchange)]
            }
            DiscoveryAct::HelloBurst => {
                let mut burst = Vec::new();
                while burst.len() < BURST {
                    let sent = self.send(DiscoveryFrame::of(self.hello()), None);
                    let exhausted = sent.exhausted();
                    burst.push(sent);
                    if exhausted {
                        break;
                    }
                }
                vec![Outgoing::burst(burst)]
            }
            DiscoveryAct::RequestRun { .. } => {
                let request = row.request().ok_or("a request run needs a request row")?;
                let models = self.models.as_mut().expect("the act is ready");
                models.run = Some(request);
                models.run_sent = 0;
                self.run_step(request)
            }
        };
        let label = sent.last().map(record_label);
        if label != Some(act.label()) {
            return Err(format!(
                "{act:?}'s frame is labelled {label:?}, not {}",
                act.label()
            ));
        }
        Ok(sent)
    }

    /// The next request of a [`DiscoveryAct::RequestRun`] of `request`, or
    /// nothing once the run ended: its last request found the bucket empty,
    /// or it sent [`BURST`].
    pub(super) fn run_step(&mut self, request: Request) -> Vec<Outgoing> {
        let models = self
            .models
            .as_mut()
            .expect("a run runs on a started stream");
        if models.run != Some(request) || models.run_sent >= BURST {
            models.run = None;
            return Vec::new();
        }
        models.run_sent += 1;
        let sent = self.ask(request);
        if sent.exhausted() {
            self.models.as_mut().expect("still started").run = None;
        }
        vec![sent]
    }

    /// The rows of the peer's discovery stream.
    fn stream_rules(&self) -> &'static [MessageRule] {
        self.stream
            .messages
            .expect("discovery's stream has a table")
    }

    /// A conformant frame of `row` and the exchange it opens or answers:
    /// the peer's own `Hello`, a new request, or the held answer, taken.
    fn row_frame(&mut self, row: DiscoveryRow) -> Result<(DiscoveryFrame, Option<u64>), String> {
        if let Some(request) = row.request() {
            let key = self.models.as_mut().expect("the act is ready").key();
            return Ok((DiscoveryFrame::of(request.message()), Some(key)));
        }
        match row.answer() {
            None => Ok((DiscoveryFrame::of(self.hello()), None)),
            Some(answer) => {
                let (message, exchange) = self
                    .take_held(answer)
                    .ok_or_else(|| format!("no held {answer:?}"))?;
                Ok((DiscoveryFrame::of(message), exchange))
            }
        }
    }

    /// The frame that breaks `rule`, and the exchange it opens or answers.
    fn bad_frame(&mut self, rule: DiscoveryRule) -> Result<(DiscoveryFrame, Option<u64>), String> {
        let (mut frame, exchange) = self.row_frame(rule.row())?;
        match (rule, &mut frame.message) {
            (DiscoveryRule::HelloSignature, DiscoveryMessage::Hello { record }) => {
                forge(record, &self.secret);
                frame = DiscoveryFrame {
                    forged: vec![0],
                    ..DiscoveryFrame::of(frame.message)
                };
            }
            (DiscoveryRule::PeersSignature, DiscoveryMessage::Peers { records }) => {
                let author = self
                    .relays
                    .iter()
                    .find(|author| author.public() == records[0].body.node_id)
                    .expect("the peer relays its own fixture authors' records");
                forge(&mut records[0], author);
                frame = DiscoveryFrame {
                    forged: vec![0],
                    ..DiscoveryFrame::of(frame.message)
                };
            }
            (DiscoveryRule::ServicesNodeId, DiscoveryMessage::Services(services)) => {
                services.node_id = NodeSecretKey::from_bytes(&[0x3f; 32]).public();
                frame = DiscoveryFrame::of(frame.message);
            }
            (DiscoveryRule::GetPeersLimit, DiscoveryMessage::GetPeers(query)) => {
                // The limit is the payload's first field, which the encoder
                // refuses as 0.
                query.limit = 0;
                frame.payload[..2].copy_from_slice(&0u16.to_le_bytes());
            }
            (DiscoveryRule::GetServicesWanted, DiscoveryMessage::GetServices(query)) => {
                // The wanted list is a count, then each service ID, which
                // the encoder refuses to repeat.
                let wanted = ZakuraServiceId::discovery();
                query.wanted_services = vec![wanted.clone(), wanted.clone()];
                let once = DiscoveryFrame::of(DiscoveryMessage::GetServices(
                    zakura_network::zakura::GetServices {
                        wanted_services: vec![wanted],
                    },
                ));
                let entry = &once.payload[2..];
                let mut payload = 2u16.to_le_bytes().to_vec();
                payload.extend_from_slice(entry);
                payload.extend_from_slice(entry);
                frame.payload = payload;
                frame.header.payload_len = frame.payload.len();
            }
            (rule, message) => unreachable!("{rule:?}'s row gives {message:?}"),
        }
        Ok((frame, exchange))
    }

    /// `record` re-signed by `author` with an expiry already past.
    fn expired(&self, record: ZakuraNodeRecord, author: &NodeSecretKey) -> ZakuraNodeRecord {
        let mut expired = body(
            author,
            &self.handshake,
            unix_now(),
            record.body.direct_addrs,
        );
        expired.expires_at_unix_secs = unix_now().saturating_sub(1);
        ZakuraNodeRecord::sign(expired, author).expect("a fixture record signs")
    }
}

/// The label of the last frame `sent` records.
fn record_label(sent: &Outgoing) -> Label {
    match sent.records.last() {
        Some(crate::peer::RecordBody::Frame { label, .. }) => *label,
        records => unreachable!("an outgoing frame records a PeerFrame, not {records:?}"),
    }
}
