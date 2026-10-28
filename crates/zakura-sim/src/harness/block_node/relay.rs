//! Labelling relays: each peer's stream-6 role models, on its link to the
//! node under test.
//!
//! A relay task sits between two in-memory channels in each direction of a
//! link. It labels each frame as the peer at the link's far end would, with
//! P3a's role models for stream 6, records the label, and forwards the
//! frame unchanged. A peer's frame to the node becomes an
//! [`Observation::PeerFrame`], and a request that opens an exchange with
//! the node an [`Observation::ExchangeOwed`] too. The node's frame to the
//! peer becomes an [`Observation::NodeFrame`], labelled under the sender's
//! rules. The probe's link has relays too, so the probe's frames get the
//! same labels as a mirror's.
//!
//! Each record carries the number of block-sync events the node's tap had
//! recorded by then, so [`super::identity`] can place it among those
//! events when it writes the generated observations.
//!
//! The models label each frame once, in send order, and a frame labelled
//! with a violation changes no model state. Their content checks are the
//! family's wire checks: the stream-6 codec's, a body's coinbase height in
//! an open range that hasn't had it, its transactions matching its header,
//! an ending that agrees with the bodies before it, and a request that
//! overlaps no open one of its sender's. The verifier's verdicts aren't on
//! the wire, so no label reads them.
//!
//! Before the node's first status, the peer's models take the node's
//! request limit as 1, the lowest a status can advertise, and its byte
//! limit as the default status's. A scenario that floods requests before
//! the node's status arrives gets `OverMargin` labels the node may not
//! act on.

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
    time::Duration,
};

use zakura_chain::block::{self, merkle, MAX_BLOCK_BYTES};
use zakura_network::zakura::{
    block_sync_streams, BlockSyncMessage, BlockSyncStatus, CloseCauseReader, Frame, FramedRecv,
    FramedSend, MessageRole, MessageRule, MSG_BS_GET_BLOCKS,
};
use zakura_scheduler::{hooks, SimInstant};

use super::tap::TapClock;
use crate::{
    model::{CadenceModel, Charge, ExchangeModel, FrameHeader, FrameModel, Label, Owed, ReaderEnd},
    observe::{RowAt, StreamId},
    Observation,
};

/// Stream 6's message table.
fn rules() -> &'static [MessageRule] {
    block_sync_streams()[0]
        .messages
        .expect("the block-sync stream has a message table")
}

/// Stream 6's ID, as generated property IDs write it.
pub(crate) fn stream() -> StreamId {
    let stream = &block_sync_streams()[0];
    StreamId {
        kind: stream.kind,
        version: stream.version,
    }
}

/// Row `name` of stream 6.
pub(crate) fn row(name: &str) -> RowAt {
    RowAt::new(stream(), name)
}

/// The name of stream 6's row for `message_type`.
///
/// # Panics
///
/// If no row has that type, which a node's own output always has.
pub(crate) fn row_name(message_type: u16) -> &'static str {
    MessageRule::find(rules(), message_type)
        .unwrap_or_else(|| panic!("stream 6 declares message type {message_type}"))
        .name
}

/// The direction a frame crosses a link in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Way {
    /// The peer's frames to the node.
    ToNode,
    /// The node's frames to the peer.
    FromNode,
}

impl Way {
    /// The other direction.
    fn back(self) -> Way {
        match self {
            Way::ToNode => Way::FromNode,
            Way::FromNode => Way::ToNode,
        }
    }
}

/// A range one side asked the other for: how many heights, and the
/// heights delivered so far.
#[derive(Clone, Debug, Default)]
struct Open {
    count: u32,
    delivered: BTreeSet<u32>,
}

/// What a label adds besides the frame's own record.
#[derive(Default)]
struct Extra {
    /// The exchange the frame belongs to.
    exchange: Option<u64>,
    /// The cadence bucket was empty.
    exhausted: bool,
    /// The exchange the frame opened, with what it owes.
    owed: Option<(u64, Owed)>,
    /// The height of a body the frame delivered.
    body: Option<u32>,
}

/// One connection's role models, as the peer on it runs them. The session
/// is the connection.
struct Labeller {
    connection: u64,
    frames: FrameModel,
    /// The node's requests to the peer, which the peer answers.
    node_requests: ExchangeModel,
    /// The peer's requests to the node, which the node answers.
    peer_requests: ExchangeModel,
    /// Each open range, by the direction of its request and its start.
    ranges: BTreeMap<(Way, u32), Open>,
    /// The node's bucket for the peer's frames.
    peer_cadence: CadenceModel,
    /// The peer's bucket for the node's frames.
    node_cadence: CadenceModel,
    /// The highest request limit the peer advertised, if it has.
    peer_limit: Option<u32>,
    /// The byte limit the peer last advertised.
    peer_bytes: u32,
    /// The byte limit the node last advertised.
    node_bytes: u32,
    /// The bodies per range the peer last advertised.
    peer_blocks: u32,
    /// The bodies per range the node last advertised.
    node_blocks: u32,
    /// The next frame's number in each direction.
    next: [u64; 2],
}

impl Labeller {
    /// The models of a new connection.
    fn new(connection: u64) -> Self {
        let mut peer_requests = ExchangeModel::new(rules(), u16::from(MSG_BS_GET_BLOCKS))
            .expect("get-blocks is a request row");
        peer_requests.advertise(connection, 1);
        let default = BlockSyncStatus::default();
        Labeller {
            connection,
            frames: FrameModel::new(rules(), ReaderEnd::Persistent),
            node_requests: peer_requests.clone(),
            peer_requests,
            ranges: BTreeMap::new(),
            peer_cadence: CadenceModel::new(),
            node_cadence: CadenceModel::new(),
            peer_limit: None,
            peer_bytes: default.max_response_bytes,
            node_bytes: default.max_response_bytes,
            peer_blocks: default.max_blocks_per_response,
            node_blocks: default.max_blocks_per_response,
            next: [0; 2],
        }
    }

    /// The exchanges `way`'s requests opened, and their model.
    fn requests(&mut self, way: Way) -> &mut ExchangeModel {
        match way {
            Way::ToNode => &mut self.peer_requests,
            Way::FromNode => &mut self.node_requests,
        }
    }

    /// Labels `frame`, sent `way` at `now`, and returns its record, the
    /// exchange it opened, and the body it delivered.
    fn label(&mut self, way: Way, frame: &Frame, now: Duration) -> (Observation, Extra) {
        let seq = self.next[way as usize];
        self.next[way as usize] += 1;
        let header = FrameHeader {
            message_type: frame.message_type,
            flags: frame.flags,
            payload_len: frame.payload.len(),
        };
        let row = self.frames.row(header.message_type);
        let mut extra = Extra::default();
        let envelope = self.frames.labels(&header);
        let label = match row {
            Some(row) if envelope.is_conformant() => {
                let label = self.content(way, row, frame, &header, &mut extra);
                if label == Label::Conformant {
                    let cadence = match way {
                        Way::ToNode => &mut self.peer_cadence,
                        Way::FromNode => &mut self.node_cadence,
                    };
                    extra.exhausted = cadence.charge(self.connection, stream().kind, row, now)
                        == Charge::Exhausted;
                }
                label
            }
            _ => envelope.first(),
        };
        let row = row.map(|row| row.name.to_owned());
        let obs = match way {
            Way::ToNode => Observation::PeerFrame {
                connection: self.connection,
                stream: stream(),
                seq,
                row,
                rule_stream: stream(),
                label,
                exhausted: extra.exhausted,
                exchange: extra.exchange,
                payload: u64::try_from(frame.payload.len()).expect("a frame fits in 64 bits"),
            },
            Way::FromNode => Observation::NodeFrame {
                connection: self.connection,
                stream: stream(),
                seq,
                row,
                rule_stream: stream(),
                label,
                exhausted: extra.exhausted,
            },
        };
        (obs, extra)
    }

    /// The content label of `frame`, whose envelope `row` admits, sent
    /// `way`. Commits a conformant frame to the models.
    fn content(
        &mut self,
        way: Way,
        row: &MessageRule,
        frame: &Frame,
        header: &FrameHeader,
        extra: &mut Extra,
    ) -> Label {
        let message = match BlockSyncMessage::decode_frame(frame.clone()) {
            Ok(message) => message,
            Err(_) => return self.undecodable(way, row, frame, extra),
        };
        match message {
            BlockSyncMessage::Status(status) => {
                self.status(way, &status);
                Label::Conformant
            }
            BlockSyncMessage::GetBlocks {
                start_height,
                count,
            } => self.request(way, start_height.0, count, extra),
            BlockSyncMessage::Block(body) => self.body(way, &body, header, extra),
            BlockSyncMessage::BlocksDone {
                start_height,
                returned,
            } => self.ending(way, start_height.0, header, extra, |open| {
                returned >= 1 && usize::try_from(returned).ok() == Some(open.delivered.len())
            }),
            BlockSyncMessage::RangeUnavailable { start_height, .. } => {
                self.ending(way, start_height.0, header, extra, |open| {
                    open.delivered.is_empty()
                })
            }
        }
    }

    /// The label of a frame the codec refuses: `Unsolicited` for a response
    /// that answers no open range, else `BadContent`.
    fn undecodable(&self, way: Way, row: &MessageRule, frame: &Frame, extra: &mut Extra) -> Label {
        let MessageRole::Response { ends_exchange, .. } = row.role else {
            return Label::BadContent;
        };
        let asked = way.back();
        let answers = if ends_exchange {
            // An ending's start follows its tag byte.
            let start = frame
                .payload
                .get(1..5)
                .and_then(|bytes| bytes.try_into().ok())
                .map(u32::from_le_bytes);
            start.filter(|start| self.ranges.contains_key(&(asked, *start)))
        } else {
            self.ranges
                .keys()
                .find(|(by, _)| *by == asked)
                .map(|(_, start)| *start)
        };
        match answers {
            Some(start) => {
                extra.exchange = Some(u64::from(start));
                Label::BadContent
            }
            None => Label::Unsolicited,
        }
    }

    /// Takes `way`'s side's advertised limits from `status`.
    fn status(&mut self, way: Way, status: &BlockSyncStatus) {
        match way {
            Way::ToNode => {
                let limit = status
                    .max_inflight_requests
                    .clamp(1, max_in_flight(rules()));
                self.peer_limit = Some(self.peer_limit.unwrap_or(0).max(limit));
                self.peer_bytes = status.max_response_bytes;
                self.peer_blocks = status.max_blocks_per_response;
            }
            Way::FromNode => {
                self.peer_requests
                    .advertise(self.connection, status.max_inflight_requests);
                self.node_bytes = status.max_response_bytes;
                self.node_blocks = status.max_blocks_per_response;
            }
        }
    }

    /// Labels a request for `count` heights from `start`, sent `way`, and
    /// opens its exchange if it's conformant. The responder serves at most
    /// the bodies per range it last advertised, so the range it commits to
    /// is that prefix, and it refuses a range that overlaps one of the
    /// sender's open ones.
    fn request(&mut self, way: Way, start: u32, count: u32, extra: &mut Extra) -> Label {
        extra.exchange = Some(u64::from(start));
        let served = count.min(match way {
            Way::ToNode => self.node_blocks,
            Way::FromNode => self.peer_blocks,
        });
        let last = start.saturating_add(served - 1);
        let overlaps = self.ranges.iter().any(|((by, first), open)| {
            *by == way && *first <= last && start <= first.saturating_add(open.count - 1)
        });
        if overlaps {
            return Label::BadContent;
        }
        let connection = self.connection;
        let limit_bytes = match way {
            Way::ToNode => self.node_bytes,
            Way::FromNode => self.peer_bytes,
        };
        let owed = Owed {
            frames: count,
            // Bodies count toward the byte limit; their tags and the ending
            // don't.
            bytes: u64::from(limit_bytes).min(u64::from(count) * MAX_BLOCK_BYTES)
                + u64::from(count)
                + 9,
        };
        match way {
            Way::ToNode => {
                let mut next = self.peer_requests.clone();
                let labels = next.request_frame(connection);
                if !labels.is_conformant() {
                    return labels.first();
                }
                next.sent(connection, u64::from(start), owed);
                extra.owed = next
                    .owed(connection, u64::from(start))
                    .map(|owed| (u64::from(start), owed));
                self.peer_requests = next;
            }
            Way::FromNode => {
                // The sender's rule: no margin over the peer's limit.
                let open = self
                    .node_requests
                    .open()
                    .filter(|(session, _, _)| *session == connection)
                    .count();
                let limit = self.peer_limit.unwrap_or(0);
                if u32::try_from(open).map_or(true, |open| open >= limit) {
                    return Label::OverMargin;
                }
                self.node_requests.sent(connection, u64::from(start), owed);
            }
        }
        self.ranges.insert(
            (way, start),
            Open {
                count: served,
                delivered: BTreeSet::new(),
            },
        );
        Label::Conformant
    }

    /// Labels a body sent `way`: it answers the open range of the other
    /// side's that holds its coinbase height and hasn't had it, and its
    /// transactions match its header.
    fn body(
        &mut self,
        way: Way,
        body: &block::Block,
        header: &FrameHeader,
        extra: &mut Extra,
    ) -> Label {
        let asked = way.back();
        let Some(height) = body.coinbase_height().map(|height| height.0) else {
            return self.undecodable_body(asked, extra);
        };
        let start = self.ranges.iter().find_map(|((by, start), open)| {
            let holds = *by == asked
                && *start <= height
                && height - start < open.count
                && !open.delivered.contains(&height);
            holds.then_some(*start)
        });
        let Some(start) = start else {
            return Label::Unsolicited;
        };
        extra.exchange = Some(u64::from(start));
        let connection = self.connection;
        let mut next = self.requests(asked).clone();
        let labels = next.response_frame(connection, u64::from(start), header);
        if !labels.is_conformant() {
            return labels.first();
        }
        if !transactions_match(body) {
            return Label::BadContent;
        }
        *self.requests(asked) = next;
        self.ranges
            .get_mut(&(asked, start))
            .expect("found above")
            .delivered
            .insert(height);
        extra.body = Some(height);
        Label::Conformant
    }

    /// The label of a body with no coinbase height, answering `asked`'s
    /// ranges.
    fn undecodable_body(&self, asked: Way, extra: &mut Extra) -> Label {
        match self.ranges.keys().find(|(by, _)| *by == asked) {
            Some((_, start)) => {
                extra.exchange = Some(u64::from(*start));
                Label::BadContent
            }
            None => Label::Unsolicited,
        }
    }

    /// Labels an ending for the range from `start`, sent `way`, whose
    /// content `agrees` checks against the bodies before it, and closes the
    /// range if it's conformant.
    fn ending(
        &mut self,
        way: Way,
        start: u32,
        header: &FrameHeader,
        extra: &mut Extra,
        agrees: impl Fn(&Open) -> bool,
    ) -> Label {
        let asked = way.back();
        let Some(open) = self.ranges.get(&(asked, start)) else {
            return Label::Unsolicited;
        };
        extra.exchange = Some(u64::from(start));
        let agrees = agrees(open);
        let connection = self.connection;
        let mut next = self.requests(asked).clone();
        let labels = next.response_frame(connection, u64::from(start), header);
        if !labels.is_conformant() {
            return labels.first();
        }
        if !agrees {
            return Label::BadContent;
        }
        if asked == Way::ToNode {
            next.answered(connection);
        }
        *self.requests(asked) = next;
        self.ranges.remove(&(asked, start));
        Label::Conformant
    }

    /// The connection closed: every exchange on it ends.
    fn end(&mut self) {
        self.node_requests.end_session(self.connection);
        self.peer_requests.end_session(self.connection);
        self.ranges.clear();
    }
}

/// The request row's `max_in_flight` in `rules`.
fn max_in_flight(rules: &[MessageRule]) -> u32 {
    rules
        .iter()
        .find_map(|row| match row.role {
            MessageRole::Request { max_in_flight, .. } => Some(max_in_flight),
            _ => None,
        })
        .expect("stream 6 has a request row")
}

/// True if `body`'s transactions hash to its header's merkle root, with no
/// transaction repeated.
fn transactions_match(body: &block::Block) -> bool {
    let hashes: Vec<_> = body.transactions.iter().map(|tx| tx.hash()).collect();
    let root: merkle::Root = hashes.iter().copied().collect();
    let distinct: BTreeSet<_> = hashes.iter().collect();
    root == body.header.merkle_root && distinct.len() == hashes.len()
}

/// A body a peer delivered: its frame, and the exchange it answered, if
/// any.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Delivered {
    /// The frame, as its `PeerFrame` numbers it.
    pub(crate) seq: u64,
    /// The exchange its label names, `None` for an unsolicited body.
    pub(crate) exchange: Option<u64>,
}

/// What the relays share with the node's tap.
#[derive(Default)]
struct WireState {
    /// The node's tap's event count, once the tap is attached.
    clock: Option<TapClock>,
    labellers: BTreeMap<u64, Labeller>,
    /// Records not yet written, each with the tap events recorded before
    /// it.
    records: Vec<(u64, Observation)>,
    /// The newest body frame for each `(connection, height)`.
    bodies: BTreeMap<(u64, u32), Delivered>,
    /// Each link's close cause, and whether its close was taken.
    causes: Vec<(u64, CloseCauseReader, bool)>,
}

/// The relays of one run's links. Cloning shares them.
#[derive(Clone, Default)]
pub struct Wire(Rc<RefCell<WireState>>);

impl Wire {
    /// Counts the tap's events from now on, for each record's place.
    pub(crate) fn set_clock(&self, clock: TapClock) {
        self.0.borrow_mut().clock = Some(clock);
    }

    /// A channel for frames sent `way` on `connection`, through a relay:
    /// the sender's end and the receiver's. Each side's end holds `depth`
    /// frames, as a plain link's does, since a sender's `try_send` fails on
    /// a full channel.
    pub(crate) fn pipe(&self, connection: u64, way: Way, depth: usize) -> (FramedSend, FramedRecv) {
        let (to_relay, from_sender) = zakura_network::zakura::framed_channel(depth);
        let (to_receiver, from_relay) = zakura_network::zakura::framed_channel(depth);
        self.0
            .borrow_mut()
            .labellers
            .entry(connection)
            .or_insert_with(|| Labeller::new(connection));
        let wire = self.clone();
        hooks::spawn(
            "block-relay",
            forward(wire, connection, way, from_sender, to_receiver),
        );
        (to_relay, from_relay)
    }

    /// Reports `connection`'s close once `cause` has one.
    pub(crate) fn watch(&self, connection: u64, cause: CloseCauseReader) {
        self.0.borrow_mut().causes.push((connection, cause, false));
    }

    /// Labels `frame`, sent `way` on `connection`, and records the label.
    fn record(&self, connection: u64, way: Way, frame: &Frame) {
        let now = hooks::now().map_or(Duration::ZERO, SimInstant::since_start);
        let mut state = self.0.borrow_mut();
        let after = state.clock.as_ref().map_or(0, TapClock::events);
        let labeller = state
            .labellers
            .get_mut(&connection)
            .expect("a relay's connection has models");
        let (obs, extra) = labeller.label(way, frame, now);
        let seq = match &obs {
            Observation::PeerFrame { seq, .. } => Some(*seq),
            _ => None,
        };
        state.records.push((after, obs));
        if let Some((exchange, owed)) = extra.owed {
            state.records.push((
                after,
                Observation::ExchangeOwed {
                    connection,
                    request: row(row_name(u16::from(MSG_BS_GET_BLOCKS))),
                    exchange,
                    frames: owed.frames,
                    bytes: owed.bytes,
                },
            ));
        }
        // Every body frame the peer sends, conformant or not, so work from a
        // body the models refused still names its frame.
        if let (Some(seq), Some(height)) = (seq, delivered_height(frame, &extra)) {
            state.bodies.insert(
                (connection, height),
                Delivered {
                    seq,
                    exchange: extra.exchange.filter(|_| extra.body.is_some()),
                },
            );
        }
    }

    /// Takes the records not yet written, in order.
    pub(crate) fn take(&self) -> Vec<(u64, Observation)> {
        std::mem::take(&mut self.0.borrow_mut().records)
    }

    /// The newest body frame at `height` on `connection`.
    pub(crate) fn body(&self, connection: u64, height: u32) -> Option<Delivered> {
        self.0.borrow().bodies.get(&(connection, height)).copied()
    }

    /// Each connection that closed since the last call, with its cause.
    /// Its exchanges end.
    pub(crate) fn closes(&self) -> Vec<(u64, &'static str)> {
        let mut state = self.0.borrow_mut();
        let state = &mut *state;
        let mut closed = Vec::new();
        for (connection, cause, taken) in &mut state.causes {
            if let (false, Some(cause)) = (*taken, cause.get()) {
                *taken = true;
                closed.push((*connection, cause));
                if let Some(labeller) = state.labellers.get_mut(connection) {
                    labeller.end();
                }
            }
        }
        closed
    }
}

/// The height a body frame carries, if it's one and decodes.
fn delivered_height(frame: &Frame, extra: &Extra) -> Option<u32> {
    if let Some(height) = extra.body {
        return Some(height);
    }
    match BlockSyncMessage::decode_frame(frame.clone()) {
        Ok(BlockSyncMessage::Block(body)) => body.coinbase_height().map(|height| height.0),
        _ => None,
    }
}

/// Labels and forwards every frame from `from` to `to`, sent `way` on
/// `connection`, until either end goes away.
async fn forward(wire: Wire, connection: u64, way: Way, mut from: FramedRecv, to: FramedSend) {
    while let Some(frame) = from.recv().await {
        wire.record(connection, way, &frame);
        if to.send(frame).await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The generated IDs name stream 6 at the regulated version.
    #[test]
    fn stream_six_is_the_regulated_block_sync_stream() {
        assert_eq!(stream().to_string(), "s6v3");
        assert_eq!(row_name(u16::from(MSG_BS_GET_BLOCKS)), "get-blocks");
    }

    /// A request beyond the peer's advertised limit is `OverMargin` under
    /// the sender's rule, with no margin, and opens nothing.
    #[test]
    fn the_node_gets_no_margin_over_the_peers_limit() {
        let mut labeller = Labeller::new(1);
        labeller.status(
            Way::ToNode,
            &BlockSyncStatus {
                max_inflight_requests: 1,
                ..BlockSyncStatus::default()
            },
        );
        let mut extra = Extra::default();
        assert_eq!(
            labeller.request(Way::FromNode, 1, 1, &mut extra),
            Label::Conformant
        );
        assert_eq!(
            labeller.request(Way::FromNode, 2, 1, &mut extra),
            Label::OverMargin
        );
        assert_eq!(labeller.ranges.len(), 1);
    }

    /// A peer's request that overlaps one of its open ranges, as the node
    /// truncates them to its advertised bodies per range, is `BadContent`,
    /// and one beyond twice the node's limit `OverMargin`.
    #[test]
    fn peer_requests_check_overlap_and_the_margin() {
        let mut labeller = Labeller::new(1);
        let mut extra = Extra::default();
        assert_eq!(
            labeller.request(Way::ToNode, 1, 4, &mut extra),
            Label::Conformant
        );
        assert_eq!(
            labeller.request(Way::ToNode, 1, 1, &mut extra),
            Label::BadContent
        );
        // The node serves one body per range, so this starts past the first.
        assert_eq!(
            labeller.request(Way::ToNode, 2, 1, &mut extra),
            Label::Conformant
        );
        assert_eq!(
            labeller.request(Way::ToNode, 3, 1, &mut extra),
            Label::OverMargin
        );
    }
}
