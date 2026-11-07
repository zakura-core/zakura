//! Labelling relays: each peer's stream-6 role models, on its link to the
//! node under test.
//!
//! A relay task sits between two in-memory channels in each direction of a
//! link. It labels each frame as the peer at the link's far end would, with
//! P3a's role models for stream 6, records the label, and forwards the
//! frame unchanged. A peer's frame to the node becomes an
//! [`Observation::PeerFrame`], a request that opens an exchange with the
//! node an [`Observation::ExchangeOwed`] too, and a body an
//! [`Observation::PeerBody`] with its height and hash. The node's frame to the
//! peer becomes an [`Observation::NodeFrame`], labelled under the sender's
//! rules, and a response the node writes an [`Observation::NodeOutput`]
//! with the frame's own payload length. The probe's link has relays too, so
//! the probe's frames get the same labels as a mirror's.
//!
//! The relay into the node stands in for a QUIC stream's reader. It reads
//! no frame until the node's service attaches its response precheck, checks
//! each header against the stream's table and that precheck before it
//! forwards the frame, and holds the precheck until the connection closes.
//! A refused header closes the connection with `ordered_read_rejected`, as
//! the QUIC reader does, and the relay records that the node judged the
//! frame. Once a connection closes, its relays forward
//! nothing more.
//!
//! Each record carries the number of block-sync events the node's tap had
//! recorded by then, so [`super::identity`] can place it among those
//! events when it writes the generated observations.
//!
//! The models label each frame once, in send order, and a frame labelled
//! with a violation changes no model state. Their content checks are the
//! family's wire checks: the stream-6 codec's, a body that is the next one
//! an open range expects with its transactions matching its header, an
//! ending that agrees with the bodies before it, and a request that
//! overlaps no live range of its sender's. A body no open range expects
//! next is the receiver's local incompatibility, not a fault
//! (peer-message-regulation.md, `Block`), so it's `Conformant` and changes
//! no range. The verifier's verdicts aren't on the wire, so no label reads
//! them.
//!
//! The node drops a peer's range when it publishes the range's ending,
//! before the ending crosses the relay. So a peer's request that overlaps a
//! range the relay still holds, or that the ranges it holds put over the
//! node's margin, waits for the node's timing: the relay records it with a
//! [`Settle`], and the adapter labels it against the ranges the node still
//! served when it judged the request. The relay opens such a request's
//! exchange anyway, after the endings of the ranges it overlaps, and the
//! adapter drops the exchange if the request settles as a fault.
//!
//! The node commits to its request limit when it opens a connection, so the
//! peer's models take that limit from the start. Before the peer's first
//! status, the node's models take the peer's request limit as none.

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
    time::Duration,
};

use zakura_chain::block::{self, merkle, MAX_BLOCK_BYTES};
use zakura_network::zakura::{
    block_sync_streams, framed_channel, read_framed_channel, BlockSyncMessage, BlockSyncStatus,
    CloseCauseReader, Frame, FramedRecv, FramedSend, MessageRole, MessageRule, StreamReader,
    MSG_BS_BLOCK, MSG_BS_GET_BLOCKS,
};
use zakura_scheduler::{hooks, SimInstant};

use super::tap::TapClock;
use crate::{
    model::{CadenceModel, Charge, ExchangeModel, FrameHeader, FrameModel, Label, Owed, ReaderEnd},
    observe::{RowAt, StreamId},
    storage::hex,
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

/// True if a request with `live` others open passes twice `limit`.
fn over_margin(live: usize, limit: u32) -> bool {
    u64::try_from(live).unwrap_or(u64::MAX).saturating_add(1) > 2 * u64::from(limit)
}

/// A range one side asked the other for.
#[derive(Clone, Debug)]
struct Range {
    /// The direction its request crossed.
    way: Way,
    start: u32,
    /// The heights asked for.
    count: u32,
    /// The heights the responder serves: the count, truncated to the
    /// bodies per range it last advertised.
    served: u32,
    /// The body bytes the responder may send for it.
    max_body_bytes: u32,
    /// The bodies delivered so far, in height order.
    delivered: u32,
    body_bytes: u64,
    /// What its exchange owes once it opens.
    owed: Owed,
    /// Live ranges it overlaps. It opens once each one's ending crosses.
    waits_on: BTreeSet<u64>,
    /// Its request frame's number.
    request: u64,
}

impl Range {
    /// True if it shares a height with `count` heights from `start`.
    fn overlaps(&self, start: u32, count: u32) -> bool {
        let last = |start: u32, count: u32| start.saturating_add(count.saturating_sub(1));
        self.start <= last(start, count) && start <= last(self.start, self.count)
    }

    /// The height of the next body it expects, if it expects one.
    fn next(&self) -> Option<u32> {
        (self.delivered < self.served).then(|| self.start.saturating_add(self.delivered))
    }

    /// True once its exchange is open.
    fn is_open(&self) -> bool {
        self.waits_on.is_empty()
    }
}

/// How the adapter labels a peer's request that waits for the node's
/// timing: the request's heights and the node's request limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Settle {
    /// The request's first height.
    pub(crate) start: u32,
    /// The heights it asks for.
    pub(crate) count: u32,
    /// The node's request limit.
    pub(crate) limit: u32,
}

impl Settle {
    /// The request's label against the `live` ranges the node still served
    /// when it judged the request, each as its first and last height:
    /// `BadContent` if it overlaps one, else `OverMargin` if they put it
    /// past twice the limit, else `Conformant`.
    pub(crate) fn label(&self, live: &[(u32, u32)]) -> Label {
        let last = self.start.saturating_add(self.count.saturating_sub(1));
        if live
            .iter()
            .any(|&(first, end)| first <= last && self.start <= end)
        {
            Label::BadContent
        } else if over_margin(live.len(), self.limit) {
            Label::OverMargin
        } else {
            Label::Conformant
        }
    }
}

/// What a label adds besides the frame's own record.
#[derive(Default)]
struct Extra {
    /// The exchange the frame belongs to.
    exchange: Option<u64>,
    /// The cadence bucket was empty.
    exhausted: bool,
    /// How to settle the frame's label, if it waits for the node.
    pending: Option<Settle>,
    /// Each exchange the frame opened: its request frame's number, its key,
    /// and what it owes.
    opened: Vec<(u64, u64, Owed)>,
    /// The height of a body the frame delivered to its range.
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
    /// Each live range, by ID.
    ranges: BTreeMap<u64, Range>,
    /// The next range ID.
    next_range: u64,
    /// The node's bucket for the peer's frames.
    peer_cadence: CadenceModel,
    /// The peer's bucket for the node's frames.
    node_cadence: CadenceModel,
    /// The highest request limit the peer advertised, if it has.
    peer_limit: Option<u32>,
    /// The highest request limit the node advertised.
    node_limit: u32,
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
    /// The models of a new connection to a node that enforces
    /// `node_limit`.
    fn new(connection: u64, node_limit: u32) -> Self {
        let requests = ExchangeModel::new(rules(), u16::from(MSG_BS_GET_BLOCKS))
            .expect("get-blocks is a request row");
        let default = BlockSyncStatus::default();
        Labeller {
            connection,
            frames: FrameModel::new(rules(), ReaderEnd::Persistent),
            node_requests: requests.clone(),
            peer_requests: requests,
            ranges: BTreeMap::new(),
            next_range: 0,
            peer_cadence: CadenceModel::new(),
            node_cadence: CadenceModel::new(),
            peer_limit: None,
            node_limit: node_limit.clamp(1, max_in_flight(rules())),
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

    /// The IDs and ranges of `way`'s requests whose exchanges are open.
    fn open(&self, way: Way) -> impl Iterator<Item = (u64, &Range)> {
        self.ranges
            .iter()
            .filter(move |(_, range)| range.way == way && range.is_open())
            .map(|(id, range)| (*id, range))
    }

    /// The start of `way`'s open range that holds `height`, if one does.
    fn containing(&self, way: Way, height: u32) -> Option<u32> {
        self.open(way)
            .find(|(_, range)| range.overlaps(height, 1))
            .map(|(_, range)| range.start)
    }

    /// Labels `frame`, sent `way` at `now`, and returns its record and what
    /// else it changed.
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
                let label = self.content(way, seq, row, frame, &header, &mut extra);
                if label == Label::Conformant || extra.pending.is_some() {
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
                quic_stream: None,
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
                quic_stream: None,
                seq,
                row,
                rule_stream: stream(),
                label,
                exhausted: extra.exhausted,
            },
        };
        (obs, extra)
    }

    /// The content label of frame `seq`, whose envelope `row` admits, sent
    /// `way`. Commits a conformant frame to the models.
    fn content(
        &mut self,
        way: Way,
        seq: u64,
        row: &MessageRule,
        frame: &Frame,
        header: &FrameHeader,
        extra: &mut Extra,
    ) -> Label {
        if header.message_type == u16::from(MSG_BS_BLOCK) {
            if let Some(label) = self.refused_body(way, header, extra) {
                return label;
            }
        }
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
            } => self.request(way, seq, start_height.0, count, extra),
            BlockSyncMessage::Block(body) => self.body(way, &body, header, extra),
            BlockSyncMessage::BlocksDone {
                start_height,
                returned,
            } => self.ending(way, start_height.0, header, extra, |range| {
                returned >= 1 && returned == range.delivered
            }),
            BlockSyncMessage::RangeUnavailable {
                start_height,
                count,
            } => self.ending(way, start_height.0, header, extra, |range| {
                range.delivered == 0 && (1..=range.count).contains(&count)
            }),
        }
    }

    /// The label of a body the reader refuses from its header alone:
    /// `Unsolicited` with no open range of the asking side's, and
    /// `OverOwed` if it's longer than every open exchange still owes.
    fn refused_body(&self, way: Way, header: &FrameHeader, extra: &mut Extra) -> Option<Label> {
        let asked = way.back();
        let model = match asked {
            Way::ToNode => &self.peer_requests,
            Way::FromNode => &self.node_requests,
        };
        let bytes = u64::try_from(header.payload_len).unwrap_or(u64::MAX);
        let mut open = self.open(asked).peekable();
        let Some(first) = open.peek().map(|(_, range)| range.start) else {
            return Some(Label::Unsolicited);
        };
        let fits = open.any(|(_, range)| {
            model
                .owed(self.connection, u64::from(range.start))
                .is_some_and(|owed| bytes <= owed.bytes)
        });
        if fits {
            return None;
        }
        extra.exchange = Some(u64::from(first));
        Some(Label::OverOwed)
    }

    /// The label of a frame the codec refuses: `Unsolicited` for an ending
    /// that answers no open range, else `BadContent`. The reader already
    /// refused a body that answers none.
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
            start.filter(|start| self.open(asked).any(|(_, range)| range.start == *start))
        } else {
            self.open(asked).next().map(|(_, range)| range.start)
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
        let limit = status
            .max_inflight_requests
            .clamp(1, max_in_flight(rules()));
        match way {
            Way::ToNode => {
                self.peer_limit = Some(self.peer_limit.unwrap_or(0).max(limit));
                self.peer_bytes = status.max_response_bytes;
                self.peer_blocks = status.max_blocks_per_response;
            }
            Way::FromNode => {
                self.node_limit = self.node_limit.max(limit);
                self.node_bytes = status.max_response_bytes;
                self.node_blocks = status.max_blocks_per_response;
            }
        }
    }

    /// Labels request `seq` for `count` heights from `start`, sent `way`,
    /// and opens its range.
    ///
    /// The node's request must overlap none of its live ranges and stay
    /// within the peer's limit, with no margin. A peer's request that
    /// overlaps a live range, or that the live ranges put past twice the
    /// node's limit, waits for the node's timing: its range opens after the
    /// ranges it overlaps, and the adapter settles its label.
    fn request(&mut self, way: Way, seq: u64, start: u32, count: u32, extra: &mut Extra) -> Label {
        extra.exchange = Some(u64::from(start));
        let overlaps: BTreeSet<u64> = self
            .ranges
            .iter()
            .filter(|(_, range)| range.way == way && range.overlaps(start, count))
            .map(|(id, _)| *id)
            .collect();
        let (max_body_bytes, blocks) = match way {
            Way::ToNode => (self.node_bytes, self.node_blocks),
            Way::FromNode => (self.peer_bytes, self.peer_blocks),
        };
        let mut range = Range {
            way,
            start,
            count,
            served: count.min(blocks),
            max_body_bytes,
            delivered: 0,
            body_bytes: 0,
            owed: Owed {
                frames: count,
                // Bodies count toward the byte limit; their tags and the
                // ending don't.
                bytes: u64::from(max_body_bytes).min(u64::from(count) * MAX_BLOCK_BYTES)
                    + u64::from(count)
                    + 9,
            },
            waits_on: BTreeSet::new(),
            request: seq,
        };
        if way == Way::FromNode {
            if !overlaps.is_empty() {
                return Label::BadContent;
            }
            let open = self.ranges.values().filter(|r| r.way == way).count();
            if !self
                .peer_limit
                .is_some_and(|limit| u64::try_from(open).is_ok_and(|open| open < u64::from(limit)))
            {
                return Label::OverMargin;
            }
            let id = self.insert(range);
            self.activate(id, extra);
            return Label::Conformant;
        }
        let live = self
            .ranges
            .values()
            .filter(|range| range.way == way)
            .count();
        let label = if !overlaps.is_empty() {
            Label::BadContent
        } else if over_margin(live, self.node_limit) {
            Label::OverMargin
        } else {
            Label::Conformant
        };
        if label != Label::Conformant {
            extra.pending = Some(Settle {
                start,
                count,
                limit: self.node_limit,
            });
        }
        range.waits_on = overlaps;
        let id = self.insert(range);
        self.activate(id, extra);
        label
    }

    /// Adds `range` and returns its ID.
    fn insert(&mut self, range: Range) -> u64 {
        let id = self.next_range;
        self.next_range += 1;
        self.ranges.insert(id, range);
        id
    }

    /// Opens range `id`'s exchange if it waits on no other range.
    fn activate(&mut self, id: u64, extra: &mut Extra) {
        let connection = self.connection;
        let range = &self.ranges[&id];
        if !range.is_open() {
            return;
        }
        let (way, key, request, owed) =
            (range.way, u64::from(range.start), range.request, range.owed);
        let model = self.requests(way);
        model.sent(connection, key, owed);
        if way == Way::ToNode {
            let owed = model
                .owed(connection, key)
                .expect("the exchange just opened");
            extra.opened.push((request, key, owed));
        }
    }

    /// Labels a body sent `way`: it's the next body an open range of the
    /// other side's expects, within the range's body bytes and what its
    /// exchange owes, and its transactions match its header.
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
        let Some((id, range)) = self
            .open(asked)
            .find(|(_, range)| range.next() == Some(height))
        else {
            // The receiver retires the connection as a local
            // incompatibility, with no fault.
            return Label::Conformant;
        };
        let start = range.start;
        extra.exchange = Some(u64::from(start));
        let body_bytes = u64::try_from(header.payload_len.saturating_sub(1)).unwrap_or(u64::MAX);
        if range.body_bytes.saturating_add(body_bytes) > u64::from(range.max_body_bytes) {
            return Label::OverOwed;
        }
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
        let range = self.ranges.get_mut(&id).expect("found above");
        range.delivered += 1;
        range.body_bytes += body_bytes;
        extra.body = Some(height);
        Label::Conformant
    }

    /// The label of a body with no coinbase height, answering `asked`'s
    /// ranges.
    fn undecodable_body(&self, asked: Way, extra: &mut Extra) -> Label {
        match self.open(asked).next() {
            Some((_, range)) => {
                extra.exchange = Some(u64::from(range.start));
                Label::BadContent
            }
            None => Label::Unsolicited,
        }
    }

    /// Labels an ending for the range from `start`, sent `way`, whose
    /// content `agrees` checks against the bodies before it. A conformant
    /// one closes the range and opens each range that waited only on it.
    fn ending(
        &mut self,
        way: Way,
        start: u32,
        header: &FrameHeader,
        extra: &mut Extra,
        agrees: impl Fn(&Range) -> bool,
    ) -> Label {
        let asked = way.back();
        let Some((id, range)) = self.open(asked).find(|(_, range)| range.start == start) else {
            return Label::Unsolicited;
        };
        extra.exchange = Some(u64::from(start));
        let agrees = agrees(range);
        let connection = self.connection;
        let mut next = self.requests(asked).clone();
        let labels = next.response_frame(connection, u64::from(start), header);
        if !labels.is_conformant() {
            return labels.first();
        }
        if !agrees {
            return Label::BadContent;
        }
        *self.requests(asked) = next;
        self.ranges.remove(&id);
        let released: Vec<u64> = self
            .ranges
            .iter_mut()
            .filter_map(|(other, range)| {
                (range.waits_on.remove(&id) && range.is_open()).then_some(*other)
            })
            .collect();
        for other in released {
            self.activate(other, extra);
        }
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

/// A record the relays write, for the adapter to place.
#[derive(Debug)]
pub(crate) enum Record {
    /// An observation to write as it is.
    Obs(Observation),
    /// A peer's request frame `seq` on `connection` whose label waits for
    /// the node to judge it, recorded with the relay's label.
    Pending {
        connection: u64,
        seq: u64,
        frame: Observation,
        settle: Settle,
    },
    /// The node's reader judged the peer's frame `seq` on `connection` from
    /// its header, and refused it.
    Judged { connection: u64, seq: u64 },
    /// An exchange's [`Observation::ExchangeOwed`] as it opens, with its
    /// request frame's number.
    Owed {
        connection: u64,
        request: u64,
        obs: Observation,
    },
}

/// A body a peer delivered: its frame, and the exchange it answered, if
/// any.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Delivered {
    /// The frame, as its `PeerFrame` numbers it.
    pub(crate) seq: u64,
    /// The exchange its label names, `None` for a body no range took.
    pub(crate) exchange: Option<u64>,
}

/// One delivery of a body, with the tap's event count when it crossed, the
/// hash its body carries, and whether work already named it.
#[derive(Clone, Debug)]
struct Delivery {
    after: u64,
    delivered: Delivered,
    /// `None` for a frame that doesn't decode.
    hash: Option<String>,
    taken: bool,
}

/// A body frame that crossed to the node, as [`Wire::body_frames`] lists it.
#[derive(Clone, Debug)]
pub(crate) struct BodyFrame {
    /// The connection it crossed on.
    pub(crate) connection: u64,
    pub(crate) delivered: Delivered,
    /// The hash its body carries, `None` for a frame that doesn't decode.
    pub(crate) hash: Option<String>,
    /// Whether work already named it.
    pub(crate) taken: bool,
}

/// What the relays share with the node's tap.
struct WireState {
    /// The node's tap's event count, once the tap is attached.
    clock: Option<TapClock>,
    /// The request limit the node enforces on each connection.
    node_limit: u32,
    labellers: BTreeMap<u64, Labeller>,
    /// Records not yet written, each with the tap events recorded before
    /// it.
    records: Vec<(u64, Record)>,
    /// Every delivery of each body, by connection and height, in order.
    bodies: BTreeMap<(u64, u32), Vec<Delivery>>,
    /// Each link's close cause, and whether its close was taken.
    causes: Vec<(u64, CloseCauseReader, bool)>,
}

/// The relays of one run's links. Cloning shares them.
#[derive(Clone)]
pub struct Wire(Rc<RefCell<WireState>>);

impl Wire {
    /// The relays of a node that enforces `node_limit` requests per
    /// connection.
    pub fn new(node_limit: u32) -> Self {
        Wire(Rc::new(RefCell::new(WireState {
            clock: None,
            node_limit,
            labellers: BTreeMap::new(),
            records: Vec::new(),
            bodies: BTreeMap::new(),
            causes: Vec::new(),
        })))
    }

    /// Counts the tap's events from now on, for each record's place.
    pub(crate) fn set_clock(&self, clock: TapClock) {
        self.0.borrow_mut().clock = Some(clock);
    }

    /// A channel for frames sent `way` on `connection`, through a relay:
    /// the sender's end, the receiver's, and for the node's end the reader
    /// the node's peer must bind. Each side's end holds `depth` frames, as a
    /// plain link's does, since a sender's `try_send` fails on a full
    /// channel.
    pub(crate) fn pipe(
        &self,
        connection: u64,
        way: Way,
        depth: usize,
    ) -> (FramedSend, FramedRecv, Option<StreamReader>) {
        let (to_relay, from_sender) = framed_channel(depth);
        let (to_receiver, from_relay, reader) = match way {
            Way::ToNode => {
                let (send, recv, reader) = read_framed_channel(depth, block_sync_streams()[0]);
                (send, recv, Some(reader))
            }
            Way::FromNode => {
                let (send, recv) = framed_channel(depth);
                (send, recv, None)
            }
        };
        let mut state = self.0.borrow_mut();
        let node_limit = state.node_limit;
        state
            .labellers
            .entry(connection)
            .or_insert_with(|| Labeller::new(connection, node_limit));
        drop(state);
        hooks::spawn(
            "block-relay",
            forward(
                self.clone(),
                connection,
                way,
                from_sender,
                to_receiver,
                reader.clone(),
            ),
        );
        (to_relay, from_relay, reader)
    }

    /// Reports `connection`'s close once `cause` has one.
    pub(crate) fn watch(&self, connection: u64, cause: CloseCauseReader) {
        self.0.borrow_mut().causes.push((connection, cause, false));
    }

    /// Labels `frame`, sent `way` on `connection`, records the label, and
    /// returns the frame's number. `None` once the connection has closed,
    /// when the frame goes no further.
    fn record(&self, connection: u64, way: Way, frame: &Frame) -> Option<u64> {
        let now = hooks::now().map_or(Duration::ZERO, SimInstant::since_start);
        let mut state = self.0.borrow_mut();
        let state = &mut *state;
        if state
            .causes
            .iter()
            .any(|(at, cause, _)| *at == connection && cause.get().is_some())
        {
            return None;
        }
        let after = state.clock.as_ref().map_or(0, TapClock::events);
        let Some(labeller) = state.labellers.get_mut(&connection) else {
            panic!("a relay's connection has models, since its pipe made them");
        };
        let (obs, extra) = labeller.label(way, frame, now);
        let decoded = BlockSyncMessage::decode_frame(frame.clone()).ok();
        let height = decoded.as_ref().and_then(frame_height);
        let output = match way {
            Way::FromNode => node_output(connection, frame, height, &extra, labeller),
            Way::ToNode => None,
        };
        let seq = match &obs {
            Observation::PeerFrame { seq, .. } | Observation::NodeFrame { seq, .. } => *seq,
            _ => unreachable!("a relay labels a frame"),
        };
        state.records.push((
            after,
            match extra.pending {
                Some(settle) => Record::Pending {
                    connection,
                    seq,
                    frame: obs,
                    settle,
                },
                None => Record::Obs(obs),
            },
        ));
        if let (Way::ToNode, Some(BlockSyncMessage::Block(body))) = (way, &decoded) {
            if let Some(height) = body.coinbase_height() {
                state.records.push((
                    after,
                    Record::Obs(Observation::PeerBody {
                        connection,
                        seq,
                        height: height.0,
                        hash: hex(&body.hash().0),
                    }),
                ));
            }
        }
        // An ending's output closes its exchange before the exchanges
        // queued behind it open, which may share its key.
        if let Some(output) = output {
            state.records.push((after, Record::Obs(output)));
        }
        for (request, exchange, owed) in extra.opened {
            let obs = Observation::ExchangeOwed {
                connection,
                request: row(row_name(u16::from(MSG_BS_GET_BLOCKS))),
                exchange,
                frames: owed.frames,
                bytes: owed.bytes,
            };
            state.records.push((
                after,
                Record::Owed {
                    connection,
                    request,
                    obs,
                },
            ));
        }
        // Every body frame the peer sends, conformant or not, so work from a
        // body the models refused still names its frame.
        if let (Way::ToNode, Some(height), true) =
            (way, height, frame.message_type == u16::from(MSG_BS_BLOCK))
        {
            state
                .bodies
                .entry((connection, height))
                .or_default()
                .push(Delivery {
                    after,
                    delivered: Delivered {
                        seq,
                        exchange: extra.exchange.filter(|_| extra.body.is_some()),
                    },
                    hash: match &decoded {
                        Some(BlockSyncMessage::Block(body)) => Some(hex(&body.hash().0)),
                        _ => None,
                    },
                    taken: false,
                });
        }
        Some(seq)
    }

    /// Records that the node's reader judged the peer's frame `seq` on
    /// `connection` from its header and refused it.
    fn refused(&self, connection: u64, seq: u64) {
        let mut state = self.0.borrow_mut();
        let after = state.clock.as_ref().map_or(0, TapClock::events);
        state
            .records
            .push((after, Record::Judged { connection, seq }));
    }

    /// Takes the records not yet written, in order.
    pub(crate) fn take(&self) -> Vec<(u64, Record)> {
        std::mem::take(&mut self.0.borrow_mut().records)
    }

    /// The body frame at `height` on `connection` that the node's work
    /// from it came from, of those that crossed before the tap's event
    /// `before`: the oldest one a range took that no work named yet, else
    /// the newest.
    pub(crate) fn body(&self, connection: u64, height: u32, before: u64) -> Option<Delivered> {
        self.untaken_body(connection, height, before).or_else(|| {
            self.0
                .borrow()
                .bodies
                .get(&(connection, height))?
                .iter()
                .rev()
                .find(|delivery| delivery.after < before)
                .map(|delivery| delivery.delivered)
        })
    }

    /// The oldest body frame at `height` on `connection` that a range took
    /// and no work named yet, of those that crossed before the tap's event
    /// `before`, which it marks named.
    fn untaken_body(&self, connection: u64, height: u32, before: u64) -> Option<Delivered> {
        let mut state = self.0.borrow_mut();
        let delivery = state
            .bodies
            .get_mut(&(connection, height))?
            .iter_mut()
            .find(|delivery| untaken(delivery, before))?;
        delivery.taken = true;
        Some(delivery.delivered)
    }

    /// Every body frame at `height`, on any connection, of those that
    /// crossed before the tap's event `before`.
    pub(crate) fn body_frames(&self, height: u32, before: u64) -> Vec<BodyFrame> {
        self.0
            .borrow()
            .bodies
            .iter()
            .filter(|((_, at), _)| *at == height)
            .flat_map(|((connection, _), deliveries)| {
                deliveries
                    .iter()
                    .filter(|delivery| delivery.after < before)
                    .map(|delivery| BodyFrame {
                        connection: *connection,
                        delivered: delivery.delivered,
                        hash: delivery.hash.clone(),
                        taken: delivery.taken,
                    })
            })
            .collect()
    }

    /// Records that a body frame carrying `hash` crossed `connection` at
    /// `height` after the tap's event `after`, as a relay would.
    #[cfg(test)]
    pub(crate) fn push_body(
        &self,
        connection: u64,
        height: u32,
        after: u64,
        delivered: Delivered,
        hash: &str,
    ) {
        self.0
            .borrow_mut()
            .bodies
            .entry((connection, height))
            .or_default()
            .push(Delivery {
                after,
                delivered,
                hash: Some(hash.to_owned()),
                taken: false,
            });
    }

    /// Marks the body frame `seq` at `height` on `connection` named by work.
    pub(crate) fn name(&self, connection: u64, height: u32, seq: u64) {
        let mut state = self.0.borrow_mut();
        if let Some(delivery) = state
            .bodies
            .get_mut(&(connection, height))
            .and_then(|deliveries| {
                deliveries
                    .iter_mut()
                    .find(|delivery| delivery.delivered.seq == seq)
            })
        {
            delivery.taken = true;
        }
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

/// Whether `delivery` crossed before the tap's event `before`, a range
/// took it, and no work named it yet.
fn untaken(delivery: &Delivery, before: u64) -> bool {
    delivery.after < before && !delivery.taken && delivery.delivered.exchange.is_some()
}

/// The height a decoded body frame carries, or the start an ending names.
fn frame_height(message: &BlockSyncMessage) -> Option<u32> {
    match message {
        BlockSyncMessage::Block(body) => body.coinbase_height().map(|height| height.0),
        BlockSyncMessage::BlocksDone { start_height, .. }
        | BlockSyncMessage::RangeUnavailable { start_height, .. } => Some(start_height.0),
        _ => None,
    }
}

/// The [`Observation::NodeOutput`] of the node's response `frame` on
/// `connection`, with the frame's payload length. Its exchange is the one
/// its label names, else the peer's open range that holds `height`, else
/// `height`, which no open exchange has.
fn node_output(
    connection: u64,
    frame: &Frame,
    height: Option<u32>,
    extra: &Extra,
    labeller: &Labeller,
) -> Option<Observation> {
    let rule = MessageRule::find(rules(), frame.message_type)?;
    let MessageRole::Response { ends_exchange, .. } = rule.role else {
        return None;
    };
    let exchange = extra.exchange.or_else(|| {
        height.map(|height| u64::from(labeller.containing(Way::ToNode, height).unwrap_or(height)))
    });
    Some(Observation::NodeOutput {
        connection,
        frame: row(rule.name),
        request: row(row_name(u16::from(MSG_BS_GET_BLOCKS))),
        exchange: exchange.unwrap_or(u64::MAX),
        payload: u64::try_from(frame.payload.len()).expect("a frame fits in 64 bits"),
        ends: ends_exchange,
    })
}

/// Labels and forwards every frame from `from` to `to`, sent `way` on
/// `connection`, until either end goes away or the connection closes.
/// Into the node, `reader` admits each frame as a QUIC stream's reader
/// would, from when the node's service chooses its precheck.
async fn forward(
    wire: Wire,
    connection: u64,
    way: Way,
    mut from: FramedRecv,
    to: FramedSend,
    reader: Option<StreamReader>,
) {
    let Some(reader) = reader else {
        while let Some(frame) = from.recv().await {
            if wire.record(connection, way, &frame).is_none() || to.send(frame).await.is_err() {
                return;
            }
        }
        return;
    };
    let closed = std::pin::pin!(reader.closed());
    let mut closed = closed;
    zakura_rt::select! {
        biased;
        _ = &mut closed => return,
        _ = reader.ready() => {}
    }
    loop {
        let frame = zakura_rt::select! {
            biased;
            _ = &mut closed => return,
            frame = from.recv() => frame,
        };
        let Some(frame) = frame else {
            return;
        };
        let Some(seq) = wire.record(connection, way, &frame) else {
            return;
        };
        if !reader.admits(&frame) {
            // Record the judgment before the close, which the tap can see
            // as soon as the reader refuses.
            wire.refused(connection, seq);
            reader.refuse();
            return;
        }
        let sent = zakura_rt::select! {
            biased;
            _ = &mut closed => return,
            sent = to.send(frame) => sent,
        };
        if sent.is_err() {
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
        let mut labeller = Labeller::new(1, 1);
        labeller.status(
            Way::ToNode,
            &BlockSyncStatus {
                max_inflight_requests: 1,
                ..BlockSyncStatus::default()
            },
        );
        let mut extra = Extra::default();
        assert_eq!(
            labeller.request(Way::FromNode, 0, 1, 1, &mut extra),
            Label::Conformant
        );
        assert_eq!(
            labeller.request(Way::FromNode, 1, 2, 1, &mut extra),
            Label::OverMargin
        );
        assert_eq!(labeller.ranges.len(), 1);
    }

    /// A peer's request that overlaps a live range of its own, compared
    /// over the heights it asked for rather than those the node serves,
    /// waits for the node: its range opens after the ranges it overlaps,
    /// and it settles as `BadContent` if it overlaps a range the node still
    /// served when it judged the request. One the live ranges put past twice
    /// the node's limit settles as `OverMargin` unless enough of them ended
    /// first.
    #[test]
    fn peer_requests_wait_for_the_nodes_endings() {
        let mut labeller = Labeller::new(1, 1);
        let mut request = |start, count| {
            let mut extra = Extra::default();
            let seq = labeller.next[0];
            labeller.next[0] += 1;
            let label = labeller.request(Way::ToNode, seq, start, count, &mut extra);
            (label, extra.pending, extra.opened.len())
        };
        assert_eq!(request(1, 4), (Label::Conformant, None, 1));
        // The node serves one body of the first range, but it covers 1–4.
        let overlap = Settle {
            start: 2,
            count: 1,
            limit: 1,
        };
        assert_eq!(request(2, 1), (Label::BadContent, Some(overlap), 0));
        assert_eq!(overlap.label(&[(1, 4)]), Label::BadContent);
        assert_eq!(overlap.label(&[]), Label::Conformant);
        let (label, settle, opened) = request(9, 1);
        assert_eq!((label, opened), (Label::OverMargin, 1));
        let settle = settle.expect("the live ranges may have ended");
        assert_eq!(settle.label(&[(1, 4), (2, 2)]), Label::OverMargin);
        assert_eq!(settle.label(&[(2, 2)]), Label::Conformant);
    }

    /// The node's ending of a range opens the requests that waited only on
    /// it, and a body that isn't the next one a range expects is
    /// `Conformant` and changes no range.
    #[test]
    fn an_ending_opens_the_requests_queued_behind_it() {
        let mut labeller = Labeller::new(1, 2);
        let mut extra = Extra::default();
        labeller.request(Way::ToNode, 0, 1, 1, &mut extra);
        let mut extra = Extra::default();
        assert_eq!(
            labeller.request(Way::ToNode, 1, 1, 1, &mut extra),
            Label::BadContent
        );
        assert!(extra.opened.is_empty());
        let header = FrameHeader {
            message_type: 5,
            flags: 0,
            payload_len: 9,
        };
        let mut extra = Extra::default();
        let label = labeller.ending(Way::FromNode, 1, &header, &mut extra, |_| true);
        assert_eq!(label, Label::Conformant);
        assert_eq!(extra.opened.len(), 1);
        assert_eq!(extra.opened[0].0, 1, "the queued request's exchange opens");
    }
}
