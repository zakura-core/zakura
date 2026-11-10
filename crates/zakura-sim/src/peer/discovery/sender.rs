//! The discovery sender adapter (disc-3.1): labels each frame the node
//! writes on a discovery stream under the sender's own rules, causally, as
//! [`Observation::NodeFrame`].
//!
//! A frame is matched to the `frame-write-started` that began it, by
//! connection, QUIC stream and `seq`, and judged against the peer frames
//! the node had read (`frame-read`) when that write started. Reads the tap
//! logged later wait, so an answer the node read after it started a write
//! never excuses that write.
//!
//! The rules are the sender's, with none of the receiver's margins
//! (properties.md, "Generated properties", per row, sender side):
//!
//! - A request while the node already holds its row's `max_in_flight`
//!   unanswered requests on the stream is `OverMargin`. An answer the node
//!   read releases its request.
//! - An answer with no unanswered request of the peer's on the stream is
//!   `Unsolicited`, so an exchange ends at most once.
//! - An answer that carries more than its request asked for is
//!   `OverOwed`: a `Peers` with more records than its `GetPeers` limit, or a
//!   `Services` with a summary its `GetServices` didn't name. The node
//!   answers a row's requests in the order it read them, and the peer's
//!   frame record says what each request asked ([`Asked`]).
//! - A conformant frame of a row with a cadence is charged to its
//!   connection's bucket when the peer reads it, and an empty bucket marks
//!   it `exhausted`.
//!
//! A frame labelled with a fault changes no model state. A frame whose
//! header breaks its row takes the envelope's label.

use std::{
    collections::{BTreeMap, VecDeque},
    time::Duration,
};

use zakura_network::zakura::{
    wire_codec::{decode_frame, WireMessage},
    DiscoveryMessage, Frame, MessageRole, MessageRule,
};

use super::Asked;
use crate::{
    model::{CadenceModel, Charge, FrameHeader, FrameModel, Label, ReaderEnd},
    observe::{StreamId, TransportFact},
    Observation,
};

/// A discovery stream, by connection and QUIC stream.
type StreamKey = (u64, u64);

/// Labels the node's frames on every discovery stream, from the node's
/// transport events and the frames the peers read.
#[derive(Debug)]
pub(crate) struct DiscoverySender {
    /// The stream every frame is on.
    stream: StreamId,
    streams: BTreeMap<StreamKey, Ledger>,
    /// The cadence buckets, by connection.
    cadence: CadenceModel,
}

impl DiscoverySender {
    /// No streams yet, on `stream`.
    pub(crate) fn new(stream: StreamId) -> Self {
        DiscoverySender {
            stream,
            streams: BTreeMap::new(),
            cadence: CadenceModel::default(),
        }
    }

    /// Takes the node's transport `event` on `connection`'s discovery
    /// stream, in the order the tap logged it. Only `header-read`,
    /// `frame-read` and `frame-write-started` matter.
    pub(crate) fn node(&mut self, connection: u64, event: &TransportFact) {
        let (quic_stream, pending) = match *event {
            TransportFact::HeaderRead {
                quic_stream,
                seq,
                message_type,
                ..
            } => {
                let ledger = self.streams.entry((connection, quic_stream)).or_default();
                ledger.headers.insert(seq, message_type);
                return;
            }
            TransportFact::FrameRead { quic_stream, seq } => {
                let ledger = self.streams.entry((connection, quic_stream)).or_default();
                let Some(message_type) = ledger.headers.remove(&seq) else {
                    return;
                };
                (quic_stream, Pending::Read { seq, message_type })
            }
            TransportFact::FrameWriteStarted {
                quic_stream,
                seq,
                message_type,
            } => (quic_stream, Pending::Started { seq, message_type }),
            _ => return,
        };
        self.streams
            .entry((connection, quic_stream))
            .or_default()
            .pending
            .push_back(pending);
    }

    /// The peer's frame `seq` on `connection`'s `quic_stream` is a request
    /// that asked for `asked`. Its record is placed before the node's answer
    /// to it, since the peer wrote it first.
    pub(crate) fn asked(&mut self, connection: u64, quic_stream: u64, seq: u64, asked: Asked) {
        self.streams
            .entry((connection, quic_stream))
            .or_default()
            .asks
            .insert(seq, asked);
    }

    /// A peer read `frame`, the node's `seq`-th on `connection`'s
    /// `quic_stream`, at `at` since the run's start. Returns its
    /// `NodeFrame`.
    ///
    /// # Errors
    ///
    /// A harness failure if the node never started writing `seq` with that
    /// frame's type, or an earlier frame on the stream was never read,
    /// since then the frame has no causal history to judge it by.
    pub(crate) fn received(
        &mut self,
        connection: u64,
        quic_stream: u64,
        seq: u64,
        frame: &Frame,
        at: Duration,
    ) -> Result<Observation, String> {
        let ledger = self.streams.entry((connection, quic_stream)).or_default();
        loop {
            let pending = ledger.pending.pop_front().ok_or_else(|| {
                format!(
                    "UnstartedFrame: a peer read the node's discovery frame {seq} on connection \
                     {connection}'s stream {quic_stream} with no frame-write-started for it"
                )
            })?;
            match pending {
                Pending::Read { seq, message_type } => ledger.read(seq, message_type),
                Pending::Started {
                    seq: started,
                    message_type,
                } if started == seq => {
                    if message_type != frame.message_type {
                        return Err(format!(
                            "MismatchedFrame: the node started discovery frame {seq} on \
                             connection {connection}'s stream {quic_stream} as type \
                             {message_type}, and a peer read type {}",
                            frame.message_type
                        ));
                    }
                    break;
                }
                Pending::Started { seq: started, .. } => {
                    return Err(format!(
                        "UnreadFrame: a peer read the node's discovery frame {seq} on \
                         connection {connection}'s stream {quic_stream} before its frame \
                         {started}"
                    ))
                }
            }
        }
        let header = FrameHeader {
            message_type: frame.message_type,
            flags: frame.flags,
            payload_len: frame.payload.len(),
        };
        let envelope = ledger.frames.labels(&header);
        let row = ledger.frames.row(header.message_type);
        let label = match row {
            Some(row) if envelope.is_conformant() => {
                ledger.send(row, decode_frame::<DiscoveryMessage>(frame).ok().as_ref())
            }
            _ => envelope.first(),
        };
        let exhausted = row.is_some_and(|row| {
            label == Label::Conformant
                && self
                    .cadence
                    .charge(connection, self.stream.version, row, at)
                    == Charge::Exhausted
        });
        Ok(Observation::NodeFrame {
            connection,
            stream: self.stream,
            quic_stream: Some(quic_stream),
            seq,
            row: row.map(|row| row.name.to_owned()),
            rule_stream: self.stream,
            label,
            exhausted,
        })
    }
}

/// What the tap logged on one stream that a later frame waits behind.
#[derive(Clone, Copy, Debug)]
enum Pending {
    /// The node read the peer's frame `seq`, of `message_type`.
    Read { seq: u64, message_type: u16 },
    /// The node started writing frame `seq` of `message_type`.
    Started { seq: u64, message_type: u16 },
}

/// One discovery stream's sender model, and what waits to be applied.
#[derive(Debug)]
struct Ledger {
    /// The tap's events since the last frame labelled, oldest first.
    pending: VecDeque<Pending>,
    /// The type of each peer frame whose header the node read, by `seq`,
    /// until it reads the frame.
    headers: BTreeMap<u64, u16>,
    /// The node's unanswered requests, by request type.
    asked: BTreeMap<u16, u32>,
    /// The peer's requests the node read and hasn't answered, by request
    /// type, oldest first, each with what it asked for if its record said.
    owed: BTreeMap<u16, VecDeque<Option<Asked>>>,
    /// What each of the peer's requests asked for, by `seq`, until the node
    /// reads it.
    asks: BTreeMap<u64, Asked>,
    frames: FrameModel,
}

impl Default for Ledger {
    fn default() -> Self {
        Ledger {
            pending: VecDeque::new(),
            headers: BTreeMap::new(),
            asked: BTreeMap::new(),
            owed: BTreeMap::new(),
            asks: BTreeMap::new(),
            frames: FrameModel::new(
                <DiscoveryMessage as WireMessage>::RULES,
                ReaderEnd::Persistent,
            ),
        }
    }
}

impl Ledger {
    /// The node read the peer's frame `seq`, of `message_type`: a request
    /// it now owes an answer, or an answer that releases one of its
    /// requests.
    fn read(&mut self, seq: u64, message_type: u16) {
        let asked = self.asks.remove(&seq);
        match self.frames.row(message_type).map(|row| row.role) {
            Some(MessageRole::Request { .. }) => {
                self.owed.entry(message_type).or_default().push_back(asked);
            }
            Some(MessageRole::Response { request, .. }) => {
                if let Some(asked) = self.asked.get_mut(&request) {
                    *asked = asked.saturating_sub(1);
                }
            }
            _ => {}
        }
    }

    /// The sender label of the node sending a frame of `row` that decodes
    /// as `message`. A conformant one is applied.
    fn send(&mut self, row: &MessageRule, message: Option<&DiscoveryMessage>) -> Label {
        match row.role {
            MessageRole::Request { max_in_flight, .. } => {
                let asked = self.asked.entry(row.message_type).or_default();
                if *asked >= max_in_flight {
                    return Label::OverMargin;
                }
                *asked += 1;
                Label::Conformant
            }
            MessageRole::Response { request, .. } => {
                let Some(owed) = self.owed.get_mut(&request).filter(|owed| !owed.is_empty()) else {
                    return Label::Unsolicited;
                };
                let over = owed
                    .front()
                    .and_then(Option::as_ref)
                    .zip(message)
                    .is_some_and(|(asked, answer)| !asked.admits(answer));
                if over {
                    return Label::OverOwed;
                }
                owed.pop_front();
                Label::Conformant
            }
            _ => Label::Conformant,
        }
    }
}

#[cfg(test)]
mod tests {
    use zakura_chain::parameters::Network;
    use zakura_network::zakura::{wire_codec::encode_frame, Services, ZakuraHandshakeConfig};
    use zakura_quic::NodeSecretKey;

    use super::*;
    use crate::peer::discovery::{record, services, unix_now, Request, FIXTURE_ADDR};

    /// Discovery's stream.
    const STREAM: StreamId = StreamId {
        kind: 4,
        version: 2,
    };

    /// The node's stream, connection 1's QUIC stream 0, with the tap's
    /// events fed in order and the peer reading each started frame.
    struct Node {
        sender: DiscoverySender,
        seq: u64,
        peer_seq: u64,
    }

    impl Node {
        fn new() -> Self {
            Node {
                sender: DiscoverySender::new(STREAM),
                seq: 0,
                peer_seq: 0,
            }
        }

        /// The node reads the peer's `message`, whose record says what it
        /// asks for.
        fn reads(&mut self, message: &DiscoveryMessage) {
            let seq = self.peer_seq;
            self.peer_seq += 1;
            if let Some(asked) = Asked::of(message) {
                self.sender.asked(1, 0, seq, asked);
            }
            let header = TransportFact::HeaderRead {
                quic_stream: 0,
                seq,
                message_type: message.message_type(),
                flags: 0,
                payload_len: 0,
            };
            self.sender.node(1, &header);
            self.sender.node(
                1,
                &TransportFact::FrameRead {
                    quic_stream: 0,
                    seq,
                },
            );
        }

        /// The node writes `message`, the peer reads it at `at`, and this
        /// returns its label and whether it emptied the bucket.
        fn writes(&mut self, message: &DiscoveryMessage, at: Duration) -> (Label, bool) {
            let seq = self.seq;
            self.seq += 1;
            let frame = encode_frame(message).expect("a fixture message encodes");
            let started = TransportFact::FrameWriteStarted {
                quic_stream: 0,
                seq,
                message_type: frame.message_type,
            };
            self.sender.node(1, &started);
            match self.sender.received(1, 0, seq, &frame, at) {
                Ok(Observation::NodeFrame {
                    label, exhausted, ..
                }) => (label, exhausted),
                other => panic!("{other:?}"),
            }
        }
    }

    /// A request past `max_in_flight` before the node read the answer is
    /// `OverMargin`, an answer to no request is `Unsolicited`, and each
    /// conformant frame of a row with a cadence spends a token, which the
    /// bucket regains over time.
    #[test]
    fn the_node_frames_take_the_sender_rules() {
        let at = Duration::ZERO;
        let get_peers = Request::GetPeers.message();
        let peers = DiscoveryMessage::Peers {
            records: Vec::new(),
        };
        let mut node = Node::new();
        assert_eq!(node.writes(&get_peers, at), (Label::Conformant, false));
        assert_eq!(node.writes(&get_peers, at).0, Label::OverMargin);
        node.reads(&peers);
        assert_eq!(node.writes(&get_peers, at), (Label::Conformant, false));
        assert_eq!(node.writes(&peers, at).0, Label::Unsolicited);
        node.reads(&get_peers);
        assert_eq!(node.writes(&peers, at), (Label::Conformant, false));
        assert_eq!(
            node.writes(&peers, at).0,
            Label::Unsolicited,
            "a second ending"
        );

        let handshake =
            ZakuraHandshakeConfig::for_network(&Network::new_regtest(Default::default()));
        let secret = NodeSecretKey::from_bytes(&[9; 32]);
        let hello = DiscoveryMessage::Hello {
            record: record(&secret, &handshake, unix_now(), vec![FIXTURE_ADDR]),
        };
        let mut node = Node::new();
        let exhausted: Vec<bool> = (0..64).map(|_| node.writes(&hello, at).1).collect();
        let first = exhausted.iter().position(|empty| *empty);
        assert!(first.is_some_and(|first| first > 0), "{exhausted:?}");

        // Hellos at the row's send interval, past its capacity, never find
        // the bucket empty: it refills between them.
        let MessageRole::Announcement { cadence } = FrameModel::new(
            <DiscoveryMessage as WireMessage>::RULES,
            ReaderEnd::Persistent,
        )
        .row(hello.message_type())
        .expect("discovery tables Hello")
        .role
        else {
            unreachable!("Hello is an announcement")
        };
        let mut node = Node::new();
        let spaced: Vec<bool> = (0..cadence.capacity + 2)
            .map(|sent| node.writes(&hello, cadence.send_interval * sent).1)
            .collect();
        assert!(!spaced.contains(&true), "{spaced:?}");
    }

    /// An answer that carries more than its request asked for is
    /// `OverOwed`, and changes no model state: the request stays owed, so
    /// a later answer within it is `Conformant`. A `Peers` may carry at most
    /// its `GetPeers` limit's records, and a `Services` only the summaries
    /// its `GetServices` named.
    #[test]
    fn an_answer_past_its_request_is_over_owed() {
        let at = Duration::ZERO;
        let handshake =
            ZakuraHandshakeConfig::for_network(&Network::new_regtest(Default::default()));
        let authors = [3, 4].map(|byte| NodeSecretKey::from_bytes(&[byte; 32]));
        let records: Vec<_> = authors
            .iter()
            .map(|author| record(author, &handshake, unix_now(), vec![FIXTURE_ADDR]))
            .collect();
        let peers = |count: usize| DiscoveryMessage::Peers {
            records: records[..count].to_vec(),
        };
        let mut node = Node::new();
        node.reads(&Request::GetOnePeer.message());
        assert_eq!(node.writes(&peers(2), at).0, Label::OverOwed);
        assert_eq!(node.writes(&peers(1), at).0, Label::Conformant);
        assert_eq!(node.writes(&peers(0), at).0, Label::Unsolicited);
        node.reads(&Request::GetPeers.message());
        assert_eq!(node.writes(&peers(2), at).0, Label::Conformant);

        let summarized = DiscoveryMessage::Services(services(authors[0].public()));
        let empty = DiscoveryMessage::Services(Services {
            summaries: Vec::new(),
            ..services(authors[0].public())
        });
        node.reads(&Request::GetHeaderSyncService.message());
        assert_eq!(node.writes(&summarized, at).0, Label::OverOwed);
        assert_eq!(node.writes(&empty, at).0, Label::Conformant);
        node.reads(&Request::GetServices.message());
        assert_eq!(node.writes(&summarized, at).0, Label::Conformant);
    }
}
