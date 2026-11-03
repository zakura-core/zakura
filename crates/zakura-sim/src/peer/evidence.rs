//! What scripted peers record, placed by the identity map, and what the
//! node's evidence means for P3b's close checks (M5-D).
//!
//! A peer's task records each frame it sends and reads with its own address
//! and the QUIC stream, which both ends share. [`WireEvidence`] holds each
//! record until the transport tap has logged the connection's `registered`
//! event and the stream's `Session` row, then logs it as a `PeerFrame`,
//! `PeerWritten` or `PeerReceived` with that connection (M5-Q4). Records of
//! one stream keep their order. A record whose stream can no longer be
//! admitted, because the node refused it or its handshake, or tore its
//! connection down, fails the run, so a peer's frames can't drop out of the
//! log unseen.
//!
//! Each `PeerReceived` is followed by the frame's `NodeFrame`, which the
//! [`SenderAdapter`] labels under the sender's rules against the peer
//! updates the node had applied when it started writing the frame (M5-E2).
//!
//! It also projects the node's evidence onto P3b's observations:
//! `FrameJudged` at the reader stage that decides the frame (M5-E1, see
//! [`super::stage`]), `ConnectionClosed` for a close decided with a cause,
//! and `Misbehavior` for the connection of the session a reactor
//! misbehavior names. A misbehavior with no origin session is a harness
//! failure, and so is a frame the reader reached with no `PeerFrame`, or a
//! response whose payload arrived before its precheck's checkpoint.

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, VecDeque},
    net::SocketAddr,
    rc::Rc,
    time::Duration,
};

use zakura_network::zakura::{v9::RegtestMessage, Frame, MessageRole, MessageRule};

use super::{
    adapter::SenderAdapter,
    stage::{header_stage, NodeView, Stage},
};
use crate::{
    model::{FrameHeader, Label},
    observe::{PeerCredit, ReactorFact, StreamId, TransitionFact, TransportFact},
    Observation, ObservationLog,
};

/// A frame the node's reader has the header of, by connection, QUIC stream
/// and `seq`.
type FrameKey = (u64, u64, u64);

/// A frame whose header the reader has and whose judgement waits.
#[derive(Clone, Copy, Debug)]
struct Waiting {
    stage: Stage,
    row: Option<&'static MessageRule>,
    payload_len: usize,
}

/// What the peers' tasks recorded and the identity map hasn't placed yet,
/// oldest first.
pub(crate) type PeerRecords = Rc<RefCell<VecDeque<PeerRecord>>>;

/// One thing a peer did on a header stream.
#[derive(Clone, Debug)]
pub(crate) struct PeerRecord {
    /// The peer's address on the connection, which the node sees as the
    /// remote.
    pub(crate) remote: SocketAddr,
    /// The QUIC stream.
    pub(crate) quic_stream: u64,
    pub(crate) body: RecordBody,
}

/// A [`PeerRecord`]'s observation, without its connection.
#[derive(Clone, Debug)]
pub(crate) enum RecordBody {
    /// [`Observation::PeerFrame`]: a frame offered, as the models labelled
    /// it.
    Frame {
        seq: u64,
        row: Option<String>,
        rule_stream: StreamId,
        label: Label,
        exhausted: bool,
        exchange: Option<u64>,
        payload: u64,
    },
    /// [`Observation::PeerWritten`].
    Written { seq: u64 },
    /// [`Observation::PeerClosed`].
    Closed,
    /// [`Observation::PeerReceived`], with the frame and when the peer
    /// read it, since the run's start, for its `NodeFrame`.
    Received {
        seq: u64,
        message_type: u16,
        payload: u64,
        credit: Option<PeerCredit>,
        frame: Frame,
        at: Duration,
    },
}

/// The identity map as the log has it, and the labels logged so far.
#[derive(Debug, Default)]
pub(crate) struct WireEvidence {
    /// How many log entries it has read.
    seen: usize,
    /// Each registered connection, by its remote address as the log
    /// writes it. A remote gets one handshake outcome at most
    /// ([`WireEvidence::first_outcome`]).
    connections: BTreeMap<String, u64>,
    /// Each admitted stream's kind and version, by connection and QUIC
    /// stream.
    streams: BTreeMap<(u64, u64), StreamId>,
    /// Each admitted session's connection, by session ID.
    sessions: BTreeMap<u64, u64>,
    /// Each logged `PeerFrame`'s label, by connection, QUIC stream and
    /// `seq`.
    labels: BTreeMap<(u64, u64, u64), Label>,
    /// Remote addresses whose handshake the node refused.
    refused_handshakes: BTreeSet<String>,
    /// Streams the node refused, by connection and QUIC stream.
    refused_streams: BTreeSet<(u64, u64)>,
    /// Connections the node tore down.
    torn_down: BTreeSet<u64>,
    /// Each header session's reader ledger, by connection and QUIC stream.
    views: BTreeMap<(u64, u64), NodeView>,
    /// Frames whose header the reader has, not judged yet.
    waiting: BTreeMap<FrameKey, Waiting>,
    /// Labels the node's frames.
    sender: SenderAdapter,
}

impl WireEvidence {
    /// Reads the identity rows logged since the last call, logs every record
    /// they place, then the P3b observations the node's new evidence means.
    pub(crate) fn observe(
        &mut self,
        records: &PeerRecords,
        log: &mut ObservationLog,
    ) -> Result<(), String> {
        let start = self.seen;
        for entry in &log.entries()[start..] {
            if let Observation::Transport {
                connection: Some(connection),
                event,
                ..
            } = &entry.obs
            {
                self.sender.node(*connection, event);
            }
            match &entry.obs {
                Observation::Transport {
                    connection: Some(connection),
                    remote,
                    event: TransportFact::Registered,
                    ..
                } => {
                    self.first_outcome(remote)?;
                    self.connections.insert(remote.clone(), *connection);
                }
                Observation::Transport {
                    connection: None,
                    remote,
                    event: TransportFact::HandshakeRefused { .. },
                    ..
                } => {
                    self.first_outcome(remote)?;
                    self.refused_handshakes.insert(remote.clone());
                }
                Observation::Transport {
                    connection: Some(connection),
                    event: TransportFact::StreamRefused { quic_stream, .. },
                    ..
                } => {
                    self.refused_streams.insert((*connection, *quic_stream));
                }
                Observation::Transport {
                    connection: Some(connection),
                    event: TransportFact::TornDown,
                    ..
                } => {
                    self.torn_down.insert(*connection);
                }
                Observation::Session {
                    connection,
                    quic_stream,
                    kind,
                    version,
                    session_id,
                    ..
                } => {
                    let stream = StreamId {
                        kind: *kind,
                        version: *version,
                    };
                    self.streams.insert((*connection, *quic_stream), stream);
                    self.sessions.insert(*session_id, *connection);
                }
                _ => {}
            }
        }
        let mut placed = Vec::new();
        let mut failed = None;
        records
            .borrow_mut()
            .retain(|record| match self.place(record) {
                Some(Ok(obs)) => {
                    placed.extend(obs);
                    false
                }
                Some(Err(why)) => {
                    failed.get_or_insert(why);
                    false
                }
                None => true,
            });
        if let Some(why) = failed {
            return Err(why);
        }
        if let Some(lost) = records.borrow().iter().find_map(|record| self.lost(record)) {
            return Err(lost);
        }
        let mut projected = Vec::new();
        for entry in &log.entries()[start..] {
            if let Some(obs) = self.project(&entry.obs)? {
                projected.push(obs);
            }
        }
        for obs in placed.into_iter().chain(projected) {
            log.push(obs);
        }
        self.seen = log.entries().len();
        Ok(())
    }

    /// Fails the run once `remote` gets a second handshake outcome,
    /// registered or refused. Records name their connection by the peer's
    /// address alone, and a node that redials a listening peer reaches it at
    /// the same address, with QUIC stream IDs counting from 0 again. So a
    /// record left from the old connection would be placed on the new one's
    /// stream, or judged by the old one's ending. Dialing peers bind a fresh
    /// address per dial, so only a node redial reuses one.
    fn first_outcome(&self, remote: &str) -> Result<(), String> {
        if self.connections.contains_key(remote) || self.refused_handshakes.contains(remote) {
            return Err(format!(
                "AliasedRemote: {remote} got a second handshake outcome. Peer records are keyed \
                 by remote address, so the harness doesn't support the node redialing a \
                 listening peer yet"
            ));
        }
        Ok(())
    }

    /// `record` as its observations, once its stream is admitted: one, or
    /// a `PeerReceived` and its `NodeFrame`. An error if the sender adapter
    /// can't label the node's frame.
    fn place(&mut self, record: &PeerRecord) -> Option<Result<Vec<Observation>, String>> {
        let connection = *self.connections.get(&record.remote.to_string())?;
        let stream = *self.streams.get(&(connection, record.quic_stream))?;
        let quic_stream = record.quic_stream;
        Some(Ok(vec![match record.body.clone() {
            RecordBody::Frame {
                seq,
                row,
                rule_stream,
                label,
                exhausted,
                exchange,
                payload,
            } => {
                self.labels.insert((connection, quic_stream, seq), label);
                Observation::PeerFrame {
                    connection,
                    stream,
                    quic_stream: Some(quic_stream),
                    seq,
                    row,
                    rule_stream,
                    label,
                    exhausted,
                    exchange,
                    payload,
                }
            }
            RecordBody::Written { seq } => Observation::PeerWritten {
                connection,
                quic_stream,
                seq,
            },
            RecordBody::Closed => Observation::PeerClosed {
                connection,
                quic_stream,
            },
            RecordBody::Received {
                seq,
                message_type,
                payload,
                credit,
                frame,
                at,
            } => {
                let node_frame =
                    match self
                        .sender
                        .received(connection, quic_stream, seq, &frame, at)
                    {
                        Ok(node_frame) => node_frame,
                        Err(why) => return Some(Err(why)),
                    };
                return Some(Ok(vec![
                    Observation::PeerReceived {
                        connection,
                        quic_stream,
                        seq,
                        message_type,
                        payload,
                        credit,
                    },
                    node_frame,
                ]));
            }
        }]))
    }

    /// Why `record`, still unplaced, never will be, if the node refused its
    /// handshake or stream or tore its connection down.
    fn lost(&self, record: &PeerRecord) -> Option<String> {
        let remote = record.remote.to_string();
        let gone = match self.connections.get(&remote) {
            None => self.refused_handshakes.contains(&remote),
            Some(connection) => {
                self.torn_down.contains(connection)
                    || self
                        .refused_streams
                        .contains(&(*connection, record.quic_stream))
            }
        };
        gone.then(|| {
            format!(
                "UnplacedRecord: {remote}'s record on stream {} can't get an identity, since the \
                 node refused or closed it before admitting the stream: {:?}",
                record.quic_stream, record.body
            )
        })
    }

    /// The P3b observation `obs` means, if any.
    fn project(&mut self, obs: &Observation) -> Result<Option<Observation>, String> {
        Ok(match obs {
            Observation::Transport {
                connection: Some(connection),
                event,
                ..
            } => match event {
                TransportFact::HeaderRead {
                    quic_stream,
                    seq,
                    message_type,
                    flags,
                    payload_len,
                } => {
                    let header = FrameHeader {
                        message_type: *message_type,
                        flags: *flags,
                        // A u32 fits usize on every supported target.
                        payload_len: *payload_len as usize,
                    };
                    self.header_read((*connection, *quic_stream, *seq), header)?
                }
                TransportFact::Regulated {
                    quic_stream,
                    transition,
                } => self.regulated(*connection, *quic_stream, transition)?,
                TransportFact::FrameRead { quic_stream, seq } => {
                    let key = (*connection, *quic_stream, *seq);
                    match self.waiting.get(&key).map(|waiting| waiting.stage) {
                        Some(Stage::Frame) => {
                            self.waiting.remove(&key);
                            Some(self.judged(key)?)
                        }
                        Some(Stage::Checkpoint) => {
                            return Err(format!(
                                "MissingCheckpoint: connection {connection} read frame {seq} \
                                 on stream {quic_stream} before its response precheck's \
                                 header-checked"
                            ))
                        }
                        // Judged at its header already. Whether the reader
                        // should have read it is for the close checks.
                        Some(Stage::Header) | None => None,
                    }
                }
                TransportFact::CloseDecided { cause: Some(cause) } => {
                    Some(Observation::ConnectionClosed {
                        connection: *connection,
                        cause: cause.clone(),
                    })
                }
                _ => None,
            },
            Observation::Reactor {
                event: ReactorFact::Misbehavior { origin, peer, .. },
                ..
            } => {
                let connection = origin
                    .and_then(|session| self.sessions.get(&session))
                    .ok_or_else(|| {
                        format!(
                            "UnattributedMisbehavior: peer {peer}'s misbehavior names no \
                             admitted session ({origin:?})"
                        )
                    })?;
                Some(Observation::Misbehavior {
                    connection: *connection,
                })
            }
            _ => None,
        })
    }

    /// The node's reader has `header` for frame `key`: judges it now if
    /// the header alone has a fault, and otherwise waits for the stage that
    /// decides it.
    fn header_read(
        &mut self,
        key: FrameKey,
        header: FrameHeader,
    ) -> Result<Option<Observation>, String> {
        let (connection, quic_stream, seq) = key;
        let stream = self.stream(connection, quic_stream)?;
        if !self.labels.contains_key(&key) {
            return Err(format!(
                "UnlabelledFrame: connection {connection} read frame {seq} on stream \
                 {quic_stream} with no PeerFrame"
            ));
        }
        let rules = rules(stream).ok_or_else(|| {
            format!("UntabledStream: connection {connection} read a frame on untabled {stream}")
        })?;
        let (stage, row) = header_stage(rules, &header);
        if stage == Stage::Header {
            // The reader still runs a response's precheck for a header whose
            // type, flags and minimum pass, so its checkpoint may follow.
            if row.is_some_and(|row| matches!(row.role, MessageRole::Response { .. })) {
                self.waiting.insert(
                    key,
                    Waiting {
                        stage,
                        row,
                        payload_len: header.payload_len,
                    },
                );
            }
            return self.judged(key).map(Some);
        }
        self.waiting.insert(
            key,
            Waiting {
                stage,
                row,
                payload_len: header.payload_len,
            },
        );
        Ok(None)
    }

    /// Applies `transition` to the ledger of `connection`'s `quic_stream`,
    /// and judges a response at its `header-checked` checkpoint if the
    /// ledger, as it stood there, decides it.
    fn regulated(
        &mut self,
        connection: u64,
        quic_stream: u64,
        transition: &TransitionFact,
    ) -> Result<Option<Observation>, String> {
        let view = self.views.entry((connection, quic_stream)).or_default();
        let TransitionFact::HeaderChecked { seq, .. } = transition else {
            view.apply(transition)?;
            return Ok(None);
        };
        let key = (connection, quic_stream, *seq);
        let Some(Waiting {
            stage: stage @ (Stage::Checkpoint | Stage::Header),
            row: Some(row),
            payload_len,
        }) = self.waiting.get(&key).copied()
        else {
            return Err(format!(
                "MissingCheckpoint: connection {connection} checked frame {seq} on stream \
                 {quic_stream}, which waits for no checkpoint"
            ));
        };
        let decided = view.decidable_at_header(row, payload_len).is_some();
        view.reserve(row, payload_len);
        if stage == Stage::Header {
            // Judged at its header already: only the ledger moves.
            self.waiting.remove(&key);
            return Ok(None);
        }
        if decided {
            self.waiting.remove(&key);
            return self.judged(key).map(Some);
        }
        self.waiting.insert(
            key,
            Waiting {
                stage: Stage::Frame,
                row: Some(row),
                payload_len,
            },
        );
        Ok(None)
    }

    /// The admitted stream of `connection`'s `quic_stream`.
    fn stream(&self, connection: u64, quic_stream: u64) -> Result<StreamId, String> {
        self.streams
            .get(&(connection, quic_stream))
            .copied()
            .ok_or_else(|| {
                format!(
                    "AmbiguousIdentity: connection {connection} read stream {quic_stream} \
                     before its session row"
                )
            })
    }

    /// `FrameJudged` for frame `key`.
    fn judged(&self, (connection, quic_stream, seq): FrameKey) -> Result<Observation, String> {
        Ok(Observation::FrameJudged {
            connection,
            stream: self.stream(connection, quic_stream)?,
            quic_stream: Some(quic_stream),
            seq,
        })
    }
}

/// The rows of a stream the wire harness runs, if it has a table.
fn rules(stream: StreamId) -> Option<&'static [MessageRule]> {
    RegtestMessage::LAYOUT
        .iter()
        .find(|layout| (layout.kind, layout.version) == (stream.kind, stream.version))
        .and_then(|layout| layout.messages)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::peer_addr;

    /// A transport event on `connection` from peer 0's first address.
    fn transport(seq: u64, connection: Option<u64>, event: TransportFact) -> Observation {
        Observation::Transport {
            instance: 0,
            seq,
            node: "node".to_owned(),
            connection,
            remote: peer_addr(0, 0).to_string(),
            event,
        }
    }

    /// Peer 0's `PeerFrame` record for its first frame on QUIC stream 4.
    fn status_record() -> PeerRecords {
        Rc::new(RefCell::new(VecDeque::from([PeerRecord {
            remote: peer_addr(0, 0),
            quic_stream: 4,
            body: RecordBody::Written { seq: 0 },
        }])))
    }

    /// A node frame the peer couldn't decode, here one with a reserved flag
    /// bit, still gets its `NodeFrame` after its `PeerReceived`, labelled
    /// from its header.
    #[test]
    fn an_undecodable_node_frame_is_labelled() {
        let frame = Frame {
            message_type: RegtestMessage::ROWS[0].message_type,
            flags: 1,
            payload: Vec::new(),
        };
        let records = Rc::new(RefCell::new(VecDeque::from([PeerRecord {
            remote: peer_addr(0, 0),
            quic_stream: 4,
            body: RecordBody::Received {
                seq: 0,
                message_type: frame.message_type,
                payload: 0,
                credit: None,
                frame: frame.clone(),
                at: Duration::ZERO,
            },
        }])));
        let mut log = ObservationLog::default();
        log.push(transport(1, Some(1), TransportFact::Registered));
        log.push(Observation::Session {
            connection: 1,
            peer: "peer-0".to_owned(),
            quic_stream: 4,
            kind: RegtestMessage::LAYOUT[0].kind,
            version: RegtestMessage::LAYOUT[0].version,
            session_id: 7,
        });
        log.push(transport(
            2,
            Some(1),
            TransportFact::FrameWriteStarted {
                quic_stream: 4,
                seq: 0,
                message_type: frame.message_type,
            },
        ));
        WireEvidence::default()
            .observe(&records, &mut log)
            .expect("the record is placed");
        let logged: Vec<_> = log.entries()[3..].iter().map(|entry| &entry.obs).collect();
        assert!(
            matches!(
                logged[..],
                [
                    Observation::PeerReceived { .. },
                    Observation::NodeFrame {
                        label: Label::ReservedFlags,
                        ..
                    }
                ]
            ),
            "{logged:?}"
        );
    }

    /// A record waits while its stream may still be admitted, and fails the
    /// run once the node refuses the stream or the handshake, or tears the
    /// connection down first, so it can't drop out of the log unseen.
    #[test]
    fn a_record_that_can_never_be_placed_fails_the_run() {
        let refused = TransportFact::StreamRefused {
            quic_stream: 4,
            kind: 5,
            version: 9,
            reason: "unnegotiated_capability".to_owned(),
        };
        let handshake = TransportFact::HandshakeRefused {
            reason: "Zakura control nonce mismatch".to_owned(),
        };
        let cases = [
            (Some(1), refused),
            (Some(1), TransportFact::TornDown),
            (None, handshake),
        ];
        for (connection, ending) in cases {
            let records = status_record();
            let mut evidence = WireEvidence::default();
            let mut log = ObservationLog::default();
            if connection.is_some() {
                log.push(transport(1, connection, TransportFact::Registered));
            }
            evidence
                .observe(&records, &mut log)
                .expect("the stream may still be admitted");
            assert_eq!(records.borrow().len(), 1, "it waits");
            log.push(transport(2, connection, ending.clone()));
            let error = evidence
                .observe(&records, &mut log)
                .expect_err("the record can never be placed");
            assert!(error.starts_with("UnplacedRecord"), "{ending:?}: {error}");
        }
    }

    /// Fed one reader event at a time, the projection judges each frame at
    /// its own stage: a request after its payload, a header fault at its
    /// header, an unsolicited page at its checkpoint, and an ending for a
    /// live subscription only after its payload, not at its checkpoint.
    #[test]
    fn each_frame_is_judged_at_its_stage() {
        let stream = StreamId {
            kind: 5,
            version: 9,
        };
        let records = PeerRecords::default();
        records.borrow_mut().extend((0..4).map(|seq| PeerRecord {
            remote: peer_addr(0, 0),
            quic_stream: 4,
            body: RecordBody::Frame {
                seq,
                row: None,
                rule_stream: stream,
                label: Label::Conformant,
                exhausted: false,
                exchange: None,
                payload: 0,
            },
        }));
        let mut evidence = WireEvidence::default();
        let mut log = ObservationLog::default();
        log.push(transport(1, Some(1), TransportFact::Registered));
        log.push(Observation::Session {
            connection: 1,
            peer: "peer-0".to_owned(),
            quic_stream: 4,
            kind: 5,
            version: 9,
            session_id: 7,
        });
        let mut event = 1;
        let mut feed = |log: &mut ObservationLog, fact: TransportFact| {
            event += 1;
            log.push(transport(event, Some(1), fact));
            evidence
                .observe(&records, log)
                .expect("the evidence is whole");
            log.entries()
                .iter()
                .filter_map(|entry| match entry.obs {
                    Observation::FrameJudged { seq, .. } => Some(seq),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        let header = |seq, message_type, flags, payload_len| TransportFact::HeaderRead {
            quic_stream: 4,
            seq,
            message_type,
            flags,
            payload_len,
        };
        let read = |seq| TransportFact::FrameRead {
            quic_stream: 4,
            seq,
        };
        let checked = |seq| TransportFact::Regulated {
            quic_stream: 4,
            transition: TransitionFact::HeaderChecked {
                seq,
                outcome: crate::observe::HeaderCheckFact::AdmittedTerminal,
            },
        };
        assert!(
            feed(&mut log, header(0, 2, 0, 107)).is_empty(),
            "a Grant waits"
        );
        assert_eq!(feed(&mut log, read(0)), [0]);
        assert_eq!(feed(&mut log, header(1, 1, 1, 122)), [0, 1], "a flag bit");
        assert_eq!(feed(&mut log, header(2, 3, 0, 263)), [0, 1], "a page waits");
        assert_eq!(feed(&mut log, checked(2)), [0, 1, 2], "with nothing live");
        let opened = TransportFact::Regulated {
            quic_stream: 4,
            transition: TransitionFact::OpenIssued {
                subscription: 1,
                objects: 1,
                bytes: 263,
            },
        };
        feed(&mut log, opened);
        feed(&mut log, header(3, 4, 0, 41));
        assert_eq!(
            feed(&mut log, checked(3)),
            [0, 1, 2],
            "an ending for a live one"
        );
        assert_eq!(feed(&mut log, read(3)), [0, 1, 2, 3]);
    }

    /// A second handshake outcome for a remote address already mapped, as a
    /// node redial to a listening peer gives, fails the run instead of
    /// moving the address to the new connection, where a record still
    /// waiting from the old one would be placed on the new one's stream.
    /// In the first case a record waits on connection 1's stream 4 when
    /// connection 2 registers from the same address and admits its own
    /// stream 4.
    #[test]
    fn a_second_handshake_from_one_remote_fails_the_run() {
        let refused = || TransportFact::HandshakeRefused {
            reason: "Zakura control nonce mismatch".to_owned(),
        };
        let cases = [
            (Some(1), Some(2), TransportFact::Registered),
            (Some(1), None, refused()),
            (None, Some(2), TransportFact::Registered),
        ];
        for (first, second, outcome) in cases {
            // A record from a refused handshake can never be placed, so only
            // a registered first connection leaves one waiting.
            let records = match first {
                Some(_) => status_record(),
                None => PeerRecords::default(),
            };
            let waiting = records.borrow().len();
            let mut evidence = WireEvidence::default();
            let mut log = ObservationLog::default();
            let first_outcome = match first {
                Some(_) => TransportFact::Registered,
                None => refused(),
            };
            log.push(transport(1, first, first_outcome));
            evidence
                .observe(&records, &mut log)
                .expect("a remote's first outcome is fine");
            log.push(transport(2, second, outcome.clone()));
            if let Some(connection) = second {
                log.push(Observation::Session {
                    connection,
                    quic_stream: 4,
                    kind: 5,
                    version: 9,
                    session_id: 1,
                    peer: "peer-0".to_owned(),
                });
            }
            let error = evidence
                .observe(&records, &mut log)
                .expect_err("the second outcome fails the run");
            assert!(error.starts_with("AliasedRemote"), "{outcome:?}: {error}");
            assert_eq!(records.borrow().len(), waiting, "nothing was placed");
        }
    }
}
