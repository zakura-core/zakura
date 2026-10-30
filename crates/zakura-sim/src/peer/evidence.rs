//! What scripted peers record, placed by the identity map, and what the
//! node's evidence means for P3b's close checks (M5-D).
//!
//! A peer's task records each frame it sends and reads with its own address,
//! the QUIC stream, which both ends share, and the position it happened at.
//! The evidence hub folds each step's rows through [`WireEvidence`] in key
//! order, and [`WireEvidence`] holds each record until the connection's
//! `registered` event and the stream's `Session` row have passed, then
//! places it as a `PeerFrame`, `PeerWritten` or `PeerReceived` with that
//! connection (M5-Q4), keyed after the row that let it resolve. Records of
//! one stream keep their order. A record whose stream can no longer be
//! admitted, because the node refused it or its handshake, or tore its
//! connection down, keeps only the `peer-emitted` row the hub logged in its
//! own step, so it never drops out of the log.
//!
//! Each `PeerReceived` is followed by the frame's `NodeFrame`, which the
//! [`SenderAdapter`] labels under the sender's rules against the peer
//! updates the node had applied when it started writing the frame (M5-E2).
//! The [`OutputLedger`] checks the node's output lives and each frame a
//! peer read against the output its write started (M6a-6).
//!
//! It also projects the node's evidence onto P3b's observations:
//! `FrameJudged` at the reader stage that decides the frame (M5-E1, see
//! [`super::stage`]), `ConnectionClosed` for a close decided with a cause,
//! and `Misbehavior` for the connection of the session a reactor
//! misbehavior names. A misbehavior with no origin session is a harness
//! failure, and so is a frame the reader reached with no `PeerFrame`, or a
//! response whose payload arrived before its precheck's checkpoint.

use std::{
    collections::{BTreeMap, BTreeSet},
    mem,
    net::SocketAddr,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use sha2::{Digest, Sha256};
use zakura_network::zakura::{v9::RegtestMessage, Frame, MessageRole, MessageRule};

use super::{
    adapter::SenderAdapter,
    output::OutputLedger,
    stage::{header_stage, NodeView, Stage},
};
use crate::{
    model::{FrameHeader, Label},
    observe::{
        ConnEnd, Direction, EmittedBody, PeerCredit, ReactorFact, StreamEnd, StreamId,
        TransitionFact, TransportFact,
    },
    Observation,
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

/// What the peers' tasks record, shared with the evidence hub that drains
/// it at each observation. The tasks that read a peer's streams record too,
/// so it's `Send`.
#[derive(Clone, Debug, Default)]
pub(crate) struct PeerRecords(Arc<Mutex<Queue>>);

/// [`PeerRecords`]' contents.
#[derive(Debug, Default)]
struct Queue {
    /// Recorded and not yet drained, oldest first.
    recorded: Vec<PeerRecord>,
    /// How many drained records the identity map hasn't placed yet.
    unplaced: usize,
}

impl PeerRecords {
    /// Locks the queue. Every change completes under the lock.
    fn lock(&self) -> std::sync::MutexGuard<'_, Queue> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records `record`.
    pub(crate) fn push(&self, record: PeerRecord) {
        self.lock().recorded.push(record);
    }

    /// Every record not drained yet, oldest first.
    pub(crate) fn take(&self) -> Vec<PeerRecord> {
        mem::take(&mut self.lock().recorded)
    }

    /// Notes that `unplaced` drained records wait for their identity.
    pub(crate) fn set_unplaced(&self, unplaced: usize) {
        self.lock().unplaced = unplaced;
    }

    /// How many records aren't placed yet, drained or not.
    pub(crate) fn unplaced(&self) -> usize {
        let queue = self.lock();
        queue.recorded.len() + queue.unplaced
    }
}

/// One thing a peer did on a header stream.
#[derive(Clone, Debug)]
pub(crate) struct PeerRecord {
    /// Where the record was made, from
    /// [`zakura_rt::sim::next_position`].
    pub(crate) pos: Option<u64>,
    /// The peer's address on the connection, which the node sees as the
    /// remote.
    pub(crate) remote: SocketAddr,
    /// The QUIC stream, or `None` for a record made before any stream.
    pub(crate) quic_stream: Option<u64>,
    pub(crate) body: RecordBody,
}

impl PeerRecord {
    /// The record's row under the identity the peer knows, for the step it
    /// was made in.
    pub(crate) fn emitted(&self) -> Observation {
        let body = match &self.body {
            RecordBody::Frame {
                seq,
                row,
                rule_stream,
                label,
                exhausted,
                exchange,
                payload,
            } => EmittedBody::Frame {
                seq: *seq,
                row: row.clone(),
                rule_stream: *rule_stream,
                label: *label,
                exhausted: *exhausted,
                exchange: *exchange,
                payload: *payload,
            },
            RecordBody::Written { seq } => EmittedBody::Written { seq: *seq },
            RecordBody::Closed => EmittedBody::Closed,
            RecordBody::Received {
                seq,
                message_type,
                payload,
                frame,
                ..
            } => EmittedBody::Received {
                seq: *seq,
                message_type: *message_type,
                flags: frame.flags,
                payload: *payload,
                digest: Sha256::digest(&frame.payload)[..8]
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect(),
            },
            RecordBody::Handled { seq } => EmittedBody::Handled { seq: *seq },
            RecordBody::ConnEnded { reason } => EmittedBody::ConnEnded {
                reason: reason.clone(),
            },
            RecordBody::StreamEnded { direction, end } => EmittedBody::StreamEnded {
                direction: *direction,
                end: end.clone(),
            },
        };
        Observation::PeerEmitted {
            remote: self.remote.to_string(),
            quic_stream: self.quic_stream,
            body,
        }
    }
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
    /// The peer's task handled the received frame `seq`. It resolves to no
    /// row of its own: its `peer-emitted` row is the evidence.
    Handled { seq: u64 },
    /// [`Observation::PeerConnEnded`].
    ConnEnded { reason: ConnEnd },
    /// [`Observation::PeerStreamEnded`].
    StreamEnded {
        direction: Direction,
        end: StreamEnd,
    },
}

/// The identity map as the log has it, and the labels logged so far.
#[derive(Debug, Default)]
pub(crate) struct WireEvidence {
    /// Each registered connection, by its remote address as the log
    /// writes it. A remote gets one handshake outcome at most
    /// ([`WireEvidence::first_outcome`]).
    connections: BTreeMap<String, u64>,
    /// Each admitted stream's kind and version, by connection and QUIC
    /// stream.
    streams: BTreeMap<(u64, u64), StreamId>,
    /// Each admitted session's connection and QUIC stream, by session ID.
    sessions: BTreeMap<u64, (u64, u64)>,
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
    /// Checks the node's outputs and the frames the peers read (M6a-6).
    outputs: OutputLedger,
}

impl WireEvidence {
    /// Reads one row the hub is writing, in key order: updates the identity
    /// map from it, and returns the P3b observations it means.
    pub(crate) fn row(&mut self, obs: &Observation) -> Result<Vec<Observation>, String> {
        if let Observation::Transport {
            connection: Some(connection),
            event,
            ..
        } = obs
        {
            self.sender.node(*connection, event);
            self.outputs.node(*connection, event)?;
        }
        match obs {
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
                self.sessions
                    .insert(*session_id, (*connection, *quic_stream));
            }
            _ => {}
        }
        Ok(self.project(obs)?.into_iter().collect())
    }

    /// Places every record of `pending` the identity map can place now, in
    /// order, each as its observations with the position it was recorded
    /// at. The rest stay in `pending`. An error if the sender adapter can't
    /// label the node's frame.
    pub(crate) fn place(
        &mut self,
        pending: &mut Vec<PeerRecord>,
    ) -> Result<Vec<(u64, Observation)>, String> {
        let mut placed = Vec::new();
        let mut failed = None;
        pending.retain(|record| match self.place_one(record) {
            Some(Ok(obs)) => {
                let pos = record.pos.unwrap_or_default();
                placed.extend(obs.into_iter().map(|obs| (pos, obs)));
                false
            }
            Some(Err(why)) => {
                failed.get_or_insert(why);
                false
            }
            None => true,
        });
        match failed {
            Some(why) => Err(why),
            None => Ok(placed),
        }
    }

    /// Drops each record of `pending` that never will be placed. Its
    /// `peer-emitted` row already holds it, so it keeps only that.
    pub(crate) fn forget_unresolvable(&self, pending: &mut Vec<PeerRecord>) {
        pending.retain(|record| !self.unresolvable(record));
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
    fn place_one(&mut self, record: &PeerRecord) -> Option<Result<Vec<Observation>, String>> {
        if let RecordBody::Handled { .. } = record.body {
            return Some(Ok(Vec::new()));
        }
        let connection = *self.connections.get(&record.remote.to_string())?;
        if let RecordBody::ConnEnded { reason } = &record.body {
            return Some(Ok(vec![Observation::PeerConnEnded {
                connection,
                quic_stream: record.quic_stream,
                reason: reason.clone(),
            }]));
        }
        let quic_stream = record.quic_stream?;
        let stream = *self.streams.get(&(connection, quic_stream))?;
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
            RecordBody::StreamEnded { direction, end } => Observation::PeerStreamEnded {
                connection,
                quic_stream,
                direction,
                end,
            },
            RecordBody::Handled { .. } | RecordBody::ConnEnded { .. } => {
                unreachable!("placed above without a stream")
            }
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
                if let Err(why) = self.outputs.received(connection, quic_stream, seq, &frame) {
                    return Some(Err(why));
                }
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

    /// Whether `record`, still unplaced, never will be: the node refused
    /// its handshake or stream, or tore its connection down before admitting
    /// the stream.
    fn unresolvable(&self, record: &PeerRecord) -> bool {
        match self.connections.get(&record.remote.to_string()) {
            None => self.refused_handshakes.contains(&record.remote.to_string()),
            Some(connection) => {
                self.torn_down.contains(connection)
                    || record.quic_stream.is_some_and(|quic_stream| {
                        self.refused_streams.contains(&(*connection, quic_stream))
                    })
            }
        }
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
                    .map(|(connection, _)| connection)
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

    /// Admitted session `session_id`'s connection, QUIC stream and stream,
    /// as its `Session` row named them (M6a-5a).
    pub(crate) fn session(&self, session_id: u64) -> Option<(u64, u64, StreamId)> {
        let (connection, quic_stream) = *self.sessions.get(&session_id)?;
        let stream = *self.streams.get(&(connection, quic_stream))?;
        Some((connection, quic_stream, stream))
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
pub(crate) fn rules(stream: StreamId) -> Option<&'static [MessageRule]> {
    RegtestMessage::LAYOUT
        .iter()
        .find(|layout| (layout.kind, layout.version) == (stream.kind, stream.version))
        .and_then(|layout| layout.messages)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::peer_addr;

    /// What the hub hands the identity map, in key order.
    enum Item {
        Row(Observation),
        Record(PeerRecord),
    }

    /// Folds `items` through `evidence` as the hub does, with `pending` the
    /// records drained before: each row's projections, then each placement
    /// it allows, then forgets the records that never will be placed.
    /// Returns the derived observations in order, or the first failure.
    fn fold(
        evidence: &mut WireEvidence,
        pending: &mut Vec<PeerRecord>,
        items: Vec<Item>,
    ) -> Result<Vec<Observation>, String> {
        let mut derived = Vec::new();
        for item in items {
            match item {
                Item::Row(obs) => derived.extend(evidence.row(&obs)?),
                Item::Record(record) => pending.push(record),
            }
            derived.extend(evidence.place(pending)?.into_iter().map(|(_, obs)| obs));
        }
        evidence.forget_unresolvable(pending);
        Ok(derived)
    }

    /// A transport event on `connection` from peer 0's first address.
    fn transport(seq: u64, connection: Option<u64>, event: TransportFact) -> Item {
        Item::Row(Observation::Transport {
            instance: 0,
            seq,
            node: "node".to_owned(),
            connection,
            remote: peer_addr(0, 0).to_string(),
            event,
        })
    }

    /// Connection `connection`'s session row for QUIC stream 4.
    fn session(connection: u64, session_id: u64) -> Item {
        Item::Row(Observation::Session {
            connection,
            peer: "peer-0".to_owned(),
            quic_stream: 4,
            kind: RegtestMessage::LAYOUT[0].kind,
            version: RegtestMessage::LAYOUT[0].version,
            session_id,
        })
    }

    /// Peer 0's record of `body` on QUIC stream 4.
    fn record(body: RecordBody) -> PeerRecord {
        PeerRecord {
            pos: Some(1),
            remote: peer_addr(0, 0),
            quic_stream: Some(4),
            body,
        }
    }

    /// Peer 0's `PeerWritten` record for its first frame on QUIC stream 4.
    fn status_record() -> Item {
        Item::Record(record(RecordBody::Written { seq: 0 }))
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
        let received = record(RecordBody::Received {
            seq: 0,
            message_type: frame.message_type,
            payload: 0,
            credit: None,
            frame: frame.clone(),
            at: Duration::ZERO,
        });
        let fingerprint = zakura_network::zakura::frame_fingerprint(&frame);
        let started = TransportFact::FrameWriteStarted {
            quic_stream: 4,
            seq: 0,
            message_type: frame.message_type,
            output: Some(0),
            fingerprint,
        };
        // The frame's output life up to its write (M6a-6).
        let life = [
            TransportFact::OutputAllocated {
                quic_stream: 4,
                output: 0,
                message_type: frame.message_type,
                payload_len: 0,
                expected: fingerprint,
            },
            TransportFact::OutputEnqueued {
                quic_stream: 4,
                output: 0,
                order: 0,
            },
            TransportFact::OutputDequeued {
                quic_stream: 4,
                output: 0,
            },
        ];
        let mut items = vec![
            transport(1, Some(1), TransportFact::Registered),
            session(1, 7),
        ];
        items.extend(life.into_iter().map(|fact| transport(2, Some(1), fact)));
        items.extend([transport(2, Some(1), started), Item::Record(received)]);
        let logged = fold(&mut WireEvidence::default(), &mut Vec::new(), items)
            .expect("the record is placed");
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

    /// A frame a peer read is checked against its output when it's placed,
    /// whether or not the peer's task ever handles it: a read of the node's
    /// write 0 whose bytes differ from output 0's fails the fold, though no
    /// `Handled` record follows (M6a-6).
    ///
    /// This fails if receipts aren't checked against the output ledger, or
    /// only once the peer handles them.
    #[test]
    fn a_receipt_never_handled_is_still_checked() {
        let written = Frame {
            message_type: RegtestMessage::ROWS[0].message_type,
            flags: 0,
            payload: vec![1; 4],
        };
        let read = Frame {
            payload: vec![2; 4],
            ..written.clone()
        };
        let fingerprint = zakura_network::zakura::frame_fingerprint(&written);
        let lived = || {
            let mut items = vec![
                transport(1, Some(1), TransportFact::Registered),
                session(1, 7),
            ];
            items.extend(
                [
                    TransportFact::OutputAllocated {
                        quic_stream: 4,
                        output: 0,
                        message_type: written.message_type,
                        payload_len: 4,
                        expected: fingerprint,
                    },
                    TransportFact::OutputEnqueued {
                        quic_stream: 4,
                        output: 0,
                        order: 0,
                    },
                    TransportFact::OutputDequeued {
                        quic_stream: 4,
                        output: 0,
                    },
                    TransportFact::FrameWriteStarted {
                        quic_stream: 4,
                        seq: 0,
                        message_type: written.message_type,
                        output: Some(0),
                        fingerprint,
                    },
                ]
                .into_iter()
                .map(|fact| transport(2, Some(1), fact)),
            );
            items
        };
        let received = |frame: &Frame| {
            Item::Record(record(RecordBody::Received {
                seq: 0,
                message_type: frame.message_type,
                payload: 4,
                credit: None,
                frame: frame.clone(),
                at: Duration::ZERO,
            }))
        };
        let mut good = lived();
        good.push(received(&written));
        fold(&mut WireEvidence::default(), &mut Vec::new(), good)
            .expect("the frame the peer read is output 0's");
        let mut bad = lived();
        bad.push(received(&read));
        let why = fold(&mut WireEvidence::default(), &mut Vec::new(), bad)
            .expect_err("the frame the peer read isn't output 0's");
        assert!(
            why.starts_with("OutputLedger: a peer read the node's write 0"),
            "{why}"
        );
    }

    /// A record waits while its stream may still be admitted, and is
    /// forgotten once the node refuses the stream or the handshake, or tears
    /// the connection down first. Its `peer-emitted` row already holds it,
    /// so the run goes on.
    #[test]
    fn a_record_that_can_never_be_placed_is_forgotten() {
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
            let mut evidence = WireEvidence::default();
            let mut pending = Vec::new();
            let mut items = vec![status_record()];
            if connection.is_some() {
                items.push(transport(1, connection, TransportFact::Registered));
            }
            fold(&mut evidence, &mut pending, items).expect("the stream may still be admitted");
            assert_eq!(pending.len(), 1, "it waits");
            let placed = fold(
                &mut evidence,
                &mut pending,
                vec![transport(2, connection, ending.clone())],
            )
            .expect("a record that can never be placed doesn't fail the run");
            assert!(placed
                .iter()
                .all(|obs| !matches!(obs, Observation::PeerWritten { .. })));
            assert!(pending.is_empty(), "{ending:?}: it's forgotten");
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
        let mut items: Vec<Item> = (0..4)
            .map(|seq| {
                Item::Record(record(RecordBody::Frame {
                    seq,
                    row: None,
                    rule_stream: stream,
                    label: Label::Conformant,
                    exhausted: false,
                    exchange: None,
                    payload: 0,
                }))
            })
            .collect();
        items.extend([
            transport(1, Some(1), TransportFact::Registered),
            session(1, 7),
        ]);
        let mut evidence = WireEvidence::default();
        let mut pending = Vec::new();
        fold(&mut evidence, &mut pending, items).expect("the evidence is whole");
        let mut judged = Vec::new();
        let mut event = 1;
        let mut feed = |fact: TransportFact| {
            event += 1;
            let derived = fold(
                &mut evidence,
                &mut pending,
                vec![transport(event, Some(1), fact)],
            )
            .expect("the evidence is whole");
            judged.extend(derived.iter().filter_map(|obs| match obs {
                Observation::FrameJudged { seq, .. } => Some(*seq),
                _ => None,
            }));
            judged.clone()
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
        assert!(feed(header(0, 2, 0, 107)).is_empty(), "a Grant waits");
        assert_eq!(feed(read(0)), [0]);
        assert_eq!(feed(header(1, 1, 1, 122)), [0, 1], "a flag bit");
        assert_eq!(feed(header(2, 3, 0, 263)), [0, 1], "a page waits");
        assert_eq!(feed(checked(2)), [0, 1, 2], "with nothing live");
        let opened = TransportFact::Regulated {
            quic_stream: 4,
            transition: TransitionFact::OpenIssued {
                subscription: 1,
                objects: 1,
                bytes: 263,
            },
        };
        feed(opened);
        feed(header(3, 4, 0, 41));
        assert_eq!(feed(checked(3)), [0, 1, 2], "an ending for a live one");
        assert_eq!(feed(read(3)), [0, 1, 2, 3]);
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
            let mut evidence = WireEvidence::default();
            let mut pending = Vec::new();
            let first_outcome = match first {
                Some(_) => TransportFact::Registered,
                None => refused(),
            };
            let mut items = vec![transport(1, first, first_outcome)];
            // A record from a refused handshake can never be placed, so only
            // a registered first connection leaves one waiting.
            if first.is_some() {
                items.insert(0, status_record());
            }
            fold(&mut evidence, &mut pending, items).expect("a remote's first outcome is fine");
            let waiting = pending.len();
            let mut items = vec![transport(2, second, outcome.clone())];
            if let Some(connection) = second {
                items.push(session(connection, 1));
            }
            let error = fold(&mut evidence, &mut pending, items)
                .expect_err("the second outcome fails the run");
            assert!(error.starts_with("AliasedRemote"), "{outcome:?}: {error}");
            assert_eq!(pending.len(), waiting, "nothing was placed");
        }
    }
}
