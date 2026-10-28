//! What scripted peers record, placed by the identity map, and what the
//! node's evidence means for P3b's close checks (M5-D).
//!
//! A peer's task records each frame it sends and reads with its own address
//! and the QUIC stream, which both ends share. [`WireEvidence`] holds each
//! record until the transport tap has logged the connection's `registered`
//! event and the stream's `Session` row, then logs it as a `PeerFrame`,
//! `PeerWritten` or `PeerReceived` with that connection (M5-Q4). Records of
//! one stream keep their order.
//!
//! It also projects the node's evidence onto P3b's observations:
//! `FrameJudged` at the node's `frame-read` of a frame labelled
//! `Conformant`, `ConnectionClosed` for a close decided with a cause, and
//! `Misbehavior` for the connection of the session a reactor misbehavior
//! names. Judging any other label needs the reader stage its label decides
//! at, which M5-E1 adds, so here a read of a frame labelled otherwise is a
//! harness failure, and so is a misbehavior with no origin session.

use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    net::SocketAddr,
    rc::Rc,
};

use crate::{
    model::Label,
    observe::{PeerCredit, ReactorFact, StreamId, TransportFact},
    Observation, ObservationLog,
};

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
    /// [`Observation::PeerReceived`].
    Received {
        seq: u64,
        message_type: u16,
        payload: u64,
        credit: Option<PeerCredit>,
    },
}

/// The identity map as the log has it, and the labels logged so far.
#[derive(Debug, Default)]
pub(crate) struct WireEvidence {
    /// How many log entries it has read.
    seen: usize,
    /// Each registered connection, by its remote address as the log
    /// writes it.
    connections: BTreeMap<String, u64>,
    /// Each admitted stream's kind and version, by connection and QUIC
    /// stream.
    streams: BTreeMap<(u64, u64), StreamId>,
    /// Each admitted session's connection, by session ID.
    sessions: BTreeMap<u64, u64>,
    /// Each logged `PeerFrame`'s label, by connection, QUIC stream and
    /// `seq`.
    labels: BTreeMap<(u64, u64, u64), Label>,
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
            match &entry.obs {
                Observation::Transport {
                    connection: Some(connection),
                    remote,
                    event: TransportFact::Registered,
                    ..
                } => {
                    self.connections.insert(remote.clone(), *connection);
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
        records
            .borrow_mut()
            .retain(|record| match self.place(record) {
                Some(obs) => {
                    placed.push(obs);
                    false
                }
                None => true,
            });
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

    /// `record` as its observation, once its stream is admitted.
    fn place(&mut self, record: &PeerRecord) -> Option<Observation> {
        let connection = *self.connections.get(&record.remote.to_string())?;
        let stream = *self.streams.get(&(connection, record.quic_stream))?;
        let quic_stream = record.quic_stream;
        Some(match record.body.clone() {
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
            RecordBody::Received {
                seq,
                message_type,
                payload,
                credit,
            } => Observation::PeerReceived {
                connection,
                quic_stream,
                seq,
                message_type,
                payload,
                credit,
            },
        })
    }

    /// The P3b observation `obs` means, if any.
    fn project(&self, obs: &Observation) -> Result<Option<Observation>, String> {
        Ok(match obs {
            Observation::Transport {
                connection: Some(connection),
                event: TransportFact::FrameRead { quic_stream, seq },
                ..
            } => {
                let stream = self
                    .streams
                    .get(&(*connection, *quic_stream))
                    .ok_or_else(|| {
                        format!(
                            "AmbiguousIdentity: connection {connection} read stream {quic_stream} \
                         before its session row"
                        )
                    })?;
                match self.labels.get(&(*connection, *quic_stream, *seq)) {
                    Some(Label::Conformant) => Some(Observation::FrameJudged {
                        connection: *connection,
                        stream: *stream,
                        seq: *seq,
                    }),
                    Some(label) => {
                        return Err(format!(
                            "UnjudgedLabel: connection {connection} read frame {seq} on stream \
                             {quic_stream}, labelled {label}, whose judging stage M5-E1 decides"
                        ))
                    }
                    None => {
                        return Err(format!(
                            "UnlabelledFrame: connection {connection} read frame {seq} on stream \
                             {quic_stream} with no PeerFrame"
                        ))
                    }
                }
            }
            Observation::Transport {
                connection: Some(connection),
                event: TransportFact::CloseDecided { cause: Some(cause) },
                ..
            } => Some(Observation::ConnectionClosed {
                connection: *connection,
                cause: cause.clone(),
            }),
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
}
