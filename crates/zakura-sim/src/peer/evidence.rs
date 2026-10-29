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
//! It also projects the node's evidence onto P3b's observations:
//! `FrameJudged` at the node's `frame-read` of a frame labelled
//! `Conformant`, `ConnectionClosed` for a close decided with a cause, and
//! `Misbehavior` for the connection of the session a reactor misbehavior
//! names. Judging any other label needs the reader stage its label decides
//! at, which M5-E1 adds, so here a read of a frame labelled otherwise is a
//! harness failure, and so is a misbehavior with no origin session.

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, VecDeque},
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
        records
            .borrow_mut()
            .retain(|record| match self.place(record) {
                Some(obs) => {
                    placed.push(obs);
                    false
                }
                None => true,
            });
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
