//! What scripted peers record, placed by the identity map, and what the
//! node's evidence means for P3b's close checks (M5-D).
//!
//! A peer's task records each frame it sends and reads with its own
//! [`Remote`], its address and the connection's shared ID, and the QUIC
//! stream, which both ends share. [`WireEvidence`] holds each record until
//! the transport tap has logged the `registered` event of the connection
//! with that shared ID and the stream's `Session` row, then logs it as a
//! `PeerFrame`, `PeerWritten` or `PeerReceived` with that connection
//! (M5-Q4). Records of one stream keep their order. A record whose stream
//! can no longer be admitted, because the node refused it or its handshake,
//! or tore its connection down, fails the run, so a peer's frames can't drop
//! out of the log unseen.
//!
//! Both ends derive the shared ID from the connection's TLS handshake
//! (`zakura_quic::Conn::shared_id`), so neither end counts connections. A
//! node that redials a listening peer reaches it at the same address each
//! time (disc-2.2), with QUIC stream IDs counting from 0 again, and a redial
//! whose QUIC handshake completes at the node but not at the peer is one
//! the node refuses and the peer never serves. The log doesn't hold the
//! shared ID: the tap hands over the ID of each `registered` and
//! `handshake-refused` event it logs in [`SharedIds`].
//!
//! Each `PeerReceived` on a header stream is followed by the frame's
//! `NodeFrame`, which the [`SenderAdapter`] labels under the sender's rules
//! against the peer updates the node had applied when it started writing
//! the frame (M5-E2).
//!
//! Each `PeerReceived` on a discovery stream is followed by the frame's
//! `NodeFrame`, which the [`DiscoverySender`] labels under the sender's
//! rules against the peer frames the node had read when it started writing
//! the frame.
//!
//! A discovery stream's frames are judged at the stage their label names
//! ([`super::stage::discovery_stage`]), from the tap's `HeaderRead` and
//! `FrameRead` (disc-3.1). The node's reader may read requests well before
//! its service admits them, and the service releases a commitment before it
//! starts writing the answer, so a request the peer's models labelled
//! `OverMargin` settles against the node's own count: its `PeerFrame` waits
//! for the serving's `CommitmentCounted` that names it. The commitments of
//! its row the serving had counted then, less those it had released, are
//! the node's count. Past twice the row's `max_in_flight` the request is
//! judged `OverMargin`, and within it the request is relabelled and judged
//! `Conformant`. The serving counts a request before it checks the margin,
//! so a close before the count was never the request's margin close. At a
//! fault close before the serving counts a request the node read, the
//! requests of its row the node had read up to it, less the commitments
//! released, bound the count the serving would have reached. Within the
//! margin, that bound proves the request `Conformant`, and it is judged
//! before the close. Past it, and at any other close, the request stays
//! unjudged. A request the node never reads keeps the peer's label when its
//! connection is torn down.
//!
//! It also projects the node's evidence onto P3b's observations:
//! `FrameJudged` at the reader stage that decides the frame (M5-E1, see
//! [`super::stage`]), or for a page carrying a header the peer's fixture
//! made invalid, once the node's storage has run the preparation of that
//! header for the page's session (hdr-4 ask 4, option (b)), so a page the
//! node never prepares is never judged, `ConnectionClosed` for a close decided with a cause,
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

use zakura_chain::block;
use zakura_network::zakura::{
    v9::RegtestMessage, wire_codec::WireMessage, DiscoveryMessage, Frame, MessageRole, MessageRule,
};
use zakura_storage::header::Outcome;

use super::{
    adapter::SenderAdapter,
    discovery::DiscoverySender,
    stage::{discovery_stage, header_stage, NodeView, Stage},
};
use crate::{
    model::{is_fault_cause, FrameHeader, FrameModel, Label, ReaderEnd},
    observe::{
        PeerCredit, ReactorFact, StorageFact, StorageRequest, StreamId, TransitionFact,
        TransportFact,
    },
    storage::{hash, hex, HEADER_SERVICE},
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

/// The ID both ends of a connection derive from its TLS handshake.
pub(crate) type SharedId = [u8; 16];

/// The shared ID of each connection whose `registered` or
/// `handshake-refused` event the transport tap logged, by the event's
/// number. The tap adds each ID as it logs the event, and [`WireEvidence`]
/// takes it as it reads the event.
pub(crate) type SharedIds = Rc<RefCell<BTreeMap<u64, SharedId>>>;

/// The peer's end of one connection, as the node sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Remote {
    /// The peer's address, which the node sees as the remote.
    pub(crate) addr: SocketAddr,
    /// The connection's shared ID.
    pub(crate) shared_id: SharedId,
}

/// One thing a peer did on a header or discovery stream.
#[derive(Clone, Debug)]
pub(crate) struct PeerRecord {
    /// The peer's end of the connection.
    pub(crate) remote: Remote,
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
        /// The header the peer's fixture made invalid, if this page carries
        /// it. The node judges such a page when it prepares that header,
        /// not when it reads it. Not logged.
        prepares: Option<InvalidPage>,
    },
    /// [`Observation::PeerWritten`].
    Written { seq: u64 },
    /// [`Observation::PeerClosed`].
    Closed,
    /// [`Observation::ActRan`].
    ActRan { act: &'static str },
    /// [`Observation::ActSkipped`].
    ActSkipped { act: &'static str },
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

/// A page carrying a header the peer's fixture made invalid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct InvalidPage {
    /// The header.
    pub(crate) header: block::Hash,
    /// The page's subscription. The node opens each subscription under its
    /// request's ID, so a preparation's owner names it.
    pub(crate) subscription: u64,
}

/// The identity map as the log has it, and the labels logged so far.
#[derive(Debug, Default)]
pub(crate) struct WireEvidence {
    /// How many log entries it has read.
    seen: usize,
    /// The shared IDs the transport tap hands over.
    shared_ids: SharedIds,
    /// Each registered connection, by its remote address as the log writes
    /// it and its shared ID.
    connections: BTreeMap<(String, SharedId), u64>,
    /// Each admitted stream's kind and version, by connection and QUIC
    /// stream.
    streams: BTreeMap<(u64, u64), StreamId>,
    /// Each admitted session's connection, by session ID.
    sessions: BTreeMap<u64, u64>,
    /// Each admitted session's QUIC stream, by session ID.
    session_streams: BTreeMap<u64, u64>,
    /// Each logged `PeerFrame`'s label, by connection, QUIC stream and
    /// `seq`.
    labels: BTreeMap<(u64, u64, u64), Label>,
    /// The remote addresses and shared IDs of the connections whose
    /// handshake the node refused.
    refused_handshakes: BTreeSet<(String, SharedId)>,
    /// Streams the node refused, by connection and QUIC stream.
    refused_streams: BTreeSet<(u64, u64)>,
    /// Connections the node tore down.
    torn_down: BTreeSet<u64>,
    /// Each header session's reader ledger, by connection and QUIC stream.
    views: BTreeMap<(u64, u64), NodeView>,
    /// Frames whose header the reader has, not judged yet.
    waiting: BTreeMap<FrameKey, Waiting>,
    /// Each placed page that carries an invalid header.
    invalid: BTreeMap<FrameKey, InvalidPage>,
    /// Pages carrying an invalid header that the reader has decided,
    /// waiting for its preparation.
    unprepared: BTreeMap<FrameKey, InvalidPage>,
    /// Each preparation the node's storage was asked for: its target's hash
    /// as the log writes it, and its work's session and request, by
    /// operation ID.
    prepares: BTreeMap<u64, (String, u64, u64)>,
    /// Judgements a preparation made, for the entry being projected.
    late: Vec<Observation>,
    /// Labels the node's frames.
    sender: SenderAdapter,
    /// The discovery stream, whose frames are judged at their label's stage.
    discovery: Option<StreamId>,
    /// Labels the node's frames on the discovery stream.
    discovery_sender: Option<DiscoverySender>,
    /// Each discovery request the peer's models labelled `OverMargin`, its
    /// `PeerFrame` waiting for the serving to count it.
    unsettled: BTreeMap<FrameKey, Unsettled>,
    /// Each discovery request row's requests the node read and commitments
    /// its serving counted and released, by connection, QUIC stream and
    /// request type.
    margins: BTreeMap<(u64, u64, u16), Margin>,
}

/// A discovery request the peer's models labelled `OverMargin`, waiting for
/// the node's count to settle it.
#[derive(Clone, Debug)]
struct Unsettled {
    /// Its `PeerFrame`.
    frame: Observation,
    /// Once the node read it, its request type and its position among that
    /// row's requests the node read, from 1.
    read: Option<(u16, u32)>,
}

/// One discovery request row's requests on one stream, as the node's reader
/// and serving saw them.
#[derive(Clone, Copy, Debug, Default)]
struct Margin {
    /// Requests the node read.
    read: u32,
    /// Commitments the serving counted.
    counted: u32,
    /// Commitments the serving released.
    released: u32,
}

impl WireEvidence {
    /// Evidence that reads each connection's shared ID from `shared_ids`.
    pub(crate) fn new(shared_ids: SharedIds) -> Self {
        WireEvidence {
            shared_ids,
            ..WireEvidence::default()
        }
    }

    /// This evidence, also logging the peers' frames on `discovery`'s
    /// stream, which it judges none of.
    pub(crate) fn with_discovery(self, discovery: StreamId) -> Self {
        WireEvidence {
            discovery: Some(discovery),
            discovery_sender: Some(DiscoverySender::new(discovery)),
            ..self
        }
    }

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
                let on_discovery = self.on_discovery(*connection, event);
                match self.discovery_sender.as_mut() {
                    Some(sender) if on_discovery => sender.node(*connection, event),
                    _ => self.sender.node(*connection, event),
                }
            }
            match &entry.obs {
                Observation::Transport {
                    seq,
                    connection: Some(connection),
                    remote,
                    event: TransportFact::Registered,
                    ..
                } => {
                    let shared_id = self.shared_id(*seq)?;
                    self.connections
                        .insert((remote.clone(), shared_id), *connection);
                }
                Observation::Transport {
                    seq,
                    connection: None,
                    remote,
                    event: TransportFact::HandshakeRefused { .. },
                    ..
                } => {
                    let shared_id = self.shared_id(*seq)?;
                    self.refused_handshakes.insert((remote.clone(), shared_id));
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
                    self.session_streams.insert(*session_id, *quic_stream);
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
            projected.append(&mut self.late);
        }
        for obs in placed.into_iter().chain(projected) {
            log.push(obs);
        }
        self.seen = log.entries().len();
        Ok(())
    }

    /// The shared ID of the connection whose handshake outcome the tap
    /// logged as event `seq`, which the tap handed over as it logged it.
    fn shared_id(&mut self, seq: u64) -> Result<SharedId, String> {
        self.shared_ids.borrow_mut().remove(&seq).ok_or_else(|| {
            format!("AmbiguousIdentity: transport event {seq} came with no shared ID")
        })
    }

    /// Whether `event` names a QUIC stream of `connection` admitted as the
    /// discovery stream.
    fn on_discovery(&self, connection: u64, event: &TransportFact) -> bool {
        let quic_stream = match event {
            TransportFact::FrameWriteStarted { quic_stream, .. }
            | TransportFact::Regulated { quic_stream, .. }
            | TransportFact::HeaderRead { quic_stream, .. }
            | TransportFact::FrameRead { quic_stream, .. } => *quic_stream,
            _ => return false,
        };
        self.is_discovery(connection, quic_stream)
    }

    /// The key of `remote` in [`WireEvidence::connections`].
    fn key(remote: Remote) -> (String, SharedId) {
        (remote.addr.to_string(), remote.shared_id)
    }

    /// `record` as its observations, once its stream is admitted: one, or
    /// a `PeerReceived` and its `NodeFrame`. An error if the sender adapter
    /// can't label the node's frame.
    fn place(&mut self, record: &PeerRecord) -> Option<Result<Vec<Observation>, String>> {
        let connection = *self.connections.get(&Self::key(record.remote))?;
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
                prepares,
            } => {
                let key = (connection, quic_stream, seq);
                self.labels.insert(key, label);
                if let Some(page) = prepares {
                    self.invalid.insert(key, page);
                }
                let frame = Observation::PeerFrame {
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
                };
                if Some(stream) == self.discovery && label == Label::OverMargin {
                    self.unsettled.insert(key, Unsettled { frame, read: None });
                    return Some(Ok(Vec::new()));
                }
                frame
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
            RecordBody::ActRan { act } => Observation::ActRan {
                connection,
                quic_stream,
                act: act.to_owned(),
            },
            RecordBody::ActSkipped { act } => Observation::ActSkipped {
                connection,
                quic_stream,
                act: act.to_owned(),
            },
            RecordBody::Received {
                seq,
                message_type,
                payload,
                credit,
                frame,
                at,
            } => {
                let received = Observation::PeerReceived {
                    connection,
                    quic_stream,
                    seq,
                    message_type,
                    payload,
                    credit,
                };
                let sent = match self.discovery_sender.as_mut() {
                    Some(sender) if Some(stream) == self.discovery => {
                        sender.received(connection, quic_stream, seq, &frame, at)
                    }
                    _ => self
                        .sender
                        .received(connection, quic_stream, seq, &frame, at),
                };
                let node_frame = match sent {
                    Ok(node_frame) => node_frame,
                    Err(why) => return Some(Err(why)),
                };
                return Some(Ok(vec![received, node_frame]));
            }
        }]))
    }

    /// Why `record`, still unplaced, never will be, if the node refused its
    /// handshake or stream or tore its connection down.
    fn lost(&self, record: &PeerRecord) -> Option<String> {
        let key = Self::key(record.remote);
        let remote = &key.0;
        let gone = match self.connections.get(&key) {
            None => self.refused_handshakes.contains(&key),
            Some(connection) => {
                self.torn_down.contains(connection)
                    || self
                        .refused_streams
                        .contains(&(*connection, record.quic_stream))
            }
        };
        gone.then(|| {
            format!(
                "UnplacedRecord: {remote}'s record on stream {} of connection {} can't get an \
                 identity, since the node refused or closed it before admitting the stream: {:?}",
                record.quic_stream,
                hex(&record.remote.shared_id),
                record.body
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
                        Some(Stage::Frame) if self.is_discovery(*connection, *quic_stream) => {
                            let waiting = self.waiting.remove(&key).expect("it waits, just read");
                            if self.discovery_read(key, waiting.row) {
                                self.judged(key)?
                            } else {
                                None
                            }
                        }
                        Some(Stage::Frame) => {
                            self.waiting.remove(&key);
                            self.judged(key)?
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
                    let closed = Observation::ConnectionClosed {
                        connection: *connection,
                        cause: cause.clone(),
                    };
                    let read: Vec<FrameKey> = self
                        .unsettled
                        .range((*connection, 0, 0)..=(*connection, u64::MAX, u64::MAX))
                        .filter(|(_, unsettled)| unsettled.read.is_some())
                        .map(|(key, _)| *key)
                        .collect();
                    let read: Vec<(FrameKey, Unsettled)> = read
                        .into_iter()
                        .map(|key| (key, self.unsettled.remove(&key).expect("just listed")))
                        .collect();
                    if !is_fault_cause(cause) {
                        self.late
                            .extend(read.into_iter().map(|(_, unsettled)| unsettled.frame));
                        return Ok(Some(closed));
                    }
                    // The node closed before its serving counted these
                    // requests. The serving counts a request before it
                    // checks the margin, so an uncounted request can't be
                    // what justified the close. Had the serving counted the
                    // requests in the order the node read them, a request's
                    // count would have been at most its position less the
                    // commitments released. Within the margin that bound
                    // proves the request `Conformant`. Past it, nothing
                    // proves the request was over the margin, so it stays
                    // unjudged.
                    let mut judged = Vec::new();
                    let mut unjudged = Vec::new();
                    for (key, unsettled) in read {
                        let (message_type, position) =
                            unsettled.read.expect("only read requests were listed");
                        let released = self
                            .margins
                            .get(&(key.0, key.1, message_type))
                            .map_or(0, |margin| margin.released);
                        let bound = position.saturating_sub(released);
                        if bound <= margin(message_type)? {
                            judged.extend(self.settle(
                                key,
                                unsettled.frame,
                                message_type,
                                bound,
                            )?);
                        } else {
                            unjudged.push(unsettled.frame);
                        }
                    }
                    judged.push(closed);
                    judged.extend(unjudged);
                    let mut judged = judged.into_iter();
                    let first = judged.next();
                    self.late.extend(judged);
                    first
                }
                TransportFact::TornDown => {
                    self.late.extend(
                        take_connection(&mut self.unsettled, *connection)
                            .into_values()
                            .map(|unsettled| unsettled.frame),
                    );
                    None
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
            Observation::Storage {
                service,
                instance: 0,
                fact,
                ..
            } if service == HEADER_SERVICE => {
                self.prepared(fact)?;
                None
            }
            _ => None,
        })
    }

    /// Follows the node's preparations. Once its storage has run one, a
    /// completion with the engine's outcome whatever it returned, every
    /// decided page of that work's request, on its session's connection and
    /// QUIC stream and its subscription, carrying the target, the header the
    /// fixture made invalid, is judged. Each judgement is appended as it
    /// is found, in frame order.
    fn prepared(&mut self, fact: &StorageFact) -> Result<(), String> {
        match fact {
            StorageFact::Submitted {
                id,
                request: StorageRequest::PrepareHeaderTarget { owner, target, .. },
                ..
            } => {
                self.prepares.insert(
                    *id,
                    (target.hash.clone(), owner.session_id, owner.request_id),
                );
            }
            StorageFact::Completed { id, outcome, .. } => {
                let Some((target, session_id, request_id)) = self.prepares.remove(id) else {
                    return Ok(());
                };
                if outcome != Outcome::Engine.name() {
                    return Ok(());
                }
                let (Some(connection), Some(stream)) = (
                    self.sessions.get(&session_id).copied(),
                    self.session_streams.get(&session_id).copied(),
                ) else {
                    return Ok(());
                };
                // The session's own stream and the request's own
                // subscription: a header stream reopened on the same
                // connection is another session, and an earlier request's
                // page carrying the same header another request, whose pages
                // their own preparations judge.
                let judged: Vec<FrameKey> = self
                    .unprepared
                    .iter()
                    .filter(|((on, quic, _), page)| {
                        (*on, *quic) == (connection, stream)
                            && page.subscription == request_id
                            && hash(page.header) == target
                    })
                    .map(|(key, _)| *key)
                    .collect();
                for key in judged {
                    self.unprepared.remove(&key);
                    let judged = self.frame_judged(key)?;
                    self.late.push(judged);
                }
            }
            _ => {}
        }
        Ok(())
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
        if Some(stream) == self.discovery {
            let stage = discovery_stage(self.labels[&key]);
            if stage == Stage::Header {
                return self.judged(key);
            }
            self.waiting.insert(
                key,
                Waiting {
                    stage,
                    row: discovery_row(header.message_type),
                    payload_len: header.payload_len,
                },
            );
            return Ok(None);
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
            return self.judged(key);
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
        match *transition {
            TransitionFact::CommitmentCounted { seq, message_type } => {
                return self.counted(connection, quic_stream, seq, message_type);
            }
            TransitionFact::CommitmentReleased { message_type } => {
                self.margins
                    .entry((connection, quic_stream, message_type))
                    .or_default()
                    .released += 1;
                return Ok(None);
            }
            _ => {}
        }
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
            return self.judged(key);
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

    /// Whether `connection`'s `quic_stream` is an admitted discovery stream.
    fn is_discovery(&self, connection: u64, quic_stream: u64) -> bool {
        self.discovery.is_some()
            && self.streams.get(&(connection, quic_stream)).copied() == self.discovery
    }

    /// The node read discovery frame `key` of `row`. Counts a request, and
    /// returns whether the frame is judged now, which a request still
    /// waiting for the serving's count isn't.
    fn discovery_read(&mut self, key: FrameKey, row: Option<&'static MessageRule>) -> bool {
        let Some(row) = row else {
            return true;
        };
        if !matches!(row.role, MessageRole::Request { .. }) {
            return true;
        }
        let margin = self
            .margins
            .entry((key.0, key.1, row.message_type))
            .or_default();
        margin.read += 1;
        let Some(unsettled) = self.unsettled.get_mut(&key) else {
            return true;
        };
        unsettled.read = Some((row.message_type, margin.read));
        false
    }

    /// The serving of `connection`'s `quic_stream` counted the commitment of
    /// request `seq` of type `message_type`. Settles the request if it waits
    /// for that count.
    fn counted(
        &mut self,
        connection: u64,
        quic_stream: u64,
        seq: Option<u64>,
        message_type: u16,
    ) -> Result<Option<Observation>, String> {
        let margin = self
            .margins
            .entry((connection, quic_stream, message_type))
            .or_default();
        margin.counted += 1;
        let open = margin.counted.saturating_sub(margin.released);
        let seq = seq.ok_or_else(|| {
            format!(
                "UntaggedCommitment: connection {connection} counted a commitment on stream \
                 {quic_stream} for no frame it read"
            )
        })?;
        let key = (connection, quic_stream, seq);
        let Some(unsettled) = self.unsettled.remove(&key) else {
            return Ok(None);
        };
        if unsettled.read.is_none() {
            return Err(format!(
                "CommitmentBeforeRead: connection {connection} counted request {seq} on \
                 stream {quic_stream} before its frame-read"
            ));
        }
        let settled = self.settle(key, unsettled.frame, message_type, open)?;
        let mut settled = settled.into_iter();
        let first = settled.next();
        self.late.extend(settled);
        Ok(first)
    }

    /// The `PeerFrame` of request `key` of type `message_type`, `frame`, and
    /// its `FrameJudged`, with `open` the node's commitments of its row,
    /// its own included, or a bound on them. Past twice the row's
    /// `max_in_flight` it keeps its
    /// `OverMargin` label, and within it the request is `Conformant`.
    fn settle(
        &mut self,
        key: FrameKey,
        mut frame: Observation,
        message_type: u16,
        open: u32,
    ) -> Result<Vec<Observation>, String> {
        if open <= margin(message_type)? {
            if let Observation::PeerFrame { label, .. } = &mut frame {
                *label = Label::Conformant;
            }
            self.labels.insert(key, Label::Conformant);
        }
        let mut settled = vec![frame];
        settled.extend(self.judged(key)?);
        Ok(settled)
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

    /// `FrameJudged` for frame `key` now that the reader decided it, or
    /// `None` for a page carrying an invalid header, which waits for that
    /// header's preparation ([`WireEvidence::prepared`]).
    fn judged(&mut self, key: FrameKey) -> Result<Option<Observation>, String> {
        if let Some(page) = self.invalid.get(&key) {
            self.unprepared.insert(key, *page);
            return Ok(None);
        }
        self.frame_judged(key).map(Some)
    }

    /// `FrameJudged` for frame `key`.
    fn frame_judged(
        &self,
        (connection, quic_stream, seq): FrameKey,
    ) -> Result<Observation, String> {
        Ok(Observation::FrameJudged {
            connection,
            stream: self.stream(connection, quic_stream)?,
            quic_stream: Some(quic_stream),
            seq,
        })
    }
}

/// Removes and returns `connection`'s entries of `frames`.
fn take_connection<T>(
    frames: &mut BTreeMap<FrameKey, T>,
    connection: u64,
) -> BTreeMap<FrameKey, T> {
    let keys: Vec<FrameKey> = frames
        .range((connection, 0, 0)..=(connection, u64::MAX, u64::MAX))
        .map(|(key, _)| *key)
        .collect();
    keys.into_iter()
        .map(|key| (key, frames.remove(&key).expect("just listed")))
        .collect()
}

/// The margin of discovery request row `message_type`: twice its
/// `max_in_flight`, the most commitments the node may hold open.
fn margin(message_type: u16) -> Result<u32, String> {
    match discovery_row(message_type).map(|row| row.role) {
        Some(MessageRole::Request { max_in_flight, .. }) => Ok(max_in_flight.saturating_mul(2)),
        _ => Err(format!(
            "UntabledCommitment: a commitment names {message_type}, no discovery request row"
        )),
    }
}

/// Discovery's row for `message_type`, if it has one.
fn discovery_row(message_type: u16) -> Option<&'static MessageRule> {
    FrameModel::new(
        <DiscoveryMessage as WireMessage>::RULES,
        ReaderEnd::Persistent,
    )
    .row(message_type)
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

    /// Peer 0's end of its connection whose shared ID is `[n; 16]`.
    fn remote(n: u8) -> Remote {
        Remote {
            addr: peer_addr(0, 0),
            shared_id: [n; 16],
        }
    }

    /// Evidence whose transport tap handed over the shared ID `[n; 16]` of
    /// event `seq`, for each `(seq, n)` in `ids`.
    fn with_ids(ids: &[(u64, u8)]) -> WireEvidence {
        let ids = ids.iter().map(|&(seq, n)| (seq, [n; 16])).collect();
        WireEvidence::new(Rc::new(RefCell::new(ids)))
    }

    /// Peer 0's `PeerFrame` record for its first frame on QUIC stream 4.
    fn status_record() -> PeerRecords {
        Rc::new(RefCell::new(VecDeque::from([PeerRecord {
            remote: remote(0),
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
            remote: remote(0),
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
        with_ids(&[(1, 0)])
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
            let mut evidence = with_ids(&[(1, 0), (2, 0)]);
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
            remote: remote(0),
            quic_stream: 4,
            body: RecordBody::Frame {
                seq,
                row: None,
                rule_stream: stream,
                label: Label::Conformant,
                exhausted: false,
                exchange: None,
                payload: 0,
                prepares: None,
            },
        }));
        let mut evidence = with_ids(&[(1, 0)]);
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

    /// A redialed listening peer reaches the node at one address, so only
    /// the shared ID tells its connections apart. A record still waiting
    /// on the first connection's stream 4 stays off the second's stream 4,
    /// and a record of the second is placed on it. After a refused first
    /// handshake, the second connection's record is placed, and the
    /// first's can never be.
    #[test]
    fn each_connection_of_one_remote_has_its_own_shared_id() {
        let refused = || TransportFact::HandshakeRefused {
            reason: "Zakura control nonce mismatch".to_owned(),
        };
        let session = |connection| Observation::Session {
            connection,
            quic_stream: 4,
            kind: 5,
            version: 9,
            session_id: connection,
            peer: "peer-0".to_owned(),
        };
        let written = |n, seq| PeerRecord {
            remote: remote(n),
            quic_stream: 4,
            body: RecordBody::Written { seq },
        };
        let placed = |log: &ObservationLog| -> Vec<(u64, u64)> {
            log.entries()
                .iter()
                .filter_map(|entry| match entry.obs {
                    Observation::PeerWritten {
                        connection, seq, ..
                    } => Some((connection, seq)),
                    _ => None,
                })
                .collect()
        };

        let records = PeerRecords::default();
        records.borrow_mut().push_back(written(0, 0));
        let mut evidence = with_ids(&[(1, 0), (2, 1)]);
        let mut log = ObservationLog::default();
        log.push(transport(1, Some(1), TransportFact::Registered));
        evidence.observe(&records, &mut log).unwrap();
        records.borrow_mut().push_back(written(1, 7));
        log.push(transport(2, Some(2), TransportFact::Registered));
        log.push(session(2));
        evidence.observe(&records, &mut log).unwrap();
        assert_eq!(placed(&log), [(2, 7)], "only the second's record");
        assert_eq!(records.borrow().len(), 1, "the first's record waits");
        log.push(session(1));
        evidence.observe(&records, &mut log).unwrap();
        assert_eq!(placed(&log), [(2, 7), (1, 0)]);

        let records = PeerRecords::default();
        records.borrow_mut().push_back(written(1, 3));
        let mut evidence = with_ids(&[(1, 0), (2, 1)]);
        let mut log = ObservationLog::default();
        log.push(transport(1, None, refused()));
        log.push(transport(2, Some(2), TransportFact::Registered));
        log.push(session(2));
        evidence.observe(&records, &mut log).unwrap();
        assert_eq!(placed(&log), [(2, 3)]);
        records.borrow_mut().push_back(written(0, 0));
        let error = evidence
            .observe(&records, &mut log)
            .expect_err("the refused connection's record can never be placed");
        assert!(error.starts_with("UnplacedRecord"), "{error}");
    }

    /// The node's handshake outcomes from one address place no record by
    /// their count. A connection cancelled during registration reports
    /// `Negotiated` and only `CloseDecided` if its registration is refused,
    /// or `CloseDecided` and then `Registered`. A redial whose QUIC
    /// handshake completed at the node alone is refused at the node and
    /// never served by the peer, so the peer records nothing of it.
    /// Each connection's records are still placed on it.
    #[test]
    fn handshake_outcomes_place_no_record_by_their_count() {
        let negotiated = |seq| {
            transport(
                seq,
                None,
                TransportFact::Negotiated {
                    local: 0,
                    offered: 0,
                    negotiated: 0,
                },
            )
        };
        let cancelled = |seq| transport(seq, None, TransportFact::CloseDecided { cause: None });
        let refused = |seq| {
            transport(
                seq,
                None,
                TransportFact::HandshakeRefused {
                    reason: "Zakura control handshake timed out".to_owned(),
                },
            )
        };
        let session = |connection| Observation::Session {
            connection,
            quic_stream: 4,
            kind: 5,
            version: 9,
            session_id: connection,
            peer: "peer-0".to_owned(),
        };
        let written = |n, seq| PeerRecord {
            remote: remote(n),
            quic_stream: 4,
            body: RecordBody::Written { seq },
        };
        let placed = |log: &ObservationLog| -> Vec<(u64, u64)> {
            log.entries()
                .iter()
                .filter_map(|entry| match entry.obs {
                    Observation::PeerWritten {
                        connection, seq, ..
                    } => Some((connection, seq)),
                    _ => None,
                })
                .collect()
        };

        let records = PeerRecords::default();
        let mut evidence = with_ids(&[(2, 0), (7, 2), (9, 3), (12, 4)]);
        let mut log = ObservationLog::default();
        // Shared ID 0 registers as connection 1.
        log.push(negotiated(1));
        log.push(transport(2, Some(1), TransportFact::Registered));
        log.push(transport(3, Some(1), TransportFact::TornDown));
        // Shared ID 1 is cancelled, and its registration refused.
        log.push(negotiated(4));
        log.push(cancelled(5));
        // Shared ID 2 registers as connection 2.
        log.push(negotiated(6));
        log.push(transport(7, Some(2), TransportFact::Registered));
        log.push(session(2));
        records.borrow_mut().push_back(written(2, 5));
        evidence.observe(&records, &mut log).unwrap();
        assert_eq!(placed(&log), [(2, 5)], "shared ID 2 is connection 2");
        // Shared ID 3's QUIC handshake completed at the node alone, so the
        // node refuses it and the peer records nothing.
        log.push(transport(8, Some(2), TransportFact::TornDown));
        log.push(refused(9));
        // Shared ID 4 is cancelled, and registers as connection 3.
        log.push(negotiated(10));
        log.push(cancelled(11));
        log.push(transport(12, Some(3), TransportFact::Registered));
        log.push(session(3));
        records.borrow_mut().push_back(written(4, 0));
        evidence.observe(&records, &mut log).unwrap();
        assert_eq!(
            placed(&log),
            [(2, 5), (3, 0)],
            "shared ID 4 is connection 3"
        );
    }

    /// A preparation the node's storage ran judges only the pages of its own
    /// request: a header stream reopened on the same connection is another
    /// session, and an earlier subscription on the same stream another
    /// request, whose pages wait for their own preparations. A faulted one
    /// judges nothing.
    #[test]
    fn a_preparation_judges_only_its_own_requests_pages() {
        use crate::observe::{StorageBranch, StoragePoint, WorkOwner, WorkScope};

        let header = block::Hash([0x11; 32]);
        let invalid = hash(header);
        let prepare = |id, session_id, request_id| StorageFact::Submitted {
            id,
            request: StorageRequest::PrepareHeaderTarget {
                owner: WorkOwner {
                    kind: "header".to_owned(),
                    scope: WorkScope {
                        header_generation: 1,
                        branch: StorageBranch {
                            anchor: String::new(),
                            target: String::new(),
                        },
                    },
                    verified_generation: None,
                    body_work_epoch: None,
                    session_id,
                    request_id,
                },
                source: String::new(),
                common_ancestor: StoragePoint {
                    height: 0,
                    hash: String::new(),
                },
                target: StoragePoint {
                    height: 1,
                    hash: invalid.clone(),
                },
                headers: 1,
                work: None,
                first: None,
                hashes: Vec::new(),
            },
            digest: String::new(),
            call: None,
        };
        let completed = |id, outcome: Outcome| StorageFact::Completed {
            id,
            outcome: outcome.name().to_owned(),
            result: crate::observe::StorageResult::PrepareFailed {
                category: "invalid_header".to_owned(),
            },
            delivered: true,
            generation: 0,
            state: String::new(),
        };
        let mut evidence = WireEvidence::default();
        // Sessions 10 and 20 on connection 1's QUIC streams 4 and 8, each
        // with a decided page of subscription 7 carrying the invalid header,
        // and on stream 4 an earlier one of subscription 6.
        let page = |subscription| InvalidPage {
            header,
            subscription,
        };
        for (session, quic) in [(10, 4), (20, 8)] {
            let stream = StreamId {
                kind: 5,
                version: 9,
            };
            evidence.streams.insert((1, quic), stream);
            evidence.sessions.insert(session, 1);
            evidence.session_streams.insert(session, quic);
            evidence.unprepared.insert((1, quic, 1), page(7));
        }
        evidence.unprepared.insert((1, 4, 0), page(6));
        let judged = |evidence: &mut WireEvidence| -> Vec<(u64, u64)> {
            std::mem::take(&mut evidence.late)
                .into_iter()
                .map(|obs| match obs {
                    Observation::FrameJudged {
                        connection,
                        quic_stream,
                        ..
                    } => (connection, quic_stream.expect("a QUIC stream")),
                    other => panic!("{other:?}"),
                })
                .collect()
        };
        evidence.prepared(&prepare(1, 10, 7)).unwrap();
        evidence.prepared(&completed(1, Outcome::Engine)).unwrap();
        assert_eq!(judged(&mut evidence), vec![(1, 4)]);
        assert!(evidence.unprepared.contains_key(&(1, 4, 0)));
        evidence.prepared(&prepare(2, 20, 7)).unwrap();
        evidence
            .prepared(&completed(
                2,
                Outcome::Fault(zakura_storage::header::Fault::LocalResource),
            ))
            .unwrap();
        assert_eq!(judged(&mut evidence), Vec::new());
        evidence.prepared(&prepare(3, 20, 7)).unwrap();
        evidence.prepared(&completed(3, Outcome::Engine)).unwrap();
        assert_eq!(judged(&mut evidence), vec![(1, 8)]);
    }

    /// What the node does after it read three `GetPeers`, in a test of
    /// [`over_margin_log`].
    #[derive(Clone, Copy, Debug)]
    enum Then {
        /// The serving counts request `seq`.
        Count(u64),
        /// The serving releases one commitment.
        Release,
        /// The node closes the connection with this cause.
        Close(&'static str),
    }

    /// The log a node leaves when a peer sends three `GetPeers` the node
    /// reads before it counts any, and the node then does `then`. The peer's
    /// models labelled the third `OverMargin`.
    fn over_margin_log(then: &[Then]) -> ObservationLog {
        let stream = StreamId {
            kind: 4,
            version: 2,
        };
        let get_peers = discovery_row(u16::from(zakura_network::zakura::MSG_DISCOVERY_GET_PEERS))
            .expect("discovery has get-peers");
        let records = PeerRecords::default();
        records.borrow_mut().extend((0..3).map(|seq| PeerRecord {
            remote: remote(0),
            quic_stream: 4,
            body: RecordBody::Frame {
                seq,
                row: Some("get-peers".to_owned()),
                rule_stream: stream,
                label: if seq == 2 {
                    Label::OverMargin
                } else {
                    Label::Conformant
                },
                exhausted: false,
                exchange: Some(seq),
                payload: 6,
                prepares: None,
            },
        }));
        let mut evidence = with_ids(&[(1, 0)]).with_discovery(stream);
        let mut log = ObservationLog::default();
        log.push(transport(1, Some(1), TransportFact::Registered));
        log.push(Observation::Session {
            connection: 1,
            peer: "peer-0".to_owned(),
            quic_stream: 4,
            kind: 4,
            version: 2,
            session_id: 7,
        });
        let mut seq = 2;
        let mut next = || {
            seq += 1;
            seq
        };
        for frame in 0..3 {
            log.push(transport(
                next(),
                Some(1),
                TransportFact::HeaderRead {
                    quic_stream: 4,
                    seq: frame,
                    message_type: get_peers.message_type,
                    flags: 0,
                    payload_len: 6,
                },
            ));
            log.push(transport(
                next(),
                Some(1),
                TransportFact::FrameRead {
                    quic_stream: 4,
                    seq: frame,
                },
            ));
        }
        let regulated = |transition| TransportFact::Regulated {
            quic_stream: 4,
            transition,
        };
        for then in then {
            let event = match *then {
                Then::Count(frame) => regulated(TransitionFact::CommitmentCounted {
                    seq: Some(frame),
                    message_type: get_peers.message_type,
                }),
                Then::Release => regulated(TransitionFact::CommitmentReleased {
                    message_type: get_peers.message_type,
                }),
                Then::Close(cause) => TransportFact::CloseDecided {
                    cause: Some(cause.to_owned()),
                },
            };
            log.push(transport(next(), Some(1), event));
        }
        log.push(transport(next(), Some(1), TransportFact::TornDown));
        evidence
            .observe(&records, &mut log)
            .expect("every record is placed");
        log
    }

    /// What `log` holds for the third request from its read on: its
    /// `PeerFrame` by its label, its `FrameJudged`, and the close.
    fn after_third_read(log: &ObservationLog) -> Vec<&'static str> {
        let read = log
            .entries()
            .iter()
            .rposition(|entry| {
                matches!(
                    entry.obs,
                    Observation::Transport {
                        event: TransportFact::FrameRead { seq: 2, .. },
                        ..
                    }
                )
            })
            .expect("the third request is read");
        log.entries()[read..]
            .iter()
            .filter_map(|entry| match &entry.obs {
                Observation::PeerFrame { seq: 2, label, .. } => Some(label.name()),
                Observation::FrameJudged { seq: 2, .. } => Some("judged"),
                Observation::ConnectionClosed { .. } => Some("closed"),
                _ => None,
            })
            .collect()
    }

    /// A harness with no view, for checks that read only the log.
    struct Logged;

    impl crate::property::HasView for Logged {
        type View = ();

        /// Nothing.
        fn view(&self) {}
    }

    /// Logs a finished log's frames, judgements and closes at step 1, the
    /// observations discovery's generated properties read.
    struct Replayed(Option<ObservationLog>);

    impl crate::ActionSource for Replayed {
        fn enabled(&self, _: zakura_scheduler::SimInstant, _: &mut Vec<(crate::Identity, u64)>) {}

        fn boundaries(&self, _: zakura_scheduler::SimInstant, _: &mut Vec<crate::Boundary>) {}

        fn execute(&mut self, _: u64, _: &mut ObservationLog) {
            unreachable!("the replay offers no actions");
        }

        /// Logs the finished log's observations once, at step 1.
        fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
            if log.step() == 0 {
                return Ok(());
            }
            for entry in self
                .0
                .take()
                .into_iter()
                .flat_map(|done| done.entries().to_vec())
            {
                if matches!(
                    entry.obs,
                    Observation::PeerFrame { .. }
                        | Observation::FrameJudged { .. }
                        | Observation::ConnectionClosed { .. }
                ) {
                    log.push(entry.obs);
                }
            }
            Ok(())
        }
    }

    /// The outcome of each of `discovery/wire`'s properties over `log`, by
    /// ID.
    fn outcomes(log: ObservationLog) -> BTreeMap<String, crate::PropertyOutcome> {
        use crate::{
            harness::discovery_service::wire_properties,
            property::{Assumptions, Registry, RegistryChecker, Source, Sources},
        };
        let registry =
            Registry::new(wire_properties::<Logged>()).expect("generated IDs are unique");
        let mut checker = RegistryChecker::new(
            registry,
            Rc::new(Logged),
            Assumptions::none(),
            &Sources::observed(&[Source::Schedule, Source::Frames, Source::Closes]),
        );
        let mut sim = crate::Sim::new(
            crate::RunConfig::default(),
            crate::PlanInput::Bytes(Vec::new()),
        );
        sim.add_source(Box::new(Replayed(Some(log))));
        sim.scheduler().spawn_root("reactor", async {
            zakura_rt::task::yield_now().await;
        });
        sim.run(&mut checker, &mut crate::MemorySink::default())
            .properties
            .into_iter()
            .map(|property| (property.property, property.outcome))
            .collect()
    }

    /// A `GetPeers` the peer's models labelled `OverMargin` settles at the
    /// serving's count of it. The node's count is the commitments of the row
    /// it counted, less those it released, so a request within twice
    /// `max_in_flight` is `Conformant`. At a fault close before the count,
    /// the request is `Conformant` if its read position less the releases is
    /// within the margin, and unjudged otherwise. Under a close that isn't a
    /// fault it stays unjudged.
    #[test]
    fn an_over_margin_request_settles_at_the_nodes_count() {
        use Then::{Close, Count, Release};
        const FAULT: &str = "service_protocol_reject";
        const CLEAN: &str = "discovery_exchange_complete";
        let cases: [(&[Then], &[&str]); 7] = [
            // Released one: the third request is the node's second open.
            (
                &[Count(0), Release, Count(1), Count(2), Close(CLEAN)],
                &["conformant", "judged", "closed"],
            ),
            // Released none: the third request is past the margin of 2.
            (
                &[Count(0), Count(1), Count(2), Close(FAULT)],
                &["over-margin", "judged", "closed"],
            ),
            (
                &[Count(0), Count(1), Count(2), Close(CLEAN)],
                &["over-margin", "judged", "closed"],
            ),
            // A fault close before the count proves it `Conformant` when its
            // bound is within the margin, and leaves it unjudged otherwise.
            (
                &[Count(0), Count(1), Close(FAULT)],
                &["closed", "over-margin"],
            ),
            (&[Count(0), Close(FAULT)], &["closed", "over-margin"]),
            (
                &[Count(0), Release, Count(1), Close(FAULT)],
                &["conformant", "judged", "closed"],
            ),
            // Any other close leaves it unjudged.
            (
                &[Count(0), Count(1), Close(CLEAN)],
                &["closed", "over-margin"],
            ),
        ];
        for (then, expected) in cases {
            let log = over_margin_log(then);
            assert_eq!(after_third_read(&log), expected, "{then:?}");
        }
    }

    /// The #2122 case: with `max_in_flight` 1, the node reads three
    /// `GetPeers` before it writes any answer, releases the first
    /// commitment, and a bug closes the connection on the third. The third
    /// was the node's second open request, within the margin, so the close
    /// punished conformant frames.
    #[test]
    fn a_close_within_the_nodes_margin_violates_conformant_never_closes() {
        use Then::{Close, Count, Release};
        let log = over_margin_log(&[
            Count(0),
            Release,
            Count(1),
            Count(2),
            Close("service_protocol_reject"),
        ]);
        let outcomes = outcomes(log);
        assert!(
            matches!(
                outcomes["discovery/s4v2/conformant-never-closes"],
                crate::PropertyOutcome::Violated { .. }
            ),
            "{outcomes:?}"
        );
        assert!(
            !matches!(
                outcomes["discovery/s4v2/get-peers/over-margin-closes"],
                crate::PropertyOutcome::Violated { .. }
            ),
            "{outcomes:?}"
        );
    }

    /// The #2240 case: with `max_in_flight` 1, the node reads three
    /// `GetPeers` while its serving has counted only the first, and a bug
    /// closes the connection with a fault cause. The serving counts a
    /// request before it checks the margin, so no uncounted request justified
    /// the close, and the close punished conformant frames.
    #[test]
    fn a_close_before_the_nodes_count_violates_conformant_never_closes() {
        use Then::{Close, Count};
        let outcomes = outcomes(over_margin_log(&[
            Count(0),
            Close("service_protocol_reject"),
        ]));
        assert!(
            matches!(
                outcomes["discovery/s4v2/conformant-never-closes"],
                crate::PropertyOutcome::Violated { .. }
            ),
            "{outcomes:?}"
        );
        assert!(
            !matches!(
                outcomes["discovery/s4v2/get-peers/over-margin-closes"],
                crate::PropertyOutcome::Violated { .. }
            ),
            "{outcomes:?}"
        );
    }

    /// The node's count past the margin, then its fault close: the request
    /// is `OverMargin` and the close discharges `over-margin-closes`, with no
    /// conformant frame punished.
    #[test]
    fn a_close_past_the_nodes_margin_holds_over_margin_closes() {
        use Then::{Close, Count};
        let log = over_margin_log(&[
            Count(0),
            Count(1),
            Count(2),
            Close("service_protocol_reject"),
        ]);
        let outcomes = outcomes(log);
        assert!(
            matches!(
                outcomes["discovery/s4v2/get-peers/over-margin-closes"],
                crate::PropertyOutcome::Held { .. }
            ),
            "{outcomes:?}"
        );
        assert!(
            !matches!(
                outcomes["discovery/s4v2/conformant-never-closes"],
                crate::PropertyOutcome::Violated { .. }
            ),
            "{outcomes:?}"
        );
    }
}
