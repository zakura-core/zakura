//! The node's identities and effects as generated-property observations
//! (properties.md, Identity chain and Downstream templates).
//!
//! [`Adapter`] writes, at each tap drain, the observations the stream-6
//! generated checks read, in the order things happened: the relays' labels,
//! placed among the node's block-sync events by the event count each label
//! carries; the storage facts handed to the tap, placed by each event's
//! storage watermark; and, from the events themselves:
//! - `SessionAdmitted` maps the session, and the peer's newest connection,
//!   to the connection;
//! - `FrameJudged`, and a relay's record of a header the node's reader
//!   refused, become [`Observation::FrameJudged`], numbered from 0 as the
//!   relay numbers the peer's frames, after the label of a request that
//!   waited for it;
//! - a body entering the sequencer's input queue becomes
//!   [`Observation::WorkAdmitted`], from the frame that delivered it. A
//!   body can arrive on another connection than its owner's: the routine
//!   that receives a body for a height re-issued elsewhere hands it off
//!   under the new owner. The adapter reads the connection from that
//!   routine's `HandoffStarted`, else tries the owner's connection, then
//!   every other;
//! - a serving lease's acquisition and release become
//!   [`Observation::Serving`] and [`Observation::ServingEnded`];
//! - a serving admission settles the label of a request that waited for
//!   the node's timing.
//!
//! A request that waits for the node's timing gets its label when the
//! node's serving session judges it: the relay's [`Settle`] reads the
//! ranges the session still served then. The adapter writes the label, then
//! the exchange's [`Observation::ExchangeOwed`] if it settles `Conformant`,
//! then the frame's [`Observation::FrameJudged`]. A request the session
//! never judged keeps the relay's label, written when the node judges the
//! frame some other way or its connection closes.
//!
//! A `Misbehavior` request becomes [`Observation::Misbehavior`] on the
//! peer's newest connection, and a link's recorded close cause
//! [`Observation::ConnectionClosed`] at the end of the drain that sees it.
//! The node decides a close before that, so a check reads the close a little
//! late, which can only miss a violation.
//!
//! The verifier's verdicts aren't on the wire, so no relay label can
//! justify a punishment that follows one. Once storage returns a
//! payload-mismatch or consensus-invalid verdict for a body a peer
//! supplied, the adapter writes no `Misbehavior` or close for that peer's
//! connections: the scenario's verifier, not the wire, judged that peer.

use std::collections::{BTreeMap, BTreeSet};

use zakura_network::zakura::{
    block_sync_observe::{BlockEvent, Holder, HolderChange, LeaseStep},
    ZakuraPeerId, MSG_BS_BLOCK, MSG_BS_GET_BLOCKS,
};

use super::relay::{row, row_name, stream, Record, Settle, Wire};
use crate::{
    model::Label,
    observe::{StorageFact, StorageRequest},
    storage::hex,
    Observation, ObservationLog,
};

/// A request that waits for the node to judge it.
struct Held {
    /// Its frame, with the relay's label.
    frame: Observation,
    settle: Settle,
    /// Its exchange's record, once the relay opened it.
    owed: Vec<Observation>,
}

/// Writes the generated observations of one node's block sync.
pub(crate) struct Adapter {
    wire: Wire,
    /// Each session's connection.
    sessions: BTreeMap<u64, u64>,
    /// The connection of the routine that last started a handoff, by
    /// height.
    handoffs: BTreeMap<u32, u64>,
    /// Each peer's newest connection, by its ID in hex.
    peers: BTreeMap<String, u64>,
    /// Each submitted body's supplier, by operation ID.
    suppliers: BTreeMap<u64, String>,
    /// Peers the verifier judged invalid.
    judged: BTreeSet<String>,
    /// Requests waiting for the node to judge them, by connection and
    /// frame number.
    held: BTreeMap<(u64, u64), Held>,
    /// The request the node judged and its serving session hasn't yet, by
    /// connection, with its frame number.
    admitting: BTreeMap<u64, (u64, Held)>,
    /// Requests that waited, by connection and frame number, and whether
    /// each settled `Conformant`.
    settled: BTreeMap<(u64, u64), bool>,
}

impl Adapter {
    /// An adapter writing `wire`'s labels.
    pub(crate) fn new(wire: Wire) -> Self {
        Adapter {
            wire,
            sessions: BTreeMap::new(),
            handoffs: BTreeMap::new(),
            peers: BTreeMap::new(),
            suppliers: BTreeMap::new(),
            judged: BTreeSet::new(),
            held: BTreeMap::new(),
            admitting: BTreeMap::new(),
            settled: BTreeMap::new(),
        }
    }

    /// Writes this drain's observations to `log`: `events` as
    /// `(seq, storage watermark, event)`, and the storage `facts` handed to
    /// the tap as `(seq, fact)`, each in order.
    pub(crate) fn write(
        &mut self,
        events: &[(u64, u64, BlockEvent)],
        facts: &[(u64, StorageFact)],
        log: &mut ObservationLog,
    ) {
        let mut records = self.wire.take().into_iter().peekable();
        let mut facts = facts.iter().peekable();
        for (seq, storage_seq, event) in events {
            while let Some((_, record)) = records.next_if(|(after, _)| after < seq) {
                self.record(record, log);
            }
            while let Some((_, fact)) = facts.next_if(|(at, _)| at <= storage_seq) {
                self.fact(fact, log);
            }
            self.event(*seq, event, log);
        }
        for (_, record) in records {
            self.record(record, log);
        }
        for (_, fact) in facts {
            self.fact(fact, log);
        }
        for (connection, cause) in self.wire.closes() {
            let unjudged: Vec<_> = self
                .held
                .keys()
                .filter(|(at, _)| *at == connection)
                .copied()
                .collect();
            if let Some((seq, held)) = self.admitting.remove(&connection) {
                self.unsettled(connection, seq, held, log);
                log.push(Observation::FrameJudged {
                    connection,
                    stream: stream(),
                    seq,
                });
            }
            for key in unjudged {
                let held = self.held.remove(&key).expect("listed above");
                self.unsettled(key.0, key.1, held, log);
            }
            if !self.judged_connection(connection) {
                log.push(Observation::ConnectionClosed {
                    connection,
                    cause: cause.to_owned(),
                });
            }
        }
    }

    /// Writes `record`, or holds it until the node judges its request.
    fn record(&mut self, record: Record, log: &mut ObservationLog) {
        match record {
            Record::Obs(obs) => log.push(obs),
            Record::Pending {
                connection,
                seq,
                frame,
                settle,
            } => {
                self.held.insert(
                    (connection, seq),
                    Held {
                        frame,
                        settle,
                        owed: Vec::new(),
                    },
                );
            }
            Record::Judged { connection, seq } => {
                if let Some(held) = self.held.remove(&(connection, seq)) {
                    self.unsettled(connection, seq, held, log);
                }
                log.push(Observation::FrameJudged {
                    connection,
                    stream: stream(),
                    seq,
                });
            }
            Record::Owed {
                connection,
                request,
                obs,
            } => {
                if let Some(held) = self.held.get_mut(&(connection, request)) {
                    held.owed.push(obs);
                } else if self.settled.get(&(connection, request)) != Some(&false) {
                    log.push(obs);
                }
            }
        }
    }

    /// Writes held request `seq` on `connection`, which the node's serving
    /// session judged against the `live` ranges, with the label they
    /// settle, and its exchange's record if it's `Conformant`.
    fn settle(
        &mut self,
        connection: u64,
        seq: u64,
        held: Held,
        live: &[(u32, u32)],
        log: &mut ObservationLog,
    ) {
        let label = held.settle.label(live);
        let mut frame = held.frame;
        if let Observation::PeerFrame { label: at, .. } = &mut frame {
            *at = label;
        }
        log.push(frame);
        let conformant = label == Label::Conformant;
        if conformant {
            for obs in held.owed {
                log.push(obs);
            }
        }
        self.settled.insert((connection, seq), conformant);
    }

    /// Writes held request `seq` on `connection`, which the node's serving
    /// session never judged, with the relay's label and no exchange.
    fn unsettled(&mut self, connection: u64, seq: u64, held: Held, log: &mut ObservationLog) {
        log.push(held.frame);
        self.settled.insert((connection, seq), false);
    }

    /// Writes that the node judged the peer's frame `seq` on `connection`,
    /// or, for a held request, waits for its serving session to judge it.
    fn judged(&mut self, connection: u64, seq: u64, log: &mut ObservationLog) {
        if let Some(held) = self.held.remove(&(connection, seq)) {
            self.admitting.insert(connection, (seq, held));
            return;
        }
        log.push(Observation::FrameJudged {
            connection,
            stream: stream(),
            seq,
        });
    }

    /// True if the verifier judged the peer on `connection`.
    fn judged_connection(&self, connection: u64) -> bool {
        self.peers
            .iter()
            .any(|(peer, at)| *at == connection && self.judged.contains(peer))
    }

    /// Writes what `fact` means for the generated checks.
    fn fact(&mut self, fact: &StorageFact, log: &mut ObservationLog) {
        match fact {
            StorageFact::BlockSubmitted {
                id,
                request: StorageRequest::SubmitBlock { source, .. },
                ..
            } => {
                self.suppliers.insert(*id, source.clone());
            }
            StorageFact::BlockSubmitted {
                request: StorageRequest::Misbehavior { peer, .. },
                ..
            } => {
                if self.judged.contains(peer) {
                    return;
                }
                if let Some(connection) = self.peers.get(peer) {
                    log.push(Observation::Misbehavior {
                        connection: *connection,
                    });
                }
            }
            StorageFact::BlockCompleted { id, outcome, .. }
                if outcome.starts_with("payload-mismatch") || outcome == "consensus-invalid" =>
            {
                if let Some(supplier) = self.suppliers.get(id) {
                    self.judged.insert(supplier.clone());
                }
            }
            _ => {}
        }
    }

    /// The newest connection of `peer`.
    fn connection(&self, peer: &ZakuraPeerId) -> Option<u64> {
        self.peers.get(&hex(peer.as_bytes())).copied()
    }

    /// Writes what `event`, the tap's event `seq`, means for the generated
    /// checks.
    fn event(&mut self, seq: u64, event: &BlockEvent, log: &mut ObservationLog) {
        let get_blocks = || row(row_name(u16::from(MSG_BS_GET_BLOCKS)));
        match event {
            BlockEvent::SessionAdmitted {
                peer,
                conn_id,
                session_id,
                ..
            } => {
                self.sessions.insert(*session_id, *conn_id);
                // Storage names a peer by its ID's bytes, and a supplier by
                // its digest. A node ID is its own digest, but both keys
                // keep that from mattering.
                self.peers.insert(hex(peer.as_bytes()), *conn_id);
                self.peers.insert(hex(&peer.digest()), *conn_id);
            }
            BlockEvent::FrameJudged {
                conn_id,
                seq: judged,
                ..
            } => {
                // The routine counts from 1, and the relay from 0.
                self.judged(*conn_id, judged - 1, log);
            }
            BlockEvent::HandoffStarted { peer, height, .. } => {
                if let Some(connection) = self.connection(peer) {
                    self.handoffs.insert(height.0, connection);
                }
            }
            // Every body the routine sends the sequencer, with or without a
            // budget charge.
            BlockEvent::Holder {
                body,
                holder: Holder::Queued { .. },
                change: HolderChange::Retained,
            } => {
                let height = body.height.0;
                let handoff = self.handoffs.remove(&height);
                let owner = self.sessions.get(&body.owner.session_id).copied();
                let mut candidates: Vec<u64> = handoff.into_iter().chain(owner).collect();
                for connection in self.sessions.values() {
                    if !candidates.contains(connection) {
                        candidates.push(*connection);
                    }
                }
                let Some(&first) = candidates.first() else {
                    return;
                };
                // Work from a body no frame delivered names no frame the
                // check can find.
                let (connection, delivered) = candidates
                    .iter()
                    .find_map(|connection| {
                        self.wire
                            .body(*connection, height, seq)
                            .map(|delivered| (*connection, Some(delivered)))
                    })
                    .unwrap_or((first, None));
                log.push(Observation::WorkAdmitted {
                    connection,
                    frame: row(row_name(u16::from(MSG_BS_BLOCK))),
                    seq: delivered.map_or(u64::MAX, |delivered| delivered.seq),
                    request: get_blocks(),
                    exchange: delivered
                        .and_then(|delivered| delivered.exchange)
                        .unwrap_or(u64::from(body.height.0)),
                    objects: 1,
                    bytes: body.bytes,
                    ends: false,
                });
            }
            BlockEvent::ServingLease {
                peer, start, step, ..
            } => {
                let Some(connection) = self.connection(peer) else {
                    return;
                };
                let (request, exchange) = (get_blocks(), u64::from(start.0));
                log.push(match step {
                    LeaseStep::Acquired => Observation::Serving {
                        connection,
                        request,
                        exchange,
                    },
                    LeaseStep::Released => Observation::ServingEnded {
                        connection,
                        request,
                        exchange,
                    },
                });
            }
            BlockEvent::ServingAdmission { peer, live, .. } => {
                let Some(connection) = self.connection(peer) else {
                    return;
                };
                let Some((judged, held)) = self.admitting.remove(&connection) else {
                    return;
                };
                let live: Vec<_> = live.iter().map(|(first, last)| (first.0, last.0)).collect();
                self.settle(connection, judged, held, &live, log);
                log.push(Observation::FrameJudged {
                    connection,
                    stream: stream(),
                    seq: judged,
                });
            }
            _ => {}
        }
    }
}
