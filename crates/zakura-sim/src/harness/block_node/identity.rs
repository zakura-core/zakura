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
//! - `FrameJudged` becomes [`Observation::FrameJudged`], numbered from 0 as
//!   the relay numbers the peer's frames;
//! - a body entering the sequencer's input queue after its handoff becomes
//!   [`Observation::WorkAdmitted`], from the frame that delivered it;
//! - a serving lease's acquisition and release become
//!   [`Observation::Serving`] and [`Observation::ServingEnded`];
//! - a queued serving frame becomes [`Observation::NodeOutput`].
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

use super::relay::{row, row_name, stream, Wire};
use crate::{
    observe::{StorageFact, StorageRequest},
    storage::hex,
    Observation, ObservationLog,
};

/// Writes the generated observations of one node's block sync.
pub(crate) struct Adapter {
    wire: Wire,
    /// Each session's connection.
    sessions: BTreeMap<u64, u64>,
    /// Each peer's newest connection, by its ID in hex.
    peers: BTreeMap<String, u64>,
    /// Bodies handed off and not yet queued, by owner session and height.
    handoffs: BTreeSet<(u64, u32)>,
    /// Each submitted body's supplier, by operation ID.
    suppliers: BTreeMap<u64, String>,
    /// Peers the verifier judged invalid.
    judged: BTreeSet<String>,
}

impl Adapter {
    /// An adapter writing `wire`'s labels.
    pub(crate) fn new(wire: Wire) -> Self {
        Adapter {
            wire,
            sessions: BTreeMap::new(),
            peers: BTreeMap::new(),
            handoffs: BTreeSet::new(),
            suppliers: BTreeMap::new(),
            judged: BTreeSet::new(),
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
            while let Some((_, obs)) = records.next_if(|(after, _)| after < seq) {
                log.push(obs);
            }
            while let Some((_, fact)) = facts.next_if(|(at, _)| at <= storage_seq) {
                self.fact(fact, log);
            }
            self.event(event, log);
        }
        for (_, obs) in records {
            log.push(obs);
        }
        for (_, fact) in facts {
            self.fact(fact, log);
        }
        for (connection, cause) in self.wire.closes() {
            if !self.judged_connection(connection) {
                log.push(Observation::ConnectionClosed {
                    connection,
                    cause: cause.to_owned(),
                });
            }
        }
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

    /// Writes what `event` means for the generated checks.
    fn event(&mut self, event: &BlockEvent, log: &mut ObservationLog) {
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
            BlockEvent::FrameJudged { conn_id, seq, .. } => {
                log.push(Observation::FrameJudged {
                    connection: *conn_id,
                    stream: stream(),
                    // The routine counts from 1, and the relay from 0.
                    seq: seq - 1,
                });
            }
            BlockEvent::HandoffStarted { owner, height, .. } => {
                self.handoffs.insert((owner.session_id, height.0));
            }
            BlockEvent::Holder {
                body,
                holder: Holder::Queued { .. },
                change: HolderChange::Retained,
            } => {
                if !self
                    .handoffs
                    .remove(&(body.owner.session_id, body.height.0))
                {
                    return;
                }
                let Some(connection) = self.sessions.get(&body.owner.session_id).copied() else {
                    return;
                };
                // Work from a body no frame delivered names no frame the
                // check can find.
                let delivered = self.wire.body(connection, body.height.0);
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
            BlockEvent::ServingFrame {
                peer,
                start,
                message_type,
                body_bytes,
            } => {
                let Some(connection) = self.connection(peer) else {
                    return;
                };
                let is_body = *message_type == MSG_BS_BLOCK;
                log.push(Observation::NodeOutput {
                    connection,
                    frame: row(row_name(u16::from(*message_type))),
                    request: get_blocks(),
                    exchange: u64::from(start.0),
                    // A body's payload is its tag and its bytes, and an
                    // ending's is its tag, start and count.
                    payload: if is_body { body_bytes + 1 } else { 9 },
                    ends: !is_body,
                });
            }
            _ => {}
        }
    }
}
