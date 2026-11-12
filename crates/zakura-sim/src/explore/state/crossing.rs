//! Crossed updates: a peer's `subscribe-headers` update that met the node's
//! ending of the same subscription in flight (fuzz-13).
//!
//! An update crossed an ending when the node wrote the ending before the
//! peer's write of the update returned, and the peer read that same ending
//! only after. A local end isn't enough, since the publisher ends a
//! subscription before it writes the ending. Log position isn't enough
//! either: a peer's reader reads a frame before its main task records it,
//! and a peer's records reach the log only once their identity rows do. So
//! the order comes from the run's [`crate::net::WireOrder`] stamps on
//! `frame-written`, `peer-written` and `peer-received`, and every other fact
//! is joined by identity: connection, QUIC stream, frame sequence and
//! subscription.
//!
//! The facts can arrive in any order. The ending's `NodeFrame` comes only
//! when the peer handles its read, which can follow the update's judgment
//! and even the connection's teardown. So [`CrossingJoin`] keeps every
//! partial fact for the whole run and reports a crossing when its last fact
//! arrives, whichever that is.

use std::collections::{BTreeMap, BTreeSet};

use crate::{model::Label, observe::TransportFact, Observation};

/// The row of a node frame that ends a subscription.
const ENDING_ROW: &str = "headers-outcome";

/// The row of a peer's subscription update.
const UPDATE_ROW: &str = "subscribe-headers";

/// One crossed update: the update's identity and the label the peer's
/// model gave it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Crossed {
    /// The connection.
    pub connection: u64,
    /// The QUIC stream both frames travelled on.
    pub quic_stream: u64,
    /// The update's position among the peer's frames on the stream.
    pub seq: u64,
    /// The subscription both frames name.
    pub subscription: u64,
    /// The update's label, from its `PeerFrame`.
    pub label: Label,
}

/// What the join knows of one node frame on a stream.
#[derive(Clone, Copy, Debug, Default)]
struct Ending {
    /// Its `frame-written` stamp.
    written: Option<u64>,
    /// The subscription it ends, once its `NodeFrame` shows it's an ending.
    subscription: Option<u64>,
    /// Its `peer-received` stamp.
    read: Option<u64>,
}

/// What the join knows of one peer frame on a stream.
#[derive(Clone, Copy, Debug, Default)]
struct Update {
    /// The subscription and label, once its `PeerFrame` shows it's an
    /// update.
    frame: Option<(u64, Label)>,
    /// Its `peer-written` stamp.
    written: Option<u64>,
    /// Whether the node judged it.
    judged: bool,
}

/// The incremental crossing join. Feed it every observation of a run, in
/// log order, through [`CrossingJoin::observe`]. It holds at most one entry
/// per frame the run wrote on a QUIC stream, and starts empty for each run.
#[derive(Debug, Default)]
pub struct CrossingJoin {
    /// The node's frames, by connection, QUIC stream and the node's frame
    /// sequence.
    endings: BTreeMap<(u64, u64), BTreeMap<u64, Ending>>,
    /// The peers' frames, by connection, QUIC stream and the peer's frame
    /// sequence.
    updates: BTreeMap<(u64, u64), BTreeMap<u64, Update>>,
    /// The updates already reported, so each crosses once.
    reported: BTreeSet<(u64, u64, u64)>,
}

impl CrossingJoin {
    /// Takes in `observation` and returns each crossing it completes.
    pub fn observe(&mut self, observation: &Observation) -> Vec<Crossed> {
        match *observation {
            Observation::Transport {
                connection: Some(connection),
                event:
                    TransportFact::FrameWritten {
                        quic_stream,
                        seq,
                        order,
                        ..
                    },
                ..
            } => {
                self.ending(connection, quic_stream, seq).written = Some(order);
                self.complete_ending(connection, quic_stream, seq)
            }
            Observation::NodeFrame {
                connection,
                quic_stream: Some(quic_stream),
                seq,
                ref row,
                exchange: Some(subscription),
                ..
            } if row.as_deref() == Some(ENDING_ROW) => {
                self.ending(connection, quic_stream, seq).subscription = Some(subscription);
                self.complete_ending(connection, quic_stream, seq)
            }
            Observation::PeerReceived {
                connection,
                quic_stream,
                seq,
                order,
                ..
            } => {
                self.ending(connection, quic_stream, seq).read = Some(order);
                self.complete_ending(connection, quic_stream, seq)
            }
            Observation::PeerFrame {
                connection,
                quic_stream: Some(quic_stream),
                seq,
                ref row,
                label,
                exchange: Some(subscription),
                ..
            } if row.as_deref() == Some(UPDATE_ROW) => {
                self.update(connection, quic_stream, seq).frame = Some((subscription, label));
                self.complete_update(connection, quic_stream, seq)
            }
            Observation::PeerWritten {
                connection,
                quic_stream,
                seq,
                order,
            } => {
                self.update(connection, quic_stream, seq).written = Some(order);
                self.complete_update(connection, quic_stream, seq)
            }
            Observation::FrameJudged {
                connection,
                quic_stream: Some(quic_stream),
                seq,
                ..
            } => {
                self.update(connection, quic_stream, seq).judged = true;
                self.complete_update(connection, quic_stream, seq)
            }
            _ => Vec::new(),
        }
    }

    /// The node frame `seq` on `connection`'s `quic_stream`.
    fn ending(&mut self, connection: u64, quic_stream: u64, seq: u64) -> &mut Ending {
        self.endings
            .entry((connection, quic_stream))
            .or_default()
            .entry(seq)
            .or_default()
    }

    /// The peer frame `seq` on `connection`'s `quic_stream`.
    fn update(&mut self, connection: u64, quic_stream: u64, seq: u64) -> &mut Update {
        self.updates
            .entry((connection, quic_stream))
            .or_default()
            .entry(seq)
            .or_default()
    }

    /// The crossings node frame `seq` completes with the stream's updates.
    fn complete_ending(&mut self, connection: u64, quic_stream: u64, seq: u64) -> Vec<Crossed> {
        let stream = (connection, quic_stream);
        let Some(ending) = self
            .endings
            .get(&stream)
            .and_then(|frames| frames.get(&seq))
        else {
            return Vec::new();
        };
        let ending = *ending;
        let updates: Vec<u64> = self
            .updates
            .get(&stream)
            .map(|frames| frames.keys().copied().collect())
            .unwrap_or_default();
        updates
            .into_iter()
            .filter_map(|update| self.cross(stream, ending, update))
            .collect()
    }

    /// The crossing peer frame `seq` completes with one of the stream's
    /// endings, if any.
    fn complete_update(&mut self, connection: u64, quic_stream: u64, seq: u64) -> Vec<Crossed> {
        let stream = (connection, quic_stream);
        let endings: Vec<Ending> = self
            .endings
            .get(&stream)
            .map(|frames| frames.values().copied().collect())
            .unwrap_or_default();
        endings
            .into_iter()
            .find_map(|ending| self.cross(stream, ending, seq))
            .into_iter()
            .collect()
    }

    /// The crossing of `ending` and peer frame `seq` on `stream`, if every
    /// fact is in, they name one subscription, the ending was written
    /// before the update's write returned and read after it, and the update
    /// hasn't crossed already.
    fn cross(&mut self, stream: (u64, u64), ending: Ending, seq: u64) -> Option<Crossed> {
        let update = *self.updates.get(&stream)?.get(&seq)?;
        let (subscription, label) = update.frame?;
        let (written, read, ended) = (ending.written?, ending.read?, ending.subscription?);
        let sent = update.written?;
        let crossed = update.judged && ended == subscription && written < sent && sent < read;
        if !crossed || !self.reported.insert((stream.0, stream.1, seq)) {
            return None;
        }
        Some(Crossed {
            connection: stream.0,
            quic_stream: stream.1,
            seq,
            subscription,
            label,
        })
    }
}

/// Every crossed update in `observations`, in the order each one's last
/// fact appears.
pub fn crossings<'a>(observations: impl IntoIterator<Item = &'a Observation>) -> Vec<Crossed> {
    let mut join = CrossingJoin::default();
    observations
        .into_iter()
        .flat_map(|observation| join.observe(observation))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::StreamId;

    /// The connection, QUIC stream and subscription every case uses.
    const C: u64 = 1;
    const Q: u64 = 4;
    const SUB: u64 = 7;

    /// Header sync's stream.
    const STREAM: StreamId = StreamId {
        kind: 5,
        version: 9,
    };

    /// The node finished writing its frame `seq`, stamped `order`.
    fn node_wrote(seq: u64, order: u64) -> Observation {
        Observation::Transport {
            instance: 0,
            seq: order,
            node: "node".to_owned(),
            connection: Some(C),
            remote: "10.0.0.2:4000".to_owned(),
            event: TransportFact::FrameWritten {
                quic_stream: Q,
                seq,
                message_type: 4,
                order,
            },
        }
    }

    /// The peer read the node's frame `seq`, stamped `order`, and the
    /// adapter labelled it an ending of `subscription`.
    fn peer_read(seq: u64, order: u64, subscription: u64) -> [Observation; 2] {
        [
            Observation::PeerReceived {
                connection: C,
                quic_stream: Q,
                seq,
                message_type: 4,
                payload: 9,
                credit: None,
                order,
            },
            Observation::NodeFrame {
                connection: C,
                stream: STREAM,
                quic_stream: Some(Q),
                seq,
                row: Some(ENDING_ROW.to_owned()),
                rule_stream: STREAM,
                label: Label::Conformant,
                exhausted: false,
                exchange: Some(subscription),
            },
        ]
    }

    /// The peer's update `seq` for `SUB`, labelled `label`.
    fn update(seq: u64, label: Label) -> Observation {
        Observation::PeerFrame {
            connection: C,
            stream: STREAM,
            quic_stream: Some(Q),
            seq,
            row: Some(UPDATE_ROW.to_owned()),
            rule_stream: STREAM,
            label,
            exhausted: false,
            exchange: Some(SUB),
            payload: 52,
        }
    }

    /// The peer's write of its frame `seq` returned, stamped `order`.
    fn peer_wrote(seq: u64, order: u64) -> Observation {
        Observation::PeerWritten {
            connection: C,
            quic_stream: Q,
            seq,
            order,
        }
    }

    /// The node judged the peer's frame `seq`.
    fn judged(seq: u64) -> Observation {
        Observation::FrameJudged {
            connection: C,
            stream: STREAM,
            quic_stream: Some(Q),
            seq,
        }
    }

    /// The connection's teardown.
    fn torn_down() -> Observation {
        Observation::Transport {
            instance: 0,
            seq: 99,
            node: "node".to_owned(),
            connection: Some(C),
            remote: "10.0.0.2:4000".to_owned(),
            event: TransportFact::TornDown,
        }
    }

    /// The crossing of update 1, labelled conformant.
    fn crossed() -> Vec<Crossed> {
        vec![Crossed {
            connection: C,
            quic_stream: Q,
            seq: 1,
            subscription: SUB,
            label: Label::Conformant,
        }]
    }

    /// The node writes its ending (W = 10), the peer's update write returns
    /// (V = 11), and the peer reads the ending (R = 12): a crossing, logged
    /// in the order the facts happened.
    #[test]
    fn an_update_written_between_the_ending_and_its_read_crosses() {
        let [received, node_frame] = peer_read(0, 12, SUB);
        let log = [
            node_wrote(0, 10),
            update(1, Label::Conformant),
            peer_wrote(1, 11),
            judged(1),
            received,
            node_frame,
        ];
        assert_eq!(crossings(&log), crossed());
    }

    /// Negative 1: the node ended the subscription but wrote the ending
    /// after the update's write returned (W > V).
    #[test]
    fn an_ending_written_after_the_update_is_no_crossing() {
        let [received, node_frame] = peer_read(0, 12, SUB);
        let log = [
            update(1, Label::Conformant),
            peer_wrote(1, 10),
            node_wrote(0, 11),
            judged(1),
            received,
            node_frame,
        ];
        assert_eq!(crossings(&log), Vec::new());
    }

    /// Negative 2: two endings on one stream, and only the unrelated one,
    /// for another subscription, brackets the update. The subscription's
    /// own ending was read before the update was written.
    #[test]
    fn an_unrelated_ending_around_the_update_is_no_crossing() {
        let [own_received, own_frame] = peer_read(0, 11, SUB);
        let [other_received, other_frame] = peer_read(1, 14, SUB + 1);
        let log = [
            node_wrote(0, 10),
            own_received,
            own_frame,
            node_wrote(1, 12),
            update(1, Label::Conformant),
            peer_wrote(1, 13),
            judged(1),
            other_received,
            other_frame,
        ];
        assert_eq!(crossings(&log), Vec::new());
    }

    /// Negative 3: the update was written inside the bracket but never
    /// judged.
    #[test]
    fn an_unjudged_update_is_no_crossing() {
        let [received, node_frame] = peer_read(0, 12, SUB);
        let log = [
            node_wrote(0, 10),
            update(1, Label::Conformant),
            peer_wrote(1, 11),
            received,
            node_frame,
        ];
        assert_eq!(crossings(&log), Vec::new());
    }

    /// Negative 4: the peer's reader read the ending (R = 11) before the
    /// update's write returned (V = 12), but its main task handled the read
    /// afterward, so `peer-received` is logged after `peer-written`. The
    /// stamps order them.
    #[test]
    fn a_read_logged_late_is_ordered_by_its_stamp() {
        let [received, node_frame] = peer_read(0, 11, SUB);
        let log = [
            node_wrote(0, 10),
            update(1, Label::Conformant),
            peer_wrote(1, 12),
            judged(1),
            received,
            node_frame,
        ];
        assert_eq!(crossings(&log), Vec::new());
    }

    /// Positive 5: the update's `PeerFrame` is logged before the ending's
    /// `frame-written`, since it's recorded before the write, but the write
    /// returns after it (V > W) and the ending is read after that.
    #[test]
    fn an_update_frame_logged_before_the_ending_still_crosses() {
        let [received, node_frame] = peer_read(0, 12, SUB);
        let log = [
            update(1, Label::Conformant),
            node_wrote(0, 10),
            peer_wrote(1, 11),
            judged(1),
            received,
            node_frame,
        ];
        assert_eq!(crossings(&log), crossed());
    }

    /// Positive 6: the update is judged before the ending's `peer-received`
    /// and `NodeFrame` are logged, which complete the crossing.
    #[test]
    fn an_ending_read_logged_after_the_judgment_completes_the_crossing() {
        let [received, node_frame] = peer_read(0, 12, SUB);
        let mut join = CrossingJoin::default();
        for obs in [
            node_wrote(0, 10),
            update(1, Label::Conformant),
            peer_wrote(1, 11),
            judged(1),
            received,
        ] {
            assert_eq!(join.observe(&obs), Vec::new());
        }
        assert_eq!(join.observe(&node_frame), crossed());
    }

    /// Positive 7: the connection is torn down between the judgment and the
    /// ending's late read, and the crossing still completes.
    #[test]
    fn a_teardown_before_the_late_read_keeps_the_crossing() {
        let [received, node_frame] = peer_read(0, 12, SUB);
        let log = [
            node_wrote(0, 10),
            update(1, Label::Conformant),
            peer_wrote(1, 11),
            judged(1),
            torn_down(),
            received,
            node_frame,
        ];
        assert_eq!(crossings(&log), crossed());
    }

    /// A crossed update keeps its own label, so a caller can tell a crossed
    /// fault from a conformant crossing, and one update crosses once.
    #[test]
    fn a_crossing_keeps_its_label_and_reports_once() {
        let [received, node_frame] = peer_read(0, 12, SUB);
        let log = [
            node_wrote(0, 10),
            update(1, Label::BadCursor),
            peer_wrote(1, 11),
            judged(1),
            received,
            node_frame,
            judged(1),
        ];
        let [crossing] = crossings(&log)[..] else {
            panic!("one crossing");
        };
        assert_eq!(crossing.label, Label::BadCursor);
    }
}
