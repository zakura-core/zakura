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
//!
//! Its work stays linear in the run's frames, and it keeps nothing for
//! ordinary traffic. A node frame enters only if it's on a header-sync
//! stream, which the node's `Session` row names before anything is written
//! on it, and has the ending's message type, which other streams reuse. A
//! peer frame enters only once its `PeerFrame` shows it's a header update.
//! On a header stream its write and judgment always come after that: a peer
//! records a frame's `PeerFrame` before its write and `peer-written` after
//! ([`crate::peer`]'s `PeerTask::write`), and the wire evidence fails a run
//! whose node reads a frame with no `PeerFrame` (`UnlabelledFrame`). Only
//! discovery frames have their `PeerFrame` withheld until they settle, and
//! none of them is a header update. A completed fact then looks only at the
//! candidates of its own subscription on its stream.

use std::collections::{BTreeMap, BTreeSet};

use zakura_network::zakura::ZAKURA_STREAM_HEADER_SYNC;

use crate::{model::Label, observe::TransportFact, Observation};

/// The row of a node frame that ends a subscription.
const ENDING_ROW: &str = "headers-outcome";

/// The message type of a node frame that ends a subscription, which
/// `frame-written` and `peer-received` carry before the `NodeFrame` names
/// its row. Other streams' messages use it too, so it counts only on a
/// header-sync stream.
const ENDING_TYPE: u16 = 4;

/// The row of a peer's subscription update.
const UPDATE_ROW: &str = "subscribe-headers";

/// A frame: its connection, QUIC stream and sequence on that stream.
type FrameKey = (u64, u64, u64);

/// A subscription on a stream: connection, QUIC stream and subscription.
type SubscriptionKey = (u64, u64, u64);

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

/// What the join knows of one node frame of the ending's message type.
#[derive(Clone, Copy, Debug, Default)]
struct Ending {
    /// Its `frame-written` stamp.
    written: Option<u64>,
    /// The subscription it ends, once its `NodeFrame` shows it's an ending.
    subscription: Option<u64>,
    /// Its `peer-received` stamp.
    read: Option<u64>,
}

/// What the join knows of one peer update.
#[derive(Clone, Copy, Debug)]
struct Update {
    /// The subscription it names.
    subscription: u64,
    /// Its label.
    label: Label,
    /// Its `peer-written` stamp.
    written: Option<u64>,
    /// Whether the node judged it.
    judged: bool,
}

/// One subscription's candidates on a stream, by frame sequence.
#[derive(Debug, Default)]
struct Candidates {
    /// The node's endings of it.
    endings: BTreeSet<u64>,
    /// The peer's updates to it.
    updates: BTreeSet<u64>,
}

/// The incremental crossing join. Feed it every observation of a run, in
/// log order, through [`CrossingJoin::observe`]. It holds one entry per
/// header-sync stream, per ending-typed node frame on one, and per peer
/// update, and starts empty for each run.
#[derive(Debug, Default)]
pub struct CrossingJoin {
    /// The header-sync streams the node admitted, by connection and QUIC
    /// stream.
    header_streams: BTreeSet<(u64, u64)>,
    /// The node's ending-typed frames.
    endings: BTreeMap<FrameKey, Ending>,
    /// The peers' updates.
    updates: BTreeMap<FrameKey, Update>,
    /// Each subscription's classified endings and updates.
    candidates: BTreeMap<SubscriptionKey, Candidates>,
    /// The updates already reported, so each crosses once.
    reported: BTreeSet<FrameKey>,
}

impl CrossingJoin {
    /// Takes in `observation` and returns each crossing it completes.
    pub fn observe(&mut self, observation: &Observation) -> Vec<Crossed> {
        match *observation {
            Observation::Session {
                connection,
                quic_stream,
                kind: ZAKURA_STREAM_HEADER_SYNC,
                ..
            } => {
                self.header_streams.insert((connection, quic_stream));
                Vec::new()
            }
            Observation::Transport {
                connection: Some(connection),
                event:
                    TransportFact::FrameWritten {
                        quic_stream,
                        seq,
                        message_type: ENDING_TYPE,
                        order,
                    },
                ..
            } if self.header_streams.contains(&(connection, quic_stream)) => {
                let key = (connection, quic_stream, seq);
                self.endings.entry(key).or_default().written = Some(order);
                self.complete_ending(key)
            }
            Observation::PeerReceived {
                connection,
                quic_stream,
                seq,
                message_type: ENDING_TYPE,
                order,
                ..
            } if self.header_streams.contains(&(connection, quic_stream)) => {
                let key = (connection, quic_stream, seq);
                self.endings.entry(key).or_default().read = Some(order);
                self.complete_ending(key)
            }
            Observation::NodeFrame {
                connection,
                quic_stream: Some(quic_stream),
                seq,
                ref row,
                exchange: Some(subscription),
                ..
            } if row.as_deref() == Some(ENDING_ROW) => {
                let key = (connection, quic_stream, seq);
                self.endings.entry(key).or_default().subscription = Some(subscription);
                self.candidates
                    .entry((connection, quic_stream, subscription))
                    .or_default()
                    .endings
                    .insert(seq);
                self.complete_ending(key)
            }
            Observation::PeerFrame {
                connection,
                quic_stream: Some(quic_stream),
                seq,
                ref row,
                label,
                exchange,
                ..
            } => {
                let key = (connection, quic_stream, seq);
                let (Some(subscription), Some(UPDATE_ROW)) = (exchange, row.as_deref()) else {
                    return Vec::new();
                };
                self.updates.insert(
                    key,
                    Update {
                        subscription,
                        label,
                        written: None,
                        judged: false,
                    },
                );
                self.candidates
                    .entry((connection, quic_stream, subscription))
                    .or_default()
                    .updates
                    .insert(seq);
                self.complete_update(key)
            }
            Observation::PeerWritten {
                connection,
                quic_stream,
                seq,
                order,
            } => {
                let key = (connection, quic_stream, seq);
                // Not an update: its `PeerFrame` came first and said so.
                let Some(update) = self.updates.get_mut(&key) else {
                    return Vec::new();
                };
                update.written = Some(order);
                self.complete_update(key)
            }
            Observation::FrameJudged {
                connection,
                quic_stream: Some(quic_stream),
                seq,
                ..
            } => {
                let key = (connection, quic_stream, seq);
                // Not an update: its `PeerFrame` came first and said so.
                let Some(update) = self.updates.get_mut(&key) else {
                    return Vec::new();
                };
                update.judged = true;
                self.complete_update(key)
            }
            _ => Vec::new(),
        }
    }

    /// The crossings ending `key` completes with its subscription's
    /// updates.
    fn complete_ending(&mut self, key: FrameKey) -> Vec<Crossed> {
        let (connection, quic_stream, _) = key;
        let Some(subscription) = self
            .endings
            .get(&key)
            .and_then(|ending| ending.subscription)
        else {
            return Vec::new();
        };
        let updates: Vec<u64> = self
            .candidates
            .get(&(connection, quic_stream, subscription))
            .map(|candidates| candidates.updates.iter().copied().collect())
            .unwrap_or_default();
        updates
            .into_iter()
            .filter_map(|update| self.cross(key, (connection, quic_stream, update)))
            .collect()
    }

    /// The crossing update `key` completes with one of its subscription's
    /// endings, if any.
    fn complete_update(&mut self, key: FrameKey) -> Vec<Crossed> {
        let (connection, quic_stream, _) = key;
        let Some(subscription) = self.updates.get(&key).map(|update| update.subscription) else {
            return Vec::new();
        };
        let endings: Vec<u64> = self
            .candidates
            .get(&(connection, quic_stream, subscription))
            .map(|candidates| candidates.endings.iter().copied().collect())
            .unwrap_or_default();
        endings
            .into_iter()
            .find_map(|ending| self.cross((connection, quic_stream, ending), key))
            .into_iter()
            .collect()
    }

    /// The crossing of `ending` and `update`, if every fact is in, they
    /// name one subscription, the ending was written before the update's
    /// write returned and read after it, the node judged the update, and
    /// the update hasn't crossed already.
    fn cross(&mut self, ending: FrameKey, update: FrameKey) -> Option<Crossed> {
        let end = *self.endings.get(&ending)?;
        let up = *self.updates.get(&update)?;
        let (written, read, ended) = (end.written?, end.read?, end.subscription?);
        let sent = up.written?;
        let crossed = up.judged && ended == up.subscription && written < sent && sent < read;
        if !crossed || !self.reported.insert(update) {
            return None;
        }
        let (connection, quic_stream, seq) = update;
        Some(Crossed {
            connection,
            quic_stream,
            seq,
            subscription: up.subscription,
            label: up.label,
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

    /// The node admitted the header-sync session on `C`'s stream `Q`.
    fn session() -> Observation {
        Observation::Session {
            connection: C,
            peer: "peer-0".to_owned(),
            quic_stream: Q,
            kind: ZAKURA_STREAM_HEADER_SYNC,
            version: 9,
            session_id: 1,
        }
    }

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
            session(),
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
            session(),
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
            session(),
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
            session(),
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
            session(),
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
            session(),
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
            session(),
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
            session(),
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
            session(),
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

    /// A long run of ordinary traffic keeps nothing: the node's pages aren't
    /// the ending's type, and a peer frame its `PeerFrame` shows isn't an
    /// update leaves its later write and judgment nothing to attach to. So
    /// each fact costs a map lookup, not a scan.
    #[test]
    fn ordinary_traffic_leaves_no_candidates() {
        let mut join = CrossingJoin::default();
        // Another service's stream on the same connection, whose messages
        // reuse the ending's type, as discovery's `GetServices` and block
        // sync's `BlocksDone` do.
        let other = Q + 4;
        let discovery = Observation::Session {
            connection: C,
            peer: "peer-0".to_owned(),
            quic_stream: other,
            kind: ZAKURA_STREAM_HEADER_SYNC + 1,
            version: 1,
            session_id: 2,
        };
        for obs in [session(), discovery] {
            join.observe(&obs);
        }
        for seq in 0..10_000 {
            let reused = Observation::Transport {
                instance: 0,
                seq,
                node: "node".to_owned(),
                connection: Some(C),
                remote: "10.0.0.2:4000".to_owned(),
                event: TransportFact::FrameWritten {
                    quic_stream: other,
                    seq,
                    message_type: ENDING_TYPE,
                    order: 3 * seq + 3,
                },
            };
            let read = Observation::PeerReceived {
                connection: C,
                quic_stream: other,
                seq,
                message_type: ENDING_TYPE,
                payload: 9,
                credit: None,
                order: 3 * seq + 4,
            };
            for obs in [reused, read] {
                assert_eq!(join.observe(&obs), Vec::new());
            }
            let page = Observation::Transport {
                instance: 0,
                seq,
                node: "node".to_owned(),
                connection: Some(C),
                remote: "10.0.0.2:4000".to_owned(),
                event: TransportFact::FrameWritten {
                    quic_stream: Q,
                    seq,
                    message_type: 3,
                    order: 2 * seq + 1,
                },
            };
            let status = Observation::PeerFrame {
                connection: C,
                stream: STREAM,
                quic_stream: Some(Q),
                seq,
                row: Some("status".to_owned()),
                rule_stream: STREAM,
                label: Label::Conformant,
                exhausted: false,
                exchange: None,
                payload: 9,
            };
            // A peer's frame in the order the peer records it: its
            // `PeerFrame`, its write, then the node's judgment.
            for obs in [page, status, peer_wrote(seq, 2 * seq + 2), judged(seq)] {
                assert_eq!(join.observe(&obs), Vec::new());
            }
        }
        assert!(join.endings.is_empty(), "{:?}", join.endings.len());
        assert!(join.updates.is_empty());
        assert!(join.candidates.is_empty());
    }
}
