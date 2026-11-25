//! Node IDs that a dial found the node has moved on from.
//!
//! A node's ID is the hash of its key list, so a node that adds a key has a
//! new ID. A dial to its earlier ID still reaches it (zakura-quic TLS-7), and
//! the connection then reports the ID the node has now. This table remembers
//! each such pair, so code that asks "is the node I know as A connected?"
//! finds the connection registered under B.
//!
//! Only this node's own completed dials add entries, and the table is not
//! saved: after a restart, one dial to the earlier ID learns the pair again.

use std::collections::VecDeque;

use zakura_quic::NodeId;
use zakura_rt::{collections::HashMap, sync::watch, time::Instant};

use crate::zakura::{ZakuraPeerId, DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF};

/// The most moves the table remembers. A forgotten move costs one redundant
/// dial, which is closed as soon as it shows the node is already connected.
pub(crate) const MAX_MOVED_NODE_IDS: usize = 1_024;

/// The ID each moved node answered a dial with, by the ID that was dialed.
#[derive(Debug, Default)]
pub(crate) struct MovedNodeIds {
    current: HashMap<NodeId, LearnedMove>,
    /// The dialed IDs in `current`, oldest first.
    learned: VecDeque<NodeId>,
}

#[derive(Debug)]
struct LearnedMove {
    connected: NodeId,
    /// Earliest time a held place may be used to check whether this move still applies.
    recheck_after: Instant,
}

impl MovedNodeIds {
    /// The ID that the node dialed as `id` last answered with, or `id` when
    /// no dial has found it under another.
    pub(crate) fn current(&self, id: NodeId) -> NodeId {
        self.current.get(&id).map_or(id, |entry| entry.connected)
    }

    /// Records that a dial to `dialed` reached the node whose ID is
    /// `connected`, replacing what an earlier dial to `dialed` found. A dial
    /// answered under the dialed ID itself forgets the move. Returns whether
    /// the table changed.
    pub(crate) fn learn(&mut self, dialed: NodeId, connected: NodeId) -> bool {
        if dialed == connected {
            return self.forget(&dialed);
        }
        let entry = LearnedMove {
            connected,
            recheck_after: Instant::now() + DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF,
        };
        match self.current.insert(dialed, entry) {
            Some(previous) => previous.connected != connected,
            None => {
                self.learned.push_back(dialed);
                if self.learned.len() > MAX_MOVED_NODE_IDS {
                    let oldest = self
                        .learned
                        .pop_front()
                        .expect("a queue over the bound holds an entry");
                    self.current.remove(&oldest);
                }
                true
            }
        }
    }

    /// Grants at most one retry per backoff interval under the original ID.
    /// A preferred gateway can then reclaim its place after a key rollback,
    /// even if the ordinary slots needed by its last known ID stay full.
    pub(crate) fn take_recheck(&mut self, dialed: NodeId, expected: NodeId, now: Instant) -> bool {
        let Some(entry) = self.current.get_mut(&dialed) else {
            return false;
        };
        if entry.connected != expected || now < entry.recheck_after {
            return false;
        }
        entry.recheck_after = now + DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF;
        true
    }

    /// Forgets the move learned for `id`. Returns whether there was one.
    pub(crate) fn forget(&mut self, id: &NodeId) -> bool {
        if self.current.remove(id).is_none() {
            return false;
        }
        self.learned.retain(|learned| learned != id);
        true
    }

    /// Whether `holds` is true of the node known as `id`, under that ID or
    /// the one it answers with now. This is the one way to ask whether a
    /// node known by an ID is connected, in flight or otherwise accounted
    /// for, since the accounts are kept under the ID a node answers with.
    pub(crate) fn under_either_id(&self, id: NodeId, holds: impl Fn(&NodeId) -> bool) -> bool {
        holds(&id)
            || self
                .current
                .get(&id)
                .is_some_and(|entry| holds(&entry.connected))
    }

    /// Whether the node known as `peer` is among `registered`, under that ID
    /// or the one it moved to.
    pub(crate) fn names_registered(
        &self,
        peer: &ZakuraPeerId,
        registered: &[ZakuraPeerId],
    ) -> bool {
        // A legacy peer id is not a node ID and never moves.
        let Some(id) = peer.node_id() else {
            return registered.contains(peer);
        };
        self.under_either_id(id, |id| {
            registered
                .iter()
                .any(|peer| peer.as_bytes() == id.as_bytes())
        })
    }
}

/// The supervisor's registered peers, read together with the moves that say
/// which of them a node's earlier ID names.
///
/// [`Self::changed`] waits for the registered peers alone. A dial records a
/// move before its connection registers, so a reader woken by a registration
/// sees the move behind it. A move learned for a node that is already
/// registered wakes nobody: a reader that was waiting for the node sees it
/// the next time it asks.
#[derive(Clone, Debug)]
pub(crate) struct RegisteredPeers {
    registered: watch::Receiver<Vec<ZakuraPeerId>>,
    moved: watch::Receiver<MovedNodeIds>,
}

impl RegisteredPeers {
    pub(crate) fn new(
        registered: watch::Receiver<Vec<ZakuraPeerId>>,
        moved: watch::Receiver<MovedNodeIds>,
    ) -> Self {
        Self { registered, moved }
    }

    /// Whether the node known as `peer` is registered now, under that ID or
    /// the one it moved to.
    pub(crate) fn names(&self, peer: &ZakuraPeerId) -> bool {
        self.moved
            .borrow()
            .names_registered(peer, &self.registered.borrow())
    }

    /// [`Self::names`], marking the registered peers seen, so
    /// [`Self::changed`] waits for the next change after this answer.
    pub(crate) fn names_and_mark_seen(&mut self, peer: &ZakuraPeerId) -> bool {
        self.moved
            .borrow()
            .names_registered(peer, &self.registered.borrow_and_update())
    }

    /// Marks registrations seen before sampling an attempt's TLS report and checking its peer.
    pub(crate) fn mark_seen(&mut self) {
        self.registered.borrow_and_update();
    }

    /// Whether the exact ID authenticated by this dial is registered.
    pub(crate) fn contains_authenticated(&self, id: NodeId) -> bool {
        self.registered
            .borrow()
            .iter()
            .any(|peer| peer.node_id() == Some(id))
    }

    /// Waits until the registered peers change. Fails once the supervisor is
    /// gone.
    pub(crate) async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.registered.changed().await
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use proptest::prelude::*;

    use super::*;

    /// A moves watch that never learns a move, for a test of code that
    /// reads one.
    pub(crate) fn no_moves() -> watch::Receiver<MovedNodeIds> {
        watch::channel(MovedNodeIds::default()).1
    }

    fn id(byte: u8) -> NodeId {
        NodeId::from_bytes(&[byte; 32])
    }

    fn peer(byte: u8) -> ZakuraPeerId {
        ZakuraPeerId::from_node_id(id(byte))
    }

    #[test]
    fn a_learned_move_resolves_the_dialed_id() {
        let mut moved = MovedNodeIds::default();
        assert_eq!(moved.current(id(1)), id(1));

        assert!(moved.learn(id(1), id(2)));
        assert!(!moved.learn(id(1), id(2)), "the same move changes nothing");
        assert_eq!(moved.current(id(1)), id(2));
        assert_eq!(moved.current(id(2)), id(2), "a move is not followed back");

        // The node added another key: a dial to the first ID finds the third.
        assert!(moved.learn(id(1), id(3)));
        assert_eq!(moved.current(id(1)), id(3));
    }

    #[test]
    fn only_one_dial_can_recheck_a_move_after_the_backoff() {
        let mut moves = MovedNodeIds::default();
        let (old, current) = (id(1), id(2));
        moves.learn(old, current);
        let now = Instant::now();
        assert!(!moves.take_recheck(old, current, now));
        let due = now + DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF;
        assert!(!moves.take_recheck(old, id(3), due));
        assert!(moves.take_recheck(old, current, due));
        assert!(!moves.take_recheck(old, current, due));
        assert_eq!(moves.current(old), current);
        assert!(moves.take_recheck(old, current, due + DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF));
    }

    #[test]
    fn a_dial_answered_under_its_own_id_forgets_the_move() {
        let mut moved = MovedNodeIds::default();
        assert!(!moved.learn(id(1), id(1)));
        moved.learn(id(1), id(2));
        assert!(moved.learn(id(1), id(1)));
        assert_eq!(moved.current(id(1)), id(1));
        assert!(!moved.forget(&id(1)));
    }

    #[test]
    fn a_moved_peer_is_registered_under_either_id() {
        let mut moved = MovedNodeIds::default();
        assert!(moved.names_registered(&peer(1), &[peer(1)]));
        assert!(!moved.names_registered(&peer(1), &[peer(2)]));

        moved.learn(id(1), id(2));
        assert!(moved.names_registered(&peer(1), &[peer(2)]));
        assert!(moved.names_registered(&peer(1), &[peer(1)]));
        assert!(!moved.names_registered(&peer(1), &[peer(3)]));
        assert!(
            !moved.names_registered(&peer(2), &[peer(1)]),
            "the new ID doesn't stand for the old one"
        );
        // A legacy peer id is not a node ID and never moves.
        let legacy = ZakuraPeerId::new(vec![7; 20]).unwrap();
        assert!(!moved.names_registered(&legacy, &[peer(2)]));
    }

    #[test]
    fn the_oldest_move_makes_room() {
        let mut moved = MovedNodeIds::default();
        let dialed = |index: usize| {
            let mut bytes = [0; 32];
            bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
            NodeId::from_bytes(&bytes)
        };
        for index in 0..=MAX_MOVED_NODE_IDS {
            moved.learn(dialed(index), id(0xFF));
        }
        assert_eq!(moved.current(dialed(0)), dialed(0), "the oldest is gone");
        assert_eq!(moved.current(dialed(1)), id(0xFF));
        assert_eq!(moved.current(dialed(MAX_MOVED_NODE_IDS)), id(0xFF));
        assert_eq!(moved.current.len(), MAX_MOVED_NODE_IDS);
    }

    proptest! {
        /// Whatever is learned and forgotten, the table stays within its
        /// bound and its queue lists exactly the moves it holds.
        #[test]
        fn the_table_stays_bounded_and_consistent(
            steps in prop::collection::vec((any::<u16>(), any::<u16>(), any::<bool>()), 0..2_500)
        ) {
            let node = |value: u16| {
                let mut bytes = [0; 32];
                bytes[..2].copy_from_slice(&value.to_le_bytes());
                NodeId::from_bytes(&bytes)
            };
            let mut moved = MovedNodeIds::default();
            for (dialed, connected, forget) in steps {
                if forget {
                    moved.forget(&node(dialed));
                } else {
                    moved.learn(node(dialed), node(connected));
                }
                prop_assert!(moved.current.len() <= MAX_MOVED_NODE_IDS);
                prop_assert_eq!(moved.current.len(), moved.learned.len());
            }
            prop_assert!(moved.learned.iter().all(|id| moved.current.contains_key(id)));
        }
    }
}
