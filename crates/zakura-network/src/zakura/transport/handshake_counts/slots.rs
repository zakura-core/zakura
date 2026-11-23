//! Connection slots, counted per remote node ID.
//!
//! A slot is held from admission until the connection's serving task exits.
//! Keeping each node's slots, not only the totals, lets a caller ask how a
//! connected node is counted while it stays connected.

use zakura_quic::NodeId;
use zakura_rt::collections::HashMap;

use super::inbound_share;

/// The connection slots one remote node holds, by direction.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SlotCount {
    /// Slots of connections the node opened to this node.
    pub inbound: usize,
    /// Slots of connections this node opened to the node.
    pub outbound: usize,
}

impl SlotCount {
    fn of(&mut self, inbound: bool) -> &mut usize {
        if inbound {
            &mut self.inbound
        } else {
            &mut self.outbound
        }
    }

    /// Slots in both directions.
    pub fn total(self) -> usize {
        self.inbound.saturating_add(self.outbound)
    }
}

/// Every held connection slot, by node, with running totals.
#[derive(Debug, Default)]
pub(super) struct ConnectionSlots {
    by_node: HashMap<NodeId, SlotCount>,
    total: usize,
    inbound: usize,
}

impl ConnectionSlots {
    pub(super) fn total(&self) -> usize {
        self.total
    }

    pub(super) fn inbound(&self) -> usize {
        self.inbound
    }

    /// Whether a slot in this direction would exceed `cap` or, for an
    /// inbound slot, the inbound share of `cap`.
    pub(super) fn full(&self, inbound: bool, cap: usize) -> bool {
        self.total >= cap || (inbound && self.inbound >= inbound_share(cap))
    }

    /// Checks the limits and charges one slot to `node` in one step, so
    /// claims made under one lock never exceed them.
    pub(super) fn claim(&mut self, node: NodeId, inbound: bool, cap: usize) -> bool {
        if self.full(inbound, cap) {
            return false;
        }
        *self.by_node.entry(node).or_default().of(inbound) += 1;
        self.total += 1;
        if inbound {
            self.inbound += 1;
        }
        true
    }

    /// Releases one slot that [`Self::claim`] charged to `node`.
    pub(super) fn release(&mut self, node: NodeId, inbound: bool) {
        let slots = self
            .by_node
            .get_mut(&node)
            .expect("a live connection's node keeps its slot entry until its last slot releases");
        let count = slots.of(inbound);
        *count = count
            .checked_sub(1)
            .expect("a live connection owns its slot in its direction");
        if slots.total() == 0 {
            self.by_node.remove(&node);
        }
        self.total = self
            .total
            .checked_sub(1)
            .expect("a live connection owns its charge");
        if inbound {
            self.inbound = self
                .inbound
                .checked_sub(1)
                .expect("an inbound connection owns its inbound charge");
        }
    }

    /// The slots `node` holds now.
    pub(super) fn of_node(&self, node: &NodeId) -> SlotCount {
        self.by_node.get(node).copied().unwrap_or_default()
    }

    /// The slots of every node added up, which equal the totals while every
    /// claim and release keeps both.
    pub(super) fn summed(&self) -> SlotCount {
        self.by_node
            .values()
            .fold(SlotCount::default(), |sum, slots| SlotCount {
                inbound: sum.inbound.saturating_add(slots.inbound),
                outbound: sum.outbound.saturating_add(slots.outbound),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(seed: u8) -> NodeId {
        zakura_quic::NodeSecretKey::from_bytes(&[seed; 32]).public()
    }

    #[test]
    fn claim_and_release_track_one_node() {
        let mut slots = ConnectionSlots::default();
        let a = node(1);
        assert!(slots.claim(a, true, 8));
        assert_eq!(
            slots.of_node(&a),
            SlotCount {
                inbound: 1,
                outbound: 0
            }
        );
        assert_eq!((slots.total(), slots.inbound()), (1, 1));
        slots.release(a, true);
        assert_eq!(slots.of_node(&a), SlotCount::default());
        assert_eq!((slots.total(), slots.inbound()), (0, 0));
        assert!(
            slots.by_node.is_empty(),
            "a node with no slots is forgotten"
        );
    }

    #[test]
    fn two_slots_of_one_node_count_twice_and_release_independently() {
        let mut slots = ConnectionSlots::default();
        let a = node(1);
        let b = node(2);
        assert!(slots.claim(a, true, 8));
        assert!(slots.claim(a, false, 8));
        assert!(slots.claim(a, false, 8));
        assert!(slots.claim(b, false, 8));
        assert_eq!(
            slots.of_node(&a),
            SlotCount {
                inbound: 1,
                outbound: 2
            }
        );
        assert_eq!((slots.total(), slots.inbound()), (4, 1));

        slots.release(a, false);
        assert_eq!(
            slots.of_node(&a),
            SlotCount {
                inbound: 1,
                outbound: 1
            }
        );
        slots.release(a, true);
        assert_eq!(
            slots.of_node(&a),
            SlotCount {
                inbound: 0,
                outbound: 1
            }
        );
        assert_eq!(
            slots.of_node(&b),
            SlotCount {
                inbound: 0,
                outbound: 1
            },
            "another node's slot is untouched"
        );
        assert_eq!((slots.total(), slots.inbound()), (2, 0));
        assert_eq!(
            slots.summed(),
            SlotCount {
                inbound: 0,
                outbound: 2
            }
        );
    }

    /// A cap of 8 leaves 6 slots to inbound connections and keeps 2 for dials.
    #[test]
    fn inbound_share_and_total_cap_refuse_further_slots() {
        let mut slots = ConnectionSlots::default();
        assert_eq!(inbound_share(8), 6);
        for seed in 0..6 {
            assert!(slots.claim(node(seed), true, 8));
        }
        assert!(!slots.claim(node(6), true, 8), "the inbound share is full");
        assert_eq!(slots.of_node(&node(6)), SlotCount::default());
        assert!(slots.claim(node(6), false, 8));
        assert!(slots.claim(node(7), false, 8));
        assert!(!slots.claim(node(8), false, 8), "the total cap is full");
        assert_eq!((slots.total(), slots.inbound()), (8, 6));

        // Releasing one inbound slot makes room for exactly one more.
        slots.release(node(0), true);
        assert!(slots.claim(node(9), true, 8));
        assert!(!slots.claim(node(10), false, 8));
        assert_eq!(slots.summed().total(), slots.total());
    }
}
