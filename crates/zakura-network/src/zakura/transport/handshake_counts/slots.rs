//! Connection slots, counted per remote node ID.
//!
//! A slot is held from admission until the connection's serving task exits.
//! Keeping each node's slots, not only the totals, lets a caller ask how a
//! connected node is counted while it stays connected.
//!
//! Slots split into preferred and ordinary ones. Each member of the
//! preferred set that holds a slot counts one preferred slot, and every
//! other slot is ordinary. The split is derived from the set at each check
//! and never stored, so a set change only changes how held slots count.

use zakura_quic::NodeId;
use zakura_rt::collections::HashMap;

use super::inbound_share;
use crate::zakura::transport::PreferredGateways;

/// How a granted connection slot was counted when it was claimed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotClass {
    /// The first slot of a preferred-set member, in a held place.
    Preferred,
    /// Any other slot.
    Ordinary,
}

/// Why a connection slot was refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotRefusal {
    /// Every slot is in use.
    Full,
    /// The ordinary slots are full: the rest are held for preferred
    /// gateways.
    Hold,
    /// The ordinary inbound slots fill the inbound share of the limit.
    InboundShare,
}

/// Slots in use, split into preferred and ordinary ones.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SlotSplit {
    /// Preferred-set members that hold a slot, one each.
    pub preferred: usize,
    /// Every other slot.
    pub ordinary: usize,
    /// The inbound slots among the ordinary ones. By convention a member
    /// that holds any inbound slot counts its preferred slot as inbound, so
    /// a gateway above the inbound share takes none of the share's slots.
    pub ordinary_inbound: usize,
}

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
    pub(super) fn nodes(&self) -> impl Iterator<Item = &NodeId> {
        self.by_node.keys()
    }

    pub(super) fn total(&self) -> usize {
        self.total
    }

    pub(super) fn inbound(&self) -> usize {
        self.inbound
    }

    /// Splits the slots in use by `preferred`'s set, with one lookup per
    /// member.
    pub(super) fn split(&self, preferred: &PreferredGateways) -> SlotSplit {
        let (mut members, mut inbound_members) = (0usize, 0usize);
        for member in preferred.set() {
            let slots = self.of_node(member);
            if slots.total() > 0 {
                members += 1;
                if slots.inbound > 0 {
                    inbound_members += 1;
                }
            }
        }
        SlotSplit {
            preferred: members,
            ordinary: self
                .total
                .checked_sub(members)
                .expect("each counted member holds at least one slot"),
            ordinary_inbound: self
                .inbound
                .checked_sub(inbound_members)
                .expect("each counted inbound member holds at least one inbound slot"),
        }
    }

    /// Whether a slot is free for an inbound caller whose node ID is not
    /// known yet: an ordinary inbound slot, or a held place that no member
    /// fills. With no hold this is the ordinary inbound check alone.
    pub(super) fn inbound_place_free(&self, cap: usize, preferred: &PreferredGateways) -> bool {
        if self.total >= cap.saturating_add(preferred.hold()) {
            return false;
        }
        let split = self.split(preferred);
        let ordinary_free = split.ordinary < cap && split.ordinary_inbound < inbound_share(cap);
        ordinary_free || split.preferred < preferred.hold()
    }

    /// Remaining ordinary places after counting live connections under the current ranking.
    pub(super) fn free_ordinary(&self, cap: usize, preferred: &PreferredGateways) -> usize {
        cap.saturating_sub(self.split(preferred).ordinary)
    }

    /// Checks the limits and charges one slot to `node` in one step, so
    /// claims made under one lock never exceed them.
    ///
    /// A claim may bring the total up to the ordinary cap plus the active hold.
    /// Each preferred gateway can use one additional place. Ordinary claims
    /// also respect the ordinary cap and, for inbound calls, its inbound share.
    pub(super) fn claim(
        &mut self,
        node: NodeId,
        inbound: bool,
        cap: usize,
        preferred: &PreferredGateways,
    ) -> Result<SlotClass, SlotRefusal> {
        if self.total >= cap.saturating_add(preferred.hold()) {
            return Err(SlotRefusal::Full);
        }
        let class = if preferred.contains(&node) && self.of_node(&node).total() == 0 {
            SlotClass::Preferred
        } else {
            let split = self.split(preferred);
            if split.ordinary >= cap {
                return Err(SlotRefusal::Hold);
            }
            if inbound && split.ordinary_inbound >= inbound_share(cap) {
                return Err(SlotRefusal::InboundShare);
            }
            SlotClass::Ordinary
        };
        *self.by_node.entry(node).or_default().of(inbound) += 1;
        self.total += 1;
        if inbound {
            self.inbound += 1;
        }
        Ok(class)
    }

    /// Moves one outbound slot from `from` to `to`: a dial to `from` reached
    /// the node whose ID is `to`. The totals don't change.
    pub(super) fn move_outbound(&mut self, from: NodeId, to: NodeId) {
        let slots = self
            .by_node
            .get_mut(&from)
            .expect("a dial's target keeps its slot entry until the dial's slot leaves");
        slots.outbound = slots
            .outbound
            .checked_sub(1)
            .expect("a dial owns its outbound slot");
        if slots.total() == 0 {
            self.by_node.remove(&from);
        }
        self.by_node.entry(to).or_default().outbound += 1;
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

    /// Claims with no preferred set, as every claim did before held places.
    fn claim(slots: &mut ConnectionSlots, node: NodeId, inbound: bool, cap: usize) -> bool {
        slots
            .claim(node, inbound, cap, &PreferredGateways::default())
            .is_ok()
    }

    #[test]
    fn claim_and_release_track_one_node() {
        let mut slots = ConnectionSlots::default();
        let a = node(1);
        assert!(claim(&mut slots, a, true, 8));
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

    /// A dial to `a` reached the node whose ID is `b`: the dial's slot moves
    /// to `b`, the totals stay, and the slot releases under `b`.
    #[test]
    fn a_moved_outbound_slot_keeps_the_totals_and_releases_under_its_new_node() {
        let mut slots = ConnectionSlots::default();
        let (a, b) = (node(1), node(2));
        assert!(claim(&mut slots, a, false, 8));
        assert!(claim(&mut slots, a, true, 8));
        assert!(claim(&mut slots, b, true, 8));

        slots.move_outbound(a, b);
        assert_eq!(
            slots.of_node(&a),
            SlotCount {
                inbound: 1,
                outbound: 0
            }
        );
        assert_eq!(
            slots.of_node(&b),
            SlotCount {
                inbound: 1,
                outbound: 1
            }
        );
        assert_eq!((slots.total(), slots.inbound()), (3, 2));
        assert_eq!(slots.summed().total(), slots.total());

        slots.release(b, false);
        slots.release(a, true);
        assert_eq!(slots.of_node(&a), SlotCount::default());
        assert!(
            !slots.by_node.contains_key(&a),
            "a node with no slots is forgotten"
        );
        assert_eq!((slots.total(), slots.inbound()), (1, 1));
    }

    /// The only slot of the dialed node moves away, and the node is forgotten.
    #[test]
    fn moving_a_nodes_last_slot_forgets_the_node() {
        let mut slots = ConnectionSlots::default();
        let (a, b) = (node(1), node(2));
        assert!(claim(&mut slots, a, false, 8));
        slots.move_outbound(a, b);
        assert!(!slots.by_node.contains_key(&a));
        assert_eq!(slots.of_node(&b).outbound, 1);
        assert_eq!(slots.total(), 1);
    }

    #[test]
    fn two_slots_of_one_node_count_twice_and_release_independently() {
        let mut slots = ConnectionSlots::default();
        let a = node(1);
        let b = node(2);
        assert!(claim(&mut slots, a, true, 8));
        assert!(claim(&mut slots, a, false, 8));
        assert!(claim(&mut slots, a, false, 8));
        assert!(claim(&mut slots, b, false, 8));
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
            assert!(claim(&mut slots, node(seed), true, 8));
        }
        assert!(
            !claim(&mut slots, node(6), true, 8),
            "the inbound share is full"
        );
        assert_eq!(slots.of_node(&node(6)), SlotCount::default());
        assert!(claim(&mut slots, node(6), false, 8));
        assert!(claim(&mut slots, node(7), false, 8));
        assert!(
            !claim(&mut slots, node(8), false, 8),
            "the total cap is full"
        );
        assert_eq!((slots.total(), slots.inbound()), (8, 6));

        // Releasing one inbound slot makes room for exactly one more.
        slots.release(node(0), true);
        assert!(claim(&mut slots, node(9), true, 8));
        assert!(!claim(&mut slots, node(10), false, 8));
        assert_eq!(slots.summed().total(), slots.total());
    }

    /// A member counts one preferred slot however many it holds, and that
    /// slot is inbound when the member holds an inbound one.
    #[test]
    fn a_member_counts_one_preferred_slot() {
        use crate::zakura::transport::{GatewayStanding, RankedGateway};

        let mut slots = ConnectionSlots::default();
        let member = node(1);
        let preferred = PreferredGateways::form(
            [RankedGateway::resolved(
                member,
                1,
                GatewayStanding::Connected,
            )],
            1,
        );
        assert_eq!(
            slots.claim(member, false, 8, &preferred),
            Ok(SlotClass::Preferred)
        );
        assert_eq!(
            slots.claim(member, true, 8, &preferred),
            Ok(SlotClass::Ordinary),
            "a second slot of a member is ordinary"
        );
        assert_eq!(
            slots.split(&preferred),
            SlotSplit {
                preferred: 1,
                ordinary: 1,
                ordinary_inbound: 0,
            }
        );
        assert_eq!(
            slots.split(&PreferredGateways::default()),
            SlotSplit {
                preferred: 0,
                ordinary: 2,
                ordinary_inbound: 1,
            },
            "with no set every slot is ordinary"
        );
    }
}
