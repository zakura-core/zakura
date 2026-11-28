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

    /// Each inbound connection above the ordinary share needs a preferred
    /// place. Keep eligible incumbents in those places across a ranking change.
    pub(super) fn protect_inbound(
        &self,
        next: &mut PreferredGateways,
        incumbents: &[NodeId],
        inbound_limit: usize,
    ) {
        next.preserve_inbound_places(
            incumbents,
            self.inbound.saturating_sub(inbound_limit),
            |node| self.of_node(node).inbound > 0,
        );
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
    /// fills and no pending transport handshake is already trying for.
    pub(super) fn inbound_place_free(
        &self,
        cap: usize,
        preferred: &PreferredGateways,
        transport_pending: usize,
    ) -> bool {
        if self.total >= cap.saturating_add(preferred.hold()) {
            return false;
        }
        let split = self.split(preferred);
        let ordinary_free = split.ordinary < cap && split.ordinary_inbound < inbound_share(cap);
        ordinary_free || transport_pending < preferred.hold().saturating_sub(split.preferred)
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

    /// Claims an inbound slot for a caller whose proof changes the gateway ranking.
    ///
    /// A displaced member stays connected as ordinary. Outranking it can exceed
    /// the ordinary total temporarily. Making a gateway ambiguous also requires
    /// room under the ordinary cap. Both changes must preserve the dial reserve.
    pub(super) fn claim_under_new_ranking(
        &mut self,
        node: NodeId,
        cap: usize,
        held_under: &PreferredGateways,
        counted_under: &PreferredGateways,
    ) -> Result<SlotClass, SlotRefusal> {
        let before = self.split(held_under);
        let class = self.claim(node, true, cap, counted_under)?;
        let disputes = counted_under.ambiguous() > held_under.ambiguous();
        if let Err(refusal) = self.check_reclassification(before, cap, counted_under, disputes) {
            self.release(node, true);
            return Err(refusal);
        }
        Ok(class)
    }

    /// A proof may leave an existing excess from a ranking change in place.
    /// It cannot increase ordinary inbound use past its share. Dials and inbound
    /// proofs that make a gateway ambiguous must also respect the ordinary total.
    fn check_reclassification(
        &self,
        before: SlotSplit,
        cap: usize,
        counted_under: &PreferredGateways,
        enforce_ordinary_cap: bool,
    ) -> Result<(), SlotRefusal> {
        let after = self.split(counted_under);
        if enforce_ordinary_cap && after.ordinary > before.ordinary && after.ordinary > cap {
            return Err(SlotRefusal::Hold);
        }
        if after.ordinary_inbound > before.ordinary_inbound
            && after.ordinary_inbound > inbound_share(cap)
        {
            return Err(SlotRefusal::InboundShare);
        }
        Ok(())
    }

    /// Settles one outbound slot once its dial has proved which node it
    /// reached: the slot was claimed for `from` and belongs to `to` from
    /// here on. The totals don't change. When `from` is `to` the slot stays
    /// where it is and only the limit is checked.
    ///
    /// `held_under` is the preferred set the slot was held under, and
    /// `counted_under` the set as the proof re-forms it. The slot passed
    /// [`Self::claim`] for `from` and has to pass for `to` as well. A dial
    /// to a preferred gateway's ID takes a held place, which `to` keeps only
    /// if it is that gateway's node too, and a proof that makes a gateway
    /// ambiguous turns its member's slot into an ordinary one. The slot is
    /// refused if reclassification would increase the ordinary total or
    /// its inbound share past its limit. The dial then keeps its original charge.
    pub(super) fn settle_outbound(
        &mut self,
        from: NodeId,
        to: NodeId,
        cap: usize,
        held_under: &PreferredGateways,
        counted_under: &PreferredGateways,
    ) -> Result<(), SlotRefusal> {
        let before = self.split(held_under);
        if from != to {
            self.rekey_outbound(from, to);
        }
        if let Err(refusal) = self.check_reclassification(before, cap, counted_under, true) {
            if from != to {
                self.rekey_outbound(to, from);
            }
            return Err(refusal);
        }
        Ok(())
    }

    /// Charges one outbound slot of `from` to `to` instead.
    fn rekey_outbound(&mut self, from: NodeId, to: NodeId) {
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

    /// Moves a slot with no preferred set, so no move is refused.
    fn move_outbound(slots: &mut ConnectionSlots, from: NodeId, to: NodeId) {
        let none = PreferredGateways::default();
        slots
            .settle_outbound(from, to, 8, &none, &none)
            .expect("no held place to lose");
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

        move_outbound(&mut slots, a, b);
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
        move_outbound(&mut slots, a, b);
        assert!(!slots.by_node.contains_key(&a));
        assert_eq!(slots.of_node(&b).outbound, 1);
        assert_eq!(slots.total(), 1);
    }

    /// A member's only slot fills a held place. Moving it to a node outside
    /// the set makes it ordinary, which the ordinary limit has to allow.
    #[test]
    fn a_held_place_moves_only_into_a_free_ordinary_slot() {
        use crate::zakura::transport::{GatewayStanding, RankedGateway};
        let (member, other_member, moved, peer) = (node(1), node(2), node(3), node(4));
        let ranked = [member, other_member]
            .map(|node| RankedGateway::resolved(node, 1, GatewayStanding::Dialable));
        let preferred = PreferredGateways::form(ranked.to_vec(), 2);
        let mut slots = ConnectionSlots::default();
        assert_eq!(
            slots.claim(peer, true, 1, &preferred),
            Ok(SlotClass::Ordinary)
        );
        assert_eq!(
            slots.claim(member, false, 1, &preferred),
            Ok(SlotClass::Preferred)
        );

        assert_eq!(
            slots.settle_outbound(member, moved, 1, &preferred, &preferred),
            Err(SlotRefusal::Hold)
        );
        assert_eq!(slots.of_node(&member).outbound, 1, "nothing changed");
        assert_eq!(slots.of_node(&moved), SlotCount::default());

        // A move to another member keeps a held place.
        assert_eq!(
            slots.settle_outbound(member, other_member, 1, &preferred, &preferred),
            Ok(())
        );
        assert_eq!(slots.split(&preferred).preferred, 1);
        // With the ordinary slot free, the slot may become ordinary.
        slots.release(peer, true);
        assert_eq!(
            slots.settle_outbound(other_member, moved, 1, &preferred, &preferred),
            Ok(())
        );
        assert_eq!(
            slots.split(&preferred),
            SlotSplit {
                preferred: 0,
                ordinary: 1,
                ordinary_inbound: 0,
            }
        );
    }

    /// A slot that stays with its node is refused all the same when the
    /// proof re-forms the set so that a member's slot becomes ordinary above
    /// the limit, and passes when the set it is counted under keeps the
    /// ordinary slots where they were.
    #[test]
    fn a_proof_that_makes_a_members_slot_ordinary_needs_a_free_ordinary_slot() {
        use crate::zakura::transport::{GatewayStanding, RankedGateway};
        let (member, dialed) = (node(1), node(2));
        let held = PreferredGateways::form(
            [RankedGateway::resolved(
                member,
                1,
                GatewayStanding::Connected,
            )],
            1,
        );
        let ambiguous = PreferredGateways::form(
            [RankedGateway::resolved(member, 1, GatewayStanding::Connected).into_ambiguous()],
            1,
        );
        let mut slots = ConnectionSlots::default();
        assert_eq!(
            slots.claim(member, true, 1, &held),
            Ok(SlotClass::Preferred)
        );
        assert_eq!(
            slots.claim(dialed, false, 1, &held),
            Ok(SlotClass::Ordinary)
        );

        assert_eq!(
            slots.settle_outbound(dialed, dialed, 1, &held, &ambiguous),
            Err(SlotRefusal::Hold),
            "the member's slot would be a second ordinary one"
        );
        assert_eq!(
            slots.settle_outbound(dialed, dialed, 1, &held, &held),
            Ok(())
        );
        assert_eq!(
            slots.settle_outbound(dialed, dialed, 2, &held, &ambiguous),
            Ok(())
        );
        assert_eq!(slots.of_node(&dialed).outbound, 1);
    }

    /// A caller fills a held place of its own while its proof makes another
    /// gateway ambiguous. That gateway's member keeps its slot as an
    /// ordinary one, which the ordinary limit has to allow, or the caller
    /// is refused and no slot changes.
    #[test]
    fn a_caller_whose_proof_makes_a_members_slot_ordinary_needs_a_free_ordinary_slot() {
        use crate::zakura::transport::{GatewayStanding, RankedGateway};
        let (member, caller, peer, other) = (node(1), node(2), node(3), node(4));
        // A third gateway keeps two places held when the member's gateway
        // turns ambiguous, so the total limit doesn't refuse the caller.
        let ranked = |member_gateway: RankedGateway| {
            let caller_gateway = RankedGateway::resolved(caller, 2, GatewayStanding::Connected);
            let other_gateway = RankedGateway::resolved(other, 1, GatewayStanding::Dialable);
            PreferredGateways::form([member_gateway, caller_gateway, other_gateway], 2)
        };
        let member_gateway = RankedGateway::resolved(member, 3, GatewayStanding::Connected);
        let held = ranked(member_gateway);
        let disputed = ranked(member_gateway.into_ambiguous());
        let mut slots = ConnectionSlots::default();
        assert_eq!(slots.claim(peer, false, 1, &held), Ok(SlotClass::Ordinary));
        assert_eq!(
            slots.claim(member, true, 1, &held),
            Ok(SlotClass::Preferred)
        );

        assert_eq!(
            slots.claim(caller, true, 1, &disputed),
            Ok(SlotClass::Preferred),
            "the plain claim sees only the caller's own held place"
        );
        slots.release(caller, true);
        assert_eq!(
            slots.claim_under_new_ranking(caller, 1, &held, &disputed),
            Err(SlotRefusal::Hold),
            "the member's slot would be a second ordinary one"
        );
        assert_eq!(slots.of_node(&caller), SlotCount::default());
        assert_eq!((slots.total(), slots.inbound()), (2, 1));

        slots.release(peer, false);
        assert_eq!(
            slots.claim_under_new_ranking(caller, 1, &held, &disputed),
            Ok(SlotClass::Preferred)
        );
        assert_eq!(
            slots.split(&disputed),
            SlotSplit {
                preferred: 1,
                ordinary: 1,
                ordinary_inbound: 1,
            }
        );
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
