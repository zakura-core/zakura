//! The discovery book's nodes in node ID order, so a gateway ID from the
//! miner mesh finds the nodes it names without a scan of the book.
//!
//! The book updates this index whenever it adds or drops a node, under the
//! same lock.

use std::collections::BTreeSet;

use zakura_chain::miner_mesh::GatewayId;
use zakura_quic::NodeId;

use crate::zakura::GatewayMatch;

/// Every node the book holds, by a signed record or as a static candidate.
///
/// A `NodeId` orders as its bytes, so the node IDs a gateway ID is a prefix
/// of are one range of this set.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct GatewayIndex {
    nodes: BTreeSet<NodeId>,
}

impl GatewayIndex {
    /// Records that the book holds `node`.
    pub(super) fn insert(&mut self, node: NodeId) {
        self.nodes.insert(node);
    }

    /// Records that the book no longer holds `node`.
    pub(super) fn remove(&mut self, node: &NodeId) {
        self.nodes.remove(node);
    }

    /// Returns the nodes the book holds whose ID starts with `gateway`.
    pub(super) fn find(&self, gateway: &GatewayId) -> GatewayMatch<NodeId> {
        let (lowest, highest) = gateway.node_id_bounds();
        let mut nodes = self
            .nodes
            .range(NodeId::from_bytes(&lowest)..=NodeId::from_bytes(&highest));
        match (nodes.next(), nodes.next()) {
            (None, _) => GatewayMatch::None,
            (Some(node), None) => GatewayMatch::One(*node),
            (Some(_), Some(_)) => GatewayMatch::Several,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A node ID whose first 20 bytes are `prefix` and whose last byte is
    /// `last`.
    fn node(prefix: u8, last: u8) -> NodeId {
        let mut bytes = [prefix; 32];
        bytes[31] = last;
        NodeId::from_bytes(&bytes)
    }

    fn gateway(prefix: u8) -> GatewayId {
        GatewayId::from_node_id(node(prefix, 0).as_bytes())
    }

    #[test]
    fn a_gateway_id_finds_the_one_node_it_starts() {
        let mut index = GatewayIndex::default();
        index.insert(node(1, 7));
        index.insert(node(2, 7));

        assert_eq!(index.find(&gateway(1)), GatewayMatch::One(node(1, 7)));
        assert_eq!(index.find(&gateway(3)), GatewayMatch::None);
    }

    #[test]
    fn two_nodes_with_one_gateway_id_are_several() {
        let mut index = GatewayIndex::default();
        index.insert(node(1, 0x00));
        index.insert(node(1, 0xff));

        assert_eq!(index.find(&gateway(1)), GatewayMatch::Several);

        index.remove(&node(1, 0x00));
        assert_eq!(index.find(&gateway(1)), GatewayMatch::One(node(1, 0xff)));
    }
}
