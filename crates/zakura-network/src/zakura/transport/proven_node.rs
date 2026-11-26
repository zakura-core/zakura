//! A connected node as its TLS handshake proved it.
//!
//! A node's ID is the hash of its key list, so a node that adds a key has a
//! new ID. Its handshake still proves the IDs it had before, while it signs
//! with the keys those IDs name (zakura-quic TLS-7). Code that knows a node
//! by an ID, such as a pinned peer entry or a mining gateway's ID in a
//! block, asks this type whether a connection reaches that node.

use zakura_quic::{NodeId, MAX_EARLIER_NODE_IDS};

/// The node ID a connection's peer has now, and the earlier IDs its
/// handshake proved it still answers to.
///
/// The earlier IDs are those of every shorter list of the peer's keys that
/// keeps its signing keys. The IDs the node had before it added keys are
/// among them, and nobody without the signing keys can prove any of them.
///
/// Only zakura-quic decides which earlier IDs a handshake proves, and it
/// reports at most [`MAX_EARLIER_NODE_IDS`] of them. A node ID alone
/// converts to a node that proved no earlier ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProvenNode<'a> {
    id: NodeId,
    earlier: &'a [NodeId],
}

impl<'a> ProvenNode<'a> {
    /// The node whose ID is `id` now and whose handshake proved `earlier`,
    /// as `Conn::remote_id` and `Conn::remote_earlier_ids` report them.
    ///
    /// Keeps at most [`MAX_EARLIER_NODE_IDS`] earlier IDs, so what a table
    /// stores for a node stays bounded whatever a caller passes.
    pub fn new(id: NodeId, earlier: &'a [NodeId]) -> Self {
        let earlier = &earlier[..earlier.len().min(MAX_EARLIER_NODE_IDS)];
        Self { id, earlier }
    }

    /// The ID the node has now, which its connection registers under.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// The IDs the node had before it added keys.
    pub fn earlier(&self) -> &'a [NodeId] {
        self.earlier
    }

    /// Every ID the node answers to: the one it has now, then the earlier
    /// ones.
    pub fn ids(&self) -> impl Iterator<Item = NodeId> + 'a {
        std::iter::once(self.id).chain(self.earlier.iter().copied())
    }

    /// Whether `id` is an ID the node had before it added keys.
    pub fn had(&self, id: &NodeId) -> bool {
        self.earlier.contains(id)
    }
}

impl From<NodeId> for ProvenNode<'static> {
    fn from(id: NodeId) -> Self {
        Self { id, earlier: &[] }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(byte: u8) -> NodeId {
        NodeId::from_bytes(&[byte; 32])
    }

    #[test]
    fn a_node_answers_to_its_id_and_its_earlier_ids() {
        let earlier = [id(1), id(2)];
        let node = ProvenNode::new(id(3), &earlier);
        assert_eq!(node.ids().collect::<Vec<_>>(), [id(3), id(1), id(2)]);
        assert!(node.had(&id(1)));
        assert!(!node.had(&id(3)), "the current ID is not an earlier one");
        assert!(!node.had(&id(4)));

        let plain = ProvenNode::from(id(3));
        assert_eq!(plain.ids().collect::<Vec<_>>(), [id(3)]);
        assert!(!plain.had(&id(1)));
    }

    #[test]
    fn a_node_keeps_no_more_earlier_ids_than_a_handshake_proves() {
        let many: Vec<NodeId> = (0..).take(MAX_EARLIER_NODE_IDS + 2).map(id).collect();
        let node = ProvenNode::new(id(0xFF), &many);
        assert_eq!(node.earlier(), &many[..MAX_EARLIER_NODE_IDS]);
    }
}
