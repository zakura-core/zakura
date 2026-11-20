//! How a config entry names a peer node.

use std::{fmt, net::SocketAddr, str::FromStr};

use zakura_quic::{KeyParsingError, NodeAddr, NodeId, NodeKey};

/// The prefix that marks a config entry's node as a node ID.
const NODE_ID_PREFIX: &str = "id:";

/// A peer node as a config entry names it, in `zakura.bootstrap_peers` and in
/// a pinned `network.unconditional_peers` name.
///
/// It prints in the form it was written in, so a config round-trips.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum ConfiguredNode {
    /// `<64 hex characters>`: the node whose only key is this Ed25519 key.
    /// This is the form every release has written, so existing configs keep
    /// their meaning.
    Key(NodeKey),
    /// `id:<64 hex characters>`: the node with this node ID. A node with
    /// more than one key can only be named this way, because its ID is the
    /// hash of all of them.
    Id(NodeId),
}

impl ConfiguredNode {
    /// The node's ID.
    pub fn node_id(&self) -> NodeId {
        match self {
            Self::Key(key) => key.node_id(),
            Self::Id(id) => *id,
        }
    }

    /// The node's Ed25519 key, if the entry gave it.
    pub fn key(&self) -> Option<NodeKey> {
        match self {
            Self::Key(key) => Some(*key),
            Self::Id(_) => None,
        }
    }

    /// The address to dial the node at `direct`. A dial to a node named by
    /// its key requires that key in TLS. A dial to a node named by its ID
    /// learns who answered from the control handshake.
    pub fn node_addr(&self, direct: impl IntoIterator<Item = SocketAddr>) -> NodeAddr {
        match self {
            Self::Key(key) => NodeAddr::for_key(*key, direct),
            Self::Id(id) => NodeAddr::with_addrs(*id, direct),
        }
    }
}

impl fmt::Display for ConfiguredNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Key(key) => write!(f, "{key}"),
            Self::Id(id) => write!(f, "{NODE_ID_PREFIX}{id}"),
        }
    }
}

/// Why text does not name a node.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConfiguredNodeError {
    /// The text has no `id:` prefix and is not an Ed25519 public key.
    #[error("not an Ed25519 public key ({0}); a node ID is written id:<64 hex characters>")]
    Key(#[source] KeyParsingError),
    /// The text after `id:` is not a node ID.
    #[error("not a node ID after `id:` ({0})")]
    Id(#[source] KeyParsingError),
}

impl FromStr for ConfiguredNode {
    type Err = ConfiguredNodeError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text.strip_prefix(NODE_ID_PREFIX) {
            Some(id) => id.parse().map(Self::Id).map_err(ConfiguredNodeError::Id),
            None => text
                .parse()
                .map(Self::Key)
                .map_err(ConfiguredNodeError::Key),
        }
    }
}

#[cfg(test)]
mod tests {
    use zakura_quic::NodeSecretKey;

    use super::*;

    /// A bare key names the node whose only key it is, and dials with that
    /// key expected. This is what every existing config entry means.
    #[test]
    fn a_bare_key_names_the_node_of_its_one_key_list() {
        let secret = NodeSecretKey::from_bytes(&[7; 32]);
        let text = secret.public_key().to_string();
        let node: ConfiguredNode = text.parse().expect("a public key parses");

        assert_eq!(node, ConfiguredNode::Key(secret.public_key()));
        assert_eq!(node.node_id(), secret.public());
        assert_eq!(node.to_string(), text);

        let addr = node.node_addr(["192.0.2.1:8234".parse().expect("a socket address")]);
        assert_eq!(addr.id, secret.public());
        assert_eq!(addr.key, Some(secret.public_key()));
    }

    /// An `id:` entry names a node by its ID alone, so its dial expects no
    /// key. Any 32 bytes are a node ID.
    #[test]
    fn an_id_entry_names_a_node_by_its_id_alone() {
        let id = NodeSecretKey::from_bytes(&[7; 32]).public();
        let text = format!("id:{id}");
        let node: ConfiguredNode = text.parse().expect("a node ID parses");

        assert_eq!(node, ConfiguredNode::Id(id));
        assert_eq!(node.node_id(), id);
        assert_eq!(node.key(), None);
        assert_eq!(node.to_string(), text);

        let addr = node.node_addr(["192.0.2.1:8234".parse().expect("a socket address")]);
        assert_eq!((addr.id, addr.key), (id, None));

        let not_a_key = format!("id:{}", "ff".repeat(32));
        assert!(not_a_key.parse::<ConfiguredNode>().is_ok());
    }

    /// A bare value that is not a key is refused with a hint at the `id:`
    /// form, since a node ID pasted without its prefix is the likely cause.
    #[test]
    fn a_bare_value_that_is_not_a_key_is_refused_with_a_hint() {
        let error = "ff"
            .repeat(32)
            .parse::<ConfiguredNode>()
            .expect_err("all-ones bytes are not an Ed25519 key");
        assert!(matches!(error, ConfiguredNodeError::Key(_)));
        assert!(error.to_string().contains("id:<64 hex characters>"));

        assert!(matches!(
            "id:not-hex".parse::<ConfiguredNode>(),
            Err(ConfiguredNodeError::Id(_))
        ));
    }
}
