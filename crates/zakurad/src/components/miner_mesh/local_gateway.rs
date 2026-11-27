//! This node's role in the miner mesh.

use zakura_chain::miner_mesh::GatewayId;

/// This node's role in the miner mesh: the gateway its coinbases name, and the gateway it
/// stands for.
///
/// Startup works it out once from the mining config. The coinbase marker, `getminermeshinfo`
/// and the gateway metrics all read it from there.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct LocalGateway {
    /// The gateway the coinbases this node builds name, if they name one. Only a node with a
    /// miner address builds coinbases.
    pub announced: Option<GatewayId>,
    /// The gateway this node stands for: the one its coinbases name, or its own ID on a relay
    /// that runs the miner mesh without a miner address. `None` on a node that stands for no
    /// gateway.
    pub stands_for: Option<GatewayId>,
}
