//! Types used in the `getminermeshinfo` RPC method.
//!
//! The response reads three sources: how much of its window the tracker that
//! counts gateways has read, the preferred gateways and connection slots that
//! v2 networking uses, and the v2 peers registered now.

use tokio::sync::watch;
use zakura_chain::{block::Height, miner_mesh::MinerSetCoverage};
use zakura_network::zakura::{
    ConnectedPeer, ConnectionPlaces, GatewayPlace, PlacedGateway, ZakuraEndpoint,
};

#[cfg(test)]
mod tests;

/// Where `getminermeshinfo` reads the miner mesh state.
///
/// The default has no source, as on a node that does not mine and runs no
/// v2 networking.
#[derive(Clone, Debug, Default)]
pub struct MinerMeshSources {
    /// How much of its window the gateway counter has read, when this node
    /// counts gateways, which it does when it mines.
    pub coverage: Option<watch::Receiver<MinerSetCoverage>>,
    /// The v2 endpoint, when this node runs v2 networking.
    pub endpoint: Option<ZakuraEndpoint>,
}

/// This node's part in the miner mesh.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MeshNode {
    /// This node's v2 node ID, when it runs v2 networking.
    pub node_id: Option<[u8; 32]>,
    /// Whether the coinbases this node builds name its gateway.
    pub announcing: bool,
    /// Whether this node builds blocks: it has a miner address.
    pub mining: bool,
    /// Whether the best tip is recent enough for the node to prefer
    /// gateways.
    pub synced: bool,
}

/// Response to a `getminermeshinfo` RPC request.
///
/// See the notes for the [`RpcServer::get_miner_mesh_info`](crate::methods::RpcServer::get_miner_mesh_info) method.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GetMinerMeshInfoResponse {
    /// This node's v2 node ID in hex, or null when it runs no v2 networking.
    pub node_id: Option<String>,

    /// Whether the coinbases this node builds name its gateway
    /// (`mining.miner_mesh` with a miner address).
    pub announcing: bool,

    /// Whether this node builds blocks: it has a miner address.
    pub mining: bool,

    /// Whether the best tip is recent enough for the node to prefer
    /// gateways. Always true on test networks.
    pub synced: bool,

    /// The blocks counted, or null when this node does not count gateways
    /// because it does not mine.
    pub window: Option<MeshWindow>,

    /// The last ranking applied by v2 networking, best first, at most 256.
    /// Only gateways named in at least 10 counted blocks are ranked. It can
    /// briefly reflect an earlier tip freshness decision than `synced`.
    pub gateways: Vec<MeshGateway>,

    /// The connection places held for preferred gateways.
    pub hold: usize,

    /// How the connection slots are used, or null when the node runs no v2
    /// networking.
    pub slots: Option<MeshSlots>,

    /// Every v2 peer connected now, ordered by node ID, at most the
    /// connection limit.
    pub peers: Vec<MeshPeer>,
}

/// The heights whose blocks count towards the gateway ranking.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MeshWindow {
    /// The best tip height the counts follow, or null before the first tip.
    pub tip: Option<u32>,

    /// The newest height that counts, 100 below the tip, or null while the
    /// tip is below 100.
    pub newest: Option<u32>,

    /// The oldest height that counts: about two days of blocks below
    /// `newest`.
    pub oldest: Option<u32>,

    /// How many heights in the window have been read.
    pub checked: usize,

    /// The share of the window read, from 0 to 1. Until it reaches 1 the
    /// block counts are a lower bound. A tip below 100 has an empty window,
    /// which is complete, so it reports 1.
    pub coverage: f64,
}

/// One ranked gateway.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MeshGateway {
    /// The ID the blocks name the gateway by, in hex: the first 20 bytes of
    /// its v2 node ID. The gateway is the one known node whose node ID
    /// starts with these bytes.
    pub gateway_id: String,

    /// Counted blocks that name the gateway.
    pub blocks: u32,

    /// Where the gateway stands against the held places.
    pub state: MeshGatewayState,

    /// Whether the gateway is connected over v2 now.
    pub connected: bool,
}

/// Where a ranked gateway stands against the connection places the node
/// holds.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MeshGatewayState {
    /// One of the highest-ranked reachable gateways: it may fill a held
    /// place, which ordinary peers cannot take.
    Preferred,
    /// Known and reachable, but ranked below the gateways that fill the
    /// held places.
    Standby,
    /// Known, but waiting out a failed dial or a lost connection. It still
    /// counts toward the shared hold and may connect inbound during backoff.
    Backoff,
    /// v2 networking has no usable dial address for it, so no place is held for it.
    Unmatched,
    /// v2 networking knows two or more nodes whose node IDs start with the
    /// gateway ID, so it prefers none of them and holds no place for it.
    Ambiguous,
}

/// How the connection slots are used.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MeshSlots {
    /// The most connection slots the node uses at once.
    pub limit: usize,

    /// The slots in use: `preferred` plus `ordinary`.
    pub in_use: usize,

    /// Preferred gateways that hold a slot, one each.
    pub preferred: usize,

    /// Every other slot in use.
    pub ordinary: usize,

    /// The slots in use by connections the remote node opened.
    pub inbound: usize,
}

/// One v2 peer connected now.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MeshPeer {
    /// The peer's v2 node ID in hex.
    pub node_id: String,

    /// Whether the peer opened the connection. A peer connected both ways
    /// counts as inbound.
    pub inbound: bool,

    /// Whether the peer is a preferred gateway.
    pub preferred: bool,
}

impl GetMinerMeshInfoResponse {
    /// Builds the response for `node` from the window coverage, when the node
    /// counts gateways, and the connection places, when it runs v2
    /// networking.
    pub fn new(
        node: MeshNode,
        coverage: Option<&MinerSetCoverage>,
        places: Option<&ConnectionPlaces>,
    ) -> Self {
        let MeshNode {
            node_id,
            announcing,
            mining,
            synced,
        } = node;
        Self {
            node_id: node_id.map(hex::encode),
            announcing,
            mining,
            synced,
            window: coverage.map(MeshWindow::from),
            gateways: places.map_or_else(Vec::new, |places| {
                places.gateways.iter().map(MeshGateway::from).collect()
            }),
            hold: places.map_or(0, |places| places.hold),
            slots: places.map(MeshSlots::from),
            peers: places.map_or_else(Vec::new, |places| {
                places.peers.iter().map(MeshPeer::from).collect()
            }),
        }
    }
}

impl From<&MinerSetCoverage> for MeshWindow {
    fn from(coverage: &MinerSetCoverage) -> Self {
        let window_len = coverage.window_len();
        let share = match (coverage.tip, window_len) {
            (None, _) => 0.0,
            (Some(_), 0) => 1.0,
            // Both are at most the heights in a two-day window, far below
            // 2^52, so the division is exact enough.
            (Some(_), len) => coverage.checked as f64 / len as f64,
        };
        Self {
            tip: coverage.tip.map(|Height(height)| height),
            newest: coverage.window.as_ref().map(|window| window.end().0),
            oldest: coverage.window.as_ref().map(|window| window.start().0),
            checked: coverage.checked,
            coverage: share,
        }
    }
}

impl From<&PlacedGateway> for MeshGateway {
    fn from(gateway: &PlacedGateway) -> Self {
        Self {
            gateway_id: gateway.gateway_id.to_string(),
            blocks: gateway.blocks,
            state: gateway.place.into(),
            connected: gateway.connected,
        }
    }
}

impl From<GatewayPlace> for MeshGatewayState {
    fn from(place: GatewayPlace) -> Self {
        match place {
            GatewayPlace::Preferred => Self::Preferred,
            GatewayPlace::Standby => Self::Standby,
            GatewayPlace::Backoff => Self::Backoff,
            GatewayPlace::Unmatched => Self::Unmatched,
            GatewayPlace::Ambiguous => Self::Ambiguous,
        }
    }
}

impl From<&ConnectionPlaces> for MeshSlots {
    fn from(places: &ConnectionPlaces) -> Self {
        Self {
            limit: places.slot_limit,
            in_use: places.slots.preferred.saturating_add(places.slots.ordinary),
            preferred: places.slots.preferred,
            ordinary: places.slots.ordinary,
            inbound: places.inbound_slots,
        }
    }
}

impl From<&ConnectedPeer> for MeshPeer {
    fn from(peer: &ConnectedPeer) -> Self {
        Self {
            node_id: hex::encode(peer.node_id),
            inbound: peer.inbound,
            preferred: peer.preferred,
        }
    }
}
