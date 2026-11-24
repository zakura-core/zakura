//! The places the node holds for preferred mining gateways, how its
//! connection slots are used, and who holds them, read so an operator can see
//! them.

use zakura_chain::miner_mesh::GatewayId;
use zakura_quic::NodeId;
use zakura_rt::{collections::HashSet, sync::MutexGuard};

use super::{GatewayPlace, HandshakeCounts, SlotSplit};

/// What the connection-slot checks hold for preferred mining gateways, and
/// the v2 peers connected now.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConnectionPlaces {
    /// Every ranked gateway, in rank order, at most 256. Reflects the last
    /// ranking applied by networking.
    pub gateways: Vec<PlacedGateway>,
    /// The connection places held for the preferred set.
    pub hold: usize,
    /// The most connection slots the node uses at once.
    pub slot_limit: usize,
    /// The slots in use, split into preferred and ordinary ones.
    pub slots: SlotSplit,
    /// The slots in use by connections the remote node opened.
    pub inbound_slots: usize,
    /// Every registered v2 peer, ordered by node ID, at most
    /// [`Self::slot_limit`].
    pub peers: Vec<ConnectedPeer>,
}

/// One ranked gateway and where it stands.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlacedGateway {
    /// The ID the blocks name the gateway by: the first 20 bytes of its v2
    /// node ID.
    pub gateway_id: GatewayId,
    /// Recent blocks that name the gateway.
    pub blocks: u32,
    /// Where the gateway stands against the held places.
    pub place: GatewayPlace,
    /// Whether the gateway is a registered v2 peer now.
    pub connected: bool,
}

/// One registered v2 peer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectedPeer {
    /// The peer's v2 node ID.
    pub node_id: [u8; 32],
    /// Whether the peer holds a slot for a connection it opened. A peer that
    /// holds slots in both directions counts as inbound, as in the slot
    /// split.
    pub inbound: bool,
    /// Whether the peer is in the preferred set.
    pub preferred: bool,
}

impl ConnectionPlaces {
    /// Reads the places and slots from the counts `lock_counts` locks, whose
    /// connection limit is `slot_limit`, for the peers `registered` now.
    ///
    /// The counts stay locked only while they are read: the peers are
    /// ordered after the lock is released.
    ///
    /// Every registration holds a slot, so there are no more than
    /// `slot_limit` of them; the peer list is cut there all the same, so the
    /// report stays bounded whatever the caller passes.
    pub(crate) fn read<'a>(
        lock_counts: impl FnOnce() -> MutexGuard<'a, HandshakeCounts>,
        slot_limit: usize,
        registered: &HashSet<NodeId>,
    ) -> Self {
        let counts = lock_counts();
        let preferred = counts.preferred_gateways();
        let gateways = preferred
            .places()
            .map(|(gateway, place)| PlacedGateway {
                gateway_id: gateway.gateway,
                blocks: gateway.blocks,
                place,
                connected: gateway
                    .node()
                    .is_some_and(|node| registered.contains(&node)),
            })
            .collect();
        let mut peers: Vec<ConnectedPeer> = registered
            .iter()
            .map(|node| ConnectedPeer {
                node_id: *node.as_bytes(),
                inbound: counts.node_connections(node).inbound > 0,
                preferred: preferred.contains(node),
            })
            .collect();
        let hold = preferred.hold();
        let slots = counts.slot_split();
        let inbound_slots = counts.inbound_connections();
        drop(counts);

        peers.sort_unstable_by_key(|peer| peer.node_id);
        peers.truncate(slot_limit);
        Self {
            gateways,
            hold,
            slot_limit,
            slots,
            inbound_slots,
            peers,
        }
    }
}
