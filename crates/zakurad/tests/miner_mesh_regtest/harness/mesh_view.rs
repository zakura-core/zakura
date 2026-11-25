//! What a node reports about the miner mesh, and the standing invariants every
//! report keeps.
//!
//! The harness checks each node's report. Scenarios check changes between
//! reports when they control the event that should cause the change.

use std::collections::{HashMap, HashSet};

use zakura_rpc::client::{GetMinerMeshInfoResponse, MeshGateway, MeshGatewayState, MeshPeer};

use super::GatewayId;

/// One node's `getminermeshinfo` report.
#[derive(Clone, Debug)]
pub struct MeshView {
    pub info: GetMinerMeshInfoResponse,
}

impl MeshView {
    /// The ranked gateway with `id`, if the node ranks it.
    pub fn gateway(&self, id: &GatewayId) -> Option<&MeshGateway> {
        let id = hex::encode(id);
        self.info
            .gateways
            .iter()
            .find(|gateway| gateway.gateway_id == id)
    }

    /// Where the node places the gateway with `id`, if it ranks it.
    pub fn state_of(&self, id: &GatewayId) -> Option<MeshGatewayState> {
        self.gateway(id).map(|gateway| gateway.state)
    }

    /// The ranked gateways, best first.
    pub fn ranking(&self) -> Vec<GatewayId> {
        self.info
            .gateways
            .iter()
            .map(|gateway| decode_id(&gateway.gateway_id))
            .collect()
    }

    /// The v2 peer whose node ID starts with the gateway ID `id`, if it is
    /// connected now.
    pub fn peer(&self, id: &GatewayId) -> Option<&MeshPeer> {
        let id = hex::encode(id);
        self.info
            .peers
            .iter()
            .find(|peer| peer.node_id.starts_with(&id))
    }

    /// Whether the node is connected to `id` now.
    pub fn is_connected_to(&self, id: &GatewayId) -> bool {
        self.peer(id).is_some()
    }

    /// The gateways in the preferred set, connected or not.
    pub fn preferred(&self) -> HashSet<GatewayId> {
        self.info
            .gateways
            .iter()
            .filter(|gateway| gateway.state == MeshGatewayState::Preferred)
            .map(|gateway| decode_id(&gateway.gateway_id))
            .collect()
    }

    /// Whether the gateway with `id` is preferred and connected.
    pub fn prefers_and_holds(&self, id: &GatewayId) -> bool {
        self.state_of(id) == Some(MeshGatewayState::Preferred) && self.is_connected_to(id)
    }

    /// The block count of every ranked gateway.
    pub fn counts(&self) -> HashMap<GatewayId, u32> {
        self.info
            .gateways
            .iter()
            .map(|gateway| (decode_id(&gateway.gateway_id), gateway.blocks))
            .collect()
    }

    /// The report on one line, for a failure message.
    pub fn describe(&self) -> String {
        let info = &self.info;
        let gateways: Vec<String> = info
            .gateways
            .iter()
            .map(|gateway| {
                format!(
                    "{} {} blocks {:?}{}",
                    gateway.gateway_id,
                    gateway.blocks,
                    gateway.state,
                    if gateway.connected { " connected" } else { "" }
                )
            })
            .collect();
        let peers: Vec<String> = info
            .peers
            .iter()
            .map(|peer| {
                format!(
                    "{}{}{}",
                    peer.node_id,
                    if peer.inbound { " in" } else { " out" },
                    if peer.preferred { " preferred" } else { "" }
                )
            })
            .collect();
        format!(
            "announces [{}], hold {}, slots {:?}, window {:?}, gateways [{}], peers [{}]",
            info.announced_gateways.join(", "),
            info.hold,
            info.slots,
            info.window,
            gateways.join(", "),
            peers.join(", ")
        )
    }
}

/// What the harness configured on a node, which its report must agree with.
#[derive(Clone, Copy, Debug)]
pub(super) struct NodeFacts {
    /// The node's whole v2 node ID.
    pub node_id: [u8; 32],
    /// The node has a miner address.
    pub mining: bool,
    /// The node mines or runs the miner mesh, so it counts gateways.
    pub prefers_gateways: bool,
    /// The gateway the node's coinbases name, if it names one.
    pub announced: Option<GatewayId>,
    pub total_capacity: usize,
    pub gateway_capacity: usize,
}

/// The invariants one report keeps. Returns each one `view` breaks.
pub(super) fn check_view(view: &MeshView, facts: &NodeFacts) -> Vec<String> {
    let info = &view.info;
    let mut broken = Vec::new();
    let Some(slots) = info.slots else {
        return vec!["the node reports no connection slots, but it runs v2".into()];
    };
    let own_id = hex::encode(facts.node_id);
    let available: Vec<&MeshGateway> = info
        .gateways
        .iter()
        .filter(|gateway| {
            matches!(
                gateway.state,
                MeshGatewayState::Preferred | MeshGatewayState::Standby
            )
        })
        .collect();
    let preferred_gateways = available
        .iter()
        .filter(|gateway| gateway.state == MeshGatewayState::Preferred)
        .count();
    let matched = info
        .gateways
        .iter()
        .filter(|gateway| {
            !matches!(
                gateway.state,
                MeshGatewayState::Unmatched | MeshGatewayState::Ambiguous
            )
        })
        .count();

    // 1. Preferred and ordinary slots add up to the slots in use, which stay
    // within the limit and hold every peer.
    if slots.preferred + slots.ordinary != slots.in_use {
        broken.push(format!("1: slots do not add up: {slots:?}"));
    }
    if slots.in_use > slots.limit || slots.inbound > slots.in_use {
        broken.push(format!("1: slots pass their bounds: {slots:?}"));
    }
    if info.peers.len() > slots.in_use {
        broken.push(format!(
            "1: {} peers hold only {} slots",
            info.peers.len(),
            slots.in_use
        ));
    }
    if slots.limit != facts.total_capacity {
        broken.push(format!(
            "1: the slot limit is {}, not the funded {}",
            slots.limit, facts.total_capacity
        ));
    }

    // 2. Preferred slots stay within the hold, at most one per member of the
    // preferred set. A dial to a member holds its preferred slot before any
    // peer is registered, so connected preferred peers are not an upper bound.
    if slots.preferred > info.hold || slots.preferred > preferred_gateways {
        broken.push(format!(
            "2: {} preferred slots for a hold of {} and {preferred_gateways} preferred gateways",
            slots.preferred, info.hold
        ));
    }

    // 3. A matched gateway can hold one of the separately funded places.
    let ceiling = facts.gateway_capacity;
    if info.hold != ceiling.min(matched) {
        broken.push(format!(
            "3: the hold is {}, not the least of {ceiling} and {matched} matched gateways",
            info.hold
        ));
    }

    // 4. The node never ranks its own gateway ID or connects to its own ID.
    if info.node_id.as_deref() != Some(own_id.as_str()) {
        broken.push(format!("4: the node reports ID {:?}", info.node_id));
    }
    if info
        .gateways
        .iter()
        .any(|gateway| own_id.starts_with(&gateway.gateway_id))
    {
        broken.push("4: the node ranks its own ID".into());
    }
    if info.peers.iter().any(|peer| peer.node_id == own_id) {
        broken.push("4: the node is connected to its own ID".into());
    }

    // 5. The preferred set is the first available gateways in rank order, as
    // many as the hold, skipping only gateways in backoff, unmatched or
    // ambiguous.
    if info
        .gateways
        .windows(2)
        .any(|pair| pair[0].blocks < pair[1].blocks)
    {
        broken.push("5: the gateways are not in rank order".into());
    }
    let leading_preferred = available
        .iter()
        .take_while(|gateway| gateway.state == MeshGatewayState::Preferred)
        .count();
    if leading_preferred != preferred_gateways
        || preferred_gateways != info.hold.min(available.len())
    {
        broken.push(format!(
            "5: the preferred gateways are not the first {} available ones: {:?}",
            info.hold.min(available.len()),
            info.gateways
        ));
    }

    // 7. A node that does not prefer gateways ranks nothing and holds
    // nothing. One that does reports the blocks it counts.
    if info.mining != facts.mining {
        broken.push(format!("7: the node reports mining = {}", info.mining));
    }
    if !facts.prefers_gateways
        && (!info.gateways.is_empty() || info.hold != 0 || info.window.is_some())
    {
        broken.push(format!(
            "7: a node that does not prefer gateways ranks {} gateways with a hold of {}",
            info.gateways.len(),
            info.hold
        ));
    }
    if facts.prefers_gateways && info.window.is_none() {
        broken.push("7: a node that prefers gateways reports no window".into());
    }

    // 8. The node's coinbases name the gateway the harness configured: its
    // own node, the relay it names, or none.
    let announced: Vec<String> = facts.announced.iter().map(hex::encode).collect();
    if info.announced_gateways != announced {
        broken.push(format!(
            "8: the node announces {:?}, not {announced:?}",
            info.announced_gateways
        ));
    }

    broken
}

fn decode_id(id: &str) -> GatewayId {
    let mut bytes = [0; 20];
    hex::decode_to_slice(id, &mut bytes)
        .expect("getminermeshinfo reports gateway IDs as 40 hex digits");
    bytes
}
