//! How `getminermeshinfo` turns the window coverage and the connection
//! places into its response.
//!
//! These tests feed the sources as values. The connection places come from a
//! real v2 endpoint in zakura-network's own tests
//! (`connection_places_report_the_ranking_and_the_peers`); building one here
//! would need a running endpoint and a dialer this crate cannot reach.

use zakura_chain::{
    block::Height,
    miner_mesh::{GatewayId, MinerSetCoverage, QUALIFYING_DEPTH},
};
use zakura_network::zakura::{
    ConnectedPeer, ConnectionPlaces, GatewayPlace, PlacedGateway, SlotSplit,
};

use super::{GetMinerMeshInfoResponse, MeshGatewayState, MeshNode, MeshWindow};

// Six arbitrary node IDs, in byte order as the endpoint reports peers.
const LOCAL_NODE: [u8; 32] =
    node_id(b"2840d419ac18afd23ff4ffe3ba55fb67b620379394c054260ae223adf3d136c7");
const PREFERRED_GATEWAY: [u8; 32] =
    node_id(b"4d70dccee88cea83ef71f9f870489b10f5c6c112d9c49725dc38d2979f6db690");
const BACKOFF_GATEWAY: [u8; 32] =
    node_id(b"67bb6f072c1140d0d2198e9f937d24aeeb8d644600205bec46c90592b7f9f112");
const STANDBY_GATEWAY: [u8; 32] =
    node_id(b"96ddfe87e3074e921304847fa147e7ee97ea3b7e430722916378ebe3ae5193ed");
const UNMATCHED_GATEWAY: [u8; 32] =
    node_id(b"ccc7a088afe130bbb69492031e9fa86cce4628852cc966a62ad73423e20bd947");
const ORDINARY_PEER: [u8; 32] =
    node_id(b"ce2480c5c1da2c6c9d28b1a6f119b1eafb5993be37b2ad01728f3d286d5d93ea");
/// A gateway whose ID two known nodes share. It is not a peer.
const AMBIGUOUS_GATEWAY: [u8; 32] =
    node_id(b"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");

const fn node_id(hex: &[u8]) -> [u8; 32] {
    match const_hex::const_decode_to_array(hex) {
        Ok(id) => id,
        Err(_) => panic!("fixture node IDs must be 32 bytes of hex"),
    }
}

/// A mining, announcing, synced node.
fn node() -> MeshNode {
    MeshNode {
        node_id: Some(LOCAL_NODE),
        announced_gateway: Some(GatewayId::from_node_id(&LOCAL_NODE)),
        mining: true,
        synced: true,
    }
}

/// The tip of [`coverage`].
const TIP: u32 = 3_000_000;

/// The newest height that counts at [`TIP`].
const NEWEST: u32 = TIP - QUALIFYING_DEPTH;

/// A Mainnet tip at height 3,000,000. Its window is two days of 75-second
/// blocks ending the qualifying depth below the tip, and half of it is read.
fn coverage() -> MinerSetCoverage {
    MinerSetCoverage {
        tip: Some(Height(TIP)),
        window: Some(Height(NEWEST - 2_304 + 1)..=Height(NEWEST)),
        checked: 1_152,
    }
}

fn gateway(node_id: [u8; 32], blocks: u32, place: GatewayPlace, connected: bool) -> PlacedGateway {
    PlacedGateway {
        gateway_id: GatewayId::from_node_id(&node_id),
        blocks,
        place,
        always_preferred: false,
        connected,
    }
}

/// Forty slots with nine kept for bootstrap peers leave one preferred place.
fn places() -> ConnectionPlaces {
    ConnectionPlaces {
        gateways: vec![
            gateway(PREFERRED_GATEWAY, 40, GatewayPlace::Preferred, true),
            gateway(BACKOFF_GATEWAY, 12, GatewayPlace::Backoff, false),
            gateway(STANDBY_GATEWAY, 7, GatewayPlace::Standby, true),
            gateway(UNMATCHED_GATEWAY, 3, GatewayPlace::Unmatched, false),
            gateway(AMBIGUOUS_GATEWAY, 2, GatewayPlace::Ambiguous, false),
        ],
        hold: 1,
        slot_limit: 40,
        slots: SlotSplit {
            preferred: 1,
            ordinary: 2,
            ordinary_inbound: 1,
        },
        inbound_slots: 2,
        peers: vec![
            ConnectedPeer {
                node_id: PREFERRED_GATEWAY,
                inbound: true,
                preferred: true,
            },
            ConnectedPeer {
                node_id: STANDBY_GATEWAY,
                inbound: false,
                preferred: false,
            },
            ConnectedPeer {
                node_id: ORDINARY_PEER,
                inbound: true,
                preferred: false,
            },
        ],
    }
}

#[test]
fn a_mining_gateway_reports_its_window_places_and_peers() {
    let response = GetMinerMeshInfoResponse::new(node(), Some(&coverage()), Some(&places()));

    insta::assert_json_snapshot!("get_miner_mesh_info_mining_gateway", response);
}

#[test]
fn each_place_maps_to_its_state_in_rank_order() {
    let response = GetMinerMeshInfoResponse::new(node(), Some(&coverage()), Some(&places()));

    let states: Vec<_> = response
        .gateways
        .iter()
        .map(|gateway| (gateway.blocks, gateway.state, gateway.connected))
        .collect();
    assert_eq!(
        states,
        vec![
            (40, MeshGatewayState::Preferred, true),
            (12, MeshGatewayState::Backoff, false),
            (7, MeshGatewayState::Standby, true),
            (3, MeshGatewayState::Unmatched, false),
            (2, MeshGatewayState::Ambiguous, false),
        ]
    );
    let slots = response.slots.expect("the node runs v2 networking");
    assert_eq!((slots.in_use, slots.preferred, slots.ordinary), (3, 1, 2));
}

#[test]
fn gateways_report_their_20_byte_id_and_peers_their_whole_node_id() {
    let response = GetMinerMeshInfoResponse::new(node(), Some(&coverage()), Some(&places()));

    assert_eq!(
        response.gateways[0].gateway_id,
        hex::encode(&PREFERRED_GATEWAY[..20])
    );
    assert_eq!(response.peers[0].node_id, hex::encode(PREFERRED_GATEWAY));
}

#[test]
fn a_node_that_names_its_relay_reports_the_relay_as_its_announced_gateway() {
    let relay = GatewayId::from_node_id(&PREFERRED_GATEWAY);
    let response = GetMinerMeshInfoResponse::new(
        MeshNode {
            announced_gateway: Some(relay),
            ..node()
        },
        Some(&coverage()),
        Some(&places()),
    );

    assert_eq!(response.announced_gateways, vec![relay.to_string()]);
    assert_eq!(
        response.node_id,
        Some(hex::encode(LOCAL_NODE)),
        "the node still reports its own node ID"
    );
}

#[test]
fn a_relay_that_prefers_gateways_reports_its_window_without_announcing() {
    let response = GetMinerMeshInfoResponse::new(
        MeshNode {
            announced_gateway: None,
            mining: false,
            ..node()
        },
        Some(&coverage()),
        Some(&places()),
    );

    assert!(response.announced_gateways.is_empty());
    assert_eq!(response.window, Some(MeshWindow::from(&coverage())));
    assert_eq!(response.hold, 1);
}

#[test]
fn a_node_without_counts_or_v2_networking_reports_neither() {
    let response = GetMinerMeshInfoResponse::new(
        MeshNode {
            node_id: None,
            announced_gateway: None,
            mining: false,
            synced: false,
        },
        None,
        None,
    );

    assert_eq!(response.node_id, None);
    assert!(response.announced_gateways.is_empty());
    assert_eq!(response.window, None);
    assert!(response.gateways.is_empty());
    assert_eq!(response.hold, 0);
    assert_eq!(response.slots, None);
    assert!(response.peers.is_empty());
}

#[test]
fn the_window_reports_its_heights_and_how_much_is_read() {
    let window = MeshWindow::from(&coverage());

    assert_eq!(window.tip, Some(TIP));
    assert_eq!(window.newest, Some(NEWEST));
    assert_eq!(window.oldest, Some(NEWEST - 2_304 + 1));
    assert_eq!(window.checked, 1_152);
    assert_eq!(window.coverage, 0.5);
}

#[test]
fn a_window_with_no_tip_is_unread_and_one_below_the_qualifying_depth_is_complete() {
    let mut coverage = MinerSetCoverage::default();
    let before_any_tip = MeshWindow::from(&coverage);
    assert_eq!(
        (
            before_any_tip.tip,
            before_any_tip.newest,
            before_any_tip.oldest
        ),
        (None, None, None)
    );
    assert_eq!(before_any_tip.coverage, 0.0);

    coverage.tip = Some(Height(QUALIFYING_DEPTH - 1));
    let too_low = MeshWindow::from(&coverage);
    assert_eq!(
        (too_low.tip, too_low.newest, too_low.oldest),
        (Some(QUALIFYING_DEPTH - 1), None, None)
    );
    assert_eq!(too_low.coverage, 1.0);
}
