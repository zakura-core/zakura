//! How `getminermeshinfo` turns the window coverage and the connection
//! places into its response.
//!
//! These tests feed the sources as values. The connection places come from a
//! real v2 endpoint in zakura-network's own tests
//! (`connection_places_report_the_ranking_and_the_peers`); building one here
//! would need a running endpoint and a dialer this crate cannot reach.

use zakura_chain::{block::Height, miner_mesh::MinerSetCoverage};
use zakura_network::zakura::{
    ConnectedPeer, ConnectionPlaces, GatewayPlace, PlacedGateway, SlotSplit,
};

use super::{GetMinerMeshInfoResponse, MeshGatewayState, MeshNode, MeshWindow};

// Public IDs from six v2 identities, in byte order as the endpoint reports peers.
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
        announcing: true,
        mining: true,
        synced: true,
    }
}

/// Mainnet height 3,000,000, whose window is two days of 75-second blocks,
/// with half of it read.
fn coverage() -> MinerSetCoverage {
    MinerSetCoverage {
        tip: Some(Height(3_000_000)),
        window: Some(Height(2_999_900 - 2_304 + 1)..=Height(2_999_900)),
        checked: 1_152,
    }
}

fn gateway(node_id: [u8; 32], blocks: u32, place: GatewayPlace, connected: bool) -> PlacedGateway {
    PlacedGateway {
        node_id,
        blocks,
        place,
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
        ]
    );
    let slots = response.slots.expect("the node runs v2 networking");
    assert_eq!((slots.in_use, slots.preferred, slots.ordinary), (3, 1, 2));
}

#[test]
fn a_node_without_counts_or_v2_networking_reports_neither() {
    let response = GetMinerMeshInfoResponse::new(
        MeshNode {
            node_id: None,
            announcing: false,
            mining: false,
            synced: false,
        },
        None,
        None,
    );

    assert_eq!(response.node_id, None);
    assert_eq!(response.window, None);
    assert!(response.gateways.is_empty());
    assert_eq!(response.hold, 0);
    assert_eq!(response.slots, None);
    assert!(response.peers.is_empty());
}

#[test]
fn the_window_reports_its_heights_and_how_much_is_read() {
    let window = MeshWindow::from(&coverage());

    assert_eq!(window.tip, Some(3_000_000));
    assert_eq!(window.newest, Some(2_999_900));
    assert_eq!(window.oldest, Some(2_999_900 - 2_304 + 1));
    assert_eq!(window.checked, 1_152);
    assert_eq!(window.coverage, 0.5);
}

#[test]
fn a_window_with_no_tip_is_unread_and_one_below_height_100_is_complete() {
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

    coverage.tip = Some(Height(99));
    let too_low = MeshWindow::from(&coverage);
    assert_eq!(
        (too_low.tip, too_low.newest, too_low.oldest),
        (Some(99), None, None)
    );
    assert_eq!(too_low.coverage, 1.0);
}
