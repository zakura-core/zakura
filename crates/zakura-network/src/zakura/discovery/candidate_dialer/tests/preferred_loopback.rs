//! Preferred gateways over real loopback connections: the dialer reaches a
//! ranked gateway that ordinary dials could not, and keeps a gateway that
//! leaves the ranking.
//!
//! Slots the tests hold stand for connections with unrelated peers. Each node
//! has [`MAX_CONNECTIONS`] slots, so it may hold up to ten places.

use std::{collections::BTreeSet, time::Duration};

use zakura_chain::miner_mesh::GatewayId;
use zakura_quic::NodeSecretKey;
use zakura_rt::sync::watch;

use super::*;
use crate::zakura::{
    inbound_share,
    testkit::{await_until, TraceCapture, ZakuraTestNode},
    ConnectedPeer, ConnectionPlaces, GatewayPlace, PlacedGateway, SlotSplit, ZakuraPeerId,
};

const MAX_CONNECTIONS: usize = 92;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// A node with [`MAX_CONNECTIONS`] slots, tracing into `capture` when given.
async fn node(seed: u64, capture: Option<&mut TraceCapture>) -> ZakuraTestNode {
    let mut limits = ZakuraLocalLimits::from_config(&crate::Config::default());
    limits.max_connections = MAX_CONNECTIONS;
    limits.max_preferred_connections = 10;
    let mut builder = ZakuraTestNode::builder(seed).limits(limits);
    if let Some(capture) = capture {
        builder = builder.tracer(capture.tracer_for_node(seed));
    }
    builder.spawn().await.expect("the test node spawns")
}

/// Starts `node`'s candidate dialer, reading its ranking from the returned
/// sender.
fn spawn_dialer(
    node: &ZakuraTestNode,
) -> (
    tokio::task::JoinHandle<()>,
    watch::Sender<MinerGatewayRanking>,
) {
    let (ranking_tx, ranking_rx) = watch::channel(MinerGatewayRanking::default());
    let dialer = tokio::spawn(run_native_discovery_dialer(
        node.endpoint(),
        node.discovery(),
        node.limits().clone(),
        Vec::new(),
        Some(ranking_rx),
    ));
    (dialer, ranking_tx)
}

fn ranking(gateway: NodeId) -> MinerGatewayRanking {
    MinerGatewayRanking {
        always_preferred: Vec::new(),
        gateways: vec![MinerGateway {
            gateway_id: GatewayId::from_node_id(gateway.as_bytes()),
            blocks: 1,
        }],
    }
}

fn registered(node: &ZakuraTestNode, peer: NodeId) -> bool {
    node.supervisor()
        .subscribe()
        .borrow()
        .iter()
        .any(|registered: &ZakuraPeerId| registered.as_bytes() == peer.as_bytes())
}

async fn await_registered(node: &ZakuraTestNode, peer: NodeId, description: &str) {
    await_until(description, CONNECT_TIMEOUT, || registered(node, peer))
        .await
        .expect("the connection registers in time");
}

// A node whose ordinary slots are all taken still dials the gateway it
// prefers, and the connection speaks only the discovery messages and trace
// fields that existed before preferred gateways.
#[tokio::test]
async fn a_node_at_its_ordinary_limit_connects_to_a_known_gateway() {
    let _guard = zakura_test::init();
    let mut capture = TraceCapture::for_test_with_keep_override(
        "a_node_at_its_ordinary_limit_connects_to_a_known_gateway",
        false,
    )
    .expect("the trace capture opens");
    let dialer_node = node(9240, Some(&mut capture)).await;
    let gateway = node(9241, Some(&mut capture)).await;
    // One place is held for the gateway, and every other slot is taken.
    let _ordinary = dialer_node
        .endpoint()
        .hold_connection_slots(MAX_CONNECTIONS, false);
    let gateway_id = dialer_node
        .insert_static_discovery_candidate(&gateway)
        .await
        .expect("the gateway inserts");
    let (dialer, ranking_tx) = spawn_dialer(&dialer_node);
    ranking_tx.send_replace(ranking(gateway_id));

    await_registered(&dialer_node, gateway_id, "the gateway registers").await;
    assert_eq!(
        dialer_node
            .endpoint()
            .discovery_dial_permits(dialer_node.discovery().ordinary_connection_cap),
        0
    );

    dialer.abort();
    dialer_node.shutdown().await;
    gateway.shutdown().await;
    capture.flush().await;
    assert_discovery_trace_schema_unchanged(&capture);
    assert!(capture
        .finish()
        .await
        .expect("the capture closes")
        .is_none());
}

// Each node's inbound share is full, so neither would accept an ordinary
// peer. Each prefers the other and both dial: the one with the lower ID dials
// at once, and the other waits out `MUTUAL_DIAL_GRACE` and takes the call in the place it
// holds, so no dial of the pair is refused.
#[tokio::test]
async fn two_nodes_with_full_inbound_shares_that_prefer_each_other_connect() {
    let _guard = zakura_test::init();
    let mut capture = TraceCapture::for_test_with_keep_override(
        "two_nodes_with_full_inbound_shares_that_prefer_each_other_connect",
        false,
    )
    .expect("the trace capture opens");
    let a = node(9242, Some(&mut capture)).await;
    let b = node(9243, Some(&mut capture)).await;
    let _a_inbound = a
        .endpoint()
        .hold_connection_slots(inbound_share(MAX_CONNECTIONS), true);
    let _b_inbound = b
        .endpoint()
        .hold_connection_slots(inbound_share(MAX_CONNECTIONS), true);
    let b_id = a
        .insert_static_discovery_candidate(&b)
        .await
        .expect("b inserts");
    let a_id = b
        .insert_static_discovery_candidate(&a)
        .await
        .expect("a inserts");

    let (a_dialer, a_ranking) = spawn_dialer(&a);
    let (b_dialer, b_ranking) = spawn_dialer(&b);
    a_ranking.send_replace(ranking(b_id));
    b_ranking.send_replace(ranking(a_id));

    await_registered(&a, b_id, "a registers b").await;
    await_registered(&b, a_id, "b registers a").await;
    // The dial settles, and the waiting node's next round sees the
    // connection rather than dialing.
    tokio::time::sleep(ZAKURA_DISCOVERY_DIAL_INTERVAL * 2).await;

    a_dialer.abort();
    b_dialer.abort();
    a.shutdown().await;
    b.shutdown().await;
    capture.flush().await;
    let reader = capture.reader().expect("the trace loads");
    let rows = reader.table("discovery").rows();
    let phases: Vec<&str> = rows
        .iter()
        .filter(|row| row["event"] == "discovery_dial_phase")
        .filter_map(|row| row["phase"].as_str())
        .collect();
    assert_eq!(
        phases.iter().filter(|phase| **phase == "recorded").count(),
        1,
        "only the node with the lower ID dials: {phases:?}"
    );
    let failed: Vec<_> = rows
        .iter()
        .filter(|row| row["event"] == "discovery_dial_result" && row["result"] != "registered")
        .collect();
    assert!(failed.is_empty(), "no dial of the pair fails: {failed:?}");
    assert!(capture
        .finish()
        .await
        .expect("the capture closes")
        .is_none());
}

// A gateway that leaves the ranking keeps its connection, now in an ordinary
// slot, and the gateway that replaces it takes the held place. No ordinary
// slot is free, so only the preferred pass can dial the replacement.
#[tokio::test]
async fn a_gateway_leaving_the_set_stays_connected_and_its_replacement_is_dialed() {
    let _guard = zakura_test::init();
    let dialer_node = node(9244, None).await;
    let first = node(9245, None).await;
    let second = node(9246, None).await;
    // Two slots stay free: the held place, and one ordinary slot that the
    // first gateway fills once it leaves the set.
    let _ordinary = dialer_node
        .endpoint()
        .hold_connection_slots(MAX_CONNECTIONS - 1, false);
    let first_id = dialer_node
        .insert_static_discovery_candidate(&first)
        .await
        .expect("the first gateway inserts");
    let (dialer, ranking_tx) = spawn_dialer(&dialer_node);
    ranking_tx.send_replace(ranking(first_id));
    await_registered(&dialer_node, first_id, "the first gateway registers").await;

    let second_id = dialer_node
        .insert_static_discovery_candidate(&second)
        .await
        .expect("the second gateway inserts");
    ranking_tx.send_replace(ranking(second_id));
    await_registered(&dialer_node, second_id, "the replacement registers").await;
    assert!(
        registered(&dialer_node, first_id),
        "the gateway that left the set keeps its connection"
    );

    dialer.abort();
    dialer_node.shutdown().await;
    first.shutdown().await;
    second.shutdown().await;
}

// What an operator reads from each endpoint: the dialing node reports the
// gateway it connected to as preferred, connected and in a held place, a
// ranked gateway v2 has no record of as unmatched, and the gateway reports
// the dialing node as an ordinary inbound peer.
#[tokio::test]
async fn connection_places_report_the_ranking_and_the_peers() {
    let _guard = zakura_test::init();
    let dialer_node = node(9250, None).await;
    let gateway = node(9251, None).await;
    let gateway_id = dialer_node
        .insert_static_discovery_candidate(&gateway)
        .await
        .expect("the gateway inserts");
    let dialer_id = dialer_node.endpoint().local_node_id();
    let unknown = NodeSecretKey::generate().public();
    let (dialer, ranking_tx) = spawn_dialer(&dialer_node);
    ranking_tx.send_replace(MinerGatewayRanking {
        always_preferred: Vec::new(),
        gateways: vec![
            MinerGateway {
                gateway_id: GatewayId::from_node_id(gateway_id.as_bytes()),
                blocks: 2,
            },
            MinerGateway {
                gateway_id: GatewayId::from_node_id(unknown.as_bytes()),
                blocks: 1,
            },
        ],
    });

    let expected = ConnectionPlaces {
        gateways: vec![
            PlacedGateway {
                gateway_id: GatewayId::from_node_id(gateway_id.as_bytes()),
                blocks: 2,
                place: GatewayPlace::Preferred,
                always_preferred: false,
                connected: true,
            },
            PlacedGateway {
                gateway_id: GatewayId::from_node_id(unknown.as_bytes()),
                blocks: 1,
                place: GatewayPlace::Unmatched,
                always_preferred: false,
                connected: false,
            },
        ],
        hold: 1,
        slot_limit: dialer_node.limits().total_connections(),
        slots: SlotSplit {
            preferred: 1,
            ordinary: 0,
            ordinary_inbound: 0,
        },
        inbound_slots: 0,
        peers: vec![ConnectedPeer {
            node_id: *gateway_id.as_bytes(),
            inbound: false,
            preferred: true,
        }],
    };
    // A connection's slot and its registration settle a moment apart.
    let _ = await_until(
        "the dialing node reports the gateway",
        CONNECT_TIMEOUT,
        || dialer_node.endpoint().connection_places() == expected,
    )
    .await;
    assert_eq!(dialer_node.endpoint().connection_places(), expected);

    await_registered(&gateway, dialer_id, "the dialing node registers").await;
    let seen_by_gateway = gateway.endpoint().connection_places();
    assert!(seen_by_gateway.gateways.is_empty());
    assert_eq!(seen_by_gateway.hold, 0);
    assert_eq!(seen_by_gateway.inbound_slots, 1);
    assert_eq!(
        seen_by_gateway.peers,
        vec![ConnectedPeer {
            node_id: *dialer_id.as_bytes(),
            inbound: true,
            preferred: false,
        }]
    );

    dialer.abort();
    dialer_node.shutdown().await;
    gateway.shutdown().await;
}

/// Asserts every discovery trace row is one of the events, with the fields,
/// that discovery traced before preferred gateways, and that no peer sent a
/// discovery frame the other refused.
fn assert_discovery_trace_schema_unchanged(capture: &TraceCapture) {
    let reader = capture.reader().expect("the trace loads");
    let rows = reader.table("discovery").rows();
    assert!(!rows.is_empty(), "the dial traces its phases");
    for row in rows {
        let row = row.as_object().expect("a trace row is an object");
        let event = row["event"].as_str().expect("a trace row names its event");
        let fields: &[&str] = match event {
            "discovery_dial_phase" => &["peer", "owner", "attempt", "phase"],
            "discovery_dial_result" => &["result", "peer"],
            "discovery_answer" => &["peer", "session_id", "query", "phase"],
            other => panic!("unexpected discovery trace event {other}: {row:?}"),
        };
        let unknown: BTreeSet<&str> = row
            .keys()
            .map(String::as_str)
            .filter(|key| !fields.contains(key) && !TRACE_ROW_FIELDS.contains(key))
            .collect();
        assert!(unknown.is_empty(), "{event} gained fields {unknown:?}");
        if event == "discovery_answer" {
            assert!(
                matches!(row["query"].as_str(), Some("peers" | "services")),
                "a discovery answer names a query that existed before: {row:?}"
            );
        }
    }
}

/// Fields the trace writer adds to every row.
const TRACE_ROW_FIELDS: &[&str] = &["event", "ts", "node", "process_trace_id"];
