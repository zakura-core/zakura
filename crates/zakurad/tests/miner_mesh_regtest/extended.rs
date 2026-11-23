//! Extended miner mesh scenarios, including connection transitions,
//! ranking, restarts, reorgs and aging.
//!
//! Run them under their own profile:
//!
//! ```sh
//! cargo nextest run --profile miner-mesh-regtest-extended --run-ignored=all -p zakura
//! ```

use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use eyre::Result;

use zakura_rpc::client::MeshGatewayState;

use super::harness::{GatewayId, NodeSpec, RegtestNet, Role, MIN_GATEWAY_BLOCKS, QUALIFYING_DEPTH, WINDOW_BLOCKS};

use super::{DIAL_TIMEOUT, DISCOVERY_TIMEOUT, QUIET_PERIOD};

/// The most gateways a node prefers.
const PREFERRED_GATEWAYS: usize = 10;

/// How long twelve pools get to prefer and hold their ten highest peers:
/// peer exchange, then ten preferred dials per node, four at a time.
const MESH_FORM_TIMEOUT: Duration = Duration::from_secs(240);

/// How long the nodes get to take a restarted gateway back. A node that
/// failed to dial the gateway while it was down waits a minute before it
/// dials it again.
const RECONNECT_TIMEOUT: Duration = Duration::from_secs(150);

/// The blocks mined between two checks while a marker ages.
const AGING_STEP: u32 = 600;

/// Twelve pools named in 21, 20, ..., 10 blocks: every mining node prefers and
/// holds the ten highest pools other than itself, and never the rest. When
/// two connected pools tie at the cutoff, the existing preferred pool keeps
/// its place through the tie and the dial rounds that follow.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts zakurad children: run with the miner-mesh-regtest-extended nextest profile"]
async fn top_ten_of_twelve_pools() -> Result<()> {
    const HUB: usize = 0;
    const POOLS: [usize; 12] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
    const M: usize = 13;
    let _init_guard = zakura_test::init();

    let mut specs = vec![NodeSpec::new(Role::Relay)];
    specs.extend(POOLS.map(|_| NodeSpec::new(Role::Pool).bootstrap_from([HUB])));
    let cutoff = PREFERRED_GATEWAYS - 1;
    specs.push(NodeSpec::new(Role::Miner).bootstrap_from([HUB, POOLS[cutoff], POOLS[cutoff + 1]]));
    let mut net = RegtestNet::start(specs).await?;

    // Give the challenger the lower node ID so the ID tie-break alone would
    // choose it over the connected incumbent.
    let mut ranked = POOLS;
    if net.gateway_id(ranked[cutoff]) < net.gateway_id(ranked[cutoff + 1]) {
        ranked.swap(cutoff, cutoff + 1);
    }
    let incumbent = net.gateway_id(ranked[cutoff]);
    let challenger = net.gateway_id(ranked[cutoff + 1]);
    let counts = (MIN_GATEWAY_BLOCKS..MIN_GATEWAY_BLOCKS + 12).rev();
    for (pool, count) in ranked.into_iter().zip(counts) {
        net.mine(pool, count).await?;
    }
    net.mine(M, QUALIFYING_DEPTH).await?;

    let choices: Vec<(usize, HashSet<GatewayId>)> = POOLS
        .into_iter()
        .chain([M])
        .map(|node| {
            let peers = ranked
                .into_iter()
                .filter(|&pool| pool != node)
                .take(PREFERRED_GATEWAYS)
                .map(|pool| net.gateway_id(pool))
                .collect();
            (node, peers)
        })
        .collect();
    let holds_choices = |net: &RegtestNet| {
        choices.iter().all(|(node, expected)| {
            let view = net.view(*node);
            view.preferred() == *expected
                && expected.iter().all(|id| view.is_connected_to(id))
                && view.info.slots.expect("the miner runs v2").preferred == PREFERRED_GATEWAYS
        }) && net.view(M).is_connected_to(&challenger)
    };

    net.wait_for(
        "the ten highest peers and the connected challenger",
        MESH_FORM_TIMEOUT,
        holds_choices,
    )
    .await?;
    net.mine(ranked[cutoff + 1], 1).await?;
    net.mine(M, QUALIFYING_DEPTH).await?;
    let view = net.view(M);
    let count = |id| view.gateway(id).map(|gateway| gateway.blocks);
    assert_eq!(
        (count(&incumbent), count(&challenger)),
        (Some(MIN_GATEWAY_BLOCKS + 2), Some(MIN_GATEWAY_BLOCKS + 2)),
        "the connected pools tie for the last preferred place"
    );
    net.hold_for(
        "every node keeping its preferred peers through the tie",
        QUIET_PERIOD,
        holds_choices,
    )
    .await?;

    Ok(())
}

/// A pool restarted with the node key the harness wrote for it keeps its node
/// ID, so the nodes that preferred it prefer and hold it again once it is back. Logs how long the first of
/// them took.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts zakurad children: run with the miner-mesh-regtest-extended nextest profile"]
async fn gateway_restart_keeps_its_id() -> Result<()> {
    const HUB: usize = 0;
    const A: usize = 1;
    const B: usize = 2;
    const M: usize = 3;
    let _init_guard = zakura_test::init();

    let mut net = RegtestNet::start(vec![
        NodeSpec::new(Role::Relay),
        NodeSpec::new(Role::Pool).bootstrap_from([HUB]),
        NodeSpec::new(Role::Pool).bootstrap_from([HUB]),
        NodeSpec::new(Role::Miner).bootstrap_from([HUB]),
    ])
    .await?;
    let (a, b) = (net.gateway_id(A), net.gateway_id(B));

    net.mine(A, MIN_GATEWAY_BLOCKS + 1).await?;
    net.mine(B, MIN_GATEWAY_BLOCKS).await?;
    net.mine(M, QUALIFYING_DEPTH).await?;
    let all_held = |net: &RegtestNet| {
        [B, M]
            .into_iter()
            .all(|node| net.view(node).prefers_and_holds(&a))
            && net.view(A).prefers_and_holds(&b)
    };
    net.wait_for("the mesh holding A and B", DISCOVERY_TIMEOUT, all_held)
        .await?;

    net.stop(A).await;
    let restarted = Instant::now();
    net.start_node(A).await?;
    net.wait_for("a node holding A again", RECONNECT_TIMEOUT, |net| {
        [B, M]
            .into_iter()
            .any(|node| net.view(node).prefers_and_holds(&a))
    })
    .await?;
    tracing::info!(
        elapsed = ?restarted.elapsed(),
        "time from a gateway's restart to its first preferred connection",
    );

    net.wait_for("the mesh holding A again", RECONNECT_TIMEOUT, all_held)
        .await?;
    net.settle().await?;

    Ok(())
}

/// Two islands that cannot reach each other mine apart, 120 blocks naming A
/// on one and 300 naming B on the other, until a relay joins them. A's island
/// reorganises onto B's chain, 120 blocks deep: past the qualifying depth,
/// within the 1,000 blocks a node can reorganise. Every mining node then ranks
/// B alone, and A's miner keeps its connection to A as an ordinary one.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts zakurad children: run with the miner-mesh-regtest-extended nextest profile"]
async fn deep_reorg_moves_the_ranking() -> Result<()> {
    const A_HUB: usize = 0;
    const A: usize = 1;
    const A_MINER: usize = 2;
    const B: usize = 3;
    const B_MINER: usize = 4;
    const BRIDGE: usize = 5;
    const A_BLOCKS: u32 = 120;
    const B_BLOCKS: u32 = 300;
    let _init_guard = zakura_test::init();

    // A's miner learns A from a relay rather than bootstrapping from it, so
    // it does not redial A as a bootstrap peer: its connection to A is the
    // one it made for a preferred gateway.
    let mut net = RegtestNet::start(vec![
        NodeSpec::new(Role::Relay),
        NodeSpec::new(Role::Pool).bootstrap_from([A_HUB]),
        NodeSpec::new(Role::Miner)
            .bootstrap_from([A_HUB])
            .trace_connections(),
        NodeSpec::new(Role::Pool),
        NodeSpec::new(Role::Miner).bootstrap_from([B]),
        NodeSpec::new(Role::Relay)
            .bootstrap_from([A_HUB, B])
            .start_later(),
    ])
    .await?;
    let (a, b) = (net.gateway_id(A), net.gateway_id(B));

    let a_tip = net
        .mine_on_island(A, A_BLOCKS, &[A_HUB, A, A_MINER])
        .await?;
    let b_tip = net.mine_on_island(B, B_BLOCKS, &[B, B_MINER]).await?;
    assert_ne!(
        net.block(B, a_tip.height.0).await?.hash(),
        a_tip.hash,
        "B's fork replaces the block at the tip of A's island"
    );
    net.wait_for(
        "each island's miner holding its pool",
        DIAL_TIMEOUT,
        |net| net.view(A_MINER).prefers_and_holds(&a) && net.view(B_MINER).prefers_and_holds(&b),
    )
    .await?;
    assert_eq!(net.view(A_MINER).ranking(), vec![a]);
    assert_eq!(
        net.view(A_MINER)
            .info
            .slots
            .expect("the miner runs v2")
            .preferred,
        1
    );
    let a_connection = net.accepted_connection_id(A_MINER, A).await?;

    // Bring the islands together only after both have counted their own
    // pool's markers.
    net.start_node(BRIDGE).await?;
    net.wait_for("A's island following B's chain", DISCOVERY_TIMEOUT, |net| {
        [A, A_MINER]
            .into_iter()
            .all(|node| net.view(node).ranking() == vec![b])
    })
    .await?;
    assert_eq!(net.settle().await?, b_tip);

    net.wait_for("every mining node holding B", DISCOVERY_TIMEOUT, |net| {
        [A, A_MINER, B_MINER]
            .into_iter()
            .all(|node| net.view(node).prefers_and_holds(&b))
    })
    .await?;
    for node in [A, A_MINER, B_MINER] {
        assert_eq!(
            net.view(node).ranking(),
            vec![b],
            "node {node} ranks B only"
        );
    }
    net.hold_for(
        "A's miner keeping A as an ordinary peer",
        QUIET_PERIOD,
        |net| is_ordinary_peer(net, A_MINER, &a, 1),
    )
    .await?;
    assert_eq!(
        net.accepted_connection_id(A_MINER, A).await?,
        a_connection,
        "the reorg kept the original connection to A"
    );

    Ok(())
}

/// A pool that stops mining leaves every ranking once fewer than the minimum
/// of its markers count. Its markers are consecutive, so that is when the
/// first of them is older than the window: two days of blocks plus the
/// qualifying depth after it, 2,404 blocks on Regtest. Its connections stay,
/// as ordinary ones.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts zakurad children: run with the miner-mesh-regtest-extended nextest profile"]
async fn markers_age_out() -> Result<()> {
    const HUB: usize = 0;
    const A: usize = 1;
    const B: usize = 2;
    const M: usize = 3;
    let _init_guard = zakura_test::init();

    let mut net = RegtestNet::start(vec![
        NodeSpec::new(Role::Relay),
        NodeSpec::new(Role::Pool).bootstrap_from([HUB]),
        NodeSpec::new(Role::Pool)
            .bootstrap_from([HUB])
            .trace_connections(),
        NodeSpec::new(Role::Miner)
            .bootstrap_from([HUB])
            .trace_connections(),
    ])
    .await?;
    let (a, b) = (net.gateway_id(A), net.gateway_id(B));

    let first_marker = net.mine(A, MIN_GATEWAY_BLOCKS).await?.height.0 + 1 - MIN_GATEWAY_BLOCKS;
    net.mine(B, 1).await?;
    let mut tip = net.mine(M, QUALIFYING_DEPTH).await?.height.0;
    net.wait_for("B and M holding A", DISCOVERY_TIMEOUT, |net| {
        [B, M]
            .into_iter()
            .all(|node| net.view(node).prefers_and_holds(&a))
    })
    .await?;

    // From here only B mines, so B stays ranked while A's markers age.
    let last_tip_counting_all_of_a = first_marker + QUALIFYING_DEPTH + WINDOW_BLOCKS - 1;
    while tip < last_tip_counting_all_of_a {
        let step = (last_tip_counting_all_of_a - tip).min(AGING_STEP);
        tip = net.mine(B, step).await?.height.0;
    }
    for node in [B, M] {
        let view = net.view(node);
        assert_eq!(
            view.gateway(&a).map(|gateway| gateway.blocks),
            Some(MIN_GATEWAY_BLOCKS),
            "node {node} still counts A's first marker at the oldest height of its window",
        );
        assert!(view.prefers_and_holds(&a), "node {node} still holds A");
    }
    assert_eq!(net.view(B).info.slots.expect("B runs v2").preferred, 1);
    assert_eq!(net.view(M).info.slots.expect("M runs v2").preferred, 2);
    let b_to_a = net.accepted_connection_id(B, A).await?;
    let m_to_a = net.accepted_connection_id(M, A).await?;

    net.mine(B, 1).await?;
    for node in [B, M] {
        assert_eq!(
            net.view(node).gateway(&a),
            None,
            "node {node} no longer ranks A, one marker short of the minimum"
        );
    }
    let only_b_is_preferred = |net: &RegtestNet| {
        is_ordinary_peer(net, B, &a, 0)
            && is_ordinary_peer(net, M, &a, 1)
            && net.view(M).ranking() == vec![b]
            && net.view(M).prefers_and_holds(&b)
    };
    net.hold_for(
        "A staying ordinary while M keeps B preferred",
        QUIET_PERIOD,
        only_b_is_preferred,
    )
    .await?;
    for (node, before) in [(B, b_to_a), (M, m_to_a)] {
        assert_eq!(
            net.accepted_connection_id(node, A).await?,
            before,
            "marker expiry kept node {node}'s original connection to A"
        );
    }

    Ok(())
}

/// Whether `node` is connected to `id`, does not rank it, and counts it as
/// an ordinary peer with only `preferred_slots` preferred places in use.
fn is_ordinary_peer(net: &RegtestNet, node: usize, id: &GatewayId, preferred_slots: usize) -> bool {
    let view = net.view(node);
    view.gateway(id).is_none()
        && view.peer(id).is_some_and(|peer| !peer.preferred)
        && view.info.hold == preferred_slots
        && view.info.slots.expect("the miner runs v2").preferred == preferred_slots
}

/// A gateway already connected as an ordinary peer when its marker qualifies
/// becomes preferred on the same connection, without a new one.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts zakurad children: run with the miner-mesh-regtest-extended nextest profile"]
async fn a_connected_gateway_is_promoted_in_place() -> Result<()> {
    const A: usize = 0;
    const B: usize = 1;
    let _init_guard = zakura_test::init();

    let mut net = RegtestNet::start(vec![
        NodeSpec::new(Role::Pool),
        NodeSpec::new(Role::Miner).bootstrap_from([A]),
    ])
    .await?;
    let a = net.gateway_id(A);

    // Either node may end up holding the connection it opened; what matters
    // is that one connection, in one direction, lasts.
    net.mine(A, MIN_GATEWAY_BLOCKS).await?;
    let one_connection = |net: &RegtestNet| {
        let view = net.view(B);
        view.is_connected_to(&a)
            && view.info.slots.expect("B runs v2").in_use == view.info.peers.len()
    };
    net.wait_for("A connected to B", DIAL_TIMEOUT, one_connection)
        .await?;
    let inbound = |net: &RegtestNet| net.view(B).peer(&a).map(|peer| peer.inbound);
    let direction = inbound(&net);
    net.hold_for("A's one connection to B", QUIET_PERIOD, |net| {
        one_connection(net) && inbound(net) == direction
    })
    .await?;
    assert_eq!(
        net.view(B).state_of(&a),
        None,
        "A's markers are not deep enough yet"
    );

    net.mine(B, QUALIFYING_DEPTH - 1).await?;
    assert_eq!(net.view(B).state_of(&a), None);
    assert!(one_connection(&net) && inbound(&net) == direction);
    #[cfg(feature = "prometheus")]
    let accepted = {
        let accepted = net.counter(B, "zakura_p2p_conn_accepted").await?;
        assert!(accepted > 0, "B registered its ordinary connection to A");
        accepted
    };

    net.mine(B, 1).await?;
    net.wait_for("B preferring A", DIAL_TIMEOUT, |net| {
        net.view(B).prefers_and_holds(&a)
    })
    .await?;
    net.hold_for("A preferred on its one connection", QUIET_PERIOD, |net| {
        net.view(B).prefers_and_holds(&a) && one_connection(net) && inbound(net) == direction
    })
    .await?;
    assert_eq!(net.view(B).info.slots.expect("B runs v2").preferred, 1);
    #[cfg(feature = "prometheus")]
    assert_eq!(net.counter(B, "zakura_p2p_conn_accepted").await?, accepted);

    Ok(())
}

/// A node ranks a gateway it has no address for, holds no place for it and
/// dials nothing, until the gateway joins the network and the node learns
/// its address by peer exchange.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts zakurad children: run with the miner-mesh-regtest-extended nextest profile"]
async fn an_unknown_gateway_waits_then_joins() -> Result<()> {
    const HUB: usize = 0;
    const B: usize = 1;
    const A: usize = 2;
    let _init_guard = zakura_test::init();

    let mut net = RegtestNet::start(vec![
        NodeSpec::new(Role::Relay).start_later(),
        NodeSpec::new(Role::Miner)
            .bootstrap_from([HUB])
            .start_later(),
        NodeSpec::new(Role::Pool).bootstrap_from([HUB]),
    ])
    .await?;
    let a = net.gateway_id(A);

    // A mines its markers alone, and the blocks reach the hub by RPC, so no
    // node has a record of A.
    net.mine(A, MIN_GATEWAY_BLOCKS).await?;
    let mut markers = Vec::new();
    for height in 1..=MIN_GATEWAY_BLOCKS {
        markers.push(net.block(A, height).await?);
    }
    net.stop(A).await;
    net.start_node(HUB).await?;
    for marker in markers {
        net.submit_block(HUB, marker).await?;
    }
    net.start_node(B).await?;
    net.settle().await?;
    net.mine(B, QUALIFYING_DEPTH).await?;

    let waits = |net: &RegtestNet| {
        let view = net.view(B);
        view.state_of(&a) == Some(MeshGatewayState::Unmatched)
            && view.info.hold == 0
            && !view.is_connected_to(&a)
    };
    net.wait_for("B ranking A, unmatched", DIAL_TIMEOUT, waits)
        .await?;
    net.hold_for("B waiting for A", QUIET_PERIOD, waits).await?;

    net.start_node(A).await?;
    net.wait_for("B connecting to A", DISCOVERY_TIMEOUT, |net| {
        net.view(B).prefers_and_holds(&a)
    })
    .await?;
    assert_eq!(net.view(B).info.hold, 1);

    Ok(())
}
