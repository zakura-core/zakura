//! Regtest networks of real `zakurad` processes, for how mining nodes find
//! and keep each other.
//!
//! Each test starts several `zakurad` children on loopback, so the tests are
//! ignored by default. Run them with:
//!
//! ```sh
//! cargo nextest run --profile miner-mesh-regtest --features prometheus --run-ignored=all -p zakura
//! ```
//!
//! The tests use the Prometheus endpoint to verify admission refusals and
//! detect replacement connections. Builds without it still check mesh state.
//!
//! The longer scenarios in [`extended`] run under the
//! `miner-mesh-regtest-extended` profile instead.

#![allow(clippy::unwrap_in_result)]

mod common;
#[path = "miner_mesh_regtest/extended.rs"]
mod extended;

use std::{collections::HashMap, time::Duration};

use eyre::Result;

use zakura_rpc::client::MeshGatewayState;

#[cfg(feature = "prometheus")]
use common::regtest_net::FillerRefusal;
use common::regtest_net::{NodeSpec, RegtestNet, Role, MIN_GATEWAY_BLOCKS, QUALIFYING_DEPTH};

/// How long nodes get to learn each other's addresses by peer exchange and
/// connect.
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(90);

/// How long a node gets to connect to a gateway it already knows: a few dial
/// rounds of about a second, after the 5 second wait before dialing a
/// gateway with a lower node ID.
const DIAL_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a state must last to show that nothing is about to change it:
/// several dial rounds, and longer than the wait before dialing a gateway
/// with a lower node ID.
const QUIET_PERIOD: Duration = Duration::from_secs(10);

/// A connection limit small enough to fill quickly.
const SMALL_LIMIT: usize = 8;

/// The inbound connections a node with [`SMALL_LIMIT`] accepts: it keeps a
/// quarter of its limit, rounded down, for its own dials.
const SMALL_LIMIT_INBOUND_PLACES: usize = 6;

/// How long fillers must keep their places, well past a few QUIC keep-alive
/// rounds and the header-sync status exchange.
const HOLD: Duration = Duration::from_secs(45);

/// Three pools and a miner that only know a relay hub find each other: every
/// mining node ranks the pools by the blocks that name them, learns their
/// addresses by peer exchange, and keeps each one in a held place. A pool
/// named in one block fewer than the minimum is counted but ranked nowhere
/// until one more of its blocks counts. The pools end up fully meshed, and a
/// miner that names no gateway is ranked nowhere yet prefers every pool.
/// Relays rank nothing.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts zakurad children: run with the miner-mesh-regtest nextest profile"]
async fn pools_form_a_mesh() -> Result<()> {
    const HUB: usize = 0;
    const A: usize = 1;
    const B: usize = 2;
    const C: usize = 3;
    const M: usize = 4;
    const RELAY: usize = 5;
    let _init_guard = zakura_test::init();

    let mut net = RegtestNet::start(vec![
        NodeSpec::new(Role::Relay),
        NodeSpec::new(Role::Pool).bootstrap_from([HUB]),
        NodeSpec::new(Role::Pool).bootstrap_from([HUB]),
        NodeSpec::new(Role::Pool).bootstrap_from([HUB]),
        NodeSpec::new(Role::Miner).bootstrap_from([HUB]),
        NodeSpec::new(Role::Relay).bootstrap_from([HUB]),
    ])
    .await?;
    let pools = [A, B, C];
    let c = net.gateway_id(C);

    net.mine(A, MIN_GATEWAY_BLOCKS + 2).await?;
    net.mine(B, MIN_GATEWAY_BLOCKS + 1).await?;
    net.mine(C, MIN_GATEWAY_BLOCKS).await?;
    // C's last block is one block short of the qualifying depth, so C counts
    // one block short of the minimum.
    net.mine(M, QUALIFYING_DEPTH - 1).await?;

    let prefers_every_other = |net: &RegtestNet, ranked: &[usize]| {
        [A, B, C, M].into_iter().all(|node| {
            let view = net.view(node);
            let others: Vec<_> = ranked.iter().filter(|&&pool| pool != node).collect();
            view.ranking().len() == others.len()
                && others
                    .iter()
                    .all(|&&pool| view.prefers_and_holds(&net.gateway_id(pool)))
        })
    };
    net.wait_for(
        "every mining node preferring and holding A and B, and not ranking C",
        DISCOVERY_TIMEOUT,
        |net| prefers_every_other(net, &[A, B]),
    )
    .await?;
    for node in [A, B, M] {
        assert_eq!(
            net.view(node).state_of(&c),
            None,
            "node {node} ranks C below the minimum"
        );
    }

    net.mine(M, 1).await?;
    let ranked_pools = net.gateway_ids(pools);
    net.wait_for(
        "every mining node preferring and holding every other pool",
        DISCOVERY_TIMEOUT,
        |net| prefers_every_other(net, &pools),
    )
    .await?;

    for node in [A, B, C, M] {
        let own = net.gateway_id(node);
        let expected: Vec<_> = ranked_pools
            .iter()
            .copied()
            .filter(|id| *id != own)
            .collect();
        assert_eq!(
            net.view(node).ranking(),
            expected,
            "node {node} ranks the pools by their blocks, without itself",
        );
    }
    for node in [HUB, RELAY] {
        assert!(
            net.view(node).ranking().is_empty(),
            "relay {node} ranks nothing"
        );
        assert_eq!(net.view(node).info.hold, 0, "relay {node} holds nothing");
    }

    Ok(())
}

/// With all ordinary slots occupied, a qualifying gateway connects in its
/// separately funded place and ordinary peers keep their connections.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts zakurad children: run with the miner-mesh-regtest nextest profile"]
async fn a_full_node_still_reaches_a_gateway() -> Result<()> {
    const HUB: usize = 0;
    const B: usize = 1;
    const A: usize = 2;
    const ORDINARY_LIMIT: usize = 1;
    #[cfg(feature = "prometheus")]
    const FAILED_PREFERRED_DIALS: &str = "zakura_p2p_discovery_dial_preferred_failed";
    let _init_guard = zakura_test::init();

    let mut net = RegtestNet::start(vec![
        NodeSpec::new(Role::Relay),
        NodeSpec::new(Role::Miner)
            .bootstrap_from([HUB])
            .max_connections(ORDINARY_LIMIT)
            .gateway_capacity(1),
        NodeSpec::new(Role::Pool)
            .bootstrap_from([HUB])
            .start_later(),
    ])
    .await?;
    let a = net.gateway_id(A);
    let hub = net.gateway_id(HUB);

    // The hub fills B's only ordinary place whichever side opens the connection.
    net.wait_for("B using every ordinary place", DIAL_TIMEOUT, |net| {
        let view = net.view(B);
        view.info.slots.expect("B runs v2").ordinary == ORDINARY_LIMIT && view.is_connected_to(&hub)
    })
    .await?;
    let hub_before = net.view(B).peer(&hub).cloned();

    net.start_node(A).await?;
    net.mine(A, MIN_GATEWAY_BLOCKS).await?;
    net.mine(B, QUALIFYING_DEPTH - 1).await?;
    assert_eq!(net.view(B).state_of(&a), None);
    assert!(!net.view(B).is_connected_to(&a));

    net.mine(B, 1).await?;
    net.wait_for(
        "B connecting to A above ordinary capacity",
        DISCOVERY_TIMEOUT,
        |net| net.view(B).prefers_and_holds(&a),
    )
    .await?;
    let view = net.view(B);
    let slots = view.info.slots.expect("B runs v2");
    assert_eq!((slots.ordinary, slots.preferred), (ORDINARY_LIMIT, 1));
    assert_eq!(view.info.hold, 1);
    assert_eq!(view.peer(&hub), hub_before.as_ref());
    #[cfg(feature = "prometheus")]
    net.expect_filler_refused(B, FillerRefusal::AdmissionLimit)
        .await?;

    #[cfg(feature = "prometheus")]
    let failures_before = net.counter(B, FAILED_PREFERRED_DIALS).await?;
    net.stop(A).await;
    #[cfg(feature = "prometheus")]
    {
        use eyre::WrapErr;

        // The report can show backoff as soon as a connection closes. Wait
        // for discovery to fail a redial to A and apply its retry delay.
        tokio::time::timeout(DISCOVERY_TIMEOUT, async {
            while net.counter(B, FAILED_PREFERRED_DIALS).await? <= failures_before {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Ok::<(), eyre::Report>(())
        })
        .await
        .wrap_err("B did not record a failed dial to stopped gateway A")??;
    }
    net.wait_for("B keeping A's place empty", DISCOVERY_TIMEOUT, |net| {
        let view = net.view(B);
        let slots = view.info.slots.expect("B runs v2");
        view.state_of(&a) == Some(MeshGatewayState::Backoff)
            && !view.is_connected_to(&a)
            && view.info.hold == 1
            && slots.ordinary == ORDINARY_LIMIT
            && slots.preferred == 0
    })
    .await?;
    #[cfg(feature = "prometheus")]
    net.expect_filler_refused(B, FillerRefusal::HeldGatewayPlace)
        .await?;
    net.start_node(A).await?;
    net.wait_for("A taking its empty place back", DISCOVERY_TIMEOUT, |net| {
        let view = net.view(B);
        view.prefers_and_holds(&a)
            && view.info.slots.expect("B runs v2").in_use == ORDINARY_LIMIT + 1
    })
    .await?;

    Ok(())
}

/// A gateway already connected as an ordinary peer when the tenth of its
/// markers qualifies becomes preferred on the same connection, without a new
/// one.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts zakurad children: run with the miner-mesh-regtest nextest profile"]
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
#[ignore = "starts zakurad children: run with the miner-mesh-regtest nextest profile"]
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

/// A node with one held place gives it to the next gateway when the first
/// one stops, and back to the first one as soon as it returns, while the
/// second stays connected as an ordinary peer.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts zakurad children: run with the miner-mesh-regtest nextest profile"]
async fn an_unreachable_gateway_gives_way() -> Result<()> {
    const HUB: usize = 0;
    const B: usize = 1;
    const A: usize = 2;
    const C: usize = 3;
    let _init_guard = zakura_test::init();

    let mut net = RegtestNet::start(vec![
        NodeSpec::new(Role::Relay),
        NodeSpec::new(Role::Miner)
            .bootstrap_from([HUB])
            .max_connections(20)
            .gateway_capacity(1),
        NodeSpec::new(Role::Pool).bootstrap_from([HUB, B]),
        NodeSpec::new(Role::Pool).bootstrap_from([HUB]),
    ])
    .await?;
    let (a, c) = (net.gateway_id(A), net.gateway_id(C));

    net.mine(A, MIN_GATEWAY_BLOCKS + 1).await?;
    net.mine(C, MIN_GATEWAY_BLOCKS).await?;
    net.mine(B, QUALIFYING_DEPTH).await?;
    net.wait_for("B holding A", DISCOVERY_TIMEOUT, |net| {
        net.view(B).prefers_and_holds(&a)
    })
    .await?;
    assert_eq!(net.view(B).ranking(), vec![a, c]);
    assert_eq!(net.view(B).info.hold, 1);

    net.stop(A).await;
    net.wait_for("C taking A's place", DISCOVERY_TIMEOUT, |net| {
        let view = net.view(B);
        view.prefers_and_holds(&c) && !view.is_connected_to(&a)
    })
    .await?;

    // A dials B as it starts, since B is one of its bootstrap peers.
    net.start_node(A).await?;
    net.wait_for("A taking its place back", DIAL_TIMEOUT, |net| {
        net.view(B).prefers_and_holds(&a)
    })
    .await?;
    net.hold_for(
        "C staying connected as an ordinary peer",
        QUIET_PERIOD,
        |net| {
            let view = net.view(B);
            view.prefers_and_holds(&a)
                && view.state_of(&c) == Some(MeshGatewayState::Standby)
                && view.peer(&c).is_some_and(|peer| !peer.preferred)
        },
    )
    .await?;

    Ok(())
}

/// A pool names its own node in every block it mines, the blocks reach every
/// node, and a miner's blocks name no gateway.
///
/// A relay hub, a pool and a miner; the pool and the miner bootstrap from the
/// hub only. After the miner restarts, it has the same chain as the others.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts zakurad children: run with the miner-mesh-regtest nextest profile"]
async fn a_pool_names_its_gateway_in_the_blocks_it_mines() -> Result<()> {
    const HUB: usize = 0;
    const POOL: usize = 1;
    const MINER: usize = 2;
    let _init_guard = zakura_test::init();

    let mut net = RegtestNet::start(vec![
        NodeSpec::new(Role::Relay),
        NodeSpec::new(Role::Pool).bootstrap_from([HUB]),
        NodeSpec::new(Role::Miner).bootstrap_from([HUB]),
    ])
    .await?;
    let pool_gateway = net.gateway_id(POOL);

    net.mine(POOL, 3).await?;
    let tip = net.mine(MINER, 2).await?;
    assert_eq!(tip.height.0, 5);

    for node in [HUB, POOL, MINER] {
        let counts = net.recount(node, 1..=tip.height.0).await?;
        assert_eq!(
            counts,
            HashMap::from([(pool_gateway, 3)]),
            "node {node}'s blocks should name the pool three times and no other gateway",
        );
    }

    net.restart(MINER).await?;
    assert_eq!(net.settle().await?, tip);

    Ok(())
}

/// A node that exits outside the harness must fail convergence, even when
/// the remaining node has the expected best block.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts zakurad children: run with the miner-mesh-regtest nextest profile"]
async fn an_unexpected_exit_fails_convergence() -> Result<()> {
    use common::regtest::MiningRpcMethods;

    let _init_guard = zakura_test::init();
    let mut net =
        RegtestNet::start(vec![NodeSpec::new(Role::Relay), NodeSpec::new(Role::Relay)]).await?;
    net.settle().await?;
    net.rpc(1)
        .json_result_from_call::<String>("stop", "[]")
        .await
        .map_err(|error| eyre::eyre!("stopping child by RPC: {error}"))?;
    tokio::time::timeout(DIAL_TIMEOUT, async {
        while net.rpc(1).blockchain_info().await.is_ok() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;
    let error = net
        .settle()
        .await
        .expect_err("the harness still expects both children");
    assert!(
        error.to_string().contains("node 1 exited unexpectedly"),
        "{error}"
    );
    Ok(())
}

/// In-process test nodes can occupy a real node's inbound places: they are
/// admitted up to the limit, the next one is refused, and the admitted ones
/// stay connected.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts zakurad children: run with the miner-mesh-regtest nextest profile"]
async fn a_testkit_node_holds_a_place_on_a_regtest_node() -> Result<()> {
    let _init_guard = zakura_test::init();

    let mut net =
        RegtestNet::start(vec![NodeSpec::new(Role::Relay).max_connections(SMALL_LIMIT)]).await?;

    net.fill(0, SMALL_LIMIT_INBOUND_PLACES).await?;
    #[cfg(feature = "prometheus")]
    net.expect_filler_refused(0, FillerRefusal::AdmissionLimit)
        .await?;

    net.hold_for("all fillers keeping their inbound places", HOLD, |net| {
        let slots = net.view(0).info.slots.expect("relay runs v2");
        slots.inbound == SMALL_LIMIT_INBOUND_PLACES
            && net.connected_fillers(0) == SMALL_LIMIT_INBOUND_PLACES
    })
    .await?;

    Ok(())
}
