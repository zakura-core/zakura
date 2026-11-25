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
#[path = "miner_mesh_regtest/harness/mod.rs"]
mod harness;

use std::time::Duration;

use eyre::Result;

use zakura_rpc::client::MeshGatewayState;

#[cfg(feature = "prometheus")]
use harness::FillerRefusal;
use harness::{NodeSpec, RegtestNet, Role, MIN_GATEWAY_BLOCKS, QUALIFYING_DEPTH};

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

/// A pool whose template node names its public relay: the blocks it mines
/// name the relay, so every other mining node prefers the relay, and so does
/// the template node itself, which holds a place for it. The relay mines
/// nothing, yet counts the blocks and prefers the other pool's gateway. The
/// template node is ranked nowhere, since no block names it.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "starts zakurad children: run with the miner-mesh-regtest nextest profile"]
async fn a_pool_names_its_relay_as_its_gateway() -> Result<()> {
    const HUB: usize = 0;
    const TEMPLATE: usize = 1;
    const RELAY: usize = 2;
    const OTHER_POOL: usize = 3;
    const M: usize = 4;
    let _init_guard = zakura_test::init();

    let mut net = RegtestNet::start(vec![
        NodeSpec::new(Role::Relay),
        NodeSpec::new(Role::Pool)
            .names_gateway(RELAY)
            .bootstrap_from([HUB]),
        NodeSpec::new(Role::MeshRelay).bootstrap_from([HUB]),
        NodeSpec::new(Role::Pool).bootstrap_from([HUB]),
        NodeSpec::new(Role::Miner).bootstrap_from([HUB]),
    ])
    .await?;
    let template = net.gateway_id(TEMPLATE);
    let relay = net.gateway_id(RELAY);
    let other_pool = net.gateway_id(OTHER_POOL);

    net.mine(TEMPLATE, MIN_GATEWAY_BLOCKS).await?;
    net.mine(OTHER_POOL, MIN_GATEWAY_BLOCKS).await?;
    net.mine(M, QUALIFYING_DEPTH).await?;

    net.wait_for(
        "every node that prefers gateways holding the relay and the other pool",
        DISCOVERY_TIMEOUT,
        |net| {
            [TEMPLATE, M].into_iter().all(|node| {
                let view = net.view(node);
                view.prefers_and_holds(&relay) && view.prefers_and_holds(&other_pool)
            }) && net.view(OTHER_POOL).prefers_and_holds(&relay)
                && net.view(RELAY).prefers_and_holds(&other_pool)
        },
    )
    .await?;

    let template_view = net.view(TEMPLATE);
    assert_eq!(
        template_view.info.announced_gateways,
        vec![hex::encode(relay)],
        "the template node's coinbases name the relay"
    );
    assert_eq!(
        template_view.info.node_id,
        Some(hex::encode(net.node_id(TEMPLATE).as_bytes())),
        "the template node still reports its own node ID"
    );

    let relay_view = net.view(RELAY);
    assert!(!relay_view.info.mining && !relay_view.info.announcing);
    assert!(
        relay_view.info.window.is_some(),
        "the relay counts the blocks"
    );
    assert_eq!(relay_view.ranking(), vec![other_pool]);

    for node in [RELAY, OTHER_POOL, M] {
        assert_eq!(
            net.view(node).state_of(&template),
            None,
            "node {node} ranks the template node, which no block names"
        );
    }
    assert!(
        net.view(HUB).ranking().is_empty(),
        "a relay without the miner mesh ranks nothing"
    );

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
