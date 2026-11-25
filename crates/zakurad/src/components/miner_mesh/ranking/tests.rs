//! Which counted gateways the publisher hands the network, and when it hands
//! it an empty ranking.

use chrono::{Duration, Utc};
use zakura_chain::{
    block::Height,
    chain_tip::mock::{MockChainTip, MockChainTipSender},
    miner_mesh::{GatewayBlocks, GatewayId, MinerSetCoverage, MinerSetSnapshot},
    parameters::Network,
};
use zakura_network::zakura::{MinerGateway, MinerGatewayRanking};

use super::{ranking_channel, MIN_GATEWAY_BLOCKS};

/// The gateway ID whose bytes are all `byte`.
fn id(byte: u8) -> GatewayId {
    GatewayId::from_bytes([byte; 20])
}

/// Counts naming three gateways, the last one block short of the minimum.
fn snapshot() -> MinerSetSnapshot {
    snapshot_with([
        (id(7), MIN_GATEWAY_BLOCKS + 2),
        (id(9), MIN_GATEWAY_BLOCKS),
        (id(8), MIN_GATEWAY_BLOCKS - 1),
    ])
}

/// Counts naming each gateway in `gateways`, best first, in a window of 30
/// blocks.
fn snapshot_with<const N: usize>(gateways: [(GatewayId, u32); N]) -> MinerSetSnapshot {
    MinerSetSnapshot {
        tip: None,
        gateways: gateways
            .into_iter()
            .map(|(gateway_id, blocks)| GatewayBlocks { gateway_id, blocks })
            .collect(),
        checked: 30,
        window: Some(Height(1)..=Height(30)),
    }
}

/// The ranking the network should get for [`snapshot`]: the gateways named
/// in at least the minimum of blocks.
fn expected_ranking() -> MinerGatewayRanking {
    MinerGatewayRanking {
        gateways: vec![
            MinerGateway {
                gateway_id: id(7),
                blocks: MIN_GATEWAY_BLOCKS + 2,
            },
            MinerGateway {
                gateway_id: id(9),
                blocks: MIN_GATEWAY_BLOCKS,
            },
        ],
    }
}

/// A chain tip whose block is `age` older than the local clock.
fn tip_aged(age: Duration) -> (MockChainTip, MockChainTipSender) {
    let (tip, sender) = MockChainTip::new();
    sender.send_best_tip_height(Height(100));
    sender.send_best_tip_block_time(Utc::now() - age);
    (tip, sender)
}

#[test]
fn a_node_that_does_not_mine_gets_no_publisher_and_an_empty_ranking() {
    let (tip, _sender) = tip_aged(Duration::zero());
    let (ranking, publisher) = ranking_channel(false, Network::Mainnet, tip);

    assert!(publisher.is_none());
    assert_eq!(*ranking.borrow(), MinerGatewayRanking::default());
}

#[test]
fn a_synced_mining_node_publishes_the_gateways_named_in_enough_blocks() {
    let (tip, _sender) = tip_aged(Duration::minutes(5));
    let (ranking, publisher) = ranking_channel(true, Network::Mainnet, tip);
    let publisher = publisher.expect("a mining node gets a publisher");

    publisher.publish(&snapshot());

    assert_eq!(*ranking.borrow(), expected_ranking());
}

#[test]
fn a_gateway_whose_count_falls_below_the_minimum_leaves_the_ranking() {
    let (tip, _sender) = tip_aged(Duration::minutes(5));
    let (mut ranking, publisher) = ranking_channel(true, Network::Mainnet, tip);
    let publisher = publisher.expect("a mining node gets a publisher");
    publisher.publish(&snapshot_with([(id(9), MIN_GATEWAY_BLOCKS)]));
    ranking.mark_unchanged();

    publisher.publish(&snapshot_with([(id(9), MIN_GATEWAY_BLOCKS - 1)]));

    assert!(ranking.has_changed().expect("the publisher is alive"));
    assert_eq!(*ranking.borrow(), MinerGatewayRanking::default());
}

#[test]
fn a_mining_node_whose_tip_is_too_old_publishes_an_empty_ranking() {
    let (tip, sender) = tip_aged(Duration::minutes(5));
    let (mut ranking, publisher) = ranking_channel(true, Network::Mainnet, tip);
    let publisher = publisher.expect("a mining node gets a publisher");
    publisher.publish(&snapshot());
    ranking.mark_unchanged();

    // More than six hours behind the local clock.
    sender.send_best_tip_block_time(Utc::now() - Duration::hours(7));
    publisher.publish(&snapshot());

    assert!(ranking.has_changed().expect("the publisher is alive"));
    assert_eq!(*ranking.borrow(), MinerGatewayRanking::default());
}

#[test]
fn a_mining_node_with_no_tip_yet_publishes_an_empty_ranking() {
    let (tip, _sender) = MockChainTip::new();
    let (ranking, publisher) = ranking_channel(true, Network::Mainnet, tip);
    let publisher = publisher.expect("a mining node gets a publisher");

    publisher.publish(&snapshot());

    assert_eq!(*ranking.borrow(), MinerGatewayRanking::default());
}

#[test]
fn a_mining_node_on_a_test_network_publishes_whatever_its_tip_age() {
    let (tip, _sender) = tip_aged(Duration::days(30));
    let network = Network::new_regtest(Default::default());
    let (ranking, publisher) = ranking_channel(true, network, tip);
    let publisher = publisher.expect("a mining node gets a publisher");

    publisher.publish(&snapshot());

    assert_eq!(*ranking.borrow(), expected_ranking());
}

#[test]
fn publishing_the_same_ranking_again_does_not_wake_the_network() {
    let (tip, _sender) = tip_aged(Duration::minutes(5));
    let (mut ranking, publisher) = ranking_channel(true, Network::Mainnet, tip);
    let publisher = publisher.expect("a mining node gets a publisher");
    publisher.publish(&snapshot());
    ranking.mark_unchanged();

    publisher.publish(&snapshot());

    assert!(!ranking.has_changed().expect("the publisher is alive"));
}

#[test]
fn the_coverage_reaches_its_receivers_whether_or_not_the_node_is_synced() {
    let (tip, _sender) = MockChainTip::new();
    let (ranking, publisher) = ranking_channel(true, Network::Mainnet, tip);
    let publisher = publisher.expect("a mining node gets a publisher");
    let coverage = publisher.coverage();
    assert_eq!(*coverage.borrow(), MinerSetCoverage::default());

    publisher.publish(&snapshot());

    assert_eq!(*ranking.borrow(), MinerGatewayRanking::default());
    assert_eq!(*coverage.borrow(), snapshot().coverage());
    assert_eq!(coverage.borrow().window_len(), 30);
}
