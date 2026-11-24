//! When the publisher hands the network the counted gateways, and when it
//! hands it an empty ranking.

use chrono::{Duration, Utc};
use zakura_chain::{
    block::Height,
    chain_tip::mock::{MockChainTip, MockChainTipSender},
    miner_mesh::{GatewayBlocks, MinerSetSnapshot},
    parameters::Network,
};
use zakura_network::zakura::{MinerGateway, MinerGatewayRanking};

use super::ranking_channel;

/// Counts naming two gateways.
fn snapshot() -> MinerSetSnapshot {
    MinerSetSnapshot {
        tip: None,
        gateways: vec![
            GatewayBlocks {
                gateway_id: [7; 32],
                blocks: 3,
            },
            GatewayBlocks {
                gateway_id: [9; 32],
                blocks: 1,
            },
        ],
        checked: 10,
        window_len: 10,
    }
}

/// The ranking the network should get for [`snapshot`].
fn expected_ranking() -> MinerGatewayRanking {
    MinerGatewayRanking {
        gateways: vec![
            MinerGateway {
                gateway_id: [7; 32],
                blocks: 3,
            },
            MinerGateway {
                gateway_id: [9; 32],
                blocks: 1,
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
fn a_synced_mining_node_publishes_every_counted_gateway() {
    let (tip, _sender) = tip_aged(Duration::minutes(5));
    let (ranking, publisher) = ranking_channel(true, Network::Mainnet, tip);
    let publisher = publisher.expect("a mining node gets a publisher");

    publisher.publish(&snapshot());

    assert_eq!(*ranking.borrow(), expected_ranking());
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
