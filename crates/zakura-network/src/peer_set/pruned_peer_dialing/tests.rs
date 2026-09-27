//! Tests for deciding when pruned peers are dialed.

use std::time::Duration;

use tokio::sync::watch;

use zakura_chain::{
    block::Height,
    chain_tip::{mock::MockChainTip, NoChainTip},
    parameters::Network,
};

use crate::constants::{
    PRUNED_PEER_DIAL_MAX_TIP_DISTANCE, REGTEST_PRUNED_PEER_DIAL_MAX_TIP_DISTANCE,
};

use super::{pruned_peers_are_dialable, track_pruned_peer_dialing};

/// Pruned peers are dialable only when our tip is within the network-specific distance.
#[test]
fn pruned_peers_are_dialable_only_near_tip() {
    let (tip, sender) = MockChainTip::new();
    let mainnet = Network::Mainnet;
    let regtest = Network::new_regtest(Default::default());

    assert!(
        !pruned_peers_are_dialable(&NoChainTip, &mainnet),
        "an empty state is never near the tip",
    );

    sender.send_best_tip_height(Height(1_000_000));
    for (network, max_distance) in [
        (&mainnet, PRUNED_PEER_DIAL_MAX_TIP_DISTANCE),
        (&regtest, REGTEST_PRUNED_PEER_DIAL_MAX_TIP_DISTANCE),
    ] {
        sender.send_estimated_distance_to_network_chain_tip(Some(0));
        assert!(pruned_peers_are_dialable(&tip, network));

        sender.send_estimated_distance_to_network_chain_tip(Some(max_distance));
        assert!(pruned_peers_are_dialable(&tip, network));

        sender.send_estimated_distance_to_network_chain_tip(Some(max_distance + 1));
        assert!(!pruned_peers_are_dialable(&tip, network));
    }
}

/// The tracker publishes changes in dialability, and stops when unused.
#[tokio::test(start_paused = true)]
async fn tracker_publishes_dialability() {
    let (tip, sender) = MockChainTip::new();
    sender.send_best_tip_height(Height(1_000_000));
    sender.send_estimated_distance_to_network_chain_tip(Some(0));

    let (dial_tx, mut dial_rx) = watch::channel(false);
    let tracker = tokio::spawn(track_pruned_peer_dialing(tip, Network::Mainnet, dial_tx));

    // The first check runs immediately.
    tokio::time::timeout(Duration::from_secs(5), dial_rx.changed())
        .await
        .expect("the first check runs without waiting for an interval")
        .expect("the tracker keeps its sender while a receiver exists");
    assert!(*dial_rx.borrow_and_update());

    // Falling behind disables dialing at the next check.
    sender
        .send_estimated_distance_to_network_chain_tip(Some(PRUNED_PEER_DIAL_MAX_TIP_DISTANCE + 1));
    tokio::time::timeout(Duration::from_secs(60), dial_rx.changed())
        .await
        .expect("the next check runs within one interval")
        .expect("the tracker keeps its sender while a receiver exists");
    assert!(!*dial_rx.borrow_and_update());

    drop(dial_rx);
    tokio::time::timeout(Duration::from_secs(60), tracker)
        .await
        .expect("the tracker stops at its next check after every receiver is dropped")
        .expect("the tracker does not panic")
        .expect("the tracker returns Ok when every receiver is dropped");
}
