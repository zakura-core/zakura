//! Decides when outbound connections to pruned peers are useful.
//!
//! Legacy peers without `NODE_NETWORK` are usually pruned nodes. They only
//! serve recent blocks, so Zakura only dials them once its own tip is close
//! to the estimated network tip.

use tokio::{
    sync::watch,
    time::{interval, MissedTickBehavior},
};
use tracing::info;

use zakura_chain::{
    block::HeightDiff,
    chain_tip::ChainTip,
    parameters::{Network, NetworkKind},
};

use crate::{
    constants::{
        PRUNED_PEER_DIAL_CHECK_INTERVAL, PRUNED_PEER_DIAL_MAX_TIP_DISTANCE,
        REGTEST_PRUNED_PEER_DIAL_MAX_TIP_DISTANCE,
    },
    BoxError,
};

#[cfg(test)]
mod tests;

/// Returns the furthest our tip can be behind the network tip for pruned
/// peers to be dialed on `network`.
fn max_tip_distance(network: &Network) -> HeightDiff {
    match network.kind() {
        NetworkKind::Regtest => REGTEST_PRUNED_PEER_DIAL_MAX_TIP_DISTANCE,
        NetworkKind::Mainnet | NetworkKind::Testnet => PRUNED_PEER_DIAL_MAX_TIP_DISTANCE,
    }
}

/// Returns true if our best tip is close enough to the estimated network tip
/// that pruned peers can serve the blocks we still need.
///
/// Returns false if the state is empty.
pub(crate) fn pruned_peers_are_dialable(
    latest_chain_tip: &impl ChainTip,
    network: &Network,
) -> bool {
    latest_chain_tip
        .estimate_distance_to_network_chain_tip(network)
        .is_some_and(|(distance, _tip)| distance <= max_tip_distance(network))
}

/// Publishes whether pruned peers are dialable to `sender`, re-checking every
/// [`PRUNED_PEER_DIAL_CHECK_INTERVAL`].
///
/// Returns `Ok` when every receiver has been dropped.
pub(crate) async fn track_pruned_peer_dialing<C>(
    latest_chain_tip: C,
    network: Network,
    sender: watch::Sender<bool>,
) -> Result<(), BoxError>
where
    C: ChainTip + Send + Sync + 'static,
{
    let mut check = interval(PRUNED_PEER_DIAL_CHECK_INTERVAL);
    check.set_missed_tick_behavior(MissedTickBehavior::Delay);

    while !sender.is_closed() {
        check.tick().await;

        let dialable = pruned_peers_are_dialable(&latest_chain_tip, &network);
        let changed = sender.send_if_modified(|current| {
            let changed = *current != dialable;
            *current = dialable;
            changed
        });

        if changed {
            info!(
                dialable,
                tip_height = ?latest_chain_tip.best_tip_height(),
                "changed whether outbound connections to pruned peers are allowed",
            );
        }
    }

    Ok(())
}
