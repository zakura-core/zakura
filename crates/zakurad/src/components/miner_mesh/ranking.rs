//! What the tracker tells the network about the mining gateways to prefer.

use tokio::sync::watch;
use zakura_chain::{chain_tip::ChainTip, miner_mesh::MinerSetSnapshot, parameters::Network};
use zakura_network::zakura::{MinerGateway, MinerGatewayRanking};
use zakura_rpc::synced_to_tip;

#[cfg(test)]
mod tests;

/// Returns the gateway ranking the network reads, and the publisher that
/// fills it when the node mines (`is_mining`, set by `mining.miner_address`).
///
/// A node that does not mine gets no publisher, so its ranking stays empty.
pub fn ranking_channel<Tip: ChainTip>(
    is_mining: bool,
    network: Network,
    latest_chain_tip: Tip,
) -> (
    watch::Receiver<MinerGatewayRanking>,
    Option<RankingPublisher<Tip>>,
) {
    let (sender, receiver) = watch::channel(MinerGatewayRanking::default());
    let publisher = is_mining.then(|| RankingPublisher {
        network,
        latest_chain_tip,
        sender,
    });
    (receiver, publisher)
}

/// Hands the network the gateways a synced mining node prefers to stay
/// connected to.
///
/// When the publisher is dropped the network treats the ranking as empty.
pub struct RankingPublisher<Tip> {
    network: Network,
    latest_chain_tip: Tip,
    sender: watch::Sender<MinerGatewayRanking>,
}

impl<Tip: ChainTip> RankingPublisher<Tip> {
    /// Publishes every gateway counted in `snapshot` while the best tip is
    /// recent enough to mine on, and an empty ranking otherwise.
    ///
    /// Whether the tip is recent enough depends on the clock, so the caller
    /// publishes again from time to time even when the counts do not change.
    /// The network only hears of a ranking that differs from the last one.
    ///
    /// Counts from a window that is still being read are published too: a
    /// pruned node never reads its whole window, and holding the ranking
    /// back would empty it on every reorg repair.
    pub(super) fn publish(&self, snapshot: &MinerSetSnapshot) {
        let ranking = if synced_to_tip(&self.network, &self.latest_chain_tip).is_ok() {
            MinerGatewayRanking {
                gateways: snapshot
                    .gateways
                    .iter()
                    .map(|gateway| MinerGateway {
                        gateway_id: gateway.gateway_id,
                        blocks: gateway.blocks,
                    })
                    .collect(),
            }
        } else {
            MinerGatewayRanking::default()
        };

        self.sender.send_if_modified(|current| {
            let changed = *current != ranking;
            if changed {
                *current = ranking;
            }
            changed
        });
    }
}
