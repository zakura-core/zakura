//! What the tracker tells the network about the mining gateways to prefer.

use tokio::sync::watch;
use zakura_chain::{
    chain_tip::ChainTip,
    miner_mesh::{MinerSetCoverage, MinerSetSnapshot},
    parameters::Network,
};
use zakura_network::zakura::{MinerGateway, MinerGatewayRanking};
use zakura_rpc::synced_to_tip;

#[cfg(test)]
mod tests;

/// The fewest counted blocks that must name a gateway before the network
/// prefers it.
///
/// To keep a place at every mining node for another node's ID, a node must
/// mine ten blocks naming it every two days. A pool that mines a block now and
/// then does not take places meant for regular miners. At 25-second blocks
/// ten is about 0.15% of a window.
pub const MIN_GATEWAY_BLOCKS: u32 = 10;

/// Returns the gateway ranking the network reads, and the publisher that
/// fills it when the node prefers gateways (`prefers_gateways`: it mines, or
/// it runs the miner mesh as a relay).
///
/// A node that does not prefer gateways gets no publisher, so its ranking
/// stays empty.
pub fn ranking_channel<Tip: ChainTip>(
    prefers_gateways: bool,
    network: Network,
    latest_chain_tip: Tip,
) -> (
    watch::Receiver<MinerGatewayRanking>,
    Option<RankingPublisher<Tip>>,
) {
    let (sender, receiver) = watch::channel(MinerGatewayRanking::default());
    let publisher = prefers_gateways.then(|| RankingPublisher {
        coverage: watch::channel(MinerSetCoverage::default()).0,
        network,
        latest_chain_tip,
        sender,
    });
    (receiver, publisher)
}

/// Hands the network the gateways a synced node prefers to stay
/// connected to, and keeps how much of the window is read for
/// `getminermeshinfo`.
///
/// When the publisher is dropped the network treats the ranking as empty.
pub struct RankingPublisher<Tip> {
    network: Network,
    latest_chain_tip: Tip,
    sender: watch::Sender<MinerGatewayRanking>,
    /// The coverage of the counts last published, synced or not.
    coverage: watch::Sender<MinerSetCoverage>,
}

impl<Tip: ChainTip> RankingPublisher<Tip> {
    /// Returns a receiver of the coverage of the counts this publisher is
    /// handed, starting with no tip. It keeps the last coverage after the
    /// publisher is dropped.
    pub fn coverage(&self) -> watch::Receiver<MinerSetCoverage> {
        self.coverage.subscribe()
    }

    /// Publishes the gateways of `snapshot` named in at least
    /// [`MIN_GATEWAY_BLOCKS`] counted blocks while the best tip is recent
    /// enough to mine on, and an empty ranking otherwise. The
    /// [`coverage`](Self::coverage) receivers get its coverage either way.
    ///
    /// Whether the tip is recent enough depends on the clock, so the caller
    /// publishes again from time to time even when the counts do not change.
    /// The network only hears of a ranking that differs from the last one.
    ///
    /// Counts from a window that is still being read are published too: a
    /// pruned node never reads its whole window, and holding the ranking
    /// back would empty it on every reorg repair.
    pub(super) fn publish(&self, snapshot: &MinerSetSnapshot) {
        let coverage = snapshot.coverage();
        self.coverage.send_if_modified(|current| {
            let changed = *current != coverage;
            if changed {
                *current = coverage;
            }
            changed
        });

        let ranking = if synced_to_tip(&self.network, &self.latest_chain_tip).is_ok() {
            MinerGatewayRanking {
                gateways: snapshot
                    .gateways
                    .iter()
                    .filter(|gateway| gateway.blocks >= MIN_GATEWAY_BLOCKS)
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
