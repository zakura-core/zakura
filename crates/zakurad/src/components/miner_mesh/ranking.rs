//! What the tracker tells the network about the peers to keep connection
//! places for: this relay's template nodes, then the mining gateways to
//! prefer.

use tokio::sync::watch;
use zakura_chain::{
    chain_tip::ChainTip,
    miner_mesh::{GatewayBlocks, GatewayId, MinerSetCoverage, MinerSetSnapshot},
    parameters::Network,
};
use zakura_network::zakura::{MinerGateway, MinerGatewayRanking};
use zakura_rpc::synced_to_tip;

#[cfg(test)]
mod tests;

/// The fewest counted blocks that must name a gateway before the network
/// prefers it.
///
/// To keep a place for another node's ID at every node that prefers gateways
/// (it mines or sets `mining.miner_mesh`), a node must mine ten blocks naming
/// it every two days. A pool that mines a block now and
/// then does not take places meant for regular miners. At 25-second blocks
/// ten is about 0.15% of a window.
pub const MIN_GATEWAY_BLOCKS: u32 = 10;

/// Returns the ranking the network reads, and the publisher that fills it
/// when the node prefers gateways (`prefers_gateways`: it mines, or it runs
/// the miner mesh as a relay).
///
/// The ranking lists `template_nodes`, the template nodes this relay keeps
/// places for (`mining.miner_mesh_template_nodes`), from the start and
/// whether or not the node is synced, since no block names them. The
/// publisher adds the gateways recent blocks name.
///
/// A node that does not prefer gateways gets no publisher, so its ranking
/// stays empty; startup refuses template nodes on such a node.
pub fn ranking_channel<Tip: ChainTip>(
    prefers_gateways: bool,
    template_nodes: Vec<GatewayId>,
    network: Network,
    latest_chain_tip: Tip,
) -> (
    watch::Receiver<MinerGatewayRanking>,
    Option<RankingPublisher<Tip>>,
) {
    let (sender, receiver) = watch::channel(MinerGatewayRanking::default());
    let publisher = prefers_gateways.then(|| {
        let publisher = RankingPublisher {
            coverage: watch::channel(MinerSetCoverage::default()).0,
            template_nodes,
            network,
            latest_chain_tip,
            sender,
        };
        // Nothing is counted yet, so this lists only the template nodes.
        publisher.send(publisher.ranking(&[]));
        publisher
    });
    (receiver, publisher)
}

/// Hands the network this relay's template nodes and the gateways a synced
/// node prefers to stay connected to, and keeps how much of the window is
/// read for `getminermeshinfo`.
///
/// When the publisher is dropped the network treats the ranking as empty.
pub struct RankingPublisher<Tip> {
    /// The template nodes this relay always keeps places for, in config order.
    template_nodes: Vec<GatewayId>,
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
    /// enough to mine on, and none otherwise, after the template nodes,
    /// which it publishes either way. The [`coverage`](Self::coverage)
    /// receivers get its coverage either way.
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

        self.send(self.ranking(&snapshot.gateways));
    }

    /// Hands the network `ranking` if it differs from the last one.
    fn send(&self, ranking: MinerGatewayRanking) {
        self.sender.send_if_modified(|current| {
            let changed = *current != ranking;
            if changed {
                *current = ranking;
            }
            changed
        });
    }

    /// The ranking for the gateways `counted` names: the template nodes, each
    /// with the counted blocks that name it, then the gateways named in at
    /// least [`MIN_GATEWAY_BLOCKS`] counted blocks while the node is synced.
    fn ranking(&self, counted: &[GatewayBlocks]) -> MinerGatewayRanking {
        let blocks_naming = |gateway_id: GatewayId| {
            counted
                .iter()
                .find(|gateway| gateway.gateway_id == gateway_id)
                .map_or(0, |gateway| gateway.blocks)
        };
        let always_preferred = self
            .template_nodes
            .iter()
            .map(|&gateway_id| MinerGateway {
                gateway_id,
                blocks: blocks_naming(gateway_id),
            })
            .collect();
        let gateways = if synced_to_tip(&self.network, &self.latest_chain_tip).is_ok() {
            counted
                .iter()
                .filter(|gateway| gateway.blocks >= MIN_GATEWAY_BLOCKS)
                .map(|gateway| MinerGateway {
                    gateway_id: gateway.gateway_id,
                    blocks: gateway.blocks,
                })
                .collect()
        } else {
            Vec::new()
        };
        MinerGatewayRanking {
            always_preferred,
            gateways,
        }
    }
}
