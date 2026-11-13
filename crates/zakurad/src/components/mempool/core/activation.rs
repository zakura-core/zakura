//! Whether the pool core is on, decided as today's pool decides.
//!
//! Today's pool turns on when the syncer is close to the tip and the tip's
//! clock estimate puts it close to the network tip, or when a debug height
//! is reached and a tip exists. It turns off only when the estimate puts it
//! more than a reorg's depth behind. The gap between the two keeps it from
//! flapping. Test networks never turn it off.

use std::time::Duration;

use tokio::sync::watch;
use zakura_chain::{
    block::Height, chain_sync_status::ChainSyncStatus, chain_tip::ChainTip, parameters::Network,
};
use zakura_state::{self as zs, ChainTipChange, LatestChainTip};

use crate::{components::sync::SyncStatus, BoxError};

use super::super::{Config, MAX_ESTIMATED_DISTANCE_TO_ENABLE};

/// Whether the node's validation context is current: the syncer is close to
/// the tip, and the tip's clock estimate puts it close to the network tip.
/// Today's pool asks this to turn on and before it starts a cooldown. It is
/// read fresh each time, and cheap to clone.
#[derive(Clone)]
pub(crate) struct Currentness {
    sync_status: SyncStatus,
    latest_chain_tip: LatestChainTip,
    network: Network,
}

impl Currentness {
    /// Whether the node is current now.
    pub fn is_current(&self) -> bool {
        self.sync_status.is_close_to_tip() && self.is_estimated_close_to_network_tip()
    }

    /// Whether a cooldown earned against a tip at `height` may start now:
    /// the node is current, and its best tip is still at that height, as
    /// today's pool requires of a failure before it starts a cooldown.
    pub fn may_cool(&self, height: Height) -> bool {
        self.is_current() && self.latest_chain_tip.best_tip_height() == Some(height)
    }

    /// Today's `is_estimated_close_to_network_tip`, read from the latest
    /// tip, which is what the chain tip change reads too.
    fn is_estimated_close_to_network_tip(&self) -> bool {
        if self.network.is_a_test_network() {
            return self.latest_chain_tip.best_tip_height().is_some();
        }
        self.latest_chain_tip
            .estimate_distance_to_network_chain_tip(&self.network)
            .is_some_and(|(distance, _height)| distance <= MAX_ESTIMATED_DISTANCE_TO_ENABLE)
    }
}

/// How often the decision is checked when the tip does not change.
const CHECK_INTERVAL: Duration = Duration::from_millis(200);

/// What the decision reads.
pub(super) struct Activation {
    latest_chain_tip: LatestChainTip,
    chain_tip_change: ChainTipChange,
    debug_enable_at_height: Option<Height>,
    currentness: Currentness,
}

impl Activation {
    /// A decision for `config`, from the node's sync status and tip.
    pub fn new(
        config: &Config,
        sync_status: SyncStatus,
        latest_chain_tip: LatestChainTip,
        chain_tip_change: ChainTipChange,
    ) -> Self {
        let currentness = Currentness {
            sync_status,
            latest_chain_tip: latest_chain_tip.clone(),
            network: chain_tip_change.network().clone(),
        };
        Self {
            latest_chain_tip,
            chain_tip_change,
            debug_enable_at_height: config.debug_enable_at_height.map(Height),
            currentness,
        }
    }

    /// What the adapter asks before it starts a cooldown, as today's pool
    /// does when it handles a failure.
    pub fn currentness(&self) -> Currentness {
        self.currentness.clone()
    }

    /// Sends the decision to `active` whenever it changes, until the core is
    /// gone.
    pub async fn run(mut self, active: watch::Sender<bool>) -> Result<(), BoxError> {
        loop {
            let current = self.currentness.is_current();
            let on = *active.borrow();
            let next = self.next(on, current);
            if next != on {
                // Today's pool's log lines, which operators and the
                // acceptance tests look for.
                if next {
                    let reason = if self.is_enabled_by_debug() {
                        tracing::info!(
                            best_tip_height = ?self.latest_chain_tip.best_tip_height(),
                            enable_at_height = ?self.debug_enable_at_height,
                            "enabling mempool for debugging"
                        );
                        "debug height reached"
                    } else {
                        "estimated close to the network tip"
                    };
                    tracing::info!(
                        tip_height = ?self.latest_chain_tip.best_tip_height(),
                        reason,
                        "activating mempool"
                    );
                } else {
                    tracing::info!(
                        estimated_distance_to_network_tip = ?self
                            .chain_tip_change
                            .estimate_distance_to_network_chain_tip()
                            .map(|(distance, _height)| distance),
                        disable_distance = zs::MAX_BLOCK_REORG_HEIGHT,
                        "deactivating mempool: Zakura is far from the network tip"
                    );
                }
                active.send_replace(next);
            }
            if active.is_closed() {
                return Ok(());
            }
            tokio::select! {
                _ = self.chain_tip_change.wait_for_tip_change() => {}
                _ = tokio::time::sleep(CHECK_INTERVAL) => {}
            }
        }
    }

    /// The decision, given whether the pool is on now and whether the node
    /// is current enough.
    fn next(&self, on: bool, current: bool) -> bool {
        let has_tip = self.latest_chain_tip.best_tip_height().is_some();
        if !on {
            return has_tip && (self.is_enabled_by_debug() || current);
        }
        !self.is_far_enough_to_disable()
    }

    /// Whether the debug height is set and reached.
    fn is_enabled_by_debug(&self) -> bool {
        self.debug_enable_at_height.is_some_and(|enable_at| {
            self.latest_chain_tip
                .best_tip_height()
                .is_some_and(|height| height >= enable_at)
        })
    }

    /// Whether the node has fallen far enough behind to turn the pool off.
    fn is_far_enough_to_disable(&self) -> bool {
        !self.is_enabled_by_debug()
            && !self.chain_tip_change.network().is_a_test_network()
            && self
                .chain_tip_change
                .estimate_distance_to_network_chain_tip()
                .is_none_or(|(distance, _height)| distance > i64::from(zs::MAX_BLOCK_REORG_HEIGHT))
    }
}

#[cfg(test)]
mod tests {
    //! The core's on and off decision, against the cases today's pool's
    //! property tests pin for its own: it needs a tip, turns on near the
    //! network tip or at a debug height, stays on until a reorg's depth
    //! behind, and never turns off on a test network.

    use std::sync::Arc;

    use chrono::Utc;
    use zakura_chain::{
        block::{self, Block},
        parameters::NetworkUpgrade,
        serialization::ZcashDeserializeInto,
    };
    use zakura_state::{ChainTipBlock, ChainTipSender, CheckpointVerifiedBlock};

    use super::*;
    use crate::components::sync::RecentSyncLengths;

    /// A tip whose clock estimate puts it `distance` blocks behind the
    /// network tip, built as today's pool's property tests build one.
    fn tip_behind(network: &Network, distance: block::HeightDiff, hash_byte: u8) -> ChainTipBlock {
        let genesis: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into()
            .expect("block test vectors are valid");
        let mut tip = ChainTipBlock::from(CheckpointVerifiedBlock::from(genesis));
        tip.height = block::Height(3_000_000 + u32::from(hash_byte));
        tip.hash = block::Hash([hash_byte; 32]);
        tip.previous_block_hash = block::Hash([hash_byte.saturating_sub(1); 32]);
        let spacing = NetworkUpgrade::target_spacing_for_height(network, tip.height);
        tip.time = Utc::now() - spacing * i32::try_from(distance).expect("a small test distance");
        tip
    }

    /// A decision on `network` with `debug_enable_at_height`, the sender of
    /// its tips, and the sync lengths it reads.
    fn decision(
        network: &Network,
        debug_enable_at_height: Option<u32>,
    ) -> (Activation, ChainTipSender, RecentSyncLengths) {
        let (sync_status, recent_syncs) = SyncStatus::new();
        let (sender, latest, change) = ChainTipSender::new(None, network);
        let config = Config {
            debug_enable_at_height,
            ..Config::default()
        };
        (
            Activation::new(&config, sync_status, latest, change),
            sender,
            recent_syncs,
        )
    }

    /// The decision for a pool that is now `on`.
    fn next(activation: &Activation, on: bool) -> bool {
        activation.next(on, activation.currentness.is_current())
    }

    /// The pool stays off without a tip, and while the syncer is far from
    /// the tip, and turns on once both the syncer and the tip's clock
    /// estimate are close.
    #[test]
    fn turns_on_only_with_a_tip_near_the_network_tip() {
        let network = Network::Mainnet;
        let (activation, mut tips, mut syncs) = decision(&network, None);
        SyncStatus::sync_close_to_tip(&mut syncs);
        assert!(!next(&activation, false), "off without a tip");

        tips.set_finalized_tip(Some(tip_behind(&network, 0, 1)));
        SyncStatus::sync_far_from_tip(&mut syncs);
        assert!(
            !next(&activation, false),
            "off while the syncer is far behind"
        );

        SyncStatus::sync_close_to_tip(&mut syncs);
        assert!(next(&activation, false), "on near the tip");

        tips.set_finalized_tip(Some(tip_behind(
            &network,
            MAX_ESTIMATED_DISTANCE_TO_ENABLE + 1,
            2,
        )));
        assert!(
            !next(&activation, false),
            "off while the clock estimate is too far behind"
        );
    }

    /// Between the two thresholds the pool keeps whichever state it is in:
    /// an active pool stays on until the estimate is past a reorg's depth,
    /// and a pool that turned off stays off until it is close again.
    #[test]
    fn keeps_its_state_between_the_thresholds() {
        let network = Network::Mainnet;
        let (activation, mut tips, mut syncs) = decision(&network, None);
        SyncStatus::sync_close_to_tip(&mut syncs);
        let between = MAX_ESTIMATED_DISTANCE_TO_ENABLE + 1;
        let beyond = i64::from(zs::MAX_BLOCK_REORG_HEIGHT) + 1;

        tips.set_finalized_tip(Some(tip_behind(&network, between, 1)));
        assert!(next(&activation, true), "an active pool stays on");
        assert!(!next(&activation, false), "an inactive pool stays off");

        tips.set_finalized_tip(Some(tip_behind(&network, beyond, 2)));
        assert!(!next(&activation, true), "off past a reorg's depth");
    }

    /// A reached debug height turns the pool on without a current syncer,
    /// and keeps it on however far behind the tip's estimate is.
    #[test]
    fn a_reached_debug_height_turns_it_on_and_keeps_it_on() {
        let network = Network::Mainnet;
        let (activation, mut tips, mut syncs) = decision(&network, Some(3_000_000));
        SyncStatus::sync_far_from_tip(&mut syncs);
        let beyond = i64::from(zs::MAX_BLOCK_REORG_HEIGHT) + 1;
        tips.set_finalized_tip(Some(tip_behind(&network, beyond, 1)));
        assert!(next(&activation, false), "on at the debug height");
        assert!(next(&activation, true), "kept on past a reorg's depth");

        let (activation, mut tips, mut syncs) = decision(&network, Some(4_000_000));
        SyncStatus::sync_far_from_tip(&mut syncs);
        tips.set_finalized_tip(Some(tip_behind(&network, 0, 1)));
        assert!(!next(&activation, false), "off below the debug height");
    }

    /// A test network never turns the pool off, however far behind the
    /// tip's estimate is.
    #[test]
    fn a_test_network_never_turns_it_off() {
        let network = Network::new_default_testnet();
        let (activation, mut tips, mut syncs) = decision(&network, None);
        SyncStatus::sync_close_to_tip(&mut syncs);
        let beyond = i64::from(zs::MAX_BLOCK_REORG_HEIGHT) + 1;
        tips.set_finalized_tip(Some(tip_behind(&network, beyond, 1)));
        assert!(next(&activation, false), "on with a tip and a close syncer");
        assert!(next(&activation, true), "never off on a test network");
    }
}
