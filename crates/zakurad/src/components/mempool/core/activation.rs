//! Whether the pool core is on, decided as today's pool decides.
//!
//! Today's pool turns on when the syncer is close to the tip and the tip's
//! clock estimate puts it close to the network tip, or when a debug height
//! is reached and a tip exists. It turns off only when the estimate puts it
//! more than a reorg's depth behind. The gap between the two keeps it from
//! flapping. Test networks never turn it off.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::sync::watch;
use zakura_chain::{block::Height, chain_sync_status::ChainSyncStatus, chain_tip::ChainTip};
use zakura_state::{self as zs, ChainTipChange, LatestChainTip};

use crate::{components::sync::SyncStatus, BoxError};

use super::super::{is_estimated_close_to_network_tip, Config};

/// How often the decision is checked when the tip does not change.
const CHECK_INTERVAL: Duration = Duration::from_millis(200);

/// What the decision reads.
pub(super) struct Activation {
    sync_status: SyncStatus,
    latest_chain_tip: LatestChainTip,
    chain_tip_change: ChainTipChange,
    debug_enable_at_height: Option<Height>,
    /// Whether the node's validation context is current enough to start
    /// cooldowns, which the verifier reads.
    current: Arc<AtomicBool>,
}

impl Activation {
    /// A decision for `config`, from the node's sync status and tip.
    pub fn new(
        config: &Config,
        sync_status: SyncStatus,
        latest_chain_tip: LatestChainTip,
        chain_tip_change: ChainTipChange,
    ) -> Self {
        Self {
            sync_status,
            latest_chain_tip,
            chain_tip_change,
            debug_enable_at_height: config.debug_enable_at_height.map(Height),
            current: Arc::default(),
        }
    }

    /// The flag that says whether the node is current enough to start
    /// cooldowns. Today's pool starts them only then.
    pub fn current_flag(&self) -> Arc<AtomicBool> {
        self.current.clone()
    }

    /// Sends the decision to `active` whenever it changes, until the core is
    /// gone.
    pub async fn run(mut self, active: watch::Sender<bool>) -> Result<(), BoxError> {
        loop {
            let current = self.is_current_enough();
            self.current.store(current, Ordering::Relaxed);
            let on = *active.borrow();
            let next = self.next(on, current);
            if next != on {
                tracing::info!(on = next, "the pool core is turning on or off");
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

    /// Whether sync throughput and the tip estimate permit the pool.
    fn is_current_enough(&self) -> bool {
        self.sync_status.is_close_to_tip()
            && is_estimated_close_to_network_tip(&self.chain_tip_change)
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
