//! Metrics for the best chain tip, recorded by the block write task after each
//! commit.
//!
//! These describe where the chain is: the tip's height, header time and
//! interval to its parent, its difficulty and the spacing and averaging
//! window in force, the value pools and the Network Sustainability Mechanism
//! (NSM) balance after it, and reorgs of the best chain. The consensus crate's
//! verified-block metrics describe what each block carries.

use std::collections::VecDeque;

use zakura_chain::{
    amount::NonNegative, block, parameters::NetworkUpgrade, value_balance::ValueBalance,
};

use crate::{
    constants::MAX_BLOCK_REORG_HEIGHT,
    service::{
        finalized_state::ZakuraDb,
        non_finalized_state::{Chain, NonFinalizedState},
    },
    HashOrHeight,
};

/// How many reported tips are kept to attribute a reorg to its fork point.
///
/// Widening from `u32` cannot lose a value on any supported target.
const RECENT_TIPS: usize = MAX_BLOCK_REORG_HEIGHT as usize + 1;

/// Remembers the tips this task has reported, so a reorg can be measured.
#[derive(Debug, Default)]
pub(super) struct TipMetrics {
    /// Reported best tips, oldest first, bounded by the reorg limit.
    recent_tips: VecDeque<(block::Height, block::Hash)>,
}

impl TipMetrics {
    /// Records the best non-finalized tip after a block commit.
    ///
    /// The parent is read from the best chain, or from the finalized database
    /// for the first non-finalized block; when neither has it, the interval and
    /// NSM change gauges are left unchanged rather than guessed.
    pub(super) fn record_committed_tip(
        &mut self,
        non_finalized_state: &NonFinalizedState,
        db: &ZakuraDb,
    ) {
        let Some(best_chain) = non_finalized_state.best_chain() else {
            return;
        };
        let Some(tip) = best_chain.tip_block() else {
            return;
        };
        let network = &non_finalized_state.network;
        let height = tip.height;
        let header = tip.block.header();

        let parent = height.previous().ok().map(HashOrHeight::from);
        let parent_time = parent.and_then(|parent| {
            best_chain
                .block(parent)
                .map(|block| block.block.header().time)
                .or_else(|| db.block_header(parent).map(|header| header.time))
        });
        let parent_pools = parent.and_then(|parent| {
            best_chain
                .block_info(parent)
                .or_else(|| db.block_info(parent))
                .map(|info| *info.value_pools())
        });
        let size = best_chain
            .block_info(tip.hash.into())
            .map_or(0, |info| info.size());

        self.record_reorg(best_chain, db, header.previous_block_hash);

        metrics::gauge!("zcash.chain.tip.height").set(f64::from(height.0));
        metrics::gauge!("zcash.chain.tip.time.seconds").set(seconds(header.time.timestamp()));
        if let Some(parent_time) = parent_time {
            metrics::gauge!("zcash.chain.tip.block_interval.seconds")
                .set(seconds(header.time.timestamp() - parent_time.timestamp()));
        }
        metrics::gauge!("zcash.chain.tip.difficulty")
            .set(header.difficulty_threshold.relative_to_network(network));
        metrics::gauge!("zcash.chain.tip.difficulty.bits").set(f64::from(u32::from_be_bytes(
            header.difficulty_threshold.bytes_in_display_order(),
        )));
        metrics::gauge!("zcash.chain.tip.target_spacing.seconds").set(seconds(
            NetworkUpgrade::target_spacing_for_height(network, height).num_seconds(),
        ));
        metrics::gauge!("zcash.chain.tip.averaging_window.blocks").set(count(
            NetworkUpgrade::averaging_window_for_height(network, height),
        ));
        metrics::gauge!("zcash.chain.tip.nu7_active").set(f64::from(u8::from(
            NetworkUpgrade::is_nu7_active(network, height),
        )));
        metrics::gauge!("zcash.chain.tip.block.size.bytes").set(f64::from(size));
        metrics::gauge!("zcash.chain.tip.block.transactions")
            .set(count(tip.block.transactions().len()));
        metrics::counter!("zcash.chain.tip.blocks.total").increment(1);
        metrics::counter!("zcash.chain.tip.transactions.total")
            .increment(u64::try_from(tip.block.transactions().len()).unwrap_or(u64::MAX));
        metrics::counter!("zcash.chain.tip.bytes.total").increment(u64::from(size));

        let pools = &best_chain.chain_value_pools;
        value_pool_gauges(pools);
        let nsm = i64::from(pools.nsm_value_balance_amount());
        metrics::gauge!("zcash.chain.nsm.value_balance.zat").set(zat(nsm));
        if let Some(parent_pools) = parent_pools {
            metrics::gauge!("zcash.chain.nsm.value_balance.change.zat")
                .set(zat(nsm - i64::from(parent_pools.nsm_value_balance_amount())));
        }

        self.recent_tips.push_back((height, tip.hash));
        while self.recent_tips.len() > RECENT_TIPS {
            self.recent_tips.pop_front();
        }
    }

    /// Counts a reorg when the new tip does not extend the last reported tip.
    fn record_reorg(&self, best_chain: &Chain, db: &ZakuraDb, previous_block_hash: block::Hash) {
        let Some(&(_, last_hash)) = self.recent_tips.back() else {
            return;
        };
        if previous_block_hash == last_hash {
            return;
        }
        let depth = fork_depth(&self.recent_tips, |hash| {
            best_chain.height_by_hash(hash).is_some() || db.height(hash).is_some()
        })
        .unwrap_or_else(|| length(self.recent_tips.len()));
        metrics::counter!("zcash.chain.reorg.total").increment(1);
        metrics::counter!("zcash.chain.reorg.blocks.total").increment(depth);
        metrics::gauge!("zcash.chain.reorg.last_depth.blocks").set(whole(depth));
    }
}

/// How many of the reported tips a reorg rolled back: the distance from the
/// last reported tip to the newest reported tip that is still on the chain.
///
/// Returns `None` when none of the remembered tips survived, which means the
/// reorg is deeper than this task remembers.
fn fork_depth(
    recent_tips: &VecDeque<(block::Height, block::Hash)>,
    on_chain: impl Fn(block::Hash) -> bool,
) -> Option<u64> {
    let (last_height, _) = recent_tips.back()?;
    let (fork_height, _) = recent_tips.iter().rev().find(|(_, hash)| on_chain(*hash))?;
    Some(u64::try_from(*last_height - *fork_height).unwrap_or(0))
}

/// Sets the per-pool and total supply gauges from the tip's value pools.
fn value_pool_gauges(pools: &ValueBalance<NonNegative>) {
    let transparent = u64::from(pools.transparent_amount());
    let sprout = u64::from(pools.sprout_amount());
    let sapling = u64::from(pools.sapling_amount());
    let orchard = u64::from(pools.orchard_amount());
    let ironwood = u64::from(pools.ironwood_amount());
    let lockbox = u64::from(pools.deferred_amount());
    metrics::gauge!("zcash.chain.value_pool.transparent.zat").set(whole(transparent));
    metrics::gauge!("zcash.chain.value_pool.sprout.zat").set(whole(sprout));
    metrics::gauge!("zcash.chain.value_pool.sapling.zat").set(whole(sapling));
    metrics::gauge!("zcash.chain.value_pool.orchard.zat").set(whole(orchard));
    metrics::gauge!("zcash.chain.value_pool.ironwood.zat").set(whole(ironwood));
    metrics::gauge!("zcash.chain.value_pool.lockbox.zat").set(whole(lockbox));
    metrics::gauge!("zcash.chain.value_pool.supply.zat").set(whole(
        transparent + sprout + sapling + orchard + ironwood + lockbox,
    ));
}

/// A signed zatoshi amount as a gauge value.
fn zat(zat: i64) -> f64 {
    // Amounts are bounded by MAX_MONEY (2.1e15 < 2^53), so the conversion is exact.
    zat as f64
}

/// A unix timestamp or block interval as a gauge value.
fn seconds(seconds: i64) -> f64 {
    // Header times and their differences are far below 2^53, so the conversion is exact.
    seconds as f64
}

/// A block-bounded count as a gauge value.
fn count(count: usize) -> f64 {
    // Counts are bounded by the block size limit, far below 2^53, so the conversion is exact.
    count as f64
}

/// An unsigned amount or count as a gauge value.
fn whole(value: u64) -> f64 {
    // Amounts are bounded by MAX_MONEY and counts by the block size limit,
    // both far below 2^53, so the conversion is exact.
    value as f64
}

/// A collection length as a counter increment.
fn length(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(byte: u8) -> block::Hash {
        block::Hash([byte; 32])
    }

    fn tips(heights: impl IntoIterator<Item = u32>) -> VecDeque<(block::Height, block::Hash)> {
        heights
            .into_iter()
            .map(|height| (block::Height(height), hash(height as u8)))
            .collect()
    }

    #[test]
    fn a_tip_that_survived_means_no_rollback() {
        let recent = tips([10, 11, 12]);
        assert_eq!(fork_depth(&recent, |_| true), Some(0));
    }

    #[test]
    fn a_one_block_fork_rolls_back_one_tip() {
        let recent = tips([10, 11, 12]);
        let survived = |hash: block::Hash| hash != self::hash(12);
        assert_eq!(fork_depth(&recent, survived), Some(1));
    }

    #[test]
    fn a_fork_below_every_remembered_tip_is_unmeasured() {
        let recent = tips([10, 11, 12]);
        assert_eq!(fork_depth(&recent, |_| false), None);
        assert_eq!(fork_depth(&VecDeque::new(), |_| true), None);
    }
}
