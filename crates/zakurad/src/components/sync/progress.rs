//! Progress tracking for blockchain syncing.

use std::{
    cmp::min,
    ops::Add,
    time::{Duration, Instant},
};

use chrono::Utc;

use tokio::sync::watch;
use zakura_chain::{
    block::{Height, HeightDiff},
    chain_sync_status::ChainSyncStatus,
    chain_tip::ChainTip,
    fmt::humantime_seconds,
    parameters::{Network, NetworkUpgrade},
};
use zakura_state::MAX_BLOCK_REORG_HEIGHT;

use crate::components::{health::ChainTipMetrics, sync::SyncStatus};

/// The amount of time between progress logs.
const LOG_INTERVAL: Duration = Duration::from_secs(60);

/// The amount of time between progress bar updates.
const PROGRESS_BAR_INTERVAL: Duration = Duration::from_secs(5);

/// The number of blocks we consider to be close to the tip.
///
/// Most chain forks are 1-7 blocks long.
const MAX_CLOSE_TO_TIP_BLOCKS: HeightDiff = 1;

/// Skip slow sync warnings when we are this close to the tip.
///
/// In testing, we've seen warnings around 30 blocks.
///
/// TODO: replace with `MAX_CLOSE_TO_TIP_BLOCKS` after fixing slow syncing near tip (#3375)
const MIN_SYNC_WARNING_BLOCKS: HeightDiff = 60;

/// The number of fractional digits in sync percentages.
const SYNC_PERCENT_FRAC_DIGITS: usize = 3;

/// The minimum number of extra blocks mined between updating a checkpoint list,
/// and running an automated test that depends on that list.
///
/// Makes sure that the block finalization code always runs in sync tests,
/// even if the miner or test node clock is wrong by a few minutes.
///
/// This is an estimate based on the time it takes to:
/// - get the tip height from `zcashd`,
/// - run `zakura-checkpoints` to update the checkpoint list,
/// - submit a pull request, and
/// - run a CI test that logs progress based on the new checkpoint height.
///
/// We might add tests that sync from a cached tip state,
/// so we only allow a few extra blocks here.
//
// TODO: change to HeightDiff?
const MIN_BLOCKS_MINED_AFTER_CHECKPOINT_UPDATE: u32 = 10;

/// Logs Zebra's estimated progress towards the chain tip every minute or so, and
/// updates a terminal progress bar every few seconds.
///
/// TODO:
/// - log progress towards, remaining blocks before, and remaining time to next network upgrade
/// - add some progress info to the metrics
pub async fn show_block_chain_progress(
    network: Network,
    latest_chain_tip: impl ChainTip,
    sync_status: SyncStatus,
    chain_tip_metrics_sender: watch::Sender<ChainTipMetrics>,
) -> ! {
    // The minimum number of extra blocks after the highest checkpoint, based on:
    // - the non-finalized state limit, and
    // - the minimum number of extra blocks mined between a checkpoint update,
    //   and the automated tests for that update.
    let min_after_checkpoint_blocks =
        MAX_BLOCK_REORG_HEIGHT + MIN_BLOCKS_MINED_AFTER_CHECKPOINT_UPDATE;
    let min_after_checkpoint_blocks: HeightDiff = min_after_checkpoint_blocks.into();

    // The minimum height of the valid best chain, based on:
    // - the hard-coded checkpoint height,
    // - the minimum number of blocks after the highest checkpoint.
    let after_checkpoint_height = network
        .checkpoint_list()
        .max_height()
        .add(min_after_checkpoint_blocks)
        .expect("hard-coded checkpoint height is far below Height::MAX");

    // The last time we downloaded and verified at least one block.
    //
    // Initialized to the start time to simplify the code.
    let mut last_state_change_time = Utc::now();
    let mut last_state_change_instant = Instant::now();

    // The state tip height, when we last downloaded and verified at least one block.
    //
    // Initialized to the genesis height to simplify the code.
    let mut last_state_change_height = Height(0);

    // The last time we logged an update.
    let mut last_log_time = Instant::now();

    #[cfg(feature = "progress-bar")]
    let block_bar = howudoin::new().label("Blocks");
    let mut is_chain_metrics_chan_closed = false;

    loop {
        let now = Utc::now();
        let instant_now = Instant::now();

        let is_syncer_stopped = sync_status.is_close_to_tip();

        if let Some(estimated_height) =
            latest_chain_tip.estimate_network_chain_tip_height(&network, now)
        {
            // The estimate/actual race doesn't matter here,
            // because we're only using it for metrics and logging.
            let current_height = latest_chain_tip
                .best_tip_height()
                .expect("unexpected empty state: estimate requires a block height");
            let network_upgrade = NetworkUpgrade::current(&network, current_height);

            // Send progress reports for block height
            //
            // TODO: split the progress bar height update into its own function.
            #[cfg(feature = "progress-bar")]
            if matches!(howudoin::cancelled(), Some(true)) {
                block_bar.close();
            } else {
                block_bar
                    .set_pos(current_height.0)
                    .set_len(u64::from(estimated_height.0));
            }

            let mut remaining_sync_blocks = estimated_height - current_height;

            if remaining_sync_blocks < 0 {
                remaining_sync_blocks = 0;
            }

            // Export sync distance metrics for observability
            metrics::gauge!("sync.estimated_network_tip_height").set(estimated_height.0 as f64);
            metrics::gauge!("sync.estimated_distance_to_tip").set(remaining_sync_blocks as f64);

            // Work out how long it has been since the state height has increased.
            //
            // Non-finalized forks can decrease the height, we only want to track increases.
            if current_height > last_state_change_height {
                last_state_change_height = current_height;
                last_state_change_time = now;
                last_state_change_instant = instant_now;
            }

            if !is_chain_metrics_chan_closed {
                if let Err(err) = chain_tip_metrics_sender.send(ChainTipMetrics::new(
                    last_state_change_instant,
                    Some(remaining_sync_blocks),
                )) {
                    tracing::warn!(?err, "chain tip metrics channel closed");
                    is_chain_metrics_chan_closed = true
                };
            }

            // Skip logging and status updates if it isn't time for them yet.
            let elapsed_since_log = instant_now.saturating_duration_since(last_log_time);
            if elapsed_since_log < LOG_INTERVAL {
                tokio::time::sleep(PROGRESS_BAR_INTERVAL).await;
                continue;
            } else {
                last_log_time = instant_now;
            }

            // TODO: split logging / status updates into their own function.

            // The block spacing at the estimated network tip sets how often the state height
            // increases near the tip. ZIP 218 shortens it at NU7.
            let target_block_spacing =
                NetworkUpgrade::target_spacing_for_height(&network, estimated_height);
            let max_block_spacing =
                NetworkUpgrade::minimum_difficulty_spacing_for_height(&network, estimated_height);
            let min_state_block_interval =
                min_state_block_interval(target_block_spacing, max_block_spacing);

            // Formatted strings for logging.
            let target_block_spacing = humantime_seconds(
                target_block_spacing
                    .to_std()
                    .expect("constant fits in std::Duration"),
            );
            let max_block_spacing = max_block_spacing
                .map(|duration| {
                    humantime_seconds(duration.to_std().expect("constant fits in std::Duration"))
                })
                .unwrap_or_else(|| "None".to_string());

            // Work out the sync progress towards the estimated tip.
            let sync_progress = f64::from(current_height.0) / f64::from(estimated_height.0);
            let sync_percent = format!(
                "{:.frac$}%",
                sync_progress * 100.0,
                frac = SYNC_PERCENT_FRAC_DIGITS,
            );

            let time_since_last_state_block_chrono =
                now.signed_duration_since(last_state_change_time);
            let time_since_last_state_block = humantime_seconds(
                time_since_last_state_block_chrono
                    .to_std()
                    .unwrap_or_default(),
            );

            if time_since_last_state_block_chrono > min_state_block_interval {
                // The state tip height hasn't increased for a long time.
                //
                // Block verification can fail if the local node's clock is wrong.
                warn!(
                    %sync_percent,
                    ?current_height,
                    ?network_upgrade,
                    %time_since_last_state_block,
                    %target_block_spacing,
                    %max_block_spacing,
                    ?is_syncer_stopped,
                    "chain updates have stalled, \
                     state height has not increased for {} minutes. \
                     Hint: check your network connection, \
                     and your computer clock and time zone",
                    time_since_last_state_block_chrono.num_minutes(),
                );

                // TODO: use add_warn(), but only add each warning once
                #[cfg(feature = "progress-bar")]
                block_bar.desc(format!("{network_upgrade}: sync has stalled"));
            } else if is_syncer_stopped && remaining_sync_blocks > MIN_SYNC_WARNING_BLOCKS {
                // We've stopped syncing blocks, but we estimate we're a long way from the tip.
                //
                // TODO: warn after fixing slow syncing near tip (#3375)
                info!(
                    %sync_percent,
                    ?current_height,
                    ?network_upgrade,
                    ?remaining_sync_blocks,
                    ?after_checkpoint_height,
                    %time_since_last_state_block,
                    "initial sync is very slow, or estimated tip is wrong. \
                     Hint: check your network connection, \
                     and your computer clock and time zone",
                );

                #[cfg(feature = "progress-bar")]
                block_bar.desc(format!(
                    "{network_upgrade}: sync is very slow, or estimated tip is wrong"
                ));
            } else if is_syncer_stopped && current_height <= after_checkpoint_height {
                // We've stopped syncing blocks,
                // but we're below the minimum height estimated from our checkpoints.
                let min_minutes_after_checkpoint_update =
                    min_minutes_after_checkpoint_update(&network, estimated_height);

                warn!(
                    %sync_percent,
                    ?current_height,
                    ?network_upgrade,
                    ?remaining_sync_blocks,
                    ?after_checkpoint_height,
                    %time_since_last_state_block,
                    "initial sync is very slow, and state is below the highest checkpoint. \
                     Hint: check your network connection, \
                     and your computer clock and time zone. \
                     Dev Hint: were the checkpoints updated in the last {} minutes?",
                    min_minutes_after_checkpoint_update,
                );

                #[cfg(feature = "progress-bar")]
                block_bar.desc(format!("{network_upgrade}: sync is very slow"));
            } else if is_syncer_stopped {
                // We've stayed near the tip for a while, and we've stopped syncing lots of blocks.
                // So we're mostly using gossiped blocks now.
                info!(
                    %sync_percent,
                    ?current_height,
                    ?network_upgrade,
                    ?remaining_sync_blocks,
                    %time_since_last_state_block,
                    "finished initial sync to chain tip, using gossiped blocks",
                );

                #[cfg(feature = "progress-bar")]
                block_bar.desc(format!("{network_upgrade}: waiting for next block"));
            } else if remaining_sync_blocks <= MAX_CLOSE_TO_TIP_BLOCKS {
                // We estimate we're near the tip, but we have been syncing lots of blocks recently.
                // We might also be using some gossiped blocks.
                info!(
                    %sync_percent,
                    ?current_height,
                    ?network_upgrade,
                    ?remaining_sync_blocks,
                    %time_since_last_state_block,
                    "close to finishing initial sync, \
                     confirming using syncer and gossiped blocks",
                );

                #[cfg(feature = "progress-bar")]
                block_bar.desc(format!("{network_upgrade}: finishing initial sync"));
            } else {
                // We estimate we're far from the tip, and we've been syncing lots of blocks.
                info!(
                    %sync_percent,
                    ?current_height,
                    ?network_upgrade,
                    ?remaining_sync_blocks,
                    %time_since_last_state_block,
                    "estimated progress to chain tip",
                );

                #[cfg(feature = "progress-bar")]
                block_bar.desc(format!("{network_upgrade}: syncing blocks"));
            }
        } else {
            let sync_percent = format!("{:.SYNC_PERCENT_FRAC_DIGITS$} %", 0.0f64,);
            #[cfg(feature = "progress-bar")]
            let network_upgrade = NetworkUpgrade::Genesis;

            if is_syncer_stopped {
                // We've stopped syncing blocks,
                // but we haven't downloaded and verified the genesis block.
                warn!(
                    %sync_percent,
                    current_height = %"None",
                    "initial sync can't download and verify the genesis block. \
                     Hint: check your network connection, \
                     and your computer clock and time zone",
                );

                #[cfg(feature = "progress-bar")]
                block_bar.desc(format!("{network_upgrade}: can't download genesis block"));
            } else {
                // We're waiting for the genesis block to be committed to the state,
                // before we can estimate the best chain tip.
                info!(
                    %sync_percent,
                    current_height = %"None",
                    "initial sync is waiting to download the genesis block",
                );

                #[cfg(feature = "progress-bar")]
                block_bar.desc(format!(
                    "{network_upgrade}: waiting to download genesis block"
                ));
            }
        }

        tokio::time::sleep(min(LOG_INTERVAL, PROGRESS_BAR_INTERVAL)).await;
    }
}

/// Returns the interval where we expect the state height to increase at least once.
///
/// The `max_block_spacing` is the Testnet minimum difficulty gap, or `None` where that rule is
/// inactive. Most chain forks are 1-7 blocks long.
///
/// TODO: remove the target_block_spacing multiplier,
///       after fixing slow syncing near tip (#3375)
fn min_state_block_interval(
    target_block_spacing: chrono::Duration,
    max_block_spacing: Option<chrono::Duration>,
) -> chrono::Duration {
    max_block_spacing.unwrap_or(target_block_spacing * 4) * 2
}

/// Returns the minimum number of minutes between a checkpoint list update and a test that depends
/// on it, based on the target spacing at the estimated network tip `height`.
fn min_minutes_after_checkpoint_update(network: &Network, height: Height) -> u64 {
    let target_spacing =
        u64::try_from(NetworkUpgrade::target_spacing_for_height(network, height).num_seconds())
            .expect("target spacings are positive");

    (u64::from(MIN_BLOCKS_MINED_AFTER_CHECKPOINT_UPDATE) * target_spacing).div_ceil(60)
}

#[cfg(test)]
mod tests {
    use zakura_chain::parameters::{testnet::ConfiguredActivationHeights, ZIP218_ENABLED};

    use super::*;

    /// Returns the stall warning interval, in seconds, when the estimated network tip is `height`.
    fn stall_interval_seconds(network: &Network, height: Height) -> i64 {
        min_state_block_interval(
            NetworkUpgrade::target_spacing_for_height(network, height),
            NetworkUpgrade::minimum_difficulty_spacing_for_height(network, height),
        )
        .num_seconds()
    }

    #[test]
    fn progress_timing_follows_tip_target_spacing() {
        let _init_guard = zakura_test::init();

        // Mainnet and Testnet have no NU7 height, so their values stay unchanged.
        let tip = Height(3_500_000);
        assert_eq!(stall_interval_seconds(&Network::Mainnet, tip), 600);
        assert_eq!(
            min_minutes_after_checkpoint_update(&Network::Mainnet, tip),
            13
        );

        let testnet = Network::new_default_testnet();
        assert_eq!(stall_interval_seconds(&testnet, tip), 900);
        assert_eq!(min_minutes_after_checkpoint_update(&testnet, tip), 13);

        const NU7: u32 = 1_000;
        let regtest = Network::new_regtest(
            ConfiguredActivationHeights {
                nu7: Some(NU7),
                ..Default::default()
            }
            .into(),
        );
        let pre_nu7 = Height(NU7 - 1);
        let post_nu7 = Height(NU7);

        assert_eq!(stall_interval_seconds(&regtest, pre_nu7), 600);
        assert_eq!(min_minutes_after_checkpoint_update(&regtest, pre_nu7), 13);

        let (post_nu7_interval, post_nu7_minutes) =
            if ZIP218_ENABLED { (200, 5) } else { (600, 13) };
        assert_eq!(
            stall_interval_seconds(&regtest, post_nu7),
            post_nu7_interval
        );
        assert_eq!(
            min_minutes_after_checkpoint_update(&regtest, post_nu7),
            post_nu7_minutes
        );
    }
}
