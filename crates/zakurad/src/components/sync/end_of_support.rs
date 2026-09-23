//! End of support checking task.

use std::time::Duration;

use color_eyre::Report;

use zakura_chain::{
    block::Height,
    chain_tip::ChainTip,
    parameters::{Network, NetworkUpgrade, POST_BLOSSOM_POW_TARGET_SPACING},
};

use crate::application::release_version;

/// The estimated height that this release will be published.
pub const ESTIMATED_RELEASE_HEIGHT: u32 = 3_494_121;

/// The estimated number of blocks per day after Blossom.
///
/// All Zakura releases ship after Blossom, so this matches the spacing seen at
/// every reachable tip height.
pub const ESTIMATED_BLOCKS_PER_DAY: u32 = 24 * 60 * 60 / POST_BLOSSOM_POW_TARGET_SPACING;

/// The maximum number of days after `ESTIMATED_RELEASE_HEIGHT` where a Zebra server will run
/// without halting.
///
/// Notes:
///
/// - Zebra will exit with a panic if the current tip height is bigger than the
///   `ESTIMATED_RELEASE_HEIGHT` plus this number of days.
/// - Currently set to 30 days
///
/// Note: v1.5.0 is planned for 2026-09-23, but its release-height floor is
/// 3,494,121 (~2026-09-25). This window halts after height 3,528,681
/// (~2026-10-25), about 32 days after the planned release.
pub const EOS_PANIC_AFTER: u32 = 30;

/// The number of days before the end of support where Zebra will display warnings.
pub const EOS_WARN_AFTER: u32 = EOS_PANIC_AFTER - 3;

/// A string which is part of the panic that will be displayed if Zebra halts.
pub const EOS_PANIC_MESSAGE_HEADER: &str = "Zakura refuses to run";

/// A string which is part of the warning that will be displayed if Zebra release is close to halting.
pub const EOS_WARN_MESSAGE_HEADER: &str = "Your Zakura release is too old and it will stop running";

/// The amount of time between end of support checks.
const CHECK_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Wait a few seconds at startup so `best_tip_height` is always `Some`.
const INITIAL_WAIT: Duration = Duration::from_secs(10);

/// The metric that reports whether end of support is enforced on this network.
///
/// Set to `1` on Mainnet and `0` on every other network.
const EOS_ENFORCED_METRIC: &str = "end_of_support.enforced";

/// The metric that reports the last height this release supports.
///
/// Only set when end of support is enforced.
const EOS_HEIGHT_METRIC: &str = "end_of_support.last_supported_height";

/// The metric that reports how many blocks are left before this release halts.
///
/// Only set when end of support is enforced. It is negative once the tip goes
/// past the last supported height, which is the check that halts the node.
const EOS_REMAINING_BLOCKS_METRIC: &str = "end_of_support.remaining_blocks";

/// Start the end of support checking task for Mainnet.
pub async fn start(
    network: Network,
    latest_chain_tip: impl ChainTip + std::fmt::Debug,
) -> Result<(), Report> {
    info!("Starting end of support task");

    if let Some(last_supported_height) = end_of_support_height(&network) {
        metrics::gauge!(EOS_ENFORCED_METRIC).set(1.0);
        metrics::gauge!(EOS_HEIGHT_METRIC).set(f64::from(last_supported_height.0));
    } else {
        metrics::gauge!(EOS_ENFORCED_METRIC).set(0.0);
    }

    tokio::time::sleep(INITIAL_WAIT).await;

    loop {
        if network == Network::Mainnet {
            if let Some(tip_height) = latest_chain_tip.best_tip_height() {
                if let Some(remaining_blocks) = remaining_blocks(tip_height, &network) {
                    // A difference between two u32 heights is exactly representable as f64.
                    metrics::gauge!(EOS_REMAINING_BLOCKS_METRIC).set(remaining_blocks as f64);
                }
                check(tip_height, &network);
            }
        } else {
            info!("Release always valid in Testnet");
        }
        tokio::time::sleep(CHECK_INTERVAL).await;
    }
}

/// Returns the last supported height, or `None` when support is not enforced.
///
/// The node runs at this height and halts when the tip goes past it. This
/// matches zcashd's `end_of_service.block_height` threshold semantics.
pub fn end_of_support_height(network: &Network) -> Option<Height> {
    (network == &Network::Mainnet).then(|| estimated_height_after_release(network, EOS_PANIC_AFTER))
}

/// Returns the estimated height `days` after [`ESTIMATED_RELEASE_HEIGHT`] on
/// `network`.
///
/// The estimate follows the target spacing at each height, so it remains valid
/// across target-spacing changes.
fn estimated_height_after_release(network: &Network, days: u32) -> Height {
    let mut height = i64::from(ESTIMATED_RELEASE_HEIGHT);
    let mut remaining_seconds = i64::from(days) * 24 * 60 * 60;

    let target_spacings: Vec<_> = NetworkUpgrade::target_spacings(network).collect();
    for (index, (_, target_spacing)) in target_spacings.iter().enumerate() {
        let target_spacing = target_spacing.num_seconds();
        let remaining_blocks = remaining_seconds / target_spacing;

        // The number of blocks after `height` that still use this target
        // spacing. The block at the next spacing's start height already uses
        // the next spacing, so it is not counted here.
        let blocks_until_next_spacing = target_spacings
            .get(index + 1)
            .map(|(next_height, _)| (i64::from(next_height.0) - height - 1).max(0));

        match blocks_until_next_spacing {
            Some(blocks) if blocks < remaining_blocks => {
                height += blocks;
                remaining_seconds -= blocks * target_spacing;
            }
            _ => {
                height += remaining_blocks;
                break;
            }
        }
    }

    Height(u32::try_from(height).expect("the support window ends below the maximum height"))
}

/// Returns the number of blocks left before this release halts, or `None` when
/// support is not enforced.
///
/// The count is negative once `tip_height` goes past the last supported height,
/// which is the state that halts the node.
pub fn remaining_blocks(tip_height: Height, network: &Network) -> Option<i64> {
    end_of_support_height(network)
        .map(|last_supported_height| i64::from(last_supported_height.0) - i64::from(tip_height.0))
}

/// Check if the current release is too old and panic if so.
pub fn check(tip_height: Height, network: &Network) {
    info!("Checking if Zakura release is inside support range ...");

    let Some(panic_height) = end_of_support_height(network) else {
        info!("Release always valid outside Mainnet");
        return;
    };
    let warn_height = estimated_height_after_release(network, EOS_WARN_AFTER);

    if tip_height > panic_height {
        panic!(
            "{EOS_PANIC_MESSAGE_HEADER} if the release date is older than {EOS_PANIC_AFTER} days. \
            \nRelease name: {}, Estimated release height: {ESTIMATED_RELEASE_HEIGHT} \
            \nHint: Download and install the latest Zakura release from: https://github.com/zakura-core/zakura/releases/latest",
            release_version()
        );
    } else if tip_height > warn_height {
        warn!(
            "{EOS_WARN_MESSAGE_HEADER} at block {}. \
            \nRelease name: {}, Estimated release height: {ESTIMATED_RELEASE_HEIGHT} \
            \nHint: Download and install the latest Zakura release from: https://github.com/zakura-core/zakura/releases/latest", panic_height.0, release_version()
        );
    } else {
        info!("Zakura release is supported until block {}, please report bugs at https://github.com/zakura-core/zakura/issues", panic_height.0);
    }
}

#[cfg(test)]
mod tests {
    use zakura_chain::parameters::testnet::{
        ConfiguredActivationHeights, ConfiguredCheckpoints, RegtestParameters,
    };

    use super::*;

    /// Returns the number of blocks per day at `upgrade`'s target spacing.
    fn blocks_per_day(upgrade: NetworkUpgrade) -> u32 {
        let seconds_per_day = 24 * 60 * 60;
        let spacing = u32::try_from(upgrade.target_spacing().num_seconds())
            .expect("target spacings are positive and fit in u32");

        seconds_per_day / spacing
    }

    /// Returns a Regtest network with Blossom at `blossom`.
    fn regtest_with_blossom(blossom: u32) -> Network {
        let genesis = Network::new_regtest(Default::default()).genesis_hash();
        Network::new_regtest(RegtestParameters {
            activation_heights: ConfiguredActivationHeights {
                blossom: Some(blossom),
                ..Default::default()
            },
            // Canopy defaults to Blossom, so the checkpoints must cover the block before it.
            checkpoints: Some(ConfiguredCheckpoints::HeightsAndHashes(vec![
                (Height(0), genesis),
                (Height(blossom - 1), zakura_chain::block::Hash([1; 32])),
            ])),
            ..Default::default()
        })
    }

    #[test]
    fn mainnet_end_of_support_height_counts_75_second_blocks() {
        let _init_guard = zakura_test::init();
        let post_blossom_blocks_per_day = blocks_per_day(NetworkUpgrade::Blossom);

        assert_eq!(
            end_of_support_height(&Network::Mainnet),
            Some(Height(
                ESTIMATED_RELEASE_HEIGHT + EOS_PANIC_AFTER * post_blossom_blocks_per_day
            )),
        );
        assert_eq!(
            estimated_height_after_release(&Network::Mainnet, EOS_WARN_AFTER),
            Height(ESTIMATED_RELEASE_HEIGHT + EOS_WARN_AFTER * post_blossom_blocks_per_day),
        );
    }

    #[test]
    fn end_of_support_height_follows_target_spacing_changes() {
        let _init_guard = zakura_test::init();
        let pre_blossom_blocks_per_day = blocks_per_day(NetworkUpgrade::Genesis);
        let post_blossom_blocks_per_day = blocks_per_day(NetworkUpgrade::Blossom);

        // Blossom activates after the support window.
        let network = regtest_with_blossom(
            ESTIMATED_RELEASE_HEIGHT + (EOS_PANIC_AFTER + 1) * pre_blossom_blocks_per_day,
        );
        assert_eq!(
            estimated_height_after_release(&network, EOS_PANIC_AFTER),
            Height(ESTIMATED_RELEASE_HEIGHT + EOS_PANIC_AFTER * pre_blossom_blocks_per_day),
        );

        // Blossom activates 7 days into the support window. Blocks up to
        // `blossom - 1` take 150 seconds, which leaves 150 seconds of the
        // seventh day for 2 blocks at 75 seconds, starting with the Blossom
        // activation block.
        let blossom = ESTIMATED_RELEASE_HEIGHT + 7 * pre_blossom_blocks_per_day;
        let network = regtest_with_blossom(blossom);
        assert_eq!(
            estimated_height_after_release(&network, EOS_PANIC_AFTER),
            Height(blossom - 1 + 2 + (EOS_PANIC_AFTER - 7) * post_blossom_blocks_per_day),
        );

        // Blossom activates at the first block after the release, so every
        // block in the window takes 75 seconds.
        let network = regtest_with_blossom(ESTIMATED_RELEASE_HEIGHT + 1);
        assert_eq!(
            estimated_height_after_release(&network, EOS_PANIC_AFTER),
            Height(ESTIMATED_RELEASE_HEIGHT + EOS_PANIC_AFTER * post_blossom_blocks_per_day),
        );
    }
}
