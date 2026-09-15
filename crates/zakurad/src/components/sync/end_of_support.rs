//! End of support checking task.

use std::time::Duration;

use color_eyre::Report;

use zakura_chain::{
    block::Height,
    chain_tip::ChainTip,
    parameters::{Network, NetworkUpgrade},
};

use crate::application::release_version;

/// The estimated height that this release will be published.
pub const ESTIMATED_RELEASE_HEIGHT: u32 = 3_480_539;

/// The maximum number of days after `ESTIMATED_RELEASE_HEIGHT` where a Zebra server will run
/// without halting.
///
/// Notes:
///
/// - Zebra will exit with a panic if the current tip height is bigger than the
///   `ESTIMATED_RELEASE_HEIGHT` plus this number of days.
/// - Currently set to 21 days
///
/// Note: v1.4.0 is estimated to release at height 3,480,539 (~2026-09-12)
/// and halts 21 days later at height 3,504,731 (~2026-10-03) — the same
/// halt block and date as the v1.4.0 release candidates and v1.3.2.
pub const EOS_PANIC_AFTER: u32 = 21;

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

/// Returns the estimated height `days` after [`ESTIMATED_RELEASE_HEIGHT`] on `network`.
///
/// The estimate follows the target spacing at each height, so ZIP 218's 25 second
/// spacing after NU7 fits three times as many blocks into each day.
fn estimated_height_after_release(network: &Network, days: u32) -> Height {
    let mut height = i64::from(ESTIMATED_RELEASE_HEIGHT);
    let mut remaining_seconds = i64::from(days) * 24 * 60 * 60;

    let target_spacings: Vec<_> = NetworkUpgrade::target_spacings(network).collect();
    for (index, (_, target_spacing)) in target_spacings.iter().enumerate() {
        let target_spacing = target_spacing.num_seconds();
        let remaining_blocks = remaining_seconds / target_spacing;

        // The number of blocks from `height` to the start of the next target spacing.
        let blocks_until_next_spacing = target_spacings
            .get(index + 1)
            .map(|(next_height, _)| (i64::from(next_height.0) - height).max(0));

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

    Height(u32::try_from(height).expect("the support window ends far below the maximum height"))
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
    use zakura_chain::parameters::{testnet::ConfiguredActivationHeights, ZIP218_ENABLED};

    use super::*;

    /// The number of blocks per day at 75 second spacing.
    const PRE_NU7_BLOCKS_PER_DAY: u32 = 1_152;

    /// The number of blocks per day at 25 second spacing.
    const POST_NU7_BLOCKS_PER_DAY: u32 = 3_456;

    /// Returns a Regtest network with NU7 at `nu7`.
    fn regtest_with_nu7(nu7: u32) -> Network {
        Network::new_regtest(
            ConfiguredActivationHeights {
                nu7: Some(nu7),
                ..Default::default()
            }
            .into(),
        )
    }

    #[test]
    fn mainnet_end_of_support_height_counts_75_second_blocks() {
        let _init_guard = zakura_test::init();

        assert_eq!(
            end_of_support_height(&Network::Mainnet),
            Some(Height(
                ESTIMATED_RELEASE_HEIGHT + EOS_PANIC_AFTER * PRE_NU7_BLOCKS_PER_DAY
            )),
        );
        assert_eq!(
            estimated_height_after_release(&Network::Mainnet, EOS_WARN_AFTER),
            Height(ESTIMATED_RELEASE_HEIGHT + EOS_WARN_AFTER * PRE_NU7_BLOCKS_PER_DAY),
        );
    }

    #[test]
    fn end_of_support_height_counts_25_second_blocks_after_nu7() {
        let _init_guard = zakura_test::init();

        let post_nu7_blocks_per_day = if ZIP218_ENABLED {
            POST_NU7_BLOCKS_PER_DAY
        } else {
            PRE_NU7_BLOCKS_PER_DAY
        };

        // NU7 activates after the support window.
        let network = regtest_with_nu7(ESTIMATED_RELEASE_HEIGHT + 30 * PRE_NU7_BLOCKS_PER_DAY);
        assert_eq!(
            estimated_height_after_release(&network, EOS_PANIC_AFTER),
            Height(ESTIMATED_RELEASE_HEIGHT + EOS_PANIC_AFTER * PRE_NU7_BLOCKS_PER_DAY),
        );

        // NU7 activates 7 days into the support window.
        let nu7 = ESTIMATED_RELEASE_HEIGHT + 7 * PRE_NU7_BLOCKS_PER_DAY;
        let network = regtest_with_nu7(nu7);
        assert_eq!(
            estimated_height_after_release(&network, EOS_PANIC_AFTER),
            Height(nu7 + (EOS_PANIC_AFTER - 7) * post_nu7_blocks_per_day),
        );

        // NU7 activates before the release.
        let network = regtest_with_nu7(ESTIMATED_RELEASE_HEIGHT - 1_000);
        assert_eq!(
            estimated_height_after_release(&network, EOS_PANIC_AFTER),
            Height(ESTIMATED_RELEASE_HEIGHT + EOS_PANIC_AFTER * post_nu7_blocks_per_day),
        );
        assert_eq!(
            estimated_height_after_release(&network, EOS_WARN_AFTER),
            Height(ESTIMATED_RELEASE_HEIGHT + EOS_WARN_AFTER * post_nu7_blocks_per_day),
        );
    }
}
