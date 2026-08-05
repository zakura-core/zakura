//! End of support checking task.

use std::time::Duration;

use color_eyre::Report;

use zakura_chain::{
    block::Height,
    chain_tip::ChainTip,
    parameters::{Network, POST_BLOSSOM_POW_TARGET_SPACING},
};

use crate::application::release_version;

/// The estimated height that this release will be published.
pub const ESTIMATED_RELEASE_HEIGHT: u32 = 3_439_627;

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
/// - Currently set to 40 days
///
/// Note: v1.1.0 is estimated to release at height 3,438,427 (~2026-08-05) and
/// halts 40 days later at height 3,484,507 (~2026-09-15).
pub const EOS_PANIC_AFTER: u32 = 40;

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

/// Start the end of support checking task for Mainnet.
pub async fn start(
    network: Network,
    latest_chain_tip: impl ChainTip + std::fmt::Debug,
) -> Result<(), Report> {
    info!("Starting end of support task");

    tokio::time::sleep(INITIAL_WAIT).await;

    loop {
        if network == Network::Mainnet {
            if let Some(tip_height) = latest_chain_tip.best_tip_height() {
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
    (network == &Network::Mainnet).then_some(Height(
        ESTIMATED_RELEASE_HEIGHT + (EOS_PANIC_AFTER * ESTIMATED_BLOCKS_PER_DAY),
    ))
}

/// Check if the current release is too old and panic if so.
pub fn check(tip_height: Height, network: &Network) {
    info!("Checking if Zakura release is inside support range ...");

    let Some(panic_height) = end_of_support_height(network) else {
        info!("Release always valid outside Mainnet");
        return;
    };
    let warn_height =
        Height(ESTIMATED_RELEASE_HEIGHT + (EOS_WARN_AFTER * ESTIMATED_BLOCKS_PER_DAY));

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
