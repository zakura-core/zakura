//! End of support checking task.

use std::{ops::Bound, time::Duration};

use eyre::Report;
use serde::{Deserialize, Serialize};

use zakura_chain::{
    block::Height,
    chain_tip::ChainTip,
    parameters::{Network, NetworkUpgrade, POST_BLOSSOM_POW_TARGET_SPACING},
};

use crate::application::release_version;

/// The estimated height that this release will be published.
///
/// Set during release preparation from the verified Mainnet release-state bundle.
/// The Mainnet halt height remains capped by [`MAINNET_EOS_HEIGHT`].
pub const ESTIMATED_RELEASE_HEIGHT: u32 = 3_611_814;

/// The estimated number of blocks per day at the post-Blossom target spacing.
///
/// From the ZIP 259 NU7 activation, blocks arrive three times as often; use
/// [`NetworkUpgrade::target_spacing_for_height`] at heights past activation.
pub const ESTIMATED_BLOCKS_PER_DAY: u32 = 24 * 60 * 60 / POST_BLOSSOM_POW_TARGET_SPACING;

/// The maximum number of days after [`ESTIMATED_RELEASE_HEIGHT`] that a Zakura
/// server will run without halting.
///
/// Notes:
///
/// - Zakura will exit with a panic if the current tip height is bigger than
///   [`ESTIMATED_RELEASE_HEIGHT`] plus this number of days at the target spacing,
///   unless [`Config::halt`] is off.
/// - The support window is capped by [`MAINNET_EOS_HEIGHT`]. This window
///   deliberately overshoots the cap, so the cap fixes the halt date.
pub const EOS_PANIC_AFTER: u32 = 45;

/// The nominal number of days after release before warnings start.
///
/// When the support height is capped, warnings still start three days before EOS.
pub const EOS_WARN_AFTER: u32 = EOS_PANIC_AFTER - 3;

/// The last Mainnet height supported by this release.
///
/// The next block projects to December 5, 2026 at 00:00:20 UTC using Mainnet
/// height 3,539,449 at its block timestamp, November 2, 2026 at 15:49:05
/// UTC, 75-second blocks through height 3,543,404, and 25-second blocks from
/// the ZIP 259 NU7 activation at height 3,543,405. This fixes EOS at
/// approximately December 5, even if release preparation advances
/// [`ESTIMATED_RELEASE_HEIGHT`]. Enforcement follows height, not time.
///
/// Earlier releases deliberately halted before the expected NU7 activation.
/// This release follows the activation, so its support window extends about
/// four weeks past it.
const MAINNET_EOS_HEIGHT: Height = Height(3_643_309);

/// A string which is part of the panic that will be displayed if Zakura halts.
pub const EOS_PANIC_MESSAGE_HEADER: &str = "Zakura refuses to run";

/// A string which is part of the warning that will be displayed if a Zakura release is close to halting.
pub const EOS_WARN_MESSAGE_HEADER: &str = "Your Zakura release is too old and it will stop running";

/// A string which is part of the warning displayed while a release that keeps
/// running past its support window is close to the end of that window.
pub const EOS_KEEPS_RUNNING_MESSAGE_HEADER: &str =
    "Your Zakura release is too old and it will be unsupported";

/// A string which is part of the warning displayed while a release keeps
/// running past its last supported height.
pub const EOS_UNSUPPORTED_MESSAGE_HEADER: &str = "Your Zakura release is unsupported";

/// A string which is part of the warning displayed while an unsupported release
/// keeps running ahead of a network upgrade.
pub const EOS_UPGRADE_AHEAD_MESSAGE_HEADER: &str =
    "Your unsupported Zakura release is approaching a network upgrade";

/// Where operators get the latest release.
const LATEST_RELEASE_URL: &str = "https://github.com/zakura-core/zakura/releases/latest";

/// The amount of time between end of support checks.
pub const CHECK_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Wait a few seconds at startup so `best_tip_height` is usually `Some`.
const INITIAL_WAIT: Duration = Duration::from_secs(10);

/// How long to wait before looking for the tip again when the state has none.
///
/// A check that finds no tip is retried at this interval rather than at
/// [`CHECK_INTERVAL`], so every start gets its first check promptly.
const TIP_RETRY_INTERVAL: Duration = INITIAL_WAIT;

/// The metric that reports whether this node halts at the end of its release's support.
///
/// Set to `1` on Mainnet unless [`Config::halt`] is off, and `0` otherwise.
const EOS_ENFORCED_METRIC: &str = "end_of_support.enforced";

/// The metric that reports the last height this release supports.
///
/// Only set on networks where support ends.
const EOS_HEIGHT_METRIC: &str = "end_of_support.last_supported_height";

/// The metric that reports how many blocks are left in this release's support window.
///
/// Only set on networks where support ends. It is negative once the tip goes
/// past the last supported height, which halts the node unless [`Config::halt`]
/// is off.
const EOS_REMAINING_BLOCKS_METRIC: &str = "end_of_support.remaining_blocks";

/// The metric that reports how many blocks are left before the next network
/// upgrade, while an unsupported release keeps running ahead of it.
///
/// Only set on networks where support ends. It is `0` in every other state. Like
/// the other gauges it is refreshed at each check, so it can lag the tip by up
/// to [`CHECK_INTERVAL`].
const EOS_BLOCKS_UNTIL_UPGRADE_METRIC: &str = "end_of_support.blocks_until_next_upgrade";

/// End of support configuration section.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Stop the node when the chain tip passes the last height its release supports.
    ///
    /// Releases are only supported for a limited time, so that nodes do not keep
    /// following outdated consensus rules. Set this to `false` to keep running an
    /// unsupported release, with warnings instead of a halt. An unsupported
    /// release might not follow the network's consensus rules after a network
    /// upgrade it was built without, so the operator has to track upgrades.
    ///
    /// Mining nodes always halt: on a network where support ends, `false` is
    /// rejected when `mining.miner_address` or `mining.internal_miner` is set.
    pub halt: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self { halt: true }
    }
}

/// The heights that bound a release's support on one network.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SupportWindow {
    /// The release is supported up to and including this height.
    last_supported_height: Height,
    /// Warnings start when the tip goes past this height.
    warn_height: Height,
}

/// Where a tip height falls in a [`SupportWindow`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SupportStatus {
    /// The release is supported, and the end of its window is not close.
    Supported,
    /// The release is supported, but the end of its window is close.
    Expiring,
    /// The tip is past the last supported height.
    Expired,
}

impl SupportWindow {
    /// Returns the support window of this release on `network`, or `None` on
    /// networks where support does not end, which is every network but Mainnet.
    pub fn of_release(network: &Network) -> Option<Self> {
        end_of_support_height(network)
            .map(|last_supported_height| Self::ending_at(last_supported_height, network))
    }

    /// Returns a window that ends at `last_supported_height` on `network`.
    ///
    /// Warnings start `EOS_PANIC_AFTER - EOS_WARN_AFTER` days before that height.
    pub fn ending_at(last_supported_height: Height, network: &Network) -> Self {
        // Convert the warning lead to blocks at the last supported height's target
        // spacing, so it covers the same days across target-spacing changes like ZIP 259.
        let blocks_per_day = u32::try_from(
            24 * 60 * 60
                / NetworkUpgrade::target_spacing_for_height(network, last_supported_height)
                    .num_seconds(),
        )
        .expect("target spacings are positive and shorter than a day");
        let warning_blocks = (EOS_PANIC_AFTER - EOS_WARN_AFTER) * blocks_per_day;

        Self {
            last_supported_height,
            warn_height: Height(last_supported_height.0.saturating_sub(warning_blocks)),
        }
    }

    /// Returns the last height the release supports.
    pub fn last_supported_height(&self) -> Height {
        self.last_supported_height
    }

    /// Returns where `tip_height` falls in this window.
    pub fn status(&self, tip_height: Height) -> SupportStatus {
        if tip_height > self.last_supported_height {
            SupportStatus::Expired
        } else if tip_height > self.warn_height {
            SupportStatus::Expiring
        } else {
            SupportStatus::Supported
        }
    }

    /// Returns the number of blocks left in this window at `tip_height`.
    ///
    /// The count is negative once `tip_height` goes past the last supported height.
    pub fn remaining_blocks(&self, tip_height: Height) -> i64 {
        i64::from(self.last_supported_height.0) - i64::from(tip_height.0)
    }
}

/// A network upgrade that this release implements and that has not activated yet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UpgradeAhead {
    /// The next network upgrade after the tip.
    pub upgrade: NetworkUpgrade,
    /// The height at which it activates.
    pub activation_height: Height,
    /// The number of blocks between the tip and the activation height.
    pub remaining_blocks: u32,
}

/// Returns the next network upgrade that activates after `tip_height` on `network`.
///
/// Only upgrades with an activation height in this release are known. An upgrade
/// that was scheduled after this release was built is not reported.
pub fn upgrade_ahead(tip_height: Height, network: &Network) -> Option<UpgradeAhead> {
    let (activation_height, upgrade) = network
        .activation_list()
        .range((Bound::Excluded(tip_height), Bound::Unbounded))
        .map(|(activation_height, upgrade)| (*activation_height, *upgrade))
        .next()?;

    Some(UpgradeAhead {
        upgrade,
        activation_height,
        remaining_blocks: activation_height.0.saturating_sub(tip_height.0),
    })
}

/// Returns the network upgrade ahead of a release that keeps running past
/// `window`, or `None` while the release is supported or halts.
pub fn unsupported_upgrade_ahead(
    tip_height: Height,
    window: &SupportWindow,
    network: &Network,
    config: Config,
) -> Option<UpgradeAhead> {
    (!config.halt && window.status(tip_height) == SupportStatus::Expired)
        .then(|| upgrade_ahead(tip_height, network))
        .flatten()
}

/// Runs the end of support checking task.
///
/// Checks the tip against `window` shortly after starting and then every hour,
/// so each start of the node reports where its release stands. On networks
/// without a window there is nothing to check.
///
/// Never returns. It panics when the tip is past the window and [`Config::halt`]
/// is on, which halts the node.
pub async fn start(
    config: Config,
    window: Option<SupportWindow>,
    network: Network,
    latest_chain_tip: impl ChainTip + std::fmt::Debug,
) -> Result<(), Report> {
    info!("Starting end of support task");

    let Some(window) = window else {
        metrics::gauge!(EOS_ENFORCED_METRIC).set(0.0);
        loop {
            info!("Release always valid outside Mainnet");
            tokio::time::sleep(CHECK_INTERVAL).await;
        }
    };

    metrics::gauge!(EOS_ENFORCED_METRIC).set(if config.halt { 1.0 } else { 0.0 });
    metrics::gauge!(EOS_HEIGHT_METRIC).set(f64::from(window.last_supported_height.0));
    if !config.halt {
        warn!(
            "end_of_support.halt is off: this node keeps running after block {}, \
            the last height its release supports",
            window.last_supported_height.0
        );
    }

    tokio::time::sleep(INITIAL_WAIT).await;

    loop {
        let Some(tip_height) = latest_chain_tip.best_tip_height() else {
            tokio::time::sleep(TIP_RETRY_INTERVAL).await;
            continue;
        };

        // A difference between two u32 heights is exactly representable as f64.
        metrics::gauge!(EOS_REMAINING_BLOCKS_METRIC)
            .set(window.remaining_blocks(tip_height) as f64);
        metrics::gauge!(EOS_BLOCKS_UNTIL_UPGRADE_METRIC).set(
            unsupported_upgrade_ahead(tip_height, &window, &network, config)
                .map_or(0.0, |upgrade_ahead| {
                    f64::from(upgrade_ahead.remaining_blocks)
                }),
        );
        check(tip_height, &window, &network, config);

        tokio::time::sleep(CHECK_INTERVAL).await;
    }
}

/// Returns the last supported height, or `None` on networks where support does
/// not end.
///
/// The node runs at this height. When the tip goes past it the node halts,
/// unless [`Config::halt`] is off. This matches zcashd's
/// `end_of_service.block_height` threshold semantics.
pub fn end_of_support_height(network: &Network) -> Option<Height> {
    (network == &Network::Mainnet)
        .then(|| estimated_height_after_release(network, EOS_PANIC_AFTER).min(MAINNET_EOS_HEIGHT))
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

/// Reports where `tip_height` falls in `window`, and panics if the release is
/// too old and [`Config::halt`] is on.
///
/// With the halt off, a release past its window gets a warning instead. It gets
/// a second warning while a network upgrade on `network` is still ahead of it.
pub fn check(tip_height: Height, window: &SupportWindow, network: &Network, config: Config) {
    info!("Checking if Zakura release is inside support range ...");

    let last_supported_height = window.last_supported_height.0;
    let release = format!(
        "Release name: {}, Estimated release height: {ESTIMATED_RELEASE_HEIGHT}",
        release_version()
    );
    let hint =
        format!("Hint: Download and install the latest Zakura release from: {LATEST_RELEASE_URL}");

    match (window.status(tip_height), config.halt) {
        (SupportStatus::Supported, _) => info!(
            "Zakura release is supported until block {last_supported_height}, \
            please report bugs at https://github.com/zakura-core/zakura/issues"
        ),
        (SupportStatus::Expiring, true) => warn!(
            "{EOS_WARN_MESSAGE_HEADER} at block {last_supported_height}. \n{release} \n{hint}"
        ),
        (SupportStatus::Expiring, false) => warn!(
            "{EOS_KEEPS_RUNNING_MESSAGE_HEADER} after block {last_supported_height}. \
            It will keep running because end_of_support.halt is off. \n{release} \n{hint}"
        ),
        (SupportStatus::Expired, true) => panic!(
            "{EOS_PANIC_MESSAGE_HEADER} past the last supported Mainnet height \
            {last_supported_height}. \n{release} \n{hint}"
        ),
        (SupportStatus::Expired, false) => warn!(
            "{EOS_UNSUPPORTED_MESSAGE_HEADER}: the chain tip is past block \
            {last_supported_height}, the last height it supports. It keeps running because \
            end_of_support.halt is off, but it might not follow the network's consensus \
            rules after a network upgrade. It cannot warn about an upgrade that was \
            scheduled after it was built. \n{release} \n{hint}"
        ),
    }

    if let Some(upgrade_ahead) = unsupported_upgrade_ahead(tip_height, window, network, config) {
        warn!(
            "{EOS_UPGRADE_AHEAD_MESSAGE_HEADER}: {} activates at block {}, in {} blocks. \
            Later releases can change how the upgrade is handled, so install a supported \
            release before then. \n{release} \n{hint}",
            upgrade_ahead.upgrade,
            upgrade_ahead.activation_height.0,
            upgrade_ahead.remaining_blocks,
        );
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

    /// Returns the last network upgrade that this release activates on Mainnet.
    fn last_mainnet_upgrade() -> (Height, NetworkUpgrade) {
        *Network::Mainnet
            .full_activation_list()
            .last()
            .expect("Mainnet has network upgrades")
    }

    #[test]
    fn config_halts_unless_turned_off() {
        let _init_guard = zakura_test::init();

        assert!(Config::default().halt);
        assert_eq!(
            toml::from_str::<Config>("").expect("an empty section is valid"),
            Config::default(),
        );
        assert!(
            !toml::from_str::<Config>("halt = false")
                .expect("the halt can be turned off")
                .halt
        );
        assert!(toml::from_str::<Config>("halt_height = 1").is_err());
    }

    #[test]
    fn support_window_status_changes_at_its_heights() {
        let _init_guard = zakura_test::init();
        let network = Network::Mainnet;
        let last_supported_height = Height(4_000_000);
        let window = SupportWindow::ending_at(last_supported_height, &network);
        let warning_blocks = (EOS_PANIC_AFTER - EOS_WARN_AFTER)
            * blocks_per_day(NetworkUpgrade::current(&network, last_supported_height));
        let warn_height = Height(last_supported_height.0 - warning_blocks);

        assert_eq!(window.last_supported_height(), last_supported_height);
        assert_eq!(window.status(Height(0)), SupportStatus::Supported);
        assert_eq!(window.status(warn_height), SupportStatus::Supported);
        assert_eq!(
            window.status(Height(warn_height.0 + 1)),
            SupportStatus::Expiring
        );
        assert_eq!(
            window.status(last_supported_height),
            SupportStatus::Expiring
        );
        assert_eq!(
            window.status(Height(last_supported_height.0 + 1)),
            SupportStatus::Expired
        );
        assert_eq!(window.status(Height::MAX), SupportStatus::Expired);

        assert_eq!(window.remaining_blocks(last_supported_height), 0);
        assert_eq!(
            window.remaining_blocks(Height(last_supported_height.0 + 1)),
            -1
        );
    }

    #[test]
    fn support_window_only_exists_on_mainnet() {
        let _init_guard = zakura_test::init();

        assert_eq!(
            SupportWindow::of_release(&Network::Mainnet).map(|window| window.last_supported_height),
            end_of_support_height(&Network::Mainnet),
        );
        assert_eq!(
            SupportWindow::of_release(&Network::new_default_testnet()),
            None
        );
        assert_eq!(
            SupportWindow::of_release(&Network::new_regtest(Default::default())),
            None
        );
    }

    #[test]
    fn upgrade_ahead_is_the_next_known_activation() {
        let _init_guard = zakura_test::init();
        let network = Network::Mainnet;
        let (activation_height, upgrade) = last_mainnet_upgrade();

        assert_eq!(
            upgrade_ahead(Height(activation_height.0 - 100), &network),
            Some(UpgradeAhead {
                upgrade,
                activation_height,
                remaining_blocks: 100,
            }),
        );
        assert_eq!(
            upgrade_ahead(Height(activation_height.0 - 1), &network),
            Some(UpgradeAhead {
                upgrade,
                activation_height,
                remaining_blocks: 1,
            }),
        );

        // The upgrade is no longer ahead once it activates, and this release
        // knows no upgrade after its last one.
        assert_eq!(upgrade_ahead(activation_height, &network), None);
        assert_eq!(upgrade_ahead(Height::MAX, &network), None);
    }

    #[test]
    fn upgrade_ahead_only_concerns_a_release_that_runs_unsupported() {
        let _init_guard = zakura_test::init();
        let network = Network::Mainnet;
        let (activation_height, _) = last_mainnet_upgrade();
        let window = SupportWindow::ending_at(Height(activation_height.0 - 1_000), &network);
        let unsupported_tip = Height(activation_height.0 - 100);
        let keeps_running = Config { halt: false };

        assert_eq!(
            unsupported_upgrade_ahead(unsupported_tip, &window, &network, keeps_running),
            upgrade_ahead(unsupported_tip, &network),
        );
        assert!(upgrade_ahead(unsupported_tip, &network).is_some());

        // A release that halts never runs unsupported.
        assert_eq!(
            unsupported_upgrade_ahead(unsupported_tip, &window, &network, Config::default()),
            None,
        );
        // A supported release is not warned about the upgrades it implements.
        assert_eq!(
            unsupported_upgrade_ahead(
                window.last_supported_height(),
                &window,
                &network,
                keeps_running
            ),
            None,
        );
        // Nothing is ahead once the last known upgrade has activated.
        assert_eq!(
            unsupported_upgrade_ahead(activation_height, &window, &network, keeps_running),
            None,
        );
    }

    #[test]
    fn mainnet_end_of_support_is_capped_at_the_release_deadline() {
        let _init_guard = zakura_test::init();

        assert_eq!(
            end_of_support_height(&Network::Mainnet),
            Some(MAINNET_EOS_HEIGHT),
        );
        // The nominal warning window extends past the fixed deadline even as
        // release preparation advances the estimated release height.
        assert!(
            estimated_height_after_release(&Network::Mainnet, EOS_WARN_AFTER) >= MAINNET_EOS_HEIGHT,
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
