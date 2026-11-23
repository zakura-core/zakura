//! Which heights count at a given tip.

use proptest::prelude::*;

use crate::{
    block::Height,
    miner_mesh::window::window,
    parameters::{
        testnet::{ConfiguredActivationHeights, RegtestParameters},
        Network, NetworkUpgrade,
    },
};

use super::network_with_nu7;

/// Two days in seconds.
const TWO_DAYS: i64 = 172_800;

/// The number of heights in the window at `tip`.
fn window_len(network: &Network, tip: u32) -> usize {
    let window = window(network, Height(tip)).expect("the tip is at height 100 or above");
    (window.start().0..=window.end().0).count()
}

/// The oldest counted height when `newest` is the newest, found one block at
/// a time: a block counts while the spacing summed from it to `newest` is
/// under two days.
fn oldest_by_walking(network: &Network, newest: u32) -> u32 {
    let mut oldest = newest;
    let mut seconds = 0;
    while oldest > 0 {
        let spacing =
            NetworkUpgrade::target_spacing_for_height(network, Height(oldest)).num_seconds();
        if seconds + spacing >= TWO_DAYS {
            break;
        }
        seconds += spacing;
        oldest -= 1;
    }
    oldest
}

#[test]
fn nothing_counts_until_the_tip_reaches_height_100() {
    let network = network_with_nu7(None);

    assert_eq!(window(&network, Height(0)), None);
    assert_eq!(window(&network, Height(99)), None);
    assert_eq!(window(&network, Height(100)), Some(Height(0)..=Height(0)));
}

#[test]
fn the_newest_counted_block_is_100_below_the_tip() {
    let network = network_with_nu7(None);

    assert_eq!(
        window(&network, Height(10_000)),
        Some(Height(7_597)..=Height(9_900)),
    );
}

#[test]
fn a_young_chain_counts_back_to_genesis() {
    let network = network_with_nu7(None);

    assert_eq!(window(&network, Height(150)), Some(Height(0)..=Height(50)));
}

#[test]
fn the_window_holds_two_days_of_blocks_at_each_spacing() {
    // Mainnet blocks are 150 seconds apart before Blossom.
    assert_eq!(window_len(&Network::Mainnet, 100_000), 1_152);

    let nu7 = Height(1_000_000);
    let network = network_with_nu7(Some(nu7));
    // 75 seconds apart before NU7, 25 seconds apart long after it.
    assert_eq!(window_len(&network, 500_000), 2_304);
    assert_eq!(window_len(&network, 2_000_000), 6_912);
}

#[test]
fn the_window_grows_block_by_block_after_the_spacing_shortens() {
    let nu7 = 1_000_000;
    let network = network_with_nu7(Some(Height(nu7)));

    // The newest block is the last one before NU7: nothing has changed yet.
    assert_eq!(window_len(&network, nu7 - 1 + 100), 2_304);
    // Three 25-second blocks stand in for one 75-second block, so the window
    // gains two heights for every three blocks after NU7.
    assert_eq!(window_len(&network, nu7 + 2 + 100), 2_304 + 2);
    assert_eq!(window_len(&network, nu7 + 299 + 100), 2_304 + 200);
    // Two days after NU7 every counted block is 25 seconds from its parent.
    assert_eq!(window_len(&network, nu7 + 6_911 + 100), 6_912);
    assert_eq!(window_len(&network, nu7 + 6_912 + 100), 6_912);
}

/// The window matches a block-by-block walk on the public networks around
/// their spacing changes, on the default Regtest, and on a Regtest where NU7
/// starts at height 1, the same height as the 75-second spacing it replaces.
#[test]
fn the_window_matches_a_block_by_block_walk_on_real_networks() {
    let regtest_with_nu7_at_1 = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu7: Some(1),
            ..Default::default()
        },
        ..Default::default()
    });

    for network in [
        Network::Mainnet,
        Network::new_default_testnet(),
        Network::new_regtest(Default::default()),
        regtest_with_nu7_at_1,
    ] {
        let mut changes: Vec<u32> = NetworkUpgrade::target_spacings(&network)
            .map(|(height, _)| height.0)
            .collect();
        // A height far from any change.
        changes.push(20_000);
        changes.dedup();

        for change in changes {
            // Newest counted blocks from just below the change to past the
            // point where the window no longer reaches back to it.
            for newest in (change.saturating_sub(3)..change + 7_000).step_by(97) {
                assert_eq!(
                    window(&network, Height(newest + 100)),
                    Some(Height(oldest_by_walking(&network, newest))..=Height(newest)),
                    "{network} with the newest counted block at {newest}",
                );
            }
        }
    }
}

proptest! {
    /// The window matches a block-by-block walk at any tip around a spacing
    /// change.
    #[test]
    fn the_window_matches_a_block_by_block_walk(
        nu7 in 3..12_000u32,
        tip in 100..24_000u32,
    ) {
        let network = network_with_nu7(Some(Height(nu7)));
        let newest = tip - 100;

        prop_assert_eq!(
            window(&network, Height(tip)),
            Some(Height(oldest_by_walking(&network, newest))..=Height(newest)),
        );
    }
}
