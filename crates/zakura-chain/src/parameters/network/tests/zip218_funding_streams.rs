//! ZIP 218 moves the third halving, and with it the funding stream address period.
//!
//! The NU7 heights here are the zakura#1059 estimates. They are not final.

use proptest::prelude::*;

use super::testnet_with_nu7;
use crate::{
    block::{Height, HeightDiff},
    parameters::{
        constants::activation_heights,
        subsidy::{
            constants::testnet as testnet_constants, funding_stream_address_period,
            height_for_halving, ParameterSubsidy,
        },
        Network, NU7_POW_TARGET_SPACING_RATIO,
    },
};

/// The Testnet NU7 activation height estimate.
const TESTNET_NU7: u32 = 4_386_000;

/// The Testnet third halving before ZIP 218.
const TESTNET_THIRD_HALVING: u32 = 4_476_000;

/// The index of the ZIP 214 Revision 2 funding streams in the built-in lists.
const REVISION_2: usize = 2;

/// Returns the Testnet network with NU7 at `nu7` and the built-in funding streams.
fn testnet_network_with_nu7(nu7: Option<u32>) -> Network {
    testnet_with_nu7(nu7)
        .to_network()
        .expect("configured network is valid")
}

/// Returns the number of recipient addresses that `height_range` needs on `network`.
fn required_addresses(height_range: &std::ops::Range<Height>, network: &Network) -> HeightDiff {
    let last_height = height_range
        .end
        .previous()
        .expect("the range ends above genesis");

    1 + funding_stream_address_period(last_height, network)
        - funding_stream_address_period(height_range.start, network)
}

#[test]
fn funding_stream_address_period_after_nu7() {
    let _init_guard = zakura_test::init();

    let ratio = HeightDiff::from(NU7_POW_TARGET_SPACING_RATIO);
    let without_nu7 = testnet_network_with_nu7(None);
    let old_period = |height: u32| funding_stream_address_period(Height(height), &without_nu7);

    // 4,386,000 is 11,000 blocks after a period boundary. 4,406,000 is on one.
    for nu7 in [TESTNET_NU7, 4_406_000] {
        let network = testnet_network_with_nu7(Some(nu7));
        let period = |height: u32| funding_stream_address_period(Height(height), &network);
        let change_interval = network.funding_stream_address_change_interval();
        assert_eq!(change_interval, 35_000);

        // The two cases agree at NU7, so the period does not jump there.
        for height in [nu7 - 1, nu7] {
            assert_eq!(period(height), old_period(height), "NU7 {nu7} at {height}");
        }
        assert_eq!(period(nu7 - 1) == period(nu7), nu7 != 4_406_000);

        // From NU7 on, the period advances once every `ratio · change_interval` blocks.
        let long_interval =
            u32::try_from(ratio * change_interval).expect("the interval fits in u32");
        for height in (nu7..TESTNET_THIRD_HALVING * 2).step_by(9_973) {
            assert_eq!(period(height + long_interval), period(height) + 1);
            assert!(period(height) <= period(height + 1));
        }

        // The period is monotonic from the start of the Revision 2 stream to the
        // ZIP 218 third halving.
        let start = testnet_constants::FUNDING_STREAMS[REVISION_2]
            .height_range()
            .start
            .0;
        let third_halving = height_for_halving(3, &network)
            .expect("the halving has a height")
            .0;
        let mut heights: Vec<u32> = (start..third_halving)
            .step_by(1_009)
            .chain(nu7 - 50..nu7 + 50)
            .chain(third_halving - 50..third_halving)
            .collect();
        heights.sort_unstable();
        heights.dedup();
        for pair in heights.windows(2) {
            assert!(period(pair[0]) <= period(pair[1]), "at {pair:?}");
        }

        // The period advances more slowly after NU7, so the built-in range before
        // ZIP 218 needs no more recipient addresses than it has.
        let built_in = testnet_constants::FUNDING_STREAMS[REVISION_2].height_range();
        assert!(
            required_addresses(built_in, &network) <= required_addresses(built_in, &without_nu7)
        );
        assert_eq!(required_addresses(built_in, &without_nu7), 27);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// For any Testnet NU7 activation height, the address period agrees with ZIP 207 before
    /// NU7, has no jump at NU7, and then advances once every `3 · 35,000` blocks. A height
    /// `h` before ZIP 218 and the height `A + 3 · (h − A)` after it have the same period, so
    /// each period keeps its wall-clock length, including the period that contains NU7.
    #[test]
    fn address_period_keeps_its_length_for_any_nu7_height(
        nu7 in activation_heights::testnet::NU6_3.0 + 1..=TESTNET_THIRD_HALVING + 200_000,
        before in 1u32..=activation_heights::testnet::NU6_3.0,
        offset in 0u32..=2_000_000,
        boundary in 0u32..60,
    ) {
        let _init_guard = zakura_test::init();

        let without_nu7 = testnet_network_with_nu7(None);
        let network = testnet_network_with_nu7(Some(nu7));
        let old_period = |height: u32| funding_stream_address_period(Height(height), &without_nu7);
        let period = |height: u32| funding_stream_address_period(Height(height), &network);
        let ratio = NU7_POW_TARGET_SPACING_RATIO;
        let long_interval = ratio * 35_000;

        prop_assert_eq!(period(before), old_period(before));
        prop_assert_eq!(period(nu7 - 1), old_period(nu7 - 1));
        prop_assert_eq!(period(nu7), old_period(nu7));

        let height = nu7 + offset;
        prop_assert_eq!(period(nu7 + ratio * offset), old_period(height));
        prop_assert!((0..=1).contains(&(period(height + 1) - period(height))));
        prop_assert_eq!(period(height + long_interval), period(height) + 1);

        // Random heights rarely fall on a period boundary, so also check the heights
        // around the `boundary`th boundary after NU7.
        let change_interval = 35_000;
        let old_offset = i64::from(nu7) - i64::from(network.height_for_first_halving().0)
            + network.post_blossom_halving_interval();
        let first_boundary = nu7
            + u32::try_from((change_interval - old_offset.rem_euclid(change_interval)) % change_interval)
                .expect("the distance fits in u32");
        let old_boundary = first_boundary + boundary * 35_000;
        let new_boundary = nu7 + ratio * (old_boundary - nu7);
        prop_assert_eq!(period(new_boundary), old_period(old_boundary));
        prop_assert_eq!(period(new_boundary), period(new_boundary - 1) + 1);
        for delta in 1..=2 {
            if old_boundary - delta >= nu7 {
                prop_assert_eq!(
                    period(nu7 + ratio * (old_boundary - delta - nu7)),
                    old_period(old_boundary - delta),
                );
            }
            prop_assert_eq!(
                period(nu7 + ratio * (old_boundary + delta - nu7)),
                old_period(old_boundary + delta),
            );
        }

        // The built-in range before ZIP 218 needs no more than its 27 addresses.
        let built_in = testnet_constants::FUNDING_STREAMS[REVISION_2].height_range();
        prop_assert!(required_addresses(built_in, &network) <= 27);
    }
}
