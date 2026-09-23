//! ZIP 218 moves the third halving, and with it the funding stream address period.
//!
//! The NU7 heights here are the zakura#1059 estimates. They are not final.

use super::testnet_with_nu7;
use crate::{
    block::{Height, HeightDiff},
    parameters::{
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
