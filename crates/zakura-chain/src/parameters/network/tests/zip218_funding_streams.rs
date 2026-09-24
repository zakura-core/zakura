//! ZIP 218 moves the third halving, and with it the end of the ZIP 214 Revision 2
//! funding streams and the funding stream address period.
//!
//! The NU7 heights here are the zakura#1059 estimates. They are not final, so the
//! expected halving heights also follow from the closed forms below.

use std::collections::HashMap;

use proptest::prelude::*;

use super::testnet_with_nu7;
use crate::{
    amount::{Amount, NonNegative},
    block::{Height, HeightDiff},
    parameters::{
        constants::activation_heights,
        subsidy::{
            constants::{mainnet, testnet as testnet_constants},
            funding_stream_address_period, halving_block_subsidy, height_for_halving,
            nu7_adjusted_funding_stream_height, FundingStreamReceiver, FundingStreams,
            ParameterSubsidy,
        },
        testnet::{self, ConfiguredFundingStreamRecipient, ConfiguredFundingStreams},
        Network, NU7_POW_TARGET_SPACING_RATIO,
    },
};

/// The Mainnet NU7 activation height estimate.
const MAINNET_NU7: u32 = 3_543_000;

/// The Testnet NU7 activation height estimate.
const TESTNET_NU7: u32 = 4_386_000;

/// The Mainnet third halving before ZIP 218.
const MAINNET_THIRD_HALVING: u32 = 4_406_400;

/// The Testnet third halving before ZIP 218.
const TESTNET_THIRD_HALVING: u32 = 4_476_000;

/// The index of the ZIP 214 Revision 2 funding streams in the built-in lists.
const REVISION_2: usize = 2;

/// Returns a configured network with the Mainnet activation heights and NU7 at `nu7`.
///
/// Mainnet has no NU7 activation height, so this network stands in for Mainnet with one.
fn mainnet_with_nu7(nu7: Option<u32>) -> Network {
    let mut activation_heights: testnet::ConfiguredActivationHeights =
        Network::Mainnet.activation_list().into();
    activation_heights.nu7 = nu7;

    testnet::Parameters::build()
        .with_activation_heights(activation_heights)
        .expect("activation heights are valid")
        .clear_funding_streams()
        .to_network()
        .expect("configured network is valid")
}

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
fn zip_218_third_and_fourth_halving_heights() {
    let _init_guard = zakura_test::init();

    // H3' = A + 3 · (H3 − A), which is 13,219,200 − 2A on Mainnet and 13,428,000 − 2A
    // on Testnet. The fourth halving is one post-NU7 halving interval later.
    for (network, third, fourth) in [
        (mainnet_with_nu7(Some(MAINNET_NU7)), 6_133_200, 11_173_200),
        (
            testnet_network_with_nu7(Some(TESTNET_NU7)),
            4_656_000,
            9_696_000,
        ),
    ] {
        assert_eq!(height_for_halving(3, &network), Some(Height(third)));
        assert_eq!(height_for_halving(4, &network), Some(Height(fourth)));

        // The block subsidy is floor(156,250,000 / 3) zatoshi from NU7 activation until
        // the third halving halves it.
        assert_eq!(
            halving_block_subsidy(Height(third - 1), &network),
            Ok(Amount::<NonNegative>::try_from(52_083_333).expect("valid amount")),
        );
        assert_eq!(
            halving_block_subsidy(Height(third), &network),
            Ok(Amount::<NonNegative>::try_from(26_041_666).expect("valid amount")),
        );
    }

    assert_eq!(
        6_133_200,
        13_219_200 - 2 * MAINNET_NU7,
        "the Mainnet closed form"
    );
    assert_eq!(
        4_656_000,
        13_428_000 - 2 * TESTNET_NU7,
        "the Testnet closed form"
    );

    // Without NU7, the halvings stay where they were.
    assert_eq!(
        height_for_halving(3, &mainnet_with_nu7(None)),
        Some(Height(MAINNET_THIRD_HALVING))
    );
    assert_eq!(
        height_for_halving(3, &testnet_network_with_nu7(None)),
        Some(Height(TESTNET_THIRD_HALVING))
    );
}

#[test]
fn nu7_adjusted_funding_stream_height_cases() {
    let ratio = NU7_POW_TARGET_SPACING_RATIO;
    let adjust = nu7_adjusted_funding_stream_height;

    // No NU7, or NU7 at or above the height, keeps the height.
    assert_eq!(adjust(Height(100), None), Some(Height(100)));
    assert_eq!(adjust(Height(100), Some(Height(100))), Some(Height(100)));
    assert_eq!(adjust(Height(100), Some(Height(101))), Some(Height(100)));

    // NU7 below the height stretches the blocks after NU7 by the spacing ratio.
    assert_eq!(
        adjust(Height(100), Some(Height(99))),
        Some(Height(99 + ratio))
    );
    assert_eq!(
        adjust(Height(100), Some(Height(40))),
        Some(Height(40 + 60 * ratio))
    );

    // A moved height above `Height::MAX` has no representation.
    let largest = (Height::MAX.0 - 1) / ratio + 1;
    assert_eq!(
        adjust(Height(largest), Some(Height(1))),
        Some(Height(1 + (largest - 1) * ratio))
    );
    assert_eq!(adjust(Height(largest + 1), Some(Height(1))), None);
    assert_eq!(adjust(Height::MAX, Some(Height(0))), None);

    // ZIP 214 Revision 3 moves the end height and keeps the start height, even when the
    // stream starts after NU7.
    let funding_streams = FundingStreams::new(Height(200)..Height(300), HashMap::new())
        .with_nu7_adjusted_end_height(Some(Height(100)));
    assert_eq!(
        funding_streams.height_range(),
        &(Height(200)..Height(100 + 200 * ratio))
    );
}

#[test]
fn revision_2_funding_streams_end_at_the_zip_218_third_halving() {
    let _init_guard = zakura_test::init();

    // Mainnet has no NU7 activation height, so the built-in streams keep their heights.
    let mainnet_streams = Network::Mainnet.all_funding_streams();
    assert_eq!(mainnet_streams.len(), 3);
    assert_eq!(
        mainnet_streams[REVISION_2].height_range().end,
        Height(MAINNET_THIRD_HALVING)
    );
    assert_eq!(mainnet_streams, &*mainnet::FUNDING_STREAMS);

    // With NU7, the Mainnet Revision 2 stream ends at the moved third halving.
    let mainnet_network = mainnet_with_nu7(Some(MAINNET_NU7));
    let mainnet_revision_2 = mainnet::FUNDING_STREAMS[REVISION_2]
        .clone()
        .with_nu7_adjusted_end_height(Some(Height(MAINNET_NU7)));
    assert_eq!(
        mainnet_revision_2.height_range(),
        &(mainnet_streams[REVISION_2].height_range().start
            ..height_for_halving(3, &mainnet_network).expect("the halving has a height")),
    );
    assert_eq!(mainnet_revision_2.height_range().end, Height(6_133_200));

    // The earlier Mainnet streams ended before NU7 and stay where they are.
    for funding_streams in &mainnet_streams[..REVISION_2] {
        assert_eq!(
            &funding_streams
                .clone()
                .with_nu7_adjusted_end_height(Some(Height(MAINNET_NU7))),
            funding_streams,
        );
    }

    // Testnet without NU7 keeps its built-in heights, which are the heights before ZIP 218.
    assert_eq!(
        testnet_constants::FUNDING_STREAMS[REVISION_2]
            .height_range()
            .end,
        Height(TESTNET_THIRD_HALVING)
    );
    assert_eq!(
        testnet_network_with_nu7(None).all_funding_streams(),
        &*testnet_constants::FUNDING_STREAMS,
    );
    assert_eq!(
        Network::new_default_testnet().all_funding_streams(),
        &*testnet_constants::FUNDING_STREAMS,
    );

    // Testnet with NU7 moves only the Revision 2 end height.
    let testnet_network = testnet_network_with_nu7(Some(TESTNET_NU7));
    let testnet_streams = testnet_network.all_funding_streams();
    assert_eq!(
        testnet_streams[..REVISION_2],
        testnet_constants::FUNDING_STREAMS[..REVISION_2]
    );
    assert_eq!(
        testnet_streams[REVISION_2].height_range(),
        &(testnet_constants::FUNDING_STREAMS[REVISION_2]
            .height_range()
            .start
            ..height_for_halving(3, &testnet_network).expect("the halving has a height")),
    );
    assert_eq!(
        testnet_streams[REVISION_2].height_range().end,
        Height(4_656_000)
    );
}

#[test]
fn revision_2_funding_streams_apply_until_the_zip_218_third_halving() {
    let _init_guard = zakura_test::init();

    let network = testnet_network_with_nu7(Some(TESTNET_NU7));
    let revision_2 = &network.all_funding_streams()[REVISION_2];
    let old_end = TESTNET_THIRD_HALVING;
    let new_end = 4_656_000;

    for height in [
        TESTNET_NU7 - 1,
        TESTNET_NU7,
        TESTNET_NU7 + 1,
        old_end - 1,
        old_end,
        old_end + 1,
        new_end - 1,
    ] {
        assert_eq!(
            network.funding_streams(Height(height)),
            Some(revision_2),
            "at {height}"
        );
    }

    for height in [new_end, new_end + 1] {
        assert_eq!(network.funding_streams(Height(height)), None, "at {height}");
    }
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

#[test]
fn moved_heights_keep_their_address_period() {
    let _init_guard = zakura_test::init();

    let without_nu7 = testnet_network_with_nu7(None);
    let old_period = |height: u32| funding_stream_address_period(Height(height), &without_nu7);

    for nu7 in [TESTNET_NU7, 4_406_000] {
        let network = testnet_network_with_nu7(Some(nu7));
        let period = |height: u32| funding_stream_address_period(Height(height), &network);

        // A height moved by ZIP 218 keeps its old period, so the last period of the
        // Revision 2 stream is unchanged.
        let adjust = |height: u32| {
            nu7_adjusted_funding_stream_height(Height(height), Some(Height(nu7)))
                .expect("the height is valid")
                .0
        };
        for height in (nu7..=TESTNET_THIRD_HALVING).step_by(997) {
            assert_eq!(period(adjust(height)), old_period(height), "at {height}");
        }
        let new_end = adjust(TESTNET_THIRD_HALVING);
        assert_eq!(Some(Height(new_end)), height_for_halving(3, &network));
        assert_eq!(period(new_end - 1), old_period(TESTNET_THIRD_HALVING - 1));
        assert_eq!(period(new_end), old_period(TESTNET_THIRD_HALVING));
    }

    // The Mainnet Revision 2 stream still needs exactly its 36 recipient addresses.
    let mainnet_network = mainnet_with_nu7(Some(MAINNET_NU7));
    let mainnet_revision_2 = mainnet::FUNDING_STREAMS[REVISION_2]
        .clone()
        .with_nu7_adjusted_end_height(Some(Height(MAINNET_NU7)));
    assert_eq!(
        required_addresses(mainnet_revision_2.height_range(), &mainnet_network),
        36,
    );
    assert_eq!(
        required_addresses(
            mainnet::FUNDING_STREAMS[REVISION_2].height_range(),
            &mainnet_with_nu7(None)
        ),
        36,
    );
    assert_eq!(
        mainnet_revision_2
            .recipient(FundingStreamReceiver::MajorGrants)
            .expect("the stream pays grants")
            .addresses()
            .len(),
        36,
    );
}

/// Returns the Revision 2 recipients with `num_addresses` distinct grants addresses.
fn revision_2_recipients(num_addresses: usize) -> Vec<ConfiguredFundingStreamRecipient> {
    let mut addresses: Vec<String> = testnet_constants::FUNDING_STREAM_ECC_ADDRESSES
        .iter()
        .map(ToString::to_string)
        .collect();
    addresses.dedup();
    addresses.truncate(num_addresses);
    assert_eq!(addresses.len(), num_addresses, "enough distinct addresses");

    vec![
        ConfiguredFundingStreamRecipient {
            receiver: FundingStreamReceiver::Deferred,
            numerator: 12,
            addresses: None,
        },
        ConfiguredFundingStreamRecipient {
            receiver: FundingStreamReceiver::MajorGrants,
            numerator: 8,
            addresses: Some(addresses),
        },
    ]
}

/// Returns funding stream configs that inherit the first two built-in streams and
/// use `revision_2` for the third.
fn configured_streams(revision_2: ConfiguredFundingStreams) -> Vec<ConfiguredFundingStreams> {
    vec![
        ConfiguredFundingStreams::default(),
        ConfiguredFundingStreams::default(),
        revision_2,
    ]
}

#[test]
fn configured_funding_stream_ranges_after_nu7() {
    let _init_guard = zakura_test::init();

    let built_in = testnet_constants::FUNDING_STREAMS[REVISION_2].height_range();
    let new_range = built_in.start..Height(4_656_000);

    let revision_2_range = |configured: ConfiguredFundingStreams| {
        testnet_with_nu7(Some(TESTNET_NU7))
            .with_funding_streams(configured_streams(configured))
            .to_network()
            .expect("configured network is valid")
            .all_funding_streams()[REVISION_2]
            .height_range()
            .clone()
    };

    // An inherited range moves, with inherited or configured recipients.
    assert_eq!(
        revision_2_range(ConfiguredFundingStreams::default()),
        new_range
    );
    assert_eq!(
        revision_2_range(ConfiguredFundingStreams {
            height_range: None,
            recipients: Some(revision_2_recipients(27)),
        }),
        new_range,
    );

    // A configured range stays as configured, even when it equals a built-in range.
    assert_eq!(
        revision_2_range(ConfiguredFundingStreams {
            height_range: Some(built_in.clone()),
            recipients: None,
        }),
        built_in.clone(),
    );

    // An empty funding stream list stays empty.
    let network = testnet_with_nu7(Some(TESTNET_NU7))
        .with_funding_streams(vec![])
        .to_network()
        .expect("configured network is valid");
    assert!(network.all_funding_streams().is_empty());

    // Extending addresses sizes them for the moved range.
    let network = testnet_with_nu7(Some(TESTNET_NU7))
        .with_funding_streams(configured_streams(ConfiguredFundingStreams {
            height_range: None,
            recipients: Some(revision_2_recipients(1)),
        }))
        .extend_funding_streams()
        .to_network()
        .expect("configured network is valid");
    assert_eq!(
        network.all_funding_streams()[REVISION_2].height_range(),
        &new_range
    );
    assert_eq!(
        network.all_funding_streams()[REVISION_2]
            .recipient(FundingStreamReceiver::MajorGrants)
            .expect("the stream pays grants")
            .addresses()
            .len(),
        27,
    );
}

#[test]
fn moved_revision_2_range_needs_exactly_the_built_in_addresses() {
    let _init_guard = zakura_test::init();

    let build = |num_addresses| {
        testnet_with_nu7(Some(TESTNET_NU7))
            .with_funding_streams(configured_streams(ConfiguredFundingStreams {
                height_range: None,
                recipients: Some(revision_2_recipients(num_addresses)),
            }))
            .to_network()
    };

    let network = build(27).expect("27 addresses cover the moved range");
    assert_eq!(
        required_addresses(
            network.all_funding_streams()[REVISION_2].height_range(),
            &network
        ),
        27,
    );

    let result = std::panic::catch_unwind(|| build(26));
    assert!(result.is_err(), "26 addresses do not cover the moved range");
}

#[test]
fn moved_funding_streams_round_trip_through_configuration() {
    let _init_guard = zakura_test::init();

    let network = testnet_network_with_nu7(Some(TESTNET_NU7));
    let params = network.parameters().expect("Testnet has parameters");

    // Writing out the resolved streams makes their ranges explicit, so reading them
    // back does not move them again.
    let configured: Vec<ConfiguredFundingStreams> =
        params.funding_streams().iter().map(Into::into).collect();
    let round_trip = testnet_with_nu7(Some(TESTNET_NU7))
        .with_funding_streams(configured)
        .to_network()
        .expect("configured network is valid");

    assert_eq!(
        round_trip.all_funding_streams(),
        network.all_funding_streams()
    );
    assert_eq!(
        round_trip.all_funding_streams()[REVISION_2]
            .height_range()
            .end,
        Height(4_656_000)
    );
}

#[test]
fn funding_stream_values_before_and_after_the_zip_218_third_halving() {
    let _init_guard = zakura_test::init();

    use crate::parameters::subsidy::funding_stream_values;

    let network = testnet_network_with_nu7(Some(TESTNET_NU7));
    let values = |height: u32| -> HashMap<FundingStreamReceiver, i64> {
        let subsidy = halving_block_subsidy(Height(height), &network).expect("valid block subsidy");
        funding_stream_values(Height(height), &network, subsidy)
            .expect("valid funding stream values")
            .into_iter()
            .map(|(receiver, amount)| (receiver, i64::from(amount)))
            .collect()
    };

    // The grants and lockbox shares round down: 8% and 12% of 52,083,333 zatoshi.
    let expected: HashMap<_, _> = [
        (FundingStreamReceiver::MajorGrants, 4_166_666),
        (FundingStreamReceiver::Deferred, 6_249_999),
    ]
    .into_iter()
    .collect();
    for height in [TESTNET_THIRD_HALVING - 1, TESTNET_THIRD_HALVING, 4_655_999] {
        assert_eq!(values(height), expected, "at {height}");
    }

    assert!(values(4_656_000).is_empty());
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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// For any NU7 activation height, the Revision 2 streams end at the ZIP 218 third
    /// halving, keep their start heights, and need exactly their built-in recipient
    /// addresses. The earlier streams do not move. NU7 at or after the old third halving
    /// does not reactivate the streams.
    #[test]
    fn revision_2_streams_fit_their_addresses_for_any_nu7_height(
        testnet_nu7 in activation_heights::testnet::NU6_3.0 + 1..=TESTNET_THIRD_HALVING + 200_000,
        mainnet_nu7 in activation_heights::mainnet::NU6_3.0 + 1..=MAINNET_THIRD_HALVING + 200_000,
    ) {
        let _init_guard = zakura_test::init();

        let ratio = NU7_POW_TARGET_SPACING_RATIO;
        let expected_end = |nu7: u32, third_halving: u32| {
            if nu7 < third_halving {
                nu7 + ratio * (third_halving - nu7)
            } else {
                third_halving
            }
        };

        // Building the network checks that the recipient addresses cover the range.
        let network = testnet_network_with_nu7(Some(testnet_nu7));
        let streams = network.all_funding_streams();
        prop_assert_eq!(&streams[..REVISION_2], &testnet_constants::FUNDING_STREAMS[..REVISION_2]);
        let range = streams[REVISION_2].height_range();
        prop_assert_eq!(
            range.start,
            testnet_constants::FUNDING_STREAMS[REVISION_2].height_range().start
        );
        prop_assert_eq!(range.end, Height(expected_end(testnet_nu7, TESTNET_THIRD_HALVING)));
        prop_assert_eq!(Some(range.end), height_for_halving(3, &network));
        prop_assert_eq!(required_addresses(range, &network), 27);
        prop_assert_eq!(network.funding_streams(range.end), None);

        let network = mainnet_with_nu7(Some(mainnet_nu7));
        let revision_2 = mainnet::FUNDING_STREAMS[REVISION_2]
            .clone()
            .with_nu7_adjusted_end_height(Some(Height(mainnet_nu7)));
        let range = revision_2.height_range();
        prop_assert_eq!(
            range.start,
            mainnet::FUNDING_STREAMS[REVISION_2].height_range().start
        );
        prop_assert_eq!(range.end, Height(expected_end(mainnet_nu7, MAINNET_THIRD_HALVING)));
        prop_assert_eq!(Some(range.end), height_for_halving(3, &network));
        prop_assert_eq!(required_addresses(range, &network), 36);
    }
}
