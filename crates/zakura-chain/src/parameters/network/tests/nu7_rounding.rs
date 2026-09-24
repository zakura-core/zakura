//! ZIP 218 splits the post-Blossom subsidy exactly over each group of three post-NU7 blocks.
//!
//! The NU7 heights here are the zakura#1059 estimates. They are not final.

use std::collections::HashMap;

use proptest::prelude::*;

use super::testnet_with_nu7;
use crate::{
    amount::{Amount, NonNegative, MAX_MONEY},
    block::Height,
    parameters::{
        constants::activation_heights,
        subsidy::{
            constants::MAX_BLOCK_SUBSIDY, funding_stream_values, halving_block_subsidy,
            halving_divisor, height_for_halving, miner_subsidy, nu7_rounding_group,
            scheduled_issuance_zatoshis, FundingStreamReceiver, BLOCK_SUBSIDY_FRACTION_DENOMINATOR,
            BLOCK_SUBSIDY_FRACTION_NUMERATOR,
        },
        Network, NU7_POW_TARGET_SPACING_RATIO,
    },
};

/// The Testnet NU7 activation height estimate.
const TESTNET_NU7: u32 = 4_386_000;

/// The Testnet third halving before ZIP 218.
const TESTNET_THIRD_HALVING: u32 = 4_476_000;

/// Returns the Testnet network with NU7 at `nu7` and the built-in funding streams.
fn testnet_network_with_nu7(nu7: Option<u32>) -> Network {
    testnet_with_nu7(nu7)
        .to_network()
        .expect("configured network is valid")
}

/// Returns `floor(MaxBlockSubsidy / (2 · 2^Halving(height)))`, the subsidy of a
/// post-Blossom block at `height`'s halving.
fn post_blossom_subsidy(height: u32, network: &Network) -> u64 {
    MAX_BLOCK_SUBSIDY / 2 / halving_divisor(Height(height), network).expect("divisor fits")
}

fn subsidy(height: u32, network: &Network) -> u64 {
    u64::try_from(i64::from(
        halving_block_subsidy(Height(height), network).expect("valid subsidy"),
    ))
    .expect("subsidies are non-negative")
}

fn streams(height: u32, network: &Network) -> HashMap<FundingStreamReceiver, u64> {
    let subsidy = halving_block_subsidy(Height(height), network).expect("valid subsidy");
    funding_stream_values(Height(height), network, subsidy)
        .expect("valid stream values")
        .into_iter()
        .map(|(receiver, value)| {
            (
                receiver,
                u64::try_from(i64::from(value)).expect("values are non-negative"),
            )
        })
        .collect()
}

fn miner(height: u32, network: &Network) -> u64 {
    let subsidy = halving_block_subsidy(Height(height), network).expect("valid subsidy");
    u64::try_from(i64::from(
        miner_subsidy(Height(height), network, subsidy).expect("valid miner subsidy"),
    ))
    .expect("the miner subsidy is non-negative")
}

fn scheduled(height: u32, network: &Network) -> u128 {
    scheduled_issuance_zatoshis(Height(height), network).expect("valid issuance")
}

/// Every group of three post-NU7 blocks pays exactly one post-Blossom block, to every
/// party, and the miner's share is the exact remainder.
#[test]
fn rounding_groups_pay_one_post_blossom_block_to_every_party() {
    let _init_guard = zakura_test::init();

    let network = testnet_network_with_nu7(Some(TESTNET_NU7));
    let grants = FundingStreamReceiver::MajorGrants;
    let lockbox = FundingStreamReceiver::Deferred;

    // The block before NU7 is unchanged.
    assert_eq!(subsidy(TESTNET_NU7 - 1, &network), 156_250_000);
    assert_eq!(streams(TESTNET_NU7 - 1, &network)[&grants], 12_500_000);
    assert_eq!(streams(TESTNET_NU7 - 1, &network)[&lockbox], 18_750_000);
    assert_eq!(miner(TESTNET_NU7 - 1, &network), 125_000_000);
    assert_eq!(nu7_rounding_group(Height(TESTNET_NU7 - 1), &network), None);

    // The first block of each group pays the plain ZIP 218 subsidy, and the later blocks
    // pay the zatoshi that its floor dropped. The lockbox's 18,750,000 divides evenly by
    // three, so it gets 6,250,000 at every block: one zatoshi more than the plain rule's
    // `floor(52,083,333 · 12 / 100)`, because the exact rule takes the share of the
    // post-Blossom value before splitting it. The miner takes the remainder.
    let expected = [
        // (subsidy, grants, lockbox, miner)
        (52_083_333, 4_166_666, 6_250_000, 41_666_667),
        (52_083_333, 4_166_667, 6_250_000, 41_666_666),
        (52_083_334, 4_166_667, 6_250_000, 41_666_667),
    ];
    for (offset, (subsidy_value, grants_value, lockbox_value, miner_value)) in
        expected.into_iter().enumerate()
    {
        let height = TESTNET_NU7 + u32::try_from(offset).expect("small offset");
        let group = nu7_rounding_group(Height(height), &network).expect("NU7 is active");
        assert_eq!(group.index(), u64::try_from(offset).expect("small offset"));
        assert_eq!(group.post_blossom_subsidy(), 156_250_000);

        assert_eq!(
            subsidy(height, &network),
            subsidy_value,
            "subsidy at {height}"
        );
        let streams = streams(height, &network);
        assert_eq!(streams[&grants], grants_value, "grants at {height}");
        assert_eq!(streams[&lockbox], lockbox_value, "lockbox at {height}");
        assert_eq!(miner(height, &network), miner_value, "miner at {height}");
        assert_eq!(
            subsidy_value,
            grants_value + lockbox_value + miner_value,
            "the coinbase balances at {height}"
        );
    }

    let total = |f: &dyn Fn(u32) -> u64| (TESTNET_NU7..TESTNET_NU7 + 3).map(f).sum::<u64>();
    assert_eq!(total(&|h| subsidy(h, &network)), 156_250_000);
    assert_eq!(total(&|h| streams(h, &network)[&grants]), 12_500_000);
    assert_eq!(total(&|h| streams(h, &network)[&lockbox]), 18_750_000);
    assert_eq!(total(&|h| miner(h, &network)), 125_000_000);

    // The third halving is a group boundary, so the plain amount returns there and the
    // pattern continues with the halved value. The built-in streams have ended.
    let third = height_for_halving(3, &network).expect("the third halving has a height");
    assert_eq!(
        third,
        Height(TESTNET_NU7 + 3 * (TESTNET_THIRD_HALVING - TESTNET_NU7))
    );
    assert_eq!(
        nu7_rounding_group(third, &network).expect("active").index(),
        0
    );
    // 78,125,000 / 3 leaves a remainder of two thirds, so the cumulative floor pays the
    // extra zatoshi at the second block already.
    assert_eq!(subsidy(third.0 - 1, &network), 52_083_334);
    assert_eq!(subsidy(third.0, &network), 26_041_666);
    assert_eq!(subsidy(third.0 + 1, &network), 26_041_667);
    assert_eq!(subsidy(third.0 + 2, &network), 26_041_667);
    assert_eq!(
        (third.0..third.0 + 3)
            .map(|h| subsidy(h, &network))
            .sum::<u64>(),
        78_125_000
    );
}

/// Without an NU7 height, or before it, the subsidy and the streams are unchanged.
#[test]
fn rounding_groups_need_nu7() {
    let _init_guard = zakura_test::init();

    let without_nu7 = testnet_network_with_nu7(None);
    for height in [1, 20_000, TESTNET_NU7, TESTNET_THIRD_HALVING, 10_000_000] {
        assert_eq!(nu7_rounding_group(Height(height), &without_nu7), None);
    }
    assert_eq!(subsidy(TESTNET_NU7, &without_nu7), 156_250_000);
    assert_eq!(subsidy(TESTNET_THIRD_HALVING, &without_nu7), 78_125_000);

    // Mainnet has no NU7 height, so nothing changes there today.
    assert_eq!(
        nu7_rounding_group(Height(4_000_000), &Network::Mainnet),
        None
    );
    assert_eq!(subsidy(4_000_000, &Network::Mainnet), 156_250_000);
}

/// The scheduled issuance sum matches the block-by-block subsidies across NU7 activation
/// and across the third halving, where the group pattern starts and changes.
#[test]
fn scheduled_issuance_matches_the_grouped_subsidies() {
    let _init_guard = zakura_test::init();

    let network = testnet_network_with_nu7(Some(TESTNET_NU7));
    let third = height_for_halving(3, &network)
        .expect("the third halving has a height")
        .0;

    for start in [TESTNET_NU7 - 7, third - 7] {
        let mut running = scheduled(start - 1, &network);
        for height in start..start + 16 {
            running += u128::from(subsidy(height, &network));
            assert_eq!(scheduled(height, &network), running, "at {height}");
        }
    }

    // Over whole groups, the post-NU7 chain issues exactly what 75-second blocks would.
    assert_eq!(
        scheduled(TESTNET_NU7 + 3 * 1_000 - 1, &network) - scheduled(TESTNET_NU7 - 1, &network),
        1_000 * 156_250_000
    );
}

/// The reissuance crossing satisfies the ZIP 237 inequality exactly, and fails it one
/// block earlier, under the grouped subsidy.
#[test]
fn nsm_crossing_is_exact_under_rounding_groups() {
    use crate::parameters::subsidy::nsm_reissuance_crossing_height;

    let _init_guard = zakura_test::init();

    let network = testnet_network_with_nu7(Some(TESTNET_NU7));
    let crossing = nsm_reissuance_crossing_height(&network)
        .expect("crossing arithmetic is valid")
        .expect("the estimated Testnet schedule has a crossing");

    let qualifies = |height: u32| {
        let reserve = u128::try_from(MAX_MONEY)
            .expect("positive")
            .saturating_sub(scheduled(height - 1, &network));
        let reference = (reserve * BLOCK_SUBSIDY_FRACTION_NUMERATOR)
            .div_ceil(BLOCK_SUBSIDY_FRACTION_DENOMINATOR);
        reference < u128::from(subsidy(height, &network))
    };

    assert!(!qualifies(crossing.0 - 1));
    assert!(qualifies(crossing.0));
    // Under the plain floor, the constant 26,041,666 subsidy crosses at H3 + 2,807,274
    // (zips#1370). The grouped run pays one zatoshi more at two blocks in three, so it
    // crosses within a group of the same height.
    let third = height_for_halving(3, &network)
        .expect("the third halving has a height")
        .0;
    let plain = third + 2_807_274;
    assert!(
        (plain - 3..=plain + 3).contains(&crossing.0),
        "crossing {crossing:?} is near the plain-floor crossing {plain}"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// For any NU7 height and any aligned group after it: the three blocks pay exactly one
    /// post-Blossom block to the subsidy, to each stream, and to the miner; every block pays
    /// the floor or one zatoshi more; the first block pays the plain ZIP 218 amount; and the
    /// scheduled issuance agrees with the block-by-block sum. Blocks before NU7 are unchanged.
    #[test]
    fn aligned_groups_are_exact_for_any_nu7_height(
        nu7 in activation_heights::testnet::NU6_3.0 + 1..=TESTNET_THIRD_HALVING + 200_000,
        offset in 0u32..=3_000_000,
        before in 20_000u32..=activation_heights::testnet::NU6_3.0,
    ) {
        let _init_guard = zakura_test::init();

        let network = testnet_network_with_nu7(Some(nu7));
        let without_nu7 = testnet_network_with_nu7(None);
        let ratio = NU7_POW_TARGET_SPACING_RATIO;

        prop_assert_eq!(subsidy(before, &network), subsidy(before, &without_nu7));
        prop_assert_eq!(streams(before, &network), streams(before, &without_nu7));
        prop_assert_eq!(subsidy(nu7 - 1, &network), subsidy(nu7 - 1, &without_nu7));

        let start = nu7 + offset - offset % ratio;
        let heights: Vec<u32> = (start..start + ratio).collect();
        let post_blossom = post_blossom_subsidy(start, &network);
        let plain = post_blossom / u64::from(ratio);

        // Halving boundaries are group boundaries, so the group shares one halving.
        prop_assert!(heights.iter().all(|h| post_blossom_subsidy(*h, &network) == post_blossom));

        let subsidies: Vec<u64> = heights.iter().map(|h| subsidy(*h, &network)).collect();
        prop_assert_eq!(subsidies.iter().sum::<u64>(), post_blossom);
        prop_assert_eq!(subsidies[0], plain, "the first block pays the plain ZIP 218 amount");
        prop_assert!(subsidies.iter().all(|s| *s == plain || *s == plain + 1));

        let stream_values: Vec<HashMap<FundingStreamReceiver, u64>> =
            heights.iter().map(|h| streams(*h, &network)).collect();
        let miners: Vec<u64> = heights.iter().map(|h| miner(*h, &network)).collect();

        // The streams are the same for the whole group, because stream ranges start and end
        // on group boundaries. When they pay, each receives exactly its share of a
        // post-Blossom block.
        let receivers: Vec<FundingStreamReceiver> = stream_values[0].keys().copied().collect();
        prop_assert!(stream_values.iter().all(|values| values.len() == receivers.len()));
        let mut streams_total = 0;
        for receiver in receivers {
            let numerator = network
                .funding_streams(Height(start))
                .and_then(|streams| streams.recipient(receiver))
                .expect("the receiver has a recipient")
                .numerator();
            let post_blossom_value = post_blossom * numerator / 100;
            let plain_value = post_blossom_value / u64::from(ratio);
            let values: Vec<u64> = stream_values.iter().map(|values| values[&receiver]).collect();
            prop_assert_eq!(values.iter().sum::<u64>(), post_blossom_value);
            prop_assert_eq!(values[0], plain_value);
            prop_assert!(values.iter().all(|v| *v == plain_value || *v == plain_value + 1));
            streams_total += post_blossom_value;
        }
        prop_assert_eq!(miners.iter().sum::<u64>(), post_blossom - streams_total);
        for (height, (subsidy, values)) in heights.iter().zip(subsidies.iter().zip(&stream_values)) {
            prop_assert_eq!(
                *subsidy,
                values.values().sum::<u64>() + miner(*height, &network),
                "the coinbase balances at {}", height
            );
        }

        prop_assert_eq!(
            scheduled(start + ratio - 1, &network) - scheduled(start - 1, &network),
            u128::from(post_blossom)
        );
    }
}

/// The exact rule keeps every stream and the miner within one zatoshi of their specified
/// share at every block, so the deviation never accumulates.
#[test]
fn deviation_from_the_exact_share_is_bounded() {
    let _init_guard = zakura_test::init();

    let network = testnet_network_with_nu7(Some(TESTNET_NU7));
    let grants = FundingStreamReceiver::MajorGrants;

    let mut paid: u128 = 0;
    for height in TESTNET_NU7..TESTNET_NU7 + 3 * 700 {
        paid += u128::from(streams(height, &network)[&grants]);
        // 8% of the 25-second-equivalent subsidy, as an exact rational scaled by 300.
        let exact_times_300 = u128::from(height - TESTNET_NU7 + 1) * 156_250_000 * 8;
        let deviation_times_300 = (paid * 300).abs_diff(exact_times_300);
        assert!(deviation_times_300 < 300, "at {height}: paid {paid}");
    }

    let amount = Amount::<NonNegative>::try_from(i64::try_from(paid).expect("fits in i64"))
        .expect("fits in an amount");
    assert_eq!(i64::from(amount), 700 * 12_500_000);
}
