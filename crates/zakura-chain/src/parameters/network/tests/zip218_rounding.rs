//! ZIP 218 divides the block subsidy by 3 at NU7, and the subsidy and funding streams round
//! down at every block. These tests pin what that rounding costs each recipient.
//!
//! The expected values restate the specification formulas with literals, independently of
//! the production constants.

use std::collections::HashMap;

use super::testnet_with_nu7;
use crate::{
    amount::{Amount, NonNegative, MAX_MONEY},
    block::Height,
    parameters::{
        subsidy::{
            block_subsidy, funding_stream_values, halving, halving_block_subsidy,
            height_for_halving, miner_subsidy, reissuance_bonus, FundingStreamReceiver,
        },
        testnet::{
            ConfiguredActivationHeights, ConfiguredFundingStreamRecipient,
            ConfiguredFundingStreams, RegtestParameters,
        },
        Network,
    },
};

/// `MaxBlockSubsidy`, 12.5 ZEC in zatoshi.
const MAX_BLOCK_SUBSIDY: u64 = 1_250_000_000;

/// The Testnet third halving before ZIP 218.
const TESTNET_THIRD_HALVING: u32 = 4_476_000;

/// Returns the Testnet network with NU7 at `nu7` and the built-in funding streams.
fn testnet_network_with_nu7(nu7: Option<u32>) -> Network {
    testnet_with_nu7(nu7)
        .to_network()
        .expect("configured network is valid")
}

/// The block subsidy, grants, lockbox, and miner amounts, summed over a height range.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Payouts {
    subsidy: i64,
    grants: i64,
    lockbox: i64,
    miner: i64,
}

/// Sums the payouts of every block in `heights` on `network`.
fn sum_payouts(network: &Network, heights: std::ops::Range<u32>) -> Payouts {
    let mut sum = Payouts::default();
    for height in heights.map(Height) {
        let subsidy =
            halving_block_subsidy(height, network).expect("the test heights have a subsidy");
        let streams = funding_stream_values(height, network, subsidy)
            .expect("stream values fit in an amount because they are below the subsidy");
        let stream = |receiver| streams.get(&receiver).copied().map_or(0, i64::from);

        sum.subsidy += i64::from(subsidy);
        sum.grants += stream(FundingStreamReceiver::MajorGrants);
        sum.lockbox += stream(FundingStreamReceiver::Deferred);
        sum.miner += i64::from(
            miner_subsidy(height, network, subsidy).expect("the streams fit in the subsidy"),
        );
    }
    sum
}

/// The blocks from NU7 to the ZIP 218 third halving cover the same time as the blocks from
/// NU7 to the old third halving, and pay the same amounts minus the per-block rounding.
///
/// Before ZIP 218 a block pays 156,250,000 zatoshi: 12,500,000 to grants, 18,750,000 to the
/// lockbox, and 125,000,000 to the miner. Three NU7 blocks pay 52,083,333 each, and the
/// streams take `floor(52,083,333 · 8 / 100)` = 4,166,666 and `floor(52,083,333 · 12 / 100)`
/// = 6,249,999. So every three NU7 blocks pay grants 2 zatoshi less, the lockbox 3 less, and
/// the miner 4 more, and issue 1 zatoshi less in total.
///
/// This test sums every block, so any height with a different amount changes the totals.
#[test]
fn nu7_rounding_loss_over_every_block_until_the_third_halving() {
    let _init_guard = zakura_test::init();

    let without_nu7 = testnet_network_with_nu7(None);

    // NU7 at the zakura#1059 estimate, at #1213's configured fork height, and one group of
    // three NU7 blocks before the old third halving. zcash/zips#1370 requires NU7 heights
    // that are multiples of 3.
    for nu7 in [4_386_000, 4_398_756, TESTNET_THIRD_HALVING - 3] {
        let network = testnet_network_with_nu7(Some(nu7));
        let old_blocks = TESTNET_THIRD_HALVING - nu7;
        let third_halving = nu7 + 3 * old_blocks;
        assert_eq!(height_for_halving(3, &network), Some(Height(third_halving)));

        let before = sum_payouts(&without_nu7, nu7..TESTNET_THIRD_HALVING);
        let after = sum_payouts(&network, nu7..third_halving);
        let groups = i64::from(old_blocks);

        assert_eq!(
            before,
            Payouts {
                subsidy: 156_250_000 * groups,
                grants: 12_500_000 * groups,
                lockbox: 18_750_000 * groups,
                miner: 125_000_000 * groups,
            },
            "NU7 at {nu7}",
        );
        assert_eq!(
            after,
            Payouts {
                subsidy: before.subsidy - groups,
                grants: before.grants - 2 * groups,
                lockbox: before.lockbox - 3 * groups,
                miner: before.miner + 4 * groups,
            },
            "NU7 at {nu7}",
        );
    }
}

/// In every halving era after NU7, the block subsidy is ZIP 218's single floor,
/// `floor(MaxBlockSubsidy / (BlossomPoWTargetSpacingRatio · NU7PoWTargetSpacingRatio ·
/// 2^Halving))`, and every three blocks issue `V mod 3` zatoshi less than one 75-second
/// block, where `V` is the 75-second subsidy of that era. The loop runs until the subsidy
/// is zero.
#[test]
fn nu7_subsidy_rounding_in_every_halving_era() {
    let _init_guard = zakura_test::init();

    let nu7 = 4_386_000;
    let network = testnet_network_with_nu7(Some(nu7));
    assert_eq!(halving(Height(nu7), &network), 2);

    let mut zero_era = None;
    for era in 2u32..=40 {
        let start = if era == 2 {
            Height(nu7)
        } else {
            height_for_halving(era, &network)
                .expect("every era below 40 starts below the maximum height")
        };
        let divisor = 1u64 << era;

        // `V`, the subsidy of a 75-second block in this era.
        let old_subsidy = MAX_BLOCK_SUBSIDY / (2 * divisor);
        let expected = MAX_BLOCK_SUBSIDY / (2 * 3 * divisor);

        for height in [
            start,
            (start + 1).expect("the era start is far below the maximum height"),
        ] {
            let subsidy = i64::from(
                halving_block_subsidy(height, &network).expect("the test heights have a subsidy"),
            );
            assert_eq!(subsidy, i64::try_from(expected).unwrap(), "era {era}");
        }
        if era > 2 {
            let previous = start.previous().expect("the halving is above genesis");
            let subsidy = i64::from(
                halving_block_subsidy(previous, &network).expect("the test heights have a subsidy"),
            );
            let previous_expected = MAX_BLOCK_SUBSIDY / (2 * 3 * (divisor / 2));
            assert_eq!(
                subsidy,
                i64::try_from(previous_expected).unwrap(),
                "era {era}"
            );
        }

        assert_eq!(3 * expected + old_subsidy % 3, old_subsidy, "era {era}");

        if expected == 0 {
            zero_era = Some(era);
            break;
        }
    }

    // floor(1,250,000,000 / (6 · 2^28)) = 0, and 2^27 still leaves 1 zatoshi.
    assert_eq!(zero_era, Some(28));
}

/// Once ZIP 234 reissuance starts, the block subsidy includes the reissuance bonus, and each
/// funding stream takes `floor(BlockSubsidy · numerator / 100)` of that sum. Flooring the
/// scheduled subsidy and the bonus separately would pay the stream less.
///
/// Public networks reach reissuance only after the ZIP 218 third halving, where the
/// streams have ended, so this uses a Regtest lockbox stream that overlaps reissuance.
#[test]
fn funding_streams_round_once_over_the_reissuance_bonus() {
    let _init_guard = zakura_test::init();

    let reissuance = Height(3);
    let network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu7: Some(2),
            ..Default::default()
        },
        funding_streams: Some(vec![ConfiguredFundingStreams {
            height_range: Some(Height(1)..Height(100)),
            recipients: Some(vec![ConfiguredFundingStreamRecipient {
                receiver: FundingStreamReceiver::Deferred,
                numerator: 12,
                addresses: None,
            }]),
        }]),
        test_nsm_reissuance_height: Some(reissuance),
        ..Default::default()
    });

    let scheduled = i64::from(
        halving_block_subsidy(reissuance, &network).expect("the test height has a subsidy"),
    );
    let mut separate_floors_differ = false;

    for balance in [0, 1, 10_000_000_000, MAX_MONEY] {
        let balance = Amount::<NonNegative>::try_from(balance).expect("the balances are money");
        let bonus = i64::from(reissuance_bonus(balance).expect("the bonus is below the balance"));
        let subsidy = block_subsidy(reissuance, &network, Some(balance))
            .expect("the network supplies a reissuance height and the balance");
        assert_eq!(i64::from(subsidy), scheduled + bonus);

        let streams: HashMap<_, _> = funding_stream_values(reissuance, &network, subsidy)
            .expect("stream values fit in an amount because they are below the subsidy")
            .into_iter()
            .map(|(receiver, amount)| (receiver, i64::from(amount)))
            .collect();
        let lockbox = (scheduled + bonus) * 12 / 100;
        assert_eq!(
            streams,
            HashMap::from([(FundingStreamReceiver::Deferred, lockbox)]),
            "balance {balance:?}",
        );
        assert_eq!(
            i64::from(
                miner_subsidy(reissuance, &network, subsidy)
                    .expect("the stream fits in the subsidy")
            ),
            scheduled + bonus - lockbox,
        );

        separate_floors_differ |= scheduled * 12 / 100 + bonus * 12 / 100 != lockbox;
    }

    assert!(
        separate_floors_differ,
        "some balance must tell one floor over the sum from two floors",
    );
}
