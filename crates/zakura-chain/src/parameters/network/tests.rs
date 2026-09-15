#![allow(clippy::unwrap_in_result)]

mod prop;
mod vectors;

use color_eyre::Report;

use super::Network;
use crate::{
    amount::{Amount, NonNegative},
    block::{Height, HeightDiff},
    parameters::{
        subsidy::{
            block_subsidy, constants::POST_BLOSSOM_HALVING_INTERVAL, funding_stream_address_period,
            halving, halving_divisor, height_for_halving, ParameterSubsidy,
        },
        NetworkUpgrade,
    },
};

#[test]
fn funding_stream_period_uses_floor_division_for_negative_periods() {
    struct TestParameters;

    impl ParameterSubsidy for TestParameters {
        fn height_for_first_halving(&self) -> Height {
            Height(100)
        }

        fn post_blossom_halving_interval(&self) -> HeightDiff {
            50
        }

        fn pre_blossom_halving_interval(&self) -> HeightDiff {
            25
        }

        fn funding_stream_address_change_interval(&self) -> HeightDiff {
            10
        }
    }

    let parameters = TestParameters;

    assert_eq!(0, funding_stream_address_period(Height(50), &parameters));
    assert_eq!(-1, funding_stream_address_period(Height(49), &parameters));
    assert_eq!(-2, funding_stream_address_period(Height(39), &parameters));
}

#[test]
fn halving_test() -> Result<(), Report> {
    let _init_guard = zakura_test::init();
    for network in Network::iter() {
        halving_for_network(&network)?;
    }

    Ok(())
}

fn halving_for_network(network: &Network) -> Result<(), Report> {
    let blossom_height = NetworkUpgrade::Blossom.activation_height(network).unwrap();
    let first_halving_height = network.height_for_first_halving();

    assert_eq!(
        1,
        halving_divisor((network.slow_start_interval() + 1).unwrap(), network).unwrap()
    );
    assert_eq!(
        1,
        halving_divisor((blossom_height - 1).unwrap(), network).unwrap()
    );
    assert_eq!(1, halving_divisor(blossom_height, network).unwrap());
    assert_eq!(
        1,
        halving_divisor((first_halving_height - 1).unwrap(), network).unwrap()
    );

    assert_eq!(2, halving_divisor(first_halving_height, network).unwrap());
    assert_eq!(
        2,
        halving_divisor((first_halving_height + 1).unwrap(), network).unwrap()
    );

    assert_eq!(
        4,
        halving_divisor(
            (first_halving_height + POST_BLOSSOM_HALVING_INTERVAL).unwrap(),
            network
        )
        .unwrap()
    );
    assert_eq!(
        8,
        halving_divisor(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 2)).unwrap(),
            network
        )
        .unwrap()
    );

    assert_eq!(
        1024,
        halving_divisor(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 9)).unwrap(),
            network
        )
        .unwrap()
    );
    assert_eq!(
        1024 * 1024,
        halving_divisor(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 19)).unwrap(),
            network
        )
        .unwrap()
    );
    assert_eq!(
        1024 * 1024 * 1024,
        halving_divisor(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 29)).unwrap(),
            network
        )
        .unwrap()
    );
    assert_eq!(
        1024 * 1024 * 1024 * 1024,
        halving_divisor(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 39)).unwrap(),
            network
        )
        .unwrap()
    );

    // The largest possible integer divisor
    assert_eq!(
        (i64::MAX as u64 + 1),
        halving_divisor(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 62)).unwrap(),
            network
        )
        .unwrap(),
    );

    // Very large divisors which should also result in zero amounts
    assert_eq!(
        None,
        halving_divisor(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 63)).unwrap(),
            network,
        ),
    );

    assert_eq!(
        None,
        halving_divisor(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 64)).unwrap(),
            network,
        ),
    );

    assert_eq!(
        None,
        halving_divisor(Height(Height::MAX_AS_U32 / 4), network),
    );

    assert_eq!(
        None,
        halving_divisor(Height(Height::MAX_AS_U32 / 2), network),
    );

    assert_eq!(None, halving_divisor(Height::MAX, network));

    Ok(())
}

#[test]
fn block_subsidy_test() -> Result<(), Report> {
    let _init_guard = zakura_test::init();

    for network in Network::iter() {
        block_subsidy_for_network(&network)?;
    }

    Ok(())
}

fn block_subsidy_for_network(network: &Network) -> Result<(), Report> {
    let blossom_height = NetworkUpgrade::Blossom.activation_height(network).unwrap();
    let first_halving_height = network.height_for_first_halving();

    // After slow-start mining and before Blossom the block subsidy is 12.5 ZEC
    // https://z.cash/support/faq/#what-is-slow-start-mining
    assert_eq!(
        Amount::<NonNegative>::try_from(1_250_000_000)?,
        block_subsidy((network.slow_start_interval() + 1).unwrap(), network)?
    );
    assert_eq!(
        Amount::<NonNegative>::try_from(1_250_000_000)?,
        block_subsidy((blossom_height - 1).unwrap(), network)?
    );

    // After Blossom the block subsidy is reduced to 6.25 ZEC without halving
    // https://z.cash/upgrade/blossom/
    assert_eq!(
        Amount::<NonNegative>::try_from(625_000_000)?,
        block_subsidy(blossom_height, network)?
    );

    // After the 1st halving, the block subsidy is reduced to 3.125 ZEC
    // https://z.cash/upgrade/canopy/
    assert_eq!(
        Amount::<NonNegative>::try_from(312_500_000)?,
        block_subsidy(first_halving_height, network)?
    );

    // After the 2nd halving, the block subsidy is reduced to 1.5625 ZEC
    // See "7.8 Calculation of Block Subsidy and Founders' Reward"
    assert_eq!(
        Amount::<NonNegative>::try_from(156_250_000)?,
        block_subsidy(
            (first_halving_height + POST_BLOSSOM_HALVING_INTERVAL).unwrap(),
            network
        )?
    );

    // After the 7th halving, the block subsidy is reduced to 0.04882812 ZEC
    // Check that the block subsidy rounds down correctly, and there are no errors
    assert_eq!(
        Amount::<NonNegative>::try_from(4_882_812)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 6)).unwrap(),
            network
        )?
    );

    // After the 29th halving, the block subsidy is 1 zatoshi
    // Check that the block subsidy is calculated correctly at the limit
    assert_eq!(
        Amount::<NonNegative>::try_from(1)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 28)).unwrap(),
            network
        )?
    );

    // After the 30th halving, there is no block subsidy
    // Check that there are no errors
    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 29)).unwrap(),
            network
        )?
    );

    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 39)).unwrap(),
            network
        )?
    );

    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 49)).unwrap(),
            network
        )?
    );

    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 59)).unwrap(),
            network
        )?
    );

    // The largest possible integer divisor
    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 62)).unwrap(),
            network
        )?
    );

    // Other large divisors which should also result in zero
    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 63)).unwrap(),
            network
        )?
    );

    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 64)).unwrap(),
            network
        )?
    );

    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(Height(Height::MAX_AS_U32 / 4), network)?
    );

    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(Height(Height::MAX_AS_U32 / 2), network)?
    );

    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(Height::MAX, network)?
    );

    Ok(())
}

#[test]
fn check_height_for_num_halvings() {
    for network in Network::iter() {
        for h in 1..1000 {
            let Some(height_for_halving) = height_for_halving(h, &network) else {
                panic!("could not find height for halving {h}");
            };

            let prev_height = height_for_halving
                .previous()
                .expect("there should be a previous height");

            assert_eq!(
                h,
                halving(height_for_halving, &network),
                "num_halvings should match the halving index"
            );

            assert_eq!(
                h - 1,
                halving(prev_height, &network),
                "num_halvings for the prev height should be 1 less than the halving index"
            );
        }
    }
}

/// Tests `halving` against ZIP 218's `Halving` formula on a configured Testnet
/// whose Blossom height is below `SlowStartShift`, with and without NU7.
///
/// On such a network the formula's pre-Blossom term is negative. A build
/// without the `nu7` feature treats NU7 as inactive, so it follows the
/// formula's Blossom case at every height.
#[test]
fn halving_matches_zip_218_when_blossom_is_below_the_slow_start_shift() {
    use crate::parameters::{
        testnet::{self, ConfiguredActivationHeights},
        NU7_POW_TARGET_SPACING_RATIO, ZIP218_ENABLED,
    };

    let _init_guard = zakura_test::init();

    let blossom = 4;
    let nu7 = 2_000_000;

    for configured_nu7 in [None, Some(nu7)] {
        let network = testnet::Parameters::build()
            .with_activation_heights(ConfiguredActivationHeights {
                blossom: Some(blossom),
                canopy: Some(blossom + 2),
                nu7: configured_nu7,
                ..Default::default()
            })
            .expect("activation heights are valid")
            .clear_funding_streams()
            .to_network()
            .expect("configured testnet is valid");

        let slow_start_shift = i128::from(network.slow_start_shift().0);
        assert!(i128::from(blossom) < slow_start_shift);

        let pre_blossom_interval = i128::from(network.pre_blossom_halving_interval());
        let post_blossom_interval = i128::from(network.post_blossom_halving_interval());
        let post_nu7_interval = post_blossom_interval * i128::from(NU7_POW_TARGET_SPACING_RATIO);
        let nu7_activation = configured_nu7.filter(|_| ZIP218_ENABLED).map(i128::from);

        // `Halving(height)` from ZIP 218, as an exact fraction over the product
        // of the three halving intervals. A negative index has no meaning, so
        // `halving` returns zero there.
        let zip_halving = |height: Height| -> u32 {
            let height = i128::from(height.0);
            let blossom = i128::from(blossom);
            let (pre_blossom_blocks, post_blossom_blocks, post_nu7_blocks) = if height < blossom {
                (height - slow_start_shift, 0, 0)
            } else {
                match nu7_activation {
                    Some(nu7) if height >= nu7 => {
                        (blossom - slow_start_shift, nu7 - blossom, height - nu7)
                    }
                    _ => (blossom - slow_start_shift, height - blossom, 0),
                }
            };

            let numerator = pre_blossom_blocks * post_blossom_interval * post_nu7_interval
                + post_blossom_blocks * pre_blossom_interval * post_nu7_interval
                + post_nu7_blocks * pre_blossom_interval * post_blossom_interval;
            let denominator = pre_blossom_interval * post_blossom_interval * post_nu7_interval;

            numerator
                .div_euclid(denominator)
                .max(0)
                .try_into()
                .expect("the test halving index fits in u32")
        };

        let slow_start_shift = network.slow_start_shift();
        let mut heights = vec![
            slow_start_shift,
            (slow_start_shift + 1).expect("the height is valid"),
            Height(nu7 - 1),
            Height(nu7),
            Height(nu7 + 1),
            Height(Height::MAX.0 / 2),
        ];
        for halving_index in 1..=4 {
            let halving_height =
                height_for_halving(halving_index, &network).expect("the halving has a height");
            heights.push(halving_height);
            heights.push(
                halving_height
                    .previous()
                    .expect("the halving is above genesis"),
            );
            assert_eq!(halving_index, zip_halving(halving_height));
        }

        for height in heights {
            assert_eq!(
                zip_halving(height),
                halving(height, &network),
                "halving at {height:?} with NU7 at {configured_nu7:?}",
            );
        }
    }
}

/// Tests the ZIP 218 target spacing, halving, and block subsidy across the NU7
/// activation boundary on a configured Testnet.
#[test]
#[cfg(feature = "nu7")]
fn post_nu7_spacing_halving_and_subsidy() -> Result<(), Report> {
    use crate::parameters::{
        testnet::{self, ConfiguredActivationHeights},
        NU7_POW_TARGET_SPACING_RATIO, POST_BLOSSOM_POW_TARGET_SPACING, POST_NU7_POW_TARGET_SPACING,
    };

    let _init_guard = zakura_test::init();

    // Choose parameters where slow_start_shift == blossom_height, so the
    // pre-Blossom term of the spec's halving sum is exactly zero and the halving
    // boundaries land on multiples of the post-Blossom halving interval.
    let blossom = 1u32;
    let canopy = blossom + u32::try_from(POST_BLOSSOM_HALVING_INTERVAL).unwrap();
    let nu7 = canopy + u32::try_from(POST_BLOSSOM_HALVING_INTERVAL * 2).unwrap();

    let network = testnet::Parameters::build()
        // slow_start_shift = slow_start_interval / 2 = 1, the Blossom height.
        .with_slow_start_interval(Height(2))
        .with_activation_heights(ConfiguredActivationHeights {
            blossom: Some(blossom),
            canopy: Some(canopy),
            nu7: Some(nu7),
            ..Default::default()
        })
        .expect("activation heights are valid")
        .clear_funding_streams()
        .to_network()
        .expect("configured testnet is valid");

    let nu7_height = Height(nu7);

    // The target spacing shortens exactly at the NU7 activation height.
    assert_eq!(
        i64::from(POST_BLOSSOM_POW_TARGET_SPACING),
        NetworkUpgrade::target_spacing_for_height(&network, (nu7_height - 1).unwrap())
            .num_seconds()
    );
    assert_eq!(
        i64::from(POST_NU7_POW_TARGET_SPACING),
        NetworkUpgrade::target_spacing_for_height(&network, nu7_height).num_seconds()
    );

    // Three post-Blossom halvings have elapsed at NU7 activation:
    //   Halving = floor(0/PreBlossom + 1 + 2) = 3
    assert_eq!(3, halving(nu7_height, &network));
    assert_eq!(8, halving_divisor(nu7_height, &network).unwrap());

    // BlockSubsidy(NU7) = floor(MAX / (BlossomRatio * NU7Ratio * 2^Halving))
    //                   = floor(1_250_000_000 / (2 * 3 * 8)) = 26_041_666 zatoshi
    assert_eq!(
        Amount::<NonNegative>::try_from(26_041_666)?,
        block_subsidy(nu7_height, &network)?,
    );

    // The third halving boundary lands exactly at NU7 here, so the block before
    // NU7 is still in halving era 2: floor(1_250_000_000 / (2 * 4)) zatoshi.
    assert_eq!(2, halving((nu7_height - 1).unwrap(), &network));
    assert_eq!(
        Amount::<NonNegative>::try_from(156_250_000)?,
        block_subsidy((nu7_height - 1).unwrap(), &network)?,
    );

    // The halving counter does not reset at NU7. The next boundary arrives after
    // one PostNU7HalvingInterval (= PostBlossomHalvingInterval * 3) of blocks.
    let post_nu7_halving_interval =
        POST_BLOSSOM_HALVING_INTERVAL * i64::from(NU7_POW_TARGET_SPACING_RATIO);
    let next_halving = (nu7_height + post_nu7_halving_interval).unwrap();
    assert_eq!(4, halving(next_halving, &network));
    assert_eq!(Some(next_halving), height_for_halving(4, &network));
    assert_eq!(
        3,
        halving(
            height_for_halving(4, &network)
                .expect("the fourth halving has a height")
                .previous()
                .expect("the fourth halving is above genesis"),
            &network,
        )
    );
    assert_eq!(16, halving_divisor(next_halving, &network).unwrap());
    assert_eq!(
        Amount::<NonNegative>::try_from(13_020_833)?,
        block_subsidy(next_halving, &network)?,
    );

    Ok(())
}

/// Tests that the ZIP 218 difficulty averaging window widens at the NU7
/// activation height.
#[test]
#[cfg(feature = "nu7")]
fn averaging_window_changes_at_nu7_activation_height() -> Result<(), Report> {
    use crate::parameters::{
        testnet::{self, ConfiguredActivationHeights},
        POST_NU7_POW_AVERAGING_WINDOW, PRE_NU7_POW_AVERAGING_WINDOW,
    };

    let _init_guard = zakura_test::init();

    let network = testnet::Parameters::build()
        .with_activation_heights(ConfiguredActivationHeights {
            blossom: Some(1),
            nu7: Some(10),
            ..Default::default()
        })
        .expect("activation heights are valid")
        .clear_funding_streams()
        .to_network()
        .expect("configured testnet is valid");

    assert_eq!(
        PRE_NU7_POW_AVERAGING_WINDOW,
        NetworkUpgrade::averaging_window_for_height(&network, Height(9))
    );
    assert_eq!(
        POST_NU7_POW_AVERAGING_WINDOW,
        NetworkUpgrade::averaging_window_for_height(&network, Height(10))
    );
    assert_eq!(
        POST_NU7_POW_AVERAGING_WINDOW,
        NetworkUpgrade::averaging_window_for_height(&network, Height(11))
    );

    Ok(())
}
