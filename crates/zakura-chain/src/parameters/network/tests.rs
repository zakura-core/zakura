#![allow(clippy::unwrap_in_result)]

mod prop;
mod vectors;

use color_eyre::Report;

use super::Network;
use crate::{
    amount::{Amount, NonNegative, MAX_MONEY},
    block::{Height, HeightDiff},
    parameters::{
        subsidy::{
            block_subsidy, constants::POST_BLOSSOM_HALVING_INTERVAL, funding_stream_address_period,
            halving, halving_block_subsidy, halving_divisor, height_for_halving, ParameterSubsidy,
            SubsidyError,
        },
        testnet::{self, ConfiguredActivationHeights},
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

        fn initial_nsm_value_balance(&self) -> Amount<NonNegative> {
            Amount::zero()
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
        block_subsidy((network.slow_start_interval() + 1).unwrap(), network, None)?
    );
    assert_eq!(
        Amount::<NonNegative>::try_from(1_250_000_000)?,
        block_subsidy((blossom_height - 1).unwrap(), network, None)?
    );

    // After Blossom the block subsidy is reduced to 6.25 ZEC without halving
    // https://z.cash/upgrade/blossom/
    assert_eq!(
        Amount::<NonNegative>::try_from(625_000_000)?,
        block_subsidy(blossom_height, network, None)?
    );

    // After the 1st halving, the block subsidy is reduced to 3.125 ZEC
    // https://z.cash/upgrade/canopy/
    assert_eq!(
        Amount::<NonNegative>::try_from(312_500_000)?,
        block_subsidy(first_halving_height, network, None)?
    );

    // After the 2nd halving, the block subsidy is reduced to 1.5625 ZEC
    // See "7.8 Calculation of Block Subsidy and Founders' Reward"
    assert_eq!(
        Amount::<NonNegative>::try_from(156_250_000)?,
        block_subsidy(
            (first_halving_height + POST_BLOSSOM_HALVING_INTERVAL).unwrap(),
            network,
            None
        )?
    );

    // After the 7th halving, the block subsidy is reduced to 0.04882812 ZEC
    // Check that the block subsidy rounds down correctly, and there are no errors
    assert_eq!(
        Amount::<NonNegative>::try_from(4_882_812)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 6)).unwrap(),
            network,
            None
        )?
    );

    // After the 29th halving, the block subsidy is 1 zatoshi
    // Check that the block subsidy is calculated correctly at the limit
    assert_eq!(
        Amount::<NonNegative>::try_from(1)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 28)).unwrap(),
            network,
            None
        )?
    );

    // After the 30th halving, there is no block subsidy
    // Check that there are no errors
    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 29)).unwrap(),
            network,
            None
        )?
    );

    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 39)).unwrap(),
            network,
            None
        )?
    );

    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 49)).unwrap(),
            network,
            None
        )?
    );

    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 59)).unwrap(),
            network,
            None
        )?
    );

    // The largest possible integer divisor
    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 62)).unwrap(),
            network,
            None
        )?
    );

    // Other large divisors which should also result in zero
    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 63)).unwrap(),
            network,
            None
        )?
    );

    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(
            (first_halving_height + (POST_BLOSSOM_HALVING_INTERVAL * 64)).unwrap(),
            network,
            None
        )?
    );

    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(Height(Height::MAX_AS_U32 / 4), network, None)?
    );

    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(Height(Height::MAX_AS_U32 / 2), network, None)?
    );

    assert_eq!(
        Amount::<NonNegative>::try_from(0)?,
        block_subsidy(Height::MAX, network, None)?
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
/// On such a network the formula's pre-Blossom term is negative.
#[test]
fn halving_matches_zip_218_when_blossom_is_below_the_slow_start_shift() {
    use crate::parameters::{
        testnet::{self, ConfiguredActivationHeights},
        NU7_POW_TARGET_SPACING_RATIO,
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
        let nu7_activation = configured_nu7.map(i128::from);

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
        halving_block_subsidy(nu7_height, &network)?,
    );

    // The third halving boundary lands exactly at NU7 here, so the block before
    // NU7 is still in halving era 2: floor(1_250_000_000 / (2 * 4)) zatoshi.
    assert_eq!(2, halving((nu7_height - 1).unwrap(), &network));
    assert_eq!(
        Amount::<NonNegative>::try_from(156_250_000)?,
        halving_block_subsidy((nu7_height - 1).unwrap(), &network)?,
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
        halving_block_subsidy(next_halving, &network)?,
    );

    Ok(())
}

/// Tests that the averaging window widens at NU7 and that every build retains
/// enough context for the wider window.
#[test]
fn averaging_window_changes_at_nu7_activation_height() -> Result<(), Report> {
    use crate::parameters::{
        testnet::{self, ConfiguredActivationHeights},
        MAX_POW_AVERAGING_WINDOW, POST_NU7_POW_AVERAGING_WINDOW, PRE_NU7_POW_AVERAGING_WINDOW,
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

    assert_eq!(POST_NU7_POW_AVERAGING_WINDOW, MAX_POW_AVERAGING_WINDOW);

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

/// Checks the ZIP 234 crossing rule on the 75-second schedule.
#[test]
fn zip234_crossing_height() {
    use crate::parameters::subsidy::{zip234_crossing_height, ZIP234_START_HALVING};

    let _init_guard = zakura_test::init();

    assert_eq!(
        zip234_crossing_height(&Network::Mainnet, ZIP234_START_HALVING),
        Some(Height(5_342_746)),
    );
    assert_eq!(
        zip234_crossing_height(&Network::new_default_testnet(), ZIP234_START_HALVING),
        Some(Height(5_412_346)),
    );

    // ZIP 234's own rule, after the second halving, gives its planned Mainnet start in
    // February 2027.
    assert_eq!(
        zip234_crossing_height(&Network::Mainnet, 2),
        Some(Height(3_662_746)),
    );

    // Regtest's short halving interval issues too little for the reserve to fall below
    // the crossing threshold.
    assert_eq!(
        zip234_crossing_height(
            &Network::new_regtest(Default::default()),
            ZIP234_START_HALVING
        ),
        None,
    );
}

/// Returns the default Testnet parameters with NU7 at `nu7`.
fn testnet_with_nu7(nu7: Option<u32>) -> testnet::ParametersBuilder {
    let mut activation_heights: ConfiguredActivationHeights = Network::new_default_testnet()
        .parameters()
        .expect("Testnet has parameters")
        .activation_heights()
        .into();
    activation_heights.nu7 = nu7;

    testnet::Parameters::build()
        .with_activation_heights(activation_heights)
        .expect("activation heights are valid")
}

/// Checks where ZIP 234 reissuance starts relative to NU7 and the crossing height.
#[test]
fn zip234_start_height_follows_nu7_and_the_crossing_rule() {
    use crate::parameters::subsidy::zip234_start_height;

    let _init_guard = zakura_test::init();

    const TESTNET_CROSSING: u32 = 5_412_346;

    // A network without NU7 never starts reissuance.
    assert_eq!(zip234_start_height(&Network::Mainnet), None);
    assert_eq!(zip234_start_height(&Network::new_default_testnet()), None);
    let no_nu7 = testnet_with_nu7(None)
        .to_network()
        .expect("configured testnet is valid");
    assert_eq!(zip234_start_height(&no_nu7), None);

    // NU7 before the crossing height maps the crossing height through the halving clock.
    // Each 75-second block after NU7 is three 25-second blocks.
    for nu7 in [4_200_000, 4_500_000, 5_000_000, TESTNET_CROSSING - 1] {
        let network = testnet_with_nu7(Some(nu7))
            .to_network()
            .expect("configured testnet is valid");
        let expected = nu7 + 3 * (TESTNET_CROSSING - nu7);

        assert_eq!(
            zip234_start_height(&network),
            Some(Height(expected)),
            "NU7 at {nu7}",
        );
    }

    // NU7 at or after the crossing height starts reissuance at NU7.
    for nu7 in [TESTNET_CROSSING, 6_000_000] {
        let network = testnet_with_nu7(Some(nu7))
            .to_network()
            .expect("configured testnet is valid");

        assert_eq!(
            zip234_start_height(&network),
            Some(Height(nu7)),
            "NU7 at {nu7}",
        );
    }

    // A configured start height replaces the crossing height, but not NU7.
    let configured = |nu7, start| {
        testnet_with_nu7(Some(nu7))
            .with_zip234_start_height(Height(start))
            .to_network()
            .expect("configured testnet is valid")
    };
    assert_eq!(
        zip234_start_height(&configured(4_200_000, 4_200_010)),
        Some(Height(4_200_010)),
    );
    assert_eq!(
        zip234_start_height(&configured(4_200_000, 1)),
        Some(Height(4_200_000)),
    );

    let regtest = |zip234_start_height| {
        Network::new_regtest(testnet::RegtestParameters {
            activation_heights: ConfiguredActivationHeights {
                nu7: Some(10),
                ..Default::default()
            },
            zip234_start_height,
            ..Default::default()
        })
    };
    assert_eq!(zip234_start_height(&regtest(None)), None);
    assert_eq!(
        zip234_start_height(&regtest(Some(Height(20)))),
        Some(Height(20)),
    );
}

/// Checks the closed-form cumulative halving subsidy against a per-height sum across
/// halvings and the NU7 spacing change.
#[test]
fn expected_issued_supply_matches_per_height_sum() {
    use crate::parameters::subsidy::expected_issued_supply;

    let _init_guard = zakura_test::init();

    let nu7 = Height(5_000);
    let network = testnet::Parameters::build()
        .with_activation_heights(ConfiguredActivationHeights {
            blossom: Some(1),
            canopy: Some(2),
            nu7: Some(nu7.0),
            ..Default::default()
        })
        .expect("activation heights are valid")
        .with_slow_start_interval(Height(100))
        .with_halving_interval(1_000)
        .expect("halving interval is valid")
        .clear_funding_streams()
        .to_network()
        .expect("configured testnet is valid");
    let last_height = Height(25_000);

    assert!(
        halving(nu7, &network) > 1 && halving(last_height, &network) > halving(nu7, &network) + 1,
        "the range must cross halvings on both sides of NU7",
    );

    // `expected_issued_supply` walks halving and spacing boundaries rather than
    // every height, so check it against the sum it is standing in for.
    let mut brute_force = Amount::<NonNegative>::zero();
    for height in 1..=last_height.0 {
        brute_force = (brute_force
            + halving_block_subsidy(Height(height), &network).expect("valid subsidy"))
        .expect("sum is in range");

        assert_eq!(
            expected_issued_supply(Height(height), &network).expect("valid cumulative subsidy"),
            brute_force,
            "cumulative subsidies must match the per-height sum at height {height}",
        );
    }
}

/// Checks the ZIP 234 reissuance bonus.
#[test]
fn zip234_issuance() {
    use crate::value_balance::ValueBalance;

    let _init_guard = zakura_test::init();

    let start = Height(1_000_000);
    let network = testnet::Parameters::build()
        .with_activation_heights(ConfiguredActivationHeights {
            blossom: Some(1),
            canopy: Some(2),
            nu7: Some(start.0),
            ..Default::default()
        })
        .expect("activation heights are valid")
        .clear_funding_streams()
        .with_zip234_start_height(start)
        .to_network()
        .expect("configured testnet is valid");

    if !cfg!(feature = "nu7") {
        // Without ZIP 234, the subsidy stays on the halving schedule.
        let balance = Amount::<NonNegative>::try_from(1_000_000_000_000i64).expect("valid amount");
        assert_eq!(
            block_subsidy(start, &network, Some(balance)).expect("valid subsidy"),
            block_subsidy(start, &network, None).expect("valid subsidy"),
        );
        return;
    }

    // ZIP 234 needs the NSM value balance at its start height.
    assert_eq!(
        block_subsidy(start, &network, None),
        Err(SubsidyError::MissingNsmValueBalance),
    );
    assert!(block_subsidy(
        start.previous().expect("start is above genesis"),
        &network,
        None
    )
    .is_ok());

    let halving_subsidy = halving_block_subsidy(start, &network).expect("valid subsidy");

    // A chain on schedule has nothing to reissue.
    assert_eq!(
        block_subsidy(start, &network, Some(Amount::zero())).expect("valid subsidy"),
        halving_subsidy,
    );

    // A balance of 10^12 zatoshi reissues `ceil(10^12 * 4126 / 10^10)` zatoshi, at 25-second
    // blocks as at 75-second blocks.
    let behind = Amount::<NonNegative>::try_from(1_000_000_000_000i64).expect("valid amount");
    assert_eq!(
        block_subsidy(start, &network, Some(behind)).expect("valid subsidy"),
        (halving_subsidy + Amount::try_from(412_600).expect("valid amount")).expect("valid amount"),
    );

    // A balance of one zatoshi rounds up to a one-zatoshi bonus.
    let one_behind = Amount::<NonNegative>::try_from(1).expect("valid amount");
    assert_eq!(
        block_subsidy(start, &network, Some(one_behind)).expect("valid subsidy"),
        (halving_subsidy + Amount::try_from(1).expect("valid amount")).expect("valid amount"),
    );

    // A chain ahead of its schedule has a negative balance, which the amount type cannot
    // represent. `Chain::push` and the finalized commit path reject such a block before it
    // reaches this function; see `nsm_value_balance_is_non_negative`.

    // The money reserve is what has never been issued plus everything removed from
    // circulation.
    assert_eq!(
        ValueBalance::<NonNegative>::zero().money_reserve(),
        Amount::<NonNegative>::try_from(MAX_MONEY).expect("valid amount"),
    );
}

/// Tests that slow-start subsidies stay unscaled when NU7 activates during slow
/// start.
///
/// ZIP 218 adds its NU7 subsidy case after the existing slow-start cases, just as
/// the Blossom case follows them, so slow start takes precedence.
#[test]
fn slow_start_subsidy_is_not_scaled_when_nu7_activates_early() -> Result<(), Report> {
    use crate::parameters::{
        subsidy::constants::MAX_BLOCK_SUBSIDY,
        testnet::{self, ConfiguredActivationHeights},
    };

    let _init_guard = zakura_test::init();

    let network = testnet::Parameters::build()
        .with_activation_heights(ConfiguredActivationHeights {
            blossom: Some(1),
            nu7: Some(2),
            ..Default::default()
        })
        .expect("activation heights are valid")
        .clear_funding_streams()
        .to_network()
        .expect("configured testnet is valid");

    let slow_start_interval = network.slow_start_interval();
    assert!(slow_start_interval > Height(3));
    assert_eq!(
        NetworkUpgrade::current(&network, Height(2)),
        NetworkUpgrade::Nu7
    );

    // floor(MaxBlockSubsidy / SlowStartInterval) * height, with no spacing ratio.
    let slow_start_rate = MAX_BLOCK_SUBSIDY / u64::from(slow_start_interval);
    assert_eq!(
        Amount::<NonNegative>::try_from(slow_start_rate * 2)?,
        block_subsidy(Height(2), &network, None)?,
    );

    Ok(())
}
