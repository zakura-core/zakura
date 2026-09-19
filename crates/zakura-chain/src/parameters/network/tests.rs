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
        testnet::{self, ConfiguredActivationHeights},
        NetworkUpgrade,
    },
};

#[test]
fn funding_stream_period_uses_floor_division_for_negative_periods() {
    struct TestParameters;

    impl ParameterSubsidy for TestParameters {
        fn initial_nsm_value_balance(&self) -> Amount<NonNegative> {
            Amount::zero()
        }

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

/// Regtest derives its first halving, so NU7's 25 second spacing moves it.
#[test]
fn regtest_first_halving_follows_the_target_spacing() {
    let _init_guard = zakura_test::init();

    let regtest = Network::new_regtest(Default::default());
    assert_eq!(regtest.height_for_first_halving(), Height(287));

    let nu7_regtest = Network::new_regtest(
        ConfiguredActivationHeights {
            nu7: Some(1),
            ..Default::default()
        }
        .into(),
    );
    let first_halving = nu7_regtest.height_for_first_halving();
    assert_eq!(first_halving, Height(859));
    assert_eq!(
        Some(first_halving),
        height_for_halving(1, &nu7_regtest),
        "the first halving matches the subsidy schedule"
    );

    // Funding stream recipients rotate on period boundaries aligned to the
    // derived first halving, which starts period 48.
    let period = |height: Height| funding_stream_address_period(height, &nu7_regtest);
    let interval = nu7_regtest.funding_stream_address_change_interval();
    assert_eq!(period(first_halving), 48);
    assert_eq!(
        period((first_halving - 1).expect("the test height is valid")),
        47
    );
    assert_eq!(
        period((first_halving + interval).expect("the test height is valid")),
        49
    );
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

/// Tests that `is_nu7_active` is true exactly from the NU7 activation height, and
/// never on networks without one.
#[test]
fn is_nu7_active_from_the_nu7_activation_height() -> Result<(), Report> {
    use crate::parameters::testnet::{self, ConfiguredActivationHeights};

    let _init_guard = zakura_test::init();

    let without_nu7 = testnet::Parameters::build()
        .with_activation_heights(ConfiguredActivationHeights {
            blossom: Some(1),
            nu6: Some(10),
            ..Default::default()
        })
        .expect("activation heights are valid")
        .clear_funding_streams()
        .to_network()
        .expect("configured testnet is valid");

    let with_nu7 = testnet::Parameters::build()
        .with_activation_heights(ConfiguredActivationHeights {
            blossom: Some(1),
            nu7: Some(10),
            ..Default::default()
        })
        .expect("activation heights are valid")
        .clear_funding_streams()
        .to_network()
        .expect("configured testnet is valid");

    for (name, network) in [
        ("Mainnet", Network::Mainnet),
        ("without NU7", without_nu7),
        ("with NU7", with_nu7),
    ] {
        let nu7_height = NetworkUpgrade::Nu7.activation_height(&network);

        for height in (0..20).map(Height) {
            assert_eq!(
                nu7_height.is_some_and(|nu7_height| height >= nu7_height),
                NetworkUpgrade::is_nu7_active(&network, height),
                "{name} at {height:?}"
            );
        }
    }

    Ok(())
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
        block_subsidy(Height(2), &network)?,
    );

    Ok(())
}

/// Compare the generalized schedule with the previous two-era consensus formula.
#[test]
fn spacing_schedule_preserves_existing_halving_and_subsidy() {
    use crate::parameters::{subsidy::constants::MAX_BLOCK_SUBSIDY, testnet};
    let _init_guard = zakura_test::init();
    let mut networks: Vec<_> = Network::iter().collect();
    for blossom in [4, 1_000] {
        networks.push(
            testnet::Parameters::build()
                .with_slow_start_interval(Height(20))
                .with_halving_interval(100)
                .unwrap()
                .with_activation_heights(testnet::ConfiguredActivationHeights {
                    blossom: Some(blossom),
                    canopy: Some(blossom + 2),
                    ..Default::default()
                })
                .unwrap()
                .clear_funding_streams()
                .to_network()
                .unwrap(),
        );
    }
    for network in networks {
        let blossom = NetworkUpgrade::Blossom.activation_height(&network).unwrap();
        let shift = network.slow_start_shift();
        let mut heights = vec![
            Height(0),
            shift,
            (shift + 1).unwrap(),
            blossom.previous().unwrap(),
            blossom,
            blossom.next().unwrap(),
            Height::MAX,
        ];
        for index in 1..=8 {
            let height = height_for_halving(index, &network).unwrap();
            heights.extend([height.previous().unwrap(), height, height.next().unwrap()]);
            assert_eq!(halving(height, &network), index);
            assert_eq!(halving(height.previous().unwrap(), &network), index - 1);
        }
        for height in heights {
            let old_halving = previous_halving(height, &network);
            assert_eq!(
                halving(height, &network),
                old_halving,
                "{network:?} at {height:?}"
            );
            if height >= network.slow_start_interval() {
                let ratio = if height < blossom { 1 } else { 2 };
                let expected = 1u64
                    .checked_shl(old_halving)
                    .map_or(0, |divisor| MAX_BLOCK_SUBSIDY / ratio / divisor);
                assert_eq!(
                    block_subsidy(height, &network).unwrap(),
                    Amount::<NonNegative>::try_from(expected).unwrap()
                );
            }
        }
    }
}

/// Checks the closed-form cumulative halving subsidy against a per-height sum across
/// halvings and the NU7 spacing change.
#[test]
fn expected_issued_supply_matches_per_height_sum() {
    use crate::parameters::{
        subsidy::{expected_issued_supply, halving_block_subsidy},
        testnet::{self, ConfiguredActivationHeights},
    };

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

#[test]
fn scheduled_issuance_boundary_differences_match_block_subsidy() {
    use crate::parameters::subsidy::{halving_block_subsidy, scheduled_issuance_zatoshis};
    for network in [
        Network::Mainnet,
        Network::new_default_testnet(),
        Network::new_regtest(Default::default()),
    ] {
        assert_eq!(scheduled_issuance_zatoshis(Height(0), &network).unwrap(), 0);
        let mut boundaries = vec![
            Height(1),
            network.slow_start_shift(),
            network.slow_start_interval(),
            Height(Height::MAX_AS_U32),
        ];
        boundaries.extend(NetworkUpgrade::target_spacings(&network).map(|(height, _)| height));
        boundaries.extend((0..35).filter_map(|n| height_for_halving(n, &network)));
        for boundary in boundaries {
            for h in [
                boundary.0.saturating_sub(1),
                boundary.0,
                boundary.0.saturating_add(1),
            ] {
                if h == 0 || h > Height::MAX_AS_U32 {
                    continue;
                }
                let previous = scheduled_issuance_zatoshis(Height(h - 1), &network).unwrap();
                let current = scheduled_issuance_zatoshis(Height(h), &network).unwrap();
                let block = u128::try_from(i64::from(
                    halving_block_subsidy(Height(h), &network).unwrap(),
                ))
                .unwrap();
                assert_eq!(current - previous, block, "network {network:?}, height {h}");
                assert_eq!(
                    block_subsidy(Height(h), &network).unwrap(),
                    halving_block_subsidy(Height(h), &network).unwrap()
                );
            }
        }
    }
}

/// The inverse must not add the slow-start shift twice for a pre-Blossom halving.
#[test]
fn pre_blossom_halving_inverse_counts_slow_start_once() {
    use crate::parameters::testnet;
    let _init_guard = zakura_test::init();
    let network = testnet::Parameters::build()
        .with_slow_start_interval(Height(20))
        .with_halving_interval(100)
        .unwrap()
        .with_activation_heights(testnet::ConfiguredActivationHeights {
            blossom: Some(1_000),
            canopy: Some(1_002),
            ..Default::default()
        })
        .unwrap()
        .clear_funding_streams()
        .to_network()
        .unwrap();
    assert_eq!(height_for_halving(1, &network), Some(Height(110)));
    assert_eq!(halving(Height(109), &network), 0);
    assert_eq!(halving(Height(110), &network), 1);
}

fn previous_halving(height: Height, network: &Network) -> u32 {
    let slow_start_shift = network.slow_start_shift();
    let blossom_height = NetworkUpgrade::Blossom
        .activation_height(network)
        .expect("blossom activation height should be available");

    let halving_index = if height < slow_start_shift {
        0
    } else if height < blossom_height {
        let pre_blossom_height = height - slow_start_shift;
        pre_blossom_height / network.pre_blossom_halving_interval()
    } else {
        let pre_blossom_height = blossom_height - slow_start_shift;
        let scaled_pre_blossom_height = pre_blossom_height
            * HeightDiff::from(
                crate::parameters::subsidy::constants::BLOSSOM_POW_TARGET_SPACING_RATIO,
            );

        let post_blossom_height = height - blossom_height;

        (scaled_pre_blossom_height + post_blossom_height) / network.post_blossom_halving_interval()
    };

    halving_index
        .try_into()
        .expect("already checked for negatives")
}

/// Reject intervals that would overflow the halving denominator or make it non-positive.
#[test]
fn halving_interval_must_be_positive_and_representable_in_seconds() {
    use super::{error::ParametersBuilderError, testnet};

    let spacing = NetworkUpgrade::Genesis.target_spacing().num_seconds();
    let max_interval = HeightDiff::MAX / spacing;
    for interval in [
        HeightDiff::MIN,
        -1,
        0,
        max_interval + 1,
        122_978_293_824_730_345,
        HeightDiff::MAX,
    ] {
        assert!(
            matches!(
                testnet::Parameters::build().with_halving_interval(interval),
                Err(ParametersBuilderError::InvalidHalvingInterval)
            ),
            "invalid interval {interval} must be rejected"
        );
    }

    for interval in [1, 100, max_interval] {
        let network = testnet::Parameters::build()
            .with_slow_start_interval(Height(0))
            .with_halving_interval(interval)
            .expect("positive interval fits in target seconds")
            .with_activation_heights(testnet::ConfiguredActivationHeights {
                blossom: Some(1),
                canopy: Some(1),
                ..Default::default()
            })
            .unwrap()
            .clear_funding_streams()
            .to_network()
            .unwrap();
        assert_eq!(network.pre_blossom_halving_interval(), interval);
        assert_eq!(network.post_blossom_halving_interval(), interval * 2);
        assert_eq!(
            halving(Height(2), &network),
            if interval == 1 { 1 } else { 0 }
        );
        if interval == max_interval {
            assert_eq!(halving(Height::MAX, &network), 0);
        }
    }
}

/// A halving interval of one overflows the halving divisor inside the default slow start.
/// Cumulative issuance must stop there, like the per-block subsidy.
#[test]
fn scheduled_issuance_stops_at_a_halving_overflow_inside_slow_start() {
    use crate::parameters::{
        subsidy::{halving_block_subsidy, halving_divisor, scheduled_issuance_zatoshis},
        testnet,
    };
    let network = testnet::Parameters::build()
        .with_halving_interval(1)
        .expect("the halving interval is valid")
        .clear_funding_streams()
        .to_network()
        .expect("the configured testnet is valid");
    let slow_start_end = network.slow_start_interval().0;
    let cutoff = (1..slow_start_end)
        .find(|h| halving_divisor(Height(*h), &network).is_none())
        .expect("the halving divisor overflows inside slow start");

    let mut direct = 0u128;
    for h in 1..=slow_start_end + 2 {
        let block = u128::try_from(i64::from(
            halving_block_subsidy(Height(h), &network).expect("valid subsidy"),
        ))
        .expect("subsidies are non-negative");
        direct += block;
        if h.abs_diff(cutoff) <= 2 || h + 2 >= slow_start_end {
            assert_eq!(
                scheduled_issuance_zatoshis(Height(h), &network).expect("valid issuance"),
                direct,
                "height {h}, cutoff {cutoff}"
            );
        }
    }
    assert_eq!(
        halving_block_subsidy(Height(cutoff), &network).expect("valid subsidy"),
        Amount::<NonNegative>::zero()
    );
}

/// The ZIP 234 fraction tracks the halving interval, so ZIP 218's 25-second blocks do not
/// pay the balance out three times as fast.
#[test]
fn block_subsidy_fraction_tracks_the_halving_interval() {
    use crate::parameters::subsidy::{block_subsidy_fraction_numerator, halving_interval};

    let _init_guard = zakura_test::init();

    let nu7 = Height(4_000_000);
    let network = testnet::Parameters::build()
        .with_activation_heights(ConfiguredActivationHeights {
            blossom: Some(1_000_000),
            canopy: Some(1_000_001),
            nu7: Some(nu7.0),
            ..Default::default()
        })
        .expect("activation heights are valid")
        .clear_funding_streams()
        .to_network()
        .expect("configured testnet is valid");

    // 150-second blocks before Blossom, 75 after it, 25 from NU7. The halving period stays
    // at about four years of wall-clock time, so the interval scales with the spacing.
    for (height, interval, numerator) in [
        (Height(999_999), 840_000, 8_252),
        (Height(1_000_000), 1_680_000, 4_126),
        (Height(nu7.0 - 1), 1_680_000, 4_126),
        (nu7, 5_040_000, 1_375),
        (Height(nu7.0 + 1), 5_040_000, 1_375),
    ] {
        assert_eq!(
            halving_interval(height, &network),
            interval,
            "halving interval at {height:?}",
        );
        assert_eq!(
            block_subsidy_fraction_numerator(height, &network),
            numerator,
            "fraction numerator at {height:?}",
        );
    }
}

/// Applying the fraction once per block over a halving interval halves the balance.
#[test]
fn block_subsidy_fraction_halves_the_balance_over_one_interval() {
    use crate::parameters::subsidy::{
        block_subsidy_fraction_numerator, halving_interval, BLOCK_SUBSIDY_FRACTION_DENOMINATOR,
    };

    let _init_guard = zakura_test::init();

    let nu7 = Height(4_000_000);
    let network = testnet::Parameters::build()
        .with_activation_heights(ConfiguredActivationHeights {
            blossom: Some(1),
            canopy: Some(2),
            nu7: Some(nu7.0),
            ..Default::default()
        })
        .expect("activation heights are valid")
        .clear_funding_streams()
        .to_network()
        .expect("configured testnet is valid");

    for height in [Height(nu7.0 - 1), nu7] {
        let numerator = block_subsidy_fraction_numerator(height, &network);
        let interval = u32::try_from(halving_interval(height, &network)).expect("a valid interval");

        // `(1 - f)^interval` in fixed point, one block at a time. Rounding each step down
        // only makes the remainder smaller, so the tolerance is one-sided in practice.
        let mut remaining = BLOCK_SUBSIDY_FRACTION_DENOMINATOR;
        for _ in 0..interval {
            remaining -= remaining * numerator / BLOCK_SUBSIDY_FRACTION_DENOMINATOR;
        }

        let half = BLOCK_SUBSIDY_FRACTION_DENOMINATOR / 2;
        let error = remaining.abs_diff(half);
        assert!(
            error * 1_000 < half,
            "one halving interval at {height:?} must leave about half, left {remaining} of \
             {BLOCK_SUBSIDY_FRACTION_DENOMINATOR}",
        );
    }
}

/// A positive balance always pays at least one zatoshi, so the balance reaches zero in a
/// finite number of blocks.
#[test]
fn reissuance_drains_a_small_balance() {
    use crate::parameters::subsidy::reissuance_bonus;

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
        .to_network()
        .expect("configured testnet is valid");

    let initial = 20i64;
    let mut balance = initial;

    for block in 0..initial {
        let bonus = i64::from(
            reissuance_bonus(
                Amount::try_from(balance).expect("valid amount"),
                start,
                &network,
            )
            .expect("valid bonus"),
        );

        assert!(
            bonus >= 1,
            "a balance of {balance} must pay at least one zatoshi, block {block}",
        );
        balance -= bonus;
    }

    assert_eq!(
        balance, 0,
        "the balance must drain within its own value in blocks"
    );
}

proptest::proptest! {
    #[test]
    fn reissuance_arithmetic_matches_integer_oracle(balance in 0i64..=crate::amount::MAX_MONEY) {
        use crate::parameters::subsidy::{reissuance_bonus, block_subsidy_fraction_numerator, BLOCK_SUBSIDY_FRACTION_DENOMINATOR};
        let network = Network::new_default_testnet();
        let height = Height(3_000_000);
        let numerator = i128::try_from(block_subsidy_fraction_numerator(height, &network)).unwrap();
        let denominator = i128::try_from(BLOCK_SUBSIDY_FRACTION_DENOMINATOR).unwrap();
        let expected = (i128::from(balance) * numerator + denominator - 1) / denominator;
        let actual = reissuance_bonus(Amount::try_from(balance).unwrap(), height, &network).unwrap();
        proptest::prop_assert_eq!(i128::from(i64::from(actual)), expected);
    }
}
