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

/// Missing NU7 or a schedule with no crossing leaves reissuance inactive.
#[test]
fn nsm_reissuance_requires_nu7_and_a_crossing() {
    use crate::parameters::subsidy::{is_zip234_active, nsm_reissuance_height};

    let _init_guard = zakura_test::init();
    let configured = testnet_with_nu7(None)
        .to_network()
        .expect("configured testnet is valid");
    let regtest = Network::new_regtest(testnet::RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu7: Some(10),
            ..Default::default()
        },
        ..Default::default()
    });

    for network in [
        Network::Mainnet,
        Network::new_default_testnet(),
        configured,
        regtest,
    ] {
        assert_eq!(nsm_reissuance_height(&network), None, "{network:?}");
        assert!(!is_zip234_active(&network, Height::MAX), "{network:?}");
    }
}

/// Artificial test fixtures still respect NU7 gating.
#[test]
fn test_nsm_reissuance_fixture_respects_nu7() {
    use crate::parameters::subsidy::{is_zip234_active, nsm_reissuance_height};

    let _init_guard = zakura_test::init();
    let no_nu7 = testnet_with_nu7(None)
        .with_test_nsm_reissuance_height(Height(4_200_010))
        .to_network()
        .expect("configured testnet is valid");
    assert_eq!(nsm_reissuance_height(&no_nu7), None);
    assert!(!is_zip234_active(&no_nu7, Height::MAX));

    for (start, expected) in [
        (1, 4_200_000),
        (4_200_000, 4_200_000),
        (4_200_010, 4_200_010),
        (12_000_000, 12_000_000),
    ] {
        let network = testnet_with_nu7(Some(4_200_000))
            .with_test_nsm_reissuance_height(Height(start))
            .to_network()
            .expect("configured testnet is valid");

        assert_eq!(nsm_reissuance_height(&network), Some(Height(expected)));
        assert!(!is_zip234_active(&network, Height(expected - 1)));
        assert!(is_zip234_active(&network, Height(expected)));
        assert!(is_zip234_active(&network, Height(expected + 1)));
    }

    for (start, expected) in [(1, 10), (10, 10), (20, 20)] {
        let network = Network::new_regtest(testnet::RegtestParameters {
            activation_heights: ConfiguredActivationHeights {
                nu7: Some(10),
                ..Default::default()
            },
            test_nsm_reissuance_height: Some(Height(start)),
            ..Default::default()
        });

        assert_eq!(nsm_reissuance_height(&network), Some(Height(expected)));
        assert!(!is_zip234_active(&network, Height(expected - 1)));
        assert!(is_zip234_active(&network, Height(expected)));
        assert!(is_zip234_active(&network, Height(expected + 1)));
    }
}

/// Derived activation reaches the subsidy path, without requiring an explicit height.
#[test]
fn derived_nsm_crossing_activates_block_subsidy() {
    use crate::parameters::subsidy::{
        block_subsidy, halving_block_subsidy, is_zip234_active, nsm_reissuance_crossing_height,
        nsm_reissuance_height, SubsidyError,
    };

    let _init_guard = zakura_test::init();
    for nu7 in [4_200_000, 4_400_000, 4_600_000] {
        let network = testnet_with_nu7(Some(nu7))
            .to_network()
            .expect("configured testnet is valid");
        let equal_network = testnet_with_nu7(Some(nu7))
            .to_network()
            .expect("configured testnet is valid");
        let crossing = nsm_reissuance_crossing_height(&network)
            .expect("valid schedule arithmetic")
            .expect("this schedule has a crossing");
        assert_eq!(nsm_reissuance_height(&network), Some(crossing));
        assert_eq!(
            network, equal_network,
            "caching must not change network equality"
        );
        assert_eq!(nsm_reissuance_height(&network.clone()), Some(crossing));

        let previous = crossing.previous().expect("crossings have a parent");
        assert!(!is_zip234_active(&network, previous));
        assert_eq!(
            block_subsidy(previous, &network, None),
            halving_block_subsidy(previous, &network)
        );
        for height in [crossing, Height(crossing.0 + 1)] {
            assert!(is_zip234_active(&network, height));
            assert_eq!(
                block_subsidy(height, &network, None),
                Err(SubsidyError::MissingNsmValueBalance)
            );
            let scheduled = halving_block_subsidy(height, &network).expect("valid subsidy");
            let balance = Amount::try_from(1).expect("one zatoshi is valid");
            assert_eq!(
                block_subsidy(height, &network, Some(balance)),
                Ok((scheduled + balance).expect("subsidy plus one fits"))
            );
        }
    }
}

/// NU7 inside the third era can cross immediately; its fourth-halving boundary is excluded.
#[test]
fn nsm_crossing_respects_network_search_boundaries() {
    use crate::parameters::subsidy::{nsm_reissuance_crossing_height, nsm_reissuance_height};

    let _init_guard = zakura_test::init();
    let baseline = testnet_with_nu7(None).to_network().expect("valid testnet");
    let third = height_for_halving(3, &baseline).expect("third halving exists");
    let fourth = height_for_halving(4, &baseline).expect("fourth halving exists");
    for nu7 in [third.0, third.0 + 1, fourth.0 - 1, fourth.0, fourth.0 + 1] {
        let network = testnet_with_nu7(Some(nu7))
            .to_network()
            .expect("valid testnet");
        let calculated = nsm_reissuance_crossing_height(&network).expect("valid arithmetic");
        assert_eq!(nsm_reissuance_height(&network), calculated);
        if nu7 >= fourth.0 {
            assert_eq!(
                calculated, None,
                "NU7 at or beyond the fourth halving has no third-era crossing"
            );
        } else {
            let crossing = calculated.expect("this third era contains a crossing");
            assert!(crossing > third);
            assert!(crossing >= Height(nu7));
            assert!(crossing < height_for_halving(4, &network).expect("fourth halving exists"));
            if nu7 == fourth.0 - 1 {
                assert_eq!(
                    crossing,
                    Height(nu7),
                    "a late NU7 starts at the first candidate"
                );
            }
        }
    }
}

/// A run includes its final height but must not accept the following block.
#[test]
fn nsm_crossing_includes_only_the_permitted_run() {
    use crate::parameters::subsidy::{
        first_nsm_crossing_in_subsidy_run, BLOCK_SUBSIDY_FRACTION_DENOMINATOR,
        BLOCK_SUBSIDY_FRACTION_NUMERATOR,
    };

    let subsidy = 26_041_666;
    let threshold =
        (subsidy - 1) * BLOCK_SUBSIDY_FRACTION_DENOMINATOR / BLOCK_SUBSIDY_FRACTION_NUMERATOR;
    for (reserve, expected) in [
        (threshold - 1, Some(10)),
        (threshold, Some(10)),
        (threshold + 1, Some(11)),
        (threshold + 2 * subsidy, Some(12)),
        (threshold + 2 * subsidy + 1, None),
    ] {
        assert_eq!(
            first_nsm_crossing_in_subsidy_run(10, 12, 0, subsidy, reserve),
            Ok(expected)
        );
    }
    assert_eq!(
        first_nsm_crossing_in_subsidy_run(10, 9, 0, subsidy, threshold),
        Ok(None)
    );
    assert_eq!(first_nsm_crossing_in_subsidy_run(10, 12, 0, 0, 0), Ok(None));
}

proptest::proptest! {
    /// Check the full network wrapper over NU7 dates before, within, and after the third era.
    #[test]
    fn derived_nsm_crossing_matches_network_inequality(nu7 in 4_200_000u32..8_000_000) {
        use crate::{amount::MAX_MONEY, parameters::subsidy::{
            halving_block_subsidy, nsm_reissuance_height, scheduled_issuance_zatoshis,
            BLOCK_SUBSIDY_FRACTION_DENOMINATOR, BLOCK_SUBSIDY_FRACTION_NUMERATOR,
        }};

        let network = testnet_with_nu7(Some(nu7)).to_network().expect("valid testnet");
        let third = height_for_halving(3, &network).expect("third halving exists");
        let fourth = height_for_halving(4, &network).expect("fourth halving exists");
        let first = Height((third.0 + 1).max(nu7));
        let qualifies = |height: Height| {
            let supply = scheduled_issuance_zatoshis(height.previous().expect("positive height"), &network).expect("valid supply");
            let reserve = u128::try_from(MAX_MONEY).expect("positive limit").saturating_sub(supply);
            let reference = (reserve * BLOCK_SUBSIDY_FRACTION_NUMERATOR).div_ceil(BLOCK_SUBSIDY_FRACTION_DENOMINATOR);
            let scheduled = u128::try_from(i64::from(halving_block_subsidy(height, &network).expect("valid subsidy"))).expect("non-negative subsidy");
            reference < scheduled
        };
        match nsm_reissuance_height(&network) {
            Some(crossing) => {
                proptest::prop_assert!(crossing >= first && crossing < fourth);
                proptest::prop_assert!(qualifies(crossing));
                if crossing > first {
                    proptest::prop_assert!(!qualifies(crossing.previous().expect("positive height")));
                }
            }
            None => {
                // Within a constant-subsidy era, the reference subsidy only decreases.
                proptest::prop_assert!(first >= fourth || !qualifies(fourth.previous().expect("positive height")));
            }
        }
    }
}

/// The direct crossing uses the spacing-aware halving and scheduled-supply rules.
#[test]
fn nsm_reissuance_crossing_uses_the_post_nu7_schedule() {
    use crate::{
        amount::MAX_MONEY,
        parameters::subsidy::{
            halving_block_subsidy, nsm_reissuance_crossing_height, scheduled_issuance_zatoshis,
            BLOCK_SUBSIDY_FRACTION_DENOMINATOR, BLOCK_SUBSIDY_FRACTION_NUMERATOR,
        },
    };

    let _init_guard = zakura_test::init();
    let reference_subsidy = |height: Height, network: &Network| {
        let parent = height
            .previous()
            .expect("crossings after the third halving have parents");
        let supply =
            scheduled_issuance_zatoshis(parent, network).expect("scheduled supply is valid");
        let reserve = u128::try_from(MAX_MONEY)
            .expect("MAX_MONEY is positive")
            .saturating_sub(supply);

        reserve
            .checked_mul(BLOCK_SUBSIDY_FRACTION_NUMERATOR)
            .expect("the reference subsidy product fits in u128")
            .div_ceil(BLOCK_SUBSIDY_FRACTION_DENOMINATOR)
    };

    let mut boundaries = Vec::new();
    for nu7 in [4_200_000, 4_400_000, 4_600_000] {
        let network = testnet_with_nu7(Some(nu7))
            .to_network()
            .expect("configured testnet is valid");
        let crossing = nsm_reissuance_crossing_height(&network)
            .expect("crossing arithmetic is valid")
            .expect("the default Testnet schedule has a crossing");
        let previous = crossing.previous().expect("the crossing is above genesis");
        let scheduled = |height| {
            u128::try_from(i64::from(
                halving_block_subsidy(height, &network).expect("scheduled subsidy is valid"),
            ))
            .expect("scheduled subsidies are non-negative")
        };

        assert_eq!(halving(crossing, &network), 3);
        assert!(
            reference_subsidy(previous, &network) >= scheduled(previous),
            "the height before the crossing must not satisfy the strict inequality"
        );
        assert!(
            reference_subsidy(crossing, &network) < scheduled(crossing),
            "the crossing must satisfy the strict inequality"
        );

        boundaries.push((
            height_for_halving(3, &network).expect("the third halving has a height"),
            crossing,
        ));
    }

    for pair in boundaries.windows(2) {
        assert!(
            pair[0].0 > pair[1].0,
            "a later NU7 height leaves fewer 25-second blocks before the third halving"
        );
        assert!(
            pair[0].1 > pair[1].1,
            "a later NU7 height leaves fewer 25-second blocks before the crossing"
        );
    }
}

/// The proposed Mainnet schedule places the direct crossing in February 2031.
///
/// The NU7 height and timestamp are the estimates from the 2026-09-17 NU7 timeline,
/// not consensus constants. This test checks the calendar interpretation of the
/// spacing-aware equation without activating reissuance.
#[test]
fn estimated_mainnet_nsm_crossing_lands_in_february_2031() {
    use chrono::{Datelike, Duration, TimeZone, Utc};

    use crate::parameters::{
        constants::activation_heights::mainnet, subsidy::nsm_reissuance_crossing_height,
    };

    const ESTIMATED_NU7_HEIGHT: u32 = 3_543_000;

    let _init_guard = zakura_test::init();
    let network = testnet::Parameters::build()
        .with_activation_heights(ConfiguredActivationHeights {
            before_overwinter: Some(mainnet::BEFORE_OVERWINTER.0),
            overwinter: Some(mainnet::OVERWINTER.0),
            sapling: Some(mainnet::SAPLING.0),
            blossom: Some(mainnet::BLOSSOM.0),
            heartwood: Some(mainnet::HEARTWOOD.0),
            canopy: Some(mainnet::CANOPY.0),
            nu5: Some(mainnet::NU5.0),
            nu6: Some(mainnet::NU6.0),
            nu6_1: Some(mainnet::NU6_1.0),
            nu6_2: Some(mainnet::NU6_2.0),
            nu6_3: Some(mainnet::NU6_3.0),
            nu7: Some(ESTIMATED_NU7_HEIGHT),
        })
        .expect("the estimated Mainnet activation schedule is ordered")
        .clear_funding_streams()
        .to_network()
        .expect("the Mainnet subsidy schedule is valid");

    assert_eq!(
        height_for_halving(2, &network),
        Some(mainnet::NU6),
        "the configured network reproduces Mainnet's second halving"
    );

    let crossing = nsm_reissuance_crossing_height(&network)
        .expect("crossing arithmetic is valid")
        .expect("the estimated Mainnet schedule has a crossing");
    assert_eq!(crossing, Height(8_940_474));
    assert_eq!(
        crate::parameters::subsidy::nsm_reissuance_height(&network),
        Some(crossing)
    );

    let estimated_nu7_time = Utc
        .with_ymd_and_hms(2026, 11, 5, 12, 0, 0)
        .single()
        .expect("the estimated NU7 timestamp is valid");
    let post_nu7_blocks = i64::from(crossing.0 - ESTIMATED_NU7_HEIGHT);
    let crossing_time = estimated_nu7_time
        + Duration::seconds(post_nu7_blocks * NetworkUpgrade::Nu7.target_spacing().num_seconds());

    assert_eq!(
        (crossing_time.year(), crossing_time.month()),
        (2031, 2),
        "estimated crossing time: {crossing_time}"
    );
}

/// A third halving era can extend to the maximum supported height without a fourth halving.
#[test]
fn nsm_reissuance_crossing_without_a_fourth_halving() {
    use crate::parameters::subsidy::nsm_reissuance_crossing_height;

    let _init_guard = zakura_test::init();
    for (interval, expected_third, expected_crossing) in [
        (
            100_000_000,
            Some(Height(1_799_999_995)),
            Some(Height(1_799_999_996)),
        ),
        (200_000_000, None, None),
    ] {
        let network = testnet::Parameters::build()
            .with_slow_start_interval(Height(0))
            .with_halving_interval(interval)
            .expect("the positive halving interval fits in target seconds")
            .with_activation_heights(ConfiguredActivationHeights {
                blossom: Some(1),
                canopy: Some(1),
                nu7: Some(1),
                ..Default::default()
            })
            .expect("the simultaneous activation heights are ordered")
            .clear_funding_streams()
            .to_network()
            .expect("the custom schedule is valid");

        assert_eq!(height_for_halving(3, &network), expected_third);
        assert_eq!(height_for_halving(4, &network), None);
        assert_eq!(
            nsm_reissuance_crossing_height(&network).expect("crossing arithmetic is valid"),
            expected_crossing,
            "a missing fourth halving must not suppress a crossing within the supported heights"
        );
    }
}

/// Networks without NU7 or without enough scheduled issuance have no direct crossing.
#[test]
fn nsm_reissuance_crossing_can_be_absent() {
    use crate::parameters::subsidy::nsm_reissuance_crossing_height;

    let _init_guard = zakura_test::init();
    assert_eq!(
        nsm_reissuance_crossing_height(&Network::Mainnet).expect("valid arithmetic"),
        None,
        "Mainnet has no NU7 height yet"
    );

    let regtest = Network::new_regtest(
        ConfiguredActivationHeights {
            nu7: Some(1),
            ..Default::default()
        }
        .into(),
    );
    assert_eq!(
        nsm_reissuance_crossing_height(&regtest).expect("valid arithmetic"),
        None,
        "Regtest's short schedule does not drain enough reserve during the third halving"
    );
}

proptest::proptest! {
    /// The closed-form constant-subsidy search agrees with a block-by-block oracle.
    #[test]
    fn nsm_crossing_run_matches_linear_oracle(
        first in 1u32..10_000,
        run_length in 0u32..100,
        supply_before_first in 0u128..1_000_000,
        subsidy in 1u128..1_000_000,
        excess_reserve in 0u128..100_000_000,
    ) {
        use crate::parameters::subsidy::{
            first_nsm_crossing_in_subsidy_run, BLOCK_SUBSIDY_FRACTION_DENOMINATOR,
            BLOCK_SUBSIDY_FRACTION_NUMERATOR,
        };

        let run_end = first + run_length;
        let threshold = (subsidy - 1) * BLOCK_SUBSIDY_FRACTION_DENOMINATOR
            / BLOCK_SUBSIDY_FRACTION_NUMERATOR;
        let max_money = supply_before_first + threshold + excess_reserve;
        let expected = (first..=run_end).find(|height| {
            let issued_in_run = u128::from(*height - first) * subsidy;
            let reserve = max_money.saturating_sub(supply_before_first + issued_in_run);
            let reference_subsidy = (reserve * BLOCK_SUBSIDY_FRACTION_NUMERATOR)
                .div_ceil(BLOCK_SUBSIDY_FRACTION_DENOMINATOR);

            reference_subsidy < subsidy
        });
        let actual = first_nsm_crossing_in_subsidy_run(
            first,
            run_end,
            supply_before_first,
            subsidy,
            max_money,
        )
        .expect("generated arithmetic fits");

        proptest::prop_assert_eq!(actual, expected);
    }
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

/// The fixed NSM fraction must be revisited if NU7 target spacing changes.
#[test]
fn block_subsidy_fraction_uses_the_nu7_spacing() {
    use crate::parameters::subsidy::{BLOCK_SUBSIDY_FRACTION_NUMERATOR, LN2_SCALED};

    let _init_guard = zakura_test::init();

    assert_eq!(NetworkUpgrade::Nu7.target_spacing().num_seconds(), 25);
    let interval = Network::Mainnet.post_blossom_halving_interval()
        * NetworkUpgrade::Blossom.target_spacing().num_seconds()
        / NetworkUpgrade::Nu7.target_spacing().num_seconds();
    assert_eq!(interval, 5_040_000);
    assert_eq!(
        BLOCK_SUBSIDY_FRACTION_NUMERATOR,
        LN2_SCALED / u128::try_from(interval).expect("the halving interval is positive"),
    );
}

/// Applying the fraction once per block over a halving interval halves the balance.
#[test]
fn block_subsidy_fraction_halves_the_balance_over_one_interval() {
    use crate::parameters::subsidy::reissuance_bonus;

    let _init_guard = zakura_test::init();

    // Exercise the actual ceiling payout on every block. Rounding the payout up
    // leaves less than the ideal exponential remainder, especially at small balances.
    let initial = 10_000_000_000i64;
    let mut remaining =
        Amount::<NonNegative>::try_from(initial).expect("100 ZEC is a valid amount");
    for _ in 0..5_040_000 {
        remaining = (remaining - reissuance_bonus(remaining).expect("valid bonus"))
            .expect("the bonus never exceeds the remaining balance");
    }

    let half = initial / 2;
    let error = i64::from(remaining).abs_diff(half);
    assert!(
        error * 1_000 < u64::try_from(half).expect("half the initial balance is positive"),
        "one halving interval must leave about half, left {remaining} of {initial}",
    );
}

/// A positive balance always pays at least one zatoshi, so the balance reaches zero in a
/// finite number of blocks.
#[test]
fn reissuance_drains_a_small_balance() {
    use crate::parameters::subsidy::{block_subsidy, halving_block_subsidy};

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
        .with_test_nsm_reissuance_height(start)
        .to_network()
        .expect("configured testnet is valid");

    let halving_subsidy = halving_block_subsidy(start, &network).expect("valid subsidy");
    let initial = 20i64;
    let mut balance = initial;

    for block in 0..initial {
        let subsidy = block_subsidy(
            start,
            &network,
            Some(Amount::try_from(balance).expect("valid amount")),
        )
        .expect("valid subsidy");
        let bonus = i64::from(subsidy) - i64::from(halving_subsidy);

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

/// Check zero, ceiling boundaries, and the largest representable balance explicitly.
#[test]
fn reissuance_bonus_rounding_boundaries() {
    use crate::{amount::MAX_MONEY, parameters::subsidy::reissuance_bonus};

    for (balance, expected) in [
        (0, 0),
        (1, 1),
        (10_000_000_000 - 1, 1_375),
        (10_000_000_000, 1_375),
        (10_000_000_000 + 1, 1_376),
        (MAX_MONEY, 288_750_000),
    ] {
        let actual = reissuance_bonus(Amount::try_from(balance).unwrap()).unwrap();
        assert_eq!(i64::from(actual), expected, "balance {balance}");
    }
}

/// A signed parent balance must be rejected, not clamped, when negative.
#[test]
fn parent_nsm_value_balance_rejects_negative_amounts() {
    use crate::{
        amount::MAX_MONEY,
        parameters::subsidy::{parent_nsm_value_balance, SubsidyError},
    };

    for balance in [-MAX_MONEY, -1] {
        assert!(matches!(
            parent_nsm_value_balance(Amount::try_from(balance).unwrap()),
            Err(SubsidyError::NegativeNsmValueBalance),
        ));
    }
    for balance in [0, 1, MAX_MONEY] {
        let actual = parent_nsm_value_balance(Amount::try_from(balance).unwrap()).unwrap();
        assert_eq!(i64::from(actual), balance);
    }
}

proptest::proptest! {
    #[test]
    fn reissuance_arithmetic_matches_integer_oracle(balance in 0i64..=crate::amount::MAX_MONEY) {
        use crate::parameters::subsidy::{
            reissuance_bonus, BLOCK_SUBSIDY_FRACTION_DENOMINATOR,
            BLOCK_SUBSIDY_FRACTION_NUMERATOR,
        };
        let numerator = i128::try_from(BLOCK_SUBSIDY_FRACTION_NUMERATOR).unwrap();
        let denominator = i128::try_from(BLOCK_SUBSIDY_FRACTION_DENOMINATOR).unwrap();
        let expected = (i128::from(balance) * numerator + denominator - 1) / denominator;
        let actual = reissuance_bonus(Amount::try_from(balance).unwrap()).unwrap();
        proptest::prop_assert_eq!(i128::from(i64::from(actual)), expected);
        proptest::prop_assert!(i64::from(actual) <= balance);
        proptest::prop_assert!(balance == 0 || i64::from(actual) > 0);
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
        .with_test_nsm_reissuance_height(start)
        .to_network()
        .expect("configured testnet is valid");

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

    // NU7 blocks are 25 seconds apart, so the halving interval is 5,040,000 blocks and the
    // fraction is 1375 / 10^10. A balance of 10^12 zatoshi reissues
    // `ceil(10^12 * 1375 / 10^10)`.
    let behind = Amount::<NonNegative>::try_from(1_000_000_000_000i64).expect("valid amount");
    assert_eq!(
        block_subsidy(start, &network, Some(behind)).expect("valid subsidy"),
        (halving_subsidy + Amount::try_from(137_500).expect("valid amount")).expect("valid amount"),
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
                    block_subsidy(Height(h), &network, None).unwrap(),
                    halving_block_subsidy(Height(h), &network).unwrap()
                );
            }
        }
    }
}

#[test]
fn reissuance_activation_and_rounding_boundary_matrix() {
    use crate::parameters::subsidy::{
        halving_block_subsidy, is_zip234_active, nsm_reissuance_height, SubsidyError,
    };
    for nu7 in [None, Some(2)] {
        for configured_start in [1, 2, 3, 10] {
            let network = Network::new_regtest(testnet::RegtestParameters {
                activation_heights: ConfiguredActivationHeights {
                    nu7,
                    ..Default::default()
                },
                test_nsm_reissuance_height: Some(Height(configured_start)),
                ..Default::default()
            });
            assert_eq!(
                nsm_reissuance_height(&network),
                nu7.map(|n| Height(n.max(configured_start)))
            );
            for height in 1..=11 {
                let active = nu7.is_some_and(|n| height >= n.max(configured_start));
                assert_eq!(is_zip234_active(&network, Height(height)), active);
                let scheduled = halving_block_subsidy(Height(height), &network).unwrap();
                if active {
                    assert_eq!(
                        block_subsidy(Height(height), &network, None),
                        Err(SubsidyError::MissingNsmValueBalance)
                    );
                } else {
                    assert_eq!(
                        block_subsidy(Height(height), &network, None).unwrap(),
                        scheduled
                    );
                }
                let numerator = 1_375;
                // The largest deficit that still rounds up to a one-zatoshi bonus.
                let boundary = i64::try_from(10_000_000_000 / numerator).unwrap();
                for deficit in [
                    0i64,
                    1,
                    2,
                    boundary,
                    boundary + 1,
                    4_999_999_999,
                    5_000_000_000,
                    5_000_000_001,
                    crate::amount::MAX_MONEY,
                ] {
                    let expected_bonus = if active {
                        reissuance_bonus_oracle(deficit, numerator)
                    } else {
                        0
                    };
                    let actual = block_subsidy(
                        Height(height),
                        &network,
                        Some(Amount::try_from(deficit).unwrap()),
                    )
                    .unwrap();
                    assert_eq!(
                        i128::from(i64::from(actual)),
                        i128::from(i64::from(scheduled)) + expected_bonus
                    );
                }
            }
        }
    }
    assert!(
        testnet::Parameters::build()
            .with_activation_heights(ConfiguredActivationHeights {
                nu7: Some(0),
                ..Default::default()
            })
            .is_err(),
        "the network builder reserves genesis for the Genesis upgrade"
    );
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
                    block_subsidy(height, &network, None).unwrap(),
                    Amount::<NonNegative>::try_from(expected).unwrap()
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

/// Restates ZIP 234's `ceil(balance * numerator / BLOCK_SUBSIDY_FRACTION_DENOMINATOR)`
/// independently of `block_subsidy`.
fn reissuance_bonus_oracle(balance: i64, numerator: i128) -> i128 {
    use crate::parameters::subsidy::BLOCK_SUBSIDY_FRACTION_DENOMINATOR;

    let denominator = i128::try_from(BLOCK_SUBSIDY_FRACTION_DENOMINATOR).unwrap();
    (i128::from(balance) * numerator + denominator - 1) / denominator
}

/// Preserve the measured seed checks from the merged accounting layer.
#[test]
fn initial_nsm_value_balances_match_pre_nu6_chain_value_pools() {
    use crate::parameters::subsidy::scheduled_issuance_zatoshis;

    let cases = [
        (
            Network::Mainnet,
            Height(2_726_399),
            "https://mainnet.zcashexplorer.app/blocks/2726399",
            [
                1_402_654_579_762_796u128, // transparent
                2_605_536_175_709,         // Sprout
                101_161_389_852_194,       // Sapling
                68_541_635_763_781,        // Orchard
                0,                         // Deferred
                0,                         // Ironwood
            ],
        ),
        (
            Network::new_default_testnet(),
            Height(2_975_999),
            "https://testnet.zcashexplorer.app/blocks/2975999",
            [
                1_433_024_042_538_559u128, // transparent
                42_832_983_037_484,        // Sprout
                123_413_239_739_335,       // Sapling
                3_798_966_269_665,         // Orchard
                0,                         // Deferred
                0,                         // Ironwood
            ],
        ),
    ];

    for (network, height, explorer, value_pools) in cases {
        let scheduled = scheduled_issuance_zatoshis(height, &network)
            .expect("the public network issuance schedule is valid");
        let issued = value_pools.into_iter().sum::<u128>();
        let measured_seed = scheduled
            .checked_sub(issued)
            .expect("scheduled issuance is at least the issued supply");
        let configured_seed = u128::try_from(i64::from(network.initial_nsm_value_balance()))
            .expect("the initial NSM value balance is non-negative");

        assert_eq!(
            measured_seed, configured_seed,
            "{network:?} value pools at {height:?}, see {explorer}",
        );
    }
}
