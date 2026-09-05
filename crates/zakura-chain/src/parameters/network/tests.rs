#![allow(clippy::unwrap_in_result)]

mod prop;
mod vectors;

use color_eyre::Report;

use super::Network;
use crate::{
    amount::{Amount, NonNegative, MAX_MONEY},
    block::Height,
    parameters::{
        subsidy::{
            block_subsidy, constants::POST_BLOSSOM_HALVING_INTERVAL, halving,
            halving_block_subsidy, halving_divisor, height_for_halving, ParameterSubsidy as _,
            SubsidyError,
        },
        testnet::{self, ConfiguredActivationHeights},
        NetworkUpgrade,
    },
};

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

/// Tests the ZIP 218 target spacing, halving, and block subsidy across the NU7
/// activation boundary on a configured Testnet.
#[test]
#[cfg(feature = "zip218")]
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
    assert_eq!(16, halving_divisor(next_halving, &network).unwrap());
    assert_eq!(
        Amount::<NonNegative>::try_from(13_020_833)?,
        halving_block_subsidy(next_halving, &network)?,
    );

    Ok(())
}

/// Tests that the ZIP 218 difficulty averaging window widens at the NU7
/// activation height.
#[test]
#[cfg(feature = "zip218")]
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

/// Checks the ZIP 234 issuance rules: the activation height, the smoothed curve, the
/// preserve-halvings bonus, and the cumulative schedule the bonus is derived from.
#[test]
fn zip234_issuance() {
    use crate::{
        parameters::{
            subsidy::{cumulative_halving_subsidies_for_tests, zip234_start_height},
            ZIP218_ENABLED, ZIP234_ENABLED, ZIP234_HALVINGS_ENABLED, ZIP234_SMOOTHING_ENABLED,
        },
        value_balance::ValueBalance,
    };

    let _init_guard = zakura_test::init();

    // A network that does not activate NU7 never reaches ZIP 234.
    let no_nu7 = testnet::Parameters::build()
        .to_network()
        .expect("configured testnet is valid");
    assert_eq!(zip234_start_height(&no_nu7), None);
    assert_eq!(zip234_start_height(&Network::Mainnet), None);

    let nu7 = 1_000_000;
    let network = testnet::Parameters::build()
        .with_activation_heights(ConfiguredActivationHeights {
            blossom: Some(1),
            canopy: Some(2),
            nu7: Some(nu7),
            ..Default::default()
        })
        .expect("activation heights are valid")
        .clear_funding_streams()
        .to_network()
        .expect("configured testnet is valid");

    let start = zip234_start_height(&network).expect("NU7 is configured");
    assert_eq!(start, Height(nu7));

    // `cumulative_halving_subsidies` walks halving and spacing boundaries rather than
    // every height, so check it against the sum it is standing in for.
    let mut brute_force = Amount::<NonNegative>::zero();
    for height in 1..=25_000u32 {
        brute_force = (brute_force
            + block_subsidy(Height(height), &network, None).expect("valid subsidy"))
        .expect("sum is in range");

        assert_eq!(
            cumulative_halving_subsidies_for_tests(Height(height), &network)
                .expect("valid cumulative subsidy"),
            brute_force,
            "cumulative subsidies must match the per-height sum at height {height}",
        );
    }

    if !ZIP234_ENABLED {
        // Without either option the subsidy stays on the halving schedule, and passing a
        // money reserve changes nothing.
        let reserve = Amount::<NonNegative>::try_from(1_000_000_000_000i64).expect("valid amount");
        assert_eq!(
            block_subsidy(start, &network, Some(reserve)).expect("valid subsidy"),
            block_subsidy(start, &network, None).expect("valid subsidy"),
        );
        return;
    }

    // Both options need the money reserve at a ZIP 234 height, and neither wants it
    // below one.
    assert_eq!(
        block_subsidy(start, &network, None),
        Err(SubsidyError::MissingMoneyReserve),
    );
    assert!(block_subsidy(
        start.previous().expect("start is above genesis"),
        &network,
        None
    )
    .is_ok());

    // A reserve of 10^12 zatoshi issues 412,600 at 75-second spacing. ZIP 218
    // divides the fraction by three at 25-second spacing, rounded up to 137,534.
    let reserve = Amount::<NonNegative>::try_from(1_000_000_000_000i64).expect("valid amount");
    let subsidy = block_subsidy(start, &network, Some(reserve)).expect("valid subsidy");
    let reissuance_subsidy = if ZIP218_ENABLED { 137_534 } else { 412_600 };

    let halving_subsidy = halving_block_subsidy(start, &network).expect("valid subsidy");

    if ZIP234_SMOOTHING_ENABLED {
        assert_eq!(
            subsidy,
            Amount::<NonNegative>::try_from(reissuance_subsidy).expect("valid amount")
        );

        // The curve replaces halvings, so an empty reserve issues nothing.
        assert_eq!(
            block_subsidy(start, &network, Some(Amount::zero())).expect("valid subsidy"),
            Amount::<NonNegative>::zero(),
        );
    }

    if ZIP234_HALVINGS_ENABLED {
        // A chain that is exactly on schedule has nothing to reissue, so the subsidy is
        // the halving schedule alone.
        let scheduled = cumulative_halving_subsidies_for_tests(
            start.previous().expect("start is above genesis"),
            &network,
        )
        .expect("valid cumulative subsidy");
        let max_money = Amount::<NonNegative>::try_from(MAX_MONEY).expect("valid amount");
        let on_schedule = (max_money - scheduled).expect("valid amount");

        assert_eq!(
            block_subsidy(start, &network, Some(on_schedule)).expect("valid subsidy"),
            halving_subsidy,
            "a chain on its own schedule reissues nothing",
        );

        // A chain 10^12 zatoshi behind its schedule adds the spacing-adjusted
        // reissuance subsidy.
        let behind = (on_schedule
            + Amount::<NonNegative>::try_from(1_000_000_000_000i64).expect("valid amount"))
        .expect("valid amount");
        assert_eq!(
            block_subsidy(start, &network, Some(behind)).expect("valid subsidy"),
            (halving_subsidy + Amount::try_from(reissuance_subsidy).expect("valid amount"))
                .expect("valid amount"),
        );
    }

    // The money reserve is what has never been issued plus everything removed from
    // circulation.
    assert_eq!(
        ValueBalance::<NonNegative>::zero().money_reserve(),
        Amount::<NonNegative>::try_from(MAX_MONEY).expect("valid amount"),
    );
}

/// Checks that a network starts ZIP 234 at NU7 or at a configured later height.
#[test]
fn zip234_deployment_height() {
    use crate::parameters::{
        network::error::ParametersBuilderError, subsidy::zip234_start_height, Zip234Deployment,
    };

    let _init_guard = zakura_test::init();

    let nu7 = 3_600_000;
    let mainnet_shaped = |zip234_deployment| {
        testnet::Parameters::build()
            .with_activation_heights(ConfiguredActivationHeights {
                blossom: Some(653_600),
                heartwood: Some(903_000),
                canopy: Some(1_046_400),
                nu7: Some(nu7),
                ..Default::default()
            })
            .expect("activation heights are valid")
            .clear_funding_streams()
            .with_zip234_deployment(zip234_deployment)
            .to_network()
    };

    let at_nu7 = mainnet_shaped(Zip234Deployment::AtNu7).expect("configured testnet is valid");
    assert_eq!(zip234_start_height(&at_nu7), Some(Height(nu7)));

    // A configured height can select either later ballot date once its block height is known.
    let ballot_height = Height(3_900_000);
    let configured = mainnet_shaped(Zip234Deployment::AtHeight(ballot_height))
        .expect("configured testnet is valid");
    assert_eq!(zip234_start_height(&configured), Some(ballot_height));

    // ZIP 234 deploys with or after NU7, so a height below NU7 activation is rejected.
    assert_eq!(
        mainnet_shaped(Zip234Deployment::AtHeight(Height(nu7 - 1))),
        Err(ParametersBuilderError::Zip234DeploymentBeforeNu7 {
            deployment_height: Height(nu7 - 1),
            nu7_activation_height: Height(nu7),
        }),
    );

    // A network built without those checks is clamped to NU7 rather than changing the
    // subsidy before NU7 activates.
    let unchecked = testnet::Parameters::build()
        .with_activation_heights(ConfiguredActivationHeights {
            nu7: Some(nu7),
            ..Default::default()
        })
        .expect("activation heights are valid")
        .with_zip234_deployment(Zip234Deployment::AtHeight(Height(1)))
        .to_network_unchecked();
    assert_eq!(zip234_start_height(&unchecked), Some(Height(nu7)));

    // A network that never activates NU7 never starts reissuance, however it is
    // configured.
    let no_nu7 = testnet::Parameters::build()
        .with_zip234_deployment(Zip234Deployment::AtHeight(Height(1)))
        .to_network()
        .expect("configured testnet is valid");
    assert_eq!(zip234_start_height(&no_nu7), None);
}
