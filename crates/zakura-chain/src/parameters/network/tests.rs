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
