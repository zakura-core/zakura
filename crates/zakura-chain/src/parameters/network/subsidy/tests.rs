//! Reissuance arithmetic at the monetary bound.

use super::*;

proptest::proptest! {
    #[test]
    fn max_money_reissuance_matches_wide_integer_oracle(balance in 0i64..=MAX_MONEY) {
        // Keep the Mainnet NU7 fraction independent of the production constants.
        for balance in [0, 1, MAX_MONEY - 1, MAX_MONEY, balance] {
            let product = u128::try_from(balance).unwrap() * 1_375;
            let expected = product / 10_000_000_000 + u128::from(!product.is_multiple_of(10_000_000_000));
            let actual = reissuance_bonus(Amount::try_from(balance).unwrap(), &Network::Mainnet).unwrap();
            proptest::prop_assert_eq!(u128::try_from(i64::from(actual)).unwrap(), expected);
            proptest::prop_assert!(i64::from(actual) <= balance);
        }
    }
}

fn custom_network(interval: i64) -> Network {
    use crate::parameters::testnet::{ConfiguredActivationHeights, Parameters};

    Parameters::build()
        .with_halving_interval(interval)
        .unwrap()
        .with_slow_start_interval(Height(0))
        .with_activation_heights(ConfiguredActivationHeights {
            nu7: Some(1),
            ..Default::default()
        })
        .unwrap()
        .clear_funding_streams()
        .to_network()
        .unwrap()
}

#[test]
fn reissuance_fraction_matches_zebra_nu7_intervals() {
    // Zebra v7.0.0-rc.0 uses floor(6_931_680_000 / post_nu7_halving_interval).
    for (network, expected) in [
        (Network::Mainnet, 1_375),
        (Network::new_default_testnet(), 1_375),
        (Network::new_regtest(Default::default()), 8_022_777),
        (custom_network(144), 8_022_777),
        (custom_network(840_000), 1_375),
        (custom_network(1_155_280_000), 1),
    ] {
        assert_eq!(block_subsidy_fraction_numerator(&network), Ok(expected));
    }
}

#[test]
fn custom_reissuance_rounds_up_and_preserves_the_halving_interval() {
    let network = custom_network(144);
    for balance in [
        0,
        1,
        10_000_000_000 - 1,
        10_000_000_000,
        10_000_000_000 + 1,
        MAX_MONEY,
    ] {
        let product = i128::from(balance) * 8_022_777;
        let expected = (product + 9_999_999_999) / 10_000_000_000;
        let bonus = reissuance_bonus(Amount::try_from(balance).unwrap(), &network).unwrap();
        assert_eq!(i128::from(i64::from(bonus)), expected);
        assert!(i64::from(bonus) <= balance);
    }
    let initial = Amount::try_from(10_000_000_000i64).unwrap();
    let mut remaining = initial;
    for _ in 0..864 {
        remaining = (remaining - reissuance_bonus(remaining, &network).unwrap()).unwrap();
    }
    assert!(i64::from(remaining).abs_diff(5_000_000_000) < 2_000_000);
}

#[test]
fn custom_crossing_matches_per_block_reference() {
    for interval in [144, 150, 1_000] {
        let network = custom_network(interval);
        let first = height_for_halving(3, &network).unwrap().0 + 1;
        let end = height_for_halving(4, &network).unwrap().0;
        let numerator = 6_931_680_000u128 / (u128::try_from(interval).unwrap() * 6);
        let expected = (first..end).find(|height| {
            let supply = scheduled_issuance_zatoshis(Height(height - 1), &network).unwrap();
            let reserve = u128::try_from(MAX_MONEY).unwrap().saturating_sub(supply);
            let product = reserve * numerator;
            let reference =
                product / 10_000_000_000 + u128::from(!product.is_multiple_of(10_000_000_000));
            reference < amount_to_u128(halving_block_subsidy(Height(*height), &network).unwrap())
        });
        assert_eq!(
            nsm_reissuance_crossing_height(&network).unwrap(),
            expected.map(Height)
        );
    }
}

#[test]
fn nu7_rejects_an_interval_that_rounds_the_fraction_to_zero() {
    use crate::parameters::testnet::{ConfiguredActivationHeights, Parameters};

    let builder = Parameters::build()
        .with_halving_interval(1_155_280_001)
        .unwrap()
        .with_slow_start_interval(Height(0))
        .with_activation_heights(ConfiguredActivationHeights {
            canopy: Some(1),
            ..Default::default()
        })
        .unwrap()
        .clear_funding_streams();
    // The legacy schedule still supports this interval without NU7.
    let network = builder.clone().to_network().unwrap();
    assert_eq!(
        block_subsidy_fraction_numerator(&network),
        Err(SubsidyError::InvalidReissuanceFraction)
    );
    assert!(builder
        .with_activation_heights(ConfiguredActivationHeights {
            nu7: Some(1),
            ..Default::default()
        })
        .unwrap()
        .to_network()
        .is_err());
}

#[test]
fn mixed_network_height_194_reward_matches_zebra() {
    use crate::parameters::testnet::{ConfiguredActivationHeights, Parameters};

    let network = Parameters::build()
        .with_halving_interval(144)
        .unwrap()
        .with_slow_start_interval(Height(0))
        .with_activation_heights(ConfiguredActivationHeights {
            blossom: Some(190),
            canopy: Some(190),
            nu6_3: Some(190),
            nu7: Some(191),
            ..Default::default()
        })
        .unwrap()
        .clear_funding_streams()
        .with_test_nsm_reissuance_height(Height(194))
        .to_network()
        .unwrap();
    // The October 20 mixed-network run shared this parent balance at height 193.
    let balance = Amount::try_from(1_000_001_818_000i64).unwrap();
    assert_eq!(
        i64::from(reissuance_bonus(balance, &network).unwrap()),
        802_279_159
    );
    assert_eq!(
        i64::from(halving_block_subsidy(Height(194), &network).unwrap()),
        104_166_666
    );
    assert_eq!(
        i64::from(block_subsidy(Height(194), &network, Some(balance)).unwrap()),
        906_445_825
    );
}

proptest::proptest! {
    #[test]
    fn custom_reissuance_matches_integer_oracle(
        interval in 1i64..=1_155_280_000,
        balance in 0i64..=MAX_MONEY,
    ) {
        let network = custom_network(interval);
        let numerator = 6_931_680_000i128 / (i128::from(interval) * 6);
        let expected = (i128::from(balance) * numerator + 9_999_999_999) / 10_000_000_000;
        let actual = reissuance_bonus(Amount::try_from(balance).unwrap(), &network).unwrap();
        proptest::prop_assert_eq!(i128::from(i64::from(actual)), expected);
        proptest::prop_assert!(i64::from(actual) <= balance);
        proptest::prop_assert!(balance == 0 || i64::from(actual) > 0);
    }
}
