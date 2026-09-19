//! Reissuance arithmetic at the monetary bound.

use super::*;
use crate::parameters::testnet::{self, ConfiguredActivationHeights};

proptest::proptest! {
    #[test]
    fn max_money_reissuance_matches_wide_integer_oracle(balance in 0i64..=MAX_MONEY) {
        let network = testnet::Parameters::build()
            .with_activation_heights(ConfiguredActivationHeights {
                blossom: Some(1_000_000), canopy: Some(1_000_001), nu7: Some(4_000_000),
                ..Default::default()
            }).unwrap().clear_funding_streams().to_network().unwrap();
        // Fixed numerators make the oracle independent of the production fraction helper.
        for (height, numerator) in [(999_999, 8_252u128), (1_000_000, 4_126),
            (3_999_999, 4_126), (4_000_000, 1_375), (4_000_001, 1_375)] {
            for balance in [0, 1, MAX_MONEY - 1, MAX_MONEY, balance] {
                let product = u128::try_from(balance).unwrap() * numerator;
                let expected = product / 10_000_000_000 + u128::from(!product.is_multiple_of(10_000_000_000));
                let actual = reissuance_bonus(Amount::try_from(balance).unwrap(), Height(height), &network).unwrap();
                proptest::prop_assert_eq!(u128::try_from(i64::from(actual)).unwrap(), expected);
                proptest::prop_assert!(i64::from(actual) <= balance);
            }
        }
    }
}
