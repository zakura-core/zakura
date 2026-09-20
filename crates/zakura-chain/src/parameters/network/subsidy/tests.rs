//! Reissuance arithmetic at the monetary bound.

use super::*;

proptest::proptest! {
    #[test]
    fn max_money_reissuance_matches_wide_integer_oracle(balance in 0i64..=MAX_MONEY) {
        // Keep the fixed NU7 fraction independent of the production constants.
        for balance in [0, 1, MAX_MONEY - 1, MAX_MONEY, balance] {
            let product = u128::try_from(balance).unwrap() * 1_375;
            let expected = product / 10_000_000_000 + u128::from(!product.is_multiple_of(10_000_000_000));
            let actual = reissuance_bonus(Amount::try_from(balance).unwrap()).unwrap();
            proptest::prop_assert_eq!(u128::try_from(i64::from(actual)).unwrap(), expected);
            proptest::prop_assert!(i64::from(actual) <= balance);
        }
    }
}
