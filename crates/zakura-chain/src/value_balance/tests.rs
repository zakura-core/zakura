//! Tests for value balances.

#![allow(clippy::unwrap_in_result)]

use crate::{
    amount::{Amount, NegativeAllowed, NonNegative},
    value_balance::{ValueBalance, ValueBalanceError},
};

mod prop;

#[test]
fn ironwood_deposit_updates_only_the_ironwood_chain_pool() {
    let _init_guard = zakura_test::init();

    let transaction_value_balance =
        ValueBalance::from_ironwood_amount(Amount::<NegativeAllowed>::try_from(-1).unwrap());
    let chain_value_pool_change = -transaction_value_balance;
    let updated = ValueBalance::<NonNegative>::zero()
        .add_chain_value_pool_change(chain_value_pool_change)
        .expect("an Ironwood deposit adds value to the Ironwood chain pool");

    assert_eq!(
        updated.ironwood_amount(),
        Amount::<NonNegative>::try_from(1).unwrap()
    );
    let zero = Amount::<NonNegative>::zero();
    assert_eq!(updated.transparent_amount(), zero);
    assert_eq!(updated.sprout_amount(), zero);
    assert_eq!(updated.sapling_amount(), zero);
    assert_eq!(updated.orchard_amount(), zero);
    assert_eq!(updated.deferred_amount(), zero);
}

#[test]
fn remaining_transaction_value_includes_ironwood() {
    let _init_guard = zakura_test::init();

    let one = Amount::<NegativeAllowed>::try_from(1).unwrap();
    let minus_one = Amount::<NegativeAllowed>::try_from(-1).unwrap();
    let mut value_balance = ValueBalance::from_transparent_amount(one);

    value_balance.set_ironwood_value_balance(ValueBalance::from_ironwood_amount(minus_one));

    assert_eq!(
        value_balance.remaining_transaction_value(),
        Ok(Amount::<NonNegative>::zero())
    );
}

#[test]
fn ironwood_chain_pool_underflow_is_reported_as_ironwood() {
    let _init_guard = zakura_test::init();

    let chain_value_pool_change =
        ValueBalance::from_ironwood_amount(Amount::<NegativeAllowed>::try_from(-1).unwrap());

    assert!(matches!(
        ValueBalance::<NonNegative>::zero().add_chain_value_pool_change(chain_value_pool_change),
        Err(ValueBalanceError::Ironwood(_))
    ));
}

#[test]
fn value_balance_bytes_keep_deferred_before_ironwood() {
    let _init_guard = zakura_test::init();

    let orchard = Amount::<NonNegative>::try_from(4).unwrap();
    let deferred = Amount::<NonNegative>::try_from(5).unwrap();
    let ironwood = Amount::<NonNegative>::try_from(6).unwrap();
    let mut value_balance = ValueBalance::from_orchard_amount(orchard);

    value_balance.set_deferred_amount(deferred);
    value_balance.set_ironwood_value_balance(ValueBalance::from_ironwood_amount(ironwood));

    let bytes = value_balance.to_bytes();
    assert_eq!(&bytes[32..40], &deferred.to_bytes());
    assert_eq!(&bytes[40..48], &ironwood.to_bytes());

    let pre_ironwood = ValueBalance::<NonNegative>::from_bytes(&bytes[..40])
        .expect("40-byte value balance parses");
    assert_eq!(pre_ironwood.orchard_amount(), orchard);
    assert_eq!(pre_ironwood.deferred_amount(), deferred);
    assert_eq!(
        pre_ironwood.ironwood_amount(),
        Amount::<NonNegative>::zero()
    );
    assert_eq!(
        ValueBalance::<NonNegative>::from_bytes(&bytes),
        Ok(value_balance)
    );
}

#[test]
fn value_balance_bytes_report_invalid_tail_pool() {
    let _init_guard = zakura_test::init();

    let mut bytes = ValueBalance::<NonNegative>::zero().to_bytes();
    bytes[32..40].copy_from_slice(&(-1_i64).to_le_bytes());
    assert!(matches!(
        ValueBalance::<NonNegative>::from_bytes(&bytes),
        Err(ValueBalanceError::Deferred(_))
    ));

    bytes[32..40].copy_from_slice(&0_i64.to_le_bytes());
    bytes[40..48].copy_from_slice(&(-1_i64).to_le_bytes());
    assert!(matches!(
        ValueBalance::<NonNegative>::from_bytes(&bytes),
        Err(ValueBalanceError::Ironwood(_))
    ));
    assert_eq!(
        ValueBalance::<NonNegative>::from_bytes(&bytes[..47]),
        Err(ValueBalanceError::Unparsable)
    );
}
