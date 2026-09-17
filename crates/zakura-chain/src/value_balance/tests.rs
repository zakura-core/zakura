//! Tests for value balances.

#![allow(clippy::unwrap_in_result)]

use crate::{
    amount::{Amount, NegativeAllowed, NonNegative, MAX_MONEY},
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

#[test]
fn chain_pool_total_limit_includes_every_pool() {
    let _init_guard = zakura_test::init();

    // Every pool contributes to a total exactly at the cap.
    #[cfg(not(zcash_unstable = "nutachyon"))]
    let share = Amount::<NonNegative>::try_from(MAX_MONEY / 6).unwrap();
    #[cfg(zcash_unstable = "nutachyon")]
    let share = Amount::<NonNegative>::try_from(MAX_MONEY / 7).unwrap();
    let at_cap = ValueBalance {
        transparent: share,
        sprout: share,
        sapling: share,
        orchard: share,
        deferred: share,
        ironwood: share,
        #[cfg(zcash_unstable = "nutachyon")]
        tachyon: share,
    };
    assert_eq!(at_cap.total(), Ok(Amount::try_from(MAX_MONEY).unwrap()));
    assert_eq!(
        at_cap.add_chain_value_pool_change(ValueBalance::zero()),
        Ok(at_cap)
    );

    let one = Amount::<NegativeAllowed>::try_from(1).unwrap();
    let mut deferred_change = ValueBalance::zero();
    deferred_change.set_deferred_amount(one);
    for change in [
        ValueBalance::from_transparent_amount(one),
        ValueBalance::from_sprout_amount(one),
        ValueBalance::from_sapling_amount(one),
        ValueBalance::from_orchard_amount(one),
        deferred_change,
        ValueBalance::from_ironwood_amount(one),
    ] {
        // Every individual pool remains valid, but the combined total is one zatoshi over.
        assert!(matches!(
            at_cap.add_chain_value_pool_change(change),
            Err(ValueBalanceError::Total(_))
        ));
        let below_cap = at_cap.add_chain_value_pool_change(-change).unwrap();
        assert_eq!(
            below_cap.total(),
            Ok(Amount::try_from(MAX_MONEY - 1).unwrap())
        );
        assert_eq!(below_cap.add_chain_value_pool_change(change), Ok(at_cap));
    }
}

#[test]
fn chain_pool_transfer_at_total_limit_is_valid() {
    let _init_guard = zakura_test::init();

    let initial =
        ValueBalance::from_transparent_amount(Amount::<NonNegative>::try_from(MAX_MONEY).unwrap());
    let one = Amount::<NegativeAllowed>::try_from(1).unwrap();
    let mut transfer = ValueBalance::from_transparent_amount(-one);
    transfer.set_ironwood_value_balance(ValueBalance::from_ironwood_amount(one));
    let updated = initial.add_chain_value_pool_change(transfer).unwrap();
    assert_eq!(updated.total(), initial.total());
    assert_eq!(updated.add_chain_value_pool_change(-transfer), Ok(initial));
}

#[test]
fn total_sums_signed_balances_before_applying_the_constraint() {
    let _init_guard = zakura_test::init();

    let max = Amount::<NegativeAllowed>::try_from(MAX_MONEY).unwrap();
    let mut balance = ValueBalance::from_transparent_amount(max);
    balance.set_sprout_value_balance(ValueBalance::from_sprout_amount(max));
    balance.set_ironwood_value_balance(ValueBalance::from_ironwood_amount(-max));
    assert_eq!(balance.total(), Ok(max));
}
