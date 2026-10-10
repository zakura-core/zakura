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
        // The NSM value balance holds value that is in no pool, so `total` excludes it.
        nsm_value_balance: Amount::zero(),
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

#[test]
fn signed_deficit_roundtrips_without_changing_monetary_totals() {
    for deficit in [-MAX_MONEY, -1, 0, 1, MAX_MONEY] {
        let mut pools =
            ValueBalance::from_transparent_amount(Amount::<NonNegative>::try_from(7).unwrap());
        pools.set_nsm_value_balance_amount(Amount::try_from(deficit).unwrap());
        assert_eq!(i64::from(pools.total().unwrap()), 7);
        assert_eq!(i64::from(pools.issued_supply()), 7);
        assert_eq!(i64::from(pools.money_reserve()), MAX_MONEY - 7);
        assert_eq!(ValueBalance::from_bytes(&pools.to_bytes()).unwrap(), pools);
        let signed = pools.constrain::<NegativeAllowed>().unwrap();
        assert_eq!(signed.constrain::<NonNegative>().unwrap(), pools);
        assert_eq!((signed + -signed).unwrap(), ValueBalance::zero());
    }
}

#[test]
fn nsm_seed_matches_measured_public_network_supply() {
    use crate::{block::Height, parameters::Network};
    for (network, height, issued, expected) in [
        (
            Network::Mainnet,
            2_726_399,
            1_574_963_141_554_480i64,
            36_858_445_520i64,
        ),
        (
            Network::new_default_testnet(),
            2_975_999,
            1_603_069_231_585_043,
            55_768_414_957,
        ),
    ] {
        let mut pools =
            ValueBalance::from_transparent_amount(Amount::<NonNegative>::try_from(issued).unwrap());
        // NSM is not part of issued supply, even if a migration already wrote it.
        pools.set_nsm_value_balance_amount(Amount::try_from(123).unwrap());
        assert_eq!(
            i64::from(
                pools
                    .initial_nsm_value_balance(Height(height), &network)
                    .unwrap()
            ),
            expected
        );
        let wrong = pools
            .add_chain_value_pool_change(ValueBalance::from_transparent_amount(
                Amount::try_from(1).unwrap(),
            ))
            .unwrap();
        assert!(matches!(
            wrong.initial_nsm_value_balance(Height(height), &network),
            Err(ValueBalanceError::NsmSeedMismatch { .. })
        ));
    }
}

#[test]
fn nsm_seed_uses_all_monetary_pools_and_rejects_overissuance() {
    use crate::{
        block::Height,
        parameters::{
            subsidy::scheduled_issuance_zatoshis,
            testnet::{ConfiguredActivationHeights, RegtestParameters},
            Network,
        },
    };
    let network = Network::new_regtest(RegtestParameters {
        activation_heights: ConfiguredActivationHeights {
            nu7: Some(3),
            ..Default::default()
        },
        ..Default::default()
    });
    let height = Height(2);
    let scheduled = i64::try_from(scheduled_issuance_zatoshis(height, &network).unwrap()).unwrap();
    let mut pools = ValueBalance::from_transparent_amount(
        Amount::<NonNegative>::try_from(scheduled - 100).unwrap(),
    );
    for leg in [
        ValueBalance::from_sprout_amount,
        ValueBalance::from_sapling_amount,
        ValueBalance::from_orchard_amount,
        ValueBalance::from_ironwood_amount,
    ] {
        pools = (pools + leg(Amount::try_from(10).unwrap())).unwrap();
    }
    pools.set_deferred_amount(Amount::try_from(10).unwrap());
    let seeded = pools.seed_nsm_value_balance(height, &network).unwrap();
    assert_eq!(i64::from(seeded.nsm_value_balance_amount()), 50);
    assert_eq!(seeded.total().unwrap(), pools.total().unwrap());
    assert_eq!(
        seeded.seed_nsm_value_balance(height, &network).unwrap(),
        seeded
    );
    assert_eq!(
        pools.seed_nsm_value_balance(Height(1), &network).unwrap(),
        pools
    );
    assert_eq!(
        pools.seed_nsm_value_balance(Height(3), &network).unwrap(),
        pools
    );
    let over = ValueBalance::from_transparent_amount(
        Amount::<NonNegative>::try_from(scheduled + 1).unwrap(),
    );
    assert!(matches!(
        over.seed_nsm_value_balance(height, &network),
        Err(ValueBalanceError::NsmValueBalance(_))
    ));
    for seed in [0, 123] {
        let configured = Network::new_regtest(RegtestParameters {
            activation_heights: ConfiguredActivationHeights {
                nu7: Some(3),
                ..Default::default()
            },
            initial_nsm_value_balance: Some(Amount::try_from(seed).unwrap()),
            ..Default::default()
        });
        assert_eq!(
            i64::from(
                over.seed_nsm_value_balance(height, &configured)
                    .unwrap()
                    .nsm_value_balance_amount()
            ),
            seed
        );
    }
    for nu7 in [None, Some(1)] {
        let network = Network::new_regtest(RegtestParameters {
            activation_heights: ConfiguredActivationHeights {
                nu7,
                ..Default::default()
            },
            ..Default::default()
        });
        let zero = ValueBalance::<NonNegative>::zero();
        assert_eq!(
            zero.seed_nsm_value_balance(Height(0), &network).unwrap(),
            zero
        );
    }
}
