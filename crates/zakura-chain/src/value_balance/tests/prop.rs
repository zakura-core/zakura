//! Randomised property tests for value balances.

use proptest::prelude::*;

use crate::{amount::*, value_balance::*};

proptest! {
    #[test]
    #[cfg(zcash_unstable = "nutachyon")]
    fn value_blance_add(
        value_balance1 in any::<ValueBalance<NegativeAllowed>>(),
        value_balance2 in any::<ValueBalance<NegativeAllowed>>())
    {
        let _init_guard = zakura_test::init();

        let transparent = value_balance1.transparent + value_balance2.transparent;
        let sprout = value_balance1.sprout + value_balance2.sprout;
        let sapling = value_balance1.sapling + value_balance2.sapling;
        let orchard = value_balance1.orchard + value_balance2.orchard;
        let deferred = value_balance1.deferred + value_balance2.deferred;
        let ironwood = value_balance1.ironwood + value_balance2.ironwood;
        let tachyon = value_balance1.tachyon + value_balance2.tachyon;
        let nsm_value_balance = value_balance1.nsm_value_balance + value_balance2.nsm_value_balance;

        match (transparent, sprout, sapling, orchard, deferred, ironwood, nsm_value_balance, tachyon) {
            (Ok(transparent), Ok(sprout), Ok(sapling), Ok(orchard), Ok(deferred), Ok(ironwood), Ok(nsm_value_balance), Ok(tachyon)) => prop_assert_eq!(
                value_balance1 + value_balance2,
                Ok(ValueBalance {
                    transparent,
                    sprout,
                    sapling,
                    orchard,
                    deferred,
                    ironwood,
                    nsm_value_balance,
                    tachyon
                })
            ),
            _ => prop_assert!(
                matches!(
                    value_balance1 + value_balance2,
                    Err(ValueBalanceError::Transparent(_)
                        | ValueBalanceError::Sprout(_)
                        | ValueBalanceError::Sapling(_)
                        | ValueBalanceError::Orchard(_)
                        | ValueBalanceError::Deferred(_)
                        | ValueBalanceError::Ironwood(_)
                        | ValueBalanceError::NsmValueBalance(_)
                        | ValueBalanceError::Tachyon(_))
                )
            ),
        }
    }
    #[test]
    #[cfg(zcash_unstable = "nutachyon")]
    fn value_balance_sub(
        value_balance1 in any::<ValueBalance<NegativeAllowed>>(),
        value_balance2 in any::<ValueBalance<NegativeAllowed>>())
    {
        let _init_guard = zakura_test::init();

        let transparent = value_balance1.transparent - value_balance2.transparent;
        let sprout = value_balance1.sprout - value_balance2.sprout;
        let sapling = value_balance1.sapling - value_balance2.sapling;
        let orchard = value_balance1.orchard - value_balance2.orchard;
        let deferred = value_balance1.deferred - value_balance2.deferred;
        let ironwood = value_balance1.ironwood - value_balance2.ironwood;
        let tachyon = value_balance1.tachyon - value_balance2.tachyon;
        let nsm_value_balance = value_balance1.nsm_value_balance - value_balance2.nsm_value_balance;

        match (transparent, sprout, sapling, orchard, deferred, ironwood, nsm_value_balance, tachyon) {
            (Ok(transparent), Ok(sprout), Ok(sapling), Ok(orchard), Ok(deferred), Ok(ironwood), Ok(nsm_value_balance), Ok(tachyon)) => prop_assert_eq!(
                value_balance1 - value_balance2,
                Ok(ValueBalance {
                    transparent,
                    sprout,
                    sapling,
                    orchard,
                    deferred,
                    ironwood,
                    nsm_value_balance,
                    tachyon
                })
            ),
            _ => prop_assert!(matches!(
                    value_balance1 - value_balance2,
                    Err(ValueBalanceError::Transparent(_)
                        | ValueBalanceError::Sprout(_)
                        | ValueBalanceError::Sapling(_)
                        | ValueBalanceError::Orchard(_)
                        | ValueBalanceError::Deferred(_)
                        | ValueBalanceError::Ironwood(_)
                        | ValueBalanceError::NsmValueBalance(_)
                        | ValueBalanceError::Tachyon(_))
                )),
        }
    }

    #[test]
    #[cfg(zcash_unstable = "nutachyon")]
    fn value_balance_sum(
        value_balance1 in any::<ValueBalance<NegativeAllowed>>(),
        value_balance2 in any::<ValueBalance<NegativeAllowed>>(),
    ) {
        let _init_guard = zakura_test::init();

        let collection = [value_balance1, value_balance2];

        let transparent = value_balance1.transparent + value_balance2.transparent;
        let sprout = value_balance1.sprout + value_balance2.sprout;
        let sapling = value_balance1.sapling + value_balance2.sapling;
        let orchard = value_balance1.orchard + value_balance2.orchard;
        let deferred = value_balance1.deferred + value_balance2.deferred;
        let ironwood = value_balance1.ironwood + value_balance2.ironwood;
        let tachyon = value_balance1.tachyon + value_balance2.tachyon;
        let nsm_value_balance = value_balance1.nsm_value_balance + value_balance2.nsm_value_balance;

        match (transparent, sprout, sapling, orchard, deferred, ironwood, nsm_value_balance, tachyon) {
            (Ok(transparent), Ok(sprout), Ok(sapling), Ok(orchard), Ok(deferred), Ok(ironwood), Ok(nsm_value_balance), Ok(tachyon)) => prop_assert_eq!(
                collection.iter().sum::<Result<ValueBalance<NegativeAllowed>, ValueBalanceError>>(),
                Ok(ValueBalance {
                    transparent,
                    sprout,
                    sapling,
                    orchard,
                    deferred,
                    ironwood,
                    nsm_value_balance,
                    tachyon
                })
            ),
            _ => prop_assert!(matches!(
                    collection.iter().sum(),
                    Err(ValueBalanceError::Transparent(_)
                        | ValueBalanceError::Sprout(_)
                        | ValueBalanceError::Sapling(_)
                        | ValueBalanceError::Orchard(_)
                        | ValueBalanceError::Deferred(_)
                        | ValueBalanceError::Ironwood(_)
                        | ValueBalanceError::NsmValueBalance(_)
                        | ValueBalanceError::Tachyon(_))
                 ))
        }
    }

    #[test]
    fn value_balance_serialization(value_balance in any::<ValueBalance<NonNegative>>()) {
        let _init_guard = zakura_test::init();

        let serialized_value_balance = ValueBalance::from_bytes(&value_balance.to_bytes())?;

        prop_assert_eq!(value_balance, serialized_value_balance);
    }

    #[test]
    fn value_balance_deserialization(bytes in any::<[u8; 56]>()) {
        let _init_guard = zakura_test::init();

        if let Ok(deserialized) = ValueBalance::<NonNegative>::from_bytes(&bytes) {
            prop_assert_eq!(bytes, &deserialized.to_bytes()[..56]);
        }
    }

    /// The legacy version of [`ValueBalance`] had 32 bytes compared to the current 56 bytes,
    /// but it's possible to correctly instantiate the current version of [`ValueBalance`] from
    /// the legacy format, so we test if Zebra can still deserialize the legacy format.
    #[test]
    fn legacy_value_balance_deserialization(bytes in any::<[u8; 32]>()) {
        let _init_guard = zakura_test::init();

        if let Ok(deserialized) = ValueBalance::<NonNegative>::from_bytes(&bytes) {
            let deserialized = deserialized.to_bytes();
            let mut extended_bytes = [0u8; VALUE_BALANCE_BYTES];
            extended_bytes[..32].copy_from_slice(&bytes);
            prop_assert_eq!(extended_bytes, deserialized);
        }
    }

    /// The previous version of [`ValueBalance`] had 40 bytes, with deferred
    /// value stored immediately after Orchard. The current version appends
    /// Ironwood after deferred, so the previous format remains a prefix of the
    /// current format.
    #[test]
    fn pre_ironwood_value_balance_deserialization(bytes in any::<[u8; 40]>()) {
        let _init_guard = zakura_test::init();

        if let Ok(deserialized) = ValueBalance::<NonNegative>::from_bytes(&bytes) {
            let deserialized = deserialized.to_bytes();
            let mut extended_bytes = [0u8; VALUE_BALANCE_BYTES];
            extended_bytes[..40].copy_from_slice(&bytes);
            prop_assert_eq!(extended_bytes, deserialized);
        }
    }

    /// The pre-Tachyon value balance was a 56-byte prefix of the current format.
    #[test]
    #[cfg(zcash_unstable = "nutachyon")]
    fn pre_tachyon_value_balance_deserialization(bytes in any::<[u8; 56]>()) {
        let _init_guard = zakura_test::init();

        if let Ok(deserialized) = ValueBalance::<NonNegative>::from_bytes(&bytes) {
            let deserialized = deserialized.to_bytes();
            let mut extended_bytes = [0u8; VALUE_BALANCE_BYTES];
            extended_bytes[..56].copy_from_slice(&bytes);
            prop_assert_eq!(extended_bytes, deserialized);
        }
    }

}

// Partition the cap across all six monetary pools and vary NSM independently.
proptest! {
    #[test]
    fn max_money_total_bounds_random_pool_distributions(
        cuts in prop::array::uniform5(0i64..=MAX_MONEY),
        nsm in prop_oneof![Just(0), Just(1), Just(MAX_MONEY - 1), Just(MAX_MONEY), 0i64..=MAX_MONEY],
    ) {
        let mut cuts = cuts;
        cuts.sort_unstable();
        let mut edges = [0; 7];
        edges[1..6].copy_from_slice(&cuts);
        edges[6] = MAX_MONEY;
        let amounts: Vec<_> = edges.windows(2)
            .map(|pair| Amount::<NonNegative>::try_from(pair[1] - pair[0]).unwrap())
            .collect();
        let pools = ValueBalance {
            transparent: amounts[0], sprout: amounts[1], sapling: amounts[2],
            orchard: amounts[3], deferred: amounts[4], ironwood: amounts[5],
            nsm_value_balance: Amount::try_from(nsm).unwrap(),
            #[cfg(zcash_unstable = "nutachyon")]
            tachyon: Amount::zero(),
        };
        prop_assert_eq!(i64::from(pools.total().unwrap()), MAX_MONEY);
        prop_assert_eq!(pools.add_chain_value_pool_change(ValueBalance::zero()), Ok(pools));
        for (index, amount) in amounts.iter().enumerate() {
            for delta in [-1i64, 0, 1] {
                let mut change = ValueBalance::<NegativeAllowed>::zero();
                let slots = [&mut change.transparent, &mut change.sprout, &mut change.sapling,
                    &mut change.orchard, &mut change.deferred, &mut change.ironwood];
                *slots.into_iter().nth(index).unwrap() = Amount::try_from(delta).unwrap();
                let result = pools.add_chain_value_pool_change(change);
                let individual = i64::from(*amount) + delta;
                if !(0..=MAX_MONEY).contains(&individual) {
                    prop_assert!(result.is_err());
                } else if delta > 0 {
                    prop_assert!(matches!(result, Err(ValueBalanceError::Total(_))));
                } else {
                    let updated = result.unwrap();
                    prop_assert_eq!(i64::from(updated.total().unwrap()), MAX_MONEY + delta);
                    prop_assert_eq!(updated.nsm_value_balance_amount(), pools.nsm_value_balance_amount());
                    prop_assert_eq!(updated.add_chain_value_pool_change(-change), Ok(pools));
                }
            }
        }
    }

    #[test]
    fn max_money_transfers_preserve_supply_and_reverse(
        value in prop_oneof![Just(0), Just(1), Just(MAX_MONEY - 1), Just(MAX_MONEY), 0i64..=MAX_MONEY],
        source in 0usize..6,
        destination_offset in 1usize..6,
        nsm in prop_oneof![Just(0), Just(1), Just(MAX_MONEY - 1), Just(MAX_MONEY), 0i64..=MAX_MONEY],
    ) {
        let destination = (source + destination_offset) % 6;
        let mut pools = ValueBalance::<NonNegative>::zero();
        let slots = [&mut pools.transparent, &mut pools.sprout, &mut pools.sapling,
            &mut pools.orchard, &mut pools.deferred, &mut pools.ironwood];
        *slots.into_iter().nth(source).unwrap() = Amount::try_from(MAX_MONEY).unwrap();
        pools.set_nsm_value_balance_amount(Amount::try_from(nsm).unwrap());
        let mut change = ValueBalance::<NegativeAllowed>::zero();
        for (index, slot) in [&mut change.transparent, &mut change.sprout, &mut change.sapling,
            &mut change.orchard, &mut change.deferred, &mut change.ironwood].into_iter().enumerate() {
            *slot = Amount::try_from(if index == source { -value } else if index == destination { value } else { 0 }).unwrap();
        }
        let updated = pools.add_chain_value_pool_change(change).unwrap();
        for (index, amount) in [updated.transparent, updated.sprout, updated.sapling,
            updated.orchard, updated.deferred, updated.ironwood].into_iter().enumerate() {
            let expected = if index == source { MAX_MONEY - value }
                else if index == destination { value } else { 0 };
            prop_assert_eq!(i64::from(amount), expected);
        }
        prop_assert_eq!(updated.total(), pools.total());
        prop_assert_eq!(updated.nsm_value_balance_amount(), pools.nsm_value_balance_amount());
        prop_assert_eq!(updated.add_chain_value_pool_change(-change), Ok(pools));
    }

    #[test]
    fn max_money_nsm_has_an_independent_upper_bound(monetary in 0i64..=MAX_MONEY) {
        let mut pools = ValueBalance::from_transparent_amount(Amount::<NonNegative>::try_from(monetary).unwrap());
        pools.set_nsm_value_balance_amount(Amount::try_from(MAX_MONEY).unwrap());
        let mut change = ValueBalance::zero();
        change.set_nsm_value_balance_amount(Amount::try_from(1).unwrap());
        prop_assert!(matches!(pools.add_chain_value_pool_change(change), Err(ValueBalanceError::NsmValueBalance(_))));
        prop_assert_eq!(pools.add_chain_value_pool_change(ValueBalance::zero()), Ok(pools));
        prop_assert_eq!(i64::from(pools.total().unwrap()), monetary);
    }
}
