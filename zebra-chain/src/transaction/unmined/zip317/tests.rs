//! ZIP-317 tests.

#[cfg(zcash_unstable = "nu6.3")]
use super::conventional_actions;
use super::{mempool_checks, Amount, Error};

#[test]
fn zip317_unpaid_actions_err() {
    let check = mempool_checks(1, Amount::try_from(1).unwrap(), 1);

    assert!(check.is_err());
    assert_eq!(check.err(), Some(Error::UnpaidActions));
}

#[test]
fn zip317_minimum_rate_fee_err() {
    let check = mempool_checks(0, Amount::try_from(1).unwrap(), 1000);

    assert!(check.is_err());
    assert_eq!(check.err(), Some(Error::FeeBelowMinimumRate));
}

#[test]
fn zip317_mempool_checks_ok() {
    assert!(mempool_checks(0, Amount::try_from(100).unwrap(), 1000).is_ok())
}

#[test]
#[cfg(zcash_unstable = "nu6.3")]
fn zip317_counts_ironwood_actions() {
    use proptest::{
        prelude::any,
        strategy::{Strategy, ValueTree},
        test_runner::TestRunner,
    };

    use crate::{
        amount::{Amount, NegativeAllowed},
        at_least_one, ironwood,
        parameters::NetworkUpgrade,
        primitives::Halo2Proof,
        transaction::{LockTime, Transaction},
    };

    let mut runner = TestRunner::default();
    let action = any::<ironwood::AuthorizedAction>()
        .new_tree(&mut runner)
        .expect("test action strategy creates a value")
        .current();
    let ironwood_shielded_data = ironwood::ShieldedData {
        flags: ironwood::Flags::ENABLE_SPENDS | ironwood::Flags::ENABLE_OUTPUTS,
        value_balance: Amount::<NegativeAllowed>::zero(),
        shared_anchor: ironwood::tree::Root::default(),
        proof: Halo2Proof(vec![]),
        actions: at_least_one![action; 3],
        binding_sig: [0u8; 64].into(),
    };
    let transaction = Transaction::V6 {
        network_upgrade: NetworkUpgrade::Nu6_3,
        lock_time: LockTime::unlocked(),
        expiry_height: crate::block::Height(1),
        inputs: Vec::new(),
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: Some(ironwood_shielded_data),
    };

    assert_eq!(conventional_actions(&transaction), 3);
}
