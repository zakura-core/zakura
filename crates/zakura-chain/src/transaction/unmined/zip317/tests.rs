//! ZIP-317 tests.

use super::{
    conventional_actions, conventional_fee, conventional_fee_weight_ratio, mempool_checks,
    unpaid_actions, Amount, Error,
};

use crate::{
    block::Height,
    parameters::NetworkUpgrade,
    transaction::{LockTime, Transaction, UnminedTx},
};

#[test]
fn mempool_fee_floor_is_400_per_action() {
    let transaction = UnminedTx::from(Transaction::V5 {
        network_upgrade: NetworkUpgrade::Nu5,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(1),
        inputs: Vec::new(),
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    });

    assert_eq!(conventional_actions(&transaction.transaction), 2);
    assert_eq!(
        conventional_fee(&transaction.transaction),
        Amount::try_from(10_000).unwrap()
    );
    assert_eq!(
        mempool_checks(&transaction, Amount::try_from(799).unwrap()),
        Err(Error::FeeBelowMinimumRate)
    );
    assert!(mempool_checks(&transaction, Amount::try_from(800).unwrap()).is_ok());
    assert_eq!(
        unpaid_actions(&transaction, Amount::try_from(800).unwrap()),
        2
    );
}

#[test]
fn zip317_caps_weight_ratio_at_ten() {
    let transaction = UnminedTx::from(Transaction::V5 {
        network_upgrade: NetworkUpgrade::Nu5,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(1),
        inputs: Vec::new(),
        outputs: Vec::new(),
        sapling_shielded_data: None,
        orchard_shielded_data: None,
    });

    let miner_fee = Amount::try_from(200_000).expect("fee is a valid amount");

    assert_eq!(conventional_fee_weight_ratio(&transaction, miner_fee), 10.0);
}

#[test]
fn zip317_counts_ironwood_actions() {
    use proptest::{
        prelude::any,
        strategy::{Strategy, ValueTree},
        test_runner::TestRunner,
    };

    use crate::{
        amount::{Amount, NegativeAllowed},
        at_least_one, ironwood,
        primitives::Halo2Proof,
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

    let transaction = UnminedTx::from(transaction);
    assert_eq!(
        mempool_checks(&transaction, Amount::try_from(1_199).unwrap()),
        Err(Error::FeeBelowMinimumRate)
    );
    assert!(mempool_checks(&transaction, Amount::try_from(1_200).unwrap()).is_ok());
}
