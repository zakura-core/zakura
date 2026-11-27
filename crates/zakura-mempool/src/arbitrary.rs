//! Generators for the storage property tests.

use proptest::prelude::*;

use zakura_chain::{
    amount::{Amount, NonNegative},
    parameters::NetworkKind,
    transaction::{Transaction, UnminedTx, VerifiedUnminedTx},
    transparent::{self, Address},
};

/// Return a [`VerifiedUnminedTx`] strategy with outputs and inputs adjusted to pass standardness.
pub fn standard_verified_unmined_tx_strategy() -> BoxedStrategy<VerifiedUnminedTx> {
    any::<Transaction>()
        .prop_map(|mut transaction| {
            standardize_transaction(&mut transaction);

            let unmined_tx = UnminedTx::from(transaction);
            let miner_fee = unmined_tx.conventional_fee();

            VerifiedUnminedTx::new(unmined_tx, miner_fee, 0, 0, std::sync::Arc::new(vec![]))
                .expect("standardized transaction should pass ZIP-317 checks")
        })
        .boxed()
}

/// Adjusts transaction data to pass mempool standardness checks.
pub fn standardize_transaction(transaction: &mut Transaction) {
    match transaction {
        Transaction::V2 { joinsplit_data, .. } | Transaction::V3 { joinsplit_data, .. } => {
            *joinsplit_data = None;
        }
        Transaction::V4 { joinsplit_data, .. } => *joinsplit_data = None,
        Transaction::V1 { .. }
        | Transaction::V5 { .. }
        | Transaction::V6 { .. }
        | Transaction::V7 { .. } => {}
    }

    let lock_script = standard_lock_script();
    let output_value = Amount::<NonNegative>::try_from(10_000).expect("valid amount");

    for input in transaction.inputs_mut() {
        if let transparent::Input::PrevOut { unlock_script, .. } = input {
            *unlock_script = transparent::Script::new(&[]);
        }
    }

    for output in transaction.outputs_mut() {
        output.lock_script = lock_script.clone();
        output.value = output_value;
    }
}

/// A P2PKH lock script, which is standard.
fn standard_lock_script() -> transparent::Script {
    Address::from_pub_key_hash(NetworkKind::Mainnet, [0u8; 20]).script()
}
