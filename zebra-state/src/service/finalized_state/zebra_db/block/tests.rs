//! Tests for finalized database blocks and transactions.

#![allow(clippy::unwrap_in_result)]

use std::collections::HashMap;

use zebra_chain::{
    amount::{Amount, NonNegative},
    block::Height,
    parameters::Network,
    transaction,
    transparent::{self, Output, Script},
};

use crate::{
    constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
    service::finalized_state::{
        disk_format::transparent::OutputLocation, ZebraDb, STATE_COLUMN_FAMILIES_IN_CODE,
    },
    Config,
};

mod common;
mod header_store_coherence;
mod prune;
mod snapshot;
mod vectors;

#[test]
fn read_spent_utxo_uses_new_outputs_for_same_block_spends() {
    let _init_guard = zebra_test::init();

    let network = Network::Mainnet;
    let db = ZebraDb::new(
        &Config::ephemeral(),
        STATE_DATABASE_KIND,
        &state_database_format_version_in_code(),
        &network,
        true,
        STATE_COLUMN_FAMILIES_IN_CODE
            .iter()
            .map(ToString::to_string),
        false,
    )
    .expect("opening the finalized state database should succeed");

    let height = Height(7);
    let tx_hash = transaction::Hash([0x11; 32]);
    let outpoint = transparent::OutPoint {
        hash: tx_hash,
        index: 2,
    };
    let tx_index_in_block = 3;
    let value = Amount::<NonNegative>::try_from(1_i64).expect("positive zatoshi amount is valid");
    let ordered_utxo = transparent::OrderedUtxo::new(
        Output::new(value, Script::new(&[])),
        height,
        tx_index_in_block,
    );

    let tx_hash_indexes = HashMap::from([(tx_hash, tx_index_in_block)]);
    let new_outputs = HashMap::from([(outpoint, ordered_utxo.clone())]);

    let (read_outpoint, output_location, utxo) =
        super::read_spent_utxo(&db, height, outpoint, &tx_hash_indexes, &new_outputs);

    assert_eq!(read_outpoint, outpoint);
    assert_eq!(
        output_location,
        OutputLocation::from_usize(
            height,
            tx_index_in_block,
            usize::try_from(outpoint.index).expect("test output index fits in usize"),
        )
    );
    assert_eq!(utxo, ordered_utxo.utxo);
}
