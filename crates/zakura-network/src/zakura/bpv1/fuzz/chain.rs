//! Post-NU7 regtest blocks for the BPv1 tests and fuzz drivers.
//!
//! The proposer and receivers check a block's coinbase commitment against
//! its header, so a driver that proposes needs real v6 transactions under a
//! header whose Merkle root and block commitment cover them.

use std::sync::Arc;

use zakura_chain::{
    amount::Amount,
    block::{
        genesis::regtest_genesis_block, merkle::AuthDataRoot, Block,
        ChainHistoryBlockTxAuthCommitmentHash, ChainHistoryMmrRootHash, Height,
    },
    parameters::{testnet::ConfiguredActivationHeights, Network, NetworkUpgrade},
    transaction::{LockTime, Transaction},
    transparent::{self, Output, Script},
};

/// Regtest with NU7 active from height 1.
pub(crate) fn regtest_nu7() -> Network {
    regtest_nu7_at(1)
}

/// Regtest with NU7 active from `height`.
pub(crate) fn regtest_nu7_at(height: u32) -> Network {
    Network::new_regtest(
        ConfiguredActivationHeights {
            nu7: Some(height),
            ..Default::default()
        }
        .into(),
    )
}

/// A synthetic non-coinbase v6 transaction with an identifying tag.
pub(crate) fn build_v6_tx(height: Height, tag: u8) -> Transaction {
    v6_tx(height, tag, 1_000, &[0x6a, 0x01, tag])
}

/// A v6 transaction at `height` spending a `tag`-derived outpoint into one
/// output paying `value` to `script`.
pub(crate) fn v6_tx(height: Height, tag: u8, value: i64, script: &[u8]) -> Transaction {
    Transaction::V6 {
        network_upgrade: NetworkUpgrade::Nu7,
        lock_time: LockTime::min_lock_time_timestamp(),
        expiry_height: height,
        inputs: vec![transparent::Input::PrevOut {
            outpoint: transparent::OutPoint {
                hash: zakura_chain::transaction::Hash([tag; 32]),
                index: 0,
            },
            unlock_script: Script::new(&[0x51]),
            sequence: u32::MAX,
        }],
        outputs: vec![Output::new(
            Amount::try_from(value).expect("valid test zatoshi"),
            Script::new(script),
        )],
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
    }
}

/// A post-NU7 v6 coinbase paying `value` to `script`, then `extra_outputs`,
/// with `input_extra` appended to the coinbase data.
pub(crate) fn build_v6_coinbase(
    height: Height,
    script: &[u8],
    value: i64,
    extra_outputs: Vec<Output>,
    input_extra: &[u8],
) -> Transaction {
    let mut outputs = vec![Output::new(
        Amount::try_from(value).expect("valid test zatoshi"),
        Script::new(script),
    )];
    outputs.extend(extra_outputs);
    let mut data = vec![0x01, 0x01];
    data.extend_from_slice(input_extra);
    Transaction::V6 {
        network_upgrade: NetworkUpgrade::Nu7,
        lock_time: LockTime::min_lock_time_timestamp(),
        expiry_height: height,
        inputs: vec![transparent::Input::Coinbase {
            height,
            data,
            sequence: u32::MAX,
        }],
        outputs,
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
    }
}

/// A v6 coinbase whose second output carries `commit_script`.
pub(crate) fn committed_coinbase(height: Height, commit_script: &[u8]) -> Transaction {
    build_v6_coinbase(
        height,
        &[0x51],
        1_000_000,
        vec![Output::new(
            Amount::try_from(0).expect("valid test zatoshi"),
            Script::new(commit_script),
        )],
        &[],
    )
}

/// A block holding `txs` under a genesis-derived header whose Merkle root and
/// block commitment cover them and the light-client root `lcr`.
pub(crate) fn block_of(lcr: [u8; 32], txs: Vec<Arc<Transaction>>) -> Block {
    let block_commitment = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
        &ChainHistoryMmrRootHash::from(lcr),
        &txs.iter().map(|t| t.as_ref()).collect::<AuthDataRoot>(),
    );
    let mut header = *regtest_genesis_block().header;
    header.merkle_root = txs.iter().map(|t| t.as_ref()).collect();
    *header.commitment_bytes = <[u8; 32]>::from(block_commitment);
    Block {
        header: Arc::new(header),
        transactions: txs,
    }
}
