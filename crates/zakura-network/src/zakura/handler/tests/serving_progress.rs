//! Large block fixtures shared by the matched QUIC download tests.

use super::*;
use zakura_chain::{serialization::ZcashSerialize, transparent};

pub(super) fn large_block_template() -> Arc<Block> {
    let mut block =
        Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES[..]).unwrap();
    let transaction = block.transactions[0].clone();
    let tx_bytes = transaction.zcash_serialized_size();
    block.transactions = vec![transaction; 1_901_000 / tx_bytes];
    Arc::new(block)
}

pub(super) fn block_at_height(template: &Arc<Block>, height: block::Height) -> Arc<Block> {
    let mut block = template.as_ref().clone();
    let mut coinbase = block.transactions[0].as_ref().clone();
    let inputs = match &mut coinbase {
        transaction::Transaction::V1 { inputs, .. }
        | transaction::Transaction::V2 { inputs, .. }
        | transaction::Transaction::V3 { inputs, .. }
        | transaction::Transaction::V4 { inputs, .. }
        | transaction::Transaction::V5 { inputs, .. }
        | transaction::Transaction::V6 { inputs, .. } => inputs,
    };
    let transparent::Input::Coinbase {
        height: coinbase_height,
        ..
    } = &mut inputs[0]
    else {
        panic!("fixture has a coinbase input");
    };
    *coinbase_height = height;
    block.transactions[0] = Arc::new(coinbase);
    Arc::new(block)
}
