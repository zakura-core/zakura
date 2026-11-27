//! The coinbase accessors of v7 transactions.

use std::sync::Arc;

use crate::{
    block::{self, Block, Header},
    serialization::ZcashSerialize,
    transaction::Transaction,
    transparent,
};

use super::{
    decode, rejection::coinbase_payload, transparent_effects, transparent_envelope, Envelope,
};

/// The encoding of the null previous output.
fn null_outpoint() -> [u8; 36] {
    transparent::OutPoint::NULL
        .zcash_serialize_to_vec()
        .expect("an outpoint serializes to a vector")
        .try_into()
        .expect("an outpoint encodes in 36 bytes")
}

/// A v7 coinbase transaction at `height` with a transparent output: its effects are the
/// transparent bundle, then the coinbase bundle.
pub(super) fn v7_coinbase_envelope(height: u32) -> Envelope {
    let mut envelope = Envelope::empty();
    let mut outputs_only = transparent_effects(null_outpoint(), 0);
    outputs_only.drain(..41);
    outputs_only.insert(0, 0);
    envelope.effects.push((0, 0, outputs_only));
    envelope
        .effects
        .push((1, 0, coinbase_payload(height, 10, 0, b"miner", &[], None)));
    envelope
}

fn v7_coinbase(height: u32) -> Transaction {
    decode(&v7_coinbase_envelope(height).encode()).expect("a valid v7 coinbase transaction")
}

#[test]
fn v7_coinbase_bundle_makes_a_coinbase_transaction() {
    let _init_guard = zakura_test::init();

    let tx = v7_coinbase(1234);
    assert!(tx.is_coinbase());
    assert!(!tx.is_valid_non_coinbase());
    assert_eq!(tx.coinbase_height(), Some(block::Height(1234)));
    let coinbase = tx.v7_coinbase().expect("the coinbase bundle");
    assert_eq!(coinbase.coinbase_data(), b"miner");
    assert!(tx.inputs().is_empty());
}

#[test]
fn v7_null_prevout_is_a_prevout_input() {
    let _init_guard = zakura_test::init();

    let tx = decode(&transparent_envelope(null_outpoint()).encode())
        .expect("a v7 transaction spending the null previous output decodes");
    let [input] = tx.inputs() else {
        panic!("one input");
    };
    assert!(matches!(input, transparent::Input::PrevOut { .. }));
    assert_eq!(input.outpoint(), Some(transparent::OutPoint::NULL));
    assert!(!tx.is_coinbase());
    assert!(tx.is_valid_non_coinbase());
    assert_eq!(tx.coinbase_height(), None);
    assert_eq!(tx.v7_coinbase(), None);
}

#[test]
fn block_coinbase_height_reads_the_v7_coinbase_bundle() {
    let _init_guard = zakura_test::init();

    let header: Header = crate::serialization::ZcashDeserializeInto::zcash_deserialize_into(
        &zakura_test::vectors::DUMMY_HEADER[..],
    )
    .expect("the dummy header decodes");
    let block = Block {
        header: Arc::new(header),
        transactions: vec![Arc::new(v7_coinbase(77))],
    };
    assert_eq!(block.coinbase_height(), Some(block::Height(77)));
}

#[test]
fn pre_v7_coinbase_height_is_unchanged() {
    let _init_guard = zakura_test::init();

    for (height, bytes) in zakura_test::vectors::MAINNET_BLOCKS.iter().take(20) {
        let block: Block =
            crate::serialization::ZcashDeserializeInto::zcash_deserialize_into(&bytes[..])
                .expect("test blocks decode");
        let coinbase = &block.transactions[0];
        assert_eq!(coinbase.coinbase_height(), Some(block::Height(*height)));
        assert_eq!(block.coinbase_height(), Some(block::Height(*height)));
        assert_eq!(
            block.transactions[1..]
                .iter()
                .find_map(|tx| tx.coinbase_height()),
            None
        );
    }
}
