//! Kept block encodings match what serializing the block produces.

use std::sync::Arc;

use crate::{
    block::{Block, BlockEncoding},
    serialization::{ZcashDeserializeInto, ZcashSerialize},
};

/// Every block test vector, from both networks.
fn vector_blocks() -> impl Iterator<Item = &'static [u8]> {
    zakura_test::vectors::MAINNET_BLOCKS
        .values()
        .chain(zakura_test::vectors::TESTNET_BLOCKS.values())
        .copied()
}

#[test]
fn decoded_blocks_keep_their_exact_encoding() {
    let _init_guard = zakura_test::init();

    for bytes in vector_blocks() {
        let expected: Block = bytes
            .zcash_deserialize_into()
            .expect("vectors are valid blocks");

        // A buffer can hold bytes before the block, such as a message type.
        let mut buffer = vec![0xa5];
        buffer.extend_from_slice(bytes);
        let (block, encoding) =
            BlockEncoding::decode(buffer.into(), 1).expect("vectors are valid blocks");
        let encoding = encoding.expect("vectors re-encode to their bytes");

        assert_eq!(block, expected);
        assert_eq!(encoding.as_bytes(), bytes);
        assert_eq!(
            encoding.as_bytes(),
            block.zcash_serialize_to_vec().expect("vec writes succeed"),
        );
        assert_eq!(encoding.len(), block.zcash_serialized_size());
        assert_eq!(encoding.transaction_count(), block.transactions.len());
        for (transaction, transaction_bytes) in
            block.transactions.iter().zip(encoding.transactions_bytes())
        {
            assert_eq!(
                transaction_bytes,
                transaction
                    .zcash_serialize_to_vec()
                    .expect("vec writes succeed"),
            );
        }
        assert_eq!(encoding.transaction_bytes(block.transactions.len()), None);
        assert_eq!(
            encoding.transaction_hashes(&block),
            block.transaction_hashes()
        );
    }
}

#[test]
fn an_encoding_must_match_its_block_byte_for_byte() {
    let _init_guard = zakura_test::init();

    let bytes = zakura_test::vectors::BLOCK_MAINNET_419201_BYTES.as_slice();
    let block: Block = bytes
        .zcash_deserialize_into()
        .expect("vectors are valid blocks");

    // The same length with one changed byte is not this block's encoding.
    for index in [0, bytes.len() / 2, bytes.len() - 1] {
        let mut changed = bytes.to_vec();
        changed[index] ^= 1;
        let length = changed.len();
        assert!(BlockEncoding::for_block(&block, Arc::from(changed), 0..length).is_none());
    }

    // Neither are fewer or more bytes, or a range outside the buffer.
    let buffer: Arc<[u8]> = Arc::from([bytes, &[0][..]].concat());
    assert!(BlockEncoding::for_block(&block, buffer.clone(), 0..bytes.len() - 1).is_none());
    assert!(BlockEncoding::for_block(&block, buffer.clone(), 0..bytes.len() + 1).is_none());
    assert!(BlockEncoding::for_block(&block, buffer.clone(), 0..bytes.len() + 2).is_none());
    assert!(BlockEncoding::for_block(&block, buffer, 0..bytes.len()).is_some());

    // Decoding rejects bytes after the block.
    assert!(BlockEncoding::decode(Arc::from([bytes, &[0][..]].concat()), 0).is_err());
}
