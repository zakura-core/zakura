use super::*;
use crate::zakura::block_sync::reorder::BufferedBlockBody;
use zakura_chain::{serialization::CompactSizeMessage, transaction::Transaction};

fn block_with_many_transactions() -> Arc<block::Block> {
    let mut block =
        block::Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES[..]).unwrap();
    // Parsing accepts empty V1 transactions. Consensus validation is separate.
    let transaction = Transaction::zcash_deserialize(&[1, 0, 0, 0, 0, 0, 0, 0, 0, 0][..]).unwrap();
    block.transactions = vec![Arc::new(transaction); 1_025];
    Arc::new(block)
}

#[test]
fn receipt_bounds_transaction_allocation_by_the_declared_count() {
    let block = block_with_many_transactions();
    let frame = BlockSyncMessage::Block(block.clone())
        .encode_frame()
        .unwrap();
    let BlockSyncMessage::Block(decoded) = BlockSyncMessage::decode_frame(frame).unwrap() else {
        panic!("the frame contains a block");
    };
    assert_eq!(decoded, block);
    assert_eq!(decoded.transactions.capacity(), decoded.transactions.len());

    let mut payload = vec![MSG_BS_BLOCK];
    block.header.zcash_serialize(&mut payload).unwrap();
    CompactSizeMessage::try_from(5_000)
        .unwrap()
        .zcash_serialize(&mut payload)
        .unwrap();
    let (result, allocations) =
        zakura_test::allocations::measure(|| BlockSyncMessage::decode(&payload));
    assert!(result.is_err());
    assert!(
        allocations.largest_request < 1_024 * std::mem::size_of::<Arc<Transaction>>(),
        "{allocations:?}"
    );
}

#[test]
fn buffered_replay_keeps_the_same_transaction_allocation_bound() {
    let expected = block_with_many_transactions();
    let frame = BlockSyncMessage::Block(expected.clone())
        .encode_frame()
        .unwrap();
    let (BlockSyncMessage::Block(decoded), Some(raw)) =
        BlockSyncMessage::decode_frame_with_raw_block_payload(frame).unwrap()
    else {
        panic!("a block frame retains its original bytes");
    };
    let mut body = BufferedBlockBody::from_decoded_block(decoded, Some(raw));
    body.retain_for_backlog_in_place();
    assert!(!body.is_decoded());
    let replayed = body.decoded_block();
    assert_eq!(replayed, expected);
    assert_eq!(
        replayed.transactions.capacity(),
        replayed.transactions.len()
    );
}
