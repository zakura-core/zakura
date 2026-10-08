use super::*;
use zakura_chain::{primitives::Bctv14Proof, sprout::JoinSplit};
use zakura_test::vectors::{BLOCK_MAINNET_1_BYTES, GENERIC_TESTNET_TX};

/// A 13-byte V2 transaction: version, no transparent inputs or outputs, lock
/// time, then a count of 1,024 JoinSplits that are missing.
pub(crate) fn truncated_transaction() -> Vec<u8> {
    let mut bytes = vec![2, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    CompactSizeMessage::try_from(1_024)
        .unwrap()
        .zcash_serialize(&mut bytes)
        .unwrap();
    bytes
}

/// A block header and one transaction, which is [`truncated_transaction`].
pub(crate) fn truncated_block() -> Vec<u8> {
    let block = Block::zcash_deserialize(&BLOCK_MAINNET_1_BYTES[..]).unwrap();
    let mut bytes = block.header.zcash_serialize_to_vec().unwrap();
    CompactSizeMessage::try_from(1)
        .unwrap()
        .zcash_serialize(&mut bytes)
        .unwrap();
    bytes.extend(truncated_transaction());
    bytes
}

/// A list payload declaring the maximum inventory count, with no items.
fn empty_max_inventory_list() -> Vec<u8> {
    let count = usize::try_from(MAX_TX_INV_IN_SENT_MESSAGE).unwrap();
    CompactSizeMessage::try_from(count)
        .unwrap()
        .zcash_serialize_to_vec()
        .unwrap()
}

/// Asserts that decoding reserved none of the list's declared capacity.
fn assert_small(allocations: zakura_test::allocations::AllocationStats) {
    assert!(allocations.largest_request < 4_096, "{allocations:?}");
}

/// A single final response chunk carrying `item` for request 7.
fn response_frame(message_type: u16, item: &[u8]) -> Frame {
    let mut payload = 7u64.to_le_bytes().to_vec();
    payload.push(1);
    payload.extend_from_slice(item);
    Frame {
        message_type,
        flags: 0,
        payload,
    }
}

#[test]
fn push_transaction_bounds_allocation_by_payload_length() {
    let transaction = Transaction::zcash_deserialize(&GENERIC_TESTNET_TX[..]).unwrap();
    let request = LegacyRequestFrame::PushTransaction(UnminedTx::from(transaction));
    let mut frame = request.encode_frame().unwrap();
    assert_eq!(
        LegacyRequestFrame::decode_frame(frame.clone()).unwrap(),
        request
    );
    frame.payload.push(0);
    assert!(matches!(
        LegacyRequestFrame::decode_frame(frame),
        Err(LegacyGossipError::TrailingBytes)
    ));

    let payload = truncated_transaction();
    assert_eq!(payload.len(), 13);
    let frame = Frame {
        message_type: MSG_REQUEST_PUSH_TRANSACTION,
        flags: 0,
        payload,
    };
    let (result, allocations) =
        zakura_test::allocations::measure(|| LegacyRequestFrame::decode_frame(frame));
    assert!(result.is_err());
    assert!(
        allocations.largest_request < 1_024 * std::mem::size_of::<JoinSplit<Bctv14Proof>>(),
        "{allocations:?}"
    );
}

#[test]
fn transaction_response_bounds_allocation_by_payload_length() {
    let transaction =
        UnminedTx::from(Transaction::zcash_deserialize(&GENERIC_TESTNET_TX[..]).unwrap());
    let frame = response_frame(MSG_RESPONSE_TRANSACTION, &GENERIC_TESTNET_TX);
    let Response::Transactions(transactions) =
        LegacyResponseCodec::decode_response(7, LegacyRequestKind::Transactions, vec![frame], None)
            .unwrap()
    else {
        panic!("a transaction request decodes to transactions");
    };
    assert!(matches!(
        transactions.as_slice(),
        [InventoryResponse::Available((received, None))] if *received == transaction
    ));

    let frame = response_frame(MSG_RESPONSE_TRANSACTION, &truncated_transaction());
    let (result, allocations) = zakura_test::allocations::measure(|| {
        LegacyResponseCodec::decode_response(7, LegacyRequestKind::Transactions, vec![frame], None)
    });
    assert!(result.is_err());
    assert!(
        allocations.largest_request < 1_024 * std::mem::size_of::<JoinSplit<Bctv14Proof>>(),
        "{allocations:?}"
    );
}

#[test]
fn block_response_bounds_allocation_by_payload_length() {
    let block = Block::zcash_deserialize(&BLOCK_MAINNET_1_BYTES[..]).unwrap();
    let frame = response_frame(MSG_RESPONSE_BLOCK, &BLOCK_MAINNET_1_BYTES);
    let Response::Blocks(blocks) =
        LegacyResponseCodec::decode_response(7, LegacyRequestKind::Blocks, vec![frame], None)
            .unwrap()
    else {
        panic!("a block request decodes to blocks");
    };
    assert!(matches!(
        blocks.as_slice(),
        [InventoryResponse::Available((received, None))] if **received == block
    ));

    let frame = response_frame(MSG_RESPONSE_BLOCK, &truncated_block());
    let (result, allocations) = zakura_test::allocations::measure(|| {
        LegacyResponseCodec::decode_response(7, LegacyRequestKind::Blocks, vec![frame], None)
    });
    assert!(result.is_err());
    assert!(
        allocations.largest_request < 1_024 * std::mem::size_of::<JoinSplit<Bctv14Proof>>(),
        "{allocations:?}"
    );
}

#[test]
fn transaction_advertisement_bounds_allocation_by_payload_length() {
    let frame = Frame {
        message_type: MSG_ADVERTISE_TX_IDS,
        flags: 0,
        payload: empty_max_inventory_list(),
    };
    let (result, allocations) =
        zakura_test::allocations::measure(|| LegacyGossipFrame::decode_frame(frame));
    assert!(result.is_err());
    assert_small(allocations);
}

#[test]
fn list_requests_bound_allocation_by_payload_length() {
    for message_type in [MSG_REQUEST_BLOCKS_BY_HASH, MSG_REQUEST_TRANSACTIONS_BY_ID] {
        let frame = Frame {
            message_type,
            flags: 0,
            payload: empty_max_inventory_list(),
        };
        let (result, allocations) =
            zakura_test::allocations::measure(|| LegacyRequestFrame::decode_frame(frame));
        assert!(result.is_err());
        assert_small(allocations);
    }
}
