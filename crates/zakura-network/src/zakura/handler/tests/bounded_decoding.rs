use super::*;
use crate::zakura::legacy_gossip::bounded_decoding::{truncated_block, truncated_transaction};
use zakura_chain::{primitives::Bctv14Proof, sprout::JoinSplit, transaction::Transaction};
use zakura_test::vectors::{BLOCK_MAINNET_1_BYTES, GENERIC_TESTNET_TX};

/// Validate `item` as a single final response chunk for request 7.
fn validate_item(
    kind: LegacyResponseKind,
    message_type: u16,
    item: &[u8],
) -> Result<(), OutboundRequestError> {
    let mut state = LegacyResponseReadState::new(LegacyResponseBudget {
        kind,
        max_items: 1,
        max_frames: 1,
        max_bytes: MAX_PROTOCOL_MESSAGE_LEN,
        max_message_bytes: MAX_PROTOCOL_MESSAGE_LEN,
    });
    let mut payload = 7u64.to_le_bytes().to_vec();
    payload.push(1);
    payload.extend_from_slice(item);
    let frame = Frame {
        message_type,
        flags: 0,
        payload,
    };
    state.validate_frame(7, &frame)
}

#[test]
fn completed_transaction_bounds_allocation_by_item_length() {
    let kind = LegacyResponseKind::Transactions;
    validate_item(kind, LEGACY_RESPONSE_TRANSACTION, &GENERIC_TESTNET_TX).unwrap();

    let item = truncated_transaction();
    let (result, allocations) = zakura_test::allocations::measure(|| {
        validate_item(kind, LEGACY_RESPONSE_TRANSACTION, &item)
    });
    assert!(result.is_err());
    assert!(
        allocations.largest_request < 1_024 * std::mem::size_of::<JoinSplit<Bctv14Proof>>(),
        "{allocations:?}"
    );
}

#[test]
fn completed_block_bounds_allocation_by_item_length() {
    let kind = LegacyResponseKind::Blocks;
    validate_item(kind, LEGACY_RESPONSE_BLOCK, &BLOCK_MAINNET_1_BYTES).unwrap();

    let item = truncated_block();
    let (result, allocations) =
        zakura_test::allocations::measure(|| validate_item(kind, LEGACY_RESPONSE_BLOCK, &item));
    assert!(result.is_err());
    assert!(
        allocations.largest_request < 1_024 * std::mem::size_of::<Arc<Transaction>>(),
        "{allocations:?}"
    );
}
