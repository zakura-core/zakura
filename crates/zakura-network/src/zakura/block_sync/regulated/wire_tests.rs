//! Exercise both the bounded envelope and the full production block decoder.

use proptest::prelude::*;
use zakura_chain::{
    block,
    serialization::{CompactSizeMessage, ZcashDeserialize, ZcashSerialize},
};

use super::wire::{Message, Range};
use crate::zakura::{
    block_sync::{BlockSyncMessage, BlockSyncStatus},
    wire_codec::{decode_frame, encode_frame, message_suite::decode_checked},
};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn stream_six_messages_round_trip_and_reject_trailing_bytes(
        kind in 1u16..=5,
        start in 0u32..1_000_000,
        count in 1u32..=128,
        extra in proptest::collection::vec(any::<u8>(), 1..32),
    ) {
        let range = Range::new(block::Height(start), count).unwrap();
        let message = match kind {
            1 => Message::Status(BlockSyncStatus {
                servable_low: range.start,
                servable_high: block::Height(start + count),
                ..Default::default()
            }),
            2 => Message::GetBlocks(range),
            3 => Message::Block(zakura_test::vectors::BLOCK_MAINNET_1_BYTES.to_vec()),
            4 => Message::BlocksDone { start: range.start, returned: count },
            _ => Message::RangeUnavailable(range),
        };
        let frame = encode_frame(&message).unwrap();
        prop_assert_eq!(decode_frame::<Message>(&frame).unwrap(), message);
        let decoded = BlockSyncMessage::decode_frame(frame.clone()).unwrap();
        prop_assert_eq!(decoded.encode_frame().unwrap(), frame.clone());
        let mut trailing = frame;
        trailing.payload.extend(extra);
        prop_assert!(BlockSyncMessage::decode_frame(trailing).is_err());
    }

    #[test]
    fn hostile_stream_six_payloads_do_not_panic_or_exceed_envelope_allocations(
        kind in 1u16..=5,
        body in proptest::collection::vec(any::<u8>(), 0..4_096),
    ) {
        let mut payload = vec![u8::try_from(kind).unwrap()];
        payload.extend(body);
        let frame = crate::zakura::Frame { message_type: kind, flags: 0, payload: payload.clone() };
        if let Ok(message) = decode_checked::<Message>(&frame) {
            prop_assert_eq!(encode_frame(&message).unwrap().payload, payload.clone());
        }
        // This also exercises nested block decoding when the discriminator is 3.
        let _ = BlockSyncMessage::decode(&payload);
    }

    #[test]
    fn hostile_block_counts_are_rejected_before_transaction_allocation(
        count in 1_025usize..100_000,
        tail in proptest::collection::vec(any::<u8>(), 0..10),
    ) {
        let block = block::Block::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES[..]).unwrap();
        let mut payload = vec![3];
        block.header.zcash_serialize(&mut payload).unwrap();
        CompactSizeMessage::try_from(count).unwrap().zcash_serialize(&mut payload).unwrap();
        payload.extend(tail);
        let (result, allocations) = zakura_test::allocations::measure(|| BlockSyncMessage::decode(&payload));
        prop_assert!(result.is_err());
        prop_assert!(allocations.largest_request < 8_192, "{:?}", allocations);
    }
}
