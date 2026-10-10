//! Shared transport conformance with structurally valid stream-6 messages.
//! The production peer-routine QUIC test separately checks GetBlocks policy.

use std::sync::{Arc, OnceLock};
use zakura_chain::{
    block,
    serialization::{ZcashDeserialize, ZcashSerialize},
};

use super::wire::{Message, Range};
use crate::zakura::{
    block_sync::{service::block_sync_streams, tests::fake_block_at_height, BlockSyncStatus},
    testkit::stream_conformance::{
        stream_conformance_suite, StreamConformance, SubscriptionUpdate,
    },
    MessageRule,
};

#[derive(Debug)]
struct GetBlocksConformance;

impl StreamConformance for GetBlocksConformance {
    type Message = Message;

    fn message(row: &MessageRule, exchange: u32) -> Message {
        let start = block::Height(exchange.checked_add(1).unwrap());
        match row.message_type {
            1 => Message::Status(BlockSyncStatus::default()),
            2 => Message::GetBlocks(Range::new(start, 1).unwrap()),
            3 => {
                static TEMPLATE: OnceLock<Arc<block::Block>> = OnceLock::new();
                let template = TEMPLATE.get_or_init(|| {
                    Arc::new(
                        block::Block::zcash_deserialize_from_slice(
                            &mut &zakura_test::vectors::BLOCK_MAINNET_1_BYTES[..],
                        )
                        .unwrap(),
                    )
                });
                Message::Block(
                    fake_block_at_height(template, start)
                        .zcash_serialize_to_vec()
                        .unwrap(),
                )
            }
            4 => Message::BlocksDone { start, returned: 1 },
            5 => Message::RangeUnavailable(Range::new(start, 1).unwrap()),
            _ => unreachable!("the stream declares only five message kinds"),
        }
    }

    fn exchange(message: &Message) -> u32 {
        let height = match message {
            Message::GetBlocks(range) | Message::RangeUnavailable(range) => range.start,
            Message::BlocksDone { start, .. } => *start,
            Message::Block(bytes) => {
                block::Block::zcash_deserialize_from_slice(&mut bytes.as_slice())
                    .unwrap()
                    .coinbase_height()
                    .unwrap()
            }
            Message::Status(_) => return 0,
        };
        height.0.checked_sub(1).unwrap()
    }

    fn update(_: &MessageRule, _: SubscriptionUpdate) -> Message {
        unreachable!("GetBlocks has no subscriptions")
    }

    fn read_update(_: &Message) -> Option<SubscriptionUpdate> {
        None
    }

    fn page(_: &MessageRule, _: u32, _: u32) -> Message {
        unreachable!("GetBlocks has no subscriptions")
    }
}

stream_conformance_suite!(stream_six, block_sync_streams(), GetBlocksConformance);
