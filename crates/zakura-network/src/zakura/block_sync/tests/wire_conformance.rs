//! Stream-6 codec conformance against [`BLOCK_SYNC_MESSAGE_RULES`].

use proptest::prelude::*;
use zakura_chain::{block, serialization::ZcashDeserializeInto};
use zakura_test::vectors::BLOCK_MAINNET_1_BYTES;

use super::super::{config::*, wire::*, BlockSyncWireError};
use crate::zakura::{
    wire_codec::conformance::{wire_conformance_tests, WireSample, WireViolation},
    Frame, WireError,
};

/// A range frame whose payload discriminator may differ from its frame type.
fn range_frame(frame_type: u8, discriminator: u8, start: u32, count: u32) -> Frame {
    let mut payload = vec![discriminator];
    payload.extend_from_slice(&start.to_le_bytes());
    payload.extend_from_slice(&count.to_le_bytes());
    Frame {
        message_type: u16::from(frame_type),
        flags: 0,
        payload,
    }
}

fn arbitrary_status() -> impl Strategy<Value = BlockSyncStatus> {
    (
        0..=block::Height::MAX.0,
        0..=block::Height::MAX.0,
        any::<[u8; 32]>(),
        1..=MAX_BS_BLOCKS_PER_REQUEST,
        1..=MAX_BS_INFLIGHT_REQUESTS,
        1..=MAX_BS_RESPONSE_BYTES,
    )
        .prop_map(
            |(low, high, tip, blocks, inflight, bytes)| BlockSyncStatus {
                servable_low: block::Height(low),
                servable_high: block::Height(high),
                tip_hash: block::Hash(tip),
                max_blocks_per_response: blocks,
                max_inflight_requests: inflight,
                max_response_bytes: bytes,
            },
        )
}

impl WireSample for BlockSyncMessage {
    // A block's shortest encoding depends on the network's Equihash solution
    // size, so the row's minimum is a lower bound, not a reachable length.
    const OPEN_ENDED_ROWS: &'static [u16] = &[MSG_BS_BLOCK as u16];

    fn samples() -> Vec<Self> {
        let status = BlockSyncStatus {
            servable_low: block::Height(1),
            servable_high: block::Height(42),
            tip_hash: block::Hash([7; 32]),
            max_blocks_per_response: 16,
            max_inflight_requests: 4,
            max_response_bytes: MAX_BS_RESPONSE_BYTES,
        };
        vec![
            Self::Status(status),
            Self::GetBlocks {
                start_height: block::Height(1),
                count: 1,
            },
            Self::GetBlocks {
                start_height: block::Height(block::Height::MAX.0 - 127),
                count: MAX_BS_BLOCKS_PER_REQUEST,
            },
            Self::Block(
                BLOCK_MAINNET_1_BYTES
                    .zcash_deserialize_into()
                    .expect("the test vector is a valid block"),
            ),
            Self::BlocksDone {
                start_height: block::Height(1),
                returned: MAX_BS_BLOCKS_PER_REQUEST,
            },
            Self::RangeUnavailable {
                start_height: block::Height::MIN,
                count: 1,
            },
        ]
    }

    fn arbitrary_valid() -> BoxedStrategy<Self> {
        let range = (
            0..=block::Height::MAX.0 - 127,
            1..=MAX_BS_BLOCKS_PER_REQUEST,
        )
            .prop_map(|(start, count)| (block::Height(start), count));
        prop_oneof![
            arbitrary_status().prop_map(Self::Status),
            range
                .clone()
                .prop_map(|(start_height, count)| Self::GetBlocks {
                    start_height,
                    count
                }),
            range
                .clone()
                .prop_map(|(start_height, returned)| Self::BlocksDone {
                    start_height,
                    returned
                }),
            range.prop_map(|(start_height, count)| Self::RangeUnavailable {
                start_height,
                count
            }),
        ]
        .boxed()
    }

    fn violations() -> Vec<WireViolation<Self>> {
        let max_height = block::Height::MAX.0;
        vec![
            WireViolation {
                name: "GetBlocks with a zero count",
                frame: range_frame(MSG_BS_GET_BLOCKS, MSG_BS_GET_BLOCKS, 1, 0),
                rejected_by: |error| matches!(error, BlockSyncWireError::ZeroBlockCount),
            },
            WireViolation {
                name: "GetBlocks above the count cap",
                frame: range_frame(MSG_BS_GET_BLOCKS, MSG_BS_GET_BLOCKS, 1, 129),
                rejected_by: |error| matches!(error, BlockSyncWireError::BlockCountLimit { .. }),
            },
            WireViolation {
                name: "GetBlocks past the maximum height",
                frame: range_frame(MSG_BS_GET_BLOCKS, MSG_BS_GET_BLOCKS, max_height, 2),
                rejected_by: |error| matches!(error, BlockSyncWireError::BlockRangeOverflow { .. }),
            },
            WireViolation {
                name: "GetBlocks starting above the maximum height",
                frame: range_frame(MSG_BS_GET_BLOCKS, MSG_BS_GET_BLOCKS, u32::MAX, 1),
                rejected_by: |error| {
                    matches!(error, BlockSyncWireError::Wire(WireError::OutOfRange(_)))
                },
            },
            WireViolation {
                name: "payload discriminator disagrees with the frame type",
                frame: range_frame(MSG_BS_GET_BLOCKS, MSG_BS_BLOCKS_DONE, 1, 1),
                rejected_by: |error| {
                    matches!(error, BlockSyncWireError::MismatchedFrameMessageType { .. })
                },
            },
            WireViolation {
                name: "BlocksDone with a zero count",
                frame: range_frame(MSG_BS_BLOCKS_DONE, MSG_BS_BLOCKS_DONE, 1, 0),
                rejected_by: |error| matches!(error, BlockSyncWireError::ZeroBlockCount),
            },
            WireViolation {
                name: "RangeUnavailable above the count cap",
                frame: range_frame(MSG_BS_RANGE_UNAVAILABLE, MSG_BS_RANGE_UNAVAILABLE, 1, 129),
                rejected_by: |error| matches!(error, BlockSyncWireError::BlockCountLimit { .. }),
            },
            WireViolation {
                name: "Block body that is not a block",
                frame: Frame {
                    message_type: u16::from(MSG_BS_BLOCK),
                    flags: 0,
                    payload: [&[MSG_BS_BLOCK][..], &[0xff; 200]].concat(),
                },
                rejected_by: |error| matches!(error, BlockSyncWireError::Wire(WireError::Item(_))),
            },
        ]
    }

    fn decode_allocation_bound(message_type: u16, _payload_len: usize) -> Option<usize> {
        // Fixed-size messages decode onto the stack. A block's allocations
        // depend on its transactions; the Zcash codec bounds each list itself.
        (message_type != u16::from(MSG_BS_BLOCK)).then_some(0)
    }
}

wire_conformance_tests!(block_sync_wire_conformance, BlockSyncMessage);
