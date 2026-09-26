//! The version-2 envelope, before authorization and full block decoding.

use std::time::Duration;

use super::super::{config::MAX_BS_INFLIGHT_REQUESTS, wire::*, *};
use crate::zakura::{
    regulation::ResponseCap,
    wire_codec::{BoundedReader, HeightLe, LeU32, WireError, WireMessage, U8},
    Cadence, MessageRole, MessageRule, PayloadLen,
};

pub(super) const GET_BLOCKS: MessageRule = MessageRule {
    message_type: 2,
    payload: PayloadLen::exact(9),
    role: MessageRole::Request {
        max_in_flight: MAX_BS_INFLIGHT_REQUESTS,
        cadence: None,
    },
};

const fn response(message_type: u8, ends_exchange: bool, payload: PayloadLen) -> MessageRule {
    MessageRule {
        // A u8 discriminator always fits in u16.
        message_type: message_type as u16,
        payload,
        role: MessageRole::Response {
            request: GET_BLOCKS.message_type,
            ends_exchange,
        },
    }
}

pub(in crate::zakura::block_sync) const RULES: &[MessageRule] = &[
    MessageRule {
        message_type: 1,
        payload: PayloadLen::exact(53),
        role: MessageRole::Announcement {
            cadence: Cadence {
                capacity: 22,
                refill_interval: Duration::from_secs(15),
                send_interval: Duration::from_secs(30),
            },
        },
    },
    GET_BLOCKS,
    response(MSG_BS_BLOCK, false, PayloadLen::between(2, 2_000_001)),
    response(MSG_BS_BLOCKS_DONE, true, PayloadLen::exact(9)),
    response(MSG_BS_RANGE_UNAVAILABLE, true, PayloadLen::exact(9)),
];

/// A range whose count and last height have passed the wire checks.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct Range {
    pub(super) start: block::Height,
    pub(super) count: u32,
}

impl Range {
    pub(super) fn new(start: block::Height, count: u32) -> Result<Self, WireError> {
        if !(1..=MAX_BS_BLOCKS_PER_REQUEST).contains(&count)
            || start
                .0
                .checked_add(count - 1)
                .is_none_or(|last| last > block::Height::MAX.0)
        {
            return Err(WireError::OutOfRange("GetBlocks range"));
        }
        Ok(Self { start, count })
    }

    /// Bodies count toward the advertised byte limit. Tags and the ending do not.
    pub(super) fn response_cap(self, body_bytes: u32) -> ResponseCap {
        ResponseCap {
            frames: self.count,
            bytes: u64::from(body_bytes).min(u64::from(self.count) * block::MAX_BLOCK_BYTES)
                + u64::from(self.count)
                + 9,
        }
    }
}

/// A checked envelope. `Block` still needs exact header authorization, bounded
/// block decoding, and the existing downstream body validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Message {
    Status(BlockSyncStatus),
    GetBlocks(Range),
    Block(Vec<u8>),
    BlocksDone { start: block::Height, returned: u32 },
    RangeUnavailable(Range),
}

impl WireMessage for Message {
    type Error = WireError;
    const RULES: &'static [MessageRule] = RULES;

    fn message_type(&self) -> u16 {
        u16::from(match self {
            Self::Status(_) => MSG_BS_STATUS,
            Self::GetBlocks(_) => MSG_BS_GET_BLOCKS,
            Self::Block(_) => MSG_BS_BLOCK,
            Self::BlocksDone { .. } => MSG_BS_BLOCKS_DONE,
            Self::RangeUnavailable(_) => MSG_BS_RANGE_UNAVAILABLE,
        })
    }

    fn encode_payload(&self, out: &mut Vec<u8>) -> Result<(), WireError> {
        // Every message type above comes from a u8 discriminator.
        let tag = self.message_type() as u8;
        out.push(tag);
        match self {
            Self::Status(status) => {
                validate_status(status)?;
                out.extend_from_slice(&status.servable_low.0.to_le_bytes());
                out.extend_from_slice(&status.servable_high.0.to_le_bytes());
                out.extend_from_slice(&status.tip_hash.0);
                for value in [
                    status.max_blocks_per_response,
                    status.max_inflight_requests,
                    status.max_response_bytes,
                ] {
                    out.extend_from_slice(&value.to_le_bytes());
                }
            }
            Self::Block(bytes) => out.extend_from_slice(bytes),
            Self::GetBlocks(range) => {
                Range::new(range.start, range.count)?;
                encode_range(out, range.start, range.count);
            }
            Self::RangeUnavailable(range) => {
                validate_height(range.start)?;
                validate_count(range.count)?;
                encode_range(out, range.start, range.count);
            }
            Self::BlocksDone { start, returned } => {
                validate_height(*start)?;
                validate_count(*returned)?;
                encode_range(out, *start, *returned);
            }
        }
        Ok(())
    }

    fn decode_payload(tag: u16, reader: &mut BoundedReader<'_>) -> Result<Self, WireError> {
        if u16::from(reader.read::<U8>()?) != tag {
            return Err(WireError::OutOfRange(
                "frame and payload discriminator agreement",
            ));
        }
        Ok(match tag {
            1 => {
                let status = BlockSyncStatus {
                    servable_low: reader.read::<HeightLe>()?,
                    servable_high: reader.read::<HeightLe>()?,
                    tip_hash: block::Hash(reader.array("tip hash")?),
                    max_blocks_per_response: reader.read::<LeU32>()?,
                    max_inflight_requests: reader.read::<LeU32>()?,
                    max_response_bytes: reader.read::<LeU32>()?,
                };
                validate_status(&status)?;
                Self::Status(status)
            }
            2 => Self::GetBlocks(Range::new(
                reader.read::<HeightLe>()?,
                reader.read::<LeU32>()?,
            )?),
            3 => Self::Block(reader.take_remaining().to_vec()),
            4 => {
                let start = reader.read::<HeightLe>()?;
                let returned = reader.read::<LeU32>()?;
                validate_count(returned)?;
                Self::BlocksDone { start, returned }
            }
            5 => {
                let start = reader.read::<HeightLe>()?;
                let count = reader.read::<LeU32>()?;
                validate_count(count)?;
                Self::RangeUnavailable(Range { start, count })
            }
            other => return Err(WireError::UnknownMessageType(other)),
        })
    }

    fn max_heap_bytes(tag: u16, payload_len: usize) -> usize {
        if tag == u16::from(MSG_BS_BLOCK) {
            payload_len.saturating_sub(1)
        } else {
            0
        }
    }
}

fn encode_range(out: &mut Vec<u8>, start: block::Height, count: u32) {
    out.extend_from_slice(&start.0.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
}

fn validate_count(count: u32) -> Result<(), WireError> {
    if !(1..=MAX_BS_BLOCKS_PER_REQUEST).contains(&count) {
        return Err(WireError::OutOfRange("block count"));
    }
    Ok(())
}

fn validate_status(status: &BlockSyncStatus) -> Result<(), WireError> {
    validate_count(status.max_blocks_per_response)?;
    if status.servable_low > status.servable_high
        || status.servable_high > block::Height::MAX
        || !(1..=MAX_BS_INFLIGHT_REQUESTS).contains(&status.max_inflight_requests)
        || !(1..=MAX_BS_RESPONSE_BYTES).contains(&status.max_response_bytes)
    {
        return Err(WireError::OutOfRange("block-sync status"));
    }
    Ok(())
}

fn validate_height(height: block::Height) -> Result<(), WireError> {
    if height > block::Height::MAX {
        return Err(WireError::OutOfRange("block height"));
    }
    Ok(())
}
