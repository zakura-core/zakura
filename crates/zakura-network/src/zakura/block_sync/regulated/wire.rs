//! The stream-6 version 3 codec: every control message's layout and checks, and
//! the `Block` envelope checked before authorization and full block decoding.

use std::time::Duration;

use super::super::{config::MAX_BS_INFLIGHT_REQUESTS, wire::*, *};
use crate::zakura::{
    regulation::ResponseCap,
    wire_codec::{BoundedReader, HeightLe, LeU32, Wire, WireError, WireMessage, U8},
    Cadence, MessageRole, MessageRule, PayloadLen,
};

pub(in crate::zakura::block_sync) const GET_BLOCKS: MessageRule = MessageRule {
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

/// A range whose count and last height have passed the wire checks. Its
/// fields are private, so every value comes from [`Range::new`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::block_sync) struct Range {
    start: block::Height,
    count: u32,
}

impl Range {
    pub(in crate::zakura::block_sync) fn start(self) -> block::Height {
        self.start
    }

    pub(in crate::zakura::block_sync) fn count(self) -> u32 {
        self.count
    }

    /// The last requested height. [`Range::new`] checked that it exists.
    pub(in crate::zakura::block_sync) fn last(self) -> block::Height {
        block::Height(self.start.0 + (self.count - 1))
    }

    /// This range cut to at most `max_count` heights, keeping at least one.
    pub(in crate::zakura::block_sync) fn truncated(self, max_count: u32) -> Self {
        Self {
            count: self.count.min(max_count.max(1)),
            ..self
        }
    }

    /// A range that skips the wire checks, for tests of the checks themselves.
    #[cfg(test)]
    pub(in crate::zakura::block_sync) fn unchecked(start: block::Height, count: u32) -> Self {
        Self { start, count }
    }

    pub(in crate::zakura::block_sync) fn new(
        start: block::Height,
        count: u32,
    ) -> Result<Self, WireError> {
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
    pub(in crate::zakura::block_sync) fn response_cap(self, body_bytes: u32) -> ResponseCap {
        ResponseCap {
            frames: self.count,
            bytes: u64::from(body_bytes).min(u64::from(self.count) * block::MAX_BLOCK_BYTES)
                + u64::from(self.count)
                + 9,
        }
    }
}

/// Which ending closed a range.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::block_sync) enum Outcome {
    /// `BlocksDone`: the peer sent a nonempty prefix.
    Done,
    /// `RangeUnavailable`: the peer sent no bodies.
    Unavailable,
}

/// An ending's start, count, and kind. The decoder checks the count, and the
/// encoder checks it again. It is not a requested interval: only a live
/// reservation can show that its start and count describe a real exchange.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::block_sync) struct RangeEnding {
    pub(in crate::zakura::block_sync) start: block::Height,
    /// The returned count for `Done`, or the unavailable count.
    pub(in crate::zakura::block_sync) count: u32,
    pub(in crate::zakura::block_sync) outcome: Outcome,
}

impl RangeEnding {
    pub(in crate::zakura::block_sync) fn done(start: block::Height, returned: u32) -> Self {
        Self {
            start,
            count: returned,
            outcome: Outcome::Done,
        }
    }

    pub(in crate::zakura::block_sync) fn unavailable(start: block::Height, count: u32) -> Self {
        Self {
            start,
            count,
            outcome: Outcome::Unavailable,
        }
    }

    pub(in crate::zakura::block_sync) fn message_type(self) -> u8 {
        match self.outcome {
            Outcome::Done => MSG_BS_BLOCKS_DONE,
            Outcome::Unavailable => MSG_BS_RANGE_UNAVAILABLE,
        }
    }
}

/// The checked stream-6 codec. Production decodes each control frame once, here.
/// `Block` still needs exact header authorization, bounded block decoding, and
/// the existing downstream body validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::block_sync) enum Message {
    Status(BlockSyncStatus),
    GetBlocks(Range),
    Block(Vec<u8>),
    End(RangeEnding),
}

impl WireMessage for Message {
    type Error = WireError;
    const RULES: &'static [MessageRule] = RULES;

    fn message_type(&self) -> u16 {
        u16::from(match self {
            Self::Status(_) => MSG_BS_STATUS,
            Self::GetBlocks(_) => MSG_BS_GET_BLOCKS,
            Self::Block(_) => MSG_BS_BLOCK,
            Self::End(ending) => ending.message_type(),
        })
    }

    fn encode_payload(&self, out: &mut Vec<u8>) -> Result<(), WireError> {
        // Every message type above comes from a u8 discriminator.
        let tag = self.message_type() as u8;
        out.push(tag);
        match self {
            Self::Status(status) => {
                validate_status(status)?;
                <(HeightLe, HeightLe)>::encode(&(status.servable_low, status.servable_high), out)?;
                out.extend_from_slice(&status.tip_hash.0);
                for value in [
                    status.max_blocks_per_response,
                    status.max_inflight_requests,
                    status.max_response_bytes,
                ] {
                    LeU32::encode(&value, out)?;
                }
            }
            Self::Block(bytes) => out.extend_from_slice(bytes),
            Self::GetBlocks(range) => {
                Range::new(range.start, range.count)?;
                encode_range(out, range.start, range.count)?;
            }
            Self::End(ending) => {
                validate_count(ending.count)?;
                encode_range(out, ending.start, ending.count)?;
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
        let Ok(tag) = u8::try_from(tag) else {
            return Err(WireError::UnknownMessageType(tag));
        };
        Ok(match tag {
            MSG_BS_STATUS => {
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
            MSG_BS_GET_BLOCKS => Self::GetBlocks(Range::new(
                reader.read::<HeightLe>()?,
                reader.read::<LeU32>()?,
            )?),
            MSG_BS_BLOCK => Self::Block(reader.take_remaining().to_vec()),
            MSG_BS_BLOCKS_DONE | MSG_BS_RANGE_UNAVAILABLE => {
                let start = reader.read::<HeightLe>()?;
                let count = reader.read::<LeU32>()?;
                validate_count(count)?;
                Self::End(if tag == MSG_BS_BLOCKS_DONE {
                    RangeEnding::done(start, count)
                } else {
                    RangeEnding::unavailable(start, count)
                })
            }
            other => return Err(WireError::UnknownMessageType(u16::from(other))),
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

/// A decoded control message: every stream-6 type except `Block`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::zakura::block_sync) enum Control {
    Status(BlockSyncStatus),
    GetBlocks(Range),
    End(RangeEnding),
}

/// Decode a control payload whose first byte is its message type.
/// Callers that hold a frame check its flags and type agreement first.
pub(in crate::zakura::block_sync) fn decode_control(payload: &[u8]) -> Result<Control, WireError> {
    let tag = payload
        .first()
        .copied()
        .ok_or(WireError::Truncated("message type"))?;
    if tag == MSG_BS_BLOCK {
        return Err(WireError::OutOfRange("a Block is not a control message"));
    }
    let mut reader = BoundedReader::new(payload);
    let message = Message::decode_payload(u16::from(tag), &mut reader)?;
    reader.finish()?;
    Ok(match message {
        Message::Status(status) => Control::Status(status),
        Message::GetBlocks(range) => Control::GetBlocks(range),
        Message::End(ending) => Control::End(ending),
        Message::Block(_) => return Err(WireError::OutOfRange("a Block is not a control message")),
    })
}

fn encode_range(out: &mut Vec<u8>, start: block::Height, count: u32) -> Result<(), WireError> {
    <(HeightLe, LeU32)>::encode(&(start, count), out)
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
        || !(1..=MAX_BS_INFLIGHT_REQUESTS).contains(&status.max_inflight_requests)
        || !(1..=MAX_BS_RESPONSE_BYTES).contains(&status.max_response_bytes)
    {
        return Err(WireError::OutOfRange("block-sync status"));
    }
    Ok(())
}
