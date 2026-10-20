use super::{
    config::*,
    error::*,
    regulated::wire::{decode_control, Control, Message, Outcome, Range, RangeEnding},
    *,
};
use crate::zakura::wire_codec::{WireError, WireMessage};

/// Zakura stream kind reserved for native block sync.
pub const ZAKURA_STREAM_BLOCK_SYNC: u16 = 6;
/// Capability for the regulated height-range contract on stream version 3.
pub const ZAKURA_CAP_BLOCK_SYNC_REGULATED: u64 = 1 << 8;
/// Version selecting bounded commitments, exact responses, and frame/cadence rules.
pub const ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION: u16 = 3;

/// Peer status advertisement.
pub const MSG_BS_STATUS: u8 = 1;
/// Request a contiguous range of block bodies by height.
pub const MSG_BS_GET_BLOCKS: u8 = 2;
/// Respond with one full block body.
pub const MSG_BS_BLOCK: u8 = 3;
/// Terminate a `GetBlocks` response.
pub const MSG_BS_BLOCKS_DONE: u8 = 4;
/// Report that a requested range is not servable.
pub const MSG_BS_RANGE_UNAVAILABLE: u8 = 5;

/// Maximum block bodies ever requested or reported by stream 6.
pub const MAX_BS_BLOCKS_PER_REQUEST: u32 = 128;
/// Maximum encoded stream-6 message bytes.
///
/// This cap is intentionally larger than Zebra's consensus block-size limit so
/// stream-6 can read and classify slightly oversized or future-expanded frames
/// in the block-sync codec instead of dropping them at the raw transport gate.
/// Decoded `Block` messages are still bounded by [`block::MAX_BLOCK_BYTES`].
pub const MAX_BS_MESSAGE_BYTES: usize = 3 * 1024 * 1024;

pub(super) const BLOCK_SYNC_MESSAGE_TYPE_BYTES: usize = 1;

const _: () = assert!(MAX_BS_MESSAGE_BYTES < 4 * 1024 * 1024);
const _: () = assert!(MAX_BS_MESSAGE_BYTES > block::MAX_BLOCK_BYTES as usize);

/// Native stream-6 block-sync message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BlockSyncMessage {
    /// Servable range and serving capacity advertisement.
    Status(BlockSyncStatus),
    /// Request `count` block bodies starting at `start_height`.
    GetBlocks {
        /// First requested height.
        start_height: block::Height,
        /// Requested block count.
        count: u32,
    },
    /// One full block body.
    Block(Arc<block::Block>),
    /// End of a `GetBlocks` response.
    BlocksDone {
        /// First requested height.
        start_height: block::Height,
        /// Number of blocks returned.
        returned: u32,
    },
    /// The peer cannot serve this range.
    RangeUnavailable {
        /// First unavailable height.
        start_height: block::Height,
        /// Unavailable block count.
        count: u32,
    },
}

impl BlockSyncMessage {
    /// Returns this message's stream-6 discriminator.
    pub fn message_type(&self) -> u8 {
        match self {
            Self::Status(_) => MSG_BS_STATUS,
            Self::GetBlocks { .. } => MSG_BS_GET_BLOCKS,
            Self::Block(_) => MSG_BS_BLOCK,
            Self::BlocksDone { .. } => MSG_BS_BLOCKS_DONE,
            Self::RangeUnavailable { .. } => MSG_BS_RANGE_UNAVAILABLE,
        }
    }

    /// Encode this message as `[u8 message_type][bounded fields...]`.
    ///
    /// Control messages use the regulated stream-6 codec and its checks.
    pub fn encode(&self) -> Result<Vec<u8>, BlockSyncWireError> {
        let mut bytes = Vec::new();
        let control = match self {
            Self::Block(block) => {
                bytes.write_u8(MSG_BS_BLOCK)?;
                block.zcash_serialize_into_vec(&mut bytes)?;
                validate_encoded_block_len(
                    bytes.len().saturating_sub(BLOCK_SYNC_MESSAGE_TYPE_BYTES),
                )?;
                validate_payload_len(bytes.len())?;
                return Ok(bytes);
            }
            Self::Status(status) => Message::Status(*status),
            Self::GetBlocks {
                start_height,
                count,
            } => Message::GetBlocks(Range::new(*start_height, *count)?),
            Self::BlocksDone {
                start_height,
                returned,
            } => Message::End(RangeEnding::done(*start_height, *returned)),
            Self::RangeUnavailable {
                start_height,
                count,
            } => Message::End(RangeEnding::unavailable(*start_height, *count)),
        };
        control.encode_payload(&mut bytes)?;
        Ok(bytes)
    }

    /// Decode a stream-6 message.
    ///
    /// Control messages use the regulated stream-6 codec and its checks.
    pub fn decode(bytes: &[u8]) -> Result<Self, BlockSyncWireError> {
        validate_payload_len(bytes.len())?;
        match bytes.first().copied() {
            Some(MSG_BS_BLOCK) => decode_block(bytes).map(Self::Block),
            Some(MSG_BS_STATUS..=MSG_BS_RANGE_UNAVAILABLE) => {
                Ok(Self::from_control(decode_control(bytes)?))
            }
            Some(other) => Err(BlockSyncWireError::UnknownMessageType(other)),
            None => Err(WireError::Truncated("message type").into()),
        }
    }

    /// The public form of a decoded control message.
    pub(super) fn from_control(control: Control) -> Self {
        match control {
            Control::Status(status) => Self::Status(status),
            Control::GetBlocks(range) => Self::GetBlocks {
                start_height: range.start(),
                count: range.count(),
            },
            Control::End(RangeEnding {
                start,
                count,
                outcome: Outcome::Done,
            }) => Self::BlocksDone {
                start_height: start,
                returned: count,
            },
            Control::End(RangeEnding {
                start,
                count,
                outcome: Outcome::Unavailable,
            }) => Self::RangeUnavailable {
                start_height: start,
                count,
            },
        }
    }

    /// Convert this message into a bounded Zakura frame.
    pub fn encode_frame(&self) -> Result<Frame, BlockSyncWireError> {
        Ok(Frame {
            message_type: u16::from(self.message_type()),
            flags: 0,
            payload: self.encode()?,
        })
    }

    /// Decode this message from a Zakura frame after checking flags and type agreement.
    pub fn decode_frame(frame: Frame) -> Result<Self, BlockSyncWireError> {
        Self::decode_frame_with_raw_block_payload(frame).map(|(message, _)| message)
    }

    /// Decode this message and return the raw frame payload for block bodies.
    ///
    /// The returned raw payload includes the stream-6 message type byte. The
    /// decoder copies the payload once into an `Arc<[u8]>`, which the reorder
    /// buffer then shares without further copies; consumers skip
    /// [`BLOCK_SYNC_MESSAGE_TYPE_BYTES`] before deserializing the block body.
    pub(super) fn decode_frame_with_raw_block_payload(
        frame: Frame,
    ) -> Result<(Self, Option<Arc<[u8]>>), BlockSyncWireError> {
        if frame.flags != 0 {
            return Err(BlockSyncWireError::UnsupportedFlags(frame.flags));
        }
        let frame_message_type = u8::try_from(frame.message_type)
            .map_err(|_| BlockSyncWireError::UnknownFrameMessageType(frame.message_type))?;

        if let Some(&payload_message_type) = frame.payload.first() {
            if payload_message_type != frame_message_type {
                return Err(BlockSyncWireError::MismatchedFrameMessageType {
                    frame: frame.message_type,
                    payload: payload_message_type,
                });
            }
        }

        // If this is a block message, keep the original raw block payload as well;
        // it can be stored in compact form in the reorder backlog.
        let (message, raw_block_payload) = if frame_message_type == MSG_BS_BLOCK {
            let (block, raw_block_payload) = decode_block_payload(frame.payload)?;
            (Self::Block(block), Some(raw_block_payload))
        } else {
            (Self::decode(&frame.payload)?, None)
        };
        Ok((message, raw_block_payload))
    }
}

/// Decode a `Block` payload, including its type byte, and keep the raw payload.
///
/// The payload is copied once into an `Arc<[u8]>`, which the reorder buffer
/// then shares without further copies; consumers skip
/// [`BLOCK_SYNC_MESSAGE_TYPE_BYTES`] before deserializing the block body.
pub(super) fn decode_block_payload(
    payload: Vec<u8>,
) -> Result<(Arc<block::Block>, Arc<[u8]>), BlockSyncWireError> {
    let raw = Arc::<[u8]>::from(payload.into_boxed_slice());
    validate_payload_len(raw.len())?;
    Ok((decode_block(&raw)?, raw))
}

/// Decode a complete block after its type byte and reject trailing bytes.
fn decode_block(payload: &[u8]) -> Result<Arc<block::Block>, BlockSyncWireError> {
    if payload.first() != Some(&MSG_BS_BLOCK) {
        return Err(WireError::OutOfRange("a Block payload starts with its type").into());
    }
    let mut block_bytes = &payload[BLOCK_SYNC_MESSAGE_TYPE_BYTES..];
    let block = Arc::new(block::Block::zcash_deserialize_from_slice(
        &mut block_bytes,
    )?);
    if !block_bytes.is_empty() {
        return Err(BlockSyncWireError::TrailingBytes);
    }
    validate_encoded_block_len(payload.len() - BLOCK_SYNC_MESSAGE_TYPE_BYTES)?;
    Ok(block)
}

pub(super) fn validate_payload_len(len: usize) -> Result<(), BlockSyncWireError> {
    if len > MAX_BS_MESSAGE_BYTES {
        return Err(BlockSyncWireError::OversizedPayload {
            actual: len,
            max: MAX_BS_MESSAGE_BYTES,
        });
    }
    Ok(())
}

fn validate_encoded_block_len(len: usize) -> Result<(), BlockSyncWireError> {
    let max = usize::try_from(block::MAX_BLOCK_BYTES)
        .map_err(|_| BlockSyncWireError::NumericOverflow("max block bytes"))?;
    if len > max {
        return Err(BlockSyncWireError::OversizedBlock { actual: len, max });
    }
    Ok(())
}

#[cfg(test)]
mod bounded_decoding;
