use super::{config::*, error::*, *};
use crate::zakura::wire_codec::{
    self, BoundedReader, HashItem, HeightLe, LeU32, Wire, WireError, WireMessage,
};

/// Zakura stream kind reserved for native block sync.
pub const ZAKURA_STREAM_BLOCK_SYNC: u16 = 6;
/// Capability bit for the native block-sync service.
pub const ZAKURA_CAP_BLOCK_SYNC: u64 = 1 << 3;
/// Version of the native block-sync stream.
pub const ZAKURA_BLOCK_SYNC_STREAM_VERSION: u16 = 2;

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

/// Fixed block-header fields before the Equihash solution: version, three
/// hashes, time, bits, and nonce. Every valid block on every network is at
/// least this long.
const MIN_BLOCK_BYTES: usize = 4 + 32 + 32 + 32 + 4 + 4 + 32;

/// Consensus block-size limit in bytes.
// The cast is lossless: the const assertion below keeps usize at least 32 bits.
const MAX_BLOCK_USIZE: usize = block::MAX_BLOCK_BYTES as usize;
const _: () = assert!(usize::BITS >= 32);

/// Discriminator byte plus the fields of each fixed-size message.
const STATUS_PAYLOAD_BYTES: usize = BLOCK_SYNC_MESSAGE_TYPE_BYTES + StatusItem::MAX_LEN;
const RANGE_PAYLOAD_BYTES: usize =
    BLOCK_SYNC_MESSAGE_TYPE_BYTES + HeightLe::MAX_LEN + LeU32::MAX_LEN;

/// Stream-6 message rules, checked from each frame header and by the codec.
///
/// The `as u16` casts widen u8 discriminators, which is lossless.
pub const BLOCK_SYNC_MESSAGE_RULES: [MessageRule; 5] = [
    MessageRule::announcement(
        MSG_BS_STATUS as u16,
        PayloadLen::exact(STATUS_PAYLOAD_BYTES),
    ),
    MessageRule::request(
        MSG_BS_GET_BLOCKS as u16,
        PayloadLen::exact(RANGE_PAYLOAD_BYTES),
    ),
    MessageRule::response(
        MSG_BS_BLOCK as u16,
        PayloadLen::between(
            BLOCK_SYNC_MESSAGE_TYPE_BYTES + MIN_BLOCK_BYTES,
            BLOCK_SYNC_MESSAGE_TYPE_BYTES + MAX_BLOCK_USIZE,
        ),
    ),
    MessageRule::response(
        MSG_BS_BLOCKS_DONE as u16,
        PayloadLen::exact(RANGE_PAYLOAD_BYTES),
    ),
    MessageRule::response(
        MSG_BS_RANGE_UNAVAILABLE as u16,
        PayloadLen::exact(RANGE_PAYLOAD_BYTES),
    ),
];

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
    pub fn encode(&self) -> Result<Vec<u8>, BlockSyncWireError> {
        self.encode_frame().map(|frame| frame.payload)
    }

    /// Decode a stream-6 payload; its first byte names the message type.
    pub fn decode(bytes: &[u8]) -> Result<Self, BlockSyncWireError> {
        let message_type = bytes
            .first()
            .ok_or(WireError::Truncated("block-sync message type"))?;
        wire_codec::decode_payload_exact(u16::from(*message_type), bytes)
    }

    /// Convert this message into a bounded Zakura frame.
    pub fn encode_frame(&self) -> Result<Frame, BlockSyncWireError> {
        wire_codec::encode_frame(self)
    }

    /// Decode this message from a Zakura frame after checking flags and type agreement.
    pub fn decode_frame(frame: Frame) -> Result<Self, BlockSyncWireError> {
        wire_codec::decode_frame(&frame)
    }

    /// Decode this message and return the raw frame payload for block bodies.
    ///
    /// The returned raw payload includes the stream-6 message type byte. Keeping
    /// the whole payload lets the decoder move the frame allocation into the
    /// reorder buffer without copying; consumers skip
    /// [`BLOCK_SYNC_MESSAGE_TYPE_BYTES`] before deserializing the block body.
    pub(super) fn decode_frame_with_raw_block_payload(
        frame: Frame,
    ) -> Result<(Self, Option<Arc<[u8]>>), BlockSyncWireError> {
        if frame.message_type != u16::from(MSG_BS_BLOCK) {
            return Ok((wire_codec::decode_frame(&frame)?, None));
        }
        if frame.flags != 0 {
            return Err(WireError::ReservedFlags(frame.flags).into());
        }
        let raw_block_payload = Arc::<[u8]>::from(frame.payload.into_boxed_slice());
        let message = wire_codec::decode_payload_exact(frame.message_type, &raw_block_payload)?;
        Ok((message, Some(raw_block_payload)))
    }

    /// Exact serialized length of a `Block` body, derived from the frame payload
    /// length the per-peer decode task already has in hand.
    ///
    /// A block payload is `[message_type][block serialization]` with no trailing
    /// bytes — `encode` writes exactly those two parts and `decode` rejects
    /// trailing bytes, so the block occupies exactly
    /// `payload_len - BLOCK_SYNC_MESSAGE_TYPE_BYTES`. Returning this lets the
    /// reactor account body bytes without re-serializing the whole block on its
    /// single thread (the per-peer task already paid the deserialization cost).
    /// Returns `None` for non-block messages.
    pub(super) fn block_body_wire_bytes(&self, payload_len: usize) -> Option<u64> {
        matches!(self, Self::Block(_)).then(|| {
            u64::try_from(payload_len.saturating_sub(BLOCK_SYNC_MESSAGE_TYPE_BYTES))
                .unwrap_or(u64::MAX)
        })
    }
}

impl WireMessage for BlockSyncMessage {
    type Error = BlockSyncWireError;
    const RULES: &'static [MessageRule] = &BLOCK_SYNC_MESSAGE_RULES;

    fn message_type(&self) -> u16 {
        u16::from(BlockSyncMessage::message_type(self))
    }

    fn encode_payload(&self, out: &mut Vec<u8>) -> Result<(), BlockSyncWireError> {
        out.push(BlockSyncMessage::message_type(self));
        match self {
            Self::Status(status) => StatusItem::encode(status, out)?,
            Self::GetBlocks {
                start_height,
                count,
            } => {
                validate_block_range(*start_height, *count)?;
                encode_range(*start_height, *count, out)?;
            }
            Self::Block(block) => {
                let block_start = out.len();
                block.zcash_serialize(&mut *out).map_err(WireError::from)?;
                validate_encoded_block_len(out.len() - block_start)?;
            }
            Self::BlocksDone {
                start_height,
                returned: count,
            }
            | Self::RangeUnavailable {
                start_height,
                count,
            } => {
                validate_block_count(*count)?;
                encode_range(*start_height, *count, out)?;
            }
        }
        Ok(())
    }

    fn decode_payload(
        message_type: u16,
        reader: &mut BoundedReader<'_>,
    ) -> Result<Self, BlockSyncWireError> {
        let [discriminator] = reader.array("block-sync message type")?;
        if u16::from(discriminator) != message_type {
            return Err(BlockSyncWireError::MismatchedFrameMessageType {
                frame: message_type,
                payload: discriminator,
            });
        }
        let message = match discriminator {
            MSG_BS_STATUS => Self::Status(reader.read::<StatusItem>()?),
            MSG_BS_GET_BLOCKS => {
                let (start_height, count) = decode_range(reader)?;
                validate_block_range(start_height, count)?;
                Self::GetBlocks {
                    start_height,
                    count,
                }
            }
            MSG_BS_BLOCK => {
                let block_len = reader.remaining();
                validate_encoded_block_len(block_len)?;
                Self::Block(Arc::new(reader.zcash::<block::Block>()?))
            }
            MSG_BS_BLOCKS_DONE => {
                let (start_height, returned) = decode_range(reader)?;
                validate_block_count(returned)?;
                Self::BlocksDone {
                    start_height,
                    returned,
                }
            }
            MSG_BS_RANGE_UNAVAILABLE => {
                let (start_height, count) = decode_range(reader)?;
                validate_block_count(count)?;
                Self::RangeUnavailable {
                    start_height,
                    count,
                }
            }
            _ => return Err(WireError::UnknownMessageType(message_type).into()),
        };
        Ok(message)
    }
}

/// Wire encoding of [`BlockSyncStatus`]: two heights, the tip hash, and three
/// `u32` limits.
///
/// Decoding clamps each limit into its local range instead of rejecting it.
/// The regulation specification does not yet say whether an out-of-range
/// advertisement is a violation; this is a known gap.
pub(super) enum StatusItem {}

impl Wire for StatusItem {
    type Value = BlockSyncStatus;
    const MIN_LEN: usize = 2 * HeightLe::MAX_LEN + HashItem::MAX_LEN + 3 * LeU32::MAX_LEN;
    const MAX_LEN: usize = Self::MIN_LEN;

    fn encode(status: &BlockSyncStatus, out: &mut Vec<u8>) -> Result<(), WireError> {
        HeightLe::encode(&status.servable_low, out)?;
        HeightLe::encode(&status.servable_high, out)?;
        HashItem::encode(&status.tip_hash, out)?;
        LeU32::encode(
            &clamp_advertised_blocks(status.max_blocks_per_response),
            out,
        )?;
        LeU32::encode(&status.max_inflight_requests, out)?;
        LeU32::encode(&status.max_response_bytes.max(1), out)
    }

    fn decode(reader: &mut BoundedReader<'_>) -> Result<BlockSyncStatus, WireError> {
        Ok(BlockSyncStatus {
            servable_low: reader.read::<HeightLe>()?,
            servable_high: reader.read::<HeightLe>()?,
            tip_hash: reader.read::<HashItem>()?,
            max_blocks_per_response: clamp_advertised_blocks(reader.read::<LeU32>()?),
            max_inflight_requests: clamp_advertised_inflight(reader.read::<LeU32>()?),
            max_response_bytes: clamp_advertised_response_bytes(reader.read::<LeU32>()?),
        })
    }
}

fn encode_range(
    start_height: block::Height,
    count: u32,
    out: &mut Vec<u8>,
) -> Result<(), WireError> {
    HeightLe::encode(&start_height, out)?;
    LeU32::encode(&count, out)
}

fn decode_range(reader: &mut BoundedReader<'_>) -> Result<(block::Height, u32), WireError> {
    Ok((reader.read::<HeightLe>()?, reader.read::<LeU32>()?))
}

pub(super) fn validate_block_count(count: u32) -> Result<(), BlockSyncWireError> {
    if count == 0 {
        return Err(BlockSyncWireError::ZeroBlockCount);
    }
    if count > MAX_BS_BLOCKS_PER_REQUEST {
        return Err(BlockSyncWireError::BlockCountLimit {
            actual: count,
            max: MAX_BS_BLOCKS_PER_REQUEST,
        });
    }
    Ok(())
}

/// Check a `GetBlocks` range: a valid count whose last height exists.
fn validate_block_range(start_height: block::Height, count: u32) -> Result<(), BlockSyncWireError> {
    validate_block_count(count)?;
    start_height
        .0
        .checked_add(count - 1)
        .filter(|last_height| *last_height <= block::Height::MAX.0)
        .ok_or(BlockSyncWireError::BlockRangeOverflow {
            start: start_height,
            count,
        })?;
    Ok(())
}

fn validate_encoded_block_len(len: usize) -> Result<(), BlockSyncWireError> {
    if len > MAX_BLOCK_USIZE {
        return Err(BlockSyncWireError::OversizedBlock {
            actual: len,
            max: MAX_BLOCK_USIZE,
        });
    }
    Ok(())
}
