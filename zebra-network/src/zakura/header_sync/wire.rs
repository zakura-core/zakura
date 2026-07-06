use super::{config::*, error::*, validation::*, *};

/// Zakura stream kind reserved for native header sync.
pub const ZAKURA_STREAM_HEADER_SYNC: u16 = 5;
/// Version of the native header-sync stream.
///
/// Version 4 carries one tree-aux root for each non-empty range header.
/// Version 6 extends each tree-aux root with the full set of per-block ZIP-221 history-leaf
/// inputs a recipient needs to rebuild the leaf and verify the roots against its own header
/// commitments during header sync, without the block body: the Ironwood note-commitment
/// root, the three per-pool shielded transaction counts (Sapling/Orchard/Ironwood), and the
/// block's ZIP-244 `auth_data_root` (the co-input to its NU5+ header commitment). This is a
/// breaking wire change: earlier version-5 Ironwood builds used a shorter root record and
/// cannot exchange header-sync ranges with version 6.
pub const ZAKURA_HEADER_SYNC_STREAM_VERSION: u16 = 6;

/// Peer status advertisement.
pub const MSG_HS_STATUS: u8 = 1;
/// Request a contiguous range of headers by height.
pub const MSG_HS_GET_HEADERS: u8 = 2;
/// Respond with a contiguous run of headers.
pub const MSG_HS_HEADERS: u8 = 3;
/// Flood a newly seen tip block, including its full body.
pub const MSG_HS_NEW_BLOCK: u8 = 4;

/// Maximum encoded header-sync v6 message bytes.
pub const MAX_HS_MESSAGE_BYTES: usize = 2 * 1024 * 1024;
/// Default number of headers advertised per response.
pub const DEFAULT_HS_RANGE: u32 = 1000;
/// Maximum number of headers ever honored by header-sync v6.
pub const MAX_HS_RANGE: u32 = 4000;
/// Default number of in-flight header requests advertised per peer.
pub const DEFAULT_HS_MAX_INFLIGHT: u16 = 10;

pub(super) const HEADER_SYNC_MESSAGE_TYPE_BYTES: usize = 1;
pub(super) const HEADER_SYNC_COUNT_BYTES: usize = 4;
pub(super) const HEADER_SYNC_HAS_ROOTS_BYTES: usize = 1;
pub(super) const HEADER_SYNC_BODY_SIZE_BYTES: usize = 4;
/// Encoded [`BlockCommitmentRoots`]: height + Sapling root + Orchard root + Ironwood root
/// + three `u64` shielded tx-counts (Sapling/Orchard/Ironwood) + auth-data root.
pub(super) const HEADER_SYNC_BLOCK_COMMITMENT_ROOTS_BYTES: usize =
    4 + 32 + 32 + 32 + 8 + 8 + 8 + 32;
pub(super) const COMMON_HEADER_BYTES: usize = 1_487;
pub(super) const REGTEST_HEADER_BYTES: usize = 177;
pub(super) const HEADER_SYNC_FANOUT: usize = 3;
pub(super) const LOCAL_MAX_HS_INFLIGHT_PER_PEER: u16 = 16;
pub(super) const EFFECTIVE_HS_OUTBOUND_INFLIGHT_PER_PEER: usize = 1;
pub(super) const HEADER_SYNC_SEEN_HASH_CAPACITY: usize = 4096;
pub(super) const DEFAULT_HS_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
pub(super) const EMPTY_HEADERS_RETRY_DELAY: Duration = Duration::from_secs(1);
pub(super) const DEFAULT_HS_STATUS_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
// v1 semantic meters intentionally use strict spacing for unsolicited status refreshes and
// distinct unseen full-block floods. Cheap duplicate `NewBlock` echoes are deduped before this
// meter, so they do not consume tokens or cause honest peers to be scored.
pub(super) const DEFAULT_HS_INBOUND_STATUS_MIN_INTERVAL: Duration = Duration::from_secs(5);
pub(super) const DEFAULT_HS_INBOUND_NEW_BLOCK_MIN_INTERVAL: Duration = Duration::from_secs(5);

const _: () = assert!(MAX_HS_MESSAGE_BYTES < LOCAL_MAX_MESSAGE_BYTES as usize);
const _: () = assert!(
    HEADER_SYNC_MESSAGE_TYPE_BYTES
        + HEADER_SYNC_COUNT_BYTES
        + (COMMON_HEADER_BYTES
            + HEADER_SYNC_BODY_SIZE_BYTES
            + HEADER_SYNC_BLOCK_COMMITMENT_ROOTS_BYTES)
            * (DEFAULT_HS_RANGE as usize)
        < MAX_HS_MESSAGE_BYTES
);

/// Native header-sync v6 message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeaderSyncMessage {
    /// Peer tip, anchor, and served-range advertisement.
    Status(HeaderSyncStatus),
    /// Request `count` headers starting at `start_height`.
    GetHeaders {
        /// First requested height.
        start_height: block::Height,
        /// Requested header count.
        count: u32,
        /// Whether the requester wants all-or-nothing tree-aux roots.
        /// A sender who is syncing in vct mode will always request these.
        /// A sender who is syncing in non-checkpoint mode does not need these but still requests them.
        /// A sender who is syncing above the last checkpoint height does not request these.
        want_tree_aux_roots: bool,
    },
    /// A bounded contiguous header run with one advisory body-size hint per header.
    ///
    /// A `0` size means "unknown"; the hint is not consensus data. Tree-aux roots
    /// are peer-sourced execution hints and are verified by state before use.
    Headers {
        /// Headers in ascending height order.
        headers: Vec<Arc<block::Header>>,
        /// Advisory serialized body sizes, parallel to `headers`.
        body_sizes: Vec<u32>,
        /// Per-height commitment roots, parallel to `headers`.
        tree_aux_roots: Vec<BlockCommitmentRoots>,
    },
    /// Full block tip-flood payload.
    NewBlock(Arc<block::Block>),
}

impl HeaderSyncMessage {
    /// Returns this message's header-sync v6 discriminator.
    pub fn message_type(&self) -> u8 {
        match self {
            Self::Status(_) => MSG_HS_STATUS,
            Self::GetHeaders { .. } => MSG_HS_GET_HEADERS,
            Self::Headers { .. } => MSG_HS_HEADERS,
            Self::NewBlock(_) => MSG_HS_NEW_BLOCK,
        }
    }

    /// Encode this message as `[u8 message_type][bounded fields...]`.
    pub fn encode(&self) -> Result<Vec<u8>, HeaderSyncWireError> {
        let mut bytes = Vec::new();
        bytes.write_u8(self.message_type())?;
        match self {
            Self::Status(status) => status.encode_to(&mut bytes)?,
            Self::GetHeaders {
                start_height,
                count,
                want_tree_aux_roots,
            } => {
                validate_get_headers_count(*count)?;
                write_height(&mut bytes, *start_height)?;
                bytes.write_u32::<LittleEndian>(*count)?;
                bytes.write_u8(u8::from(*want_tree_aux_roots))?;
            }
            Self::Headers {
                headers,
                body_sizes,
                tree_aux_roots,
            } => {
                validate_headers_len(headers.len(), usize_from_u32(MAX_HS_RANGE, "headers cap")?)?;
                validate_body_sizes_len(headers.len(), body_sizes.len())?;
                validate_tree_aux_roots_len(headers.len(), tree_aux_roots.len())?;
                bytes.write_u32::<LittleEndian>(u32_from_usize(headers.len(), "headers count")?)?;
                bytes.write_u8(u8::from(!tree_aux_roots.is_empty()))?;
                for (header, body_size) in headers.iter().zip(body_sizes) {
                    header.zcash_serialize(&mut bytes)?;
                    bytes.write_u32::<LittleEndian>(*body_size)?;
                }
                for roots in tree_aux_roots {
                    roots.zcash_serialize(&mut bytes)?;
                }
            }
            Self::NewBlock(block) => {
                block.zcash_serialize(&mut bytes)?;
            }
        }
        if bytes.len() > MAX_HS_MESSAGE_BYTES {
            return Err(HeaderSyncWireError::OversizedPayload {
                actual: bytes.len(),
                max: MAX_HS_MESSAGE_BYTES,
            });
        }
        Ok(bytes)
    }

    /// Decode a header-sync v6 message using the peer/request bounds in `context`.
    pub fn decode(
        bytes: &[u8],
        context: HeaderSyncDecodeContext,
    ) -> Result<Self, HeaderSyncWireError> {
        if bytes.len() > MAX_HS_MESSAGE_BYTES {
            return Err(HeaderSyncWireError::OversizedPayload {
                actual: bytes.len(),
                max: MAX_HS_MESSAGE_BYTES,
            });
        }
        let mut reader = Cursor::new(bytes);
        let message_type = reader.read_u8()?;
        let message = match message_type {
            MSG_HS_STATUS => Self::Status(HeaderSyncStatus::decode_from(&mut reader)?),
            MSG_HS_GET_HEADERS => {
                let start_height = read_height(&mut reader)?;
                let count = reader.read_u32::<LittleEndian>()?;
                let want_tree_aux_roots = read_bool_marker(&mut reader, "want_tree_aux_roots")?;
                validate_get_headers_count(count)?;
                Self::GetHeaders {
                    start_height,
                    count,
                    want_tree_aux_roots,
                }
            }
            MSG_HS_HEADERS => {
                let count = usize_from_u32(reader.read_u32::<LittleEndian>()?, "headers count")?;
                let has_roots = read_bool_marker(&mut reader, "has_roots")?;
                let Some(max_headers) = context.headers_response_limit()? else {
                    return Err(HeaderSyncWireError::UnsolicitedHeaders);
                };
                if has_roots && !context.wants_tree_aux_roots() {
                    return Err(HeaderSyncWireError::UnrequestedTreeAuxRoots);
                }
                validate_headers_len(count, max_headers)?;
                let mut headers = Vec::with_capacity(count);
                let mut body_sizes = Vec::with_capacity(count);
                let mut tree_aux_roots = if has_roots {
                    Vec::with_capacity(count)
                } else {
                    Vec::new()
                };
                for _ in 0..count {
                    headers.push(Arc::new(block::Header::zcash_deserialize(&mut reader)?));
                    body_sizes.push(reader.read_u32::<LittleEndian>()?);
                }
                if has_roots {
                    for _ in 0..count {
                        tree_aux_roots.push(BlockCommitmentRoots::zcash_deserialize(&mut reader)?);
                    }
                }
                validate_tree_aux_roots_len(count, tree_aux_roots.len())?;
                if let Some(requested) = context.requested {
                    validate_tree_aux_root_heights(requested.start_height, &tree_aux_roots)?;
                }
                Self::Headers {
                    headers,
                    body_sizes,
                    tree_aux_roots,
                }
            }
            MSG_HS_NEW_BLOCK => {
                Self::NewBlock(Arc::new(block::Block::zcash_deserialize(&mut reader)?))
            }
            value => return Err(HeaderSyncWireError::UnknownMessageType(value)),
        };
        reject_trailing(bytes, &reader)?;
        Ok(message)
    }

    /// Convert this message into a bounded Zakura frame.
    pub fn encode_frame(&self) -> Result<Frame, HeaderSyncWireError> {
        Ok(Frame {
            message_type: u16::from(self.message_type()),
            flags: 0,
            payload: self.encode()?,
        })
    }

    /// Decode this message from a Zakura frame after checking flags and type agreement.
    pub fn decode_frame(
        frame: Frame,
        context: HeaderSyncDecodeContext,
    ) -> Result<Self, HeaderSyncWireError> {
        if frame.flags != 0 {
            return Err(HeaderSyncWireError::UnsupportedFlags(frame.flags));
        }
        let message = Self::decode(&frame.payload, context)?;
        let frame_message_type = u8::try_from(frame.message_type)
            .map_err(|_| HeaderSyncWireError::UnknownFrameMessageType(frame.message_type))?;
        if frame_message_type != message.message_type() {
            return Err(HeaderSyncWireError::MismatchedFrameMessageType {
                frame: frame.message_type,
                payload: message.message_type(),
            });
        }
        Ok(message)
    }
}
