use super::{config::*, error::*, events::HeaderSyncRequestId, validation::*, *};

/// Zakura stream kind reserved for native header sync.
pub const ZAKURA_STREAM_HEADER_SYNC: u16 = 5;
/// Version of the native header-sync stream.
///
/// Version 4 carries one tree-aux root for each non-empty range header.
/// Version 6 extends each tree-aux root with the full set of per-block ZIP-221 history-leaf
/// inputs a recipient needs to rebuild the leaf and verify the roots against its own header
/// commitments during header sync, without the block body: the Ironwood note-commitment
/// root, the three per-pool shielded transaction counts (Sapling/Orchard/Ironwood), and the
/// block's ZIP-244 `auth_data_root` (the co-input to its NU5+ header commitment).
/// Version 7 prefixes `GetHeaders`/`Headers` with a non-zero request ID so a response is
/// correlated with the exact request that solicited it, instead of by FIFO arrival order.
/// Version 8 makes correlated messages self-contained and encodes each `Headers` entry as
/// one atomic `[height][header][body_size][tree_aux_root]` record when roots are requested.
/// Each of these is a breaking wire change: a peer that speaks an earlier version cannot
/// exchange header-sync ranges with version 8 and does not negotiate header sync at all.
pub const ZAKURA_HEADER_SYNC_STREAM_VERSION: u16 = 8;

/// Peer status advertisement.
pub const MSG_HS_STATUS: u8 = 1;
/// Request a contiguous range of headers by height.
pub const MSG_HS_GET_HEADERS: u8 = 2;
/// Respond with a contiguous run of headers.
pub const MSG_HS_HEADERS: u8 = 3;
/// Flood a newly seen tip block, including its full body.
pub const MSG_HS_NEW_BLOCK: u8 = 4;

/// Maximum encoded header-sync message bytes.
pub const MAX_HS_MESSAGE_BYTES: usize = 2 * 1024 * 1024;
/// Default number of headers advertised per response.
pub const DEFAULT_HS_RANGE: u32 = 1000;
/// Maximum number of headers ever honored by header sync.
pub const MAX_HS_RANGE: u32 = 4000;
/// Default number of in-flight header requests advertised per peer.
pub const DEFAULT_HS_MAX_INFLIGHT: u16 = 10;

pub(super) const HEADER_SYNC_MESSAGE_TYPE_BYTES: usize = 1;
pub(super) const HEADER_SYNC_REQUEST_ID_BYTES: usize = 8;
pub(super) const HEADER_SYNC_COUNT_BYTES: usize = 4;
pub(super) const HEADER_SYNC_HAS_ROOTS_BYTES: usize = 1;
pub(super) const HEADER_SYNC_HEIGHT_BYTES: usize = 4;
pub(super) const HEADER_SYNC_BODY_SIZE_BYTES: usize = 4;
/// Encoded [`BlockCommitmentRoots`]: height + Sapling root + Orchard root + Ironwood root
/// + three `u64` shielded tx-counts (Sapling/Orchard/Ironwood) + auth-data root.
pub(super) const HEADER_SYNC_BLOCK_COMMITMENT_ROOTS_BYTES: usize =
    4 + 32 + 32 + 32 + 8 + 8 + 8 + 32;
pub(super) const COMMON_HEADER_BYTES: usize = 1_487;
pub(super) const REGTEST_HEADER_BYTES: usize = 177;
pub(super) const LOCAL_MAX_HS_INFLIGHT_PER_PEER: u16 = 16;
pub(super) const HEADER_SYNC_MAX_RESIDENT_BATCHES: u32 = 16;
pub(super) const HEADER_SYNC_RETRY_AVOIDANCE: Duration = Duration::from_millis(50);
pub(super) const STATUS_PUBLICATION_RETRY_DELAY: Duration = Duration::from_millis(50);
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
        + HEADER_SYNC_REQUEST_ID_BYTES
        + HEADER_SYNC_COUNT_BYTES
        + (HEADER_SYNC_HEIGHT_BYTES
            + COMMON_HEADER_BYTES
            + HEADER_SYNC_BODY_SIZE_BYTES
            + HEADER_SYNC_BLOCK_COMMITMENT_ROOTS_BYTES)
            * (DEFAULT_HS_RANGE as usize)
        < MAX_HS_MESSAGE_BYTES
);

/// Native versioned header-sync message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeaderSyncMessage {
    /// Peer tip, anchor, and served-range advertisement.
    Status(HeaderSyncStatus),
    /// Request `count` headers starting at `start_height`.
    GetHeaders {
        /// Request ID allocated within this stream session.
        request_id: HeaderSyncRequestId,
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
        /// Request ID echoed from the matching `GetHeaders`.
        request_id: HeaderSyncRequestId,
        /// Structurally aligned per-height records in ascending order.
        entries: Vec<HeaderRangeEntry>,
    },
    /// Full block tip-flood payload.
    NewBlock(Arc<block::Block>),
}

impl HeaderSyncMessage {
    /// Returns this message's header-sync discriminator.
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
                request_id,
                start_height,
                count,
                want_tree_aux_roots,
            } => {
                validate_get_headers_count(*count)?;
                bytes.write_u64::<LittleEndian>(request_id.get())?;
                write_height(&mut bytes, *start_height)?;
                bytes.write_u32::<LittleEndian>(*count)?;
                bytes.write_u8(u8::from(*want_tree_aux_roots))?;
            }
            Self::Headers {
                request_id,
                entries,
            } => {
                validate_headers_len(entries.len(), usize_from_u32(MAX_HS_RANGE, "headers cap")?)?;
                validate_entries(entries)?;
                bytes.write_u64::<LittleEndian>(request_id.get())?;
                bytes.write_u32::<LittleEndian>(u32_from_usize(entries.len(), "headers count")?)?;
                let has_roots = entries
                    .first()
                    .is_some_and(|entry| entry.tree_aux_root.is_some());
                bytes.write_u8(u8::from(has_roots))?;
                for entry in entries {
                    write_height(&mut bytes, entry.height)?;
                    entry.header.zcash_serialize(&mut bytes)?;
                    bytes.write_u32::<LittleEndian>(entry.body_size)?;
                    if has_roots {
                        entry
                            .tree_aux_root
                            .as_ref()
                            .expect("validated rooted entries all have roots")
                            .zcash_serialize(&mut bytes)?;
                    }
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

    /// Decode a header-sync message using the request bounds in `context`.
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
                let request_id = HeaderSyncRequestId::new(reader.read_u64::<LittleEndian>()?)
                    .ok_or(HeaderSyncWireError::MissingRequestId {
                        message: "GetHeaders",
                    })?;
                let start_height = read_height(&mut reader)?;
                let count = reader.read_u32::<LittleEndian>()?;
                let want_tree_aux_roots = read_bool_marker(&mut reader, "want_tree_aux_roots")?;
                validate_get_headers_count(count)?;
                Self::GetHeaders {
                    request_id,
                    start_height,
                    count,
                    want_tree_aux_roots,
                }
            }
            MSG_HS_HEADERS => {
                let request_id = HeaderSyncRequestId::new(reader.read_u64::<LittleEndian>()?)
                    .ok_or(HeaderSyncWireError::MissingRequestId { message: "Headers" })?;
                let count = usize_from_u32(reader.read_u32::<LittleEndian>()?, "headers count")?;
                let has_roots = read_bool_marker(&mut reader, "has_roots")?;
                let Some(max_headers) = context.headers_response_limit()? else {
                    return Err(HeaderSyncWireError::UnsolicitedHeaders);
                };
                if has_roots && !context.wants_tree_aux_roots() {
                    return Err(HeaderSyncWireError::UnrequestedTreeAuxRoots);
                }
                validate_headers_len(count, max_headers)?;
                if count != 0 && context.wants_tree_aux_roots() && !has_roots {
                    return Err(HeaderSyncWireError::TreeAuxRootCountMismatch {
                        headers: count,
                        roots: 0,
                    });
                }
                let requested = context
                    .requested
                    .expect("a bounded Headers response has a matching request");
                let mut entries = Vec::with_capacity(count);
                for offset in 0..count {
                    let height = read_height(&mut reader)?;
                    let height_offset = u32::try_from(offset)
                        .map_err(|_| HeaderSyncWireError::NumericOverflow("entry height offset"))?;
                    let expected_height = requested
                        .start_height
                        .0
                        .checked_add(height_offset)
                        .map(block::Height)
                        .ok_or(HeaderSyncWireError::NumericOverflow("entry height"))?;
                    if height != expected_height {
                        return Err(HeaderSyncWireError::EntryHeightMismatch {
                            offset,
                            expected_height,
                            entry_height: height,
                        });
                    }
                    let header = Arc::new(block::Header::zcash_deserialize(&mut reader)?);
                    let body_size = reader.read_u32::<LittleEndian>()?;
                    let tree_aux_root = if has_roots {
                        Some(BlockCommitmentRoots::zcash_deserialize(&mut reader)?)
                    } else {
                        None
                    };
                    entries.push(HeaderRangeEntry {
                        height,
                        header,
                        body_size,
                        tree_aux_root,
                    });
                }
                validate_entries(&entries)?;
                Self::Headers {
                    request_id,
                    entries,
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

    /// Return a `Headers` request ID without decoding the full header payload.
    pub fn peek_headers_request_id(
        bytes: &[u8],
    ) -> Result<HeaderSyncRequestId, HeaderSyncWireError> {
        let mut reader = Cursor::new(bytes);
        let message_type = reader.read_u8()?;
        if message_type != MSG_HS_HEADERS {
            return Err(HeaderSyncWireError::UnknownMessageType(message_type));
        }
        HeaderSyncRequestId::new(reader.read_u64::<LittleEndian>()?)
            .ok_or(HeaderSyncWireError::MissingRequestId { message: "Headers" })
    }

    /// Convert this message into a bounded Zakura frame.
    pub fn encode_frame(&self) -> Result<Frame, HeaderSyncWireError> {
        Ok(Frame {
            message_type: u16::from(self.message_type()),
            flags: 0,
            payload: self.encode()?,
        })
    }

    /// Decode this message from a Zakura frame, after checking flags and type agreement.
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

fn validate_entries(entries: &[HeaderRangeEntry]) -> Result<(), HeaderSyncWireError> {
    let roots = entries
        .iter()
        .filter(|entry| entry.tree_aux_root.is_some())
        .count();
    if roots != 0 && roots != entries.len() {
        return Err(HeaderSyncWireError::TreeAuxRootCountMismatch {
            headers: entries.len(),
            roots,
        });
    }

    for (offset, adjacent) in entries.windows(2).enumerate() {
        let expected_height = adjacent[0]
            .height
            .0
            .checked_add(1)
            .map(block::Height)
            .ok_or(HeaderSyncWireError::NumericOverflow("entry height"))?;
        if adjacent[1].height != expected_height {
            return Err(HeaderSyncWireError::EntryHeightMismatch {
                offset: offset + 1,
                expected_height,
                entry_height: adjacent[1].height,
            });
        }
    }

    if roots != 0 {
        let first_root_height = entries
            .first()
            .and_then(|entry| entry.tree_aux_root.as_ref())
            .expect("all non-empty rooted entries have roots")
            .height;
        let last_root_height = entries
            .last()
            .and_then(|entry| entry.tree_aux_root.as_ref())
            .expect("all non-empty rooted entries have roots")
            .height;
        for (offset, entry) in entries.iter().enumerate() {
            let root_height = entry
                .tree_aux_root
                .as_ref()
                .expect("all rooted entries have roots")
                .height;
            if root_height != entry.height {
                return Err(HeaderSyncWireError::TreeAuxRootHeightMismatch {
                    offset,
                    expected_height: entry.height,
                    root_height,
                    first_root_height,
                    last_root_height,
                });
            }
        }
    }

    Ok(())
}
