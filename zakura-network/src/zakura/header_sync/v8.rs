//! Version-8 header-sync wire types and bounded codec.

use std::{io, sync::Arc};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use thiserror::Error;
use zakura_chain::{
    block::{self, merkle::AuthDataRoot},
    ironwood, orchard,
    parameters::{Network, NetworkUpgrade},
    sapling,
    serialization::{SerializationError, ZcashDeserialize, ZcashSerialize},
    work::difficulty::U256,
};

use super::{header_sync_header_bytes_for_network, Frame, MAX_HS_MESSAGE_BYTES, MAX_HS_RANGE};

/// Version-8 `Status` discriminator.
pub const MSG_HS_V8_STATUS: u8 = 1;
/// Version-8 `GetHeaders` discriminator.
pub const MSG_HS_V8_GET_HEADERS: u8 = 2;
/// Version-8 `Headers` discriminator.
pub const MSG_HS_V8_HEADERS: u8 = 3;
/// Version-8 `HeadersOutcome` discriminator.
pub const MSG_HS_V8_HEADERS_OUTCOME: u8 = 4;

const MAX_LOCATOR_HASHES: usize = 13;
const MAX_BODY_SIZE_HINT: u32 = 2_000_000;
const KNOWN_TREE_AUX_SCHEMA_MASK: u32 = 1;
/// Exact encoded length of one immutable tree-aux schema-1 record.
pub const TREE_AUX_SCHEMA_V1_BYTES: usize = 4 + 32 + 32 + 32 + 8 + 8 + 8 + 32;

/// Errors produced while constructing, encoding, or bounded-decoding v8 messages.
#[derive(Debug, Error)]
pub enum HeaderSyncV8WireError {
    /// A payload exceeded the negotiated or hard v8 cap.
    #[error("Zakura header-sync v8 payload length {actual} exceeds cap {max}")]
    OversizedPayload {
        /// Actual payload length.
        actual: usize,
        /// Effective negotiated and hard maximum.
        max: usize,
    },
    /// An unknown message discriminator was received.
    #[error("unknown Zakura header-sync v8 message type {0}")]
    UnknownMessageType(u8),
    /// A frame message type did not fit the one-byte application discriminator.
    #[error("unknown Zakura header-sync v8 frame message type {0}")]
    UnknownFrameMessageType(u16),
    /// A frame and its duplicated payload discriminator disagreed.
    #[error("Zakura header-sync v8 frame type {frame} does not match payload type {payload}")]
    MismatchedFrameMessageType {
        /// Outer frame discriminator.
        frame: u16,
        /// Inner payload discriminator.
        payload: u8,
    },
    /// Version 8 defines no frame flags.
    #[error("unsupported Zakura header-sync v8 frame flags {0:#06x}")]
    UnsupportedFlags(u16),
    /// A request ID was zero.
    #[error("Zakura header-sync v8 {0} request ID must be non-zero")]
    ZeroRequestId(&'static str),
    /// A height exceeded the locally supported block-height range.
    #[error("Zakura header-sync v8 height {0} exceeds the supported range")]
    HeightOutOfRange(u32),
    /// A boolean byte was not its canonical zero or one encoding.
    #[error("Zakura header-sync v8 {field} boolean has invalid value {value}")]
    InvalidBool {
        /// Field containing the marker.
        field: &'static str,
        /// Rejected marker value.
        value: u8,
    },
    /// A count was outside its wire or negotiated bound.
    #[error("Zakura header-sync v8 {field} count {actual} is outside 1..={max}")]
    CountOutOfRange {
        /// Count field being validated.
        field: &'static str,
        /// Rejected count.
        actual: usize,
        /// Effective maximum count.
        max: usize,
    },
    /// A body-size hint exceeded the consensus block-size ceiling.
    #[error("Zakura header-sync v8 body-size hint {0} exceeds 2,000,000 bytes")]
    BodySizeHintOutOfRange(u32),
    /// A schema selector was unknown or was not advertised by the receiver.
    #[error("unsupported Zakura header-sync v8 tree-aux schema {0}")]
    UnsupportedTreeAuxSchema(u8),
    /// A response selector did not match the matching request.
    #[error("Zakura header-sync v8 response schema {actual} does not match requested schema {requested}")]
    ResponseTreeAuxSchemaMismatch {
        /// Schema selected by the matching request.
        requested: u8,
        /// Schema selected by the response.
        actual: u8,
    },
    /// A `Headers` response was decoded without matching request bounds.
    #[error("unsolicited Zakura header-sync v8 Headers response")]
    UnsolicitedHeaders,
    /// Parallel in-memory vectors did not have the same length.
    #[error(
        "Zakura header-sync v8 Headers entry count {entries} does not match auxiliary count {aux}"
    )]
    ParallelLengthMismatch {
        /// Number of header entries.
        entries: usize,
        /// Number of entries carrying auxiliary records.
        aux: usize,
    },
    /// A zero/nonzero header response violated completion semantics.
    #[error("invalid Zakura header-sync v8 Headers completion semantics")]
    InvalidHeadersCompletion,
    /// A returned run did not link to its advertised common ancestor.
    #[error("non-contiguous Zakura header-sync v8 header run")]
    NonContiguousHeaders,
    /// A tree-aux record had the wrong inferred height.
    #[error("Zakura header-sync v8 tree-aux height {actual:?} does not match inferred height {expected:?}")]
    TreeAuxHeightMismatch {
        /// Height inferred from the common ancestor and record offset.
        expected: block::Height,
        /// Height encoded in the record.
        actual: block::Height,
    },
    /// Activation-dependent schema-1 defaults were violated.
    #[error("invalid Zakura header-sync v8 tree-aux defaults at height {height:?}: {field}")]
    InvalidTreeAuxDefault {
        /// Record height.
        height: block::Height,
        /// Field that violated its activation-dependent default.
        field: &'static str,
    },
    /// An outcome discriminator was outside the fixed 1 through 4 range.
    #[error("unknown Zakura header-sync v8 HeadersOutcome value {0}")]
    UnknownOutcome(u8),
    /// Checked message-size or height arithmetic overflowed.
    #[error("numeric overflow while handling Zakura header-sync v8 {0}")]
    NumericOverflow(&'static str),
    /// Bytes remained after the selected message was decoded.
    #[error("trailing bytes in Zakura header-sync v8 payload")]
    TrailingBytes,
    /// An I/O error occurred while handling the message.
    #[error("Zakura header-sync v8 wire I/O error: {0}")]
    Io(#[from] io::Error),
    /// A canonical Zcash type failed to serialize or deserialize.
    #[error("Zakura header-sync v8 Zcash serialization error: {0}")]
    Serialization(#[from] SerializationError),
}

/// Immutable tree-aux selector values understood by this implementation.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum AuxSchemaV8 {
    /// No tree auxiliary records are requested or returned.
    #[default]
    None = 0,
    /// The immutable 156-byte schema defined by protocol version 8.
    V1 = 1,
}

impl AuxSchemaV8 {
    fn decode(value: u8) -> Result<Self, HeaderSyncV8WireError> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::V1),
            value => Err(HeaderSyncV8WireError::UnsupportedTreeAuxSchema(value)),
        }
    }

    fn mask_bit(self) -> u32 {
        match self {
            Self::None => 0,
            Self::V1 => 1,
        }
    }

    fn wire_value(self) -> u8 {
        match self {
            Self::None => 0,
            Self::V1 => 1,
        }
    }
}

/// Peer frontier and resource-cap advertisement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatusV8 {
    /// Height of the anchor excluded from `suffix_cumulative_work`.
    pub work_anchor_height: block::Height,
    /// Hash of the work anchor.
    pub work_anchor_hash: block::Hash,
    /// Height of the sender's selected header tip.
    pub selected_tip_height: block::Height,
    /// Hash of the sender's selected header tip.
    pub selected_tip_hash: block::Hash,
    /// Exact suffix work after the anchor through the selected tip.
    pub suffix_cumulative_work: U256,
    /// Lowest height whose branch data the sender may retain.
    pub oldest_retained_height: block::Height,
    /// Sender's advisory per-response header cap.
    pub max_headers_per_response: u32,
    /// Sender's advisory concurrent-request cap.
    pub max_inflight_requests: u16,
    /// Sender's advisory message byte cap.
    pub max_message_bytes: u32,
    /// Bit `n - 1` advertises support for auxiliary schema `n`.
    pub tree_aux_schema_mask: u32,
}

/// Locator-based request for headers on one exact target branch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetHeadersV8 {
    /// Nonzero correlation identifier.
    pub request_id: u64,
    /// Exact retained target branch to serve.
    pub target_tip_hash: block::Hash,
    /// Ordered, deduplicated locator hashes, limited to 13.
    pub locator_hashes: Vec<block::Hash>,
    /// Maximum number of headers accepted in one response.
    pub max_header_count: u32,
    /// Requested auxiliary schema, or none.
    pub tree_aux_schema: AuxSchemaV8,
}

/// One header with its parallel advisory metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderEntryV8 {
    /// Canonical Zcash block header.
    pub header: Arc<block::Header>,
    /// Unauthenticated serialized-body-size hint; zero means unknown.
    pub body_size: u32,
    /// Parallel schema-1 record when the response selects schema 1.
    pub tree_aux: Option<TreeAuxRecordV1>,
}

/// A bounded response on the exact requested target branch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeadersV8 {
    /// Nonzero correlation identifier.
    pub request_id: u64,
    /// Exact target copied from the matching request.
    pub target_tip_hash: block::Hash,
    /// Height of the selected locator intersection.
    pub common_ancestor_height: block::Height,
    /// Hash of the selected locator intersection.
    pub common_ancestor_hash: block::Hash,
    /// Whether this response reaches `target_tip_hash`.
    pub complete: bool,
    /// Returned auxiliary schema, either none or the requested schema.
    pub tree_aux_schema: AuxSchemaV8,
    /// Headers in ascending order with parallel hints and optional records.
    pub entries: Vec<HeaderEntryV8>,
}

/// Non-data outcomes for a syntactically valid v8 header request.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum HeadersOutcomeCodeV8 {
    /// The exact requested target is no longer retained.
    TargetNotRetained = 1,
    /// No sent locator hash lies on the target path.
    NoLocatorIntersection = 2,
    /// Required target-path history has been pruned.
    HistoryPruned = 3,
    /// The server is temporarily unable to serve the request.
    Busy = 4,
}

impl HeadersOutcomeCodeV8 {
    fn decode(value: u8) -> Result<Self, HeaderSyncV8WireError> {
        match value {
            1 => Ok(Self::TargetNotRetained),
            2 => Ok(Self::NoLocatorIntersection),
            3 => Ok(Self::HistoryPruned),
            4 => Ok(Self::Busy),
            value => Err(HeaderSyncV8WireError::UnknownOutcome(value)),
        }
    }

    fn wire_value(self) -> u8 {
        match self {
            Self::TargetNotRetained => 1,
            Self::NoLocatorIntersection => 2,
            Self::HistoryPruned => 3,
            Self::Busy => 4,
        }
    }
}

/// Explicit non-data response to `GetHeadersV8`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeadersOutcomeV8 {
    /// Nonzero correlation identifier.
    pub request_id: u64,
    /// Exact target copied from the matching request.
    pub target_tip_hash: block::Hash,
    /// Explicit non-data outcome.
    pub outcome: HeadersOutcomeCodeV8,
}

/// Native version-8 header-sync message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeaderSyncMessageV8 {
    /// Advisory peer snapshot and serving caps.
    Status(StatusV8),
    /// Locator-based exact-target request.
    GetHeaders(GetHeadersV8),
    /// Bounded exact-target header response.
    Headers(HeadersV8),
    /// Explicit non-data request outcome.
    HeadersOutcome(HeadersOutcomeV8),
}

impl HeaderSyncMessageV8 {
    /// Return the exact version-8 message discriminator.
    pub fn message_type(&self) -> u8 {
        match self {
            Self::Status(_) => MSG_HS_V8_STATUS,
            Self::GetHeaders(_) => MSG_HS_V8_GET_HEADERS,
            Self::Headers(_) => MSG_HS_V8_HEADERS,
            Self::HeadersOutcome(_) => MSG_HS_V8_HEADERS_OUTCOME,
        }
    }
}

/// Immutable schema-1 commitment inputs for one inferred block height.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeAuxRecordV1 {
    /// Exact inferred height of this record.
    pub height: block::Height,
    /// End-of-block Sapling note-commitment root.
    pub sapling_root: sapling::tree::Root,
    /// End-of-block Orchard root, empty below NU5.
    pub orchard_root: orchard::tree::Root,
    /// End-of-block Ironwood root, empty before configured NU7.
    pub ironwood_root: ironwood::tree::Root,
    /// Per-block Sapling shielded transaction count.
    pub sapling_tx_count: u64,
    /// Per-block Orchard shielded transaction count, zero below NU5.
    pub orchard_tx_count: u64,
    /// Per-block Ironwood shielded transaction count, zero before configured NU7.
    pub ironwood_tx_count: u64,
    /// ZIP-244 authorizing-data root, all zero below NU5.
    pub auth_data_root: AuthDataRoot,
}

impl TreeAuxRecordV1 {
    /// Validate the inferred height and all activation-dependent canonical defaults.
    pub fn validate_for(
        &self,
        expected_height: block::Height,
        network: &Network,
    ) -> Result<(), HeaderSyncV8WireError> {
        if self.height != expected_height {
            return Err(HeaderSyncV8WireError::TreeAuxHeightMismatch {
                expected: expected_height,
                actual: self.height,
            });
        }

        if NetworkUpgrade::Nu5
            .activation_height(network)
            .is_none_or(|height| expected_height < height)
        {
            if self.orchard_root != orchard::tree::Root::default() {
                return Err(HeaderSyncV8WireError::InvalidTreeAuxDefault {
                    height: expected_height,
                    field: "orchard_root",
                });
            }
            if self.orchard_tx_count != 0 {
                return Err(HeaderSyncV8WireError::InvalidTreeAuxDefault {
                    height: expected_height,
                    field: "orchard_tx_count",
                });
            }
            if self.auth_data_root != AuthDataRoot::from([0; 32]) {
                return Err(HeaderSyncV8WireError::InvalidTreeAuxDefault {
                    height: expected_height,
                    field: "auth_data_root",
                });
            }
        }

        if NetworkUpgrade::Nu7
            .activation_height(network)
            .is_none_or(|height| expected_height < height)
        {
            if self.ironwood_root != ironwood::tree::Root::default() {
                return Err(HeaderSyncV8WireError::InvalidTreeAuxDefault {
                    height: expected_height,
                    field: "ironwood_root",
                });
            }
            if self.ironwood_tx_count != 0 {
                return Err(HeaderSyncV8WireError::InvalidTreeAuxDefault {
                    height: expected_height,
                    field: "ironwood_tx_count",
                });
            }
        }
        Ok(())
    }

    fn encode_to<W: io::Write>(&self, writer: &mut W) -> Result<(), HeaderSyncV8WireError> {
        writer.write_u32::<LittleEndian>(self.height.0)?;
        self.sapling_root.zcash_serialize(&mut *writer)?;
        self.orchard_root.zcash_serialize(&mut *writer)?;
        self.ironwood_root.zcash_serialize(&mut *writer)?;
        writer.write_u64::<LittleEndian>(self.sapling_tx_count)?;
        writer.write_u64::<LittleEndian>(self.orchard_tx_count)?;
        writer.write_u64::<LittleEndian>(self.ironwood_tx_count)?;
        writer.write_all(&<[u8; 32]>::from(self.auth_data_root))?;
        Ok(())
    }

    fn decode_from<R: io::Read>(reader: &mut R) -> Result<Self, HeaderSyncV8WireError> {
        let height = read_height(reader)?;
        let sapling_root = sapling::tree::Root::zcash_deserialize(&mut *reader)?;
        let orchard_root = orchard::tree::Root::zcash_deserialize(&mut *reader)?;
        let ironwood_root = ironwood::tree::Root::zcash_deserialize(&mut *reader)?;
        let sapling_tx_count = reader.read_u64::<LittleEndian>()?;
        let orchard_tx_count = reader.read_u64::<LittleEndian>()?;
        let ironwood_tx_count = reader.read_u64::<LittleEndian>()?;
        let mut auth_data_root = [0; 32];
        reader.read_exact(&mut auth_data_root)?;
        Ok(Self {
            height,
            sapling_root,
            orchard_root,
            ironwood_root,
            sapling_tx_count,
            orchard_tx_count,
            ironwood_tx_count,
            auth_data_root: AuthDataRoot::from(auth_data_root),
        })
    }
}

/// Bounds from the matching in-flight request, required before decoding a response vector.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HeaderSyncV8DecodeContext {
    /// Maximum count selected by the matching request.
    pub max_header_count: u32,
    /// Auxiliary schema selected by the matching request.
    pub requested_tree_aux_schema: AuxSchemaV8,
}

/// Standalone bounded codec for the negotiated v8 protocol.
#[derive(Clone, Debug)]
pub struct HeaderSyncV8Codec {
    network: Network,
    message_byte_limit: usize,
    header_count_limit: u32,
    tree_aux_schema_mask: u32,
}

impl HeaderSyncV8Codec {
    /// Construct a codec using negotiated caps, always narrowed by hard protocol limits.
    pub fn new(
        network: Network,
        negotiated_message_bytes: u32,
        local_header_count_limit: u32,
        tree_aux_schema_mask: u32,
    ) -> Self {
        Self {
            network,
            message_byte_limit: usize::try_from(negotiated_message_bytes)
                .unwrap_or(usize::MAX)
                .min(MAX_HS_MESSAGE_BYTES),
            header_count_limit: local_header_count_limit.min(MAX_HS_RANGE),
            tree_aux_schema_mask,
        }
    }

    /// Encode a locally constructed message after applying every v8 wire invariant.
    pub fn encode(&self, message: &HeaderSyncMessageV8) -> Result<Vec<u8>, HeaderSyncV8WireError> {
        let mut bytes = Vec::new();
        bytes.write_u8(message.message_type())?;
        match message {
            HeaderSyncMessageV8::Status(status) => self.encode_status(&mut bytes, status)?,
            HeaderSyncMessageV8::GetHeaders(request) => {
                self.validate_get_headers(request)?;
                write_request_id(&mut bytes, request.request_id, "GetHeaders")?;
                request.target_tip_hash.zcash_serialize(&mut bytes)?;
                bytes.write_u8(
                    u8::try_from(request.locator_hashes.len())
                        .map_err(|_| HeaderSyncV8WireError::NumericOverflow("locator count"))?,
                )?;
                for hash in &request.locator_hashes {
                    hash.zcash_serialize(&mut bytes)?;
                }
                bytes.write_u32::<LittleEndian>(request.max_header_count)?;
                bytes.write_u8(request.tree_aux_schema.wire_value())?;
            }
            HeaderSyncMessageV8::Headers(response) => self.encode_headers(&mut bytes, response)?,
            HeaderSyncMessageV8::HeadersOutcome(outcome) => {
                write_request_id(&mut bytes, outcome.request_id, "HeadersOutcome")?;
                outcome.target_tip_hash.zcash_serialize(&mut bytes)?;
                bytes.write_u8(outcome.outcome.wire_value())?;
            }
        }
        self.check_payload_size(bytes.len())?;
        Ok(bytes)
    }

    /// Convert a message into a bounded Zakura frame.
    pub fn encode_frame(
        &self,
        message: &HeaderSyncMessageV8,
    ) -> Result<Frame, HeaderSyncV8WireError> {
        Ok(Frame {
            message_type: u16::from(message.message_type()),
            flags: 0,
            payload: self.encode(message)?,
        })
    }

    /// Decode a message, requiring request bounds for `Headers` and no other message.
    pub fn decode(
        &self,
        bytes: &[u8],
        response_context: Option<HeaderSyncV8DecodeContext>,
    ) -> Result<HeaderSyncMessageV8, HeaderSyncV8WireError> {
        self.check_payload_size(bytes.len())?;
        let mut reader = io::Cursor::new(bytes);
        let message = match reader.read_u8()? {
            MSG_HS_V8_STATUS => HeaderSyncMessageV8::Status(self.decode_status(&mut reader)?),
            MSG_HS_V8_GET_HEADERS => {
                let request_id = read_request_id(&mut reader, "GetHeaders")?;
                let target_tip_hash = block::Hash::zcash_deserialize(&mut reader)?;
                let locator_count = usize::from(reader.read_u8()?);
                validate_nonzero_count("locator", locator_count, MAX_LOCATOR_HASHES)?;
                require_remaining(
                    &reader,
                    bytes.len(),
                    locator_count
                        .checked_mul(32)
                        .and_then(|bytes| bytes.checked_add(5))
                        .ok_or(HeaderSyncV8WireError::NumericOverflow("locator bytes"))?,
                    "locator bytes",
                )?;
                let mut locator_hashes = Vec::with_capacity(locator_count);
                for _ in 0..locator_count {
                    locator_hashes.push(block::Hash::zcash_deserialize(&mut reader)?);
                }
                let max_header_count = reader.read_u32::<LittleEndian>()?;
                let tree_aux_schema = AuxSchemaV8::decode(reader.read_u8()?)?;
                let request = GetHeadersV8 {
                    request_id,
                    target_tip_hash,
                    locator_hashes,
                    max_header_count,
                    tree_aux_schema,
                };
                self.validate_get_headers(&request)?;
                HeaderSyncMessageV8::GetHeaders(request)
            }
            MSG_HS_V8_HEADERS => HeaderSyncMessageV8::Headers(self.decode_headers(
                &mut reader,
                bytes.len(),
                response_context.ok_or(HeaderSyncV8WireError::UnsolicitedHeaders)?,
            )?),
            MSG_HS_V8_HEADERS_OUTCOME => HeaderSyncMessageV8::HeadersOutcome(HeadersOutcomeV8 {
                request_id: read_request_id(&mut reader, "HeadersOutcome")?,
                target_tip_hash: block::Hash::zcash_deserialize(&mut reader)?,
                outcome: HeadersOutcomeCodeV8::decode(reader.read_u8()?)?,
            }),
            value => return Err(HeaderSyncV8WireError::UnknownMessageType(value)),
        };
        reject_trailing(bytes.len(), &reader)?;
        Ok(message)
    }

    /// Decode a v8 message from a Zakura frame after checking type agreement.
    pub fn decode_frame(
        &self,
        frame: Frame,
        response_context: Option<HeaderSyncV8DecodeContext>,
    ) -> Result<HeaderSyncMessageV8, HeaderSyncV8WireError> {
        if frame.flags != 0 {
            return Err(HeaderSyncV8WireError::UnsupportedFlags(frame.flags));
        }
        let message = self.decode(&frame.payload, response_context)?;
        let frame_message_type = u8::try_from(frame.message_type)
            .map_err(|_| HeaderSyncV8WireError::UnknownFrameMessageType(frame.message_type))?;
        if frame_message_type != message.message_type() {
            return Err(HeaderSyncV8WireError::MismatchedFrameMessageType {
                frame: frame.message_type,
                payload: message.message_type(),
            });
        }
        Ok(message)
    }

    fn encode_status<W: io::Write>(
        &self,
        writer: &mut W,
        status: &StatusV8,
    ) -> Result<(), HeaderSyncV8WireError> {
        write_height(writer, status.work_anchor_height)?;
        status.work_anchor_hash.zcash_serialize(&mut *writer)?;
        write_height(writer, status.selected_tip_height)?;
        status.selected_tip_hash.zcash_serialize(&mut *writer)?;
        writer.write_all(&status.suffix_cumulative_work.to_little_endian())?;
        write_height(writer, status.oldest_retained_height)?;
        writer.write_u32::<LittleEndian>(status.max_headers_per_response)?;
        writer.write_u16::<LittleEndian>(status.max_inflight_requests)?;
        writer.write_u32::<LittleEndian>(status.max_message_bytes)?;
        writer.write_u32::<LittleEndian>(status.tree_aux_schema_mask)?;
        Ok(())
    }

    fn decode_status<R: io::Read>(
        &self,
        reader: &mut R,
    ) -> Result<StatusV8, HeaderSyncV8WireError> {
        let work_anchor_height = read_height(reader)?;
        let work_anchor_hash = block::Hash::zcash_deserialize(&mut *reader)?;
        let selected_tip_height = read_height(reader)?;
        let selected_tip_hash = block::Hash::zcash_deserialize(&mut *reader)?;
        let mut work = [0; 32];
        reader.read_exact(&mut work)?;
        Ok(StatusV8 {
            work_anchor_height,
            work_anchor_hash,
            selected_tip_height,
            selected_tip_hash,
            suffix_cumulative_work: U256::from_little_endian(&work),
            oldest_retained_height: read_height(reader)?,
            max_headers_per_response: reader.read_u32::<LittleEndian>()?,
            max_inflight_requests: reader.read_u16::<LittleEndian>()?,
            max_message_bytes: reader.read_u32::<LittleEndian>()?,
            tree_aux_schema_mask: reader.read_u32::<LittleEndian>()?,
        })
    }

    fn validate_get_headers(&self, request: &GetHeadersV8) -> Result<(), HeaderSyncV8WireError> {
        if request.request_id == 0 {
            return Err(HeaderSyncV8WireError::ZeroRequestId("GetHeaders"));
        }
        validate_nonzero_count("locator", request.locator_hashes.len(), MAX_LOCATOR_HASHES)?;
        validate_nonzero_count(
            "max_header",
            usize::try_from(request.max_header_count).unwrap_or(usize::MAX),
            usize::try_from(self.header_count_limit).unwrap_or(usize::MAX),
        )?;
        if request.tree_aux_schema != AuxSchemaV8::None
            && self.tree_aux_schema_mask & request.tree_aux_schema.mask_bit() == 0
        {
            return Err(HeaderSyncV8WireError::UnsupportedTreeAuxSchema(
                request.tree_aux_schema.wire_value(),
            ));
        }
        Ok(())
    }

    fn encode_headers<W: io::Write>(
        &self,
        writer: &mut W,
        response: &HeadersV8,
    ) -> Result<(), HeaderSyncV8WireError> {
        validate_nonzero_id(response.request_id, "Headers")?;
        validate_count_allow_zero(
            "header",
            response.entries.len(),
            usize::try_from(self.header_count_limit).unwrap_or(usize::MAX),
        )?;
        self.validate_headers_semantics(response)?;
        write_request_id(writer, response.request_id, "Headers")?;
        response.target_tip_hash.zcash_serialize(&mut *writer)?;
        write_height(writer, response.common_ancestor_height)?;
        response
            .common_ancestor_hash
            .zcash_serialize(&mut *writer)?;
        writer.write_u32::<LittleEndian>(
            u32::try_from(response.entries.len())
                .map_err(|_| HeaderSyncV8WireError::NumericOverflow("header count"))?,
        )?;
        writer.write_u8(u8::from(response.complete))?;
        writer.write_u8(response.tree_aux_schema.wire_value())?;
        for entry in &response.entries {
            entry.header.zcash_serialize(&mut *writer)?;
        }
        for entry in &response.entries {
            validate_body_size(entry.body_size)?;
            writer.write_u32::<LittleEndian>(entry.body_size)?;
        }
        if response.tree_aux_schema == AuxSchemaV8::V1 {
            for entry in &response.entries {
                entry
                    .tree_aux
                    .as_ref()
                    .expect("schema validation requires one record per entry")
                    .encode_to(writer)?;
            }
        }
        Ok(())
    }

    fn decode_headers(
        &self,
        reader: &mut io::Cursor<&[u8]>,
        total_bytes: usize,
        context: HeaderSyncV8DecodeContext,
    ) -> Result<HeadersV8, HeaderSyncV8WireError> {
        let request_id = read_request_id(reader, "Headers")?;
        let target_tip_hash = block::Hash::zcash_deserialize(&mut *reader)?;
        let common_ancestor_height = read_height(reader)?;
        let common_ancestor_hash = block::Hash::zcash_deserialize(&mut *reader)?;
        let count = usize::try_from(reader.read_u32::<LittleEndian>()?)
            .map_err(|_| HeaderSyncV8WireError::NumericOverflow("header count"))?;
        let complete = read_bool(reader, "complete")?;
        let tree_aux_schema = AuxSchemaV8::decode(reader.read_u8()?)?;
        let max_count = context.max_header_count.min(self.header_count_limit);
        validate_count_allow_zero(
            "header",
            count,
            usize::try_from(max_count).unwrap_or(usize::MAX),
        )?;
        if tree_aux_schema != AuxSchemaV8::None
            && tree_aux_schema != context.requested_tree_aux_schema
        {
            return Err(HeaderSyncV8WireError::ResponseTreeAuxSchemaMismatch {
                requested: context.requested_tree_aux_schema.wire_value(),
                actual: tree_aux_schema.wire_value(),
            });
        }
        let per_entry_min = header_sync_header_bytes_for_network(&self.network)
            .checked_add(4)
            .and_then(|bytes| {
                bytes.checked_add(if tree_aux_schema == AuxSchemaV8::V1 {
                    TREE_AUX_SCHEMA_V1_BYTES
                } else {
                    0
                })
            })
            .ok_or(HeaderSyncV8WireError::NumericOverflow(
                "minimum response size",
            ))?;
        require_remaining(
            reader,
            total_bytes,
            count
                .checked_mul(per_entry_min)
                .ok_or(HeaderSyncV8WireError::NumericOverflow(
                    "minimum response size",
                ))?,
            "header response bytes",
        )?;

        let mut headers = Vec::with_capacity(count);
        for _ in 0..count {
            headers.push(Arc::new(block::Header::zcash_deserialize(&mut *reader)?));
        }
        let mut body_sizes = Vec::with_capacity(count);
        for _ in 0..count {
            let body_size = reader.read_u32::<LittleEndian>()?;
            validate_body_size(body_size)?;
            body_sizes.push(body_size);
        }
        let mut aux = Vec::new();
        if tree_aux_schema == AuxSchemaV8::V1 {
            aux.reserve(count);
            for _ in 0..count {
                aux.push(TreeAuxRecordV1::decode_from(reader)?);
            }
        }
        let entries = headers
            .into_iter()
            .zip(body_sizes)
            .enumerate()
            .map(|(index, (header, body_size))| HeaderEntryV8 {
                header,
                body_size,
                tree_aux: if tree_aux_schema == AuxSchemaV8::V1 {
                    Some(aux[index].clone())
                } else {
                    None
                },
            })
            .collect();
        let response = HeadersV8 {
            request_id,
            target_tip_hash,
            common_ancestor_height,
            common_ancestor_hash,
            complete,
            tree_aux_schema,
            entries,
        };
        self.validate_headers_semantics(&response)?;
        Ok(response)
    }

    fn validate_headers_semantics(
        &self,
        response: &HeadersV8,
    ) -> Result<(), HeaderSyncV8WireError> {
        let empty = response.entries.is_empty();
        if empty != (response.complete && response.common_ancestor_hash == response.target_tip_hash)
        {
            return Err(HeaderSyncV8WireError::InvalidHeadersCompletion);
        }
        if let Some(first) = response.entries.first() {
            if first.header.previous_block_hash != response.common_ancestor_hash {
                return Err(HeaderSyncV8WireError::NonContiguousHeaders);
            }
            for pair in response.entries.windows(2) {
                if block::Hash::from(pair[0].header.as_ref()) != pair[1].header.previous_block_hash
                {
                    return Err(HeaderSyncV8WireError::NonContiguousHeaders);
                }
            }
            if response.complete
                && block::Hash::from(
                    response
                        .entries
                        .last()
                        .expect("non-empty response has a last entry")
                        .header
                        .as_ref(),
                ) != response.target_tip_hash
            {
                return Err(HeaderSyncV8WireError::InvalidHeadersCompletion);
            }
        }

        for (offset, entry) in response.entries.iter().enumerate() {
            validate_body_size(entry.body_size)?;
            let offset = u32::try_from(offset)
                .map_err(|_| HeaderSyncV8WireError::NumericOverflow("tree-aux height offset"))?;
            let inferred_height = response
                .common_ancestor_height
                .0
                .checked_add(1)
                .and_then(|height| height.checked_add(offset))
                .map(block::Height)
                .filter(|height| *height <= block::Height::MAX)
                .ok_or(HeaderSyncV8WireError::NumericOverflow(
                    "inferred header height",
                ))?;
            match (response.tree_aux_schema, &entry.tree_aux) {
                (AuxSchemaV8::None, None) => {}
                (AuxSchemaV8::V1, Some(aux)) => aux.validate_for(inferred_height, &self.network)?,
                _ => {
                    return Err(HeaderSyncV8WireError::ParallelLengthMismatch {
                        entries: response.entries.len(),
                        aux: response
                            .entries
                            .iter()
                            .filter(|entry| entry.tree_aux.is_some())
                            .count(),
                    });
                }
            }
        }
        Ok(())
    }

    fn check_payload_size(&self, actual: usize) -> Result<(), HeaderSyncV8WireError> {
        if actual > self.message_byte_limit {
            return Err(HeaderSyncV8WireError::OversizedPayload {
                actual,
                max: self.message_byte_limit,
            });
        }
        Ok(())
    }
}

fn validate_nonzero_id(
    request_id: u64,
    message: &'static str,
) -> Result<(), HeaderSyncV8WireError> {
    if request_id == 0 {
        return Err(HeaderSyncV8WireError::ZeroRequestId(message));
    }
    Ok(())
}

fn write_request_id<W: io::Write>(
    writer: &mut W,
    request_id: u64,
    message: &'static str,
) -> Result<(), HeaderSyncV8WireError> {
    validate_nonzero_id(request_id, message)?;
    writer.write_u64::<LittleEndian>(request_id)?;
    Ok(())
}

fn read_request_id<R: io::Read>(
    reader: &mut R,
    message: &'static str,
) -> Result<u64, HeaderSyncV8WireError> {
    let request_id = reader.read_u64::<LittleEndian>()?;
    validate_nonzero_id(request_id, message)?;
    Ok(request_id)
}

fn write_height<W: io::Write>(
    writer: &mut W,
    height: block::Height,
) -> Result<(), HeaderSyncV8WireError> {
    if height > block::Height::MAX {
        return Err(HeaderSyncV8WireError::HeightOutOfRange(height.0));
    }
    writer.write_u32::<LittleEndian>(height.0)?;
    Ok(())
}

fn read_height<R: io::Read>(reader: &mut R) -> Result<block::Height, HeaderSyncV8WireError> {
    let height = block::Height(reader.read_u32::<LittleEndian>()?);
    if height > block::Height::MAX {
        return Err(HeaderSyncV8WireError::HeightOutOfRange(height.0));
    }
    Ok(height)
}

fn read_bool<R: io::Read>(
    reader: &mut R,
    field: &'static str,
) -> Result<bool, HeaderSyncV8WireError> {
    match reader.read_u8()? {
        0 => Ok(false),
        1 => Ok(true),
        value => Err(HeaderSyncV8WireError::InvalidBool { field, value }),
    }
}

fn validate_nonzero_count(
    field: &'static str,
    actual: usize,
    max: usize,
) -> Result<(), HeaderSyncV8WireError> {
    if actual == 0 || actual > max {
        return Err(HeaderSyncV8WireError::CountOutOfRange { field, actual, max });
    }
    Ok(())
}

fn validate_count_allow_zero(
    field: &'static str,
    actual: usize,
    max: usize,
) -> Result<(), HeaderSyncV8WireError> {
    if actual > max {
        return Err(HeaderSyncV8WireError::CountOutOfRange { field, actual, max });
    }
    Ok(())
}

fn validate_body_size(body_size: u32) -> Result<(), HeaderSyncV8WireError> {
    if body_size > MAX_BODY_SIZE_HINT {
        return Err(HeaderSyncV8WireError::BodySizeHintOutOfRange(body_size));
    }
    Ok(())
}

fn require_remaining(
    reader: &io::Cursor<&[u8]>,
    total_bytes: usize,
    required: usize,
    field: &'static str,
) -> Result<(), HeaderSyncV8WireError> {
    let consumed = usize::try_from(reader.position())
        .map_err(|_| HeaderSyncV8WireError::NumericOverflow("cursor position"))?;
    let remaining = total_bytes
        .checked_sub(consumed)
        .ok_or(HeaderSyncV8WireError::NumericOverflow("remaining bytes"))?;
    if remaining < required {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("{field} require {required} bytes, only {remaining} remain"),
        )
        .into());
    }
    Ok(())
}

fn reject_trailing(
    total_bytes: usize,
    reader: &io::Cursor<&[u8]>,
) -> Result<(), HeaderSyncV8WireError> {
    let consumed = usize::try_from(reader.position())
        .map_err(|_| HeaderSyncV8WireError::NumericOverflow("cursor position"))?;
    if consumed != total_bytes {
        return Err(HeaderSyncV8WireError::TrailingBytes);
    }
    Ok(())
}

const _: () = assert!(TREE_AUX_SCHEMA_V1_BYTES == 156);
const _: () = assert!(KNOWN_TREE_AUX_SCHEMA_MASK == 1);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zakura::{HeaderSyncDecodeContext, HeaderSyncMessage};
    use zakura_chain::block::genesis::regtest_genesis_block;

    fn codec() -> HeaderSyncV8Codec {
        HeaderSyncV8Codec::new(
            Network::new_regtest(Default::default()),
            u32::try_from(MAX_HS_MESSAGE_BYTES).expect("the 2 MiB hard cap fits in u32"),
            MAX_HS_RANGE,
            KNOWN_TREE_AUX_SCHEMA_MASK,
        )
    }

    fn hash(byte: u8) -> block::Hash {
        block::Hash([byte; 32])
    }

    fn request() -> GetHeadersV8 {
        GetHeadersV8 {
            request_id: 0x0807_0605_0403_0201,
            target_tip_hash: hash(0x22),
            locator_hashes: vec![hash(0x33), hash(0x44)],
            max_header_count: 4000,
            tree_aux_schema: AuxSchemaV8::V1,
        }
    }

    fn empty_aux(height: block::Height) -> TreeAuxRecordV1 {
        TreeAuxRecordV1 {
            height,
            sapling_root: sapling::tree::Root::default(),
            orchard_root: orchard::tree::Root::default(),
            ironwood_root: ironwood::tree::Root::default(),
            sapling_tx_count: 0,
            orchard_tx_count: 0,
            ironwood_tx_count: 0,
            auth_data_root: AuthDataRoot::from([0; 32]),
        }
    }

    #[test]
    fn status_golden_vector_both_directions_and_exact_work_width() {
        let status = StatusV8 {
            work_anchor_height: block::Height(7),
            work_anchor_hash: hash(0x11),
            selected_tip_height: block::Height(9),
            selected_tip_hash: hash(0x22),
            suffix_cumulative_work: U256::from_little_endian(&[
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                23, 24, 25, 26, 27, 28, 29, 30, 31,
            ]),
            oldest_retained_height: block::Height(5),
            max_headers_per_response: 4000,
            max_inflight_requests: 16,
            max_message_bytes: 2_097_152,
            tree_aux_schema_mask: 0x8000_0001,
        };
        let mut golden = vec![MSG_HS_V8_STATUS];
        golden.extend_from_slice(&7u32.to_le_bytes());
        golden.extend_from_slice(&[0x11; 32]);
        golden.extend_from_slice(&9u32.to_le_bytes());
        golden.extend_from_slice(&[0x22; 32]);
        golden.extend(0u8..32);
        golden.extend_from_slice(&5u32.to_le_bytes());
        golden.extend_from_slice(&4000u32.to_le_bytes());
        golden.extend_from_slice(&16u16.to_le_bytes());
        golden.extend_from_slice(&2_097_152u32.to_le_bytes());
        golden.extend_from_slice(&0x8000_0001u32.to_le_bytes());

        let message = HeaderSyncMessageV8::Status(status);
        assert_eq!(
            codec().encode(&message).expect("valid status encodes"),
            golden
        );
        assert_eq!(
            codec()
                .decode(&golden, None)
                .expect("golden status decodes"),
            message
        );
        assert!(matches!(
            codec().decode(&golden[..golden.len() - 1], None),
            Err(HeaderSyncV8WireError::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof
        ));

        let mut zero_caps = golden;
        zero_caps[109..123].fill(0);
        assert!(matches!(
            codec()
                .decode(&zero_caps, None)
                .expect("zero received serving caps are a valid pure-requester status"),
            HeaderSyncMessageV8::Status(StatusV8 {
                max_headers_per_response: 0,
                max_inflight_requests: 0,
                max_message_bytes: 0,
                tree_aux_schema_mask: 0,
                ..
            })
        ));
    }

    #[test]
    fn get_headers_golden_vector_both_directions() {
        let request = request();
        let mut golden = vec![MSG_HS_V8_GET_HEADERS];
        golden.extend_from_slice(&request.request_id.to_le_bytes());
        golden.extend_from_slice(&[0x22; 32]);
        golden.push(2);
        golden.extend_from_slice(&[0x33; 32]);
        golden.extend_from_slice(&[0x44; 32]);
        golden.extend_from_slice(&4000u32.to_le_bytes());
        golden.push(1);

        let message = HeaderSyncMessageV8::GetHeaders(request);
        assert_eq!(
            codec().encode(&message).expect("valid request encodes"),
            golden
        );
        assert_eq!(
            codec()
                .decode(&golden, None)
                .expect("golden request decodes"),
            message
        );
    }

    #[test]
    fn headers_golden_vector_uses_parallel_wire_sections() {
        let header = regtest_genesis_block().header.clone();
        let target_tip_hash = block::Hash::from(header.as_ref());
        let response = HeadersV8 {
            request_id: 9,
            target_tip_hash,
            common_ancestor_height: block::Height(0),
            common_ancestor_hash: header.previous_block_hash,
            complete: true,
            tree_aux_schema: AuxSchemaV8::V1,
            entries: vec![HeaderEntryV8 {
                header: header.clone(),
                body_size: 2_000_000,
                tree_aux: Some(empty_aux(block::Height(1))),
            }],
        };
        let mut golden = vec![MSG_HS_V8_HEADERS];
        golden.extend_from_slice(&9u64.to_le_bytes());
        target_tip_hash
            .zcash_serialize(&mut golden)
            .expect("hash serialization succeeds");
        golden.extend_from_slice(&0u32.to_le_bytes());
        header
            .previous_block_hash
            .zcash_serialize(&mut golden)
            .expect("hash serialization succeeds");
        golden.extend_from_slice(&1u32.to_le_bytes());
        golden.push(1);
        golden.push(1);
        header
            .zcash_serialize(&mut golden)
            .expect("header serialization succeeds");
        golden.extend_from_slice(&2_000_000u32.to_le_bytes());
        empty_aux(block::Height(1))
            .encode_to(&mut golden)
            .expect("tree aux serialization succeeds");

        assert_eq!(
            codec()
                .encode(&HeaderSyncMessageV8::Headers(response.clone()))
                .expect("valid response encodes"),
            golden
        );
        assert_eq!(
            codec()
                .decode(
                    &golden,
                    Some(HeaderSyncV8DecodeContext {
                        max_header_count: 1,
                        requested_tree_aux_schema: AuxSchemaV8::V1,
                    }),
                )
                .expect("golden response decodes"),
            HeaderSyncMessageV8::Headers(response)
        );

        let header_offset = 1 + 8 + 32 + 4 + 32 + 4 + 1 + 1;
        let hint_offset = header_offset + header_sync_header_bytes_for_network(&codec().network);
        let aux_offset = hint_offset + 4;
        assert_eq!(
            &golden[hint_offset..aux_offset],
            &2_000_000u32.to_le_bytes()
        );
        assert_eq!(golden.len() - aux_offset, TREE_AUX_SCHEMA_V1_BYTES);
    }

    #[test]
    fn headers_outcome_golden_vector_both_directions() {
        let message = HeaderSyncMessageV8::HeadersOutcome(HeadersOutcomeV8 {
            request_id: 7,
            target_tip_hash: hash(0xaa),
            outcome: HeadersOutcomeCodeV8::HistoryPruned,
        });
        let mut golden = vec![MSG_HS_V8_HEADERS_OUTCOME];
        golden.extend_from_slice(&7u64.to_le_bytes());
        golden.extend_from_slice(&[0xaa; 32]);
        golden.push(3);
        assert_eq!(
            codec().encode(&message).expect("valid outcome encodes"),
            golden
        );
        assert_eq!(
            codec()
                .decode(&golden, None)
                .expect("golden outcome decodes"),
            message
        );
    }

    #[test]
    fn discriminant_four_is_decoded_only_by_the_negotiated_codec() {
        let outcome = HeaderSyncMessageV8::HeadersOutcome(HeadersOutcomeV8 {
            request_id: 7,
            target_tip_hash: hash(0xaa),
            outcome: HeadersOutcomeCodeV8::HistoryPruned,
        });
        let outcome_frame = codec()
            .encode_frame(&outcome)
            .expect("valid v8 outcome encodes as a frame");
        assert_eq!(
            codec()
                .decode_frame(outcome_frame.clone(), None)
                .expect("a negotiated v8 codec decodes outcome discriminator 4"),
            outcome
        );
        assert!(
            HeaderSyncMessage::decode_frame(outcome_frame, HeaderSyncDecodeContext::control())
                .is_err(),
            "a negotiated v7 codec must not reinterpret a v8 outcome as NewBlock"
        );

        let new_block = HeaderSyncMessage::NewBlock(regtest_genesis_block());
        let new_block_frame = new_block
            .encode_frame(None)
            .expect("valid v7 NewBlock encodes as a frame");
        assert!(
            codec().decode_frame(new_block_frame, None).is_err(),
            "a negotiated v8 codec must not reinterpret a v7 NewBlock as an outcome"
        );
    }

    #[test]
    fn bounded_decode_rejects_discriminants_ids_bools_heights_and_trailing_bytes() {
        assert!(matches!(
            codec().decode(&[5], None),
            Err(HeaderSyncV8WireError::UnknownMessageType(5))
        ));

        let outcome = codec()
            .encode(&HeaderSyncMessageV8::HeadersOutcome(HeadersOutcomeV8 {
                request_id: 1,
                target_tip_hash: hash(0),
                outcome: HeadersOutcomeCodeV8::Busy,
            }))
            .expect("valid outcome encodes");
        let mut zero_id = outcome.clone();
        zero_id[1..9].fill(0);
        assert!(matches!(
            codec().decode(&zero_id, None),
            Err(HeaderSyncV8WireError::ZeroRequestId("HeadersOutcome"))
        ));
        let mut unknown_outcome = outcome.clone();
        *unknown_outcome.last_mut().expect("outcome has a code") = 5;
        assert!(matches!(
            codec().decode(&unknown_outcome, None),
            Err(HeaderSyncV8WireError::UnknownOutcome(5))
        ));
        let mut trailing = outcome;
        trailing.push(0);
        assert!(matches!(
            codec().decode(&trailing, None),
            Err(HeaderSyncV8WireError::TrailingBytes)
        ));

        let empty_headers = HeadersV8 {
            request_id: 1,
            target_tip_hash: hash(1),
            common_ancestor_height: block::Height(1),
            common_ancestor_hash: hash(1),
            complete: true,
            tree_aux_schema: AuxSchemaV8::None,
            entries: vec![],
        };
        let bytes = codec()
            .encode(&HeaderSyncMessageV8::Headers(empty_headers))
            .expect("valid empty completion encodes");
        let mut invalid_bool = bytes.clone();
        invalid_bool[81] = 2;
        assert!(matches!(
            codec().decode(
                &invalid_bool,
                Some(HeaderSyncV8DecodeContext {
                    max_header_count: 1,
                    requested_tree_aux_schema: AuxSchemaV8::None,
                })
            ),
            Err(HeaderSyncV8WireError::InvalidBool { .. })
        ));
        let mut invalid_height = bytes;
        invalid_height[41..45].copy_from_slice(&(block::Height::MAX.0 + 1).to_le_bytes());
        assert!(matches!(
            codec().decode(
                &invalid_height,
                Some(HeaderSyncV8DecodeContext {
                    max_header_count: 1,
                    requested_tree_aux_schema: AuxSchemaV8::None,
                })
            ),
            Err(HeaderSyncV8WireError::HeightOutOfRange(_))
        ));
    }

    #[test]
    fn request_bounds_and_schema_advertisement_are_enforced() {
        for locator_count in [0, 14] {
            let mut request = request();
            request.locator_hashes = vec![hash(1); locator_count];
            assert!(matches!(
                codec().encode(&HeaderSyncMessageV8::GetHeaders(request)),
                Err(HeaderSyncV8WireError::CountOutOfRange {
                    field: "locator",
                    ..
                })
            ));
        }
        for count in [0, MAX_HS_RANGE + 1] {
            let mut request = request();
            request.max_header_count = count;
            assert!(matches!(
                codec().encode(&HeaderSyncMessageV8::GetHeaders(request)),
                Err(HeaderSyncV8WireError::CountOutOfRange {
                    field: "max_header",
                    ..
                })
            ));
        }
        let no_aux_codec = HeaderSyncV8Codec::new(Network::Mainnet, 1000, MAX_HS_RANGE, 0);
        assert!(matches!(
            no_aux_codec.encode(&HeaderSyncMessageV8::GetHeaders(request())),
            Err(HeaderSyncV8WireError::UnsupportedTreeAuxSchema(1))
        ));

        let mut encoded = codec()
            .encode(&HeaderSyncMessageV8::GetHeaders(request()))
            .expect("valid request encodes");
        *encoded.last_mut().expect("request has a schema byte") = 2;
        assert!(matches!(
            codec().decode(&encoded, None),
            Err(HeaderSyncV8WireError::UnsupportedTreeAuxSchema(2))
        ));
    }

    #[test]
    fn payload_and_response_count_caps_apply_before_vector_decode() {
        let small_codec = HeaderSyncV8Codec::new(Network::Mainnet, 8, MAX_HS_RANGE, 1);
        assert!(matches!(
            small_codec.decode(&[0; 9], None),
            Err(HeaderSyncV8WireError::OversizedPayload { actual: 9, max: 8 })
        ));

        let mut response = vec![MSG_HS_V8_HEADERS];
        response.extend_from_slice(&1u64.to_le_bytes());
        response.extend_from_slice(&[0; 32]);
        response.extend_from_slice(&0u32.to_le_bytes());
        response.extend_from_slice(&[0; 32]);
        response.extend_from_slice(&2u32.to_le_bytes());
        response.push(0);
        response.push(0);
        assert!(matches!(
            codec().decode(
                &response,
                Some(HeaderSyncV8DecodeContext {
                    max_header_count: 1,
                    requested_tree_aux_schema: AuxSchemaV8::None,
                })
            ),
            Err(HeaderSyncV8WireError::CountOutOfRange {
                field: "header",
                ..
            })
        ));
    }

    #[test]
    fn body_hints_completion_and_aux_defaults_are_enforced() {
        let header = regtest_genesis_block().header.clone();
        let mut response = HeadersV8 {
            request_id: 1,
            target_tip_hash: block::Hash::from(header.as_ref()),
            common_ancestor_height: block::Height(0),
            common_ancestor_hash: header.previous_block_hash,
            complete: true,
            tree_aux_schema: AuxSchemaV8::None,
            entries: vec![HeaderEntryV8 {
                header,
                body_size: 2_000_001,
                tree_aux: None,
            }],
        };
        assert!(matches!(
            codec().encode(&HeaderSyncMessageV8::Headers(response.clone())),
            Err(HeaderSyncV8WireError::BodySizeHintOutOfRange(2_000_001))
        ));
        response.entries[0].body_size = 0;
        assert!(codec()
            .encode(&HeaderSyncMessageV8::Headers(response.clone()))
            .is_ok());
        response.complete = false;
        response.entries.clear();
        assert!(matches!(
            codec().encode(&HeaderSyncMessageV8::Headers(response)),
            Err(HeaderSyncV8WireError::InvalidHeadersCompletion)
        ));

        let network = Network::Mainnet;
        let pre_nu5 = NetworkUpgrade::Nu5
            .activation_height(&network)
            .expect("mainnet has NU5")
            .previous()
            .expect("NU5 activates above genesis");
        let mut aux = empty_aux(pre_nu5);
        aux.orchard_tx_count = 1;
        assert!(matches!(
            aux.validate_for(pre_nu5, &network),
            Err(HeaderSyncV8WireError::InvalidTreeAuxDefault {
                field: "orchard_tx_count",
                ..
            })
        ));
        let before_unconfigured_nu7 = NetworkUpgrade::Nu5
            .activation_height(&network)
            .expect("mainnet has NU5");
        let mut aux = empty_aux(before_unconfigured_nu7);
        aux.ironwood_tx_count = 1;
        assert!(matches!(
            aux.validate_for(before_unconfigured_nu7, &network),
            Err(HeaderSyncV8WireError::InvalidTreeAuxDefault {
                field: "ironwood_tx_count",
                ..
            })
        ));
    }

    #[test]
    fn v8_discriminator_four_never_guesses_v7_new_block() {
        let block = regtest_genesis_block();
        let mut v7_shaped = vec![MSG_HS_V8_HEADERS_OUTCOME];
        block
            .zcash_serialize(&mut v7_shaped)
            .expect("genesis block serialization succeeds");
        assert!(codec().decode(&v7_shaped, None).is_err());
    }

    #[test]
    fn decode_rejects_each_vector_hint_schema_and_byte_boundary() {
        let valid_request = codec()
            .encode(&HeaderSyncMessageV8::GetHeaders(request()))
            .expect("valid request encodes");
        for locator_count in [0, 14] {
            let mut malformed = valid_request.clone();
            malformed[41] = locator_count;
            assert!(matches!(
                codec().decode(&malformed, None),
                Err(HeaderSyncV8WireError::CountOutOfRange {
                    field: "locator",
                    ..
                })
            ));
        }
        let max_count_offset = valid_request.len() - 5;
        for count in [0, MAX_HS_RANGE + 1] {
            let mut malformed = valid_request.clone();
            malformed[max_count_offset..max_count_offset + 4].copy_from_slice(&count.to_le_bytes());
            assert!(matches!(
                codec().decode(&malformed, None),
                Err(HeaderSyncV8WireError::CountOutOfRange {
                    field: "max_header",
                    ..
                })
            ));
        }

        let header = regtest_genesis_block().header.clone();
        let response = HeadersV8 {
            request_id: 3,
            target_tip_hash: block::Hash::from(header.as_ref()),
            common_ancestor_height: block::Height(0),
            common_ancestor_hash: header.previous_block_hash,
            complete: true,
            tree_aux_schema: AuxSchemaV8::V1,
            entries: vec![HeaderEntryV8 {
                header,
                body_size: 1,
                tree_aux: Some(empty_aux(block::Height(1))),
            }],
        };
        let encoded = codec()
            .encode(&HeaderSyncMessageV8::Headers(response))
            .expect("valid response encodes");
        let context = HeaderSyncV8DecodeContext {
            max_header_count: 1,
            requested_tree_aux_schema: AuxSchemaV8::V1,
        };
        let header_offset = 83;
        let hint_offset = header_offset + header_sync_header_bytes_for_network(&codec().network);
        let aux_offset = hint_offset + 4;

        let mut bad_hint = encoded.clone();
        bad_hint[hint_offset..aux_offset].copy_from_slice(&2_000_001u32.to_le_bytes());
        assert!(matches!(
            codec().decode(&bad_hint, Some(context)),
            Err(HeaderSyncV8WireError::BodySizeHintOutOfRange(2_000_001))
        ));
        let mut wrong_aux_height = encoded.clone();
        wrong_aux_height[aux_offset..aux_offset + 4].copy_from_slice(&2u32.to_le_bytes());
        assert!(matches!(
            codec().decode(&wrong_aux_height, Some(context)),
            Err(HeaderSyncV8WireError::TreeAuxHeightMismatch { .. })
        ));
        assert!(matches!(
            codec().decode(&encoded[..encoded.len() - 1], Some(context)),
            Err(HeaderSyncV8WireError::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof
        ));

        let mut mismatched_schema = encoded;
        mismatched_schema[82] = AuxSchemaV8::V1.wire_value();
        assert!(matches!(
            codec().decode(
                &mismatched_schema,
                Some(HeaderSyncV8DecodeContext {
                    max_header_count: 1,
                    requested_tree_aux_schema: AuxSchemaV8::None,
                })
            ),
            Err(HeaderSyncV8WireError::ResponseTreeAuxSchemaMismatch {
                requested: 0,
                actual: 1
            })
        ));

        let hard_cap_codec = HeaderSyncV8Codec::new(Network::Mainnet, u32::MAX, 1, 0);
        let over_hard_cap = vec![0; MAX_HS_MESSAGE_BYTES + 1];
        assert!(matches!(
            hard_cap_codec.decode(&over_hard_cap, None),
            Err(HeaderSyncV8WireError::OversizedPayload {
                actual,
                max: MAX_HS_MESSAGE_BYTES
            }) if actual == MAX_HS_MESSAGE_BYTES + 1
        ));
    }
}
