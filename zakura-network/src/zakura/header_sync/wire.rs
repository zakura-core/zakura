//! Native Zakura header-sync wire types and bounded codec.

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

use super::{header_sync_header_bytes_for_network, Frame, HeaderSyncRequestId};

/// Zakura stream kind reserved for native header sync.
pub const ZAKURA_STREAM_HEADER_SYNC: u16 = 5;
/// The sole supported header-sync stream version.
///
/// This is a transport compatibility barrier, not a codec selector.
pub const ZAKURA_HEADER_SYNC_STREAM_VERSION: u16 = 8;
/// Maximum encoded header-sync message bytes.
pub const MAX_HS_MESSAGE_BYTES: usize = 2 * 1024 * 1024;
/// Default number of headers advertised per response.
pub const DEFAULT_HS_RANGE: u32 = 1000;
/// Maximum number of headers accepted in one response.
pub const MAX_HS_RANGE: u32 = 4000;
/// Default number of in-flight header requests advertised per peer.
pub const DEFAULT_HS_MAX_INFLIGHT: u16 = 10;

/// Header-sync `Status` discriminator.
pub const MSG_HS_STATUS: u8 = 1;
/// Header-sync `GetHeaders` discriminator.
pub const MSG_HS_GET_HEADERS: u8 = 2;
/// Header-sync `Headers` discriminator.
pub const MSG_HS_HEADERS: u8 = 3;
/// Header-sync `HeadersOutcome` discriminator.
pub const MSG_HS_HEADERS_OUTCOME: u8 = 4;

const MAX_LOCATOR_HASHES: usize = 13;
const MAX_BODY_SIZE_HINT: u32 = 2_000_000;
const KNOWN_TREE_AUX_SCHEMA_MASK: u32 = 1;
/// Exact encoded length of one immutable tree-aux schema-1 record.
pub const TREE_AUX_SCHEMA_V1_BYTES: usize = 4 + 32 + 32 + 32 + 8 + 8 + 8 + 32;

/// Errors produced while constructing, encoding, or bounded-decoding messages.
#[derive(Debug, Error)]
pub enum HeaderSyncWireError {
    /// A payload exceeded the negotiated or hard cap.
    #[error("Zakura header-sync payload length {actual} exceeds cap {max}")]
    OversizedPayload {
        /// Actual payload length.
        actual: usize,
        /// Effective negotiated and hard maximum.
        max: usize,
    },
    /// An unknown message discriminator was received.
    #[error("unknown Zakura header-sync message type {0}")]
    UnknownMessageType(u8),
    /// A frame message type did not fit the one-byte application discriminator.
    #[error("unknown Zakura header-sync frame message type {0}")]
    UnknownFrameMessageType(u16),
    /// A frame and its duplicated payload discriminator disagreed.
    #[error("Zakura header-sync frame type {frame} does not match payload type {payload}")]
    MismatchedFrameMessageType {
        /// Outer frame discriminator.
        frame: u16,
        /// Inner payload discriminator.
        payload: u8,
    },
    /// Header sync defines no frame flags.
    #[error("unsupported Zakura header-sync frame flags {0:#06x}")]
    UnsupportedFlags(u16),
    /// A request ID was zero.
    #[error("Zakura header-sync {0} request ID must be non-zero")]
    ZeroRequestId(&'static str),
    /// A height exceeded the locally supported block-height range.
    #[error("Zakura header-sync height {0} exceeds the supported range")]
    HeightOutOfRange(u32),
    /// A boolean byte was not its canonical zero or one encoding.
    #[error("Zakura header-sync {field} boolean has invalid value {value}")]
    InvalidBool {
        /// Field containing the marker.
        field: &'static str,
        /// Rejected marker value.
        value: u8,
    },
    /// A count was outside its wire or negotiated bound.
    #[error("Zakura header-sync {field} count {actual} is outside 1..={max}")]
    CountOutOfRange {
        /// Count field being validated.
        field: &'static str,
        /// Rejected count.
        actual: usize,
        /// Effective maximum count.
        max: usize,
    },
    /// A body-size hint exceeded the consensus block-size ceiling.
    #[error("Zakura header-sync body-size hint {0} exceeds 2,000,000 bytes")]
    BodySizeHintOutOfRange(u32),
    /// A schema selector was unknown or was not advertised by the receiver.
    #[error("unsupported Zakura header-sync tree-aux schema {0}")]
    UnsupportedTreeAuxSchema(u8),
    /// A response selector did not match the matching request.
    #[error(
        "Zakura header-sync response schema {actual} does not match requested schema {requested}"
    )]
    ResponseTreeAuxSchemaMismatch {
        /// Schema selected by the matching request.
        requested: u8,
        /// Schema selected by the response.
        actual: u8,
    },
    /// A `Headers` response was decoded without matching request bounds.
    #[error("unsolicited Zakura header-sync Headers response")]
    UnsolicitedHeaders,
    /// Parallel in-memory vectors did not have the same length.
    #[error(
        "Zakura header-sync Headers entry count {entries} does not match auxiliary count {aux}"
    )]
    ParallelLengthMismatch {
        /// Number of header entries.
        entries: usize,
        /// Number of entries carrying auxiliary records.
        aux: usize,
    },
    /// A zero/nonzero header response violated completion semantics.
    #[error("invalid Zakura header-sync Headers completion semantics")]
    InvalidHeadersCompletion,
    /// A returned run did not link to its advertised common ancestor.
    #[error("non-contiguous Zakura header-sync header run")]
    NonContiguousHeaders,
    /// A tree-aux record had the wrong inferred height.
    #[error(
        "Zakura header-sync tree-aux height {actual:?} does not match inferred height {expected:?}"
    )]
    TreeAuxHeightMismatch {
        /// Height inferred from the common ancestor and record offset.
        expected: block::Height,
        /// Height encoded in the record.
        actual: block::Height,
    },
    /// Activation-dependent schema-1 defaults were violated.
    #[error("invalid Zakura header-sync tree-aux defaults at height {height:?}: {field}")]
    InvalidTreeAuxDefault {
        /// Record height.
        height: block::Height,
        /// Field that violated its activation-dependent default.
        field: &'static str,
    },
    /// An outcome discriminator was outside the fixed 1 through 4 range.
    #[error("unknown Zakura header-sync HeadersOutcome value {0}")]
    UnknownOutcome(u8),
    /// Checked message-size or height arithmetic overflowed.
    #[error("numeric overflow while handling Zakura header-sync {0}")]
    NumericOverflow(&'static str),
    /// Bytes remained after the selected message was decoded.
    #[error("trailing bytes in Zakura header-sync payload")]
    TrailingBytes,
    /// An I/O error occurred while handling the message.
    #[error("Zakura header-sync wire I/O error: {0}")]
    Io(#[from] io::Error),
    /// A canonical Zcash type failed to serialize or deserialize.
    #[error("Zakura header-sync Zcash serialization error: {0}")]
    Serialization(#[from] SerializationError),
}

/// Immutable tree-aux selector values understood by this implementation.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum AuxSchema {
    /// No tree auxiliary records are requested or returned.
    #[default]
    None = 0,
    /// The immutable 156-byte schema defined by protocol version 8.
    V1 = 1,
}

impl AuxSchema {
    fn decode(value: u8) -> Result<Self, HeaderSyncWireError> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::V1),
            value => Err(HeaderSyncWireError::UnsupportedTreeAuxSchema(value)),
        }
    }

    pub(crate) fn mask_bit(self) -> u32 {
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
pub struct Status {
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

/// Effective nonzero local serving limits advertised in [`Status`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ServeCapabilities {
    /// Maximum headers returned in one response.
    max_headers_per_response: u32,
    /// Maximum concurrent requests served for one peer.
    max_inflight_requests: u16,
    /// Maximum encoded application-message bytes.
    max_message_bytes: u32,
    /// Supported auxiliary schema bits.
    tree_aux_schema_mask: u32,
}

impl ServeCapabilities {
    /// Construct effective local limits, rejecting unusable zero serving caps.
    pub fn new(
        max_headers_per_response: u32,
        max_inflight_requests: u16,
        max_message_bytes: u32,
        tree_aux_schema_mask: u32,
    ) -> Option<Self> {
        (max_headers_per_response != 0 && max_inflight_requests != 0 && max_message_bytes != 0)
            .then_some(Self {
                max_headers_per_response,
                max_inflight_requests,
                max_message_bytes,
                tree_aux_schema_mask,
            })
    }

    pub(crate) fn max_headers_per_response(self) -> u32 {
        self.max_headers_per_response
    }

    pub(crate) fn tree_aux_schema_mask(self) -> u32 {
        self.tree_aux_schema_mask
    }
}

impl Status {
    /// Build one advertisement exclusively from an atomic committed snapshot and local limits.
    pub fn from_snapshot(
        snapshot: &zakura_header_chain::EngineSnapshot,
        capabilities: &ServeCapabilities,
    ) -> Self {
        Self {
            work_anchor_height: snapshot.frontiers.finalized.height,
            work_anchor_hash: snapshot.frontiers.finalized.hash,
            selected_tip_height: snapshot.frontiers.header_best.height,
            selected_tip_hash: snapshot.frontiers.header_best.hash,
            suffix_cumulative_work: snapshot.header_best_score.suffix_work.as_u256(),
            oldest_retained_height: snapshot.oldest_retained_height,
            max_headers_per_response: capabilities.max_headers_per_response,
            max_inflight_requests: capabilities.max_inflight_requests,
            max_message_bytes: capabilities.max_message_bytes,
            tree_aux_schema_mask: capabilities.tree_aux_schema_mask,
        }
    }
}

/// Locator-based request for headers on one exact target branch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetHeaders {
    /// Nonzero correlation identifier.
    pub request_id: u64,
    /// Exact retained target branch to serve.
    pub target_tip_hash: block::Hash,
    /// Ordered, deduplicated locator hashes, limited to 13.
    pub locator_hashes: Vec<block::Hash>,
    /// Maximum number of headers accepted in one response.
    pub max_header_count: u32,
    /// Requested auxiliary schema, or none.
    pub tree_aux_schema: AuxSchema,
}

/// One header with its parallel advisory metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderEntry {
    /// Canonical Zcash block header.
    pub header: Arc<block::Header>,
    /// Unauthenticated serialized-body-size hint; zero means unknown.
    pub body_size: u32,
    /// Parallel schema-1 record when the response selects schema 1.
    pub tree_aux: Option<TreeAuxRecordV1>,
}

/// A bounded response on the exact requested target branch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Headers {
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
    pub tree_aux_schema: AuxSchema,
    /// Headers in ascending order with parallel hints and optional records.
    pub entries: Vec<HeaderEntry>,
}

/// Non-data outcomes for a syntactically valid header request.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum HeadersOutcomeCode {
    /// The exact requested target is no longer retained.
    TargetNotRetained = 1,
    /// No sent locator hash lies on the target path.
    NoLocatorIntersection = 2,
    /// Required target-path history has been pruned.
    HistoryPruned = 3,
    /// The server is temporarily unable to serve the request.
    Busy = 4,
}

impl HeadersOutcomeCode {
    fn decode(value: u8) -> Result<Self, HeaderSyncWireError> {
        match value {
            1 => Ok(Self::TargetNotRetained),
            2 => Ok(Self::NoLocatorIntersection),
            3 => Ok(Self::HistoryPruned),
            4 => Ok(Self::Busy),
            value => Err(HeaderSyncWireError::UnknownOutcome(value)),
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

/// Explicit non-data response to `GetHeaders`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeadersOutcome {
    /// Nonzero correlation identifier.
    pub request_id: u64,
    /// Exact target copied from the matching request.
    pub target_tip_hash: block::Hash,
    /// Explicit non-data outcome.
    pub outcome: HeadersOutcomeCode,
}

/// Native header-sync header-sync message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeaderSyncMessage {
    /// Advisory peer snapshot and serving caps.
    Status(Status),
    /// Locator-based exact-target request.
    GetHeaders(GetHeaders),
    /// Bounded exact-target header response.
    Headers(Headers),
    /// Explicit non-data request outcome.
    HeadersOutcome(HeadersOutcome),
}

impl HeaderSyncMessage {
    /// Return the exact header-sync message discriminator.
    pub fn message_type(&self) -> u8 {
        match self {
            Self::Status(_) => MSG_HS_STATUS,
            Self::GetHeaders(_) => MSG_HS_GET_HEADERS,
            Self::Headers(_) => MSG_HS_HEADERS,
            Self::HeadersOutcome(_) => MSG_HS_HEADERS_OUTCOME,
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
    ) -> Result<(), HeaderSyncWireError> {
        if self.height != expected_height {
            return Err(HeaderSyncWireError::TreeAuxHeightMismatch {
                expected: expected_height,
                actual: self.height,
            });
        }

        if NetworkUpgrade::Nu5
            .activation_height(network)
            .is_none_or(|height| expected_height < height)
        {
            if self.orchard_root != orchard::tree::Root::default() {
                return Err(HeaderSyncWireError::InvalidTreeAuxDefault {
                    height: expected_height,
                    field: "orchard_root",
                });
            }
            if self.orchard_tx_count != 0 {
                return Err(HeaderSyncWireError::InvalidTreeAuxDefault {
                    height: expected_height,
                    field: "orchard_tx_count",
                });
            }
            if self.auth_data_root != AuthDataRoot::from([0; 32]) {
                return Err(HeaderSyncWireError::InvalidTreeAuxDefault {
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
                return Err(HeaderSyncWireError::InvalidTreeAuxDefault {
                    height: expected_height,
                    field: "ironwood_root",
                });
            }
            if self.ironwood_tx_count != 0 {
                return Err(HeaderSyncWireError::InvalidTreeAuxDefault {
                    height: expected_height,
                    field: "ironwood_tx_count",
                });
            }
        }
        Ok(())
    }

    fn encode_to<W: io::Write>(&self, writer: &mut W) -> Result<(), HeaderSyncWireError> {
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

    fn decode_from<R: io::Read>(reader: &mut R) -> Result<Self, HeaderSyncWireError> {
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
pub struct HeaderSyncDecodeContext {
    /// Maximum count selected by the matching request.
    pub max_header_count: u32,
    /// Auxiliary schema selected by the matching request.
    pub requested_tree_aux_schema: AuxSchema,
}

/// Standalone bounded codec for the negotiated protocol.
#[derive(Clone, Debug)]
pub struct HeaderSyncCodec {
    network: Network,
    message_byte_limit: usize,
    header_count_limit: u32,
    tree_aux_schema_mask: u32,
}

impl HeaderSyncCodec {
    /// Read only the correlation ID needed to select bounded response decode context.
    pub(crate) fn peek_headers_request_id(
        frame: &Frame,
    ) -> Result<HeaderSyncRequestId, HeaderSyncWireError> {
        if u8::try_from(frame.message_type).ok() != Some(MSG_HS_HEADERS) {
            return Err(HeaderSyncWireError::UnknownFrameMessageType(
                frame.message_type,
            ));
        }
        let mut reader = io::Cursor::new(frame.payload.as_slice());
        if reader.read_u8()? != MSG_HS_HEADERS {
            return Err(HeaderSyncWireError::MismatchedFrameMessageType {
                frame: frame.message_type,
                payload: frame.payload.first().copied().unwrap_or_default(),
            });
        }
        let request_id = read_request_id(&mut reader, "Headers")?;
        HeaderSyncRequestId::new(request_id).ok_or(HeaderSyncWireError::ZeroRequestId("Headers"))
    }

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

    /// Encode a locally constructed message after applying every wire invariant.
    pub fn encode(&self, message: &HeaderSyncMessage) -> Result<Vec<u8>, HeaderSyncWireError> {
        let mut bytes = Vec::new();
        bytes.write_u8(message.message_type())?;
        match message {
            HeaderSyncMessage::Status(status) => self.encode_status(&mut bytes, status)?,
            HeaderSyncMessage::GetHeaders(request) => {
                self.validate_get_headers(request)?;
                write_request_id(&mut bytes, request.request_id, "GetHeaders")?;
                request.target_tip_hash.zcash_serialize(&mut bytes)?;
                bytes.write_u8(
                    u8::try_from(request.locator_hashes.len())
                        .map_err(|_| HeaderSyncWireError::NumericOverflow("locator count"))?,
                )?;
                for hash in &request.locator_hashes {
                    hash.zcash_serialize(&mut bytes)?;
                }
                bytes.write_u32::<LittleEndian>(request.max_header_count)?;
                bytes.write_u8(request.tree_aux_schema.wire_value())?;
            }
            HeaderSyncMessage::Headers(response) => self.encode_headers(&mut bytes, response)?,
            HeaderSyncMessage::HeadersOutcome(outcome) => {
                write_request_id(&mut bytes, outcome.request_id, "HeadersOutcome")?;
                outcome.target_tip_hash.zcash_serialize(&mut bytes)?;
                bytes.write_u8(outcome.outcome.wire_value())?;
            }
        }
        self.check_payload_size(bytes.len())?;
        Ok(bytes)
    }

    /// Convert a message into a bounded Zakura frame.
    pub fn encode_frame(&self, message: &HeaderSyncMessage) -> Result<Frame, HeaderSyncWireError> {
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
        response_context: Option<HeaderSyncDecodeContext>,
    ) -> Result<HeaderSyncMessage, HeaderSyncWireError> {
        self.check_payload_size(bytes.len())?;
        let mut reader = io::Cursor::new(bytes);
        let message = match reader.read_u8()? {
            MSG_HS_STATUS => HeaderSyncMessage::Status(self.decode_status(&mut reader)?),
            MSG_HS_GET_HEADERS => {
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
                        .ok_or(HeaderSyncWireError::NumericOverflow("locator bytes"))?,
                    "locator bytes",
                )?;
                let mut locator_hashes = Vec::with_capacity(locator_count);
                for _ in 0..locator_count {
                    locator_hashes.push(block::Hash::zcash_deserialize(&mut reader)?);
                }
                let max_header_count = reader.read_u32::<LittleEndian>()?;
                let tree_aux_schema = AuxSchema::decode(reader.read_u8()?)?;
                let request = GetHeaders {
                    request_id,
                    target_tip_hash,
                    locator_hashes,
                    max_header_count,
                    tree_aux_schema,
                };
                self.validate_get_headers(&request)?;
                HeaderSyncMessage::GetHeaders(request)
            }
            MSG_HS_HEADERS => HeaderSyncMessage::Headers(self.decode_headers(
                &mut reader,
                bytes.len(),
                response_context.ok_or(HeaderSyncWireError::UnsolicitedHeaders)?,
            )?),
            MSG_HS_HEADERS_OUTCOME => HeaderSyncMessage::HeadersOutcome(HeadersOutcome {
                request_id: read_request_id(&mut reader, "HeadersOutcome")?,
                target_tip_hash: block::Hash::zcash_deserialize(&mut reader)?,
                outcome: HeadersOutcomeCode::decode(reader.read_u8()?)?,
            }),
            value => return Err(HeaderSyncWireError::UnknownMessageType(value)),
        };
        reject_trailing(bytes.len(), &reader)?;
        Ok(message)
    }

    /// Decode a message from a Zakura frame after checking type agreement.
    pub fn decode_frame(
        &self,
        frame: Frame,
        response_context: Option<HeaderSyncDecodeContext>,
    ) -> Result<HeaderSyncMessage, HeaderSyncWireError> {
        if frame.flags != 0 {
            return Err(HeaderSyncWireError::UnsupportedFlags(frame.flags));
        }
        let message = self.decode(&frame.payload, response_context)?;
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

    fn encode_status<W: io::Write>(
        &self,
        writer: &mut W,
        status: &Status,
    ) -> Result<(), HeaderSyncWireError> {
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

    fn decode_status<R: io::Read>(&self, reader: &mut R) -> Result<Status, HeaderSyncWireError> {
        let work_anchor_height = read_height(reader)?;
        let work_anchor_hash = block::Hash::zcash_deserialize(&mut *reader)?;
        let selected_tip_height = read_height(reader)?;
        let selected_tip_hash = block::Hash::zcash_deserialize(&mut *reader)?;
        let mut work = [0; 32];
        reader.read_exact(&mut work)?;
        Ok(Status {
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

    fn validate_get_headers(&self, request: &GetHeaders) -> Result<(), HeaderSyncWireError> {
        if request.request_id == 0 {
            return Err(HeaderSyncWireError::ZeroRequestId("GetHeaders"));
        }
        validate_nonzero_count("locator", request.locator_hashes.len(), MAX_LOCATOR_HASHES)?;
        validate_nonzero_count(
            "max_header",
            usize::try_from(request.max_header_count).unwrap_or(usize::MAX),
            usize::try_from(self.header_count_limit).unwrap_or(usize::MAX),
        )?;
        if request.tree_aux_schema != AuxSchema::None
            && self.tree_aux_schema_mask & request.tree_aux_schema.mask_bit() == 0
        {
            return Err(HeaderSyncWireError::UnsupportedTreeAuxSchema(
                request.tree_aux_schema.wire_value(),
            ));
        }
        Ok(())
    }

    fn encode_headers<W: io::Write>(
        &self,
        writer: &mut W,
        response: &Headers,
    ) -> Result<(), HeaderSyncWireError> {
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
                .map_err(|_| HeaderSyncWireError::NumericOverflow("header count"))?,
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
        if response.tree_aux_schema == AuxSchema::V1 {
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
        context: HeaderSyncDecodeContext,
    ) -> Result<Headers, HeaderSyncWireError> {
        let request_id = read_request_id(reader, "Headers")?;
        let target_tip_hash = block::Hash::zcash_deserialize(&mut *reader)?;
        let common_ancestor_height = read_height(reader)?;
        let common_ancestor_hash = block::Hash::zcash_deserialize(&mut *reader)?;
        let count = usize::try_from(reader.read_u32::<LittleEndian>()?)
            .map_err(|_| HeaderSyncWireError::NumericOverflow("header count"))?;
        let complete = read_bool(reader, "complete")?;
        let tree_aux_schema = AuxSchema::decode(reader.read_u8()?)?;
        let max_count = context.max_header_count.min(self.header_count_limit);
        validate_count_allow_zero(
            "header",
            count,
            usize::try_from(max_count).unwrap_or(usize::MAX),
        )?;
        if tree_aux_schema != AuxSchema::None
            && tree_aux_schema != context.requested_tree_aux_schema
        {
            return Err(HeaderSyncWireError::ResponseTreeAuxSchemaMismatch {
                requested: context.requested_tree_aux_schema.wire_value(),
                actual: tree_aux_schema.wire_value(),
            });
        }
        let per_entry_min = header_sync_header_bytes_for_network(&self.network)
            .checked_add(4)
            .and_then(|bytes| {
                bytes.checked_add(if tree_aux_schema == AuxSchema::V1 {
                    TREE_AUX_SCHEMA_V1_BYTES
                } else {
                    0
                })
            })
            .ok_or(HeaderSyncWireError::NumericOverflow(
                "minimum response size",
            ))?;
        require_remaining(
            reader,
            total_bytes,
            count
                .checked_mul(per_entry_min)
                .ok_or(HeaderSyncWireError::NumericOverflow(
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
        if tree_aux_schema == AuxSchema::V1 {
            aux.reserve(count);
            for _ in 0..count {
                aux.push(TreeAuxRecordV1::decode_from(reader)?);
            }
        }
        let entries = headers
            .into_iter()
            .zip(body_sizes)
            .enumerate()
            .map(|(index, (header, body_size))| HeaderEntry {
                header,
                body_size,
                tree_aux: if tree_aux_schema == AuxSchema::V1 {
                    Some(aux[index].clone())
                } else {
                    None
                },
            })
            .collect();
        let response = Headers {
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

    fn validate_headers_semantics(&self, response: &Headers) -> Result<(), HeaderSyncWireError> {
        let empty = response.entries.is_empty();
        if empty != (response.complete && response.common_ancestor_hash == response.target_tip_hash)
        {
            return Err(HeaderSyncWireError::InvalidHeadersCompletion);
        }
        if let Some(first) = response.entries.first() {
            if first.header.previous_block_hash != response.common_ancestor_hash {
                return Err(HeaderSyncWireError::NonContiguousHeaders);
            }
            for pair in response.entries.windows(2) {
                if block::Hash::from(pair[0].header.as_ref()) != pair[1].header.previous_block_hash
                {
                    return Err(HeaderSyncWireError::NonContiguousHeaders);
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
                return Err(HeaderSyncWireError::InvalidHeadersCompletion);
            }
        }

        for (offset, entry) in response.entries.iter().enumerate() {
            validate_body_size(entry.body_size)?;
            let offset = u32::try_from(offset)
                .map_err(|_| HeaderSyncWireError::NumericOverflow("tree-aux height offset"))?;
            let inferred_height = response
                .common_ancestor_height
                .0
                .checked_add(1)
                .and_then(|height| height.checked_add(offset))
                .map(block::Height)
                .filter(|height| *height <= block::Height::MAX)
                .ok_or(HeaderSyncWireError::NumericOverflow(
                    "inferred header height",
                ))?;
            match (response.tree_aux_schema, &entry.tree_aux) {
                (AuxSchema::None, None) => {}
                (AuxSchema::V1, Some(aux)) => aux.validate_for(inferred_height, &self.network)?,
                _ => {
                    return Err(HeaderSyncWireError::ParallelLengthMismatch {
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

    fn check_payload_size(&self, actual: usize) -> Result<(), HeaderSyncWireError> {
        if actual > self.message_byte_limit {
            return Err(HeaderSyncWireError::OversizedPayload {
                actual,
                max: self.message_byte_limit,
            });
        }
        Ok(())
    }
}

fn validate_nonzero_id(request_id: u64, message: &'static str) -> Result<(), HeaderSyncWireError> {
    if request_id == 0 {
        return Err(HeaderSyncWireError::ZeroRequestId(message));
    }
    Ok(())
}

fn write_request_id<W: io::Write>(
    writer: &mut W,
    request_id: u64,
    message: &'static str,
) -> Result<(), HeaderSyncWireError> {
    validate_nonzero_id(request_id, message)?;
    writer.write_u64::<LittleEndian>(request_id)?;
    Ok(())
}

fn read_request_id<R: io::Read>(
    reader: &mut R,
    message: &'static str,
) -> Result<u64, HeaderSyncWireError> {
    let request_id = reader.read_u64::<LittleEndian>()?;
    validate_nonzero_id(request_id, message)?;
    Ok(request_id)
}

fn write_height<W: io::Write>(
    writer: &mut W,
    height: block::Height,
) -> Result<(), HeaderSyncWireError> {
    if height > block::Height::MAX {
        return Err(HeaderSyncWireError::HeightOutOfRange(height.0));
    }
    writer.write_u32::<LittleEndian>(height.0)?;
    Ok(())
}

fn read_height<R: io::Read>(reader: &mut R) -> Result<block::Height, HeaderSyncWireError> {
    let height = block::Height(reader.read_u32::<LittleEndian>()?);
    if height > block::Height::MAX {
        return Err(HeaderSyncWireError::HeightOutOfRange(height.0));
    }
    Ok(height)
}

fn read_bool<R: io::Read>(
    reader: &mut R,
    field: &'static str,
) -> Result<bool, HeaderSyncWireError> {
    match reader.read_u8()? {
        0 => Ok(false),
        1 => Ok(true),
        value => Err(HeaderSyncWireError::InvalidBool { field, value }),
    }
}

fn validate_nonzero_count(
    field: &'static str,
    actual: usize,
    max: usize,
) -> Result<(), HeaderSyncWireError> {
    if actual == 0 || actual > max {
        return Err(HeaderSyncWireError::CountOutOfRange { field, actual, max });
    }
    Ok(())
}

fn validate_count_allow_zero(
    field: &'static str,
    actual: usize,
    max: usize,
) -> Result<(), HeaderSyncWireError> {
    if actual > max {
        return Err(HeaderSyncWireError::CountOutOfRange { field, actual, max });
    }
    Ok(())
}

fn validate_body_size(body_size: u32) -> Result<(), HeaderSyncWireError> {
    if body_size > MAX_BODY_SIZE_HINT {
        return Err(HeaderSyncWireError::BodySizeHintOutOfRange(body_size));
    }
    Ok(())
}

fn require_remaining(
    reader: &io::Cursor<&[u8]>,
    total_bytes: usize,
    required: usize,
    field: &'static str,
) -> Result<(), HeaderSyncWireError> {
    let consumed = usize::try_from(reader.position())
        .map_err(|_| HeaderSyncWireError::NumericOverflow("cursor position"))?;
    let remaining = total_bytes
        .checked_sub(consumed)
        .ok_or(HeaderSyncWireError::NumericOverflow("remaining bytes"))?;
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
) -> Result<(), HeaderSyncWireError> {
    let consumed = usize::try_from(reader.position())
        .map_err(|_| HeaderSyncWireError::NumericOverflow("cursor position"))?;
    if consumed != total_bytes {
        return Err(HeaderSyncWireError::TrailingBytes);
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

    fn codec() -> HeaderSyncCodec {
        HeaderSyncCodec::new(
            Network::new_regtest(Default::default()),
            u32::try_from(MAX_HS_MESSAGE_BYTES).expect("the 2 MiB hard cap fits in u32"),
            MAX_HS_RANGE,
            KNOWN_TREE_AUX_SCHEMA_MASK,
        )
    }

    fn hash(byte: u8) -> block::Hash {
        block::Hash([byte; 32])
    }

    #[test]
    fn local_serve_capabilities_reject_zero_resource_limits() {
        assert!(ServeCapabilities::new(0, 1, 1, 1).is_none());
        assert!(ServeCapabilities::new(1, 0, 1, 1).is_none());
        assert!(ServeCapabilities::new(1, 1, 0, 1).is_none());
        assert!(ServeCapabilities::new(1, 1, 1, 0).is_some());
    }

    fn request() -> GetHeaders {
        GetHeaders {
            request_id: 0x0807_0605_0403_0201,
            target_tip_hash: hash(0x22),
            locator_hashes: vec![hash(0x33), hash(0x44)],
            max_header_count: 4000,
            tree_aux_schema: AuxSchema::V1,
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
        let status = Status {
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
        let mut golden = vec![MSG_HS_STATUS];
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

        let message = HeaderSyncMessage::Status(status);
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
            Err(HeaderSyncWireError::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof
        ));

        let mut zero_caps = golden;
        zero_caps[109..123].fill(0);
        assert!(matches!(
            codec()
                .decode(&zero_caps, None)
                .expect("zero received serving caps are a valid pure-requester status"),
            HeaderSyncMessage::Status(Status {
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
        let mut golden = vec![MSG_HS_GET_HEADERS];
        golden.extend_from_slice(&request.request_id.to_le_bytes());
        golden.extend_from_slice(&[0x22; 32]);
        golden.push(2);
        golden.extend_from_slice(&[0x33; 32]);
        golden.extend_from_slice(&[0x44; 32]);
        golden.extend_from_slice(&4000u32.to_le_bytes());
        golden.push(1);

        let message = HeaderSyncMessage::GetHeaders(request);
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
        let response = Headers {
            request_id: 9,
            target_tip_hash,
            common_ancestor_height: block::Height(0),
            common_ancestor_hash: header.previous_block_hash,
            complete: true,
            tree_aux_schema: AuxSchema::V1,
            entries: vec![HeaderEntry {
                header: header.clone(),
                body_size: 2_000_000,
                tree_aux: Some(empty_aux(block::Height(1))),
            }],
        };
        let mut golden = vec![MSG_HS_HEADERS];
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
                .encode(&HeaderSyncMessage::Headers(response.clone()))
                .expect("valid response encodes"),
            golden
        );
        assert_eq!(
            codec()
                .decode(
                    &golden,
                    Some(HeaderSyncDecodeContext {
                        max_header_count: 1,
                        requested_tree_aux_schema: AuxSchema::V1,
                    }),
                )
                .expect("golden response decodes"),
            HeaderSyncMessage::Headers(response)
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
        let message = HeaderSyncMessage::HeadersOutcome(HeadersOutcome {
            request_id: 7,
            target_tip_hash: hash(0xaa),
            outcome: HeadersOutcomeCode::HistoryPruned,
        });
        let mut golden = vec![MSG_HS_HEADERS_OUTCOME];
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
    fn discriminant_four_is_decoded_only_as_headers_outcome() {
        let outcome = HeaderSyncMessage::HeadersOutcome(HeadersOutcome {
            request_id: 7,
            target_tip_hash: hash(0xaa),
            outcome: HeadersOutcomeCode::HistoryPruned,
        });
        let outcome_frame = codec()
            .encode_frame(&outcome)
            .expect("valid outcome encodes as a frame");
        assert_eq!(
            codec()
                .decode_frame(outcome_frame, None)
                .expect("a negotiated codec decodes outcome discriminator 4"),
            outcome
        );
        let mut block_relay_payload = vec![MSG_HS_HEADERS_OUTCOME];
        regtest_genesis_block()
            .zcash_serialize(&mut block_relay_payload)
            .expect("genesis block serialization succeeds");
        let block_relay_frame = Frame {
            message_type: u16::from(MSG_HS_HEADERS_OUTCOME),
            flags: 0,
            payload: block_relay_payload,
        };
        assert!(codec().decode_frame(block_relay_frame, None).is_err());
    }

    #[test]
    fn bounded_decode_rejects_discriminants_ids_bools_heights_and_trailing_bytes() {
        assert!(matches!(
            codec().decode(&[5], None),
            Err(HeaderSyncWireError::UnknownMessageType(5))
        ));

        let outcome = codec()
            .encode(&HeaderSyncMessage::HeadersOutcome(HeadersOutcome {
                request_id: 1,
                target_tip_hash: hash(0),
                outcome: HeadersOutcomeCode::Busy,
            }))
            .expect("valid outcome encodes");
        let mut zero_id = outcome.clone();
        zero_id[1..9].fill(0);
        assert!(matches!(
            codec().decode(&zero_id, None),
            Err(HeaderSyncWireError::ZeroRequestId("HeadersOutcome"))
        ));
        let mut unknown_outcome = outcome.clone();
        *unknown_outcome.last_mut().expect("outcome has a code") = 5;
        assert!(matches!(
            codec().decode(&unknown_outcome, None),
            Err(HeaderSyncWireError::UnknownOutcome(5))
        ));
        let mut trailing = outcome;
        trailing.push(0);
        assert!(matches!(
            codec().decode(&trailing, None),
            Err(HeaderSyncWireError::TrailingBytes)
        ));

        let empty_headers = Headers {
            request_id: 1,
            target_tip_hash: hash(1),
            common_ancestor_height: block::Height(1),
            common_ancestor_hash: hash(1),
            complete: true,
            tree_aux_schema: AuxSchema::None,
            entries: vec![],
        };
        let bytes = codec()
            .encode(&HeaderSyncMessage::Headers(empty_headers))
            .expect("valid empty completion encodes");
        let mut invalid_bool = bytes.clone();
        invalid_bool[81] = 2;
        assert!(matches!(
            codec().decode(
                &invalid_bool,
                Some(HeaderSyncDecodeContext {
                    max_header_count: 1,
                    requested_tree_aux_schema: AuxSchema::None,
                })
            ),
            Err(HeaderSyncWireError::InvalidBool { .. })
        ));
        let mut invalid_height = bytes;
        invalid_height[41..45].copy_from_slice(&(block::Height::MAX.0 + 1).to_le_bytes());
        assert!(matches!(
            codec().decode(
                &invalid_height,
                Some(HeaderSyncDecodeContext {
                    max_header_count: 1,
                    requested_tree_aux_schema: AuxSchema::None,
                })
            ),
            Err(HeaderSyncWireError::HeightOutOfRange(_))
        ));
    }

    #[test]
    fn request_bounds_and_schema_advertisement_are_enforced() {
        for locator_count in [0, 14] {
            let mut request = request();
            request.locator_hashes = vec![hash(1); locator_count];
            assert!(matches!(
                codec().encode(&HeaderSyncMessage::GetHeaders(request)),
                Err(HeaderSyncWireError::CountOutOfRange {
                    field: "locator",
                    ..
                })
            ));
        }
        for count in [0, MAX_HS_RANGE + 1] {
            let mut request = request();
            request.max_header_count = count;
            assert!(matches!(
                codec().encode(&HeaderSyncMessage::GetHeaders(request)),
                Err(HeaderSyncWireError::CountOutOfRange {
                    field: "max_header",
                    ..
                })
            ));
        }
        let no_aux_codec = HeaderSyncCodec::new(Network::Mainnet, 1000, MAX_HS_RANGE, 0);
        assert!(matches!(
            no_aux_codec.encode(&HeaderSyncMessage::GetHeaders(request())),
            Err(HeaderSyncWireError::UnsupportedTreeAuxSchema(1))
        ));

        let mut encoded = codec()
            .encode(&HeaderSyncMessage::GetHeaders(request()))
            .expect("valid request encodes");
        *encoded.last_mut().expect("request has a schema byte") = 2;
        assert!(matches!(
            codec().decode(&encoded, None),
            Err(HeaderSyncWireError::UnsupportedTreeAuxSchema(2))
        ));
    }

    #[test]
    fn payload_and_response_count_caps_apply_before_vector_decode() {
        let small_codec = HeaderSyncCodec::new(Network::Mainnet, 8, MAX_HS_RANGE, 1);
        assert!(matches!(
            small_codec.decode(&[0; 9], None),
            Err(HeaderSyncWireError::OversizedPayload { actual: 9, max: 8 })
        ));

        let mut response = vec![MSG_HS_HEADERS];
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
                Some(HeaderSyncDecodeContext {
                    max_header_count: 1,
                    requested_tree_aux_schema: AuxSchema::None,
                })
            ),
            Err(HeaderSyncWireError::CountOutOfRange {
                field: "header",
                ..
            })
        ));
    }

    #[test]
    fn body_hints_completion_and_aux_defaults_are_enforced() {
        let header = regtest_genesis_block().header.clone();
        let mut response = Headers {
            request_id: 1,
            target_tip_hash: block::Hash::from(header.as_ref()),
            common_ancestor_height: block::Height(0),
            common_ancestor_hash: header.previous_block_hash,
            complete: true,
            tree_aux_schema: AuxSchema::None,
            entries: vec![HeaderEntry {
                header,
                body_size: 2_000_001,
                tree_aux: None,
            }],
        };
        assert!(matches!(
            codec().encode(&HeaderSyncMessage::Headers(response.clone())),
            Err(HeaderSyncWireError::BodySizeHintOutOfRange(2_000_001))
        ));
        response.entries[0].body_size = 0;
        assert!(codec()
            .encode(&HeaderSyncMessage::Headers(response.clone()))
            .is_ok());
        response.complete = false;
        response.entries.clear();
        assert!(matches!(
            codec().encode(&HeaderSyncMessage::Headers(response)),
            Err(HeaderSyncWireError::InvalidHeadersCompletion)
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
            Err(HeaderSyncWireError::InvalidTreeAuxDefault {
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
            Err(HeaderSyncWireError::InvalidTreeAuxDefault {
                field: "ironwood_tx_count",
                ..
            })
        ));
    }

    #[test]
    fn discriminator_four_never_guesses_block_relay_payload() {
        let block = regtest_genesis_block();
        let mut block_relay_payload = vec![MSG_HS_HEADERS_OUTCOME];
        block
            .zcash_serialize(&mut block_relay_payload)
            .expect("genesis block serialization succeeds");
        assert!(codec().decode(&block_relay_payload, None).is_err());
    }

    #[test]
    fn decode_rejects_each_vector_hint_schema_and_byte_boundary() {
        let valid_request = codec()
            .encode(&HeaderSyncMessage::GetHeaders(request()))
            .expect("valid request encodes");
        for locator_count in [0, 14] {
            let mut malformed = valid_request.clone();
            malformed[41] = locator_count;
            assert!(matches!(
                codec().decode(&malformed, None),
                Err(HeaderSyncWireError::CountOutOfRange {
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
                Err(HeaderSyncWireError::CountOutOfRange {
                    field: "max_header",
                    ..
                })
            ));
        }

        let header = regtest_genesis_block().header.clone();
        let response = Headers {
            request_id: 3,
            target_tip_hash: block::Hash::from(header.as_ref()),
            common_ancestor_height: block::Height(0),
            common_ancestor_hash: header.previous_block_hash,
            complete: true,
            tree_aux_schema: AuxSchema::V1,
            entries: vec![HeaderEntry {
                header,
                body_size: 1,
                tree_aux: Some(empty_aux(block::Height(1))),
            }],
        };
        let encoded = codec()
            .encode(&HeaderSyncMessage::Headers(response))
            .expect("valid response encodes");
        let context = HeaderSyncDecodeContext {
            max_header_count: 1,
            requested_tree_aux_schema: AuxSchema::V1,
        };
        let header_offset = 83;
        let hint_offset = header_offset + header_sync_header_bytes_for_network(&codec().network);
        let aux_offset = hint_offset + 4;

        let mut bad_hint = encoded.clone();
        bad_hint[hint_offset..aux_offset].copy_from_slice(&2_000_001u32.to_le_bytes());
        assert!(matches!(
            codec().decode(&bad_hint, Some(context)),
            Err(HeaderSyncWireError::BodySizeHintOutOfRange(2_000_001))
        ));
        let mut wrong_aux_height = encoded.clone();
        wrong_aux_height[aux_offset..aux_offset + 4].copy_from_slice(&2u32.to_le_bytes());
        assert!(matches!(
            codec().decode(&wrong_aux_height, Some(context)),
            Err(HeaderSyncWireError::TreeAuxHeightMismatch { .. })
        ));
        assert!(matches!(
            codec().decode(&encoded[..encoded.len() - 1], Some(context)),
            Err(HeaderSyncWireError::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof
        ));

        let mut mismatched_schema = encoded;
        mismatched_schema[82] = AuxSchema::V1.wire_value();
        assert!(matches!(
            codec().decode(
                &mismatched_schema,
                Some(HeaderSyncDecodeContext {
                    max_header_count: 1,
                    requested_tree_aux_schema: AuxSchema::None,
                })
            ),
            Err(HeaderSyncWireError::ResponseTreeAuxSchemaMismatch {
                requested: 0,
                actual: 1
            })
        ));

        let hard_cap_codec = HeaderSyncCodec::new(Network::Mainnet, u32::MAX, 1, 0);
        let over_hard_cap = vec![0; MAX_HS_MESSAGE_BYTES + 1];
        assert!(matches!(
            hard_cap_codec.decode(&over_hard_cap, None),
            Err(HeaderSyncWireError::OversizedPayload {
                actual,
                max: MAX_HS_MESSAGE_BYTES
            }) if actual == MAX_HS_MESSAGE_BYTES + 1
        ));
    }
}
