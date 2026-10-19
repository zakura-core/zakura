//! Native Zakura discovery wire messages and signed node records.

use std::{
    cmp::Reverse,
    collections::{hash_map::RandomState, HashMap, HashSet},
    fmt,
    io::{self, Cursor, Read, Write},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use rand_10::seq::SliceRandom;
use thiserror::Error;
use tokio::sync::{watch, Mutex};
use zakura_chain::{
    block,
    diagnostic::task::CheckForPanics,
    primitives::ed25519::{Signature, SigningKey},
};
use zakura_quic::{NodeAddr, NodeId, NodeSecretKey};

use super::wire::{record_len, write_record_body, NODE_ID_BYTES, SIGNATURE_BYTES};
use crate::zakura::{
    canonical_ip, regulation::SlotBudget, BlockSyncStatus, ServiceAdmissionDecision,
    ServicePeerDirection, ServicePeerLimits, ServicePeerSnapshot, ZakuraConnId, ZakuraNetworkId,
    ZakuraPeerId, MAX_BS_BLOCKS_PER_REQUEST, MAX_BS_RESPONSE_BYTES, ZAKURA_ALPN_MISMATCH_BACKOFF,
};

/// Native discovery stream kind.
pub const ZAKURA_STREAM_DISCOVERY: u16 = 4;
/// Native discovery stream version.
///
/// Version 2 carries the discriminator in the frame header and bounds every
/// message by the limits below. Version 1 is retired.
pub const ZAKURA_DISCOVERY_STREAM_VERSION: u16 = 2;

/// Discovery hello message carrying the sender's signed record.
pub const MSG_DISCOVERY_HELLO: u8 = 1;
/// Discovery peer sample request.
pub const MSG_DISCOVERY_GET_PEERS: u8 = 2;
/// Discovery peer sample response.
pub const MSG_DISCOVERY_PEERS: u8 = 3;
/// First-party live service summary request.
pub const MSG_DISCOVERY_GET_SERVICES: u8 = 4;
/// First-party live service summary response.
pub const MSG_DISCOVERY_SERVICES: u8 = 5;

/// Maximum bytes in an encoded node record body, its extension area included.
///
/// The fixed fields with [`MAX_DIRECT_ADDRS_PER_RECORD`] IPv6 addresses and
/// [`MAX_SERVICES_PER_RECORD`] maximal service ids fill it exactly.
pub const MAX_NODE_RECORD_BODY_BYTES: usize = 580;
/// Maximum bytes in an encoded node record: its length, body, and signature.
pub const MAX_NODE_RECORD_BYTES: usize = 4 + MAX_NODE_RECORD_BODY_BYTES + SIGNATURE_BYTES;
/// Maximum direct addresses in a node record.
pub const MAX_DIRECT_ADDRS_PER_RECORD: usize = 8;
/// Maximum services in a node record.
pub const MAX_SERVICES_PER_RECORD: usize = 8;
/// Maximum wanted services in a `GetPeers` query.
pub const MAX_GET_PEERS_WANTED_SERVICES: usize = 8;
/// Maximum services requested in a first-party live-service query.
pub const MAX_GET_SERVICES_WANTED: usize = 32;
/// Maximum first-party live summaries in a service response.
pub const MAX_SERVICE_SUMMARIES_PER_RESPONSE: usize = 8;
/// Maximum bytes in one length-delimited live summary payload.
pub const MAX_SERVICE_SUMMARY_BYTES: usize = 256;
/// Maximum records in a discovery response.
pub const MAX_DISCOVERY_RECORDS_PER_RESPONSE: usize = 32;
/// Maximum excluded node ids in a discovery query.
pub const MAX_DISCOVERY_EXCLUDED_NODE_IDS: usize = 256;
/// Maximum bytes in a service id.
pub const MAX_ZAKURA_SERVICE_ID_BYTES: usize = 32;
/// Default lifetime for locally authored discovery records.
pub const DEFAULT_DISCOVERY_RECORD_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// Default minimum interval between active discovery refreshes.
pub const DEFAULT_DISCOVERY_REFRESH_INTERVAL: Duration = Duration::from_secs(10 * 60);
/// Default peer sample size requested by active discovery.
pub const DEFAULT_DISCOVERY_PEER_SAMPLE_LIMIT: usize = 32;
/// Default maximum accepted future TTL on imported records.
pub const DEFAULT_DISCOVERY_MAX_RECORD_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// Default expiry clock-skew tolerance for imported records.
pub const DEFAULT_DISCOVERY_CLOCK_SKEW_TOLERANCE: Duration = Duration::from_secs(5 * 60);
/// Default base backoff for discovery dials.
pub const DEFAULT_DISCOVERY_DIAL_BACKOFF_BASE: Duration = Duration::from_secs(60);
/// Default maximum backoff for discovery dials.
pub const DEFAULT_DISCOVERY_DIAL_BACKOFF_MAX: Duration = Duration::from_secs(60 * 60);
/// Default concurrent native discovery dial cap.
pub const DEFAULT_MAX_CONCURRENT_DISCOVERY_DIALS: usize = 4;
/// Default connection slots kept out of discovery dialing.
pub const DEFAULT_DISCOVERY_CONNECTION_HEADROOM: usize = 4;
/// Default Zakura connection cap used by standalone discovery-state tests.
pub const DEFAULT_DISCOVERY_ZAKURA_MAX_CONNECTIONS: usize = 32;
/// Default lifetime for first-party live service summaries.
pub const DEFAULT_LIVE_SERVICE_SUMMARY_TTL: Duration = Duration::from_secs(30);
/// Epoch length for per-requester peer-sample windows.
///
/// Fixed rather than configurable: it is a disclosure-policy bound, not a tuning knob, and a
/// per-node override would only weaken the property for that node's peers.
pub const DISCOVERY_DISCLOSURE_EPOCH: Duration = Duration::from_secs(10 * 60);

/// Native peer discovery service id.
pub const SERVICE_ID_DISCOVERY: &str = "zakura.discovery.v1";
/// Native header-sync service id.
///
/// Generation `v2` is the request-id correlated header-sync service. Generation `v1`
/// is retired, so a peer advertising only `v1` is not a header-sync candidate.
pub const SERVICE_ID_HEADER_SYNC: &str = "zakura.header_sync.v2";
/// Retired header-sync service id, kept only to keep a pre-upgrade peer's summaries
/// wire-valid. Never advertised, never selected on by itself.
pub const SERVICE_ID_HEADER_SYNC_RETIRED: &str = "zakura.header_sync.v1";
/// Native block-sync service id.
pub const SERVICE_ID_BLOCK_SYNC: &str = "zakura.block_sync.v1";
/// Native legacy gossip service id.
pub const SERVICE_ID_LEGACY_GOSSIP: &str = "zakura.legacy_gossip.v1";
/// Native legacy requests service id.
pub const SERVICE_ID_LEGACY_REQUESTS: &str = "zakura.legacy_requests.v1";
/// Native service discovery service id.
pub const SERVICE_ID_SERVICE_DISCOVERY: &str = "zakura.service_discovery.v1";
/// Retired header-sync summary tag, bound to [`SERVICE_ID_HEADER_SYNC_RETIRED`].
///
/// A pre-upgrade peer still sends this tag with the retired service id. Keeping the
/// binding lets its `Services` messages validate, so retiring header-sync v6 does not
/// tear down discovery with peers that have not upgraded. We never emit it, and the
/// summary is advisory only when the peer independently advertises the current
/// header-sync service.
pub const SUMMARY_TAG_HEADER_SYNC_RETIRED: u16 = 1;
/// Summary tag for [`DiscoveryServiceSummary`].
pub const SUMMARY_TAG_DISCOVERY_V1: u16 = 2;
/// Summary tag for [`BlockSyncServiceSummary`].
pub const SUMMARY_TAG_BLOCK_SYNC_V1: u16 = 3;
/// Summary tag for [`HeaderSyncServiceSummary`].
///
/// A distinct tag rather than a reuse of [`SUMMARY_TAG_HEADER_SYNC_RETIRED`]: peers
/// that predate it fall through to the unknown-tag path and ignore the summary,
/// instead of rejecting it as a service/tag mismatch.
pub const SUMMARY_TAG_HEADER_SYNC: u16 = 4;

const ZAKURA_NODE_RECORD_SIG_DOMAIN: &[u8] = b"zakura-node-record-v1";
/// Node record encoding version, bound into the signature pre-image.
///
/// Records are relayed third-party data, so this version is not negotiated per hop.
/// Version 2 added [`ZakuraNodeRecordBody::extensions`]. Future soft changes go in that
/// extension area and keep this version; only a change to the fixed fields bumps it.
const ZAKURA_NODE_RECORD_FORMAT_VERSION: u16 = 2;

/// A bounded ASCII Zakura service identifier.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ZakuraServiceId(String);

impl ZakuraServiceId {
    /// Creates a bounded ASCII service id.
    pub fn new(value: impl Into<String>) -> Result<Self, DiscoveryWireError> {
        let value = value.into();
        validate_service_id(value.as_bytes())?;
        Ok(Self(value))
    }

    /// Returns the service id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the native peer discovery service id.
    pub fn discovery() -> Self {
        Self::new(SERVICE_ID_DISCOVERY)
            .expect("built-in Zakura discovery service id is non-empty bounded ASCII")
    }

    /// Returns the native header-sync service id.
    pub fn header_sync() -> Self {
        Self::new(SERVICE_ID_HEADER_SYNC)
            .expect("built-in Zakura header-sync service id is non-empty bounded ASCII")
    }

    /// Returns the retired header-sync service id.
    ///
    /// Only used to validate a pre-upgrade peer's summaries; never advertised.
    pub fn header_sync_retired() -> Self {
        Self::new(SERVICE_ID_HEADER_SYNC_RETIRED)
            .expect("built-in retired Zakura header-sync service id is non-empty bounded ASCII")
    }

    /// Returns the native block-sync service id.
    pub fn block_sync() -> Self {
        Self::new(SERVICE_ID_BLOCK_SYNC)
            .expect("built-in Zakura block-sync service id is non-empty bounded ASCII")
    }

    /// Returns the native legacy gossip service id.
    pub fn legacy_gossip() -> Self {
        Self::new(SERVICE_ID_LEGACY_GOSSIP)
            .expect("built-in Zakura legacy gossip service id is non-empty bounded ASCII")
    }

    /// Returns the native legacy requests service id.
    pub fn legacy_requests() -> Self {
        Self::new(SERVICE_ID_LEGACY_REQUESTS)
            .expect("built-in Zakura legacy requests service id is non-empty bounded ASCII")
    }

    /// Returns the native service discovery service id.
    pub fn service_discovery() -> Self {
        Self::new(SERVICE_ID_SERVICE_DISCOVERY)
            .expect("built-in Zakura service discovery id is non-empty bounded ASCII")
    }
}

impl fmt::Display for ZakuraServiceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl TryFrom<&str> for ZakuraServiceId {
    type Error = DiscoveryWireError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<String> for ZakuraServiceId {
    type Error = DiscoveryWireError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// The signed fields in a Zakura node record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ZakuraNodeRecordBody {
    /// The authoring node id.
    pub node_id: NodeId,
    /// Direct dial addresses advertised by the author.
    pub direct_addrs: Vec<SocketAddr>,
    /// Native services advertised by the author.
    pub services: Vec<ZakuraServiceId>,
    /// Lowest supported Zakura protocol version.
    pub zakura_protocol_min: u16,
    /// Highest supported Zakura protocol version.
    pub zakura_protocol_max: u16,
    /// Zakura network id.
    pub network_id: ZakuraNetworkId,
    /// Genesis hash / chain id.
    pub chain_id: [u8; 32],
    /// Monotonic author sequence number.
    pub sequence: u64,
    /// Unix timestamp when this record expires.
    pub expires_at_unix_secs: u64,
    /// Opaque signed extension area: every body byte after the fixed fields.
    ///
    /// Future record fields go here as TLVs (`u16` type, `u16` length, value bytes), so a
    /// release can add record contents, such as a new address kind, without a hard break.
    /// A reader ignores TLVs it does not know. This release does not parse the area: it
    /// keeps the bytes verbatim, so the signature still verifies and a relayed record
    /// reaches the next hop unchanged. [`MAX_NODE_RECORD_BODY_BYTES`] bounds the whole
    /// body, extension area included.
    ///
    /// A new address kind MUST NOT appear as a new family byte in `direct_addrs`: a release
    /// that does not know the family skips the whole record.
    pub extensions: Vec<u8>,
}

impl ZakuraNodeRecordBody {
    /// Encodes this body into canonical bytes for signing and verification.
    pub fn encode_for_signature(&self) -> Result<Vec<u8>, DiscoveryWireError> {
        validate_record_body_bounds(self)?;

        let mut bytes = Vec::new();
        bytes.write_all(ZAKURA_NODE_RECORD_SIG_DOMAIN)?;
        bytes.write_u16::<LittleEndian>(ZAKURA_NODE_RECORD_FORMAT_VERSION)?;
        let body_start = bytes.len();
        write_record_body(self, &mut bytes);
        // Bound the body, not the pre-image, so every body that decodes also verifies.
        let body_len = bytes.len() - body_start;
        if body_len > MAX_NODE_RECORD_BODY_BYTES {
            return Err(DiscoveryWireError::OversizedPayload {
                actual: body_len,
                max: MAX_NODE_RECORD_BODY_BYTES,
            });
        }
        Ok(bytes)
    }
}

/// A self-signed Zakura node record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ZakuraNodeRecord {
    /// Signed record body.
    pub body: ZakuraNodeRecordBody,
    /// Ed25519 signature over [`ZakuraNodeRecordBody::encode_for_signature`].
    pub signature: Signature,
}

impl ZakuraNodeRecord {
    /// Signs `body` with the node secret key matching `body.node_id`.
    pub fn sign(
        body: ZakuraNodeRecordBody,
        secret_key: &NodeSecretKey,
    ) -> Result<Self, DiscoveryWireError> {
        if body.node_id != secret_key.public() {
            return Err(DiscoveryWireError::SigningKeyMismatch);
        }
        let signing_key = SigningKey::from(secret_key.to_bytes());
        let signature = signing_key.sign(&body.encode_for_signature()?);
        Ok(Self { body, signature })
    }

    /// Verifies this record signature and advisory import context.
    ///
    /// The author's checks run first, so an expired record with a bad
    /// signature fails as [`DiscoveryRecordError::InvalidSignature`].
    pub fn verify(
        &self,
        context: &DiscoveryRecordValidationContext,
    ) -> Result<(), DiscoveryRecordError> {
        self.verify_authentic(context)?;
        validate_record_expiry(&self.body, context)
    }

    /// Verifies what only the record's author controls: bounds, signature,
    /// network, chain, and protocol overlap. Expiry depends on the local clock,
    /// so [`validate_record_expiry`] checks it separately.
    pub(super) fn verify_authentic(
        &self,
        context: &DiscoveryRecordValidationContext,
    ) -> Result<(), DiscoveryRecordError> {
        validate_record_body_bounds(&self.body)?;
        let body_bytes = self
            .body
            .encode_for_signature()
            .map_err(DiscoveryRecordError::Wire)?;
        // The TLS handshake verifies with the same strict policy. A key with a
        // torsion component passes the cofactored equation alone, but it can
        // never authenticate a dial, so its record is useless to the book.
        self.body
            .node_id
            .verify_strict(&body_bytes, &self.signature.to_bytes())
            .map_err(|_| DiscoveryRecordError::InvalidSignature)?;
        validate_record_context(&self.body, context)
    }
}

/// Context used when validating a node record for import.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryRecordValidationContext {
    /// Expected local network id.
    pub expected_network_id: ZakuraNetworkId,
    /// Expected local chain id.
    pub expected_chain_id: [u8; 32],
    /// Current Unix timestamp.
    pub current_unix_secs: u64,
    /// Lowest locally supported Zakura protocol version.
    pub supported_protocol_min: u16,
    /// Highest locally supported Zakura protocol version.
    pub supported_protocol_max: u16,
    /// Maximum accepted record TTL beyond `current_unix_secs`.
    pub max_record_ttl: Duration,
    /// Accepted wall-clock skew on expiry edges.
    pub clock_skew_tolerance: Duration,
}

/// Bounded peer sample request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetPeers {
    /// Maximum requested records.
    pub limit: u16,
    /// Optional service filter.
    pub wanted_services: Vec<ZakuraServiceId>,
    /// Node ids the responder should not return, as raw bytes in ascending
    /// order.
    pub exclude_node_ids: Vec<[u8; 32]>,
}

/// First-party live service summary request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetServices {
    /// Requested service ids, or empty for every locally available summary.
    pub wanted_services: Vec<ZakuraServiceId>,
}

impl GetServices {
    /// Whether this query asks for `service`. An empty wanted list asks for
    /// every service.
    pub(crate) fn wants(&self, service: &ZakuraServiceId) -> bool {
        self.wanted_services.is_empty() || self.wanted_services.contains(service)
    }
}

/// First-party live service summary response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Services {
    /// Node id of the responder that authored these live summaries.
    pub node_id: NodeId,
    /// Unix timestamp after which these live summaries are stale.
    pub expires_at_unix_secs: u64,
    /// Length-delimited live summaries for this responder only.
    pub summaries: Vec<ServiceSummaryEnvelope>,
}

/// Length-delimited live service summary envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceSummaryEnvelope {
    /// Service this summary describes.
    pub service_id: ZakuraServiceId,
    /// Versioned summary tag for the payload.
    pub summary_tag: u16,
    /// Bounded encoded summary payload.
    pub summary_bytes: Vec<u8>,
}

impl ServiceSummaryEnvelope {
    /// Build a header-sync summary envelope.
    pub fn header_sync(summary: &HeaderSyncServiceSummary) -> Result<Self, DiscoveryWireError> {
        let mut summary_bytes = Vec::new();
        encode_header_sync_summary(summary, &mut summary_bytes)?;
        Ok(Self {
            service_id: ZakuraServiceId::header_sync(),
            summary_tag: SUMMARY_TAG_HEADER_SYNC,
            summary_bytes,
        })
    }

    /// Build a discovery summary envelope.
    pub fn discovery(summary: &DiscoveryServiceSummary) -> Result<Self, DiscoveryWireError> {
        let mut summary_bytes = Vec::new();
        encode_discovery_summary(summary, &mut summary_bytes)?;
        Ok(Self {
            service_id: ZakuraServiceId::discovery(),
            summary_tag: SUMMARY_TAG_DISCOVERY_V1,
            summary_bytes,
        })
    }

    /// Build a block-sync summary envelope.
    pub fn block_sync(summary: &BlockSyncServiceSummary) -> Result<Self, DiscoveryWireError> {
        let mut summary_bytes = Vec::new();
        encode_block_sync_summary(summary, &mut summary_bytes)?;
        Ok(Self {
            service_id: ZakuraServiceId::block_sync(),
            summary_tag: SUMMARY_TAG_BLOCK_SYNC_V1,
            summary_bytes,
        })
    }

    /// Decode this envelope as a header-sync summary when its tag/service pair matches.
    pub fn decode_header_sync(
        &self,
    ) -> Result<Option<HeaderSyncServiceSummary>, DiscoveryWireError> {
        let is_current_header_summary = self.summary_tag == SUMMARY_TAG_HEADER_SYNC
            && self.service_id == ZakuraServiceId::header_sync();
        let is_retired_header_summary = self.summary_tag == SUMMARY_TAG_HEADER_SYNC_RETIRED
            && self.service_id == ZakuraServiceId::header_sync_retired();

        if !is_current_header_summary && !is_retired_header_summary {
            return Ok(None);
        }

        decode_header_sync_summary(&self.summary_bytes).map(Some)
    }

    /// Decode this envelope as a discovery summary when its tag matches.
    pub fn decode_discovery(&self) -> Result<Option<DiscoveryServiceSummary>, DiscoveryWireError> {
        if self.summary_tag == SUMMARY_TAG_DISCOVERY_V1 {
            decode_discovery_summary(&self.summary_bytes).map(Some)
        } else {
            Ok(None)
        }
    }

    /// Decode this envelope as a block-sync summary when its tag matches.
    pub fn decode_block_sync(&self) -> Result<Option<BlockSyncServiceSummary>, DiscoveryWireError> {
        if self.summary_tag == SUMMARY_TAG_BLOCK_SYNC_V1 {
            decode_block_sync_summary(&self.summary_bytes).map(Some)
        } else {
            Ok(None)
        }
    }
}

/// Header-sync live summary used only as a dial/admission hint.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HeaderSyncServiceSummary {
    /// Best known header height.
    pub best_height: block::Height,
    /// Best known header hash.
    pub best_hash: block::Hash,
    /// Finalized height, when cheaply available.
    pub finalized_height: Option<block::Height>,
    /// Whether this node is currently serving headers.
    pub serving_headers: bool,
    /// Free inbound header-sync slots.
    pub inbound_slots_free: u16,
    /// Maximum inbound header-sync slots.
    pub inbound_slots_max: u16,
    /// Free outbound header-sync slots.
    pub outbound_slots_free: u16,
    /// Maximum outbound header-sync slots.
    pub outbound_slots_max: u16,
}

impl HeaderSyncServiceSummary {
    /// Build a summary from the header-sync reactor's cheap local snapshots.
    pub fn from_snapshot(
        best_height: block::Height,
        best_hash: block::Hash,
        finalized_height: Option<block::Height>,
        serving_headers: bool,
        peers: ServicePeerSnapshot,
    ) -> Self {
        Self {
            best_height,
            best_hash,
            finalized_height,
            serving_headers,
            inbound_slots_free: saturating_u16(peers.inbound_slots_free),
            inbound_slots_max: saturating_u16(
                peers.inbound_peers.saturating_add(peers.inbound_slots_free),
            ),
            outbound_slots_free: saturating_u16(peers.outbound_slots_free),
            outbound_slots_max: saturating_u16(
                peers
                    .outbound_peers
                    .saturating_add(peers.outbound_slots_free),
            ),
        }
    }
}

/// Discovery live summary used only as a dial/admission hint.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryServiceSummary {
    /// Free inbound discovery exchange slots.
    pub peer_exchange_slots_free: u16,
    /// Maximum records this node will return in one peer response.
    pub max_records_per_response: u16,
    /// Whether this node expects to disconnect after one exchange.
    pub expected_disconnect_after_exchange: bool,
}

impl DiscoveryServiceSummary {
    /// Build a summary from the discovery runtime's cheap local snapshots.
    pub fn from_snapshot(
        peers: ServicePeerSnapshot,
        max_records_per_response: usize,
        expected_disconnect_after_exchange: bool,
    ) -> Self {
        Self {
            peer_exchange_slots_free: saturating_u16(peers.inbound_slots_free),
            max_records_per_response: saturating_u16(
                max_records_per_response.min(MAX_DISCOVERY_RECORDS_PER_RESPONSE),
            ),
            expected_disconnect_after_exchange,
        }
    }
}

/// Block-sync live summary used only as a dial/admission hint.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct BlockSyncServiceSummary {
    /// Earliest committed body height this node can serve.
    pub servable_low: block::Height,
    /// Highest committed body height this node can serve.
    pub servable_high: block::Height,
    /// Hash of [`servable_high`](Self::servable_high).
    pub tip_hash: block::Hash,
    /// Free serving slots for inbound `GetBlocks` requests.
    pub free_slots: u16,
    /// Maximum blocks this node will return in one block-sync response.
    pub max_blocks_per_response: u32,
    /// Maximum encoded bytes this node will return in one block-sync response.
    pub max_response_bytes: u32,
}

impl BlockSyncServiceSummary {
    /// Build a summary from the block-sync reactor's cheap local status and peer snapshots.
    pub fn from_status_and_snapshot(status: BlockSyncStatus, peers: ServicePeerSnapshot) -> Self {
        Self {
            servable_low: status.servable_low,
            servable_high: status.servable_high,
            tip_hash: status.tip_hash,
            free_slots: saturating_u16(peers.inbound_slots_free),
            max_blocks_per_response: status.max_blocks_per_response,
            max_response_bytes: status.max_response_bytes,
        }
    }
}

/// Native discovery protocol messages.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DiscoveryMessage {
    /// Sender's signed self-record.
    Hello {
        /// Signed node record.
        record: ZakuraNodeRecord,
    },
    /// Request a bounded peer sample.
    GetPeers(GetPeers),
    /// Bounded signed peer records.
    Peers {
        /// Signed node records.
        records: Vec<ZakuraNodeRecord>,
    },
    /// Request first-party live service summaries.
    GetServices(GetServices),
    /// First-party live service summaries.
    Services(Services),
}

/// A malformed discovery wire message.
#[derive(Error, Debug)]
pub enum DiscoveryWireError {
    /// A payload exceeded its hard cap.
    #[error("Zakura discovery payload length {actual} exceeds hard cap {max}")]
    OversizedPayload {
        /// Actual payload length.
        actual: usize,
        /// Maximum allowed payload length.
        max: usize,
    },

    /// An I/O error while encoding or decoding.
    #[error("Zakura discovery wire I/O error: {0}")]
    Io(#[from] io::Error),

    /// The network id is unknown.
    #[error("invalid Zakura discovery network id {0}")]
    InvalidNetworkId(u32),

    /// A first-party live field described a different node than the authenticated peer.
    #[error("Zakura discovery {field} does not match authenticated peer node id")]
    MismatchedNodeId {
        /// Field name.
        field: &'static str,
    },

    /// A decoded payload had trailing bytes.
    #[error("trailing bytes in Zakura discovery payload")]
    TrailingBytes,

    /// A required field was empty.
    #[error("empty Zakura discovery {0}")]
    Empty(&'static str),

    /// A service id was not ASCII.
    #[error("Zakura discovery service id is not ASCII")]
    NonAsciiServiceId,

    /// A boolean field was not encoded as 0 or 1.
    #[error("invalid Zakura discovery boolean {field} value {value}")]
    InvalidBoolean {
        /// Field name.
        field: &'static str,
        /// Encoded value.
        value: u8,
    },

    /// A block height exceeded Zebra's accepted height range.
    #[error("invalid Zakura discovery block height {0}")]
    InvalidHeight(u32),

    /// A numeric conversion failed while handling bounded data.
    #[error("numeric overflow while encoding Zakura discovery {0}")]
    NumericOverflow(&'static str),

    /// The signing key does not match the record author.
    #[error("Zakura discovery signing key does not match record node id")]
    SigningKeyMismatch,

    /// The record has an invalid protocol range.
    #[error("invalid Zakura discovery protocol range")]
    InvalidProtocolRange,

    /// This node advertises more services than its record carries.
    #[error("this node advertises {actual} distinct services, but a Zakura discovery record carries at most {max}")]
    TooManyAdvertisedServices {
        /// Distinct services this node advertises.
        actual: usize,
        /// Maximum services in a record.
        max: usize,
    },
}

/// A signed record failed validation for import.
#[derive(Error, Debug)]
pub enum DiscoveryRecordError {
    /// The record wire shape or local bounds are invalid.
    #[error(transparent)]
    Wire(#[from] DiscoveryWireError),

    /// The record's public key bytes are malformed.
    #[error("malformed Zakura discovery node id")]
    MalformedNodeId,

    /// The record signature does not verify.
    #[error("invalid Zakura discovery record signature")]
    InvalidSignature,

    /// The record is expired beyond clock skew tolerance.
    #[error("expired Zakura discovery record")]
    Expired,

    /// The record expires too far in the future.
    #[error("far-future Zakura discovery record expiry")]
    FarFutureExpiry,

    /// The record is for another network.
    #[error("wrong Zakura discovery network")]
    WrongNetwork,

    /// The record is for another chain.
    #[error("wrong Zakura discovery chain")]
    WrongChain,

    /// The record's protocol range is invalid or incompatible.
    #[error("incompatible Zakura discovery protocol range")]
    IncompatibleProtocol,
}

/// Default maximum discovery book entries learned from peers.
pub const DEFAULT_MAX_DISCOVERY_BOOK_RECORDS: usize = 10_000;

/// Default maximum third-party discovery book entries attributed to one relay.
///
/// An honest relay discloses at most one window of 32 records per requester per
/// 10-minute epoch, so this holds eight epochs of distinct windows. It stops one
/// relay from filling the book. A relay at its quota adds an entry only in
/// place of its own expired entries. The quota does not bound insert work; the
/// `Peers` reservation, 32 records per `GetPeers`, does.
pub const DEFAULT_MAX_DISCOVERY_RECORDS_PER_SOURCE: usize = 256;

const HIGH_DIAL_FAILURE_COUNT: u32 = 3;

/// Local storage limits for the native Zakura discovery book.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ZakuraDiscoveryBookLimits {
    /// Maximum non-static records learned from peers.
    pub max_records: usize,
    /// Maximum records imported from one peer response.
    pub max_imported_records_per_response: usize,
    /// Maximum direct addresses in an imported record.
    pub max_direct_addrs_per_record: usize,
    /// Maximum services in an imported record.
    pub max_services_per_record: usize,
    /// Maximum encoded record size accepted by the book.
    pub max_encoded_record_bytes: usize,
    /// Maximum third-party records attributed to one relay.
    pub max_records_per_source: usize,
}

impl Default for ZakuraDiscoveryBookLimits {
    fn default() -> Self {
        Self {
            max_records: DEFAULT_MAX_DISCOVERY_BOOK_RECORDS,
            max_imported_records_per_response: MAX_DISCOVERY_RECORDS_PER_RESPONSE,
            max_direct_addrs_per_record: MAX_DIRECT_ADDRS_PER_RECORD,
            max_services_per_record: MAX_SERVICES_PER_RECORD,
            max_encoded_record_bytes: MAX_NODE_RECORD_BYTES,
            max_records_per_source: DEFAULT_MAX_DISCOVERY_RECORDS_PER_SOURCE,
        }
    }
}

/// A stored Zakura discovery record with local dial metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ZakuraDiscoveryEntry {
    record: ZakuraNodeRecord,
    source: Option<NodeId>,
    is_static: bool,
    last_seen: u64,
    last_dial_attempt: Option<u64>,
    last_success: Option<u64>,
    last_short_lived_exchange: Option<u64>,
    last_confirmed: Option<u64>,
    failure_count: u32,
    /// No dial before this Unix time after an ALPN mismatch (zakura-quic DIAL-5).
    alpn_backoff_until: Option<u64>,
}

impl ZakuraDiscoveryEntry {
    /// Returns the latest valid signed record for this node.
    pub fn record(&self) -> &ZakuraNodeRecord {
        &self.record
    }

    /// Returns the peer that supplied this record, if any.
    pub fn source(&self) -> Option<NodeId> {
        self.source
    }

    /// Returns true when this entry came from static/bootstrap configuration.
    pub fn is_static(&self) -> bool {
        self.is_static
    }

    /// Returns when this entry was last seen as a Unix timestamp.
    pub fn last_seen(&self) -> u64 {
        self.last_seen
    }

    /// Returns the last dial attempt as a Unix timestamp.
    pub fn last_dial_attempt(&self) -> Option<u64> {
        self.last_dial_attempt
    }

    /// Returns the last successful dial as a Unix timestamp.
    pub fn last_success(&self) -> Option<u64> {
        self.last_success
    }

    /// Returns the last successful short-lived discovery exchange as a Unix timestamp.
    pub fn last_short_lived_exchange(&self) -> Option<u64> {
        self.last_short_lived_exchange
    }

    /// Returns when this entry's identity was last confirmed as a Unix timestamp.
    ///
    /// Confirmation is the explicit trust transition that makes a stored record eligible
    /// for peer samples: a first-party import from the authenticated record owner, a
    /// trusted static/operator import, or a successful local dial. Third-party gossiped
    /// records stay unconfirmed local dial hints until one of those transitions happens.
    pub fn last_confirmed(&self) -> Option<u64> {
        self.last_confirmed
    }

    /// Returns the consecutive dial failure count.
    pub fn failure_count(&self) -> u32 {
        self.failure_count
    }
}

/// A persistence-layer entry for the discovery book.
///
/// This type intentionally contains no filesystem behavior. Cache readers and
/// writers can serialize these values, then re-import them through the book so
/// every loaded record is re-validated before use.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ZakuraDiscoveryPersistedEntry {
    /// The signed node record.
    pub record: ZakuraNodeRecord,
    /// The peer that supplied the record, if any.
    pub source: Option<NodeId>,
    /// Whether this entry came from static/bootstrap configuration.
    pub is_static: bool,
    /// Last seen Unix timestamp.
    pub last_seen: u64,
    /// Last dial attempt Unix timestamp.
    pub last_dial_attempt: Option<u64>,
    /// Last successful dial Unix timestamp.
    pub last_success: Option<u64>,
    /// Last first-party/dial confirmation Unix timestamp.
    pub last_confirmed: Option<u64>,
    /// Consecutive dial failure count.
    pub failure_count: u32,
}

/// A locally dialable discovery candidate.
///
/// Candidates may come from signed discovery records or from trusted static bootstrap
/// configuration. Only signed records are eligible for peer samples; unsigned static candidates
/// are local dial hints.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ZakuraDiscoveryDialCandidate {
    /// Candidate node id.
    pub node_id: NodeId,
    /// Direct addresses to pass to iroh for dialing.
    pub direct_addrs: Vec<SocketAddr>,
    /// Whether this candidate came from static/operator configuration.
    pub is_static: bool,
}

impl ZakuraDiscoveryDialCandidate {
    /// Converts this candidate into a dial address.
    pub fn node_addr(&self) -> NodeAddr {
        NodeAddr::with_addrs(self.node_id, self.direct_addrs.clone())
    }
}

/// Header-sync candidate selection hints owned by the header-sync reactor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ZakuraHeaderSyncCandidateState {
    /// Lowest header height that would make a new peer useful.
    pub target_height: block::Height,
    /// Peers already admitted by header sync; advisory "full" summaries do not remove them.
    pub admitted_node_ids: Vec<NodeId>,
    /// Peers in local, non-punitive advisory backoff after failing to confirm usefulness.
    pub backed_off_node_ids: Vec<NodeId>,
}

impl Default for ZakuraHeaderSyncCandidateState {
    fn default() -> Self {
        Self {
            target_height: block::Height::MIN,
            admitted_node_ids: Vec::new(),
            backed_off_node_ids: Vec::new(),
        }
    }
}

/// Block-sync candidate selection hints owned by the block-sync reactor.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ZakuraBlockSyncCandidateState {
    /// Header-known body heights currently missing from local state.
    pub missing_block_bodies: Vec<block::Height>,
    /// Peers already admitted by block sync; advisory "full" summaries do not remove them.
    pub admitted_node_ids: Vec<NodeId>,
}

/// The result of importing one signed discovery record.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ImportOutcome {
    /// A new node entry was inserted.
    Added,
    /// A newer signed record replaced the previous signed record.
    Updated,
    /// An equal-sequence record refreshed local metadata only.
    MetadataUpdated,
    /// An older record was valid but ignored.
    IgnoredOlder,
}

/// Summary of importing a bounded batch of discovery records.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct ImportBatchOutcome {
    /// Number of records offered to the book before the per-response cap stopped import.
    pub attempted: usize,
    /// Number of newly added records.
    pub added: usize,
    /// Number of updated records.
    pub updated: usize,
    /// Number of metadata-only updates.
    pub metadata_updated: usize,
    /// Number of older records ignored.
    pub ignored_older: usize,
    /// Number of records rejected by validation.
    pub rejected: usize,
    /// Number of records skipped because the response exceeded the import cap.
    pub dropped_for_limit: usize,
}

impl ImportBatchOutcome {
    fn record_success(&mut self, outcome: ImportOutcome) {
        match outcome {
            ImportOutcome::Added => self.added += 1,
            ImportOutcome::Updated => self.updated += 1,
            ImportOutcome::MetadataUpdated => self.metadata_updated += 1,
            ImportOutcome::IgnoredOlder => self.ignored_older += 1,
        }
    }
}

/// A discovery book import or storage error.
#[derive(Error, Debug)]
pub enum DiscoveryBookError {
    /// The signed record failed validation.
    #[error(transparent)]
    Record(#[from] DiscoveryRecordError),

    /// The record describes the local node and self-record import was not requested.
    #[error("Zakura discovery book rejected local self-record")]
    SelfRecord,

    /// A connected peer's self-record did not match its authenticated identity.
    #[error("Zakura discovery connected self-record author does not match authenticated peer")]
    MismatchedConnectedPeerRecord,

    /// The record has no direct address usable for a future dial attempt.
    #[error("Zakura discovery record has no usable direct address")]
    NoUsableDirectAddress,

    /// The record contains a direct address that must not be used for discovery.
    #[error("Zakura discovery record contains non-dialable direct address {addr}")]
    NonDialableDirectAddress {
        /// The rejected direct address.
        addr: SocketAddr,
    },

    /// The runtime shut down before the record's verification ran.
    #[error("Zakura discovery record verification was cancelled")]
    VerificationCancelled,

    /// A record field exceeded a book-specific storage limit.
    #[error("Zakura discovery record {field} count {actual} exceeds book limit {max}")]
    Limit {
        /// The limited field.
        field: &'static str,
        /// Actual count or size.
        actual: usize,
        /// Maximum accepted count or size.
        max: usize,
    },
}

/// What a failed record import means for the peer that sent it.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum RecordVerdict {
    /// Local policy or the local clock rejected an authentic record.
    Drop,
    /// The peer sent a record no conformant peer sends.
    Disconnect,
}

/// The verdict for a failed `Hello` import.
///
/// The match names every variant, so a new error fails the build until it is
/// classified. Expiry depends on the local clock and TTL, and address and
/// storage policy are local, so those only drop the record.
pub(super) fn hello_verdict(error: &DiscoveryBookError) -> RecordVerdict {
    match error {
        DiscoveryBookError::Record(error) => match error {
            DiscoveryRecordError::Wire(_)
            | DiscoveryRecordError::MalformedNodeId
            | DiscoveryRecordError::InvalidSignature
            | DiscoveryRecordError::WrongNetwork
            | DiscoveryRecordError::WrongChain
            | DiscoveryRecordError::IncompatibleProtocol => RecordVerdict::Disconnect,
            DiscoveryRecordError::Expired | DiscoveryRecordError::FarFutureExpiry => {
                RecordVerdict::Drop
            }
        },
        DiscoveryBookError::MismatchedConnectedPeerRecord => RecordVerdict::Disconnect,
        DiscoveryBookError::SelfRecord
        | DiscoveryBookError::NoUsableDirectAddress
        | DiscoveryBookError::NonDialableDirectAddress { .. }
        | DiscoveryBookError::Limit { .. }
        | DiscoveryBookError::VerificationCancelled => RecordVerdict::Drop,
    }
}

/// The verdict for a relayed record that failed its author checks.
///
/// The match names every variant, so a new error fails the build until it is
/// classified. A relay cannot change a third party's signed bytes, and it can
/// know nodes whose protocol range does not overlap this node's, so that
/// record only drops. A record can expire in transit, so expiry only drops.
fn relay_verdict(error: &DiscoveryRecordError) -> RecordVerdict {
    match error {
        DiscoveryRecordError::IncompatibleProtocol
        | DiscoveryRecordError::Expired
        | DiscoveryRecordError::FarFutureExpiry => RecordVerdict::Drop,
        DiscoveryRecordError::Wire(_)
        | DiscoveryRecordError::MalformedNodeId
        | DiscoveryRecordError::InvalidSignature
        | DiscoveryRecordError::WrongNetwork
        | DiscoveryRecordError::WrongChain => RecordVerdict::Disconnect,
    }
}

/// Runs the author checks of relayed records. Returns the authentic records
/// and the number dropped, or the first error that disconnects the relay.
fn verify_relayed(
    records: Vec<ZakuraNodeRecord>,
    context: &DiscoveryRecordValidationContext,
) -> Result<(Vec<ZakuraNodeRecord>, usize), DiscoveryRecordError> {
    let mut authentic = Vec::with_capacity(records.len());
    let mut dropped = 0;
    for record in records {
        match record.verify_authentic(context) {
            Ok(()) => authentic.push(record),
            Err(error) => match relay_verdict(&error) {
                RecordVerdict::Drop => dropped += 1,
                RecordVerdict::Disconnect => return Err(error),
            },
        }
    }
    Ok((authentic, dropped))
}

/// In-memory storage for signed Zakura discovery records.
#[derive(Clone, Debug)]
pub struct ZakuraDiscoveryBook {
    entries: HashMap<NodeId, ZakuraDiscoveryEntry>,
    static_candidates: HashMap<NodeId, ZakuraStaticDiscoveryCandidate>,
    limits: ZakuraDiscoveryBookLimits,
    local_node_id: Option<NodeId>,
    /// Process-local SipHash key that places each requester's sample window.
    ///
    /// Never leaves this process; a remote peer must not be able to predict the window
    /// another requester receives. `RandomState` is used for its randomly generated
    /// 128-bit key, not for hashing any collection.
    disclosure_key: RandomState,
    /// The shuffled sample candidates of the current disclosure epoch.
    sample_snapshot: Option<SampleSnapshot>,
    /// Changes that can add sample candidates: a new or newly confirmed
    /// entry, or a new record for an entry. A small snapshot compares it to
    /// tell when the book may have new candidates for it.
    generation: u64,
    /// Third-party entries attributed to each relay. A relay with no entries
    /// has no key.
    relayed: HashMap<NodeId, usize>,
}

/// One disclosure epoch's peer-sample candidates, in random order.
///
/// A requester's sample is a window over this list. Each epoch starts a new
/// list. Within an epoch the list only grows, and only while one window
/// covers it, so no requester learns more than one window of ids per epoch.
#[derive(Clone, Debug)]
struct SampleSnapshot {
    epoch: u64,
    node_ids: Vec<NodeId>,
    /// The book's `generation` when this snapshot last took candidates.
    generation: u64,
}

/// Passive runtime limits and timing policy for native Zakura discovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ZakuraDiscoveryConfig {
    /// Lifetime of locally authored self-records.
    pub record_ttl: Duration,
    /// How long before its expiry the self-record is re-signed.
    pub refresh_interval: Duration,
    /// Default peer sample size for future exchange logic.
    pub peer_sample_limit: usize,
    /// Discovery book storage limits.
    pub book_limits: ZakuraDiscoveryBookLimits,
    /// Maximum node ids this node excludes in its own `GetPeers`.
    pub max_excluded_node_ids: usize,
    /// Base backoff for future discovery dials.
    pub dial_backoff_base: Duration,
    /// Maximum backoff for future discovery dials.
    pub dial_backoff_max: Duration,
    /// Maximum concurrent future discovery dials.
    pub max_concurrent_discovery_dials: usize,
    /// Slots reserved below the Zakura connection cap for maintained peers.
    pub discovery_connection_headroom: usize,
    /// Total Zakura connection cap used to derive the discovery soft cap.
    pub max_zakura_connections: usize,
    /// Discovery peer caps and queue limits owned by this runtime.
    pub peer_limits: ServicePeerLimits,
    /// Maximum accepted future TTL on imported records.
    pub max_record_ttl: Duration,
    /// Accepted clock skew around expiry checks.
    pub clock_skew_tolerance: Duration,
}

impl Default for ZakuraDiscoveryConfig {
    fn default() -> Self {
        Self {
            record_ttl: DEFAULT_DISCOVERY_RECORD_TTL,
            refresh_interval: DEFAULT_DISCOVERY_REFRESH_INTERVAL,
            peer_sample_limit: DEFAULT_DISCOVERY_PEER_SAMPLE_LIMIT,
            book_limits: ZakuraDiscoveryBookLimits::default(),
            max_excluded_node_ids: MAX_DISCOVERY_EXCLUDED_NODE_IDS,
            dial_backoff_base: DEFAULT_DISCOVERY_DIAL_BACKOFF_BASE,
            dial_backoff_max: DEFAULT_DISCOVERY_DIAL_BACKOFF_MAX,
            max_concurrent_discovery_dials: DEFAULT_MAX_CONCURRENT_DISCOVERY_DIALS,
            discovery_connection_headroom: DEFAULT_DISCOVERY_CONNECTION_HEADROOM,
            max_zakura_connections: DEFAULT_DISCOVERY_ZAKURA_MAX_CONNECTIONS,
            peer_limits: ServicePeerLimits::default(),
            max_record_ttl: DEFAULT_DISCOVERY_MAX_RECORD_TTL,
            clock_skew_tolerance: DEFAULT_DISCOVERY_CLOCK_SKEW_TOLERANCE,
        }
    }
}

/// Construction inputs for the locally authored Zakura discovery self-record.
#[derive(Clone)]
pub struct ZakuraDiscoveryLocalConfig {
    /// Local node secret key used to sign self-records.
    pub secret_key: NodeSecretKey,
    /// Configured direct listener addresses to advertise if routable.
    pub direct_addrs: Vec<SocketAddr>,
    /// Locally supported native Zakura services.
    pub services: Vec<ZakuraServiceId>,
    /// Lowest supported Zakura protocol version.
    pub zakura_protocol_min: u16,
    /// Highest supported Zakura protocol version.
    pub zakura_protocol_max: u16,
    /// Local Zakura network id.
    pub network_id: ZakuraNetworkId,
    /// Local genesis hash / chain id.
    pub chain_id: [u8; 32],
    /// Last locally authored self-record sequence loaded from durable storage.
    ///
    /// Until cache persistence wires this seed at startup, self-record sequences are derived from
    /// wall-clock nanoseconds. They strictly increase across a same-key restart only while the
    /// system clock is monotonic; a backward clock step can still produce a non-increasing sequence.
    pub last_authored_sequence: Option<u64>,
}

impl fmt::Debug for ZakuraDiscoveryLocalConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ZakuraDiscoveryLocalConfig")
            .field("node_id", &self.secret_key.public())
            .field("direct_addrs", &self.direct_addrs)
            .field("services", &self.services)
            .field("zakura_protocol_min", &self.zakura_protocol_min)
            .field("zakura_protocol_max", &self.zakura_protocol_max)
            .field("network_id", &self.network_id)
            .field("chain_id", &self.chain_id)
            .field("last_authored_sequence", &self.last_authored_sequence)
            .finish()
    }
}

/// Cloneable passive runtime handle for native Zakura discovery state.
#[derive(Clone)]
pub struct ZakuraDiscoveryHandle {
    inner: Arc<Mutex<ZakuraDiscoveryInner>>,
    connected: watch::Receiver<Vec<ZakuraPeerId>>,
    self_record: watch::Receiver<Arc<ZakuraNodeRecord>>,
    self_record_tx: watch::Sender<Arc<ZakuraNodeRecord>>,
    peer_snapshot: watch::Receiver<ServicePeerSnapshot>,
    peer_snapshot_tx: watch::Sender<ServicePeerSnapshot>,
    import_validation: DiscoveryImportValidation,
    /// Node-wide slots for record signature verification. Each session's
    /// reader verifies one message at a time, so a peer holds at most one slot.
    verify_slots: SlotBudget,
}

/// Signature verification slots: a quarter of the cores, and at least one.
fn record_verify_slots() -> SlotBudget {
    let slots = std::thread::available_parallelism()
        .map_or(1, |cores| cores.get() / 4)
        .max(1);
    SlotBudget::new(slots)
        .expect("the slot count is at least one and far below the semaphore maximum")
}

/// Immutable inputs needed to validate and bound a peer record batch without holding the
/// global discovery mutex.
///
/// These mirror the fields [`ZakuraDiscoveryInner::validation_context`] reads, all of which
/// are fixed at construction (local identity/network parameters and import limits never
/// change at runtime). Snapshotting them on the handle lets record imports run CPU-heavy
/// Ed25519 verification outside the lock.
#[derive(Clone, Debug)]
struct DiscoveryImportValidation {
    expected_network_id: ZakuraNetworkId,
    expected_chain_id: [u8; 32],
    supported_protocol_min: u16,
    supported_protocol_max: u16,
    max_record_ttl: Duration,
    clock_skew_tolerance: Duration,
    max_imported_records_per_response: usize,
}

impl DiscoveryImportValidation {
    fn context(&self, now: u64) -> DiscoveryRecordValidationContext {
        DiscoveryRecordValidationContext {
            expected_network_id: self.expected_network_id,
            expected_chain_id: self.expected_chain_id,
            current_unix_secs: now,
            supported_protocol_min: self.supported_protocol_min,
            supported_protocol_max: self.supported_protocol_max,
            max_record_ttl: self.max_record_ttl,
            clock_skew_tolerance: self.clock_skew_tolerance,
        }
    }
}

impl fmt::Debug for ZakuraDiscoveryHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ZakuraDiscoveryHandle")
            .field(
                "self_record_node_id",
                &self.self_record.borrow().body.node_id,
            )
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct ZakuraDiscoveryInner {
    book: ZakuraDiscoveryBook,
    active_services: HashMap<NodeId, ZakuraActiveServiceEntry>,
    admitted_peers: HashMap<ZakuraPeerId, ZakuraDiscoveryAdmittedPeer>,
    last_connected_node_ids: HashSet<NodeId>,
    local: ZakuraLocalDiscoveryState,
    config: ZakuraDiscoveryConfig,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct ZakuraDiscoveryAdmittedPeer {
    conn_id: ZakuraConnId,
    session_id: u64,
    direction: ServicePeerDirection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ZakuraActiveServiceEntry {
    record: Option<ZakuraNodeRecord>,
    services: Vec<ZakuraServiceId>,
    live_summaries: Vec<ZakuraCachedLiveServiceSummary>,
}

impl ZakuraActiveServiceEntry {
    fn from_record(record: ZakuraNodeRecord, services: Vec<ZakuraServiceId>) -> Self {
        Self {
            record: Some(record),
            services,
            live_summaries: Vec::new(),
        }
    }

    fn from_live_summaries(summaries: Vec<ZakuraCachedLiveServiceSummary>) -> Self {
        let mut services = Vec::new();
        for summary in &summaries {
            push_unique_service(&mut services, summary.service_id.clone());
        }
        Self {
            record: None,
            services,
            live_summaries: summaries,
        }
    }

    fn update_record(&mut self, record: ZakuraNodeRecord, services: Vec<ZakuraServiceId>) {
        self.record = Some(record);
        self.services = services;
    }

    fn update_live_summaries(&mut self, summaries: Vec<ZakuraCachedLiveServiceSummary>) {
        self.live_summaries.retain(|cached| {
            !summaries.iter().any(|summary| {
                summary.service_id == cached.service_id && summary.summary_tag == cached.summary_tag
            })
        });
        for summary in summaries {
            self.live_summaries.push(summary);
        }
        if self.record.is_none() {
            self.refresh_live_derived_services();
        }
    }

    fn prune_expired_live_summaries(&mut self, now_unix_secs: u64) {
        self.live_summaries
            .retain(|summary| summary.expires_at_unix_secs > now_unix_secs);
        if self.record.is_none() {
            self.refresh_live_derived_services();
        }
    }

    fn refresh_live_derived_services(&mut self) {
        let mut services = Vec::new();
        for summary in &self.live_summaries {
            push_unique_service(&mut services, summary.service_id.clone());
        }
        self.services = services;
    }

    fn is_empty_live_only(&self) -> bool {
        self.record.is_none() && self.live_summaries.is_empty()
    }
}

/// A decoded first-party live service summary cached while its peer remains connected.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ZakuraCachedLiveServiceSummary {
    /// Service this cached summary describes.
    pub service_id: ZakuraServiceId,
    /// Versioned summary payload tag.
    pub summary_tag: u16,
    /// Decoded payload for known service summary tags.
    pub summary: ZakuraLiveServiceSummary,
    /// Local Unix timestamp when this summary was observed.
    pub observed_at_unix_secs: u64,
    /// Effective local expiry, clamped to the local live-summary TTL.
    pub expires_at_unix_secs: u64,
}

impl ZakuraCachedLiveServiceSummary {
    fn from_envelope(
        envelope: &ServiceSummaryEnvelope,
        observed_at_unix_secs: u64,
        expires_at_unix_secs: u64,
    ) -> Result<Self, DiscoveryWireError> {
        let summary = if let Some(summary) = envelope.decode_header_sync()? {
            ZakuraLiveServiceSummary::HeaderSync(summary)
        } else if let Some(summary) = envelope.decode_discovery()? {
            ZakuraLiveServiceSummary::Discovery(summary)
        } else if let Some(summary) = envelope.decode_block_sync()? {
            ZakuraLiveServiceSummary::BlockSync(summary)
        } else {
            ZakuraLiveServiceSummary::Unknown
        };
        Ok(Self {
            service_id: envelope.service_id.clone(),
            summary_tag: envelope.summary_tag,
            summary,
            observed_at_unix_secs,
            expires_at_unix_secs,
        })
    }
}

/// Decoded payload for a cached first-party live service summary.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ZakuraLiveServiceSummary {
    /// Header-sync dial/admission hint.
    HeaderSync(HeaderSyncServiceSummary),
    /// Discovery exchange hint.
    Discovery(DiscoveryServiceSummary),
    /// Block-sync dial/admission hint.
    BlockSync(BlockSyncServiceSummary),
    /// Unknown or reserved summary tag, kept only as a freshness hint.
    Unknown,
}

#[derive(Clone)]
struct ZakuraLocalDiscoveryState {
    secret_key: NodeSecretKey,
    node_id: NodeId,
    direct_addrs: Vec<SocketAddr>,
    services: Vec<ZakuraServiceId>,
    zakura_protocol_min: u16,
    zakura_protocol_max: u16,
    network_id: ZakuraNetworkId,
    chain_id: [u8; 32],
    next_sequence: u64,
}

impl fmt::Debug for ZakuraLocalDiscoveryState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ZakuraLocalDiscoveryState")
            .field("node_id", &self.node_id)
            .field("direct_addrs", &self.direct_addrs)
            .field("services", &self.services)
            .field("zakura_protocol_min", &self.zakura_protocol_min)
            .field("zakura_protocol_max", &self.zakura_protocol_max)
            .field("network_id", &self.network_id)
            .field("chain_id", &self.chain_id)
            .field("next_sequence", &self.next_sequence)
            .finish()
    }
}

#[allow(dead_code)]
impl ZakuraDiscoveryHandle {
    /// Creates passive discovery state using the current wall-clock sequence.
    pub fn new(
        local_config: ZakuraDiscoveryLocalConfig,
        config: ZakuraDiscoveryConfig,
        connected: watch::Receiver<Vec<ZakuraPeerId>>,
    ) -> Result<Self, DiscoveryWireError> {
        Self::new_at(
            local_config,
            config,
            connected,
            current_unix_secs(),
            current_sequence_tick(),
        )
    }

    fn new_at(
        local_config: ZakuraDiscoveryLocalConfig,
        config: ZakuraDiscoveryConfig,
        connected: watch::Receiver<Vec<ZakuraPeerId>>,
        now_unix_secs: u64,
        wall_clock_sequence: u64,
    ) -> Result<Self, DiscoveryWireError> {
        let mut local = ZakuraLocalDiscoveryState::new(local_config, wall_clock_sequence)?;
        let self_record = Arc::new(local.build_self_record(
            now_unix_secs,
            wall_clock_sequence,
            config.record_ttl,
        )?);
        let (self_record_tx, self_record_rx) = watch::channel(self_record);
        let (peer_snapshot_tx, peer_snapshot_rx) =
            watch::channel(ServicePeerSnapshot::new(0, 0, config.peer_limits));
        let book = ZakuraDiscoveryBook::with_local_node_id(config.book_limits, local.node_id);
        let import_validation = DiscoveryImportValidation {
            expected_network_id: local.network_id,
            expected_chain_id: local.chain_id,
            supported_protocol_min: local.zakura_protocol_min,
            supported_protocol_max: local.zakura_protocol_max,
            max_record_ttl: config.max_record_ttl,
            clock_skew_tolerance: config.clock_skew_tolerance,
            max_imported_records_per_response: config.book_limits.max_imported_records_per_response,
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(ZakuraDiscoveryInner {
                book,
                active_services: HashMap::new(),
                admitted_peers: HashMap::new(),
                last_connected_node_ids: HashSet::new(),
                local,
                config,
            })),
            connected,
            self_record: self_record_rx,
            self_record_tx,
            peer_snapshot: peer_snapshot_rx,
            peer_snapshot_tx,
            import_validation,
            verify_slots: record_verify_slots(),
        })
    }

    /// Returns the current signed local self-record without taking the state lock.
    pub fn current_self_record(&self) -> Arc<ZakuraNodeRecord> {
        self.self_record.borrow().clone()
    }

    /// Returns a locally authored record that remains valid through the next refresh interval.
    ///
    /// A node can keep its native connections longer than the record TTL. Refreshing lazily before
    /// a discovery exchange prevents a long-running node from sending an expired `Hello` to a new
    /// peer after its startup record has aged out.
    pub(crate) async fn current_self_record_for_gossip(
        &self,
    ) -> Result<Arc<ZakuraNodeRecord>, DiscoveryWireError> {
        self.current_self_record_for_gossip_at(current_unix_secs(), current_sequence_tick())
            .await
    }

    async fn current_self_record_for_gossip_at(
        &self,
        now_unix_secs: u64,
        wall_clock_sequence: u64,
    ) -> Result<Arc<ZakuraNodeRecord>, DiscoveryWireError> {
        let mut inner = self.inner.lock().await;
        let current = self.current_self_record();
        if current.body.expires_at_unix_secs
            > now_unix_secs.saturating_add(inner.config.refresh_interval.as_secs())
        {
            return Ok(current);
        }
        let record_ttl = inner.config.record_ttl;
        let record = Arc::new(inner.local.build_self_record(
            now_unix_secs,
            wall_clock_sequence,
            record_ttl,
        )?);
        // Publish before releasing the state lock so concurrent refreshers cannot publish a lower
        // sequence after a newer record.
        self.self_record_tx.send_replace(record.clone());
        Ok(record)
    }

    /// Returns the local node id used in authored self-records.
    pub fn local_node_id(&self) -> NodeId {
        self.self_record.borrow().body.node_id
    }

    /// Returns the configured peer sample limit for active exchanges.
    pub async fn peer_sample_limit(&self) -> usize {
        let inner = self.inner.lock().await;
        inner.config.peer_sample_limit
    }

    /// Holds the discovery state lock until the returned guard drops.
    #[cfg(test)]
    pub(crate) async fn lock_for_test(&self) -> impl Sized + '_ {
        self.inner.lock().await
    }

    /// Return the currently cached discovery peer slot snapshot.
    pub fn peer_snapshot(&self) -> ServicePeerSnapshot {
        *self.peer_snapshot.borrow()
    }

    /// Subscribe to discovery peer slot snapshots.
    pub fn subscribe_peer_snapshot(&self) -> watch::Receiver<ServicePeerSnapshot> {
        self.peer_snapshot.clone()
    }

    /// Build this node's first-party discovery service summary.
    pub async fn local_discovery_summary(&self) -> DiscoveryServiceSummary {
        DiscoveryServiceSummary::from_snapshot(
            self.peer_snapshot(),
            self.peer_sample_limit().await,
            true,
        )
    }

    /// Wrap first-party live summaries in a short-lived service response.
    pub fn local_services_response(&self, summaries: Vec<ServiceSummaryEnvelope>) -> Services {
        Services {
            node_id: self.local_node_id(),
            expires_at_unix_secs: current_unix_secs()
                .saturating_add(DEFAULT_LIVE_SERVICE_SUMMARY_TTL.as_secs()),
            summaries,
        }
    }

    pub(crate) async fn admit_peer_session(
        &self,
        conn_id: ZakuraConnId,
        session_id: u64,
        peer_id: ZakuraPeerId,
        direction: ServicePeerDirection,
    ) -> ServiceAdmissionDecision {
        let mut inner = self.inner.lock().await;
        if let Some(peer) = inner.admitted_peers.get_mut(&peer_id) {
            if (conn_id, session_id) <= (peer.conn_id, peer.session_id) {
                return ServiceAdmissionDecision::RejectNotUseful;
            }

            *peer = ZakuraDiscoveryAdmittedPeer {
                conn_id,
                session_id,
                direction,
            };
            self.publish_peer_snapshot_locked(&inner);
            return ServiceAdmissionDecision::Admit;
        }

        let admitted = inner
            .admitted_peers
            .values()
            .filter(|peer| peer.direction == direction)
            .count();
        let cap = match direction {
            ServicePeerDirection::Inbound => inner.config.peer_limits.max_inbound_peers,
            ServicePeerDirection::Outbound => inner.config.peer_limits.max_outbound_peers,
        };

        if admitted >= cap {
            ServiceAdmissionDecision::RejectFull
        } else {
            inner.admitted_peers.insert(
                peer_id,
                ZakuraDiscoveryAdmittedPeer {
                    conn_id,
                    session_id,
                    direction,
                },
            );
            self.publish_peer_snapshot_locked(&inner);
            ServiceAdmissionDecision::Admit
        }
    }

    /// Remove a locally closed or disconnected discovery service session.
    pub async fn remove_peer(&self, peer_id: &ZakuraPeerId, conn_id: ZakuraConnId) {
        let mut inner = self.inner.lock().await;
        if inner
            .admitted_peers
            .get(peer_id)
            .is_some_and(|peer| peer.conn_id == conn_id)
        {
            inner.admitted_peers.remove(peer_id);
        }
        self.publish_peer_snapshot_locked(&inner);
    }

    /// Remove one exact ordered discovery stream generation.
    pub(crate) async fn remove_session(
        &self,
        peer_id: &ZakuraPeerId,
        conn_id: ZakuraConnId,
        session_id: u64,
    ) {
        let mut inner = self.inner.lock().await;
        if inner
            .admitted_peers
            .get(peer_id)
            .is_some_and(|peer| (peer.conn_id, peer.session_id) == (conn_id, session_id))
        {
            inner.admitted_peers.remove(peer_id);
        }
        self.publish_peer_snapshot_locked(&inner);
    }

    /// Returns whether an ordered discovery stream is the currently admitted generation.
    pub(crate) async fn is_current_session(
        &self,
        peer_id: &ZakuraPeerId,
        conn_id: ZakuraConnId,
        session_id: u64,
    ) -> bool {
        let inner = self.inner.lock().await;
        inner
            .admitted_peers
            .get(peer_id)
            .is_some_and(|peer| (peer.conn_id, peer.session_id) == (conn_id, session_id))
    }

    fn publish_peer_snapshot_locked(&self, inner: &ZakuraDiscoveryInner) {
        let inbound = inner
            .admitted_peers
            .values()
            .filter(|peer| peer.direction == ServicePeerDirection::Inbound)
            .count();
        let outbound = inner
            .admitted_peers
            .values()
            .filter(|peer| peer.direction == ServicePeerDirection::Outbound)
            .count();
        let _ = self.peer_snapshot_tx.send(ServicePeerSnapshot::new(
            inbound,
            outbound,
            inner.config.peer_limits,
        ));
    }

    /// Returns bounded node ids to exclude from a peer-sample request.
    ///
    /// Exclusions include the local node, currently connected peers, and recent known records.
    /// The list holds raw node ids in byte order, as the wire requires.
    pub async fn peer_sample_exclusions(&self) -> Vec<[u8; 32]> {
        let connected = connected_peer_node_ids(&self.connected.borrow());
        let inner = self.inner.lock().await;
        let max = inner
            .config
            .max_excluded_node_ids
            .min(MAX_DISCOVERY_EXCLUDED_NODE_IDS);
        let mut excluded = Vec::with_capacity(max);
        push_excluded_node_id(&mut excluded, inner.local.node_id, max);
        for node_id in connected {
            push_excluded_node_id(&mut excluded, node_id, max);
        }
        for node_id in inner
            .book
            .recent_node_ids(max.saturating_sub(excluded.len()))
        {
            push_excluded_node_id(&mut excluded, node_id, max);
        }
        excluded.sort_unstable();
        excluded
    }

    /// Imports a relayed batch of peer-supplied signed records from `source`.
    ///
    /// Verification of every record finishes before any record is imported:
    ///
    /// 1. One lock hold compares the batch with the stored records. A record identical
    ///    to the one stored for its node was verified when it was stored, so it skips
    ///    verification.
    /// 2. The rest verify off the lock, under one node-wide verification slot.
    /// 3. One lock hold offers the authentic records to the book, up to
    ///    `max_imported_records_per_response`, and applies expiry, staleness,
    ///    address, storage, and per-relay policy. Each of those only drops its
    ///    record: a record can expire in transit, and policy is local.
    ///
    /// The import limit applies after verification, so a forged record past it
    /// still rejects the batch. The caller bounds the batch: a `Peers` holds at
    /// most 32 records.
    ///
    /// Returns the first verification error that disconnects the relay; see
    /// [`relay_verdict`]. Nothing from the batch is imported then.
    pub async fn import_peer_records(
        &self,
        records: impl IntoIterator<Item = ZakuraNodeRecord>,
        source: Option<NodeId>,
    ) -> Result<ImportBatchOutcome, DiscoveryRecordError> {
        let max_records = self.import_validation.max_imported_records_per_response;
        let mut outcome = ImportBatchOutcome::default();
        let batch: Vec<_> = records.into_iter().collect();
        if batch.is_empty() {
            return Ok(outcome);
        }

        let (mut authentic, unverified): (Vec<_>, Vec<_>) = {
            let inner = self.inner.lock().await;
            batch
                .into_iter()
                .partition(|record| inner.holds_record(record))
        };
        if !unverified.is_empty() {
            let unverified_len = unverified.len();
            let Some(verified) = self
                .verify_off_lock(move |context| verify_relayed(unverified, context))
                .await
            else {
                // Shutdown cancelled the task. The relay is not at fault.
                outcome.rejected += unverified_len;
                return Ok(outcome);
            };
            let (fresh, dropped) = verified?;
            outcome.rejected += dropped;
            authentic.extend(fresh);
        }

        let now = current_unix_secs();
        let mut inner = self.inner.lock().await;
        let context = inner.validation_context(now);
        for record in authentic {
            if outcome.attempted >= max_records {
                outcome.dropped_for_limit += 1;
                continue;
            }
            outcome.attempted += 1;
            match inner
                .book
                .import_verified_record(record, source, now, &context)
            {
                Ok(import_outcome) => outcome.record_success(import_outcome),
                Err(_) => outcome.rejected += 1,
            }
        }
        Ok(outcome)
    }

    /// Imports a connected peer's first-party live service summaries.
    ///
    /// Live summaries are accepted only from the authenticated peer they describe, are bounded by
    /// the connected-peer set, and expire independently of durable signed node records.
    pub async fn import_connected_peer_services(
        &self,
        services: Services,
        peer_node_id: NodeId,
    ) -> Result<(), DiscoveryWireError> {
        self.import_connected_peer_services_at(services, peer_node_id, current_unix_secs())
            .await
    }

    async fn import_connected_peer_services_at(
        &self,
        services: Services,
        peer_node_id: NodeId,
        now: u64,
    ) -> Result<(), DiscoveryWireError> {
        if services.node_id != peer_node_id {
            return Err(DiscoveryWireError::MismatchedNodeId {
                field: "services node id",
            });
        }

        let connected_node_ids = connected_peer_node_ids(&self.connected.borrow());
        let mut inner = self.inner.lock().await;
        inner.sync_active_services(&connected_node_ids, now);
        if !connected_node_ids.contains(&peer_node_id) {
            return Ok(());
        }

        let Some(expires_at_unix_secs) = effective_live_summary_expiry(
            services.expires_at_unix_secs,
            now,
            inner.config.clock_skew_tolerance,
        ) else {
            if let Some(entry) = inner.active_services.get_mut(&peer_node_id) {
                entry.live_summaries.clear();
                if entry.record.is_none() {
                    entry.refresh_live_derived_services();
                }
            }
            inner
                .active_services
                .retain(|_, entry| !entry.is_empty_live_only());
            return Ok(());
        };

        let mut live_summaries = Vec::with_capacity(services.summaries.len());
        for envelope in &services.summaries {
            live_summaries.push(ZakuraCachedLiveServiceSummary::from_envelope(
                envelope,
                now,
                expires_at_unix_secs,
            )?);
        }

        inner
            .active_services
            .entry(peer_node_id)
            .and_modify(|entry| entry.update_live_summaries(live_summaries.clone()))
            .or_insert_with(|| ZakuraActiveServiceEntry::from_live_summaries(live_summaries));
        Ok(())
    }

    /// Imports one trusted static signed record through the static validation path.
    pub async fn import_static_record(
        &self,
        record: ZakuraNodeRecord,
    ) -> Result<ImportOutcome, DiscoveryBookError> {
        let now = current_unix_secs();
        let mut inner = self.inner.lock().await;
        let context = inner.validation_context(now);
        inner.book.import_static_record(record, now, &context)
    }

    /// Imports a connected peer's latest self-record and updates active service hints.
    ///
    /// Active service hints come only from the authenticated peer's own signed `Hello`.
    /// Addressless or locally non-dialable records can still update active services, because the
    /// live connection is already established; they are still rejected from dialable storage.
    ///
    /// Signature verification runs outside the discovery lock. A record identical to the
    /// stored one skips it, and policy runs in one short lock hold.
    pub async fn import_connected_peer_record(
        &self,
        record: ZakuraNodeRecord,
        peer_node_id: NodeId,
    ) -> Result<ImportOutcome, DiscoveryBookError> {
        if record.body.node_id != peer_node_id {
            return Err(DiscoveryBookError::MismatchedConnectedPeerRecord);
        }
        let record = self.verify_unless_stored(record).await?;

        let now = current_unix_secs();
        let services = normalize_services(record.body.services.clone());
        let mut inner = self.inner.lock().await;
        let context = inner.validation_context(now);
        if record.body.services.len() > inner.config.book_limits.max_services_per_record {
            return Err(DiscoveryBookError::Limit {
                field: "service",
                actual: record.body.services.len(),
                max: inner.config.book_limits.max_services_per_record,
            });
        }

        let import_result =
            inner
                .book
                .import_verified_record(record.clone(), Some(peer_node_id), now, &context);
        if import_result.is_ok()
            || matches!(import_result, Err(ref error) if is_direct_address_import_error(error))
        {
            inner.update_active_services_from_connected_record(
                peer_node_id,
                record,
                services,
                &import_result,
                now,
            );
        }
        import_result
    }

    /// Verifies `record`'s author checks off the lock, unless the record is
    /// already stored.
    ///
    /// A record identical to the one stored for its node, in the book or in the live
    /// service hints, was verified when it was stored, so it skips verification.
    async fn verify_unless_stored(
        &self,
        record: ZakuraNodeRecord,
    ) -> Result<ZakuraNodeRecord, DiscoveryBookError> {
        if self.inner.lock().await.holds_record(&record) {
            return Ok(record);
        }
        self.verify_off_lock(move |context| record.verify_authentic(context).map(|()| record))
            .await
            .ok_or(DiscoveryBookError::VerificationCancelled)?
            .map_err(DiscoveryBookError::Record)
    }

    /// Runs `verify` in a blocking task under a node-wide verification slot,
    /// with the validation context for now. Returns `None` if shutdown
    /// cancelled the task.
    async fn verify_off_lock<T: Send + 'static>(
        &self,
        verify: impl FnOnce(&DiscoveryRecordValidationContext) -> T + Send + 'static,
    ) -> Option<T> {
        let slot = self.verify_slots.reserve().await;
        let context = self.import_validation.context(current_unix_secs());
        tokio::task::spawn_blocking(move || {
            let _slot = slot;
            verify(&context)
        })
        .await
        .check_for_panics_with(false)
        .ok()
    }

    /// Imports records loaded from persistent storage through the validation path.
    pub async fn import_persisted_entries(
        &self,
        entries: impl IntoIterator<Item = ZakuraDiscoveryPersistedEntry>,
    ) -> ImportBatchOutcome {
        let now = current_unix_secs();
        let mut inner = self.inner.lock().await;
        let context = inner.validation_context(now);
        inner.book.import_persisted_entries(entries, now, &context)
    }

    /// Inserts a trusted static/bootstrap dial candidate.
    pub async fn insert_static_candidate(
        &self,
        node_addr: NodeAddr,
    ) -> Result<(), DiscoveryBookError> {
        let mut inner = self.inner.lock().await;
        inner
            .book
            .insert_static_candidate(node_addr, current_unix_secs())
    }

    /// Returns a bounded sample of owned peer records for one authenticated requester.
    ///
    /// Only confirmed records are disclosed, and results come from a per-requester
    /// per-epoch disclosure window; see [`ZakuraDiscoveryBook::sample_peers`].
    /// `exclude_node_ids` must be in ascending byte order, as the wire decoder
    /// guarantees.
    pub async fn sample_peers(
        &self,
        requester: NodeId,
        limit: usize,
        wanted_services: &[ZakuraServiceId],
        exclude_node_ids: &[[u8; 32]],
    ) -> Vec<ZakuraNodeRecord> {
        let now = current_unix_secs();
        let mut inner = self.inner.lock().await;
        let limit = limit.min(inner.config.peer_sample_limit);
        inner
            .book
            .sample_peers(limit, wanted_services, exclude_node_ids, now, requester)
    }

    /// Returns owned dial candidates, excluding peers currently registered with the supervisor.
    pub async fn dial_candidates(
        &self,
        wanted_services: &[ZakuraServiceId],
        in_flight_node_ids: &[NodeId],
    ) -> Vec<ZakuraDiscoveryDialCandidate> {
        let connected = self.connected.borrow().clone();
        let (limit, dial_backoff_base, dial_backoff_max, book_limits) = {
            let inner = self.inner.lock().await;
            (
                discovery_dial_slot_limit(
                    connected.len(),
                    in_flight_node_ids.len(),
                    inner.config.max_zakura_connections,
                    inner.config.discovery_connection_headroom,
                    inner.config.max_concurrent_discovery_dials,
                ),
                inner.config.dial_backoff_base,
                inner.config.dial_backoff_max,
                inner.config.book_limits,
            )
        };
        if limit == 0 {
            return Vec::new();
        }

        let connected_node_ids = connected_peer_node_ids(&connected);
        let now = current_unix_secs();
        let inner = self.inner.lock().await;
        let wanted_services = bounded_services(wanted_services, book_limits);
        let mut rng = rand_10::rng();
        inner.book.dial_candidates(
            limit,
            &wanted_services,
            &[],
            DialCandidateExclusions {
                connected_node_ids: &connected_node_ids,
                in_flight_node_ids,
            },
            now,
            (dial_backoff_base, dial_backoff_max),
            &mut rng,
        )
    }

    /// Returns service-matching candidates first, followed by general candidates.
    pub(crate) async fn dial_candidates_preferring_any_service(
        &self,
        preferred_services: &[ZakuraServiceId],
        in_flight_node_ids: &[NodeId],
    ) -> Vec<ZakuraDiscoveryDialCandidate> {
        let connected = self.connected.borrow().clone();
        let (limit, dial_backoff_base, dial_backoff_max) = {
            let inner = self.inner.lock().await;
            (
                discovery_dial_slot_limit(
                    connected.len(),
                    in_flight_node_ids.len(),
                    inner.config.max_zakura_connections,
                    inner.config.discovery_connection_headroom,
                    inner.config.max_concurrent_discovery_dials,
                ),
                inner.config.dial_backoff_base,
                inner.config.dial_backoff_max,
            )
        };
        if limit == 0 {
            return Vec::new();
        }

        let connected_node_ids = connected_peer_node_ids(&connected);
        let now = current_unix_secs();
        let inner = self.inner.lock().await;
        let mut rng = rand_10::rng();
        inner.book.dial_candidates(
            limit,
            &[],
            preferred_services,
            DialCandidateExclusions {
                connected_node_ids: &connected_node_ids,
                in_flight_node_ids,
            },
            now,
            (dial_backoff_base, dial_backoff_max),
            &mut rng,
        )
    }

    pub(crate) async fn dial_backoff(&self) -> (Duration, Duration) {
        let inner = self.inner.lock().await;
        (
            inner.config.dial_backoff_base,
            inner.config.dial_backoff_max,
        )
    }

    /// Marks a discovery dial attempt for `node_id`.
    pub async fn mark_dial_attempt(&self, node_id: &NodeId) {
        let mut inner = self.inner.lock().await;
        inner.book.mark_dial_attempt(node_id, current_unix_secs());
    }

    /// Marks a successful discovery dial for `node_id`.
    pub async fn mark_dial_success(&self, node_id: &NodeId) {
        let mut inner = self.inner.lock().await;
        inner.book.mark_dial_success(node_id, current_unix_secs());
    }

    /// Marks a failed discovery dial for `node_id`.
    pub async fn mark_dial_failure(&self, node_id: &NodeId) {
        let mut inner = self.inner.lock().await;
        inner.book.mark_dial_failure(node_id, current_unix_secs());
    }

    /// Marks a dial that failed on an ALPN mismatch (zakura-quic DIAL-5).
    pub async fn mark_alpn_mismatch(&self, node_id: &NodeId) {
        let mut inner = self.inner.lock().await;
        inner.book.mark_alpn_mismatch(node_id, current_unix_secs());
    }

    /// Marks a completed short-lived discovery exchange for local redial backoff.
    pub async fn mark_short_lived_exchange(&self, node_id: &NodeId) {
        let mut inner = self.inner.lock().await;
        inner
            .book
            .mark_short_lived_exchange(node_id, current_unix_secs());
    }

    /// Returns a connected peer's advertised services as a derived supervisor-watch projection.
    pub async fn active_services(&self, node_id: NodeId) -> Option<Vec<ZakuraServiceId>> {
        let connected = connected_peer_node_ids(&self.connected.borrow());
        let now = current_unix_secs();
        let mut inner = self.inner.lock().await;
        inner.sync_active_services(&connected, now);
        if !connected.contains(&node_id) {
            return None;
        }

        inner
            .active_services
            .get(&node_id)
            .map(|entry| entry.services.clone())
    }

    /// Returns fresh first-party live summaries cached for a connected peer.
    pub async fn live_service_summaries(
        &self,
        node_id: NodeId,
    ) -> Option<Vec<ZakuraCachedLiveServiceSummary>> {
        self.live_service_summaries_at(node_id, current_unix_secs())
            .await
    }

    async fn live_service_summaries_at(
        &self,
        node_id: NodeId,
        now: u64,
    ) -> Option<Vec<ZakuraCachedLiveServiceSummary>> {
        let connected = connected_peer_node_ids(&self.connected.borrow());
        let mut inner = self.inner.lock().await;
        inner.sync_active_services(&connected, now);
        if !connected.contains(&node_id) {
            return None;
        }

        inner
            .active_services
            .get(&node_id)
            .map(|entry| entry.live_summaries.clone())
    }

    /// Holds every free verification slot until the returned permits drop.
    #[cfg(test)]
    pub(super) fn hold_verify_slots(&self) -> Vec<crate::zakura::regulation::SlotPermit> {
        self.verify_slots.hold_free()
    }

    /// Returns a stored discovery record for `node_id`, if present.
    pub async fn record_for(&self, node_id: NodeId) -> Option<ZakuraNodeRecord> {
        let inner = self.inner.lock().await;
        inner.book.get(&node_id).map(|entry| entry.record().clone())
    }

    /// Returns owned entries suitable for a future Zakura-specific persistent cache writer.
    pub async fn persisted_entries(&self) -> Vec<ZakuraDiscoveryPersistedEntry> {
        let inner = self.inner.lock().await;
        inner.book.persisted_entries()
    }
}

impl ZakuraDiscoveryInner {
    fn validation_context(&self, now: u64) -> DiscoveryRecordValidationContext {
        DiscoveryRecordValidationContext {
            expected_network_id: self.local.network_id,
            expected_chain_id: self.local.chain_id,
            current_unix_secs: now,
            supported_protocol_min: self.local.zakura_protocol_min,
            supported_protocol_max: self.local.zakura_protocol_max,
            max_record_ttl: self.config.max_record_ttl,
            clock_skew_tolerance: self.config.clock_skew_tolerance,
        }
    }

    fn sync_active_services(&mut self, connected_node_ids: &[NodeId], now_unix_secs: u64) {
        let connected_node_ids: HashSet<_> = connected_node_ids.iter().copied().collect();
        self.active_services
            .retain(|node_id, _| connected_node_ids.contains(node_id));
        for entry in self.active_services.values_mut() {
            entry.prune_expired_live_summaries(now_unix_secs);
            if entry
                .record
                .as_ref()
                .is_some_and(|record| record.body.expires_at_unix_secs < now_unix_secs)
            {
                entry.record = None;
                entry.refresh_live_derived_services();
            }
        }
        self.active_services
            .retain(|_, entry| !entry.is_empty_live_only());
        self.last_connected_node_ids = connected_node_ids;
    }

    /// Whether the book or the live service hints hold exactly `record`.
    fn holds_record(&self, record: &ZakuraNodeRecord) -> bool {
        let node_id = &record.body.node_id;
        self.book
            .get(node_id)
            .is_some_and(|entry| entry.record() == record)
            || self
                .active_services
                .get(node_id)
                .and_then(|active| active.record.as_ref())
                .is_some_and(|active| active == record)
    }

    fn update_active_services_from_connected_record(
        &mut self,
        node_id: NodeId,
        record: ZakuraNodeRecord,
        services: Vec<ZakuraServiceId>,
        import_result: &Result<ImportOutcome, DiscoveryBookError>,
        now: u64,
    ) {
        // An expired hint no longer orders the peer's records.
        if self.active_services.get(&node_id).is_some_and(|active| {
            active.record.as_ref().is_some_and(|active_record| {
                active_record.body.expires_at_unix_secs >= now
                    && record.body.sequence <= active_record.body.sequence
            })
        }) {
            return;
        }

        let should_update = match import_result {
            Ok(ImportOutcome::Added | ImportOutcome::Updated) => true,
            Ok(ImportOutcome::MetadataUpdated) => self
                .book
                .get(&node_id)
                .is_some_and(|entry| entry.record() == &record),
            Ok(ImportOutcome::IgnoredOlder) => false,
            Err(error) if is_direct_address_import_error(error) => {
                self.book.get(&node_id).is_none_or(|entry| {
                    connected_record_is_fresh_for_stored_record(&record, entry, now)
                })
            }
            Err(_) => false,
        };

        if should_update {
            self.active_services
                .entry(node_id)
                .and_modify(|entry| entry.update_record(record.clone(), services.clone()))
                .or_insert_with(|| ZakuraActiveServiceEntry::from_record(record, services));
        }
    }
}

impl ZakuraLocalDiscoveryState {
    fn new(
        config: ZakuraDiscoveryLocalConfig,
        wall_clock_sequence: u64,
    ) -> Result<Self, DiscoveryWireError> {
        let node_id = config.secret_key.public();
        let persisted_next_sequence = config
            .last_authored_sequence
            .map(|sequence| sequence.saturating_add(1))
            .unwrap_or(0);
        Ok(Self {
            secret_key: config.secret_key,
            node_id,
            direct_addrs: normalize_direct_addrs(config.direct_addrs),
            services: advertised_services(config.services)?,
            zakura_protocol_min: config.zakura_protocol_min,
            zakura_protocol_max: config.zakura_protocol_max,
            network_id: config.network_id,
            chain_id: config.chain_id,
            next_sequence: persisted_next_sequence.max(wall_clock_sequence),
        })
    }

    fn build_self_record(
        &mut self,
        now_unix_secs: u64,
        wall_clock_sequence: u64,
        record_ttl: Duration,
    ) -> Result<ZakuraNodeRecord, DiscoveryWireError> {
        let sequence = self.next_sequence.max(wall_clock_sequence);
        self.next_sequence = sequence.saturating_add(1);
        ZakuraNodeRecord::sign(
            ZakuraNodeRecordBody {
                node_id: self.node_id,
                direct_addrs: self.direct_addrs.clone(),
                services: self.services.clone(),
                zakura_protocol_min: self.zakura_protocol_min,
                zakura_protocol_max: self.zakura_protocol_max,
                network_id: self.network_id,
                chain_id: self.chain_id,
                sequence,
                expires_at_unix_secs: now_unix_secs.saturating_add(record_ttl.as_secs()),
                extensions: Vec::new(),
            },
            &self.secret_key,
        )
    }
}

impl Default for ZakuraDiscoveryBook {
    fn default() -> Self {
        Self::new(ZakuraDiscoveryBookLimits::default())
    }
}

impl ZakuraDiscoveryBook {
    /// Creates an empty discovery book with `limits`.
    pub fn new(limits: ZakuraDiscoveryBookLimits) -> Self {
        Self {
            entries: HashMap::new(),
            static_candidates: HashMap::new(),
            limits,
            local_node_id: None,
            disclosure_key: RandomState::new(),
            sample_snapshot: None,
            generation: 0,
            relayed: HashMap::new(),
        }
    }

    /// Creates an empty discovery book that rejects the local node's record.
    pub fn with_local_node_id(limits: ZakuraDiscoveryBookLimits, local_node_id: NodeId) -> Self {
        Self {
            local_node_id: Some(local_node_id),
            ..Self::new(limits)
        }
    }

    /// Returns the storage limits.
    pub fn limits(&self) -> ZakuraDiscoveryBookLimits {
        self.limits
    }

    /// Returns the total number of stored records, including static records.
    pub fn len(&self) -> usize {
        self.entries.len()
            + self
                .static_candidates
                .keys()
                .filter(|node_id| !self.entries.contains_key(*node_id))
                .count()
    }

    /// Returns true when the book has no records.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the number of non-static records learned from peers.
    pub fn discovered_len(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| !entry.is_static)
            .count()
    }

    /// Returns the entry for `node_id`, if present.
    pub fn get(&self, node_id: &NodeId) -> Option<&ZakuraDiscoveryEntry> {
        self.entries.get(node_id)
    }

    /// Inserts or refreshes one trusted static/bootstrap dial candidate.
    ///
    /// Configured bootstrap peers are not signed discovery records, so they are stored separately
    /// from peer-supplied records and are never returned in peer samples.
    pub fn insert_static_candidate(
        &mut self,
        node_addr: NodeAddr,
        now: u64,
    ) -> Result<(), DiscoveryBookError> {
        if self.local_node_id == Some(node_addr.id) {
            return Err(DiscoveryBookError::SelfRecord);
        }

        let mut direct_addrs = node_addr.direct;
        direct_addrs.sort_unstable();
        direct_addrs.dedup();
        if direct_addrs.is_empty() {
            return Err(DiscoveryBookError::NoUsableDirectAddress);
        }
        for addr in &direct_addrs {
            if !is_static_discovery_configured_addr_usable(addr) {
                return Err(DiscoveryBookError::NonDialableDirectAddress { addr: *addr });
            }
        }

        let candidate = self
            .static_candidates
            .entry(node_addr.id)
            .or_insert_with(|| ZakuraStaticDiscoveryCandidate {
                node_id: node_addr.id,
                direct_addrs: Vec::new(),
                last_seen: now,
                last_dial_attempt: None,
                last_success: None,
                last_short_lived_exchange: None,
                failure_count: 0,
                alpn_backoff_until: None,
            });
        candidate.direct_addrs.extend(direct_addrs);
        candidate.direct_addrs.sort_unstable();
        candidate.direct_addrs.dedup();
        candidate.last_seen = now;
        Ok(())
    }

    /// Imports one signed peer record from a discovery source.
    pub fn import_record(
        &mut self,
        record: ZakuraNodeRecord,
        source: Option<NodeId>,
        now: u64,
        context: &DiscoveryRecordValidationContext,
    ) -> Result<ImportOutcome, DiscoveryBookError> {
        self.import_record_inner(record, source, false, false, now, context)
    }

    /// Imports one peer record whose author checks already passed.
    ///
    /// Expiry, storage limits, address policy, and sequence order still run
    /// here.
    fn import_verified_record(
        &mut self,
        record: ZakuraNodeRecord,
        source: Option<NodeId>,
        now: u64,
        context: &DiscoveryRecordValidationContext,
    ) -> Result<ImportOutcome, DiscoveryBookError> {
        self.import_record_inner(record, source, false, true, now, context)
    }

    /// Imports one signed static/bootstrap record.
    ///
    /// Static records are trusted-by-operator configuration, so this import path permits loopback
    /// and other local direct addresses needed by single-host regtest deployments. Untrusted
    /// peer/gossip imports through [`Self::import_record`] keep requiring globally dialable
    /// addresses.
    pub fn import_static_record(
        &mut self,
        record: ZakuraNodeRecord,
        now: u64,
        context: &DiscoveryRecordValidationContext,
    ) -> Result<ImportOutcome, DiscoveryBookError> {
        self.import_record_inner(record, None, true, false, now, context)
    }

    /// Returns a bounded peer sample from the requester's disclosure window.
    ///
    /// Only confirmed entries (see [`ZakuraDiscoveryEntry::last_confirmed`]) are eligible:
    /// a third-party gossiped record is a local dial hint, not something this node vouches
    /// for to other peers.
    ///
    /// Each disclosure epoch has one shuffled snapshot of the eligible node ids. A
    /// requester's window is the `max_imported_records_per_response` ids, at most 32, that
    /// start at an offset keyed by the book's process-local `disclosure_key` over
    /// `(requester, epoch)`.
    /// The requester-controlled service filter and exclusion list apply only inside that
    /// window, so repeated queries from one requester within an epoch reveal one stable
    /// bounded subset of the book, no matter how the requester varies its limits, filters,
    /// or exclusions. The limit is a ceiling: a window with too few matches returns a short
    /// sample, and the sample never probes past its window.
    ///
    /// The bound is per identity: a sybil holding many authenticated node ids still sees one
    /// window per identity per epoch, so total disclosure is throttled by connection slots.
    ///
    /// A request costs one keyed hash, at most one map lookup per window slot, and at most
    /// `limit` record clones, whatever the book size. Collecting the snapshot scans the book
    /// once per epoch. While the snapshot is shorter than a window, every window covers
    /// all of it, so the book's new candidates join it until it fills one window: a new or
    /// newly confirmed entry, or a new record for an entry. A new node therefore discloses
    /// its first peers without waiting for the next epoch. The snapshot never drops an id
    /// within an epoch, so book churn cannot widen what a requester learns.
    ///
    /// `exclude_node_ids` must be in ascending byte order.
    pub fn sample_peers(
        &mut self,
        limit: usize,
        wanted_services: &[ZakuraServiceId],
        exclude_node_ids: &[[u8; 32]],
        now: u64,
        requester: NodeId,
    ) -> Vec<ZakuraNodeRecord> {
        debug_assert!(
            exclude_node_ids.is_sorted(),
            "the decoder admits only sorted exclusions"
        );
        let window = self.sample_window();
        let limit = limit.min(window);
        self.refresh_sample_snapshot(now);
        let snapshot = self
            .sample_snapshot
            .as_ref()
            .expect("refreshing always leaves a snapshot");
        let len = snapshot.node_ids.len();
        if len == 0 || limit == 0 {
            return Vec::new();
        }
        // The remainder is below `len`, so it fits usize.
        let offset = (self.disclosure_hash(requester, snapshot.epoch) % len as u64) as usize;

        let mut records: Vec<ZakuraNodeRecord> = snapshot
            .node_ids
            .iter()
            .cycle()
            .skip(offset)
            .take(window.min(len))
            .filter(|node_id| exclude_node_ids.binary_search(node_id.as_bytes()).is_err())
            .filter_map(|node_id| self.entries.get(node_id))
            .filter(|entry| {
                self.is_sample_eligible(entry, now)
                    && advertises_all_services(&entry.record, wanted_services)
            })
            .take(limit)
            .map(|entry| entry.record.clone())
            .collect();
        // The order would reveal each record's place in the snapshot.
        records.shuffle(&mut rand_10::rng());
        records
    }

    /// A requester's window: the import limit, and never more than one
    /// response's records, so a larger configured limit cannot widen what a
    /// requester learns in an epoch.
    fn sample_window(&self) -> usize {
        self.limits
            .max_imported_records_per_response
            .min(MAX_DISCOVERY_RECORDS_PER_RESPONSE)
    }

    /// Whether `entry` may appear in a peer sample now.
    fn is_sample_eligible(&self, entry: &ZakuraDiscoveryEntry, now: u64) -> bool {
        self.local_node_id != Some(entry.record.body.node_id)
            && !entry_is_expired(entry, now)
            && entry.last_confirmed.is_some()
            && has_discovery_dialable_direct_addrs(&entry.record)
    }

    /// Starts a new snapshot for a new epoch, and adds the book's new
    /// candidates to a snapshot shorter than one window.
    ///
    /// See [`Self::sample_peers`] for why.
    fn refresh_sample_snapshot(&mut self, now: u64) {
        let epoch = now / DISCOVERY_DISCLOSURE_EPOCH.as_secs();
        let window = self.sample_window();
        let snapshot = match self.sample_snapshot.take() {
            Some(mut snapshot) if snapshot.epoch == epoch => {
                let room = window.saturating_sub(snapshot.node_ids.len());
                if room > 0 && snapshot.generation != self.generation {
                    let added = self.sample_candidates(now, &snapshot.node_ids);
                    snapshot.node_ids.extend(added.into_iter().take(room));
                    snapshot.generation = self.generation;
                }
                snapshot
            }
            _ => SampleSnapshot {
                epoch,
                node_ids: self.sample_candidates(now, &[]),
                generation: self.generation,
            },
        };
        self.sample_snapshot = Some(snapshot);
    }

    /// The sample-eligible ids not in `known`, in random order.
    fn sample_candidates(&self, now: u64, known: &[NodeId]) -> Vec<NodeId> {
        let mut node_ids: Vec<NodeId> = self
            .entries
            .iter()
            .filter(|(node_id, entry)| {
                self.is_sample_eligible(entry, now) && !known.contains(node_id)
            })
            .map(|(node_id, _)| *node_id)
            .collect();
        node_ids.shuffle(&mut rand_10::rng());
        node_ids
    }

    /// Keys a requester's sample window for one epoch.
    ///
    /// The offset only has to be unpredictable to remote peers. `disclosure_key` supplies
    /// SipHash-1-3 with a random 128-bit key, so the secret keys the hash rather than
    /// sitting in the hashed message; a requester that collects its own windows across many
    /// epochs still cannot solve for the key and predict another requester's window.
    fn disclosure_hash(&self, requester: NodeId, epoch: u64) -> u64 {
        use std::hash::{BuildHasher, Hasher};

        let mut hasher = self.disclosure_key.build_hasher();
        hasher.write(requester.as_bytes());
        hasher.write_u64(epoch);
        hasher.finish()
    }

    /// Returns bounded dial candidates for later dial-loop code.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn dial_candidates<R: rand_10::Rng + ?Sized>(
        &self,
        limit: usize,
        required_services: &[ZakuraServiceId],
        preferred_services: &[ZakuraServiceId],
        exclusions: DialCandidateExclusions<'_>,
        now: u64,
        dial_backoff: (Duration, Duration),
        rng: &mut R,
    ) -> Vec<ZakuraDiscoveryDialCandidate> {
        let connected_node_ids: HashSet<_> =
            exclusions.connected_node_ids.iter().copied().collect();
        let in_flight_node_ids: HashSet<_> =
            exclusions.in_flight_node_ids.iter().copied().collect();
        let candidates: Vec<_> = self
            .entries
            .values()
            .filter(|entry| {
                !connected_node_ids.contains(&entry.record.body.node_id)
                    && !in_flight_node_ids.contains(&entry.record.body.node_id)
                    && self.local_node_id != Some(entry.record.body.node_id)
                    && !entry_is_expired(entry, now)
                    && !entry_in_dial_backoff(entry, now, dial_backoff.0, dial_backoff.1)
                    && !in_alpn_backoff(entry.alpn_backoff_until, now)
                    && !entry_in_short_lived_exchange_backoff(
                        entry,
                        now,
                        dial_backoff.0,
                        dial_backoff.1,
                    )
                    && advertises_all_services(&entry.record, required_services)
                    && has_discovery_usable_direct_addrs(entry)
            })
            .map(DialCandidateRef::SignedRecord)
            .chain(self.static_candidates.values().filter_map(|candidate| {
                if !required_services.is_empty()
                    || connected_node_ids.contains(&candidate.node_id)
                    || in_flight_node_ids.contains(&candidate.node_id)
                    || self.local_node_id == Some(candidate.node_id)
                    || self.entries.contains_key(&candidate.node_id)
                    || in_alpn_backoff(candidate.alpn_backoff_until, now)
                    || entry_metadata_in_dial_backoff(
                        candidate.last_dial_attempt,
                        candidate.failure_count,
                        now,
                        dial_backoff.0,
                        dial_backoff.1,
                    )
                    || entry_metadata_in_short_lived_exchange_backoff(
                        candidate.last_short_lived_exchange,
                        now,
                        dial_backoff.0,
                        dial_backoff.1,
                    )
                {
                    return None;
                }
                Some(DialCandidateRef::StaticConfigured(candidate))
            }))
            .collect();

        // Bounded top-k selection instead of a full sort. The book can hold `max_records`
        // (default 10_000) entries and this selection runs under the global discovery mutex on the
        // per-second candidate-dialer path, yet only `limit` candidates are ever returned. Sorting
        // the whole candidate set (O(n log n)) just to `take(limit)` is replaced with an O(n)
        // partial select of the best `limit` candidates plus an O(limit log limit) sort of the
        // survivors, keeping the order identical within each preference group. See finding
        // `claude-discovery-expensive-work-under-global-mutex` (SR-2/SR-4).
        let mut keyed: Vec<((bool, DialCandidateSortKey), DialCandidateRef<'_>)> = candidates
            .into_iter()
            .map(|candidate| {
                let is_general = match &candidate {
                    DialCandidateRef::SignedRecord(entry) => {
                        !advertises_any_service(&entry.record, preferred_services)
                    }
                    DialCandidateRef::StaticConfigured(_) => true,
                };
                (
                    (is_general, dial_candidate_sort_key(&candidate, rng)),
                    candidate,
                )
            })
            .collect();

        let take = limit.min(keyed.len());
        if take < keyed.len() {
            keyed.select_nth_unstable_by(take, |a, b| a.0.cmp(&b.0));
            keyed.truncate(take);
        }
        keyed.sort_by_key(|a| a.0);

        keyed
            .into_iter()
            .map(|(_, candidate)| candidate.into_candidate())
            .collect()
    }

    /// Marks a dial attempt for `node_id`.
    pub fn mark_dial_attempt(&mut self, node_id: &NodeId, now: u64) {
        if let Some(entry) = self.entries.get_mut(node_id) {
            entry.last_dial_attempt = Some(now);
        }
        if let Some(candidate) = self.static_candidates.get_mut(node_id) {
            candidate.last_dial_attempt = Some(now);
        }
    }

    /// Marks a successful dial for `node_id`, confirming its record for peer samples.
    pub fn mark_dial_success(&mut self, node_id: &NodeId, now: u64) {
        if let Some(entry) = self.entries.get_mut(node_id) {
            entry.last_success = Some(now);
            if entry.last_confirmed.replace(now).is_none() {
                self.generation += 1;
            }
            entry.failure_count = 0;
        }
        if let Some(candidate) = self.static_candidates.get_mut(node_id) {
            candidate.last_success = Some(now);
            candidate.failure_count = 0;
        }
    }

    /// Marks a failed dial for `node_id` without blacklisting it.
    pub fn mark_dial_failure(&mut self, node_id: &NodeId, _now: u64) {
        if let Some(entry) = self.entries.get_mut(node_id) {
            entry.failure_count = entry.failure_count.saturating_add(1);
        }
        if let Some(candidate) = self.static_candidates.get_mut(node_id) {
            candidate.failure_count = candidate.failure_count.saturating_add(1);
        }
    }

    /// Marks a dial that failed on an ALPN mismatch. The node isn't dialed again
    /// for [`ZAKURA_ALPN_MISMATCH_BACKOFF`], whatever addresses it advertises
    /// next (zakura-quic DIAL-5).
    pub fn mark_alpn_mismatch(&mut self, node_id: &NodeId, now: u64) {
        let until = now.saturating_add(ZAKURA_ALPN_MISMATCH_BACKOFF.as_secs());
        self.mark_dial_failure(node_id, now);
        if let Some(entry) = self.entries.get_mut(node_id) {
            entry.alpn_backoff_until = Some(until);
        }
        if let Some(candidate) = self.static_candidates.get_mut(node_id) {
            candidate.alpn_backoff_until = Some(until);
        }
    }

    /// Marks a successful short-lived discovery exchange for `node_id`.
    pub fn mark_short_lived_exchange(&mut self, node_id: &NodeId, now: u64) {
        if let Some(entry) = self.entries.get_mut(node_id) {
            entry.last_short_lived_exchange = Some(now);
        }
        if let Some(candidate) = self.static_candidates.get_mut(node_id) {
            candidate.last_short_lived_exchange = Some(now);
        }
    }

    fn recent_node_ids(&self, limit: usize) -> Vec<NodeId> {
        let mut entries: Vec<_> = self.entries.values().collect();
        entries.sort_by_key(|entry| {
            (
                Reverse(entry.last_seen),
                node_id_sort_key(&entry.record.body.node_id),
            )
        });
        entries
            .into_iter()
            .take(limit)
            .map(|entry| entry.record.body.node_id)
            .collect()
    }

    /// Returns entries suitable for a Zakura-specific persistent cache.
    pub fn persisted_entries(&self) -> Vec<ZakuraDiscoveryPersistedEntry> {
        let mut entries: Vec<_> = self
            .entries
            .values()
            .map(|entry| ZakuraDiscoveryPersistedEntry {
                record: entry.record.clone(),
                source: entry.source,
                is_static: entry.is_static,
                last_seen: entry.last_seen,
                last_dial_attempt: entry.last_dial_attempt,
                last_success: entry.last_success,
                last_confirmed: entry.last_confirmed,
                failure_count: entry.failure_count,
            })
            .collect();
        entries.sort_by_key(|entry| node_id_sort_key(&entry.record.body.node_id));
        entries
    }

    /// Re-validates and imports entries loaded from a persistent cache.
    pub fn import_persisted_entries(
        &mut self,
        entries: impl IntoIterator<Item = ZakuraDiscoveryPersistedEntry>,
        now: u64,
        context: &DiscoveryRecordValidationContext,
    ) -> ImportBatchOutcome {
        let mut outcome = ImportBatchOutcome::default();
        for entry in entries {
            outcome.attempted += 1;
            match self.import_persisted_entry(entry, now, context) {
                Ok(import_outcome) => outcome.record_success(import_outcome),
                Err(_) => outcome.rejected += 1,
            }
        }
        outcome
    }

    fn import_persisted_entry(
        &mut self,
        entry: ZakuraDiscoveryPersistedEntry,
        now: u64,
        context: &DiscoveryRecordValidationContext,
    ) -> Result<ImportOutcome, DiscoveryBookError> {
        let metadata = DiscoveryEntryMetadata {
            source: entry.source,
            is_static: entry.is_static,
            last_seen: entry.last_seen,
            last_dial_attempt: entry.last_dial_attempt,
            last_success: entry.last_success,
            last_confirmed: entry.last_confirmed,
            failure_count: entry.failure_count,
        };
        self.import_validated_record(entry.record, metadata, false, now, context)
    }

    fn import_record_inner(
        &mut self,
        record: ZakuraNodeRecord,
        source: Option<NodeId>,
        is_static: bool,
        pre_verified: bool,
        now: u64,
        context: &DiscoveryRecordValidationContext,
    ) -> Result<ImportOutcome, DiscoveryBookError> {
        // First-party imports arrive from the authenticated record owner itself (its `Hello`,
        // or its own self-record inside its `Peers` response). Those and trusted static
        // configuration confirm the identity; a third-party gossiped record does not.
        let is_first_party = source == Some(record.body.node_id);
        let metadata = DiscoveryEntryMetadata {
            source,
            is_static,
            last_seen: now,
            last_dial_attempt: None,
            last_success: None,
            last_confirmed: (is_static || is_first_party).then_some(now),
            failure_count: 0,
        };
        self.import_validated_record(record, metadata, pre_verified, now, context)
    }

    /// Validates and stores one record.
    ///
    /// When `pre_verified` is set, the caller already ran the author checks outside the
    /// global discovery mutex, so only expiry runs here. The storage-limit,
    /// address-policy, insertion, and eviction steps always run under the caller's lock.
    fn import_validated_record(
        &mut self,
        record: ZakuraNodeRecord,
        metadata: DiscoveryEntryMetadata,
        pre_verified: bool,
        now: u64,
        context: &DiscoveryRecordValidationContext,
    ) -> Result<ImportOutcome, DiscoveryBookError> {
        if self.local_node_id == Some(record.body.node_id) {
            return Err(DiscoveryBookError::SelfRecord);
        }

        if pre_verified {
            validate_record_expiry(&record.body, context)?;
        } else {
            record.verify(context)?;
        }
        self.validate_record_storage_limits(&record)?;
        let direct_addr_policy = if metadata.is_static {
            DiscoveryDirectAddrPolicy::StaticConfigured
        } else {
            DiscoveryDirectAddrPolicy::UntrustedPeer
        };
        validate_discovery_direct_addrs(&record, direct_addr_policy)?;

        let node_id = record.body.node_id;
        // A relay may hold only so many third-party entries.
        let full_relay = relay_of(metadata.source, node_id).filter(|relay| {
            self.relayed.get(relay).copied().unwrap_or(0) >= self.limits.max_records_per_source
        });
        if let Some(entry) = self.entries.get_mut(&node_id) {
            // An update keeps the book's size, so it is never refused. A full
            // relay leaves the entry's source alone, so two relays cannot pass
            // entries back and forth to add without bound.
            let mut metadata = metadata;
            if full_relay.is_some() {
                metadata.source = entry.source;
            }
            let old_relay = relay_of(entry.source, node_id);
            let was_confirmed = entry.last_confirmed.is_some();
            let outcome = update_existing_entry(entry, record, metadata, now);
            if outcome == ImportOutcome::Updated
                || (!was_confirmed && entry.last_confirmed.is_some())
            {
                self.generation += 1;
            }
            let new_relay = relay_of(entry.source, node_id);
            if new_relay != old_relay {
                self.unattribute(old_relay);
                self.attribute(new_relay);
            }
            return Ok(outcome);
        }
        if let Some(relay) = full_relay {
            if !self.remove_expired_relayed(relay, now) {
                return Err(DiscoveryBookError::Limit {
                    field: "records per relay",
                    actual: self.limits.max_records_per_source + 1,
                    max: self.limits.max_records_per_source,
                });
            }
        }
        if metadata.last_confirmed.is_some() {
            self.generation += 1;
        }
        self.attribute(relay_of(metadata.source, node_id));

        self.entries.insert(
            node_id,
            ZakuraDiscoveryEntry {
                record,
                source: metadata.source,
                is_static: metadata.is_static,
                last_seen: metadata.last_seen,
                last_dial_attempt: metadata.last_dial_attempt,
                last_success: metadata.last_success,
                last_short_lived_exchange: None,
                last_confirmed: metadata.last_confirmed,
                failure_count: metadata.failure_count,
                alpn_backoff_until: None,
            },
        );
        self.evict_to_limits(now);
        if !self.entries.contains_key(&node_id) {
            // Every other entry outranks the new one, so it was evicted.
            return Err(DiscoveryBookError::Limit {
                field: "records",
                actual: self.limits.max_records + 1,
                max: self.limits.max_records,
            });
        }
        Ok(ImportOutcome::Added)
    }

    fn validate_record_storage_limits(
        &self,
        record: &ZakuraNodeRecord,
    ) -> Result<(), DiscoveryBookError> {
        if record.body.direct_addrs.len() > self.limits.max_direct_addrs_per_record {
            return Err(DiscoveryBookError::Limit {
                field: "direct address",
                actual: record.body.direct_addrs.len(),
                max: self.limits.max_direct_addrs_per_record,
            });
        }
        if record.body.services.len() > self.limits.max_services_per_record {
            return Err(DiscoveryBookError::Limit {
                field: "service",
                actual: record.body.services.len(),
                max: self.limits.max_services_per_record,
            });
        }

        let encoded_len = record_len(record);
        if encoded_len > self.limits.max_encoded_record_bytes {
            return Err(DiscoveryBookError::Limit {
                field: "encoded record byte",
                actual: encoded_len,
                max: self.limits.max_encoded_record_bytes,
            });
        }

        Ok(())
    }

    fn evict_to_limits(&mut self, now: u64) {
        while self.discovered_len() > self.limits.max_records {
            let Some(node_id) = self.next_eviction_candidate(now) else {
                break;
            };
            self.remove_entry(&node_id);
        }
    }

    /// Removes `node_id`'s entry and its attribution.
    fn remove_entry(&mut self, node_id: &NodeId) {
        if let Some(entry) = self.entries.remove(node_id) {
            self.unattribute(relay_of(entry.source, *node_id));
        }
    }

    /// Removes `relay`'s expired third-party entries. Returns whether any
    /// were removed.
    ///
    /// It scans the book, but only for a relay at its quota.
    fn remove_expired_relayed(&mut self, relay: NodeId, now: u64) -> bool {
        let expired: Vec<NodeId> = self
            .entries
            .iter()
            .filter(|(node_id, entry)| {
                relay_of(entry.source, **node_id) == Some(relay) && entry_is_expired(entry, now)
            })
            .map(|(node_id, _)| *node_id)
            .collect();
        for node_id in &expired {
            self.remove_entry(node_id);
        }
        !expired.is_empty()
    }

    /// Counts one more entry for `relay`.
    fn attribute(&mut self, relay: Option<NodeId>) {
        if let Some(relay) = relay {
            *self.relayed.entry(relay).or_default() += 1;
        }
    }

    /// Counts one fewer entry for `relay`.
    fn unattribute(&mut self, relay: Option<NodeId>) {
        let Some(relay) = relay else {
            return;
        };
        if let std::collections::hash_map::Entry::Occupied(mut count) = self.relayed.entry(relay) {
            *count.get_mut() -= 1;
            if *count.get() == 0 {
                count.remove();
            }
        }
    }

    fn next_eviction_candidate(&self, now: u64) -> Option<NodeId> {
        self.entries
            .iter()
            .filter(|(_, entry)| !entry.is_static && entry_is_expired(entry, now))
            .min_by_key(|(node_id, entry)| {
                (
                    entry.record.body.expires_at_unix_secs,
                    node_id_sort_key(node_id),
                )
            })
            .map(|(node_id, _)| *node_id)
            .or_else(|| {
                self.entries
                    .iter()
                    .filter(|(_, entry)| {
                        !entry.is_static
                            && entry.last_success.is_none()
                            && entry.failure_count >= HIGH_DIAL_FAILURE_COUNT
                    })
                    .max_by_key(|(node_id, entry)| {
                        (
                            entry.failure_count,
                            Reverse(entry.last_seen),
                            Reverse(node_id_sort_key(node_id)),
                        )
                    })
                    .map(|(node_id, _)| *node_id)
            })
            .or_else(|| {
                // Unconfirmed relayed records go first, so relays cannot
                // evict the confirmed entries that peer samples disclose.
                self.entries
                    .iter()
                    .filter(|(_, entry)| !entry.is_static)
                    .min_by_key(|(node_id, entry)| {
                        (
                            entry.last_confirmed.is_some(),
                            entry.last_success.unwrap_or(0),
                            entry.last_seen,
                            node_id_sort_key(node_id),
                        )
                    })
                    .map(|(node_id, _)| *node_id)
            })
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct DiscoveryEntryMetadata {
    source: Option<NodeId>,
    is_static: bool,
    last_seen: u64,
    last_dial_attempt: Option<u64>,
    last_success: Option<u64>,
    last_confirmed: Option<u64>,
    failure_count: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ZakuraStaticDiscoveryCandidate {
    node_id: NodeId,
    direct_addrs: Vec<SocketAddr>,
    last_seen: u64,
    last_dial_attempt: Option<u64>,
    last_success: Option<u64>,
    last_short_lived_exchange: Option<u64>,
    failure_count: u32,
    /// No dial before this Unix time after an ALPN mismatch (zakura-quic DIAL-5).
    alpn_backoff_until: Option<u64>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct DialCandidateExclusions<'a> {
    connected_node_ids: &'a [NodeId],
    in_flight_node_ids: &'a [NodeId],
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum DiscoveryDirectAddrPolicy {
    UntrustedPeer,
    StaticConfigured,
}

enum DialCandidateRef<'a> {
    SignedRecord(&'a ZakuraDiscoveryEntry),
    StaticConfigured(&'a ZakuraStaticDiscoveryCandidate),
}

impl DialCandidateRef<'_> {
    fn node_id(&self) -> NodeId {
        match self {
            Self::SignedRecord(entry) => entry.record.body.node_id,
            Self::StaticConfigured(candidate) => candidate.node_id,
        }
    }

    fn is_static(&self) -> bool {
        match self {
            Self::SignedRecord(entry) => entry.is_static,
            Self::StaticConfigured(_) => true,
        }
    }

    fn last_seen(&self) -> u64 {
        match self {
            Self::SignedRecord(entry) => entry.last_seen,
            Self::StaticConfigured(candidate) => candidate.last_seen,
        }
    }

    fn last_success(&self) -> Option<u64> {
        match self {
            Self::SignedRecord(entry) => entry.last_success,
            Self::StaticConfigured(candidate) => candidate.last_success,
        }
    }

    fn failure_count(&self) -> u32 {
        match self {
            Self::SignedRecord(entry) => entry.failure_count,
            Self::StaticConfigured(candidate) => candidate.failure_count,
        }
    }

    fn into_candidate(self) -> ZakuraDiscoveryDialCandidate {
        match self {
            Self::SignedRecord(entry) => ZakuraDiscoveryDialCandidate {
                node_id: entry.record.body.node_id,
                direct_addrs: entry.record.body.direct_addrs.clone(),
                is_static: entry.is_static,
            },
            Self::StaticConfigured(candidate) => ZakuraDiscoveryDialCandidate {
                node_id: candidate.node_id,
                direct_addrs: candidate.direct_addrs.clone(),
                is_static: true,
            },
        }
    }
}

/// Total dial-priority key for one candidate: prefer signed records over static ones, then most
/// recent success, fewest failures, most recently seen, with a per-call random tie-break for
/// non-static candidates and a deterministic node-id tie-break for static ones.
type DialCandidateSortKey = (
    bool,
    Reverse<u64>,
    u32,
    Reverse<u64>,
    u64,
    [u8; NODE_ID_BYTES],
);

/// Computes the dial-priority key used to select the best dial candidates.
///
/// Factored out of [`ZakuraDiscoveryBook::dial_candidates`] so the key can be computed once per
/// candidate and fed to a bounded top-k selection instead of a full sort, which keeps expensive
/// per-book ordering work bounded under the global discovery mutex (finding
/// `claude-discovery-expensive-work-under-global-mutex`).
fn dial_candidate_sort_key<R: rand_10::Rng + ?Sized>(
    candidate: &DialCandidateRef<'_>,
    rng: &mut R,
) -> DialCandidateSortKey {
    let non_static_random_tie = if !candidate.is_static() {
        rand_10::RngExt::random::<u64>(rng)
    } else {
        0
    };
    let static_deterministic_tie = if candidate.is_static() {
        node_id_sort_key(&candidate.node_id())
    } else {
        [0; NODE_ID_BYTES]
    };
    (
        !candidate.is_static(),
        Reverse(candidate.last_success().unwrap_or(0)),
        candidate.failure_count(),
        Reverse(candidate.last_seen()),
        non_static_random_tie,
        static_deterministic_tie,
    )
}

/// The relay an entry with `source` is attributed to: its source, unless the
/// source authored the record for `node_id`.
fn relay_of(source: Option<NodeId>, node_id: NodeId) -> Option<NodeId> {
    source.filter(|source| *source != node_id)
}

fn update_existing_entry(
    entry: &mut ZakuraDiscoveryEntry,
    record: ZakuraNodeRecord,
    metadata: DiscoveryEntryMetadata,
    now: u64,
) -> ImportOutcome {
    let incoming_sequence = record.body.sequence;
    let stored_sequence = entry.record.body.sequence;
    // An expired record no longer orders its node's records, so a node that
    // reset its sequence recovers once its old record expires.
    let stored_orders = !entry_is_expired(entry, now);

    if stored_orders && incoming_sequence < stored_sequence {
        return ImportOutcome::IgnoredOlder;
    }
    let same_sequence = stored_orders && incoming_sequence == stored_sequence;

    let preserve_first_party_source = same_sequence
        && entry.source == Some(entry.record.body.node_id)
        && metadata.source != Some(entry.record.body.node_id);

    if !preserve_first_party_source {
        entry.source = metadata.source;
    }
    entry.is_static |= metadata.is_static;
    entry.last_seen = metadata.last_seen;
    // Confirmation attaches to the node identity, not one record body: a later record is
    // still author-signed, so a fresh first-party/static import confirms and a third-party
    // import (`last_confirmed: None`) keeps any earlier confirmation.
    entry.last_confirmed = entry.last_confirmed.max(metadata.last_confirmed);

    if same_sequence {
        return ImportOutcome::MetadataUpdated;
    }

    entry.record = record;
    ImportOutcome::Updated
}

fn connected_record_is_fresh_for_stored_record(
    record: &ZakuraNodeRecord,
    entry: &ZakuraDiscoveryEntry,
    now: u64,
) -> bool {
    // An expired record no longer orders the peer's records.
    if entry_is_expired(entry, now) {
        return true;
    }
    match record.body.sequence.cmp(&entry.record.body.sequence) {
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => entry.record() == record,
        std::cmp::Ordering::Greater => true,
    }
}

fn is_direct_address_import_error(error: &DiscoveryBookError) -> bool {
    matches!(
        error,
        DiscoveryBookError::NoUsableDirectAddress
            | DiscoveryBookError::NonDialableDirectAddress { .. }
    )
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

/// Interim wall-clock-nanosecond self-record sequence seed.
///
/// This strictly increases across a same-key restart only while the system clock is monotonic. A
/// backward clock step can still produce a non-increasing sequence; the durable fix is to persist
/// and reload `last_authored_sequence` in the cache-persistence task.
fn current_sequence_tick() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn normalize_direct_addrs(addrs: Vec<SocketAddr>) -> Vec<SocketAddr> {
    let mut addrs: Vec<_> = addrs
        .into_iter()
        .filter(is_discovery_dialable_addr)
        .collect();
    addrs.sort_unstable();
    addrs.dedup();
    addrs.truncate(MAX_DIRECT_ADDRS_PER_RECORD);
    addrs
}

fn normalize_services(mut services: Vec<ZakuraServiceId>) -> Vec<ZakuraServiceId> {
    services.sort_unstable();
    services.dedup();
    services
}

/// Normalizes this node's advertised services.
///
/// It rejects a list the self-record cannot carry. Truncating the sorted list
/// would silently drop built-in `zakura.*` services behind custom ids.
fn advertised_services(
    services: Vec<ZakuraServiceId>,
) -> Result<Vec<ZakuraServiceId>, DiscoveryWireError> {
    let services = normalize_services(services);
    if services.len() > MAX_SERVICES_PER_RECORD {
        return Err(DiscoveryWireError::TooManyAdvertisedServices {
            actual: services.len(),
            max: MAX_SERVICES_PER_RECORD,
        });
    }
    Ok(services)
}

fn push_unique_service(services: &mut Vec<ZakuraServiceId>, service: ZakuraServiceId) {
    if services.iter().any(|existing| existing == &service) {
        return;
    }
    if services.len() < MAX_SERVICES_PER_RECORD {
        services.push(service);
        services.sort_unstable();
    }
}

fn effective_live_summary_expiry(
    declared_expires_at: u64,
    now_unix_secs: u64,
    clock_skew_tolerance: Duration,
) -> Option<u64> {
    let local_expiry = now_unix_secs.saturating_add(DEFAULT_LIVE_SERVICE_SUMMARY_TTL.as_secs());
    if declared_expires_at > now_unix_secs {
        return Some(declared_expires_at.min(local_expiry));
    }
    if declared_expires_at.saturating_add(clock_skew_tolerance.as_secs()) <= now_unix_secs {
        return None;
    }
    // This response arrived on the authenticated live stream but is already expired locally. Use
    // a bounded receipt TTL only inside the accepted skew window; future expiries still honor a
    // peer's intentionally shorter lifetime.
    Some(local_expiry)
}

fn bounded_services(
    services: &[ZakuraServiceId],
    limits: ZakuraDiscoveryBookLimits,
) -> Vec<ZakuraServiceId> {
    services
        .iter()
        .take(limits.max_services_per_record)
        .cloned()
        .collect()
}

fn push_excluded_node_id(excluded: &mut Vec<[u8; 32]>, node_id: NodeId, max_node_ids: usize) {
    let node_id = *node_id.as_bytes();
    if excluded.len() < max_node_ids && !excluded.contains(&node_id) {
        excluded.push(node_id);
    }
}

fn connected_peer_node_ids(connected: &[ZakuraPeerId]) -> Vec<NodeId> {
    connected
        .iter()
        .filter_map(|peer_id| {
            let bytes: [u8; NODE_ID_BYTES] = peer_id.as_bytes().try_into().ok()?;
            NodeId::from_bytes(&bytes).ok()
        })
        .collect()
}

fn discovery_dial_slot_limit(
    connected_count: usize,
    in_flight_count: usize,
    max_connections: usize,
    connection_headroom: usize,
    max_concurrent_dials: usize,
) -> usize {
    let soft_cap = max_connections.saturating_sub(connection_headroom);
    let available_connection_slots = soft_cap.saturating_sub(connected_count);
    let available_dial_slots = max_concurrent_dials.saturating_sub(in_flight_count);
    available_connection_slots.min(available_dial_slots)
}

fn advertises_all_services(record: &ZakuraNodeRecord, wanted_services: &[ZakuraServiceId]) -> bool {
    wanted_services
        .iter()
        .all(|wanted| record.body.services.contains(wanted))
}

fn advertises_any_service(record: &ZakuraNodeRecord, wanted_services: &[ZakuraServiceId]) -> bool {
    wanted_services
        .iter()
        .any(|wanted| record.body.services.contains(wanted))
}

/// Validates direct addresses for discovery-book storage.
///
/// Untrusted peer/gossip records must only contain globally routable addresses, preserving the
/// discovery security rule that gossiped records cannot inject loopback, link-local, multicast,
/// broadcast, RFC 1918 private, RFC 6598 shared (CGNAT), or RFC 4193 unique-local targets. Static
/// records are trusted-by-configuration bootstrap records, so they may use
/// local addresses for regtest and single-host deployments, but still reject empty, unspecified, and
/// port-0 targets that cannot be dialed as configured peers.
fn validate_discovery_direct_addrs(
    record: &ZakuraNodeRecord,
    policy: DiscoveryDirectAddrPolicy,
) -> Result<(), DiscoveryBookError> {
    if record.body.direct_addrs.is_empty() {
        return Err(DiscoveryBookError::NoUsableDirectAddress);
    }

    for addr in &record.body.direct_addrs {
        let is_valid = match policy {
            DiscoveryDirectAddrPolicy::UntrustedPeer => is_discovery_dialable_addr(addr),
            DiscoveryDirectAddrPolicy::StaticConfigured => {
                is_static_discovery_configured_addr_usable(addr)
            }
        };
        if !is_valid {
            return Err(DiscoveryBookError::NonDialableDirectAddress { addr: *addr });
        }
    }

    Ok(())
}

fn has_discovery_dialable_direct_addrs(record: &ZakuraNodeRecord) -> bool {
    !record.body.direct_addrs.is_empty()
        && record
            .body
            .direct_addrs
            .iter()
            .all(is_discovery_dialable_addr)
}

fn has_discovery_usable_direct_addrs(entry: &ZakuraDiscoveryEntry) -> bool {
    !entry.record.body.direct_addrs.is_empty()
        && entry.record.body.direct_addrs.iter().all(|addr| {
            if entry.is_static {
                is_static_discovery_configured_addr_usable(addr)
            } else {
                is_discovery_dialable_addr(addr)
            }
        })
}

/// Returns true only for addresses that are safe to dial from untrusted discovery gossip.
///
/// A signed `ZakuraNodeRecord` proves control of the node key, not ownership of the advertised
/// `direct_addrs`. To stop an authenticated discovery peer from steering the candidate dialer at
/// arbitrary non-public targets, gossiped records must advertise only globally routable addresses.
/// Besides the obvious loopback/link-local/multicast/broadcast cases, this rejects RFC 1918 private,
/// RFC 6598 shared (CGNAT), and RFC 4193 unique-local ranges, which are reachable internal targets
/// the dialer would otherwise scan on the operator's network.
fn is_discovery_dialable_addr(addr: &SocketAddr) -> bool {
    if addr.port() == 0 {
        return false;
    }

    match canonical_ip(addr.ip()) {
        IpAddr::V4(ip) => is_discovery_dialable_ipv4(&ip),
        IpAddr::V6(ip) => {
            !ip.is_unspecified()
                && !ip.is_loopback()
                && !ip.is_multicast()
                && !is_ipv6_unicast_link_local(&ip)
                && !is_ipv6_unique_local(&ip)
                && !is_ipv6_site_local(&ip)
                && !is_ipv6_nat64_translation(&ip)
                && !is_ipv6_documentation(&ip)
        }
    }
}

fn is_discovery_dialable_ipv4(ip: &Ipv4Addr) -> bool {
    !ip.is_unspecified()
        && ip.octets()[0] != 0
        && !ip.is_loopback()
        && !ip.is_multicast()
        && !ip.is_broadcast()
        && !ip.is_link_local()
        && !ip.is_private()
        && !is_ipv4_shared(ip)
        && !is_ipv4_protocol_assignment(ip)
        && !is_ipv4_documentation(ip)
        && !is_ipv4_benchmarking(ip)
        && !is_ipv4_reserved(ip)
}

fn is_static_discovery_configured_addr_usable(addr: &SocketAddr) -> bool {
    if addr.port() == 0 {
        return false;
    }

    match canonical_ip(addr.ip()) {
        IpAddr::V4(ip) => !ip.is_unspecified() && !ip.is_multicast() && !ip.is_broadcast(),
        IpAddr::V6(ip) => !ip.is_unspecified() && !ip.is_multicast(),
    }
}

fn is_ipv6_unicast_link_local(ip: &Ipv6Addr) -> bool {
    (ip.segments()[0] & 0xffc0) == 0xfe80
}

fn is_ipv4_protocol_assignment(ip: &Ipv4Addr) -> bool {
    let octets = ip.octets();
    octets[0] == 192 && octets[1] == 0 && octets[2] == 0
}

fn is_ipv4_benchmarking(ip: &Ipv4Addr) -> bool {
    let octets = ip.octets();
    octets[0] == 198 && matches!(octets[1], 18 | 19)
}

fn is_ipv4_reserved(ip: &Ipv4Addr) -> bool {
    ip.octets()[0] >= 240
}

// `Ipv4Addr::is_documentation` is still unstable, so match the three RFC 5737
// documentation ranges (TEST-NET-1/2/3) directly. These are reserved for docs
// and examples, never globally routable, so an advertised documentation address
// can only be an attempt to steer a dial at an internal or bogus target.
fn is_ipv4_documentation(ip: &Ipv4Addr) -> bool {
    let octets = ip.octets();
    matches!(
        (octets[0], octets[1], octets[2]),
        (192, 0, 2) | (198, 51, 100) | (203, 0, 113)
    )
}

// `Ipv6Addr::is_documentation` is still unstable, so match the RFC 3849
// 2001:db8::/32 documentation range directly (IPv6 counterpart of the RFC 5737
// TEST-NET ranges).
fn is_ipv6_documentation(ip: &Ipv6Addr) -> bool {
    let segments = ip.segments();
    segments[0] == 0x2001 && segments[1] == 0x0db8
}

// `Ipv4Addr::is_shared` is still unstable, so match the RFC 6598 100.64.0.0/10 range directly,
// mirroring `is_ipv6_unicast_link_local`.
fn is_ipv4_shared(ip: &Ipv4Addr) -> bool {
    let octets = ip.octets();
    octets[0] == 100 && (octets[1] & 0b1100_0000) == 0b0100_0000
}

// `Ipv6Addr::is_unique_local` is still unstable, so match the RFC 4193 fc00::/7 range directly.
fn is_ipv6_unique_local(ip: &Ipv6Addr) -> bool {
    (ip.segments()[0] & 0xfe00) == 0xfc00
}

// Deprecated RFC 3879 site-local addresses remain internal targets even though they are outside
// the modern RFC 4193 unique-local range.
fn is_ipv6_site_local(ip: &Ipv6Addr) -> bool {
    (ip.segments()[0] & 0xffc0) == 0xfec0
}

fn is_ipv6_nat64_translation(ip: &Ipv6Addr) -> bool {
    let segments = ip.segments();
    segments[0] == 0x0064 && segments[1] == 0xff9b && (segments[2..6] == [0; 4] || segments[2] == 1)
}

// Import accepts records inside the clock-skew window, but runtime liveness is strict.
fn entry_is_expired(entry: &ZakuraDiscoveryEntry, now: u64) -> bool {
    entry.record.body.expires_at_unix_secs < now
}

fn entry_in_dial_backoff(
    entry: &ZakuraDiscoveryEntry,
    now: u64,
    dial_backoff_base: Duration,
    dial_backoff_max: Duration,
) -> bool {
    entry_metadata_in_dial_backoff(
        entry.last_dial_attempt,
        entry.failure_count,
        now,
        dial_backoff_base,
        dial_backoff_max,
    )
}

fn entry_metadata_in_dial_backoff(
    last_dial_attempt: Option<u64>,
    failure_count: u32,
    now: u64,
    dial_backoff_base: Duration,
    dial_backoff_max: Duration,
) -> bool {
    let Some(last_dial_attempt) = last_dial_attempt else {
        return false;
    };
    let backoff = dial_backoff_secs(
        failure_count,
        dial_backoff_base.as_secs(),
        dial_backoff_max.as_secs(),
    );
    now < last_dial_attempt.saturating_add(backoff)
}

fn in_alpn_backoff(alpn_backoff_until: Option<u64>, now: u64) -> bool {
    alpn_backoff_until.is_some_and(|until| now < until)
}

fn entry_in_short_lived_exchange_backoff(
    entry: &ZakuraDiscoveryEntry,
    now: u64,
    dial_backoff_base: Duration,
    dial_backoff_max: Duration,
) -> bool {
    entry_metadata_in_short_lived_exchange_backoff(
        entry.last_short_lived_exchange,
        now,
        dial_backoff_base,
        dial_backoff_max,
    )
}

fn entry_metadata_in_short_lived_exchange_backoff(
    last_short_lived_exchange: Option<u64>,
    now: u64,
    dial_backoff_base: Duration,
    dial_backoff_max: Duration,
) -> bool {
    let Some(last_short_lived_exchange) = last_short_lived_exchange else {
        return false;
    };
    let backoff = dial_backoff_secs(1, dial_backoff_base.as_secs(), dial_backoff_max.as_secs());
    now < last_short_lived_exchange.saturating_add(backoff)
}

fn dial_backoff_secs(failure_count: u32, base_secs: u64, max_secs: u64) -> u64 {
    if failure_count == 0 {
        return 0;
    }

    let shift = failure_count.saturating_sub(1).min(10);
    base_secs.saturating_mul(1u64 << shift).min(max_secs)
}

fn node_id_sort_key(node_id: &NodeId) -> [u8; NODE_ID_BYTES] {
    *node_id.as_bytes()
}

/// Check the record's network, chain, and protocol range against ours.
fn validate_record_context(
    body: &ZakuraNodeRecordBody,
    context: &DiscoveryRecordValidationContext,
) -> Result<(), DiscoveryRecordError> {
    if body.network_id != context.expected_network_id {
        return Err(DiscoveryRecordError::WrongNetwork);
    }
    if body.chain_id != context.expected_chain_id {
        return Err(DiscoveryRecordError::WrongChain);
    }
    if body.zakura_protocol_min > body.zakura_protocol_max
        || context.supported_protocol_min > context.supported_protocol_max
        || body.zakura_protocol_max < context.supported_protocol_min
        || context.supported_protocol_max < body.zakura_protocol_min
    {
        return Err(DiscoveryRecordError::IncompatibleProtocol);
    }
    Ok(())
}

/// Check the record's expiry against the local clock and TTL limit.
pub(super) fn validate_record_expiry(
    body: &ZakuraNodeRecordBody,
    context: &DiscoveryRecordValidationContext,
) -> Result<(), DiscoveryRecordError> {
    let skew = context.clock_skew_tolerance.as_secs();
    if body.expires_at_unix_secs.saturating_add(skew) < context.current_unix_secs {
        return Err(DiscoveryRecordError::Expired);
    }

    let max_future = context
        .current_unix_secs
        .saturating_add(context.max_record_ttl.as_secs())
        .saturating_add(skew);
    if body.expires_at_unix_secs > max_future {
        return Err(DiscoveryRecordError::FarFutureExpiry);
    }

    Ok(())
}

pub(super) fn validate_record_body_bounds(
    body: &ZakuraNodeRecordBody,
) -> Result<(), DiscoveryWireError> {
    if body.direct_addrs.len() > MAX_DIRECT_ADDRS_PER_RECORD {
        return Err(DiscoveryWireError::OversizedPayload {
            actual: body.direct_addrs.len(),
            max: MAX_DIRECT_ADDRS_PER_RECORD,
        });
    }
    if body.services.len() > MAX_SERVICES_PER_RECORD {
        return Err(DiscoveryWireError::OversizedPayload {
            actual: body.services.len(),
            max: MAX_SERVICES_PER_RECORD,
        });
    }
    if body.zakura_protocol_min > body.zakura_protocol_max {
        return Err(DiscoveryWireError::InvalidProtocolRange);
    }
    Ok(())
}

/// The service id a known summary tag is bound to, or `None` for an unknown tag.
pub(super) fn summary_tag_service(summary_tag: u16) -> Option<&'static str> {
    match summary_tag {
        SUMMARY_TAG_HEADER_SYNC => Some(SERVICE_ID_HEADER_SYNC),
        SUMMARY_TAG_HEADER_SYNC_RETIRED => Some(SERVICE_ID_HEADER_SYNC_RETIRED),
        SUMMARY_TAG_DISCOVERY_V1 => Some(SERVICE_ID_DISCOVERY),
        SUMMARY_TAG_BLOCK_SYNC_V1 => Some(SERVICE_ID_BLOCK_SYNC),
        _ => None,
    }
}

pub(super) fn validate_known_summary_payload(
    summary_tag: u16,
    summary_bytes: &[u8],
) -> Result<(), DiscoveryWireError> {
    match summary_tag {
        SUMMARY_TAG_HEADER_SYNC | SUMMARY_TAG_HEADER_SYNC_RETIRED => {
            decode_header_sync_summary(summary_bytes)?;
        }
        SUMMARY_TAG_DISCOVERY_V1 => {
            decode_discovery_summary(summary_bytes)?;
        }
        SUMMARY_TAG_BLOCK_SYNC_V1 => {
            decode_block_sync_summary(summary_bytes)?;
        }
        _ => {}
    }
    Ok(())
}

fn validate_service_id(bytes: &[u8]) -> Result<(), DiscoveryWireError> {
    if bytes.is_empty() {
        return Err(DiscoveryWireError::Empty("service id"));
    }
    if bytes.len() > MAX_ZAKURA_SERVICE_ID_BYTES {
        return Err(DiscoveryWireError::OversizedPayload {
            actual: bytes.len(),
            max: MAX_ZAKURA_SERVICE_ID_BYTES,
        });
    }
    if !bytes.is_ascii() {
        return Err(DiscoveryWireError::NonAsciiServiceId);
    }
    Ok(())
}

pub(super) fn network_id_from_code(value: u32) -> Result<ZakuraNetworkId, DiscoveryWireError> {
    match value {
        1 => Ok(ZakuraNetworkId::Mainnet),
        2 => Ok(ZakuraNetworkId::Testnet),
        3 => Ok(ZakuraNetworkId::Regtest),
        4 => Ok(ZakuraNetworkId::Configured),
        _ => Err(DiscoveryWireError::InvalidNetworkId(value)),
    }
}

fn encode_header_sync_summary(
    summary: &HeaderSyncServiceSummary,
    writer: &mut impl Write,
) -> Result<(), DiscoveryWireError> {
    write_height(writer, summary.best_height)?;
    writer.write_all(&summary.best_hash.0)?;
    write_optional_height(writer, summary.finalized_height)?;
    write_bool(writer, summary.serving_headers)?;
    writer.write_u16::<LittleEndian>(summary.inbound_slots_free)?;
    writer.write_u16::<LittleEndian>(summary.inbound_slots_max)?;
    writer.write_u16::<LittleEndian>(summary.outbound_slots_free)?;
    writer.write_u16::<LittleEndian>(summary.outbound_slots_max)?;
    Ok(())
}

fn decode_header_sync_summary(
    bytes: &[u8],
) -> Result<HeaderSyncServiceSummary, DiscoveryWireError> {
    if bytes.len() > MAX_SERVICE_SUMMARY_BYTES {
        return Err(DiscoveryWireError::OversizedPayload {
            actual: bytes.len(),
            max: MAX_SERVICE_SUMMARY_BYTES,
        });
    }
    let mut reader = Cursor::new(bytes);
    let best_height = read_height(&mut reader)?;
    let mut best_hash = [0u8; 32];
    reader.read_exact(&mut best_hash)?;
    let finalized_height = read_optional_height(&mut reader)?;
    let serving_headers = read_bool(&mut reader, "header sync serving_headers")?;
    let inbound_slots_free = reader.read_u16::<LittleEndian>()?;
    let inbound_slots_max = reader.read_u16::<LittleEndian>()?;
    let outbound_slots_free = reader.read_u16::<LittleEndian>()?;
    let outbound_slots_max = reader.read_u16::<LittleEndian>()?;
    reject_trailing(bytes, &reader)?;
    Ok(HeaderSyncServiceSummary {
        best_height,
        best_hash: block::Hash(best_hash),
        finalized_height,
        serving_headers,
        inbound_slots_free,
        inbound_slots_max,
        outbound_slots_free,
        outbound_slots_max,
    })
}

fn encode_discovery_summary(
    summary: &DiscoveryServiceSummary,
    writer: &mut impl Write,
) -> Result<(), DiscoveryWireError> {
    writer.write_u16::<LittleEndian>(summary.peer_exchange_slots_free)?;
    writer.write_u16::<LittleEndian>(summary.max_records_per_response)?;
    write_bool(writer, summary.expected_disconnect_after_exchange)?;
    Ok(())
}

fn decode_discovery_summary(bytes: &[u8]) -> Result<DiscoveryServiceSummary, DiscoveryWireError> {
    if bytes.len() > MAX_SERVICE_SUMMARY_BYTES {
        return Err(DiscoveryWireError::OversizedPayload {
            actual: bytes.len(),
            max: MAX_SERVICE_SUMMARY_BYTES,
        });
    }
    let mut reader = Cursor::new(bytes);
    let peer_exchange_slots_free = reader.read_u16::<LittleEndian>()?;
    let max_records_per_response = reader.read_u16::<LittleEndian>()?;
    let expected_disconnect_after_exchange =
        read_bool(&mut reader, "discovery expected_disconnect_after_exchange")?;
    reject_trailing(bytes, &reader)?;
    Ok(DiscoveryServiceSummary {
        peer_exchange_slots_free,
        max_records_per_response,
        expected_disconnect_after_exchange,
    })
}

fn encode_block_sync_summary(
    summary: &BlockSyncServiceSummary,
    writer: &mut impl Write,
) -> Result<(), DiscoveryWireError> {
    validate_block_sync_summary(summary)?;
    write_height(writer, summary.servable_low)?;
    write_height(writer, summary.servable_high)?;
    writer.write_all(&summary.tip_hash.0)?;
    writer.write_u16::<LittleEndian>(summary.free_slots)?;
    writer.write_u32::<LittleEndian>(summary.max_blocks_per_response)?;
    writer.write_u32::<LittleEndian>(summary.max_response_bytes)?;
    Ok(())
}

fn decode_block_sync_summary(bytes: &[u8]) -> Result<BlockSyncServiceSummary, DiscoveryWireError> {
    if bytes.len() > MAX_SERVICE_SUMMARY_BYTES {
        return Err(DiscoveryWireError::OversizedPayload {
            actual: bytes.len(),
            max: MAX_SERVICE_SUMMARY_BYTES,
        });
    }
    let mut reader = Cursor::new(bytes);
    let servable_low = read_height(&mut reader)?;
    let servable_high = read_height(&mut reader)?;
    let mut tip_hash = [0u8; 32];
    reader.read_exact(&mut tip_hash)?;
    let free_slots = reader.read_u16::<LittleEndian>()?;
    let max_blocks_per_response = reader.read_u32::<LittleEndian>()?;
    let max_response_bytes = reader.read_u32::<LittleEndian>()?;
    reject_trailing(bytes, &reader)?;
    let summary = BlockSyncServiceSummary {
        servable_low,
        servable_high,
        tip_hash: block::Hash(tip_hash),
        free_slots,
        max_blocks_per_response,
        max_response_bytes,
    };
    validate_block_sync_summary(&summary)?;
    Ok(summary)
}

fn validate_block_sync_summary(
    summary: &BlockSyncServiceSummary,
) -> Result<(), DiscoveryWireError> {
    if summary.servable_low > summary.servable_high {
        return Err(DiscoveryWireError::InvalidProtocolRange);
    }
    if summary.max_blocks_per_response == 0
        || summary.max_blocks_per_response > MAX_BS_BLOCKS_PER_REQUEST
    {
        return Err(DiscoveryWireError::OversizedPayload {
            actual: usize::try_from(summary.max_blocks_per_response).unwrap_or(usize::MAX),
            max: usize::try_from(MAX_BS_BLOCKS_PER_REQUEST).unwrap_or(usize::MAX),
        });
    }
    if summary.max_response_bytes == 0 || summary.max_response_bytes > MAX_BS_RESPONSE_BYTES {
        return Err(DiscoveryWireError::OversizedPayload {
            actual: usize::try_from(summary.max_response_bytes).unwrap_or(usize::MAX),
            max: usize::try_from(MAX_BS_RESPONSE_BYTES).unwrap_or(usize::MAX),
        });
    }
    Ok(())
}

fn write_height(writer: &mut impl Write, height: block::Height) -> Result<(), DiscoveryWireError> {
    writer.write_u32::<LittleEndian>(height.0)?;
    Ok(())
}

fn read_height(reader: &mut impl Read) -> Result<block::Height, DiscoveryWireError> {
    let height = reader.read_u32::<LittleEndian>()?;
    block::Height::try_from(height).map_err(|_| DiscoveryWireError::InvalidHeight(height))
}

fn write_optional_height(
    writer: &mut impl Write,
    height: Option<block::Height>,
) -> Result<(), DiscoveryWireError> {
    match height {
        Some(height) => {
            writer.write_u8(1)?;
            write_height(writer, height)?;
        }
        None => writer.write_u8(0)?,
    }
    Ok(())
}

fn read_optional_height(
    reader: &mut impl Read,
) -> Result<Option<block::Height>, DiscoveryWireError> {
    match reader.read_u8()? {
        0 => Ok(None),
        1 => read_height(reader).map(Some),
        value => Err(DiscoveryWireError::InvalidBoolean {
            field: "optional height present",
            value,
        }),
    }
}

fn write_bool(writer: &mut impl Write, value: bool) -> Result<(), DiscoveryWireError> {
    writer.write_u8(u8::from(value))?;
    Ok(())
}

fn read_bool(reader: &mut impl Read, field: &'static str) -> Result<bool, DiscoveryWireError> {
    match reader.read_u8()? {
        0 => Ok(false),
        1 => Ok(true),
        value => Err(DiscoveryWireError::InvalidBoolean { field, value }),
    }
}

fn saturating_u16(value: usize) -> u16 {
    u16::try_from(value).unwrap_or(u16::MAX)
}

fn reject_trailing(bytes: &[u8], reader: &Cursor<&[u8]>) -> Result<(), DiscoveryWireError> {
    let consumed = usize::try_from(reader.position())
        .map_err(|_| DiscoveryWireError::NumericOverflow("cursor position"))?;
    if consumed != bytes.len() {
        return Err(DiscoveryWireError::TrailingBytes);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{net::IpAddr, time::Duration};

    use rand_10::{rngs::StdRng, SeedableRng};
    use zakura_quic::NodeSecretKey;

    use super::*;
    use crate::zakura::{
        wire_codec::{decode_frame, encode_frame, WireError},
        Frame,
    };

    const NOW: u64 = 1_700_000_000;
    const CHAIN_ID: [u8; 32] = [7; 32];

    fn secret_key() -> NodeSecretKey {
        NodeSecretKey::generate()
    }

    fn service(index: usize) -> ZakuraServiceId {
        ZakuraServiceId::new(format!("zakura.test.{index}.v1")).expect("test service id is valid")
    }

    fn context() -> DiscoveryRecordValidationContext {
        DiscoveryRecordValidationContext {
            expected_network_id: ZakuraNetworkId::Regtest,
            expected_chain_id: CHAIN_ID,
            current_unix_secs: NOW,
            supported_protocol_min: crate::zakura::ZAKURA_PROTOCOL_VERSION_CURRENT,
            supported_protocol_max: crate::zakura::ZAKURA_PROTOCOL_VERSION_CURRENT,
            max_record_ttl: Duration::from_secs(24 * 60 * 60),
            clock_skew_tolerance: Duration::from_secs(300),
        }
    }

    fn body(secret_key: &NodeSecretKey) -> ZakuraNodeRecordBody {
        ZakuraNodeRecordBody {
            node_id: secret_key.public(),
            direct_addrs: vec![
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(45, 33, 30, 10)), 8233),
                SocketAddr::new(
                    IpAddr::V6(Ipv6Addr::new(0x2606, 0x4700, 0x4700, 0, 0, 0, 0, 0x10)),
                    18233,
                ),
            ],
            services: vec![
                ZakuraServiceId::discovery(),
                ZakuraServiceId::legacy_gossip(),
                ZakuraServiceId::legacy_requests(),
            ],
            zakura_protocol_min: crate::zakura::ZAKURA_PROTOCOL_VERSION_CURRENT,
            zakura_protocol_max: crate::zakura::ZAKURA_PROTOCOL_VERSION_CURRENT,
            network_id: ZakuraNetworkId::Regtest,
            chain_id: CHAIN_ID,
            sequence: 42,
            expires_at_unix_secs: NOW + 60,
            extensions: Vec::new(),
        }
    }

    fn signed_record() -> ZakuraNodeRecord {
        let secret_key = secret_key();
        ZakuraNodeRecord::sign(body(&secret_key), &secret_key).expect("test record signs")
    }

    fn sign_record_with_prefix(
        body: ZakuraNodeRecordBody,
        secret_key: &NodeSecretKey,
        domain: &[u8],
        record_format_version: u16,
    ) -> ZakuraNodeRecord {
        let mut bytes = Vec::new();
        bytes.write_all(domain).expect("test domain writes");
        bytes
            .write_u16::<LittleEndian>(record_format_version)
            .expect("test record format version writes");
        write_record_body(&body, &mut bytes);

        let signing_key = SigningKey::from(secret_key.to_bytes());
        let signature = signing_key.sign(&bytes);
        ZakuraNodeRecord { body, signature }
    }

    fn signed_record_with(
        sequence: u64,
        service: ZakuraServiceId,
        addr: SocketAddr,
    ) -> ZakuraNodeRecord {
        signed_record_with_addrs(sequence, service, vec![addr])
    }

    /// Signs a record that outlives the default 60-second test TTL.
    ///
    /// Tests that step `now` across disclosure epochs need records that stay unexpired for
    /// the whole walk; the declared expiry stays inside `DEFAULT_DISCOVERY_MAX_RECORD_TTL`.
    fn long_lived_record_with(
        sequence: u64,
        service: ZakuraServiceId,
        addr: SocketAddr,
    ) -> ZakuraNodeRecord {
        let secret_key = secret_key();
        let mut body = body(&secret_key);
        body.sequence = sequence;
        body.direct_addrs = vec![addr];
        body.services = vec![service];
        body.expires_at_unix_secs = NOW + DEFAULT_DISCOVERY_MAX_RECORD_TTL.as_secs();
        ZakuraNodeRecord::sign(body, &secret_key).expect("test record signs")
    }

    fn signed_record_with_addrs(
        sequence: u64,
        service: ZakuraServiceId,
        addrs: Vec<SocketAddr>,
    ) -> ZakuraNodeRecord {
        let secret_key = secret_key();
        let mut body = body(&secret_key);
        body.sequence = sequence;
        body.direct_addrs = addrs;
        body.services = vec![service];
        ZakuraNodeRecord::sign(body, &secret_key).expect("test record signs")
    }

    fn record_with_secret(
        secret_key: &NodeSecretKey,
        sequence: u64,
        service: ZakuraServiceId,
    ) -> ZakuraNodeRecord {
        let mut body = body(secret_key);
        body.sequence = sequence;
        body.services = vec![service];
        ZakuraNodeRecord::sign(body, secret_key).expect("test record signs")
    }

    fn test_addr(index: u8) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(45, 33, 30, index)), 8233)
    }

    fn candidate_for(record: &ZakuraNodeRecord, is_static: bool) -> ZakuraDiscoveryDialCandidate {
        ZakuraDiscoveryDialCandidate {
            node_id: record.body.node_id,
            direct_addrs: record.body.direct_addrs.clone(),
            is_static,
        }
    }

    fn import_confirmed_record(
        book: &mut ZakuraDiscoveryBook,
        record: ZakuraNodeRecord,
    ) -> Result<ImportOutcome, DiscoveryBookError> {
        let source = record.body.node_id;
        book.import_record(record, Some(source), NOW, &context())
    }

    /// The authenticated requester used by peer-sample tests.
    ///
    /// Window placement is keyed by a per-book random `RandomState`, so tests assert
    /// the set properties — stability within an epoch, rotation across epochs, and the
    /// disclosure bound — rather than any specific node id ordering.
    fn test_requester() -> NodeId {
        secret_key().public()
    }

    /// A `now` inside the disclosure epoch after the one containing `now`.
    fn next_epoch(now: u64) -> u64 {
        now + DISCOVERY_DISCLOSURE_EPOCH.as_secs()
    }

    fn small_book(max_records: usize) -> ZakuraDiscoveryBook {
        ZakuraDiscoveryBook::new(ZakuraDiscoveryBookLimits {
            max_records,
            ..ZakuraDiscoveryBookLimits::default()
        })
    }

    fn runtime_record_with(
        sequence: u64,
        service: ZakuraServiceId,
        addr: SocketAddr,
    ) -> ZakuraNodeRecord {
        let secret_key = secret_key();
        runtime_record_with_secret(&secret_key, sequence, service, addr)
    }

    fn runtime_record_with_secret(
        secret_key: &NodeSecretKey,
        sequence: u64,
        service: ZakuraServiceId,
        addr: SocketAddr,
    ) -> ZakuraNodeRecord {
        runtime_record_with_secret_and_addrs(secret_key, sequence, service, vec![addr])
    }

    fn runtime_record_with_secret_and_addrs(
        secret_key: &NodeSecretKey,
        sequence: u64,
        service: ZakuraServiceId,
        addrs: Vec<SocketAddr>,
    ) -> ZakuraNodeRecord {
        let mut body = body(secret_key);
        body.sequence = sequence;
        body.direct_addrs = addrs;
        body.services = vec![service];
        body.expires_at_unix_secs = current_unix_secs() + DEFAULT_DISCOVERY_RECORD_TTL.as_secs();
        ZakuraNodeRecord::sign(body, secret_key).expect("runtime test record signs")
    }

    fn local_config_with(
        secret_key: NodeSecretKey,
        direct_addrs: Vec<SocketAddr>,
        services: Vec<ZakuraServiceId>,
    ) -> ZakuraDiscoveryLocalConfig {
        ZakuraDiscoveryLocalConfig {
            secret_key,
            direct_addrs,
            services,
            zakura_protocol_min: crate::zakura::ZAKURA_PROTOCOL_VERSION_CURRENT,
            zakura_protocol_max: crate::zakura::ZAKURA_PROTOCOL_VERSION_CURRENT,
            network_id: ZakuraNetworkId::Regtest,
            chain_id: CHAIN_ID,
            last_authored_sequence: None,
        }
    }

    fn discovery_handle_at(
        local_config: ZakuraDiscoveryLocalConfig,
        config: ZakuraDiscoveryConfig,
        connected: watch::Receiver<Vec<ZakuraPeerId>>,
        now: u64,
    ) -> ZakuraDiscoveryHandle {
        ZakuraDiscoveryHandle::new_at(local_config, config, connected, now, now)
            .expect("test discovery state constructs")
    }

    fn discovery_handle_at_with_sequence(
        local_config: ZakuraDiscoveryLocalConfig,
        config: ZakuraDiscoveryConfig,
        connected: watch::Receiver<Vec<ZakuraPeerId>>,
        now: u64,
        wall_clock_sequence: u64,
    ) -> ZakuraDiscoveryHandle {
        ZakuraDiscoveryHandle::new_at(local_config, config, connected, now, wall_clock_sequence)
            .expect("test discovery state constructs")
    }

    /// Imports one peer record through the relayed-batch path and asserts the book accepted it.
    async fn import_one_peer_record(
        handle: &ZakuraDiscoveryHandle,
        record: ZakuraNodeRecord,
        source: Option<NodeId>,
    ) {
        let outcome = handle
            .import_peer_records([record], source)
            .await
            .expect("authentic test record verifies");
        assert_eq!(outcome.rejected, 0, "test record imports into the book");
    }

    fn discovery_handle_with_connected(
        connected: watch::Receiver<Vec<ZakuraPeerId>>,
    ) -> ZakuraDiscoveryHandle {
        discovery_handle_at(
            local_config_with(
                secret_key(),
                vec![test_addr(200)],
                vec![ZakuraServiceId::discovery()],
            ),
            ZakuraDiscoveryConfig::default(),
            connected,
            NOW,
        )
    }

    fn peer_id_for(node_id: NodeId) -> ZakuraPeerId {
        ZakuraPeerId::new(node_id.as_bytes().to_vec()).expect("node id is a valid peer id")
    }

    fn header_summary() -> HeaderSyncServiceSummary {
        HeaderSyncServiceSummary {
            best_height: block::Height(42),
            best_hash: block::Hash([7; 32]),
            finalized_height: Some(block::Height(40)),
            serving_headers: true,
            inbound_slots_free: 3,
            inbound_slots_max: 10,
            outbound_slots_free: 4,
            outbound_slots_max: 11,
        }
    }

    fn retired_header_sync_envelope(summary: &HeaderSyncServiceSummary) -> ServiceSummaryEnvelope {
        let mut summary_bytes = Vec::new();
        encode_header_sync_summary(summary, &mut summary_bytes)
            .expect("test header summary encodes");
        ServiceSummaryEnvelope {
            service_id: ZakuraServiceId::header_sync_retired(),
            summary_tag: SUMMARY_TAG_HEADER_SYNC_RETIRED,
            summary_bytes,
        }
    }

    fn discovery_summary() -> DiscoveryServiceSummary {
        DiscoveryServiceSummary {
            peer_exchange_slots_free: 5,
            max_records_per_response: 12,
            expected_disconnect_after_exchange: true,
        }
    }

    fn block_sync_summary() -> BlockSyncServiceSummary {
        BlockSyncServiceSummary {
            servable_low: block::Height(10),
            servable_high: block::Height(42),
            tip_hash: block::Hash([9; 32]),
            free_slots: 3,
            max_blocks_per_response: 16,
            max_response_bytes: MAX_BS_RESPONSE_BYTES,
        }
    }

    fn block_sync_status() -> BlockSyncStatus {
        BlockSyncStatus {
            servable_low: block::Height(10),
            servable_high: block::Height(42),
            tip_hash: block::Hash([9; 32]),
            max_blocks_per_response: 16,
            max_inflight_requests: 4,
            max_response_bytes: MAX_BS_RESPONSE_BYTES,
        }
    }

    fn services_message(summaries: Vec<ServiceSummaryEnvelope>) -> DiscoveryMessage {
        DiscoveryMessage::Services(Services {
            node_id: secret_key().public(),
            expires_at_unix_secs: NOW + 30,
            summaries,
        })
    }

    fn first_party_services(
        node_id: NodeId,
        expires_at_unix_secs: u64,
        summaries: Vec<ServiceSummaryEnvelope>,
    ) -> Services {
        Services {
            node_id,
            expires_at_unix_secs,
            summaries,
        }
    }

    /// Tests write a discovery message as its frame type byte followed by its
    /// payload, so hand-built fixtures name their type inline.
    trait TestFraming: Sized {
        fn encode(&self) -> Result<Vec<u8>, WireError>;
        fn decode(bytes: &[u8]) -> Result<Self, WireError>;
    }

    impl TestFraming for DiscoveryMessage {
        fn encode(&self) -> Result<Vec<u8>, WireError> {
            let frame = encode_frame(self)?;
            let message_type = u8::try_from(frame.message_type)
                .map_err(|_| WireError::UnknownMessageType(frame.message_type))?;
            let mut bytes = vec![message_type];
            bytes.extend_from_slice(&frame.payload);
            Ok(bytes)
        }

        fn decode(bytes: &[u8]) -> Result<Self, WireError> {
            let (&message_type, payload) =
                bytes.split_first().ok_or(WireError::Truncated("type"))?;
            decode_frame(&Frame {
                message_type: u16::from(message_type),
                flags: 0,
                payload: payload.to_vec(),
            })
        }
    }

    #[test]
    fn service_id_accepts_valid_ids_and_rejects_invalid_ids() {
        assert_eq!(
            ZakuraServiceId::new("zakura.custom.v1").unwrap().as_str(),
            "zakura.custom.v1"
        );
        assert!(matches!(
            ZakuraServiceId::new(""),
            Err(DiscoveryWireError::Empty("service id"))
        ));
        assert!(matches!(
            ZakuraServiceId::new("zakura.\u{2603}.v1"),
            Err(DiscoveryWireError::NonAsciiServiceId)
        ));
        assert!(matches!(
            ZakuraServiceId::new("a".repeat(MAX_ZAKURA_SERVICE_ID_BYTES + 1)),
            Err(DiscoveryWireError::OversizedPayload { .. })
        ));
    }

    #[test]
    fn block_sync_summary_uses_live_peer_slots() {
        let limits = ServicePeerLimits {
            max_inbound_peers: 2,
            ..ServicePeerLimits::default()
        };
        let status = block_sync_status();

        let open = BlockSyncServiceSummary::from_status_and_snapshot(
            status,
            ServicePeerSnapshot::new(1, 0, limits),
        );
        assert_eq!(open.free_slots, 1);

        let full = BlockSyncServiceSummary::from_status_and_snapshot(
            status,
            ServicePeerSnapshot::new(2, 0, limits),
        );
        assert_eq!(full.free_slots, 0);
    }

    #[test]
    fn node_record_encode_decode_roundtrip() {
        let record = signed_record();
        let signed_bytes = record.body.encode_for_signature().expect("record signs");
        let mut signed_bytes_reader =
            Cursor::new(&signed_bytes[ZAKURA_NODE_RECORD_SIG_DOMAIN.len()..]);
        assert!(signed_bytes.starts_with(ZAKURA_NODE_RECORD_SIG_DOMAIN));
        assert_eq!(
            signed_bytes_reader.read_u16::<LittleEndian>().unwrap(),
            ZAKURA_NODE_RECORD_FORMAT_VERSION
        );

        let mut body_bytes = Vec::new();
        write_record_body(&record.body, &mut body_bytes);
        assert_eq!(
            &signed_bytes[ZAKURA_NODE_RECORD_SIG_DOMAIN.len() + 2..],
            body_bytes.as_slice()
        );

        let bytes = DiscoveryMessage::Hello {
            record: record.clone(),
        }
        .encode()
        .expect("hello encodes");
        let encoded_body_len =
            usize::try_from(Cursor::new(&bytes[1..]).read_u32::<LittleEndian>().unwrap())
                .expect("encoded record body length fits in usize");
        assert_eq!(encoded_body_len, body_bytes.len());
        assert_eq!(bytes.len() - 1, record_len(&record));

        let DiscoveryMessage::Hello { record: decoded } =
            DiscoveryMessage::decode(&bytes).expect("hello decodes")
        else {
            panic!("expected a hello message");
        };

        assert_eq!(decoded, record);
        assert_eq!(decoded.body.encode_for_signature().unwrap(), signed_bytes);
        decoded.verify(&context()).expect("record verifies");
    }

    #[test]
    fn discovery_message_roundtrips_every_variant() {
        let record = signed_record();
        let other = signed_record();
        let services = vec![ZakuraServiceId::discovery(), service(1)];
        let mut excluded = [
            *record.body.node_id.as_bytes(),
            *other.body.node_id.as_bytes(),
        ];
        excluded.sort_unstable();
        let header_envelope =
            ServiceSummaryEnvelope::header_sync(&header_summary()).expect("summary encodes");
        let discovery_envelope =
            ServiceSummaryEnvelope::discovery(&discovery_summary()).expect("summary encodes");
        let block_envelope =
            ServiceSummaryEnvelope::block_sync(&block_sync_summary()).expect("summary encodes");
        let messages = vec![
            DiscoveryMessage::Hello {
                record: record.clone(),
            },
            DiscoveryMessage::GetPeers(GetPeers {
                limit: 2,
                wanted_services: services.clone(),
                exclude_node_ids: excluded.to_vec(),
            }),
            DiscoveryMessage::Peers {
                records: vec![record.clone(), other.clone()],
            },
            DiscoveryMessage::GetServices(GetServices {
                wanted_services: services,
            }),
            services_message(vec![header_envelope, discovery_envelope, block_envelope]),
        ];

        for message in messages {
            let encoded = message.encode().expect("message encodes");
            assert_eq!(DiscoveryMessage::decode(&encoded).unwrap(), message);
        }
    }

    #[test]
    fn service_summary_envelopes_roundtrip_and_unknown_tags_are_skipped() {
        let header = header_summary();
        let discovery = discovery_summary();
        let block = block_sync_summary();
        let header_envelope =
            ServiceSummaryEnvelope::header_sync(&header).expect("header summary encodes");
        let discovery_envelope =
            ServiceSummaryEnvelope::discovery(&discovery).expect("discovery summary encodes");
        let block_envelope =
            ServiceSummaryEnvelope::block_sync(&block).expect("block summary encodes");
        let unknown_envelope = ServiceSummaryEnvelope {
            service_id: service(99),
            summary_tag: 999,
            summary_bytes: vec![1, 2, 3, 4],
        };

        assert_eq!(header_envelope.decode_header_sync().unwrap(), Some(header));
        assert_eq!(
            discovery_envelope.decode_discovery().unwrap(),
            Some(discovery)
        );
        assert_eq!(block_envelope.decode_block_sync().unwrap(), Some(block));

        let message = services_message(vec![
            unknown_envelope.clone(),
            header_envelope.clone(),
            discovery_envelope.clone(),
            block_envelope.clone(),
        ]);
        let decoded = DiscoveryMessage::decode(&message.encode().expect("message encodes"))
            .expect("message decodes");

        let DiscoveryMessage::Services(services) = decoded else {
            panic!("expected service response");
        };
        assert_eq!(services.summaries[0], unknown_envelope);
        assert_eq!(
            services.summaries[1].decode_header_sync().unwrap(),
            Some(header)
        );
        assert_eq!(
            services.summaries[2].decode_discovery().unwrap(),
            Some(discovery)
        );
        assert_eq!(
            services.summaries[3].decode_block_sync().unwrap(),
            Some(block)
        );
    }

    #[test]
    fn service_summary_decode_rejects_lying_truncated_and_oversized_lengths() {
        let node_id = secret_key().public();
        let mut lying = vec![MSG_DISCOVERY_SERVICES];
        lying.write_all(node_id.as_bytes()).unwrap();
        lying.write_u64::<LittleEndian>(NOW + 30).unwrap();
        lying.write_u16::<LittleEndian>(1).unwrap();
        super::super::wire::write_service_id(&service(1), &mut lying);
        lying.write_u16::<LittleEndian>(999).unwrap();
        lying.write_u16::<LittleEndian>(4).unwrap();
        lying.write_u8(1).unwrap();

        assert!(matches!(
            DiscoveryMessage::decode(&lying),
            Err(WireError::Truncated("service summary"))
        ));

        let mut truncated_known = vec![MSG_DISCOVERY_SERVICES];
        truncated_known.write_all(node_id.as_bytes()).unwrap();
        truncated_known.write_u64::<LittleEndian>(NOW + 30).unwrap();
        truncated_known.write_u16::<LittleEndian>(1).unwrap();
        super::super::wire::write_service_id(&ZakuraServiceId::header_sync(), &mut truncated_known);
        truncated_known
            .write_u16::<LittleEndian>(SUMMARY_TAG_HEADER_SYNC)
            .unwrap();
        truncated_known.write_u16::<LittleEndian>(1).unwrap();
        truncated_known.write_u8(0).unwrap();

        assert!(DiscoveryMessage::decode(&truncated_known).is_err());

        let mut truncated_block = vec![MSG_DISCOVERY_SERVICES];
        truncated_block.write_all(node_id.as_bytes()).unwrap();
        truncated_block.write_u64::<LittleEndian>(NOW + 30).unwrap();
        truncated_block.write_u16::<LittleEndian>(1).unwrap();
        super::super::wire::write_service_id(&ZakuraServiceId::block_sync(), &mut truncated_block);
        truncated_block
            .write_u16::<LittleEndian>(SUMMARY_TAG_BLOCK_SYNC_V1)
            .unwrap();
        truncated_block.write_u16::<LittleEndian>(8).unwrap();
        write_height(&mut truncated_block, block::Height(10)).unwrap();
        write_height(&mut truncated_block, block::Height(42)).unwrap();

        assert!(DiscoveryMessage::decode(&truncated_block).is_err());

        let mut oversized = vec![MSG_DISCOVERY_SERVICES];
        oversized.write_all(node_id.as_bytes()).unwrap();
        oversized.write_u64::<LittleEndian>(NOW + 30).unwrap();
        oversized.write_u16::<LittleEndian>(1).unwrap();
        super::super::wire::write_service_id(&ZakuraServiceId::block_sync(), &mut oversized);
        oversized
            .write_u16::<LittleEndian>(SUMMARY_TAG_BLOCK_SYNC_V1)
            .unwrap();
        oversized
            .write_u16::<LittleEndian>(
                u16::try_from(MAX_SERVICE_SUMMARY_BYTES + 1).expect("test length fits"),
            )
            .unwrap();

        assert!(matches!(
            DiscoveryMessage::decode(&oversized),
            Err(WireError::OutOfRange("service summary length"))
        ));
    }

    #[test]
    fn known_service_summary_payload_trailing_bytes_are_rejected() {
        let mut envelope =
            ServiceSummaryEnvelope::header_sync(&header_summary()).expect("summary encodes");
        envelope.summary_bytes.push(0);

        assert!(matches!(
            services_message(vec![envelope.clone()]).encode(),
            Err(WireError::OutOfRange("service summary"))
        ));

        let node_id = secret_key().public();
        let mut encoded = vec![MSG_DISCOVERY_SERVICES];
        encoded.write_all(node_id.as_bytes()).unwrap();
        encoded.write_u64::<LittleEndian>(NOW + 30).unwrap();
        encoded.write_u16::<LittleEndian>(1).unwrap();
        super::super::wire::write_service_id(&envelope.service_id, &mut encoded);
        encoded
            .write_u16::<LittleEndian>(envelope.summary_tag)
            .unwrap();
        encoded
            .write_u16::<LittleEndian>(
                u16::try_from(envelope.summary_bytes.len()).expect("test length fits"),
            )
            .unwrap();
        encoded.write_all(&envelope.summary_bytes).unwrap();

        assert!(matches!(
            DiscoveryMessage::decode(&encoded),
            Err(WireError::OutOfRange("service summary"))
        ));

        let mut block_envelope =
            ServiceSummaryEnvelope::block_sync(&block_sync_summary()).expect("summary encodes");
        block_envelope.summary_bytes.push(0);

        assert!(matches!(
            services_message(vec![block_envelope]).encode(),
            Err(WireError::OutOfRange("service summary"))
        ));
    }

    #[test]
    fn signature_verifies_for_author_and_fails_after_mutation() {
        let record = signed_record();
        record.verify(&context()).expect("record verifies");
        assert!(record
            .body
            .encode_for_signature()
            .expect("record pre-image encodes")
            .starts_with(ZAKURA_NODE_RECORD_SIG_DOMAIN));

        let mutations: Vec<
            Box<dyn FnOnce(&mut ZakuraNodeRecord, &mut DiscoveryRecordValidationContext)>,
        > = vec![
            Box::new(|record, _context| {
                record
                    .body
                    .direct_addrs
                    .push("45.33.30.1:8233".parse().unwrap())
            }),
            Box::new(|record, _context| record.body.services.push(service(9))),
            Box::new(|record, _context| record.body.zakura_protocol_min = 0),
            Box::new(|record, _context| record.body.zakura_protocol_max += 1),
            Box::new(|record, context| {
                record.body.network_id = ZakuraNetworkId::Mainnet;
                context.expected_network_id = ZakuraNetworkId::Mainnet;
            }),
            Box::new(|record, context| {
                record.body.chain_id[0] ^= 1;
                context.expected_chain_id = record.body.chain_id;
            }),
            Box::new(|record, _context| record.body.sequence += 1),
            Box::new(|record, _context| record.body.expires_at_unix_secs += 1),
        ];

        for mutate in mutations {
            let mut mutated = record.clone();
            let mut context = context();
            mutate(&mut mutated, &mut context);
            assert!(matches!(
                mutated.verify(&context),
                Err(DiscoveryRecordError::InvalidSignature)
            ));
        }

        let other_secret = secret_key();
        let mut mutated = record;
        mutated.body.node_id = other_secret.public();
        assert!(matches!(
            mutated.verify(&context()),
            Err(DiscoveryRecordError::InvalidSignature)
        ));
    }

    /// F-61: a node id with a torsion component signs a record that the
    /// cofactored equation accepts, but TLS can never authenticate that node id.
    #[test]
    fn record_from_mixed_order_node_id_is_rejected() {
        use curve25519_dalek::{
            constants::ED25519_BASEPOINT_POINT, edwards::CompressedEdwardsY, Scalar,
        };
        use sha2::{Digest as _, Sha512};
        use zakura_chain::primitives::ed25519::VerificationKey;

        /// A point of order 8.
        const ORDER_8: [u8; 32] = [
            0xc7, 0x17, 0x6a, 0x70, 0x3d, 0x4d, 0xd8, 0x4f, 0xba, 0x3c, 0x0b, 0x76, 0x0d, 0x10,
            0x67, 0x0f, 0x2a, 0x20, 0x53, 0xfa, 0x2c, 0x39, 0xcc, 0xc6, 0x4e, 0xc7, 0xfd, 0x77,
            0x92, 0xac, 0x03, 0x7a,
        ];

        // Derive the honest secret scalar `a` as RFC 8032 does.
        let secret_key = NodeSecretKey::from_bytes(&[61; 32]);
        let expanded = Sha512::digest(secret_key.to_bytes());
        let mut a_bytes = [0u8; 32];
        a_bytes.copy_from_slice(&expanded[..32]);
        a_bytes[0] &= 248;
        a_bytes[31] &= 127;
        a_bytes[31] |= 64;
        let a = Scalar::from_bytes_mod_order(a_bytes);

        // The mixed-order key is A' = aB + T for a point T of order 8.
        let torsion = CompressedEdwardsY(ORDER_8)
            .decompress()
            .expect("the order-8 constant is a curve point");
        let mixed_bytes = (a * ED25519_BASEPOINT_POINT + torsion)
            .compress()
            .to_bytes();
        let mixed = NodeId::from_bytes(&mixed_bytes).expect("a mixed-order key parses");

        let honest =
            ZakuraNodeRecord::sign(body(&secret_key), &secret_key).expect("test record signs");
        let mut body = body(&secret_key);
        body.node_id = mixed;
        let message = body
            .encode_for_signature()
            .expect("test record body encodes");

        // Sign with challenge H(R || A' || M). The cofactored equation
        // multiplies T away, so the signature verifies under A'.
        let r = Scalar::from_bytes_mod_order([3; 32]);
        let r_point = (r * ED25519_BASEPOINT_POINT).compress().to_bytes();
        let challenge: [u8; 64] = Sha512::new()
            .chain_update(r_point)
            .chain_update(mixed_bytes)
            .chain_update(&message)
            .finalize()
            .into();
        let s = r + Scalar::from_bytes_mod_order_wide(&challenge) * a;
        let mut signature_bytes = [0u8; 64];
        signature_bytes[..32].copy_from_slice(&r_point);
        signature_bytes[32..].copy_from_slice(s.as_bytes());
        let signature = Signature::from_bytes(&signature_bytes);

        VerificationKey::try_from(mixed_bytes)
            .expect("a mixed-order key is a valid cofactored verification key")
            .verify(&signature, &message)
            .expect("the cofactored equation accepts the mixed-order signature");

        let record = ZakuraNodeRecord { body, signature };
        assert!(matches!(
            record.verify(&context()),
            Err(DiscoveryRecordError::InvalidSignature)
        ));

        // Control: the same secret key signs a record for its honest node id,
        // and that record verifies.
        assert!(honest.verify(&context()).is_ok());
    }

    /// A record's author checks run before its expiry, so an expired or
    /// incompatible record with a bad signature fails as a bad signature.
    #[test]
    fn bad_signature_outranks_expiry_and_protocol() {
        let secret_key = secret_key();
        let mut expired = body(&secret_key);
        expired.expires_at_unix_secs = NOW - 3_600;
        let mut expired = ZakuraNodeRecord::sign(expired, &secret_key).expect("test record signs");
        assert!(matches!(
            expired.verify(&context()),
            Err(DiscoveryRecordError::Expired)
        ));
        expired.body.sequence += 1;
        assert!(matches!(
            expired.verify(&context()),
            Err(DiscoveryRecordError::InvalidSignature)
        ));

        let mut incompatible = body(&secret_key);
        incompatible.zakura_protocol_min = u16::MAX;
        incompatible.zakura_protocol_max = u16::MAX;
        let mut incompatible =
            ZakuraNodeRecord::sign(incompatible, &secret_key).expect("test record signs");
        assert!(matches!(
            incompatible.verify(&context()),
            Err(DiscoveryRecordError::IncompatibleProtocol)
        ));
        incompatible.body.sequence += 1;
        assert!(matches!(
            incompatible.verify(&context()),
            Err(DiscoveryRecordError::InvalidSignature)
        ));
    }

    /// A node that reset its sequence recovers once its stored record expires.
    #[test]
    fn expired_record_no_longer_orders_its_node() {
        let secret_key = secret_key();
        let node_id = secret_key.public();
        let mut book = ZakuraDiscoveryBook::default();
        let mut stored = body(&secret_key);
        stored.sequence = 10;
        stored.expires_at_unix_secs = NOW + 60;
        let stored = ZakuraNodeRecord::sign(stored, &secret_key).expect("test record signs");
        book.import_record(stored, Some(node_id), NOW, &context())
            .expect("first record imports");

        let mut reset = body(&secret_key);
        reset.sequence = 1;
        reset.expires_at_unix_secs = NOW + 3_600;
        let reset = ZakuraNodeRecord::sign(reset, &secret_key).expect("test record signs");
        assert_eq!(
            book.import_record(reset.clone(), Some(node_id), NOW, &context())
                .expect("an older record is ignored, not rejected"),
            ImportOutcome::IgnoredOlder
        );

        let later = NOW + 120;
        let later_context = DiscoveryRecordValidationContext {
            current_unix_secs: later,
            ..context()
        };
        assert_eq!(
            book.import_record(reset.clone(), Some(node_id), later, &later_context)
                .expect("the reset record imports after the stored one expires"),
            ImportOutcome::Updated
        );
        assert_eq!(
            book.get(&node_id).map(ZakuraDiscoveryEntry::record),
            Some(&reset)
        );
    }

    #[test]
    fn signature_fails_with_wrong_domain_or_record_format_version() {
        let secret_key = secret_key();
        let body = body(&secret_key);

        let wrong_domain = sign_record_with_prefix(
            body.clone(),
            &secret_key,
            b"zakura-other-record-v1",
            ZAKURA_NODE_RECORD_FORMAT_VERSION,
        );
        assert!(matches!(
            wrong_domain.verify(&context()),
            Err(DiscoveryRecordError::InvalidSignature)
        ));

        let wrong_version = sign_record_with_prefix(
            body,
            &secret_key,
            ZAKURA_NODE_RECORD_SIG_DOMAIN,
            ZAKURA_NODE_RECORD_FORMAT_VERSION + 1,
        );
        assert!(matches!(
            wrong_version.verify(&context()),
            Err(DiscoveryRecordError::InvalidSignature)
        ));
    }

    #[test]
    fn discovery_network_id_decode_matches_handshake_wire_codes() {
        for network_id in [
            ZakuraNetworkId::Mainnet,
            ZakuraNetworkId::Testnet,
            ZakuraNetworkId::Regtest,
            ZakuraNetworkId::Configured,
        ] {
            assert_eq!(
                network_id_from_code(network_id.code()).expect("network id code is valid"),
                network_id
            );
        }
    }

    #[test]
    fn record_validation_rejects_expired_wrong_network_wrong_chain_and_protocol() {
        let record = signed_record();

        let secret_key = secret_key();
        let mut expired = body(&secret_key);
        expired.expires_at_unix_secs = NOW - context().clock_skew_tolerance.as_secs() - 1;
        let expired = ZakuraNodeRecord::sign(expired, &secret_key).expect("test record signs");
        assert!(matches!(
            expired.verify(&context()),
            Err(DiscoveryRecordError::Expired)
        ));

        let mut wrong_network_context = context();
        wrong_network_context.expected_network_id = ZakuraNetworkId::Mainnet;
        assert!(matches!(
            record.verify(&wrong_network_context),
            Err(DiscoveryRecordError::WrongNetwork)
        ));

        let mut wrong_chain_context = context();
        wrong_chain_context.expected_chain_id[0] ^= 1;
        assert!(matches!(
            record.verify(&wrong_chain_context),
            Err(DiscoveryRecordError::WrongChain)
        ));

        let mut incompatible_context = context();
        incompatible_context.supported_protocol_min = 1;
        incompatible_context.supported_protocol_max = 1;
        assert!(matches!(
            record.verify(&incompatible_context),
            Err(DiscoveryRecordError::IncompatibleProtocol)
        ));
    }

    /// A record whose protocol range shares one version with ours imports,
    /// so nodes on either side of a protocol break still discover each other.
    /// A disjoint range on either side never reaches the dialer.
    #[test]
    fn record_protocol_range_must_overlap_ours() {
        let secret_key = secret_key();
        let mut local = context();
        local.supported_protocol_min = 3;
        local.supported_protocol_max = 5;
        let range = |min, max| {
            let mut body = body(&secret_key);
            body.zakura_protocol_min = min;
            body.zakura_protocol_max = max;
            ZakuraNodeRecord::sign(body, &secret_key)
                .expect("test record signs")
                .verify(&local)
        };

        for (min, max) in [(1, 3), (5, 9), (4, 4), (1, 9)] {
            assert!(range(min, max).is_ok(), "{min}..={max} overlaps 3..=5");
        }
        for (min, max) in [(1, 2), (6, 9)] {
            assert!(
                matches!(
                    range(min, max),
                    Err(DiscoveryRecordError::IncompatibleProtocol)
                ),
                "{min}..={max} must not import"
            );
        }
    }

    #[test]
    fn record_bounds_reject_too_many_addresses_and_services() {
        let secret_key = secret_key();
        let mut too_many_addrs = body(&secret_key);
        too_many_addrs.direct_addrs =
            vec!["45.33.30.1:8233".parse().unwrap(); MAX_DIRECT_ADDRS_PER_RECORD + 1];
        assert!(ZakuraNodeRecord::sign(too_many_addrs, &secret_key).is_err());

        let mut too_many_services = body(&secret_key);
        too_many_services.services = (0..=MAX_SERVICES_PER_RECORD).map(service).collect();
        assert!(ZakuraNodeRecord::sign(too_many_services, &secret_key).is_err());
    }

    #[test]
    fn response_and_query_bounds_are_enforced() {
        let records = vec![signed_record(); MAX_DISCOVERY_RECORDS_PER_RESPONSE + 1];
        assert!(DiscoveryMessage::Peers {
            records: records.clone()
        }
        .encode()
        .is_err());

        let wanted_services = (0..=MAX_SERVICES_PER_RECORD).map(service).collect();
        assert!(DiscoveryMessage::GetPeers(GetPeers {
            limit: 1,
            wanted_services,
            exclude_node_ids: Vec::new(),
        })
        .encode()
        .is_err());

        let wanted_services = (0..=MAX_GET_SERVICES_WANTED).map(service).collect();
        assert!(
            DiscoveryMessage::GetServices(GetServices { wanted_services })
                .encode()
                .is_err()
        );

        assert!(DiscoveryMessage::GetPeers(GetPeers {
            limit: u16::try_from(MAX_DISCOVERY_RECORDS_PER_RESPONSE + 1)
                .expect("test limit fits in u16"),
            wanted_services: Vec::new(),
            exclude_node_ids: Vec::new(),
        })
        .encode()
        .is_err());

        let too_many_summaries = vec![
            ServiceSummaryEnvelope {
                service_id: service(1),
                summary_tag: 999,
                summary_bytes: Vec::new(),
            };
            MAX_SERVICE_SUMMARIES_PER_RESPONSE + 1
        ];
        assert!(services_message(too_many_summaries).encode().is_err());

        let oversized_summary = ServiceSummaryEnvelope {
            service_id: service(1),
            summary_tag: 999,
            summary_bytes: vec![0; MAX_SERVICE_SUMMARY_BYTES + 1],
        };
        assert!(services_message(vec![oversized_summary]).encode().is_err());

        let large_summary = ServiceSummaryEnvelope {
            service_id: service(1),
            summary_tag: 999,
            summary_bytes: vec![0; MAX_SERVICE_SUMMARY_BYTES],
        };
        let total_oversized = vec![large_summary; 16];
        assert!(matches!(
            services_message(total_oversized).encode(),
            Err(WireError::CountOutOfRange { count: 16, .. })
        ));
    }

    /// Retiring header-sync v6 must not break discovery with peers that have not
    /// upgraded. A pre-upgrade peer still sends the retired tag with the retired
    /// service id; that pairing must stay wire-valid, or every `Services` exchange
    /// with such a peer fails as a protocol violation and takes discovery down with
    /// it. The payload remains a valid advisory summary for peers that independently
    /// advertise the current header-sync service.
    #[test]
    fn retired_header_sync_summary_from_a_pre_upgrade_peer_decodes_as_advisory() {
        let summary = header_summary();
        let retired = retired_header_sync_envelope(&summary);

        let encoded = services_message(vec![retired.clone()])
            .encode()
            .expect("a pre-upgrade peer's header-sync summary must still encode");
        assert!(
            DiscoveryMessage::decode(&encoded).is_ok(),
            "a pre-upgrade peer's Services message must not be a wire protocol violation"
        );

        assert_eq!(
            retired
                .decode_header_sync()
                .expect("the retired payload is well-formed"),
            Some(summary),
            "a retired-generation summary remains an advisory payload"
        );
    }

    #[test]
    fn known_summary_tags_must_match_their_service_ids() {
        let mismatched_header = ServiceSummaryEnvelope {
            service_id: ZakuraServiceId::discovery(),
            summary_tag: SUMMARY_TAG_HEADER_SYNC,
            summary_bytes: {
                let mut bytes = Vec::new();
                encode_header_sync_summary(&header_summary(), &mut bytes)
                    .expect("test header summary encodes");
                bytes
            },
        };
        assert!(matches!(
            services_message(vec![mismatched_header.clone()]).encode(),
            Err(WireError::OutOfRange("summary tag service"))
        ));
        let mut mismatched_header_wire = vec![MSG_DISCOVERY_SERVICES];
        mismatched_header_wire
            .write_all(secret_key().public().as_bytes())
            .expect("test node id writes");
        mismatched_header_wire
            .write_u64::<LittleEndian>(NOW + 30)
            .expect("test expiry writes");
        mismatched_header_wire
            .write_u16::<LittleEndian>(1)
            .expect("test summary count writes");
        super::super::wire::write_service_id(
            &ZakuraServiceId::discovery(),
            &mut mismatched_header_wire,
        );
        mismatched_header_wire
            .write_u16::<LittleEndian>(SUMMARY_TAG_HEADER_SYNC)
            .expect("test summary tag writes");
        mismatched_header_wire
            .write_u16::<LittleEndian>(
                u16::try_from(mismatched_header.summary_bytes.len())
                    .expect("test summary length fits in u16"),
            )
            .expect("test summary length writes");
        mismatched_header_wire
            .write_all(&mismatched_header.summary_bytes)
            .expect("test summary payload writes");
        assert!(matches!(
            DiscoveryMessage::decode(&mismatched_header_wire),
            Err(WireError::OutOfRange("summary tag service"))
        ));

        let mismatched_discovery = ServiceSummaryEnvelope {
            service_id: ZakuraServiceId::header_sync(),
            summary_tag: SUMMARY_TAG_DISCOVERY_V1,
            summary_bytes: {
                let mut bytes = Vec::new();
                encode_discovery_summary(&discovery_summary(), &mut bytes)
                    .expect("test discovery summary encodes");
                bytes
            },
        };
        assert!(matches!(
            services_message(vec![mismatched_discovery]).encode(),
            Err(WireError::OutOfRange("summary tag service"))
        ));

        let mismatched_block = ServiceSummaryEnvelope {
            service_id: ZakuraServiceId::header_sync(),
            summary_tag: SUMMARY_TAG_BLOCK_SYNC_V1,
            summary_bytes: {
                let mut bytes = Vec::new();
                encode_block_sync_summary(&block_sync_summary(), &mut bytes)
                    .expect("test block summary encodes");
                bytes
            },
        };
        assert!(matches!(
            services_message(vec![mismatched_block]).encode(),
            Err(WireError::OutOfRange("summary tag service"))
        ));
    }

    #[test]
    fn decode_rejects_too_many_records_services_and_excluded_ids_before_allocation() {
        fn too_many(count: usize) -> u16 {
            u16::try_from(count + 1).expect("test count fits in u16")
        }

        let mut peers = vec![MSG_DISCOVERY_PEERS];
        peers
            .write_u16::<LittleEndian>(too_many(MAX_DISCOVERY_RECORDS_PER_RESPONSE))
            .unwrap();
        assert!(matches!(
            DiscoveryMessage::decode(&peers),
            Err(WireError::CountOutOfRange { .. })
        ));

        let mut query = vec![MSG_DISCOVERY_GET_PEERS];
        query.write_u16::<LittleEndian>(1).unwrap();
        query
            .write_u16::<LittleEndian>(too_many(MAX_GET_PEERS_WANTED_SERVICES))
            .unwrap();
        query.write_u16::<LittleEndian>(0).unwrap();
        assert!(matches!(
            DiscoveryMessage::decode(&query),
            Err(WireError::CountOutOfRange { .. })
        ));

        let mut query = vec![MSG_DISCOVERY_GET_PEERS];
        query.write_u16::<LittleEndian>(1).unwrap();
        query.write_u16::<LittleEndian>(0).unwrap();
        query
            .write_u16::<LittleEndian>(too_many(MAX_DISCOVERY_EXCLUDED_NODE_IDS))
            .unwrap();
        assert!(matches!(
            DiscoveryMessage::decode(&query),
            Err(WireError::CountOutOfRange { .. })
        ));

        let mut service_query = vec![MSG_DISCOVERY_GET_SERVICES];
        service_query
            .write_u16::<LittleEndian>(too_many(MAX_GET_SERVICES_WANTED))
            .unwrap();
        assert!(matches!(
            DiscoveryMessage::decode(&service_query),
            Err(WireError::CountOutOfRange { .. })
        ));

        let mut service_response = vec![MSG_DISCOVERY_SERVICES];
        service_response
            .write_all(secret_key().public().as_bytes())
            .unwrap();
        service_response
            .write_u64::<LittleEndian>(NOW + 30)
            .unwrap();
        service_response
            .write_u16::<LittleEndian>(too_many(MAX_SERVICE_SUMMARIES_PER_RESPONSE))
            .unwrap();
        assert!(matches!(
            DiscoveryMessage::decode(&service_response),
            Err(WireError::CountOutOfRange { .. })
        ));

        // A count within its cap still needs the bytes to back it.
        let mut peers = vec![MSG_DISCOVERY_PEERS];
        peers.write_u16::<LittleEndian>(2).unwrap();
        peers.write_all(&[0; 200]).unwrap();
        assert!(matches!(
            DiscoveryMessage::decode(&peers),
            Err(WireError::CountExceedsPayload { count: 2, .. })
        ));

        let mut oversized_summary = vec![MSG_DISCOVERY_SERVICES];
        oversized_summary
            .write_all(secret_key().public().as_bytes())
            .unwrap();
        oversized_summary
            .write_u64::<LittleEndian>(NOW + 30)
            .unwrap();
        oversized_summary.write_u16::<LittleEndian>(1).unwrap();
        super::super::wire::write_service_id(&service(1), &mut oversized_summary);
        oversized_summary.write_u16::<LittleEndian>(999).unwrap();
        oversized_summary
            .write_u16::<LittleEndian>(
                u16::try_from(MAX_SERVICE_SUMMARY_BYTES + 1).expect("summary cap fits in u16"),
            )
            .unwrap();
        assert!(matches!(
            DiscoveryMessage::decode(&oversized_summary),
            Err(WireError::OutOfRange("service summary length"))
        ));
    }

    #[test]
    fn peers_response_cannot_encode_live_summaries_structurally() {
        let message = DiscoveryMessage::Peers {
            records: vec![signed_record()],
        };
        let encoded = message.encode().expect("peers message encodes");
        let decoded = DiscoveryMessage::decode(&encoded).expect("peers message decodes");

        match decoded {
            DiscoveryMessage::Peers { records } => assert_eq!(records.len(), 1),
            DiscoveryMessage::Services(_) => panic!("PEERS decoded as SERVICES"),
            _ => panic!("unexpected discovery message variant"),
        }
    }

    #[test]
    fn decode_rejects_malformed_record_body_counts_before_allocation() {
        fn hello_with_body(mut body_bytes: Vec<u8>, record: &ZakuraNodeRecord) -> Vec<u8> {
            body_bytes.resize(super::super::wire::RECORD_BODY_FIXED_BYTES, 0);
            let mut encoded = vec![MSG_DISCOVERY_HELLO];
            encoded
                .write_u32::<LittleEndian>(
                    u32::try_from(body_bytes.len()).expect("test body length fits in u32"),
                )
                .unwrap();
            encoded.write_all(&body_bytes).unwrap();
            encoded.write_all(&record.signature.to_bytes()).unwrap();
            encoded
        }

        let record = signed_record();
        let mut body_bytes = Vec::new();
        body_bytes
            .write_all(record.body.node_id.as_bytes())
            .unwrap();
        body_bytes
            .write_u16::<LittleEndian>(
                u16::try_from(MAX_DIRECT_ADDRS_PER_RECORD + 1)
                    .expect("test address count fits in u16"),
            )
            .unwrap();
        assert!(matches!(
            DiscoveryMessage::decode(&hello_with_body(body_bytes, &record)),
            Err(WireError::CountOutOfRange { .. })
        ));

        let mut body_bytes = Vec::new();
        body_bytes
            .write_all(record.body.node_id.as_bytes())
            .unwrap();
        body_bytes.write_u16::<LittleEndian>(0).unwrap();
        body_bytes
            .write_u16::<LittleEndian>(
                u16::try_from(MAX_SERVICES_PER_RECORD + 1).expect("test service count fits in u16"),
            )
            .unwrap();
        assert!(matches!(
            DiscoveryMessage::decode(&hello_with_body(body_bytes, &record)),
            Err(WireError::CountOutOfRange { .. })
        ));
    }

    #[test]
    fn signature_fails_when_signed_vectors_are_reordered() {
        let record = signed_record();
        let signed_bytes = record
            .body
            .encode_for_signature()
            .expect("record pre-image encodes");

        let mut reordered_services = record.clone();
        reordered_services.body.services.swap(0, 1);
        assert_ne!(
            reordered_services
                .body
                .encode_for_signature()
                .expect("reordered services pre-image encodes"),
            signed_bytes
        );
        assert!(matches!(
            reordered_services.verify(&context()),
            Err(DiscoveryRecordError::InvalidSignature)
        ));

        let mut reordered_addrs = record;
        reordered_addrs.body.direct_addrs.swap(0, 1);
        assert_ne!(
            reordered_addrs
                .body
                .encode_for_signature()
                .expect("reordered addrs pre-image encodes"),
            signed_bytes
        );
        assert!(matches!(
            reordered_addrs.verify(&context()),
            Err(DiscoveryRecordError::InvalidSignature)
        ));
    }

    #[test]
    fn far_future_expiry_and_clock_skew_edges_are_enforced() {
        let mut record = signed_record();

        record.body.expires_at_unix_secs =
            NOW + context().max_record_ttl.as_secs() + context().clock_skew_tolerance.as_secs();
        let signing_secret = secret_key();
        record = ZakuraNodeRecord::sign(
            ZakuraNodeRecordBody {
                node_id: signing_secret.public(),
                ..record.body
            },
            &signing_secret,
        )
        .expect("record signs");
        record.verify(&context()).expect("skew edge is accepted");

        let signing_secret = secret_key();
        let mut too_far = body(&signing_secret);
        too_far.expires_at_unix_secs =
            NOW + context().max_record_ttl.as_secs() + context().clock_skew_tolerance.as_secs() + 1;
        let too_far = ZakuraNodeRecord::sign(too_far, &signing_secret).expect("record signs");
        assert!(matches!(
            too_far.verify(&context()),
            Err(DiscoveryRecordError::FarFutureExpiry)
        ));

        let signing_secret = secret_key();
        let mut just_expired = body(&signing_secret);
        just_expired.expires_at_unix_secs = NOW - context().clock_skew_tolerance.as_secs();
        let just_expired =
            ZakuraNodeRecord::sign(just_expired, &signing_secret).expect("record signs");
        just_expired
            .verify(&context())
            .expect("just expired within skew is accepted");
    }

    /// The signed extension area is relayed verbatim: it survives decode and re-encode,
    /// the signature still verifies, and the extension bytes are covered by it.
    #[test]
    fn node_record_extensions_round_trip_and_verify() {
        let secret_key = secret_key();
        let mut body = body(&secret_key);
        // An unknown TLV: type 0x7001, length 3.
        body.extensions = vec![0x01, 0x70, 0x03, 0x00, 0xaa, 0xbb, 0xcc];
        let record = ZakuraNodeRecord::sign(body, &secret_key).expect("record signs");

        let encoded = DiscoveryMessage::Hello {
            record: record.clone(),
        }
        .encode()
        .expect("message encodes");
        let DiscoveryMessage::Hello { record: decoded } =
            DiscoveryMessage::decode(&encoded).expect("message decodes")
        else {
            panic!("expected a hello message");
        };
        assert_eq!(decoded, record);
        decoded
            .verify(&context())
            .expect("extended record verifies");
        assert_eq!(
            DiscoveryMessage::Hello {
                record: decoded.clone()
            }
            .encode()
            .expect("message re-encodes"),
            encoded,
            "a relayed record is byte-identical"
        );

        assert_eq!(record_len(&record), encoded.len() - 1);

        let mut mutated = decoded;
        mutated.body.extensions[4] ^= 1;
        assert!(matches!(
            mutated.verify(&context()),
            Err(DiscoveryRecordError::InvalidSignature)
        ));
    }

    /// A record body this release cannot decode fails the whole `Peers`
    /// message. New record contents go only in the extension area, so a
    /// conformant relay never sends such a body.
    #[test]
    fn peers_message_rejects_undecodable_record_bodies() {
        fn push_record(record: &ZakuraNodeRecord, out: &mut Vec<u8>) {
            let mut body = Vec::new();
            write_record_body(&record.body, &mut body);
            out.write_u32::<LittleEndian>(u32::try_from(body.len()).unwrap())
                .unwrap();
            out.write_all(&body).unwrap();
            out.write_all(&record.signature.to_bytes()).unwrap();
        }

        let first = signed_record_with(1, service(1), test_addr(1));
        let mut valid = vec![MSG_DISCOVERY_PEERS];
        valid.write_u16::<LittleEndian>(1).unwrap();
        push_record(&first, &mut valid);
        assert_eq!(
            DiscoveryMessage::decode(&valid).expect("message decodes"),
            DiscoveryMessage::Peers {
                records: vec![first.clone()],
            }
        );

        // A record with an address family this release does not know.
        let unknown_family = signed_record_with(3, service(3), test_addr(3));
        let mut unknown_body = Vec::new();
        write_record_body(&unknown_family.body, &mut unknown_body);
        let family_offset = NODE_ID_BYTES + 2;
        assert_eq!(unknown_body[family_offset], 4);
        unknown_body[family_offset] = 9;

        let mut encoded = vec![MSG_DISCOVERY_PEERS];
        encoded.write_u16::<LittleEndian>(2).unwrap();
        push_record(&first, &mut encoded);
        encoded
            .write_u32::<LittleEndian>(u32::try_from(unknown_body.len()).unwrap())
            .unwrap();
        encoded.write_all(&unknown_body).unwrap();
        encoded
            .write_all(&unknown_family.signature.to_bytes())
            .unwrap();
        assert!(matches!(
            DiscoveryMessage::decode(&encoded),
            Err(WireError::OutOfRange("address family"))
        ));

        // A truncated signature fails the whole message.
        let mut truncated = valid.clone();
        truncated.pop();
        assert!(matches!(
            DiscoveryMessage::decode(&truncated),
            Err(WireError::Truncated("record signature"))
        ));

        // An oversize body length fails before the reader takes the body.
        let mut oversize = vec![MSG_DISCOVERY_PEERS];
        oversize.write_u16::<LittleEndian>(1).unwrap();
        oversize
            .write_u32::<LittleEndian>(u32::try_from(MAX_NODE_RECORD_BODY_BYTES + 1).unwrap())
            .unwrap();
        oversize.resize(1 + 2 + super::super::wire::NODE_RECORD_MIN_BYTES, 0);
        assert!(matches!(
            DiscoveryMessage::decode(&oversize),
            Err(WireError::OutOfRange("record body length"))
        ));
    }

    /// A full sample of maximal records fills one `Peers` message exactly.
    #[test]
    fn sample_peers_fits_one_peers_message() {
        let mut book = ZakuraDiscoveryBook::default();
        for index in 1..=40u8 {
            let secret_key = secret_key();
            let mut body = body(&secret_key);
            body.direct_addrs = vec![test_addr(index)];
            let mut body_bytes = Vec::new();
            write_record_body(&body, &mut body_bytes);
            body.extensions = vec![index; MAX_NODE_RECORD_BODY_BYTES - body_bytes.len()];
            let record = ZakuraNodeRecord::sign(body, &secret_key).expect("record signs");
            assert_eq!(record_len(&record), MAX_NODE_RECORD_BYTES);
            book.import_record(record, Some(secret_key.public()), NOW, &context())
                .expect("first-party record imports");
        }

        let sample = book.sample_peers(
            MAX_DISCOVERY_RECORDS_PER_RESPONSE,
            &[],
            &[],
            NOW,
            test_requester(),
        );
        assert_eq!(sample.len(), MAX_DISCOVERY_RECORDS_PER_RESPONSE);
        let encoded = DiscoveryMessage::Peers { records: sample }
            .encode()
            .expect("the sample fits one Peers message");
        assert_eq!(encoded.len() - 1, super::super::wire::PEERS_MAX_BYTES);
    }

    #[test]
    fn decode_rejects_trailing_and_unknown_bytes() {
        let mut encoded = DiscoveryMessage::Hello {
            record: signed_record(),
        }
        .encode()
        .expect("message encodes");
        encoded.push(0);
        assert!(matches!(
            DiscoveryMessage::decode(&encoded),
            Err(WireError::TrailingBytes)
        ));

        assert!(matches!(
            DiscoveryMessage::decode(&[99]),
            Err(WireError::UnknownMessageType(99))
        ));
    }

    /// A node with more connected peers than the exclusion cap still sends an
    /// exclusion list the wire accepts: capped, unique, and in byte order.
    #[tokio::test]
    async fn peer_sample_exclusions_always_encode() {
        let connected: Vec<ZakuraPeerId> = (0..MAX_DISCOVERY_EXCLUDED_NODE_IDS + 10)
            .map(|_| peer_id_for(secret_key().public()))
            .collect();
        let (_connected_tx, connected_rx) = watch::channel(connected);
        let handle = discovery_handle_with_connected(connected_rx);

        let exclude_node_ids = handle.peer_sample_exclusions().await;
        assert_eq!(exclude_node_ids.len(), MAX_DISCOVERY_EXCLUDED_NODE_IDS);
        assert!(exclude_node_ids.windows(2).all(|pair| pair[0] < pair[1]));
        DiscoveryMessage::GetPeers(GetPeers {
            limit: 1,
            wanted_services: Vec::new(),
            exclude_node_ids,
        })
        .encode()
        .expect("the local exclusion list encodes");
    }

    #[test]
    fn discovery_book_imports_valid_record() {
        let mut book = ZakuraDiscoveryBook::default();
        let source = secret_key().public();
        let record = signed_record();
        let node_id = record.body.node_id;

        assert_eq!(
            book.import_record(record.clone(), Some(source), NOW, &context())
                .expect("valid record imports"),
            ImportOutcome::Added
        );

        let entry = book.get(&node_id).expect("entry was inserted");
        assert_eq!(entry.record(), &record);
        assert_eq!(entry.source(), Some(source));
        assert_eq!(entry.last_seen(), NOW);
        assert!(!entry.is_static());
    }

    #[test]
    fn discovery_book_rejects_invalid_signature_expired_wrong_network_and_wrong_chain() {
        let mut book = ZakuraDiscoveryBook::default();

        let mut invalid_signature = signed_record();
        invalid_signature.body.sequence += 1;
        assert!(matches!(
            book.import_record(invalid_signature, None, NOW, &context()),
            Err(DiscoveryBookError::Record(
                DiscoveryRecordError::InvalidSignature
            ))
        ));

        let signing_secret = secret_key();
        let mut expired = body(&signing_secret);
        expired.expires_at_unix_secs = NOW - context().clock_skew_tolerance.as_secs() - 1;
        let expired = ZakuraNodeRecord::sign(expired, &signing_secret).expect("record signs");
        assert!(matches!(
            book.import_record(expired, None, NOW, &context()),
            Err(DiscoveryBookError::Record(DiscoveryRecordError::Expired))
        ));

        let mut wrong_network_context = context();
        wrong_network_context.expected_network_id = ZakuraNetworkId::Mainnet;
        assert!(matches!(
            book.import_record(signed_record(), None, NOW, &wrong_network_context),
            Err(DiscoveryBookError::Record(
                DiscoveryRecordError::WrongNetwork
            ))
        ));

        let mut wrong_chain_context = context();
        wrong_chain_context.expected_chain_id[0] ^= 1;
        assert!(matches!(
            book.import_record(signed_record(), None, NOW, &wrong_chain_context),
            Err(DiscoveryBookError::Record(DiscoveryRecordError::WrongChain))
        ));
    }

    #[test]
    fn discovery_book_rejects_self_record_no_usable_address_and_far_future_expiry() {
        let local_secret = secret_key();
        let local_record = record_with_secret(&local_secret, 1, service(1));
        let mut book = ZakuraDiscoveryBook::with_local_node_id(
            ZakuraDiscoveryBookLimits::default(),
            local_secret.public(),
        );

        assert!(matches!(
            book.import_record(local_record.clone(), None, NOW, &context()),
            Err(DiscoveryBookError::SelfRecord)
        ));
        assert!(book.get(&local_secret.public()).is_none());

        let signing_secret = secret_key();
        let mut no_addr = body(&signing_secret);
        no_addr.direct_addrs = Vec::new();
        let no_addr = ZakuraNodeRecord::sign(no_addr, &signing_secret).expect("record signs");
        assert!(matches!(
            book.import_record(no_addr, None, NOW, &context()),
            Err(DiscoveryBookError::NoUsableDirectAddress)
        ));

        let signing_secret = secret_key();
        let mut too_far = body(&signing_secret);
        too_far.expires_at_unix_secs =
            NOW + context().max_record_ttl.as_secs() + context().clock_skew_tolerance.as_secs() + 1;
        let too_far = ZakuraNodeRecord::sign(too_far, &signing_secret).expect("record signs");
        assert!(matches!(
            book.import_record(too_far, None, NOW, &context()),
            Err(DiscoveryBookError::Record(
                DiscoveryRecordError::FarFutureExpiry
            ))
        ));
    }

    #[test]
    fn discovery_book_rejects_non_dialable_direct_addresses() {
        let bad_addrs = [
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8233),
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 8233),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8233),
            SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 8233),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)), 8233),
            SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0xff02, 0, 0, 0, 0, 0, 0, 1)), 8233),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::BROADCAST), 8233),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(169, 254, 1, 1)), 8233),
            SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1)), 8233),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(45, 33, 30, 20)), 0),
        ];

        for (index, bad_addr) in bad_addrs.into_iter().enumerate() {
            let mut book = ZakuraDiscoveryBook::default();
            let record = signed_record_with_addrs(
                u64::try_from(index).expect("small test index fits in u64"),
                service(index),
                vec![test_addr(20), bad_addr],
            );

            assert!(matches!(
                book.import_record(record, None, NOW, &context()),
                Err(DiscoveryBookError::NonDialableDirectAddress { addr }) if addr == bad_addr
            ));
            assert!(book.is_empty());
        }
    }

    #[test]
    fn discovery_book_peer_import_rejects_private_and_internal_direct_addresses() {
        // A signed record only proves control of the node key, not ownership of the advertised
        // direct addresses. Untrusted gossip must therefore be restricted to globally routable
        // targets; otherwise an authenticated discovery peer could seed signed records pointing at
        // the honest node's private/internal network and turn the candidate dialer into an internal
        // SSRF/scanner. Private, shared, special-purpose, IPv4-embedded, NAT64 translation, and
        // unique/site-local ranges are not all covered by the standard loopback/link-local checks.
        let bad_addrs = [
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)), 8233),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 16, 5, 5)), 8233),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8233),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1)), 8233),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 1, 2, 3)), 8233),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 0, 1)), 8233),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(198, 18, 0, 1)), 8233),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(240, 0, 0, 1)), 8233),
            // RFC 5737 documentation ranges (TEST-NET-1/2/3) are reserved for
            // docs, never globally routable.
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 5)), 8233),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 5)), 8233),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 5)), 8233),
            // RFC 3849 IPv6 documentation range 2001:db8::/32.
            SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 5)),
                8233,
            ),
            SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 1)), 8233),
            SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(0xfd12, 0x3456, 0, 0, 0, 0, 0, 1)),
                8233,
            ),
            SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0x7f00, 1)),
                8233,
            ),
            SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0x0a00, 1)),
                8233,
            ),
            // 6to4 embeds 10.0.0.1 in bits 16..48.
            SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(0x2002, 0x0a00, 1, 0, 0, 0, 0, 0)),
                8233,
            ),
            // Teredo stores the client IPv4 address bitwise-inverted in the final 32 bits.
            SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(
                    0x2001, 0, 0xc000, 0x022d, 0, 0, 0xf5ff, 0xfffe,
                )),
                8233,
            ),
            SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0x0a00, 1)), 8233),
            SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(0x0064, 0xff9b, 0, 0, 0, 0, 0x0a00, 1)),
                8233,
            ),
            SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(0x0064, 0xff9b, 1, 0, 0, 0, 0x0a00, 1)),
                8233,
            ),
            SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0xfec0, 0, 0, 0, 0, 0, 0, 1)), 8233),
        ];

        for (index, bad_addr) in bad_addrs.into_iter().enumerate() {
            let mut book = ZakuraDiscoveryBook::default();
            // Pair the malicious target with a genuinely routable address to prove the whole
            // record is rejected, not just dropped down to its usable addresses.
            let record = signed_record_with_addrs(
                u64::try_from(index).expect("small test index fits in u64"),
                service(index),
                vec![test_addr(30), bad_addr],
            );

            assert!(
                matches!(
                    book.import_record(record, Some(secret_key().public()), NOW, &context()),
                    Err(DiscoveryBookError::NonDialableDirectAddress { addr }) if addr == bad_addr
                ),
                "untrusted gossip must reject internal direct address {bad_addr}"
            );
            assert!(book.is_empty());
        }

        // A genuinely globally routable gossip target still imports.
        let mut book = ZakuraDiscoveryBook::default();
        let public_record = signed_record_with_addrs(99, service(99), vec![test_addr(40)]);
        assert_eq!(
            book.import_record(public_record, Some(secret_key().public()), NOW, &context())
                .expect("public gossip record imports"),
            ImportOutcome::Added
        );

        // Trusted/static configuration is unchanged: operators may still configure private targets
        // for LAN/regtest deployments.
        let mut static_book = ZakuraDiscoveryBook::default();
        let private_static = signed_record_with_addrs(
            1,
            service(1),
            vec![SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
                8233,
            )],
        );
        assert_eq!(
            static_book
                .import_static_record(private_static, NOW, &context())
                .expect("configured static private record imports"),
            ImportOutcome::Added
        );
    }

    #[test]
    fn discovery_book_gossiped_public_third_party_record_is_dialable() {
        let mut book = ZakuraDiscoveryBook::default();
        let gossip_source = secret_key().public();
        let public_target = signed_record_with_addrs(
            1,
            service(1),
            vec![SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)),
                8233,
            )],
        );
        let target_id = public_target.body.node_id;

        assert_eq!(
            book.import_record(public_target.clone(), Some(gossip_source), NOW, &context())
                .expect("valid public third-party gossip record still imports"),
            ImportOutcome::Added
        );
        assert_eq!(
            book.get(&target_id).expect("entry is stored").record(),
            &public_target
        );

        assert_eq!(
            book.dial_candidates(
                10,
                &[service(1)],
                &[],
                DialCandidateExclusions {
                    connected_node_ids: &[],
                    in_flight_node_ids: &[],
                },
                NOW,
                (
                    DEFAULT_DISCOVERY_DIAL_BACKOFF_BASE,
                    DEFAULT_DISCOVERY_DIAL_BACKOFF_MAX,
                ),
                &mut StdRng::seed_from_u64(7),
            ),
            vec![candidate_for(&public_target, false)],
            "valid public gossip must expand discovery beyond directly connected peers"
        );

        // Dialability is a local hint only: until the record owner confirms first-party or a
        // local dial succeeds, the record must not be re-gossiped through peer samples.
        assert!(
            book.sample_peers(10, &[], &[], NOW, test_requester())
                .is_empty(),
            "unconfirmed third-party gossip must stay out of peer samples"
        );

        // A later first-party exchange updates the record's source metadata, keeps its
        // dialability, and confirms it for peer samples.
        assert_eq!(
            book.import_record(public_target.clone(), Some(target_id), NOW + 1, &context())
                .expect("first-party self-record confirmation imports"),
            ImportOutcome::MetadataUpdated
        );
        assert_eq!(
            book.sample_peers(10, &[], &[], NOW + 1, test_requester()),
            vec![public_target.clone()],
            "first-party confirmation makes the record sample-eligible"
        );
        assert_eq!(
            book.dial_candidates(
                10,
                &[service(1)],
                &[],
                DialCandidateExclusions {
                    connected_node_ids: &[],
                    in_flight_node_ids: &[],
                },
                NOW + 1,
                (
                    DEFAULT_DISCOVERY_DIAL_BACKOFF_BASE,
                    DEFAULT_DISCOVERY_DIAL_BACKOFF_MAX,
                ),
                &mut StdRng::seed_from_u64(7),
            ),
            vec![candidate_for(&public_target, false)]
        );
    }

    #[test]
    fn discovery_book_static_import_allows_loopback_direct_address() {
        let mut book = ZakuraDiscoveryBook::default();
        let loopback_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8233);
        let record = signed_record_with_addrs(1, service(1), vec![loopback_addr]);
        let node_id = record.body.node_id;

        assert_eq!(
            book.import_static_record(record.clone(), NOW, &context())
                .expect("configured static loopback record imports"),
            ImportOutcome::Added
        );

        let entry = book
            .get(&node_id)
            .expect("static loopback entry was stored");
        assert!(entry.is_static());
        assert_eq!(entry.record(), &record);
    }

    #[test]
    fn discovery_book_peer_import_still_rejects_loopback_direct_address() {
        let mut book = ZakuraDiscoveryBook::default();
        let loopback_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8233);
        let record = signed_record_with_addrs(1, service(1), vec![loopback_addr]);

        assert!(matches!(
            book.import_record(record, Some(secret_key().public()), NOW, &context()),
            Err(DiscoveryBookError::NonDialableDirectAddress { addr }) if addr == loopback_addr
        ));
        assert!(book.is_empty());
    }

    #[test]
    fn discovery_book_static_import_rejects_unspecified_and_port_zero_addresses() {
        let bad_addrs = [
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8233),
            SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 8233),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)), 8233),
            SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0xff02, 0, 0, 0, 0, 0, 0, 1)), 8233),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::BROADCAST), 8233),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        ];

        for (index, bad_addr) in bad_addrs.into_iter().enumerate() {
            let mut book = ZakuraDiscoveryBook::default();
            let record = signed_record_with_addrs(
                u64::try_from(index).expect("small test index fits in u64"),
                service(index),
                vec![bad_addr],
            );

            assert!(matches!(
                book.import_static_record(record, NOW, &context()),
                Err(DiscoveryBookError::NonDialableDirectAddress { addr }) if addr == bad_addr
            ));
            assert!(book.is_empty());
        }
    }

    #[test]
    fn discovery_book_inserts_static_bootstrap_candidate_without_gossiping_it() {
        let mut book = ZakuraDiscoveryBook::default();
        let node_id = secret_key().public();
        let loopback_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8233);
        let second_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8234);
        let node_addr = NodeAddr::with_addrs(node_id, [loopback_addr]);

        book.insert_static_candidate(node_addr, NOW)
            .expect("configured static loopback candidate imports");
        book.insert_static_candidate(NodeAddr::with_addrs(node_id, [second_addr]), NOW + 1)
            .expect("additional address for the same static identity imports");

        assert!(book
            .sample_peers(10, &[], &[], NOW, test_requester())
            .is_empty());
        let mut rng = StdRng::seed_from_u64(7);
        assert_eq!(
            book.dial_candidates(
                10,
                &[],
                &[],
                DialCandidateExclusions {
                    connected_node_ids: &[],
                    in_flight_node_ids: &[],
                },
                NOW,
                (
                    DEFAULT_DISCOVERY_DIAL_BACKOFF_BASE,
                    DEFAULT_DISCOVERY_DIAL_BACKOFF_MAX,
                ),
                &mut rng,
            ),
            vec![ZakuraDiscoveryDialCandidate {
                node_id,
                direct_addrs: vec![loopback_addr, second_addr],
                is_static: true,
            }]
        );
    }

    #[test]
    fn discovery_book_static_bootstrap_candidate_rejects_unusable_addresses() {
        let bad_addrs = [
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8233),
            SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 8233),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)), 8233),
            SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0xff02, 0, 0, 0, 0, 0, 0, 1)), 8233),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::BROADCAST), 8233),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        ];

        for bad_addr in bad_addrs {
            let mut book = ZakuraDiscoveryBook::default();
            let node_addr = NodeAddr::with_addrs(secret_key().public(), [bad_addr]);

            assert!(matches!(
                book.insert_static_candidate(node_addr, NOW),
                Err(DiscoveryBookError::NonDialableDirectAddress { addr }) if addr == bad_addr
            ));
            assert!(book.is_empty());
        }
    }

    #[test]
    fn discovery_book_sequence_rules_keep_newest_record() {
        let mut book = ZakuraDiscoveryBook::default();
        let secret = secret_key();
        let newer = record_with_secret(&secret, 10, service(1));
        let older = record_with_secret(&secret, 9, service(2));
        let equal = record_with_secret(&secret, 10, service(3));
        let node_id = newer.body.node_id;

        assert_eq!(
            book.import_record(newer.clone(), None, NOW, &context())
                .unwrap(),
            ImportOutcome::Added
        );
        assert_eq!(
            book.import_record(older, Some(secret_key().public()), NOW + 1, &context())
                .unwrap(),
            ImportOutcome::IgnoredOlder
        );
        assert_eq!(
            book.get(&node_id).unwrap().record().body.services,
            newer.body.services
        );

        let equal_source = secret_key().public();
        assert_eq!(
            book.import_record(equal, Some(equal_source), NOW + 2, &context())
                .unwrap(),
            ImportOutcome::MetadataUpdated
        );
        let entry = book.get(&node_id).unwrap();
        assert_eq!(entry.record().body.services, newer.body.services);
        assert_eq!(entry.source(), Some(equal_source));
        assert_eq!(entry.last_seen(), NOW + 2);

        let newest = record_with_secret(&secret, 11, service(4));
        assert_eq!(
            book.import_record(newest.clone(), None, NOW + 3, &context())
                .unwrap(),
            ImportOutcome::Updated
        );
        assert_eq!(book.get(&node_id).unwrap().record(), &newest);
    }

    #[test]
    fn discovery_book_total_capacity_and_eviction_order_are_enforced() {
        let mut book = small_book(2);

        let expired_secret = secret_key();
        let mut expired_body = body(&expired_secret);
        expired_body.sequence = 1;
        expired_body.expires_at_unix_secs = NOW - 1;
        let expired_context = DiscoveryRecordValidationContext {
            current_unix_secs: NOW - context().clock_skew_tolerance.as_secs(),
            ..context()
        };
        let expired = ZakuraNodeRecord::sign(expired_body, &expired_secret).expect("record signs");
        let expired_id = expired.body.node_id;
        book.import_record(
            expired,
            None,
            NOW - context().clock_skew_tolerance.as_secs(),
            &expired_context,
        )
        .expect("expired-later record imports before eviction time");

        let failed = signed_record_with(2, service(2), test_addr(2));
        let failed_id = failed.body.node_id;
        book.import_record(failed, None, NOW, &context()).unwrap();
        for _ in 0..HIGH_DIAL_FAILURE_COUNT {
            book.mark_dial_failure(&failed_id, NOW);
        }

        let survivor = signed_record_with(3, service(3), test_addr(3));
        let survivor_id = survivor.body.node_id;
        book.import_record(survivor, None, NOW, &context()).unwrap();

        assert!(book.get(&expired_id).is_none());
        assert!(book.get(&failed_id).is_some());
        assert!(book.get(&survivor_id).is_some());

        let replacement = signed_record_with(4, service(4), test_addr(4));
        let replacement_id = replacement.body.node_id;
        book.import_record(replacement, None, NOW, &context())
            .unwrap();

        assert!(book.get(&failed_id).is_none());
        assert!(book.get(&survivor_id).is_some());
        assert!(book.get(&replacement_id).is_some());
        assert_eq!(book.discovered_len(), 2);
    }

    #[test]
    fn discovery_book_static_entries_survive_eviction_storm() {
        let mut book = small_book(1);
        let static_record = signed_record_with(1, service(1), test_addr(1));
        let static_id = static_record.body.node_id;
        book.import_static_record(static_record, NOW, &context())
            .expect("static record imports");

        for index in 2..=10 {
            // Each flood record either replaces the last one or is evicted
            // itself.
            assert!(matches!(
                book.import_record(
                    signed_record_with(index.into(), service(index as usize), test_addr(index)),
                    Some(secret_key().public()),
                    NOW,
                    &context(),
                ),
                Ok(ImportOutcome::Added) | Err(DiscoveryBookError::Limit { .. })
            ));
        }

        assert!(book
            .get(&static_id)
            .expect("static entry remains")
            .is_static());
        assert_eq!(book.discovered_len(), 1);
        assert_eq!(book.len(), 2);
    }

    #[test]
    fn discovery_book_evicts_least_recently_successful_entry() {
        let mut book = small_book(2);
        let older_success = signed_record_with(1, service(1), test_addr(1));
        let recent_success = signed_record_with(2, service(2), test_addr(2));
        let replacement = signed_record_with(3, service(3), test_addr(3));
        let older_success_id = older_success.body.node_id;
        let recent_success_id = recent_success.body.node_id;
        let replacement_id = replacement.body.node_id;

        let persisted = [older_success, recent_success, replacement]
            .into_iter()
            .enumerate()
            .map(|(index, record)| ZakuraDiscoveryPersistedEntry {
                record,
                source: None,
                is_static: false,
                last_seen: NOW,
                last_dial_attempt: None,
                last_success: Some(
                    NOW + u64::try_from(index).expect("small test index fits in u64") + 1,
                ),
                last_confirmed: None,
                failure_count: 0,
            });

        let outcome = book.import_persisted_entries(persisted, NOW, &context());

        assert_eq!(outcome.added, 3);
        assert!(book.get(&older_success_id).is_none());
        assert!(book.get(&recent_success_id).is_some());
        assert!(book.get(&replacement_id).is_some());
        assert_eq!(book.discovered_len(), 2);
    }

    /// A full book evicts unconfirmed relayed records before confirmed ones,
    /// so relays cannot empty the sample snapshot's windows.
    #[test]
    fn relayed_records_cannot_evict_confirmed_entries() {
        let mut book = small_book(2);
        let requester = test_requester();
        // The confirmed entry is the least recently seen.
        let confirmed = signed_record_with(1, service(1), test_addr(1));
        let author = confirmed.body.node_id;
        book.import_record(confirmed.clone(), Some(author), NOW - 1, &context())
            .expect("a first-party record imports");
        assert_eq!(
            book.sample_peers(32, &[], &[], NOW, requester),
            vec![confirmed.clone()]
        );

        let relay = secret_key().public();
        for index in 2..=5u8 {
            // Each relayed record either replaces the other unconfirmed
            // entry or is evicted itself.
            assert!(matches!(
                book.import_record(
                    signed_record_with(index.into(), service(index.into()), test_addr(index)),
                    Some(relay),
                    NOW,
                    &context(),
                ),
                Ok(ImportOutcome::Added) | Err(DiscoveryBookError::Limit { .. })
            ));
        }
        assert_eq!(book.discovered_len(), 2);
        assert_eq!(
            book.sample_peers(32, &[], &[], NOW, requester),
            vec![confirmed.clone()]
        );

        // A book full of confirmed entries refuses a relayed record, because
        // the record would evict itself.
        let mut full = small_book(1);
        import_confirmed_record(&mut full, confirmed).expect("a first-party record imports");
        let relayed = signed_record_with(6, service(6), test_addr(6));
        let relayed_node_id = relayed.body.node_id;
        assert!(matches!(
            full.import_record(relayed, Some(relay), NOW, &context()),
            Err(DiscoveryBookError::Limit { .. })
        ));
        assert!(full.get(&relayed_node_id).is_none());
    }

    /// A small snapshot is rebuilt when an entry it left out for expiry gets a
    /// new record, so the entry returns to samples within the epoch.
    #[test]
    fn a_renewed_record_rejoins_a_small_snapshot() {
        let mut book = ZakuraDiscoveryBook::default();
        let requester = test_requester();
        let secret = secret_key();
        let mut first = body(&secret);
        first.sequence = 1;
        let first = ZakuraNodeRecord::sign(first, &secret).expect("test record signs");
        import_confirmed_record(&mut book, first).expect("a first-party record imports");

        // The record expires at `NOW + 60`, inside the epoch that starts before `NOW`.
        let later = NOW + 120;
        assert_eq!(
            later / DISCOVERY_DISCLOSURE_EPOCH.as_secs(),
            NOW / DISCOVERY_DISCLOSURE_EPOCH.as_secs()
        );
        assert!(book.sample_peers(32, &[], &[], later, requester).is_empty());

        let mut renewed = body(&secret);
        renewed.sequence = 2;
        renewed.expires_at_unix_secs = later + 60;
        let renewed = ZakuraNodeRecord::sign(renewed, &secret).expect("test record signs");
        assert_eq!(
            book.import_record(renewed.clone(), Some(secret.public()), later, &context())
                .expect("a renewed first-party record imports"),
            ImportOutcome::Updated
        );
        assert_eq!(
            book.sample_peers(32, &[], &[], later, requester),
            vec![renewed]
        );
    }

    /// Each requester's window starts at its own keyed offset, so requesters
    /// see different windows of a book larger than one window.
    #[test]
    fn windows_depend_on_the_requester() {
        let mut book = ZakuraDiscoveryBook::default();
        for index in 1..=4 * MAX_DISCOVERY_RECORDS_PER_RESPONSE {
            let index = u8::try_from(index).expect("128 records fit u8 indices");
            import_confirmed_record(
                &mut book,
                signed_record_with(index.into(), service(index.into()), test_addr(index)),
            )
            .expect("a first-party record imports");
        }
        let windows: HashSet<Vec<NodeId>> = (0..8)
            .map(|_| {
                let mut window: Vec<NodeId> = book
                    .sample_peers(32, &[], &[], NOW, test_requester())
                    .into_iter()
                    .map(|record| record.body.node_id)
                    .collect();
                window.sort_by_key(|node_id| *node_id.as_bytes());
                window
            })
            .collect();
        assert!(windows.len() > 1, "eight requesters share one window");
    }

    /// A configured import limit above one response's records does not widen
    /// a requester's window.
    #[test]
    fn sample_window_never_exceeds_one_response() {
        let mut book = ZakuraDiscoveryBook::new(ZakuraDiscoveryBookLimits {
            max_imported_records_per_response: 2 * MAX_DISCOVERY_RECORDS_PER_RESPONSE,
            ..ZakuraDiscoveryBookLimits::default()
        });
        let requester = test_requester();
        for index in 1..=40u8 {
            import_confirmed_record(
                &mut book,
                signed_record_with(index.into(), service(index.into()), test_addr(index)),
            )
            .unwrap();
        }
        let first = book.sample_peers(MAX_DISCOVERY_RECORDS_PER_RESPONSE, &[], &[], NOW, requester);
        assert_eq!(first.len(), MAX_DISCOVERY_RECORDS_PER_RESPONSE);
        let mut exclude: Vec<[u8; 32]> = first
            .iter()
            .map(|record| *record.body.node_id.as_bytes())
            .collect();
        exclude.sort_unstable();
        assert!(book
            .sample_peers(
                MAX_DISCOVERY_RECORDS_PER_RESPONSE,
                &[],
                &exclude,
                NOW,
                requester,
            )
            .is_empty());
    }

    #[test]
    fn discovery_book_samples_are_bounded_excluded_and_service_filtered() {
        let mut book = ZakuraDiscoveryBook::default();
        let wanted = service(1);
        let other = service(2);
        let matching_a = signed_record_with(1, wanted.clone(), test_addr(1));
        let matching_b = signed_record_with(2, wanted.clone(), test_addr(2));
        let matching_c = signed_record_with(3, wanted.clone(), test_addr(3));
        let non_matching = signed_record_with(4, other, test_addr(4));
        let excluded = matching_a.body.node_id;
        import_confirmed_record(&mut book, matching_a).unwrap();
        import_confirmed_record(&mut book, matching_b).unwrap();
        import_confirmed_record(&mut book, matching_c).unwrap();
        import_confirmed_record(&mut book, non_matching).unwrap();

        let sample = book.sample_peers(
            1,
            std::slice::from_ref(&wanted),
            &[*excluded.as_bytes()],
            NOW,
            test_requester(),
        );
        let sample_ids: HashSet<_> = sample.iter().map(|record| record.body.node_id).collect();

        assert_eq!(sample.len(), 1);
        assert!(!sample_ids.contains(&excluded));
        assert!(sample
            .iter()
            .all(|record| record.body.services.contains(&wanted)));
    }

    #[test]
    fn discovery_book_samples_skip_expired_records() {
        let mut book = ZakuraDiscoveryBook::default();
        let record = signed_record_with(1, service(1), test_addr(1));
        import_confirmed_record(&mut book, record).unwrap();

        let sample = book.sample_peers(10, &[], &[], NOW + 61, test_requester());

        assert!(sample.is_empty());
    }

    #[test]
    fn discovery_book_dial_metadata_updates_candidates_without_blacklisting() {
        let mut book = ZakuraDiscoveryBook::default();
        let preferred = signed_record_with(1, service(1), test_addr(1));
        let failed = signed_record_with(2, service(1), test_addr(2));
        let failed_id = failed.body.node_id;
        let preferred_id = preferred.body.node_id;

        import_confirmed_record(&mut book, failed.clone()).unwrap();
        import_confirmed_record(&mut book, preferred.clone()).unwrap();
        book.mark_dial_attempt(&failed_id, NOW);
        book.mark_dial_failure(&failed_id, NOW);
        book.mark_dial_success(&preferred_id, NOW + 1);

        let mut rng = StdRng::seed_from_u64(7);
        let candidates = book.dial_candidates(
            10,
            &[service(1)],
            &[],
            DialCandidateExclusions {
                connected_node_ids: &[],
                in_flight_node_ids: &[],
            },
            NOW + 1,
            (
                DEFAULT_DISCOVERY_DIAL_BACKOFF_BASE,
                DEFAULT_DISCOVERY_DIAL_BACKOFF_MAX,
            ),
            &mut rng,
        );
        assert_eq!(candidates, vec![candidate_for(&preferred, false)]);
        assert_eq!(book.get(&failed_id).unwrap().failure_count(), 1);

        let candidates = book.dial_candidates(
            10,
            &[service(1)],
            &[],
            DialCandidateExclusions {
                connected_node_ids: &[],
                in_flight_node_ids: &[],
            },
            NOW + 60,
            (
                DEFAULT_DISCOVERY_DIAL_BACKOFF_BASE,
                DEFAULT_DISCOVERY_DIAL_BACKOFF_MAX,
            ),
            &mut rng,
        );
        assert_eq!(
            candidates,
            vec![
                candidate_for(&preferred, false),
                candidate_for(&failed, false)
            ]
        );
    }

    #[test]
    fn discovery_book_randomizes_comparable_non_static_dial_candidates() {
        let mut book = ZakuraDiscoveryBook::default();
        let records = (1u8..=32)
            .map(|index| signed_record_with(index.into(), service(1), test_addr(index)))
            .collect::<Vec<_>>();

        for record in &records {
            import_confirmed_record(&mut book, record.clone()).expect("test record imports");
        }

        let mut deterministic = records
            .iter()
            .map(|record| candidate_for(record, false))
            .collect::<Vec<_>>();
        deterministic.sort_by_key(|candidate| node_id_sort_key(&candidate.node_id));

        let mut rng = StdRng::seed_from_u64(11);
        let sampled = book.dial_candidates(
            records.len(),
            &[service(1)],
            &[],
            DialCandidateExclusions {
                connected_node_ids: &[],
                in_flight_node_ids: &[],
            },
            NOW,
            (
                DEFAULT_DISCOVERY_DIAL_BACKOFF_BASE,
                DEFAULT_DISCOVERY_DIAL_BACKOFF_MAX,
            ),
            &mut rng,
        );
        let mut other_rng = StdRng::seed_from_u64(12);
        let other_sampled = book.dial_candidates(
            records.len(),
            &[service(1)],
            &[],
            DialCandidateExclusions {
                connected_node_ids: &[],
                in_flight_node_ids: &[],
            },
            NOW,
            (
                DEFAULT_DISCOVERY_DIAL_BACKOFF_BASE,
                DEFAULT_DISCOVERY_DIAL_BACKOFF_MAX,
            ),
            &mut other_rng,
        );

        let sampled_ids: HashSet<_> = sampled.iter().map(|candidate| candidate.node_id).collect();
        assert_eq!(sampled_ids.len(), records.len());
        assert_eq!(
            sampled_ids,
            deterministic
                .iter()
                .map(|candidate| candidate.node_id)
                .collect::<HashSet<_>>()
        );
        assert_ne!(sampled, deterministic);
        assert_ne!(sampled, other_sampled);
    }

    #[test]
    fn discovery_book_dial_backoff_uses_configured_bounds() {
        let mut book = ZakuraDiscoveryBook::default();
        let failed = signed_record_with(1, service(1), test_addr(1));
        let failed_id = failed.body.node_id;

        import_confirmed_record(&mut book, failed.clone()).unwrap();
        book.mark_dial_attempt(&failed_id, NOW);
        book.mark_dial_failure(&failed_id, NOW);

        assert!(book
            .dial_candidates(
                10,
                &[service(1)],
                &[],
                DialCandidateExclusions {
                    connected_node_ids: &[],
                    in_flight_node_ids: &[],
                },
                NOW + 29,
                (Duration::from_secs(30), Duration::from_secs(300)),
                &mut StdRng::seed_from_u64(7),
            )
            .is_empty());
        let mut rng = StdRng::seed_from_u64(7);
        assert_eq!(
            book.dial_candidates(
                10,
                &[service(1)],
                &[],
                DialCandidateExclusions {
                    connected_node_ids: &[],
                    in_flight_node_ids: &[],
                },
                NOW + 30,
                (Duration::from_secs(30), Duration::from_secs(300)),
                &mut rng,
            ),
            vec![candidate_for(&failed, false)]
        );
    }

    // V12 F-305594: an ALPN mismatch used to count as an ordinary failure on
    // the node, so a record rotated to a fresh address became dialable after
    // 60 s. The node now waits out the ten-minute floor whatever it advertises.
    #[test]
    fn discovery_book_alpn_backoff_survives_an_address_rotation() {
        let mut book = ZakuraDiscoveryBook::default();
        let secret_key = secret_key();
        let record_at = |sequence, addr| {
            let mut body = body(&secret_key);
            body.sequence = sequence;
            body.direct_addrs = vec![addr];
            body.services = vec![service(1)];
            body.expires_at_unix_secs = NOW + DEFAULT_DISCOVERY_MAX_RECORD_TTL.as_secs();
            ZakuraNodeRecord::sign(body, &secret_key).expect("test record signs")
        };
        let first = record_at(1, test_addr(1));
        let node_id = first.body.node_id;
        import_confirmed_record(&mut book, first).unwrap();
        book.mark_dial_attempt(&node_id, NOW);
        book.mark_alpn_mismatch(&node_id, NOW);

        let rotated = record_at(2, test_addr(2));
        assert_eq!(
            import_confirmed_record(&mut book, rotated.clone()).unwrap(),
            ImportOutcome::Updated
        );

        let candidates_at = |book: &ZakuraDiscoveryBook, now: u64| {
            book.dial_candidates(
                10,
                &[service(1)],
                &[],
                DialCandidateExclusions {
                    connected_node_ids: &[],
                    in_flight_node_ids: &[],
                },
                now,
                (Duration::from_secs(60), Duration::from_secs(3_600)),
                &mut StdRng::seed_from_u64(7),
            )
        };
        let floor = ZAKURA_ALPN_MISMATCH_BACKOFF.as_secs();
        assert!(candidates_at(&book, NOW + 61).is_empty());
        assert!(candidates_at(&book, NOW + floor - 1).is_empty());
        assert_eq!(
            candidates_at(&book, NOW + floor),
            vec![candidate_for(&rotated, false)]
        );
    }

    #[test]
    fn discovery_book_short_lived_exchange_backoff_is_non_punitive() {
        let mut book = ZakuraDiscoveryBook::default();
        let exchanged = signed_record_with(1, service(1), test_addr(1));
        let exchanged_id = exchanged.body.node_id;

        import_confirmed_record(&mut book, exchanged.clone()).unwrap();
        book.mark_dial_success(&exchanged_id, NOW);
        book.mark_short_lived_exchange(&exchanged_id, NOW);

        let entry = book.get(&exchanged_id).unwrap();
        assert_eq!(entry.failure_count(), 0);
        assert_eq!(entry.last_short_lived_exchange(), Some(NOW));

        assert!(book
            .dial_candidates(
                10,
                &[service(1)],
                &[],
                DialCandidateExclusions {
                    connected_node_ids: &[],
                    in_flight_node_ids: &[],
                },
                NOW + 29,
                (Duration::from_secs(30), Duration::from_secs(300)),
                &mut StdRng::seed_from_u64(7),
            )
            .is_empty());
        assert_eq!(
            book.dial_candidates(
                10,
                &[service(1)],
                &[],
                DialCandidateExclusions {
                    connected_node_ids: &[],
                    in_flight_node_ids: &[],
                },
                NOW + 30,
                (Duration::from_secs(30), Duration::from_secs(300)),
                &mut StdRng::seed_from_u64(7),
            ),
            vec![candidate_for(&exchanged, false)]
        );
    }

    #[test]
    fn discovery_book_services_and_persistence_hooks_revalidate_records() {
        let mut book = ZakuraDiscoveryBook::default();
        let record = signed_record_with(1, service(9), test_addr(9));
        let node_id = record.body.node_id;
        import_confirmed_record(&mut book, record.clone()).unwrap();
        book.mark_dial_attempt(&node_id, NOW + 1);
        book.mark_dial_failure(&node_id, NOW + 1);

        let mut persisted = book.persisted_entries();
        persisted.push(ZakuraDiscoveryPersistedEntry {
            record: {
                let mut invalid = record.clone();
                invalid.body.sequence += 1;
                invalid
            },
            source: None,
            is_static: false,
            last_seen: NOW,
            last_dial_attempt: None,
            last_success: None,
            last_confirmed: None,
            failure_count: 0,
        });

        let mut reloaded = ZakuraDiscoveryBook::default();
        let outcome = reloaded.import_persisted_entries(persisted, NOW, &context());

        assert_eq!(outcome.added, 1);
        assert_eq!(outcome.rejected, 1);
        let reloaded_entry = reloaded
            .get(&node_id)
            .expect("valid persisted entry reloads");
        assert_eq!(reloaded_entry.record(), &record);
        assert_eq!(reloaded_entry.last_dial_attempt(), Some(NOW + 1));
        assert_eq!(reloaded_entry.failure_count(), 1);
        assert_eq!(
            reloaded_entry.last_confirmed(),
            Some(NOW),
            "confirmation must survive a persistence round-trip"
        );
    }

    #[test]
    fn self_record_includes_local_node_addresses_services_and_verifies() {
        let secret = secret_key();
        let node_id = secret.public();
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_at(
            local_config_with(
                secret,
                vec![test_addr(42)],
                vec![
                    ZakuraServiceId::legacy_requests(),
                    ZakuraServiceId::discovery(),
                ],
            ),
            ZakuraDiscoveryConfig::default(),
            connected_rx,
            NOW,
        );

        let record = handle.current_self_record();

        assert_eq!(record.body.node_id, node_id);
        assert_eq!(record.body.direct_addrs, vec![test_addr(42)]);
        assert_eq!(
            record.body.services,
            vec![
                ZakuraServiceId::discovery(),
                ZakuraServiceId::legacy_requests()
            ]
        );
        record.verify(&context()).expect("self-record verifies");
    }

    #[test]
    fn startup_rejects_more_advertised_services_than_a_record_carries() {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        // Custom ids sort ahead of the built-ins, so truncation would drop built-ins.
        let mut services = crate::zakura::discovery::default_advertised_services();
        let custom = MAX_SERVICES_PER_RECORD + 1 - services.len();
        services.extend((0..custom).map(|index| {
            ZakuraServiceId::new(format!("valar.custom.{index}.v1")).expect("valid service id")
        }));

        let Err(error) = ZakuraDiscoveryHandle::new_at(
            local_config_with(secret_key(), vec![test_addr(43)], services.clone()),
            ZakuraDiscoveryConfig::default(),
            connected_rx,
            NOW,
            NOW,
        ) else {
            panic!("an oversized service list fails startup");
        };
        assert!(matches!(
            error,
            DiscoveryWireError::TooManyAdvertisedServices { actual, max: MAX_SERVICES_PER_RECORD }
                if actual == MAX_SERVICES_PER_RECORD + 1
        ));

        // Duplicates do not count against the limit.
        services.pop();
        services.push(services[0].clone());
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_at(
            local_config_with(secret_key(), vec![test_addr(43)], services),
            ZakuraDiscoveryConfig::default(),
            connected_rx,
            NOW,
        );
        assert_eq!(
            handle.current_self_record().body.services.len(),
            MAX_SERVICES_PER_RECORD
        );
    }

    #[tokio::test]
    async fn self_record_for_gossip_refreshes_before_expiry() {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let config = ZakuraDiscoveryConfig {
            record_ttl: Duration::from_secs(60),
            refresh_interval: Duration::from_secs(10),
            ..ZakuraDiscoveryConfig::default()
        };
        let handle = discovery_handle_at_with_sequence(
            local_config_with(secret_key(), vec![test_addr(43)], vec![service(1)]),
            config,
            connected_rx,
            NOW,
            100,
        );
        let initial = handle.current_self_record();

        let still_current = handle
            .current_self_record_for_gossip_at(NOW + 49, 101)
            .await
            .expect("current self-record remains valid past the refresh window");
        assert_eq!(still_current, initial);

        let refreshed = handle
            .current_self_record_for_gossip_at(NOW + 50, 102)
            .await
            .expect("self-record refresh signs");
        assert!(refreshed.body.sequence > initial.body.sequence);
        assert_eq!(refreshed.body.expires_at_unix_secs, NOW + 110);
        assert_eq!(handle.current_self_record(), refreshed);
    }

    #[test]
    fn derived_sequence_does_not_regress_after_restart() {
        let secret = secret_key();
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let first = discovery_handle_at(
            local_config_with(secret.clone(), vec![test_addr(44)], vec![service(1)]),
            ZakuraDiscoveryConfig::default(),
            connected_rx,
            NOW,
        );
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let restarted = discovery_handle_at(
            local_config_with(secret, vec![test_addr(44)], vec![service(1)]),
            ZakuraDiscoveryConfig::default(),
            connected_rx,
            NOW + 1,
        );

        assert!(
            restarted.current_self_record().body.sequence
                > first.current_self_record().body.sequence
        );
        assert_eq!(
            restarted.current_self_record().body.node_id,
            first.current_self_record().body.node_id
        );
    }

    #[test]
    fn persisted_sequence_seed_makes_fast_restart_record_newer() {
        let secret = secret_key();
        let same_unix_second = NOW;
        let same_sequence_tick = 10_000_000_000;
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let first = discovery_handle_at_with_sequence(
            local_config_with(secret.clone(), vec![test_addr(44)], vec![service(1)]),
            ZakuraDiscoveryConfig::default(),
            connected_rx,
            same_unix_second,
            same_sequence_tick,
        );
        let first_record = first.current_self_record();

        let mut restarted_config = local_config_with(secret, vec![test_addr(45)], vec![service(2)]);
        restarted_config.last_authored_sequence = Some(first_record.body.sequence);
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let restarted = discovery_handle_at_with_sequence(
            restarted_config,
            ZakuraDiscoveryConfig::default(),
            connected_rx,
            same_unix_second,
            same_sequence_tick,
        );
        let restarted_record = restarted.current_self_record();

        assert_eq!(restarted_record.body.node_id, first_record.body.node_id);
        assert!(restarted_record.body.sequence > first_record.body.sequence);
        assert_eq!(restarted_record.body.direct_addrs, vec![test_addr(45)]);
        assert_eq!(restarted_record.body.services, vec![service(2)]);
    }

    #[test]
    fn self_record_filters_non_routable_direct_addresses() {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_at(
            local_config_with(
                secret_key(),
                vec![
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8233),
                    SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 8233),
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8233),
                    SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 8233),
                    test_addr(45),
                ],
                vec![service(1)],
            ),
            ZakuraDiscoveryConfig::default(),
            connected_rx,
            NOW,
        );

        assert_eq!(
            handle.current_self_record().body.direct_addrs,
            vec![test_addr(45)]
        );
    }

    #[tokio::test]
    async fn active_services_follow_supervisor_watch_connect_and_disconnect() {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let record = runtime_record_with(1, service(9), test_addr(9));
        let node_id = record.body.node_id;
        connected_tx.send_replace(vec![peer_id_for(node_id)]);
        handle
            .import_connected_peer_record(record, node_id)
            .await
            .expect("connected self-record imports");

        assert_eq!(
            handle.active_services(node_id).await,
            Some(vec![service(9)])
        );

        connected_tx.send_replace(Vec::new());
        assert_eq!(handle.active_services(node_id).await, None);
    }

    #[tokio::test]
    async fn active_services_drop_connected_hello_absent_from_supervisor_watch() {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let record = runtime_record_with(1, service(9), test_addr(9));
        let node_id = record.body.node_id;
        handle
            .import_connected_peer_record(record, node_id)
            .await
            .expect("connected self-record imports");
        assert_eq!(handle.active_services(node_id).await, None);
        assert!(handle.inner.lock().await.active_services.is_empty());
    }

    #[tokio::test]
    async fn gossiped_service_record_does_not_create_active_services() {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let record = runtime_record_with(1, service(9), test_addr(9));
        let node_id = record.body.node_id;

        handle
            .import_peer_records([record], None)
            .await
            .expect("authentic test records import");
        connected_tx.send_replace(vec![peer_id_for(node_id)]);

        assert_eq!(handle.active_services(node_id).await, None);
    }

    #[tokio::test]
    async fn connected_services_import_creates_and_updates_first_party_live_summary() {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let node_id = secret_key().public();
        let now = current_unix_secs();
        connected_tx.send_replace(vec![peer_id_for(node_id)]);

        let first_summary = DiscoveryServiceSummary {
            peer_exchange_slots_free: 1,
            ..discovery_summary()
        };
        handle
            .import_connected_peer_services_at(
                first_party_services(
                    node_id,
                    u64::MAX,
                    vec![ServiceSummaryEnvelope::discovery(&first_summary)
                        .expect("test discovery summary encodes")],
                ),
                node_id,
                now,
            )
            .await
            .expect("first-party services import");

        assert_eq!(
            handle.active_services(node_id).await,
            Some(vec![ZakuraServiceId::discovery()])
        );
        let cached = handle
            .live_service_summaries_at(node_id, now)
            .await
            .expect("live summary entry exists");
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].observed_at_unix_secs, now);
        assert_eq!(
            cached[0].expires_at_unix_secs,
            now + DEFAULT_LIVE_SERVICE_SUMMARY_TTL.as_secs()
        );
        assert_eq!(
            cached[0].summary,
            ZakuraLiveServiceSummary::Discovery(first_summary)
        );

        let updated_summary = DiscoveryServiceSummary {
            peer_exchange_slots_free: 9,
            ..discovery_summary()
        };
        handle
            .import_connected_peer_services_at(
                first_party_services(
                    node_id,
                    now + 10,
                    vec![ServiceSummaryEnvelope::discovery(&updated_summary)
                        .expect("test discovery summary encodes")],
                ),
                node_id,
                now + 1,
            )
            .await
            .expect("newer first-party services update");

        let cached = handle
            .live_service_summaries_at(node_id, now + 1)
            .await
            .expect("updated live summary entry exists");
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].observed_at_unix_secs, now + 1);
        assert_eq!(cached[0].expires_at_unix_secs, now + 10);
        assert_eq!(
            cached[0].summary,
            ZakuraLiveServiceSummary::Discovery(updated_summary)
        );
    }

    #[tokio::test]
    async fn connected_services_tolerate_clock_skew_with_a_local_receipt_ttl() {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let node_id = secret_key().public();
        connected_tx.send_replace(vec![peer_id_for(node_id)]);

        handle
            .import_connected_peer_services_at(
                first_party_services(
                    node_id,
                    NOW - 1,
                    vec![ServiceSummaryEnvelope::discovery(&discovery_summary())
                        .expect("test discovery summary encodes")],
                ),
                node_id,
                NOW,
            )
            .await
            .expect("a live first-party summary inside the clock-skew window imports");
        let cached = handle
            .live_service_summaries_at(node_id, NOW)
            .await
            .expect("clock-skew-tolerant summary remains live");
        assert_eq!(
            cached[0].expires_at_unix_secs,
            NOW + DEFAULT_LIVE_SERVICE_SUMMARY_TTL.as_secs()
        );

        handle
            .import_connected_peer_services_at(
                first_party_services(
                    node_id,
                    NOW - DEFAULT_DISCOVERY_CLOCK_SKEW_TOLERANCE.as_secs() - 1,
                    vec![ServiceSummaryEnvelope::discovery(&discovery_summary())
                        .expect("test discovery summary encodes")],
                ),
                node_id,
                NOW,
            )
            .await
            .expect("a stale live summary is ignored without rejecting the connection");
        assert!(handle
            .live_service_summaries_at(node_id, NOW)
            .await
            .is_none());
    }

    #[tokio::test]
    async fn connected_services_mismatched_node_id_rejected_without_live_summary() {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let peer_node_id = secret_key().public();
        let claimed_node_id = secret_key().public();
        connected_tx.send_replace(vec![peer_id_for(peer_node_id)]);

        let error = handle
            .import_connected_peer_services_at(
                first_party_services(
                    claimed_node_id,
                    NOW + 30,
                    vec![ServiceSummaryEnvelope::discovery(&discovery_summary())
                        .expect("test discovery summary encodes")],
                ),
                peer_node_id,
                NOW,
            )
            .await
            .expect_err("mismatched services are rejected");

        assert!(matches!(
            error,
            DiscoveryWireError::MismatchedNodeId {
                field: "services node id"
            }
        ));
        assert_eq!(
            handle.live_service_summaries_at(peer_node_id, NOW).await,
            None
        );
        assert_eq!(
            handle.live_service_summaries_at(claimed_node_id, NOW).await,
            None
        );
    }

    #[tokio::test]
    async fn peers_response_and_gossiped_record_do_not_create_live_summary() {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let record = runtime_record_with(1, service(9), test_addr(9));
        let node_id = record.body.node_id;
        let message = DiscoveryMessage::Peers {
            records: vec![record],
        };
        let encoded = message.encode().expect("test peers response encodes");
        let DiscoveryMessage::Peers { records } =
            DiscoveryMessage::decode(&encoded).expect("test peers response decodes")
        else {
            panic!("PEERS response decoded as a different message");
        };

        handle
            .import_peer_records(records, None)
            .await
            .expect("authentic test records import");
        connected_tx.send_replace(vec![peer_id_for(node_id)]);

        assert_eq!(handle.live_service_summaries_at(node_id, NOW).await, None);
        assert_eq!(handle.active_services(node_id).await, None);
        assert!(handle.record_for(node_id).await.is_some());
    }

    #[tokio::test]
    async fn active_services_prefer_latest_connected_hello_over_book_record() {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let secret = secret_key();
        let active_record = runtime_record_with_secret(&secret, 1, service(1), test_addr(1));
        let stale_book_record = runtime_record_with_secret(&secret, 2, service(2), test_addr(2));
        let node_id = active_record.body.node_id;

        handle
            .import_connected_peer_record(active_record, node_id)
            .await
            .expect("connected self-record imports");
        import_one_peer_record(&handle, stale_book_record, Some(secret_key().public())).await;
        connected_tx.send_replace(vec![peer_id_for(node_id)]);

        assert_eq!(
            handle.active_services(node_id).await,
            Some(vec![service(1)])
        );
    }

    /// A `Hello` identical to the stored record, in the book or only in the
    /// live service hints, skips verification and needs no verification slot.
    /// A changed record waits for a slot.
    #[tokio::test]
    async fn identical_hello_skips_verification() {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let secret = secret_key();
        let stored = runtime_record_with_secret(&secret, 1, service(1), test_addr(1));
        let newer = runtime_record_with_secret(&secret, 2, service(1), test_addr(1));
        let node_id = stored.body.node_id;
        let addressless_secret = secret_key();
        let addressless =
            runtime_record_with_secret_and_addrs(&addressless_secret, 1, service(2), Vec::new());
        let addressless_node_id = addressless.body.node_id;
        connected_tx.send_replace(vec![peer_id_for(node_id), peer_id_for(addressless_node_id)]);
        handle
            .import_connected_peer_record(stored.clone(), node_id)
            .await
            .expect("the first hello imports");
        assert!(matches!(
            handle
                .import_connected_peer_record(addressless.clone(), addressless_node_id)
                .await,
            Err(DiscoveryBookError::NoUsableDirectAddress)
        ));

        let held = handle.hold_verify_slots();
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(5),
                handle.import_connected_peer_record(stored, node_id),
            )
            .await
            .expect("an identical hello needs no verification slot")
            .expect("an identical hello imports"),
            ImportOutcome::MetadataUpdated
        );
        assert!(matches!(
            tokio::time::timeout(
                Duration::from_secs(5),
                handle.import_connected_peer_record(addressless, addressless_node_id),
            )
            .await
            .expect("an identical addressless hello needs no verification slot"),
            Err(DiscoveryBookError::NoUsableDirectAddress)
        ));
        let waiting = tokio::spawn({
            let handle = handle.clone();
            async move { handle.import_connected_peer_record(newer, node_id).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !waiting.is_finished(),
            "a changed hello waits for a verification slot"
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(5), handle.record_for(node_id))
                .await
                .is_ok(),
            "a hello waiting for a slot does not hold the discovery lock"
        );

        drop(held);
        assert_eq!(
            waiting
                .await
                .expect("the import task does not panic")
                .expect("a changed hello imports once a slot is free"),
            ImportOutcome::Updated
        );
    }

    #[tokio::test]
    async fn active_services_ignore_stale_and_equal_sequence_conflicting_hellos() {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let secret = secret_key();
        let newer = runtime_record_with_secret(&secret, 2, service(2), test_addr(2));
        let older = runtime_record_with_secret(&secret, 1, service(1), test_addr(1));
        let equal_conflicting = runtime_record_with_secret(&secret, 2, service(3), test_addr(3));
        let stored_newer = runtime_record_with_secret(&secret, 4, service(4), test_addr(4));
        let older_than_stored = runtime_record_with_secret(&secret, 3, service(3), test_addr(3));
        let node_id = newer.body.node_id;
        connected_tx.send_replace(vec![peer_id_for(node_id)]);

        handle
            .import_connected_peer_record(newer, node_id)
            .await
            .expect("newer connected self-record imports");
        assert_eq!(
            handle
                .import_connected_peer_record(older, node_id)
                .await
                .expect("older connected self-record verifies and is ignored"),
            ImportOutcome::IgnoredOlder
        );
        assert_eq!(
            handle
                .import_connected_peer_record(equal_conflicting, node_id)
                .await
                .expect("equal-sequence connected self-record refreshes metadata only"),
            ImportOutcome::MetadataUpdated
        );
        import_one_peer_record(&handle, stored_newer, Some(secret_key().public())).await;
        assert_eq!(
            handle
                .import_connected_peer_record(older_than_stored, node_id)
                .await
                .expect("connected self-record older than the book is ignored"),
            ImportOutcome::IgnoredOlder
        );

        assert_eq!(
            handle.active_services(node_id).await,
            Some(vec![service(2)])
        );
    }

    #[tokio::test]
    async fn addressless_active_services_ignore_stale_and_equal_sequence_conflicting_hellos() {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let secret = secret_key();
        let newer = runtime_record_with_secret_and_addrs(&secret, 2, service(2), Vec::new());
        let older = runtime_record_with_secret_and_addrs(&secret, 1, service(1), Vec::new());
        let equal_conflicting =
            runtime_record_with_secret_and_addrs(&secret, 2, service(3), Vec::new());
        let node_id = newer.body.node_id;
        connected_tx.send_replace(vec![peer_id_for(node_id)]);

        assert!(matches!(
            handle.import_connected_peer_record(newer, node_id).await,
            Err(DiscoveryBookError::NoUsableDirectAddress)
        ));
        assert!(matches!(
            handle.import_connected_peer_record(older, node_id).await,
            Err(DiscoveryBookError::NoUsableDirectAddress)
        ));
        assert!(matches!(
            handle
                .import_connected_peer_record(equal_conflicting, node_id)
                .await,
            Err(DiscoveryBookError::NoUsableDirectAddress)
        ));

        assert_eq!(
            handle.active_services(node_id).await,
            Some(vec![service(2)])
        );
    }

    /// A peer that reset its sequence and dropped its addresses regains its
    /// services once its old dialable record expires.
    #[tokio::test]
    async fn addressless_reset_hello_replaces_an_expired_dialable_record() {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let secret = secret_key();
        let node_id = secret.public();
        connected_tx.send_replace(vec![peer_id_for(node_id)]);
        // Expired, but within the clock-skew tolerance, so it imports.
        let mut old = runtime_record_with_secret(&secret, 10, service(2), test_addr(2)).body;
        old.expires_at_unix_secs = current_unix_secs() - 1;
        let old = ZakuraNodeRecord::sign(old, &secret).expect("record signs");
        handle
            .import_connected_peer_record(old, node_id)
            .await
            .expect("a dialable record imports");

        let reset = runtime_record_with_secret_and_addrs(&secret, 1, service(3), Vec::new());
        assert!(matches!(
            handle.import_connected_peer_record(reset, node_id).await,
            Err(DiscoveryBookError::NoUsableDirectAddress)
        ));
        assert_eq!(
            handle.active_services(node_id).await,
            Some(vec![service(3)])
        );
    }

    #[tokio::test]
    async fn expired_connected_hello_does_not_create_active_services() {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let secret = secret_key();
        let mut body = body(&secret);
        body.sequence = 1;
        body.direct_addrs = vec![test_addr(1)];
        body.services = vec![service(1)];
        body.expires_at_unix_secs = current_unix_secs()
            .saturating_sub(DEFAULT_DISCOVERY_CLOCK_SKEW_TOLERANCE.as_secs())
            .saturating_sub(1);
        let expired = ZakuraNodeRecord::sign(body, &secret).expect("expired record signs");
        let node_id = expired.body.node_id;
        connected_tx.send_replace(vec![peer_id_for(node_id)]);

        assert!(matches!(
            handle.import_connected_peer_record(expired, node_id).await,
            Err(DiscoveryBookError::Record(DiscoveryRecordError::Expired))
        ));
        assert_eq!(handle.active_services(node_id).await, None);
    }

    #[tokio::test]
    async fn first_party_block_sync_summary_only_imports_from_authenticated_peer() {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let peer_node_id = secret_key().public();
        let claimed_node_id = secret_key().public();
        let now = current_unix_secs();
        connected_tx.send_replace(vec![peer_id_for(peer_node_id)]);

        let error = handle
            .import_connected_peer_services_at(
                first_party_services(
                    claimed_node_id,
                    now + 30,
                    vec![ServiceSummaryEnvelope::block_sync(&block_sync_summary())
                        .expect("test block summary encodes")],
                ),
                peer_node_id,
                now,
            )
            .await
            .expect_err("mismatched first-party block summary is rejected");

        assert!(matches!(
            error,
            DiscoveryWireError::MismatchedNodeId {
                field: "services node id"
            }
        ));
        assert_eq!(
            handle.live_service_summaries_at(peer_node_id, now).await,
            None
        );
        assert_eq!(
            handle.live_service_summaries_at(claimed_node_id, now).await,
            None
        );

        let record = runtime_record_with(10, ZakuraServiceId::block_sync(), test_addr(10));
        let node_id = record.body.node_id;
        let message = DiscoveryMessage::Peers {
            records: vec![record],
        };
        let encoded = message.encode().expect("test peers response encodes");
        let DiscoveryMessage::Peers { records } =
            DiscoveryMessage::decode(&encoded).expect("test peers response decodes")
        else {
            panic!("PEERS response decoded as a different message");
        };

        handle
            .import_peer_records(records, None)
            .await
            .expect("authentic test records import");
        connected_tx.send_replace(vec![peer_id_for(peer_node_id), peer_id_for(node_id)]);

        assert_eq!(handle.live_service_summaries_at(node_id, now).await, None);
        assert_eq!(handle.active_services(node_id).await, None);
    }

    #[tokio::test]
    async fn live_summaries_expire_independently_of_signed_records() {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let record = runtime_record_with(1, ZakuraServiceId::discovery(), test_addr(1));
        let node_id = record.body.node_id;
        connected_tx.send_replace(vec![peer_id_for(node_id)]);

        handle
            .import_connected_peer_record(record.clone(), node_id)
            .await
            .expect("connected self-record imports");
        handle
            .import_connected_peer_services_at(
                first_party_services(
                    node_id,
                    NOW + 5,
                    vec![ServiceSummaryEnvelope::discovery(&discovery_summary())
                        .expect("test discovery summary encodes")],
                ),
                node_id,
                NOW,
            )
            .await
            .expect("first-party discovery summary imports");

        assert_eq!(
            handle
                .live_service_summaries_at(node_id, NOW)
                .await
                .expect("fresh summary is cached")
                .len(),
            1
        );
        assert_eq!(
            handle.live_service_summaries_at(node_id, NOW + 6).await,
            Some(Vec::new())
        );
        assert_eq!(handle.record_for(node_id).await, Some(record));
        assert_eq!(
            handle.active_services(node_id).await,
            Some(vec![ZakuraServiceId::discovery()])
        );
    }

    #[tokio::test]
    async fn expired_live_only_service_membership_is_pruned() {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let service = service(42);
        let live_only_node_id = secret_key().public();
        let record_backed = runtime_record_with(1, service.clone(), test_addr(42));
        let record_backed_node_id = record_backed.body.node_id;
        let now = current_unix_secs();
        connected_tx.send_replace(vec![
            peer_id_for(live_only_node_id),
            peer_id_for(record_backed_node_id),
        ]);

        handle
            .import_connected_peer_record(record_backed, record_backed_node_id)
            .await
            .expect("connected record imports");
        for node_id in [live_only_node_id, record_backed_node_id] {
            handle
                .import_connected_peer_services_at(
                    first_party_services(
                        node_id,
                        now + 30,
                        vec![ServiceSummaryEnvelope {
                            service_id: service.clone(),
                            summary_tag: 999,
                            summary_bytes: vec![1, 2, 3],
                        }],
                    ),
                    node_id,
                    now,
                )
                .await
                .expect("first-party live summary imports");
        }

        assert_eq!(
            handle.active_services(live_only_node_id).await,
            Some(vec![service.clone()])
        );
        assert_eq!(
            handle.active_services(record_backed_node_id).await,
            Some(vec![service.clone()])
        );

        for node_id in [live_only_node_id, record_backed_node_id] {
            handle
                .import_connected_peer_services_at(
                    first_party_services(
                        node_id,
                        now.saturating_sub(DEFAULT_DISCOVERY_CLOCK_SKEW_TOLERANCE.as_secs()),
                        vec![ServiceSummaryEnvelope {
                            service_id: service.clone(),
                            summary_tag: 999,
                            summary_bytes: vec![1, 2, 3],
                        }],
                    ),
                    node_id,
                    now,
                )
                .await
                .expect("stale first-party live summary is dropped");
        }

        assert_eq!(handle.active_services(live_only_node_id).await, None);
        assert_eq!(
            handle.active_services(record_backed_node_id).await,
            Some(vec![service.clone()])
        );
    }

    #[tokio::test]
    async fn expired_connected_record_service_membership_is_pruned() {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let record = runtime_record_with(1, service(43), test_addr(43));
        let node_id = record.body.node_id;
        let expires_at = record.body.expires_at_unix_secs;
        connected_tx.send_replace(vec![peer_id_for(node_id)]);

        handle
            .import_connected_peer_record(record, node_id)
            .await
            .expect("connected record imports");
        assert_eq!(
            handle.active_services(node_id).await,
            Some(vec![service(43)])
        );

        let mut inner = handle.inner.lock().await;
        inner.sync_active_services(&[node_id], expires_at.saturating_add(1));
        assert!(!inner.active_services.contains_key(&node_id));
    }

    #[tokio::test]
    async fn handle_imports_records_through_storage_validation() {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let valid = runtime_record_with(1, service(1), test_addr(1));
        let signing_secret = secret_key();
        let mut no_addr = body(&signing_secret);
        no_addr.direct_addrs = Vec::new();
        no_addr.expires_at_unix_secs = current_unix_secs() + DEFAULT_DISCOVERY_RECORD_TTL.as_secs();
        let invalid = ZakuraNodeRecord::sign(no_addr, &signing_secret).expect("record signs");

        let outcome = handle
            .import_peer_records([valid, invalid], None)
            .await
            .expect("an address policy failure only drops its record");

        assert_eq!(outcome.attempted, 2);
        assert_eq!(outcome.added, 1);
        assert_eq!(outcome.rejected, 1);
    }

    /// Regression test for `claude-discovery-expensive-work-under-global-mutex`.
    ///
    /// An authenticated discovery peer can send a full `Peers` batch whose records carry
    /// invalid signatures. Each record triggers a full Ed25519 verification — the
    /// expensive, attacker-driven work. Verification runs in a blocking task under a
    /// verification slot, never under the global discovery mutex, so other discovery
    /// operations make progress while a batch waits for or holds a slot.
    #[tokio::test]
    async fn import_peer_records_verifies_signatures_without_holding_global_mutex() {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);

        // A full Peers batch of records with valid bodies but tampered signatures: bumping
        // the sequence after signing leaves body validation passing while the Ed25519 check
        // runs in full and fails.
        let mut batch = Vec::with_capacity(MAX_DISCOVERY_RECORDS_PER_RESPONSE);
        for index in 0..MAX_DISCOVERY_RECORDS_PER_RESPONSE {
            let octet = u8::try_from(index + 1).expect("test batch index fits in u8");
            let sequence = u64::try_from(index + 1).expect("test batch index fits in u64");
            let mut record = runtime_record_with(sequence, service(1), test_addr(octet));
            record.body.sequence += 1;
            batch.push(record);
        }

        let held = handle.verify_slots.hold_free();
        let import = tokio::spawn({
            let handle = handle.clone();
            async move { handle.import_peer_records(batch, None).await }
        });
        // Let the batch reach the slot wait.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let guard = tokio::time::timeout(Duration::from_secs(5), handle.inner.lock())
            .await
            .expect("a batch waiting for verification does not hold the global mutex");
        assert!(
            !import.is_finished(),
            "the batch waits for a verification slot"
        );
        drop(guard);
        drop(held);

        let verdict = tokio::time::timeout(Duration::from_secs(5), import)
            .await
            .expect("the batch verifies once a slot frees")
            .expect("the import task does not panic");
        assert!(matches!(
            verdict,
            Err(DiscoveryRecordError::InvalidSignature)
        ));
    }

    /// Relayed records follow the relay verdict table: a forged record anywhere in the
    /// batch, even past the import limit, disconnects the relay before any record is
    /// imported, and records that only fail local or compatibility checks drop alone.
    #[tokio::test]
    async fn relayed_batch_verdicts() {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let relay = secret_key().public();
        let valid = || runtime_record_with(1, service(1), test_addr(1));

        for position in 0..3 {
            let handle = discovery_handle_with_connected(connected_rx.clone());
            let mut batch: Vec<_> = (0..3)
                .map(|index| runtime_record_with(1, service(index), test_addr(10 + index as u8)))
                .collect();
            batch[position].body.sequence += 1;
            assert!(matches!(
                handle.import_peer_records(batch, Some(relay)).await,
                Err(DiscoveryRecordError::InvalidSignature)
            ));
            assert_eq!(
                handle.inner.lock().await.book.len(),
                0,
                "position {position}"
            );
        }

        // A forged record past a smaller import limit still rejects the batch.
        let handle = discovery_handle_at(
            local_config_with(
                secret_key(),
                vec![test_addr(200)],
                vec![ZakuraServiceId::discovery()],
            ),
            ZakuraDiscoveryConfig {
                book_limits: ZakuraDiscoveryBookLimits {
                    max_imported_records_per_response: 1,
                    ..ZakuraDiscoveryBookLimits::default()
                },
                ..ZakuraDiscoveryConfig::default()
            },
            connected_rx.clone(),
            current_unix_secs(),
        );
        let mut forged = runtime_record_with(1, service(2), test_addr(2));
        forged.body.sequence += 1;
        assert!(matches!(
            handle
                .import_peer_records([valid(), forged], Some(relay))
                .await,
            Err(DiscoveryRecordError::InvalidSignature)
        ));
        assert_eq!(handle.inner.lock().await.book.len(), 0);

        let handle = discovery_handle_with_connected(connected_rx.clone());
        let now = current_unix_secs();
        let signer = secret_key();
        let mut incompatible = body(&signer);
        incompatible.zakura_protocol_min = u16::MAX - 1;
        incompatible.zakura_protocol_max = u16::MAX;
        incompatible.expires_at_unix_secs = now + DEFAULT_DISCOVERY_RECORD_TTL.as_secs();
        let incompatible = ZakuraNodeRecord::sign(incompatible, &signer).expect("record signs");
        let signer = secret_key();
        let mut expired = body(&signer);
        expired.expires_at_unix_secs = now - 3_600;
        let expired = ZakuraNodeRecord::sign(expired, &signer).expect("record signs");
        let outcome = handle
            .import_peer_records([incompatible, expired, valid()], Some(relay))
            .await
            .expect("incompatible and expired records only drop");
        assert_eq!(outcome.added, 1);
        assert_eq!(outcome.rejected, 2);

        // An expired record with a bad signature is forged, not stale.
        let signer = secret_key();
        let mut forged = body(&signer);
        forged.expires_at_unix_secs = now - 3_600;
        let mut forged = ZakuraNodeRecord::sign(forged, &signer).expect("record signs");
        forged.body.sequence += 1;
        assert!(matches!(
            handle.import_peer_records([forged], Some(relay)).await,
            Err(DiscoveryRecordError::InvalidSignature)
        ));
    }

    /// A record identical to the stored one is not verified again.
    #[tokio::test]
    async fn identical_relayed_records_skip_verification() {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let record = runtime_record_with(1, service(1), test_addr(1));
        handle
            .import_peer_records([record.clone()], None)
            .await
            .expect("an authentic record imports");

        let _held = handle.verify_slots.hold_free();
        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            handle.import_peer_records([record], None),
        )
        .await
        .expect("an identical record needs no verification slot")
        .expect("an authentic record imports");
        assert_eq!(outcome.metadata_updated, 1);
    }

    /// Recounts each relay's third-party entries from the book.
    fn recount_relayed(book: &ZakuraDiscoveryBook) -> HashMap<NodeId, usize> {
        let mut relayed = HashMap::new();
        for (node_id, entry) in &book.entries {
            if let Some(relay) = relay_of(entry.source, *node_id) {
                *relayed.entry(relay).or_default() += 1;
            }
        }
        relayed
    }

    /// One relay adds at most its quota of new third-party entries. Updates and
    /// other relays still import.
    #[test]
    fn per_relay_quota_bounds_new_entries() {
        const QUOTA: usize = 3;
        let mut book = ZakuraDiscoveryBook::new(ZakuraDiscoveryBookLimits {
            max_records_per_source: QUOTA,
            ..ZakuraDiscoveryBookLimits::default()
        });
        let relay = secret_key().public();
        let other_relay = secret_key().public();
        let mut records = Vec::new();
        for index in 0..QUOTA {
            let octet = u8::try_from(index + 1).expect("small test index fits in u8");
            let record = long_lived_record_with(1, service(index), test_addr(octet));
            book.import_record(record.clone(), Some(relay), NOW, &context())
                .expect("the relay is under its quota");
            records.push(record);
        }
        let extra = long_lived_record_with(1, service(9), test_addr(50));
        assert!(matches!(
            book.import_record(extra.clone(), Some(relay), NOW, &context()),
            Err(DiscoveryBookError::Limit {
                field: "records per relay",
                ..
            })
        ));
        book.import_record(records[0].clone(), Some(relay), NOW, &context())
            .expect("an update adds no entry");
        book.import_record(extra, Some(other_relay), NOW, &context())
            .expect("another relay has its own quota");

        // Re-attributing an entry to another relay frees room for the first.
        book.import_record(records[1].clone(), Some(other_relay), NOW, &context())
            .expect("an update adds no entry");
        book.import_record(
            long_lived_record_with(1, service(10), test_addr(51)),
            Some(relay),
            NOW,
            &context(),
        )
        .expect("the relay is back under its quota");

        // A relay at its quota cannot take over another relay's entry.
        book.import_record(
            long_lived_record_with(1, service(11), test_addr(52)),
            Some(other_relay),
            NOW,
            &context(),
        )
        .expect("the other relay is under its quota");
        book.import_record(records[0].clone(), Some(other_relay), NOW, &context())
            .expect("an update adds no entry");
        assert_eq!(
            book.get(&records[0].body.node_id)
                .expect("the entry is stored")
                .source,
            Some(relay)
        );
        assert_eq!(book.relayed.get(&relay), Some(&QUOTA));
        assert_eq!(book.relayed.get(&other_relay), Some(&QUOTA));
        assert_eq!(book.relayed, recount_relayed(&book));
    }

    /// A relay at its quota adds a new entry in place of its own expired entries,
    /// and an entry its author sent never counts against a quota.
    #[test]
    fn a_full_relay_reuses_its_expired_entries() {
        const QUOTA: usize = 2;
        let mut book = ZakuraDiscoveryBook::new(ZakuraDiscoveryBookLimits {
            max_records_per_source: QUOTA,
            ..ZakuraDiscoveryBookLimits::default()
        });
        let relay_secret = secret_key();
        let relay = relay_secret.public();
        book.import_record(
            record_with_secret(&relay_secret, 1, service(0)),
            Some(relay),
            NOW,
            &context(),
        )
        .expect("a relay's own record imports");
        let short_lived = signed_record_with(1, service(1), test_addr(1));
        book.import_record(short_lived.clone(), Some(relay), NOW, &context())
            .expect("the relay is under its quota");
        book.import_record(
            long_lived_record_with(1, service(2), test_addr(2)),
            Some(relay),
            NOW,
            &context(),
        )
        .expect("the relay's own record does not count against its quota");
        assert_eq!(book.relayed.get(&relay), Some(&QUOTA));

        let fresh = || long_lived_record_with(1, service(3), test_addr(3));
        assert!(matches!(
            book.import_record(fresh(), Some(relay), NOW, &context()),
            Err(DiscoveryBookError::Limit { .. })
        ));
        let later = short_lived.body.expires_at_unix_secs + 1;
        book.import_record(fresh(), Some(relay), later, &context())
            .expect("the expired entry makes room");
        assert!(book.get(&short_lived.body.node_id).is_none());
        assert_eq!(book.relayed.get(&relay), Some(&QUOTA));
        assert_eq!(book.relayed, recount_relayed(&book));
    }

    /// Book eviction returns the evicted entry's attribution.
    #[test]
    fn eviction_returns_the_relays_attribution() {
        let mut book = small_book(2);
        let relay = secret_key().public();
        for index in 0..3u8 {
            let _ = book.import_record(
                long_lived_record_with(1, service(usize::from(index)), test_addr(index + 1)),
                Some(relay),
                NOW,
                &context(),
            );
        }
        assert_eq!(book.len(), 2);
        assert_eq!(book.relayed.get(&relay), Some(&2));
        assert_eq!(book.relayed, recount_relayed(&book));
    }

    /// A third-party gossiped record is a local dial hint only. Until its owner confirms
    /// first-party or a local dial succeeds, it is not eligible for `sample_peers`, so the
    /// node does not re-gossip records it has only learned second-hand.
    #[test]
    fn discovery_book_third_party_records_require_confirmation_before_sampling() {
        let mut book = ZakuraDiscoveryBook::default();
        let gossiped = signed_record_with(1, service(1), test_addr(1));
        let gossiped_id = gossiped.body.node_id;
        book.import_record(
            gossiped.clone(),
            Some(secret_key().public()),
            NOW,
            &context(),
        )
        .expect("third-party gossip record imports");

        assert!(
            book.sample_peers(10, &[], &[], NOW, test_requester())
                .is_empty(),
            "unconfirmed third-party record must not be re-gossiped"
        );

        // Re-learning the same record from other third parties never confirms it.
        book.import_record(
            gossiped.clone(),
            Some(secret_key().public()),
            NOW,
            &context(),
        )
        .expect("repeated third-party gossip imports");
        assert!(book
            .sample_peers(10, &[], &[], NOW, test_requester())
            .is_empty());
        assert!(book
            .get(&gossiped_id)
            .expect("gossiped entry is stored")
            .last_confirmed()
            .is_none());

        // A successful local dial is an explicit trust transition that makes it shareable.
        book.mark_dial_success(&gossiped_id, NOW + 1);
        assert_eq!(
            book.sample_peers(10, &[], &[], NOW + 1, test_requester()),
            vec![gossiped],
            "locally dialed record becomes sample-eligible"
        );
    }

    /// `sample_peers` answers each requester from a bounded per-`(requester, epoch)`
    /// window, and carries forward the sampling clone/scan bound from
    /// `claude-discovery-expensive-work-under-global-mutex`: no matter how a requester
    /// varies limits, filters, and exclusions, one `(requester, epoch)` scope can only ever
    /// observe — and force clones of — one bounded window.
    #[test]
    fn discovery_book_sample_disclosure_set_is_stable_and_bounded_within_epoch() {
        const DISCLOSURE_LIMIT: usize = 4;
        const BOOK_SIZE: usize = 20;

        let mut book = ZakuraDiscoveryBook::new(ZakuraDiscoveryBookLimits {
            max_imported_records_per_response: DISCLOSURE_LIMIT,
            ..ZakuraDiscoveryBookLimits::default()
        });
        for index in 0..BOOK_SIZE {
            let octet = u8::try_from(index + 1).expect("small test index fits in u8");
            let record = long_lived_record_with(1, service(index), test_addr(octet));
            import_confirmed_record(&mut book, record).expect("test record imports");
        }
        let requester = test_requester();

        // Identical queries return the same records, in random order.
        let node_ids = |records: Vec<ZakuraNodeRecord>| -> HashSet<NodeId> {
            records.iter().map(|record| record.body.node_id).collect()
        };
        let first = book.sample_peers(DISCLOSURE_LIMIT, &[], &[], NOW, requester);
        assert_eq!(first.len(), DISCLOSURE_LIMIT);
        let disclosure_set = node_ids(first);
        assert_eq!(
            disclosure_set,
            node_ids(book.sample_peers(DISCLOSURE_LIMIT, &[], &[], NOW, requester))
        );

        // Exclude everything already returned and query each record's unique service id.
        // Nothing outside the epoch's window is ever revealed.
        let mut seen = disclosure_set.clone();
        let mut excluded: Vec<_> = seen.iter().map(|id| *id.as_bytes()).collect();
        excluded.sort_unstable();
        for record in book.sample_peers(DISCLOSURE_LIMIT, &[], &excluded, NOW, requester) {
            seen.insert(record.body.node_id);
        }
        for index in 0..BOOK_SIZE {
            for record in
                book.sample_peers(DISCLOSURE_LIMIT, &[service(index)], &[], NOW, requester)
            {
                seen.insert(record.body.node_id);
            }
        }
        assert_eq!(
            seen, disclosure_set,
            "filters and exclusions must not reveal records outside the window"
        );

        // The set is keyed by requester and epoch. Walking `now` forward one epoch at a time
        // grows the union beyond one epoch's set, so a single requester identity in a single
        // epoch does not pin the whole book's disclosure order.
        let mut across_epochs = disclosure_set.clone();
        let mut now = NOW;
        for _ in 0..48 {
            now = next_epoch(now);
            for record in book.sample_peers(DISCLOSURE_LIMIT, &[], &[], now, requester) {
                across_epochs.insert(record.body.node_id);
            }
        }
        assert!(
            across_epochs.len() > disclosure_set.len(),
            "windows must rotate across epochs"
        );
    }

    /// Samples for one fixed requester, so every call reads the same window.
    fn sampled_node_ids(book: &mut ZakuraDiscoveryBook, now: u64) -> HashSet<NodeId> {
        let requester = NodeSecretKey::from_bytes(&[7; 32]).public();
        book.sample_peers(usize::MAX, &[], &[], now, requester)
            .iter()
            .map(|record| record.body.node_id)
            .collect()
    }

    /// While one window covers the whole snapshot, a newly confirmed entry
    /// enters the next sample in the same epoch. A full snapshot keeps every
    /// window stable until the next epoch, so new entries wait for it.
    #[test]
    fn sample_snapshot_grows_within_an_epoch_only_while_one_window_covers_it() {
        const WINDOW: usize = 4;
        let mut book = ZakuraDiscoveryBook::new(ZakuraDiscoveryBookLimits {
            max_imported_records_per_response: WINDOW,
            ..ZakuraDiscoveryBookLimits::default()
        });

        // A gossiped record is unconfirmed until a dial confirms it.
        let gossiped = long_lived_record_with(1, service(0), test_addr(1));
        let gossiped_id = gossiped.body.node_id;
        book.import_record(gossiped, Some(test_requester()), NOW, &context())
            .expect("test record imports");
        assert!(sampled_node_ids(&mut book, NOW).is_empty());
        book.mark_dial_success(&gossiped_id, NOW);
        assert_eq!(
            sampled_node_ids(&mut book, NOW),
            HashSet::from([gossiped_id])
        );

        for index in 1..=WINDOW {
            let octet = u8::try_from(index + 1).expect("small test index fits in u8");
            import_confirmed_record(
                &mut book,
                long_lived_record_with(1, service(index), test_addr(octet)),
            )
            .expect("test record imports");
        }
        let window = sampled_node_ids(&mut book, NOW);
        assert_eq!(window.len(), WINDOW, "the snapshot now fills one window");
        assert!(
            window.contains(&gossiped_id),
            "an id disclosed earlier in the epoch stays in the window"
        );

        let late = long_lived_record_with(1, service(9), test_addr(50));
        let late_id = late.body.node_id;
        import_confirmed_record(&mut book, late).expect("test record imports");
        assert_eq!(sampled_node_ids(&mut book, NOW), window);

        let mut now = NOW;
        let mut seen_late = false;
        for _ in 0..48 {
            now = next_epoch(now);
            seen_late |= sampled_node_ids(&mut book, now).contains(&late_id);
        }
        assert!(seen_late, "a later epoch's snapshot includes the new entry");
    }

    /// A requester learns at most one window of ids per epoch, even when every
    /// id it learned expires and new candidates replace them.
    #[test]
    fn book_churn_does_not_widen_an_epochs_disclosure() {
        let mut book = ZakuraDiscoveryBook::default();
        let window = MAX_DISCOVERY_RECORDS_PER_RESPONSE;
        let short_lived = (1..=window).map(|index| {
            let octet = u8::try_from(index).expect("small test index fits in u8");
            signed_record_with(octet.into(), service(1), test_addr(octet))
        });
        for record in short_lived {
            import_confirmed_record(&mut book, record).expect("test record imports");
        }
        let mut seen = sampled_node_ids(&mut book, NOW);
        assert_eq!(seen.len(), window);

        // Every record expires at `NOW + 60`, inside the epoch.
        let later = NOW + 61;
        assert_eq!(
            later / DISCOVERY_DISCLOSURE_EPOCH.as_secs(),
            NOW / DISCOVERY_DISCLOSURE_EPOCH.as_secs()
        );
        for index in window + 1..=2 * window {
            let octet = u8::try_from(index).expect("small test index fits in u8");
            import_confirmed_record(
                &mut book,
                long_lived_record_with(octet.into(), service(1), test_addr(octet)),
            )
            .expect("test record imports");
        }
        seen.extend(sampled_node_ids(&mut book, later));
        assert_eq!(seen.len(), window);

        // The next epoch discloses the new candidates.
        let next = sampled_node_ids(&mut book, next_epoch(later));
        assert_eq!(next.len(), window);
        assert!(next.is_disjoint(&seen));
    }

    /// Each sampled entry is rechecked, so one that expires leaves samples
    /// before its snapshot does.
    #[test]
    fn sample_rechecks_snapshot_entries() {
        let mut book = ZakuraDiscoveryBook::new(ZakuraDiscoveryBookLimits::default());
        let short_lived = signed_record_with_addrs(1, service(0), vec![test_addr(1)]);
        let short_lived_id = short_lived.body.node_id;
        let long_lived = long_lived_record_with(1, service(1), test_addr(2));
        let long_lived_id = long_lived.body.node_id;
        import_confirmed_record(&mut book, short_lived).expect("test record imports");
        import_confirmed_record(&mut book, long_lived).expect("test record imports");
        assert_eq!(
            sampled_node_ids(&mut book, NOW),
            HashSet::from([short_lived_id, long_lived_id])
        );

        let expired = NOW + 61;
        assert_eq!(
            expired / DISCOVERY_DISCLOSURE_EPOCH.as_secs(),
            NOW / DISCOVERY_DISCLOSURE_EPOCH.as_secs(),
            "the test stays in one epoch"
        );
        assert_eq!(
            sampled_node_ids(&mut book, expired),
            HashSet::from([long_lived_id])
        );
    }

    /// Guards the bounded top-k dial-candidate selection added for
    /// `claude-discovery-expensive-work-under-global-mutex`.
    ///
    /// `dial_candidates` now selects the best `limit` candidates with a partial select instead of
    /// sorting the whole book before `take(limit)`. With far more matching candidates than `limit`,
    /// the result must still be exactly the highest dial-priority candidates (most recent
    /// successful dial first), proving the partial selection keeps the same ordering as a full
    /// sort.
    #[test]
    fn dial_candidates_selects_top_priority_subset_under_limit() {
        let mut book = ZakuraDiscoveryBook::default();
        let records = (1u8..=10)
            .map(|index| signed_record_with(index.into(), service(1), test_addr(index)))
            .collect::<Vec<_>>();
        for record in &records {
            import_confirmed_record(&mut book, record.clone()).expect("test record imports");
        }

        // Three candidates get strictly increasing last-success times, so dial priority is
        // deterministic (most recent success first); the remaining seven have no successful dial.
        book.mark_dial_success(&records[5].body.node_id, NOW + 1);
        book.mark_dial_success(&records[3].body.node_id, NOW + 2);
        book.mark_dial_success(&records[7].body.node_id, NOW + 3);
        let expected_top = vec![
            records[7].body.node_id,
            records[3].body.node_id,
            records[5].body.node_id,
        ];

        let mut rng = StdRng::seed_from_u64(99);
        let selected = book.dial_candidates(
            3,
            &[service(1)],
            &[],
            DialCandidateExclusions {
                connected_node_ids: &[],
                in_flight_node_ids: &[],
            },
            NOW,
            (
                DEFAULT_DISCOVERY_DIAL_BACKOFF_BASE,
                DEFAULT_DISCOVERY_DIAL_BACKOFF_MAX,
            ),
            &mut rng,
        );

        assert_eq!(
            selected
                .iter()
                .map(|candidate| candidate.node_id)
                .collect::<Vec<_>>(),
            expected_top,
        );
    }

    #[tokio::test]
    async fn handle_samples_records_through_storage_path() {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let wanted = ZakuraServiceId::block_sync();
        let matching = runtime_record_with(1, wanted.clone(), test_addr(1));
        let excluded = runtime_record_with(2, wanted.clone(), test_addr(2));
        let excluded_id = excluded.body.node_id;
        let other = runtime_record_with(3, ZakuraServiceId::legacy_gossip(), test_addr(3));
        for record in [matching.clone(), excluded, other] {
            let author = record.body.node_id;
            import_one_peer_record(&handle, record, Some(author)).await;
        }

        let sample = handle
            .sample_peers(
                secret_key().public(),
                10,
                std::slice::from_ref(&wanted),
                &[*excluded_id.as_bytes()],
            )
            .await;

        assert_eq!(sample, vec![matching]);
    }

    /// A custom (non-built-in) service id in a `GetPeers` filter must not reveal any record
    /// a plain unfiltered query would not. Confirmation gates injected records out of
    /// samples, and the per-requester window bounds the rest: a filter selects
    /// strictly inside the set the requester can already see in full. The filter is
    /// therefore left free-form rather than restricted to an allowlist of built-in ids,
    /// which would add nothing and would silently stop filtering whenever a new service id
    /// ships.
    #[tokio::test]
    async fn handle_sample_hides_unconfirmed_records_from_custom_service_filters() {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let custom_service = service(1);
        let custom_record = runtime_record_with(1, custom_service.clone(), test_addr(1));
        let requester = secret_key().public();

        // A third-party record is invisible to samples regardless of the filter, so a unique
        // service id cannot reveal whether that record reached this node's book.
        handle
            .import_peer_records([custom_record.clone()], Some(secret_key().public()))
            .await
            .expect("authentic test records import");
        assert!(handle
            .sample_peers(requester, 10, std::slice::from_ref(&custom_service), &[])
            .await
            .is_empty());
        assert!(handle
            .sample_peers(requester, 10, &[], &[])
            .await
            .is_empty());

        // Once its owner confirms it first-party, the record is one this node vouches for and
        // the filtered query reveals nothing the unfiltered query does not already return.
        let author = custom_record.body.node_id;
        import_one_peer_record(&handle, custom_record.clone(), Some(author)).await;
        assert_eq!(
            handle
                .sample_peers(requester, 10, std::slice::from_ref(&custom_service), &[])
                .await,
            vec![custom_record.clone()]
        );
        assert_eq!(
            handle.sample_peers(requester, 10, &[], &[]).await,
            vec![custom_record]
        );

        // Non-matching filters still constrain results.
        assert!(handle
            .sample_peers(requester, 10, &[ZakuraServiceId::block_sync()], &[])
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn stale_discovery_admit_cannot_overwrite_newer_session() {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let peer = peer_id_for(secret_key().public());

        assert_eq!(
            handle
                .admit_peer_session(2, 0, peer.clone(), ServicePeerDirection::Inbound)
                .await,
            ServiceAdmissionDecision::Admit
        );
        assert_eq!(handle.peer_snapshot().inbound_peers, 1);

        assert_eq!(
            handle
                .admit_peer_session(1, 0, peer.clone(), ServicePeerDirection::Inbound)
                .await,
            ServiceAdmissionDecision::RejectNotUseful
        );
        assert_eq!(handle.peer_snapshot().inbound_peers, 1);

        handle.remove_peer(&peer, 1).await;
        assert_eq!(
            handle.peer_snapshot().inbound_peers,
            1,
            "stale discovery cleanup must not remove the newer admission",
        );

        handle.remove_peer(&peer, 2).await;
        assert_eq!(handle.peer_snapshot().inbound_peers, 0);
    }

    #[tokio::test]
    async fn stale_discovery_stream_cleanup_cannot_remove_newer_stream_on_same_connection() {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let peer = peer_id_for(secret_key().public());

        assert_eq!(
            handle
                .admit_peer_session(2, 1, peer.clone(), ServicePeerDirection::Inbound)
                .await,
            ServiceAdmissionDecision::Admit
        );
        assert_eq!(
            handle
                .admit_peer_session(2, 2, peer.clone(), ServicePeerDirection::Inbound)
                .await,
            ServiceAdmissionDecision::Admit
        );
        assert_eq!(
            handle
                .admit_peer_session(2, 2, peer.clone(), ServicePeerDirection::Inbound)
                .await,
            ServiceAdmissionDecision::RejectNotUseful
        );

        handle.remove_session(&peer, 2, 1).await;
        assert_eq!(handle.peer_snapshot().inbound_peers, 1);

        handle.remove_session(&peer, 2, 2).await;
        assert_eq!(handle.peer_snapshot().inbound_peers, 0);
    }

    #[tokio::test]
    async fn discovery_state_construction_works_with_default_config() {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = ZakuraDiscoveryHandle::new(
            local_config_with(secret_key(), vec![test_addr(46)], vec![service(1)]),
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )
        .expect("default discovery state constructs");

        assert_eq!(
            handle.current_self_record().body.direct_addrs,
            vec![test_addr(46)]
        );
    }

    #[tokio::test]
    async fn dial_candidates_exclude_connected_peers_and_respect_soft_cap() {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_at(
            local_config_with(secret_key(), vec![test_addr(47)], vec![service(1)]),
            ZakuraDiscoveryConfig {
                max_zakura_connections: 2,
                discovery_connection_headroom: 1,
                ..ZakuraDiscoveryConfig::default()
            },
            connected_rx,
            NOW,
        );
        let candidate = runtime_record_with(1, service(1), test_addr(1));
        let connected = runtime_record_with(2, service(1), test_addr(2));
        let connected_id = connected.body.node_id;
        let candidate_id = candidate.body.node_id;
        import_one_peer_record(&handle, candidate.clone(), Some(candidate_id)).await;
        handle
            .import_connected_peer_record(connected, connected_id)
            .await
            .expect("connected self-record imports");

        connected_tx.send_replace(vec![peer_id_for(connected_id)]);
        assert!(handle.dial_candidates(&[service(1)], &[]).await.is_empty());

        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_at(
            local_config_with(secret_key(), vec![test_addr(48)], vec![service(1)]),
            ZakuraDiscoveryConfig {
                max_zakura_connections: 4,
                discovery_connection_headroom: 1,
                ..ZakuraDiscoveryConfig::default()
            },
            connected_rx,
            NOW,
        );
        let connected = runtime_record_with(2, service(1), test_addr(2));
        let connected_id = connected.body.node_id;
        let candidate_id = candidate.body.node_id;
        import_one_peer_record(&handle, candidate.clone(), Some(candidate_id)).await;
        handle
            .import_connected_peer_record(connected, connected_id)
            .await
            .expect("connected self-record imports");

        connected_tx.send_replace(vec![peer_id_for(connected_id)]);
        assert_eq!(
            handle.dial_candidates(&[service(1)], &[]).await,
            vec![candidate_for(&candidate, false)]
        );
    }

    #[tokio::test]
    async fn dial_candidates_exclude_in_flight_peers() {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let candidate = runtime_record_with(1, service(1), test_addr(1));
        let candidate_id = candidate.body.node_id;
        import_one_peer_record(&handle, candidate, Some(secret_key().public())).await;

        assert!(handle
            .dial_candidates(&[service(1)], &[candidate_id])
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn dial_candidates_prefer_any_matching_service() {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let wanted_a = service(1);
        let wanted_b = service(2);
        let matching_a = runtime_record_with(1, wanted_a.clone(), test_addr(1));
        let matching_b = runtime_record_with(2, wanted_b.clone(), test_addr(2));
        let neither = runtime_record_with(3, service(3), test_addr(3));

        for record in [&matching_a, &matching_b, &neither] {
            import_one_peer_record(&handle, record.clone(), Some(record.body.node_id)).await;
        }

        assert!(handle
            .dial_candidates(&[wanted_a, wanted_b], &[])
            .await
            .is_empty());

        let prioritized = handle
            .dial_candidates_preferring_any_service(&[service(1), service(2)], &[])
            .await;
        assert_eq!(prioritized.len(), 3);
        assert_eq!(
            prioritized[..2]
                .iter()
                .map(|candidate| candidate.node_id)
                .collect::<HashSet<_>>(),
            HashSet::from([matching_a.body.node_id, matching_b.body.node_id])
        );
        assert_eq!(prioritized[2].node_id, neither.body.node_id);

        let general = handle
            .dial_candidates_preferring_any_service(&[], &[])
            .await;
        assert_eq!(
            general
                .iter()
                .map(|candidate| candidate.node_id)
                .collect::<HashSet<_>>(),
            HashSet::from([
                matching_a.body.node_id,
                matching_b.body.node_id,
                neither.body.node_id,
            ])
        );
    }

    #[test]
    fn discovery_dial_slot_limit_reserves_headroom_and_concurrent_cap() {
        assert_eq!(discovery_dial_slot_limit(0, 0, 8, 2, 4), 4);
        assert_eq!(discovery_dial_slot_limit(5, 0, 8, 2, 4), 1);
        assert_eq!(discovery_dial_slot_limit(5, 1, 8, 2, 4), 1);
        assert_eq!(discovery_dial_slot_limit(2, 4, 8, 2, 4), 0);
        assert_eq!(discovery_dial_slot_limit(2, 0, 2, 4, 4), 0);
    }

    #[tokio::test]
    async fn handle_records_are_owned_after_storage_changes() {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handle = discovery_handle_with_connected(connected_rx);
        let secret = secret_key();
        let old = {
            let mut record_body = body(&secret);
            record_body.sequence = 1;
            record_body.direct_addrs = vec![test_addr(10)];
            record_body.services = vec![service(1)];
            record_body.expires_at_unix_secs =
                current_unix_secs() + DEFAULT_DISCOVERY_RECORD_TTL.as_secs();
            ZakuraNodeRecord::sign(record_body, &secret).expect("old record signs")
        };
        let new = {
            let mut record_body = body(&secret);
            record_body.sequence = 2;
            record_body.direct_addrs = vec![test_addr(11)];
            record_body.services = vec![service(1)];
            record_body.expires_at_unix_secs =
                current_unix_secs() + DEFAULT_DISCOVERY_RECORD_TTL.as_secs();
            ZakuraNodeRecord::sign(record_body, &secret).expect("new record signs")
        };

        let author = old.body.node_id;
        handle
            .import_peer_records([old.clone()], Some(author))
            .await
            .expect("authentic test records import");
        let sample = handle
            .sample_peers(secret_key().public(), 1, &[], &[])
            .await;
        handle
            .import_peer_records([new], Some(author))
            .await
            .expect("authentic test records import");

        assert_eq!(sample, vec![old]);
    }

    #[test]
    fn hello_verdicts_disconnect_only_for_records_no_conformant_peer_sends() {
        use crate::zakura::discovery::protocol::DiscoveryWireError;

        let disconnect = [
            DiscoveryBookError::Record(DiscoveryRecordError::Wire(
                DiscoveryWireError::InvalidProtocolRange,
            )),
            DiscoveryBookError::Record(DiscoveryRecordError::MalformedNodeId),
            DiscoveryBookError::Record(DiscoveryRecordError::InvalidSignature),
            DiscoveryBookError::Record(DiscoveryRecordError::WrongNetwork),
            DiscoveryBookError::Record(DiscoveryRecordError::WrongChain),
            DiscoveryBookError::Record(DiscoveryRecordError::IncompatibleProtocol),
            DiscoveryBookError::MismatchedConnectedPeerRecord,
        ];
        for error in disconnect {
            assert_eq!(hello_verdict(&error), RecordVerdict::Disconnect, "{error}");
        }

        let drop = [
            DiscoveryBookError::Record(DiscoveryRecordError::Expired),
            DiscoveryBookError::Record(DiscoveryRecordError::FarFutureExpiry),
            DiscoveryBookError::SelfRecord,
            DiscoveryBookError::NoUsableDirectAddress,
            DiscoveryBookError::NonDialableDirectAddress {
                addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8233),
            },
            DiscoveryBookError::Limit {
                field: "service",
                actual: 9,
                max: 8,
            },
            DiscoveryBookError::VerificationCancelled,
        ];
        for error in drop {
            assert_eq!(hello_verdict(&error), RecordVerdict::Drop, "{error}");
        }
    }
}
