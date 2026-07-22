//! Version-one value codecs for the fork-aware header-chain column families.

#![allow(dead_code)] // The serialized state adapter consumes these codecs in PR-8.

use std::{num::NonZeroU64, sync::Arc};

use chrono::{DateTime, TimeZone, Utc};
use thiserror::Error;
use zakura_chain::{
    block,
    parameters::NetworkKind,
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    work::difficulty::U256,
};
use zakura_header_chain::{
    AlarmSet, AuxAuthentication, AuxDelivery, BodyRuleId, BodySizeHint, BodyUnavailableSummary,
    BodyValidationState, BranchId, ChainScore, EligibilityReason, EligibilityState, EngineMetadata,
    EngineMode, EvidenceId, FinalityEpoch, FinalityRecord, FinalitySource, Frontier, FrontierSet,
    HeaderChainDiskVersion, HeaderContextFact, HeaderGeneration, HeaderNode, HeaderValidationState,
    OperatorInvalidationId, SourceId, StateVersion, SuffixWork, VerifiedGeneration, WorkCoordinate,
    WorkOwner,
};

const MAX_HEADER_BYTES: usize = 2 * 1024;
const MAX_RULE_ID_BYTES: usize = 128;

/// Malformed, truncated, oversized, or unknown version-one value data.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum HeaderChainValueError {
    /// A value ended before a complete field could be read.
    #[error("truncated header-chain value")]
    Truncated,
    /// Bytes remained after decoding the one expected value.
    #[error("trailing bytes in header-chain value")]
    Trailing,
    /// A length prefix exceeded its field's version-one bound.
    #[error("header-chain value field {field} has oversized length {length}")]
    Oversized {
        /// Stable field name.
        field: &'static str,
        /// Supplied byte count.
        length: usize,
    },
    /// A stable enum discriminant was not assigned in version one.
    #[error("unknown {field} discriminant {value}")]
    UnknownDiscriminant {
        /// Stable enum name.
        field: &'static str,
        /// Supplied discriminant.
        value: u8,
    },
    /// A boolean byte was neither zero nor one.
    #[error("invalid boolean byte {0}")]
    InvalidBoolean(u8),
    /// A nonzero field contained zero.
    #[error("zero in nonzero field {0}")]
    Zero(&'static str),
    /// A canonical Zcash header failed decoding or had trailing bytes.
    #[error("invalid canonical Zcash header")]
    Header,
    /// The redundant node hash disagreed with its canonical header.
    #[error("header-node hash does not match its canonical header")]
    HeaderHashMismatch,
    /// The singleton metadata used an unsupported disk format.
    #[error("unsupported header-chain disk format {0}")]
    UnsupportedDiskFormat(u32),
    /// A UTC seconds/nanoseconds pair was outside chrono's supported range.
    #[error("invalid UTC timestamp")]
    Timestamp,
    /// A UTF-8 rule identifier was malformed.
    #[error("invalid UTF-8 rule identifier")]
    RuleId,
}

/// Explicit stable value codec used instead of implementation-derived serialization.
pub trait HeaderChainValue: Sized {
    /// Encode one complete version-one value.
    fn encode(&self) -> Result<Vec<u8>, HeaderChainValueError>;
    /// Decode one complete version-one value, rejecting trailing bytes.
    fn decode(bytes: &[u8]) -> Result<Self, HeaderChainValueError>;
}

#[derive(Default)]
struct Encoder(Vec<u8>);

impl Encoder {
    fn u8(&mut self, value: u8) {
        self.0.push(value);
    }
    fn bool(&mut self, value: bool) {
        self.u8(u8::from(value));
    }
    fn u32(&mut self, value: u32) {
        self.0.extend(value.to_be_bytes());
    }
    fn u64(&mut self, value: u64) {
        self.0.extend(value.to_be_bytes());
    }
    fn i64(&mut self, value: i64) {
        self.0.extend(value.to_be_bytes());
    }
    fn fixed(&mut self, value: &[u8]) {
        self.0.extend(value);
    }
    fn bounded(
        &mut self,
        field: &'static str,
        value: &[u8],
        maximum: usize,
    ) -> Result<(), HeaderChainValueError> {
        if value.len() > maximum {
            return Err(HeaderChainValueError::Oversized {
                field,
                length: value.len(),
            });
        }
        let length = u32::try_from(value.len()).map_err(|_| HeaderChainValueError::Oversized {
            field,
            length: value.len(),
        })?;
        self.u32(length);
        self.fixed(value);
        Ok(())
    }
}

struct Decoder<'a> {
    remaining: &'a [u8],
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }
    fn take(&mut self, length: usize) -> Result<&'a [u8], HeaderChainValueError> {
        if self.remaining.len() < length {
            return Err(HeaderChainValueError::Truncated);
        }
        let (value, remaining) = self.remaining.split_at(length);
        self.remaining = remaining;
        Ok(value)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N], HeaderChainValueError> {
        self.take(N)?
            .try_into()
            .map_err(|_| HeaderChainValueError::Truncated)
    }
    fn u8(&mut self) -> Result<u8, HeaderChainValueError> {
        Ok(self.array::<1>()?[0])
    }
    fn bool(&mut self) -> Result<bool, HeaderChainValueError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(HeaderChainValueError::InvalidBoolean(other)),
        }
    }
    fn u32(&mut self) -> Result<u32, HeaderChainValueError> {
        Ok(u32::from_be_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64, HeaderChainValueError> {
        Ok(u64::from_be_bytes(self.array()?))
    }
    fn i64(&mut self) -> Result<i64, HeaderChainValueError> {
        Ok(i64::from_be_bytes(self.array()?))
    }
    fn bounded(
        &mut self,
        field: &'static str,
        maximum: usize,
    ) -> Result<&'a [u8], HeaderChainValueError> {
        let raw_length = self.u32()?;
        let length = usize::try_from(raw_length).map_err(|_| HeaderChainValueError::Oversized {
            field,
            length: usize::MAX,
        })?;
        if length > maximum {
            return Err(HeaderChainValueError::Oversized { field, length });
        }
        self.take(length)
    }
    fn finish(self) -> Result<(), HeaderChainValueError> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(HeaderChainValueError::Trailing)
        }
    }
}

fn put_frontier(encoder: &mut Encoder, frontier: Frontier) {
    encoder.u32(frontier.height.0);
    encoder.fixed(&frontier.hash.0);
}

fn get_frontier(decoder: &mut Decoder<'_>) -> Result<Frontier, HeaderChainValueError> {
    Ok(Frontier::new(
        block::Height(decoder.u32()?),
        block::Hash(decoder.array()?),
    ))
}

fn put_time(encoder: &mut Encoder, time: DateTime<Utc>) {
    encoder.i64(time.timestamp());
    encoder.u32(time.timestamp_subsec_nanos());
}

fn get_time(decoder: &mut Decoder<'_>) -> Result<DateTime<Utc>, HeaderChainValueError> {
    Utc.timestamp_opt(decoder.i64()?, decoder.u32()?)
        .single()
        .ok_or(HeaderChainValueError::Timestamp)
}

/// Direct eligibility reason stored under one eligibility-root key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeaderEligibilityReasonDisk {
    /// Compiled settled-upgrade conflict.
    SettledUpgrade {
        /// Conflicting height.
        height: block::Height,
        /// Required hash.
        expected: block::Hash,
    },
    /// Authenticated local checkpoint conflict.
    LocalCheckpoint {
        /// Conflicting height.
        height: block::Height,
        /// Required hash.
        expected: block::Hash,
    },
    /// Immutable finality conflict.
    Finality(Frontier),
    /// Deterministic body consensus failure.
    ConsensusBody {
        /// Verifier evidence.
        evidence: EvidenceId,
        /// Stable consensus rule ID.
        rule: String,
    },
    /// Reversible operator invalidation.
    Operator([u8; 16]),
}

impl HeaderEligibilityReasonDisk {
    /// Convert one direct domain reason into its stable disk value.
    pub fn from_domain(reason: &EligibilityReason) -> Self {
        match reason {
            EligibilityReason::SettledUpgradeConflict { height, expected } => {
                Self::SettledUpgrade {
                    height: *height,
                    expected: *expected,
                }
            }
            EligibilityReason::CheckpointConflict { height, expected } => Self::LocalCheckpoint {
                height: *height,
                expected: *expected,
            },
            EligibilityReason::FinalityConflict { finalized } => Self::Finality(*finalized),
            EligibilityReason::ConsensusBodyInvalid { evidence, rule } => Self::ConsensusBody {
                evidence: *evidence,
                rule: rule.as_str().to_owned(),
            },
            EligibilityReason::OperatorInvalid { id } => Self::Operator(id.bytes()),
        }
    }

    /// Convert one decoded disk reason into its domain representation.
    pub fn into_domain(self) -> EligibilityReason {
        match self {
            Self::SettledUpgrade { height, expected } => {
                EligibilityReason::SettledUpgradeConflict { height, expected }
            }
            Self::LocalCheckpoint { height, expected } => {
                EligibilityReason::CheckpointConflict { height, expected }
            }
            Self::Finality(finalized) => EligibilityReason::FinalityConflict { finalized },
            Self::ConsensusBody { evidence, rule } => EligibilityReason::ConsensusBodyInvalid {
                evidence,
                rule: BodyRuleId::new(rule),
            },
            Self::Operator(bytes) => EligibilityReason::OperatorInvalid {
                id: OperatorInvalidationId::new(bytes),
            },
        }
    }
}

impl HeaderChainValue for HeaderEligibilityReasonDisk {
    fn encode(&self) -> Result<Vec<u8>, HeaderChainValueError> {
        let mut encoder = Encoder::default();
        match self {
            Self::SettledUpgrade { height, expected }
            | Self::LocalCheckpoint { height, expected } => {
                encoder.u8(if matches!(self, Self::SettledUpgrade { .. }) {
                    0
                } else {
                    1
                });
                encoder.u32(height.0);
                encoder.fixed(&expected.0);
            }
            Self::Finality(frontier) => {
                encoder.u8(2);
                put_frontier(&mut encoder, *frontier);
            }
            Self::ConsensusBody { evidence, rule } => {
                encoder.u8(3);
                encoder.fixed(&evidence.digest());
                encoder.bounded("body_rule", rule.as_bytes(), MAX_RULE_ID_BYTES)?;
            }
            Self::Operator(id) => {
                encoder.u8(4);
                encoder.fixed(id);
            }
        }
        Ok(encoder.0)
    }

    fn decode(bytes: &[u8]) -> Result<Self, HeaderChainValueError> {
        let mut decoder = Decoder::new(bytes);
        let value = match decoder.u8()? {
            tag @ (0 | 1) => {
                let height = block::Height(decoder.u32()?);
                let expected = block::Hash(decoder.array()?);
                if tag == 0 {
                    Self::SettledUpgrade { height, expected }
                } else {
                    Self::LocalCheckpoint { height, expected }
                }
            }
            2 => Self::Finality(get_frontier(&mut decoder)?),
            3 => Self::ConsensusBody {
                evidence: EvidenceId::from_digest(decoder.array()?),
                rule: std::str::from_utf8(decoder.bounded("body_rule", MAX_RULE_ID_BYTES)?)
                    .map_err(|_| HeaderChainValueError::RuleId)?
                    .to_owned(),
            },
            4 => Self::Operator(decoder.array()?),
            value => {
                return Err(HeaderChainValueError::UnknownDiscriminant {
                    field: "eligibility_reason",
                    value,
                });
            }
        };
        decoder.finish()?;
        Ok(value)
    }
}

/// Body state stored inside one node value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeaderBodyStateDisk {
    /// No body conclusion.
    Unknown,
    /// Header/body commitments matched.
    CommitmentMatched,
    /// Full-state acceptance evidence.
    Verified(EvidenceId),
    /// Deterministic body invalidity and stable rule ID.
    ConsensusInvalid { evidence: EvidenceId, rule: String },
    /// Retry episode summary with no eligibility effect.
    Unavailable(BodyUnavailableSummary),
}

/// One node row without reconstructible child or candidate lists.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderNodeDisk {
    /// Canonical Zcash header.
    pub header: Arc<block::Header>,
    /// Redundant hash checked against the row key and canonical header.
    pub hash: block::Hash,
    /// Exact parent hash.
    pub parent_hash: block::Hash,
    /// Locally inferred height.
    pub height: block::Height,
    /// Exact block work as 256-bit big-endian bytes on disk.
    pub block_work: U256,
    /// Immutable work-coordinate origin.
    pub work_origin: block::Hash,
    /// Exact cumulative coordinate.
    pub cumulative_work: U256,
    /// Optional local future-admission instant.
    pub deferred_until: Option<DateTime<Utc>>,
    /// Cached nearest ineligible ancestor.
    pub inherited_from: Option<block::Hash>,
    /// Body evidence state.
    pub body: HeaderBodyStateDisk,
    /// Bounded hash-keyed auxiliary delivery IDs.
    pub aux_delivery_ids: Vec<EvidenceId>,
}

impl HeaderNodeDisk {
    /// Convert one domain node into its version-one durable representation.
    pub fn from_domain(node: &HeaderNode) -> Self {
        let body = match &node.body {
            BodyValidationState::Unknown => HeaderBodyStateDisk::Unknown,
            BodyValidationState::CommitmentMatched => HeaderBodyStateDisk::CommitmentMatched,
            BodyValidationState::Verified { evidence } => HeaderBodyStateDisk::Verified(*evidence),
            BodyValidationState::ConsensusInvalid { evidence, rule } => {
                HeaderBodyStateDisk::ConsensusInvalid {
                    evidence: *evidence,
                    rule: rule.as_str().to_owned(),
                }
            }
            BodyValidationState::Unavailable(summary) => HeaderBodyStateDisk::Unavailable(*summary),
        };
        Self {
            header: node.header.clone(),
            hash: node.hash,
            parent_hash: node.parent_hash,
            height: node.height,
            block_work: node.block_work.as_u256(),
            work_origin: node.work_coordinate().origin_hash(),
            cumulative_work: node.work_coordinate().cumulative_work(),
            deferred_until: match node.validation {
                HeaderValidationState::Valid => None,
                HeaderValidationState::DeferredUntil(until) => Some(until),
            },
            inherited_from: node.eligibility.inherited_from,
            body,
            aux_delivery_ids: node.aux_delivery_ids.clone(),
        }
    }

    /// Reconstruct one domain node after its direct-reason rows were decoded.
    pub fn into_domain(
        self,
        direct_reasons: impl IntoIterator<Item = EligibilityReason>,
    ) -> Result<HeaderNode, HeaderChainValueError> {
        let block_work = self
            .header
            .difficulty_threshold
            .to_work()
            .filter(|work| work.as_u256() == self.block_work)
            .ok_or(HeaderChainValueError::Header)?;
        let body = match self.body {
            HeaderBodyStateDisk::Unknown => BodyValidationState::Unknown,
            HeaderBodyStateDisk::CommitmentMatched => BodyValidationState::CommitmentMatched,
            HeaderBodyStateDisk::Verified(evidence) => BodyValidationState::Verified { evidence },
            HeaderBodyStateDisk::ConsensusInvalid { evidence, rule } => {
                BodyValidationState::ConsensusInvalid {
                    evidence,
                    rule: BodyRuleId::new(rule),
                }
            }
            HeaderBodyStateDisk::Unavailable(summary) => BodyValidationState::Unavailable(summary),
        };
        HeaderNode::from_durable_parts(
            self.header,
            self.hash,
            self.parent_hash,
            self.height,
            block_work,
            WorkCoordinate::new(self.work_origin, self.cumulative_work),
            self.deferred_until.map_or(
                HeaderValidationState::Valid,
                HeaderValidationState::DeferredUntil,
            ),
            EligibilityState {
                direct_reasons: direct_reasons.into_iter().collect(),
                inherited_from: self.inherited_from,
            },
            body,
            self.aux_delivery_ids,
        )
        .map_err(|_| HeaderChainValueError::Header)
    }
}

impl HeaderChainValue for HeaderNodeDisk {
    fn encode(&self) -> Result<Vec<u8>, HeaderChainValueError> {
        let mut encoder = Encoder::default();
        let header = self
            .header
            .zcash_serialize_to_vec()
            .map_err(|_| HeaderChainValueError::Header)?;
        encoder.bounded("header", &header, MAX_HEADER_BYTES)?;
        encoder.fixed(&self.hash.0);
        encoder.fixed(&self.parent_hash.0);
        encoder.u32(self.height.0);
        encoder.fixed(&self.block_work.to_big_endian());
        encoder.fixed(&self.work_origin.0);
        encoder.fixed(&self.cumulative_work.to_big_endian());
        encoder.bool(self.deferred_until.is_some());
        if let Some(time) = self.deferred_until {
            put_time(&mut encoder, time);
        }
        encoder.bool(self.inherited_from.is_some());
        if let Some(hash) = self.inherited_from {
            encoder.fixed(&hash.0);
        }
        put_body(&mut encoder, &self.body)?;
        encoder.u32(u32::try_from(self.aux_delivery_ids.len()).map_err(|_| {
            HeaderChainValueError::Oversized {
                field: "aux_delivery_ids",
                length: self.aux_delivery_ids.len(),
            }
        })?);
        for id in &self.aux_delivery_ids {
            encoder.fixed(&id.digest());
        }
        Ok(encoder.0)
    }

    fn decode(bytes: &[u8]) -> Result<Self, HeaderChainValueError> {
        let mut decoder = Decoder::new(bytes);
        let header_bytes = decoder.bounded("header", MAX_HEADER_BYTES)?;
        let header: block::Header = header_bytes
            .zcash_deserialize_into()
            .map_err(|_| HeaderChainValueError::Header)?;
        let hash = block::Hash(decoder.array()?);
        if header.hash() != hash
            || header
                .zcash_serialize_to_vec()
                .map_err(|_| HeaderChainValueError::Header)?
                != header_bytes
        {
            return Err(HeaderChainValueError::HeaderHashMismatch);
        }
        let parent_hash = block::Hash(decoder.array()?);
        let height = block::Height(decoder.u32()?);
        let block_work = U256::from_big_endian(&decoder.array::<32>()?);
        let work_origin = block::Hash(decoder.array()?);
        let cumulative_work = U256::from_big_endian(&decoder.array::<32>()?);
        let deferred_until = decoder
            .bool()?
            .then(|| get_time(&mut decoder))
            .transpose()?;
        let inherited_from = decoder
            .bool()?
            .then(|| decoder.array().map(block::Hash))
            .transpose()?;
        let body = get_body(&mut decoder)?;
        let count =
            usize::try_from(decoder.u32()?).map_err(|_| HeaderChainValueError::Oversized {
                field: "aux_delivery_ids",
                length: usize::MAX,
            })?;
        if count > zakura_chain::parameters::MAX_NON_FINALIZED_CHAIN_FORKS * 16 {
            return Err(HeaderChainValueError::Oversized {
                field: "aux_delivery_ids",
                length: count,
            });
        }
        let mut aux_delivery_ids = Vec::with_capacity(count);
        for _ in 0..count {
            aux_delivery_ids.push(EvidenceId::from_digest(decoder.array()?));
        }
        decoder.finish()?;
        Ok(Self {
            header: Arc::new(header),
            hash,
            parent_hash,
            height,
            block_work,
            work_origin,
            cumulative_work,
            deferred_until,
            inherited_from,
            body,
            aux_delivery_ids,
        })
    }
}

fn put_body(
    encoder: &mut Encoder,
    body: &HeaderBodyStateDisk,
) -> Result<(), HeaderChainValueError> {
    match body {
        HeaderBodyStateDisk::Unknown => encoder.u8(0),
        HeaderBodyStateDisk::CommitmentMatched => encoder.u8(1),
        HeaderBodyStateDisk::Verified(evidence) => {
            encoder.u8(2);
            encoder.fixed(&evidence.digest());
        }
        HeaderBodyStateDisk::ConsensusInvalid { evidence, rule } => {
            encoder.u8(3);
            encoder.fixed(&evidence.digest());
            encoder.bounded("body_rule", rule.as_bytes(), MAX_RULE_ID_BYTES)?;
        }
        HeaderBodyStateDisk::Unavailable(summary) => {
            encoder.u8(4);
            encoder.u32(summary.attempts);
            encoder.u32(summary.suppliers);
            encoder.bool(summary.alarmed);
        }
    }
    Ok(())
}

fn get_body(decoder: &mut Decoder<'_>) -> Result<HeaderBodyStateDisk, HeaderChainValueError> {
    match decoder.u8()? {
        0 => Ok(HeaderBodyStateDisk::Unknown),
        1 => Ok(HeaderBodyStateDisk::CommitmentMatched),
        2 => Ok(HeaderBodyStateDisk::Verified(EvidenceId::from_digest(
            decoder.array()?,
        ))),
        3 => Ok(HeaderBodyStateDisk::ConsensusInvalid {
            evidence: EvidenceId::from_digest(decoder.array()?),
            rule: std::str::from_utf8(decoder.bounded("body_rule", MAX_RULE_ID_BYTES)?)
                .map_err(|_| HeaderChainValueError::RuleId)?
                .to_owned(),
        }),
        4 => Ok(HeaderBodyStateDisk::Unavailable(BodyUnavailableSummary {
            attempts: decoder.u32()?,
            suppliers: decoder.u32()?,
            alarmed: decoder.bool()?,
        })),
        value => Err(HeaderChainValueError::UnknownDiscriminant {
            field: "body_state",
            value,
        }),
    }
}

/// Hash-keyed auxiliary delivery value with complete provenance.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HeaderAuxDeliveryDisk(pub AuxDelivery);

impl HeaderChainValue for HeaderAuxDeliveryDisk {
    fn encode(&self) -> Result<Vec<u8>, HeaderChainValueError> {
        let mut encoder = Encoder::default();
        put_aux(&mut encoder, self.0);
        Ok(encoder.0)
    }

    fn decode(bytes: &[u8]) -> Result<Self, HeaderChainValueError> {
        let mut decoder = Decoder::new(bytes);
        let value = Self(get_aux(&mut decoder)?);
        decoder.finish()?;
        Ok(value)
    }
}

fn put_aux(encoder: &mut Encoder, value: AuxDelivery) {
    encoder.fixed(&value.delivery_id.digest());
    encoder.fixed(&value.header_hash.0);
    encoder.fixed(&value.source.digest());
    put_owner(encoder, value.owner);
    encoder.u32(match value.body_size {
        BodySizeHint::Unknown => 0,
        BodySizeHint::Known(size) => size.get(),
    });
    encoder.bool(value.payload_digest.is_some());
    if let Some(digest) = value.payload_digest {
        encoder.fixed(&digest);
    }
    match value.authentication {
        AuxAuthentication::Unauthenticated => encoder.u8(0),
        AuxAuthentication::Authenticated {
            evidence,
            boundary_hash,
        } => {
            encoder.u8(1);
            encoder.fixed(&evidence.digest());
            encoder.fixed(&boundary_hash.0);
        }
        AuxAuthentication::Rejected { evidence } => {
            encoder.u8(2);
            encoder.fixed(&evidence.digest());
        }
    }
}

fn get_aux(decoder: &mut Decoder<'_>) -> Result<AuxDelivery, HeaderChainValueError> {
    let delivery_id = EvidenceId::from_digest(decoder.array()?);
    let header_hash = block::Hash(decoder.array()?);
    let source = SourceId::from_digest(decoder.array()?);
    let owner = get_owner(decoder)?;
    let body_size =
        BodySizeHint::new(decoder.u32()?).map_err(|_| HeaderChainValueError::Oversized {
            field: "body_size",
            length: usize::MAX,
        })?;
    let payload_digest = decoder.bool()?.then(|| decoder.array()).transpose()?;
    let authentication = match decoder.u8()? {
        0 => AuxAuthentication::Unauthenticated,
        1 => AuxAuthentication::Authenticated {
            evidence: EvidenceId::from_digest(decoder.array()?),
            boundary_hash: block::Hash(decoder.array()?),
        },
        2 => AuxAuthentication::Rejected {
            evidence: EvidenceId::from_digest(decoder.array()?),
        },
        value => {
            return Err(HeaderChainValueError::UnknownDiscriminant {
                field: "aux_authentication",
                value,
            });
        }
    };
    Ok(AuxDelivery {
        delivery_id,
        header_hash,
        source,
        owner,
        body_size,
        payload_digest,
        authentication,
    })
}

fn put_owner(encoder: &mut Encoder, owner: WorkOwner) {
    encoder.u64(owner.state_version.get());
    encoder.u64(owner.header_generation.get());
    encoder.bool(owner.verified_generation.is_some());
    if let Some(generation) = owner.verified_generation {
        encoder.u64(generation.get());
    }
    encoder.fixed(&owner.branch.anchor_hash.0);
    encoder.fixed(&owner.branch.target_tip_hash.0);
    encoder.u64(owner.session_id);
    encoder.u64(owner.request_id.get());
}

fn get_owner(decoder: &mut Decoder<'_>) -> Result<WorkOwner, HeaderChainValueError> {
    let state_version = StateVersion::new(decoder.u64()?);
    let header_generation = HeaderGeneration::new(decoder.u64()?);
    let verified_generation = decoder
        .bool()?
        .then(|| decoder.u64().map(VerifiedGeneration::new))
        .transpose()?;
    let branch = BranchId::new(block::Hash(decoder.array()?), block::Hash(decoder.array()?));
    let session_id = decoder.u64()?;
    let request_id =
        NonZeroU64::new(decoder.u64()?).ok_or(HeaderChainValueError::Zero("request_id"))?;
    Ok(WorkOwner {
        state_version,
        header_generation,
        verified_generation,
        branch,
        session_id,
        request_id,
    })
}

/// Append-only finality-history value.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HeaderFinalityRecordDisk(pub FinalityRecord);

impl HeaderChainValue for HeaderFinalityRecordDisk {
    fn encode(&self) -> Result<Vec<u8>, HeaderChainValueError> {
        let mut encoder = Encoder::default();
        put_frontier(&mut encoder, self.0.previous);
        put_frontier(&mut encoder, self.0.current);
        match self.0.source {
            FinalitySource::FullState { evidence } => {
                encoder.u8(0);
                encoder.fixed(&evidence.digest());
            }
            FinalitySource::HeadersOnlyDepth { selected_tip } => {
                encoder.u8(1);
                put_frontier(&mut encoder, selected_tip);
            }
            FinalitySource::MigratedHeadersOnly => encoder.u8(2),
        }
        encoder.u64(self.0.epoch.get());
        Ok(encoder.0)
    }

    fn decode(bytes: &[u8]) -> Result<Self, HeaderChainValueError> {
        let mut decoder = Decoder::new(bytes);
        let previous = get_frontier(&mut decoder)?;
        let current = get_frontier(&mut decoder)?;
        let source = match decoder.u8()? {
            0 => FinalitySource::FullState {
                evidence: EvidenceId::from_digest(decoder.array()?),
            },
            1 => FinalitySource::HeadersOnlyDepth {
                selected_tip: get_frontier(&mut decoder)?,
            },
            2 => FinalitySource::MigratedHeadersOnly,
            value => {
                return Err(HeaderChainValueError::UnknownDiscriminant {
                    field: "finality_source",
                    value,
                });
            }
        };
        let epoch = FinalityEpoch::new(decoder.u64()?);
        decoder.finish()?;
        Ok(Self(FinalityRecord {
            previous,
            current,
            source,
            epoch,
        }))
    }
}

/// Immutable canonical predecessor below the selectable finalized anchor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderValidationContextDisk {
    /// Canonical context header, including its backward link.
    pub header: Arc<block::Header>,
    /// Locally authenticated height of this context header.
    pub height: block::Height,
}

impl HeaderValidationContextDisk {
    /// Return the contextual validation fact authenticated by this row.
    pub fn fact(&self) -> HeaderContextFact {
        HeaderContextFact {
            frontier: Frontier::new(self.height, self.header.hash()),
            difficulty_threshold: self.header.difficulty_threshold,
            time: self.header.time,
        }
    }
}

impl HeaderChainValue for HeaderValidationContextDisk {
    fn encode(&self) -> Result<Vec<u8>, HeaderChainValueError> {
        let mut encoder = Encoder::default();
        let header = self
            .header
            .zcash_serialize_to_vec()
            .map_err(|_| HeaderChainValueError::Header)?;
        encoder.bounded("context_header", &header, MAX_HEADER_BYTES)?;
        encoder.u32(self.height.0);
        Ok(encoder.0)
    }

    fn decode(bytes: &[u8]) -> Result<Self, HeaderChainValueError> {
        let mut decoder = Decoder::new(bytes);
        let header: block::Header = decoder
            .bounded("context_header", MAX_HEADER_BYTES)?
            .zcash_deserialize_into()
            .map_err(|_| HeaderChainValueError::Header)?;
        let height = block::Height(decoder.u32()?);
        decoder.finish()?;
        Ok(Self {
            header: Arc::new(header),
            height,
        })
    }
}

/// Singleton authoritative engine-metadata value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderEngineMetadataDisk(pub EngineMetadata);

impl HeaderChainValue for HeaderEngineMetadataDisk {
    fn encode(&self) -> Result<Vec<u8>, HeaderChainValueError> {
        let value = &self.0;
        let mut encoder = Encoder::default();
        encoder.u32(value.disk_format.0);
        encoder.u8(match value.mode {
            EngineMode::Integrated => 0,
            EngineMode::HeadersOnly => 1,
        });
        encoder.u8(match value.network_id {
            NetworkKind::Mainnet => 0,
            NetworkKind::Testnet => 1,
            NetworkKind::Regtest => 2,
        });
        encoder.fixed(&value.anchor_manifest_digest);
        put_frontier(&mut encoder, value.work_origin);
        encoder.u64(value.state_version.get());
        encoder.u64(value.header_generation.get());
        encoder.u64(value.verified_generation.get());
        encoder.u64(value.finality_epoch.get());
        put_frontier(&mut encoder, value.frontiers.finalized);
        put_frontier(&mut encoder, value.frontiers.header_best);
        put_frontier(&mut encoder, value.frontiers.verified_best);
        encoder.fixed(
            &value
                .header_best_score
                .suffix_work
                .as_u256()
                .to_big_endian(),
        );
        encoder.fixed(&value.header_best_score.tip_hash.0);
        encoder.u32(value.oldest_retained_height.0);
        encoder.bool(value.alarms.resource_stalled);
        encoder.bool(value.alarms.header_best_body_unavailable.is_some());
        if let Some(summary) = value.alarms.header_best_body_unavailable {
            encoder.u32(summary.attempts);
            encoder.u32(summary.suppliers);
            encoder.bool(summary.alarmed);
        }
        encoder.fixed(&value.last_transition_id.digest());
        encoder.bool(value.alarms.migrated_pin_refuted.is_some());
        if let Some(pin) = value.alarms.migrated_pin_refuted {
            put_frontier(&mut encoder, pin);
        }
        Ok(encoder.0)
    }

    fn decode(bytes: &[u8]) -> Result<Self, HeaderChainValueError> {
        let mut decoder = Decoder::new(bytes);
        let disk_format = decoder.u32()?;
        if disk_format != 1 {
            return Err(HeaderChainValueError::UnsupportedDiskFormat(disk_format));
        }
        let disk_format = HeaderChainDiskVersion(disk_format);
        let mode = match decoder.u8()? {
            0 => EngineMode::Integrated,
            1 => EngineMode::HeadersOnly,
            value => {
                return Err(HeaderChainValueError::UnknownDiscriminant {
                    field: "engine_mode",
                    value,
                })
            }
        };
        let network_id = match decoder.u8()? {
            0 => NetworkKind::Mainnet,
            1 => NetworkKind::Testnet,
            2 => NetworkKind::Regtest,
            value => {
                return Err(HeaderChainValueError::UnknownDiscriminant {
                    field: "network_kind",
                    value,
                })
            }
        };
        let anchor_manifest_digest = decoder.array()?;
        let work_origin = get_frontier(&mut decoder)?;
        let state_version = StateVersion::new(decoder.u64()?);
        let header_generation = HeaderGeneration::new(decoder.u64()?);
        let verified_generation = VerifiedGeneration::new(decoder.u64()?);
        let finality_epoch = FinalityEpoch::new(decoder.u64()?);
        let frontiers = FrontierSet {
            finalized: get_frontier(&mut decoder)?,
            header_best: get_frontier(&mut decoder)?,
            verified_best: get_frontier(&mut decoder)?,
        };
        let header_best_score = ChainScore::new(
            SuffixWork::new(U256::from_big_endian(&decoder.array::<32>()?)),
            block::Hash(decoder.array()?),
        );
        let oldest_retained_height = block::Height(decoder.u32()?);
        let resource_stalled = decoder.bool()?;
        let header_best_body_unavailable = decoder
            .bool()?
            .then(|| {
                Ok(BodyUnavailableSummary {
                    attempts: decoder.u32()?,
                    suppliers: decoder.u32()?,
                    alarmed: decoder.bool()?,
                })
            })
            .transpose()?;
        let last_transition_id = EvidenceId::from_digest(decoder.array()?);
        let migrated_pin_refuted = if decoder.remaining.is_empty() {
            None
        } else {
            decoder
                .bool()?
                .then(|| get_frontier(&mut decoder))
                .transpose()?
        };
        decoder.finish()?;
        Ok(Self(EngineMetadata {
            disk_format,
            mode,
            network_id,
            anchor_manifest_digest,
            work_origin,
            state_version,
            header_generation,
            verified_generation,
            finality_epoch,
            frontiers,
            header_best_score,
            oldest_retained_height,
            alarms: AlarmSet {
                resource_stalled,
                header_best_body_unavailable,
                migrated_pin_refuted,
            },
            last_transition_id,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;
    use sha2::{Digest, Sha256};
    use zakura_chain::block::genesis::regtest_genesis_block;

    fn frontier(height: u32, byte: u8) -> Frontier {
        Frontier::new(block::Height(height), block::Hash([byte; 32]))
    }

    fn digest(bytes: &[u8]) -> String {
        hex::encode(Sha256::digest(bytes))
    }

    #[test]
    fn node_round_trip_contains_all_normative_fields() {
        let block = regtest_genesis_block();
        let node = HeaderNodeDisk {
            header: block.header.clone(),
            hash: block.hash(),
            parent_hash: block.header.previous_block_hash,
            height: block::Height(0x0102_0304),
            block_work: U256::from(7),
            work_origin: block.hash(),
            cumulative_work: U256::from(9),
            deferred_until: Some(
                Utc.timestamp_opt(10, 20)
                    .single()
                    .expect("valid fixture time"),
            ),
            inherited_from: Some(block::Hash([3; 32])),
            body: HeaderBodyStateDisk::Unavailable(BodyUnavailableSummary {
                attempts: 1,
                suppliers: 2,
                alarmed: true,
            }),
            aux_delivery_ids: vec![EvidenceId::from_digest([4; 32])],
        };
        let bytes = node.encode().expect("the fixture node encodes");
        let header_len = block
            .header
            .zcash_serialize_to_vec()
            .expect("fixture header serializes")
            .len();
        assert_eq!(
            &bytes[..4],
            &u32::try_from(header_len)
                .expect("header length fits u32")
                .to_be_bytes()
        );
        assert_eq!(HeaderNodeDisk::decode(&bytes), Ok(node));
        let mut wrong_hash = bytes.clone();
        wrong_hash[4 + header_len] ^= 1;
        assert_eq!(
            HeaderNodeDisk::decode(&wrong_hash),
            Err(HeaderChainValueError::HeaderHashMismatch)
        );

        let reason = HeaderEligibilityReasonDisk::ConsensusBody {
            evidence: EvidenceId::from_digest([5; 32]),
            rule: "body.commitment".to_owned(),
        };
        let reason_bytes = reason.encode().expect("the fixture reason encodes");
        assert_eq!(reason_bytes[0], 3);
        assert_eq!(
            HeaderEligibilityReasonDisk::decode(&reason_bytes),
            Ok(reason)
        );

        let context = HeaderValidationContextDisk {
            header: block.header.clone(),
            height: block::Height(7),
        };
        assert_eq!(
            HeaderValidationContextDisk::decode(&context.encode().expect("context encodes")),
            Ok(context.clone())
        );
        assert_eq!(
            [
                digest(&bytes),
                digest(&reason_bytes),
                digest(&context.encode().expect("context encodes"))
            ],
            [
                "c7e3448aa1cabc72e6ed1bff3de3a65183f4906f8fab9b052c12b0805710a266",
                "095c753ad1f2a99c1a29f14db8f4e36c528c159c7e436957ac0f18a46dde7049",
                "dcb21b5799e73e2ca54fd1448f50dd56d5d7994cb173e5279d28942350534863",
            ]
        );
    }

    #[test]
    fn aux_finality_and_metadata_values_round_trip_exactly() {
        let owner = WorkOwner {
            state_version: StateVersion::new(1),
            header_generation: HeaderGeneration::new(2),
            verified_generation: Some(VerifiedGeneration::new(3)),
            branch: BranchId::new(block::Hash([1; 32]), block::Hash([2; 32])),
            session_id: 4,
            request_id: NonZeroU64::new(5).expect("five is nonzero"),
        };
        let aux = HeaderAuxDeliveryDisk(AuxDelivery {
            delivery_id: EvidenceId::from_digest([6; 32]),
            header_hash: block::Hash([7; 32]),
            source: SourceId::from_digest([8; 32]),
            owner,
            body_size: BodySizeHint::Known(NonZeroU32::new(9).expect("nine is nonzero")),
            payload_digest: Some([10; 32]),
            authentication: AuxAuthentication::Authenticated {
                evidence: EvidenceId::from_digest([11; 32]),
                boundary_hash: block::Hash([12; 32]),
            },
        });
        assert_eq!(
            HeaderAuxDeliveryDisk::decode(&aux.encode().expect("aux encodes")),
            Ok(aux)
        );

        let finality = HeaderFinalityRecordDisk(FinalityRecord {
            previous: frontier(10, 1),
            current: frontier(11, 2),
            source: FinalitySource::HeadersOnlyDepth {
                selected_tip: frontier(1_011, 3),
            },
            epoch: FinalityEpoch::new(4),
        });
        assert_eq!(
            HeaderFinalityRecordDisk::decode(&finality.encode().expect("finality encodes")),
            Ok(finality)
        );

        let metadata = HeaderEngineMetadataDisk(EngineMetadata {
            disk_format: HeaderChainDiskVersion(1),
            mode: EngineMode::HeadersOnly,
            network_id: NetworkKind::Regtest,
            anchor_manifest_digest: [13; 32],
            work_origin: frontier(0, 1),
            state_version: StateVersion::new(2),
            header_generation: HeaderGeneration::new(3),
            verified_generation: VerifiedGeneration::new(4),
            finality_epoch: FinalityEpoch::new(5),
            frontiers: FrontierSet {
                finalized: frontier(1, 2),
                header_best: frontier(2, 3),
                verified_best: frontier(1, 2),
            },
            header_best_score: ChainScore::new(
                SuffixWork::new(U256::from(6)),
                block::Hash([3; 32]),
            ),
            oldest_retained_height: block::Height(1),
            alarms: AlarmSet {
                resource_stalled: true,
                header_best_body_unavailable: Some(BodyUnavailableSummary {
                    attempts: 7,
                    suppliers: 8,
                    alarmed: true,
                }),
                migrated_pin_refuted: Some(frontier(1, 2)),
            },
            last_transition_id: EvidenceId::from_digest([14; 32]),
        });
        let bytes = metadata.encode().expect("metadata encodes");
        assert_eq!(&bytes[..6], &[0, 0, 0, 1, 1, 2]);
        assert_eq!(
            HeaderEngineMetadataDisk::decode(&bytes),
            Ok(metadata.clone())
        );
        let mut legacy_bytes = bytes.clone();
        legacy_bytes.truncate(
            legacy_bytes
                .len()
                .checked_sub(37)
                .expect("the optional alarm is one tag plus one frontier"),
        );
        let mut legacy_metadata = metadata.clone();
        legacy_metadata.0.alarms.migrated_pin_refuted = None;
        assert_eq!(
            HeaderEngineMetadataDisk::decode(&legacy_bytes),
            Ok(legacy_metadata)
        );
        assert_eq!(
            [
                digest(&aux.encode().expect("aux encodes")),
                digest(&finality.encode().expect("finality encodes")),
                digest(&bytes),
            ],
            [
                "329b695b06b38c807523cbc452661423cb0ead8df60d641d635d516c3ee3dd33",
                "b887bf384510dfb1a255221a8c97066617cb145eaf3e272ad70dc94cd17a3802",
                "a07e697728327c41b90f4ff71890e737d20f32337b2ce84a059659957fa3b483",
            ]
        );
    }

    #[test]
    fn unknown_truncated_oversized_and_trailing_values_fail_closed() {
        assert!(matches!(
            HeaderEligibilityReasonDisk::decode(&[9]),
            Err(HeaderChainValueError::UnknownDiscriminant {
                field: "eligibility_reason",
                value: 9
            })
        ));
        assert_eq!(
            HeaderFinalityRecordDisk::decode(&[]),
            Err(HeaderChainValueError::Truncated)
        );
        let mut oversized =
            Vec::from((u32::try_from(MAX_HEADER_BYTES).expect("bound fits u32") + 1).to_be_bytes());
        oversized.resize(4 + MAX_HEADER_BYTES + 1, 0);
        assert!(matches!(
            HeaderNodeDisk::decode(&oversized),
            Err(HeaderChainValueError::Oversized {
                field: "header",
                ..
            })
        ));
        let reason = HeaderEligibilityReasonDisk::Operator([1; 16]);
        let mut trailing = reason.encode().expect("reason encodes");
        trailing.push(0);
        assert_eq!(
            HeaderEligibilityReasonDisk::decode(&trailing),
            Err(HeaderChainValueError::Trailing)
        );
        assert!(matches!(
            get_body(&mut Decoder::new(&[9])),
            Err(HeaderChainValueError::UnknownDiscriminant {
                field: "body_state",
                value: 9
            })
        ));
        assert_eq!(
            Decoder::new(&[2]).bool(),
            Err(HeaderChainValueError::InvalidBoolean(2))
        );
        let mut metadata = vec![0, 0, 0, 2];
        metadata.resize(512, 0);
        assert_eq!(
            HeaderEngineMetadataDisk::decode(&metadata),
            Err(HeaderChainValueError::UnsupportedDiskFormat(2))
        );
        metadata[3] = 1;
        metadata[4] = 9;
        assert!(matches!(
            HeaderEngineMetadataDisk::decode(&metadata),
            Err(HeaderChainValueError::UnknownDiscriminant {
                field: "engine_mode",
                value: 9
            })
        ));
    }

    #[test]
    fn reopening_an_untouched_header_store_is_a_no_op() {
        use crate::{
            constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
            service::finalized_state::{ZakuraDb, STATE_COLUMN_FAMILIES_IN_CODE},
            Config,
        };
        use zakura_chain::parameters::Network;

        let cache = tempfile::tempdir().expect("the test cache directory is created");
        let config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: false,
            debug_skip_non_finalized_state_backup_task: true,
            ..Config::default()
        };
        let open = || {
            ZakuraDb::new(
                &config,
                STATE_DATABASE_KIND,
                &state_database_format_version_in_code(),
                &Network::Mainnet,
                true,
                STATE_COLUMN_FAMILIES_IN_CODE
                    .iter()
                    .map(ToString::to_string),
                false,
            )
            .expect("the persistent fixture database opens")
        };
        let db = open();
        let path = db.path().to_owned();
        drop(db);

        assert_header_families_empty(&path);
        let reopened = open();
        assert_eq!(reopened.path(), path);
        drop(reopened);
        assert_header_families_empty(&path);
    }

    fn assert_header_families_empty(path: &std::path::Path) {
        let options = rocksdb::Options::default();
        let names = rocksdb::DB::list_cf(&options, path)
            .expect("the persistent fixture column-family list is readable");
        let db = rocksdb::DB::open_cf(&options, path, names)
            .expect("the persistent fixture reopens through raw RocksDB");
        for name in [
            crate::service::finalized_state::HEADER_NODE_BY_HASH,
            crate::service::finalized_state::HEADER_CHILD,
            crate::service::finalized_state::HEADER_HEIGHT_HASH,
            crate::service::finalized_state::HEADER_SELECTED,
            crate::service::finalized_state::HEADER_VERIFIED,
            crate::service::finalized_state::HEADER_CANDIDATE,
            crate::service::finalized_state::HEADER_ELIGIBILITY_ROOT,
            crate::service::finalized_state::HEADER_AUX_DELIVERY,
            crate::service::finalized_state::HEADER_DEFERRED,
            crate::service::finalized_state::HEADER_FINALITY_HISTORY,
            crate::service::finalized_state::HEADER_VALIDATION_CONTEXT,
            crate::service::finalized_state::HEADER_ENGINE_META,
        ] {
            let family = db
                .cf_handle(name)
                .expect("every header-chain column family was opened");
            assert!(
                db.iterator_cf(&family, rocksdb::IteratorMode::Start)
                    .next()
                    .is_none(),
                "untouched header-chain column family must remain empty: {name}"
            );
        }
    }
}
