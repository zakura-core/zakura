//! Version-one value codecs for the fork-aware header-chain column families.

#![allow(dead_code)] // The serialized state adapter consumes these codecs in PR-8.

use std::{num::NonZeroU64, sync::Arc};

use chrono::{DateTime, TimeZone, Utc};
use thiserror::Error;
use zakura_chain::{
    block::{self, merkle::AuthDataRoot},
    ironwood, orchard,
    parameters::NetworkKind,
    sapling,
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    work::difficulty::U256,
};
use zakura_header_chain::{
    AlarmSet, AuxDelivery, BodyRuleId, BodySizeHint, BodyUnavailableSummary, BodyValidationState,
    BodyWorkAuthority, BodyWorkOwner, BranchId, ChainScore, ConsensusInvalidBodyTombstone,
    EligibilityReason, EligibilityState, EngineMetadata, EngineMode, EvidenceId, FinalityEpoch,
    FinalityHistoryCheckpoint, FinalityRecord, FinalitySource, Frontier, FrontierSet,
    HeaderChainDiskVersion, HeaderContextFact, HeaderGeneration, HeaderNode, HeaderSyncWorkOwner,
    HeaderValidationState, HeaderWorkAuthority, HeaderWorkOwner, OperatorInvalidationId, SourceId,
    StateVersion, SuffixWork, TransitionDomain, TransitionFingerprint, TreeAuxRecordV1,
    UntrustedAuxDeliveryRow, VerifiedGeneration, WorkCoordinate,
};

use super::FallibleDiskValue;

const MAX_HEADER_BYTES: usize = 2 * 1024;
const MAX_RULE_ID_BYTES: usize = 128;
const MAX_AUX_DELIVERY_IDS: usize = zakura_chain::parameters::MAX_NON_FINALIZED_CHAIN_FORKS * 16;

/// Bounded collection count stored with the atomic header-chain root.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HeaderRowCountDisk(pub u64);

impl FallibleDiskValue for HeaderRowCountDisk {
    type Error = HeaderChainValueError;

    fn encode(&self) -> Result<Vec<u8>, Self::Error> {
        let mut encoder = Encoder::default();
        encoder.u8(1);
        encoder.u64(self.0);
        Ok(encoder.0)
    }

    fn decode(bytes: &[u8]) -> Result<Self, Self::Error> {
        let mut decoder = Decoder::new(bytes);
        match decoder.u8()? {
            1 => {}
            value => {
                return Err(HeaderChainValueError::UnknownDiscriminant {
                    field: "header_row_count_version",
                    value,
                })
            }
        }
        let count = Self(decoder.u64()?);
        decoder.finish()?;
        Ok(count)
    }
}

/// Malformed, truncated, oversized, or unknown version-one value data.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum HeaderChainValueError {
    /// The decoder reached the value's end before it read a complete field.
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
    /// Version one did not assign this stable enum discriminant.
    #[error("unknown {field} discriminant {value}")]
    UnknownDiscriminant {
        /// Stable enum name.
        field: &'static str,
        /// Supplied discriminant.
        value: u8,
    },
    /// A boolean byte contained a value other than zero or one.
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
    #[error(
        "unsupported header-chain disk format {found}: this build stores format {current} and \
         startup found no migration to it, so the state directory must be deleted and \
         resynchronized",
        found = .0,
        current = HeaderChainDiskVersion::CURRENT.0
    )]
    UnsupportedDiskFormat(u32),
    /// Chrono cannot represent the UTC seconds and nanoseconds pair.
    #[error("invalid UTC timestamp")]
    Timestamp,
    /// A rule identifier contained malformed UTF-8.
    #[error("invalid UTF-8 rule identifier")]
    RuleId,
    /// The decoder found a noncanonical auxiliary commitment-tree root.
    #[error("invalid {0} auxiliary commitment-tree root")]
    TreeAuxRoot(&'static str),
    /// The decoder found a malformed sealed auxiliary outcome.
    #[error("invalid auxiliary outcome")]
    InvalidAuxOutcome,
}

/// Durable phase of bounded full-state/header reconciliation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HeaderReconstructionPhaseDisk {
    /// Reconciliation processes canonical finalized headers in bounded chunks.
    FinalizedPath,
    /// Reconciliation processes restored non-finalized headers.
    RestoredPath,
    /// Reconciliation restored all paths.
    /// The final exhaustive audit remains.
    FinalAudit,
}

/// Versioned restart marker for bounded header-runtime reconstruction.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HeaderReconstructionProgressDisk {
    /// Source full-state network.
    pub network: NetworkKind,
    /// Canonical finalized target fixed or rebased for this attempt.
    pub target: Frontier,
    /// First canonical height not yet reconciled.
    pub next_height: block::Height,
    /// Current bounded reconstruction phase.
    pub phase: HeaderReconstructionPhaseDisk,
    /// Last canonical frontier committed with this marker.
    pub last_committed: Frontier,
}

impl FallibleDiskValue for HeaderReconstructionProgressDisk {
    type Error = HeaderChainValueError;

    fn encode(&self) -> Result<Vec<u8>, Self::Error> {
        let mut encoder = Encoder::default();
        encoder.u8(1);
        encoder.u8(match self.network {
            NetworkKind::Mainnet => 0,
            NetworkKind::Testnet => 1,
            NetworkKind::Regtest => 2,
        });
        put_frontier(&mut encoder, self.target);
        encoder.u32(self.next_height.0);
        encoder.u8(match self.phase {
            HeaderReconstructionPhaseDisk::FinalizedPath => 0,
            HeaderReconstructionPhaseDisk::RestoredPath => 1,
            HeaderReconstructionPhaseDisk::FinalAudit => 2,
        });
        put_frontier(&mut encoder, self.last_committed);
        Ok(encoder.0)
    }

    fn decode(bytes: &[u8]) -> Result<Self, Self::Error> {
        let mut decoder = Decoder::new(bytes);
        match decoder.u8()? {
            1 => {}
            value => {
                return Err(HeaderChainValueError::UnknownDiscriminant {
                    field: "header_reconstruction_version",
                    value,
                })
            }
        }
        let network = match decoder.u8()? {
            0 => NetworkKind::Mainnet,
            1 => NetworkKind::Testnet,
            2 => NetworkKind::Regtest,
            value => {
                return Err(HeaderChainValueError::UnknownDiscriminant {
                    field: "header_reconstruction_network",
                    value,
                })
            }
        };
        let target = get_frontier(&mut decoder)?;
        let next_height = block::Height(decoder.u32()?);
        let phase = match decoder.u8()? {
            0 => HeaderReconstructionPhaseDisk::FinalizedPath,
            1 => HeaderReconstructionPhaseDisk::RestoredPath,
            2 => HeaderReconstructionPhaseDisk::FinalAudit,
            value => {
                return Err(HeaderChainValueError::UnknownDiscriminant {
                    field: "header_reconstruction_phase",
                    value,
                })
            }
        };
        let last_committed = get_frontier(&mut decoder)?;
        decoder.finish()?;
        Ok(Self {
            network,
            target,
            next_height,
            phase,
            last_committed,
        })
    }
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
    fn optional<T>(&mut self, value: Option<T>, put: impl FnOnce(&mut Self, T)) {
        self.bool(value.is_some());
        if let Some(value) = value {
            put(self, value);
        }
    }
    fn counted<T>(
        &mut self,
        field: &'static str,
        values: &[T],
        maximum: usize,
        mut put: impl FnMut(&mut Self, &T),
    ) -> Result<(), HeaderChainValueError> {
        if values.len() > maximum {
            return Err(HeaderChainValueError::Oversized {
                field,
                length: values.len(),
            });
        }
        self.u32(
            u32::try_from(values.len()).map_err(|_| HeaderChainValueError::Oversized {
                field,
                length: values.len(),
            })?,
        );
        for value in values {
            put(self, value);
        }
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
    fn optional<T>(
        &mut self,
        get: impl FnOnce(&mut Self) -> Result<T, HeaderChainValueError>,
    ) -> Result<Option<T>, HeaderChainValueError> {
        self.bool()?.then(|| get(self)).transpose()
    }
    fn counted<T>(
        &mut self,
        field: &'static str,
        maximum: usize,
        mut get: impl FnMut(&mut Self) -> Result<T, HeaderChainValueError>,
    ) -> Result<Vec<T>, HeaderChainValueError> {
        let count = usize::try_from(self.u32()?).map_err(|_| HeaderChainValueError::Oversized {
            field,
            length: usize::MAX,
        })?;
        if count > maximum {
            return Err(HeaderChainValueError::Oversized {
                field,
                length: count,
            });
        }
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(get(self)?);
        }
        Ok(values)
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

impl FallibleDiskValue for ConsensusInvalidBodyTombstone {
    type Error = HeaderChainValueError;

    fn encode(&self) -> Result<Vec<u8>, Self::Error> {
        let mut encoder = Encoder::default();
        encoder.u8(2);
        encoder.fixed(&self.hash.0);
        encoder.u32(self.height.0);
        encoder.fixed(&self.evidence.digest());
        encoder.bounded(
            "consensus_invalid_rule",
            self.rule.as_str().as_bytes(),
            MAX_RULE_ID_BYTES,
        )?;
        Ok(encoder.0)
    }

    fn decode(bytes: &[u8]) -> Result<Self, Self::Error> {
        let mut decoder = Decoder::new(bytes);
        match decoder.u8()? {
            2 => {}
            value => {
                return Err(HeaderChainValueError::UnknownDiscriminant {
                    field: "consensus_invalid_tombstone_version",
                    value,
                })
            }
        }
        let hash = block::Hash(decoder.array()?);
        let height = block::Height(decoder.u32()?);
        let evidence = EvidenceId::from_digest(decoder.array()?);
        let rule =
            std::str::from_utf8(decoder.bounded("consensus_invalid_rule", MAX_RULE_ID_BYTES)?)
                .map_err(|_| HeaderChainValueError::RuleId)?;
        decoder.finish()?;
        Ok(Self {
            hash,
            height,
            evidence,
            rule: BodyRuleId::new(rule),
        })
    }
}

/// Decode a released version-one tombstone, supplying the height omitted by v1.
pub(crate) fn decode_v1_consensus_invalid_body_tombstone(
    bytes: &[u8],
    header_height: block::Height,
) -> Result<ConsensusInvalidBodyTombstone, HeaderChainValueError> {
    let mut decoder = Decoder::new(bytes);
    if decoder.u8()? != 1 {
        return Err(HeaderChainValueError::UnknownDiscriminant {
            field: "consensus_invalid_tombstone_version",
            value: bytes.first().copied().unwrap_or_default(),
        });
    }
    let hash = block::Hash(decoder.array()?);
    let evidence = EvidenceId::from_digest(decoder.array()?);
    let rule = std::str::from_utf8(decoder.bounded("consensus_invalid_rule", MAX_RULE_ID_BYTES)?)
        .map_err(|_| HeaderChainValueError::RuleId)?;
    decoder.finish()?;
    Ok(ConsensusInvalidBodyTombstone {
        hash,
        height: header_height,
        evidence,
        rule: BodyRuleId::new(rule),
    })
}

/// Durable full-state evidence authority for one retained body-validation projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FullStateBodyValidationEvidenceAuthorityDisk {
    /// Full state accepted the exact block hash under this evidence identity.
    Verified {
        /// Canonical block hash.
        hash: block::Hash,
        /// Exact full-state evidence identity.
        evidence: EvidenceId,
    },
    /// Full state rejected the exact block hash under this evidence and rule.
    ConsensusInvalid(ConsensusInvalidBodyTombstone),
}

impl FullStateBodyValidationEvidenceAuthorityDisk {
    /// Build authority for a body-validation state that requires full-state authentication.
    pub fn from_body_validation_state(
        header_hash: block::Hash,
        header_height: block::Height,
        body_validation_state: &BodyValidationState,
    ) -> Option<Self> {
        match body_validation_state {
            BodyValidationState::Verified { evidence } => Some(Self::Verified {
                hash: header_hash,
                evidence: *evidence,
            }),
            BodyValidationState::ConsensusInvalid { evidence, rule } => {
                Some(Self::ConsensusInvalid(ConsensusInvalidBodyTombstone {
                    hash: header_hash,
                    height: header_height,
                    evidence: *evidence,
                    rule: rule.clone(),
                }))
            }
            _ => None,
        }
    }

    /// Return whether this authority attests to the exact body-validation state.
    pub fn attests_to_body_validation_state(
        &self,
        header_hash: block::Hash,
        body_validation_state: &BodyValidationState,
    ) -> bool {
        match (self, body_validation_state) {
            (
                Self::Verified {
                    hash: authority_hash,
                    evidence: authority_evidence,
                },
                BodyValidationState::Verified { evidence },
            ) => *authority_hash == header_hash && authority_evidence == evidence,
            (
                Self::ConsensusInvalid(authority),
                BodyValidationState::ConsensusInvalid { evidence, rule },
            ) => {
                authority.hash == header_hash
                    && authority.evidence == *evidence
                    && authority.rule == *rule
            }
            _ => false,
        }
    }
}

impl FallibleDiskValue for FullStateBodyValidationEvidenceAuthorityDisk {
    type Error = HeaderChainValueError;

    fn encode(&self) -> Result<Vec<u8>, Self::Error> {
        let mut encoder = Encoder::default();
        encoder.u8(2);
        match self {
            Self::Verified { hash, evidence } => {
                encoder.u8(0);
                encoder.fixed(&hash.0);
                encoder.fixed(&evidence.digest());
            }
            Self::ConsensusInvalid(tombstone) => {
                encoder.u8(1);
                encoder.fixed(&tombstone.hash.0);
                encoder.u32(tombstone.height.0);
                encoder.fixed(&tombstone.evidence.digest());
                encoder.bounded(
                    "body_evidence_rule",
                    tombstone.rule.as_str().as_bytes(),
                    MAX_RULE_ID_BYTES,
                )?;
            }
        }
        Ok(encoder.0)
    }

    fn decode(bytes: &[u8]) -> Result<Self, Self::Error> {
        let mut decoder = Decoder::new(bytes);
        if decoder.u8()? != 2 {
            return Err(HeaderChainValueError::UnknownDiscriminant {
                field: "body_evidence_authority_version",
                value: bytes.first().copied().unwrap_or_default(),
            });
        }
        let kind = decoder.u8()?;
        let hash = block::Hash(decoder.array()?);
        let authority = match kind {
            0 => Self::Verified {
                hash,
                evidence: EvidenceId::from_digest(decoder.array()?),
            },
            1 => {
                let height = block::Height(decoder.u32()?);
                let evidence = EvidenceId::from_digest(decoder.array()?);
                let rule =
                    std::str::from_utf8(decoder.bounded("body_evidence_rule", MAX_RULE_ID_BYTES)?)
                        .map_err(|_| HeaderChainValueError::RuleId)?;
                Self::ConsensusInvalid(ConsensusInvalidBodyTombstone {
                    hash,
                    height,
                    evidence,
                    rule: BodyRuleId::new(rule),
                })
            }
            value => {
                return Err(HeaderChainValueError::UnknownDiscriminant {
                    field: "body_evidence_authority_kind",
                    value,
                })
            }
        };
        decoder.finish()?;
        Ok(authority)
    }
}

/// Decode released version-one full-state authority, supplying the height omitted by v1.
pub(crate) fn decode_v1_full_state_body_validation_evidence_authority(
    bytes: &[u8],
    header_height: block::Height,
) -> Result<FullStateBodyValidationEvidenceAuthorityDisk, HeaderChainValueError> {
    let mut decoder = Decoder::new(bytes);
    if decoder.u8()? != 1 {
        return Err(HeaderChainValueError::UnknownDiscriminant {
            field: "body_evidence_authority_version",
            value: bytes.first().copied().unwrap_or_default(),
        });
    }
    let kind = decoder.u8()?;
    let hash = block::Hash(decoder.array()?);
    let evidence = EvidenceId::from_digest(decoder.array()?);
    let authority = match kind {
        0 => FullStateBodyValidationEvidenceAuthorityDisk::Verified { hash, evidence },
        1 => {
            let rule =
                std::str::from_utf8(decoder.bounded("body_evidence_rule", MAX_RULE_ID_BYTES)?)
                    .map_err(|_| HeaderChainValueError::RuleId)?;
            FullStateBodyValidationEvidenceAuthorityDisk::ConsensusInvalid(
                ConsensusInvalidBodyTombstone {
                    hash,
                    height: header_height,
                    evidence,
                    rule: BodyRuleId::new(rule),
                },
            )
        }
        value => {
            return Err(HeaderChainValueError::UnknownDiscriminant {
                field: "body_evidence_authority_kind",
                value,
            })
        }
    };
    decoder.finish()?;
    Ok(authority)
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
    Operator {
        /// Independently removable operator identity.
        id: [u8; 16],
        /// Canonical target-and-identity binding.
        reason_digest: [u8; 32],
        /// Authenticated action evidence.
        evidence: EvidenceId,
    },
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
            EligibilityReason::OperatorInvalid {
                id,
                reason_digest,
                evidence,
            } => Self::Operator {
                id: id.bytes(),
                reason_digest: *reason_digest,
                evidence: *evidence,
            },
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
            Self::Operator {
                id,
                reason_digest,
                evidence,
            } => EligibilityReason::OperatorInvalid {
                id: OperatorInvalidationId::new(id),
                reason_digest,
                evidence,
            },
        }
    }
}

impl FallibleDiskValue for HeaderEligibilityReasonDisk {
    type Error = HeaderChainValueError;

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
            Self::Operator {
                id,
                reason_digest,
                evidence,
            } => {
                encoder.u8(4);
                encoder.fixed(id);
                encoder.fixed(reason_digest);
                encoder.fixed(&evidence.digest());
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
            4 => Self::Operator {
                id: decoder.array()?,
                reason_digest: decoder.array()?,
                evidence: EvidenceId::from_digest(decoder.array()?),
            },
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
pub enum HeaderBodyValidationStateDisk {
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
    /// Body-validation state.
    pub body_validation_state: HeaderBodyValidationStateDisk,
    /// Bounded hash-keyed auxiliary delivery IDs.
    pub aux_delivery_ids: Vec<EvidenceId>,
}

impl HeaderNodeDisk {
    /// Convert one domain node into its version-one durable representation.
    pub fn from_domain(node: &HeaderNode) -> Self {
        let body_validation_state = match &node.body_validation_state {
            BodyValidationState::Unknown => HeaderBodyValidationStateDisk::Unknown,
            BodyValidationState::CommitmentMatched => {
                HeaderBodyValidationStateDisk::CommitmentMatched
            }
            BodyValidationState::Verified { evidence } => {
                HeaderBodyValidationStateDisk::Verified(*evidence)
            }
            BodyValidationState::ConsensusInvalid { evidence, rule } => {
                HeaderBodyValidationStateDisk::ConsensusInvalid {
                    evidence: *evidence,
                    rule: rule.as_str().to_owned(),
                }
            }
            BodyValidationState::Unavailable(summary) => {
                HeaderBodyValidationStateDisk::Unavailable(*summary)
            }
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
            body_validation_state,
            aux_delivery_ids: node.aux_delivery_ids.clone(),
        }
    }

    /// Reconstruct one domain node after the decoder reads its direct-reason rows.
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
        let body_validation_state = match self.body_validation_state {
            HeaderBodyValidationStateDisk::Unknown => BodyValidationState::Unknown,
            HeaderBodyValidationStateDisk::CommitmentMatched => {
                BodyValidationState::CommitmentMatched
            }
            HeaderBodyValidationStateDisk::Verified(evidence) => {
                BodyValidationState::Verified { evidence }
            }
            HeaderBodyValidationStateDisk::ConsensusInvalid { evidence, rule } => {
                BodyValidationState::ConsensusInvalid {
                    evidence,
                    rule: BodyRuleId::new(rule),
                }
            }
            HeaderBodyValidationStateDisk::Unavailable(summary) => {
                BodyValidationState::Unavailable(summary)
            }
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
            body_validation_state,
            self.aux_delivery_ids,
        )
        .map_err(|_| HeaderChainValueError::Header)
    }
}

impl FallibleDiskValue for HeaderNodeDisk {
    type Error = HeaderChainValueError;

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
        encoder.optional(self.deferred_until, put_time);
        encoder.optional(self.inherited_from, |encoder, hash| {
            encoder.fixed(&hash.0);
        });
        put_body_validation_state(&mut encoder, &self.body_validation_state)?;
        encoder.counted(
            "aux_delivery_ids",
            &self.aux_delivery_ids,
            MAX_AUX_DELIVERY_IDS,
            |encoder, id| {
                encoder.fixed(&id.digest());
            },
        )?;
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
        let deferred_until = decoder.optional(get_time)?;
        let inherited_from = decoder.optional(|decoder| decoder.array().map(block::Hash))?;
        let body_validation_state = get_body_validation_state(&mut decoder)?;
        let aux_delivery_ids =
            decoder.counted("aux_delivery_ids", MAX_AUX_DELIVERY_IDS, |decoder| {
                decoder.array().map(EvidenceId::from_digest)
            })?;
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
            body_validation_state,
            aux_delivery_ids,
        })
    }
}

fn put_body_validation_state(
    encoder: &mut Encoder,
    body: &HeaderBodyValidationStateDisk,
) -> Result<(), HeaderChainValueError> {
    match body {
        HeaderBodyValidationStateDisk::Unknown => encoder.u8(0),
        HeaderBodyValidationStateDisk::CommitmentMatched => encoder.u8(1),
        HeaderBodyValidationStateDisk::Verified(evidence) => {
            encoder.u8(2);
            encoder.fixed(&evidence.digest());
        }
        HeaderBodyValidationStateDisk::ConsensusInvalid { evidence, rule } => {
            encoder.u8(3);
            encoder.fixed(&evidence.digest());
            encoder.bounded("body_rule", rule.as_bytes(), MAX_RULE_ID_BYTES)?;
        }
        HeaderBodyValidationStateDisk::Unavailable(summary) => {
            encoder.u8(4);
            put_unavailable(encoder, *summary);
        }
    }
    Ok(())
}

fn get_body_validation_state(
    decoder: &mut Decoder<'_>,
) -> Result<HeaderBodyValidationStateDisk, HeaderChainValueError> {
    match decoder.u8()? {
        0 => Ok(HeaderBodyValidationStateDisk::Unknown),
        1 => Ok(HeaderBodyValidationStateDisk::CommitmentMatched),
        2 => Ok(HeaderBodyValidationStateDisk::Verified(
            EvidenceId::from_digest(decoder.array()?),
        )),
        3 => Ok(HeaderBodyValidationStateDisk::ConsensusInvalid {
            evidence: EvidenceId::from_digest(decoder.array()?),
            rule: std::str::from_utf8(decoder.bounded("body_rule", MAX_RULE_ID_BYTES)?)
                .map_err(|_| HeaderChainValueError::RuleId)?
                .to_owned(),
        }),
        4 => Ok(HeaderBodyValidationStateDisk::Unavailable(get_unavailable(
            decoder,
        )?)),
        value => Err(HeaderChainValueError::UnknownDiscriminant {
            field: "body_state",
            value,
        }),
    }
}

fn put_unavailable(encoder: &mut Encoder, summary: BodyUnavailableSummary) {
    put_time(encoder, summary.started_at);
    encoder.u32(summary.attempts);
    encoder.u32(summary.suppliers);
    encoder.fixed(&summary.supplier_set_digest);
    encoder.bool(summary.alarmed);
    put_time(encoder, summary.next_probe_at);
}

fn get_unavailable(
    decoder: &mut Decoder<'_>,
) -> Result<BodyUnavailableSummary, HeaderChainValueError> {
    Ok(BodyUnavailableSummary {
        started_at: get_time(decoder)?,
        attempts: decoder.u32()?,
        suppliers: decoder.u32()?,
        supplier_set_digest: decoder.array()?,
        alarmed: decoder.bool()?,
        next_probe_at: get_time(decoder)?,
    })
}

impl FallibleDiskValue for AuxDelivery {
    type Error = HeaderChainValueError;

    fn encode(&self) -> Result<Vec<u8>, HeaderChainValueError> {
        let mut encoder = Encoder::default();
        put_aux(&mut encoder, *self);
        Ok(encoder.0)
    }

    fn decode(bytes: &[u8]) -> Result<Self, HeaderChainValueError> {
        let untrusted_row = decode_untrusted_aux_delivery(bytes)?;
        if untrusted_row.outcome_status_code() == 0
            && untrusted_row.observation_digests() == [None, None]
            && untrusted_row.outcome_boundary_hash().is_none()
        {
            Ok(untrusted_row.delivery())
        } else {
            Err(HeaderChainValueError::InvalidAuxOutcome)
        }
    }
}

fn put_aux(encoder: &mut Encoder, value: AuxDelivery) {
    put_aux_base(encoder, value);
    put_aux_outcome(encoder, value);
}

fn put_aux_base(encoder: &mut Encoder, value: AuxDelivery) {
    encoder.fixed(&value.delivery_id.digest());
    encoder.fixed(&value.header_hash.0);
    encoder.fixed(&value.source.digest());
    put_owner(encoder, value.owner);
    encoder.u32(match value.body_size {
        BodySizeHint::Unknown => 0,
        BodySizeHint::Known(size) => size.get(),
    });
    encoder.optional(value.tree_aux, |encoder, aux| {
        encoder.u32(aux.height.0);
        encoder.fixed(&<[u8; 32]>::from(aux.sapling_root));
        encoder.fixed(&<[u8; 32]>::from(aux.orchard_root));
        encoder.fixed(&<[u8; 32]>::from(aux.ironwood_root));
        encoder.u64(aux.sapling_tx_count);
        encoder.u64(aux.orchard_tx_count);
        encoder.u64(aux.ironwood_tx_count);
        encoder.fixed(&<[u8; 32]>::from(aux.auth_data_root));
    });
}

fn put_aux_outcome(encoder: &mut Encoder, value: AuxDelivery) {
    let status_code = if value.is_unauthenticated() {
        0
    } else if value.is_authenticated() {
        1
    } else if value.is_rejected() {
        2
    } else {
        3
    };
    encoder.u8(status_code);
    if status_code != 0 {
        for observation_id in value.observation_ids() {
            encoder.optional(observation_id, |encoder, observation_id| {
                encoder.fixed(&observation_id.digest());
            });
        }
        encoder.fixed(
            &value
                .outcome_boundary_hash()
                .expect("derived auxiliary outcomes always retain their boundary")
                .0,
        );
    }
}

fn get_aux_base(decoder: &mut Decoder<'_>) -> Result<AuxDelivery, HeaderChainValueError> {
    let delivery_id = EvidenceId::from_digest(decoder.array()?);
    let header_hash = block::Hash(decoder.array()?);
    let source = SourceId::from_digest(decoder.array()?);
    let owner = get_owner(decoder)?;
    let body_size =
        BodySizeHint::new(decoder.u32()?).map_err(|_| HeaderChainValueError::Oversized {
            field: "body_size",
            length: usize::MAX,
        })?;
    let tree_aux = decoder.optional(|decoder| {
        Ok(TreeAuxRecordV1 {
            height: block::Height(decoder.u32()?),
            sapling_root: sapling::tree::Root::try_from(decoder.array()?)
                .map_err(|_| HeaderChainValueError::TreeAuxRoot("Sapling"))?,
            orchard_root: orchard::tree::Root::try_from(decoder.array()?)
                .map_err(|_| HeaderChainValueError::TreeAuxRoot("Orchard"))?,
            ironwood_root: ironwood::tree::Root::try_from(decoder.array()?)
                .map_err(|_| HeaderChainValueError::TreeAuxRoot("Ironwood"))?,
            sapling_tx_count: decoder.u64()?,
            orchard_tx_count: decoder.u64()?,
            ironwood_tx_count: decoder.u64()?,
            auth_data_root: AuthDataRoot::from(decoder.array()?),
        })
    })?;
    Ok(AuxDelivery::new(
        delivery_id,
        header_hash,
        source,
        owner,
        body_size,
        tree_aux,
    ))
}

fn get_aux(decoder: &mut Decoder<'_>) -> Result<UntrustedAuxDeliveryRow, HeaderChainValueError> {
    let delivery = get_aux_base(decoder)?;
    let status_code = match decoder.u8()? {
        value @ 0..=3 => value,
        value => {
            return Err(HeaderChainValueError::UnknownDiscriminant {
                field: "aux_authentication",
                value,
            });
        }
    };
    let (observation_digests, boundary_hash) = if status_code == 0 {
        ([None, None], None)
    } else {
        let first = decoder.optional(|decoder| decoder.array())?;
        let second = decoder.optional(|decoder| decoder.array())?;
        ([first, second], Some(block::Hash(decoder.array()?)))
    };
    Ok(UntrustedAuxDeliveryRow::new(
        delivery,
        status_code,
        observation_digests,
        boundary_hash,
    ))
}

pub(crate) fn decode_untrusted_aux_delivery(
    bytes: &[u8],
) -> Result<UntrustedAuxDeliveryRow, HeaderChainValueError> {
    let mut decoder = Decoder::new(bytes);
    let row = get_aux(&mut decoder)?;
    decoder.finish()?;
    Ok(row)
}

/// Decode a version-one delivery and discard its caller-selected outcome.
pub(crate) fn decode_v1_aux_delivery(bytes: &[u8]) -> Result<AuxDelivery, HeaderChainValueError> {
    let mut decoder = Decoder::new(bytes);
    let delivery = get_aux_base(&mut decoder)?;
    match decoder.u8()? {
        0 => {}
        1 => {
            let _evidence = decoder.array::<32>()?;
            let _boundary_hash = decoder.array::<32>()?;
        }
        2 | 3 => {
            let _evidence = decoder.array::<32>()?;
        }
        value => {
            return Err(HeaderChainValueError::UnknownDiscriminant {
                field: "aux_authentication",
                value,
            });
        }
    }
    decoder.finish()?;
    Ok(delivery)
}

fn put_owner(encoder: &mut Encoder, owner: HeaderSyncWorkOwner) {
    // The encoder preserves the version-one byte layout.
    // Version one did not use this field to authorize asynchronous work.
    // The encoder reserves the field as zero.
    encoder.u64(0);
    let header = owner.header_authority();
    encoder.u64(header.header_generation.get());
    encoder.optional(
        owner.body_authority().map(|body| body.verified_generation),
        |encoder, generation| {
            encoder.u64(generation.get());
        },
    );
    encoder.fixed(&header.branch.anchor_hash.0);
    encoder.fixed(&header.branch.target_tip_hash.0);
    encoder.u64(owner.session_id());
    encoder.u64(owner.request_id().get());
}

fn get_owner(decoder: &mut Decoder<'_>) -> Result<HeaderSyncWorkOwner, HeaderChainValueError> {
    // Legacy rows store their former global state version here.
    // The state version did not authorize work.
    // The decoder accepts and discards the legacy value.
    let _reserved_state_version = decoder.u64()?;
    let header_generation = HeaderGeneration::new(decoder.u64()?);
    let verified_generation =
        decoder.optional(|decoder| decoder.u64().map(VerifiedGeneration::new))?;
    let branch = BranchId::new(block::Hash(decoder.array()?), block::Hash(decoder.array()?));
    let session_id = decoder.u64()?;
    let request_id =
        NonZeroU64::new(decoder.u64()?).ok_or(HeaderChainValueError::Zero("request_id"))?;
    let header = HeaderWorkAuthority {
        header_generation,
        branch,
    };
    Ok(match verified_generation {
        Some(verified_generation) => BodyWorkOwner {
            authority: BodyWorkAuthority {
                header,
                verified_generation,
                body_work_epoch: zakura_header_chain::BodyWorkEpoch::default(),
            },
            session_id,
            request_id,
        }
        .into(),
        None => HeaderWorkOwner {
            authority: header,
            session_id,
            request_id,
        }
        .into(),
    })
}

impl FallibleDiskValue for FinalityRecord {
    type Error = HeaderChainValueError;

    fn encode(&self) -> Result<Vec<u8>, HeaderChainValueError> {
        let mut encoder = Encoder::default();
        put_frontier(&mut encoder, self.previous);
        put_frontier(&mut encoder, self.current);
        match self.source {
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
        encoder.u64(self.epoch.get());
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
        Ok(FinalityRecord {
            previous,
            current,
            source,
            epoch,
        })
    }
}

impl FallibleDiskValue for FinalityHistoryCheckpoint {
    type Error = HeaderChainValueError;

    fn encode(&self) -> Result<Vec<u8>, Self::Error> {
        let mut encoder = Encoder::default();
        encoder.u8(1);
        encoder.u64(self.epoch.get());
        put_frontier(&mut encoder, self.frontier);
        Ok(encoder.0)
    }

    fn decode(bytes: &[u8]) -> Result<Self, Self::Error> {
        let mut decoder = Decoder::new(bytes);
        match decoder.u8()? {
            1 => {}
            value => {
                return Err(HeaderChainValueError::UnknownDiscriminant {
                    field: "finality_history_checkpoint_version",
                    value,
                })
            }
        }
        let checkpoint = Self {
            epoch: FinalityEpoch::new(decoder.u64()?),
            frontier: get_frontier(&mut decoder)?,
        };
        decoder.finish()?;
        Ok(checkpoint)
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
            header: self.header.clone(),
        }
    }
}

impl FallibleDiskValue for HeaderValidationContextDisk {
    type Error = HeaderChainValueError;

    fn encode(&self) -> Result<Vec<u8>, HeaderChainValueError> {
        let mut encoder = Encoder::default();
        let header_start = encoder.0.len();
        encoder.u32(0);
        if self.header.zcash_serialize(&mut encoder.0).is_err() {
            encoder.0.truncate(header_start);
            return Err(HeaderChainValueError::Header);
        }
        let header_length = encoder.0.len().saturating_sub(header_start + 4);
        if header_length > MAX_HEADER_BYTES {
            encoder.0.truncate(header_start);
            return Err(HeaderChainValueError::Oversized {
                field: "context_header",
                length: header_length,
            });
        }
        let length = u32::try_from(header_length).map_err(|_| {
            encoder.0.truncate(header_start);
            HeaderChainValueError::Oversized {
                field: "context_header",
                length: header_length,
            }
        })?;
        encoder.0[header_start..header_start + 4].copy_from_slice(&length.to_be_bytes());
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

impl FallibleDiskValue for EngineMetadata {
    type Error = HeaderChainValueError;

    fn encode(&self) -> Result<Vec<u8>, HeaderChainValueError> {
        let value = self;
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
        encoder.fixed(&value.network_policy_digest);
        encoder.fixed(&value.anchor_manifest_digest);
        put_frontier(&mut encoder, value.work_origin);
        encoder.u64(value.state_version.get());
        encoder.u64(value.header_generation.get());
        encoder.u64(value.verified_generation.get());
        encoder.u64(value.finality_epoch.get());
        encoder.optional(value.headers_only_migration_epoch, |encoder, epoch| {
            encoder.u64(epoch.get());
        });
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
        encoder.optional(value.alarms.header_best_body_unavailable, put_unavailable);
        encoder.optional(value.last_transition, |encoder, fingerprint| {
            encoder.u8(fingerprint.domain().code());
            encoder.fixed(&fingerprint.evidence().digest());
            encoder.fixed(&fingerprint.payload_digest());
        });
        encoder.optional(value.alarms.migrated_pin_refuted, put_frontier);
        Ok(encoder.0)
    }

    fn decode(bytes: &[u8]) -> Result<Self, HeaderChainValueError> {
        decode_engine_metadata(bytes, None)
    }
}

/// Decode metadata that carries the released version-one marker.
///
/// Version one predates the durable network policy. The migration accepts this decoded metadata
/// only when the recorded network kind identifies one fixed policy.
pub(crate) fn decode_v1_engine_metadata(
    bytes: &[u8],
    network_policy_digest: [u8; 32],
) -> Result<EngineMetadata, HeaderChainValueError> {
    decode_engine_metadata(bytes, Some(network_policy_digest))
}

/// Decode metadata, reading the network policy digest unless the caller supplies a version-one one.
fn decode_engine_metadata(
    bytes: &[u8],
    v1_network_policy_digest: Option<[u8; 32]>,
) -> Result<EngineMetadata, HeaderChainValueError> {
    let expected_disk_format = match v1_network_policy_digest {
        Some(_) => HeaderChainDiskVersion(1),
        None => HeaderChainDiskVersion::CURRENT,
    };
    let mut decoder = Decoder::new(bytes);
    let disk_format = decoder.u32()?;
    if disk_format != expected_disk_format.0 {
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
    let network_policy_digest = match v1_network_policy_digest {
        Some(digest) => digest,
        None => decoder.array()?,
    };
    let anchor_manifest_digest = decoder.array()?;
    let work_origin = get_frontier(&mut decoder)?;
    let state_version = StateVersion::new(decoder.u64()?);
    let header_generation = HeaderGeneration::new(decoder.u64()?);
    let verified_generation = VerifiedGeneration::new(decoder.u64()?);
    let finality_epoch = FinalityEpoch::new(decoder.u64()?);
    let headers_only_migration_epoch =
        decoder.optional(|decoder| Ok(FinalityEpoch::new(decoder.u64()?)))?;
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
    let header_best_body_unavailable = decoder.optional(get_unavailable)?;
    let last_transition = decoder.optional(|decoder| {
        let code = decoder.u8()?;
        let domain = TransitionDomain::from_code(code).ok_or(
            HeaderChainValueError::UnknownDiscriminant {
                field: "transition_domain",
                value: code,
            },
        )?;
        Ok(TransitionFingerprint::from_parts(
            domain,
            EvidenceId::from_digest(decoder.array()?),
            decoder.array()?,
        ))
    })?;
    let migrated_pin_refuted = decoder.optional(get_frontier)?;
    decoder.finish()?;
    Ok(EngineMetadata {
        disk_format,
        mode,
        network_id,
        network_policy_digest,
        anchor_manifest_digest,
        work_origin,
        state_version,
        header_generation,
        verified_generation,
        finality_epoch,
        headers_only_migration_epoch,
        frontiers,
        header_best_score,
        oldest_retained_height,
        alarms: AlarmSet {
            resource_stalled,
            header_best_body_unavailable,
            migrated_pin_refuted,
        },
        last_transition,
    })
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
    fn reconstruction_progress_round_trips_and_rejects_unknown_versions() {
        let progress = HeaderReconstructionProgressDisk {
            network: NetworkKind::Testnet,
            target: frontier(200, 2),
            next_height: block::Height(151),
            phase: HeaderReconstructionPhaseDisk::RestoredPath,
            last_committed: frontier(150, 1),
        };
        let bytes = progress.encode().expect("progress fixture encodes");
        assert_eq!(
            HeaderReconstructionProgressDisk::decode(&bytes),
            Ok(progress)
        );

        let mut unknown_version = bytes.clone();
        unknown_version[0] = 2;
        assert_eq!(
            HeaderReconstructionProgressDisk::decode(&unknown_version),
            Err(HeaderChainValueError::UnknownDiscriminant {
                field: "header_reconstruction_version",
                value: 2,
            })
        );

        let mut trailing = bytes;
        trailing.push(0);
        assert_eq!(
            HeaderReconstructionProgressDisk::decode(&trailing),
            Err(HeaderChainValueError::Trailing)
        );
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
            body_validation_state: HeaderBodyValidationStateDisk::Unavailable(
                BodyUnavailableSummary {
                    started_at: Utc
                        .timestamp_opt(30, 40)
                        .single()
                        .expect("valid fixture time"),
                    attempts: 1,
                    suppliers: 2,
                    supplier_set_digest: [7; 32],
                    alarmed: true,
                    next_probe_at: Utc
                        .timestamp_opt(50, 60)
                        .single()
                        .expect("valid fixture time"),
                },
            ),
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
                "a10768fc091c74d250189203381578212a7b3eb7ea3e759ee85584015bf04f89",
                "095c753ad1f2a99c1a29f14db8f4e36c528c159c7e436957ac0f18a46dde7049",
                "dcb21b5799e73e2ca54fd1448f50dd56d5d7994cb173e5279d28942350534863",
            ]
        );
    }

    #[test]
    fn aux_finality_and_metadata_values_round_trip_exactly() {
        let owner = BodyWorkOwner {
            authority: BodyWorkAuthority {
                header: HeaderWorkAuthority {
                    header_generation: HeaderGeneration::new(2),
                    branch: BranchId::new(block::Hash([1; 32]), block::Hash([2; 32])),
                },
                verified_generation: VerifiedGeneration::new(3),
                body_work_epoch: zakura_header_chain::BodyWorkEpoch::default(),
            },
            session_id: 4,
            request_id: NonZeroU64::new(5).expect("five is nonzero"),
        }
        .into();
        let base_aux = AuxDelivery::new(
            EvidenceId::from_digest([6; 32]),
            block::Hash([7; 32]),
            SourceId::from_digest([8; 32]),
            owner,
            BodySizeHint::Known(NonZeroU32::new(9).expect("nine is nonzero")),
            Some(TreeAuxRecordV1 {
                height: block::Height(10),
                sapling_root: sapling::tree::Root::default(),
                orchard_root: orchard::tree::Root::default(),
                ironwood_root: ironwood::tree::Root::default(),
                sapling_tx_count: 11,
                orchard_tx_count: 12,
                ironwood_tx_count: 13,
                auth_data_root: AuthDataRoot::from([14; 32]),
            }),
        );
        let aux = base_aux
            .test_only_with_outcome(1, [Some([11; 32]), None], Some(block::Hash([12; 32])))
            .expect("the authenticated outcome is coherent");
        assert_eq!(
            decode_untrusted_aux_delivery(&aux.encode().expect("aux encodes")),
            Ok(UntrustedAuxDeliveryRow::new(
                base_aux,
                1,
                [Some([11; 32]), None],
                Some(block::Hash([12; 32])),
            ))
        );
        let rejected_aux = base_aux
            .test_only_with_outcome(2, [Some([13; 32]), None], Some(block::Hash([14; 32])))
            .expect("the rejected outcome is coherent");
        assert_eq!(
            decode_untrusted_aux_delivery(&rejected_aux.encode().expect("rejected aux encodes")),
            Ok(UntrustedAuxDeliveryRow::new(
                base_aux,
                2,
                [Some([13; 32]), None],
                Some(block::Hash([14; 32])),
            ))
        );
        let disputed_aux = base_aux
            .test_only_with_outcome(3, [Some([15; 32]), None], Some(block::Hash([16; 32])))
            .expect("the disputed outcome is coherent");
        assert_eq!(
            decode_untrusted_aux_delivery(&disputed_aux.encode().expect("disputed aux encodes")),
            Ok(UntrustedAuxDeliveryRow::new(
                base_aux,
                3,
                [Some([15; 32]), None],
                Some(block::Hash([16; 32])),
            ))
        );
        let mut legacy_aux = aux.encode().expect("aux encodes");
        const OWNER_STATE_VERSION_OFFSET: usize = 32 + 32 + 32;
        legacy_aux[OWNER_STATE_VERSION_OFFSET..OWNER_STATE_VERSION_OFFSET + 8]
            .copy_from_slice(&99_u64.to_be_bytes());
        assert_eq!(
            decode_untrusted_aux_delivery(&legacy_aux),
            Ok(UntrustedAuxDeliveryRow::new(
                base_aux,
                1,
                [Some([11; 32]), None],
                Some(block::Hash([12; 32])),
            ))
        );
        let mut malformed_aux = aux.encode().expect("aux encodes");
        const SAPLING_ROOT_OFFSET: usize = 32 + 32 + 32 + 105 + 4 + 1 + 4;
        malformed_aux[SAPLING_ROOT_OFFSET..SAPLING_ROOT_OFFSET + 32].fill(0xff);
        assert_eq!(
            decode_untrusted_aux_delivery(&malformed_aux),
            Err(HeaderChainValueError::TreeAuxRoot("Sapling"))
        );
        let legacy_outcome = |status_code, evidence: [u8; 32], boundary: Option<[u8; 32]>| {
            let mut bytes = base_aux.encode().expect("base auxiliary delivery encodes");
            *bytes
                .last_mut()
                .expect("the encoded delivery ends in its status code") = status_code;
            bytes.extend(evidence);
            if let Some(boundary) = boundary {
                bytes.extend(boundary);
            }
            bytes
        };
        for bytes in [
            legacy_outcome(1, [17; 32], Some([18; 32])),
            legacy_outcome(2, [19; 32], None),
            legacy_outcome(3, [20; 32], None),
        ] {
            assert_eq!(decode_v1_aux_delivery(&bytes), Ok(base_aux));
        }
        let mut truncated_legacy = legacy_outcome(2, [21; 32], None);
        truncated_legacy.pop();
        assert_eq!(
            decode_v1_aux_delivery(&truncated_legacy),
            Err(HeaderChainValueError::Truncated)
        );
        let finality = FinalityRecord {
            previous: frontier(10, 1),
            current: frontier(11, 2),
            source: FinalitySource::HeadersOnlyDepth {
                selected_tip: frontier(1_011, 3),
            },
            epoch: FinalityEpoch::new(4),
        };
        assert_eq!(
            FinalityRecord::decode(&finality.encode().expect("finality encodes")),
            Ok(finality)
        );

        let metadata = EngineMetadata {
            disk_format: HeaderChainDiskVersion::CURRENT,
            mode: EngineMode::HeadersOnly,
            network_id: NetworkKind::Regtest,
            network_policy_digest: [12; 32],
            anchor_manifest_digest: [13; 32],
            work_origin: frontier(0, 1),
            state_version: StateVersion::new(2),
            header_generation: HeaderGeneration::new(3),
            verified_generation: VerifiedGeneration::new(4),
            finality_epoch: FinalityEpoch::new(5),
            headers_only_migration_epoch: Some(FinalityEpoch::new(4)),
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
                    started_at: Utc
                        .timestamp_opt(70, 80)
                        .single()
                        .expect("valid fixture time"),
                    attempts: 7,
                    suppliers: 8,
                    supplier_set_digest: [9; 32],
                    alarmed: true,
                    next_probe_at: Utc
                        .timestamp_opt(90, 100)
                        .single()
                        .expect("valid fixture time"),
                }),
                migrated_pin_refuted: Some(frontier(1, 2)),
            },
            last_transition: Some(TransitionFingerprint::from_parts(
                TransitionDomain::OperatorInvalidate,
                EvidenceId::from_digest([14; 32]),
                [15; 32],
            )),
        };
        let bytes = metadata.encode().expect("metadata encodes");
        assert_eq!(&bytes[..6], &[0, 0, 0, 3, 1, 2]);
        assert_eq!(EngineMetadata::decode(&bytes), Ok(metadata.clone()));
        // Version one wrote no network policy digest, so its row is the current row with the
        // marker rolled back and that field removed.
        let mut version_one_bytes = bytes.clone();
        version_one_bytes[..4].copy_from_slice(&1_u32.to_be_bytes());
        version_one_bytes.drain(6..38);
        assert_eq!(
            EngineMetadata::decode(&version_one_bytes),
            Err(HeaderChainValueError::UnsupportedDiskFormat(1))
        );
        assert_eq!(
            decode_v1_engine_metadata(&version_one_bytes, metadata.network_policy_digest),
            Ok(EngineMetadata {
                disk_format: HeaderChainDiskVersion(1),
                ..metadata.clone()
            })
        );
        let mut legacy_bytes = bytes.clone();
        legacy_bytes.truncate(
            legacy_bytes
                .len()
                .checked_sub(37)
                .expect("the optional alarm is one tag plus one frontier"),
        );
        assert_eq!(
            EngineMetadata::decode(&legacy_bytes),
            Err(HeaderChainValueError::Truncated)
        );
        // These digests pin the on-disk encodings. Regenerate a digest only together with a
        // deliberate encoding change; an unexplained change means a value's layout drifted.
        // The metadata digest last moved when the header-chain disk version advanced to 3.
        assert_eq!(
            [
                digest(&aux.encode().expect("aux encodes")),
                digest(&finality.encode().expect("finality encodes")),
                digest(&bytes),
            ],
            [
                "c041fc819cc43fcd28dd3ba7fe296271ae0c7225c9bbcdf1dd38152dc313346a",
                "b887bf384510dfb1a255221a8c97066617cb145eaf3e272ad70dc94cd17a3802",
                "a57d37f3cadf2a983019c448ab61b130b1a2230af1e8206b6020c759d37984dc",
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
            FinalityRecord::decode(&[]),
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
        let reason = HeaderEligibilityReasonDisk::Operator {
            id: [1; 16],
            reason_digest: [2; 32],
            evidence: EvidenceId::from_digest([3; 32]),
        };
        let mut trailing = reason.encode().expect("reason encodes");
        trailing.push(0);
        assert_eq!(
            HeaderEligibilityReasonDisk::decode(&trailing),
            Err(HeaderChainValueError::Trailing)
        );
        assert!(matches!(
            get_body_validation_state(&mut Decoder::new(&[9])),
            Err(HeaderChainValueError::UnknownDiscriminant {
                field: "body_state",
                value: 9
            })
        ));
        assert_eq!(
            Decoder::new(&[2]).bool(),
            Err(HeaderChainValueError::InvalidBoolean(2))
        );
        let oversized_ids = vec![EvidenceId::from_digest([0; 32]); MAX_AUX_DELIVERY_IDS + 1];
        assert!(matches!(
            Encoder::default().counted(
                "aux_delivery_ids",
                &oversized_ids,
                MAX_AUX_DELIVERY_IDS,
                |_, _| {}
            ),
            Err(HeaderChainValueError::Oversized {
                field: "aux_delivery_ids",
                ..
            })
        ));
        let oversized_count = u32::try_from(MAX_AUX_DELIVERY_IDS + 1)
            .expect("the auxiliary delivery bound fits u32")
            .to_be_bytes();
        assert!(matches!(
            Decoder::new(&oversized_count).counted(
                "aux_delivery_ids",
                MAX_AUX_DELIVERY_IDS,
                |decoder| decoder.array::<32>()
            ),
            Err(HeaderChainValueError::Oversized {
                field: "aux_delivery_ids",
                ..
            })
        ));
        let mut metadata = vec![0, 0, 0, 4];
        metadata.resize(512, 0);
        assert_eq!(
            EngineMetadata::decode(&metadata),
            Err(HeaderChainValueError::UnsupportedDiskFormat(4))
        );
        metadata[3] = 3;
        metadata[4] = 9;
        assert!(matches!(
            EngineMetadata::decode(&metadata),
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
            crate::service::finalized_state::HEADER_CONSENSUS_INVALID_BODY_TOMBSTONE,
            crate::service::finalized_state::HEADER_BODY_EVIDENCE_AUTHORITY,
            crate::service::finalized_state::HEADER_CHILD,
            crate::service::finalized_state::HEADER_SELECTED,
            crate::service::finalized_state::HEADER_VERIFIED,
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
