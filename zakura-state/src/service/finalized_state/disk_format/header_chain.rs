//! Stable ordered key encodings for the fork-aware header-chain schema.

#![allow(dead_code)] // These codecs are consumed by the serialized state adapter in PR-8.

use thiserror::Error;
use zakura_chain::{block, work::difficulty::U256};
use zakura_header_chain::{ChainScore, EvidenceId, FinalityEpoch, SuffixWork};

use super::{FromDisk, IntoDisk};

/// A malformed version-one header-chain key.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub enum HeaderChainKeyError {
    /// A fixed-width key had a different byte length.
    #[error("header-chain key has length {actual}, expected {expected}")]
    Length {
        /// Required version-one length.
        expected: usize,
        /// Supplied length.
        actual: usize,
    },
    /// An eligibility-reason key used an unassigned discriminant.
    #[error("unknown eligibility-reason key discriminant {0}")]
    UnknownReason(u8),
    /// Deferred-time nanoseconds were outside `0..1_000_000_000`.
    #[error("invalid deferred-time nanoseconds {0}")]
    InvalidNanoseconds(u32),
}

fn fixed<const N: usize>(bytes: impl AsRef<[u8]>) -> Result<[u8; N], HeaderChainKeyError> {
    let bytes = bytes.as_ref();
    bytes.try_into().map_err(|_| HeaderChainKeyError::Length {
        expected: N,
        actual: bytes.len(),
    })
}

/// Parent-hash plus child-hash adjacency key.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct HeaderChildKey {
    /// Exact parent hash.
    pub parent: block::Hash,
    /// Exact child hash.
    pub child: block::Hash,
}

impl IntoDisk for HeaderChildKey {
    type Bytes = [u8; 64];

    fn as_bytes(&self) -> Self::Bytes {
        let mut bytes = [0; 64];
        bytes[..32].copy_from_slice(&self.parent.0);
        bytes[32..].copy_from_slice(&self.child.0);
        bytes
    }
}

impl FromDisk for HeaderChildKey {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let bytes = fixed::<64>(bytes).expect("header-child keys have a fixed v1 width");
        Self {
            parent: block::Hash(
                bytes[..32]
                    .try_into()
                    .expect("the slice is exactly 32 bytes"),
            ),
            child: block::Hash(
                bytes[32..]
                    .try_into()
                    .expect("the slice is exactly 32 bytes"),
            ),
        }
    }
}

/// Big-endian height plus raw hash multimap key.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct HeaderHeightHashKey {
    /// Exact locally inferred height.
    pub height: block::Height,
    /// One header at that height.
    pub hash: block::Hash,
}

/// Fixed four-byte big-endian projection height key.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct HeaderHeightKey(pub block::Height);

impl IntoDisk for HeaderHeightKey {
    type Bytes = [u8; 4];

    fn as_bytes(&self) -> Self::Bytes {
        let Self(height) = self;
        height.0.to_be_bytes()
    }
}

impl FromDisk for HeaderHeightKey {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        Self(block::Height(u32::from_be_bytes(
            fixed::<4>(bytes).expect("projection height keys have a fixed v1 width"),
        )))
    }
}

impl IntoDisk for HeaderHeightHashKey {
    type Bytes = [u8; 36];

    fn as_bytes(&self) -> Self::Bytes {
        let mut bytes = [0; 36];
        bytes[..4].copy_from_slice(&self.height.0.to_be_bytes());
        bytes[4..].copy_from_slice(&self.hash.0);
        bytes
    }
}

impl FromDisk for HeaderHeightHashKey {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let bytes = fixed::<36>(bytes).expect("height-hash keys have a fixed v1 width");
        Self {
            height: block::Height(u32::from_be_bytes(
                bytes[..4].try_into().expect("the slice is exactly 4 bytes"),
            )),
            hash: block::Hash(
                bytes[4..]
                    .try_into()
                    .expect("the slice is exactly 32 bytes"),
            ),
        }
    }
}

/// Ordered greatest-work candidate key: 256-bit work then raw tip hash.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HeaderCandidateKey(pub ChainScore);

impl IntoDisk for HeaderCandidateKey {
    type Bytes = [u8; 64];

    fn as_bytes(&self) -> Self::Bytes {
        let mut bytes = [0; 64];
        bytes[..32].copy_from_slice(&self.0.suffix_work.as_u256().to_big_endian());
        bytes[32..].copy_from_slice(&self.0.tip_hash.0);
        bytes
    }
}

impl FromDisk for HeaderCandidateKey {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let bytes = fixed::<64>(bytes).expect("candidate keys have a fixed v1 width");
        Self(ChainScore::new(
            SuffixWork::new(U256::from_big_endian(&bytes[..32])),
            block::Hash(
                bytes[32..]
                    .try_into()
                    .expect("the slice is exactly 32 bytes"),
            ),
        ))
    }
}

/// Stable reason-kind ordering used by the eligibility-root index.
#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum EligibilityReasonKind {
    /// Compiled settled-upgrade conflict.
    SettledUpgrade = 0,
    /// Authenticated local checkpoint conflict.
    LocalCheckpoint = 1,
    /// Immutable finality conflict.
    Finality = 2,
    /// Deterministic body-consensus failure.
    ConsensusBody = 3,
    /// Reversible operator invalidation.
    Operator = 4,
}

impl TryFrom<u8> for EligibilityReasonKind {
    type Error = HeaderChainKeyError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::SettledUpgrade),
            1 => Ok(Self::LocalCheckpoint),
            2 => Ok(Self::Finality),
            3 => Ok(Self::ConsensusBody),
            4 => Ok(Self::Operator),
            other => Err(HeaderChainKeyError::UnknownReason(other)),
        }
    }
}

impl EligibilityReasonKind {
    const fn discriminant(self) -> u8 {
        match self {
            Self::SettledUpgrade => 0,
            Self::LocalCheckpoint => 1,
            Self::Finality => 2,
            Self::ConsensusBody => 3,
            Self::Operator => 4,
        }
    }
}

/// Reason kind, direct root hash, and stable evidence identity.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HeaderEligibilityRootKey {
    /// Stable reason category.
    pub kind: EligibilityReasonKind,
    /// Header carrying the direct reason.
    pub root: block::Hash,
    /// Stable reason evidence.
    pub evidence: EvidenceId,
}

impl HeaderEligibilityRootKey {
    /// Decode while rejecting unassigned reason discriminants.
    pub fn try_from_bytes(bytes: impl AsRef<[u8]>) -> Result<Self, HeaderChainKeyError> {
        let bytes = fixed::<65>(bytes)?;
        let mut root = [0; 32];
        root.copy_from_slice(&bytes[1..33]);
        let mut evidence = [0; 32];
        evidence.copy_from_slice(&bytes[33..]);
        Ok(Self {
            kind: bytes[0].try_into()?,
            root: block::Hash(root),
            evidence: EvidenceId::from_digest(evidence),
        })
    }
}

impl IntoDisk for HeaderEligibilityRootKey {
    type Bytes = [u8; 65];

    fn as_bytes(&self) -> Self::Bytes {
        let mut bytes = [0; 65];
        bytes[0] = self.kind.discriminant();
        bytes[1..33].copy_from_slice(&self.root.0);
        bytes[33..].copy_from_slice(&self.evidence.digest());
        bytes
    }
}

impl FromDisk for HeaderEligibilityRootKey {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        Self::try_from_bytes(bytes).expect("eligibility-root keys use valid v1 discriminants")
    }
}

/// Header hash plus delivery identity.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HeaderAuxDeliveryKey {
    /// Exact retained header.
    pub header: block::Hash,
    /// Stable delivery evidence.
    pub delivery: EvidenceId,
}

impl IntoDisk for HeaderAuxDeliveryKey {
    type Bytes = [u8; 64];

    fn as_bytes(&self) -> Self::Bytes {
        let mut bytes = [0; 64];
        bytes[..32].copy_from_slice(&self.header.0);
        bytes[32..].copy_from_slice(&self.delivery.digest());
        bytes
    }
}

impl FromDisk for HeaderAuxDeliveryKey {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        let bytes = fixed::<64>(bytes).expect("aux-delivery keys have a fixed v1 width");
        Self {
            header: block::Hash(
                bytes[..32]
                    .try_into()
                    .expect("the slice is exactly 32 bytes"),
            ),
            delivery: EvidenceId::from_digest(
                bytes[32..]
                    .try_into()
                    .expect("the slice is exactly 32 bytes"),
            ),
        }
    }
}

/// Order-preserving UTC seconds/nanoseconds plus deferred header hash.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HeaderDeferredKey {
    /// Signed Unix seconds.
    pub seconds: i64,
    /// Subsecond nanoseconds.
    pub nanoseconds: u32,
    /// Exact deferred header.
    pub hash: block::Hash,
}

impl HeaderDeferredKey {
    /// Construct a valid UTC instant key.
    pub fn new(
        seconds: i64,
        nanoseconds: u32,
        hash: block::Hash,
    ) -> Result<Self, HeaderChainKeyError> {
        if nanoseconds >= 1_000_000_000 {
            return Err(HeaderChainKeyError::InvalidNanoseconds(nanoseconds));
        }
        Ok(Self {
            seconds,
            nanoseconds,
            hash,
        })
    }

    /// Decode a durable deferred key while rejecting malformed timestamps.
    pub fn try_from_bytes(bytes: impl AsRef<[u8]>) -> Result<Self, HeaderChainKeyError> {
        let bytes = fixed::<44>(bytes)?;
        let ordered = u64::from_be_bytes(fixed::<8>(&bytes[..8])?);
        let seconds = i64::from_be_bytes((ordered ^ (1_u64 << 63)).to_be_bytes());
        let nanoseconds = u32::from_be_bytes(fixed::<4>(&bytes[8..12])?);
        Self::new(
            seconds,
            nanoseconds,
            block::Hash(fixed::<32>(&bytes[12..])?),
        )
    }
}

impl IntoDisk for HeaderDeferredKey {
    type Bytes = [u8; 44];

    fn as_bytes(&self) -> Self::Bytes {
        let mut bytes = [0; 44];
        let ordered_seconds = u64::from_be_bytes(self.seconds.to_be_bytes()) ^ (1_u64 << 63);
        bytes[..8].copy_from_slice(&ordered_seconds.to_be_bytes());
        bytes[8..12].copy_from_slice(&self.nanoseconds.to_be_bytes());
        bytes[12..].copy_from_slice(&self.hash.0);
        bytes
    }
}

impl FromDisk for HeaderDeferredKey {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        Self::try_from_bytes(bytes).expect("deferred keys contain a valid v1 timestamp")
    }
}

/// Big-endian finality-epoch history key.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HeaderFinalityKey(pub FinalityEpoch);

impl IntoDisk for HeaderFinalityKey {
    type Bytes = [u8; 8];

    fn as_bytes(&self) -> Self::Bytes {
        self.0.get().to_be_bytes()
    }
}

impl FromDisk for HeaderFinalityKey {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self {
        Self(FinalityEpoch::new(u64::from_be_bytes(
            fixed::<8>(bytes).expect("finality keys have a fixed v1 width"),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_golden_bytes_are_fixed_and_order_preserving() {
        let height_hash = HeaderHeightHashKey {
            height: block::Height(0x0102_0304),
            hash: block::Hash([0xaa; 32]),
        };
        assert_eq!(&height_hash.as_bytes()[..4], &[1, 2, 3, 4]);
        assert_eq!(
            HeaderHeightHashKey::from_bytes(height_hash.as_bytes()),
            height_hash
        );
        assert_eq!(
            HeaderHeightKey::from_bytes(HeaderHeightKey(height_hash.height).as_bytes()),
            HeaderHeightKey(height_hash.height)
        );

        let child = HeaderChildKey {
            parent: block::Hash([1; 32]),
            child: block::Hash([2; 32]),
        };
        assert_eq!(&child.as_bytes()[..32], &[1; 32]);
        assert_eq!(&child.as_bytes()[32..], &[2; 32]);
        assert_eq!(HeaderChildKey::from_bytes(child.as_bytes()), child);

        let score = HeaderCandidateKey(ChainScore::new(
            SuffixWork::new(U256::from(0x0102_u32)),
            block::Hash([3; 32]),
        ));
        let score_bytes = score.as_bytes();
        assert_eq!(&score_bytes[30..32], &[1, 2]);
        assert_eq!(&score_bytes[32..], &[3; 32]);
        assert_eq!(HeaderCandidateKey::from_bytes(score_bytes), score);

        let negative = HeaderDeferredKey::new(-1, 0x0102_0304, block::Hash([4; 32]))
            .expect("the fixture nanoseconds are valid");
        let zero =
            HeaderDeferredKey::new(0, 0, block::Hash([0; 32])).expect("zero is a valid instant");
        assert!(negative.as_bytes() < zero.as_bytes());
        assert_eq!(
            &negative.as_bytes()[..8],
            &[0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
        );
        assert_eq!(&negative.as_bytes()[8..12], &[1, 2, 3, 4]);
        assert_eq!(HeaderDeferredKey::from_bytes(negative.as_bytes()), negative);

        let epoch = HeaderFinalityKey(FinalityEpoch::new(0x0102_0304_0506_0708));
        assert_eq!(epoch.as_bytes(), [1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(HeaderFinalityKey::from_bytes(epoch.as_bytes()), epoch);
    }

    #[test]
    fn composite_key_discriminants_and_lengths_fail_closed() {
        use crate::service::finalized_state::{
            HEADER_AUX_DELIVERY, HEADER_CANDIDATE, HEADER_CHILD, HEADER_DEFERRED,
            HEADER_ELIGIBILITY_ROOT, HEADER_ENGINE_META, HEADER_FINALITY_HISTORY,
            HEADER_HEIGHT_HASH, HEADER_NODE_BY_HASH, HEADER_SELECTED, HEADER_VALIDATION_CONTEXT,
            HEADER_VERIFIED, STATE_COLUMN_FAMILIES_IN_CODE,
        };

        let required = [
            HEADER_NODE_BY_HASH,
            HEADER_CHILD,
            HEADER_HEIGHT_HASH,
            HEADER_SELECTED,
            HEADER_VERIFIED,
            HEADER_CANDIDATE,
            HEADER_ELIGIBILITY_ROOT,
            HEADER_AUX_DELIVERY,
            HEADER_DEFERRED,
            HEADER_FINALITY_HISTORY,
            HEADER_VALIDATION_CONTEXT,
            HEADER_ENGINE_META,
        ];
        for name in required {
            assert_eq!(
                STATE_COLUMN_FAMILIES_IN_CODE
                    .iter()
                    .filter(|candidate| **candidate == name)
                    .count(),
                1,
                "header-chain column family must be opened exactly once: {name}"
            );
        }

        let key = HeaderEligibilityRootKey {
            kind: EligibilityReasonKind::ConsensusBody,
            root: block::Hash([5; 32]),
            evidence: EvidenceId::from_digest([6; 32]),
        };
        assert_eq!(key.as_bytes()[0], 3);
        assert_eq!(
            HeaderEligibilityRootKey::try_from_bytes(key.as_bytes()),
            Ok(key)
        );
        let mut unknown = key.as_bytes();
        unknown[0] = 5;
        assert_eq!(
            HeaderEligibilityRootKey::try_from_bytes(unknown),
            Err(HeaderChainKeyError::UnknownReason(5))
        );
        assert!(matches!(
            HeaderEligibilityRootKey::try_from_bytes([0; 64]),
            Err(HeaderChainKeyError::Length {
                expected: 65,
                actual: 64
            })
        ));
        assert_eq!(
            HeaderDeferredKey::new(0, 1_000_000_000, block::Hash([0; 32])),
            Err(HeaderChainKeyError::InvalidNanoseconds(1_000_000_000))
        );
        let mut invalid_deferred = [0; 44];
        invalid_deferred[8..12].copy_from_slice(&1_000_000_000_u32.to_be_bytes());
        assert_eq!(
            HeaderDeferredKey::try_from_bytes(invalid_deferred),
            Err(HeaderChainKeyError::InvalidNanoseconds(1_000_000_000))
        );

        let aux = HeaderAuxDeliveryKey {
            header: block::Hash([7; 32]),
            delivery: EvidenceId::from_digest([8; 32]),
        };
        assert_eq!(HeaderAuxDeliveryKey::from_bytes(aux.as_bytes()), aux);
    }

    #[test]
    fn registered_header_chain_column_families_open_in_the_existing_database() {
        use crate::{
            constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
            service::finalized_state::{ZakuraDb, STATE_COLUMN_FAMILIES_IN_CODE},
            Config,
        };
        use zakura_chain::parameters::Network;

        let config = Config::ephemeral();
        let db = ZakuraDb::new(
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
        .expect("the existing finalized-state database opens every registered column family");
        let names = rocksdb::DB::list_cf(&rocksdb::Options::default(), db.path())
            .expect("the open RocksDB column-family manifest is readable");
        for expected in [
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
            assert!(
                names.iter().any(|name| name == expected),
                "missing opened column family {expected}"
            );
        }
        assert_eq!(db.format_version_in_code().minor, 1);
    }
}
