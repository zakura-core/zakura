//! The block header.

use std::{
    fmt,
    ops::{Deref, DerefMut},
    sync::{Arc, OnceLock},
};

use chrono::{DateTime, Duration, Utc};
use thiserror::Error;

use crate::{
    fmt::HexDebug,
    parameters::Network,
    serialization::{
        sha256d, CompactSize64, TrustedPreallocate, ZcashSerialize, MAX_HEADERS_PER_MESSAGE,
    },
    work::{difficulty::CompactDifficulty, equihash::Solution},
};

use super::{merkle, Commitment, CommitmentError, Hash, Height};

#[cfg(any(test, feature = "proptest-impl"))]
use proptest_derive::Arbitrary;

/// A block header, containing metadata about a block.
///
/// How are blocks chained together? They are chained together via the
/// backwards reference (previous header hash) present in the block
/// header. Each block points backwards to its parent, all the way
/// back to the genesis block (the first block in the blockchain).
///
/// The first hash calculation is retained with these fields. Mutable access clears
/// that result before exposing the fields, including through `Arc::make_mut`.
/// The saved hash is local metadata, excluded from serialization and equality.
#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Header {
    fields: HeaderFields,
    #[serde(skip)]
    hash: OnceLock<Hash>,
}

/// The editable fields used to construct a block header.
///
/// Convert these fields with [`Header::new`] before sharing or hashing the header.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HeaderFields {
    /// The block's version field. This is supposed to be `4`:
    ///
    /// > The current and only defined block version number for Zcash is 4.
    ///
    /// but this was not enforced by the consensus rules, and defective mining
    /// software created blocks with other versions, so instead it's effectively
    /// a free field. The only constraint is that it must be at least `4` when
    /// interpreted as an `i32`.
    pub version: u32,

    /// The hash of the previous block, used to create a chain of blocks back to
    /// the genesis block.
    ///
    /// This ensures no previous block can be changed without also changing this
    /// block's header.
    pub previous_block_hash: Hash,

    /// The root of the Bitcoin-inherited transaction Merkle tree, binding the
    /// block header to the transactions in the block.
    ///
    /// Note that because of a flaw in Bitcoin's design, the `merkle_root` does
    /// not always precisely bind the contents of the block (CVE-2012-2459). It
    /// is sometimes possible for an attacker to create multiple distinct sets of
    /// transactions with the same Merkle root, although only one set will be
    /// valid.
    pub merkle_root: merkle::Root,

    /// Zcash blocks contain different kinds of commitments to their contents,
    /// depending on the network and height.
    ///
    /// The interpretation of this field has been changed multiple times,
    /// without incrementing the block [`version`](Self::version). Therefore,
    /// this field cannot be parsed without the network and height. Use
    /// [`Block::commitment`](super::Block::commitment) to get the parsed
    /// [`Commitment`].
    pub commitment_bytes: HexDebug<[u8; 32]>,

    /// The block timestamp is a Unix epoch time (UTC) when the miner
    /// started hashing the header (according to the miner).
    pub time: DateTime<Utc>,

    /// An encoded version of the target threshold this block's header
    /// hash must be less than or equal to, in the same nBits format
    /// used by Bitcoin.
    ///
    /// For a block at block height `height`, bits MUST be equal to
    /// `ThresholdBits(height)`.
    ///
    /// [Bitcoin-nBits](https://bitcoin.org/en/developer-reference#target-nbits)
    pub difficulty_threshold: CompactDifficulty,

    /// An arbitrary field that miners can change to modify the header
    /// hash in order to produce a hash less than or equal to the
    /// target threshold.
    pub nonce: HexDebug<[u8; 32]>,

    /// The Equihash solution.
    pub solution: Solution,
}

/// TODO: Use this error as the source for zakura_consensus::error::BlockError::Time,
/// and make `BlockError::Time` add additional context.
/// See <https://github.com/ZcashFoundation/zebra/issues/1021> for more details.
#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum BlockTimeError {
    #[error("invalid time {0:?} in block header {1:?} {2:?}: block time is more than 2 hours in the future ({3:?}). Hint: check your machine's date, time, and time zone.")]
    InvalidBlockTime(
        DateTime<Utc>,
        crate::block::Height,
        crate::block::Hash,
        DateTime<Utc>,
    ),
}

impl Header {
    /// Constructs a header whose hash is computed on its first use.
    pub fn new(fields: HeaderFields) -> Self {
        Self {
            fields,
            hash: OnceLock::new(),
        }
    }

    /// Copies the editable fields, without copying cached metadata.
    pub fn fields(&self) -> HeaderFields {
        self.fields
    }

    /// Shared observable-header validation delegates to this canonical time calculation.
    #[allow(clippy::unwrap_in_result)]
    pub fn time_is_valid_at(
        &self,
        now: DateTime<Utc>,
        height: &Height,
        hash: &Hash,
    ) -> Result<(), BlockTimeError> {
        let two_hours_in_the_future = now
            .checked_add_signed(Duration::hours(2))
            .expect("calculating 2 hours in the future does not overflow");
        if self.time <= two_hours_in_the_future {
            Ok(())
        } else {
            Err(BlockTimeError::InvalidBlockTime(
                self.time,
                *height,
                *hash,
                two_hours_in_the_future,
            ))?
        }
    }

    /// Get the parsed block [`Commitment`] for this header.
    /// Its interpretation depends on the given `network` and block `height`.
    pub fn commitment(
        &self,
        network: &Network,
        height: Height,
    ) -> Result<Commitment, CommitmentError> {
        Commitment::from_bytes(*self.commitment_bytes, network, height)
    }

    /// Returns this header's hash, computing it once until its fields are edited.
    pub fn hash(&self) -> Hash {
        *self.hash.get_or_init(|| {
            let mut writer = sha256d::Writer::default();
            self.zcash_serialize(&mut writer)
                .expect("Sha256dWriter is infallible for a valid header");
            Hash(writer.finish())
        })
    }
}

impl From<HeaderFields> for Header {
    fn from(fields: HeaderFields) -> Self {
        Self::new(fields)
    }
}

impl Deref for Header {
    type Target = HeaderFields;

    fn deref(&self) -> &HeaderFields {
        &self.fields
    }
}

impl DerefMut for Header {
    fn deref_mut(&mut self) -> &mut HeaderFields {
        self.hash.take();
        &mut self.fields
    }
}

impl PartialEq for Header {
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields
    }
}

impl Eq for Header {}

impl fmt::Debug for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Header")
            .field("version", &self.version)
            .field("previous_block_hash", &self.previous_block_hash)
            .field("merkle_root", &self.merkle_root)
            .field("commitment_bytes", &self.commitment_bytes)
            .field("time", &self.time)
            .field("difficulty_threshold", &self.difficulty_threshold)
            .field("nonce", &self.nonce)
            .field("solution", &self.solution)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serialization::{BytesInDisplayOrder, ZcashDeserialize};

    fn genesis_header() -> Header {
        Header::zcash_deserialize(&zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES[..])
            .expect("the Mainnet genesis fixture has a valid header")
    }

    fn hash_serialized_header(header: &Header) -> Hash {
        let bytes = header
            .zcash_serialize_to_vec()
            .expect("the header is valid");
        use sha2::{Digest, Sha256};
        Hash(Sha256::digest(Sha256::digest(bytes)).into())
    }

    #[test]
    fn header_hash_is_saved_and_matches_the_genesis_vector() {
        let header = genesis_header();
        assert!(header.hash.get().is_none());
        let hash = Hash::from(&header);
        assert_eq!(header.hash.get(), Some(&hash));
        assert_eq!(hash, hash_serialized_header(&header));
        assert_eq!(
            hash.to_string(),
            "00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08"
        );
        assert_eq!(Hash::from(header.clone()), hash);
        assert_eq!(Hash::from(Arc::new(header.clone())), hash);
        assert_eq!(header.hash(), hash);
    }

    #[test]
    fn editing_any_header_field_discards_the_saved_hash() {
        let edits: [fn(&mut Header); 8] = [
            |header| header.version += 1,
            |header| header.previous_block_hash.0[0] ^= 1,
            |header| header.merkle_root.0[0] ^= 1,
            |header| header.commitment_bytes.0[0] ^= 1,
            |header| header.time += Duration::seconds(1),
            |header| header.difficulty_threshold.0 ^= 1,
            |header| header.nonce.0[0] ^= 1,
            |header| header.solution = Solution::for_proposal(),
        ];
        for edit in edits {
            let mut header = genesis_header();
            let original = header.hash();
            edit(&mut header);
            assert!(header.hash.get().is_none());
            let updated = header.hash();
            assert_ne!(updated, original);
            assert_eq!(updated, hash_serialized_header(&header));
        }
    }

    #[test]
    fn copying_a_shared_header_for_an_edit_keeps_the_original_hash() {
        let original = Arc::new(genesis_header());
        let original_hash = original.hash();
        let mut edited = original.clone();
        Arc::make_mut(&mut edited).nonce.0[0] ^= 1;
        assert_eq!(original.hash.get(), Some(&original_hash));
        assert_eq!(original.hash(), original_hash);
        assert_ne!(edited.hash(), original_hash);
    }

    #[test]
    fn saved_hash_does_not_change_serialization_or_equality() {
        let header = genesis_header();
        let cold = header.clone();
        let bytes = header.zcash_serialize_to_vec().unwrap();
        let json = serde_json::to_value(&header).unwrap();
        header.hash();
        assert_eq!(header, cold);
        assert_eq!(header.zcash_serialize_to_vec().unwrap(), bytes);
        assert_eq!(serde_json::to_value(&header).unwrap(), json);
        assert_eq!(format!("{header:?}"), format!("{cold:?}"));
        let decoded: Header = serde_json::from_value(json).unwrap();
        assert!(decoded.hash.get().is_none());
        assert_eq!(decoded.hash(), header.hash());
        assert_eq!(decoded.hash().bytes_in_serialized_order(), header.hash().0);
    }

    #[test]
    fn concurrent_header_hash_reads_share_the_saved_result() {
        let header = Arc::new(genesis_header());
        let expected = hash_serialized_header(&header);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let header = header.clone();
                scope.spawn(move || assert_eq!(header.hash(), expected));
            }
        });
        assert_eq!(header.hash.get(), Some(&expected));
    }
}

/// A header with a count of the number of transactions in its block.
/// This structure is used in the Bitcoin network protocol.
///
/// The transaction count field is always zero, so we don't store it in the struct.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(any(test, feature = "proptest-impl"), derive(Arbitrary))]
pub struct CountedHeader {
    /// The header for a block
    pub header: Arc<Header>,
}

/// The number of bytes in a serialized header before the Equihash solution:
/// the Equihash input fields, then the 32-byte nonce.
pub(crate) const HEADER_BYTES_BEFORE_SOLUTION: usize = Solution::INPUT_LENGTH + 32;

/// The serialized size of a Mainnet or Testnet header, the largest on any
/// network. Regtest headers are smaller, so a buffer of this size fits any header.
pub(crate) const MAX_SERIALIZED_HEADER_BYTES: usize = HEADER_BYTES_BEFORE_SOLUTION
    // Cast safe: SOLUTION_SIZE is the constant 1344, which fits in u64.
    + CompactSize64::encoded_len(crate::work::equihash::SOLUTION_SIZE as u64)
    + crate::work::equihash::SOLUTION_SIZE;

/// The Zcash accepted block version.
///
/// The consensus rules do not force the block version to be this value but just equal or greater than it.
/// However, it is suggested that submitted block versions to be of this exact value.
pub const ZCASH_BLOCK_VERSION: u32 = 4;

impl TrustedPreallocate for CountedHeader {
    fn min_serialized_size() -> u64 {
        const ZERO_TRANSACTION_COUNT_BYTES: usize = 1;

        // Regtest's 36-byte solution needs a one-byte length prefix. Mainnet
        // and Testnet require 1,344 solution bytes and a three-byte prefix.
        // Use the smallest supported encoding so no valid network is excluded.
        let solution_bytes = crate::work::equihash::REGTEST_SOLUTION_SIZE;
        let solution_length_prefix = CompactSize64::encoded_len(
            u64::try_from(solution_bytes).expect("fixed Equihash solution sizes fit in u64"),
        );
        let minimum_bytes = HEADER_BYTES_BEFORE_SOLUTION
            + solution_length_prefix
            + solution_bytes
            + ZERO_TRANSACTION_COUNT_BYTES;
        u64::try_from(minimum_bytes).expect("the fixed counted header size fits in u64")
    }

    /// Cap `CountedHeader` preallocation at the existing protocol-level
    /// constant `MAX_HEADERS_PER_MESSAGE = 160`. The previous return value was
    /// derived from `MAX_PROTOCOL_MESSAGE_LEN`, allowing peer-controlled
    /// preallocation amplification — same shape as GHSA-xr93-pcq3-pxf8 for
    /// `AddrV1`/`AddrV2` (PR #10494).
    fn max_allocation() -> u64 {
        // Cast safe: MAX_HEADERS_PER_MESSAGE is the constant 160, which fits
        // trivially in u64 on every platform.
        MAX_HEADERS_PER_MESSAGE as u64
    }
}
