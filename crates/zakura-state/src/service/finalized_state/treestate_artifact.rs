//! Binary artifacts that let a fast-synced node serve historical treestates.
//!
//! Two artifacts, with deliberately different trust stories (see
//! `docs/design/verified-commitment-trees.md` §16):
//!
//! - The **frontier artifact** holds per-pool note commitment frontiers at a sparse height grid.
//!   Every entry is checked against the authenticated root in `commitment_roots_by_height` before
//!   use, so the artifact carries no trust weight: a corrupt or hostile one is rejected rather
//!   than absorbed. That is what lets it be coarse, small, and distributed outside the binary.
//! - The **subtree-root artifact** holds completed subtree roots. The final frontier pins all of
//!   them through its ommers, so the embedded artifact is checked against the embedded frontier
//!   before a read service can use it.
//!
//! Both follow the framing the Sprout history artifact established: magic, version, network byte,
//! explicit record counts, and a SHA-256 over the non-digest header fields and payload, with the
//! parser validating the whole frame before any record is used.

use std::sync::{Arc, OnceLock};

use bincode::Options;
use sha2::{Digest, Sha256};
use thiserror::Error;

use zakura_chain::{
    block::Height,
    ironwood, orchard,
    parameters::{Network, NetworkKind},
    sapling,
    subtree::{NoteCommitmentSubtreeData, NoteCommitmentSubtreeIndex},
    subtree_verify::SubtreeRootsError,
};

use crate::service::finalized_state::disk_format::IntoDisk;

use super::commitment_aux::FinalFrontiers;

/// Deserializes a note commitment tree blob without panicking on malformed bytes.
///
/// The blobs use the same encoding as [`IntoDisk`], but the matching `FromDisk` impl `expect`s on
/// a decode failure, which is right for the node's own database and wrong here: an artifact is
/// untrusted input, and a hostile one must be rejected rather than crash the node.
fn decode_tree<T: serde::de::DeserializeOwned>(blob: &[u8]) -> Option<T> {
    bincode::DefaultOptions::new().deserialize(blob).ok()
}

/// Magic bytes identifying a frontier artifact.
const FRONTIER_MAGIC: &[u8; 8] = b"ZKVCTFR1";

/// Magic bytes identifying a subtree-root artifact.
const SUBTREE_MAGIC: &[u8; 8] = b"ZKVCTST1";

/// Fixed header length: magic, version, network, grid spacing, checkpoint, record count, digest.
const FRONTIER_HEADER_LEN: usize = 8 + 2 + 1 + 4 + 4 + 4 + 32;

/// The most frontier entries an artifact may declare.
///
/// A one-block grid across a chain far longer than any real one still fits well inside this, so a
/// declared count above it is a corrupt or hostile header rather than a legitimate artifact. This
/// is a coarse sanity cap; the payload-length check in [`FrontierArtifact::decode`] is what stops
/// a still-legal count from driving a huge allocation.
const MAX_FRONTIER_ENTRIES: usize = 16_000_000;

/// Minimum encoded size of one frontier entry: a height and three `u32` length prefixes.
///
/// Empty tree blobs are legal in the framing, so this is the floor. Decode rejects a count that
/// cannot fit in the payload at this size before allocating the entry vector.
const MIN_FRONTIER_ENTRY_LEN: usize = 4 + 4 + 4 + 4;

/// Offset of the digest after magic, version, network, spacing, checkpoint, and record count.
const FRONTIER_DIGEST_OFFSET: usize = 8 + 2 + 1 + 4 + 4 + 4;

/// Reviewed completed-subtree roots shipped with the Mainnet last checkpoint.
pub(super) const MAINNET_SUBTREES: &[u8] = include_bytes!("vct/mainnet-subtrees.bin");

/// The format version treestate artifacts are written at.
const VERSION: u16 = 1;

/// Offset of the digest after magic, version, network, last_checkpoint, and three record counts.
const SUBTREE_DIGEST_OFFSET: usize = 8 + 2 + 1 + 4 + 4 + 4 + 4;

/// Fixed header length, including the digest.
const SUBTREE_HEADER_LEN: usize = SUBTREE_DIGEST_OFFSET + 32;

/// Bytes per subtree record: index, end height, root.
const SUBTREE_RECORD_LEN: usize = 2 + 4 + 32;

/// The most subtree records an artifact may declare per pool.
///
/// Subtree indexes are `u16`, so a pool can never complete more than this many.
const MAX_SUBTREE_RECORDS: usize = u16::MAX as usize + 1;

const SAPLING_POOL: &str = "sapling";
const ORCHARD_POOL: &str = "orchard";
const IRONWOOD_POOL: &str = "ironwood";
const SUBTREE_POOLS: [&str; 3] = [SAPLING_POOL, ORCHARD_POOL, IRONWOOD_POOL];

/// Why an artifact could not be parsed or did not describe what the caller expected.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum TreestateArtifactError {
    /// The artifact does not start with the expected magic bytes.
    #[error("not a {kind} artifact: wrong magic bytes")]
    InvalidMagic {
        /// Which artifact was expected.
        kind: &'static str,
    },

    /// The artifact declares a format version this binary does not implement.
    #[error("unsupported {kind} artifact version {found}, expected {VERSION}")]
    UnsupportedVersion {
        /// Which artifact was being parsed.
        kind: &'static str,
        /// The version the artifact declares.
        found: u16,
    },

    /// The artifact was generated for a different network.
    #[error("{kind} artifact is for network byte {found}, expected {expected}")]
    WrongNetwork {
        /// Which artifact was being parsed.
        kind: &'static str,
        /// The network byte the artifact declares.
        found: u8,
        /// The network byte this node expects.
        expected: u8,
    },

    /// The artifact was generated for a different last checkpoint.
    #[error("{kind} artifact last checkpoint {found:?} does not match expected last checkpoint {expected:?}")]
    WrongLastCheckpoint {
        /// Which artifact was being parsed.
        kind: &'static str,
        /// The last checkpoint encoded in the artifact.
        found: Height,
        /// The last checkpoint expected by this binary.
        expected: Height,
    },

    /// The artifact is shorter than its own framing requires.
    #[error("{kind} artifact is truncated at offset {offset}: needs {needed} more bytes")]
    Truncated {
        /// Which artifact was being parsed.
        kind: &'static str,
        /// Where the parse ran out of bytes.
        offset: usize,
        /// How many more bytes the frame required.
        needed: usize,
    },

    /// The artifact declares more records than the format allows.
    #[error("{kind} artifact declares {found} records, more than the {max} limit")]
    TooManyRecords {
        /// Which artifact was being parsed.
        kind: &'static str,
        /// The declared count.
        found: usize,
        /// The format's limit.
        max: usize,
    },

    /// The bytes authenticated by this artifact format do not hash to the stored digest.
    #[error("{kind} artifact contents do not match the digest in its header")]
    DigestMismatch {
        /// Which artifact was being parsed.
        kind: &'static str,
    },

    /// Bytes remain after the last declared record.
    #[error("{kind} artifact has {trailing} trailing bytes after its last record")]
    TrailingBytes {
        /// Which artifact was being parsed.
        kind: &'static str,
        /// How many bytes remain.
        trailing: usize,
    },

    /// A pool's declared subtree indexes are not the contiguous range `0..count`.
    ///
    /// Frontier verification authenticates root values in vector order, but serving selects by the
    /// declared index. Requiring indexes to equal their ordinal binds each authenticated root to
    /// the only position it can correctly occupy.
    #[error(
        "{pool} subtree indexes are not contiguous from zero: \
         expected index {expected}, found {found}"
    )]
    NonContiguousSubtreeIndex {
        /// The pool whose indexes failed.
        pool: &'static str,
        /// The index required at this position.
        expected: u16,
        /// The index the record declared.
        found: u16,
    },

    /// A pool's `end_height` values are not strictly increasing.
    ///
    /// Completed subtrees finish in index order, so heights must rise with indexes. A decrease or
    /// plateau would let tip-bound serving admit or withhold a root relative to the wrong tip.
    #[error(
        "{pool} subtree end heights are out of order: \
         index {previous_index} ends at {previous_height:?}, \
         but index {found_index} ends at {found_height:?}"
    )]
    NonIncreasingEndHeight {
        /// The pool whose heights failed.
        pool: &'static str,
        /// The preceding record's index.
        previous_index: u16,
        /// The preceding record's end height.
        previous_height: Height,
        /// The offending record's index.
        found_index: u16,
        /// The offending record's end height.
        found_height: Height,
    },

    /// A record claims to complete above the artifact's last checkpoint.
    ///
    /// The artifact covers subtrees completed through that checkpoint; a height above it escapes
    /// the tip bound that keeps unverified blocks from being served from published roots.
    #[error(
        "{pool} subtree index {index} ends at {end_height:?}, \
         which is above last checkpoint {last_checkpoint:?}"
    )]
    EndHeightAboveCheckpoint {
        /// The pool whose height failed.
        pool: &'static str,
        /// The record's index.
        index: u16,
        /// The record's claimed end height.
        end_height: Height,
        /// The artifact's last checkpoint.
        last_checkpoint: Height,
    },

    /// A subtree root is not a canonical node encoding for its pool.
    #[error("{pool} subtree root at index {index} is not a valid {pool} node")]
    MalformedSubtreeRoot {
        /// The pool the root belongs to.
        pool: &'static str,
        /// The subtree index that failed to decode.
        index: u16,
    },

    /// The subtree roots do not match the frontier that pins them.
    #[error("{pool} subtree roots do not match the {pool} frontier: {source}")]
    UnverifiedSubtreeRoots {
        /// The pool whose roots failed.
        pool: &'static str,
        /// Why the check failed.
        #[source]
        source: SubtreeRootsError,
    },

    /// The frontier the roots were to be checked against could not be parsed.
    #[error("cannot check subtree roots: {error}")]
    InvalidFrontier {
        /// Why the frontier could not be parsed.
        error: String,
    },

    /// This network ships no frontier to check subtree roots against.
    #[error("no embedded frontier is available for this network")]
    NoEmbeddedFrontier,

    /// A note commitment tree blob could not be deserialized.
    #[error("{kind} artifact has an unreadable {pool} tree at height {height:?}")]
    UnreadableTree {
        /// Which artifact was being parsed.
        kind: &'static str,
        /// Which pool's tree failed.
        pool: &'static str,
        /// The entry's height.
        height: Height,
    },

    /// Entry heights are not strictly increasing.
    ///
    /// Ordering is what makes "nearest entry at or below `h`" a binary search rather than a scan.
    #[error("{kind} artifact entries are out of order: {previous:?} is followed by {found:?}")]
    OutOfOrder {
        /// Which artifact was being parsed.
        kind: &'static str,
        /// The previous entry's key.
        previous: u32,
        /// The offending entry's key.
        found: u32,
    },
}

/// How many subtree roots were proven against a frontier, per pool.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VerifiedSubtreeCounts {
    /// Proven Sapling roots.
    pub sapling: usize,
    /// Proven Orchard roots.
    pub orchard: usize,
    /// Proven Ironwood roots.
    pub ironwood: usize,
}

impl VerifiedSubtreeCounts {
    /// Returns the total number of roots proven.
    pub fn total(&self) -> usize {
        self.sapling + self.orchard + self.ironwood
    }
}

/// Returns the network byte an artifact for `network` carries.
fn network_byte(network: &Network) -> u8 {
    match network.kind() {
        NetworkKind::Mainnet => 1,
        NetworkKind::Testnet => 2,
        NetworkKind::Regtest => 3,
    }
}

/// Reads a fixed-size array from `bytes` at `offset`, or reports a truncated frame.
fn read_array<const N: usize>(
    bytes: &[u8],
    offset: usize,
    kind: &'static str,
) -> Result<[u8; N], TreestateArtifactError> {
    bytes
        .get(offset..offset.saturating_add(N))
        .and_then(|slice| slice.try_into().ok())
        .ok_or(TreestateArtifactError::Truncated {
            kind,
            offset,
            needed: N,
        })
}

/// Reads a `u32`-length-prefixed blob at `offset`, returning it and the offset just past it.
fn read_blob(
    bytes: &[u8],
    offset: usize,
    kind: &'static str,
) -> Result<(Vec<u8>, usize), TreestateArtifactError> {
    // Zakura's supported platforms have at least 32-bit `usize`, so every `u32` length fits.
    let len = u32::from_le_bytes(read_array::<4>(bytes, offset, kind)?) as usize;
    let start = offset
        .checked_add(4)
        .ok_or(TreestateArtifactError::Truncated {
            kind,
            offset,
            needed: 4,
        })?;
    let end = start
        .checked_add(len)
        .ok_or(TreestateArtifactError::Truncated {
            kind,
            offset: start,
            needed: len,
        })?;
    let blob = bytes
        .get(start..end)
        .ok_or(TreestateArtifactError::Truncated {
            kind,
            offset: start,
            needed: len,
        })?;

    Ok((blob.to_vec(), end))
}

/// Appends a `u32`-length-prefixed blob to `out`.
fn write_blob(out: &mut Vec<u8>, blob: &[u8]) {
    let len = u32::try_from(blob.len()).expect("a note commitment tree fits in u32 bytes");
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(blob);
}

/// Per-pool note commitment frontiers at one height in the grid.
#[derive(Clone, Debug)]
pub struct FrontierEntry {
    /// The height these frontiers are the state at the end of.
    pub height: Height,

    /// The Sapling frontier.
    pub sapling: Arc<sapling::tree::NoteCommitmentTree>,

    /// The Orchard frontier.
    pub orchard: Arc<orchard::tree::NoteCommitmentTree>,

    /// The Ironwood frontier.
    pub ironwood: Arc<ironwood::tree::NoteCommitmentTree>,
}

/// Note commitment frontiers at a sparse height grid, used to anchor on-demand derivation.
///
/// Entries must be strictly increasing in height. Nothing here is trusted: a consumer checks each
/// entry's roots against authenticated per-height commitment roots before using it.
#[derive(Clone, Debug)]
pub struct FrontierArtifact {
    /// The height spacing the grid was generated at.
    ///
    /// Recorded for provenance; consumers locate entries by searching rather than assuming this
    /// spacing.
    pub spacing: u32,

    /// The last checkpoint the grid was generated against.
    pub last_checkpoint: Height,

    /// Grid entries, strictly increasing in height.
    pub entries: Vec<FrontierEntry>,
}

impl FrontierArtifact {
    /// The name used in error messages.
    const KIND: &'static str = "frontier";

    /// Serializes to the artifact byte format.
    pub fn encode(&self, network: &Network) -> Vec<u8> {
        let mut payload = Vec::new();
        for entry in &self.entries {
            payload.extend_from_slice(&entry.height.0.to_le_bytes());
            write_blob(&mut payload, &IntoDisk::as_bytes(&*entry.sapling));
            write_blob(&mut payload, &IntoDisk::as_bytes(&*entry.orchard));
            write_blob(&mut payload, &IntoDisk::as_bytes(&*entry.ironwood));
        }

        let mut out = Vec::with_capacity(FRONTIER_HEADER_LEN + payload.len());
        out.extend_from_slice(FRONTIER_MAGIC);
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.push(network_byte(network));
        out.extend_from_slice(&self.spacing.to_le_bytes());
        out.extend_from_slice(&self.last_checkpoint.0.to_le_bytes());
        out.extend_from_slice(
            &u32::try_from(self.entries.len())
                .expect("entry count is bounded by MAX_FRONTIER_ENTRIES")
                .to_le_bytes(),
        );
        debug_assert_eq!(out.len(), FRONTIER_DIGEST_OFFSET);

        let digest = {
            let mut hasher = Sha256::new();
            hasher.update(&out);
            hasher.update(&payload);
            hasher.finalize()
        };
        out.extend_from_slice(&digest);
        out.extend_from_slice(&payload);

        out
    }

    /// Parses the artifact byte format, validating the whole frame before returning any entry.
    pub fn decode(bytes: &[u8], network: &Network) -> Result<Self, TreestateArtifactError> {
        let kind = Self::KIND;

        if read_array::<8>(bytes, 0, kind)? != *FRONTIER_MAGIC {
            return Err(TreestateArtifactError::InvalidMagic { kind });
        }

        let version = u16::from_le_bytes(read_array::<2>(bytes, 8, kind)?);
        if version != VERSION {
            return Err(TreestateArtifactError::UnsupportedVersion {
                kind,
                found: version,
            });
        }

        let found = read_array::<1>(bytes, 10, kind)?[0];
        let expected = network_byte(network);
        if found != expected {
            return Err(TreestateArtifactError::WrongNetwork {
                kind,
                found,
                expected,
            });
        }

        let spacing = u32::from_le_bytes(read_array::<4>(bytes, 11, kind)?);
        let last_checkpoint = Height(u32::from_le_bytes(read_array::<4>(bytes, 15, kind)?));
        // Zakura's supported platforms have at least 32-bit `usize`, so every `u32` count fits.
        let count = u32::from_le_bytes(read_array::<4>(bytes, 19, kind)?) as usize;
        if count > MAX_FRONTIER_ENTRIES {
            return Err(TreestateArtifactError::TooManyRecords {
                kind,
                found: count,
                max: MAX_FRONTIER_ENTRIES,
            });
        }

        let digest = read_array::<32>(bytes, FRONTIER_DIGEST_OFFSET, kind)?;
        let payload =
            bytes
                .get(FRONTIER_HEADER_LEN..)
                .ok_or(TreestateArtifactError::Truncated {
                    kind,
                    offset: FRONTIER_HEADER_LEN,
                    needed: 0,
                })?;

        // Check the digest before decoding anything: a record is only worth parsing once the
        // frame is known to be the one the encoder wrote.
        let actual_digest = {
            let mut hasher = Sha256::new();
            hasher.update(&bytes[..FRONTIER_DIGEST_OFFSET]);
            hasher.update(payload);
            hasher.finalize()
        };
        if actual_digest.as_slice() != digest {
            return Err(TreestateArtifactError::DigestMismatch { kind });
        }

        // Keep this structural check even though the digest covers the count: an artifact producer
        // can create a valid digest over an invalid frame, and parsing must remain allocation-safe.
        let min_payload_len = count.saturating_mul(MIN_FRONTIER_ENTRY_LEN);
        if min_payload_len > payload.len() {
            return Err(TreestateArtifactError::Truncated {
                kind,
                offset: FRONTIER_HEADER_LEN,
                needed: min_payload_len - payload.len(),
            });
        }

        let mut entries = Vec::with_capacity(count);
        let mut offset = 0;
        let mut previous = None;

        for _ in 0..count {
            let height = Height(u32::from_le_bytes(read_array::<4>(payload, offset, kind)?));
            offset += 4;

            if let Some(previous) = previous {
                if height.0 <= previous {
                    return Err(TreestateArtifactError::OutOfOrder {
                        kind,
                        previous,
                        found: height.0,
                    });
                }
            }
            previous = Some(height.0);

            let (sapling, next) = read_blob(payload, offset, kind)?;
            let (orchard, next) = read_blob(payload, next, kind)?;
            let (ironwood, next) = read_blob(payload, next, kind)?;
            offset = next;

            let unreadable = |pool| TreestateArtifactError::UnreadableTree { kind, pool, height };

            entries.push(FrontierEntry {
                height,
                sapling: Arc::new(decode_tree(&sapling).ok_or_else(|| unreadable("sapling"))?),
                orchard: Arc::new(decode_tree(&orchard).ok_or_else(|| unreadable("orchard"))?),
                ironwood: Arc::new(decode_tree(&ironwood).ok_or_else(|| unreadable("ironwood"))?),
            });
        }

        if offset != payload.len() {
            return Err(TreestateArtifactError::TrailingBytes {
                kind,
                trailing: payload.len() - offset,
            });
        }

        Ok(Self {
            spacing,
            last_checkpoint,
            entries,
        })
    }

    /// Returns the highest entry at or below `height`, the anchor a derivation replays from.
    pub fn anchor_at_or_below(&self, height: Height) -> Option<&FrontierEntry> {
        let index = self
            .entries
            .partition_point(|entry| entry.height <= height)
            .checked_sub(1)?;

        self.entries.get(index)
    }

    /// The most blocks a cold request would replay from this grid on a from-scratch node, where
    /// heights below the first entry have no stored trees to anchor on.
    pub fn max_cold_replay_blocks(&self) -> u64 {
        self.max_cold_replay_blocks_from(Height(0))
    }

    /// The most blocks a cold request would replay if coverage started at `coverage_from`.
    ///
    /// Entries at or above [`Self::last_checkpoint`] cannot anchor a request in the half-open
    /// absent band and are ignored.
    pub fn max_cold_replay_blocks_from(&self, coverage_from: Height) -> u64 {
        let band_end = self.last_checkpoint.0;
        if band_end <= coverage_from.0 {
            return 0;
        }

        let mut previous = coverage_from.0.checked_sub(1);
        let mut max_blocks = 0u64;

        for height in self
            .entries
            .iter()
            .map(|entry| entry.height.0)
            .filter(|height| *height < band_end)
        {
            if height < coverage_from.0 {
                previous = Some(previous.map_or(height, |anchor| anchor.max(height)));
                continue;
            }

            if height > coverage_from.0 {
                max_blocks = max_blocks.max(cold_replay_len(previous, height - 1));
            }
            previous = Some(height);
        }

        max_blocks.max(cold_replay_len(previous, band_end - 1))
    }
}

/// How many block bodies a replay from `anchor` (or genesis) through `height` would apply.
fn cold_replay_len(anchor: Option<u32>, height: u32) -> u64 {
    match anchor {
        Some(anchor) if anchor >= height => 0,
        Some(anchor) => u64::from(height.saturating_sub(anchor)),
        None => u64::from(height).saturating_add(1),
    }
}

/// One completed subtree's root and the height it completed at.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubtreeRecord {
    /// The subtree index.
    pub index: NoteCommitmentSubtreeIndex,

    /// The height the subtree's last leaf was added at.
    pub end_height: Height,

    /// The subtree root.
    pub root: [u8; 32],
}

/// Completed subtree roots per pool, in index order.
///
/// Unlike [`FrontierArtifact`], individual entries cannot be checked against per-height roots.
/// The complete ordered lists can be checked efficiently against the final frontier that pins
/// them, so embedded artifacts are verified as a whole before use.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubtreeArtifact {
    /// The last checkpoint this was generated against.
    pub last_checkpoint: Height,

    /// Completed Sapling subtrees, in index order.
    pub sapling: Vec<SubtreeRecord>,

    /// Completed Orchard subtrees, in index order.
    pub orchard: Vec<SubtreeRecord>,

    /// Completed Ironwood subtrees, in index order.
    pub ironwood: Vec<SubtreeRecord>,
}

impl Default for SubtreeArtifact {
    fn default() -> Self {
        Self {
            last_checkpoint: Height(0),
            sapling: Vec::new(),
            orchard: Vec::new(),
            ironwood: Vec::new(),
        }
    }
}

/// Validates one pool's declared indexes and end heights against the artifact checkpoint.
///
/// Indexes must be exactly `0..records.len()`. End heights must be strictly increasing and at or
/// below `last_checkpoint`, matching the final frontier that authenticates the artifact.
fn validate_pool_metadata(
    pool: &'static str,
    last_checkpoint: Height,
    records: &[SubtreeRecord],
) -> Result<(), TreestateArtifactError> {
    let mut previous_end_height = None;

    for (ordinal, record) in records.iter().enumerate() {
        let expected =
            u16::try_from(ordinal).map_err(|_| TreestateArtifactError::TooManyRecords {
                kind: SubtreeArtifact::KIND,
                found: records.len(),
                max: MAX_SUBTREE_RECORDS,
            })?;
        if record.index.0 != expected {
            return Err(TreestateArtifactError::NonContiguousSubtreeIndex {
                pool,
                expected,
                found: record.index.0,
            });
        }

        if record.end_height > last_checkpoint {
            return Err(TreestateArtifactError::EndHeightAboveCheckpoint {
                pool,
                index: record.index.0,
                end_height: record.end_height,
                last_checkpoint,
            });
        }

        if let Some((previous_index, previous_height)) = previous_end_height {
            if record.end_height <= previous_height {
                return Err(TreestateArtifactError::NonIncreasingEndHeight {
                    pool,
                    previous_index,
                    previous_height,
                    found_index: record.index.0,
                    found_height: record.end_height,
                });
            }
        }
        previous_end_height = Some((record.index.0, record.end_height));
    }

    Ok(())
}

impl SubtreeArtifact {
    /// The name used in error messages.
    const KIND: &'static str = "subtree-root";

    /// Serializes to the artifact byte format.
    pub fn encode(&self, network: &Network) -> Vec<u8> {
        let mut payload = Vec::new();
        for pool in [&self.sapling, &self.orchard, &self.ironwood] {
            for record in pool {
                payload.extend_from_slice(&record.index.0.to_le_bytes());
                payload.extend_from_slice(&record.end_height.0.to_le_bytes());
                payload.extend_from_slice(&record.root);
            }
        }

        let count = |pool: &Vec<SubtreeRecord>| {
            u32::try_from(pool.len()).expect("subtree indexes are u16, so counts fit in u32")
        };

        let mut out = Vec::with_capacity(SUBTREE_HEADER_LEN + payload.len());
        out.extend_from_slice(SUBTREE_MAGIC);
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.push(network_byte(network));
        out.extend_from_slice(&self.last_checkpoint.0.to_le_bytes());
        out.extend_from_slice(&count(&self.sapling).to_le_bytes());
        out.extend_from_slice(&count(&self.orchard).to_le_bytes());
        out.extend_from_slice(&count(&self.ironwood).to_le_bytes());
        debug_assert_eq!(out.len(), SUBTREE_DIGEST_OFFSET);

        let digest = {
            let mut hasher = Sha256::new();
            hasher.update(&out);
            hasher.update(&payload);
            hasher.finalize()
        };
        out.extend_from_slice(&digest);
        out.extend_from_slice(&payload);

        out
    }

    /// Parses the artifact byte format, validating the whole frame before returning any record.
    pub fn decode(bytes: &[u8], network: &Network) -> Result<Self, TreestateArtifactError> {
        let kind = Self::KIND;

        if read_array::<8>(bytes, 0, kind)? != *SUBTREE_MAGIC {
            return Err(TreestateArtifactError::InvalidMagic { kind });
        }

        let version = u16::from_le_bytes(read_array::<2>(bytes, 8, kind)?);
        if version != VERSION {
            return Err(TreestateArtifactError::UnsupportedVersion {
                kind,
                found: version,
            });
        }

        let digest = read_array::<32>(bytes, SUBTREE_DIGEST_OFFSET, kind)?;
        let payload = bytes
            .get(SUBTREE_HEADER_LEN..)
            .ok_or(TreestateArtifactError::Truncated {
                kind,
                offset: SUBTREE_HEADER_LEN,
                needed: 0,
            })?;

        let actual_digest = {
            let mut hasher = Sha256::new();
            hasher.update(&bytes[..SUBTREE_DIGEST_OFFSET]);
            hasher.update(payload);
            hasher.finalize()
        };
        if actual_digest.as_slice() != digest {
            return Err(TreestateArtifactError::DigestMismatch { kind });
        }

        let found = read_array::<1>(bytes, 10, kind)?[0];
        let expected = network_byte(network);
        if found != expected {
            return Err(TreestateArtifactError::WrongNetwork {
                kind,
                found,
                expected,
            });
        }

        let last_checkpoint = Height(u32::from_le_bytes(read_array::<4>(bytes, 11, kind)?));

        let mut counts = [0usize; 3];
        for (index, count) in counts.iter_mut().enumerate() {
            *count = u32::from_le_bytes(read_array::<4>(bytes, 15 + index * 4, kind)?) as usize;
            if *count > MAX_SUBTREE_RECORDS {
                return Err(TreestateArtifactError::TooManyRecords {
                    kind,
                    found: *count,
                    max: MAX_SUBTREE_RECORDS,
                });
            }
        }

        let mut offset = 0;
        let mut pools = Vec::with_capacity(3);
        for (pool_index, count) in counts.into_iter().enumerate() {
            let pool = SUBTREE_POOLS[pool_index];
            let mut records = Vec::with_capacity(count);

            for _ in 0..count {
                let index = u16::from_le_bytes(read_array::<2>(payload, offset, kind)?);
                let end_height = Height(u32::from_le_bytes(read_array::<4>(
                    payload,
                    offset + 2,
                    kind,
                )?));
                let root = read_array::<32>(payload, offset + 6, kind)?;
                offset += SUBTREE_RECORD_LEN;

                let root_is_valid = match pool {
                    SAPLING_POOL => sapling_crypto::Node::from_bytes(root)
                        .into_option()
                        .is_some(),
                    ORCHARD_POOL | IRONWOOD_POOL => {
                        orchard::tree::Node::try_from(root.as_slice()).is_ok()
                    }
                    _ => unreachable!("all artifact pools have canonical node decoders"),
                };
                if !root_is_valid {
                    return Err(TreestateArtifactError::MalformedSubtreeRoot { pool, index });
                }

                records.push(SubtreeRecord {
                    index: NoteCommitmentSubtreeIndex(index),
                    end_height,
                    root,
                });
            }

            pools.push(records);
        }

        if offset != payload.len() {
            return Err(TreestateArtifactError::TrailingBytes {
                kind,
                trailing: payload.len() - offset,
            });
        }

        let mut pools = pools.into_iter();
        let artifact = Self {
            last_checkpoint,
            sapling: pools.next().expect("three pools were decoded"),
            orchard: pools.next().expect("three pools were decoded"),
            ironwood: pools.next().expect("three pools were decoded"),
        };
        artifact.validate_metadata()?;
        Ok(artifact)
    }

    /// Parses an artifact and verifies that it belongs to `expected_last_checkpoint`.
    pub fn decode_at_last_checkpoint(
        bytes: &[u8],
        network: &Network,
        expected_last_checkpoint: Height,
    ) -> Result<Self, TreestateArtifactError> {
        let artifact = Self::decode(bytes, network)?;
        if artifact.last_checkpoint != expected_last_checkpoint {
            return Err(TreestateArtifactError::WrongLastCheckpoint {
                kind: Self::KIND,
                found: artifact.last_checkpoint,
                expected: expected_last_checkpoint,
            });
        }

        Ok(artifact)
    }

    /// Checks declared indexes and end heights for every pool.
    ///
    /// Frontier verification authenticates ordered root *values*, not the metadata beside them.
    /// Serving and tip-bound merging consume those metadata fields directly, so they must be
    /// structurally sound before any root is published or proven.
    pub fn validate_metadata(&self) -> Result<(), TreestateArtifactError> {
        for (pool, records) in [
            (SAPLING_POOL, self.sapling.as_slice()),
            (ORCHARD_POOL, self.orchard.as_slice()),
            (IRONWOOD_POOL, self.ironwood.as_slice()),
        ] {
            validate_pool_metadata(pool, self.last_checkpoint, records)?;
        }
        Ok(())
    }

    /// Checks every root in this artifact against the frontiers that pin them.
    ///
    /// Subtree roots are interior nodes, so nothing else in the artifact's framing tests their
    /// values: an artifact full of wrong roots, or of no roots at all, parses exactly like a
    /// correct one. A frontier's ommers are the pairwise hashes of the subtrees it has already
    /// completed, so folding these roots must reproduce them.
    ///
    /// The frontiers must be at the height this artifact is bound to. Ironwood shares Orchard's
    /// tree type, so it is checked the same way.
    pub fn verify_against_frontiers(
        &self,
        sapling: &zakura_chain::sapling::tree::NoteCommitmentTree,
        orchard: &orchard::tree::NoteCommitmentTree,
        ironwood: &orchard::tree::NoteCommitmentTree,
    ) -> Result<VerifiedSubtreeCounts, TreestateArtifactError> {
        self.validate_metadata()?;

        let sapling_roots = self
            .sapling
            .iter()
            .map(|record| {
                sapling_crypto::Node::from_bytes(record.root)
                    .into_option()
                    .ok_or(TreestateArtifactError::MalformedSubtreeRoot {
                        pool: SAPLING_POOL,
                        index: record.index.0,
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let pallas_roots = |pool: &'static str, records: &[SubtreeRecord]| {
            records
                .iter()
                .map(|record| {
                    orchard::tree::Node::try_from(record.root.as_slice()).map_err(|_| {
                        TreestateArtifactError::MalformedSubtreeRoot {
                            pool,
                            index: record.index.0,
                        }
                    })
                })
                .collect::<Result<Vec<_>, _>>()
        };

        let orchard_roots = pallas_roots(ORCHARD_POOL, &self.orchard)?;
        let ironwood_roots = pallas_roots(IRONWOOD_POOL, &self.ironwood)?;

        let verified = |pool: &'static str, result: Result<usize, SubtreeRootsError>| {
            result.map_err(|source| TreestateArtifactError::UnverifiedSubtreeRoots { pool, source })
        };

        Ok(VerifiedSubtreeCounts {
            sapling: verified(
                SAPLING_POOL,
                sapling.verify_completed_subtree_roots(&sapling_roots),
            )?,
            orchard: verified(
                ORCHARD_POOL,
                orchard.verify_completed_subtree_roots(&orchard_roots),
            )?,
            ironwood: verified(
                IRONWOOD_POOL,
                ironwood.verify_completed_subtree_roots(&ironwood_roots),
            )?,
        })
    }

    /// Returns the Sapling subtrees in `range`, as `z_getsubtreesbyindex` serves them.
    pub fn sapling_range(
        &self,
        range: impl std::ops::RangeBounds<NoteCommitmentSubtreeIndex> + Clone,
    ) -> Vec<(
        NoteCommitmentSubtreeIndex,
        NoteCommitmentSubtreeData<sapling_crypto::Node>,
    )> {
        self.sapling
            .iter()
            .filter(|record| range.contains(&record.index))
            .filter_map(|record| {
                sapling_crypto::Node::from_bytes(record.root)
                    .into_option()
                    .map(|root| {
                        (
                            record.index,
                            NoteCommitmentSubtreeData::new(record.end_height, root),
                        )
                    })
            })
            .collect()
    }

    /// Returns the Orchard subtrees in `range`, as `z_getsubtreesbyindex` serves them.
    pub fn orchard_range(
        &self,
        range: impl std::ops::RangeBounds<NoteCommitmentSubtreeIndex> + Clone,
    ) -> Vec<(
        NoteCommitmentSubtreeIndex,
        NoteCommitmentSubtreeData<orchard::tree::Node>,
    )> {
        Self::pallas_range(&self.orchard, range)
    }

    /// Returns the Ironwood subtrees in `range`, as `z_getsubtreesbyindex` serves them.
    pub fn ironwood_range(
        &self,
        range: impl std::ops::RangeBounds<NoteCommitmentSubtreeIndex> + Clone,
    ) -> Vec<(
        NoteCommitmentSubtreeIndex,
        NoteCommitmentSubtreeData<orchard::tree::Node>,
    )> {
        Self::pallas_range(&self.ironwood, range)
    }

    /// Shared body for the two Pallas-based pools, which use the same node type.
    fn pallas_range(
        records: &[SubtreeRecord],
        range: impl std::ops::RangeBounds<NoteCommitmentSubtreeIndex> + Clone,
    ) -> Vec<(
        NoteCommitmentSubtreeIndex,
        NoteCommitmentSubtreeData<orchard::tree::Node>,
    )> {
        records
            .iter()
            .filter(|record| range.contains(&record.index))
            .filter_map(|record| {
                orchard::tree::Node::try_from(record.root.as_slice())
                    .ok()
                    .map(|root| {
                        (
                            record.index,
                            NoteCommitmentSubtreeData::new(record.end_height, root),
                        )
                    })
            })
            .collect()
    }
}

/// Returns subtree roots verified against `network`'s embedded final frontier.
///
/// Mainnet verifies its embedded artifact once per process before any read service can use it.
/// Other networks do not use the Mainnet artifact.
pub(crate) fn embedded_historical_subtrees(network: &Network) -> Option<SubtreeArtifact> {
    match network {
        Network::Mainnet => {
            static VERIFIED_MAINNET_SUBTREES: OnceLock<SubtreeArtifact> = OnceLock::new();

            Some(
                VERIFIED_MAINNET_SUBTREES
                    .get_or_init(|| {
                        let artifact = SubtreeArtifact::decode_at_last_checkpoint(
                            MAINNET_SUBTREES,
                            network,
                            network.checkpoint_list().max_height(),
                        )
                        .unwrap_or_else(|error| {
                            panic!("invalid embedded Mainnet subtree-root artifact: {error}")
                        });
                        let frontiers = super::vct::embedded_final_frontiers(network)
                            .expect("Mainnet has an embedded final frontier");

                        artifact
                            .verify_against_frontiers(
                                &frontiers.sapling,
                                &frontiers.orchard,
                                &frontiers.ironwood,
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "embedded Mainnet subtree-root artifact does not match \
                                     the embedded final frontier: {error}"
                                )
                            });

                        artifact
                    })
                    .clone(),
            )
        }
        Network::Testnet(_) => None,
    }
}

/// Checks a candidate subtree-root artifact against a frontier, with no database and no network.
///
/// `frontier_bytes` is the serialized frontier artifact to check against; `None` uses the one
/// embedded in this binary.
///
/// The artifact is bound to a last checkpoint, and which one it must match depends on the
/// frontier:
///
/// - With an embedded frontier, that is this binary's last checkpoint. The embedded pair is
///   already known to describe it, so anything else is the wrong artifact for this binary.
/// - With a supplied frontier, it is that frontier's own height. A candidate bundle is bound to a
///   checkpoint *ahead* of the binary checking it, so requiring the binary's own last checkpoint
///   would reject every bundle that advances the checkpoint list — which is all of them. Pairing
///   the two supplied files against each other proves the bundle is internally consistent, which
///   is what can be established before the bundle is imported. That the checkpoint itself is the
///   expected one is a separate check, made by the importer and re-made afterwards against the
///   embedded pair.
pub fn verify_subtree_artifact(
    network: &Network,
    subtree_bytes: &[u8],
    frontier_bytes: Option<&[u8]>,
) -> Result<VerifiedSubtreeCounts, TreestateArtifactError> {
    let frontiers = match frontier_bytes {
        Some(bytes) => FinalFrontiers::from_bytes(bytes).map_err(|error| {
            TreestateArtifactError::InvalidFrontier {
                error: error.to_string(),
            }
        })?,
        None => super::vct::embedded_final_frontiers(network)
            .ok_or(TreestateArtifactError::NoEmbeddedFrontier)?,
    };

    let expected_last_checkpoint = match frontier_bytes {
        Some(_) => frontiers.height,
        None => network.checkpoint_list().max_height(),
    };

    let artifact = SubtreeArtifact::decode_at_last_checkpoint(
        subtree_bytes,
        network,
        expected_last_checkpoint,
    )?;

    artifact.verify_against_frontiers(&frontiers.sapling, &frontiers.orchard, &frontiers.ironwood)
}

#[cfg(test)]
mod tests {
    use super::*;

    use zakura_chain::parameters::Network;

    fn sapling_tree(count: u8) -> Arc<sapling::tree::NoteCommitmentTree> {
        let mut tree = sapling::tree::NoteCommitmentTree::default();
        for value in 0..count {
            let commitment =
                sapling_crypto::note::ExtractedNoteCommitment::from_bytes(&[value; 32]);
            if let Some(commitment) = commitment.into_option() {
                tree.append(commitment).expect("test tree is not full");
            }
        }
        Arc::new(tree)
    }

    fn pallas_tree(count: u8) -> Arc<orchard::tree::NoteCommitmentTree> {
        let mut tree = orchard::tree::NoteCommitmentTree::default();
        for value in 1..=count {
            tree.append(halo2::pasta::pallas::Base::from(u64::from(value)))
                .expect("test tree is not full");
        }
        Arc::new(tree)
    }

    fn sample_frontiers() -> FrontierArtifact {
        FrontierArtifact {
            spacing: 10,
            last_checkpoint: Height(31),
            entries: (0..3)
                .map(|index| {
                    let tree_size = u8::try_from(index).expect("sample entry indexes fit in u8");
                    FrontierEntry {
                        height: Height(index * 10),
                        sapling: sapling_tree(tree_size),
                        orchard: pallas_tree(tree_size),
                        ironwood: pallas_tree(tree_size.saturating_add(1)),
                    }
                })
                .collect(),
        }
    }

    fn replace_frontier_payload(bytes: &mut Vec<u8>, payload: &[u8]) {
        bytes.truncate(FRONTIER_HEADER_LEN);
        bytes.extend_from_slice(payload);
        let digest = {
            let mut hasher = Sha256::new();
            hasher.update(&bytes[..FRONTIER_DIGEST_OFFSET]);
            hasher.update(payload);
            hasher.finalize()
        };
        bytes[FRONTIER_DIGEST_OFFSET..FRONTIER_HEADER_LEN].copy_from_slice(&digest);
    }

    #[test]
    fn frontier_artifact_round_trips() {
        let artifact = sample_frontiers();
        let bytes = artifact.encode(&Network::Mainnet);
        let decoded = FrontierArtifact::decode(&bytes, &Network::Mainnet)
            .expect("a freshly encoded artifact decodes");

        assert_eq!(decoded.spacing, artifact.spacing);
        assert_eq!(decoded.last_checkpoint, artifact.last_checkpoint);
        assert_eq!(decoded.entries.len(), artifact.entries.len());
        for (decoded, original) in decoded.entries.iter().zip(&artifact.entries) {
            assert_eq!(decoded.height, original.height);
            assert_eq!(decoded.sapling.root(), original.sapling.root());
            assert_eq!(decoded.orchard.root(), original.orchard.root());
            assert_eq!(decoded.ironwood.root(), original.ironwood.root());
        }
    }

    #[test]
    fn frontier_encoding_is_deterministic() {
        let artifact = sample_frontiers();
        assert_eq!(
            artifact.encode(&Network::Mainnet),
            artifact.encode(&Network::Mainnet)
        );
    }

    #[test]
    fn frontier_artifact_rejects_duplicate_heights() {
        let mut artifact = sample_frontiers();
        artifact.entries[1].height = artifact.entries[0].height;

        assert_eq!(
            FrontierArtifact::decode(&artifact.encode(&Network::Mainnet), &Network::Mainnet).err(),
            Some(TreestateArtifactError::OutOfOrder {
                kind: "frontier",
                previous: 0,
                found: 0,
            })
        );
    }

    #[test]
    fn frontier_artifact_rejects_malformed_trees_with_a_valid_digest() {
        let artifact = sample_frontiers();
        let entry = &artifact.entries[0];
        let valid_blobs = [
            IntoDisk::as_bytes(&*entry.sapling),
            IntoDisk::as_bytes(&*entry.orchard),
            IntoDisk::as_bytes(&*entry.ironwood),
        ];

        for (malformed_pool, expected_pool) in [(0, "sapling"), (1, "orchard"), (2, "ironwood")] {
            let mut payload = entry.height.0.to_le_bytes().to_vec();
            for (pool, valid_blob) in valid_blobs.iter().enumerate() {
                if pool == malformed_pool {
                    write_blob(&mut payload, &[0xff]);
                } else {
                    write_blob(&mut payload, valid_blob);
                }
            }

            let mut bytes = artifact.encode(&Network::Mainnet);
            bytes[19..23].copy_from_slice(&1u32.to_le_bytes());
            replace_frontier_payload(&mut bytes, &payload);

            assert_eq!(
                FrontierArtifact::decode(&bytes, &Network::Mainnet).err(),
                Some(TreestateArtifactError::UnreadableTree {
                    kind: "frontier",
                    pool: expected_pool,
                    height: Height(0),
                })
            );
        }
    }

    #[test]
    fn frontier_artifact_rejects_trailing_bytes_with_a_valid_digest() {
        let mut bytes = sample_frontiers().encode(&Network::Mainnet);
        let mut payload = bytes[FRONTIER_HEADER_LEN..].to_vec();
        payload.extend_from_slice(&[0xaa, 0xbb]);
        replace_frontier_payload(&mut bytes, &payload);

        assert_eq!(
            FrontierArtifact::decode(&bytes, &Network::Mainnet).err(),
            Some(TreestateArtifactError::TrailingBytes {
                kind: "frontier",
                trailing: 2,
            })
        );
    }

    #[test]
    fn frontier_artifact_rejects_header_tampering() {
        let good = sample_frontiers().encode(&Network::Mainnet);

        let mut network_changed = good.clone();
        network_changed[10] = network_byte(&Network::new_default_testnet());
        assert!(matches!(
            FrontierArtifact::decode(&network_changed, &Network::Mainnet),
            Err(TreestateArtifactError::WrongNetwork { .. })
        ));

        // These fields have no independent identity check, so the frame digest must bind them to
        // the payload. Keep each mutation structurally plausible to isolate digest validation.
        for offset in [11, 15, 19] {
            let mut changed_header = good.clone();
            changed_header[offset] ^= 0x01;
            assert_eq!(
                FrontierArtifact::decode(&changed_header, &Network::Mainnet).err(),
                Some(TreestateArtifactError::DigestMismatch { kind: "frontier" }),
                "tampering with frontier header byte {offset} must invalidate the frame digest"
            );
        }
    }

    #[test]
    fn frontier_artifact_rejects_tampering_and_truncation() {
        let good = sample_frontiers().encode(&Network::Mainnet);

        let mut wrong_magic = good.clone();
        wrong_magic[0] ^= 0xff;
        assert_eq!(
            FrontierArtifact::decode(&wrong_magic, &Network::Mainnet).err(),
            Some(TreestateArtifactError::InvalidMagic { kind: "frontier" })
        );

        let mut wrong_version = good.clone();
        wrong_version[8] = 9;
        assert!(matches!(
            FrontierArtifact::decode(&wrong_version, &Network::Mainnet),
            Err(TreestateArtifactError::UnsupportedVersion { .. })
        ));

        let mut flipped = good.clone();
        *flipped.last_mut().expect("artifact has a payload") ^= 0x01;
        assert_eq!(
            FrontierArtifact::decode(&flipped, &Network::Mainnet).err(),
            Some(TreestateArtifactError::DigestMismatch { kind: "frontier" })
        );

        for truncate_to in [0, 4, FRONTIER_HEADER_LEN - 1, FRONTIER_HEADER_LEN + 2] {
            assert!(
                FrontierArtifact::decode(&good[..truncate_to], &Network::Mainnet).is_err(),
                "a frame truncated to {truncate_to} bytes must be rejected"
            );
        }
    }

    #[test]
    fn frontier_artifact_bounds_record_allocation_by_limit_and_payload() {
        let mut absurd = sample_frontiers().encode(&Network::Mainnet);
        absurd[19..23].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            FrontierArtifact::decode(&absurd, &Network::Mainnet),
            Err(TreestateArtifactError::TooManyRecords { .. })
        ));

        let mut empty = FrontierArtifact {
            spacing: 10,
            last_checkpoint: Height(31),
            entries: Vec::new(),
        }
        .encode(&Network::Mainnet);
        empty[19..23].copy_from_slice(
            &u32::try_from(MAX_FRONTIER_ENTRIES)
                .expect("the frontier entry limit fits in its count field")
                .to_le_bytes(),
        );
        let payload = empty[FRONTIER_HEADER_LEN..].to_vec();
        replace_frontier_payload(&mut empty, &payload);
        assert!(matches!(
            FrontierArtifact::decode(&empty, &Network::Mainnet),
            Err(TreestateArtifactError::Truncated { .. })
        ));
    }

    #[test]
    fn frontier_anchor_selection_picks_the_nearest_entry_at_or_below() {
        let artifact = sample_frontiers();

        assert!(
            artifact.anchor_at_or_below(Height(0)).is_some(),
            "an exact match on the first entry is an anchor"
        );
        assert!(
            artifact.anchor_at_or_below(Height(9)).is_some(),
            "a height above the first entry has an anchor"
        );
        assert_eq!(
            artifact
                .anchor_at_or_below(Height(15))
                .expect("an entry exists below 15")
                .height,
            Height(10)
        );
        assert_eq!(
            artifact
                .anchor_at_or_below(Height(9_999))
                .expect("an entry exists below 9999")
                .height,
            Height(20)
        );
    }

    #[test]
    fn frontier_max_cold_replay_blocks_measures_the_largest_hole() {
        assert_eq!(
            FrontierArtifact {
                spacing: 10,
                last_checkpoint: Height(31),
                entries: Vec::new(),
            }
            .max_cold_replay_blocks(),
            31
        );
        assert_eq!(sample_frontiers().max_cold_replay_blocks(), 10);

        let mid_chain = FrontierArtifact {
            spacing: 1,
            last_checkpoint: Height(2_000_100),
            entries: vec![FrontierEntry {
                height: Height(2_000_000),
                sapling: Arc::new(Default::default()),
                orchard: Arc::new(Default::default()),
                ironwood: Arc::new(Default::default()),
            }],
        };
        assert_eq!(mid_chain.max_cold_replay_blocks_from(Height(2_000_000)), 99);
        assert_eq!(mid_chain.max_cold_replay_blocks(), 2_000_000);
    }

    fn sample_subtrees() -> SubtreeArtifact {
        SubtreeArtifact {
            last_checkpoint: Height(31),
            sapling: vec![
                SubtreeRecord {
                    index: NoteCommitmentSubtreeIndex(0),
                    end_height: Height(7),
                    root: [1; 32],
                },
                SubtreeRecord {
                    index: NoteCommitmentSubtreeIndex(1),
                    end_height: Height(19),
                    root: [2; 32],
                },
            ],
            orchard: vec![SubtreeRecord {
                index: NoteCommitmentSubtreeIndex(0),
                end_height: Height(21),
                root: [3; 32],
            }],
            ironwood: Vec::new(),
        }
    }

    #[test]
    fn subtree_artifact_round_trips() {
        let artifact = sample_subtrees();
        let bytes = artifact.encode(&Network::Mainnet);

        assert_eq!(
            SubtreeArtifact::decode(&bytes, &Network::Mainnet),
            Ok(artifact)
        );
    }

    #[test]
    fn subtree_artifact_is_bound_to_last_checkpoint() {
        let artifact = sample_subtrees();
        let bytes = artifact.encode(&Network::Mainnet);

        assert_eq!(
            SubtreeArtifact::decode_at_last_checkpoint(&bytes, &Network::Mainnet, Height(32)),
            Err(TreestateArtifactError::WrongLastCheckpoint {
                kind: "subtree-root",
                found: Height(31),
                expected: Height(32),
            })
        );
    }

    #[test]
    fn embedded_subtrees_match_mainnet_last_checkpoint() {
        let artifact = embedded_historical_subtrees(&Network::Mainnet)
            .expect("Mainnet ships an embedded subtree-root artifact");

        assert_eq!(
            artifact.last_checkpoint,
            Network::Mainnet.checkpoint_list().max_height()
        );
        assert!(
            embedded_historical_subtrees(&Network::new_default_testnet()).is_none(),
            "the Mainnet trust bundle must not be used on Testnet"
        );
    }

    /// Proves the embedded Mainnet subtree roots against the embedded Mainnet frontier.
    ///
    /// Everything else guarding this artifact is structural — framing, artifact digest, manifest
    /// hash, handoff height — and passes just as happily on an artifact whose roots are wrong or
    /// absent. This is the only check that reads the roots themselves.
    ///
    /// Keep `frontier` in the name: `.github/workflows/update-release-state.yml` re-proves each
    /// imported release-state bundle with `cargo test -p zakura-state --lib -- frontier
    /// sprout_change`, so the name is what makes this run against every future artifact.
    #[test]
    fn embedded_subtree_roots_match_embedded_frontier() {
        let counts = verify_subtree_artifact(&Network::Mainnet, MAINNET_SUBTREES, None)
            .expect("embedded Mainnet subtree roots must match the embedded Mainnet frontier");

        let artifact = embedded_historical_subtrees(&Network::Mainnet)
            .expect("Mainnet ships an embedded subtree-root artifact");

        assert_eq!(counts.sapling, artifact.sapling.len());
        assert_eq!(counts.orchard, artifact.orchard.len());
        assert_eq!(counts.ironwood, artifact.ironwood.len());

        // A Mainnet last checkpoint above three million has completed hundreds of subtrees in
        // both long-lived pools. Zero here means an empty artifact shipped, which is what
        // happened once already, and every structural check passed.
        assert!(
            counts.sapling > 0 && counts.orchard > 0,
            "embedded artifact proved {counts:?}; an artifact with no roots serves nothing"
        );
    }

    /// A supplied frontier is paired with the artifact by its own height, so a bundle can be
    /// checked by a binary whose last checkpoint is still the older one.
    #[test]
    fn a_supplied_frontier_pairs_with_the_artifact_by_its_own_height() {
        const MAINNET_FRONTIER: &[u8] = include_bytes!("vct/mainnet-frontier.bin");

        let counts =
            verify_subtree_artifact(&Network::Mainnet, MAINNET_SUBTREES, Some(MAINNET_FRONTIER))
                .expect("the committed pair proves against each other");

        assert_eq!(
            counts,
            verify_subtree_artifact(&Network::Mainnet, MAINNET_SUBTREES, None)
                .expect("and against the embedded frontier")
        );

        // A frontier from a different height is still rejected, so the pairing is real.
        let mut wrong_height = MAINNET_FRONTIER.to_vec();
        wrong_height[0] ^= 0xff;
        assert!(matches!(
            verify_subtree_artifact(&Network::Mainnet, MAINNET_SUBTREES, Some(&wrong_height)),
            Err(TreestateArtifactError::WrongLastCheckpoint { .. })
        ));
    }

    /// The regression this check exists for: an empty artifact, correctly framed.
    #[test]
    fn an_empty_artifact_is_rejected_against_the_embedded_frontier() {
        let empty = SubtreeArtifact {
            last_checkpoint: Network::Mainnet.checkpoint_list().max_height(),
            sapling: Vec::new(),
            orchard: Vec::new(),
            ironwood: Vec::new(),
        }
        .encode(&Network::Mainnet);

        // It parses, its digest is valid, and it is bound to the right checkpoint.
        SubtreeArtifact::decode_at_last_checkpoint(
            &empty,
            &Network::Mainnet,
            Network::Mainnet.checkpoint_list().max_height(),
        )
        .expect("an empty artifact is structurally valid, which is the problem");

        assert!(matches!(
            verify_subtree_artifact(&Network::Mainnet, &empty, None),
            Err(TreestateArtifactError::UnverifiedSubtreeRoots {
                pool: SAPLING_POOL,
                source: SubtreeRootsError::CountMismatch { found: 0, .. },
            })
        ));
    }

    #[test]
    fn subtree_artifact_rejects_tampering() {
        let artifact = sample_subtrees();
        let good = artifact.encode(&Network::Mainnet);

        let mut wrong_magic = good.clone();
        wrong_magic[0] ^= 0xff;
        assert_eq!(
            SubtreeArtifact::decode(&wrong_magic, &Network::Mainnet),
            Err(TreestateArtifactError::InvalidMagic {
                kind: "subtree-root"
            })
        );

        // Every semantic header field is authenticated. Magic and version are checked separately
        // because they identify the format and therefore select the digest algorithm.
        for offset in [10, 11, 15, 19, 23] {
            let mut flipped = good.clone();
            flipped[offset] ^= 0x01;
            assert_eq!(
                SubtreeArtifact::decode(&flipped, &Network::Mainnet),
                Err(TreestateArtifactError::DigestMismatch {
                    kind: "subtree-root"
                })
            );
        }

        // Flipping a root byte is the failure that matters most here: this artifact's records
        // cannot be re-derived cheaply by a consumer, so the digest is the only thing standing
        // between a corrupted root and a wrong witness.
        let mut flipped = good.clone();
        *flipped.last_mut().expect("artifact is not empty") ^= 0x01;
        assert_eq!(
            SubtreeArtifact::decode(&flipped, &Network::Mainnet),
            Err(TreestateArtifactError::DigestMismatch {
                kind: "subtree-root"
            })
        );
    }

    #[test]
    fn subtree_artifact_rejects_malformed_roots_with_a_valid_digest() {
        for (pool, mut artifact) in [
            (
                SAPLING_POOL,
                SubtreeArtifact {
                    sapling: vec![SubtreeRecord {
                        index: NoteCommitmentSubtreeIndex(0),
                        end_height: Height(1),
                        root: [0xff; 32],
                    }],
                    ..SubtreeArtifact::default()
                },
            ),
            (
                ORCHARD_POOL,
                SubtreeArtifact {
                    orchard: vec![SubtreeRecord {
                        index: NoteCommitmentSubtreeIndex(0),
                        end_height: Height(1),
                        root: [0xff; 32],
                    }],
                    ..SubtreeArtifact::default()
                },
            ),
            (
                IRONWOOD_POOL,
                SubtreeArtifact {
                    ironwood: vec![SubtreeRecord {
                        index: NoteCommitmentSubtreeIndex(0),
                        end_height: Height(1),
                        root: [0xff; 32],
                    }],
                    ..SubtreeArtifact::default()
                },
            ),
        ] {
            artifact.last_checkpoint = Height(31);
            let bytes = artifact.encode(&Network::Mainnet);

            assert_eq!(
                SubtreeArtifact::decode(&bytes, &Network::Mainnet),
                Err(TreestateArtifactError::MalformedSubtreeRoot { pool, index: 0 })
            );
        }
    }

    /// Declared indexes are what range selection and tip-bound serving consume. Frontier
    /// verification only sees root values in vector order, so a shifted or gapped list would
    /// otherwise associate an authenticated root with the wrong subtree index.
    #[test]
    fn subtree_artifact_rejects_noncontiguous_indexes() {
        let mut shifted = sample_subtrees();
        shifted.sapling[0].index = NoteCommitmentSubtreeIndex(1);
        shifted.sapling[1].index = NoteCommitmentSubtreeIndex(2);
        assert_eq!(
            SubtreeArtifact::decode(&shifted.encode(&Network::Mainnet), &Network::Mainnet),
            Err(TreestateArtifactError::NonContiguousSubtreeIndex {
                pool: SAPLING_POOL,
                expected: 0,
                found: 1,
            })
        );

        let mut gapped = sample_subtrees();
        gapped.sapling[1].index = NoteCommitmentSubtreeIndex(3);
        assert_eq!(
            SubtreeArtifact::decode(&gapped.encode(&Network::Mainnet), &Network::Mainnet),
            Err(TreestateArtifactError::NonContiguousSubtreeIndex {
                pool: SAPLING_POOL,
                expected: 1,
                found: 3,
            })
        );
    }

    /// End heights drive tip-bound serving. They must rise with indexes and stay at or below the
    /// artifact checkpoint, or a root can become eligible before its block is verified — or
    /// remain hidden after it should be public.
    #[test]
    fn subtree_artifact_rejects_invalid_end_heights() {
        let mut non_increasing = sample_subtrees();
        non_increasing.sapling[1].end_height = Height(7);
        assert_eq!(
            SubtreeArtifact::decode(&non_increasing.encode(&Network::Mainnet), &Network::Mainnet),
            Err(TreestateArtifactError::NonIncreasingEndHeight {
                pool: SAPLING_POOL,
                previous_index: 0,
                previous_height: Height(7),
                found_index: 1,
                found_height: Height(7),
            })
        );

        let mut decreasing = sample_subtrees();
        decreasing.sapling[1].end_height = Height(3);
        assert_eq!(
            SubtreeArtifact::decode(&decreasing.encode(&Network::Mainnet), &Network::Mainnet),
            Err(TreestateArtifactError::NonIncreasingEndHeight {
                pool: SAPLING_POOL,
                previous_index: 0,
                previous_height: Height(7),
                found_index: 1,
                found_height: Height(3),
            })
        );

        let mut at_checkpoint = sample_subtrees();
        at_checkpoint.sapling[1].end_height = Height(31);
        assert_eq!(
            SubtreeArtifact::decode(&at_checkpoint.encode(&Network::Mainnet), &Network::Mainnet),
            Ok(at_checkpoint),
            "a subtree completed by the checkpoint is part of its final frontier"
        );

        let mut above_checkpoint = sample_subtrees();
        above_checkpoint.orchard[0].end_height = Height(40);
        assert_eq!(
            SubtreeArtifact::decode(
                &above_checkpoint.encode(&Network::Mainnet),
                &Network::Mainnet
            ),
            Err(TreestateArtifactError::EndHeightAboveCheckpoint {
                pool: ORCHARD_POOL,
                index: 0,
                end_height: Height(40),
                last_checkpoint: Height(31),
            })
        );
    }

    /// In-memory artifacts skip `decode`, so export and verify paths must reject bad metadata
    /// before folding roots into a frontier.
    #[test]
    fn verify_against_frontiers_rejects_invalid_metadata() {
        let empty = zakura_chain::sapling::tree::NoteCommitmentTree::default();
        let empty_orchard = orchard::tree::NoteCommitmentTree::default();

        let mut shifted = sample_subtrees();
        shifted.sapling[0].index = NoteCommitmentSubtreeIndex(1);
        shifted.sapling[1].index = NoteCommitmentSubtreeIndex(2);

        assert_eq!(
            shifted.verify_against_frontiers(&empty, &empty_orchard, &empty_orchard),
            Err(TreestateArtifactError::NonContiguousSubtreeIndex {
                pool: SAPLING_POOL,
                expected: 0,
                found: 1,
            })
        );

        let mut above_checkpoint = sample_subtrees();
        above_checkpoint.sapling[1].end_height = Height(above_checkpoint.last_checkpoint.0 + 1);
        assert_eq!(
            above_checkpoint.verify_against_frontiers(&empty, &empty_orchard, &empty_orchard),
            Err(TreestateArtifactError::EndHeightAboveCheckpoint {
                pool: SAPLING_POOL,
                index: 1,
                end_height: Height(32),
                last_checkpoint: Height(31),
            })
        );
    }

    #[test]
    fn subtree_artifact_serves_index_ranges() {
        let artifact = sample_subtrees();

        let all = artifact.sapling_range(..);
        assert_eq!(all.len(), 2, "an unbounded range serves every subtree");

        let from_one = artifact.sapling_range(NoteCommitmentSubtreeIndex(1)..);
        assert_eq!(from_one.len(), 1);
        assert_eq!(from_one[0].0, NoteCommitmentSubtreeIndex(1));
        assert_eq!(from_one[0].1.end_height, Height(19));

        assert_eq!(artifact.orchard_range(..).len(), 1);
        assert!(
            artifact.ironwood_range(..).is_empty(),
            "a pool with no completed subtrees serves nothing"
        );
    }
}
