//! Binary artifacts that let a fast-synced node serve historical treestates.
//!
//! Two artifacts, with deliberately different trust stories (see
//! `docs/design/historical-treestate-serving.md` §4.1, §4.2 and §4.6):
//!
//! - The **frontier artifact** holds per-pool note commitment frontiers at a sparse height grid.
//!   Every entry is checked against the authenticated root in `commitment_roots_by_height` before
//!   use, so the artifact carries no trust weight: a corrupt or hostile one is rejected rather
//!   than absorbed. That is what lets it be coarse, small, and distributed outside the binary.
//! - The **subtree-root artifact** holds completed subtree roots, which a node cannot check the
//!   same way without replaying each subtree's 65,536 leaves.
//!
//! Both follow the framing the Sprout history artifact established: magic, version, network byte,
//! explicit record counts, and a SHA-256 over the payload, with the parser validating the whole
//! frame before any record is used.

use std::sync::Arc;

use sha2::{Digest, Sha256};
use thiserror::Error;

use zakura_chain::{
    block::Height,
    ironwood, orchard,
    parameters::{Network, NetworkKind},
    sapling,
    subtree::{NoteCommitmentSubtreeData, NoteCommitmentSubtreeIndex},
};

use bincode::Options;

use crate::service::finalized_state::disk_format::IntoDisk;

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

/// The format version both artifacts are written at.
const VERSION: u16 = 1;

/// Fixed header length: magic, version, network, grid spacing, handoff, record count, digest.
const FRONTIER_HEADER_LEN: usize = 8 + 2 + 1 + 4 + 4 + 4 + 32;

/// Fixed header length: magic, version, network, handoff, three record counts, digest.
const SUBTREE_HEADER_LEN: usize = 8 + 2 + 1 + 4 + 4 + 4 + 4 + 32;

/// Bytes per subtree record: index, end height, root.
const SUBTREE_RECORD_LEN: usize = 2 + 4 + 32;

/// The most frontier entries an artifact may declare.
///
/// A one-block grid across a chain far longer than any real one still fits well inside this, so a
/// declared count above it is a corrupt or hostile header rather than a legitimate artifact. The
/// bound is what stops a bad count from driving a huge allocation before any bytes are validated.
const MAX_FRONTIER_ENTRIES: usize = 16_000_000;

/// The most subtree records an artifact may declare per pool.
///
/// Subtree indexes are `u16`, so a pool can never complete more than this many.
const MAX_SUBTREE_RECORDS: usize = u16::MAX as usize + 1;

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

    /// The payload does not hash to the digest in the header.
    #[error("{kind} artifact payload does not match the digest in its header")]
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
    /// Ordering is what makes "nearest entry at or below `h`" a binary search rather than a scan,
    /// and what makes the append-only prefix contract checkable.
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
    let len = u32::from_le_bytes(read_array::<4>(bytes, offset, kind)?) as usize;
    let start = offset + 4;
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
/// Entries are strictly increasing in height. Nothing here is trusted: a consumer checks each
/// entry's roots against `commitment_roots_by_height` before using it (§4.2).
#[derive(Clone, Debug)]
pub struct FrontierArtifact {
    /// The height spacing the grid was generated at.
    ///
    /// Recorded for provenance and for the append-only contract; consumers locate entries by
    /// searching rather than by assuming this spacing.
    pub spacing: u32,

    /// The checkpoint handoff the grid was generated against.
    pub handoff: Height,

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
        out.extend_from_slice(&self.handoff.0.to_le_bytes());
        out.extend_from_slice(
            &u32::try_from(self.entries.len())
                .expect("entry count is bounded by MAX_FRONTIER_ENTRIES")
                .to_le_bytes(),
        );
        out.extend_from_slice(&Sha256::digest(&payload));
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
        let handoff = Height(u32::from_le_bytes(read_array::<4>(bytes, 15, kind)?));
        let count = u32::from_le_bytes(read_array::<4>(bytes, 19, kind)?) as usize;
        if count > MAX_FRONTIER_ENTRIES {
            return Err(TreestateArtifactError::TooManyRecords {
                kind,
                found: count,
                max: MAX_FRONTIER_ENTRIES,
            });
        }

        let digest = read_array::<32>(bytes, 23, kind)?;
        let payload =
            bytes
                .get(FRONTIER_HEADER_LEN..)
                .ok_or(TreestateArtifactError::Truncated {
                    kind,
                    offset: FRONTIER_HEADER_LEN,
                    needed: 0,
                })?;

        // Check the digest before decoding anything: a record is only worth parsing once the
        // bytes are known to be the ones the generator wrote.
        if Sha256::digest(payload).as_slice() != digest {
            return Err(TreestateArtifactError::DigestMismatch { kind });
        }

        let mut entries = Vec::with_capacity(count);
        let mut offset = 0;
        let mut previous: Option<u32> = None;

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
            handoff,
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
/// Unlike [`FrontierArtifact`], a consumer cannot check these against a stored root without
/// replaying each subtree's leaves, so this ships in the reviewed, committed bundle (§4.6).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubtreeArtifact {
    /// The checkpoint handoff this was generated against.
    pub handoff: Height,

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
            handoff: Height(0),
            sapling: Vec::new(),
            orchard: Vec::new(),
            ironwood: Vec::new(),
        }
    }
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
        out.extend_from_slice(&self.handoff.0.to_le_bytes());
        out.extend_from_slice(&count(&self.sapling).to_le_bytes());
        out.extend_from_slice(&count(&self.orchard).to_le_bytes());
        out.extend_from_slice(&count(&self.ironwood).to_le_bytes());
        out.extend_from_slice(&Sha256::digest(&payload));
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

        let found = read_array::<1>(bytes, 10, kind)?[0];
        let expected = network_byte(network);
        if found != expected {
            return Err(TreestateArtifactError::WrongNetwork {
                kind,
                found,
                expected,
            });
        }

        let handoff = Height(u32::from_le_bytes(read_array::<4>(bytes, 11, kind)?));

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

        let digest = read_array::<32>(bytes, 27, kind)?;
        let payload = bytes
            .get(SUBTREE_HEADER_LEN..)
            .ok_or(TreestateArtifactError::Truncated {
                kind,
                offset: SUBTREE_HEADER_LEN,
                needed: 0,
            })?;

        if Sha256::digest(payload).as_slice() != digest {
            return Err(TreestateArtifactError::DigestMismatch { kind });
        }

        let mut offset = 0;
        let mut pools = Vec::with_capacity(3);
        for count in counts {
            let mut records = Vec::with_capacity(count);
            let mut previous: Option<u32> = None;

            for _ in 0..count {
                let index = u16::from_le_bytes(read_array::<2>(payload, offset, kind)?);
                let end_height = Height(u32::from_le_bytes(read_array::<4>(
                    payload,
                    offset + 2,
                    kind,
                )?));
                let root = read_array::<32>(payload, offset + 6, kind)?;
                offset += SUBTREE_RECORD_LEN;

                if let Some(previous) = previous {
                    if u32::from(index) <= previous {
                        return Err(TreestateArtifactError::OutOfOrder {
                            kind,
                            previous,
                            found: u32::from(index),
                        });
                    }
                }
                previous = Some(u32::from(index));

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
        Ok(Self {
            handoff,
            sapling: pools.next().expect("three pools were decoded"),
            orchard: pools.next().expect("three pools were decoded"),
            ironwood: pools.next().expect("three pools were decoded"),
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

#[cfg(test)]
mod tests {
    use super::*;

    use zakura_chain::parameters::Network;

    /// Builds a Sapling tree holding `count` distinct commitments.
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

    /// Builds an Orchard/Ironwood tree holding `count` distinct commitments.
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
            handoff: Height(31),
            entries: (0..3)
                .map(|index| FrontierEntry {
                    height: Height(index * 10),
                    sapling: sapling_tree(index as u8),
                    orchard: pallas_tree(index as u8),
                    ironwood: pallas_tree(index as u8 + 1),
                })
                .collect(),
        }
    }

    fn sample_subtrees() -> SubtreeArtifact {
        SubtreeArtifact {
            handoff: Height(31),
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
    fn frontier_artifact_round_trips() {
        let artifact = sample_frontiers();
        let bytes = artifact.encode(&Network::Mainnet);
        let decoded = FrontierArtifact::decode(&bytes, &Network::Mainnet)
            .expect("a freshly encoded artifact decodes");

        assert_eq!(decoded.spacing, artifact.spacing);
        assert_eq!(decoded.handoff, artifact.handoff);
        assert_eq!(decoded.entries.len(), artifact.entries.len());
        for (decoded, original) in decoded.entries.iter().zip(&artifact.entries) {
            assert_eq!(decoded.height, original.height);
            assert_eq!(decoded.sapling.root(), original.sapling.root());
            assert_eq!(decoded.orchard.root(), original.orchard.root());
            assert_eq!(decoded.ironwood.root(), original.ironwood.root());
        }
    }

    /// Encoding is a pure function of the artifact, which is what the determinism gate needs:
    /// two independent generator runs over the same chain must agree byte for byte.
    #[test]
    fn frontier_encoding_is_deterministic() {
        let artifact = sample_frontiers();
        assert_eq!(
            artifact.encode(&Network::Mainnet),
            artifact.encode(&Network::Mainnet)
        );
    }

    /// A later export must extend an earlier one, never rewrite it, so the release workflow can
    /// verify updates as pure appends.
    #[test]
    fn frontier_encoding_is_prefix_compatible_across_tips() {
        let mut earlier = sample_frontiers();
        let later = earlier.clone();
        earlier.entries.pop();

        let earlier_bytes = earlier.encode(&Network::Mainnet);
        let later_bytes = later.encode(&Network::Mainnet);

        // The headers differ (the record count changed), so the append contract holds over the
        // payload, which is where the entries live.
        assert_eq!(
            later_bytes[FRONTIER_HEADER_LEN..][..earlier_bytes.len() - FRONTIER_HEADER_LEN],
            earlier_bytes[FRONTIER_HEADER_LEN..],
            "a later export must extend the earlier payload, not rewrite it"
        );
    }

    #[test]
    fn frontier_artifact_rejects_tampering() {
        let artifact = sample_frontiers();
        let good = artifact.encode(&Network::Mainnet);

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

        assert!(matches!(
            FrontierArtifact::decode(&good, &Network::new_default_testnet()),
            Err(TreestateArtifactError::WrongNetwork { .. })
        ));

        // A flipped payload byte is exactly what the digest exists to catch, and it must be
        // caught before any record is decoded.
        let mut flipped = good.clone();
        *flipped.last_mut().expect("artifact is not empty") ^= 0x01;
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

    /// A declared count far beyond the format's limit must be rejected from the header, before it
    /// can drive an allocation.
    #[test]
    fn frontier_artifact_rejects_absurd_record_count() {
        let mut bytes = sample_frontiers().encode(&Network::Mainnet);
        bytes[19..23].copy_from_slice(&u32::MAX.to_le_bytes());

        assert!(matches!(
            FrontierArtifact::decode(&bytes, &Network::Mainnet),
            Err(TreestateArtifactError::TooManyRecords { .. })
        ));
    }

    #[test]
    fn frontier_anchor_selection_picks_the_nearest_entry_at_or_below() {
        let artifact = sample_frontiers();

        assert!(
            artifact.anchor_at_or_below(Height(0)).is_some(),
            "an exact match on the first entry is an anchor"
        );
        assert_eq!(
            artifact
                .anchor_at_or_below(Height(15))
                .expect("an entry exists below 15")
                .height,
            Height(10),
            "a height between entries anchors on the one below it"
        );
        assert_eq!(
            artifact
                .anchor_at_or_below(Height(9_999))
                .expect("an entry exists below 9999")
                .height,
            Height(20),
            "a height above every entry anchors on the last one"
        );
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
