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

use sha2::{Digest, Sha256};
use thiserror::Error;

use zakura_chain::{
    block::Height,
    orchard,
    parameters::{Network, NetworkKind},
    subtree::{NoteCommitmentSubtreeData, NoteCommitmentSubtreeIndex},
};

/// Magic bytes identifying a subtree-root artifact.
const SUBTREE_MAGIC: &[u8; 8] = b"ZKVCTST1";

/// The format version both artifacts are written at.
const VERSION: u16 = 1;

/// Fixed header length: magic, version, network, handoff, three record counts, digest.
const SUBTREE_HEADER_LEN: usize = 8 + 2 + 1 + 4 + 4 + 4 + 4 + 32;

/// Bytes per subtree record: index, end height, root.
const SUBTREE_RECORD_LEN: usize = 2 + 4 + 32;

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
