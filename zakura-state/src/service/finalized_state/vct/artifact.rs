//! Validated canonical Sprout-history artifacts for VCT database repair.
//!
//! This format intentionally contains only blocks that change Sprout. It is
//! independent from peer-delivered VCT roots and is never accepted from peers.

use std::io::{Cursor, ErrorKind, Read};

use sha2::{Digest, Sha256};
use thiserror::Error;
use zakura_chain::{block, sprout};

use crate::{
    constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
    request::HashOrHeight,
    service::finalized_state::{ZakuraDb, STATE_COLUMN_FAMILIES_IN_CODE},
    Config, StateInitError,
};

use super::embedded_final_frontiers;

const VERSION: u16 = 1;
const MAGIC: &[u8; 8] = b"ZKVCTSP1";
const MAINNET_NETWORK: u8 = 1;
const MAX_RECORDS: usize = 1_000_000;
const MAX_COMMITMENTS_PER_RECORD: usize = 65_535;
const HEADER_LEN: usize = 8 + 2 + 1 + 4 + 32 + 4 + 32 + 32;
const MAINNET_ARTIFACT_LEN: usize = 71_710_871;

/// One historical block that changed the Sprout commitment tree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Record {
    pub(crate) height: block::Height,
    pub(crate) block_hash: block::Hash,
    pub(crate) commitments: Vec<sprout::commitment::NoteCommitment>,
    pub(crate) resulting_root: sprout::tree::Root,
}

/// Errors parsing or replaying a canonical Sprout-history artifact.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum Error {
    #[error("invalid Sprout history artifact magic")]
    InvalidMagic,
    #[error("unsupported Sprout history artifact version {actual}")]
    UnsupportedVersion { actual: u16 },
    #[error("Sprout history artifact is not for Mainnet")]
    WrongNetwork,
    #[error("Sprout history artifact is truncated")]
    Truncated,
    #[error("could not read the Sprout history artifact")]
    ReadFailure,
    #[error("Sprout history artifact length does not match the reviewed artifact")]
    ArtifactLengthMismatch,
    #[error("Sprout history artifact has too many records")]
    TooManyRecords,
    #[error("Sprout history artifact record has too many commitments")]
    TooManyCommitments,
    #[error(
        "Sprout history artifact record heights are not strictly increasing and within its handoff"
    )]
    InvalidHeight,
    #[error("Sprout history artifact payload digest does not match")]
    DigestMismatch,
    #[error("Sprout history artifact replay root does not match record {height:?}")]
    RecordRootMismatch { height: block::Height },
    #[error("Sprout history artifact terminal root does not match its header")]
    TerminalRootMismatch,
    #[error("Sprout history artifact has trailing bytes")]
    TrailingBytes,
    #[error("Sprout history artifact handoff {actual:?} does not match expected {expected:?}")]
    HandoffMismatch {
        actual: block::Height,
        expected: block::Height,
    },
    #[error("Sprout history artifact handoff block hash does not match the canonical database")]
    HandoffHashMismatch,
    #[error("Sprout history artifact terminal root does not match the embedded handoff frontier")]
    HandoffRootMismatch,
    #[error("canonical database is missing Sprout history artifact record {height:?}")]
    MissingCanonicalRecord { height: block::Height },
    #[error("Sprout history artifact block hash does not match canonical record {height:?}")]
    CanonicalHashMismatch { height: block::Height },
    #[error(
        "canonical reverse block index does not match Sprout history artifact record {height:?}"
    )]
    CanonicalReverseIndexMismatch { height: block::Height },
}

fn read_exact(reader: &mut impl Read, bytes: &mut [u8]) -> Result<(), Error> {
    reader.read_exact(bytes).map_err(|error| {
        if error.kind() == ErrorKind::UnexpectedEof {
            Error::Truncated
        } else {
            Error::ReadFailure
        }
    })
}

#[allow(clippy::type_complexity)]
#[allow(clippy::unwrap_in_result)]
fn decode_header(
    bytes: &[u8; HEADER_LEN],
) -> Result<
    (
        block::Height,
        block::Hash,
        usize,
        sprout::tree::Root,
        [u8; 32],
    ),
    Error,
> {
    if &bytes[..8] != MAGIC {
        return Err(Error::InvalidMagic);
    }
    let version = u16::from_le_bytes(bytes[8..10].try_into().expect("fixed slice length"));
    if version != VERSION {
        return Err(Error::UnsupportedVersion { actual: version });
    }
    if bytes[10] != MAINNET_NETWORK {
        return Err(Error::WrongNetwork);
    }
    let checkpoint = block::Height(u32::from_le_bytes(
        bytes[11..15].try_into().expect("fixed slice length"),
    ));
    let handoff_hash = block::Hash(bytes[15..47].try_into().expect("fixed slice length"));
    let record_count = usize::try_from(u32::from_le_bytes(
        bytes[47..51].try_into().expect("fixed slice length"),
    ))
    .expect("u32 fits in usize on supported platforms");
    if record_count > MAX_RECORDS {
        return Err(Error::TooManyRecords);
    }
    let terminal_root = <[u8; 32]>::try_from(&bytes[51..83])
        .expect("fixed slice length")
        .into();
    let expected_digest = bytes[83..115].try_into().expect("fixed slice length");

    Ok((
        checkpoint,
        handoff_hash,
        record_count,
        terminal_root,
        expected_digest,
    ))
}

/// A decoded, independently replay-validated artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Artifact {
    last_checkpoint: block::Height,
    last_checkpoint_hash: block::Hash,
    terminal_root: sprout::tree::Root,
    records: Vec<Record>,
}

impl Artifact {
    pub(crate) fn encode(
        last_checkpoint: block::Height,
        last_checkpoint_hash: block::Hash,
        records: impl IntoIterator<Item = Record>,
    ) -> Result<Vec<u8>, Error> {
        let records: Vec<_> = records.into_iter().collect();
        if records.len() > MAX_RECORDS {
            return Err(Error::TooManyRecords);
        }

        let mut payload = Vec::new();
        let mut previous = 0u32;
        let mut tree = sprout::tree::NoteCommitmentTree::default();
        for record in &records {
            if record.height.0 <= previous || record.height > last_checkpoint {
                return Err(Error::InvalidHeight);
            }
            if record.commitments.is_empty()
                || record.commitments.len() > MAX_COMMITMENTS_PER_RECORD
            {
                return Err(Error::TooManyCommitments);
            }
            payload.extend_from_slice(&(record.height.0 - previous).to_le_bytes());
            payload.extend_from_slice(&record.block_hash.0);
            let count =
                u16::try_from(record.commitments.len()).map_err(|_| Error::TooManyCommitments)?;
            payload.extend_from_slice(&count.to_le_bytes());
            for commitment in &record.commitments {
                payload.extend_from_slice(&<[u8; 32]>::from(commitment));
                tree.append(*commitment)
                    .map_err(|_| Error::RecordRootMismatch {
                        height: record.height,
                    })?;
            }
            if tree.root() != record.resulting_root {
                return Err(Error::RecordRootMismatch {
                    height: record.height,
                });
            }
            payload.extend_from_slice(&<[u8; 32]>::from(record.resulting_root));
            previous = record.height.0;
        }

        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.push(MAINNET_NETWORK);
        out.extend_from_slice(&last_checkpoint.0.to_le_bytes());
        out.extend_from_slice(&last_checkpoint_hash.0);
        let record_count = u32::try_from(records.len()).map_err(|_| Error::TooManyRecords)?;
        out.extend_from_slice(&record_count.to_le_bytes());
        out.extend_from_slice(&<[u8; 32]>::from(tree.root()));
        out.extend_from_slice(&Sha256::digest(&payload));
        out.extend_from_slice(&payload);
        Ok(out)
    }

    #[allow(clippy::unwrap_in_result)]
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        Self::decode_from_readers(bytes.len(), || Cursor::new(bytes), None)
    }

    #[allow(clippy::unwrap_in_result)]
    fn decode_from_readers<R: Read>(
        source_len: usize,
        mut reader: impl FnMut() -> R,
        expected_len: Option<usize>,
    ) -> Result<Self, Error> {
        if let Some(expected_len) = expected_len {
            if source_len != expected_len {
                return Err(Error::ArtifactLengthMismatch);
            }
        }
        if source_len < HEADER_LEN {
            return Err(Error::Truncated);
        }

        let mut first_pass = reader();
        let mut header = [0; HEADER_LEN];
        read_exact(&mut first_pass, &mut header)?;
        let (checkpoint, handoff_hash, record_count, terminal_root, expected_payload_digest) =
            decode_header(&header)?;

        let mut payload_digest = Sha256::new();
        let mut buffer = [0; 64 * 1024];
        let mut remaining = source_len - HEADER_LEN;
        while remaining > 0 {
            let read_len = remaining.min(buffer.len());
            read_exact(&mut first_pass, &mut buffer[..read_len])?;
            payload_digest.update(&buffer[..read_len]);
            remaining -= read_len;
        }

        if <[u8; 32]>::from(payload_digest.finalize()) != expected_payload_digest {
            return Err(Error::DigestMismatch);
        }

        let mut second_pass = reader();
        read_exact(&mut second_pass, &mut header)?;
        let mut cursor = HEADER_LEN;
        let mut previous = 0u32;
        let mut tree = sprout::tree::NoteCommitmentTree::default();
        let mut records = Vec::with_capacity(record_count);
        for _ in 0..record_count {
            let mut record_header = [0; 38];
            read_exact(&mut second_pass, &mut record_header)?;
            let delta =
                u32::from_le_bytes(record_header[..4].try_into().expect("fixed slice length"));
            let block_hash =
                block::Hash(record_header[4..36].try_into().expect("fixed slice length"));
            let count = usize::from(u16::from_le_bytes(
                record_header[36..].try_into().expect("fixed slice length"),
            ));
            cursor += 38;
            let height = previous
                .checked_add(delta)
                .map(block::Height)
                .ok_or(Error::InvalidHeight)?;
            if delta == 0 || height > checkpoint {
                return Err(Error::InvalidHeight);
            }
            if count == 0 || count > MAX_COMMITMENTS_PER_RECORD {
                return Err(Error::TooManyCommitments);
            }
            let length = count.checked_mul(32).ok_or(Error::Truncated)?;
            let mut commitments = Vec::with_capacity(count);
            for _ in 0..count {
                let mut bytes = [0; 32];
                read_exact(&mut second_pass, &mut bytes)?;
                let commitment = sprout::commitment::NoteCommitment::from(bytes);
                tree.append(commitment)
                    .map_err(|_| Error::RecordRootMismatch { height })?;
                commitments.push(commitment);
            }
            cursor += length;
            let mut root_bytes = [0; 32];
            read_exact(&mut second_pass, &mut root_bytes)?;
            let root: sprout::tree::Root = root_bytes.into();
            cursor += 32;
            if tree.root() != root {
                return Err(Error::RecordRootMismatch { height });
            }
            records.push(Record {
                height,
                block_hash,
                commitments,
                resulting_root: root,
            });
            previous = height.0;
        }
        if cursor != source_len {
            return Err(Error::TrailingBytes);
        }
        if tree.root() != terminal_root {
            return Err(Error::TerminalRootMismatch);
        }
        Ok(Self {
            last_checkpoint: checkpoint,
            last_checkpoint_hash: handoff_hash,
            terminal_root,
            records,
        })
    }

    pub(crate) fn validate_last_checkpoint(
        &self,
        last_checkpoint: block::Height,
        last_checkpoint_hash: block::Hash,
        sprout_root: sprout::tree::Root,
    ) -> Result<(), Error> {
        if self.last_checkpoint != last_checkpoint {
            return Err(Error::HandoffMismatch {
                actual: self.last_checkpoint,
                expected: last_checkpoint,
            });
        }
        if self.last_checkpoint_hash != last_checkpoint_hash {
            return Err(Error::HandoffHashMismatch);
        }
        if self.terminal_root != sprout_root {
            return Err(Error::HandoffRootMismatch);
        }
        Ok(())
    }

    pub(crate) fn validate_canonical(
        &self,
        hash_by_height: impl Fn(block::Height) -> Option<block::Hash>,
        height_by_hash: impl Fn(block::Hash) -> Option<block::Height>,
    ) -> Result<(), Error> {
        self.validate_canonical_through(self.last_checkpoint, hash_by_height, height_by_hash)
    }

    pub(crate) fn validate_canonical_through(
        &self,
        height: block::Height,
        hash_by_height: impl Fn(block::Height) -> Option<block::Hash>,
        height_by_hash: impl Fn(block::Hash) -> Option<block::Height>,
    ) -> Result<(), Error> {
        for record in self.records_through(height) {
            let Some(canonical_hash) = hash_by_height(record.height) else {
                return Err(Error::MissingCanonicalRecord {
                    height: record.height,
                });
            };
            if canonical_hash != record.block_hash {
                return Err(Error::CanonicalHashMismatch {
                    height: record.height,
                });
            }
            if height_by_hash(record.block_hash) != Some(record.height) {
                return Err(Error::CanonicalReverseIndexMismatch {
                    height: record.height,
                });
            }
        }
        Ok(())
    }

    pub(crate) fn records_through(&self, height: block::Height) -> impl Iterator<Item = &Record> {
        self.records
            .iter()
            .take_while(move |record| record.height <= height)
    }
}

/// Errors produced by the offline canonical Mainnet artifact generator.
#[derive(Debug, Error)]
pub enum GeneratorError {
    /// The source archive could not be opened.
    #[error("could not open the Mainnet archive database read-only")]
    OpenDatabase(#[source] StateInitError),
    /// A canonical height has no retained forward hash index.
    #[error("Mainnet archive is missing canonical block {height:?}")]
    MissingCanonicalHash {
        /// The missing block height.
        height: block::Height,
    },
    /// A canonical hash does not map back to its height.
    #[error("Mainnet archive reverse index does not match block {height:?}")]
    CanonicalReverseIndexMismatch {
        /// The inconsistent block height.
        height: block::Height,
    },
    /// The archive is missing a required historical block body.
    #[error("Mainnet archive is missing the body for block {height:?}")]
    MissingBlockBody {
        /// The missing block height.
        height: block::Height,
    },
    /// A retained body does not hash to its canonical index entry.
    #[error("Mainnet archive block body hash does not match its canonical index at {height:?}")]
    BlockBodyHashMismatch {
        /// The inconsistent block height.
        height: block::Height,
    },
    /// Artifact encoding or self-validation failed.
    #[error("could not construct the Sprout repair artifact: {0}")]
    Artifact(String),
}

/// Generate corrected version-1 artifact bytes from a complete, current-format Mainnet archive.
///
/// The result is deliberately returned to an offline caller and is never installed as the
/// runtime artifact. Release review must independently approve the bytes and digest first.
pub fn generate_mainnet_from_archive(config: &Config) -> Result<Vec<u8>, GeneratorError> {
    let network = zakura_chain::parameters::Network::Mainnet;
    let db = ZakuraDb::new(
        config,
        STATE_DATABASE_KIND,
        &state_database_format_version_in_code(),
        &network,
        true,
        STATE_COLUMN_FAMILIES_IN_CODE
            .iter()
            .map(ToString::to_string),
        true,
    )
    .map_err(GeneratorError::OpenDatabase)?;
    let frontiers = embedded_final_frontiers(&network)
        .expect("Mainnet always has an embedded VCT handoff frontier");
    let last_checkpoint = frontiers.height;
    let last_checkpoint_hash =
        db.hash(last_checkpoint)
            .ok_or(GeneratorError::MissingCanonicalHash {
                height: last_checkpoint,
            })?;

    let mut tree = sprout::tree::NoteCommitmentTree::default();
    let mut records = Vec::new();
    for raw_height in 0..=last_checkpoint.0 {
        let height = block::Height(raw_height);
        let canonical_hash = db
            .hash(height)
            .ok_or(GeneratorError::MissingCanonicalHash { height })?;
        if db.height(canonical_hash) != Some(height) {
            return Err(GeneratorError::CanonicalReverseIndexMismatch { height });
        }
        let block = db
            .block(HashOrHeight::Height(height))
            .ok_or(GeneratorError::MissingBlockBody { height })?;
        if block.hash() != canonical_hash {
            return Err(GeneratorError::BlockBodyHashMismatch { height });
        }
        let commitments: Vec<_> = block.sprout_note_commitments().cloned().collect();
        if commitments.is_empty() {
            continue;
        }
        for commitment in &commitments {
            tree.append(*commitment).map_err(|_| {
                GeneratorError::Artifact(Error::RecordRootMismatch { height }.to_string())
            })?;
        }
        records.push(Record {
            height,
            block_hash: canonical_hash,
            commitments,
            resulting_root: tree.root(),
        });
    }

    let bytes = Artifact::encode(last_checkpoint, last_checkpoint_hash, records)
        .map_err(|error| GeneratorError::Artifact(error.to_string()))?;
    let decoded =
        Artifact::decode(&bytes).map_err(|error| GeneratorError::Artifact(error.to_string()))?;
    decoded
        .validate_last_checkpoint(
            last_checkpoint,
            last_checkpoint_hash,
            frontiers.sprout.root(),
        )
        .map_err(|error| GeneratorError::Artifact(error.to_string()))?;
    decoded
        .validate_canonical(|height| db.hash(height), |hash| db.height(hash))
        .map_err(|error| GeneratorError::Artifact(error.to_string()))?;
    Ok(bytes)
}

/// Loads the canonical artifact once independently reviewed bytes are embedded.
///
/// This must never be replaced with downloaded or guessed bytes.
pub(crate) fn embedded_mainnet() -> Result<Artifact, Error> {
    Artifact::decode_from_readers(
        zakura_vct_sprout_history::TOTAL_LEN,
        zakura_vct_sprout_history::Reader::new,
        Some(MAINNET_ARTIFACT_LEN),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_mainnet_artifact_matches_handoff() {
        let network = zakura_chain::parameters::Network::Mainnet;
        let frontiers =
            embedded_final_frontiers(&network).expect("Mainnet has embedded final frontiers");
        let handoff_hash = network
            .checkpoint_list()
            .hash(frontiers.height)
            .expect("the embedded frontier height has a Mainnet checkpoint");
        let artifact = embedded_mainnet().expect("the reviewed Mainnet artifact decodes");

        artifact
            .validate_last_checkpoint(frontiers.height, handoff_hash, frontiers.sprout.root())
            .expect("the reviewed Mainnet artifact matches the embedded handoff");
    }

    #[test]
    fn artifact_round_trip_replays_each_record() {
        let mut tree = sprout::tree::NoteCommitmentTree::default();
        let first = sprout::commitment::NoteCommitment::from([1; 32]);
        tree.append(first).expect("test tree has capacity");
        let bytes = Artifact::encode(
            block::Height(20),
            block::Hash([20; 32]),
            [Record {
                height: block::Height(10),
                block_hash: block::Hash([10; 32]),
                commitments: vec![first],
                resulting_root: tree.root(),
            }],
        )
        .expect("valid fixture encodes");

        let artifact = Artifact::decode(&bytes).expect("fixture decodes");
        artifact
            .validate_last_checkpoint(block::Height(20), block::Hash([20; 32]), tree.root())
            .expect("matching handoff validates");
        assert_eq!(artifact.records_through(block::Height(9)).count(), 0);
        assert_eq!(artifact.records_through(block::Height(10)).count(), 1);
    }

    #[test]
    fn artifact_length_is_checked_independently() {
        let mut tree = sprout::tree::NoteCommitmentTree::default();
        let commitment = sprout::commitment::NoteCommitment::from([7; 32]);
        tree.append(commitment).expect("test tree has capacity");
        let bytes = Artifact::encode(
            block::Height(1),
            block::Hash([1; 32]),
            [Record {
                height: block::Height(1),
                block_hash: block::Hash([1; 32]),
                commitments: vec![commitment],
                resulting_root: tree.root(),
            }],
        )
        .expect("valid fixture encodes");

        Artifact::decode_from_readers(
            bytes.len(),
            || Cursor::new(bytes.as_slice()),
            Some(bytes.len()),
        )
        .expect("artifact with the expected length decodes");
        assert_eq!(
            Artifact::decode_from_readers(
                bytes.len(),
                || Cursor::new(bytes.as_slice()),
                Some(bytes.len() + 1),
            ),
            Err(Error::ArtifactLengthMismatch)
        );
    }

    #[test]
    fn artifact_rejects_digest_tampering() {
        let mut tree = sprout::tree::NoteCommitmentTree::default();
        let commitment = sprout::commitment::NoteCommitment::from([2; 32]);
        tree.append(commitment).expect("test tree has capacity");
        let mut bytes = Artifact::encode(
            block::Height(1),
            block::Hash([1; 32]),
            [Record {
                height: block::Height(1),
                block_hash: block::Hash([1; 32]),
                commitments: vec![commitment],
                resulting_root: tree.root(),
            }],
        )
        .expect("valid fixture encodes");
        *bytes.last_mut().expect("encoded artifact has payload") ^= 1;

        assert_eq!(Artifact::decode(&bytes), Err(Error::DigestMismatch));
    }

    #[test]
    fn artifact_authenticates_handoff_and_record_hashes() {
        let mut tree = sprout::tree::NoteCommitmentTree::default();
        let commitment = sprout::commitment::NoteCommitment::from([3; 32]);
        tree.append(commitment).expect("test tree has capacity");
        let height = block::Height(10);
        let block_hash = block::Hash([10; 32]);
        let handoff = block::Height(20);
        let handoff_hash = block::Hash([20; 32]);
        let artifact = Artifact::decode(
            &Artifact::encode(
                handoff,
                handoff_hash,
                [Record {
                    height,
                    block_hash,
                    commitments: vec![commitment],
                    resulting_root: tree.root(),
                }],
            )
            .expect("valid fixture encodes"),
        )
        .expect("valid fixture decodes");

        artifact
            .validate_last_checkpoint(handoff, handoff_hash, tree.root())
            .expect("matching handoff identity validates");
        artifact
            .validate_canonical(
                |candidate| (candidate == height).then_some(block_hash),
                |candidate| (candidate == block_hash).then_some(height),
            )
            .expect("matching canonical indexes validate");

        assert_eq!(
            artifact.validate_last_checkpoint(handoff, block::Hash([21; 32]), tree.root()),
            Err(Error::HandoffHashMismatch)
        );
        assert_eq!(
            artifact.validate_canonical(|_| None, |_| Some(height)),
            Err(Error::MissingCanonicalRecord { height })
        );
        assert_eq!(
            artifact.validate_canonical(|_| Some(block::Hash([11; 32])), |_| Some(height)),
            Err(Error::CanonicalHashMismatch { height })
        );
        assert_eq!(
            artifact.validate_canonical(|_| Some(block_hash), |_| Some(block::Height(11))),
            Err(Error::CanonicalReverseIndexMismatch { height })
        );
    }

    #[test]
    fn prefix_validation_never_queries_records_above_the_tip() {
        let mut tree = sprout::tree::NoteCommitmentTree::default();
        let first_height = block::Height(10);
        let first_hash = block::Hash([10; 32]);
        let later_height = block::Height(20);
        let later_hash = block::Hash([99; 32]);
        let records = [(first_height, first_hash, 5), (later_height, later_hash, 6)].map(
            |(height, block_hash, value)| {
                let commitment = sprout::commitment::NoteCommitment::from([value; 32]);
                tree.append(commitment).expect("test tree has capacity");
                Record {
                    height,
                    block_hash,
                    commitments: vec![commitment],
                    resulting_root: tree.root(),
                }
            },
        );
        let artifact = Artifact::decode(
            &Artifact::encode(block::Height(30), block::Hash([30; 32]), records)
                .expect("valid fixture encodes"),
        )
        .expect("valid fixture globally replays");

        artifact
            .validate_canonical_through(
                first_height,
                |height| {
                    assert_eq!(
                        height, first_height,
                        "records above the finalized tip must not be queried"
                    );
                    Some(first_hash)
                },
                |hash| {
                    assert_eq!(
                        hash, first_hash,
                        "records above the finalized tip must not be reverse-queried"
                    );
                    Some(first_height)
                },
            )
            .expect("a mismatch above the finalized tip is deferred");
    }

    #[test]
    fn artifact_digest_covers_canonical_block_hashes() {
        let mut tree = sprout::tree::NoteCommitmentTree::default();
        let commitment = sprout::commitment::NoteCommitment::from([4; 32]);
        tree.append(commitment).expect("test tree has capacity");
        let mut bytes = Artifact::encode(
            block::Height(1),
            block::Hash([1; 32]),
            [Record {
                height: block::Height(1),
                block_hash: block::Hash([1; 32]),
                commitments: vec![commitment],
                resulting_root: tree.root(),
            }],
        )
        .expect("valid fixture encodes");

        // The first record hash begins after the fixed header and height delta.
        bytes[115 + 4] ^= 1;
        assert_eq!(Artifact::decode(&bytes), Err(Error::DigestMismatch));
    }
}
