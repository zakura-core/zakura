//! Validated canonical Sprout-history artifacts for VCT database repair.
//!
//! This format intentionally contains only blocks that change Sprout. It is
//! independent from peer-delivered VCT roots and is never accepted from peers.

use std::{
    collections::HashMap,
    fs,
    io::Write,
    ops::RangeInclusive,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use rayon::prelude::*;
use sha2::{Digest, Sha256};
use thiserror::Error;
use zakura_chain::{
    block::{self, merkle},
    sprout,
    transaction::{self, Transaction},
};

use crate::{
    constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
    service::finalized_state::{
        disk_format::{FromDisk, TransactionLocation},
        ZakuraDb, STATE_COLUMN_FAMILIES_IN_CODE,
    },
    Config, StateInitError,
};

use super::embedded_final_frontiers;

const VERSION: u16 = 1;
const MAGIC: &[u8; 8] = b"ZKVCTSP1";
const MAINNET_NETWORK: u8 = 1;
const MAX_RECORDS: usize = 1_000_000;
const MAX_COMMITMENTS_PER_RECORD: usize = 65_535;
const MAINNET_ARTIFACT: Option<&[u8]> = None;

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
    #[error("Sprout history artifact is unavailable: canonical Mainnet bytes have not been verified and embedded")]
    CanonicalArtifactUnavailable,
    #[error("invalid Sprout history artifact magic")]
    InvalidMagic,
    #[error("unsupported Sprout history artifact version {actual}")]
    UnsupportedVersion { actual: u16 },
    #[error("Sprout history artifact is not for Mainnet")]
    WrongNetwork,
    #[error("Sprout history artifact is truncated")]
    Truncated,
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
        const HEADER_LEN: usize = 8 + 2 + 1 + 4 + 32 + 4 + 32 + 32;
        if bytes.len() < HEADER_LEN {
            return Err(Error::Truncated);
        }
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
        let expected_digest: [u8; 32] = bytes[83..115].try_into().expect("fixed slice length");
        let payload = &bytes[HEADER_LEN..];
        if Sha256::digest(payload).as_slice() != expected_digest {
            return Err(Error::DigestMismatch);
        }

        let mut cursor = 0usize;
        let mut previous = 0u32;
        let mut tree = sprout::tree::NoteCommitmentTree::default();
        let mut records = Vec::with_capacity(record_count);
        for _ in 0..record_count {
            let header = payload.get(cursor..cursor + 38).ok_or(Error::Truncated)?;
            let delta = u32::from_le_bytes(header[..4].try_into().expect("fixed slice length"));
            let block_hash = block::Hash(header[4..36].try_into().expect("fixed slice length"));
            let count = usize::from(u16::from_le_bytes(
                header[36..].try_into().expect("fixed slice length"),
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
            let commitment_bytes = payload
                .get(cursor..cursor + length)
                .ok_or(Error::Truncated)?;
            let mut commitments = Vec::with_capacity(count);
            for bytes in commitment_bytes.chunks_exact(32) {
                let commitment = sprout::commitment::NoteCommitment::from(
                    <[u8; 32]>::try_from(bytes).expect("chunks have fixed length"),
                );
                tree.append(commitment)
                    .map_err(|_| Error::RecordRootMismatch { height })?;
                commitments.push(commitment);
            }
            cursor += length;
            let root: sprout::tree::Root =
                <[u8; 32]>::try_from(payload.get(cursor..cursor + 32).ok_or(Error::Truncated)?)
                    .expect("fixed slice length")
                    .into();
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
        if cursor != payload.len() {
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
    /// A canonical database index has a gap, duplicate, or mismatched row.
    #[error("Mainnet archive canonical index is inconsistent at {height:?}: {reason}")]
    CanonicalIndex {
        /// The affected height.
        height: block::Height,
        /// Details about the inconsistency.
        reason: String,
    },
    /// A transaction row or its stored hash is inconsistent.
    #[error("Mainnet archive transaction data is inconsistent at {height:?}: {reason}")]
    TransactionData {
        /// The affected height.
        height: block::Height,
        /// Details about the inconsistency.
        reason: String,
    },
    /// A checkpoint could not be read, validated, or written.
    #[error("VCT generator checkpoint error: {0}")]
    Checkpoint(String),
    /// Artifact encoding or self-validation failed.
    #[error("could not construct the Sprout repair artifact: {0}")]
    Artifact(String),
}

/// Tuning and crash-recovery options for offline artifact generation.
#[derive(Clone)]
pub struct GeneratorOptions {
    /// Number of contiguous height shards scanned concurrently.
    pub shards: usize,
    /// Rayon worker threads used by shard scans and transaction decoding.
    pub workers: usize,
    /// RocksDB iterator readahead per column-family scan.
    pub readahead_size: usize,
    /// Optional directory for resumable shard outputs.
    pub checkpoint_dir: Option<PathBuf>,
    /// Reuse completed shards from `checkpoint_dir`.
    pub resume: bool,
    /// Optional progress callback, invoked after each completed shard.
    pub progress: Option<Arc<dyn Fn(GeneratorProgress) + Send + Sync>>,
}

/// Aggregate progress from the parallel archive scan.
#[derive(Copy, Clone, Debug)]
pub struct GeneratorProgress {
    /// Heights scanned or loaded from checkpoints.
    pub completed_heights: u64,
    /// Total heights through the handoff.
    pub total_heights: u64,
    /// Elapsed wall time.
    pub elapsed: Duration,
}

impl Default for GeneratorOptions {
    fn default() -> Self {
        let workers = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1);
        Self {
            shards: workers.clamp(1, 32),
            workers,
            readahead_size: 16 * 1024 * 1024,
            checkpoint_dir: None,
            resume: false,
            progress: None,
        }
    }
}

#[derive(Clone, Debug)]
struct CandidateRecord {
    height: block::Height,
    block_hash: block::Hash,
    commitments: Vec<sprout::commitment::NoteCommitment>,
}

#[derive(Clone, Debug)]
struct ScanShard {
    index: usize,
    range: RangeInclusive<block::Height>,
}

/// Generate corrected version-1 artifact bytes from a complete, current-format Mainnet archive.
///
/// The result is deliberately returned to an offline caller and is never installed as the
/// runtime artifact. Release review must independently approve the bytes and digest first.
pub fn generate_mainnet_from_archive(config: &Config) -> Result<Vec<u8>, GeneratorError> {
    generate_mainnet_from_archive_with_options(config, &GeneratorOptions::default())
}

/// Generate corrected version-1 artifact bytes using ordered, parallel archive scans.
pub fn generate_mainnet_from_archive_with_options(
    config: &Config,
    options: &GeneratorOptions,
) -> Result<Vec<u8>, GeneratorError> {
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

    if options.shards == 0 || options.workers == 0 {
        return Err(GeneratorError::Artifact(
            "generator shard and worker counts must be non-zero".to_string(),
        ));
    }

    let reverse_index = Arc::new(load_reverse_index(
        &db,
        last_checkpoint,
        options.readahead_size,
    )?);
    let shards = plan_shards(last_checkpoint, options.shards);
    prepare_checkpoint_dir(
        config,
        options,
        last_checkpoint,
        last_checkpoint_hash,
        &shards,
    )?;

    let started = Instant::now();
    let completed_heights = AtomicU64::new(0);
    let total_heights = u64::from(last_checkpoint.0) + 1;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(options.workers)
        .thread_name(|index| format!("vct-artifact-{index}"))
        .build()
        .map_err(|error| GeneratorError::Artifact(error.to_string()))?;

    let shard_records = pool.install(|| {
        shards
            .par_iter()
            .map(|shard| {
                let checkpoint = options
                    .checkpoint_dir
                    .as_ref()
                    .map(|directory| shard_path(directory, shard.index));
                let records = if options.resume {
                    checkpoint
                        .as_deref()
                        .filter(|path| path.exists())
                        .map(|path| read_shard(path, shard, last_checkpoint, last_checkpoint_hash))
                        .transpose()?
                } else {
                    None
                };

                let records = match records {
                    Some(records) => records,
                    None => {
                        let records =
                            scan_shard(&db, shard, &reverse_index, options.readahead_size)?;
                        if let Some(path) = checkpoint {
                            write_shard_atomic(
                                &path,
                                shard,
                                last_checkpoint,
                                last_checkpoint_hash,
                                &records,
                            )?;
                        }
                        records
                    }
                };

                let end = shard.range.end().0;
                let shard_heights =
                    u64::from(end.saturating_sub(shard.range.start().0).saturating_add(1));
                let completed = completed_heights
                    .fetch_add(shard_heights, Ordering::Relaxed)
                    .saturating_add(shard_heights);
                if let Some(progress) = &options.progress {
                    progress(GeneratorProgress {
                        completed_heights: completed,
                        total_heights,
                        elapsed: started.elapsed(),
                    });
                }
                tracing::info!(
                    shard = shard.index,
                    start_height = shard.range.start().0,
                    end_height = end,
                    records = records.len(),
                    elapsed = ?started.elapsed(),
                    "VCT artifact scan shard complete"
                );
                Ok::<_, GeneratorError>(records)
            })
            .collect::<Result<Vec<_>, _>>()
    })?;

    let records = replay_candidates(shard_records.into_iter().flatten())?;

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

fn replay_candidates(
    candidates: impl IntoIterator<Item = CandidateRecord>,
) -> Result<Vec<Record>, GeneratorError> {
    let mut tree = sprout::tree::NoteCommitmentTree::default();
    let mut records = Vec::new();
    let mut previous_height = None;
    for candidate in candidates {
        if previous_height.is_some_and(|previous| previous >= candidate.height) {
            return Err(GeneratorError::Artifact(Error::InvalidHeight.to_string()));
        }
        for commitment in &candidate.commitments {
            tree.append(*commitment).map_err(|_| {
                GeneratorError::Artifact(
                    Error::RecordRootMismatch {
                        height: candidate.height,
                    }
                    .to_string(),
                )
            })?;
        }
        records.push(Record {
            height: candidate.height,
            block_hash: candidate.block_hash,
            commitments: candidate.commitments,
            resulting_root: tree.root(),
        });
        previous_height = Some(candidate.height);
    }
    Ok(records)
}

#[allow(clippy::unwrap_in_result)]
fn load_reverse_index(
    db: &ZakuraDb,
    last_checkpoint: block::Height,
    readahead_size: usize,
) -> Result<HashMap<block::Hash, block::Height>, GeneratorError> {
    let mut reverse = HashMap::with_capacity(
        usize::try_from(last_checkpoint.0)
            .expect("block heights fit in usize")
            .saturating_add(1),
    );
    for (hash, height) in db.heights_by_hash(readahead_size) {
        if height > last_checkpoint {
            continue;
        }
        if reverse.insert(hash, height).is_some() {
            return Err(GeneratorError::CanonicalIndex {
                height,
                reason: "duplicate reverse-index block hash".to_string(),
            });
        }
    }
    Ok(reverse)
}

fn plan_shards(last_checkpoint: block::Height, requested: usize) -> Vec<ScanShard> {
    let height_count = u64::from(last_checkpoint.0) + 1;
    let shard_count =
        requested.min(usize::try_from(height_count).expect("supported block heights fit in usize"));
    let shard_count_u64 = u64::try_from(shard_count).expect("shard count fits in u64");

    (0..shard_count)
        .map(|index| {
            let index_u64 = u64::try_from(index).expect("shard index fits in u64");
            let start = height_count * index_u64 / shard_count_u64;
            let end = height_count * (index_u64 + 1) / shard_count_u64 - 1;
            ScanShard {
                index,
                range: block::Height(
                    u32::try_from(start).expect("shard start is a supported height"),
                )
                    ..=block::Height(u32::try_from(end).expect("shard end is a supported height")),
            }
        })
        .collect()
}

#[allow(clippy::unwrap_in_result)]
fn scan_shard(
    db: &ZakuraDb,
    shard: &ScanShard,
    reverse_index: &HashMap<block::Hash, block::Height>,
    readahead_size: usize,
) -> Result<Vec<CandidateRecord>, GeneratorError> {
    let start = *shard.range.start();
    let end = *shard.range.end();
    let mut hashes = db.hashes_by_height_range(shard.range.clone(), readahead_size);
    let mut headers = db.raw_block_headers_by_height_range(shard.range.clone(), readahead_size);
    let mut metadata = Vec::with_capacity(
        usize::try_from(end.0 - start.0 + 1).expect("shard height count fits in usize"),
    );

    for raw_height in start.0..=end.0 {
        let height = block::Height(raw_height);
        let (hash_height, canonical_hash) = hashes
            .next()
            .ok_or(GeneratorError::MissingCanonicalHash { height })?;
        if hash_height != height {
            return Err(GeneratorError::CanonicalIndex {
                height,
                reason: format!("forward hash row is keyed by {hash_height:?}"),
            });
        }
        if reverse_index.get(&canonical_hash) != Some(&height) {
            return Err(GeneratorError::CanonicalReverseIndexMismatch { height });
        }

        let (header_height, raw_header) = headers
            .next()
            .ok_or(GeneratorError::MissingBlockBody { height })?;
        if header_height != height {
            return Err(GeneratorError::CanonicalIndex {
                height,
                reason: format!("block header row is keyed by {header_height:?}"),
            });
        }
        let header = block::Header::from_bytes(raw_header.raw_bytes());
        if block::Hash::from(header) != canonical_hash {
            return Err(GeneratorError::BlockBodyHashMismatch { height });
        }
        metadata.push((canonical_hash, header.merkle_root));
    }
    if hashes.next().is_some() || headers.next().is_some() {
        return Err(GeneratorError::CanonicalIndex {
            height: end,
            reason: "bounded metadata iterator returned an out-of-range row".to_string(),
        });
    }

    let location_range =
        TransactionLocation::min_for_height(start)..=TransactionLocation::max_for_height(end);
    let mut transactions = db
        .raw_transactions_by_location_range_for_bulk_scan(location_range.clone(), readahead_size)
        .peekable();
    let mut stored_hashes = db
        .transaction_hashes_by_location_range(location_range, readahead_size)
        .peekable();
    let mut candidates = Vec::new();

    for raw_height in start.0..=end.0 {
        let height = block::Height(raw_height);
        let mut expected_index = 0u16;
        let mut transaction_hashes = Vec::new();
        let mut commitments = Vec::new();

        while transactions
            .peek()
            .is_some_and(|(location, _)| location.height == height)
        {
            let (location, raw_transaction) = transactions
                .next()
                .expect("the transaction iterator was just inspected");
            let (hash_location, stored_hash) =
                stored_hashes
                    .next()
                    .ok_or_else(|| GeneratorError::TransactionData {
                        height,
                        reason: "missing stored transaction hash".to_string(),
                    })?;
            if location != hash_location || location.index.index() != expected_index {
                return Err(GeneratorError::TransactionData {
                    height,
                    reason: format!(
                        "expected transaction index {expected_index}, found {location:?} and \
                         hash row {hash_location:?}"
                    ),
                });
            }

            let transaction = Transaction::from_bytes(raw_transaction.raw_bytes());
            let computed_hash = transaction::Hash::from(&transaction);
            if computed_hash != stored_hash {
                return Err(GeneratorError::TransactionData {
                    height,
                    reason: format!("stored transaction hash mismatch at {location:?}"),
                });
            }
            transaction_hashes.push(computed_hash);
            commitments.extend(transaction.sprout_note_commitments().copied());
            expected_index =
                expected_index
                    .checked_add(1)
                    .ok_or_else(|| GeneratorError::TransactionData {
                        height,
                        reason: "transaction index overflow".to_string(),
                    })?;
        }

        if transaction_hashes.is_empty() {
            return Err(GeneratorError::MissingBlockBody { height });
        }
        let merkle_root: merkle::Root = transaction_hashes.into_iter().collect();
        let metadata_index =
            usize::try_from(raw_height - start.0).expect("metadata index fits in usize");
        let (canonical_hash, expected_merkle_root) = metadata[metadata_index];
        if merkle_root != expected_merkle_root {
            return Err(GeneratorError::TransactionData {
                height,
                reason: "transaction Merkle root does not match the canonical header".to_string(),
            });
        }
        if !commitments.is_empty() {
            candidates.push(CandidateRecord {
                height,
                block_hash: canonical_hash,
                commitments,
            });
        }
    }

    if let Some((location, _)) = transactions.next() {
        return Err(GeneratorError::TransactionData {
            height: location.height,
            reason: "bounded transaction iterator returned an out-of-range row".to_string(),
        });
    }
    if let Some((location, _)) = stored_hashes.next() {
        return Err(GeneratorError::TransactionData {
            height: location.height,
            reason: "stored transaction hash has no transaction row".to_string(),
        });
    }

    Ok(candidates)
}

const SHARD_MAGIC: &[u8; 8] = b"ZKVCTSH1";
const MANIFEST_NAME: &str = "manifest";

fn shard_path(directory: &Path, index: usize) -> PathBuf {
    directory.join(format!("shard-{index:04}.bin"))
}

fn manifest_contents(
    config: &Config,
    options: &GeneratorOptions,
    handoff: block::Height,
    handoff_hash: block::Hash,
    shards: &[ScanShard],
) -> Result<String, GeneratorError> {
    let source = fs::canonicalize(&config.cache_dir)
        .map_err(|error| GeneratorError::Checkpoint(error.to_string()))?;
    let ranges = shards
        .iter()
        .map(|shard| format!("{}-{}", shard.range.start().0, shard.range.end().0))
        .collect::<Vec<_>>()
        .join(",");
    Ok(format!(
        "ZKVCTGEN1\nsource={}\ndb_format={}\nhandoff={}\nhandoff_hash={}\nshards={}\nranges={ranges}\n",
        source.display(),
        state_database_format_version_in_code(),
        handoff.0,
        hex::encode(handoff_hash.0),
        options.shards,
    ))
}

fn prepare_checkpoint_dir(
    config: &Config,
    options: &GeneratorOptions,
    handoff: block::Height,
    handoff_hash: block::Hash,
    shards: &[ScanShard],
) -> Result<(), GeneratorError> {
    let Some(directory) = &options.checkpoint_dir else {
        if options.resume {
            return Err(GeneratorError::Checkpoint(
                "--resume requires a checkpoint directory".to_string(),
            ));
        }
        return Ok(());
    };
    fs::create_dir_all(directory).map_err(|error| GeneratorError::Checkpoint(error.to_string()))?;
    let manifest_path = directory.join(MANIFEST_NAME);
    let expected = manifest_contents(config, options, handoff, handoff_hash, shards)?;

    if options.resume {
        let actual = fs::read_to_string(&manifest_path)
            .map_err(|error| GeneratorError::Checkpoint(error.to_string()))?;
        if actual != expected {
            return Err(GeneratorError::Checkpoint(
                "checkpoint manifest does not match this source or invocation".to_string(),
            ));
        }
    } else {
        if manifest_path.exists()
            || shards
                .iter()
                .any(|shard| shard_path(directory, shard.index).exists())
        {
            return Err(GeneratorError::Checkpoint(
                "checkpoint directory already contains generator state; use --resume or another directory"
                    .to_string(),
            ));
        }
        write_atomic(&manifest_path, expected.as_bytes())?;
    }
    Ok(())
}

fn write_shard_atomic(
    path: &Path,
    shard: &ScanShard,
    handoff: block::Height,
    handoff_hash: block::Hash,
    records: &[CandidateRecord],
) -> Result<(), GeneratorError> {
    let mut payload = Vec::new();
    for record in records {
        payload.extend_from_slice(&record.height.0.to_le_bytes());
        payload.extend_from_slice(&record.block_hash.0);
        let count = u16::try_from(record.commitments.len())
            .map_err(|_| GeneratorError::Artifact(Error::TooManyCommitments.to_string()))?;
        payload.extend_from_slice(&count.to_le_bytes());
        for commitment in &record.commitments {
            payload.extend_from_slice(&<[u8; 32]>::from(commitment));
        }
    }

    let mut bytes = Vec::new();
    bytes.extend_from_slice(SHARD_MAGIC);
    bytes.extend_from_slice(&shard.range.start().0.to_le_bytes());
    bytes.extend_from_slice(&shard.range.end().0.to_le_bytes());
    bytes.extend_from_slice(&handoff.0.to_le_bytes());
    bytes.extend_from_slice(&handoff_hash.0);
    bytes.extend_from_slice(
        &u32::try_from(records.len())
            .map_err(|_| GeneratorError::Artifact(Error::TooManyRecords.to_string()))?
            .to_le_bytes(),
    );
    bytes.extend_from_slice(&Sha256::digest(&payload));
    bytes.extend_from_slice(&payload);
    write_atomic(path, &bytes)
}

#[allow(clippy::unwrap_in_result)]
fn read_shard(
    path: &Path,
    shard: &ScanShard,
    handoff: block::Height,
    handoff_hash: block::Hash,
) -> Result<Vec<CandidateRecord>, GeneratorError> {
    const HEADER_LEN: usize = 8 + 4 + 4 + 4 + 32 + 4 + 32;
    let bytes = fs::read(path).map_err(|error| GeneratorError::Checkpoint(error.to_string()))?;
    if bytes.len() < HEADER_LEN || &bytes[..8] != SHARD_MAGIC {
        return Err(GeneratorError::Checkpoint(format!(
            "invalid shard file {}",
            path.display()
        )));
    }
    let read_u32 = |range: std::ops::Range<usize>| {
        u32::from_le_bytes(bytes[range].try_into().expect("fixed shard header field"))
    };
    if read_u32(8..12) != shard.range.start().0
        || read_u32(12..16) != shard.range.end().0
        || read_u32(16..20) != handoff.0
        || bytes[20..52] != handoff_hash.0
    {
        return Err(GeneratorError::Checkpoint(format!(
            "shard identity mismatch in {}",
            path.display()
        )));
    }
    let record_count = usize::try_from(read_u32(52..56)).expect("u32 fits in usize");
    let payload = &bytes[HEADER_LEN..];
    if Sha256::digest(payload).as_slice() != &bytes[56..88] {
        return Err(GeneratorError::Checkpoint(format!(
            "shard digest mismatch in {}",
            path.display()
        )));
    }

    let mut cursor = 0usize;
    let mut records = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        let header = payload
            .get(cursor..cursor + 38)
            .ok_or_else(|| GeneratorError::Checkpoint("truncated shard record".to_string()))?;
        let height = block::Height(u32::from_le_bytes(
            header[..4].try_into().expect("fixed shard height"),
        ));
        if !shard.range.contains(&height) {
            return Err(GeneratorError::Checkpoint(
                "shard record is outside its height range".to_string(),
            ));
        }
        let block_hash = block::Hash(header[4..36].try_into().expect("fixed shard hash"));
        let count = usize::from(u16::from_le_bytes(
            header[36..38].try_into().expect("fixed shard count"),
        ));
        cursor += 38;
        let byte_count = count
            .checked_mul(32)
            .ok_or_else(|| GeneratorError::Checkpoint("invalid shard count".to_string()))?;
        let commitment_bytes = payload
            .get(cursor..cursor + byte_count)
            .ok_or_else(|| GeneratorError::Checkpoint("truncated shard commitments".to_string()))?;
        let commitments = commitment_bytes
            .chunks_exact(32)
            .map(|bytes| {
                sprout::commitment::NoteCommitment::from(
                    <[u8; 32]>::try_from(bytes).expect("fixed commitment chunk"),
                )
            })
            .collect();
        cursor += byte_count;
        records.push(CandidateRecord {
            height,
            block_hash,
            commitments,
        });
    }
    if cursor != payload.len()
        || records
            .windows(2)
            .any(|records| records[0].height >= records[1].height)
    {
        return Err(GeneratorError::Checkpoint(
            "invalid shard record ordering or trailing bytes".to_string(),
        ));
    }
    Ok(records)
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), GeneratorError> {
    let parent = path
        .parent()
        .ok_or_else(|| GeneratorError::Checkpoint("checkpoint path has no parent".to_string()))?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .map_err(|error| GeneratorError::Checkpoint(error.to_string()))?;
    temporary
        .write_all(bytes)
        .and_then(|()| temporary.as_file_mut().sync_all())
        .map_err(|error| GeneratorError::Checkpoint(error.to_string()))?;
    temporary
        .persist_noclobber(path)
        .map_err(|error| GeneratorError::Checkpoint(error.error.to_string()))?;
    #[cfg(unix)]
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| GeneratorError::Checkpoint(error.to_string()))?;
    Ok(())
}

/// Loads the canonical artifact once independently reviewed bytes are embedded.
///
/// This must never be replaced with downloaded or guessed bytes.
pub(crate) fn embedded_mainnet() -> Result<Artifact, Error> {
    MAINNET_ARTIFACT
        .map(Artifact::decode)
        .transpose()?
        .ok_or(Error::CanonicalArtifactUnavailable)
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn shard_plan_covers_each_height_exactly_once() {
        let shards = plan_shards(block::Height(10), 4);
        let heights: Vec<_> = shards
            .iter()
            .flat_map(|shard| shard.range.start().0..=shard.range.end().0)
            .collect();

        assert_eq!(heights, (0..=10).collect::<Vec<_>>());
        assert_eq!(shards.len(), 4);
    }

    #[test]
    fn resumable_shard_round_trip_preserves_ordered_candidates() {
        let directory = tempfile::tempdir().expect("temporary checkpoint directory is created");
        let path = directory.path().join("shard.bin");
        let shard = ScanShard {
            index: 0,
            range: block::Height(0)..=block::Height(10),
        };
        let handoff = block::Height(10);
        let handoff_hash = block::Hash([10; 32]);
        let records = vec![
            CandidateRecord {
                height: block::Height(2),
                block_hash: block::Hash([2; 32]),
                commitments: vec![sprout::commitment::NoteCommitment::from([20; 32])],
            },
            CandidateRecord {
                height: block::Height(9),
                block_hash: block::Hash([9; 32]),
                commitments: vec![
                    sprout::commitment::NoteCommitment::from([90; 32]),
                    sprout::commitment::NoteCommitment::from([91; 32]),
                ],
            },
        ];

        write_shard_atomic(&path, &shard, handoff, handoff_hash, &records)
            .expect("checkpoint shard is written");
        let decoded =
            read_shard(&path, &shard, handoff, handoff_hash).expect("checkpoint shard is read");

        assert_eq!(decoded.len(), records.len());
        for (decoded, expected) in decoded.iter().zip(&records) {
            assert_eq!(decoded.height, expected.height);
            assert_eq!(decoded.block_hash, expected.block_hash);
            assert_eq!(decoded.commitments, expected.commitments);
        }

        let uninterrupted = Artifact::encode(
            handoff,
            handoff_hash,
            replay_candidates(records).expect("uninterrupted candidates replay"),
        )
        .expect("uninterrupted artifact encodes");
        let resumed = Artifact::encode(
            handoff,
            handoff_hash,
            replay_candidates(decoded).expect("resumed candidates replay"),
        )
        .expect("resumed artifact encodes");
        assert_eq!(resumed, uninterrupted);
    }

    #[test]
    fn resumable_shard_rejects_corruption() {
        let directory = tempfile::tempdir().expect("temporary checkpoint directory is created");
        let path = directory.path().join("shard.bin");
        let shard = ScanShard {
            index: 0,
            range: block::Height(0)..=block::Height(1),
        };
        write_shard_atomic(
            &path,
            &shard,
            block::Height(1),
            block::Hash([1; 32]),
            &[CandidateRecord {
                height: block::Height(1),
                block_hash: block::Hash([1; 32]),
                commitments: vec![sprout::commitment::NoteCommitment::from([1; 32])],
            }],
        )
        .expect("checkpoint shard is written");
        let mut bytes = fs::read(&path).expect("checkpoint shard is readable");
        *bytes.last_mut().expect("checkpoint has a payload") ^= 1;
        fs::write(&path, bytes).expect("checkpoint corruption is written");

        assert!(matches!(
            read_shard(&path, &shard, block::Height(1), block::Hash([1; 32])),
            Err(GeneratorError::Checkpoint(_))
        ));
    }

    #[test]
    fn resume_manifest_rejects_changed_shard_plan() {
        let source = tempfile::tempdir().expect("source directory is created");
        let checkpoint_parent = tempfile::tempdir().expect("checkpoint parent is created");
        let checkpoint = checkpoint_parent.path().join("progress");
        let config = Config {
            cache_dir: source.path().to_path_buf(),
            ..Config::default()
        };
        let mut options = GeneratorOptions {
            shards: 2,
            workers: 2,
            readahead_size: 16 * 1024 * 1024,
            checkpoint_dir: Some(checkpoint),
            resume: false,
            progress: None,
        };
        let handoff = block::Height(10);
        let handoff_hash = block::Hash([10; 32]);
        prepare_checkpoint_dir(
            &config,
            &options,
            handoff,
            handoff_hash,
            &plan_shards(handoff, options.shards),
        )
        .expect("initial manifest is written");

        options.resume = true;
        options.shards = 3;
        assert!(matches!(
            prepare_checkpoint_dir(
                &config,
                &options,
                handoff,
                handoff_hash,
                &plan_shards(handoff, options.shards)
            ),
            Err(GeneratorError::Checkpoint(_))
        ));
    }
}
