//! Reth-style static files for finalized transaction bodies.
//!
//! Finalized transaction bytes never change, so like reth, Zakura keeps them in
//! append-only files instead of a key-value store, and the database keeps only
//! the indexes. Raw transactions are about 95% of an archive node's bytes, and
//! keeping them out of RocksDB removes them from compaction.
//!
//! # Layout
//!
//! The layout follows reth's NippyJar static files. Each file set ("jar") covers
//! part of a fixed block range, and its base name records the first block it
//! holds and the end of its range: `static_file_transactions_{first}_{end}`.
//!
//! - The data file holds raw transactions, concatenated in block and
//!   transaction order with no delimiters.
//! - `.off` holds one offset width byte (8), then one little-endian `u64` data
//!   offset per transaction, then the data length.
//! - `.blk` holds one offset width byte (8), then the first transaction row of
//!   each block, then the row count. Reth keeps this block index in its database.
//!   Zcash transaction locations are per block, so it lives with the rows here.
//! - `.conf` holds the committed [`JarConfig`]. It is written last, atomically,
//!   so it is the commit marker.
//!
//! Ranges start on multiples of the blocks per file, so pruning can delete whole
//! files. A jar can start after its range start when earlier blocks were never
//! moved here, for example below a pruned node's retention window.
//!
//! # Crash consistency
//!
//! Like reth, a commit syncs the data, offsets and block index files before it
//! replaces the config. Files longer than their config are interrupted appends
//! or truncations, and writable opens cut them back. Files shorter than their
//! config are corrupt.
//!
//! The database records the highest block moved here, see
//! [`ZakuraDb::static_file_transactions_tip`](super::ZakuraDb::static_file_transactions_tip).
//! Like reth, the database wins when the static files are ahead.
//!
//! # Untrusted files
//!
//! Static files can arrive in a state snapshot built by someone else. Readers
//! use safe Rust and positioned reads instead of memory maps, and check every
//! offset against the committed config and the maximum block size before they
//! allocate or read.

// A poisoned lock means another thread panicked while changing the static
// files, so callers panic too instead of getting a static file error.
#![allow(clippy::unwrap_in_result)]

use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io,
    ops::{Range, RangeInclusive},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
};

use zakura_chain::block::{Height, MAX_BLOCK_BYTES};

use super::disk_format::RawBytes;

#[cfg(test)]
mod tests;

/// The directory inside the state database directory that holds static files.
pub(crate) const STATIC_FILE_DIR: &str = "static_files";

/// The base name prefix for transaction static files.
const FILE_PREFIX: &str = "static_file_transactions";

/// Reth's default number of blocks per static file, used by archive nodes.
pub(crate) const DEFAULT_BLOCKS_PER_FILE: u32 = 500_000;

/// The smallest number of blocks per static file, from reth's pruned-segment rule.
const MIN_BLOCKS_PER_FILE: u32 = 1_000;

/// The static file config format version.
const CONFIG_VERSION: u32 = 1;

/// The magic bytes that start a `.conf` file.
const CONFIG_MAGIC: [u8; 4] = *b"ZKSF";

/// The length of an encoded `.conf` file.
const CONFIG_LEN: usize = 36;

/// The offset width byte that starts `.off` and `.blk` files.
const OFFSET_WIDTH: u8 = 8;

/// The length of the width byte that starts `.off` and `.blk` files.
const INDEX_HEADER_LEN: u64 = 1;

/// The largest row count a config may claim. It keeps index file lengths far
/// from `u64` overflow while allowing many times more transactions than any
/// chain will hold.
const MAX_ROWS: u64 = 1 << 48;

/// Returns the number of blocks per static file for a node's pruning setting.
///
/// Archive nodes use reth's 500,000 blocks. Pruned nodes delete whole files, so
/// like reth's distance-pruned segments they use a quarter of the retention
/// window, between 1,000 and 500,000 blocks. With Zakura's 10,000 block minimum
/// retention that is 2,500 blocks, so a pruned node keeps at most a quarter more
/// transaction history than it was asked to.
pub(crate) fn blocks_per_file(tx_retention: Option<u32>) -> u32 {
    tx_retention.map_or(DEFAULT_BLOCKS_PER_FILE, |retention| {
        (retention / 4).clamp(MIN_BLOCKS_PER_FILE, DEFAULT_BLOCKS_PER_FILE)
    })
}

/// An error reading or writing static files.
#[derive(Debug, thiserror::Error)]
pub enum StaticFileError {
    /// A file operation failed.
    #[error("static file I/O failed at {path:?}: {source}")]
    Io {
        /// The file or directory.
        path: PathBuf,
        /// The underlying error.
        #[source]
        source: io::Error,
    },

    /// A file doesn't match its committed config.
    #[error("static file {path:?} is corrupt: {reason}")]
    Corrupt {
        /// The file.
        path: PathBuf,
        /// What didn't match.
        reason: String,
    },

    /// The database half of a move failed. The moved blocks stay in `tx_by_loc`.
    #[error("recording a static file move in the database failed: {0}")]
    Database(#[from] rocksdb::Error),

    /// A block was appended at or below the static file tip.
    #[error("static file append at {height:?} must be above the static file tip {tip:?}")]
    OutOfOrder {
        /// The appended block height.
        height: Height,
        /// The highest block already in static files.
        tip: Option<Height>,
    },
}

/// Returns a closure that wraps an I/O error with `path`.
fn io_error(path: &Path) -> impl FnOnce(io::Error) -> StaticFileError + '_ {
    move |source| StaticFileError::Io {
        path: path.to_owned(),
        source,
    }
}

/// Returns a corruption error for `path`.
fn corrupt(path: &Path, reason: impl Into<String>) -> StaticFileError {
    StaticFileError::Corrupt {
        path: path.to_owned(),
        reason: reason.into(),
    }
}

/// The committed state of one jar, stored in its `.conf` file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct JarConfig {
    /// The config format version, [`CONFIG_VERSION`].
    version: u32,
    /// The first block this jar holds.
    first_block: u32,
    /// The last block this jar's range allows.
    expected_end: u32,
    /// The number of committed blocks, starting at `first_block`.
    blocks: u32,
    /// The number of committed transactions.
    rows: u64,
    /// The committed data file length.
    data_len: u64,
}

impl JarConfig {
    /// Returns the config of a new jar with no blocks.
    fn empty(first_block: u32, expected_end: u32) -> Self {
        JarConfig {
            version: CONFIG_VERSION,
            first_block,
            expected_end,
            blocks: 0,
            rows: 0,
            data_len: 0,
        }
    }

    /// Encodes this config: magic, then little-endian version, first block,
    /// range end, block count, row count and data length.
    fn to_bytes(self) -> [u8; CONFIG_LEN] {
        let mut bytes = [0; CONFIG_LEN];
        bytes[0..4].copy_from_slice(&CONFIG_MAGIC);
        bytes[4..8].copy_from_slice(&self.version.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.first_block.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.expected_end.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.blocks.to_le_bytes());
        bytes[20..28].copy_from_slice(&self.rows.to_le_bytes());
        bytes[28..36].copy_from_slice(&self.data_len.to_le_bytes());
        bytes
    }

    /// Decodes a config written by [`JarConfig::to_bytes`], without validating it.
    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let bytes: &[u8; CONFIG_LEN] = bytes.try_into().ok()?;
        if bytes[0..4] != CONFIG_MAGIC {
            return None;
        }
        let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().expect("4 bytes"));
        let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().expect("8 bytes"));
        Some(JarConfig {
            version: u32_at(4),
            first_block: u32_at(8),
            expected_end: u32_at(12),
            blocks: u32_at(16),
            rows: u64_at(20),
            data_len: u64_at(28),
        })
    }

    /// Returns the last committed block, if any.
    fn last_block(&self) -> Option<u32> {
        // `validate` checks `first_block + blocks - 1 <= expected_end`, so this can't overflow.
        self.blocks.checked_sub(1).map(|n| self.first_block + n)
    }

    /// Returns `true` if `height` is a committed block of this jar.
    fn contains(&self, height: u32) -> bool {
        height >= self.first_block && self.last_block().is_some_and(|last| height <= last)
    }

    /// Returns the committed `.off` length: the width byte and `rows + 1` offsets.
    fn off_len(&self) -> u64 {
        // `validate` bounds `rows` by `MAX_ROWS`, so this can't overflow.
        INDEX_HEADER_LEN + 8 * (self.rows + 1)
    }

    /// Returns the committed `.blk` length: the width byte and `blocks + 1` entries.
    fn blk_len(&self) -> u64 {
        INDEX_HEADER_LEN + 8 * (u64::from(self.blocks) + 1)
    }

    /// Checks that this config read from `path` is well formed and matches its
    /// file name's `first_block` and `expected_end`.
    fn validate(
        &self,
        path: &Path,
        first_block: u32,
        expected_end: u32,
    ) -> Result<(), StaticFileError> {
        if self.version != CONFIG_VERSION {
            return Err(corrupt(
                path,
                format!("unknown config version {}", self.version),
            ));
        }
        if self.first_block != first_block || self.expected_end != expected_end {
            return Err(corrupt(path, "config range doesn't match the file name"));
        }
        if first_block > expected_end
            || u64::from(self.blocks) > u64::from(expected_end - first_block) + 1
        {
            return Err(corrupt(path, "config block count doesn't fit its range"));
        }
        if self.rows > MAX_ROWS || self.data_len > MAX_ROWS.saturating_mul(MAX_BLOCK_BYTES) {
            return Err(corrupt(
                path,
                "config row count or data length is too large",
            ));
        }
        Ok(())
    }
}

/// The open files of one jar.
#[derive(Debug)]
struct JarFiles {
    /// The base path, which is also the data file path.
    base: PathBuf,
    data: File,
    off: File,
    blk: File,
}

/// One committed jar: its open files and committed config.
#[derive(Clone, Debug)]
struct Jar {
    files: Arc<JarFiles>,
    config: JarConfig,
}

/// Returns the base path of the jar starting at `first_block` in `dir`.
fn jar_base(dir: &Path, first_block: u32, expected_end: u32) -> PathBuf {
    dir.join(format!("{FILE_PREFIX}_{first_block}_{expected_end}"))
}

/// Returns `base` with `.{suffix}` appended.
fn suffixed(base: &Path, suffix: &str) -> PathBuf {
    let mut path = OsString::from(base.as_os_str());
    path.push(".");
    path.push(suffix);
    PathBuf::from(path)
}

/// Parses a static file name into its first block, range end, and suffix.
fn parse_file_name(name: &str) -> Option<(u32, u32, Option<&str>)> {
    let rest = name.strip_prefix(FILE_PREFIX)?.strip_prefix('_')?;
    let (range, suffix) = match rest.split_once('.') {
        Some((range, suffix)) => (range, Some(suffix)),
        None => (rest, None),
    };
    let (first, end) = range.split_once('_')?;
    Some((first.parse().ok()?, end.parse().ok()?, suffix))
}

/// Reads exactly `buf.len()` bytes at `offset`.
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::FileExt::read_exact_at(file, buf, offset)
    }
    #[cfg(windows)]
    {
        let mut done = 0;
        while done < buf.len() {
            // `done` is at most `buf.len()`, which fits in a `u64`.
            let read = std::os::windows::fs::FileExt::seek_read(
                file,
                &mut buf[done..],
                offset + done as u64,
            )?;
            if read == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            done += read;
        }
        Ok(())
    }
}

/// Writes all of `buf` at `offset`.
fn write_all_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::FileExt::write_all_at(file, buf, offset)
    }
    #[cfg(windows)]
    {
        let mut done = 0;
        while done < buf.len() {
            // `done` is at most `buf.len()`, which fits in a `u64`.
            let written = std::os::windows::fs::FileExt::seek_write(
                file,
                &buf[done..],
                offset + done as u64,
            )?;
            if written == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            done += written;
        }
        Ok(())
    }
}

/// Syncs `dir` so renames and new files in it are durable.
fn sync_dir(dir: &Path) -> Result<(), StaticFileError> {
    #[cfg(unix)]
    {
        File::open(dir)
            .and_then(|dir| dir.sync_all())
            .map_err(io_error(dir))
    }
    // Windows can't open a directory as a file, and NTFS journals renames.
    #[cfg(windows)]
    {
        let _ = dir;
        Ok(())
    }
}

/// Reads `count` little-endian `u64` entries starting at entry `first` of an
/// index file whose entries start after the width byte.
fn read_entries(
    file: &File,
    path: &Path,
    first: u64,
    count: u64,
) -> Result<Vec<u64>, StaticFileError> {
    // Callers bound `first` and `count` by the committed config, which bounds
    // them by `MAX_ROWS`, so these can't overflow and the buffer fits in memory.
    let mut bytes = vec![0; (count * 8) as usize];
    read_exact_at(file, &mut bytes, INDEX_HEADER_LEN + first * 8).map_err(io_error(path))?;
    let (entries, _) = bytes.as_chunks::<8>();
    Ok(entries
        .iter()
        .map(|entry| u64::from_le_bytes(*entry))
        .collect())
}

/// Encodes `entries` as little-endian `u64`s.
fn encode_entries(entries: &[u64]) -> Vec<u8> {
    entries
        .iter()
        .flat_map(|entry| entry.to_le_bytes())
        .collect()
}

/// Writes `config` to `base.conf` atomically: a synced temporary file renamed
/// over the old config, then a directory sync.
fn write_config(base: &Path, config: &JarConfig) -> Result<(), StaticFileError> {
    let path = suffixed(base, "conf");
    let tmp = suffixed(base, "conf.tmp");
    let bytes = config.to_bytes();

    let file = File::create(&tmp).map_err(io_error(&tmp))?;
    write_all_at(&file, &bytes, 0).map_err(io_error(&tmp))?;
    file.sync_all().map_err(io_error(&tmp))?;
    drop(file);

    fs::rename(&tmp, &path).map_err(io_error(&path))?;
    sync_dir(
        base.parent()
            .expect("jar paths are inside the static file directory"),
    )
}

/// Removes `path`, treating a missing file as already removed.
fn remove_file(path: &Path) -> Result<(), StaticFileError> {
    match fs::remove_file(path) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(io_error(path)(error)),
        _ => Ok(()),
    }
}

/// Deletes a jar's files, config first so a crash leaves no committed jar behind.
fn delete_jar_files(base: &Path) -> Result<(), StaticFileError> {
    remove_file(&suffixed(base, "conf"))?;
    sync_dir(
        base.parent()
            .expect("jar paths are inside the static file directory"),
    )?;
    for suffix in ["off", "blk", "conf.tmp"] {
        remove_file(&suffixed(base, suffix))?;
    }
    remove_file(base)
}

impl Jar {
    /// Opens the committed jar for `first_block..=expected_end` in `dir`.
    ///
    /// Returns `None` if the jar has no committed config. Writable opens cut
    /// files back to their committed lengths. Every open rejects files shorter
    /// than their committed lengths.
    fn open(
        dir: &Path,
        first_block: u32,
        expected_end: u32,
        writable: bool,
    ) -> Result<Option<Jar>, StaticFileError> {
        let base = jar_base(dir, first_block, expected_end);
        let conf_path = suffixed(&base, "conf");
        let config_bytes = match fs::read(&conf_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io_error(&conf_path)(error)),
        };
        let config = JarConfig::from_bytes(&config_bytes)
            .ok_or_else(|| corrupt(&conf_path, "unreadable config"))?;
        config.validate(&conf_path, first_block, expected_end)?;

        let open = |path: &Path| {
            OpenOptions::new()
                .read(true)
                .write(writable)
                .open(path)
                .map_err(io_error(path))
        };
        let off_path = suffixed(&base, "off");
        let blk_path = suffixed(&base, "blk");
        let files = JarFiles {
            data: open(&base)?,
            off: open(&off_path)?,
            blk: open(&blk_path)?,
            base,
        };

        for (file, path, committed_len) in [
            (&files.data, files.base.as_path(), config.data_len),
            (&files.off, off_path.as_path(), config.off_len()),
            (&files.blk, blk_path.as_path(), config.blk_len()),
        ] {
            let len = file.metadata().map_err(io_error(path))?.len();
            if len < committed_len {
                return Err(corrupt(
                    path,
                    format!("file is {len} bytes, but its config commits {committed_len}"),
                ));
            }
            if len > committed_len && writable {
                tracing::info!(
                    ?path,
                    len,
                    committed_len,
                    "truncating an interrupted static file write"
                );
                file.set_len(committed_len).map_err(io_error(path))?;
                file.sync_all().map_err(io_error(path))?;
            }
        }

        for (file, path) in [(&files.off, &off_path), (&files.blk, &blk_path)] {
            let mut width = [0];
            read_exact_at(file, &mut width, 0).map_err(io_error(path))?;
            if width[0] != OFFSET_WIDTH {
                return Err(corrupt(path, format!("unknown offset width {}", width[0])));
            }
        }

        Ok(Some(Jar {
            files: Arc::new(files),
            config,
        }))
    }

    /// Creates the files of a new empty jar. They are committed by the first
    /// [`Jar::commit`], which writes the config.
    fn create(dir: &Path, first_block: u32, expected_end: u32) -> Result<Jar, StaticFileError> {
        let base = jar_base(dir, first_block, expected_end);
        let create = |path: &Path, contents: &[u8]| {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .map_err(io_error(path))?;
            write_all_at(&file, contents, 0).map_err(io_error(path))?;
            Ok::<_, StaticFileError>(file)
        };

        let mut empty_index = vec![OFFSET_WIDTH];
        empty_index.extend_from_slice(&0u64.to_le_bytes());

        let files = JarFiles {
            data: create(&base, &[])?,
            off: create(&suffixed(&base, "off"), &empty_index)?,
            blk: create(&suffixed(&base, "blk"), &empty_index)?,
            base,
        };

        Ok(Jar {
            files: Arc::new(files),
            config: JarConfig::empty(first_block, expected_end),
        })
    }

    /// Returns the next block this jar can hold, if its range has room.
    fn next_block(&self) -> Option<u32> {
        let next = self
            .config
            .last_block()
            .map_or(Some(self.config.first_block), |last| last.checked_add(1))?;
        (next <= self.config.expected_end).then_some(next)
    }

    /// Returns the `.off` path.
    fn off_path(&self) -> PathBuf {
        suffixed(&self.files.base, "off")
    }

    /// Returns the `.blk` path.
    fn blk_path(&self) -> PathBuf {
        suffixed(&self.files.base, "blk")
    }

    /// Returns the transaction rows of committed block `height`.
    fn block_rows(&self, height: u32) -> Result<Option<Range<u64>>, StaticFileError> {
        if !self.config.contains(height) {
            return Ok(None);
        }
        let index = u64::from(height - self.config.first_block);
        let entries = read_entries(&self.files.blk, &self.blk_path(), index, 2)?;
        let (start, end) = (entries[0], entries[1]);
        if start > end || end > self.config.rows {
            return Err(corrupt(
                &self.blk_path(),
                format!(
                    "block {height} has rows {start}..{end} outside 0..{}",
                    self.config.rows
                ),
            ));
        }
        Ok(Some(start..end))
    }

    /// Reads transaction `rows`, which must be committed rows of a single block.
    fn read_rows(&self, rows: Range<u64>) -> Result<Vec<RawBytes>, StaticFileError> {
        let count = rows.end - rows.start;
        if count == 0 {
            return Ok(Vec::new());
        }
        // Every transaction is at least one byte, so one block can't hold more
        // transactions than its maximum size in bytes.
        if count > MAX_BLOCK_BYTES {
            return Err(corrupt(
                &self.blk_path(),
                format!("block has {count} transactions"),
            ));
        }

        let offsets = read_entries(&self.files.off, &self.off_path(), rows.start, count + 1)?;
        if offsets.windows(2).any(|pair| pair[0] > pair[1]) {
            return Err(corrupt(&self.off_path(), "offsets go backwards"));
        }
        let (start, end) = (offsets[0], offsets[offsets.len() - 1]);
        if end > self.config.data_len || end - start > MAX_BLOCK_BYTES {
            return Err(corrupt(
                &self.off_path(),
                format!("block data {start}..{end} is outside the committed data or too large"),
            ));
        }

        // The length was just checked against the maximum block size.
        let mut data = vec![0; (end - start) as usize];
        read_exact_at(&self.files.data, &mut data, start).map_err(io_error(&self.files.base))?;

        Ok(offsets
            .windows(2)
            // Offsets are within `start..=end`, and that span fits in `data`.
            .map(|pair| {
                RawBytes::new_raw_bytes(
                    data[(pair[0] - start) as usize..(pair[1] - start) as usize].to_vec(),
                )
            })
            .collect())
    }

    /// Appends `blocks` after this jar's committed blocks and commits them like
    /// reth: data, offsets and block index synced in that order, then the config.
    ///
    /// Each block is its transactions, and the caller checks heights follow on.
    fn commit(&self, blocks: &[&[RawBytes]]) -> Result<Jar, StaticFileError> {
        let mut config = self.config;
        let mut data = Vec::new();
        let mut offsets = Vec::new();
        let mut block_ends = Vec::new();
        for transactions in blocks {
            for transaction in *transactions {
                data.extend_from_slice(transaction.raw_bytes());
                // Data lengths fit in a `u64`.
                offsets.push(config.data_len + data.len() as u64);
            }
            // Row counts fit in a `u64`.
            block_ends.push(config.rows + offsets.len() as u64);
        }

        write_all_at(&self.files.data, &data, config.data_len)
            .map_err(io_error(&self.files.base))?;
        self.files
            .data
            .sync_data()
            .map_err(io_error(&self.files.base))?;
        write_all_at(&self.files.off, &encode_entries(&offsets), config.off_len())
            .map_err(io_error(&self.off_path()))?;
        self.files
            .off
            .sync_data()
            .map_err(io_error(&self.off_path()))?;
        write_all_at(
            &self.files.blk,
            &encode_entries(&block_ends),
            config.blk_len(),
        )
        .map_err(io_error(&self.blk_path()))?;
        self.files
            .blk
            .sync_data()
            .map_err(io_error(&self.blk_path()))?;

        // Block counts per jar fit in a `u32`, because they fit in the jar's range.
        config.blocks += blocks.len() as u32;
        config.rows += offsets.len() as u64;
        config.data_len += data.len() as u64;
        write_config(&self.files.base, &config)?;

        Ok(Jar {
            files: self.files.clone(),
            config,
        })
    }

    /// Removes the blocks after `last_block` from this jar.
    ///
    /// The smaller config is committed first, so a crash leaves files longer
    /// than their config, which the next writable open cuts back.
    fn truncate_after(&self, last_block: u32) -> Result<Jar, StaticFileError> {
        let blocks = last_block - self.config.first_block + 1;
        let rows = read_entries(&self.files.blk, &self.blk_path(), u64::from(blocks), 1)?[0];
        if rows > self.config.rows {
            return Err(corrupt(
                &self.blk_path(),
                "block index points past the committed rows",
            ));
        }
        let data_len = read_entries(&self.files.off, &self.off_path(), rows, 1)?[0];
        if data_len > self.config.data_len {
            return Err(corrupt(
                &self.off_path(),
                "offset points past the committed data",
            ));
        }

        let config = JarConfig {
            blocks,
            rows,
            data_len,
            ..self.config
        };
        write_config(&self.files.base, &config)?;

        for (file, path, len) in [
            (&self.files.data, self.files.base.clone(), config.data_len),
            (&self.files.off, self.off_path(), config.off_len()),
            (&self.files.blk, self.blk_path(), config.blk_len()),
        ] {
            file.set_len(len).map_err(io_error(&path))?;
            file.sync_all().map_err(io_error(&path))?;
        }

        Ok(Jar {
            files: self.files.clone(),
            config,
        })
    }
}

/// The transaction static files of one state database.
///
/// Clones of a `ZakuraDb` share one instance. Reads take a shared lock and
/// positioned reads. Appends, truncation and deletion are serialized.
#[derive(Debug)]
pub struct StaticFiles {
    dir: PathBuf,
    writable: bool,
    blocks_per_file: u32,
    /// Committed jars, keyed by their first block. Their block ranges don't overlap.
    jars: RwLock<BTreeMap<u32, Jar>>,
    /// Serializes changes to `jars` and the files.
    writer: Mutex<()>,
}

impl StaticFiles {
    /// Opens the static files in `dir`.
    ///
    /// A writable open creates `dir`, removes files that were never committed,
    /// and cuts interrupted writes back to their committed lengths. A read-only
    /// open of a missing directory has no static files until the writer creates
    /// some.
    pub fn open(
        dir: PathBuf,
        writable: bool,
        blocks_per_file: u32,
    ) -> Result<Self, StaticFileError> {
        if writable {
            fs::create_dir_all(&dir).map_err(io_error(&dir))?;
        }
        let static_files = StaticFiles {
            jars: RwLock::new(Self::scan(&dir, writable)?),
            dir,
            writable,
            blocks_per_file,
            writer: Mutex::new(()),
        };
        Ok(static_files)
    }

    /// Opens every committed jar in `dir`, cleaning up after crashes if `writable`.
    fn scan(dir: &Path, writable: bool) -> Result<BTreeMap<u32, Jar>, StaticFileError> {
        let entries = match fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound && !writable => {
                return Ok(BTreeMap::new())
            }
            Err(error) => return Err(io_error(dir)(error)),
        };

        let mut ranges = BTreeMap::new();
        for entry in entries {
            let entry = entry.map_err(io_error(dir))?;
            let name = entry.file_name();
            if let Some((first, end, _)) = name.to_str().and_then(parse_file_name) {
                ranges.insert(first, end);
            }
        }

        let mut jars = BTreeMap::new();
        let mut previous_last: Option<u32> = None;
        for (first, end) in ranges {
            let base = jar_base(dir, first, end);
            let jar = match Jar::open(dir, first, end, writable)? {
                Some(jar) if jar.config.blocks > 0 => jar,
                // A jar without a committed config never held committed blocks,
                // and an empty jar is left by truncation. Neither holds data.
                _ => {
                    if writable {
                        delete_jar_files(&base)?;
                    }
                    continue;
                }
            };
            if previous_last.is_some_and(|last| first <= last) {
                return Err(corrupt(&base, "jar overlaps the previous jar's blocks"));
            }
            previous_last = jar.config.last_block();
            jars.insert(first, jar);
        }

        Ok(jars)
    }

    /// Returns the static file directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Returns the committed bytes of every jar's data, offsets, block index and config.
    pub fn committed_bytes(&self) -> u64 {
        let jars = self
            .jars
            .read()
            .expect("static file lock is never poisoned");
        jars.values()
            .map(|jar| {
                // Config bounds keep these sums far from overflow.
                jar.config.data_len
                    + jar.config.off_len()
                    + jar.config.blk_len()
                    + CONFIG_LEN as u64
            })
            .sum()
    }

    /// Returns the highest block in static files.
    pub fn tip(&self) -> Option<Height> {
        let jars = self
            .jars
            .read()
            .expect("static file lock is never poisoned");
        jars.values()
            .next_back()
            .and_then(|jar| jar.config.last_block())
            .map(Height)
    }

    /// Returns the committed block ranges that intersect `range`, in height order.
    pub fn ranges_within(&self, range: RangeInclusive<Height>) -> Vec<RangeInclusive<Height>> {
        let (start, end) = (range.start().0, range.end().0);
        let jars = self
            .jars
            .read()
            .expect("static file lock is never poisoned");
        jars.values()
            .filter_map(|jar| {
                let last = jar.config.last_block()?;
                let first = jar.config.first_block.max(start);
                let last = last.min(end);
                (first <= last).then_some(Height(first)..=Height(last))
            })
            .collect()
    }

    /// Returns the jar holding `height`, if any.
    fn jar_for(&self, height: u32) -> Option<Jar> {
        let jars = self
            .jars
            .read()
            .expect("static file lock is never poisoned");
        let (_, jar) = jars.range(..=height).next_back()?;
        jar.config.contains(height).then(|| jar.clone())
    }

    /// Returns the jar holding `height`, rescanning the directory first in
    /// read-only mode if `height` is above the known tip, because another
    /// process may have committed it.
    fn find_jar(&self, height: Height) -> Result<Option<Jar>, StaticFileError> {
        if let Some(jar) = self.jar_for(height.0) {
            return Ok(Some(jar));
        }
        if !self.writable && self.tip().is_none_or(|tip| height > tip) {
            let rescanned = Self::scan(&self.dir, false)?;
            *self
                .jars
                .write()
                .expect("static file lock is never poisoned") = rescanned;
            return Ok(self.jar_for(height.0));
        }
        Ok(None)
    }

    /// Returns the raw transactions of block `height`, or `None` if it isn't in static files.
    pub fn block_transactions(
        &self,
        height: Height,
    ) -> Result<Option<Vec<RawBytes>>, StaticFileError> {
        let Some(jar) = self.find_jar(height)? else {
            return Ok(None);
        };
        let Some(rows) = jar.block_rows(height.0)? else {
            return Ok(None);
        };
        if rows.is_empty() {
            return Err(corrupt(
                &jar.blk_path(),
                format!("block {height:?} has no transactions"),
            ));
        }
        jar.read_rows(rows).map(Some)
    }

    /// Returns raw transaction `index` of block `height`, or `None` if it isn't in static files.
    pub fn transaction(
        &self,
        height: Height,
        index: usize,
    ) -> Result<Option<RawBytes>, StaticFileError> {
        let Some(jar) = self.find_jar(height)? else {
            return Ok(None);
        };
        let Some(rows) = jar.block_rows(height.0)? else {
            return Ok(None);
        };
        // Transaction indexes fit in a `u64`.
        let Some(row) = rows
            .start
            .checked_add(index as u64)
            .filter(|row| *row < rows.end)
        else {
            return Ok(None);
        };
        Ok(jar.read_rows(row..row + 1)?.pop())
    }

    /// Returns `true` if block `height` is in static files.
    pub fn contains(&self, height: Height) -> Result<bool, StaticFileError> {
        Ok(self.find_jar(height)?.is_some())
    }

    /// Appends `blocks` above the static file tip and commits them.
    ///
    /// Heights must increase. A block that doesn't follow the last jar starts a
    /// new jar, and so does a block past the end of the last jar's range.
    pub fn append_blocks(&self, blocks: &[(Height, Vec<RawBytes>)]) -> Result<(), StaticFileError> {
        assert!(
            self.writable,
            "only a writable database appends static files"
        );
        let _writer = self
            .writer
            .lock()
            .expect("static file lock is never poisoned");

        let mut tip = self.tip();
        let mut target: Option<Jar> = self.last_jar();
        let mut pending: Vec<&[RawBytes]> = Vec::new();

        for (height, transactions) in blocks {
            if tip.is_some_and(|tip| *height <= tip) {
                return Err(StaticFileError::OutOfOrder {
                    height: *height,
                    tip,
                });
            }
            let next = target.as_ref().and_then(|jar| {
                // Pending blocks follow the jar's committed blocks.
                // Pending counts are bounded by the jar's range, so they fit in a `u32`.
                jar.next_block()
                    .map(|next| next.checked_add(pending.len() as u32))
            });
            let fits = next.flatten().is_some_and(|next| {
                next == height.0
                    && target
                        .as_ref()
                        .is_some_and(|jar| next <= jar.config.expected_end)
            });

            if !fits {
                if let Some(jar) = target.take() {
                    if !pending.is_empty() {
                        self.insert(jar.commit(&pending)?);
                        pending.clear();
                    }
                }
                target = Some(Jar::create(&self.dir, height.0, self.range_end(*height))?);
            }

            pending.push(transactions);
            tip = Some(*height);
        }

        if let Some(jar) = target {
            if !pending.is_empty() {
                self.insert(jar.commit(&pending)?);
            }
        }

        Ok(())
    }

    /// Returns the last committed jar.
    fn last_jar(&self) -> Option<Jar> {
        let jars = self
            .jars
            .read()
            .expect("static file lock is never poisoned");
        jars.values().next_back().cloned()
    }

    /// Replaces or adds a committed jar.
    fn insert(&self, jar: Jar) {
        let mut jars = self
            .jars
            .write()
            .expect("static file lock is never poisoned");
        jars.insert(jar.config.first_block, jar);
    }

    /// Returns the last block of the fixed range that holds `height`.
    fn range_end(&self, height: Height) -> u32 {
        let blocks_per_file = u64::from(self.blocks_per_file);
        let end = (u64::from(height.0) / blocks_per_file + 1) * blocks_per_file - 1;
        u32::try_from(end).unwrap_or(u32::MAX)
    }

    /// Removes every block above `height`, or every block if `height` is `None`.
    ///
    /// Like reth's unwind, this is how the database wins when the static files
    /// are ahead of it, and how rollback removes static file blocks.
    pub fn truncate_above(&self, height: Option<Height>) -> Result<(), StaticFileError> {
        assert!(
            self.writable,
            "only a writable database truncates static files"
        );
        let _writer = self
            .writer
            .lock()
            .expect("static file lock is never poisoned");

        let jars: Vec<Jar> = self
            .jars
            .read()
            .expect("static file lock is never poisoned")
            .values()
            .rev()
            .cloned()
            .collect();

        for jar in jars {
            let Some(last) = jar.config.last_block() else {
                continue;
            };
            if height.is_some_and(|height| last <= height.0) {
                break;
            }
            let first = jar.config.first_block;
            match height {
                Some(height) if height.0 >= first => {
                    self.insert(jar.truncate_after(height.0)?);
                }
                _ => {
                    delete_jar_files(&jar.files.base)?;
                    self.jars
                        .write()
                        .expect("static file lock is never poisoned")
                        .remove(&first);
                }
            }
        }

        Ok(())
    }

    /// Deletes whole jars whose blocks are all below `height`.
    ///
    /// Like reth, pruning only deletes whole files. Pruned nodes size their
    /// files with [`blocks_per_file`], so the blocks this keeps are bounded.
    pub fn delete_below(&self, height: Height) -> Result<(), StaticFileError> {
        assert!(
            self.writable,
            "only a writable database deletes static files"
        );
        let _writer = self
            .writer
            .lock()
            .expect("static file lock is never poisoned");

        let jars: Vec<Jar> = self
            .jars
            .read()
            .expect("static file lock is never poisoned")
            .values()
            .cloned()
            .collect();

        for jar in jars {
            if jar.config.last_block().is_none_or(|last| last >= height.0) {
                break;
            }
            delete_jar_files(&jar.files.base)?;
            self.jars
                .write()
                .expect("static file lock is never poisoned")
                .remove(&jar.config.first_block);
        }

        Ok(())
    }
}
