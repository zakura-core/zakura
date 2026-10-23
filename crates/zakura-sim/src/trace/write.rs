//! Writing a trace directory: `trace.jsonl` and its blobs.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use sha2::{Digest, Sha256};

use super::{
    meta::{hex, MetaRecord},
    read::{read_bounded, Missing, ReadError},
    BLOBS_DIR, MAX_BLOB_BYTES, TRACE_FILE,
};
use crate::{Record, RecordSink};

/// Writes a run's records to `trace.jsonl`, `meta` first.
///
/// Each line goes to the file in one write as soon as it's made, so a crash
/// leaves a readable prefix.
#[derive(Debug)]
pub struct FileSink {
    file: File,
    /// The first write that failed. Later records are dropped.
    error: Option<io::Error>,
}

impl FileSink {
    /// Creates `dir` if needed and starts its `trace.jsonl` with `meta`.
    /// Fails if the file already exists, so a trace is never overwritten.
    pub fn create(dir: &Path, meta: &MetaRecord) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dir.join(TRACE_FILE))?;
        let mut sink = FileSink { file, error: None };
        sink.write(&Record::Meta(Box::new(meta.clone())))?;
        Ok(sink)
    }

    /// Syncs the file and returns the first write error, if any.
    pub fn finish(self) -> io::Result<()> {
        match self.error {
            Some(error) => Err(error),
            None => self.file.sync_all(),
        }
    }

    /// Writes `record` and its newline in one call.
    fn write(&mut self, record: &Record) -> io::Result<()> {
        let mut line = record.to_line();
        line.push('\n');
        self.file.write_all(line.as_bytes())
    }
}

impl RecordSink for FileSink {
    fn record(&mut self, record: Record) {
        if self.error.is_none() {
            self.error = self.write(&record).err();
        }
    }
}

/// Where a trace's blobs live: its own `blobs/` first, then the store
/// committed at `crates/zakura-sim/tests/fixtures/blobs/`, so a committed
/// trace can name a large fixture without a copy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobStore {
    /// The directories to look in, in order. [`BlobStore::put`] writes to the
    /// first.
    dirs: Vec<PathBuf>,
}

impl BlobStore {
    /// The store for the trace in `dir`.
    pub fn for_trace(dir: &Path) -> Self {
        BlobStore::new(vec![dir.join(BLOBS_DIR), Self::committed()])
    }

    /// A store that looks in `dirs`, in order.
    pub fn new(dirs: Vec<PathBuf>) -> Self {
        BlobStore { dirs }
    }

    /// The committed store, in this crate's test fixtures.
    pub fn committed() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/blobs")
    }

    /// Writes `bytes` to the first directory, named by their SHA-256, and
    /// returns the digest.
    ///
    /// Older traces may reference the blob, so a regular file that already
    /// holds it is kept untouched. Anything else at that name, such as a
    /// corrupt copy or a symlink, is replaced by renaming a complete temporary
    /// file over it, which never follows a symlink and never leaves a partial
    /// blob. Fails for more than [`MAX_BLOB_BYTES`], which the reader would
    /// refuse.
    pub fn put(&self, bytes: &[u8]) -> io::Result<String> {
        // A usize always fits in a u64 on supported targets.
        if bytes.len() as u64 > MAX_BLOB_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("a blob holds at most {MAX_BLOB_BYTES} bytes"),
            ));
        }
        let sha256 = hex(&Sha256::digest(bytes));
        let dir = self
            .dirs
            .first()
            .ok_or_else(|| io::Error::other("a blob store needs a directory"))?;
        fs::create_dir_all(dir)?;
        let path = dir.join(&sha256);
        if let Ok(Some(existing)) = read_bounded(&path, MAX_BLOB_BYTES) {
            if hex(&Sha256::digest(&existing)) == sha256 {
                return Ok(sha256);
            }
        }
        let (temp, mut file) = temp_file(dir, &sha256)?;
        let written = file
            .write_all(bytes)
            .and_then(|()| file.sync_all())
            .and_then(|()| fs::rename(&temp, &path));
        if written.is_err() {
            let _ = fs::remove_file(&temp);
        }
        written.map(|()| sha256)
    }

    /// The blob named `sha256`, from the first directory that has it. A copy
    /// whose contents don't hash to its name is a [`Missing::DigestMismatch`],
    /// even when a later directory has a good one. A copy that isn't a
    /// regular file, or is over [`MAX_BLOB_BYTES`], is refused.
    pub fn get(&self, sha256: &str) -> Result<Vec<u8>, ReadError> {
        let missing = || {
            ReadError::Missing(Missing::MissingBlob {
                sha256: sha256.to_owned(),
            })
        };
        // A name that isn't a digest could reach outside the store.
        if !is_sha256(sha256) {
            return Err(missing());
        }
        for dir in &self.dirs {
            match read_bounded(&dir.join(sha256), MAX_BLOB_BYTES)? {
                Some(bytes) if hex(&Sha256::digest(&bytes)) == sha256 => return Ok(bytes),
                Some(_) => {
                    return Err(ReadError::Missing(Missing::DigestMismatch {
                        sha256: sha256.to_owned(),
                    }))
                }
                None => {}
            }
        }
        Err(missing())
    }
}

/// Creates a new, uniquely named file in `dir` for the blob `sha256`. Its
/// name starts with `.`, so it can never be taken for a blob.
fn temp_file(dir: &Path, sha256: &str) -> io::Result<(PathBuf, File)> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    loop {
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let temp = dir.join(format!(".{sha256}.{}.{n}.tmp", std::process::id()));
        // create_new refuses an existing path, a symlink included.
        match OpenOptions::new().write(true).create_new(true).open(&temp) {
            Ok(file) => return Ok((temp, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
}

/// True if `text` is a lowercase hex SHA-256.
pub(crate) fn is_sha256(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
