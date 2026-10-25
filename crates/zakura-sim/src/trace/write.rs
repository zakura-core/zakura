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
    read::{read_bounded, refuse_symlinked_dir, Missing, ReadError},
    BLOBS_DIR, MAX_BLOB_BYTES, MAX_JSON_DEPTH, MAX_LINE_BYTES, MAX_TRACE_BYTES, TRACE_FILE,
};
use crate::{Record, RecordSink};

/// Writes a run's records to `trace.jsonl`, `meta` first.
///
/// Each line goes to the file in one write as soon as it's made, so a crash
/// leaves a readable prefix. It keeps to the reader's limits: a record whose
/// line is over [`MAX_LINE_BYTES`], or that would take the file past
/// [`MAX_TRACE_BYTES`], isn't written. That record and every later one are
/// dropped, the file stays a readable prefix, and [`FileSink::finish`]
/// returns the error.
#[derive(Debug)]
pub struct FileSink {
    file: File,
    /// Bytes written so far.
    written: u64,
    /// The most bytes the file may hold.
    max_bytes: u64,
    /// The first write that failed or was refused. Later records are
    /// dropped.
    error: Option<io::Error>,
}

impl FileSink {
    /// Creates `dir` if needed and starts its `trace.jsonl` with `meta`.
    /// Fails if the file already exists, so a trace is never overwritten, or
    /// if `meta` nests too deep or holds a float (see
    /// [`MetaRecord::too_deep_field`] and [`MetaRecord::float_field`]).
    pub fn create(dir: &Path, meta: &MetaRecord) -> io::Result<Self> {
        Self::create_limited(dir, meta, MAX_TRACE_BYTES)
    }

    /// [`FileSink::create`], with the file limited to `max_bytes`.
    pub(crate) fn create_limited(
        dir: &Path,
        meta: &MetaRecord,
        max_bytes: u64,
    ) -> io::Result<Self> {
        if let Some(field) = meta.too_deep_field() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("meta.{field} nests more than {MAX_JSON_DEPTH} levels deep"),
            ));
        }
        if let Some(field) = meta.float_field() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("meta.{field} holds a float, and traces hold only integers"),
            ));
        }
        // An oversized meta fails here, before the file exists.
        let meta = line(&Record::Meta(Box::new(meta.clone())));
        check_line(&meta, 0, max_bytes)?;
        fs::create_dir_all(dir)?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dir.join(TRACE_FILE))?;
        let mut sink = FileSink {
            file,
            written: 0,
            max_bytes,
            error: None,
        };
        sink.write(&meta)?;
        Ok(sink)
    }

    /// Syncs the file and returns the first write error, if any.
    pub fn finish(self) -> io::Result<()> {
        match self.error {
            Some(error) => Err(error),
            None => self.file.sync_all(),
        }
    }

    /// Writes `line` in one call, if it fits the limits.
    fn write(&mut self, line: &str) -> io::Result<()> {
        check_line(line, self.written, self.max_bytes)?;
        self.file.write_all(line.as_bytes())?;
        // A usize always fits in a u64 on supported targets.
        self.written += line.len() as u64;
        Ok(())
    }
}

impl RecordSink for FileSink {
    fn record(&mut self, record: Record) {
        if self.error.is_none() {
            self.error = self.write(&line(&record)).err();
        }
    }
}

/// `record`'s line, with its newline.
fn line(record: &Record) -> String {
    let mut line = record.to_line();
    line.push('\n');
    line
}

/// Fails if `line` is over [`MAX_LINE_BYTES`], or if writing it after
/// `written` bytes would take the file past `max_bytes`.
fn check_line(line: &str, written: u64, max_bytes: u64) -> io::Result<()> {
    if line.len() > MAX_LINE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("a trace line holds at most {MAX_LINE_BYTES} bytes"),
        ));
    }
    // A usize always fits in a u64 on supported targets.
    if written.saturating_add(line.len() as u64) > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::FileTooLarge,
            format!("a trace holds at most {max_bytes} bytes"),
        ));
    }
    Ok(())
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
    /// refuse, or if the directory is itself a symlink.
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
        refuse_symlinked_dir(dir).map_err(|error| io::Error::other(error.to_string()))?;
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
    /// regular file, or is over [`MAX_BLOB_BYTES`], is refused, and so is a
    /// directory that is itself a symlink.
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
            refuse_symlinked_dir(dir)?;
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

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;
    use crate::{
        plan::By,
        record::{InitRecord, StepRecord},
        trace::{
            meta::{BudgetRecord, SchedulerRecord, RNG},
            BuildInfo, TraceReader,
        },
        RunConfig,
    };

    /// A small `meta` record.
    fn meta() -> MetaRecord {
        let config = RunConfig::default();
        MetaRecord {
            format: crate::record::FORMAT_VERSION,
            contract: crate::record::CONTRACT_VERSION,
            generator: 1,
            build: BuildInfo::current(),
            sim: "test/limits".into(),
            scenario: json!({}),
            assumptions: Vec::new(),
            scheduler: SchedulerRecord::from(config.scheduler),
            budget: BudgetRecord::from(&config),
            rng: RNG.into(),
            fixtures: Vec::new(),
            properties: Vec::new(),
            input: None,
            derived_from: None,
        }
    }

    /// Step `step`, a forced pick of time.
    fn step(step: u64) -> Record {
        Record::Step(StepRecord {
            step,
            pick: format!("advance-time:{step}"),
            key: None,
            options: 1,
            by: By::Forced,
            offset: None,
            draws: Vec::new(),
            obs: None,
        })
    }

    #[test]
    fn a_full_trace_stops_at_a_readable_prefix() {
        let dir = TempDir::new().unwrap();
        let len = |record: &Record| u64::try_from(line(record).len()).unwrap();
        let meta_len = len(&Record::Meta(Box::new(meta())));
        let init_len = len(&Record::Init(InitRecord::default()));
        let step_len = len(&step(1));
        // Room for meta, init and exactly three steps.
        let max = meta_len + init_len + 3 * step_len;
        let mut sink = FileSink::create_limited(dir.path(), &meta(), max).unwrap();
        sink.record(Record::Init(InitRecord::default()));
        for n in 1..=5 {
            sink.record(step(n));
        }
        let error = sink.finish().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::FileTooLarge);

        let bytes = fs::read(dir.path().join(TRACE_FILE)).unwrap();
        assert_eq!(u64::try_from(bytes.len()).unwrap(), max);
        let prefix = TraceReader::open_prefix(dir.path()).unwrap();
        assert_eq!(prefix.steps.len(), 3);
        assert!(!prefix.is_complete());
    }
}
