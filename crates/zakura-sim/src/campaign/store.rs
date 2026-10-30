//! The store's layout, its digests, and the writes that never leave a
//! partial file under a final name.

use std::{
    fmt,
    fs::{self, File, OpenOptions, TryLockError},
    io::{self, BufReader, Write},
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::{seed::sha256, trace::meta::hex};

/// The version of every record in the store: build records, snapshot
/// indexes, manifests, attempts and end records. Bump it when a record's
/// fields change meaning.
pub const STORE_VERSION: u32 = 1;

/// The directory of build records.
const BUILDS_DIR: &str = "builds";
/// The directory of snapshots.
const SNAPSHOTS_DIR: &str = "snapshots";
/// The directory of manifests.
const MANIFESTS_DIR: &str = "manifests";
/// The directory of runs.
const RUNS_DIR: &str = "runs";
/// The file a run's supervisor locks while it runs.
const LOCK_FILE: &str = "lock";
/// The file `start` and `resume` lock while they pick a parent and write a
/// manifest, so two of them can't both continue one run.
const STORE_LOCK_FILE: &str = "store.lock";

/// How long [`lock`] waits for a lock that another process holds.
const LOCK_WAIT: Duration = Duration::from_secs(1);

/// How often [`lock`] retries.
const LOCK_RETRY: Duration = Duration::from_millis(10);

/// The temporary file's number within this process.
static NEXT_PARTIAL: AtomicU64 = AtomicU64::new(0);

/// A SHA-256 digest in lowercase hex: what names every file in the store.
///
/// It parses only from 64 lowercase hex digits, so a digest read from a
/// record or the command line can't name a path outside its directory.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Digest(String);

impl Digest {
    /// The digest of `bytes`.
    pub fn of(bytes: &[u8]) -> Digest {
        Digest(sha256(bytes))
    }

    /// The digest of the file at `path`, read in pieces, so a large file
    /// costs no memory.
    pub fn of_file(path: &Path) -> io::Result<Digest> {
        let mut hasher = Sha256::default();
        io::copy(&mut File::open(path)?, &mut hasher)?;
        Ok(Digest(hex(&sha2::Digest::finalize(hasher))))
    }

    /// The hex digits.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Digest {
    type Error = String;

    /// Accepts exactly 64 lowercase hex digits.
    fn try_from(text: String) -> Result<Self, Self::Error> {
        if text.len() == 64
            && text
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            Ok(Digest(text))
        } else {
            Err(format!("`{text}` isn't a SHA-256 digest in lowercase hex"))
        }
    }
}

impl std::str::FromStr for Digest {
    type Err = String;

    /// [`Digest::try_from`].
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Digest::try_from(text.to_owned())
    }
}

impl From<Digest> for String {
    /// The hex digits.
    fn from(digest: Digest) -> String {
        digest.0
    }
}

impl fmt::Display for Digest {
    /// The hex digits.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Why a campaign command failed.
#[derive(Debug)]
pub enum CampaignError {
    /// A file operation failed.
    Io {
        /// What the command was doing.
        context: String,
        /// The error.
        error: io::Error,
    },
    /// A record in the store doesn't match its digest, doesn't parse, or
    /// disagrees with the records that name it.
    Corrupt {
        /// The file.
        path: PathBuf,
        /// What's wrong with it.
        reason: String,
    },
    /// The command can't go on, and changed nothing in the store.
    Refused(String),
}

impl CampaignError {
    /// An [`CampaignError::Io`] error with `context`.
    pub(super) fn io(context: impl Into<String>) -> impl FnOnce(io::Error) -> CampaignError {
        let context = context.into();
        move |error| CampaignError::Io { context, error }
    }

    /// A [`CampaignError::Corrupt`] error for `path`.
    pub(super) fn corrupt(path: &Path, reason: impl Into<String>) -> CampaignError {
        CampaignError::Corrupt {
            path: path.to_owned(),
            reason: reason.into(),
        }
    }
}

impl fmt::Display for CampaignError {
    /// What failed, and where.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CampaignError::Io { context, error } => write!(f, "{context}: {error}"),
            CampaignError::Corrupt { path, reason } => {
                write!(f, "{} is corrupt: {reason}", path.display())
            }
            CampaignError::Refused(reason) => f.write_str(reason),
        }
    }
}

impl std::error::Error for CampaignError {}

/// Whether a run is going, and how it ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunState {
    /// A supervisor holds the run's lock.
    Running,
    /// No supervisor holds the lock, and the run has no end record: its
    /// supervisor stopped, or never started.
    Open,
    /// The run has its end record.
    Ended,
}

/// A campaign's store: a directory on durable storage.
#[derive(Clone, Debug)]
pub struct Store {
    /// The store's directory.
    root: PathBuf,
}

impl Store {
    /// The store at `root`, which is created if it doesn't exist.
    pub fn open(root: &Path) -> Result<Store, CampaignError> {
        for dir in [BUILDS_DIR, SNAPSHOTS_DIR, MANIFESTS_DIR, RUNS_DIR] {
            fs::create_dir_all(root.join(dir))
                .map_err(CampaignError::io(format!("creating {}", root.display())))?;
        }
        Ok(Store {
            root: root.to_owned(),
        })
    }

    /// The store's directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The directory of the build of the fuzzer binary `binary`.
    pub fn build_dir(&self, binary: &Digest) -> PathBuf {
        self.root.join(BUILDS_DIR).join(binary.as_str())
    }

    /// The directory of snapshots.
    pub(super) fn snapshots_dir(&self) -> PathBuf {
        self.root.join(SNAPSHOTS_DIR)
    }

    /// The directory of the snapshot `digest`.
    pub fn snapshot_dir(&self, digest: &Digest) -> PathBuf {
        self.snapshots_dir().join(digest.as_str())
    }

    /// The directory of manifests.
    pub(super) fn manifests_dir(&self) -> PathBuf {
        self.root.join(MANIFESTS_DIR)
    }

    /// The file of the manifest `digest`.
    pub fn manifest_path(&self, digest: &Digest) -> PathBuf {
        self.manifests_dir().join(format!("{digest}.json"))
    }

    /// The directory of the run of manifest `digest`.
    pub fn run_dir(&self, digest: &Digest) -> PathBuf {
        self.root.join(RUNS_DIR).join(digest.as_str())
    }

    /// Whether the run of manifest `digest` is going, and whether it ended.
    /// A run whose lock a supervisor holds is running, whatever files its
    /// directory holds.
    pub fn run_state(&self, digest: &Digest) -> Result<RunState, CampaignError> {
        let dir = self.run_dir(digest);
        if dir.exists() && !probe(&dir.join(LOCK_FILE))? {
            return Ok(RunState::Running);
        }
        if dir.join(super::run::END_FILE).exists() {
            return Ok(RunState::Ended);
        }
        Ok(RunState::Open)
    }

    /// Locks the run of manifest `digest` for this process. Refuses if
    /// another process holds the lock.
    pub(super) fn lock_run(&self, digest: &Digest) -> Result<File, CampaignError> {
        let dir = self.run_dir(digest);
        fs::create_dir_all(&dir).map_err(CampaignError::io(format!(
            "creating the run directory {}",
            dir.display()
        )))?;
        lock(&dir.join(LOCK_FILE))?.ok_or_else(|| {
            CampaignError::Refused(format!(
                "run {digest} is still going: another process holds {}",
                dir.join(LOCK_FILE).display()
            ))
        })
    }

    /// Locks the store against another `start` or `resume`. Refuses if
    /// another process holds the lock.
    pub(super) fn lock_store(&self) -> Result<File, CampaignError> {
        let path = self.root.join(STORE_LOCK_FILE);
        lock(&path)?.ok_or_else(|| {
            CampaignError::Refused(format!(
                "another `start` or `resume` holds {}",
                path.display()
            ))
        })
    }
}

/// Takes the exclusive lock on the file at `path`, creating it. `None` if
/// another process holds it for longer than [`LOCK_WAIT`], since a
/// [`probe`] holds it only for a moment.
fn lock(path: &Path) -> Result<Option<File>, CampaignError> {
    let file = open_lock(path)?;
    let deadline = Instant::now() + LOCK_WAIT;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(Some(file)),
            Err(TryLockError::WouldBlock) if Instant::now() < deadline => thread::sleep(LOCK_RETRY),
            Err(TryLockError::WouldBlock) => return Ok(None),
            Err(TryLockError::Error(error)) => {
                return Err(CampaignError::Io {
                    context: format!("locking {}", path.display()),
                    error,
                })
            }
        }
    }
}

/// True if no process holds the exclusive lock on the file at `path`. It
/// takes a shared lock for a moment, which [`lock`] waits out.
fn probe(path: &Path) -> Result<bool, CampaignError> {
    match open_lock(path)?.try_lock_shared() {
        Ok(()) => Ok(true),
        Err(TryLockError::WouldBlock) => Ok(false),
        Err(TryLockError::Error(error)) => Err(CampaignError::Io {
            context: format!("locking {}", path.display()),
            error,
        }),
    }
}

/// The lock file at `path`, created if it doesn't exist.
fn open_lock(path: &Path) -> Result<File, CampaignError> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .map_err(CampaignError::io(format!("opening {}", path.display())))
}

/// A fresh temporary name in `dir`. Every reader skips names that start
/// with `.`.
pub(super) fn partial_path(dir: &Path) -> PathBuf {
    dir.join(format!(
        ".partial-{}-{}",
        process::id(),
        NEXT_PARTIAL.fetch_add(1, Ordering::Relaxed)
    ))
}

/// Writes `bytes` to a new synced file at `path`. Fails if the file exists.
pub(super) fn write_new_synced(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

/// Syncs the directory `dir`, so the names linked or renamed into it last.
pub(super) fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Writes `bytes` at `path` if nothing is there, and returns whether it
/// wrote them. The bytes go to a synced temporary file, which is then
/// hard-linked to `path`, so a reader sees either no file or the whole
/// file, and two writers can't both win.
pub(super) fn write_once(path: &Path, bytes: &[u8]) -> io::Result<bool> {
    let dir = path
        .parent()
        .expect("every store path names a file inside a directory");
    fs::create_dir_all(dir)?;
    let partial = partial_path(dir);
    let linked =
        write_new_synced(&partial, bytes).and_then(|()| match fs::hard_link(&partial, path) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(false),
            Err(error) => Err(error),
        });
    // The write's error matters more than a failed cleanup.
    let _ = fs::remove_file(&partial);
    let wrote = linked?;
    sync_dir(dir)?;
    Ok(wrote)
}

/// Writes `bytes` at `path`, replacing any file there, by a synced
/// temporary file and a rename. For files that only report progress, such
/// as a worker's process ID.
pub(super) fn replace(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let dir = path
        .parent()
        .expect("every store path names a file inside a directory");
    let partial = partial_path(dir);
    let renamed = write_new_synced(&partial, bytes).and_then(|()| fs::rename(&partial, path));
    if renamed.is_err() {
        let _ = fs::remove_file(&partial);
    }
    renamed
}

/// `value` as the store writes a record: pretty JSON with a final newline.
/// Its key order is the struct's field order, so it doesn't depend on
/// `serde_json`'s `preserve_order`.
pub(super) fn record_bytes(value: &impl Serialize) -> Vec<u8> {
    let mut bytes =
        serde_json::to_vec_pretty(value).expect("store records serialize to JSON without error");
    bytes.push(b'\n');
    bytes
}

/// Reads the record at `path`, and checks its digest first if `digest`
/// names it. Neither step holds the file's bytes in memory, so a file that
/// doesn't match or doesn't parse costs no more memory than its records.
pub(super) fn read_record<T: for<'de> Deserialize<'de>>(
    path: &Path,
    digest: Option<&Digest>,
) -> Result<T, CampaignError> {
    let io = CampaignError::io(format!("reading {}", path.display()));
    if let Some(digest) = digest {
        let actual = Digest::of_file(path).map_err(io)?;
        if &actual != digest {
            return Err(CampaignError::corrupt(
                path,
                format!("it's named {digest}, and its digest is {actual}"),
            ));
        }
    }
    let file =
        File::open(path).map_err(CampaignError::io(format!("reading {}", path.display())))?;
    serde_json::from_reader(BufReader::new(file))
        .map_err(|error| CampaignError::corrupt(path, error.to_string()))
}

/// Seconds since the Unix epoch.
pub(super) fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A digest parses only from 64 lowercase hex digits.
    #[test]
    fn digests_parse_only_from_lowercase_hex() {
        let digest = Digest::of(b"x");
        assert_eq!(digest.as_str().parse::<Digest>(), Ok(digest.clone()));
        assert!(digest.as_str().to_uppercase().parse::<Digest>().is_err());
        assert!("../x".parse::<Digest>().is_err());
        assert!(digest.as_str()[1..].parse::<Digest>().is_err());
    }

    /// A file's digest is the digest of its bytes.
    #[test]
    fn file_digests_match_byte_digests() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("file");
        let bytes: Vec<u8> = (0..200_000u32).map(|n| n as u8).collect();
        fs::write(&path, &bytes).unwrap();
        assert_eq!(Digest::of_file(&path).unwrap(), Digest::of(&bytes));
    }
}
