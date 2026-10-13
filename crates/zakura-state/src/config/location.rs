//! Locates the state database on disk, moves a previous-major database into place, and deletes
//! stale databases.
//!
//! A database lives in `{cache_dir}/{db_kind}/v{major}/{network}`. The format major version is
//! frozen, so a database moves only when it comes from a release that used an older major
//! directory. Every code path that locates a database calls [`find_database`] or
//! [`adopt_previous_major_database`], so they all agree on its location.

use std::{
    fs::{self, DirEntry, File, OpenOptions},
    io,
    path::{Path, PathBuf},
};

use tokio::task::{spawn_blocking, JoinHandle};
use tracing::Span;

use zakura_chain::parameters::Network;

use crate::{
    constants::{MIN_UPGRADABLE_DATABASE_FORMAT_VERSION, STATE_DATABASE_KIND},
    state_database_format_version_in_code, Config, StateInitError,
};

/// Returns the directory of the existing database for `major_version`.
///
/// Looks in the `major_version` directory first, then in older major directories down to the
/// oldest upgradable format. Returns `None` for an ephemeral config, or when no directory exists.
pub(crate) fn find_database(
    config: &Config,
    db_kind: &str,
    major_version: u64,
    network: &Network,
) -> Option<PathBuf> {
    if config.ephemeral {
        return None;
    }

    (MIN_UPGRADABLE_DATABASE_FORMAT_VERSION.major..=major_version)
        .rev()
        .map(|major| config.db_path(db_kind, major, network))
        .find(|path| path.exists())
}

/// Moves the newest previous-major database into the `major_version` directory.
///
/// Read-write startup calls this before it opens the database. It does nothing when the
/// `major_version` directory already holds a database, or when no previous-major database exists.
/// It holds the previous database's RocksDB lock during the move, so it never moves a database
/// that a running older node has open.
///
/// # Errors
///
/// Returns an error, instead of letting startup create an empty database, when:
/// - another process has the previous database open,
/// - the previous database resolves to a path outside the cache directory, or
/// - the move fails.
pub(crate) fn adopt_previous_major_database(
    config: &Config,
    db_kind: &str,
    major_version: u64,
    network: &Network,
) -> Result<(), StateInitError> {
    if config.ephemeral {
        return Ok(());
    }

    let current_path = config.db_path(db_kind, major_version, network);
    if is_database(&current_path) {
        return Ok(());
    }

    let Some(previous_path) = (MIN_UPGRADABLE_DATABASE_FORMAT_VERSION.major..major_version)
        .rev()
        .map(|major| config.db_path(db_kind, major, network))
        .find(|path| is_database(path))
    else {
        return Ok(());
    };

    if !is_inside_cache_dir(config, &previous_path) {
        return Err(StateInitError::PreviousDatabaseOutsideCacheDir {
            path: previous_path,
            current_path,
        });
    }

    let lock = DatabaseLock::try_acquire(&previous_path).map_err(|source| {
        StateInitError::PreviousDatabaseInUse {
            path: previous_path.clone(),
            source,
        }
    })?;

    move_database(&previous_path, &current_path).map_err(|source| {
        StateInitError::PreviousDatabaseMove {
            from: previous_path.clone(),
            to: current_path.clone(),
            source,
        }
    })?;

    drop(lock);

    info!(
        from = ?previous_path,
        to = ?current_path,
        "moved the previous-major state database"
    );

    if let Some(previous_major_dir) = previous_path.parent() {
        remove_empty_dir(previous_major_dir);
    }

    Ok(())
}

/// Returns `true` if `path` holds a RocksDB database with at least one column family.
fn is_database(path: &Path) -> bool {
    rocksdb::DB::list_cf(&rocksdb::Options::default(), path).is_ok_and(|cfs| !cfs.is_empty())
}

/// Returns `true` if `path` resolves to a location inside the cache directory.
///
/// A symlink can point a database directory elsewhere, and that location might hold other files.
/// Zakura does not guard against an attacker who replaces the path between this check and its
/// use, because Zakura should not run with elevated privileges.
fn is_inside_cache_dir(config: &Config, path: &Path) -> bool {
    match (fs::canonicalize(&config.cache_dir), fs::canonicalize(path)) {
        (Ok(cache_dir), Ok(path)) => path.starts_with(cache_dir),
        _ => false,
    }
}

/// Renames the database directory `from` to `to`, creating the parent of `to` first.
fn move_database(from: &Path, to: &Path) -> io::Result<()> {
    fs::create_dir_all(
        to.parent()
            .expect("database paths have a major version parent"),
    )?;

    // An interrupted earlier startup can leave an empty directory at the destination. Removing it
    // fails if it is not empty, and that error stops the move.
    if to.exists() {
        fs::remove_dir(to)?;
    }

    fs::rename(from, to)
}

/// Removes `dir` if it is empty, and logs any other failure.
fn remove_empty_dir(dir: &Path) {
    let is_empty = fs::read_dir(dir).is_ok_and(|mut entries| entries.next().is_none());
    if !is_empty {
        return;
    }

    match fs::remove_dir(dir) {
        Ok(()) => info!(?dir, "removed empty old state directory"),
        Err(error) => warn!(?dir, %error, "could not remove empty old state directory"),
    }
}

/// Holds the RocksDB lock of a database that this process has not opened.
///
/// RocksDB locks the database's `LOCK` file. On Unix, it takes an `fcntl` record lock, and on
/// Windows, it keeps the file open without sharing. This guard takes the same lock, so acquiring
/// it fails while any RocksDB instance has the database open.
struct DatabaseLock {
    /// The open `LOCK` file, which holds the lock until this guard drops.
    _file: Option<File>,
}

impl DatabaseLock {
    /// Takes the RocksDB lock of the database at `db_path`.
    #[cfg(unix)]
    #[allow(unsafe_code)]
    fn try_acquire(db_path: &Path) -> io::Result<Self> {
        use std::os::fd::AsRawFd;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(db_path.join("LOCK"))?;

        // On Linux, an open file description lock conflicts with RocksDB's record lock even when
        // RocksDB runs in this process, and closing this file releases only this lock. Other Unix
        // systems lack these locks, and this process never opens the database it locks.
        #[cfg(target_os = "linux")]
        let command = libc::F_OFD_SETLK;
        #[cfg(not(target_os = "linux"))]
        let command = libc::F_SETLK;

        // SAFETY: `flock` is a plain C struct, so all-zero bytes are a valid value.
        let mut lock: libc::flock = unsafe { std::mem::zeroed() };
        // `F_WRLCK` and `SEEK_SET` are small constants that fit the `flock` field types on every
        // Unix platform.
        lock.l_type = libc::F_WRLCK as _;
        lock.l_whence = libc::SEEK_SET as _;
        // A zero start and length lock the whole file, as RocksDB does.

        // SAFETY: the file descriptor stays open during the call, and `lock` is a valid `flock`.
        if unsafe { libc::fcntl(file.as_raw_fd(), command, &lock) } == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self { _file: Some(file) })
    }

    /// Checks that no RocksDB instance has the database at `db_path` open.
    ///
    /// Windows refuses to move or delete a directory that contains an open file, so the guard
    /// does not need to hold the file. Opening `LOCK` without sharing fails while RocksDB has it
    /// open.
    #[cfg(windows)]
    fn try_acquire(db_path: &Path) -> io::Result<Self> {
        use std::os::windows::fs::OpenOptionsExt;

        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .share_mode(0)
            .open(db_path.join("LOCK"))?;

        Ok(Self { _file: None })
    }

    /// Other platforms have no lock to check.
    #[cfg(not(any(unix, windows)))]
    fn try_acquire(_db_path: &Path) -> io::Result<Self> {
        Ok(Self { _file: None })
    }
}

/// Spawns a task that deletes stale state databases.
///
/// See [`check_and_delete_old_databases`] for details.
pub fn check_and_delete_old_state_databases(config: &Config, network: &Network) -> JoinHandle<()> {
    check_and_delete_old_databases(
        config,
        STATE_DATABASE_KIND,
        state_database_format_version_in_code().major,
        network,
    )
}

/// Spawns a task that deletes stale databases for `network`.
///
/// Once the `major_version` database exists, every `network` database in an older major
/// directory is stale. The task deletes each one that no other process has open, and keeps the
/// rest. It deletes nothing for an ephemeral config, or when `delete_old_database` is unset.
pub fn check_and_delete_old_databases(
    config: &Config,
    db_kind: impl AsRef<str>,
    major_version: u64,
    network: &Network,
) -> JoinHandle<()> {
    let current_span = Span::current();
    let config = config.clone();
    let db_kind = db_kind.as_ref().to_string();
    let network = network.clone();

    spawn_blocking(move || {
        current_span.in_scope(|| {
            delete_old_databases(&config, &db_kind, major_version, &network);
            info!("finished old database version cleanup task");
        })
    })
}

/// Deletes stale databases for `network`.
///
/// See [`check_and_delete_old_databases`] for details.
pub(crate) fn delete_old_databases(
    config: &Config,
    db_kind: &str,
    major_version: u64,
    network: &Network,
) {
    if config.ephemeral || !config.delete_old_database {
        return;
    }

    // A failed move leaves an older database as the only copy, so keep every older database until
    // the current one exists.
    if !is_database(&config.db_path(db_kind, major_version, network)) {
        return;
    }

    info!(db_kind, "checking for old database versions");

    let Ok(major_dirs) = fs::read_dir(config.cache_dir.join(db_kind)) else {
        return;
    };

    for major_dir in major_dirs.flatten() {
        if let Some(deleted_db) = delete_old_database(config, major_version, network, &major_dir) {
            info!(?deleted_db, "deleted outdated {db_kind} database directory");
        }
    }
}

/// Deletes the `network` database in `major_dir` if `major_dir` is an older major directory and
/// no other process has the database open.
///
/// Returns the deleted path.
fn delete_old_database(
    config: &Config,
    major_version: u64,
    network: &Network,
    major_dir: &DirEntry,
) -> Option<PathBuf> {
    let dir_major_version = major_dir
        .file_name()
        .to_str()?
        .strip_prefix('v')?
        .parse::<u64>()
        .ok()?;

    if dir_major_version >= major_version {
        return None;
    }

    let outdated_path = major_dir.path().join(network.lowercase_name());
    if !outdated_path.is_dir() {
        return None;
    }

    if !is_inside_cache_dir(config, &outdated_path) {
        info!(
            skipped_path = ?outdated_path,
            cache_path = ?config.cache_dir,
            "skipped cleanup of outdated state directory: state is outside cache directory",
        );

        return None;
    }

    let lock = match DatabaseLock::try_acquire(&outdated_path) {
        Ok(lock) => lock,
        Err(error) => {
            info!(
                skipped_path = ?outdated_path,
                %error,
                "skipped cleanup of outdated state directory: another process has it open",
            );

            return None;
        }
    };

    fs::remove_dir_all(&outdated_path).ok()?;
    drop(lock);
    remove_empty_dir(&major_dir.path());

    Some(outdated_path)
}

#[cfg(test)]
mod tests;
