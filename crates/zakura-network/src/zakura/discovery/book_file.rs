//! Durable storage for the discovery book, so a restarted node can dial the
//! peers it already knew instead of starting again from its bootstrap peers.
//!
//! The cache stores this node's observations about past dials. Every loaded entry goes
//! back through the book's import path, which re-verifies the record's
//! signature, network, chain and expiry before the dialer can use it. Static
//! entries are stored as ordinary records: only the configured bootstrap peers
//! grant static trust, but a restart still remembers their signed records.

use std::{
    fs::File,
    io::{self, Read},
    path::PathBuf,
    time::Duration,
};

use tracing::{info, warn};
use zakura_chain::common::atomic_write;
use zakura_node_services::task::spawn_erased_named;
use zakura_rt::{
    sync::CancellationToken,
    task::{spawn_blocking_named, JoinHandle},
    time::sleep,
};

use super::protocol::{current_unix_secs, ZakuraDiscoveryHandle, ZakuraDiscoveryPersistedEntry};
use super::wire::{
    decode_persisted_entries, encode_persisted_entries, MAX_BOOK_FILE_BYTES,
    MAX_PERSISTED_DISCOVERY_ENTRIES,
};
use crate::Config;

/// How often the running node rewrites the book file. Shutdown also writes it.
pub(crate) const DISCOVERY_BOOK_SAVE_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// The file that holds this node's saved discovery book.
#[derive(Clone, Debug)]
pub(super) struct DiscoveryBookFile {
    path: PathBuf,
}

impl DiscoveryBookFile {
    pub(super) fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// The stored entries, or none when no file was stored. An unreadable or
    /// undecodable file is logged and reads as empty, so discovery starts from
    /// the bootstrap peers as it did before this file existed.
    pub(super) fn load(&self) -> Vec<ZakuraDiscoveryPersistedEntry> {
        let bytes = match File::open(&self.path).and_then(read_book_bytes) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Vec::new(),
            Err(error) => {
                warn!(?error, path = ?self.path, "could not read the saved discovery book");
                return Vec::new();
            }
        };
        decode_persisted_entries(&bytes, MAX_PERSISTED_DISCOVERY_ENTRIES).unwrap_or_else(|error| {
            warn!(?error, path = ?self.path, "ignoring an undecodable saved discovery book");
            Vec::new()
        })
    }

    /// Atomically replaces the stored book with the most useful `entries`.
    ///
    /// Keeps the previous file when no entry is worth storing, so a short run
    /// that learned no peers does not erase the peers an earlier run found.
    pub(super) fn store(
        &self,
        entries: Vec<ZakuraDiscoveryPersistedEntry>,
        now: u64,
    ) -> io::Result<()> {
        let entries = entries_to_persist(entries, now);
        if entries.is_empty() {
            return Ok(());
        }
        let bytes = encode_persisted_entries(&entries, MAX_PERSISTED_DISCOVERY_ENTRIES)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        atomic_write(self.path.clone(), &bytes)?.map(|_| ())
    }
}

/// Reads at most one byte past the largest valid book, including if the file
/// grows while it is being read.
fn read_book_bytes(reader: impl Read) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    // The fixed maximum encoded book size plus one byte fits in u64.
    let limit = (MAX_BOOK_FILE_BYTES + 1) as u64;
    reader.take(limit).read_to_end(&mut bytes)?;
    if bytes.len() > MAX_BOOK_FILE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "discovery book exceeds its byte limit",
        ));
    }
    Ok(bytes)
}

/// Keeps live records, preferring the most recent successful dial, then the most recent sighting.
fn entries_to_persist(
    mut entries: Vec<ZakuraDiscoveryPersistedEntry>,
    now: u64,
) -> Vec<ZakuraDiscoveryPersistedEntry> {
    entries.retain(|entry| entry.record.body.expires_at_unix_secs >= now);
    entries.sort_by(|a, b| {
        b.last_success
            .cmp(&a.last_success)
            .then(b.last_seen.cmp(&a.last_seen))
    });
    entries.truncate(MAX_PERSISTED_DISCOVERY_ENTRIES);
    entries
}

/// Restores this node's configured peer cache before dialing and starts its
/// periodic saver. A disabled cache performs no file I/O and starts no task.
pub(crate) async fn start_discovery_book_cache(
    discovery: &ZakuraDiscoveryHandle,
    config: &Config,
    shutdown: CancellationToken,
) -> Option<JoinHandle<()>> {
    let path = config
        .cache_dir
        .discovery_book_file_path(&config.network, &discovery.local_node_id())?;
    let file = DiscoveryBookFile::new(path);
    load_saved_discovery_book(discovery, &file).await;
    Some(spawn_discovery_book_saver(
        discovery.clone(),
        file,
        shutdown,
    ))
}

/// Imports the saved book into `discovery`, re-verifying every record.
async fn load_saved_discovery_book(discovery: &ZakuraDiscoveryHandle, file: &DiscoveryBookFile) {
    let loader = file.clone();
    let entries = match spawn_blocking_named("discovery_book_load", move || loader.load()).await {
        Ok(entries) => entries,
        Err(error) => {
            warn!(?error, "the saved discovery book load task failed");
            return;
        }
    };
    if entries.is_empty() {
        return;
    }
    let outcome = discovery.import_persisted_entries(entries).await;
    info!(
        attempted = outcome.attempted,
        added = outcome.added,
        rejected = outcome.rejected,
        "loaded the saved Zakura discovery book"
    );
}

/// Saves `discovery`'s book every [`DISCOVERY_BOOK_SAVE_INTERVAL`], and once
/// more when `shutdown` is cancelled, so the next start dials the peers this
/// run knew.
fn spawn_discovery_book_saver(
    discovery: ZakuraDiscoveryHandle,
    file: DiscoveryBookFile,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    spawn_erased_named("discovery_book_saver", async move {
        loop {
            let stopping = zakura_rt::select! {
                _ = shutdown.cancelled() => true,
                _ = sleep(DISCOVERY_BOOK_SAVE_INTERVAL) => false,
            };
            save_discovery_book(&discovery, &file).await;
            if stopping {
                break;
            }
        }
    })
}

async fn save_discovery_book(discovery: &ZakuraDiscoveryHandle, file: &DiscoveryBookFile) {
    let entries = discovery.persisted_entries().await;
    let file = file.clone();
    let path = file.path.clone();
    let now = current_unix_secs();
    match spawn_blocking_named("discovery_book_save", move || file.store(entries, now)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(?error, ?path, "could not save the Zakura discovery book"),
        Err(error) => warn!(?error, "the Zakura discovery book save task failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_discovery_book_stops_reading_at_the_limit() {
        let bytes = vec![0; MAX_BOOK_FILE_BYTES + 100];
        assert_eq!(
            read_book_bytes(&bytes[..MAX_BOOK_FILE_BYTES])
                .unwrap()
                .len(),
            MAX_BOOK_FILE_BYTES
        );
        let mut reader = io::Cursor::new(bytes);
        let error = read_book_bytes(&mut reader).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            usize::try_from(reader.position()).unwrap(),
            MAX_BOOK_FILE_BYTES + 1
        );
    }
}
