//! Durable storage for the discovery book, so a restarted node can dial the
//! peers it already knew instead of starting again from its bootstrap peers.
//!
//! The file is a local cache, not a source of trust. Every loaded entry goes
//! back through the book's import path, which re-verifies the record's
//! signature, network, chain and expiry before the dialer can use it. Static
//! entries are stored as ordinary records: only the configured bootstrap peers
//! grant static trust, but a restart still remembers their signed records.

use std::{io, path::PathBuf, time::Duration};

use tracing::{info, warn};
use zakura_chain::common::atomic_write;
use zakura_node_services::task::spawn_erased_named;
use zakura_rt::{
    sync::CancellationToken,
    task::{spawn_blocking_named, JoinHandle},
    time::sleep,
};

use super::protocol::{ZakuraDiscoveryHandle, ZakuraDiscoveryPersistedEntry};
use super::wire::{decode_persisted_entries, encode_persisted_entries};

/// The most entries the book file holds.
///
/// Loading re-verifies each record's signature on the discovery task, so the
/// cap keeps that work to a few milliseconds while still leaving far more
/// candidates than a node has connection slots.
pub(crate) const MAX_PERSISTED_DISCOVERY_ENTRIES: usize = 256;

/// How often the running node rewrites the book file. Shutdown also writes it.
pub(crate) const DISCOVERY_BOOK_SAVE_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// The file that holds this node's saved discovery book.
#[derive(Clone, Debug)]
pub(crate) struct DiscoveryBookFile {
    path: PathBuf,
}

impl DiscoveryBookFile {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// The stored entries, or none when no file was stored. An unreadable or
    /// undecodable file is logged and reads as empty, so discovery starts from
    /// the bootstrap peers as it did before this file existed.
    pub(crate) fn load(&self) -> Vec<ZakuraDiscoveryPersistedEntry> {
        let bytes = match std::fs::read(&self.path) {
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
    pub(crate) fn store(&self, entries: Vec<ZakuraDiscoveryPersistedEntry>) -> io::Result<()> {
        let entries = entries_to_persist(entries);
        if entries.is_empty() {
            return Ok(());
        }
        let bytes = encode_persisted_entries(&entries, MAX_PERSISTED_DISCOVERY_ENTRIES)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        atomic_write(self.path.clone(), &bytes)?.map(|_| ())
    }
}

/// Keeps the entries most likely to accept a dial: the most recent successful
/// dial first, then the most recently seen record. Static entries become
/// ordinary entries, because static trust comes only from the config.
fn entries_to_persist(
    mut entries: Vec<ZakuraDiscoveryPersistedEntry>,
) -> Vec<ZakuraDiscoveryPersistedEntry> {
    for entry in &mut entries {
        entry.is_static = false;
    }
    entries.sort_by(|a, b| {
        b.last_success
            .cmp(&a.last_success)
            .then(b.last_seen.cmp(&a.last_seen))
    });
    entries.truncate(MAX_PERSISTED_DISCOVERY_ENTRIES);
    entries
}

/// Imports the saved book into `discovery`, re-verifying every record.
pub(crate) async fn load_saved_discovery_book(
    discovery: &ZakuraDiscoveryHandle,
    file: &DiscoveryBookFile,
) {
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
pub(crate) fn spawn_discovery_book_saver(
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
    match spawn_blocking_named("discovery_book_save", move || file.store(entries)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(?error, ?path, "could not save the Zakura discovery book"),
        Err(error) => warn!(?error, "the Zakura discovery book save task failed"),
    }
}
