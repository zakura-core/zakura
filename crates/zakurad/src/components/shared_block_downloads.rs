//! One legacy block download per block hash, shared by the syncer and the gossip queue.
//!
//! The syncer and the gossip queue discover blocks independently, and each keeps its own
//! per-hash queue. Without coordination, a block that both discover near the tip crosses the
//! wire twice. [`SharedBlockDownloads`] lets the first caller start the download and every
//! later caller for the same hash wait for that download instead.
//!
//! Each caller keeps its own admission checks and sends its own verification request: only
//! the network transfer is shared. The reply, including the address of the peer that supplied
//! the body, reaches every caller unchanged, so rejections stay attributable to that peer.
//!
//! An entry lives while any caller still holds its [`Interest`]. Callers hold it until their
//! verification finishes, so a caller that arrives after the transfer but before the block is
//! committed reuses the body instead of downloading it again. The transfer runs in its own
//! task, so it continues while any caller still wants it, and stops once none does.
//!
//! A body that a caller received but did not verify removes its entry immediately, so the
//! next request for that hash downloads it again. A faulty body therefore never blocks a
//! corrected body with the same header hash.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
    time::Instant,
};

use futures::{future::BoxFuture, FutureExt};
use tokio::{sync::watch, task::AbortHandle};

use zakura_chain::block;
use zakura_network as zn;

use crate::BoxError;

/// The most block downloads that can be shared at once. Further callers download on their
/// own, as they would without sharing.
pub const MAX_SHARED_BLOCK_DOWNLOADS: usize = 256;

/// The most decoded block bytes that finished shared downloads keep for later callers.
///
/// The kept bodies are the same allocations the callers verify, not copies, so this bounds how
/// long downloads keep them alive rather than adding memory of its own.
pub const MAX_RETAINED_BODY_BYTES: u64 = 256 * 1024 * 1024;

/// Which queue asked for a block, used to label metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Requester {
    /// The syncer's download queue.
    Sync,
    /// The inbound gossip download queue.
    Gossip,
}

impl Requester {
    fn label(self) -> &'static str {
        match self {
            Requester::Sync => "sync",
            Requester::Gossip => "gossip",
        }
    }
}

/// A failed shared download, as every caller of that download sees it.
#[derive(Clone, Debug)]
enum Failure {
    /// A peer error, kept with its type so callers can still classify `notfound` replies.
    Peer(zn::SharedPeerError),
    /// Any other error, kept as its message.
    Other(Arc<str>),
}

impl Failure {
    fn from_error(error: BoxError) -> Self {
        match error.downcast::<zn::SharedPeerError>() {
            Ok(peer) => Failure::Peer(*peer),
            Err(error) => Failure::Other(error.to_string().into()),
        }
    }

    fn into_error(self) -> BoxError {
        match self {
            Failure::Peer(peer) => peer.into(),
            Failure::Other(message) => SharedDownloadError(message).into(),
        }
    }
}

/// A shared download failed with an error that could not be passed on with its own type.
#[derive(Clone, Debug, thiserror::Error)]
#[error("shared block download failed: {0}")]
pub struct SharedDownloadError(Arc<str>);

type Outcome = Result<zn::Response, Failure>;

struct Entry {
    /// Distinguishes this entry from a later one for the same hash.
    id: u64,
    owner: Requester,
    /// `None` until the transfer finishes.
    result: watch::Sender<Option<Outcome>>,
    /// Callers still holding an [`Interest`] in this entry.
    interests: usize,
    transfer: Option<AbortHandle>,
    /// Decoded bytes of the body kept for later callers; zero while pending.
    retained_bytes: u64,
}

#[derive(Default)]
struct Registry {
    entries: HashMap<block::Hash, Entry>,
    next_id: u64,
    retained_bytes: u64,
}

impl Registry {
    /// Removes the entry for `hash` if it is still entry `id`, stopping a pending transfer.
    fn remove(&mut self, hash: block::Hash, id: u64) -> bool {
        if self.entries.get(&hash).map(|entry| entry.id) != Some(id) {
            return false;
        }
        let entry = self
            .entries
            .remove(&hash)
            .expect("entry exists because its id was just read");
        if let Some(transfer) = entry.transfer {
            // Aborting a finished transfer task has no effect.
            transfer.abort();
        }
        self.retained_bytes -= entry.retained_bytes;
        true
    }

    fn record_gauges(&self) {
        // Precision loss is acceptable for gauges.
        metrics::gauge!("sync.block.shared.entries").set(self.entries.len() as f64);
        metrics::gauge!("sync.block.shared.retained.bytes").set(self.retained_bytes as f64);
    }
}

/// Coordinates legacy block downloads across the syncer and gossip queues of one node.
///
/// Cloning shares the coordinator.
#[derive(Clone)]
pub struct SharedBlockDownloads {
    registry: Arc<Mutex<Registry>>,
    max_entries: usize,
    max_retained_bytes: u64,
}

impl Default for SharedBlockDownloads {
    fn default() -> Self {
        Self::new(MAX_SHARED_BLOCK_DOWNLOADS, MAX_RETAINED_BODY_BYTES)
    }
}

impl std::fmt::Debug for SharedBlockDownloads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let registry = self.registry();
        f.debug_struct("SharedBlockDownloads")
            .field("entries", &registry.entries.len())
            .field("retained_bytes", &registry.retained_bytes)
            .finish()
    }
}

/// What a caller should do to get a block.
#[must_use]
pub enum Claim {
    /// No download of this block is in progress: start one with [`OwnerSlot::start`].
    Owner(OwnerSlot),
    /// A download of this block is in progress or finished: wait for it.
    Joined(Interest),
    /// Sharing is at capacity: download the block without sharing.
    Bypass,
}

impl SharedBlockDownloads {
    /// A coordinator that shares at most `max_entries` downloads and keeps at most
    /// `max_retained_bytes` of finished bodies.
    pub fn new(max_entries: usize, max_retained_bytes: u64) -> Self {
        Self {
            registry: Arc::default(),
            max_entries,
            max_retained_bytes,
        }
    }

    /// Joins the download of `hash` if there is one, and otherwise reserves it for the caller.
    ///
    /// Two callers can never both become the owner of the same hash.
    pub fn claim(&self, hash: block::Hash, requester: Requester) -> Claim {
        let mut registry = self.registry();
        if let Some(entry) = registry.entries.get_mut(&hash) {
            entry.interests += 1;
            let ready = entry.result.borrow().is_some();
            let interest = Interest {
                downloads: self.clone(),
                hash,
                id: entry.id,
                result: entry.result.subscribe(),
                joined: Some((Instant::now(), entry.owner)),
                delivered: false,
                verified: false,
            };
            let result = if ready { "joined_ready" } else { "joined" };
            metrics::counter!("sync.block.shared.claims", "result" => result, "requester" => requester.label())
                .increment(1);
            return Claim::Joined(interest);
        }

        if registry.entries.len() >= self.max_entries {
            metrics::counter!("sync.block.shared.claims", "result" => "bypass", "requester" => requester.label())
                .increment(1);
            return Claim::Bypass;
        }

        let id = registry.next_id;
        registry.next_id += 1;
        let (result, receiver) = watch::channel(None);
        registry.entries.insert(
            hash,
            Entry {
                id,
                owner: requester,
                result,
                interests: 1,
                transfer: None,
                retained_bytes: 0,
            },
        );
        registry.record_gauges();
        metrics::counter!("sync.block.shared.claims", "result" => "owner", "requester" => requester.label())
            .increment(1);
        Claim::Owner(OwnerSlot {
            interest: Some(Interest {
                downloads: self.clone(),
                hash,
                id,
                result: receiver,
                joined: None,
                delivered: false,
                verified: false,
            }),
        })
    }

    fn registry(&self) -> MutexGuard<'_, Registry> {
        self.registry
            .lock()
            .expect("no code panics while holding the shared download registry lock")
    }

    /// Publishes the result of entry `id`'s transfer to its callers.
    fn finish(&self, hash: block::Hash, id: u64, result: Result<zn::Response, BoxError>) {
        let mut registry = self.registry();
        let Some(entry) = registry
            .entries
            .get_mut(&hash)
            .filter(|entry| entry.id == id)
        else {
            // Every caller left before the transfer finished.
            return;
        };
        entry.transfer = None;
        match result {
            Ok(response) => {
                let bytes = response_bytes(&response);
                entry.result.send_replace(Some(Ok(response)));
                if registry.retained_bytes.saturating_add(bytes) > self.max_retained_bytes {
                    // The current callers have the body; later callers download it again.
                    metrics::counter!("sync.block.shared.not_retained.count").increment(1);
                    registry.remove(hash, id);
                } else {
                    entry_mut(&mut registry, hash).retained_bytes = bytes;
                    registry.retained_bytes += bytes;
                }
            }
            Err(error) => {
                metrics::counter!("sync.block.shared.failed.count").increment(1);
                entry
                    .result
                    .send_replace(Some(Err(Failure::from_error(error))));
                // A failed download is never reused: the next caller starts a new one.
                registry.remove(hash, id);
            }
        }
        registry.record_gauges();
    }

    /// Fails entry `id` because its owner never started the transfer.
    fn abandon(&self, hash: block::Hash, id: u64) {
        let mut registry = self.registry();
        if let Some(entry) = registry.entries.get(&hash).filter(|entry| entry.id == id) {
            entry.result.send_replace(Some(Err(Failure::Other(
                "the caller that reserved this download stopped before starting it".into(),
            ))));
            registry.remove(hash, id);
            registry.record_gauges();
        }
    }

    /// Ends one caller's interest in entry `id`, removing the entry when `discard` is set or
    /// when no caller is left.
    fn release(&self, hash: block::Hash, id: u64, discard: bool) {
        let mut registry = self.registry();
        let Some(entry) = registry
            .entries
            .get_mut(&hash)
            .filter(|entry| entry.id == id)
        else {
            return;
        };
        entry.interests -= 1;
        if discard {
            metrics::counter!("sync.block.shared.discarded.count").increment(1);
            registry.remove(hash, id);
        } else if entry.interests == 0 {
            registry.remove(hash, id);
        }
        registry.record_gauges();
    }

    /// The number of shared downloads in progress or kept, for tests.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.registry().entries.len()
    }

    /// Whether the coordinator has no shared downloads, for tests.
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn entry_mut(registry: &mut Registry, hash: block::Hash) -> &mut Entry {
    registry
        .entries
        .get_mut(&hash)
        .expect("the caller holds the lock and just found this entry")
}

/// Decoded bytes of the blocks in a block reply.
fn response_bytes(response: &zn::Response) -> u64 {
    match response {
        zn::Response::Blocks(blocks) => blocks
            .iter()
            .filter_map(|block| block.available())
            .map(|(block, _supplier)| block.decoded_bytes())
            .fold(0, u64::saturating_add),
        _ => 0,
    }
}

/// The right to start the download of a block that nobody else is downloading.
///
/// Dropping it without calling [`OwnerSlot::start`] fails the callers that joined meanwhile.
#[must_use]
pub struct OwnerSlot {
    interest: Option<Interest>,
}

impl OwnerSlot {
    /// Runs `transfer` as the shared download, and returns the owner's interest in it.
    ///
    /// The transfer runs in its own task, so it keeps going while any caller is still
    /// interested, including after the owner stops waiting.
    pub fn start<F, E>(mut self, transfer: F) -> Interest
    where
        F: std::future::Future<Output = Result<zn::Response, E>> + Send + 'static,
        E: Into<BoxError>,
    {
        let interest = self
            .interest
            .take()
            .expect("the interest is only taken here, and start consumes the slot");
        let downloads = interest.downloads.clone();
        let (hash, id) = (interest.hash, interest.id);
        let mut registry = downloads.registry();
        let task = tokio::spawn({
            let downloads = downloads.clone();
            async move {
                let result = transfer.await.map_err(Into::into);
                downloads.finish(hash, id, result);
            }
        });
        // The registry lock is held, so the task can't finish before its handle is stored.
        if let Some(entry) = registry
            .entries
            .get_mut(&hash)
            .filter(|entry| entry.id == id)
        {
            entry.transfer = Some(task.abort_handle());
        }
        drop(registry);
        interest
    }
}

impl Drop for OwnerSlot {
    fn drop(&mut self) {
        if let Some(interest) = &self.interest {
            interest.downloads.abandon(interest.hash, interest.id);
        }
    }
}

/// One caller's interest in a shared download.
///
/// Dropping it ends the interest. A caller that received the body and drops its interest
/// without calling [`Interest::verified`] removes the entry, so the block is downloaded
/// again the next time anyone asks for it.
pub struct Interest {
    downloads: SharedBlockDownloads,
    hash: block::Hash,
    id: u64,
    result: watch::Receiver<Option<Outcome>>,
    /// When and from whom a joining caller joined; `None` for the owner.
    joined: Option<(Instant, Requester)>,
    delivered: bool,
    verified: bool,
}

impl Interest {
    /// Waits for the shared download, and returns the network's reply to it.
    pub async fn response(&mut self) -> Result<zn::Response, BoxError> {
        let outcome = match self.result.wait_for(Option::is_some).await {
            Ok(outcome) => outcome
                .clone()
                .expect("wait_for only returns once the result is set"),
            Err(_) => Err(Failure::Other(
                "the shared download ended without a result".into(),
            )),
        };
        if let Some((joined_at, owner)) = self.joined.take() {
            metrics::histogram!("sync.block.shared.join.wait_seconds", "owner" => owner.label())
                .record(joined_at.elapsed().as_secs_f64());
        }
        self.delivered = outcome.is_ok();
        outcome.map_err(Failure::into_error)
    }

    /// Records that this caller's verification accepted the body.
    pub fn verified(&mut self) {
        self.verified = true;
    }
}

impl Drop for Interest {
    fn drop(&mut self) {
        let discard = self.delivered && !self.verified;
        self.downloads.release(self.hash, self.id, discard);
    }
}

/// A block download, either shared through [`SharedBlockDownloads`] or made alone.
pub enum BlockFetch {
    /// A download shared with other callers.
    Shared(Interest),
    /// A download made without sharing; `None` once awaited.
    Alone(Option<BoxFuture<'static, Result<zn::Response, BoxError>>>),
}

impl BlockFetch {
    /// A download made without sharing.
    pub fn alone<F, E>(transfer: F) -> Self
    where
        F: std::future::Future<Output = Result<zn::Response, E>> + Send + 'static,
        E: Into<BoxError>,
    {
        BlockFetch::Alone(Some(
            transfer.map(|result| result.map_err(Into::into)).boxed(),
        ))
    }

    /// Waits for the network's reply. Call it once.
    pub async fn response(&mut self) -> Result<zn::Response, BoxError> {
        match self {
            BlockFetch::Shared(interest) => interest.response().await,
            BlockFetch::Alone(transfer) => {
                transfer
                    .take()
                    .expect("the response of a block download is only awaited once")
                    .await
            }
        }
    }

    /// Records that the caller's verification accepted the body.
    pub fn verified(&mut self) {
        if let BlockFetch::Shared(interest) = self {
            interest.verified();
        }
    }
}

#[cfg(test)]
mod tests;
