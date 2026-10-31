//! Pace legacy block advertisements using the corresponding block writes.
//!
//! Each broadcast advertises to at most its advert window of peers at a time, and moves on to
//! the next peer once an earlier one finishes. [`AdvertFanout`] sizes that window from the
//! block's size and the measured upload bandwidth. Configured sidecars are advertised first,
//! outside the window. Writing an `inv` uses only that window. A peer takes one of the
//! [`BLOCK_UPLOAD_CONCURRENCY`] upload slots, shared by every broadcast, only once it requests
//! the block, and keeps it through the body write and its drain allowance. A
//! peer waiting for a slot keeps its window entry, so busy uploads still slow each broadcast's
//! fan-out, but they cannot delay another block's first `inv`s.
//!
//! A broadcast for a different block supersedes broadcasts started earlier, unless both heights
//! are known and its block is lower. Superseded broadcasts advertise to no further peers, and drop their adverts that are not yet written. Their written adverts still track, and
//! account for, the uploads that peers request.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use futures::{future::BoxFuture, FutureExt, StreamExt};
use tokio::{
    sync::{mpsc, oneshot, watch, OwnedSemaphorePermit, Semaphore},
    time::{sleep_until, timeout, Instant},
};
use zakura_chain::{block, chain_tip::ChainTip};

use crate::{
    constants::REQUEST_TIMEOUT,
    peer::{advert_fanout::AdvertFanout, MinimumPeerVersion, SharedPeerError},
    peer_registry::PeerRegistryUpdater,
    protocol::external::types::Version,
    AdvertisedBlock, BoxError, Request, Response,
};

/// Pending block adverts per connection. Slow peers can catch up through later tip gossip.
pub(crate) const BLOCK_ADVERT_QUEUE: usize = 4;

/// A block `inv` for one connection to write, separately from its ordinary requests.
///
/// The connection answers `result` once the local write completes, even while another request
/// is awaiting its response. A successful result does not mean the peer received the `inv`.
#[derive(Debug)]
pub(crate) struct BlockAdvert {
    /// The block to advertise.
    pub(crate) hash: block::Hash,
    /// Receives the outcome of the local write.
    pub(crate) result: oneshot::Sender<Result<(), BlockAdvertError>>,
}

/// Why a connection did not write a block advert.
#[derive(Clone, Debug, thiserror::Error)]
pub(crate) enum BlockAdvertError {
    /// The peer was banned, or fell below the minimum protocol version, after the advert was
    /// queued. Nothing was written.
    #[error("peer authorization was revoked before advert delivery")]
    AuthorizationRevoked,

    /// The local write failed, and the connection was closed.
    #[error(transparent)]
    Connection(#[from] SharedPeerError),
}

/// Maximum concurrent block uploads requested through legacy block broadcasts, across broadcasts.
pub(crate) const BLOCK_UPLOAD_CONCURRENCY: usize = 32;
/// Every this many mined-block advertisements, a random peer replaces the next-lowest RTT.
///
/// Peers with the lowest RTT could otherwise hold every slot without completing uploads.
const MINED_RANDOM_INTERVAL: usize = 4;
/// Allow the kernel send queue to drain after the framed block write completes.
const BLOCK_WRITE_GRACE: Duration = Duration::from_millis(750);
/// Peers that already have a block might never request it.
const BLOCK_REQUEST_WAIT: Duration = Duration::from_secs(3);
/// Bound both inbound service work and the subsequent block write.
const BLOCK_UPLOAD_WAIT: Duration = Duration::from_secs(2 * REQUEST_TIMEOUT.as_secs());

#[derive(Clone, Copy, Debug)]
enum UploadState {
    /// Requests before this advertisement's inventory belong to an earlier generation.
    Queued,
    Waiting,
    Requested(Instant),
    Written(Instant),
    Finished(bool),
}

impl UploadState {
    fn start_waiting(&mut self) -> bool {
        let queued = matches!(self, UploadState::Queued);
        if queued {
            *self = UploadState::Waiting;
        }
        queued
    }
}

/// Observers exist only while local broadcasts await this connection's uploads.
#[derive(Clone, Debug, Default)]
pub(crate) struct BlockUploads(Arc<Mutex<HashMap<block::Hash, Weak<watch::Sender<UploadState>>>>>);

impl BlockUploads {
    fn register(&self, hash: block::Hash) -> (watch::Receiver<UploadState>, Option<UploadClaim>) {
        let mut uploads = self.0.lock().expect("block upload mutex is never poisoned");
        uploads.retain(|_, upload| upload.strong_count() > 0);
        if let Some(upload) = uploads.get(&hash).and_then(Weak::upgrade) {
            return (upload.subscribe(), None);
        }
        let (sender, receiver) = watch::channel(UploadState::Queued);
        let sender = Arc::new(sender);
        uploads.insert(hash, Arc::downgrade(&sender));
        (receiver, Some(UploadClaim(sender)))
    }

    fn update(&self, hash: block::Hash, state: UploadState) {
        let uploads = self.0.lock().expect("block upload mutex is never poisoned");
        if let Some(upload) = uploads.get(&hash).and_then(Weak::upgrade) {
            upload.send_if_modified(|current| {
                let replace = matches!(*current, UploadState::Waiting)
                    || matches!(
                        (*current, state),
                        (
                            UploadState::Requested(_),
                            UploadState::Written(_) | UploadState::Finished(_)
                        )
                    );
                if replace {
                    *current = state;
                }
                replace
            });
        }
    }

    /// Start tracking requests once the connection writes this advertisement's inventory.
    pub(crate) fn advertised(&self, hash: block::Hash) {
        let uploads = self.0.lock().expect("block upload mutex is never poisoned");
        if let Some(upload) = uploads.get(&hash).and_then(Weak::upgrade) {
            upload.send_if_modified(UploadState::start_waiting);
        }
    }

    pub(crate) fn requested(&self, hash: block::Hash) {
        self.update(hash, UploadState::Requested(Instant::now()));
    }

    pub(crate) fn written(&self, hash: block::Hash) {
        self.update(hash, UploadState::Written(Instant::now()));
    }

    pub(crate) fn unavailable(&self, hash: block::Hash) {
        self.update(hash, UploadState::Finished(false));
    }

    /// Fail requested hashes that the finished block request did not write.
    ///
    /// The inbound service can truncate a request or fail it without naming each hash.
    pub(crate) fn request_finished<'a>(&self, hashes: impl IntoIterator<Item = &'a block::Hash>) {
        let uploads = self.0.lock().expect("block upload mutex is never poisoned");
        for upload in hashes
            .into_iter()
            .filter_map(|hash| uploads.get(hash).and_then(Weak::upgrade))
        {
            upload.send_if_modified(|state| {
                let requested = matches!(state, UploadState::Requested(_));
                if requested {
                    *state = UploadState::Finished(false);
                }
                requested
            });
        }
    }

    pub(crate) fn disconnected(&self) {
        for upload in self
            .0
            .lock()
            .expect("block upload mutex is never poisoned")
            .values()
            .filter_map(Weak::upgrade)
        {
            upload.send_replace(UploadState::Finished(false));
        }
    }
}

struct UploadClaim(Arc<watch::Sender<UploadState>>);

impl Drop for UploadClaim {
    fn drop(&mut self) {
        self.0.send_if_modified(|state| {
            if matches!(state, UploadState::Finished(_)) {
                false
            } else {
                *state = UploadState::Finished(false);
                true
            }
        });
    }
}

/// A connection's block advert queue, which remains usable while the connection is busy.
#[derive(Clone, Debug)]
pub(crate) struct BlockGossipPeer {
    pub(crate) sender: mpsc::Sender<BlockAdvert>,
    pub(crate) uploads: BlockUploads,
    pub(crate) registry: Option<PeerRegistryUpdater>,
    pub(crate) version: Version,
    /// Configured sidecars may wait for a busy connection to write inventory.
    pub(crate) sidecar: bool,
}

impl BlockGossipPeer {
    pub(crate) fn rtt(&self) -> Option<Duration> {
        self.registry.as_ref().and_then(PeerRegistryUpdater::rtt)
    }

    /// Advertises `broadcast`'s block to this peer, then tracks the upload the peer requests.
    ///
    /// Writing the `inv` takes no upload slot. The peer takes one only once it requests the
    /// block, and keeps it through the block write and the drain allowance. Returns false if the
    /// `inv` was not written, including when a newer broadcast superseded it first.
    pub(crate) async fn advertise(self, broadcast: BlockBroadcast) -> bool {
        let hash = broadcast.hash;
        let mut retried = false;
        let (mut progress, claim) = loop {
            let (mut progress, claim) = self.uploads.register(hash);
            if let Some(claim) = claim {
                break (progress, claim);
            }
            // Early and committed announcements join an existing upload for this peer.
            let succeeded = loop {
                if let UploadState::Finished(succeeded) = *progress.borrow_and_update() {
                    break succeeded;
                }
                if progress.changed().await.is_err() {
                    break false;
                }
            };
            if succeeded || retried || self.sender.is_closed() || broadcast.is_superseded() {
                return succeeded;
            }
            // The joined upload can fail without reaching this peer, for example when early
            // inventory ends in `notfound`. Advertise once more after its owner releases the claim.
            while progress.changed().await.is_ok() {}
            retried = true;
        };
        let (result, written) = oneshot::channel();
        let advert = BlockAdvert { hash, result };
        // A sidecar learns the tip only from inventory, so it waits for its busy connection.
        // Other connections that cannot write inventory promptly must not hold the slot.
        let inventory_wait = if self.sidecar {
            BLOCK_UPLOAD_WAIT
        } else {
            BLOCK_REQUEST_WAIT
        };
        // Dropping `written` makes the connection skip an advert that is still queued.
        let advertised = tokio::select! {
            biased;
            advertised = timeout(inventory_wait, async {
                self.sender.send(advert).await.ok()?;
                written.await.ok()?.ok()
            }) => advertised,
            () = broadcast.superseded() => {
                metrics::counter!("peer.block_gossip.advert_superseded").increment(1);
                return false;
            }
        };
        if !matches!(advertised, Ok(Some(_))) {
            return false;
        }
        // The connection usually marks the write first; senders without a connection rely on this.
        claim.0.send_if_modified(UploadState::start_waiting);

        let request_deadline = Instant::now() + BLOCK_REQUEST_WAIT;
        let mut upload = None;
        let succeeded = loop {
            let state = *progress.borrow_and_update();
            let deadline = match state {
                UploadState::Queued => unreachable!("written inventory leaves the queued state"),
                UploadState::Waiting => request_deadline,
                UploadState::Requested(started) => started + BLOCK_UPLOAD_WAIT,
                UploadState::Written(written) => {
                    // A peer that requested while every slot was held keeps this broadcast's
                    // window entry until it gets one, so busy uploads pace every broadcast's
                    // further fan-out.
                    if upload.is_none() {
                        upload = Some(broadcast.scheduler.upload_slot().await);
                    }
                    sleep_until(written + BLOCK_WRITE_GRACE).await;
                    break true;
                }
                UploadState::Finished(succeeded) => break succeeded,
            };
            let requested = matches!(state, UploadState::Requested(_));
            tokio::select! {
                biased;
                changed = progress.changed() => if changed.is_err() { break false; },
                slot = broadcast.scheduler.upload_slot(), if requested && upload.is_none() => {
                    upload = Some(slot);
                }
                _ = sleep_until(deadline) => {
                    let reason = if matches!(state, UploadState::Waiting) { "no_request" } else { "upload_timeout" };
                    metrics::counter!("peer.block_gossip.slot_timeout", "reason" => reason).increment(1);
                    // Advertisement completion does not promise that the peer requested the block.
                    break true;
                }
            }
        };
        drop(upload);
        claim.0.send_replace(UploadState::Finished(succeeded));
        succeeded
    }
}

/// Shared by every legacy block broadcast: the upload slots, and which broadcast is the newest.
#[derive(Clone, Debug)]
pub(crate) struct BlockGossipScheduler(Arc<SchedulerState>);

#[derive(Debug)]
struct SchedulerState {
    uploads: Arc<Semaphore>,
    upload_capacity: usize,
    /// Sizes each broadcast's advert window.
    fanout: AdvertFanout,
    /// The newest broadcast's generation and block.
    newest: watch::Sender<NewestBroadcast>,
}

/// The broadcast that every earlier broadcast of a different block yields to.
#[derive(Clone, Copy, Debug, Default)]
struct NewestBroadcast {
    /// Starts at zero and increases with each superseding broadcast, so no live broadcast has
    /// generation zero.
    generation: u64,
    hash: Option<block::Hash>,
    height: Option<block::Height>,
}

/// The generation of a broadcast that starts superseded, because a higher block's broadcast
/// already started.
const SUPERSEDED_AT_START: u64 = 0;

impl BlockGossipScheduler {
    /// Returns a scheduler that accounts for at most `upload_capacity` concurrent uploads, and
    /// sizes each broadcast's advert window with `fanout`.
    pub(crate) fn new(upload_capacity: usize, fanout: AdvertFanout) -> Self {
        Self(Arc::new(SchedulerState {
            uploads: Arc::new(Semaphore::new(upload_capacity)),
            upload_capacity,
            fanout,
            newest: watch::channel(NewestBroadcast::default()).0,
        }))
    }

    /// Starts a broadcast of `block`, with an advert window sized for it.
    ///
    /// The broadcast supersedes broadcasts of other blocks started earlier, unless both heights
    /// are known and `block` is lower: then it starts superseded and advertises to no peers.
    /// Blocks at the same height, or with an unknown height, supersede in request order.
    ///
    /// Back-to-back broadcasts of the same block, such as its early and committed broadcasts,
    /// share a generation, so neither drops the other's adverts.
    pub(crate) fn start(&self, block: AdvertisedBlock) -> BlockBroadcast {
        let mut generation = SUPERSEDED_AT_START;
        self.0.newest.send_if_modified(|newest| {
            if newest.hash == Some(block.hash) {
                newest.height = newest.height.or(block.height);
                generation = newest.generation;
                return false;
            }
            if let (Some(height), Some(newest_height)) = (block.height, newest.height) {
                if height < newest_height {
                    metrics::counter!("peer.block_gossip.lower_block_superseded").increment(1);
                    return false;
                }
            }
            newest.generation += 1;
            newest.hash = Some(block.hash);
            newest.height = block.height;
            generation = newest.generation;
            true
        });
        BlockBroadcast {
            hash: block.hash,
            generation,
            window: self.0.fanout.window(block.size),
            scheduler: self.clone(),
        }
    }

    /// Returns the number of upload slots not held by a requested upload.
    pub(crate) fn available_uploads(&self) -> usize {
        self.0.uploads.available_permits()
    }

    /// Waits for an upload slot, which is returned when the slot is dropped.
    async fn upload_slot(&self) -> UploadSlot {
        let permit = self
            .0
            .uploads
            .clone()
            .acquire_owned()
            .await
            .expect("the scheduler never closes its upload semaphore");
        self.record_uploads_in_use();
        UploadSlot {
            permit: Some(permit),
            scheduler: self.clone(),
        }
    }

    fn record_uploads_in_use(&self) {
        metrics::gauge!("peer.block_gossip.uploads_in_use")
            // Upload counts are at most `BLOCK_UPLOAD_CONCURRENCY`, so they convert exactly.
            .set((self.0.upload_capacity - self.available_uploads()) as f64);
    }
}

/// One accounted upload, held from the peer's getdata through the drain allowance.
struct UploadSlot {
    permit: Option<OwnedSemaphorePermit>,
    scheduler: BlockGossipScheduler,
}

impl Drop for UploadSlot {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.scheduler.record_uploads_in_use();
    }
}

/// One block broadcast, its advert window, and the generation that a broadcast of a different
/// block supersedes.
#[derive(Clone, Debug)]
pub(crate) struct BlockBroadcast {
    hash: block::Hash,
    generation: u64,
    /// The most non-sidecar peers this broadcast has advertised to and is not yet finished with.
    window: usize,
    scheduler: BlockGossipScheduler,
}

impl BlockBroadcast {
    /// Returns true if a superseding broadcast started after this one.
    fn is_superseded(&self) -> bool {
        self.scheduler.0.newest.borrow().generation > self.generation
    }

    /// Resolves once a superseding broadcast starts after this one.
    async fn superseded(&self) {
        let mut newest = self.scheduler.0.newest.subscribe();
        // The scheduler, and so the sender, outlives this broadcast.
        let _ = newest
            .wait_for(|newest| newest.generation > self.generation)
            .await;
    }
}

/// Advertises `broadcast`'s block to `peers`, paced through their block uploads.
///
/// Configured sidecars are all advertised at once, first. Other peers are advertised to at most
/// [`BlockBroadcast::window`] at a time. Advertises to no further peers once a newer broadcast
/// supersedes this one. Succeeds if at least one peer was advertised to.
pub(crate) fn broadcast_block(
    peers: Vec<BlockGossipPeer>,
    mined: bool,
    broadcast: BlockBroadcast,
) -> BoxFuture<'static, Result<Response, BoxError>> {
    let mut sidecars = order_peers(peers, mined, &mut rand_10::rng());
    let others = sidecars.split_off(sidecars.iter().take_while(|peer| peer.sidecar).count());
    async move {
        let advertise = |peers: Vec<BlockGossipPeer>, window: usize| {
            let (superseded, broadcast) = (broadcast.clone(), broadcast.clone());
            futures::stream::iter(peers)
                .take_until(async move { superseded.superseded().await }.boxed())
                .map(move |peer| peer.advertise(broadcast.clone()))
                .buffer_unordered(window)
        };
        let sidecar_window = sidecars.len().max(1);
        // `select` polls the sidecars first, so their adverts are queued first.
        let mut sends = futures::stream::select(
            advertise(sidecars, sidecar_window),
            advertise(others, broadcast.window),
        );
        let mut succeeded = false;
        while let Some(sent) = sends.next().await {
            succeeded |= sent;
        }
        if succeeded {
            Ok(Response::Nil)
        } else if broadcast.is_superseded() {
            Err(
                std::io::Error::other("block broadcast was superseded before reaching a peer")
                    .into(),
            )
        } else {
            Err(std::io::Error::other("block broadcast reached no peers").into())
        }
    }
    .boxed()
}

/// Block advertisements that do not wait for the peer set to have an idle peer.
///
/// The peer set only accepts a request once one of its peers is ready, and a peer awaiting a
/// response is not ready. This advertiser instead uses the block advert queue of every connected
/// peer, including busy ones, which the peer set publishes whenever it changes.
#[derive(Clone, Debug)]
pub(crate) struct BlockAdvertiser<C> {
    /// Every connected peer's block advert queue, as last published by the peer set.
    pub(crate) peers: watch::Receiver<Vec<BlockGossipPeer>>,
    /// Shared with the peer set's own block broadcasts.
    pub(crate) scheduler: BlockGossipScheduler,
    pub(crate) minimum_peer_version: MinimumPeerVersion<C>,
}

impl<C: ChainTip> BlockAdvertiser<C> {
    /// Returns true if `request` is a block advertisement this advertiser sends.
    pub(crate) fn handles(request: &Request) -> bool {
        matches!(
            request,
            Request::AdvertiseBlock(..)
                | Request::AdvertiseBlockToAll(_)
                | Request::AdvertiseMinedBlock(_)
        )
    }

    /// Advertises the block in `request` to every eligible connected peer.
    ///
    /// Waits until at least one connected peer meets the minimum protocol version, so the latest
    /// block still reaches the first peer that connects.
    pub(crate) fn advertise(
        &mut self,
        request: Request,
    ) -> BoxFuture<'static, Result<Response, BoxError>> {
        let (block, mined) = match request {
            Request::AdvertiseBlock(block, _) | Request::AdvertiseBlockToAll(block) => {
                (block, false)
            }
            Request::AdvertiseMinedBlock(block) => (block, true),
            other => {
                return futures::future::ready(Err(format!(
                    "block advertiser cannot send {}",
                    other.command()
                )
                .into()))
                .boxed()
            }
        };
        let minimum_version = self.minimum_peer_version.current();
        let eligible = move |peer: &BlockGossipPeer| {
            peer.version >= minimum_version && !peer.sender.is_closed()
        };
        let mut peers = self.peers.clone();
        // Start the broadcast now, so broadcasts supersede each other in request order.
        let broadcast = self.scheduler.start(block);
        async move {
            let peers: Vec<_> = peers
                .wait_for(|peers| peers.iter().any(eligible))
                .await
                .map_err(|_| "peer set stopped")?
                .iter()
                .filter(|peer| eligible(peer))
                .cloned()
                .collect();
            broadcast_block(peers, mined, broadcast).await
        }
        .boxed()
    }
}

/// Randomize relays and equal RTTs; prioritize configured sidecars within the cap.
///
/// Mined blocks follow RTT order, except that every [`MINED_RANDOM_INTERVAL`]th non-sidecar
/// advertisement goes to a random remaining peer.
pub(crate) fn order_peers(
    peers: Vec<BlockGossipPeer>,
    mined: bool,
    rng: &mut impl rand_10::Rng,
) -> Vec<BlockGossipPeer> {
    use rand_10::{seq::SliceRandom, RngExt};
    // Snapshot RTT once: heartbeat updates must not change comparisons during sorting.
    let mut peers: Vec<_> = peers
        .into_iter()
        .map(|peer| {
            let rtt = if mined {
                peer.rtt().unwrap_or(Duration::MAX)
            } else {
                Duration::ZERO
            };
            (peer.sidecar, rtt, peer)
        })
        .collect();
    peers.shuffle(rng);
    peers.sort_unstable_by_key(|(sidecar, rtt, _)| (!sidecar, *rtt));
    let sidecars = peers.iter().take_while(|(sidecar, ..)| *sidecar).count();
    let mut remaining = peers.split_off(sidecars);
    let mut ordered: Vec<_> = peers.into_iter().map(|(_, _, peer)| peer).collect();
    for position in 1..=remaining.len() {
        let index = if mined && position % MINED_RANDOM_INTERVAL == 0 {
            rng.random_range(0..remaining.len())
        } else {
            0
        };
        ordered.push(remaining.remove(index).2);
    }
    ordered
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        peer::advert_fanout::UploadBandwidth, peer_registry::PeerRegistry, Config, ConnectedPeer,
    };
    use futures::FutureExt;
    use rand_10::{rngs::StdRng, SeedableRng};

    /// The advert window of [`scheduler`]: #1874's fixed window, which these tests were
    /// written for. Tests of the adaptive window build their own scheduler.
    const TEST_WINDOW: usize = 32;

    /// Returns a scheduler with `upload_capacity` slots and a fixed [`TEST_WINDOW`].
    fn scheduler(upload_capacity: usize) -> BlockGossipScheduler {
        let config = Config {
            advert_window_fixed: Some(TEST_WINDOW),
            ..Config::default()
        };
        let bandwidth = UploadBandwidth::from_config(&config);
        scheduler_with(upload_capacity, config, bandwidth)
    }

    /// Returns a scheduler with `upload_capacity` slots and the fan-out policy in `config`,
    /// which reads `bandwidth`.
    fn scheduler_with(
        upload_capacity: usize,
        config: Config,
        bandwidth: UploadBandwidth,
    ) -> BlockGossipScheduler {
        BlockGossipScheduler::new(
            upload_capacity,
            AdvertFanout::from_config(&config, bandwidth),
        )
    }

    fn peer() -> (BlockGossipPeer, mpsc::Receiver<BlockAdvert>) {
        let (sender, receiver) = mpsc::channel(BLOCK_ADVERT_QUEUE);
        (
            BlockGossipPeer {
                sender,
                uploads: BlockUploads::default(),
                registry: None,
                version: crate::constants::CURRENT_NETWORK_PROTOCOL_VERSION,
                sidecar: false,
            },
            receiver,
        )
    }

    async fn accept(receiver: &mut mpsc::Receiver<BlockAdvert>, hash: block::Hash) {
        let advert = receiver
            .recv()
            .await
            .expect("peer receives an advertisement");
        assert_eq!(advert.hash, hash);
        advert.result.send(Ok(())).unwrap();
        tokio::task::yield_now().await;
    }

    /// Builds `count` peers whose connections write every queued advert at once, marking it
    /// advertised as [`Connection`](crate::peer::Connection) does, and report each write.
    fn connected_peers(
        count: usize,
    ) -> (
        Vec<BlockGossipPeer>,
        mpsc::UnboundedReceiver<(usize, block::Hash)>,
    ) {
        let (peers, writes, _bodies) = requesting_peers(count, None);
        (peers, writes)
    }

    /// Body writes that peers requested and that have not completed yet.
    #[derive(Debug, Default)]
    struct BodyWrites {
        current: std::sync::atomic::AtomicUsize,
        max: std::sync::atomic::AtomicUsize,
    }

    /// Like [`connected_peers`], but if `body_write` is set, each peer requests every advertised
    /// block at once, and its connection writes the body that long afterwards, whether or not
    /// gossip has an upload slot for it, as the inbound service does.
    fn requesting_peers(
        count: usize,
        body_write: Option<Duration>,
    ) -> (
        Vec<BlockGossipPeer>,
        mpsc::UnboundedReceiver<(usize, block::Hash)>,
        Arc<BodyWrites>,
    ) {
        use std::sync::atomic::Ordering::SeqCst;
        let (writes_tx, writes) = mpsc::unbounded_channel();
        let bodies = Arc::new(BodyWrites::default());
        let peers = (0..count)
            .map(|index| {
                let (peer, mut receiver) = peer();
                let uploads = peer.uploads.clone();
                let writes_tx = writes_tx.clone();
                let bodies = bodies.clone();
                tokio::spawn(async move {
                    while let Some(advert) = receiver.recv().await {
                        if advert.result.is_closed() {
                            continue;
                        }
                        let hash = advert.hash;
                        uploads.advertised(hash);
                        let _ = advert.result.send(Ok(()));
                        let _ = writes_tx.send((index, hash));
                        let Some(body_write) = body_write else {
                            continue;
                        };
                        uploads.requested(hash);
                        let current = bodies.current.fetch_add(1, SeqCst) + 1;
                        bodies.max.fetch_max(current, SeqCst);
                        let (uploads, bodies) = (uploads.clone(), bodies.clone());
                        tokio::spawn(async move {
                            tokio::time::sleep(body_write).await;
                            bodies.current.fetch_sub(1, SeqCst);
                            uploads.written(hash);
                        });
                    }
                });
                peer
            })
            .collect();
        (peers, writes, bodies)
    }

    /// What an older broadcast's peers do after its inventory is written.
    #[derive(Clone, Copy, Debug)]
    enum OlderBroadcastPeers {
        /// The peers already have the block, so they never send getdata.
        NeverRequest,
        /// The peers requested the block, and its body is still being written.
        Uploading,
        /// The body write completed, so the peers are inside the post-write grace.
        InWriteGrace,
    }

    /// Returns how long a fresh block's first inventory write waits after an older broadcast
    /// reached every peer, optionally as overlapping early and committed broadcasts.
    async fn fresh_inventory_wait(
        older: OlderBroadcastPeers,
        early_and_committed: bool,
    ) -> Duration {
        let parent = block::Hash([0xa0; 32]);
        let child = block::Hash([0xb0; 32]);
        let scheduler = scheduler(BLOCK_UPLOAD_CONCURRENCY);
        let (peers, mut writes) = connected_peers(BLOCK_UPLOAD_CONCURRENCY);
        let mut broadcasts = vec![tokio::spawn(broadcast_block(
            peers.clone(),
            true,
            scheduler.start(parent.into()),
        ))];
        if early_and_committed {
            broadcasts.push(tokio::spawn(broadcast_block(
                peers.clone(),
                true,
                scheduler.start(parent.into()),
            )));
        }
        for _ in 0..BLOCK_UPLOAD_CONCURRENCY {
            let (index, hash) = writes.recv().await.expect("mock connections stay open");
            assert_eq!(hash, parent, "each peer gets the parent inventory once");
            let uploads = &peers[index].uploads;
            match older {
                OlderBroadcastPeers::NeverRequest => {}
                OlderBroadcastPeers::Uploading => uploads.requested(parent),
                OlderBroadcastPeers::InWriteGrace => {
                    uploads.requested(parent);
                    uploads.written(parent);
                }
            }
        }
        tokio::task::yield_now().await;
        let expected_uploads = match older {
            OlderBroadcastPeers::NeverRequest => BLOCK_UPLOAD_CONCURRENCY,
            OlderBroadcastPeers::Uploading | OlderBroadcastPeers::InWriteGrace => 0,
        };
        assert_eq!(scheduler.available_uploads(), expected_uploads);

        let mined = Instant::now();
        broadcasts.push(tokio::spawn(broadcast_block(
            peers,
            true,
            scheduler.start(child.into()),
        )));
        let wait = loop {
            let (_, hash) = writes.recv().await.expect("mock connections stay open");
            if hash == child {
                break mined.elapsed();
            }
        };
        broadcasts.iter().for_each(|broadcast| broadcast.abort());
        wait
    }

    /// The fleet's fresh-block wait: an older broadcast's peers that never request, are
    /// uploading, or are inside the write grace must not delay a new block's first inventory.
    /// Before upload slots were taken only on request, these waits were 3 s, 40 s, 750 ms and 3 s.
    #[tokio::test(start_paused = true)]
    async fn fresh_block_inventory_does_not_wait_for_older_broadcast_slots() {
        for (older, early_and_committed) in [
            (OlderBroadcastPeers::NeverRequest, false),
            (OlderBroadcastPeers::Uploading, false),
            (OlderBroadcastPeers::InWriteGrace, false),
            (OlderBroadcastPeers::NeverRequest, true),
        ] {
            let wait = fresh_inventory_wait(older, early_and_committed).await;
            assert_eq!(
                wait,
                Duration::ZERO,
                "{older:?} (early and committed: {early_and_committed}) holds the fresh inventory"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn upload_slot_is_taken_on_request_and_held_through_grace() {
        let hash = block::Hash([1; 32]);
        let scheduler = scheduler(1);
        let (first, mut first_rx) = peer();
        let uploads = first.uploads.clone();
        let (second, mut second_rx) = peer();
        let first_task = tokio::spawn(first.advertise(scheduler.start(hash.into())));
        accept(&mut first_rx, hash).await;
        assert_eq!(
            scheduler.available_uploads(),
            1,
            "written inventory takes no upload slot"
        );
        uploads.requested(hash);
        tokio::task::yield_now().await;
        assert_eq!(scheduler.available_uploads(), 0);
        // Another peer's inventory does not wait for the upload.
        let second_task = tokio::spawn(second.advertise(scheduler.start(hash.into())));
        accept(&mut second_rx, hash).await;
        tokio::time::advance(BLOCK_REQUEST_WAIT + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(second_task.await.unwrap(), "the unrequested advert ends");
        assert_eq!(
            scheduler.available_uploads(),
            0,
            "getdata keeps the slot beyond the no-request timeout"
        );
        uploads.written(hash);
        tokio::task::yield_now().await;
        tokio::time::advance(BLOCK_WRITE_GRACE - Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(scheduler.available_uploads(), 0);
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(first_task.await.unwrap());
        assert_eq!(scheduler.available_uploads(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn unrequested_inventory_holds_no_upload_slot() {
        let hash = block::Hash([2; 32]);
        let scheduler = scheduler(1);
        let (first, mut first_rx) = peer();
        let task = tokio::spawn(first.advertise(scheduler.start(hash.into())));
        accept(&mut first_rx, hash).await;
        tokio::time::advance(BLOCK_REQUEST_WAIT - Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert!(!task.is_finished(), "the advert waits for a request");
        assert_eq!(scheduler.available_uploads(), 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(task.await.unwrap());
        assert_eq!(scheduler.available_uploads(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn duplicate_announcements_share_upload_and_slot() {
        let hash = block::Hash([3; 32]);
        let scheduler = scheduler(1);
        let (peer, mut receiver) = peer();
        let first = tokio::spawn(peer.clone().advertise(scheduler.start(hash.into())));
        accept(&mut receiver, hash).await;
        let second = tokio::spawn(peer.clone().advertise(scheduler.start(hash.into())));
        tokio::task::yield_now().await;
        assert!(receiver.recv().now_or_never().is_none());
        peer.uploads.requested(hash);
        peer.uploads.written(hash);
        assert!(first.await.unwrap());
        assert!(second.await.unwrap());
        assert_eq!(scheduler.available_uploads(), 1);
        assert!(receiver.recv().now_or_never().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_wakes_duplicate_and_returns_permit() {
        let hash = block::Hash([4; 32]);
        let scheduler = scheduler(1);
        let (peer, mut receiver) = peer();
        let first = tokio::spawn(peer.clone().advertise(scheduler.start(hash.into())));
        accept(&mut receiver, hash).await;
        let duplicate = tokio::spawn(peer.clone().advertise(scheduler.start(hash.into())));
        tokio::task::yield_now().await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        // The duplicate takes over the advertisement instead of reporting the cancelled one.
        accept(&mut receiver, hash).await;
        peer.uploads.requested(hash);
        peer.uploads.written(hash);
        assert!(duplicate.await.unwrap());
        assert_eq!(scheduler.available_uploads(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn joined_announcement_readvertises_after_unavailable_upload() {
        let hash = block::Hash([7; 32]);
        let scheduler = scheduler(1);
        let (peer, mut receiver) = peer();
        let early = tokio::spawn(peer.clone().advertise(scheduler.start(hash.into())));
        accept(&mut receiver, hash).await;
        let committed = tokio::spawn(peer.clone().advertise(scheduler.start(hash.into())));
        tokio::task::yield_now().await;
        peer.uploads.requested(hash);
        peer.uploads.unavailable(hash);
        assert!(!early.await.unwrap());
        accept(&mut receiver, hash).await;
        peer.uploads.requested(hash);
        peer.uploads.written(hash);
        assert!(committed.await.unwrap());
        assert_eq!(scheduler.available_uploads(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn joined_announcement_does_not_retry_after_disconnect() {
        let hash = block::Hash([8; 32]);
        let scheduler = scheduler(1);
        let (peer, mut receiver) = peer();
        let first = tokio::spawn(peer.clone().advertise(scheduler.start(hash.into())));
        accept(&mut receiver, hash).await;
        let duplicate = tokio::spawn(peer.clone().advertise(scheduler.start(hash.into())));
        tokio::task::yield_now().await;
        drop(receiver);
        peer.uploads.disconnected();
        assert!(!first.await.unwrap());
        assert!(!duplicate.await.unwrap());
        assert_eq!(scheduler.available_uploads(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn upload_timeout_and_disconnect_release_slots() {
        for disconnect in [false, true] {
            let hash = block::Hash([5; 32]);
            let scheduler = scheduler(1);
            let (peer, mut receiver) = peer();
            let uploads = peer.uploads.clone();
            let task = tokio::spawn(peer.advertise(scheduler.start(hash.into())));
            accept(&mut receiver, hash).await;
            uploads.requested(hash);
            tokio::task::yield_now().await;
            assert_eq!(
                scheduler.available_uploads(),
                0,
                "the upload holds its slot"
            );
            if disconnect {
                uploads.disconnected();
            } else {
                tokio::time::advance(BLOCK_UPLOAD_WAIT).await;
            }
            assert_eq!(task.await.unwrap(), !disconnect);
            assert_eq!(scheduler.available_uploads(), 1);
        }
    }

    /// Requested uploads of different blocks share the cap, but inventory does not wait for it.
    #[tokio::test(start_paused = true)]
    async fn requested_uploads_of_different_blocks_share_the_cap() {
        let scheduler = scheduler(2);
        let mut tasks = Vec::new();
        let mut uploads = Vec::new();
        for i in 0..3 {
            let hash = block::Hash([20 + i; 32]);
            let (peer, mut receiver) = peer();
            let upload = peer.uploads.clone();
            tasks.push(tokio::spawn(peer.advertise(scheduler.start(hash.into()))));
            accept(&mut receiver, hash).await;
            upload.requested(hash);
            uploads.push((hash, upload));
            tokio::task::yield_now().await;
        }
        assert_eq!(scheduler.available_uploads(), 0);
        // The third upload waits for a slot, not for the first upload's timers.
        let (hash, first) = &uploads[0];
        first.written(*hash);
        tokio::task::yield_now().await;
        tokio::time::advance(BLOCK_WRITE_GRACE).await;
        assert!(tasks.remove(0).await.unwrap());
        tokio::task::yield_now().await;
        assert_eq!(
            scheduler.available_uploads(),
            0,
            "the waiting upload takes the slot"
        );
        for (hash, upload) in &uploads[1..] {
            upload.written(*hash);
        }
        tokio::task::yield_now().await;
        tokio::time::advance(BLOCK_WRITE_GRACE).await;
        for task in tasks {
            assert!(task.await.unwrap());
        }
        assert_eq!(scheduler.available_uploads(), 2);
    }

    /// A peer that requested while every slot was held, then disconnected, holds nothing.
    #[tokio::test(start_paused = true)]
    async fn disconnect_while_waiting_for_an_upload_slot_takes_none() {
        let hash = block::Hash([12; 32]);
        let scheduler = scheduler(1);
        let (holder, mut holder_rx) = peer();
        let (waiter, mut waiter_rx) = peer();
        let holder_uploads = holder.uploads.clone();
        let waiter_uploads = waiter.uploads.clone();
        let holder = tokio::spawn(holder.advertise(scheduler.start(hash.into())));
        let waiter = tokio::spawn(waiter.advertise(scheduler.start(hash.into())));
        accept(&mut holder_rx, hash).await;
        accept(&mut waiter_rx, hash).await;
        holder_uploads.requested(hash);
        tokio::task::yield_now().await;
        waiter_uploads.requested(hash);
        tokio::task::yield_now().await;
        waiter_uploads.disconnected();
        assert!(!waiter.await.unwrap());
        holder_uploads.written(hash);
        tokio::task::yield_now().await;
        tokio::time::advance(BLOCK_WRITE_GRACE).await;
        assert!(holder.await.unwrap());
        assert_eq!(scheduler.available_uploads(), 1);
    }

    /// A newer block's broadcast drops the older broadcast's unwritten adverts and advertises it
    /// to no further peers, but still accounts for the parent uploads that peers request.
    #[tokio::test(start_paused = true)]
    async fn newer_block_supersedes_unsent_adverts_but_tracks_requested_parent() {
        let parent = block::Hash([13; 32]);
        let child = block::Hash([14; 32]);
        let scheduler = scheduler(BLOCK_UPLOAD_CONCURRENCY);

        // A busy connection has not written the parent advert yet.
        let (busy, mut busy_rx) = peer();
        let queued = tokio::spawn(busy.advertise(scheduler.start(parent.into())));
        tokio::task::yield_now().await;

        // A parent broadcast with more peers than its window, which never request.
        let (peers, mut writes) = connected_peers(TEST_WINDOW + 8);
        let parent_broadcast = tokio::spawn(broadcast_block(
            peers.clone(),
            true,
            scheduler.start(parent.into()),
        ));
        for _ in 0..TEST_WINDOW {
            assert_eq!(writes.recv().await.unwrap().1, parent);
        }
        // One peer that got the parent inventory requests it only after the child is mined.
        let (requester, mut requester_rx) = peer();
        let requester_uploads = requester.uploads.clone();
        let requested = tokio::spawn(requester.advertise(scheduler.start(parent.into())));
        accept(&mut requester_rx, parent).await;

        let child_broadcast = tokio::spawn(broadcast_block(
            peers.clone(),
            true,
            scheduler.start(child.into()),
        ));
        assert!(
            !queued.await.unwrap(),
            "the unwritten parent advert is dropped"
        );
        let dropped = busy_rx.recv().await.expect("the advert stays queued");
        assert!(
            dropped.result.is_closed(),
            "the connection skips the superseded advert"
        );

        requester_uploads.requested(parent);
        tokio::task::yield_now().await;
        assert_eq!(
            scheduler.available_uploads(),
            BLOCK_UPLOAD_CONCURRENCY - 1,
            "the requested parent upload is still accounted for"
        );

        // Every peer gets the child; no further peer gets the parent.
        let mut children = 0;
        while children < peers.len() {
            let (_, hash) = writes.recv().await.unwrap();
            assert_eq!(
                hash, child,
                "a superseded broadcast writes no further inventory"
            );
            children += 1;
        }
        assert!(parent_broadcast.await.unwrap().is_ok());
        assert!(child_broadcast.await.unwrap().is_ok());

        requester_uploads.written(parent);
        tokio::task::yield_now().await;
        tokio::time::advance(BLOCK_WRITE_GRACE).await;
        assert!(requested.await.unwrap());
        assert_eq!(scheduler.available_uploads(), BLOCK_UPLOAD_CONCURRENCY);
    }

    /// Early and committed broadcasts of one block share a generation; a different block
    /// supersedes both, and a later broadcast of an older block supersedes it in turn.
    #[test]
    fn broadcasts_supersede_in_request_order_except_repeats() {
        let scheduler = scheduler(1);
        let (x, y) = (block::Hash([15; 32]), block::Hash([16; 32]));
        let early = scheduler.start(x.into());
        let committed = scheduler.start(x.into());
        assert!(!early.is_superseded() && !committed.is_superseded());
        let child = scheduler.start(y.into());
        assert!(early.is_superseded() && committed.is_superseded());
        let late = scheduler.start(x.into());
        assert!(child.is_superseded() && !late.is_superseded());
    }

    fn block_at(byte: u8, height: u32) -> AdvertisedBlock {
        AdvertisedBlock {
            hash: block::Hash([byte; 32]),
            height: Some(block::Height(height)),
            size: None,
        }
    }

    /// A broadcast supersedes only lower blocks. A late broadcast of a lower block, such as a
    /// committed parent that arrives after its child's early broadcast, starts superseded.
    /// Same-height and unknown-height blocks keep request order.
    #[test]
    fn broadcasts_supersede_only_lower_blocks() {
        let scheduler = scheduler(1);
        let child = scheduler.start(block_at(30, 10));
        let late_parent = scheduler.start(block_at(31, 9));
        assert!(
            late_parent.is_superseded(),
            "the lower block starts superseded"
        );
        assert!(!child.is_superseded(), "the higher block keeps advertising");

        let committed_child = scheduler.start(block_at(30, 10));
        assert!(!child.is_superseded() && !committed_child.is_superseded());

        let sibling = scheduler.start(block_at(32, 10));
        assert!(child.is_superseded() && committed_child.is_superseded());
        let child_again = scheduler.start(block_at(30, 10));
        assert!(sibling.is_superseded() && !child_again.is_superseded());

        let unknown = scheduler.start(block::Hash([33; 32]).into());
        assert!(child_again.is_superseded() && !unknown.is_superseded());
        let grandchild = scheduler.start(block_at(34, 11));
        assert!(unknown.is_superseded() && !grandchild.is_superseded());
        assert!(scheduler.start(block_at(35, 10)).is_superseded());
        assert!(!grandchild.is_superseded());
    }

    /// Interleaved early and committed broadcasts no longer swap: a parent's late broadcast
    /// reaches no peers and does not stop its child's.
    #[tokio::test(start_paused = true)]
    async fn late_lower_broadcast_does_not_stop_the_higher_one() {
        let scheduler = scheduler(BLOCK_UPLOAD_CONCURRENCY);
        let (child, parent) = (block_at(36, 20), block_at(37, 19));
        let (peers, mut writes) = connected_peers(TEST_WINDOW + 8);
        let child_broadcast = tokio::spawn(broadcast_block(
            peers.clone(),
            false,
            scheduler.start(child),
        ));
        for _ in 0..TEST_WINDOW {
            assert_eq!(writes.recv().await.unwrap().1, child.hash);
        }
        let parent_broadcast = tokio::spawn(broadcast_block(
            peers.clone(),
            false,
            scheduler.start(parent),
        ));
        assert!(
            parent_broadcast.await.unwrap().is_err(),
            "the lower block reaches no peers"
        );
        for _ in TEST_WINDOW..peers.len() {
            assert_eq!(
                writes.recv().await.unwrap().1,
                child.hash,
                "the higher block reaches every peer"
            );
        }
        assert!(child_broadcast.await.unwrap().is_ok());
    }

    /// Returns a measured bandwidth whose estimate is `bytes_per_second`, from one upload that
    /// peers acknowledged over one second.
    fn bandwidth_measured_at(bytes_per_second: u64) -> UploadBandwidth {
        let bandwidth = UploadBandwidth::measured();
        let started = std::time::Instant::now();
        bandwidth.upload_started(started);
        bandwidth.upload_finished(bytes_per_second, started + Duration::from_secs(1));
        bandwidth
    }

    /// Starts a broadcast of a block of `size` bytes to `peers` with the fan-out policy in
    /// `config`, which reads `bandwidth`, and returns the peers it advertised to before any
    /// advert finished.
    async fn first_wave(
        config: Config,
        bandwidth: UploadBandwidth,
        peers: Vec<BlockGossipPeer>,
        size: u32,
    ) -> Vec<usize> {
        let scheduler = scheduler_with(BLOCK_UPLOAD_CONCURRENCY, config, bandwidth);
        let (peers, mut writes) = peers_writing(peers);
        let block = AdvertisedBlock {
            size: Some(size),
            ..block_at(40, 100)
        };
        let broadcast = tokio::spawn(broadcast_block(peers, false, scheduler.start(block)));
        let mut wave = Vec::new();
        // These peers never request the block, so no advert finishes before the request wait.
        while let Ok(Some((index, _))) = timeout(BLOCK_REQUEST_WAIT / 2, writes.recv()).await {
            wave.push(index);
        }
        broadcast.abort();
        wave
    }

    /// Wires `peers`' advert queues to connections that write every advert at once.
    fn peers_writing(
        peers: Vec<BlockGossipPeer>,
    ) -> (
        Vec<BlockGossipPeer>,
        mpsc::UnboundedReceiver<(usize, block::Hash)>,
    ) {
        let (connected, writes) = connected_peers(peers.len());
        let peers = peers
            .into_iter()
            .zip(connected)
            .map(|(peer, connected)| BlockGossipPeer {
                sidecar: peer.sidecar,
                ..connected
            })
            .collect();
        (peers, writes)
    }

    fn plain_peers(count: usize) -> Vec<BlockGossipPeer> {
        (0..count).map(|_| peer().0).collect()
    }

    /// Before the upload bandwidth is measured, every block goes to the initial 32 peers at
    /// once, whatever its size. This is also the window on platforms that cannot measure it.
    #[tokio::test(start_paused = true)]
    async fn advert_window_is_initial_before_measurement() {
        for size in [2_000, 1_500_000, 10_000_000] {
            let config = Config::default();
            let bandwidth = UploadBandwidth::from_config(&config);
            let wave = first_wave(config, bandwidth, plain_peers(100), size).await;
            assert_eq!(wave.len(), 32, "{size} byte block");
        }
        for size in [2_000, 10_000_000] {
            let wave = first_wave(
                Config::default(),
                UploadBandwidth::unmeasured(),
                plain_peers(100),
                size,
            )
            .await;
            assert_eq!(wave.len(), 32, "unmeasured {size} byte block");
        }
    }

    /// Once 12.5 MB/s is measured, with the default 1 s target and 16..=264 window, a 1.5 MB
    /// block computes ceil(8.33) = 9 peers and clamps to the floor of 16, a 2 KB block goes to
    /// every peer up to the ceiling of 264, and a 10 MB block goes to the floor.
    #[tokio::test(start_paused = true)]
    async fn measured_advert_window_follows_block_size() {
        let wave = |peers, size| {
            first_wave(
                Config::default(),
                bandwidth_measured_at(12_500_000),
                plain_peers(peers),
                size,
            )
        };
        assert_eq!(wave(300, 1_500_000).await.len(), 16);
        assert_eq!(wave(300, 2_000).await.len(), 264);
        assert_eq!(wave(100, 2_000).await.len(), 100);
        assert_eq!(wave(300, 10_000_000).await.len(), 16);
    }

    /// Below the default floor, the measured window is the unclamped `ceil(B̂ · τ / S)`: a
    /// 1.5 MB block at 12.5 MB/s and 1 s goes to ceil(8.33) = 9 peers at once, and at 3 MB/s
    /// and 2 s to 4.
    #[tokio::test(start_paused = true)]
    async fn measured_advert_window_below_the_default_floor_is_unclamped() {
        let low_floor = Config {
            advert_window_min: 1,
            ..Config::default()
        };
        let wave = first_wave(
            low_floor.clone(),
            bandwidth_measured_at(12_500_000),
            plain_peers(100),
            1_500_000,
        )
        .await;
        assert_eq!(wave.len(), 9);
        let slower = Config {
            advert_target: Duration::from_secs(2),
            ..low_floor
        };
        let wave = first_wave(
            slower,
            bandwidth_measured_at(3_000_000),
            plain_peers(100),
            1_500_000,
        )
        .await;
        assert_eq!(wave.len(), 4);
    }

    /// `advert_window_fixed` advertises every block to that many peers at once, even with a
    /// measured bandwidth.
    #[tokio::test(start_paused = true)]
    async fn fixed_advert_window_ignores_block_size() {
        for size in [2_000, 1_500_000, 10_000_000] {
            let config = Config {
                advert_window_fixed: Some(8),
                ..Config::default()
            };
            let wave = first_wave(
                config,
                bandwidth_measured_at(12_500_000),
                plain_peers(100),
                size,
            )
            .await;
            assert_eq!(wave.len(), 8);
        }
    }

    /// Sidecars beyond the window are all advertised at once, outside the window.
    #[tokio::test(start_paused = true)]
    async fn sidecars_beyond_the_window_go_first() {
        // More sidecars than the default floor of 16, which a 10 MB block uses.
        const SIDECARS: usize = 20;
        let mut peers = plain_peers(SIDECARS + 40);
        for peer in &mut peers[..SIDECARS] {
            peer.sidecar = true;
        }
        let wave = first_wave(
            Config::default(),
            bandwidth_measured_at(12_500_000),
            peers,
            10_000_000,
        )
        .await;
        assert_eq!(
            wave.len(),
            SIDECARS + 16,
            "every sidecar plus a floor-sized window: {wave:?}"
        );
        assert!(
            (0..SIDECARS).all(|sidecar| wave.contains(&sidecar)),
            "every sidecar is in the first wave: {wave:?}"
        );
        assert!(
            wave[..SIDECARS].iter().all(|index| *index < SIDECARS),
            "sidecar adverts are written first: {wave:?}"
        );
    }

    /// A committed broadcast does not drop its early broadcast's queued adverts.
    #[tokio::test(start_paused = true)]
    async fn committed_broadcast_keeps_early_adverts() {
        let hash = block::Hash([17; 32]);
        let scheduler = scheduler(1);
        let (peer, mut receiver) = peer();
        let early = tokio::spawn(peer.clone().advertise(scheduler.start(hash.into())));
        tokio::task::yield_now().await;
        let committed = tokio::spawn(peer.clone().advertise(scheduler.start(hash.into())));
        tokio::task::yield_now().await;
        accept(&mut receiver, hash).await;
        assert!(
            receiver.recv().now_or_never().is_none(),
            "the committed broadcast joins"
        );
        peer.uploads.requested(hash);
        peer.uploads.written(hash);
        tokio::task::yield_now().await;
        tokio::time::advance(BLOCK_WRITE_GRACE).await;
        assert!(early.await.unwrap());
        assert!(committed.await.unwrap());
    }

    /// An early broadcast whose inventory write fails is retried by the committed broadcast.
    #[tokio::test(start_paused = true)]
    async fn failed_early_advert_is_sent_by_the_committed_broadcast() {
        let hash = block::Hash([18; 32]);
        let scheduler = scheduler(1);
        let (peer, mut receiver) = peer();
        let early = tokio::spawn(broadcast_block(
            vec![peer.clone()],
            true,
            scheduler.start(hash.into()),
        ));
        let advert = receiver.recv().await.unwrap();
        advert
            .result
            .send(Err(BlockAdvertError::AuthorizationRevoked))
            .unwrap();
        assert!(early.await.unwrap().is_err());
        let committed = tokio::spawn(broadcast_block(
            vec![peer],
            true,
            scheduler.start(hash.into()),
        ));
        accept(&mut receiver, hash).await;
        tokio::time::advance(BLOCK_REQUEST_WAIT).await;
        assert!(committed.await.unwrap().is_ok());
    }

    /// Sustained relay: a block every second to peers that request every block, with bodies
    /// that take two seconds to write. Each block's first inventory is still immediate, upload
    /// slots never exceed the cap, and body writes stay within the cap plus one advert window.
    #[tokio::test(start_paused = true)]
    async fn sustained_relay_keeps_uploads_bounded() {
        const BLOCKS: u8 = 8;
        let scheduler = scheduler(BLOCK_UPLOAD_CONCURRENCY);
        let (peers, mut writes, bodies) =
            requesting_peers(2 * TEST_WINDOW, Some(Duration::from_secs(2)));
        let mut broadcasts = Vec::new();
        let mut advertised = Vec::new();
        for block in 0..BLOCKS {
            let hash = block::Hash([0xc0 + block; 32]);
            let mined = Instant::now();
            broadcasts.push(tokio::spawn(broadcast_block(
                peers.clone(),
                false,
                scheduler.start(hash.into()),
            )));
            let (_, first) = writes.recv().await.unwrap();
            assert_eq!(
                first, hash,
                "older blocks write no inventory after a newer one"
            );
            assert_eq!(mined.elapsed(), Duration::ZERO, "block {block} waited");
            let mut count = 1;
            let next_block = mined + Duration::from_secs(1);
            while let Ok(Some((_, written))) =
                tokio::time::timeout_at(next_block, writes.recv()).await
            {
                assert_eq!(
                    written, hash,
                    "older blocks write no inventory after a newer one"
                );
                count += 1;
                assert!(scheduler.available_uploads() <= BLOCK_UPLOAD_CONCURRENCY);
            }
            advertised.push(count);
        }
        let max_bodies = bodies.max.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            max_bodies <= BLOCK_UPLOAD_CONCURRENCY + TEST_WINDOW,
            "{max_bodies} concurrent body writes"
        );
        assert!(
            advertised.iter().all(|count| *count <= TEST_WINDOW),
            "busy upload slots stop each broadcast's fan-out after its window: {advertised:?}"
        );
        broadcasts.iter().for_each(|broadcast| broadcast.abort());
    }

    #[tokio::test(start_paused = true)]
    async fn unavailable_early_block_can_be_advertised_again_after_commit() {
        let hash = block::Hash([6; 32]);
        let scheduler = scheduler(1);
        let (peer, mut receiver) = peer();
        let early = tokio::spawn(peer.clone().advertise(scheduler.start(hash.into())));
        accept(&mut receiver, hash).await;
        peer.uploads.requested(hash);
        peer.uploads.unavailable(hash);
        assert!(!early.await.unwrap());
        let committed = tokio::spawn(peer.clone().advertise(scheduler.start(hash.into())));
        accept(&mut receiver, hash).await;
        peer.uploads.requested(hash);
        peer.uploads.written(hash);
        assert!(committed.await.unwrap());
        assert_eq!(scheduler.available_uploads(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn events_before_inventory_do_not_finish_the_advertisement() {
        let hash = block::Hash([9; 32]);
        let scheduler = scheduler(1);
        let (peer, mut receiver) = peer();
        let task = tokio::spawn(peer.clone().advertise(scheduler.start(hash.into())));
        tokio::task::yield_now().await;
        // A getdata that follows an earlier inventory resolves before this inventory is written.
        peer.uploads.requested(hash);
        peer.uploads.unavailable(hash);
        accept(&mut receiver, hash).await;
        tokio::time::advance(BLOCK_REQUEST_WAIT - Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(task.await.unwrap());
        assert_eq!(scheduler.available_uploads(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn unwritten_requested_hash_fails_when_the_request_finishes() {
        let hash = block::Hash([10; 32]);
        let scheduler = scheduler(1);
        let (peer, mut receiver) = peer();
        let task = tokio::spawn(peer.clone().advertise(scheduler.start(hash.into())));
        accept(&mut receiver, hash).await;
        peer.uploads.requested(hash);
        // The inbound service truncated the request before reaching this hash.
        peer.uploads.request_finished(&[hash]);
        assert!(!task.await.unwrap());
        assert_eq!(scheduler.available_uploads(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn only_sidecars_wait_for_busy_connections_to_write_inventory() {
        for sidecar in [false, true] {
            let hash = block::Hash([11; 32]);
            let scheduler = scheduler(1);
            let (mut peer, mut receiver) = peer();
            peer.sidecar = sidecar;
            let task = tokio::spawn(peer.advertise(scheduler.start(hash.into())));
            tokio::task::yield_now().await;
            tokio::time::advance(BLOCK_REQUEST_WAIT + Duration::from_millis(1)).await;
            tokio::task::yield_now().await;
            assert_eq!(task.is_finished(), !sidecar);
            if sidecar {
                accept(&mut receiver, hash).await;
                tokio::time::advance(BLOCK_REQUEST_WAIT).await;
                assert!(task.await.unwrap());
            } else {
                assert!(!task.await.unwrap());
            }
            assert_eq!(scheduler.available_uploads(), 1);
        }
    }

    /// Builds non-sidecar peers whose registry entries report `rtts` in milliseconds.
    fn measured_peers(
        registry: &PeerRegistry,
        guards: &mut Vec<crate::peer_registry::PeerRegistryGuard>,
        rtts: impl IntoIterator<Item = Option<u64>>,
    ) -> Vec<BlockGossipPeer> {
        rtts.into_iter()
            .enumerate()
            .map(|(i, millis)| {
                let (mut peer, _receiver) = peer();
                let (guard, updater) = registry.register_legacy(ConnectedPeer {
                    addr: format!("127.0.0.1:{}", i + 1).parse().unwrap(),
                    user_agent: "test".into(),
                    version: crate::constants::CURRENT_NETWORK_PROTOCOL_VERSION,
                    is_inbound: false,
                    rtt: millis.map(Duration::from_millis),
                    ping_sent_at: None,
                });
                guards.push(guard);
                peer.registry = Some(updater);
                peer
            })
            .collect()
    }

    #[test]
    fn miners_use_rtt_and_relays_use_random_order() {
        let registry = PeerRegistry::default();
        let mut guards = Vec::new();
        let peers = measured_peers(
            &registry,
            &mut guards,
            [Some(80), Some(10), None, Some(30), Some(10)],
        );
        let mined = order_peers(peers.clone(), true, &mut StdRng::seed_from_u64(9));
        let measured: Vec<_> = mined.iter().map(BlockGossipPeer::rtt).collect();
        assert_eq!(
            measured[..3],
            [Some(10), Some(10), Some(30)].map(|value| value.map(Duration::from_millis))
        );
        // The fourth advertisement goes to a random remaining peer.
        let mut rest = measured[3..].to_vec();
        rest.sort();
        assert_eq!(rest, [None, Some(Duration::from_millis(80))]);
        let relay = order_peers(peers, false, &mut StdRng::seed_from_u64(9));
        assert_ne!(
            relay.iter().map(BlockGossipPeer::rtt).collect::<Vec<_>>(),
            measured
        );
    }

    #[test]
    fn low_rtt_peers_cannot_fill_the_first_mined_wave() {
        let registry = PeerRegistry::default();
        let mut guards = Vec::new();
        let peers = measured_peers(&registry, &mut guards, (1..=100).map(Some));
        let mined: Vec<_> = order_peers(peers, true, &mut StdRng::seed_from_u64(3))
            .iter()
            .map(|peer| peer.rtt().unwrap().as_millis())
            .collect();
        for (position, window) in mined.chunks(MINED_RANDOM_INTERVAL).enumerate() {
            let rtt_ordered = &window[..window.len().min(MINED_RANDOM_INTERVAL - 1)];
            assert!(
                rtt_ordered.is_sorted(),
                "RTT order outside random slots in chunk {position}"
            );
        }
        let first_wave = &mined[..TEST_WINDOW];
        assert!(
            first_wave.iter().any(|rtt| *rtt > TEST_WINDOW as u128),
            "a random slot reaches beyond the lowest-RTT peers: {first_wave:?}"
        );
    }
}
