//! Schedule legacy block advertisements independently of peer requests and uploads.
//!
//! Each broadcast advertises immediately to its size-dependent initial group and sidecars.
//! Every remaining peer becomes eligible at one absolute deadline. Peer silence, slow writes,
//! and uploads cannot postpone another peer's announcement.
//!
//! Upload tracking deduplicates overlapping broadcasts and reports delivery failures. Its
//! request expiry and write grace do not control announcement timing or limit body writes.
//! A newer block does not cancel an older broadcast: peers can still need its parent.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use futures::{future::BoxFuture, FutureExt, StreamExt};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{sleep_until, timeout, Instant},
};
use zakura_chain::{block, chain_tip::ChainTip};

use crate::{
    constants::{BLOCK_GOSSIP_TIMEOUT, REQUEST_TIMEOUT},
    peer::{MinimumPeerVersion, SharedPeerError},
    peer_registry::PeerRegistryUpdater,
    protocol::external::types::Version,
    BannedIps, BoxError, Config, PeerSocketAddr, Request, Response,
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

/// Selects the initial peer count and the remaining peers' delay from the block size.
#[derive(Clone, Copy, Debug)]
pub(crate) struct AdvertFanout {
    min: usize,
    max: usize,
    small_size_bytes: u64,
    full_size_bytes: u64,
    delay_min: Duration,
    delay_max: Duration,
}

impl AdvertFanout {
    /// Returns the fan-out policy in `config`.
    pub(crate) fn from_config(config: &Config) -> Self {
        Self {
            min: config.advert_window_min,
            max: config.advert_window_max,
            small_size_bytes: config.advert_window_small_size_bytes,
            full_size_bytes: config.advert_window_full_size_bytes,
            delay_min: config.advert_delay_min,
            delay_max: config.advert_delay_max,
        }
    }

    /// Returns the initial peer count for `size` serialized bytes and records it in metrics.
    ///
    /// Blocks at or below `small_size_bytes` use `max`.
    /// Blocks at or above `full_size_bytes`, and blocks of unknown size, use `min`.
    /// Between these sizes, the window decreases linearly from `max` to `min`, rounded down.
    pub(crate) fn window(&self, size: Option<u32>) -> usize {
        let window = size.map_or(self.min, |size| {
            let size = u64::from(size).max(self.small_size_bytes);
            let below_full = self.full_size_bytes.saturating_sub(size);
            let size_range = self.full_size_bytes.saturating_sub(self.small_size_bytes);
            // Both factors fit in 64 bits, so the product cannot overflow.
            let above_min = self.max.saturating_sub(self.min) as u128 * u128::from(below_full)
                / u128::from(size_range.max(1));
            // `above_min` is at most `max - min`.
            usize::try_from(above_min).map_or(self.max, |above_min| self.min + above_min)
        });
        if let Some(size) = size {
            metrics::histogram!("peer.block_gossip.advert_block_size_bytes")
                .record(f64::from(size));
        }
        // Windows are at most a few hundred peers, so they convert exactly.
        #[allow(clippy::cast_precision_loss)]
        metrics::histogram!("peer.block_gossip.advert_window").record(window as f64);
        window
    }

    /// Returns the delay before announcing to peers outside the initial group.
    ///
    /// Unknown sizes use the maximum. Known sizes interpolate between the configured
    /// thresholds, with nanosecond precision and no floating-point rounding.
    pub(crate) fn delay(&self, size: Option<u32>) -> Duration {
        let Some(size) = size else {
            return self.delay_max;
        };
        let range = self
            .full_size_bytes
            .saturating_sub(self.small_size_bytes)
            .max(1);
        let above_small = u64::from(size)
            .saturating_sub(self.small_size_bytes)
            .min(range);
        let span = self.delay_max.saturating_sub(self.delay_min).as_nanos();
        // Divide before multiplying: Duration::MAX nanoseconds times u64::MAX can overflow.
        let range = u128::from(range);
        let fraction = u128::from(above_small);
        let nanos = (span / range) * fraction + (span % range) * fraction / range;
        let seconds =
            u64::try_from(nanos / 1_000_000_000).expect("the interpolated delay fits in Duration");
        let subsecond =
            u32::try_from(nanos % 1_000_000_000).expect("subsecond nanoseconds fit in u32");
        self.delay_min + Duration::new(seconds, subsecond)
    }
}

/// Every this many mined-block advertisements, a random peer replaces the next-lowest RTT.
///
/// Random peers keep the initial group from depending entirely on the lowest RTTs.
const MINED_RANDOM_INTERVAL: usize = 4;
/// Allow the kernel send queue to drain after the framed block write completes.
const BLOCK_WRITE_GRACE: Duration = Duration::from_millis(750);
/// Expire request tracking for peers that already have the block. This does not pace gossip.
pub(super) const BLOCK_REQUEST_WAIT: Duration = Duration::from_secs(2);
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
    Revoked,
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

/// Retain successful announcements through a broadcast and its timeout fallback.
const SUCCESSFUL_ADVERT_RETENTION: Duration = BLOCK_GOSSIP_TIMEOUT.saturating_mul(2);
/// Bounds duplicate-suppression history, not pending advertisements or uploads.
const SUCCESSFUL_ADVERT_LIMIT: usize = 128;

#[derive(Debug, Default)]
struct UploadRegistry {
    active: HashMap<block::Hash, Weak<watch::Sender<UploadState>>>,
    successful: indexmap::IndexMap<block::Hash, Instant>,
}

/// Tracks active uploads and remembers recent successful announcements for this connection.
#[derive(Clone, Debug, Default)]
pub(crate) struct BlockUploads(Arc<Mutex<UploadRegistry>>);

impl BlockUploads {
    fn register(&self, hash: block::Hash) -> (watch::Receiver<UploadState>, Option<UploadClaim>) {
        let mut uploads = self.0.lock().expect("block upload mutex is never poisoned");
        uploads.active.retain(|_, upload| upload.strong_count() > 0);
        uploads
            .successful
            .retain(|_, completed| completed.elapsed() < SUCCESSFUL_ADVERT_RETENTION);
        if let Some(upload) = uploads.active.get(&hash).and_then(Weak::upgrade) {
            return (upload.subscribe(), None);
        }
        if uploads.successful.contains_key(&hash) {
            return (watch::channel(UploadState::Finished(true)).1, None);
        }
        let (sender, receiver) = watch::channel(UploadState::Queued);
        let sender = Arc::new(sender);
        uploads.active.insert(hash, Arc::downgrade(&sender));
        (receiver, Some(UploadClaim(sender)))
    }

    /// Finalize the observer and its history under the same lock used by late failure events.
    fn finish(&self, hash: block::Hash, claim: &UploadClaim, succeeded: bool) -> bool {
        let mut uploads = self.0.lock().expect("block upload mutex is never poisoned");
        // Preserve a failure published after the final state check, including a disconnect
        // racing the end of the write grace.
        claim.0.send_if_modified(|state| {
            if matches!(state, UploadState::Finished(_) | UploadState::Revoked) {
                false
            } else {
                *state = UploadState::Finished(succeeded);
                true
            }
        });
        let succeeded = matches!(*claim.0.borrow(), UploadState::Finished(true));
        if succeeded {
            uploads.successful.shift_remove(&hash);
            if uploads.successful.len() == SUCCESSFUL_ADVERT_LIMIT {
                uploads.successful.shift_remove_index(0);
            }
            uploads.successful.insert(hash, Instant::now());
        }
        succeeded
    }

    fn update(&self, hash: block::Hash, state: UploadState) {
        let mut uploads = self.0.lock().expect("block upload mutex is never poisoned");
        if matches!(state, UploadState::Finished(false)) {
            uploads.successful.shift_remove(&hash);
        }
        if let Some(upload) = uploads.active.get(&hash).and_then(Weak::upgrade) {
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
        if let Some(upload) = uploads.active.get(&hash).and_then(Weak::upgrade) {
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
        let mut uploads = self.0.lock().expect("block upload mutex is never poisoned");
        for hash in hashes {
            // A late request can fail after its observer expires. Do not let old success
            // suppress a committed announcement when the early body was unavailable.
            uploads.successful.shift_remove(hash);
            if let Some(upload) = uploads.active.get(hash).and_then(Weak::upgrade) {
                upload.send_if_modified(|state| {
                    let requested = matches!(state, UploadState::Requested(_));
                    if requested {
                        *state = UploadState::Finished(false);
                    }
                    requested
                });
            }
        }
    }

    pub(crate) fn disconnected(&self) {
        let mut uploads = self.0.lock().expect("block upload mutex is never poisoned");
        uploads.successful.clear();
        for upload in uploads.active.values().filter_map(Weak::upgrade) {
            upload.send_if_modified(|state| {
                if matches!(state, UploadState::Finished(_) | UploadState::Revoked) {
                    false
                } else {
                    *state = UploadState::Finished(false);
                    true
                }
            });
        }
    }
}

struct UploadClaim(Arc<watch::Sender<UploadState>>);

impl Drop for UploadClaim {
    fn drop(&mut self) {
        self.0.send_if_modified(|state| {
            if matches!(state, UploadState::Finished(_) | UploadState::Revoked) {
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
    /// Address used for live authorization checks, absent for isolated connections.
    pub(crate) address: Option<PeerSocketAddr>,
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
    /// Returns false if the `inv` was not written, or if the peer requested the block and the
    /// upload failed or did not finish in time.
    #[cfg(test)]
    pub(crate) async fn advertise(self, hash: block::Hash) -> bool {
        matches!(self.advertise_outcome(hash).await, AdvertOutcome::Sent)
    }

    /// Advertises the block and distinguishes authorization revocation from delivery failure.
    async fn advertise_outcome(self, hash: block::Hash) -> AdvertOutcome {
        let mut retried = false;
        let (mut progress, claim) = loop {
            let (mut progress, claim) = self.uploads.register(hash);
            if let Some(claim) = claim {
                break (progress, claim);
            }
            // Early and committed announcements join an existing upload for this peer.
            let succeeded = loop {
                match *progress.borrow_and_update() {
                    UploadState::Finished(succeeded) => break succeeded,
                    UploadState::Revoked => return AdvertOutcome::Revoked,
                    _ => {}
                }
                if progress.changed().await.is_err() {
                    break false;
                }
            };
            if succeeded || retried || self.sender.is_closed() {
                return AdvertOutcome::from(succeeded);
            }
            // The joined upload can fail without reaching this peer, for example when early
            // inventory ends in `notfound`. Advertise once more after its owner releases the claim.
            while progress.changed().await.is_ok() {}
            retried = true;
        };
        let (result, written) = oneshot::channel();
        let advert = BlockAdvert { hash, result };
        // A sidecar learns the tip only from inventory, so it waits for its busy connection.
        // Other connections have a shorter inventory-write timeout.
        let inventory_wait = if self.sidecar {
            BLOCK_UPLOAD_WAIT
        } else {
            BLOCK_REQUEST_WAIT
        };
        // Dropping `written` makes the connection skip an advert that it has not started writing.
        let advertised = timeout(inventory_wait, async {
            self.sender.send(advert).await.ok()?;
            written.await.ok()
        })
        .await;
        match advertised {
            Ok(Some(Ok(()))) => {}
            Ok(Some(Err(BlockAdvertError::AuthorizationRevoked))) => {
                claim.0.send_replace(UploadState::Revoked);
                return AdvertOutcome::Revoked;
            }
            _ => return AdvertOutcome::Failed,
        }
        // The connection usually marks the write first; senders without a connection rely on this.
        claim.0.send_if_modified(UploadState::start_waiting);

        let request_deadline = Instant::now() + BLOCK_REQUEST_WAIT;
        let succeeded = loop {
            let state = *progress.borrow_and_update();
            let deadline = match state {
                UploadState::Queued => unreachable!("written inventory leaves the queued state"),
                UploadState::Waiting => request_deadline,
                UploadState::Requested(started) => started + BLOCK_UPLOAD_WAIT,
                UploadState::Written(written) => {
                    sleep_until(written + BLOCK_WRITE_GRACE).await;
                    // A disconnect during the grace can discard the body in the send queue.
                    break !matches!(*progress.borrow(), UploadState::Finished(false));
                }
                UploadState::Finished(succeeded) => break succeeded,
                UploadState::Revoked => return AdvertOutcome::Revoked,
            };
            tokio::select! {
                biased;
                changed = progress.changed() => if changed.is_err() { break false; },
                _ = sleep_until(deadline) => {
                    let reason = if matches!(state, UploadState::Waiting) { "no_request" } else { "upload_timeout" };
                    metrics::counter!("peer.block_gossip.tracking_timeout", "reason" => reason).increment(1);
                    // A peer that already has the block never requests it, so the advert still
                    // succeeded. A requested upload that did not finish was not delivered.
                    break matches!(state, UploadState::Waiting);
                }
            }
        };
        AdvertOutcome::from(self.uploads.finish(hash, &claim, succeeded))
    }
}

#[derive(Clone, Copy)]
enum AdvertOutcome {
    Sent,
    Revoked,
    Failed,
}

impl From<bool> for AdvertOutcome {
    fn from(sent: bool) -> Self {
        if sent {
            Self::Sent
        } else {
            Self::Failed
        }
    }
}

/// At least one selected connection lost authorization and none succeeded.
#[derive(Debug, thiserror::Error)]
#[error("block advertisement candidates lost authorization")]
struct RevokedCandidates;

/// Announces to sidecars and the initial group immediately, then all remaining peers at
/// `delayed_at`. Delivery tracking never gates another peer's announcement.
pub(crate) fn broadcast_block(
    peers: Vec<BlockGossipPeer>,
    hash: block::Hash,
    window: usize,
    mined: bool,
    delayed_at: Instant,
) -> BoxFuture<'static, Result<Response, BoxError>> {
    let mut initial = order_peers(peers, mined, &mut rand_10::rng());
    let sidecars = initial.iter().take_while(|peer| peer.sidecar).count();
    let delayed = initial.split_off(sidecars.saturating_add(window).min(initial.len()));
    async move {
        let advertise = |peers: Vec<BlockGossipPeer>| {
            futures::stream::iter(peers)
                .map(move |peer| peer.advertise_outcome(hash))
                .buffer_unordered(usize::MAX)
        };
        let delayed = futures::stream::once(async move {
            if !delayed.is_empty() && delayed_at > Instant::now() {
                sleep_until(delayed_at).await;
            }
            advertise(delayed)
        })
        .flatten();
        // Poll the initial group first, including sidecars, without waiting for delivery.
        let sends = futures::stream::select(advertise(initial), delayed);
        futures::pin_mut!(sends);
        let mut succeeded = false;
        let mut revoked = false;
        while let Some(outcome) = sends.next().await {
            succeeded |= matches!(outcome, AdvertOutcome::Sent);
            revoked |= matches!(outcome, AdvertOutcome::Revoked);
        }
        if succeeded {
            Ok(Response::Nil)
        } else if revoked {
            Err(Box::new(RevokedCandidates) as BoxError)
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
    /// Selects each broadcast's initial group and delay. Shared with the peer set.
    pub(crate) fanout: AdvertFanout,
    pub(crate) minimum_peer_version: MinimumPeerVersion<C>,
    pub(crate) bans: BannedIps,
    /// Acknowledged after the owner processes pending discovery and authorization events.
    pub(crate) refresh: Option<mpsc::Sender<oneshot::Sender<()>>>,
}

/// Always ready: an advertisement waits inside its own future for a connected peer, never for
/// an idle one. Requests other than block advertisements fail.
impl<C: ChainTip + Clone + Send + 'static> tower::Service<Request> for BlockAdvertiser<C> {
    type Response = Response;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Response, BoxError>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), BoxError>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        self.advertise(request)
    }
}

impl<C: ChainTip + Clone + Send + 'static> BlockAdvertiser<C> {
    /// Advertises the block in `request` to every eligible connected peer.
    ///
    /// Waits for an authorized connected peer, refreshing candidates after delivery failure. The
    /// caller owns the overall deadline and cancellation, including any wait for new peers.
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
        let mut minimum = self.minimum_peer_version.clone();
        let bans = self.bans.clone();
        let refresh = self.refresh.clone();
        let mut peers = self.peers.clone();
        let window = self.fanout.window(block.size);
        let delayed_at = Instant::now() + self.fanout.delay(block.size);
        async move {
            loop {
                if let Some(refresh) = &refresh {
                    let (done, ready) = oneshot::channel();
                    refresh.send(done).await.map_err(|_| "peer set stopped")?;
                    ready.await.map_err(|_| "peer set stopped")?;
                }
                let minimum_version = minimum.current();
                let candidates: Vec<_> = peers
                    .borrow_and_update()
                    .iter()
                    .filter(|peer| {
                        peer.version >= minimum_version
                            && !peer.sender.is_closed()
                            && !peer.address.is_some_and(|addr| bans.contains(addr.ip()))
                    })
                    .cloned()
                    .collect();
                if !candidates.is_empty() {
                    match broadcast_block(candidates, block.hash, window, mined, delayed_at).await {
                        Ok(response) => return Ok(response),
                        Err(error) => tracing::debug!(
                            hash = ?block.hash,
                            %error,
                            "waiting for new peers after block advertisement failed"
                        ),
                    }
                }
                // Do not replay the same rejected snapshot or reset the caller's deadline.
                peers.changed().await.map_err(|_| "peer set stopped")?;
            }
        }
        .boxed()
    }
}

/// Randomize relays and equal RTTs; put configured sidecars first.
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
    use crate::{peer_registry::PeerRegistry, AdvertisedBlock, ConnectedPeer};
    use futures::FutureExt;
    use rand_10::{rngs::StdRng, SeedableRng};

    /// The advert window these tests use. Tests of the size-based window use their own.
    const TEST_WINDOW: usize = 32;

    fn peer() -> (BlockGossipPeer, mpsc::Receiver<BlockAdvert>) {
        let (sender, receiver) = mpsc::channel(BLOCK_ADVERT_QUEUE);
        (
            BlockGossipPeer {
                sender,
                address: None,
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
        total: std::sync::atomic::AtomicUsize,
    }

    /// Like [`connected_peers`], but if `body_write` is set, each peer requests every advertised
    /// block at once, and its connection writes the body that long afterwards.
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
                        bodies.total.fetch_add(1, SeqCst);
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

        let (peers, mut writes) = connected_peers(TEST_WINDOW);
        let mut broadcasts = vec![tokio::spawn(broadcast_block(
            peers.clone(),
            parent,
            TEST_WINDOW,
            true,
            Instant::now() + Duration::from_millis(500),
        ))];
        if early_and_committed {
            broadcasts.push(tokio::spawn(broadcast_block(
                peers.clone(),
                parent,
                TEST_WINDOW,
                true,
                Instant::now() + Duration::from_millis(500),
            )));
        }
        for _ in 0..TEST_WINDOW {
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

        let mined = Instant::now();
        broadcasts.push(tokio::spawn(broadcast_block(
            peers,
            child,
            TEST_WINDOW,
            true,
            Instant::now() + Duration::from_millis(500),
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
    #[tokio::test(start_paused = true)]
    async fn fresh_block_inventory_does_not_wait_for_older_broadcasts() {
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
    async fn requested_upload_is_tracked_through_write_grace() {
        let hash = block::Hash([1; 32]);

        let (first, mut first_rx) = peer();
        let uploads = first.uploads.clone();
        let (second, mut second_rx) = peer();
        let first_task = tokio::spawn(first.advertise(hash));
        accept(&mut first_rx, hash).await;
        uploads.requested(hash);
        tokio::task::yield_now().await;
        // Another peer's inventory does not wait for the upload.
        let second_task = tokio::spawn(second.advertise(hash));
        accept(&mut second_rx, hash).await;
        tokio::time::advance(BLOCK_REQUEST_WAIT + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(second_task.await.unwrap(), "the unrequested advert ends");
        uploads.written(hash);
        tokio::task::yield_now().await;
        tokio::time::advance(BLOCK_WRITE_GRACE - Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert!(!first_task.is_finished());
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(first_task.await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn unrequested_inventory_tracking_expires() {
        let hash = block::Hash([2; 32]);

        let (first, mut first_rx) = peer();
        let task = tokio::spawn(first.advertise(hash));
        accept(&mut first_rx, hash).await;
        tokio::time::advance(BLOCK_REQUEST_WAIT - Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert!(!task.is_finished(), "the advert waits for a request");
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(task.await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn duplicate_announcements_share_upload_tracking() {
        let hash = block::Hash([3; 32]);

        let (peer, mut receiver) = peer();
        let first = tokio::spawn(peer.clone().advertise(hash));
        accept(&mut receiver, hash).await;
        let second = tokio::spawn(peer.clone().advertise(hash));
        tokio::task::yield_now().await;
        assert!(receiver.recv().now_or_never().is_none());
        peer.uploads.requested(hash);
        peer.uploads.written(hash);
        assert!(first.await.unwrap());
        assert!(second.await.unwrap());
        assert!(receiver.recv().now_or_never().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_fallback_skips_successful_peers_and_retries_unfinished_peer() {
        let registry = PeerRegistry::default();
        let mut guards = Vec::new();
        let measured = measured_peers(&registry, &mut guards, [Some(1), Some(2), Some(3)]);
        let (mut peers, mut writes) = connected_peers(2);
        let (slow, mut receiver) = peer();
        let uploads = slow.uploads.clone();
        peers.push(slow);
        for (peer, measured) in peers.iter_mut().zip(measured) {
            peer.registry = measured.registry;
        }
        let hash = block::Hash([83; 32]);
        let start = Instant::now();
        let first = tokio::spawn(timeout(
            BLOCK_GOSSIP_TIMEOUT,
            broadcast_block(
                peers.clone(),
                hash,
                2,
                true,
                start + Duration::from_secs(60),
            ),
        ));
        for _ in 0..2 {
            assert_eq!(writes.recv().await.unwrap().1, hash);
        }
        let advert = receiver.recv().await.unwrap();
        assert_eq!(start.elapsed(), Duration::from_secs(60));
        uploads.advertised(hash);
        uploads.requested(hash);
        advert.result.send(Ok(())).unwrap();
        assert!(first.await.unwrap().is_err());
        assert_eq!(start.elapsed(), BLOCK_GOSSIP_TIMEOUT);

        let fallback = tokio::spawn(timeout(
            BLOCK_GOSSIP_TIMEOUT,
            broadcast_block(
                peers,
                hash,
                2,
                true,
                Instant::now() + Duration::from_secs(60),
            ),
        ));
        let advert = receiver.recv().await.unwrap();
        assert_eq!(advert.hash, hash);
        uploads.advertised(hash);
        uploads.requested(hash);
        uploads.written(hash);
        advert.result.send(Ok(())).unwrap();
        assert!(fallback.await.unwrap().unwrap().is_ok());
        assert!(
            writes.try_recv().is_err(),
            "successful peers must not receive a replay"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn successful_advert_history_is_bounded_and_expires_without_limiting_fanout() {
        let (peers, mut writes) = connected_peers(1);
        let peer = &peers[0];
        let mut last = block::Hash([0; 32]);
        for index in 0..=SUCCESSFUL_ADVERT_LIMIT {
            let mut bytes = [0; 32];
            bytes[..8].copy_from_slice(&u64::try_from(index).unwrap().to_le_bytes());
            last = block::Hash(bytes);
            let task = tokio::spawn(peer.clone().advertise(last));
            assert_eq!(writes.recv().await.unwrap().1, last);
            // Finish uploads promptly so the burst stays within the retention period.
            peer.uploads.requested(last);
            peer.uploads.written(last);
            assert!(task.await.unwrap());
        }
        assert!(peer.clone().advertise(last).await);
        assert!(
            writes.try_recv().is_err(),
            "recent success suppresses a duplicate"
        );
        let evicted = block::Hash([0; 32]);
        let replay = tokio::spawn(peer.clone().advertise(evicted));
        assert_eq!(writes.recv().await.unwrap().1, evicted);
        assert!(replay.await.unwrap());

        tokio::time::advance(SUCCESSFUL_ADVERT_RETENTION).await;
        let expired = tokio::spawn(peer.clone().advertise(last));
        assert_eq!(writes.recv().await.unwrap().1, last);
        assert!(expired.await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn late_failed_request_invalidates_successful_advert_history() {
        for unavailable in [false, true] {
            let (peer, mut receiver) = peer();
            let hash = block::Hash([84; 32]);
            let early = tokio::spawn(peer.clone().advertise(hash));
            accept(&mut receiver, hash).await;
            assert!(early.await.unwrap());
            peer.uploads.requested(hash);
            if unavailable {
                peer.uploads.unavailable(hash);
            } else {
                peer.uploads.request_finished(&[hash]);
            }
            let committed = tokio::spawn(peer.advertise(hash));
            accept(&mut receiver, hash).await;
            assert!(committed.await.unwrap());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_wakes_duplicate() {
        let hash = block::Hash([4; 32]);

        let (peer, mut receiver) = peer();
        let first = tokio::spawn(peer.clone().advertise(hash));
        accept(&mut receiver, hash).await;
        let duplicate = tokio::spawn(peer.clone().advertise(hash));
        tokio::task::yield_now().await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        // The duplicate takes over the advertisement instead of reporting the cancelled one.
        accept(&mut receiver, hash).await;
        peer.uploads.requested(hash);
        peer.uploads.written(hash);
        assert!(duplicate.await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn joined_announcement_readvertises_after_unavailable_upload() {
        let hash = block::Hash([7; 32]);

        let (peer, mut receiver) = peer();
        let early = tokio::spawn(peer.clone().advertise(hash));
        accept(&mut receiver, hash).await;
        let committed = tokio::spawn(peer.clone().advertise(hash));
        tokio::task::yield_now().await;
        peer.uploads.requested(hash);
        peer.uploads.unavailable(hash);
        assert!(!early.await.unwrap());
        accept(&mut receiver, hash).await;
        peer.uploads.requested(hash);
        peer.uploads.written(hash);
        assert!(committed.await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn joined_announcement_does_not_retry_after_disconnect() {
        let hash = block::Hash([8; 32]);

        let (peer, mut receiver) = peer();
        let first = tokio::spawn(peer.clone().advertise(hash));
        accept(&mut receiver, hash).await;
        let duplicate = tokio::spawn(peer.clone().advertise(hash));
        tokio::task::yield_now().await;
        drop(receiver);
        peer.uploads.disconnected();
        assert!(!first.await.unwrap());
        assert!(!duplicate.await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn upload_timeout_and_disconnect_fail_tracking() {
        for disconnect in [false, true] {
            let hash = block::Hash([5; 32]);

            let (peer, mut receiver) = peer();
            let uploads = peer.uploads.clone();
            let task = tokio::spawn(peer.advertise(hash));
            accept(&mut receiver, hash).await;
            uploads.requested(hash);
            tokio::task::yield_now().await;
            if disconnect {
                uploads.disconnected();
            } else {
                tokio::time::advance(BLOCK_UPLOAD_WAIT).await;
            }
            assert!(!task.await.unwrap(), "the upload did not finish");
        }
    }

    /// A disconnect during the drain allowance can discard the written body, so the upload fails.
    #[tokio::test(start_paused = true)]
    async fn disconnect_during_write_grace_fails_the_upload() {
        let hash = block::Hash([19; 32]);

        let (peer, mut receiver) = peer();
        let uploads = peer.uploads.clone();
        let task = tokio::spawn(peer.advertise(hash));
        accept(&mut receiver, hash).await;
        uploads.requested(hash);
        uploads.written(hash);
        tokio::task::yield_now().await;
        uploads.disconnected();
        tokio::time::advance(BLOCK_WRITE_GRACE).await;
        assert!(!task.await.unwrap());
    }

    /// A newer block must not stop an older block's fan-out: the peers still need the parent, and
    /// the newer block might never become the tip.
    #[tokio::test(start_paused = true)]
    async fn newer_block_does_not_stop_older_broadcast() {
        let parent = block::Hash([13; 32]);
        let child = block::Hash([14; 32]);

        // A busy connection has not written the parent advert yet.
        let (busy, mut busy_rx) = peer();
        let queued = tokio::spawn(busy.advertise(parent));
        tokio::task::yield_now().await;

        // A parent broadcast with more peers than its window, which never request.
        let (peers, mut writes) = connected_peers(TEST_WINDOW + 8);
        let parent_broadcast = tokio::spawn(broadcast_block(
            peers.clone(),
            parent,
            TEST_WINDOW,
            true,
            Instant::now() + Duration::from_millis(500),
        ));
        for _ in 0..TEST_WINDOW {
            assert_eq!(writes.recv().await.unwrap().1, parent);
        }

        let mined = Instant::now();
        let child_broadcast = tokio::spawn(broadcast_block(
            peers.clone(),
            child,
            TEST_WINDOW,
            true,
            Instant::now() + Duration::from_millis(500),
        ));
        for _ in 0..TEST_WINDOW {
            assert_eq!(writes.recv().await.unwrap().1, child);
            assert_eq!(mined.elapsed(), Duration::ZERO, "the child waited");
        }
        accept(&mut busy_rx, parent).await;
        assert!(queued.await.unwrap(), "the queued parent advert is written");

        // Both broadcasts reach their remaining peers at their own configured deadlines.
        let (mut parents, mut children) = (TEST_WINDOW, TEST_WINDOW);
        while parents < peers.len() || children < peers.len() {
            let write = timeout(Duration::from_secs(60), writes.recv())
                .await
                .expect("each broadcast reaches every peer at its deadline");
            if write.unwrap().1 == parent {
                parents += 1;
            } else {
                children += 1;
            }
        }
        assert!(parent_broadcast.await.unwrap().is_ok());
        assert!(child_broadcast.await.unwrap().is_ok());
    }

    /// Starts a broadcast of a block of `size` bytes to `peers` with the fan-out policy in
    /// `config`, and returns the peers it advertised to before any advert finished.
    async fn first_wave(config: Config, peers: Vec<BlockGossipPeer>, size: u32) -> Vec<usize> {
        let fanout = AdvertFanout::from_config(&config);
        let (peers, mut writes) = peers_writing(peers);
        let block = AdvertisedBlock {
            size: Some(size),
            ..block::Hash([40; 32]).into()
        };
        let broadcast = tokio::spawn(broadcast_block(
            peers,
            block.hash,
            fanout.window(block.size),
            false,
            Instant::now() + Duration::from_millis(500),
        ));
        let mut wave = Vec::new();
        // Stop before the delayed group becomes eligible.
        while let Ok(Some((index, _))) = timeout(Duration::from_millis(250), writes.recv()).await {
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

    /// Explicit initial-group bounds for size-dependent scheduling tests.
    fn sized_window_config() -> Config {
        Config {
            advert_window_min: 32,
            advert_window_max: 64,
            ..Config::default()
        }
    }

    #[test]
    fn defaults_use_sized_initial_groups_without_delay() {
        let fanout = AdvertFanout::from_config(&Config::default());
        assert_eq!(fanout.window(Some(102_400)), 64);
        assert_eq!(fanout.window(Some(1_572_864)), 32);
        assert_eq!(fanout.window(None), 32);
        for size in [Some(0), Some(102_401), Some(u32::MAX), None] {
            assert_eq!(fanout.delay(size), Duration::ZERO);
        }
    }

    /// The window falls linearly from 64 at 100 KiB to 32 at 1.5 MiB.
    #[test]
    fn advert_window_follows_block_size() {
        /// A full NU7 block, just below the reference size.
        const FULL_NU7_BLOCK: u32 = 1_513_987;
        const FULL_SIZE: u32 = 1_572_864;
        const SMALL_SIZE: u32 = 102_400;
        const MID_SIZE: u32 = (SMALL_SIZE + FULL_SIZE) / 2;
        let fanout = AdvertFanout::from_config(&sized_window_config());

        assert_eq!(fanout.window(Some(FULL_NU7_BLOCK)), 33);
        assert_eq!(fanout.window(Some(FULL_SIZE)), 32);
        assert_eq!(fanout.window(Some(FULL_SIZE + 1)), 32);
        assert_eq!(fanout.window(Some(u32::MAX)), 32);
        assert_eq!(fanout.window(Some(0)), 64);
        assert_eq!(fanout.window(Some(SMALL_SIZE - 1)), 64);
        assert_eq!(fanout.window(Some(SMALL_SIZE)), 64);
        assert_eq!(fanout.window(Some(SMALL_SIZE + 1)), 63);
        assert_eq!(fanout.window(None), 32);
        // The mid-point is exact, and the result floors on either side of it.
        assert_eq!(fanout.window(Some(MID_SIZE)), 48);
        assert_eq!(fanout.window(Some(MID_SIZE - 1)), 48);
        assert_eq!(fanout.window(Some(MID_SIZE + 1)), 47);
    }

    /// Equal bounds give a fixed window, and large bounds do not overflow.
    #[test]
    fn advert_window_uses_configured_bounds() {
        let fanout = |min, max| {
            AdvertFanout::from_config(&Config {
                advert_window_min: min,
                advert_window_max: max,
                advert_window_full_size_bytes: u64::MAX,
                ..Config::default()
            })
        };
        for size in [Some(0), Some(1_500_000), Some(u32::MAX), None] {
            assert_eq!(fanout(9, 9).window(size), 9);
        }
        assert_eq!(fanout(1, usize::MAX).window(Some(0)), usize::MAX);
    }

    #[test]
    fn advert_delay_interpolates_and_clamps() {
        let fanout = AdvertFanout::from_config(&Config {
            advert_delay_min: Duration::from_millis(100),
            advert_delay_max: Duration::from_millis(500),
            advert_window_small_size_bytes: 100,
            advert_window_full_size_bytes: 1_000,
            ..sized_window_config()
        });
        for size in [0, 99, 100] {
            assert_eq!(fanout.delay(Some(size)), Duration::from_millis(100));
        }
        assert!(fanout.delay(Some(101)) > Duration::from_millis(100));
        assert_eq!(fanout.delay(Some(550)), Duration::from_millis(300));
        for size in [Some(1_000), Some(u32::MAX), None] {
            assert_eq!(fanout.delay(size), Duration::from_millis(500));
        }
        let extreme = AdvertFanout::from_config(&Config {
            advert_delay_max: Duration::MAX,
            advert_window_full_size_bytes: u64::MAX,
            ..sized_window_config()
        });
        assert!(extreme.delay(Some(u32::MAX)) < Duration::MAX);
        assert_eq!(extreme.delay(None), Duration::MAX);
    }

    /// Silent peers cannot create repeated request-timeout waves, regardless of block size,
    /// peer count, or whether the block was mined locally.
    #[tokio::test(start_paused = true)]
    async fn all_peers_are_announced_by_one_size_dependent_deadline() {
        for mined in [false, true] {
            for count in [16, 100, 300, 1_001] {
                for size in [
                    Some(102_399),
                    Some(102_400),
                    Some(102_401),
                    Some(837_632),
                    Some(1_572_864),
                    Some(u32::MAX),
                    None,
                ] {
                    let fanout = AdvertFanout::from_config(&Config {
                        advert_delay_min: Duration::from_millis(100),
                        advert_delay_max: Duration::from_millis(500),
                        ..sized_window_config()
                    });
                    let (peers, mut writes) = connected_peers(count);
                    let hash = block::Hash([0xd1; 32]);
                    let start = Instant::now();
                    let delay = fanout.delay(size);
                    let task = tokio::spawn(broadcast_block(
                        peers,
                        hash,
                        fanout.window(size),
                        mined,
                        start + delay,
                    ));
                    let mut seen = std::collections::HashSet::new();
                    for position in 0..count {
                        let (index, written) = writes.recv().await.unwrap();
                        assert_eq!(written, hash);
                        assert!(seen.insert(index), "peer {index} advertised twice");
                        let elapsed = start.elapsed();
                        if position < fanout.window(size) {
                            assert_eq!(elapsed, Duration::ZERO);
                        } else {
                            // Tokio timers have millisecond resolution.
                            assert!(
                                elapsed >= delay && elapsed <= delay + Duration::from_millis(1),
                                "{count} peers, {size:?}, mined={mined}: {elapsed:?} vs {delay:?}"
                            );
                        }
                    }
                    assert!(task.await.unwrap().is_ok());
                    assert!(
                        writes.try_recv().is_err(),
                        "tracking expiry started another wave"
                    );
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn zero_delay_announces_every_size_immediately() {
        for size in [Some(102_400), Some(102_401), Some(1_572_864), None] {
            let fanout = AdvertFanout::from_config(&sized_window_config());
            let (peers, mut writes) = connected_peers(300);
            let start = Instant::now();
            let task = tokio::spawn(broadcast_block(
                peers,
                block::Hash([0xd2; 32]),
                fanout.window(size),
                false,
                start + fanout.delay(size),
            ));
            for _ in 0..300 {
                writes.recv().await.unwrap();
                assert_eq!(start.elapsed(), Duration::ZERO);
            }
            assert!(task.await.unwrap().is_ok());
        }
    }

    /// Measure announcement latency and requested payload volume under the worst-case demand:
    /// every mock peer requests every block and its body write takes two seconds.
    #[tokio::test(start_paused = true)]
    #[allow(
        clippy::print_stderr,
        reason = "This test emits reproducible load measurements."
    )]
    async fn announcement_latency_and_upload_demand() {
        use std::sync::atomic::Ordering::SeqCst;
        for count in [64, 300, 1_001] {
            for size in [102_400, 102_401, 1_572_864] {
                for (min, max) in [(0, 0), (100, 500)] {
                    let fanout = AdvertFanout::from_config(&Config {
                        advert_delay_min: Duration::from_millis(min),
                        advert_delay_max: Duration::from_millis(max),
                        ..sized_window_config()
                    });
                    let (peers, mut writes, bodies) =
                        requesting_peers(count, Some(Duration::from_secs(2)));
                    let start = Instant::now();
                    let delay = fanout.delay(Some(size));
                    let initial = fanout.window(Some(size));
                    let task = tokio::spawn(broadcast_block(
                        peers,
                        block::Hash([0xd6; 32]),
                        initial,
                        false,
                        start + delay,
                    ));
                    for _ in 0..count {
                        writes.recv().await.unwrap();
                    }
                    let last = start.elapsed();
                    assert!(last <= delay + Duration::from_millis(1));
                    if count > initial {
                        assert!(last >= delay);
                    } else {
                        assert_eq!(last, Duration::ZERO);
                    }
                    assert!(task.await.unwrap().is_ok());
                    assert_eq!(bodies.total.load(SeqCst), count);
                    assert_eq!(bodies.current.load(SeqCst), 0);
                    assert_eq!(bodies.max.load(SeqCst), count);
                    // This measures requested payload volume, not socket throughput.
                    // usize fits in u128 on every supported target.
                    let payload_bytes = u128::from(size) * count as u128;
                    eprintln!(
                        "peers={count} size={size} delays={min}/{max}ms last={last:?} requested_payload={payload_bytes}B peak_bodies={count}"
                    );
                }
            }
        }
    }

    /// A tracking expiry before the configured delay must not release the delayed group early.
    #[tokio::test(start_paused = true)]
    async fn tracking_expiry_does_not_change_announcement_deadline() {
        let (peers, mut writes) = connected_peers(300);
        let start = Instant::now();
        let delay = BLOCK_REQUEST_WAIT + Duration::from_secs(1);
        let task = tokio::spawn(broadcast_block(
            peers,
            block::Hash([0xd3; 32]),
            32,
            false,
            start + delay,
        ));
        for position in 0..300 {
            writes.recv().await.unwrap();
            assert_eq!(
                start.elapsed(),
                if position < 32 { Duration::ZERO } else { delay }
            );
        }
        assert!(task.await.unwrap().is_ok());
        assert!(writes.try_recv().is_err());
    }

    /// Inventory writes that never complete cannot hold another peer's announcement.
    #[tokio::test(start_paused = true)]
    async fn stalled_inventory_writes_do_not_delay_other_peers() {
        let (queued_tx, mut queued) = mpsc::unbounded_channel();
        let peers = (0..300)
            .map(|index| {
                let (peer, mut receiver) = peer();
                let queued_tx = queued_tx.clone();
                tokio::spawn(async move {
                    let mut held = Vec::new();
                    while let Some(advert) = receiver.recv().await {
                        held.push(advert);
                        queued_tx.send(index).unwrap();
                    }
                });
                peer
            })
            .collect();
        let start = Instant::now();
        let delay = Duration::from_millis(500);
        let task = tokio::spawn(broadcast_block(
            peers,
            block::Hash([0xd4; 32]),
            32,
            false,
            start + delay,
        ));
        let mut seen = std::collections::HashSet::new();
        for position in 0..300 {
            assert!(seen.insert(queued.recv().await.unwrap()));
            assert_eq!(
                start.elapsed(),
                if position < 32 { Duration::ZERO } else { delay }
            );
        }
        assert!(
            task.await.unwrap().is_err(),
            "no connection wrote its inventory"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_uploads_and_joined_broadcasts_do_not_delay_other_peers() {
        let (peers, mut writes, bodies) =
            requesting_peers(300, Some(BLOCK_UPLOAD_WAIT + Duration::from_secs(1)));
        let hash = block::Hash([0xd5; 32]);
        let start = Instant::now();
        let deadline = start + Duration::from_millis(500);
        let first = tokio::spawn(broadcast_block(peers.clone(), hash, 32, false, deadline));
        for _ in 0..32 {
            writes.recv().await.unwrap();
            assert_eq!(start.elapsed(), Duration::ZERO);
        }
        // All peers enter the joined broadcast immediately. Existing claims must not block
        // its attempts to announce to other peers.
        let joined = tokio::spawn(broadcast_block(peers, hash, 32, false, start));
        for _ in 32..300 {
            writes.recv().await.unwrap();
            assert_eq!(start.elapsed(), Duration::ZERO);
        }
        assert_eq!(
            bodies.current.load(std::sync::atomic::Ordering::SeqCst),
            300
        );
        tokio::time::advance(Duration::from_millis(500)).await;
        tokio::task::yield_now().await;
        assert!(
            writes.try_recv().is_err(),
            "the original delayed group must join existing claims"
        );
        first.abort();
        joined.abort();
    }

    /// A full connection queue holds only that peer's adverts. It can drain a burst before
    /// expiry, or skip expired adverts and accept later blocks.
    #[tokio::test(start_paused = true)]
    async fn block_burst_isolates_full_peer_queue_and_recovers() {
        for expire in [false, true] {
            let (slow, mut receiver) = peer();
            let (mut peers, mut writes) = connected_peers(1);
            peers.push(slow.clone());
            let start = Instant::now();
            let hashes: Vec<_> = (0..6).map(|index| block::Hash([index; 32])).collect();
            assert!(hashes.len() > BLOCK_ADVERT_QUEUE);
            let mut broadcasts = Vec::new();
            for hash in &hashes {
                broadcasts.push(tokio::spawn(broadcast_block(
                    peers.clone(),
                    *hash,
                    1,
                    false,
                    start,
                )));
            }
            let mut healthy = std::collections::HashSet::new();
            for _ in &hashes {
                assert!(healthy.insert(writes.recv().await.unwrap().1));
                assert_eq!(start.elapsed(), Duration::ZERO);
            }
            assert_eq!(receiver.len(), BLOCK_ADVERT_QUEUE);

            if expire {
                tokio::time::advance(BLOCK_REQUEST_WAIT + Duration::from_millis(1)).await;
                for broadcast in broadcasts {
                    assert!(
                        broadcast.await.unwrap().is_ok(),
                        "the healthy peer succeeded"
                    );
                }
                for _ in 0..BLOCK_ADVERT_QUEUE {
                    assert!(receiver.try_recv().unwrap().result.is_closed());
                }
                assert!(receiver.try_recv().is_err(), "waiting sends expired too");
            } else {
                let mut recovered = std::collections::HashSet::new();
                for _ in &hashes {
                    let advert = receiver.recv().await.unwrap();
                    assert!(recovered.insert(advert.hash));
                    advert.result.send(Ok(())).unwrap();
                    assert_eq!(start.elapsed(), Duration::ZERO);
                }
                assert_eq!(recovered, healthy);
                for broadcast in broadcasts {
                    assert!(broadcast.await.unwrap().is_ok());
                }
            }

            let next = block::Hash([6; 32]);
            let fresh = tokio::spawn(slow.advertise(next));
            accept(&mut receiver, next).await;
            assert!(fresh.await.unwrap());
        }
    }

    /// Custom size boundaries control both plateaus and the interpolation between them.
    #[test]
    fn advert_window_uses_configured_sizes() {
        let fanout = AdvertFanout::from_config(&Config {
            advert_window_min: 4,
            advert_window_max: 12,
            advert_window_small_size_bytes: 100,
            advert_window_full_size_bytes: 1_000,
            ..Config::default()
        });
        for size in [0, 99, 100] {
            assert_eq!(fanout.window(Some(size)), 12);
        }
        assert_eq!(fanout.window(Some(101)), 11);
        assert_eq!(fanout.window(Some(550)), 8);
        for size in [1_000, 1_001, u32::MAX] {
            assert_eq!(fanout.window(Some(size)), 4);
        }
        assert_eq!(fanout.window(None), 4);
    }

    /// Broadcasts advertise to the window's peers at once, up to the peers they have.
    #[tokio::test(start_paused = true)]
    async fn first_wave_follows_block_size() {
        let wave = |peers, size| first_wave(sized_window_config(), plain_peers(peers), size);
        assert_eq!(wave(100, 1_513_987).await.len(), 33);
        assert_eq!(wave(100, 10_000_000).await.len(), 32);
        assert_eq!(wave(100, 2_000).await.len(), 64);
        assert_eq!(wave(100, 102_400).await.len(), 64);
        assert_eq!(wave(100, 102_401).await.len(), 63);
        assert_eq!(wave(40, 2_000).await.len(), 40);
    }

    /// Sidecars beyond the window are all advertised at once, outside the window.
    #[tokio::test(start_paused = true)]
    async fn sidecars_beyond_the_window_go_first() {
        // More sidecars than the minimum window of 32, which a 10 MB block uses.
        const SIDECARS: usize = 40;
        let mut peers = plain_peers(SIDECARS + 40);
        for peer in &mut peers[..SIDECARS] {
            peer.sidecar = true;
        }
        let wave = first_wave(sized_window_config(), peers, 10_000_000).await;
        assert_eq!(
            wave.len(),
            SIDECARS + 32,
            "every sidecar plus a minimum window: {wave:?}"
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

        let (peer, mut receiver) = peer();
        let early = tokio::spawn(peer.clone().advertise(hash));
        tokio::task::yield_now().await;
        let committed = tokio::spawn(peer.clone().advertise(hash));
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

        let (peer, mut receiver) = peer();
        let early = tokio::spawn(broadcast_block(
            vec![peer.clone()],
            hash,
            TEST_WINDOW,
            true,
            Instant::now() + Duration::from_millis(500),
        ));
        let advert = receiver.recv().await.unwrap();
        advert
            .result
            .send(Err(BlockAdvertError::AuthorizationRevoked))
            .unwrap();
        assert!(early.await.unwrap().is_err());
        let committed = tokio::spawn(broadcast_block(
            vec![peer],
            hash,
            TEST_WINDOW,
            true,
            Instant::now() + Duration::from_millis(500),
        ));
        accept(&mut receiver, hash).await;
        tokio::time::advance(BLOCK_REQUEST_WAIT).await;
        assert!(committed.await.unwrap().is_ok());
    }

    /// Sustained relay: a block every second to peers that request every block, with bodies
    /// that take two seconds to write. Every peer receives inventory within the configured delay.
    #[tokio::test(start_paused = true)]
    async fn sustained_relay_does_not_wait_for_uploads() {
        const BLOCKS: u8 = 8;

        let (peers, mut writes, bodies) =
            requesting_peers(2 * TEST_WINDOW, Some(Duration::from_secs(2)));
        let mut broadcasts = Vec::new();
        for block in 0..BLOCKS {
            let hash = block::Hash([0xc0 + block; 32]);
            let mined = Instant::now();
            broadcasts.push(tokio::spawn(broadcast_block(
                peers.clone(),
                hash,
                TEST_WINDOW,
                false,
                Instant::now() + Duration::from_millis(500),
            )));
            let next_block = mined + Duration::from_secs(1);
            let mut first = None;
            let mut count = 0;
            while let Ok(Some((_, written))) =
                tokio::time::timeout_at(next_block, writes.recv()).await
            {
                if written == hash {
                    first.get_or_insert(mined.elapsed());
                    count += 1;
                }
            }
            assert_eq!(first, Some(Duration::ZERO), "block {block} waited");
            assert_eq!(
                count,
                peers.len(),
                "block {block} did not finish announcing"
            );
        }
        let max_bodies = bodies.max.load(std::sync::atomic::Ordering::SeqCst);
        // A body write lasts two block intervals; at most three broadcasts overlap.
        assert!(
            max_bodies <= 3 * peers.len(),
            "{max_bodies} concurrent body writes"
        );
        broadcasts.iter().for_each(|broadcast| broadcast.abort());
    }

    #[tokio::test(start_paused = true)]
    async fn unavailable_early_block_can_be_advertised_again_after_commit() {
        let hash = block::Hash([6; 32]);

        let (peer, mut receiver) = peer();
        let early = tokio::spawn(peer.clone().advertise(hash));
        accept(&mut receiver, hash).await;
        peer.uploads.requested(hash);
        peer.uploads.unavailable(hash);
        assert!(!early.await.unwrap());
        let committed = tokio::spawn(peer.clone().advertise(hash));
        accept(&mut receiver, hash).await;
        peer.uploads.requested(hash);
        peer.uploads.written(hash);
        assert!(committed.await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn events_before_inventory_do_not_finish_the_advertisement() {
        let hash = block::Hash([9; 32]);

        let (peer, mut receiver) = peer();
        let task = tokio::spawn(peer.clone().advertise(hash));
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
    }

    #[tokio::test(start_paused = true)]
    async fn unwritten_requested_hash_fails_when_the_request_finishes() {
        let hash = block::Hash([10; 32]);

        let (peer, mut receiver) = peer();
        let task = tokio::spawn(peer.clone().advertise(hash));
        accept(&mut receiver, hash).await;
        peer.uploads.requested(hash);
        // The inbound service truncated the request before reaching this hash.
        peer.uploads.request_finished(&[hash]);
        assert!(!task.await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn only_sidecars_wait_for_busy_connections_to_write_inventory() {
        for sidecar in [false, true] {
            let hash = block::Hash([11; 32]);

            let (mut peer, mut receiver) = peer();
            peer.sidecar = sidecar;
            let task = tokio::spawn(peer.advertise(hash));
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
                    start_height: zakura_chain::block::Height(0),
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
    #[tokio::test(start_paused = true)]
    async fn revoked_snapshot_waits_for_later_authorized_connection() {
        timeout(Duration::from_secs(10), async {
            let (first, mut first_rx) = peer();
            let (published, peers) = watch::channel(vec![first]);

            let advertiser = BlockAdvertiser {
                fanout: AdvertFanout::from_config(&Config {
                    advert_window_min: 1,
                    advert_window_max: 2,
                    advert_window_small_size_bytes: 0,
                    advert_window_full_size_bytes: 100,
                    advert_delay_min: Duration::from_secs(1),
                    advert_delay_max: Duration::from_secs(1),
                    ..Config::default()
                }),
                peers,

                minimum_peer_version: MinimumPeerVersion::new(
                    zakura_chain::chain_tip::NoChainTip,
                    &zakura_chain::parameters::Network::Mainnet,
                ),
                bans: BannedIps::default(),
                refresh: None,
            };
            let hash = block::Hash([61; 32]);
            let mut broadcast = tokio::spawn(tower::ServiceExt::oneshot(
                advertiser,
                Request::AdvertiseMinedBlock(AdvertisedBlock::new(hash, 0)),
            ));
            let advert = first_rx.recv().await.unwrap();
            advert
                .result
                .send(Err(BlockAdvertError::AuthorizationRevoked))
                .unwrap();
            tokio::task::yield_now().await;
            assert!((&mut broadcast).now_or_never().is_none());
            assert!(
                first_rx.try_recv().is_err(),
                "the rejected snapshot is not retried"
            );
            tokio::time::advance(Duration::from_millis(750)).await;
            let (later, mut writes) = connected_peers(3);
            published.send_replace(later);
            for _ in 0..2 {
                assert_eq!(writes.recv().await.unwrap().1, hash);
            }
            tokio::task::yield_now().await;
            assert!(
                writes.try_recv().is_err(),
                "the refreshed broadcast keeps the size-based window of two"
            );
            let refreshed = Instant::now();
            assert_eq!(writes.recv().await.unwrap().1, hash);
            assert_eq!(refreshed.elapsed(), Duration::from_millis(250));
            assert!(broadcast.await.unwrap().is_ok());
        })
        .await
        .expect("the pending announcement reaches a later authorized peer");
    }

    #[tokio::test(start_paused = true)]
    async fn failed_snapshot_retries_replacement_without_resetting_deadline() {
        for replace_before_failure in [false, true] {
            let (first, mut first_rx) = peer();
            let (published, peers) = watch::channel(vec![first]);
            let advertiser = BlockAdvertiser {
                fanout: AdvertFanout::from_config(&Config {
                    advert_window_min: 1,
                    advert_window_max: 1,
                    advert_delay_min: Duration::from_secs(1),
                    advert_delay_max: Duration::from_secs(1),
                    ..Config::default()
                }),
                peers,
                minimum_peer_version: MinimumPeerVersion::new(
                    zakura_chain::chain_tip::NoChainTip,
                    &zakura_chain::parameters::Network::Mainnet,
                ),
                bans: BannedIps::default(),
                refresh: None,
            };
            let start = Instant::now();
            let hash = block::Hash([82; 32]);
            let broadcast = tokio::spawn(tower::ServiceExt::oneshot(
                advertiser,
                Request::AdvertiseMinedBlock(hash.into()),
            ));
            let advert = first_rx.recv().await.unwrap();
            let (later, mut writes) = connected_peers(2);
            if replace_before_failure {
                published.send_replace(later.clone());
            }
            drop(first_rx);
            drop(advert);
            tokio::task::yield_now().await;
            if !replace_before_failure {
                assert!(!broadcast.is_finished());
                tokio::time::advance(Duration::from_millis(750)).await;
                published.send_replace(later);
            }
            assert_eq!(writes.recv().await.unwrap().1, hash);
            assert_eq!(writes.recv().await.unwrap().1, hash);
            assert_eq!(start.elapsed(), Duration::from_secs(1));
            assert!(broadcast.await.unwrap().is_ok());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn joined_advertisements_preserve_revocation() {
        timeout(Duration::from_secs(1), async {
            let (peer, mut receiver) = peer();

            let hash = block::Hash([62; 32]);
            let first = tokio::spawn(peer.clone().advertise_outcome(hash));
            let advert = receiver.recv().await.unwrap();
            let second = tokio::spawn(peer.advertise_outcome(hash));
            tokio::task::yield_now().await;
            advert
                .result
                .send(Err(BlockAdvertError::AuthorizationRevoked))
                .unwrap();
            assert!(matches!(first.await.unwrap(), AdvertOutcome::Revoked));
            assert!(matches!(second.await.unwrap(), AdvertOutcome::Revoked));
            assert!(receiver.try_recv().is_err());
        })
        .await
        .expect("joined rejections finish promptly");
    }
    #[tokio::test(start_paused = true)]
    async fn waiting_advert_rechecks_version_and_existing_bans() {
        timeout(Duration::from_secs(10), async {
            let (minimum, tip) = MinimumPeerVersion::with_mock_chain_tip(
                &zakura_chain::parameters::Network::Mainnet,
            );
            let old_version = crate::protocol::external::types::Version::min_remote_for_height(
                &zakura_chain::parameters::Network::Mainnet,
                None,
            );
            let (published, peers) = watch::channel(Vec::new());
            let banned: PeerSocketAddr = "127.0.0.2:8233"
                .parse::<std::net::SocketAddr>()
                .unwrap()
                .into();
            let advertiser = BlockAdvertiser {
                fanout: AdvertFanout::from_config(&Config::default()),
                peers,

                minimum_peer_version: minimum,
                bans: BannedIps::with_banned_ip(banned.ip()),
                refresh: None,
            };
            let hash = block::Hash([72; 32]);
            let broadcast = tokio::spawn(tower::ServiceExt::oneshot(
                advertiser,
                Request::AdvertiseMinedBlock(hash.into()),
            ));
            tokio::task::yield_now().await;
            let height = zakura_chain::parameters::NetworkUpgrade::Nu6_3
                .activation_height(&zakura_chain::parameters::Network::Mainnet)
                .unwrap();
            tip.send_best_tip_height(Some(height));
            let (mut outdated, mut outdated_rx) = peer();
            outdated.version = old_version;
            let (mut forbidden, mut forbidden_rx) = peer();
            forbidden.address = Some(banned);
            let (allowed, mut allowed_rx) = peer();
            published.send_replace(vec![outdated, forbidden, allowed]);
            accept(&mut allowed_rx, hash).await;
            assert!(outdated_rx.try_recv().is_err());
            assert!(forbidden_rx.try_recv().is_err());
            assert!(broadcast.await.unwrap().is_ok());
        })
        .await
        .expect("selection uses current authorization after waiting");
    }
    #[tokio::test(start_paused = true)]
    async fn revoked_wait_obeys_caller_deadline() {
        let (peer, mut receiver) = peer();
        let (_published, peers) = watch::channel(vec![peer]);

        let advertiser = BlockAdvertiser {
            fanout: AdvertFanout::from_config(&Config::default()),
            peers,

            minimum_peer_version: MinimumPeerVersion::new(
                zakura_chain::chain_tip::NoChainTip,
                &zakura_chain::parameters::Network::Mainnet,
            ),
            bans: BannedIps::default(),
            refresh: None,
        };
        let hash = block::Hash([73; 32]);
        let deadline = timeout(
            Duration::from_secs(1),
            tower::ServiceExt::oneshot(advertiser, Request::AdvertiseMinedBlock(hash.into())),
        );
        let reject = async {
            let advert = receiver.recv().await.unwrap();
            advert
                .result
                .send(Err(BlockAdvertError::AuthorizationRevoked))
                .unwrap();
        };
        let (result, ()) = tokio::join!(deadline, reject);
        assert!(result.is_err());
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn disconnect_preserves_a_completed_authorization_rejection() {
        let uploads = BlockUploads::default();
        let (progress, claim) = uploads.register(block::Hash([74; 32]));
        let claim = claim.unwrap();
        claim.0.send_replace(UploadState::Revoked);
        uploads.disconnected();
        assert!(matches!(*progress.borrow(), UploadState::Revoked));
    }
}
