//! A task that gossips newly verified [`block::Hash`]es to peers.
//!
//! Each peer-to-peer protocol keeps the latest committed best chain tip waiting to be announced,
//! and a short queue of blocks this node mined. A newer tip replaces the one waiting rather than
//! queueing behind it, because a peer that missed the older block catches up through ordinary
//! sync, while a peer that is sent a run of buried blocks can lose the one block it actually
//! needs among them. A mined block that commits buries every own block below its height the same
//! way, but blocks at one height are independent: two competing solutions carry no ancestry
//! between them, so each keeps its own announcement.
//!
//! So a node with nobody to announce to holds one wait for a peer per protocol, not one per
//! block, and the first peer to connect is sent the current tip and this node's own newest mined
//! blocks instead of the whole backlog.
//!
//! Announcements finish in any order, so a late completion of an older block never displaces a
//! newer block's waiting announcement or its delivery record. Each protocol sends a bounded
//! number of announcements at once; the rest wait their turn.
//!
//! [`block::Hash`]: zakura_chain::block::Hash

use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    time::Duration,
};

use futures::{future::BoxFuture, FutureExt, TryFutureExt};
use thiserror::Error;
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};
use tower::{Service, ServiceExt};
use tracing::Instrument;

use zakura_chain::{block, chain_tip::ChainTip};
use zakura_network as zn;
use zakura_rpc::{MinedBlockEvent, MinedBlockSubmission, PendingBlockSignal};
use zakura_state::{self as zs, ChainTipChange};

use crate::{components::sync::SyncStatus, BoxError};

use BlockGossipError::*;

/// Bounds a full paced broadcast, including several upload waves.
pub(super) const BLOCK_GOSSIP_TIMEOUT: Duration = Duration::from_secs(90);

/// Bounds the state read of a committed tip's size. A tip block is in memory, so this read is
/// fast; if it is slow, the broadcast proceeds without the size rather than waiting.
const BLOCK_SIZE_READ_TIMEOUT: Duration = Duration::from_millis(100);

/// How many of this node's own blocks one protocol keeps waiting for a peer.
///
/// Blocks at one height are independent, so each needs its own announcement. The RPC admits at
/// most this many submissions at once, so a longer backlog can only hold blocks that a newer
/// committed one has buried.
pub(super) const MAX_WAITING_MINED: usize = 16;

/// How many announcements one protocol sends at once.
///
/// With a peer connected, every block is sent as soon as it arrives, so this only matters for a
/// run of submissions faster than their announcements finish: later ones wait their turn instead
/// of each starting a task.
pub(super) const MAX_ACTIVE_BROADCASTS: usize = 16;

/// Which waiting announcement a block replaces.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Slot {
    /// The committed best chain tip.
    Tip,

    /// A block this node mined, which need not be the best chain tip.
    Mined,
}

/// A block this node submitted, which is not committed yet.
#[derive(Clone, Debug)]
struct EarlyMined {
    /// Reports whether the block is still valid, so a block that fails contextual verification
    /// after it was announced stops being announced.
    pending: PendingBlockSignal,

    /// When the block was submitted, for the submission latency metric.
    submitted_at: std::time::Instant,

    /// Which RPC method submitted the block.
    submission: MinedBlockSubmission,
}

/// A block announcement waiting for a peer that can receive it.
#[derive(Clone, Debug)]
struct Announcement {
    /// The waiting announcement this one replaces.
    slot: Slot,

    hash: block::Hash,

    height: block::Height,

    /// The block's serialized size, read from the state at dispatch when it is not known here.
    size: Option<usize>,

    /// Sent as a mined block, which peers fetch from their lowest-latency connections first.
    mined: bool,

    /// Set while this node's own block has been submitted but not committed.
    early: Option<EarlyMined>,

    /// Counts towards the committed-tip suppression and its one retry.
    tracked: bool,

    /// When this announcement stops being worth sending.
    deadline: Instant,
}

impl Announcement {
    /// Returns an announcement of the committed best chain tip.
    fn tip(hash: block::Hash, height: block::Height) -> Self {
        Self {
            slot: Slot::Tip,
            hash,
            height,
            size: None,
            mined: false,
            early: None,
            tracked: false,
            deadline: Instant::now() + BLOCK_GOSSIP_TIMEOUT,
        }
    }

    /// Returns an announcement of a block this node mined.
    ///
    /// A submitted block that is not committed yet is not tracked: its announcement must not
    /// satisfy the announcement of the same block once it commits.
    fn mined(
        hash: block::Hash,
        height: block::Height,
        size: usize,
        early: Option<EarlyMined>,
    ) -> Self {
        Self {
            slot: Slot::Mined,
            hash,
            height,
            size: Some(size),
            mined: true,
            tracked: early.is_none(),
            early,
            deadline: Instant::now() + BLOCK_GOSSIP_TIMEOUT,
        }
    }

    /// Returns the one retry of a committed mined block whose announcement failed.
    ///
    /// Untracked, so the retry does not schedule another retry of its own.
    fn retry(hash: block::Hash, height: block::Height, size: Option<usize>) -> Self {
        Self {
            slot: Slot::Tip,
            hash,
            height,
            size,
            mined: true,
            early: None,
            tracked: false,
            deadline: Instant::now() + BLOCK_GOSSIP_TIMEOUT,
        }
    }

    /// Logs the block this node is about to announce.
    fn log_accepted(&self) {
        let message = match (self.slot, self.early.is_some()) {
            (Slot::Mined, true) => "announcing this node's submitted block",
            (Slot::Mined, false) => "announcing this node's committed block",
            (Slot::Tip, _) => "announcing the committed tip",
        };
        info!(
            height = ?self.height,
            hash = ?self.hash,
            size = ?self.size,
            "{message}",
        );
    }

    /// Returns whether this block is still worth announcing at `now`.
    fn is_current(&self, now: Instant) -> bool {
        now < self.deadline
            && self
                .early
                .as_ref()
                .is_none_or(|early| early.pending.is_valid())
    }
}

/// How a dispatched announcement ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    /// At least one peer was advertised to.
    Delivered,

    /// The advertisement failed, or ran out of time.
    Failed,

    /// Every connected peer had gone by the time the advertisement was sent, so nothing was sent.
    NoPeers,
}

/// A dispatched announcement finished.
#[derive(Debug)]
struct BroadcastCompleted {
    /// Which protocol sent it.
    protocol: usize,

    outcome: Outcome,

    /// The announcement, so it can go back to waiting when no peer was left to send it to.
    announcement: Announcement,
}

/// One peer-to-peer protocol's block announcements.
struct Protocol<ZN> {
    /// The protocol's name, for logs.
    name: &'static str,

    /// Advertises blocks to this protocol's peers. Ready once one of them can receive an
    /// advertisement.
    service: ZN,

    /// The latest committed best chain tip waiting for a peer.
    tip: Option<Announcement>,

    /// The blocks this node mined waiting for a peer, oldest first, with distinct hashes and at
    /// most [`MAX_WAITING_MINED`] of them.
    mined: VecDeque<Announcement>,

    /// The height of the newest block this node mined and committed. Every own block below it is
    /// buried, so it is neither kept waiting nor put back after a dispatch.
    newest_committed_mined: Option<block::Height>,

    /// The wait for a peer, while this protocol has an announcement to send.
    ready: Option<BoxFuture<'static, Result<ZN, BoxError>>>,

    /// Announcements dispatched and not reported back yet.
    active: usize,

    /// Committed mined-block announcements, so the committed-tip path does not duplicate them.
    broadcasts: MinedBlockBroadcasts,

    /// Set once this protocol's service stops answering, which ends its announcements.
    stopped: bool,
}

impl<ZN> Protocol<ZN>
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + Clone + 'static,
    ZN::Future: Send,
{
    fn new(name: &'static str, service: ZN) -> Self {
        Self {
            name,
            service,
            tip: None,
            mined: VecDeque::new(),
            newest_committed_mined: None,
            ready: None,
            active: 0,
            broadcasts: MinedBlockBroadcasts::default(),
            stopped: false,
        }
    }

    /// Returns whether this protocol has an announcement waiting for a peer.
    fn is_waiting(&self) -> bool {
        self.tip.is_some() || !self.mined.is_empty()
    }

    /// Returns whether a newer committed block of this node's own has buried `announcement`.
    fn is_buried(&self, announcement: &Announcement) -> bool {
        self.newest_committed_mined
            .is_some_and(|newest| announcement.height < newest)
    }

    /// Returns whether a block this node mined is waiting to be announced.
    fn is_waiting_mined(&self, hash: &block::Hash) -> bool {
        self.mined.iter().any(|waiting| waiting.hash == *hash)
    }

    /// Accepts the committed best chain tip.
    ///
    /// A tip this protocol is already announcing as a mined block, waiting or sent, is not
    /// accepted, and the tip still waiting is dropped, since that announcement covers the tip.
    /// The one retry of a failed mined announcement keeps its mined priority over the same
    /// block's tip event.
    fn accept_tip(&mut self, announcement: Announcement) {
        if self.stopped {
            return;
        }
        if self.is_waiting_mined(&announcement.hash)
            || self.broadcasts.is_in_flight(&announcement.hash)
            || self.broadcasts.was_delivered(&announcement.hash)
        {
            debug!(
                height = ?announcement.height,
                hash = ?announcement.hash,
                protocol = self.name,
                "skipping committed block announcement: it is announced as a mined block",
            );
            // The mined announcement is this protocol's announcement of the tip, so whatever tip
            // was still waiting is older and obsolete.
            self.tip = None;
            return;
        }
        if self
            .tip
            .as_ref()
            .is_some_and(|waiting| waiting.hash == announcement.hash && waiting.mined)
        {
            debug!(
                height = ?announcement.height,
                hash = ?announcement.hash,
                protocol = self.name,
                "keeping the mined retry of the committed tip",
            );
            return;
        }
        self.tip = Some(announcement);
    }

    /// Accepts a block this node mined.
    ///
    /// A committed block buries every waiting own block below its height, and a block below the
    /// newest committed one is not accepted: it is either an ancestor the newer block implies, or
    /// on a fork this node left when it committed the newer block. Blocks at one height are kept
    /// together, since neither implies the other. A committed block takes the place of its own
    /// submission. When the queue is full, the oldest waiting block makes room.
    fn accept_mined(&mut self, announcement: Announcement) {
        if self.stopped {
            return;
        }
        self.prune(Instant::now());
        if self.is_buried(&announcement) {
            debug!(
                height = ?announcement.height,
                hash = ?announcement.hash,
                protocol = self.name,
                "dropping a mined block announcement: a newer block of this node's own committed",
            );
            return;
        }
        if announcement.early.is_none() {
            self.newest_committed_mined = Some(announcement.height);
            let name = self.name;
            self.mined.retain(|waiting| {
                let buried = waiting.height < announcement.height;
                if buried {
                    debug!(
                        height = ?waiting.height,
                        hash = ?waiting.hash,
                        protocol = name,
                        "dropping a waiting mined block announcement: a newer block committed",
                    );
                }
                !buried
            });
        }
        if let Some(waiting) = self
            .mined
            .iter_mut()
            .find(|waiting| waiting.hash == announcement.hash)
        {
            *waiting = announcement;
            return;
        }
        if self.mined.len() >= MAX_WAITING_MINED {
            if let Some(oldest) = self.mined.pop_front() {
                debug!(
                    height = ?oldest.height,
                    hash = ?oldest.hash,
                    protocol = self.name,
                    "dropping the oldest waiting mined block announcement: too many are waiting",
                );
            }
        }
        self.mined.push_back(announcement);
    }

    /// Takes the next announcement to send.
    ///
    /// This node's own blocks go first, oldest first: a miner's next template waits on them. The
    /// tip waiting as a mined block too is one announcement.
    fn take_next(&mut self) -> Option<Announcement> {
        let next = self.mined.pop_front().or_else(|| self.tip.take())?;
        if self.tip.as_ref().is_some_and(|tip| tip.hash == next.hash) {
            self.tip = None;
        }
        Some(next)
    }

    /// Puts a dispatched announcement back, if it is still an announcement this protocol wants.
    ///
    /// One that ran out of time, one that failed verification, a tip that is no longer the tip,
    /// and a mined block that a newer committed one has buried are all dropped instead, except
    /// that a mined block which is the best tip is still wanted: its tip event was consumed when
    /// it was sent. A mined block goes back ahead of the blocks accepted after it.
    fn restore(&mut self, announcement: Announcement, best_tip: Option<block::Hash>) {
        if self.stopped || !announcement.is_current(Instant::now()) {
            return;
        }
        match announcement.slot {
            Slot::Tip => {
                if best_tip == Some(announcement.hash) && self.tip.is_none() {
                    self.tip = Some(announcement);
                }
            }
            Slot::Mined => {
                let buried = self.is_buried(&announcement) && best_tip != Some(announcement.hash);
                if buried
                    || self.is_waiting_mined(&announcement.hash)
                    || self.mined.len() >= MAX_WAITING_MINED
                {
                    return;
                }
                self.mined.push_front(announcement);
            }
        }
    }

    /// Drops waiting announcements that are no longer worth sending.
    fn prune(&mut self, now: Instant) {
        let name = self.name;
        let dropping = |announcement: &Announcement| {
            debug!(
                height = ?announcement.height,
                hash = ?announcement.hash,
                protocol = name,
                "dropping a block announcement that is no longer worth sending",
            );
        };
        self.mined.retain(|waiting| {
            let current = waiting.is_current(now);
            if !current {
                dropping(waiting);
            }
            current
        });
        if let Some(tip) = self.tip.as_ref().filter(|tip| !tip.is_current(now)) {
            dropping(tip);
            self.tip = None;
        }
    }

    /// Starts or drops the wait for a peer, to match the waiting announcements.
    ///
    /// One wait covers every waiting announcement: a newer block replaces a waiting one without
    /// restarting it. A protocol already sending [`MAX_ACTIVE_BROADCASTS`] announcements does not
    /// wait: the next completion is an event, after which the wait starts.
    fn match_readiness(&mut self) {
        if self.stopped || !self.is_waiting() || self.active >= MAX_ACTIVE_BROADCASTS {
            self.ready = None;
        } else if self.ready.is_none() {
            self.ready = Some(self.service.clone().ready_oneshot().boxed());
        }
    }

    /// Ends this protocol's announcements after its service stopped answering.
    fn stop(&mut self) {
        self.stopped = true;
        self.tip = None;
        self.mined.clear();
        self.ready = None;
    }
}

#[derive(Debug)]
enum GossipEvent<T, R> {
    BroadcastCompleted(BroadcastCompleted),
    MinedBlock(MinedBlockEvent),
    Expired,
    ServiceReady(R),
    CommittedTip(T),
}

/// Returns the next event the gossip task acts on.
///
/// Prefers finished announcements, then this node's own blocks, then expiry, then a protocol that
/// now has a peer, then a committed tip. The committed-tip path is a fallback, so selecting it
/// before a mined block can duplicate a mined block's announcement, and a peer that is already
/// waiting must not be used for an announcement that has just been replaced.
async fn next_gossip_event<T, R>(
    mined_block_receiver: Option<&mut mpsc::UnboundedReceiver<MinedBlockEvent>>,
    completion_receiver: &mut mpsc::UnboundedReceiver<BroadcastCompleted>,
    expiry_fut: impl Future<Output = ()>,
    ready_fut: impl Future<Output = R>,
    committed_tip_fut: impl Future<Output = T>,
) -> GossipEvent<T, R> {
    tokio::select! {
        biased;

        Some(completed) = completion_receiver.recv() => {
            GossipEvent::BroadcastCompleted(completed)
        },

        Some(mined_block) = next_mined_block(mined_block_receiver) => {
            GossipEvent::MinedBlock(mined_block)
        },

        () = expiry_fut => GossipEvent::Expired,

        ready = ready_fut => GossipEvent::ServiceReady(ready),

        committed_tip = committed_tip_fut => {
            GossipEvent::CommittedTip(committed_tip)
        },
    }
}

/// Returns this node's next mined-block lifecycle event, and never returns when this node does
/// not mine.
async fn next_mined_block(
    receiver: Option<&mut mpsc::UnboundedReceiver<MinedBlockEvent>>,
) -> Option<MinedBlockEvent> {
    match receiver {
        Some(receiver) => receiver.recv().await,
        None => std::future::pending().await,
    }
}

/// Returns once one protocol has a peer that can receive an announcement.
///
/// A wait that is not selected keeps its progress: it is polled again on the next event.
async fn next_ready<ZN>(protocols: &mut [Protocol<ZN>]) -> (usize, Result<ZN, BoxError>) {
    futures::future::poll_fn(|cx| {
        for (index, protocol) in protocols.iter_mut().enumerate() {
            let Some(ready) = protocol.ready.as_mut() else {
                continue;
            };
            if let std::task::Poll::Ready(result) = ready.poll_unpin(cx) {
                protocol.ready = None;
                return std::task::Poll::Ready((index, result));
            }
        }
        std::task::Poll::Pending
    })
    .await
}

/// Waits until `deadline`, and never returns without one.
async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// Returns when the first waiting announcement stops being worth sending.
fn next_deadline<ZN>(protocols: &[Protocol<ZN>]) -> Option<Instant> {
    protocols
        .iter()
        .flat_map(|protocol| protocol.mined.iter().chain(protocol.tip.as_ref()))
        .map(|announcement| announcement.deadline)
        .min()
}

/// Errors that can occur when gossiping committed blocks
#[derive(Error, Debug)]
pub enum BlockGossipError {
    #[error("chain tip sender was dropped")]
    TipChange(watch::error::RecvError),

    #[error("sync status sender was dropped")]
    SyncStatus(watch::error::RecvError),
}

/// Run continuously, gossiping newly verified [`block::Hash`]es to peers.
///
/// Once the state has reached the chain tip, announce the [`block::Hash`]es of newly verified
/// blocks to the peers of each protocol that is enabled: `legacy_network` advertises over the
/// legacy Zcash protocol, and `zakura_network` over the Zakura protocol. Each protocol waits for
/// its own peers, so a protocol with none never delays the other.
///
/// Each protocol announces the latest committed best tip and the latest block this node mined.
/// Blocks that are replaced while waiting for a peer are left to ordinary sync.
///
/// Each announcement carries the block's serialized size, which sizes the legacy broadcast.
/// Mined-block events carry the size; committed tips read it from `read_state`.
///
/// [`block::Hash`]: zakura_chain::block::Hash
pub async fn gossip_best_tip_block_hashes<ZN, RS>(
    sync_status: SyncStatus,
    mut chain_state: ChainTipChange,
    legacy_network: Option<ZN>,
    zakura_network: Option<ZN>,
    read_state: RS,
    mut mined_block_receiver: Option<mpsc::UnboundedReceiver<MinedBlockEvent>>,
) -> Result<(), BlockGossipError>
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + Clone + 'static,
    ZN::Future: Send,
    RS: Service<zs::ReadRequest, Response = zs::ReadResponse, Error = BoxError>
        + Send
        + Clone
        + 'static,
    RS::Future: Send,
{
    info!("initializing block gossip task");

    let (completion_sender, mut completion_receiver) = mpsc::unbounded_channel();
    let mut protocols: Vec<Protocol<ZN>> = [("legacy", legacy_network), ("zakura", zakura_network)]
        .into_iter()
        .filter_map(|(name, service)| service.map(|service| Protocol::new(name, service)))
        .collect();
    let mut deliveries = MinedDeliveries::default();
    // The submission whose latency has been recorded, so two protocols record one sample.
    let mut recorded_submission = None;

    loop {
        // Record a finished announcement before deciding whether the committed-tip path should
        // announce the same block. `try_recv()` keeps this non-blocking: `Empty` just means no
        // announcement has reported back yet, so the loop can keep making progress.
        while let Ok(completed) = completion_receiver.try_recv() {
            finish_broadcast(
                completed,
                &mut protocols,
                &mut chain_state,
                &mut deliveries,
                &mut recorded_submission,
            );
        }

        let now = Instant::now();
        for protocol in &mut protocols {
            protocol.prune(now);
            protocol.match_readiness();
        }

        let mut sync_status = sync_status.clone();
        let mut chain_tip = chain_state.clone_for_task();

        let committed_tip_fut = async move {
            /// A brief duration to wait after a tip change for a new message in the mined block channel.
            const WAIT_FOR_BLOCK_SUBMISSION_DELAY: Duration = Duration::from_micros(100);

            // Block gossip has no delay between successive blocks: every relay hop that waits
            // adds its delay to the block's propagation time.
            //
            // wait for at least one tip change, to make sure we have a new block hash to broadcast
            let tip_action = chain_tip.wait_for_tip_change().await.map_err(TipChange)?;

            // wait for block submissions to be received through the `mined_block_receiver` if the tip
            // change is from a block submission.
            tokio::time::sleep(WAIT_FOR_BLOCK_SUBMISSION_DELAY).await;

            // wait until we're close to the tip, because broadcasts are only useful for nodes near the tip
            // (if they're a long way from the tip, they use the syncer and block locators), unless a mined block
            // hash is received before `wait_until_close_to_tip()` is ready.
            sync_status
                .wait_until_close_to_tip()
                .map_err(SyncStatus)
                .await?;

            // get the latest tip change when close to tip - it might be different to the change we awaited,
            // because the syncer might take a long time to reach the tip
            let best_tip = chain_tip
                .last_tip_change()
                .unwrap_or(tip_action)
                .best_tip_hash_and_height();

            Ok((best_tip, chain_tip))
        }
        .in_current_span();

        let event = next_gossip_event(
            mined_block_receiver.as_mut(),
            &mut completion_receiver,
            wait_until(next_deadline(&protocols)),
            next_ready(&mut protocols),
            committed_tip_fut,
        )
        .await;

        match event {
            GossipEvent::BroadcastCompleted(completed) => finish_broadcast(
                completed,
                &mut protocols,
                &mut chain_state,
                &mut deliveries,
                &mut recorded_submission,
            ),

            GossipEvent::MinedBlock(mined_block) => {
                let announcement = match mined_block {
                    MinedBlockEvent::Early {
                        hash,
                        height,
                        size,
                        submitted_at,
                        submission,
                        pending,
                    } => Announcement::mined(
                        hash,
                        height,
                        size,
                        Some(EarlyMined {
                            pending,
                            submitted_at,
                            submission,
                        }),
                    ),
                    MinedBlockEvent::Committed { hash, height, size } => {
                        Announcement::mined(hash, height, size, None)
                    }
                };
                announcement.log_accepted();
                for protocol in &mut protocols {
                    protocol.accept_mined(announcement.clone());
                }
            }

            // The loop prunes expired announcements before it waits again.
            GossipEvent::Expired => {}

            GossipEvent::ServiceReady((index, ready)) => {
                let protocol = &mut protocols[index];
                match ready {
                    Ok(service) => {
                        protocol.prune(Instant::now());
                        if let Some(announcement) = protocol.take_next() {
                            if announcement.tracked {
                                protocol.broadcasts.start(announcement.hash);
                            }
                            protocol.active += 1;
                            dispatch(
                                index,
                                protocol.name,
                                service,
                                announcement,
                                read_state.clone(),
                                completion_sender.clone(),
                            );
                        }
                    }
                    Err(error) => {
                        warn!(
                            ?error,
                            protocol = protocol.name,
                            "block advertisement service stopped; ending its announcements",
                        );
                        protocol.stop();
                    }
                }
            }

            GossipEvent::CommittedTip(committed_tip) => {
                let ((hash, height), updated_chain_state) = committed_tip?;
                chain_state = updated_chain_state;
                let announcement = Announcement::tip(hash, height);
                announcement.log_accepted();
                for protocol in &mut protocols {
                    protocol.accept_tip(announcement.clone());
                }
            }
        }
    }
}

/// Sends `announcement` to `protocol`'s peers through its ready `service`, and reports the
/// outcome back to the gossip task.
fn dispatch<ZN, RS>(
    protocol: usize,
    name: &'static str,
    service: ZN,
    announcement: Announcement,
    read_state: RS,
    completion_sender: mpsc::UnboundedSender<BroadcastCompleted>,
) where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + Clone + 'static,
    ZN::Future: Send,
    RS: Service<zs::ReadRequest, Response = zs::ReadResponse, Error = BoxError>
        + Send
        + Clone
        + 'static,
    RS::Future: Send,
{
    let Announcement {
        hash,
        height,
        size,
        mined,
        deadline,
        ref early,
        ..
    } = announcement;
    let early = early.clone();

    debug!(
        ?height,
        ?hash,
        ?size,
        mined,
        early = early.is_some(),
        protocol = name,
        "sending a block announcement",
    );

    tokio::spawn(async move {
        let block = match size {
            Some(size) => zn::AdvertisedBlock::new(hash, size),
            // Only read the size of a block that is being announced now.
            None => zn::AdvertisedBlock {
                size: committed_block_size(read_state, hash).await,
                ..hash.into()
            },
        };
        let block = zn::AdvertisedBlock {
            submitted: early.is_some(),
            ..block
        };
        // block announcements inform other nodes about new blocks,
        // so our internal Grow or Reset state doesn't matter to them
        let request = if mined {
            zn::Request::AdvertiseMinedBlock(block)
        } else {
            zn::Request::AdvertiseBlock(block, None)
        };
        let broadcast = broadcast_until(service, request, hash, deadline);

        let outcome = match early {
            Some(EarlyMined { mut pending, .. }) => {
                if !pending.is_valid() {
                    Outcome::Failed
                } else {
                    tokio::select! {
                        biased;
                        _ = pending.wait_for_failure() => Outcome::Failed,
                        outcome = broadcast => outcome,
                    }
                }
            }
            None => broadcast.await,
        };

        let _ = completion_sender.send(BroadcastCompleted {
            protocol,
            outcome,
            announcement,
        });
    });
}

/// Reads the serialized size of the committed block `hash` from the state.
///
/// Returns `None` if the block is not found, or the read fails or takes longer than
/// [`BLOCK_SIZE_READ_TIMEOUT`]. The broadcast then uses its minimum advert window.
async fn committed_block_size<RS>(read_state: RS, hash: block::Hash) -> Option<u32>
where
    RS: Service<zs::ReadRequest, Response = zs::ReadResponse, Error = BoxError>,
{
    let read = read_state.oneshot(zs::ReadRequest::BlockInfo(hash.into()));
    let size = match tokio::time::timeout(BLOCK_SIZE_READ_TIMEOUT, read).await {
        Ok(Ok(zs::ReadResponse::BlockInfo(info))) => info.map(|info| info.size()),
        Ok(Ok(_)) => unreachable!("the state responds to BlockInfo with BlockInfo"),
        Ok(Err(error)) => {
            debug!(?hash, ?error, "could not read the committed block's size");
            None
        }
        Err(_elapsed) => None,
    };
    if size.is_none() {
        metrics::counter!("sync.gossip.block_size_unknown").increment(1);
    }
    size
}

/// Sends `request` for the block with `hash`, with `deadline` bounding the wait for a peer and
/// the broadcast itself.
async fn broadcast_until<ZN>(
    mut service: ZN,
    request: zn::Request,
    hash: block::Hash,
    deadline: Instant,
) -> Outcome
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError>,
{
    // `service` is the ready service this announcement waited for, so it is called directly.
    match tokio::time::timeout_at(deadline, service.call(request)).await {
        Ok(Ok(_response)) => Outcome::Delivered,
        Ok(Err(error)) if error.is::<zn::NoEligibleBlockPeers>() => Outcome::NoPeers,
        Ok(Err(error)) => {
            debug!(?error, ?hash, "block announcement failed");
            Outcome::Failed
        }
        Err(_elapsed) => {
            debug!(?hash, "block announcement ran out of time");
            Outcome::Failed
        }
    }
}

/// Records a finished announcement, and keeps the block announced if it still needs to be.
///
/// An announcement that found no peer goes back to waiting for one. A committed mined block whose
/// announcement failed is announced once more, while it is still the tip.
fn finish_broadcast<ZN>(
    completed: BroadcastCompleted,
    protocols: &mut [Protocol<ZN>],
    chain_state: &mut ChainTipChange,
    deliveries: &mut MinedDeliveries,
    recorded_submission: &mut Option<block::Hash>,
) where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + Clone + 'static,
    ZN::Future: Send,
{
    // A protocol that stopped announces nothing, so it does not hold up the suppression.
    let protocol_count = protocols
        .iter()
        .filter(|protocol| !protocol.stopped)
        .count();
    let best_tip = chain_state.latest_chain_tip().best_tip_hash();
    let BroadcastCompleted {
        protocol: protocol_index,
        outcome,
        announcement,
    } = completed;
    let (hash, height, size, tracked) = (
        announcement.hash,
        announcement.height,
        announcement.size,
        announcement.tracked,
    );
    let protocol = &mut protocols[protocol_index];
    protocol.active = protocol.active.saturating_sub(1);

    if outcome == Outcome::NoPeers {
        // Nothing was sent, so this is not a failed delivery and does not use up the one retry.
        if tracked {
            protocol.broadcasts.finish(hash, false);
        }
        debug!(
            ?height,
            ?hash,
            protocol = protocol.name,
            "no peer could receive a block announcement; waiting for one",
        );
        protocol.restore(announcement, best_tip);
        return;
    }

    let delivered = outcome == Outcome::Delivered;
    if delivered {
        if let Some(early) = &announcement.early {
            // Two protocols announcing one submission is still one submission.
            if *recorded_submission != Some(hash) {
                *recorded_submission = Some(hash);
                metrics::counter!("mining.optimistic_inventory.early_inventories").increment(1);
                metrics::histogram!("mining.submit_to_inventory.duration_seconds", "method" => early.submission.rpc_method())
                    .record(early.submitted_at.elapsed().as_secs_f64());
            }
        }
    }

    if !tracked {
        return;
    }

    let needs_retry = protocol.broadcasts.finish(hash, delivered);

    if delivered {
        // Every protocol has the block, so its tip change needs no committed-tip announcement.
        if deliveries.delivered(hash, protocol_index, protocol_count) {
            chain_state.mark_last_change_hash(hash);
        }
        return;
    }

    // A newer tip gets its own committed-tip announcement.
    if !needs_retry || best_tip != Some(hash) {
        return;
    }

    info!(
        ?height,
        ?hash,
        protocol = protocol.name,
        "announcing a committed block again after its mined block announcement failed",
    );
    protocol.accept_tip(Announcement::retry(hash, height, size));
}

/// How many delivered mined blocks a delivery record remembers.
///
/// Announcements finish in any order, so one remembered block is not enough: a slower
/// announcement of an older block would erase a newer block's record. Sixteen outlasts every
/// announcement that can still be in flight, since the RPC admits at most that many submissions
/// at once.
const RECENT_DELIVERIES: usize = 16;

/// Records which protocols have announced each recently committed mined block.
///
/// Every protocol shares one committed-tip path, so that path is only suppressed once every
/// protocol has announced the block. A block can commit more than once, when the state evicted
/// it and it was accepted again, so each protocol is remembered rather than counted: two
/// announcements by one protocol do not stand in for the other protocol's.
#[derive(Debug, Default)]
struct MinedDeliveries {
    /// The protocols that have announced each block, one bit per protocol index, oldest block
    /// first, at most [`RECENT_DELIVERIES`] blocks.
    recent: VecDeque<(block::Hash, u64)>,
}

impl MinedDeliveries {
    /// Records that the protocol at index `protocol` announced `hash`, returning whether at
    /// least `protocols` distinct protocols have announced it. A protocol that stopped is not
    /// counted in `protocols`, so its index can be past that count.
    fn delivered(&mut self, hash: block::Hash, protocol: usize, protocols: usize) -> bool {
        assert!(
            protocol < u64::BITS as usize,
            "protocol index {protocol} fits the announced-protocol bits"
        );
        let index = match self.recent.iter().position(|(recent, _)| *recent == hash) {
            Some(index) => index,
            None => {
                if self.recent.len() >= RECENT_DELIVERIES {
                    self.recent.pop_front();
                }
                self.recent.push_back((hash, 0));
                self.recent.len() - 1
            }
        };
        let (_, announced) = &mut self.recent[index];
        *announced |= 1 << protocol;
        // At most 64 set bits, so the count fits a usize.
        announced.count_ones() as usize >= protocols
    }
}

/// Committed mined-block announcements, tracked so the committed-tip path does not duplicate
/// them, per protocol.
#[derive(Debug, Default)]
struct MinedBlockBroadcasts {
    /// Announcements that have not reported completion, counted per hash.
    ///
    /// Every dispatched announcement reports completion within its deadline, so entries do not
    /// accumulate.
    in_flight: HashMap<block::Hash, usize>,

    /// The hashes whose mined-block announcement succeeded, oldest first, at most
    /// [`RECENT_DELIVERIES`] of them.
    delivered: VecDeque<block::Hash>,
}

impl MinedBlockBroadcasts {
    fn start(&mut self, hash: block::Hash) {
        *self.in_flight.entry(hash).or_default() += 1;
    }

    fn is_in_flight(&self, hash: &block::Hash) -> bool {
        self.in_flight.contains_key(hash)
    }

    /// Returns whether a mined-block announcement of `hash` succeeded recently.
    fn was_delivered(&self, hash: &block::Hash) -> bool {
        self.delivered.contains(hash)
    }

    /// Records a completed announcement.
    ///
    /// Returns `true` if the hash now needs announcing again: this announcement failed, no other
    /// announcement of the hash is in flight, and none has succeeded.
    fn finish(&mut self, hash: block::Hash, succeeded: bool) -> bool {
        if let Some(count) = self.in_flight.get_mut(&hash) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.in_flight.remove(&hash);
            }
        }

        if succeeded {
            if !self.was_delivered(&hash) {
                if self.delivered.len() >= RECENT_DELIVERIES {
                    self.delivered.pop_front();
                }
                self.delivered.push_back(hash);
            }
            return false;
        }

        !self.is_in_flight(&hash) && !self.was_delivered(&hash)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        next_gossip_event, Announcement, BroadcastCompleted, EarlyMined, GossipEvent,
        MinedBlockBroadcasts, MinedDeliveries, Outcome, Protocol, Slot, MAX_ACTIVE_BROADCASTS,
        MAX_WAITING_MINED, RECENT_DELIVERIES,
    };

    use std::future;

    use tokio::sync::mpsc;
    use tower::util::BoxCloneService;
    use zakura_chain::block;
    use zakura_rpc::{MinedBlockEvent, MinedBlockSubmission, PendingBlockSignal};

    use crate::BoxError;

    // Repeat the vector so removing `biased;` reliably exposes randomized
    // selection among the ready events.
    const READY_EVENT_ATTEMPTS: usize = 64;

    /// Returns a completed announcement of the committed mined block `hash`.
    fn completed(hash: block::Hash, outcome: Outcome) -> BroadcastCompleted {
        BroadcastCompleted {
            protocol: 0,
            outcome,
            announcement: Announcement::mined(hash, block::Height(1), 1_000, None),
        }
    }

    #[tokio::test]
    async fn ready_gossip_events_are_selected_in_priority_order() {
        let submitted_hash = block::Hash([1; 32]);

        for _ in 0..READY_EVENT_ATTEMPTS {
            let (mined_block_sender, mut mined_block_receiver) = mpsc::unbounded_channel();
            let (completion_sender, mut completion_receiver) = mpsc::unbounded_channel();

            mined_block_sender
                .send(MinedBlockEvent::Committed {
                    hash: submitted_hash,
                    height: block::Height(1),
                    size: 1_000,
                })
                .unwrap();
            completion_sender
                .send(completed(submitted_hash, Outcome::Delivered))
                .unwrap();

            let event = next_gossip_event(
                Some(&mut mined_block_receiver),
                &mut completion_receiver,
                future::ready(()),
                future::ready("ready"),
                future::ready("committed tip"),
            )
            .await;

            assert!(matches!(
                event,
                GossipEvent::BroadcastCompleted(event)
                    if event.announcement.hash == submitted_hash
                        && event.outcome == Outcome::Delivered
            ));

            let event = next_gossip_event(
                Some(&mut mined_block_receiver),
                &mut completion_receiver,
                future::ready(()),
                future::ready("ready"),
                future::ready("committed tip"),
            )
            .await;

            assert!(matches!(
                event,
                GossipEvent::MinedBlock(MinedBlockEvent::Committed {
                    hash,
                    height: block::Height(1),
                    ..
                }) if hash == submitted_hash
            ));

            let event = next_gossip_event(
                Some(&mut mined_block_receiver),
                &mut completion_receiver,
                future::ready(()),
                future::ready("ready"),
                future::ready("committed tip"),
            )
            .await;

            assert!(matches!(event, GossipEvent::Expired));

            let event: GossipEvent<&str, &str> = next_gossip_event(
                Some(&mut mined_block_receiver),
                &mut completion_receiver,
                future::pending(),
                future::ready("ready"),
                future::ready("committed tip"),
            )
            .await;

            assert!(matches!(event, GossipEvent::ServiceReady("ready")));

            let event: GossipEvent<&str, &str> = next_gossip_event(
                Some(&mut mined_block_receiver),
                &mut completion_receiver,
                future::pending(),
                future::pending(),
                future::ready("committed tip"),
            )
            .await;

            assert!(matches!(event, GossipEvent::CommittedTip("committed tip")));
        }
    }

    #[test]
    fn failed_mined_block_broadcast_needs_fallback_once_nothing_is_in_flight() {
        let hash = block::Hash([2; 32]);
        let mut broadcasts = MinedBlockBroadcasts::default();

        broadcasts.start(hash);
        broadcasts.start(hash);
        assert!(broadcasts.is_in_flight(&hash));

        assert!(
            !broadcasts.finish(hash, false),
            "another broadcast of the hash can still succeed",
        );
        assert!(broadcasts.is_in_flight(&hash));

        assert!(broadcasts.finish(hash, false));
        assert!(!broadcasts.is_in_flight(&hash));
    }

    #[test]
    fn failed_mined_block_broadcast_after_success_needs_no_fallback() {
        let hash = block::Hash([2; 32]);
        let mut broadcasts = MinedBlockBroadcasts::default();

        broadcasts.start(hash);
        broadcasts.start(hash);

        assert!(!broadcasts.finish(hash, true));
        assert!(!broadcasts.finish(hash, false));
        assert!(!broadcasts.is_in_flight(&hash));
        assert!(broadcasts.was_delivered(&hash));
    }

    #[test]
    fn a_late_success_for_an_older_mined_block_keeps_the_newer_delivery() {
        let older = block::Hash([7; 32]);
        let newer = block::Hash([8; 32]);
        let mut broadcasts = MinedBlockBroadcasts::default();

        broadcasts.start(older);
        broadcasts.start(newer);
        assert!(!broadcasts.finish(newer, true));
        assert!(!broadcasts.finish(older, true));

        assert!(broadcasts.was_delivered(&newer));
        assert!(broadcasts.was_delivered(&older));
        assert!(
            !broadcasts.finish(newer, false),
            "a delivered block needs no retry",
        );
    }

    #[test]
    fn delivery_records_remember_a_bounded_number_of_blocks() {
        let mut broadcasts = MinedBlockBroadcasts::default();
        let mut deliveries = MinedDeliveries::default();
        let hash = |index: usize| block::Hash([u8::try_from(index).expect("small index"); 32]);

        for index in 0..=RECENT_DELIVERIES {
            broadcasts.finish(hash(index), true);
            assert!(!deliveries.delivered(hash(index), 0, 2));
        }

        assert!(
            !broadcasts.was_delivered(&hash(0)),
            "the oldest is forgotten"
        );
        assert!(broadcasts.was_delivered(&hash(1)));
        assert!(broadcasts.was_delivered(&hash(RECENT_DELIVERIES)));
        assert!(
            deliveries.delivered(hash(RECENT_DELIVERIES), 1, 2),
            "the newest record is kept"
        );
        assert!(
            !deliveries.delivered(hash(0), 1, 2),
            "the oldest record starts again",
        );
    }

    #[test]
    fn the_committed_tip_is_suppressed_once_every_protocol_has_the_block() {
        let hash = block::Hash([3; 32]);
        let other = block::Hash([4; 32]);
        let mut deliveries = MinedDeliveries::default();

        assert!(
            !deliveries.delivered(hash, 0, 2),
            "the second protocol has not announced the block yet",
        );
        assert!(deliveries.delivered(hash, 1, 2));

        assert!(
            !deliveries.delivered(other, 1, 2),
            "a newer block starts its own record",
        );
    }

    #[test]
    fn a_stopped_protocol_does_not_hold_up_the_suppression() {
        let hash = block::Hash([6; 32]);
        let mut deliveries = MinedDeliveries::default();

        // The first protocol stopped, so only the second one counts, at its own index.
        assert!(
            deliveries.delivered(hash, 1, 1),
            "the one live protocol announcing the block completes the record",
        );
    }

    #[test]
    fn a_protocol_announcing_a_block_twice_does_not_stand_in_for_the_other() {
        let hash = block::Hash([5; 32]);
        let mut deliveries = MinedDeliveries::default();

        assert!(!deliveries.delivered(hash, 0, 2));
        assert!(
            !deliveries.delivered(hash, 0, 2),
            "the block committed again and the same protocol announced it again, but the \
             other protocol still has not",
        );
        assert!(
            deliveries.delivered(hash, 1, 2),
            "the other protocol's first announcement completes the record",
        );
    }

    #[test]
    fn a_late_delivery_of_an_older_mined_block_does_not_reset_the_count() {
        let older = block::Hash([9; 32]);
        let newer = block::Hash([10; 32]);
        let mut deliveries = MinedDeliveries::default();

        assert!(!deliveries.delivered(newer, 0, 2));
        assert!(!deliveries.delivered(older, 0, 2));
        assert!(
            deliveries.delivered(newer, 1, 2),
            "the older block's late delivery kept the newer block's record",
        );
    }

    /// A protocol whose service answers every advertisement.
    fn protocol(
    ) -> Protocol<BoxCloneService<zakura_network::Request, zakura_network::Response, BoxError>>
    {
        Protocol::new(
            "legacy",
            BoxCloneService::new(tower::service_fn(|_: zakura_network::Request| async {
                Ok::<_, BoxError>(zakura_network::Response::Nil)
            })),
        )
    }

    /// A submission of a block that is not committed yet.
    fn early(pending: PendingBlockSignal) -> EarlyMined {
        EarlyMined {
            pending,
            submitted_at: std::time::Instant::now(),
            submission: MinedBlockSubmission::FullBlock,
        }
    }

    #[test]
    fn a_waiting_submission_goes_before_an_older_block_committing() {
        let waiting = block::Hash([5; 32]);
        let older = block::Hash([6; 32]);

        let mut protocol = protocol();
        protocol.accept_mined(Announcement::mined(
            waiting,
            block::Height(2),
            1_000,
            Some(early(PendingBlockSignal::valid_for_tests())),
        ));
        protocol.accept_mined(Announcement::mined(older, block::Height(1), 1_000, None));

        let next = protocol.take_next().expect("a submission is waiting");
        assert_eq!(next.hash, waiting);
        assert_eq!(next.slot, Slot::Mined);
        let next = protocol
            .take_next()
            .expect("the committed block is announced after the submission");
        assert_eq!(next.hash, older);
        assert!(protocol.take_next().is_none());
    }

    #[test]
    fn equal_height_mined_blocks_are_both_kept_waiting() {
        let first = block::Hash([21; 32]);
        let second = block::Hash([22; 32]);

        let mut protocol = protocol();
        protocol.accept_mined(Announcement::mined(first, block::Height(2), 1_000, None));
        protocol.accept_mined(Announcement::mined(second, block::Height(2), 1_000, None));

        assert_eq!(protocol.take_next().map(|next| next.hash), Some(first));
        assert_eq!(protocol.take_next().map(|next| next.hash), Some(second));
        assert!(protocol.take_next().is_none());
    }

    #[test]
    fn a_newer_committed_block_buries_lower_waiting_blocks() {
        let lower = block::Hash([23; 32]);
        let submitted = block::Hash([24; 32]);
        let newest = block::Hash([25; 32]);
        let late = block::Hash([26; 32]);

        let mut protocol = protocol();
        protocol.accept_mined(Announcement::mined(lower, block::Height(2), 1_000, None));
        protocol.accept_mined(Announcement::mined(
            submitted,
            block::Height(3),
            1_000,
            Some(early(PendingBlockSignal::valid_for_tests())),
        ));
        protocol.accept_mined(Announcement::mined(newest, block::Height(4), 1_000, None));
        protocol.accept_mined(Announcement::mined(late, block::Height(3), 1_000, None));

        assert_eq!(
            protocol.take_next().map(|next| next.hash),
            Some(newest),
            "the newest committed block buries the lower ones",
        );
        assert!(
            protocol.take_next().is_none(),
            "a committed block below the newest one is buried too",
        );
    }

    #[test]
    fn an_equal_height_mined_block_is_put_back_ahead_of_later_ones() {
        let first = block::Hash([27; 32]);
        let second = block::Hash([28; 32]);

        let mut protocol = protocol();
        protocol.accept_mined(Announcement::mined(first, block::Height(2), 1_000, None));
        let first_sent = protocol.take_next().expect("the first block is waiting");
        protocol.accept_mined(Announcement::mined(second, block::Height(2), 1_000, None));

        protocol.restore(first_sent, None);
        assert_eq!(protocol.take_next().map(|next| next.hash), Some(first));
        assert_eq!(protocol.take_next().map(|next| next.hash), Some(second));
    }

    #[test]
    fn a_buried_mined_block_that_is_the_tip_is_put_back() {
        let tip = block::Hash([30; 32]);
        let side = block::Hash([31; 32]);

        let mut protocol = protocol();
        protocol.accept_mined(Announcement::mined(tip, block::Height(2), 1_000, None));
        let tip_sent = protocol.take_next().expect("the block is waiting");
        // A higher own block committed on a side chain, so the sent block is still the tip.
        protocol.accept_mined(Announcement::mined(side, block::Height(3), 1_000, None));

        protocol.restore(tip_sent, Some(tip));
        assert_eq!(
            protocol.take_next().map(|next| next.hash),
            Some(tip),
            "the best tip is still wanted, since its tip event was consumed when it was sent",
        );
        assert_eq!(protocol.take_next().map(|next| next.hash), Some(side));
    }

    #[test]
    fn the_mined_queue_drops_the_oldest_block_when_full() {
        let hash = |index: usize| block::Hash([u8::try_from(index).expect("small index"); 32]);

        let mut protocol = protocol();
        for index in 1..=MAX_WAITING_MINED + 1 {
            protocol.accept_mined(Announcement::mined(
                hash(index),
                block::Height(2),
                1_000,
                None,
            ));
        }

        assert_eq!(protocol.mined.len(), MAX_WAITING_MINED);
        assert_eq!(
            protocol.take_next().map(|next| next.hash),
            Some(hash(2)),
            "the oldest block made room",
        );
    }

    #[test]
    fn a_protocol_sending_the_active_bound_does_not_wait_for_a_peer() {
        let mut protocol = protocol();
        protocol.accept_mined(Announcement::mined(
            block::Hash([29; 32]),
            block::Height(2),
            1_000,
            None,
        ));

        protocol.active = MAX_ACTIVE_BROADCASTS;
        protocol.match_readiness();
        assert!(protocol.ready.is_none(), "every announcement slot is busy");

        protocol.active -= 1;
        protocol.match_readiness();
        assert!(protocol.ready.is_some(), "a completion frees a slot");
    }

    #[test]
    fn a_rejected_submission_does_not_block_a_committed_block() {
        let submitted = block::Hash([11; 32]);
        let committed = block::Hash([12; 32]);
        let (pending, failure) = PendingBlockSignal::waiting_for_tests();

        let mut protocol = protocol();
        protocol.accept_mined(Announcement::mined(
            submitted,
            block::Height(3),
            1_000,
            Some(early(pending)),
        ));
        failure.fail();
        protocol.accept_mined(Announcement::mined(
            committed,
            block::Height(2),
            1_000,
            None,
        ));

        let next = protocol
            .take_next()
            .expect("the committed block is waiting");
        assert_eq!(next.hash, committed);
        assert_eq!(next.slot, Slot::Mined);
    }

    #[test]
    fn a_mined_block_replaced_while_it_was_being_sent_is_not_put_back() {
        let older = block::Hash([13; 32]);
        let newer = block::Hash([14; 32]);

        let mut protocol = protocol();
        protocol.accept_mined(Announcement::mined(older, block::Height(2), 1_000, None));
        let older_sent = protocol.take_next().expect("the older block is waiting");
        protocol.accept_mined(Announcement::mined(newer, block::Height(3), 1_000, None));
        let newer_sent = protocol.take_next().expect("the newer block is waiting");

        protocol.restore(older_sent, None);
        assert!(
            protocol.take_next().is_none(),
            "the older block was replaced while it was being sent",
        );

        protocol.restore(newer_sent, None);
        let next = protocol.take_next().expect("the newer block is put back");
        assert_eq!(next.hash, newer);
    }

    #[test]
    fn a_tip_whose_mined_announcement_found_no_peers_is_put_back_ahead_of_a_newer_mined_block() {
        let tip = block::Hash([18; 32]);
        let newer = block::Hash([19; 32]);

        let mut protocol = protocol();
        protocol.accept_mined(
            Some(tip),
            Announcement::mined(tip, block::Height(2), 1_000, None),
        );
        let sent = protocol.take_next().expect("the mined block is waiting");
        protocol.broadcasts.start(sent.hash);
        // Its tip event arrives while the mined announcement is being sent, so the mined
        // announcement is the only announcement of the tip.
        protocol.accept_tip(Announcement::tip(tip, block::Height(2)));
        assert!(
            protocol.take_next().is_none(),
            "the tip is being announced as mined"
        );
        // A higher own block commits on a side chain, so the sent block stays the best tip
        // while the newest own height moves past it.
        protocol.accept_mined(
            Some(tip),
            Announcement::mined(newer, block::Height(3), 1_000, None),
        );

        // Then the mined announcement comes back with nothing sent.
        protocol.broadcasts.finish(sent.hash, false);
        protocol.restore(sent, Some(tip));

        let next = protocol
            .take_next()
            .expect("the tip is put back although a higher own block buried its height");
        assert_eq!(next.hash, tip);
        assert!(next.mined, "it keeps its mined priority at the peers");
        let next = protocol
            .take_next()
            .expect("the newer mined block waits behind it");
        assert_eq!(next.hash, newer);
    }

    #[test]
    fn a_committed_tip_announced_as_a_mined_block_clears_the_waiting_tip() {
        let older = block::Hash([15; 32]);
        let mined = block::Hash([16; 32]);
        let newest = block::Hash([17; 32]);

        let mut protocol = protocol();
        protocol.accept_tip(Announcement::tip(older, block::Height(2)));
        protocol.broadcasts.start(mined);
        protocol.accept_tip(Announcement::tip(mined, block::Height(3)));
        assert!(
            protocol.take_next().is_none(),
            "the mined announcement in flight covers the tip",
        );

        protocol.broadcasts.finish(mined, true);
        protocol.accept_tip(Announcement::tip(older, block::Height(2)));
        protocol.accept_tip(Announcement::tip(mined, block::Height(3)));
        assert!(
            protocol.take_next().is_none(),
            "the delivered mined announcement covers the tip",
        );

        protocol.accept_tip(Announcement::tip(newest, block::Height(4)));
        let next = protocol.take_next().expect("a newer tip is announced");
        assert_eq!(next.hash, newest);
        assert!(!next.mined);
    }

    #[test]
    fn a_committed_tip_keeps_the_waiting_retry_of_the_same_block() {
        let hash = block::Hash([18; 32]);

        let mut protocol = protocol();
        protocol.accept_tip(Announcement::retry(hash, block::Height(3), Some(1_000)));
        protocol.accept_tip(Announcement::tip(hash, block::Height(3)));

        let next = protocol.take_next().expect("the retry is waiting");
        assert_eq!(next.hash, hash);
        assert!(next.mined, "the retry keeps its mined priority");
        assert_eq!(next.size, Some(1_000));
    }

    #[test]
    fn a_newer_tip_replaces_a_waiting_retry() {
        let older = block::Hash([19; 32]);
        let newer = block::Hash([20; 32]);

        let mut protocol = protocol();
        protocol.accept_tip(Announcement::retry(older, block::Height(3), Some(1_000)));
        protocol.accept_tip(Announcement::tip(newer, block::Height(4)));

        let next = protocol.take_next().expect("the newer tip is waiting");
        assert_eq!(next.hash, newer);
        assert!(!next.mined);
    }
}
