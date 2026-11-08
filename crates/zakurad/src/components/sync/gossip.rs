//! A task that gossips newly verified [`block::Hash`]es to peers.
//!
//! [`block::Hash`]: zakura_chain::block::Hash

use std::{collections::HashMap, time::Duration};

use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tower::{Service, ServiceExt};

use zakura_chain::chain_sync_status::ChainSyncStatus;
use zakura_chain::{block, chain_tip::ChainTip};
use zakura_network as zn;
use zakura_node_services::task::spawn_erased;
use zakura_rpc::EarlyMinedBlock;
use zakura_state::{self as zs, BlockAnnouncements, BlockOrigin, LatestChainTip};

use crate::{components::sync::SyncStatus, BoxError};

use BlockGossipError::*;

pub(super) use zn::constants::BLOCK_GOSSIP_TIMEOUT;

/// Bounds the state read of a committed tip's size. A tip block is in memory, so this read is
/// fast; if it is slow, the broadcast proceeds without the size rather than waiting.
const BLOCK_SIZE_READ_TIMEOUT: Duration = Duration::from_millis(100);

/// A spawned committed mined-block broadcast finished.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MinedBlockBroadcastCompleted {
    block: zn::AdvertisedBlock,
    succeeded: bool,
}

/// Errors that can occur when gossiping committed blocks
#[derive(Error, Debug)]
pub enum BlockGossipError {
    #[error("chain tip sender was dropped")]
    AnnouncementsClosed,

    #[error("sync status sender was dropped")]
    SyncStatus(watch::error::RecvError),
}

/// Run continuously, gossiping newly verified [`block::Hash`]es to peers.
///
/// Once the state has reached the chain tip, broadcast the [`block::Hash`]es
/// of newly verified blocks to all ready peers.
///
/// Ordinary announcements coalesce to the latest best-chain tip and wait until sync is close
/// to the network tip. Successful local submissions are retained in commit order, including
/// side-chain blocks, and are announced regardless of sync distance.
///
/// Each advertisement carries the block's serialized size, which sizes the legacy broadcast. Mined-block events carry the size; committed tips read it from `read_state`.
///
/// State publishes committed announcements with their source. RPC contributes only early,
/// cancellable announcements; committed gossip never waits for RPC progress.
///
/// [`block::Hash`]: zakura_chain::block::Hash
pub async fn gossip_best_tip_block_hashes<ZN, RS>(
    mut sync_status: SyncStatus,
    latest_chain_tip: LatestChainTip,
    mut announcements: BlockAnnouncements,
    broadcast_network: ZN,
    read_state: RS,
    mut mined_block_receiver: Option<mpsc::UnboundedReceiver<EarlyMinedBlock>>,
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

    let (mined_block_completion_sender, mut mined_block_completion_receiver) =
        mpsc::unbounded_channel();
    let mut mined_block_broadcasts = MinedBlockBroadcasts::default();

    loop {
        // Drain local completion notifications from spawned mined-block
        // broadcasts before deciding whether the committed-tip fallback should
        // run. These are not peer acknowledgements: a success appears here only
        // after our mined-block broadcast future completed successfully.
        //
        // `try_recv()` keeps this non-blocking. `Empty` just means no spawned
        // broadcast has reported back yet, so the gossip loop can keep making
        // progress.
        while let Ok(completed) = mined_block_completion_receiver.try_recv() {
            finish_mined_block_broadcast(
                completed,
                &latest_chain_tip,
                &mut mined_block_broadcasts,
                &broadcast_network,
            );
        }

        let close_to_tip = sync_status.is_close_to_tip();
        let (hash, height, is_block_submission, early, size) = tokio::select! {
            biased;
            Some(completed) = mined_block_completion_receiver.recv() => {
                finish_mined_block_broadcast(completed, &latest_chain_tip, &mut mined_block_broadcasts, &broadcast_network);
                continue;
            }
            result = sync_status.changed() => {
                result.map_err(SyncStatus)?;
                continue;
            }
            early = async {
                match mined_block_receiver.as_mut() {
                    Some(receiver) => receiver.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                let Some(EarlyMinedBlock { hash, height, size, submitted_at, submission, pending, admission }) = early else {
                    mined_block_receiver = None;
                    continue;
                };
                (hash, height, true, Some((pending, submitted_at, submission, admission)), Some(size))
            }
            committed = announcements.recv(close_to_tip) => {
                let committed = committed.ok_or(AnnouncementsClosed)?;
                let (local, size) = match committed.origin {
                    BlockOrigin::Network => (false, None),
                    BlockOrigin::LocalSubmission { size } => (true, Some(size)),
                };
                (committed.hash, committed.height, local, None, size)
            }
        };

        // Without a delay between gossips, the committed-tip path sees a mined block's tip change
        // before that block's own broadcast completes. If the broadcast fails,
        // `finish_mined_block_broadcast` sends this fallback instead.
        if !is_block_submission && mined_block_broadcasts.is_in_flight(&hash) {
            debug!(
                ?height,
                ?hash,
                "skipping committed block broadcast: mined block broadcast is in flight",
            );
            continue;
        }

        // TODO: Move logic for calling the peer set to its own method.

        info!(
            ?height,
            ?hash,
            ?size,
            is_block_submission,
            "sending block announcement"
        );
        // Include readiness in the deadline. The event loop must keep consuming lifecycle and tip
        // events when the peer set has no ready service.
        let network = broadcast_network.clone();
        let read_state = read_state.clone();
        // Keep the committed-tip fallback until the committed broadcast succeeds. An admitted
        // body can be served immediately, but contextual failure still cancels early inventory.
        let completion_tx = (is_block_submission && early.is_none()).then(|| {
            mined_block_broadcasts.start(hash);
            mined_block_completion_sender.clone()
        });
        tokio::spawn(async move {
            let block = match size {
                Some(size) => zn::AdvertisedBlock::new(hash, size),
                None => zn::AdvertisedBlock {
                    size: committed_block_size(read_state, hash).await,
                    ..hash.into()
                },
            };
            // block broadcasts inform other nodes about new blocks,
            // so our internal Grow or Reset state doesn't matter to them
            let request = if is_block_submission {
                zn::Request::AdvertiseMinedBlock(block)
            } else {
                zn::Request::AdvertiseBlock(block, None)
            };
            let succeeded = match early {
                Some((mut pending, submitted_at, submission, admission)) => {
                    if !pending.is_valid() {
                        false
                    } else {
                        let succeeded = tokio::select! {
                            biased;
                            _ = pending.wait_for_failure() => false,
                            succeeded = broadcast_early_with_timeout(network, request, admission) => succeeded,
                        };
                        if succeeded {
                            metrics::counter!("mining.optimistic_inventory.early_inventories")
                                .increment(1);
                            metrics::histogram!("mining.submit_to_inventory.duration_seconds", "method" => submission.rpc_method())
                                .record(submitted_at.elapsed().as_secs_f64());
                        }
                        succeeded
                    }
                }
                None => broadcast_with_timeout(network, request).await,
            };

            if let Some(completion_tx) = completion_tx {
                let _ = completion_tx.send(MinedBlockBroadcastCompleted { block, succeeded });
            }
        });
    }
}

/// Reads the serialized size of the committed block `hash` from the state.
///
/// Returns `None` if the block is not found, or the read fails or takes longer than
/// [`BLOCK_SIZE_READ_TIMEOUT`]. The broadcast then uses its full-block advert window.
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

/// Sends a block broadcast, returning `true` if it succeeded within [`BLOCK_GOSSIP_TIMEOUT`].
async fn broadcast_with_timeout<ZN>(network: ZN, request: zn::Request) -> bool
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError>,
{
    tokio::time::timeout(BLOCK_GOSSIP_TIMEOUT, network.oneshot(request))
        .await
        .is_ok_and(|result| result.is_ok())
}

/// Dispatches early inventory only while the submission has not committed.
///
/// A commit cancels readiness waits. Once dispatched, the early broadcast can finish normally
/// and share active uploads with the committed broadcast; contextual failure still cancels it.
pub(super) async fn broadcast_early_with_timeout<ZN>(
    mut network: ZN,
    request: zn::Request,
    admission: zs::BlockAdmission,
) -> bool
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError>,
{
    tokio::time::timeout(BLOCK_GOSSIP_TIMEOUT, async {
        let ready = tokio::select! {
            biased;
            _ = admission.wait_for_commit() => return false,
            ready = network.ready() => ready,
        };
        let Ok(ready) = ready else {
            return false;
        };
        // Readiness may resolve concurrently with the writer's commit notification.
        if admission.is_committed() {
            return false;
        }
        ready.call(request).await.is_ok()
    })
    .await
    .unwrap_or(false)
}

/// Records a completed mined-block broadcast, and advertises the hash through the committed-tip
/// fallback if the broadcast failed.
fn finish_mined_block_broadcast<ZN>(
    completed: MinedBlockBroadcastCompleted,
    latest_chain_tip: &LatestChainTip,
    mined_block_broadcasts: &mut MinedBlockBroadcasts,
    broadcast_network: &ZN,
) where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + Clone + 'static,
    ZN::Future: Send,
{
    let hash = completed.block.hash;
    let needs_fallback = mined_block_broadcasts.finish(completed);

    if completed.succeeded {
        return;
    }

    // A newer tip gets its own committed-tip broadcast.
    if !needs_fallback || latest_chain_tip.best_tip_hash() != Some(hash) {
        return;
    }

    let request = zn::Request::AdvertiseMinedBlock(completed.block);
    info!(
        ?request,
        "sending committed block broadcast after the mined block broadcast failed",
    );
    spawn_erased(broadcast_with_timeout(broadcast_network.clone(), request));
}

/// Committed mined-block broadcasts, tracked so the committed-tip path does not duplicate them.
#[derive(Debug, Default)]
struct MinedBlockBroadcasts {
    /// Broadcasts that have not reported completion, counted per hash.
    ///
    /// Every spawned broadcast reports completion within [`BLOCK_GOSSIP_TIMEOUT`], so entries
    /// do not accumulate.
    in_flight: HashMap<block::Hash, usize>,

    /// The most recent hash whose mined-block broadcast succeeded.
    delivered: Option<block::Hash>,
}

impl MinedBlockBroadcasts {
    fn start(&mut self, hash: block::Hash) {
        *self.in_flight.entry(hash).or_default() += 1;
    }

    fn is_in_flight(&self, hash: &block::Hash) -> bool {
        self.in_flight.contains_key(hash)
    }

    /// Records a completed broadcast.
    ///
    /// Returns `true` if the hash now needs the committed-tip fallback: this broadcast failed, no
    /// other broadcast of the hash is in flight, and none has succeeded.
    fn finish(&mut self, completed: MinedBlockBroadcastCompleted) -> bool {
        let MinedBlockBroadcastCompleted { block, succeeded } = completed;
        let hash = block.hash;

        if let Some(count) = self.in_flight.get_mut(&hash) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.in_flight.remove(&hash);
            }
        }

        if succeeded {
            self.delivered = Some(hash);
            return false;
        }

        !self.is_in_flight(&hash) && self.delivered != Some(hash)
    }
}

#[cfg(test)]
mod tests {
    use super::{MinedBlockBroadcastCompleted, MinedBlockBroadcasts};
    use zakura_chain::block;

    #[test]
    fn failed_mined_block_broadcast_needs_fallback_once_nothing_is_in_flight() {
        let hash = block::Hash([2; 32]);
        let failed = MinedBlockBroadcastCompleted {
            block: hash.into(),
            succeeded: false,
        };
        let mut broadcasts = MinedBlockBroadcasts::default();

        broadcasts.start(hash);
        broadcasts.start(hash);
        assert!(broadcasts.is_in_flight(&hash));

        assert!(
            !broadcasts.finish(failed),
            "another broadcast of the hash can still succeed",
        );
        assert!(broadcasts.is_in_flight(&hash));

        assert!(broadcasts.finish(failed));
        assert!(!broadcasts.is_in_flight(&hash));
    }

    #[test]
    fn failed_mined_block_broadcast_after_success_needs_no_fallback() {
        let hash = block::Hash([2; 32]);
        let mut broadcasts = MinedBlockBroadcasts::default();

        broadcasts.start(hash);
        broadcasts.start(hash);

        assert!(!broadcasts.finish(MinedBlockBroadcastCompleted {
            block: hash.into(),
            succeeded: true,
        }));
        assert!(!broadcasts.finish(MinedBlockBroadcastCompleted {
            block: hash.into(),
            succeeded: false,
        }));
        assert!(!broadcasts.is_in_flight(&hash));
    }
}
