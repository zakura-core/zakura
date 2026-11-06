//! A task that gossips newly verified [`block::Hash`]es to peers.
//!
//! [`block::Hash`]: zakura_chain::block::Hash

use std::{collections::HashMap, future::Future, time::Duration};

use futures::TryFutureExt;
use thiserror::Error;
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};
use tower::{Service, ServiceExt};
use tracing::Instrument;

use zakura_chain::{block, chain_tip::ChainTip};
use zakura_network as zn;
use zakura_node_services::task::spawn_erased;
use zakura_rpc::{MinedBlockEvent, MinedSubmissions};
use zakura_state::{self as zs, ChainTipChange};

use crate::{components::sync::SyncStatus, BoxError};

use BlockGossipError::*;

/// Bounds a full paced broadcast, including several upload waves.
pub(super) const BLOCK_GOSSIP_TIMEOUT: Duration = Duration::from_secs(90);

/// Bounds the state read of a committed tip's size. A tip block is in memory, so this read is
/// fast; if it is slow, the broadcast proceeds without the size rather than waiting.
const BLOCK_SIZE_READ_TIMEOUT: Duration = Duration::from_millis(100);

/// A spawned committed mined-block broadcast finished.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MinedBlockBroadcastCompleted {
    block: zn::AdvertisedBlock,
    succeeded: bool,
}

#[derive(Debug)]
enum GossipEvent<T> {
    MinedBlockBroadcastCompleted(MinedBlockBroadcastCompleted),
    MinedBlock(MinedBlockEvent),
    MinedBlockChannelClosed,
    CommittedTip(T),
}

async fn next_gossip_event<T>(
    mined_block_receiver: Option<&mut mpsc::UnboundedReceiver<MinedBlockEvent>>,
    mined_block_completion_receiver: &mut mpsc::UnboundedReceiver<MinedBlockBroadcastCompleted>,
    committed_tip_fut: impl Future<Output = T>,
) -> GossipEvent<T> {
    if let Some(mined_block_receiver) = mined_block_receiver {
        tokio::select! {
            biased;

            Some(completed) = mined_block_completion_receiver.recv() => {
                GossipEvent::MinedBlockBroadcastCompleted(completed)
            },

            tip_change = mined_block_receiver.recv() => {
                match tip_change {
                    Some(tip_change) => GossipEvent::MinedBlock(tip_change),
                    None => GossipEvent::MinedBlockChannelClosed,
                }
            },

            committed_tip = committed_tip_fut => {
                GossipEvent::CommittedTip(committed_tip)
            },
        }
    } else {
        tokio::select! {
            biased;

            Some(completed) = mined_block_completion_receiver.recv() => {
                GossipEvent::MinedBlockBroadcastCompleted(completed)
            },

            committed_tip = committed_tip_fut => {
                GossipEvent::CommittedTip(committed_tip)
            },
        }
    }
}

/// Returns a mined-block event queued after the selector polled its channel, instead of a
/// committed tip that resolved in the same poll.
///
/// A local submission's final event can arrive, and its reservation be released, between those
/// two polls, so the tip path no longer waits for it. Preferring the queued event keeps the block
/// on the mined path. The committed tip is not lost: the gossip state only records a tip taken
/// from the committed path, so the next iteration observes it again.
fn prefer_queued_mined_block<T, E>(
    event: GossipEvent<Result<T, E>>,
    mined_block_receiver: Option<&mut mpsc::UnboundedReceiver<MinedBlockEvent>>,
) -> GossipEvent<Result<T, E>> {
    match event {
        GossipEvent::CommittedTip(Ok(tip)) => {
            match mined_block_receiver.and_then(|receiver| receiver.try_recv().ok()) {
                Some(mined) => GossipEvent::MinedBlock(mined),
                None => GossipEvent::CommittedTip(Ok(tip)),
            }
        }
        other => other,
    }
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
/// Once the state has reached the chain tip, broadcast the [`block::Hash`]es
/// of newly verified blocks to all ready peers.
///
/// Blocks are only gossiped if they are:
/// - on the best chain, and
/// - the most recent block verified since the last gossip.
///
/// In particular, if a lot of blocks are committed at the same time,
/// gossips will be disabled or skipped until the state reaches the latest tip.
///
/// Each advertisement carries the block's serialized size, which sizes the legacy broadcast. Mined-block events carry the size; committed tips read it from `read_state`.
///
/// `local_submissions` are the blocks `submitblock` is submitting, shared with the RPC. A local
/// tip waits for its mined-block event or reservation release, bounded by [`BLOCK_GOSSIP_TIMEOUT`].
///
/// [`block::Hash`]: zakura_chain::block::Hash
pub async fn gossip_best_tip_block_hashes<ZN, RS>(
    sync_status: SyncStatus,
    mut chain_state: ChainTipChange,
    broadcast_network: ZN,
    read_state: RS,
    mut mined_block_receiver: Option<mpsc::UnboundedReceiver<MinedBlockEvent>>,
    local_submissions: MinedSubmissions,
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
    // Keep the deadline outside the cancellable tip future. Other mining events must not extend
    // how long a stuck submission can suppress this committed tip.
    let mut submission_deadline = None;

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
                &mut chain_state,
                &mut mined_block_broadcasts,
                &broadcast_network,
            );
        }

        let has_mined_receiver = mined_block_receiver
            .as_ref()
            .is_some_and(|receiver| !receiver.is_closed());
        let tip_change_close_to_network_tip_fut = wait_for_committed_tip(
            sync_status.clone(),
            chain_state.clone_for_task(),
            &local_submissions,
            &mut submission_deadline,
            has_mined_receiver,
        )
        .in_current_span();

        // TODO: Move this logic for selecting the first ready future and updating `chain_state` to its own method.
        //
        // Prefer mined-block completions and submissions when multiple
        // branches are ready. The committed-tip path is a fallback, so
        // selecting it first can duplicate a mined-block broadcast.
        let event = next_gossip_event(
            mined_block_receiver.as_mut(),
            &mut mined_block_completion_receiver,
            tip_change_close_to_network_tip_fut,
        )
        .await;
        let event = prefer_queued_mined_block(event, mined_block_receiver.as_mut());
        let (((hash, height), log_msg, updated_chain_state), is_block_submission, early, size) =
            match event {
                GossipEvent::MinedBlockChannelClosed => {
                    mined_block_receiver = None;
                    continue;
                }
                GossipEvent::MinedBlockBroadcastCompleted(completed) => {
                    finish_mined_block_broadcast(
                        completed,
                        &mut chain_state,
                        &mut mined_block_broadcasts,
                        &broadcast_network,
                    );
                    continue;
                }
                GossipEvent::MinedBlock(MinedBlockEvent::Early {
                    hash,
                    height,
                    size,
                    submitted_at,
                    submission,
                    pending,
                }) => (
                    (
                        (hash, height),
                        "sending early mined block broadcast",
                        chain_state,
                    ),
                    true,
                    Some((pending, submitted_at, submission)),
                    Some(size),
                ),
                GossipEvent::MinedBlock(MinedBlockEvent::Committed { hash, height, size }) => (
                    (
                        (hash, height),
                        "sending committed mined block broadcast",
                        chain_state,
                    ),
                    true,
                    None,
                    Some(size),
                ),
                GossipEvent::CommittedTip(tip_change_close_to_network_tip) => {
                    let (best_tip, chain_tip) = tip_change_close_to_network_tip?;
                    (
                        (best_tip, "sending committed block broadcast", chain_tip),
                        false,
                        None,
                        None,
                    )
                }
            };

        chain_state = updated_chain_state;

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

        info!(?height, ?hash, ?size, is_block_submission, log_msg);
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
            let broadcast = broadcast_with_timeout(network, request);
            let succeeded = match early {
                Some((mut pending, submitted_at, submission)) => {
                    if !pending.is_valid() {
                        false
                    } else {
                        let succeeded = tokio::select! {
                            biased;
                            _ = pending.wait_for_failure() => false,
                            succeeded = broadcast => succeeded,
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
                None => broadcast.await,
            };

            if let Some(completion_tx) = completion_tx {
                let _ = completion_tx.send(MinedBlockBroadcastCompleted { block, succeeded });
            }
        });
    }
}

/// Returns the latest committed tip eligible for ordinary relay and its updated monitor.
///
/// A local submission queues its final mining event before releasing its reservation. Wait for
/// that release instead of sleeping, while following newer tips. The caller still selects mining
/// events and completions first, and rechecks queued events before accepting this fallback.
/// `submission_deadline` survives cancellation so unrelated events cannot keep resetting the bound.
async fn wait_for_committed_tip(
    mut sync_status: SyncStatus,
    mut chain_tip: ChainTipChange,
    local_submissions: &MinedSubmissions,
    submission_deadline: &mut Option<(block::Hash, Instant)>,
    has_mined_receiver: bool,
) -> Result<((block::Hash, block::Height), ChainTipChange), BlockGossipError> {
    let mut tip_action = chain_tip.wait_for_tip_change().await.map_err(TipChange)?;
    loop {
        sync_status
            .wait_until_close_to_tip()
            .map_err(SyncStatus)
            .await?;
        tip_action = chain_tip.last_tip_change().unwrap_or(tip_action);
        let best_tip = tip_action.best_tip_hash_and_height();
        let hash = best_tip.0;

        if !has_mined_receiver || !local_submissions.contains(&hash) {
            *submission_deadline = None;
            return Ok((best_tip, chain_tip));
        }

        let deadline = match *submission_deadline {
            Some((pending, deadline)) if pending == hash => deadline,
            _ => {
                let deadline = Instant::now() + BLOCK_GOSSIP_TIMEOUT;
                *submission_deadline = Some((hash, deadline));
                deadline
            }
        };
        if Instant::now() >= deadline {
            warn!(
                ?hash,
                "local submission stalled; gossiping the committed tip"
            );
            return Ok((best_tip, chain_tip));
        }

        tokio::select! {
            biased;
            changed = chain_tip.wait_for_tip_change() => {
                tip_action = changed.map_err(TipChange)?;
            }
            _ = tokio::time::timeout_at(
                deadline,
                local_submissions.wait_until_released(hash),
            ) => {}
        }
    }
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

/// Sends a block broadcast, returning `true` if it succeeded within [`BLOCK_GOSSIP_TIMEOUT`].
async fn broadcast_with_timeout<ZN>(network: ZN, request: zn::Request) -> bool
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError>,
{
    tokio::time::timeout(BLOCK_GOSSIP_TIMEOUT, network.oneshot(request))
        .await
        .is_ok_and(|result| result.is_ok())
}

/// Records a completed mined-block broadcast, and advertises the hash through the committed-tip
/// fallback if the broadcast failed.
fn finish_mined_block_broadcast<ZN>(
    completed: MinedBlockBroadcastCompleted,
    chain_state: &mut ChainTipChange,
    mined_block_broadcasts: &mut MinedBlockBroadcasts,
    broadcast_network: &ZN,
) where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + Clone + 'static,
    ZN::Future: Send,
{
    let hash = completed.block.hash;
    let needs_fallback = mined_block_broadcasts.finish(completed);

    if completed.succeeded {
        chain_state.mark_last_change_hash(hash);
        return;
    }

    // A newer tip gets its own committed-tip broadcast.
    if !needs_fallback || chain_state.latest_chain_tip().best_tip_hash() != Some(hash) {
        return;
    }

    let request = zn::Request::AdvertiseMinedBlock(completed.block);
    info!(
        ?request,
        "sending committed block broadcast after the mined block broadcast failed",
    );
    chain_state.mark_last_change_hash(hash);
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
    use super::{
        next_gossip_event, prefer_queued_mined_block, GossipEvent, MinedBlockBroadcastCompleted,
        MinedBlockBroadcasts,
    };

    use std::future;

    use tokio::sync::mpsc;
    use zakura_chain::block;
    use zakura_rpc::MinedBlockEvent;

    // Repeat the vector so removing `biased;` reliably exposes randomized
    // selection among the ready events.
    const READY_EVENT_ATTEMPTS: usize = 64;

    #[tokio::test]
    async fn ready_gossip_events_are_selected_in_priority_order() {
        let submitted_hash = block::Hash([1; 32]);

        for _ in 0..READY_EVENT_ATTEMPTS {
            let (mined_block_sender, mut mined_block_receiver) = mpsc::unbounded_channel();
            let (mark_sender, mut mark_receiver) = mpsc::unbounded_channel();

            mined_block_sender
                .send(MinedBlockEvent::Committed {
                    hash: submitted_hash,
                    height: block::Height(1),
                    size: 1_000,
                })
                .unwrap();
            let completed = MinedBlockBroadcastCompleted {
                block: submitted_hash.into(),
                succeeded: true,
            };
            mark_sender.send(completed).unwrap();

            let event = next_gossip_event(
                Some(&mut mined_block_receiver),
                &mut mark_receiver,
                future::ready(()),
            )
            .await;

            assert!(matches!(
                event,
                GossipEvent::MinedBlockBroadcastCompleted(event) if event == completed
            ));

            let event = next_gossip_event(
                Some(&mut mined_block_receiver),
                &mut mark_receiver,
                future::ready(()),
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
                &mut mark_receiver,
                future::ready("committed tip"),
            )
            .await;

            assert!(matches!(event, GossipEvent::CommittedTip("committed tip")));
        }
    }

    /// A local submission's final event can be queued after the selector polled the mined-block
    /// channel and before the committed tip resolved in the same poll. The queued event still
    /// wins, so the block stays on the mined path.
    #[tokio::test]
    async fn mined_block_queued_while_the_tip_resolves_is_preferred() {
        let submitted_hash = block::Hash([3; 32]);
        let (mined_block_sender, mut mined_block_receiver) = mpsc::unbounded_channel();
        let (_mark_sender, mut mark_receiver) = mpsc::unbounded_channel();

        // The RPC sends its final event while the tip future is polled, after the selector found
        // the mined-block channel empty.
        let tip = async {
            mined_block_sender
                .send(MinedBlockEvent::Committed {
                    hash: submitted_hash,
                    height: block::Height(3),
                    size: 1_000,
                })
                .unwrap();
            Ok::<_, ()>("committed tip")
        };
        let event =
            next_gossip_event(Some(&mut mined_block_receiver), &mut mark_receiver, tip).await;
        assert!(
            matches!(event, GossipEvent::CommittedTip(Ok("committed tip"))),
            "the selector polled the empty channel first"
        );

        let event = prefer_queued_mined_block(event, Some(&mut mined_block_receiver));
        assert!(matches!(
            event,
            GossipEvent::MinedBlock(MinedBlockEvent::Committed { hash, .. }) if hash == submitted_hash
        ));
    }

    /// Without a queued mined-block event, a committed tip is used, and a failed tip is not
    /// replaced.
    #[test]
    fn committed_tip_is_kept_without_a_queued_mined_block() {
        let (mined_block_sender, mut mined_block_receiver) = mpsc::unbounded_channel();

        let event = prefer_queued_mined_block(
            GossipEvent::CommittedTip(Ok::<_, ()>("committed tip")),
            Some(&mut mined_block_receiver),
        );
        assert!(matches!(
            event,
            GossipEvent::CommittedTip(Ok("committed tip"))
        ));

        mined_block_sender
            .send(MinedBlockEvent::Committed {
                hash: block::Hash([4; 32]),
                height: block::Height(4),
                size: 1_000,
            })
            .unwrap();
        let event = prefer_queued_mined_block(
            GossipEvent::CommittedTip(Err::<&str, _>("tip error")),
            Some(&mut mined_block_receiver),
        );
        assert!(matches!(event, GossipEvent::CommittedTip(Err("tip error"))));

        let event = prefer_queued_mined_block(
            GossipEvent::CommittedTip(Ok::<_, ()>("committed tip")),
            None,
        );
        assert!(matches!(
            event,
            GossipEvent::CommittedTip(Ok("committed tip"))
        ));
    }

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
