//! A task that gossips newly verified [`block::Hash`]es to peers.
//!
//! [`block::Hash`]: zakura_chain::block::Hash

use std::{collections::HashMap, future::Future, time::Duration};

use futures::TryFutureExt;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tower::{Service, ServiceExt};
use tracing::Instrument;

use zakura_chain::{block, chain_tip::ChainTip};
use zakura_network as zn;
use zakura_rpc::MinedBlockEvent;
use zakura_state::ChainTipChange;

use crate::{
    components::sync::{SyncStatus, TIPS_RESPONSE_TIMEOUT},
    BoxError,
};

use BlockGossipError::*;

/// A spawned committed mined-block broadcast finished.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MinedBlockBroadcastCompleted {
    hash: block::Hash,
    succeeded: bool,
}

#[derive(Debug)]
enum GossipEvent<T> {
    MinedBlockBroadcastCompleted(MinedBlockBroadcastCompleted),
    MinedBlock(MinedBlockEvent),
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

            Some(tip_change) = mined_block_receiver.recv() => {
                GossipEvent::MinedBlock(tip_change)
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
/// [`block::Hash`]: zakura_chain::block::Hash
pub async fn gossip_best_tip_block_hashes<ZN>(
    sync_status: SyncStatus,
    mut chain_state: ChainTipChange,
    broadcast_network: ZN,
    mut mined_block_receiver: Option<mpsc::UnboundedReceiver<MinedBlockEvent>>,
) -> Result<(), BlockGossipError>
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + Clone + 'static,
    ZN::Future: Send,
{
    info!("initializing block gossip task");

    let (mined_block_completion_sender, mut mined_block_completion_receiver) =
        mpsc::unbounded_channel();
    let mut mined_block_broadcasts = MinedBlockBroadcasts::default();

    loop {
        // Drain local completion notifications from spawned mined-block
        // broadcasts before deciding whether the committed-tip fallback should
        // run. These are not peer acknowledgements: a success appears here only
        // after our `AdvertiseBlockToAll` future completed successfully.
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

        // TODO: Refactor this into a struct and move the contents of this loop into its own method.
        let mut sync_status = sync_status.clone();
        let mut chain_tip = chain_state.clone_for_task();

        // TODO: Move the contents of this async block to its own method
        let tip_change_close_to_network_tip_fut = async move {
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

            Ok((best_tip, "sending committed block broadcast", chain_tip))
        }
        .in_current_span();

        // TODO: Move this logic for selecting the first ready future and updating `chain_state` to its own method.
        //
        // Prefer mined-block completions and submissions when multiple
        // branches are ready. The committed-tip path is a fallback, so
        // selecting it first can duplicate a mined-block broadcast.
        let (((hash, height), log_msg, updated_chain_state), is_block_submission, early) =
            match next_gossip_event(
                mined_block_receiver.as_mut(),
                &mut mined_block_completion_receiver,
                tip_change_close_to_network_tip_fut,
            )
            .await
            {
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
                    submitted_at,
                    pending,
                }) => (
                    (
                        (hash, height),
                        "sending early mined block broadcast",
                        chain_state,
                    ),
                    true,
                    Some((pending, submitted_at)),
                ),
                GossipEvent::MinedBlock(MinedBlockEvent::Committed { hash, height }) => (
                    (
                        (hash, height),
                        "sending committed mined block broadcast",
                        chain_state,
                    ),
                    true,
                    None,
                ),
                GossipEvent::CommittedTip(tip_change_close_to_network_tip) => {
                    (tip_change_close_to_network_tip?, false, None)
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

        // block broadcasts inform other nodes about new blocks,
        // so our internal Grow or Reset state doesn't matter to them
        let request = if is_block_submission {
            zn::Request::AdvertiseBlockToAll(hash)
        } else {
            zn::Request::AdvertiseBlock(hash, None)
        };

        info!(?height, ?request, log_msg);
        // Include readiness in the deadline. The event loop must keep consuming lifecycle and tip
        // events when the peer set has no ready service.
        let network = broadcast_network.clone();
        // Only a committed broadcast may suppress the committed-tip fallback. Early inventory
        // advertises a hash whose body this node cannot serve yet, so a peer that follows it can
        // exhaust `PENDING_BLOCK_WAIT` and receive `notfound`. The fallback is what re-advertises
        // the hash to that peer when the later committed broadcast also fails, so marking on an
        // early broadcast would remove the last prompt. Marking here is also redundant: a block
        // that commits always sends `Committed`, and that broadcast marks the same hash.
        let completion_tx = (is_block_submission && early.is_none()).then(|| {
            mined_block_broadcasts.start(hash);
            mined_block_completion_sender.clone()
        });
        tokio::spawn(async move {
            let broadcast = broadcast_with_timeout(network, request);
            let succeeded = match early {
                Some((mut pending, submitted_at)) => {
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
                            metrics::histogram!("mining.submit_to_inventory.duration_seconds")
                                .record(submitted_at.elapsed().as_secs_f64());
                        }
                        succeeded
                    }
                }
                None => broadcast.await,
            };

            if let Some(completion_tx) = completion_tx {
                let _ = completion_tx.send(MinedBlockBroadcastCompleted { hash, succeeded });
            }
        });
    }
}

/// Sends a block broadcast, returning `true` if it succeeded within [`TIPS_RESPONSE_TIMEOUT`].
async fn broadcast_with_timeout<ZN>(network: ZN, request: zn::Request) -> bool
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError>,
{
    tokio::time::timeout(TIPS_RESPONSE_TIMEOUT, network.oneshot(request))
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
    let hash = completed.hash;
    let needs_fallback = mined_block_broadcasts.finish(completed);

    if completed.succeeded {
        chain_state.mark_last_change_hash(hash);
        return;
    }

    // A newer tip gets its own committed-tip broadcast.
    if !needs_fallback || chain_state.latest_chain_tip().best_tip_hash() != Some(hash) {
        return;
    }

    let request = zn::Request::AdvertiseBlock(hash, None);
    info!(
        ?request,
        "sending committed block broadcast after the mined block broadcast failed",
    );
    chain_state.mark_last_change_hash(hash);
    tokio::spawn(broadcast_with_timeout(broadcast_network.clone(), request));
}

/// Committed mined-block broadcasts, tracked so the committed-tip path does not duplicate them.
#[derive(Debug, Default)]
struct MinedBlockBroadcasts {
    /// Broadcasts that have not reported completion, counted per hash.
    ///
    /// Every spawned broadcast reports completion within [`TIPS_RESPONSE_TIMEOUT`], so entries
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
        let MinedBlockBroadcastCompleted { hash, succeeded } = completed;

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
        next_gossip_event, GossipEvent, MinedBlockBroadcastCompleted, MinedBlockBroadcasts,
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
                })
                .unwrap();
            let completed = MinedBlockBroadcastCompleted {
                hash: submitted_hash,
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

    #[test]
    fn failed_mined_block_broadcast_needs_fallback_once_nothing_is_in_flight() {
        let hash = block::Hash([2; 32]);
        let failed = MinedBlockBroadcastCompleted {
            hash,
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
            hash,
            succeeded: true,
        }));
        assert!(!broadcasts.finish(MinedBlockBroadcastCompleted {
            hash,
            succeeded: false,
        }));
        assert!(!broadcasts.is_in_flight(&hash));
    }
}
