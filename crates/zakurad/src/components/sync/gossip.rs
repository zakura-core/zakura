//! A task that gossips newly verified [`block::Hash`]es to peers.
//!
//! [`block::Hash`]: zakura_chain::block::Hash

use std::{future::Future, time::Duration};

use futures::TryFutureExt;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tower::{Service, ServiceExt};
use tracing::Instrument;

use zakura_chain::block;
use zakura_network as zn;
use zakura_rpc::MinedBlockEvent;
use zakura_state::ChainTipChange;

use crate::{
    components::sync::{SyncStatus, PEER_GOSSIP_DELAY, TIPS_RESPONSE_TIMEOUT},
    BoxError,
};

use BlockGossipError::*;

#[derive(Debug)]
enum GossipEvent<T> {
    MinedBlockBroadcastCompleted(block::Hash),
    MinedBlock(MinedBlockEvent),
    CommittedTip(T),
}

#[derive(Clone, Copy, Debug)]
struct CommittedTipTiming {
    pacing_delay: Duration,
    tip_to_request: Duration,
}

async fn next_gossip_event<T>(
    mined_block_receiver: Option<&mut mpsc::UnboundedReceiver<MinedBlockEvent>>,
    mined_block_mark_receiver: &mut mpsc::UnboundedReceiver<block::Hash>,
    committed_tip_fut: impl Future<Output = T>,
) -> GossipEvent<T> {
    if let Some(mined_block_receiver) = mined_block_receiver {
        tokio::select! {
            biased;

            Some(mark_hash) = mined_block_mark_receiver.recv() => {
                GossipEvent::MinedBlockBroadcastCompleted(mark_hash)
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

            Some(mark_hash) = mined_block_mark_receiver.recv() => {
                GossipEvent::MinedBlockBroadcastCompleted(mark_hash)
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

    let (mined_block_mark_sender, mut mined_block_mark_receiver) = mpsc::unbounded_channel();

    loop {
        // Drain local completion notifications from spawned mined-block
        // broadcasts before deciding whether the committed-tip fallback should
        // run. These are not peer acknowledgements: a hash appears here only
        // after our `AdvertiseBlockToAll` future completed successfully.
        //
        // `try_recv()` keeps this non-blocking. `Empty` just means no completed
        // broadcast has reported back yet, so the gossip loop can keep making
        // progress.
        while let Ok(hash) = mined_block_mark_receiver.try_recv() {
            chain_state.mark_last_change_hash(hash);
        }

        // TODO: Refactor this into a struct and move the contents of this loop into its own method.
        let mut sync_status = sync_status.clone();
        let mut chain_tip = chain_state.clone_for_task();

        // TODO: Move the contents of this async block to its own method
        let tip_change_close_to_network_tip_fut = async move {
            /// A brief duration to wait after a tip change for a new message in the mined block channel.
            // TODO: Add a test to check that Zakura does not advertise mined blocks to peers twice.
            const WAIT_FOR_BLOCK_SUBMISSION_DELAY: Duration = Duration::from_micros(100);

            // Observe tip changes on a separate cursor while preserving the existing pacing
            // behavior on `chain_tip`. This tells us how long a committed tip was held behind
            // the timer without consuming or changing the production gossip cursor.
            let pacing_deadline = Instant::now() + PEER_GOSSIP_DELAY;
            let pacing_sleep = tokio::time::sleep_until(pacing_deadline);
            tokio::pin!(pacing_sleep);
            let mut timing_chain_tip = chain_tip.clone_for_task();
            let tip_observed_before_deadline = tokio::select! {
                tip_change = timing_chain_tip.wait_for_tip_change() => {
                    tip_change.map_err(TipChange)?;
                    Some(Instant::now())
                }
                () = &mut pacing_sleep => None,
            };

            if tip_observed_before_deadline.is_some() {
                pacing_sleep.await;
            }

            // wait for at least one tip change, to make sure we have a new block hash to broadcast
            let tip_action = chain_tip.wait_for_tip_change().await.map_err(TipChange)?;
            let tip_observed_at = tip_observed_before_deadline.unwrap_or_else(Instant::now);

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
            let request_started_at = Instant::now();
            let timing = CommittedTipTiming {
                pacing_delay: pacing_deadline.saturating_duration_since(tip_observed_at),
                tip_to_request: request_started_at.saturating_duration_since(tip_observed_at),
            };

            Ok((
                best_tip,
                "sending committed block broadcast",
                chain_tip,
                Some(timing),
            ))
        }
        .in_current_span();

        // TODO: Move this logic for selecting the first ready future and updating `chain_state` to its own method.
        //
        // Prefer mined-block completions and submissions when multiple
        // branches are ready. The committed-tip path is a fallback, so
        // selecting it first can duplicate a mined-block broadcast.
        let (
            ((hash, height), log_msg, updated_chain_state, committed_tip_timing),
            is_block_submission,
            early,
        ) = match next_gossip_event(
            mined_block_receiver.as_mut(),
            &mut mined_block_mark_receiver,
            tip_change_close_to_network_tip_fut,
        )
        .await
        {
            GossipEvent::MinedBlockBroadcastCompleted(mark_hash) => {
                chain_state.mark_last_change_hash(mark_hash);
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
                    None,
                ),
                true,
                Some((pending, submitted_at)),
            ),
            GossipEvent::MinedBlock(MinedBlockEvent::Committed { hash, height }) => (
                (
                    (hash, height),
                    "sending committed mined block broadcast",
                    chain_state,
                    None,
                ),
                true,
                None,
            ),
            GossipEvent::CommittedTip(tip_change_close_to_network_tip) => {
                (tip_change_close_to_network_tip?, false, None)
            }
        };

        chain_state = updated_chain_state;

        if let Some(timing) = committed_tip_timing {
            let pacing_result = if timing.pacing_delay.is_zero() {
                "ready"
            } else {
                "blocked"
            };
            metrics::counter!(
                "block.gossip.committed_tip.pacing.total",
                "result" => pacing_result
            )
            .increment(1);
            metrics::histogram!("block.gossip.committed_tip.pacing_delay.duration_seconds")
                .record(timing.pacing_delay.as_secs_f64());
            metrics::histogram!("block.gossip.committed_tip.tip_to_request.duration_seconds")
                .record(timing.tip_to_request.as_secs_f64());
            info!(
                ?hash,
                ?height,
                pacing_result,
                pacing_delay_seconds = timing.pacing_delay.as_secs_f64(),
                tip_to_request_seconds = timing.tip_to_request.as_secs_f64(),
                "measured committed block gossip scheduling delay"
            );
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
        let mark_tx = mined_block_mark_sender.clone();
        // Only a committed broadcast may suppress the committed-tip fallback. Early inventory
        // advertises a hash whose body this node cannot serve yet, so a peer that follows it can
        // exhaust `PENDING_BLOCK_WAIT` and receive `notfound`. The fallback is what re-advertises
        // the hash to that peer when the later committed broadcast also fails, so marking on an
        // early broadcast would remove the last prompt. Marking here is also redundant: a block
        // that commits always sends `Committed`, and that broadcast marks the same hash.
        let marks_broadcast = is_block_submission && early.is_none();
        let broadcast_path = match (is_block_submission, early.is_some()) {
            (false, _) => "committed_tip",
            (true, true) => "mined_early",
            (true, false) => "mined_committed",
        };
        tokio::spawn(async move {
            let broadcast_started_at = Instant::now();
            let broadcast = async move {
                match tokio::time::timeout(TIPS_RESPONSE_TIMEOUT, network.oneshot(request)).await {
                    Ok(Ok(_)) => "success",
                    Ok(Err(_)) => "error",
                    Err(_) => "timeout",
                }
            };
            let broadcast_result = match early {
                Some((mut pending, submitted_at)) => {
                    if !pending.is_valid() {
                        "invalidated"
                    } else {
                        let broadcast_result = tokio::select! {
                            biased;
                            _ = pending.wait_for_failure() => "invalidated",
                            broadcast_result = broadcast => broadcast_result,
                        };
                        if broadcast_result == "success" {
                            metrics::counter!("mining.optimistic_inventory.early_inventories")
                                .increment(1);
                            metrics::histogram!("mining.submit_to_inventory.duration_seconds")
                                .record(submitted_at.elapsed().as_secs_f64());
                        }
                        broadcast_result
                    }
                }
                None => broadcast.await,
            };
            let succeeded = broadcast_result == "success";
            metrics::counter!(
                "block.gossip.broadcast.total",
                "path" => broadcast_path,
                "result" => broadcast_result
            )
            .increment(1);
            metrics::histogram!(
                "block.gossip.broadcast.duration_seconds",
                "path" => broadcast_path,
                "result" => broadcast_result
            )
            .record(broadcast_started_at.elapsed().as_secs_f64());

            if succeeded && marks_broadcast {
                let _ = mark_tx.send(hash);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{next_gossip_event, GossipEvent};

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
            mark_sender.send(submitted_hash).unwrap();

            let event = next_gossip_event(
                Some(&mut mined_block_receiver),
                &mut mark_receiver,
                future::ready(()),
            )
            .await;

            assert!(matches!(
                event,
                GossipEvent::MinedBlockBroadcastCompleted(hash) if hash == submitted_hash
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
}
