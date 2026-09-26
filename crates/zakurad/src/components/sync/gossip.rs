//! A task that gossips newly verified [`block::Hash`]es to peers.
//!
//! [`block::Hash`]: zakura_chain::block::Hash

use std::{future::Future, time::Duration};

use futures::future::BoxFuture;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tower::{Service, ServiceExt};
use tracing::Instrument;

use zakura_chain::block;
use zakura_network as zn;
use zakura_rpc::MinedBlockEvent;
use zakura_state::ChainTipChange;

use crate::{
    components::sync::{SyncStatus, TIPS_RESPONSE_TIMEOUT},
    BoxError,
};

use BlockGossipError::*;

#[derive(Debug)]
enum GossipEvent<T> {
    MinedBlockBroadcastCompleted(block::Hash),
    OrdinaryBroadcastCompleted(block::Hash, bool),
    OrdinaryBroadcastReady,
    MinedBlock(MinedBlockEvent),
    CommittedTip(T),
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

    // Keep the ordinary operation here rather than spawning it: its readiness wait and send
    // share one deadline, cancellation drops it, and tip churn cannot create overlapping sends.
    let mut ordinary_broadcast: Option<BoxFuture<'static, (block::Hash, bool)>> = None;
    let mut pending_tip = None;
    let mut retry_at = tokio::time::Instant::now();
    const RETRY_DELAY: Duration = Duration::from_secs(1);
    const WAIT_FOR_BLOCK_SUBMISSION_DELAY: Duration = Duration::from_micros(100);
    let mut submission_grace_until = retry_at;

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
            if let Some(tip) = chain_state.last_tip_change() {
                pending_tip = Some(tip.best_tip_hash_and_height());
                submission_grace_until =
                    tokio::time::Instant::now() + WAIT_FOR_BLOCK_SUBMISSION_DELAY;
            }
            if pending_tip.is_some_and(|(pending_hash, _)| pending_hash == hash) {
                pending_tip = None;
            }
        }

        // TODO: Refactor this into a struct and move the contents of this loop into its own method.
        let mut chain_tip = chain_state.clone_for_task();
        let can_broadcast = ordinary_broadcast.is_none() && pending_tip.is_some();
        let mut broadcast_sync_status = sync_status.clone();

        // Observe tips even while sending or catching up. Keeping the observed cursor
        // separate from pending delivery also lets channel closure terminate failed retries.
        let tip_change_fut = async move {
            let tip_action = chain_tip.wait_for_tip_change().await.map_err(TipChange)?;
            let best_tip = chain_tip
                .last_tip_change()
                .unwrap_or(tip_action)
                .best_tip_hash_and_height();
            Ok((best_tip, chain_tip))
        }
        .in_current_span();

        // TODO: Move this logic for selecting the first ready future and updating `chain_state` to its own method.
        //
        // Prefer mined-block completions and submissions when multiple
        // branches are ready. The committed-tip path is a fallback, so
        // selecting it first can duplicate a mined-block broadcast.
        let event = tokio::select! {
            biased;
            // Poll the in-flight operation first so a stream of mined notifications
            // cannot starve its send or timeout. Pending sends still allow mined events.
            (hash, succeeded) = async {
                match ordinary_broadcast.as_mut() {
                    Some(broadcast) => broadcast.await,
                    None => std::future::pending().await,
                }
            } => GossipEvent::OrdinaryBroadcastCompleted(hash, succeeded),
            event = next_gossip_event(
                mined_block_receiver.as_mut(),
                &mut mined_block_mark_receiver,
                tip_change_fut,
            ) => event,
            ready = async {
                if !can_broadcast {
                    std::future::pending::<()>().await;
                }
                tokio::time::sleep_until(retry_at.max(submission_grace_until)).await;
                broadcast_sync_status.wait_until_close_to_tip().await.map_err(SyncStatus)
            } => {
                ready?;
                GossipEvent::OrdinaryBroadcastReady
            },
        };
        let (((hash, height), log_msg), is_block_submission, early) = match event {
            GossipEvent::MinedBlockBroadcastCompleted(mark_hash) => {
                // Observe a newer tip before suppressing its mined announcement. Otherwise
                // marking an unobserved tip could leave an obsolete failed tip pending forever.
                if let Some(tip) = chain_state.last_tip_change() {
                    pending_tip = Some(tip.best_tip_hash_and_height());
                    submission_grace_until =
                        tokio::time::Instant::now() + WAIT_FOR_BLOCK_SUBMISSION_DELAY;
                }
                if pending_tip.is_some_and(|(hash, _)| hash == mark_hash) {
                    pending_tip = None;
                }
                continue;
            }
            GossipEvent::OrdinaryBroadcastCompleted(hash, succeeded) => {
                ordinary_broadcast = None;
                if succeeded {
                    // A late completion must not clear a newer pending selected tip.
                    if pending_tip.is_some_and(|(pending_hash, _)| pending_hash == hash) {
                        pending_tip = None;
                    }
                    retry_at = tokio::time::Instant::now();
                } else {
                    retry_at = tokio::time::Instant::now() + RETRY_DELAY;
                }
                continue;
            }
            GossipEvent::MinedBlock(MinedBlockEvent::Early {
                hash,
                height,
                submitted_at,
                pending,
            }) => (
                ((hash, height), "sending early mined block broadcast"),
                true,
                Some((pending, submitted_at)),
            ),
            GossipEvent::MinedBlock(MinedBlockEvent::Committed { hash, height }) => (
                ((hash, height), "sending committed mined block broadcast"),
                true,
                None,
            ),
            GossipEvent::CommittedTip(tip_change_close_to_network_tip) => {
                let (tip, updated_chain_state) = tip_change_close_to_network_tip?;
                chain_state = updated_chain_state;
                pending_tip = Some(tip);
                submission_grace_until =
                    tokio::time::Instant::now() + WAIT_FOR_BLOCK_SUBMISSION_DELAY;
                continue;
            }
            GossipEvent::OrdinaryBroadcastReady => (
                (
                    pending_tip.expect("a pending tip enables the broadcast branch"),
                    "sending committed block broadcast",
                ),
                false,
                None,
            ),
        };

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
        if !is_block_submission {
            ordinary_broadcast = Some(Box::pin(async move {
                let succeeded =
                    tokio::time::timeout(TIPS_RESPONSE_TIMEOUT, network.oneshot(request))
                        .await
                        .is_ok_and(|result| result.is_ok());
                (hash, succeeded)
            }));
            continue;
        }
        let mark_tx = mined_block_mark_sender.clone();
        // Only a committed broadcast may suppress the committed-tip fallback. Early inventory
        // advertises a hash whose body this node cannot serve yet, so a peer that follows it can
        // exhaust `PENDING_BLOCK_WAIT` and receive `notfound`. The fallback is what re-advertises
        // the hash to that peer when the later committed broadcast also fails, so marking on an
        // early broadcast would remove the last prompt. Marking here is also redundant: a block
        // that commits always sends `Committed`, and that broadcast marks the same hash.
        let marks_broadcast = is_block_submission && early.is_none();
        tokio::spawn(async move {
            let broadcast = async move {
                tokio::time::timeout(TIPS_RESPONSE_TIMEOUT, network.oneshot(request))
                    .await
                    .is_ok_and(|result| result.is_ok())
            };
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
