//! A task that gossips newly verified [`block::Hash`]es to peers.
//!
//! [`block::Hash`]: zakura_chain::block::Hash

use std::{collections::HashMap, future::Future, time::Duration};

use thiserror::Error;
use tokio::{
    sync::{mpsc, watch},
    task::{Id, JoinError, JoinSet},
};
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

/// Maximum active early/committed mined relay operations owned by the gossip task.
pub(super) const MAX_MINED_BROADCASTS: usize = 64;

#[derive(Debug)]
enum GossipEvent<T> {
    MinedBlockBroadcastCompleted(Result<(Id, bool), JoinError>),
    OrdinaryBroadcastCompleted(block::Hash, bool),
    OrdinaryBroadcastReady,
    MinedBlock(MinedBlockEvent),
    CommittedTip(T),
}

async fn next_gossip_event<T>(
    mined_block_receiver: Option<&mut mpsc::UnboundedReceiver<MinedBlockEvent>>,
    mined_broadcasts: &mut JoinSet<bool>,
    committed_tip_fut: impl Future<Output = T>,
) -> GossipEvent<T> {
    if let Some(mined_block_receiver) = mined_block_receiver {
        tokio::select! {
            biased;

            Some(completion) = mined_broadcasts.join_next_with_id() => {
                GossipEvent::MinedBlockBroadcastCompleted(completion)
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

            Some(completion) = mined_broadcasts.join_next_with_id() => {
                GossipEvent::MinedBlockBroadcastCompleted(completion)
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

    // Bound active work and metadata; excess lifecycle notifications stay in the existing
    // input channel. Joining owned tasks also preserves panic isolation and shutdown cleanup.
    let mut mined_broadcasts = JoinSet::new();
    let mut mined_in_flight = HashMap::new();

    // Own at most one ordinary task: readiness and send share one deadline, shutdown
    // aborts it, and failed joins release the slot just like ordinary send failures.
    let mut ordinary_broadcast = JoinSet::new();
    let mut ordinary_hash = None;
    let mut pending_tip = None;
    let mut retry_at = tokio::time::Instant::now();
    const RETRY_DELAY: Duration = Duration::from_secs(1);
    const WAIT_FOR_BLOCK_SUBMISSION_DELAY: Duration = Duration::from_micros(100);
    let mut submission_grace_until = retry_at;

    loop {
        // TODO: Refactor this into a struct and move the contents of this loop into its own method.
        let mut chain_tip = chain_state.clone_for_task();
        let can_broadcast = ordinary_hash.is_none()
            && pending_tip.is_some_and(|(hash, _)| {
                !mined_in_flight
                    .values()
                    .any(|(mined_hash, committed)| *committed && *mined_hash == hash)
            });
        let accept_mined = mined_in_flight.len() < MAX_MINED_BROADCASTS;
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
            // Observe ordinary completion first so a mined-notification backlog cannot
            // delay releasing its slot. Owned send tasks make progress independently.
            Some(completion) = ordinary_broadcast.join_next() => {
                let hash = ordinary_hash.expect("an ordinary task has its hash until it is joined");
                let succeeded = match completion {
                    Ok(succeeded) => succeeded,
                    Err(error) => {
                        warn!(%error, ?hash, "ordinary block broadcast task failed");
                        false
                    }
                };
                GossipEvent::OrdinaryBroadcastCompleted(hash, succeeded)
            },
            event = next_gossip_event(
                if accept_mined { mined_block_receiver.as_mut() } else { None },
                &mut mined_broadcasts,
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
            GossipEvent::MinedBlockBroadcastCompleted(completion) => {
                let (task_id, succeeded) = match completion {
                    Ok((task_id, succeeded)) => (task_id, succeeded),
                    Err(error) => {
                        warn!(%error, task_id = ?error.id(), broadcast = ?mined_in_flight.get(&error.id()),
                            "mined block broadcast task failed");
                        (error.id(), false)
                    }
                };
                let (mark_hash, committed) = mined_in_flight
                    .remove(&task_id)
                    .expect("each owned mined broadcast has metadata until it is joined");
                if !committed || !succeeded {
                    continue;
                }
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
                ordinary_hash = None;
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
            ordinary_hash = Some(hash);
            ordinary_broadcast.spawn(async move {
                tokio::time::timeout(TIPS_RESPONSE_TIMEOUT, network.oneshot(request))
                    .await
                    .is_ok_and(|result| result.is_ok())
            });
            continue;
        }
        // Early inventory cannot defer or suppress committed-body fallback: a peer may
        // have already exhausted its body wait before this block became available.
        let committed = early.is_none();
        let task = mined_broadcasts.spawn(async move {
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

            succeeded
        });
        mined_in_flight.insert(task.id(), (hash, committed));
    }
}

#[cfg(test)]
mod tests {
    use super::{next_gossip_event, GossipEvent};

    use std::future;

    use tokio::{sync::mpsc, task::JoinSet};
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
            let mut mined_broadcasts = JoinSet::new();

            mined_block_sender
                .send(MinedBlockEvent::Committed {
                    hash: submitted_hash,
                    height: block::Height(1),
                })
                .unwrap();
            mined_broadcasts.spawn(async { true });
            tokio::task::yield_now().await;

            let event = next_gossip_event(
                Some(&mut mined_block_receiver),
                &mut mined_broadcasts,
                future::ready(()),
            )
            .await;

            assert!(matches!(
                event,
                GossipEvent::MinedBlockBroadcastCompleted(Ok((_, true)))
            ));

            let event = next_gossip_event(
                Some(&mut mined_block_receiver),
                &mut mined_broadcasts,
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
                &mut mined_broadcasts,
                future::ready("committed tip"),
            )
            .await;

            assert!(matches!(event, GossipEvent::CommittedTip("committed tip")));
        }
    }
}
