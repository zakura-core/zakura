//! A task that gossips newly verified [`block::Hash`]es to peers.
//!
//! [`block::Hash`]: zakura_chain::block::Hash

use std::time::Duration;

use futures::TryFutureExt;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tower::{timeout::Timeout, Service, ServiceExt};
use tracing::Instrument;

use zakura_chain::{block, chain_tip::ChainTip};
use zakura_network as zn;
use zakura_state::ChainTipChange;

use crate::{
    components::sync::{SyncStatus, PEER_GOSSIP_DELAY, TIPS_RESPONSE_TIMEOUT},
    BoxError,
};

use BlockGossipError::*;

/// How many completed mined block broadcasts can wait to mark the chain tip.
/// In normal operations, we expect at most 1 pending mark.
/// The main loop can be busy for several seconds in the committed-tip path.
/// During that window, multiple mined-block broadcasts could finish and
/// try to send marks. A capacity of 16 with 25-75s block times
/// is chosen arbitrarily high to be safe.
const MINED_BLOCK_MARK_CHANNEL_CAPACITY: usize = 16;

/// Errors that can occur when gossiping committed blocks
#[derive(Error, Debug)]
pub enum BlockGossipError {
    #[error("chain tip sender was dropped")]
    TipChange(watch::error::RecvError),

    #[error("sync status sender was dropped")]
    SyncStatus(watch::error::RecvError),

    #[error("permanent peer set failure")]
    PeerSetReadiness(zn::BoxError),
}

/// Mark the chain tip hash as gossiped after a successful mined block broadcast.
///
/// This suppresses the committed tip gossip path for the same hash, but only
/// after the all-peers broadcast completes successfully.
fn apply_mined_block_mark(
    chain_state: &mut ChainTipChange,
    mined_block_channel_empty: bool,
    hash: block::Hash,
) {
    if mined_block_channel_empty
        && chain_state.latest_chain_tip().best_tip_hash() == Some(hash)
    {
        chain_state.mark_last_change_hash(hash);
    }
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
    mut mined_block_receiver: Option<mpsc::Receiver<(block::Hash, block::Height)>>,
) -> Result<(), BlockGossipError>
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + Clone + 'static,
    ZN::Future: Send,
{
    info!("initializing block gossip task");

    // use the same timeout as tips requests,
    // so broadcasts don't delay the syncer too long
    let mut broadcast_network = Timeout::new(broadcast_network, TIPS_RESPONSE_TIMEOUT);

    let (mined_block_mark_sender, mut mined_block_mark_receiver) =
        mpsc::channel(MINED_BLOCK_MARK_CHANNEL_CAPACITY);

    loop {
        while let Ok(hash) = mined_block_mark_receiver.try_recv() {
            apply_mined_block_mark(
                &mut chain_state,
                mined_block_receiver
                    .as_ref()
                    .is_none_or(mpsc::Receiver::is_empty),
                hash,
            );
        }

        // TODO: Refactor this into a struct and move the contents of this loop into its own method.
        let mut sync_status = sync_status.clone();
        let mut chain_tip = chain_state.clone_for_task();

        // TODO: Move the contents of this async block to its own method
        let tip_change_close_to_network_tip_fut = async move {
            /// A brief duration to wait after a tip change for a new message in the mined block channel.
            // TODO: Add a test to check that Zebra does not advertise mined blocks to peers twice.
            const WAIT_FOR_BLOCK_SUBMISSION_DELAY: Duration = Duration::from_micros(100);

            // wait for at least the network timeout between gossips
            //
            // in practice, we expect blocks to arrive approximately every 75 seconds,
            // so waiting 6 seconds won't make much difference
            tokio::time::sleep(PEER_GOSSIP_DELAY).await;

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
        let (((hash, height), log_msg, updated_chain_state), is_block_submission) =
            if let Some(mined_block_receiver) = mined_block_receiver.as_mut() {
                tokio::select! {
                    tip_change_close_to_network_tip = tip_change_close_to_network_tip_fut => {
                        (tip_change_close_to_network_tip?, false)
                    },

                    Some(tip_change) = mined_block_receiver.recv() => {
                       ((tip_change, "sending mined block broadcast", chain_state), true)
                    },

                    Some(mark_hash) = mined_block_mark_receiver.recv() => {
                        apply_mined_block_mark(
                            &mut chain_state,
                            mined_block_receiver.is_empty(),
                            mark_hash,
                        );
                        continue;
                    },
                }
            } else {
                tokio::select! {
                    tip_change_close_to_network_tip = tip_change_close_to_network_tip_fut => {
                        (tip_change_close_to_network_tip?, false)
                    },

                    Some(mark_hash) = mined_block_mark_receiver.recv() => {
                        apply_mined_block_mark(&mut chain_state, true, mark_hash);
                        continue;
                    },
                }
            };

        chain_state = updated_chain_state;

        // TODO: Move logic for calling the peer set to its own method.

        // block broadcasts inform other nodes about new blocks,
        // so our internal Grow or Reset state doesn't matter to them
        let request = if is_block_submission {
            zn::Request::AdvertiseBlockToAll(hash)
        } else {
            zn::Request::AdvertiseBlock(hash, None)
        };

        info!(?height, ?request, log_msg);
        let broadcast_fut = broadcast_network
            .ready()
            .await
            .map_err(PeerSetReadiness)?
            .call(request);

        // Await the broadcast future in a spawned task to avoid waiting on
        // `AdvertiseBlockToAll` requests when there are unready peers.
        // Broadcast requests don't return errors, and we'd just want to ignore them anyway.
        if is_block_submission {
            let mark_tx = mined_block_mark_sender.clone();
            let submission_hash = hash;
            tokio::spawn(async move {
                if broadcast_fut.await.is_ok() {
                    let _ = mark_tx.send(submission_hash).await;
                }
            });
        } else {
            tokio::spawn(broadcast_fut);
        }
    }
}
