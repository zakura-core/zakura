//! Syncer task for maintaining a non-finalized [`ReadStateService`] and updating
//! [`ChainTipSender`] via RPCs.

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::task::JoinHandle;
use tonic::{Status, Streaming};
use tower::BoxError;
use zakura_chain::{
    block::{self, Block, Height},
    parameters::Network,
    serialization::BytesInDisplayOrder,
};
use zakura_state::{
    spawn_init_read_only, ChainTipBlock, ChainTipChange, ChainTipSender, CheckpointVerifiedBlock,
    HashOrHeight, LatestChainTip, NonFinalizedState, ReadStateService, SemanticallyVerifiedBlock,
    ValidateContextError, ZakuraDb,
};

use zakura_chain::diagnostic::task::WaitForPanics;

use crate::indexer::{
    indexer_client::IndexerClient, BlockAndHash, BlockRequest, Empty,
    NonFinalizedStateChangeRequest,
};

/// How long to wait between failed calls to
/// `subscribe_to_non_finalized_state_change`.
const POLL_DELAY: Duration = Duration::from_secs(5);

/// How long to wait for a message on a gRPC subscription stream before
/// assuming the stream is dead and re-subscribing.
///
/// Generous, because legitimate gaps between blocks or tip changes can be
/// several minutes. This is a backstop against a wedged connection that the
/// keep-alive ping below doesn't catch. Re-subscribing resumes from the
/// syncer's current chain tips, so a false trigger during a quiet period is
/// harmless.
const STREAM_MESSAGE_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// HTTP/2 keep-alive ping interval for the indexer gRPC connection, so a
/// half-open connection is detected instead of hanging a stream indefinitely.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);

/// How long to wait for a keep-alive response before treating the connection
/// as dead.
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// How long to wait before re-subscribing after a block fails to commit to the non-finalized state.
///
/// A block can persistently fail to commit (e.g. [`ValidateContextError::NotReadyToBeCommitted`])
/// when the secondary finalized state hasn't yet caught up with the primary. Without this delay,
/// re-subscribing immediately turns that into a full-speed busy loop that saturates the logs.
const COMMIT_RETRY_DELAY: Duration = Duration::from_secs(1);

/// How long to wait for a `get_block` fetch while bridging a finalized gap.
///
/// A co-located node should respond promptly, so this bounds a wedged
/// connection. The bridge retries on the next subscription.
const GET_BLOCK_TIMEOUT: Duration = Duration::from_secs(30);

/// How long to wait to establish a gRPC subscription stream before assuming
/// the request is wedged and retrying.
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(30);

/// Syncs non-finalized best-chain blocks from a trusted primary node's RPCs.
#[derive(Debug)]
pub struct TrustedChainSync {
    /// gRPC client for calling the primary node's indexer methods.
    pub indexer_rpc_client: IndexerClient<tonic::transport::Channel>,
    /// The read state service.
    db: ZakuraDb,
    /// The non-finalized state - currently only contains the best chain.
    non_finalized_state: NonFinalizedState,
    /// The chain tip sender for updating [`LatestChainTip`] and [`ChainTipChange`].
    chain_tip_sender: ChainTipSender,
    /// Publishes finalized tips without allowing stale snapshots to regress the
    /// active tip.
    finalized_tip_publisher: FinalizedTipPublisher,
    /// The non-finalized state sender, for updating the [`ReadStateService`]
    /// when the non-finalized best chain changes.
    non_finalized_state_sender: tokio::sync::watch::Sender<NonFinalizedState>,
    /// Flipped once `sync()` receives its first parseable block from the
    /// `non_finalized_state_change` stream. This stops
    /// [`update_finalized_chain_tip`], making `sync()` the sole caller of
    /// `try_catch_up_with_primary` on the shared secondary database.
    started_sync_sender: tokio::sync::watch::Sender<bool>,
    /// The finalized-tip updater, retained so `sync()` can wait for any in-flight
    /// secondary database catch-up before committing a streamed block.
    finalized_tip_updater: Option<JoinHandle<()>>,
}

/// Serializes finalized-tip publication and ignores stale lower snapshots.
///
/// The finalized-tip updater and the initial stream sync can read different
/// secondary database snapshots concurrently. Tracking the highest published
/// height under the same lock as the send prevents an older snapshot from
/// overwriting a newer one. Non-finalized publications do not use this guard,
/// because a valid best-chain reorg can reduce their height.
#[derive(Clone, Debug)]
struct FinalizedTipPublisher {
    inner: Arc<Mutex<FinalizedTipPublisherInner>>,
}

#[derive(Debug)]
struct FinalizedTipPublisherInner {
    sender: ChainTipSender,
    highest_published_height: Option<Height>,
}

impl FinalizedTipPublisher {
    fn new(sender: ChainTipSender) -> Self {
        Self {
            inner: Arc::new(Mutex::new(FinalizedTipPublisherInner {
                sender,
                highest_published_height: None,
            })),
        }
    }

    fn publish(&self, tip: ChainTipBlock) {
        let mut inner = self
            .inner
            .lock()
            .expect("finalized tip publication does not panic while holding the lock");

        if inner
            .highest_published_height
            .is_some_and(|published_height| tip.height < published_height)
        {
            tracing::debug!(
                stale_height = ?tip.height,
                highest_published_height = ?inner.highest_published_height,
                "ignoring stale finalized tip update"
            );
            return;
        }

        inner.highest_published_height = Some(tip.height);
        inner.sender.set_finalized_tip(tip);
    }
}

/// Signals the finalized-tip updater to stop, then waits for it to finish.
///
/// Waiting makes the handoff a barrier: after this function returns, the updater
/// cannot still be changing the secondary database's finalized view.
async fn stop_finalized_tip_updater(
    started_sync_sender: &tokio::sync::watch::Sender<bool>,
    finalized_tip_updater: JoinHandle<()>,
) {
    started_sync_sender.send_replace(true);
    finalized_tip_updater.wait_for_panics().await;
}

/// Returns `true` if `block_height` is at or below `finalized_tip_height`.
fn block_height_is_finalized(finalized_tip_height: Option<Height>, block_height: Height) -> bool {
    finalized_tip_height.is_some_and(|tip| block_height <= tip)
}

/// The result of trying to commit a streamed block.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum CommitOutcome {
    /// The block was committed to the local non-finalized state.
    Committed,
    /// The secondary database caught up past the block before it was committed.
    AlreadyFinalized,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum FinalizedGapBlockError {
    MissingHeight,
    UnexpectedHeight {
        expected: Height,
        actual: Height,
    },
    UnexpectedParentHash {
        expected: block::Hash,
        actual: block::Hash,
    },
    UnexpectedBlockHash {
        advertised: block::Hash,
        computed: block::Hash,
    },
}

fn validate_finalized_gap_block(
    expected_height: Height,
    expected_parent_hash: block::Hash,
    actual_height: Option<Height>,
    actual_parent_hash: block::Hash,
    advertised_hash: block::Hash,
    computed_hash: block::Hash,
) -> Result<(), FinalizedGapBlockError> {
    let actual_height = actual_height.ok_or(FinalizedGapBlockError::MissingHeight)?;

    if actual_height != expected_height {
        return Err(FinalizedGapBlockError::UnexpectedHeight {
            expected: expected_height,
            actual: actual_height,
        });
    }

    if actual_parent_hash != expected_parent_hash {
        return Err(FinalizedGapBlockError::UnexpectedParentHash {
            expected: expected_parent_hash,
            actual: actual_parent_hash,
        });
    }

    if advertised_hash != computed_hash {
        return Err(FinalizedGapBlockError::UnexpectedBlockHash {
            advertised: advertised_hash,
            computed: computed_hash,
        });
    }

    Ok(())
}

fn gap_fill_reached_expected_tip(
    actual_tip: Option<(Height, block::Hash)>,
    expected_tip: (Height, block::Hash),
) -> bool {
    actual_tip == Some(expected_tip)
}

async fn update_finalized_chain_tip(
    db: ZakuraDb,
    mut indexer_rpc_client: IndexerClient<tonic::transport::Channel>,
    finalized_tip_publisher: FinalizedTipPublisher,
    mut started_sync_receiver: tokio::sync::watch::Receiver<bool>,
) {
    let mut chain_tip_change_stream = None;

    loop {
        // Stop after `sync()` receives its first parseable non-finalized block.
        // From then on, `sync()` is the sole caller of
        // `try_catch_up_with_primary` on the shared secondary. This task must
        // not advance the secondary between `sync()`'s check and commit.
        // `sync()` also owns the published chain tip from then on, so publishing
        // our lagging finalized tip would drag the reported tip backwards.
        if *started_sync_receiver.borrow() {
            return;
        }

        let Some(ref mut chain_tip_change) = chain_tip_change_stream else {
            chain_tip_change_stream = match tokio::time::timeout(
                SUBSCRIBE_TIMEOUT,
                indexer_rpc_client.chain_tip_change(Empty {}),
            )
            .await
            {
                Ok(Ok(response)) => Some(response.into_inner()),
                Ok(Err(err)) => {
                    tracing::warn!(?err, "failed to subscribe to chain tip changes");
                    tokio::time::sleep(POLL_DELAY).await;
                    None
                }
                Err(_) => {
                    tracing::warn!("timed out subscribing to chain tip changes");
                    tokio::time::sleep(POLL_DELAY).await;
                    None
                }
            };

            continue;
        };

        // The message only signals that the primary's best chain advanced. We
        // publish our finalized tip below, not the primary's non-finalized tip,
        // so the hash is unused.
        //
        // Stop immediately if `sync()` takes over while we're parked here, so
        // we never catch up concurrently with `sync()`'s commits.
        let message = tokio::select! {
            biased;
            _ = started_sync_receiver.changed() => return,
            message = tokio::time::timeout(STREAM_MESSAGE_TIMEOUT, chain_tip_change.message()) => message,
        };

        match message {
            Ok(Ok(Some(_block_hash_and_height))) => {}
            Ok(Ok(None)) => {
                tracing::warn!("chain_tip_change stream ended unexpectedly");
                chain_tip_change_stream = None;
                continue;
            }
            Ok(Err(err)) => {
                tracing::warn!(?err, "error receiving chain tip change");
                chain_tip_change_stream = None;
                continue;
            }
            Err(_) => {
                tracing::debug!("chain tip change stream timed out, re-subscribing");
                chain_tip_change_stream = None;
                continue;
            }
        }

        // Don't advance the secondary or publish once `sync()` has taken over,
        // even if it did so while we waited above.
        if *started_sync_receiver.borrow() {
            return;
        }

        // Catch the secondary's finalized state up to the primary, then publish
        // its finalized tip.
        //
        // This keeps the finalized tip current while the secondary is catching
        // up, before `TrustedChainSync::sync()` has non-finalized blocks to
        // publish.
        //
        // # Correctness
        //
        // Catching up with the primary database concurrently with commits to
        // the non-finalized state while the primary node is syncing rapidly
        // could cause repeated commit failures.
        if let Err(error) = db.spawn_try_catch_up_with_primary().await {
            tracing::debug!(
                ?error,
                "failed to catch up to the primary database while updating the finalized tip"
            );
            continue;
        }

        if let Some(tip_block) = finalized_chain_tip_block(&db).await {
            // Re-check immediately before publishing, after the awaits above:
            // `sync()` can take over while we wait on
            // `spawn_try_catch_up_with_primary` or `finalized_chain_tip_block`,
            // so checking earlier could still publish the stale tip.
            //
            // `sync()`'s `ChainTipSender` latches onto the non-finalized tip, but
            // this task holds a separate `finalized_sender()` that does not.
            // Calling `set_finalized_tip` would keep overwriting the
            // non-finalized tip rather than becoming a no-op, so this task stops
            // itself. The non-finalized state only grows once populated, making
            // this handover permanent.
            if *started_sync_receiver.borrow() {
                return;
            }

            finalized_tip_publisher.publish(tip_block);
        }
    }
}

/// Reads the secondary database's finalized tip and converts it to a
/// [`ChainTipBlock`].
async fn finalized_chain_tip_block(db: &ZakuraDb) -> Option<ChainTipBlock> {
    let db = db.clone();
    tokio::task::spawn_blocking(move || {
        let (height, hash) = db.tip()?;
        db.block(height.into())
            .map(|block| CheckpointVerifiedBlock::with_hash(block, hash))
            .map(ChainTipBlock::from)
    })
    .wait_for_panics()
    .await
}

impl TrustedChainSync {
    /// Creates a new [`TrustedChainSync`] with a [`ChainTipSender`], then spawns
    /// a task to sync blocks from the node's non-finalized best chain.
    ///
    /// Returns the [`LatestChainTip`], [`ChainTipChange`], and a [`JoinHandle`] for the sync task.
    pub async fn spawn(
        indexer_rpc_address: SocketAddr,
        db: ZakuraDb,
        non_finalized_state_sender: tokio::sync::watch::Sender<NonFinalizedState>,
    ) -> Result<(LatestChainTip, ChainTipChange, JoinHandle<()>), BoxError> {
        let non_finalized_state = NonFinalizedState::new(&db.network());
        let (chain_tip_sender, latest_chain_tip, chain_tip_change) =
            ChainTipSender::new(None, &db.network());
        let channel =
            tonic::transport::Endpoint::from_shared(format!("http://{indexer_rpc_address}"))?
                .keep_alive_while_idle(true)
                .http2_keep_alive_interval(KEEPALIVE_INTERVAL)
                .keep_alive_timeout(KEEPALIVE_TIMEOUT)
                .connect()
                .await?;
        let indexer_rpc_client = IndexerClient::new(channel);
        let finalized_tip_publisher =
            FinalizedTipPublisher::new(chain_tip_sender.finalized_sender());

        // `sync()` flips this after receiving its first parseable non-finalized
        // block. This stops `update_finalized_chain_tip`, making `sync()` the
        // sole caller of `try_catch_up_with_primary` on the secondary database.
        let (started_sync_sender, started_sync_receiver) = tokio::sync::watch::channel(false);

        // Retain this handle in the syncer so takeover waits for any in-flight
        // finalized-state catch-up to finish.
        let finalized_tip_updater_db = db.clone();
        let finalized_tip_updater_client = indexer_rpc_client.clone();
        let finalized_tip_updater_publisher = finalized_tip_publisher.clone();
        let finalized_tip_updater = tokio::spawn(async move {
            update_finalized_chain_tip(
                finalized_tip_updater_db,
                finalized_tip_updater_client,
                finalized_tip_updater_publisher,
                started_sync_receiver,
            )
            .await
        });

        let mut syncer = Self {
            indexer_rpc_client,
            db,
            non_finalized_state,
            chain_tip_sender,
            finalized_tip_publisher,
            non_finalized_state_sender,
            started_sync_sender,
            finalized_tip_updater: Some(finalized_tip_updater),
        };

        let sync_task = tokio::spawn(async move {
            syncer.sync().await;
        });

        Ok((latest_chain_tip, chain_tip_change, sync_task))
    }

    /// Stops finalized-tip updates and waits for any in-flight database catch-up.
    async fn take_over_finalized_tip_updates(&mut self) {
        let Some(finalized_tip_updater) = self.finalized_tip_updater.take() else {
            return;
        };

        stop_finalized_tip_updater(&self.started_sync_sender, finalized_tip_updater).await;
    }

    /// Syncs the primary node's non-finalized best chain and finalized tip.
    ///
    /// When the primary best-chain tip is unavailable locally, gets the missing
    /// blocks from the RPC server, adds them to the local non-finalized state,
    /// then sends the updated tip and state to the chain-tip channels.
    #[tracing::instrument(skip_all)]
    async fn sync(&mut self) {
        let mut non_finalized_blocks_listener = None;
        // The hash of the block that most recently failed to commit, used to
        // avoid re-logging the same warning at full rate.
        let mut last_failed_commit_hash = None;
        self.try_catch_up_with_primary().await;
        if let Some(finalized_tip_block) = finalized_chain_tip_block(&self.db).await {
            self.finalized_tip_publisher.publish(finalized_tip_block);
        }

        loop {
            let Some(ref mut non_finalized_state_change) = non_finalized_blocks_listener else {
                non_finalized_blocks_listener = match self
                    .subscribe_to_non_finalized_state_change()
                    .await
                {
                    Ok(listener) => Some(listener),
                    Err(err) => {
                        tracing::warn!(?err, "failed to subscribe to non-finalized state changes");
                        tokio::time::sleep(POLL_DELAY).await;
                        None
                    }
                };

                continue;
            };

            let message = match tokio::time::timeout(
                STREAM_MESSAGE_TIMEOUT,
                non_finalized_state_change.message(),
            )
            .await
            {
                Ok(Ok(Some(block_and_hash))) => block_and_hash,
                Ok(Ok(None)) => {
                    tracing::warn!("non-finalized state change stream ended unexpectedly");
                    non_finalized_blocks_listener = None;
                    continue;
                }
                Ok(Err(err)) => {
                    tracing::warn!(?err, "error receiving non-finalized state change");
                    non_finalized_blocks_listener = None;
                    continue;
                }
                Err(_) => {
                    tracing::debug!("non-finalized state change stream timed out, re-subscribing");
                    non_finalized_blocks_listener = None;
                    continue;
                }
            };

            let Some((block, hash)) = message.decode() else {
                tracing::warn!("received malformed non-finalized state change message");
                non_finalized_blocks_listener = None;
                continue;
            };

            // We have a parseable block from the stream, so take over finalized
            // tip updates. Waiting for the updater is a barrier against an
            // in-flight secondary database catch-up.
            self.take_over_finalized_tip_updates().await;

            if self.non_finalized_state.any_chain_contains(&hash) {
                // Expected and harmless: on a resumed or multi-chain stream the
                // server can re-send a block the syncer already has, such as a
                // fork's shared ancestor. Log at debug to avoid noise.
                tracing::debug!(
                    ?hash,
                    "non-finalized state already contains block, skipping"
                );
                continue;
            }

            let block = SemanticallyVerifiedBlock::with_hash(Arc::new(block), hash);
            match self.try_commit(block).await {
                Ok(CommitOutcome::Committed) => {
                    last_failed_commit_hash = None;
                }
                Ok(CommitOutcome::AlreadyFinalized) => {
                    last_failed_commit_hash = None;
                }
                Err(error) => {
                    // Only log on transitions to avoid saturating the logs when
                    // the same block persistently fails to commit.
                    if last_failed_commit_hash != Some(hash) {
                        tracing::warn!(
                            ?error,
                            ?hash,
                            "failed to commit block to non-finalized state"
                        );
                        last_failed_commit_hash = Some(hash);
                    }

                    non_finalized_blocks_listener = None;

                    // Back off so a persistently failing block doesn't turn
                    // re-subscription into a full-speed busy loop.
                    tokio::time::sleep(COMMIT_RETRY_DELAY).await;
                }
            };
        }
    }

    async fn try_commit(
        &mut self,
        block: SemanticallyVerifiedBlock,
    ) -> Result<CommitOutcome, ValidateContextError> {
        self.try_catch_up_with_primary().await;

        if block_height_is_finalized(self.db.finalized_tip_height(), block.height) {
            tracing::debug!(
                height = ?block.height,
                "skipping block finalized while the secondary caught up"
            );
            self.publish_current_state().await;
            return Ok(CommitOutcome::AlreadyFinalized);
        }

        // If the incoming block does not build on an empty secondary's finalized
        // tip, its finalized state lags the primary. Streamed non-finalized
        // blocks start at the primary's finalized tip, which can be several
        // blocks above ours. Fetch the missing finalized blocks so the incoming
        // block has a contiguous chain to commit onto.
        if self.non_finalized_state.best_chain().is_none()
            && self.db.finalized_tip_hash() != block.block.header.previous_block_hash
        {
            self.fill_finalized_gap(block.height).await;
        }

        // Gap filling catches up the secondary before each fetch. Re-check the
        // target because the primary might have finalized it while the gap was
        // being bridged.
        if block_height_is_finalized(self.db.finalized_tip_height(), block.height) {
            tracing::debug!(
                height = ?block.height,
                "skipping block finalized while bridging the finalized gap"
            );
            self.publish_current_state().await;
            return Ok(CommitOutcome::AlreadyFinalized);
        }

        self.commit(block)?;

        Ok(CommitOutcome::Committed)
    }

    /// Commits `block` to the non-finalized state, starting a new chain if it
    /// builds on the finalized tip or extending an existing chain otherwise.
    /// Then prunes finalized blocks and publishes the updated state.
    ///
    /// Updating the channels here means bridge blocks committed by
    /// [`Self::fill_finalized_gap`] also advance the published chain tip.
    fn commit(&mut self, block: SemanticallyVerifiedBlock) -> Result<(), ValidateContextError> {
        if self.db.finalized_tip_hash() == block.block.header.previous_block_hash {
            let _ = self.prune_finalized();
            self.non_finalized_state.commit_new_chain(block, &self.db)?;
        } else {
            self.non_finalized_state.commit_block(block, &self.db)?;
            let _ = self.prune_finalized();
        }

        self.update_channels();

        Ok(())
    }

    /// Fetches blocks between the secondary's finalized tip and `target_height`
    /// (exclusive), then commits them to the non-finalized state.
    ///
    /// These blocks are finalized on the primary but not the lagging secondary.
    /// Committing them as bridge blocks gives the block at `target_height` a
    /// contiguous chain. [`Self::prune_finalized`] drops them as the secondary
    /// catches up.
    ///
    /// This is best-effort. If a block can't be fetched or committed, it returns
    /// early so the caller retries on the next subscription.
    async fn fill_finalized_gap(&mut self, target_height: Height) {
        loop {
            // Try to advance the secondary's finalized state first. This can
            // close the gap without fetching blocks and drops bridge blocks the
            // secondary has since finalized. Recompute from the resulting tip.
            self.try_catch_up_with_primary().await;

            // The next height is above the highest block we have: the
            // non-finalized tip after bridge commits, or the finalized tip.
            let Some((highest_height, highest_hash)) = self.highest_local_tip() else {
                return;
            };

            let Ok(next_height) = highest_height.next() else {
                return;
            };

            // Stop once catch-up or fetched bridge blocks reach the streamed
            // block's parent.
            if next_height >= target_height {
                return;
            }

            let (block, hash) = match self.get_block(next_height.into()).await {
                Ok(block_and_hash) => block_and_hash,
                Err(error) => {
                    tracing::warn!(
                        ?error,
                        ?next_height,
                        "failed to fetch a block while bridging the finalized gap; \
                         will retry on the next subscription"
                    );
                    return;
                }
            };

            if let Err(error) = validate_finalized_gap_block(
                next_height,
                highest_hash,
                block.coinbase_height(),
                block.header.previous_block_hash,
                hash,
                block.hash(),
            ) {
                tracing::warn!(
                    ?error,
                    ?next_height,
                    ?hash,
                    "primary returned an invalid finalized-gap block; will retry on the next \
                     subscription"
                );
                return;
            }

            let block = SemanticallyVerifiedBlock::with_hash(Arc::new(block), hash);
            if let Err(error) = self.commit(block) {
                tracing::warn!(
                    ?error,
                    ?next_height,
                    "failed to commit a block while bridging the finalized gap; \
                     will retry on the next subscription"
                );
                return;
            }

            let actual_tip = self.highest_local_tip();
            if !gap_fill_reached_expected_tip(actual_tip, (next_height, hash)) {
                tracing::warn!(
                    ?actual_tip,
                    ?next_height,
                    ?hash,
                    "finalized-gap commit did not advance to the requested block; will retry on \
                     the next subscription"
                );
                return;
            }
        }
    }

    /// Returns the highest local non-finalized or finalized chain tip.
    fn highest_local_tip(&self) -> Option<(Height, block::Hash)> {
        match (self.non_finalized_state.best_tip(), self.db.tip()) {
            (Some(non_finalized_tip), Some(finalized_tip))
                if non_finalized_tip.0 >= finalized_tip.0 =>
            {
                Some(non_finalized_tip)
            }
            (Some(_non_finalized_tip), Some(finalized_tip)) => Some(finalized_tip),
            (Some(non_finalized_tip), None) => Some(non_finalized_tip),
            (None, Some(finalized_tip)) => Some(finalized_tip),
            (None, None) => None,
        }
    }

    /// Fetches one block from the primary by hash or height.
    async fn get_block(
        &self,
        hash_or_height: HashOrHeight,
    ) -> Result<(Block, block::Hash), Status> {
        // Encode a hash in display order or a height in big-endian order. The
        // server distinguishes them by their protocol-defined lengths.
        let hash_or_height = match hash_or_height {
            HashOrHeight::Hash(hash) => hash.bytes_in_display_order().to_vec(),
            HashOrHeight::Height(height) => height.0.to_be_bytes().to_vec(),
        };
        let request = BlockRequest { hash_or_height };

        let response = tokio::time::timeout(
            GET_BLOCK_TIMEOUT,
            self.indexer_rpc_client.clone().get_block(request),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("get_block request timed out"))??;

        response
            .into_inner()
            .decode()
            .ok_or_else(|| Status::internal("failed to decode block from get_block response"))
    }

    /// Subscribes to non-finalized state changes and returns the response stream.
    ///
    /// Passes every local chain tip so the server only streams missing blocks,
    /// rather than the whole state on each subscription. With no local chains,
    /// the server streams every non-finalized block.
    async fn subscribe_to_non_finalized_state_change(
        &mut self,
    ) -> Result<Streaming<BlockAndHash>, Status> {
        let request = NonFinalizedStateChangeRequest {
            chain_tip_hashes: self
                .non_finalized_state
                .chain_iter()
                .map(|c| c.non_finalized_tip_hash().bytes_in_display_order().to_vec())
                .collect(),
        };

        tokio::time::timeout(
            SUBSCRIBE_TIMEOUT,
            self.indexer_rpc_client
                .clone()
                .non_finalized_state_change(request),
        )
        .await
        .map_err(|_| {
            Status::deadline_exceeded("non_finalized_state_change subscription timed out")
        })?
        .map(|a| a.into_inner())
    }

    /// Catches up to the primary database, then prunes and publishes any blocks
    /// that became finalized.
    async fn try_catch_up_with_primary(&mut self) {
        let _ = self.db.spawn_try_catch_up_with_primary().await;

        if self.prune_finalized() {
            self.publish_current_state().await;
        }
    }

    /// Finalizes non-finalized blocks at or below the finalized tip.
    ///
    /// Catch-up can advance the secondary past the non-finalized root. This
    /// drops those newly finalized blocks. An empty state is unchanged.
    fn prune_finalized(&mut self) -> bool {
        let finalized_tip_height = self.db.finalized_tip_height().unwrap_or(Height::MIN);
        let mut pruned = false;

        while self
            .non_finalized_state
            .root_height()
            .is_some_and(|root_height| root_height <= finalized_tip_height)
        {
            tracing::trace!("finalizing block past the reorg limit");
            self.non_finalized_state.finalize();
            pruned = true;
        }

        pruned
    }

    /// Publishes the current non-finalized state and its effective chain tip.
    ///
    /// If catch-up pruned every non-finalized block, a dedicated finalized
    /// sender bypasses the primary sender's non-finalized-tip latch and publishes
    /// the secondary database tip.
    async fn publish_current_state(&mut self) {
        if self.non_finalized_state.best_chain().is_some() {
            self.update_channels();
            return;
        }

        // If the final receiver was just dropped, ignore the error.
        let _ = self
            .non_finalized_state_sender
            .send(self.non_finalized_state.clone());

        if let Some(finalized_tip_block) = finalized_chain_tip_block(&self.db).await {
            self.finalized_tip_publisher.publish(finalized_tip_block);
        }
    }

    /// Sends the new chain tip and non-finalized state to the latest chain channels.
    // TODO: Replace this with the `update_latest_chain_channels()` fn in `write.rs`.
    fn update_channels(&mut self) {
        // If the final receiver was just dropped, ignore the error.
        let _ = self
            .non_finalized_state_sender
            .send(self.non_finalized_state.clone());

        let best_chain = self.non_finalized_state.best_chain().expect("unexpected empty non-finalized state: must commit at least one block before updating channels");

        let tip_block = best_chain
            .tip_block()
            .expect(
                "unexpected empty chain: must commit at least one block before updating channels",
            )
            .clone();

        self.chain_tip_sender
            .set_best_non_finalized_tip(Some(tip_block.into()));
    }
}

/// Accepts a [zakura-state configuration](zakura_state::Config), a [`Network`], and
/// the [`SocketAddr`] of a primary node's RPC server.
///
/// Initializes a [`ReadStateService`] and a [`TrustedChainSync`] to update the
/// non-finalized best chain and the latest chain tip.
///
/// Returns a [`ReadStateService`], [`LatestChainTip`], [`ChainTipChange`], and
/// a [`JoinHandle`] for the sync task.
pub fn init_read_state_with_syncer(
    config: zakura_state::Config,
    network: &Network,
    indexer_rpc_address: SocketAddr,
) -> tokio::task::JoinHandle<
    Result<
        (
            ReadStateService,
            LatestChainTip,
            ChainTipChange,
            tokio::task::JoinHandle<()>,
        ),
        BoxError,
    >,
> {
    let network = network.clone();
    tokio::spawn(async move {
        if config.ephemeral {
            return Err("standalone read state service cannot be used with ephemeral state".into());
        }

        // The outer `?` propagates a `JoinError` if the blocking task panicked or was
        // cancelled, and the inner `?` propagates a `StateInitError` (e.g. a missing
        // read-only database).
        let (read_state, db, non_finalized_state_sender) =
            spawn_init_read_only(config, &network).await??;
        let (latest_chain_tip, chain_tip_change, sync_task) =
            TrustedChainSync::spawn(indexer_rpc_address, db, non_finalized_state_sender).await?;
        Ok((read_state, latest_chain_tip, chain_tip_change, sync_task))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::chain_tip::ChainTip;

    fn test_chain_tip(height: Height, hash_byte: u8) -> ChainTipBlock {
        ChainTipBlock {
            hash: block::Hash([hash_byte; 32]),
            height,
            time: chrono::Utc::now(),
            transactions: Vec::new(),
            transaction_hashes: Arc::from([]),
            previous_block_hash: block::Hash([hash_byte.saturating_sub(1); 32]),
        }
    }

    #[test]
    fn finalized_height_check_includes_the_tip() {
        let finalized_tip = Height(10);

        assert!(!block_height_is_finalized(None, Height(9)));
        assert!(block_height_is_finalized(Some(finalized_tip), Height(9)));
        assert!(block_height_is_finalized(Some(finalized_tip), Height(10)));
        assert!(!block_height_is_finalized(Some(finalized_tip), Height(11)));
    }

    #[test]
    fn finalized_tip_publisher_ignores_stale_lower_tip() {
        let (sender, latest_chain_tip, _chain_tip_change) =
            ChainTipSender::new(None, &Network::Mainnet);
        let publisher = FinalizedTipPublisher::new(sender.finalized_sender());
        let newer_tip = test_chain_tip(Height(11), 11);

        publisher.publish(newer_tip.clone());
        publisher.publish(test_chain_tip(Height(10), 10));

        assert_eq!(
            latest_chain_tip.best_tip_height_and_hash(),
            Some((newer_tip.height, newer_tip.hash))
        );
    }

    #[test]
    fn finalized_gap_block_must_match_request_and_chain() {
        let expected_height = Height(11);
        let expected_parent = block::Hash([10; 32]);
        let expected_hash = block::Hash([11; 32]);

        assert_eq!(
            validate_finalized_gap_block(
                expected_height,
                expected_parent,
                Some(expected_height),
                expected_parent,
                expected_hash,
                expected_hash,
            ),
            Ok(())
        );
        assert_eq!(
            validate_finalized_gap_block(
                expected_height,
                expected_parent,
                None,
                expected_parent,
                expected_hash,
                expected_hash,
            ),
            Err(FinalizedGapBlockError::MissingHeight)
        );
        assert_eq!(
            validate_finalized_gap_block(
                expected_height,
                expected_parent,
                Some(Height(12)),
                expected_parent,
                expected_hash,
                expected_hash,
            ),
            Err(FinalizedGapBlockError::UnexpectedHeight {
                expected: expected_height,
                actual: Height(12),
            })
        );
        assert_eq!(
            validate_finalized_gap_block(
                expected_height,
                expected_parent,
                Some(expected_height),
                block::Hash([9; 32]),
                expected_hash,
                expected_hash,
            ),
            Err(FinalizedGapBlockError::UnexpectedParentHash {
                expected: expected_parent,
                actual: block::Hash([9; 32]),
            })
        );
        assert_eq!(
            validate_finalized_gap_block(
                expected_height,
                expected_parent,
                Some(expected_height),
                expected_parent,
                expected_hash,
                block::Hash([12; 32]),
            ),
            Err(FinalizedGapBlockError::UnexpectedBlockHash {
                advertised: expected_hash,
                computed: block::Hash([12; 32]),
            })
        );
    }

    #[test]
    fn finalized_gap_fill_requires_expected_tip() {
        let expected_tip = (Height(11), block::Hash([11; 32]));

        assert!(gap_fill_reached_expected_tip(
            Some(expected_tip),
            expected_tip
        ));
        assert!(!gap_fill_reached_expected_tip(None, expected_tip));
        assert!(!gap_fill_reached_expected_tip(
            Some((Height(10), expected_tip.1)),
            expected_tip
        ));
        assert!(!gap_fill_reached_expected_tip(
            Some((expected_tip.0, block::Hash([12; 32]))),
            expected_tip
        ));
    }

    #[tokio::test]
    async fn finalized_tip_handoff_waits_for_updater() {
        let (started_sender, mut started_receiver) = tokio::sync::watch::channel(false);
        let (observed_sender, observed_receiver) = tokio::sync::oneshot::channel();
        let (release_sender, release_receiver) = tokio::sync::oneshot::channel();

        let updater = tokio::spawn(async move {
            started_receiver
                .changed()
                .await
                .expect("started sender remains open during handoff");
            assert!(*started_receiver.borrow());
            observed_sender
                .send(())
                .expect("handoff test keeps the observation receiver open");
            release_receiver
                .await
                .expect("handoff test releases the updater");
        });

        let handoff_sender = started_sender.clone();
        let handoff = tokio::spawn(async move {
            stop_finalized_tip_updater(&handoff_sender, updater).await;
        });

        tokio::time::timeout(Duration::from_secs(1), observed_receiver)
            .await
            .expect("updater should observe the handoff signal")
            .expect("updater should send the handoff observation");
        assert!(
            !handoff.is_finished(),
            "handoff must wait for the updater to finish"
        );

        release_sender
            .send(())
            .expect("updater remains alive until it is released");
        tokio::time::timeout(Duration::from_secs(1), handoff)
            .await
            .expect("handoff should finish after the updater exits")
            .expect("handoff task should not panic");
        assert!(*started_sender.borrow());
    }
}
