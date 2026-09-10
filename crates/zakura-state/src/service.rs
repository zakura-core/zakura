//! [`tower::Service`]s for Zebra's cached chain state.
//!
//! Zebra provides cached state access via two main services:
//! - [`StateService`]: a read-write service that writes blocks to the state,
//!   and redirects most read requests to the [`ReadStateService`].
//! - [`ReadStateService`]: a read-only service that answers from the most
//!   recent committed block.
//!
//! Most users should prefer [`ReadStateService`], unless they need to write blocks to the state.
//!
//! Zebra also provides access to the best chain tip via:
//! - [`LatestChainTip`]: a read-only channel that contains the latest committed
//!   tip.
//! - [`ChainTipChange`]: a read-only channel that can asynchronously await
//!   chain tip changes.

use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    ops::Bound,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures::future::FutureExt;
use indexmap::IndexMap;
use tokio::sync::oneshot;
use tower::{util::BoxService, Service, ServiceExt};
use tracing::{instrument, Instrument, Span};

#[cfg(any(test, feature = "proptest-impl"))]
use tower::buffer::Buffer;

use zakura_chain::{
    block::{self, CountedHeader, HeightDiff},
    diagnostic::CodeTimer,
    parallel::commitment_aux::BlockCommitmentRoots,
    parameters::{Network, NetworkUpgrade},
    serialization::ZcashSerialize,
    subtree::{NoteCommitmentSubtreeData, NoteCommitmentSubtreeIndex},
};

use crate::{
    constants::{
        MAX_BLOCK_REORG_HEIGHT, MAX_FIND_BLOCK_HASHES_RESULTS, MAX_FIND_BLOCK_HEADERS_RESULTS,
        MAX_HEADER_SYNC_HEIGHT_RANGE, MAX_HISTORICAL_TREE_REPLAY_BLOCKS, MAX_LEGACY_CHAIN_BLOCKS,
    },
    error::{CommitBlockError, CommitCheckpointVerifiedError, InvalidateError, ReconsiderError},
    request::TimedSpan,
    response::NonFinalizedBlocksListener,
    service::{
        block_iter::any_ancestor_blocks,
        chain_tip::{ChainTipBlock, ChainTipChange, ChainTipSender, LatestChainTip},
        finalized_state::{
            header_chain::{HeaderChainStore, HeaderChainStoreError},
            FinalizedState, ZakuraDb,
        },
        non_finalized_state::{Chain, NonFinalizedState},
        pending_utxos::PendingUtxos,
        queued_blocks::QueuedBlocks,
        read::find,
        watch_receiver::WatchReceiver,
    },
    BoxError, CheckpointVerifiedBlock, CommitSemanticallyVerifiedError, Config, HashOrHeight,
    HistoricalTreeUnavailable, KnownBlock, ReadRequest, ReadResponse, Request, Response,
    SemanticallyVerifiedBlock, StateInitError, ValidateContextError,
};

pub mod block_iter;
pub mod chain_tip;
pub mod watch_receiver;

pub mod check;

pub(crate) mod finalized_state;
pub(crate) mod non_finalized_state;
mod pending_utxos;
mod queued_blocks;
pub(crate) mod read;
mod traits;
mod write;

#[cfg(any(test, feature = "proptest-impl"))]
pub mod arbitrary;

#[cfg(test)]
mod tests;

pub use finalized_state::{OutputLocation, TransactionIndex, TransactionLocation};
use write::NonFinalizedWriteMessage;
pub use write::{VctRootRepairState, VctRootRepairStatus};

use self::queued_blocks::{QueuedCheckpointVerified, QueuedSemanticallyVerified, SentHashes};

pub use self::traits::{ReadState, State};

fn finalized_chain_tip(db: &ZakuraDb) -> Option<ChainTipBlock> {
    let (height, hash) = db.tip()?;
    if let Some(block) = db.tip_block() {
        return Some(ChainTipBlock::from(CheckpointVerifiedBlock::from(block)));
    }

    let header = db.block_header(height.into())?;
    (header.hash() == hash)
        .then(|| ChainTipBlock::from_pruned_finalized_header(hash, height, header))
}

/// A read-write service for Zebra's cached blockchain state.
///
/// This service modifies and provides access to:
/// - the non-finalized state: the most recent blocks, up to
///   [`MAX_BLOCK_REORG_HEIGHT`](crate::MAX_BLOCK_REORG_HEIGHT) of them.
///   Zebra allows chain forks in the non-finalized state,
///   stores it in memory, and re-downloads it when restarted.
/// - the finalized state: older blocks that have many confirmations.
///   Zebra stores the single best chain in the finalized state,
///   and re-loads it from disk when restarted.
///
/// Read requests to this service are buffered, then processed concurrently.
/// Block write requests are buffered, then queued, then processed in order by a separate task.
///
/// Most state users can get faster read responses using the [`ReadStateService`],
/// because its requests do not share a [`tower::buffer::Buffer`] with block write requests.
///
/// To quickly get the latest block, use [`LatestChainTip`] or [`ChainTipChange`].
/// They can read the latest block directly, without queueing any requests.
#[derive(Debug)]
pub(crate) struct StateService {
    // Configuration
    //
    /// The configured Zcash network.
    network: Network,

    /// The height that we start storing UTXOs from finalized blocks.
    ///
    /// This height should be lower than the last few checkpoints,
    /// so the full verifier can verify UTXO spends from those blocks,
    /// even if they haven't been committed to the finalized state yet.
    full_verifier_utxo_lookahead: block::Height,

    /// The maximum checkpoint height for this network.
    ///
    /// This is the height of the last block the checkpoint verifier commits to the finalized state.
    /// Once the finalized tip reaches this height, the block write task can hand off from committing
    /// checkpoint verified blocks to committing semantically verified blocks, without waiting for a
    /// semantically verified block to arrive. See [`StateService::try_handoff_to_non_finalized_write`].
    max_checkpoint_height: block::Height,

    // Queued Blocks
    //
    /// Queued blocks for the [`NonFinalizedState`] that arrived out of order.
    /// These blocks are awaiting their parent blocks before they can do contextual verification.
    non_finalized_state_queued_blocks: QueuedBlocks,

    /// Queued blocks for the [`FinalizedState`] that arrived out of order.
    /// These blocks are awaiting their parent blocks before they can do contextual verification.
    ///
    /// Indexed by their parent block hash.
    finalized_state_queued_blocks: HashMap<block::Hash, QueuedCheckpointVerified>,

    /// Channels to send blocks to the block write task.
    block_write_sender: write::BlockWriteSender,

    /// The [`block::Hash`] of the most recent block sent on
    /// `finalized_block_write_sender` or `non_finalized_block_write_sender`.
    ///
    /// On startup, this is:
    /// - the finalized tip, if there are stored blocks, or
    /// - the genesis block's parent hash, if the database is empty.
    ///
    /// If `invalid_block_write_reset_receiver` gets a reset, this is:
    /// - the hash of the last valid committed block (the parent of the invalid block).
    finalized_block_write_last_sent_hash: block::Hash,

    /// A set of block hashes that have been sent to the block write task.
    /// Hashes of blocks below the finalized tip height are periodically pruned.
    non_finalized_block_write_sent_hashes: SentHashes,

    /// Recent local write failures used to complete descendants that arrive after the failure.
    non_finalized_failed_ancestors:
        IndexMap<block::Hash, (block::Hash, write::NonFinalizedWriteFailureKind)>,

    /// If an invalid block is sent on `finalized_block_write_sender`
    /// or `non_finalized_block_write_sender`,
    /// this channel gets the [`block::Hash`] of the valid tip.
    //
    // TODO: add tests for finalized and non-finalized resets (#2654)
    invalid_block_write_reset_receiver: tokio::sync::mpsc::UnboundedReceiver<block::Hash>,

    /// Receives the hash of every non-finalized block that the write task
    /// rejected, so the corresponding entry can be removed from
    /// `non_finalized_block_write_sent_hashes`.
    ///
    /// Without this, a rejected same-hash block locks out a later honest
    /// re-delivery of a block at the same hash as a "duplicate" until restart
    /// or reorg.
    non_finalized_rejected_receiver:
        tokio::sync::mpsc::UnboundedReceiver<write::NonFinalizedWriteFailure>,

    // Pending UTXO Request Tracking
    //
    /// The set of outpoints with pending requests for their associated transparent::Output.
    pending_utxos: PendingUtxos,

    /// Instant tracking the last time `pending_utxos` was pruned.
    last_prune: Instant,

    // Updating Concurrently Readable State
    //
    /// A cloneable [`ReadStateService`], used to answer concurrent read requests.
    ///
    /// TODO: move users of read [`Request`]s to [`ReadStateService`], and remove `read_service`.
    read_service: ReadStateService,

    // Metrics
    //
    /// A metric tracking the maximum height that's currently in `finalized_state_queued_blocks`
    ///
    /// Set to `f64::NAN` if `finalized_state_queued_blocks` is empty, because grafana shows NaNs
    /// as a break in the graph.
    max_finalized_queue_height: f64,
}

/// A read-only service for accessing Zebra's cached blockchain state.
///
/// This service provides read-only access to:
/// - the non-finalized state: the most recent blocks, up to
///   [`MAX_BLOCK_REORG_HEIGHT`](crate::MAX_BLOCK_REORG_HEIGHT) of them.
/// - the finalized state: older blocks that have many confirmations.
///
/// Requests to this service are processed in parallel,
/// ignoring any blocks queued by the read-write [`StateService`].
///
/// This quick response behavior is better for most state users.
/// It allows other async tasks to make progress while concurrently reading data from disk.
#[derive(Clone, Debug)]
pub struct ReadStateService {
    // Configuration
    //
    /// The configured Zcash network.
    network: Network,

    // Shared Concurrently Readable State
    //
    /// A watch channel with a cached copy of the [`NonFinalizedState`].
    ///
    /// This state is only updated between requests,
    /// so it might include some block data that is also on `disk`.
    non_finalized_state_receiver: WatchReceiver<NonFinalizedState>,

    /// The shared inner on-disk database for the finalized state.
    ///
    /// RocksDB allows reads and writes via a shared reference,
    /// but [`ZakuraDb`] doesn't expose any write methods or types.
    ///
    /// This chain is updated concurrently with requests,
    /// so it might include some block data that is also in `best_mem`.
    db: ZakuraDb,

    /// A shared handle to a task that writes blocks to the [`NonFinalizedState`] or [`FinalizedState`],
    /// once the queues have received all their parent blocks.
    ///
    /// Used to check for panics when writing blocks.
    block_write_task: Option<Arc<std::thread::JoinHandle<write::BlockWriteTaskExit>>>,
    /// Shared fail-closed attachment result, visible to every clone without joining the worker.
    block_write_failure: Arc<OnceLock<write::BlockWriteTaskFailure>>,

    /// Note commitment frontiers this service has derived and root-checked for heights in a
    /// verified-commitment-trees fast-synced database's absent band.
    ///
    /// Shared across clones so a wallet's sequential scan anchors each request on the previous
    /// one. Empty, and never written, on a node that does not derive
    /// ([`Config::derive_historical_trees`]) or has no frontier grid configured.
    historical_trees: Arc<Mutex<read::HistoricalTreeCache>>,

    /// Published completed subtree roots for heights below the last checkpoint.
    ///
    /// `None` on networks without an embedded artifact, in which case `z_getsubtreesbyindex`
    /// keeps reporting the absent band rather than serving unchecked data.
    historical_subtrees: Option<Arc<finalized_state::SubtreeArtifact>>,

    /// Watch channel publishing the next VCT supplied-root repair needed by the finalized writer.
    vct_root_repair_receiver: tokio::sync::watch::Receiver<VctRootRepairStatus>,

    /// Committed header-engine snapshots, absent until the semantic handoff audit succeeds.
    header_chain_snapshot_receiver:
        tokio::sync::watch::Receiver<Option<zakura_header_chain::EngineSnapshot>>,

    /// Atomic committed header views used by body-work coordinators.
    header_chain_view_receiver:
        tokio::sync::watch::Receiver<Option<zakura_header_chain::CommittedHeaderChainView>>,

    /// Explicit durable header-runtime attachment and readiness lifecycle.
    header_runtime_status_receiver:
        tokio::sync::watch::Receiver<zakura_node_services::sync_lifecycle::HeaderRuntimeStatus>,

    /// Coherent durable header-engine reader, absent until semantic handoff.
    header_chain_reader_receiver:
        tokio::sync::watch::Receiver<Option<finalized_state::header_chain::HeaderChainReader>>,
}

#[derive(Clone, Debug)]
struct HeaderChainSubscriptions {
    snapshots: tokio::sync::watch::Receiver<Option<zakura_header_chain::EngineSnapshot>>,
    views: tokio::sync::watch::Receiver<Option<zakura_header_chain::CommittedHeaderChainView>>,
    runtime_status:
        tokio::sync::watch::Receiver<zakura_node_services::sync_lifecycle::HeaderRuntimeStatus>,
    reader: tokio::sync::watch::Receiver<Option<finalized_state::header_chain::HeaderChainReader>>,
}

impl Drop for StateService {
    fn drop(&mut self) {
        // The state service owns the state, tasks, and channels,
        // so dropping it should shut down everything.

        // Close the channels (non-blocking)
        // This makes the block write thread exit the next time it checks the channels.
        // We want to do this here so we get any errors or panics from the block write task before it shuts down.
        self.invalid_block_write_reset_receiver.close();
        self.non_finalized_rejected_receiver.close();

        std::mem::drop(self.block_write_sender.finalized.take());
        std::mem::drop(self.block_write_sender.non_finalized.take());

        self.clear_finalized_block_queue(CommitBlockError::WriteTaskExited);
        self.clear_non_finalized_block_queue(CommitBlockError::WriteTaskExited);

        // Log database metrics before shutting down
        info!("dropping the state: logging database metrics");
        self.log_db_metrics();

        // Then drop self.read_service, which checks the block write task for panics,
        // and tries to shut down the database.
    }
}

impl Drop for ReadStateService {
    fn drop(&mut self) {
        // The read state service shares the state,
        // so dropping it should check if we can shut down.

        // TODO: move this into a try_shutdown() method
        if let Some(block_write_task) = self.block_write_task.take() {
            if let Some(block_write_task_handle) = Arc::into_inner(block_write_task) {
                // We're the last database user, so we can tell it to shut down (blocking):
                // - flushes the database to disk, and
                // - drops the database, which cleans up any database tasks correctly.
                self.db.shutdown(true);

                // This state owns the last reference to the thread.
                // The state can wait for the block write task and then check for panics.
                // (We'd also like to abort the thread, but std::thread::JoinHandle can't do that.)

                // This log is verbose during tests.
                #[cfg(not(test))]
                info!("waiting for the block write task to finish");
                #[cfg(test)]
                debug!("waiting for the block write task to finish");

                // TODO: move this into a check_for_panics() method
                match block_write_task_handle.join() {
                    Err(thread_panic) => std::panic::resume_unwind(thread_panic),
                    Ok(write::BlockWriteTaskExit::HeaderChainAttachmentFailed(error)) => {
                        tracing::error!(?error, "block write task stopped during header attachment")
                    }
                    Ok(write::BlockWriteTaskExit::HeaderChainRuntimeFailed(error)) => {
                        tracing::error!(?error, "block write task stopped after a runtime failure")
                    }
                    Ok(write::BlockWriteTaskExit::Completed) => {
                        debug!("shutting down the state because the block write task has finished")
                    }
                }
            }
        } else {
            // Even if we're not the last database user, try shutting it down.
            //
            // TODO: rename this to try_shutdown()?
            self.db.shutdown(false);
        }
    }
}

impl StateService {
    const PRUNE_INTERVAL: Duration = Duration::from_secs(30);
    // The 1,000-block reorg bound fits every supported usize target.
    const FAILED_ANCESTOR_LIMIT: usize = MAX_BLOCK_REORG_HEIGHT as usize * 2;

    /// Creates a new state service for the state `config` and `network`.
    ///
    /// Uses the `max_checkpoint_height` and `checkpoint_verify_concurrency_limit`
    /// to work out when it is near the final checkpoint.
    ///
    /// Returns the read-write and read-only state services,
    /// and read-only watch channels for its best chain tip.
    ///
    /// # Errors
    ///
    /// Returns a [`StateInitError`] if historical tree derivation is misconfigured or its
    /// frontier artifact cannot be loaded.
    pub async fn new(
        config: Config,
        network: &Network,
        max_checkpoint_height: block::Height,
        checkpoint_verify_concurrency_limit: usize,
    ) -> Result<(Self, ReadStateService, LatestChainTip, ChainTipChange), StateInitError> {
        let (finalized_state, finalized_tip, historical_trees, timer) = {
            let config = config.clone();
            let network = network.clone();
            tokio::task::spawn_blocking(move || {
                let timer = CodeTimer::start();
                // `expect` would format the error with `Debug`, which drops the actionable
                // guidance each `StateInitError` carries in its `Display` message.
                let finalized_state = FinalizedState::new(&config, &network)
                    .unwrap_or_else(|error| match error {
                        // This database cannot be repaired, and the generic hint below would
                        // send the operator looking at permissions and disk space instead.
                        error @ StateInitError::VctSproutHistoryUnrepairable => {
                            panic!("{error}")
                        }
                        error => panic!(
                            "opening the read-write finalized state database failed: {error}; \
                             check that the state cache directory is writable and not locked by \
                             another Zakura instance, and that there is free disk space"
                        ),
                    })
                    .with_checkpoint_raw_tx_retention(max_checkpoint_height, &config);
                timer.finish_desc("opening finalized state database");

                let timer = CodeTimer::start();
                let finalized_tip = finalized_chain_tip(&finalized_state.db);
                let historical_trees = load_historical_frontier_artifact(
                    &network,
                    &config,
                    finalized_state.db.vct_synced_below().is_some(),
                )?;
                let historical_trees =
                    historical_trees.discard_if_before_vct_handoff(&config, &finalized_state.db);

                Ok::<_, StateInitError>((finalized_state, finalized_tip, historical_trees, timer))
            })
            .await
            .expect("failed to join blocking task")?
        };

        // # Correctness
        //
        // The state service must set the finalized block write sender to `None`
        // if there are blocks in the restored non-finalized state that are above
        // the max checkpoint height so that non-finalized blocks can be written, otherwise,
        // Zebra will be unable to commit semantically verified blocks, and its chain sync will stall.
        //
        // The state service must not set the finalized block write sender to `None` if there
        // aren't blocks in the restored non-finalized state that are above the max checkpoint height,
        // otherwise, unless checkpoint sync is disabled in the zakura-consensus configuration,
        // Zebra will be unable to commit checkpoint verified blocks, and its chain sync will stall.
        let is_finalized_tip_past_max_checkpoint = if let Some(tip) = &finalized_tip {
            tip.height >= max_checkpoint_height
        } else {
            false
        };
        let backup_dir_path = config.non_finalized_state_backup_dir(network);
        let skip_backup_task = config.debug_skip_non_finalized_state_backup_task;
        let (non_finalized_state, non_finalized_state_sender, non_finalized_state_receiver) =
            NonFinalizedState::new(network)
                .with_backup(
                    backup_dir_path.clone(),
                    &finalized_state.db,
                    is_finalized_tip_past_max_checkpoint,
                    config.debug_skip_non_finalized_state_backup_task,
                )
                .await;

        let non_finalized_block_write_sent_hashes = SentHashes::new(&non_finalized_state);
        let initial_tip = non_finalized_state
            .best_tip_block()
            .map(|cv_block| cv_block.block.clone())
            .map(CheckpointVerifiedBlock::from)
            .map(ChainTipBlock::from)
            .or(finalized_tip);

        tracing::info!(chain_tip = ?initial_tip.as_ref().map(|tip| (tip.hash, tip.height)), "loaded Zakura state cache");

        let (chain_tip_sender, latest_chain_tip, chain_tip_change) =
            ChainTipSender::new(initial_tip, network);

        let finalized_state_for_writing = finalized_state.clone();
        let should_use_finalized_block_write_sender = non_finalized_state.is_chain_set_empty();
        let sync_backup_dir_path = backup_dir_path.filter(|_| skip_backup_task);
        let (header_chain_snapshot_sender, header_chain_snapshot_receiver) =
            tokio::sync::watch::channel(None);
        let (header_chain_view_sender, header_chain_view_receiver) =
            tokio::sync::watch::channel(None);
        let durable_header_runtime_exists =
            HeaderChainStore::new(finalized_state.db.header_chain_disk_db())
                .is_initialized()
                .expect("the opened state database can classify its durable header runtime");
        let (header_runtime_status_sender, header_runtime_status_receiver) =
            tokio::sync::watch::channel(
                zakura_node_services::sync_lifecycle::HeaderRuntimeStatus::Detached {
                    epoch: zakura_node_services::sync_lifecycle::LifecycleEpoch::INITIAL,
                    reason: if durable_header_runtime_exists {
                        zakura_node_services::sync_lifecycle::HeaderRuntimeDetachedReason::AttachmentPending
                    } else {
                        zakura_node_services::sync_lifecycle::HeaderRuntimeDetachedReason::AwaitingSemanticHandoff
                    },
                },
            );
        let (header_chain_reader_sender, header_chain_reader_receiver) =
            tokio::sync::watch::channel(None);
        let (
            block_write_sender,
            invalid_block_write_reset_receiver,
            non_finalized_rejected_receiver,
            vct_root_repair_receiver,
            block_write_failure,
            block_write_task,
        ) = write::BlockWriteSender::spawn(
            finalized_state_for_writing,
            non_finalized_state,
            chain_tip_sender,
            non_finalized_state_sender,
            should_use_finalized_block_write_sender,
            sync_backup_dir_path,
            write::HeaderChainObservers::new(
                header_chain_snapshot_sender,
                header_chain_view_sender,
                header_chain_reader_sender,
                header_runtime_status_sender,
            ),
        );

        let read_service = ReadStateService::new(
            &finalized_state,
            block_write_task,
            block_write_failure,
            non_finalized_state_receiver,
            vct_root_repair_receiver,
            HeaderChainSubscriptions {
                snapshots: header_chain_snapshot_receiver,
                views: header_chain_view_receiver,
                runtime_status: header_runtime_status_receiver,
                reader: header_chain_reader_receiver,
            },
            historical_trees,
        );

        let full_verifier_utxo_lookahead = max_checkpoint_height
            - HeightDiff::try_from(checkpoint_verify_concurrency_limit)
                .expect("fits in HeightDiff");
        let full_verifier_utxo_lookahead =
            full_verifier_utxo_lookahead.unwrap_or(block::Height::MIN);
        let non_finalized_state_queued_blocks = QueuedBlocks::default();
        let pending_utxos = PendingUtxos::default();

        let finalized_block_write_last_sent_hash =
            tokio::task::spawn_blocking(move || finalized_state.db.finalized_tip_hash())
                .await
                .expect("failed to join blocking task");

        let state = Self {
            network: network.clone(),
            full_verifier_utxo_lookahead,
            max_checkpoint_height,
            non_finalized_state_queued_blocks,
            finalized_state_queued_blocks: HashMap::new(),
            block_write_sender,
            finalized_block_write_last_sent_hash,
            non_finalized_block_write_sent_hashes,
            non_finalized_failed_ancestors: IndexMap::new(),
            invalid_block_write_reset_receiver,
            non_finalized_rejected_receiver,
            pending_utxos,
            last_prune: Instant::now(),
            read_service: read_service.clone(),
            max_finalized_queue_height: f64::NAN,
        };
        timer.finish_desc("initializing state service");

        tracing::info!("starting legacy chain check");
        let timer = CodeTimer::start();

        if let (Some(tip), Some(nu5_activation_height)) = (
            {
                let read_state = state.read_service.clone();
                tokio::task::spawn_blocking(move || read_state.best_tip())
                    .await
                    .expect("task should not panic")
            },
            NetworkUpgrade::Nu5.activation_height(network),
        ) {
            if let Err(error) = check::legacy_chain(
                nu5_activation_height,
                any_ancestor_blocks(
                    &state.read_service.latest_non_finalized_state(),
                    &state.read_service.db,
                    tip.1,
                ),
                &state.network,
                MAX_LEGACY_CHAIN_BLOCKS,
            ) {
                let legacy_db_path = state.read_service.db.path().to_path_buf();
                panic!(
                    "Cached state contains a legacy chain.\n\
                     An outdated Zebra version did not know about a recent network upgrade,\n\
                     so it followed a legacy chain using outdated consensus branch rules.\n\
                     Hint: Delete your database, and restart Zebra to do a full sync.\n\
                     Database path: {legacy_db_path:?}\n\
                     Error: {error:?}",
                );
            }
        }

        tracing::info!("cached state consensus branch is valid: no legacy chain found");
        timer.finish_desc("legacy chain check");

        // Spawn a background task to periodically export RocksDB metrics to Prometheus
        let db_for_metrics = read_service.db.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                db_for_metrics.export_metrics();
            }
        });

        Ok((state, read_service, latest_chain_tip, chain_tip_change))
    }

    /// Call read only state service to log rocksdb database metrics.
    pub fn log_db_metrics(&self) {
        self.read_service.db.print_db_metrics();
    }

    /// Queue a checkpoint verified block for verification and storage in the finalized state.
    ///
    /// Returns a channel receiver that provides the result of the block commit.
    fn queue_and_commit_to_finalized_state(
        &mut self,
        checkpoint_verified: CheckpointVerifiedBlock,
    ) -> oneshot::Receiver<Result<block::Hash, CommitCheckpointVerifiedError>> {
        // # Correctness & Performance
        //
        // This method must not block, access the database, or perform CPU-intensive tasks,
        // because it is called directly from the tokio executor's Future threads.

        let queued_prev_hash = checkpoint_verified.block.header.previous_block_hash;
        let queued_height = checkpoint_verified.height;

        // If we're close to the final checkpoint, make the block's UTXOs available for
        // semantic block verification, even when it is in the channel.
        if self.is_close_to_final_checkpoint(queued_height) {
            self.non_finalized_block_write_sent_hashes
                .add_finalized(&checkpoint_verified)
        }

        let (rsp_tx, rsp_rx) = oneshot::channel();
        let queued = (checkpoint_verified, rsp_tx);

        if self.block_write_sender.finalized.is_some() {
            // We're still committing checkpoint verified blocks
            if let Some(duplicate_queued) = self
                .finalized_state_queued_blocks
                .insert(queued_prev_hash, queued)
            {
                Self::send_checkpoint_verified_block_error(
                    duplicate_queued,
                    CommitBlockError::new_duplicate(
                        Some(queued_prev_hash.into()),
                        KnownBlock::Queue,
                    ),
                );
            }

            self.drain_finalized_queue_and_commit();
        } else {
            // We've finished committing checkpoint verified blocks to the finalized state,
            // so drop any repeated queued blocks, and return an error.
            //
            // TODO: track the latest sent height, and drop any blocks under that height
            //       every time we send some blocks (like QueuedSemanticallyVerifiedBlocks)
            Self::send_checkpoint_verified_block_error(
                queued,
                CommitBlockError::new_duplicate(None, KnownBlock::Finalized),
            );

            self.clear_finalized_block_queue(CommitBlockError::new_duplicate(
                None,
                KnownBlock::Finalized,
            ));
        }

        if self.finalized_state_queued_blocks.is_empty() {
            self.max_finalized_queue_height = f64::NAN;
        } else if self.max_finalized_queue_height.is_nan()
            || self.max_finalized_queue_height < queued_height.0 as f64
        {
            // if there are still blocks in the queue, then either:
            //   - the new block was lower than the old maximum, and there was a gap before it,
            //     so the maximum is still the same (and we skip this code), or
            //   - the new block is higher than the old maximum, and there is at least one gap
            //     between the finalized tip and the new maximum
            self.max_finalized_queue_height = queued_height.0 as f64;
        }

        metrics::gauge!("state.checkpoint.queued.max.height").set(self.max_finalized_queue_height);
        metrics::gauge!("state.checkpoint.queued.block.count")
            .set(self.finalized_state_queued_blocks.len() as f64);

        rsp_rx
    }

    /// Finds finalized state queue blocks to be committed to the state in order,
    /// removes them from the queue, and sends them to the block commit task.
    ///
    /// After queueing a finalized block, this method checks whether the newly
    /// queued block (and any of its descendants) can be committed to the state.
    ///
    /// Returns an error if the block commit channel has been closed.
    pub fn drain_finalized_queue_and_commit(&mut self) {
        use tokio::sync::mpsc::error::{SendError, TryRecvError};

        // # Correctness & Performance
        //
        // This method must not block, access the database, or perform CPU-intensive tasks,
        // because it is called directly from the tokio executor's Future threads.

        // If a block failed, we need to start again from a valid tip.
        match self.invalid_block_write_reset_receiver.try_recv() {
            Ok(reset_tip_hash) => self.finalized_block_write_last_sent_hash = reset_tip_hash,
            Err(TryRecvError::Disconnected) => {
                info!("Block commit task closed the block reset channel. Is Zakura shutting down?");
                return;
            }
            // There are no errors, so we can just use the last block hash we sent
            Err(TryRecvError::Empty) => {}
        }

        while let Some(queued_block) = self
            .finalized_state_queued_blocks
            .remove(&self.finalized_block_write_last_sent_hash)
        {
            let last_sent_finalized_block_height = queued_block.0.height;

            self.finalized_block_write_last_sent_hash = queued_block.0.hash;

            // If we've finished sending finalized blocks, ignore any repeated blocks.
            // (Blocks can be repeated after a syncer reset.)
            if let Some(finalized_block_write_sender) = &self.block_write_sender.finalized {
                let send_result = finalized_block_write_sender.send(queued_block);

                // If the receiver is closed, we can't send any more blocks.
                if let Err(SendError(queued)) = send_result {
                    // If Zebra is shutting down, drop blocks and return an error.
                    Self::send_checkpoint_verified_block_error(
                        queued,
                        CommitBlockError::WriteTaskExited,
                    );

                    self.clear_finalized_block_queue(CommitBlockError::WriteTaskExited);
                } else {
                    metrics::gauge!("state.checkpoint.sent.block.height")
                        .set(last_sent_finalized_block_height.0 as f64);
                };
            }
        }
    }

    /// Drain failed writes, clear their sent hashes, and complete queued descendants.
    ///
    /// This closes the lockout window where a rejected block keeps its hash
    /// recorded as "sent", so a subsequent honest re-delivery of a block at
    /// the same hash is not short-circuited as a false "duplicate".
    ///
    /// # Correctness & Performance
    ///
    /// Like the other drain methods on `StateService`, this must not block,
    /// access the database, or perform CPU-intensive work, because it is
    /// called directly from the tokio executor's Future threads.
    fn drain_non_finalized_rejected_hashes(&mut self) {
        use tokio::sync::mpsc::error::TryRecvError;

        loop {
            match self.non_finalized_rejected_receiver.try_recv() {
                Ok(failure) => self.handle_non_finalized_write_failure(failure),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    info!(
                        "Block commit task closed the non-finalized rejected hash channel. \
                         Is Zakura shutting down?"
                    );
                    break;
                }
            }
        }
    }

    fn poll_non_finalized_write_failures(&mut self, cx: &mut Context<'_>) {
        while let Poll::Ready(Some(failure)) =
            Pin::new(&mut self.non_finalized_rejected_receiver).poll_recv(cx)
        {
            self.handle_non_finalized_write_failure(failure);
        }
    }

    fn handle_non_finalized_write_failure(&mut self, failure: write::NonFinalizedWriteFailure) {
        self.non_finalized_block_write_sent_hashes
            .remove(&failure.hash);
        let error = Self::failed_ancestor_error(failure.hash, failure.kind);
        let descendants = self
            .non_finalized_state_queued_blocks
            .fail_descendants(failure.hash, error.into());
        for descendant in descendants {
            self.remember_failed_ancestor(descendant, failure.hash, failure.kind);
        }
        self.remember_failed_ancestor(failure.hash, failure.hash, failure.kind);
    }

    fn failed_ancestor_error(
        ancestor: block::Hash,
        kind: write::NonFinalizedWriteFailureKind,
    ) -> CommitBlockError {
        match kind {
            write::NonFinalizedWriteFailureKind::Invalid => CommitBlockError::ValidateContextError(
                Box::new(ValidateContextError::InvalidAncestorBlock(ancestor)),
            ),
            write::NonFinalizedWriteFailureKind::Retryable => CommitBlockError::HeaderChainError {
                error: format!(
                    "ancestor {ancestor} did not commit because of a local state write failure"
                ),
            },
        }
    }

    fn remember_failed_ancestor(
        &mut self,
        hash: block::Hash,
        ancestor: block::Hash,
        kind: write::NonFinalizedWriteFailureKind,
    ) {
        self.non_finalized_failed_ancestors.shift_remove(&hash);
        self.non_finalized_failed_ancestors
            .insert(hash, (ancestor, kind));
        while self.non_finalized_failed_ancestors.len() > Self::FAILED_ANCESTOR_LIMIT {
            self.non_finalized_failed_ancestors.shift_remove_index(0);
        }
    }

    /// Drops all finalized state queue blocks, and sends an error on their result channels.
    fn clear_finalized_block_queue(
        &mut self,
        error: impl Into<CommitCheckpointVerifiedError> + Clone,
    ) {
        for (_hash, queued) in self.finalized_state_queued_blocks.drain() {
            Self::send_checkpoint_verified_block_error(queued, error.clone());
        }
    }

    /// Send an error on a `QueuedCheckpointVerified` block's result channel, and drop the block
    fn send_checkpoint_verified_block_error(
        queued: QueuedCheckpointVerified,
        error: impl Into<CommitCheckpointVerifiedError>,
    ) {
        let (finalized, rsp_tx) = queued;

        // The block sender might have already given up on this block,
        // so ignore any channel send errors.
        let _ = rsp_tx.send(Err(error.into()));
        std::mem::drop(finalized);
    }

    /// Drops all non-finalized state queue blocks, and sends an error on their result channels.
    fn clear_non_finalized_block_queue(
        &mut self,
        error: impl Into<CommitSemanticallyVerifiedError> + Clone,
    ) {
        for (_hash, queued) in self.non_finalized_state_queued_blocks.drain() {
            Self::send_semantically_verified_block_error(queued, error.clone());
        }
    }

    /// Send an error on a `QueuedSemanticallyVerified` block's result channel, and drop the block
    fn send_semantically_verified_block_error(
        queued: QueuedSemanticallyVerified,
        error: impl Into<CommitSemanticallyVerifiedError>,
    ) {
        let (finalized, rsp_tx) = queued;

        // The block sender might have already given up on this block,
        // so ignore any channel send errors.
        let _ = rsp_tx.send(Err(error.into()));
        std::mem::drop(finalized);
    }

    /// Attempts to hand the block write task off from committing checkpoint verified blocks to the
    /// finalized state to committing semantically verified blocks to the non-finalized state.
    ///
    /// We've finished sending checkpoint verified blocks once the last checkpoint block has been
    /// durably written to disk, i.e. the finalized tip hash on disk matches the last finalized block
    /// hash we sent, and either:
    /// - the finalized tip has reached the maximum checkpoint height (the last block the checkpoint
    ///   verifier commits to the finalized state), or
    /// - a semantically verified child of the last block we sent is already queued.
    ///
    /// The height condition is the one that matters in production: the checkpoint verifier only
    /// commits blocks up to `max_checkpoint_height`, so once the finalized tip reaches that height
    /// the handoff happens immediately, **without** waiting for a semantically verified block to
    /// arrive. The first semantically verified block then has a valid finalized parent the instant
    /// it shows up, instead of the pipeline stalling at the checkpoint boundary.
    ///
    /// The queued-child condition is a fallback for configurations with no finite checkpoint height
    /// (`max_checkpoint_height == Height::MAX`, e.g. full-verification test setups), where the
    /// height condition can never be met.
    ///
    /// Returns `true` if the handoff was performed on this call.
    ///
    /// # Correctness & Performance
    ///
    /// This method must not block or perform CPU-intensive tasks, because it is called directly
    /// from the tokio executor's Future threads, including from [`Service::poll_ready()`]. Once the
    /// handoff has happened, `block_write_sender.finalized` is `None`, so the cheap first check
    /// short-circuits and the database is not read again.
    fn try_handoff_to_non_finalized_write(&mut self) -> bool {
        // The database tip is only read while we are still committing checkpoint verified blocks, so
        // the cheap `is_some()` check short-circuits this for the rest of the node's life.
        if self.block_write_sender.finalized.is_some()
            && self.read_service.db.finalized_tip_hash()
                == self.finalized_block_write_last_sent_hash
            && (self
                .read_service
                .db
                .finalized_tip_height()
                .is_some_and(|tip_height| tip_height >= self.max_checkpoint_height)
                || self
                    .non_finalized_state_queued_blocks
                    .has_queued_children(self.finalized_block_write_last_sent_hash))
        {
            // Tell the block write task to stop committing checkpoint verified blocks to the
            // finalized state, and move on to committing semantically verified blocks to the
            // non-finalized state.
            std::mem::drop(self.block_write_sender.finalized.take());
            // Remove any checkpoint-verified block hashes from `non_finalized_block_write_sent_hashes`.
            self.non_finalized_block_write_sent_hashes = SentHashes::default();
            // Mark `SentHashes` as usable by the `can_fork_chain_at()` method.
            self.non_finalized_block_write_sent_hashes
                .can_fork_chain_at_hashes = true;
            // Send blocks from non-finalized queue
            self.send_ready_non_finalized_queued(self.finalized_block_write_last_sent_hash);
            // We've finished committing checkpoint verified blocks to finalized state, so drop any repeated queued blocks.
            self.clear_finalized_block_queue(CommitBlockError::new_duplicate(
                None,
                KnownBlock::Finalized,
            ));

            true
        } else {
            false
        }
    }

    /// Queue a semantically verified block for contextual verification and check if any queued
    /// blocks are ready to be verified and committed to the state.
    ///
    /// This function encodes the logic for [committing non-finalized blocks][1]
    /// in RFC0005.
    ///
    /// [1]: https://zebra.zfnd.org/dev/rfcs/0005-state-updates.html#committing-non-finalized-blocks
    #[instrument(level = "debug", skip(self, semantically_verified))]
    fn queue_and_commit_to_non_finalized_state(
        &mut self,
        semantically_verified: SemanticallyVerifiedBlock,
    ) -> oneshot::Receiver<Result<block::Hash, CommitSemanticallyVerifiedError>> {
        tracing::debug!(block = %semantically_verified.block, "queueing block for contextual verification");
        let parent_hash = semantically_verified.block.header.previous_block_hash;

        // Drop hashes of any blocks the write task has rejected before checking
        // the SentHashes membership below. Without this, a rejected same-hash
        // block would lock out a later honest re-delivery of a block at the
        // same hash as a false "duplicate".
        self.drain_non_finalized_rejected_hashes();

        if let Some((ancestor, kind)) = self
            .non_finalized_failed_ancestors
            .get(&parent_hash)
            .copied()
        {
            if self.can_fork_chain_at(&parent_hash) {
                self.non_finalized_failed_ancestors
                    .shift_remove(&parent_hash);
            } else {
                let child_hash = semantically_verified.hash;
                self.remember_failed_ancestor(child_hash, ancestor, kind);
                let (rsp_tx, rsp_rx) = oneshot::channel();
                let _ = rsp_tx.send(Err(Self::failed_ancestor_error(ancestor, kind).into()));
                return rsp_rx;
            }
        }

        if self
            .non_finalized_block_write_sent_hashes
            .contains(&semantically_verified.hash)
        {
            let (rsp_tx, rsp_rx) = oneshot::channel();
            let _ = rsp_tx.send(Err(CommitBlockError::new_duplicate(
                Some(semantically_verified.hash.into()),
                KnownBlock::WriteChannel,
            )
            .into()));
            return rsp_rx;
        }

        if self
            .read_service
            .db
            .contains_height(semantically_verified.height)
        {
            let (rsp_tx, rsp_rx) = oneshot::channel();
            let _ = rsp_tx.send(Err(CommitBlockError::new_duplicate(
                Some(semantically_verified.height.into()),
                KnownBlock::Finalized,
            )
            .into()));
            return rsp_rx;
        }

        // [`Request::CommitSemanticallyVerifiedBlock`] contract: a request to commit a block which
        // has been queued but not yet committed to the state fails the older request and replaces
        // it with the newer request.
        let rsp_rx = if let Some((_, old_rsp_tx)) = self
            .non_finalized_state_queued_blocks
            .get_mut(&semantically_verified.hash)
        {
            tracing::debug!("replacing older queued request with new request");
            let (mut rsp_tx, rsp_rx) = oneshot::channel();
            std::mem::swap(old_rsp_tx, &mut rsp_tx);
            let _ = rsp_tx.send(Err(CommitBlockError::new_duplicate(
                Some(semantically_verified.hash.into()),
                KnownBlock::Queue,
            )
            .into()));
            rsp_rx
        } else {
            let (rsp_tx, rsp_rx) = oneshot::channel();
            self.non_finalized_state_queued_blocks
                .queue((semantically_verified, rsp_tx));
            rsp_rx
        };

        // Attempt to hand off from committing checkpoint verified blocks to committing
        // semantically verified blocks. This usually already happened in `poll_ready()` once the
        // final checkpoint write became durable, but we also check here in case this block is what
        // completes the handoff condition.
        if self.try_handoff_to_non_finalized_write() {
            // The handoff happened on this call: the queued children, including the block just
            // queued above, were sent to the non-finalized write task.
        } else if !self.can_fork_chain_at(&parent_hash) {
            tracing::trace!("unready to verify, returning early");
        } else if self.block_write_sender.finalized.is_none() {
            // Wait until block commit task is ready to write non-finalized blocks before dequeuing them
            self.send_ready_non_finalized_queued(parent_hash);

            let finalized_tip_height = self.read_service.db.finalized_tip_height().expect(
                "Finalized state must have at least one block before committing non-finalized state",
            );

            self.non_finalized_state_queued_blocks
                .prune_by_height(finalized_tip_height);

            self.non_finalized_block_write_sent_hashes
                .prune_by_height(finalized_tip_height);
        }

        rsp_rx
    }

    /// Returns `true` if `hash` is a valid previous block hash for new non-finalized blocks.
    fn can_fork_chain_at(&self, hash: &block::Hash) -> bool {
        self.non_finalized_block_write_sent_hashes
            .can_fork_chain_at(hash)
            || &self.read_service.db.finalized_tip_hash() == hash
    }

    /// Returns `true` if `queued_height` is near the final checkpoint.
    ///
    /// The semantic block verifier needs access to UTXOs from checkpoint verified blocks
    /// near the final checkpoint, so that it can verify blocks that spend those UTXOs.
    ///
    /// If it doesn't have the required UTXOs, some blocks will time out,
    /// but succeed after a syncer restart.
    fn is_close_to_final_checkpoint(&self, queued_height: block::Height) -> bool {
        queued_height >= self.full_verifier_utxo_lookahead
    }

    /// Sends all queued blocks whose parents have recently arrived starting from `new_parent`
    /// in breadth-first ordering to the block write task which will attempt to validate and commit them
    #[tracing::instrument(level = "debug", skip(self, new_parent))]
    fn send_ready_non_finalized_queued(&mut self, new_parent: block::Hash) {
        use tokio::sync::mpsc::error::SendError;
        if let Some(non_finalized_block_write_sender) = &self.block_write_sender.non_finalized {
            let mut new_parents: Vec<block::Hash> = vec![new_parent];

            while let Some(parent_hash) = new_parents.pop() {
                let queued_children = self
                    .non_finalized_state_queued_blocks
                    .dequeue_children(parent_hash);

                for queued_child in queued_children {
                    let (SemanticallyVerifiedBlock { hash, .. }, _) = queued_child;

                    self.non_finalized_block_write_sent_hashes
                        .add(&queued_child.0);
                    let send_result = non_finalized_block_write_sender.send(queued_child.into());

                    if let Err(SendError(NonFinalizedWriteMessage::Commit(queued))) = send_result {
                        // If Zebra is shutting down, drop blocks and return an error.
                        Self::send_semantically_verified_block_error(
                            queued,
                            CommitBlockError::WriteTaskExited,
                        );

                        self.clear_non_finalized_block_queue(CommitBlockError::WriteTaskExited);

                        return;
                    };

                    new_parents.push(hash);
                }
            }

            self.non_finalized_block_write_sent_hashes.finish_batch();
        };
    }

    /// Return the tip of the current best chain.
    pub fn best_tip(&self) -> Option<(block::Height, block::Hash)> {
        self.read_service.best_tip()
    }

    fn send_invalidate_block(
        &self,
        hash: block::Hash,
    ) -> oneshot::Receiver<Result<block::Hash, InvalidateError>> {
        let (rsp_tx, rsp_rx) = oneshot::channel();

        let Some(sender) = &self.block_write_sender.non_finalized else {
            let _ = rsp_tx.send(Err(InvalidateError::ProcessingCheckpointedBlocks));
            return rsp_rx;
        };

        if let Err(tokio::sync::mpsc::error::SendError(error)) =
            sender.send(NonFinalizedWriteMessage::Invalidate { hash, rsp_tx })
        {
            let NonFinalizedWriteMessage::Invalidate { rsp_tx, .. } = error else {
                unreachable!("should return the same Invalidate message could not be sent");
            };

            let _ = rsp_tx.send(Err(InvalidateError::SendInvalidateRequestFailed));
        }

        rsp_rx
    }

    fn send_reconsider_block(
        &self,
        hash: block::Hash,
    ) -> oneshot::Receiver<Result<Vec<block::Hash>, ReconsiderError>> {
        let (rsp_tx, rsp_rx) = oneshot::channel();

        let Some(sender) = &self.block_write_sender.non_finalized else {
            let _ = rsp_tx.send(Err(ReconsiderError::CheckpointCommitInProgress));
            return rsp_rx;
        };

        if let Err(tokio::sync::mpsc::error::SendError(error)) =
            sender.send(NonFinalizedWriteMessage::Reconsider { hash, rsp_tx })
        {
            let NonFinalizedWriteMessage::Reconsider { rsp_tx, .. } = error else {
                unreachable!("should return the same Reconsider message could not be sent");
            };

            let _ = rsp_tx.send(Err(ReconsiderError::ReconsiderSendFailed));
        }

        rsp_rx
    }

    fn send_header_chain_insert(
        &self,
        prepared: crate::PreparedHeaderChainInsert,
    ) -> oneshot::Receiver<Result<zakura_header_chain::ApplyResult, HeaderChainStoreError>> {
        let (rsp_tx, rsp_rx) = oneshot::channel();
        let Some(sender) = &self.block_write_sender.non_finalized else {
            let _ = rsp_tx.send(Err(HeaderChainStoreError::Uninitialized));
            return rsp_rx;
        };
        if let Err(tokio::sync::mpsc::error::SendError(message)) =
            sender.send(NonFinalizedWriteMessage::ApplyHeaderChainInsert { prepared, rsp_tx })
        {
            let NonFinalizedWriteMessage::ApplyHeaderChainInsert { rsp_tx, .. } = message else {
                unreachable!("the failed send returns the same header insertion message");
            };
            let _ = rsp_tx.send(Err(HeaderChainStoreError::Uninitialized));
        }
        rsp_rx
    }

    fn send_header_chain_body_unavailable(
        &self,
        prepared: crate::PreparedHeaderChainBodyEvidence,
    ) -> oneshot::Receiver<Result<zakura_header_chain::ApplyResult, HeaderChainStoreError>> {
        let (rsp_tx, rsp_rx) = oneshot::channel();
        let Some(sender) = &self.block_write_sender.non_finalized else {
            let _ = rsp_tx.send(Err(HeaderChainStoreError::Uninitialized));
            return rsp_rx;
        };
        if let Err(tokio::sync::mpsc::error::SendError(message)) = sender
            .send(NonFinalizedWriteMessage::RecordHeaderChainBodyUnavailable { prepared, rsp_tx })
        {
            let NonFinalizedWriteMessage::RecordHeaderChainBodyUnavailable { rsp_tx, .. } = message
            else {
                unreachable!("the failed send returns the same body-availability message");
            };
            let _ = rsp_tx.send(Err(HeaderChainStoreError::Uninitialized));
        }
        rsp_rx
    }

    fn send_header_chain_body_invalid(
        &self,
        prepared: crate::PreparedHeaderChainBodyEvidence,
    ) -> oneshot::Receiver<Result<zakura_header_chain::ApplyResult, HeaderChainStoreError>> {
        let (rsp_tx, rsp_rx) = oneshot::channel();
        let Some(sender) = &self.block_write_sender.non_finalized else {
            let _ = rsp_tx.send(Err(HeaderChainStoreError::Uninitialized));
            return rsp_rx;
        };
        if let Err(tokio::sync::mpsc::error::SendError(message)) =
            sender.send(NonFinalizedWriteMessage::RecordHeaderChainBodyInvalid { prepared, rsp_tx })
        {
            let NonFinalizedWriteMessage::RecordHeaderChainBodyInvalid { rsp_tx, .. } = message
            else {
                unreachable!("the failed send returns the same invalid-body message");
            };
            let _ = rsp_tx.send(Err(HeaderChainStoreError::Uninitialized));
        }
        rsp_rx
    }

    fn send_header_chain_body_availability_restart(
        &self,
        prepared: crate::PreparedHeaderChainBodyEvidence,
    ) -> oneshot::Receiver<Result<zakura_header_chain::ApplyResult, HeaderChainStoreError>> {
        let (rsp_tx, rsp_rx) = oneshot::channel();
        let Some(sender) = &self.block_write_sender.non_finalized else {
            let _ = rsp_tx.send(Err(HeaderChainStoreError::Uninitialized));
            return rsp_rx;
        };
        if let Err(tokio::sync::mpsc::error::SendError(message)) = sender
            .send(NonFinalizedWriteMessage::RestartHeaderChainBodyAvailability { prepared, rsp_tx })
        {
            let NonFinalizedWriteMessage::RestartHeaderChainBodyAvailability { rsp_tx, .. } =
                message
            else {
                unreachable!("the failed send returns the same body-restart message");
            };
            let _ = rsp_tx.send(Err(HeaderChainStoreError::Uninitialized));
        }
        rsp_rx
    }

    fn send_header_chain_body_availability_retry(
        &self,
        prepared: crate::PreparedHeaderChainBodyEvidence,
    ) -> oneshot::Receiver<Result<zakura_header_chain::ApplyResult, HeaderChainStoreError>> {
        let (rsp_tx, rsp_rx) = oneshot::channel();
        let Some(sender) = &self.block_write_sender.non_finalized else {
            let _ = rsp_tx.send(Err(HeaderChainStoreError::Uninitialized));
            return rsp_rx;
        };
        if let Err(tokio::sync::mpsc::error::SendError(message)) = sender
            .send(NonFinalizedWriteMessage::RetryHeaderChainBodyAvailability { prepared, rsp_tx })
        {
            let NonFinalizedWriteMessage::RetryHeaderChainBodyAvailability { rsp_tx, .. } = message
            else {
                unreachable!("the failed send returns the same operator-retry message");
            };
            let _ = rsp_tx.send(Err(HeaderChainStoreError::Uninitialized));
        }
        rsp_rx
    }

    /// Assert some assumptions about the semantically verified `block` before it is queued.
    fn assert_block_can_be_validated(&self, block: &SemanticallyVerifiedBlock) {
        // required by `Request::CommitSemanticallyVerifiedBlock` call
        assert!(
            block.height > self.network.mandatory_checkpoint_height(),
            "invalid semantically verified block height: the canopy checkpoint is mandatory, pre-canopy \
            blocks, and the canopy activation block, must be committed to the state as finalized \
            blocks"
        );
    }

    fn known_sent_hash(&self, hash: &block::Hash) -> Option<KnownBlock> {
        self.non_finalized_block_write_sent_hashes
            .contains(hash)
            .then_some(KnownBlock::WriteChannel)
    }
}

impl ReadStateService {
    /// Creates a new read-only state service, using the provided finalized state and
    /// block write task handle.
    ///
    /// Returns the newly created service,
    /// and a watch channel for updating the shared recent non-finalized chain.
    fn new(
        finalized_state: &FinalizedState,
        block_write_task: Option<Arc<std::thread::JoinHandle<write::BlockWriteTaskExit>>>,
        block_write_failure: Arc<OnceLock<write::BlockWriteTaskFailure>>,
        non_finalized_state_receiver: WatchReceiver<NonFinalizedState>,
        vct_root_repair_receiver: tokio::sync::watch::Receiver<VctRootRepairStatus>,
        header_chain: HeaderChainSubscriptions,
        historical_trees: Arc<Mutex<read::HistoricalTreeCache>>,
    ) -> Self {
        let historical_subtrees =
            finalized_state::embedded_historical_subtrees(&finalized_state.network()).map(Arc::new);

        let read_service = Self {
            network: finalized_state.network(),
            db: finalized_state.db.clone(),
            non_finalized_state_receiver,
            block_write_task,
            block_write_failure,
            historical_trees,
            historical_subtrees,
            vct_root_repair_receiver,
            header_chain_snapshot_receiver: header_chain.snapshots,
            header_chain_view_receiver: header_chain.views,
            header_runtime_status_receiver: header_chain.runtime_status,
            header_chain_reader_receiver: header_chain.reader,
        };

        tracing::debug!("created new read-only state service");

        read_service
    }

    /// Return the tip of the current best chain.
    pub fn best_tip(&self) -> Option<(block::Height, block::Hash)> {
        read::best_tip(&self.latest_non_finalized_state(), &self.db)
    }

    /// Returns the embedded subtree artifact and this database's fast-sync marker when the
    /// artifact can fill the skip band that marker describes.
    ///
    /// The marker is the height this database originally fast-synced to; it does not move when
    /// later releases raise the last checkpoint. A newer artifact is still eligible because it is
    /// append-only and therefore still contains every root skipped at that marker. An older
    /// artifact is not, because it cannot cover the extra skipped indexes.
    ///
    /// Serving clips published records to the marker, so the extra suffix of a newer artifact
    /// never answers for heights this node synced itself.
    ///
    /// This check is made when serving rather than at construction because the durable last
    /// checkpoint marker can be written after the read service starts.
    fn historical_subtrees_at_last_checkpoint(
        &self,
    ) -> Option<(&finalized_state::SubtreeArtifact, block::Height)> {
        let artifact = self.historical_subtrees.as_deref()?;
        let vct_applied_below = self.db.vct_synced_below()?;

        (artifact.last_checkpoint >= vct_applied_below).then_some((artifact, vct_applied_below))
    }

    /// Whether a published frontier grid is loaded and covers the durable VCT handoff.
    ///
    /// A marker written after construction can make the loaded grid too old. The first request
    /// that observes that transition reports it and discards the grid, so later requests see the
    /// absent band as unavailable without repeatedly checking the stale artifact.
    fn has_usable_historical_frontier_grid(&self) -> bool {
        let mut historical_trees = self
            .historical_trees
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let artifact_checkpoint = historical_trees.last_checkpoint();
        if let Some((artifact_checkpoint, vct_handoff)) =
            frontier_grid_ends_before_vct_handoff(artifact_checkpoint, self.db.vct_synced_below())
        {
            tracing::warn!(
                ?artifact_checkpoint,
                ?vct_handoff,
                "discarding historical frontier artifact that does not cover the database's VCT \
                 handoff"
            );
            metrics::counter!("state.historical_tree.artifact_before_vct_handoff").increment(1);
            *historical_trees = read::HistoricalTreeCache::default();
            return false;
        }

        artifact_checkpoint.is_some()
    }

    /// Subscribe to VCT supplied-root repair needs discovered by the finalized writer.
    pub fn subscribe_vct_root_repairs(&self) -> tokio::sync::watch::Receiver<VctRootRepairStatus> {
        self.vct_root_repair_receiver.clone()
    }

    /// Subscribe to snapshots published only after a durable header-engine commit.
    pub fn subscribe_header_chain_snapshots(
        &self,
    ) -> tokio::sync::watch::Receiver<Option<zakura_header_chain::EngineSnapshot>> {
        self.header_chain_snapshot_receiver.clone()
    }

    /// Subscribe to atomic snapshots and body-work epochs after durable commits.
    pub fn subscribe_header_chain_views(
        &self,
    ) -> tokio::sync::watch::Receiver<Option<zakura_header_chain::CommittedHeaderChainView>> {
        self.header_chain_view_receiver.clone()
    }

    /// Subscribe to explicit durable header-runtime attachment and readiness state.
    pub fn subscribe_header_runtime_status(
        &self,
    ) -> tokio::sync::watch::Receiver<zakura_node_services::sync_lifecycle::HeaderRuntimeStatus>
    {
        self.header_runtime_status_receiver.clone()
    }

    /// Gets a clone of the latest non-finalized state from the `non_finalized_state_receiver`
    fn latest_non_finalized_state(&self) -> NonFinalizedState {
        self.non_finalized_state_receiver.cloned_watch_data()
    }

    /// Gets a clone of the latest, best non-finalized chain from the `non_finalized_state_receiver`
    fn latest_best_chain(&self) -> Option<Arc<Chain>> {
        self.non_finalized_state_receiver
            .borrow_mapped(|non_finalized_state| non_finalized_state.best_chain().cloned())
    }

    /// Test-only access to the inner database.
    /// Can be used to modify the database without doing any consensus checks.
    #[cfg(any(test, feature = "proptest-impl"))]
    pub fn db(&self) -> &ZakuraDb {
        &self.db
    }

    /// Logs rocksdb metrics using the read only state service.
    pub fn log_db_metrics(&self) {
        self.db.print_db_metrics();
    }
}

impl Service<Request> for StateService {
    type Response = Response;
    type Error = BoxError;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Check for panics in the block write task
        let poll = self.read_service.poll_ready(cx);

        self.poll_non_finalized_write_failures(cx);

        // Hand off from finalized to non-finalized writes as soon as the final checkpoint block is
        // durably written, without waiting for a semantically verified block to arrive.
        self.try_handoff_to_non_finalized_write();

        // Prune outdated UTXO requests
        let now = Instant::now();

        if self.last_prune + Self::PRUNE_INTERVAL < now {
            let tip = self.best_tip();
            let old_len = self.pending_utxos.len();

            self.pending_utxos.prune();
            self.last_prune = now;

            let new_len = self.pending_utxos.len();
            let prune_count = old_len
                .checked_sub(new_len)
                .expect("prune does not add any utxo requests");
            if prune_count > 0 {
                tracing::debug!(
                    ?old_len,
                    ?new_len,
                    ?prune_count,
                    ?tip,
                    "pruned utxo requests"
                );
            } else {
                tracing::debug!(len = ?old_len, ?tip, "no utxo requests needed pruning");
            }
        }

        poll
    }

    #[instrument(name = "state", skip(self, req))]
    fn call(&mut self, req: Request) -> Self::Future {
        req.count_metric();
        let span = Span::current();

        match req {
            Request::ApplyHeaderChainInsert { prepared } => {
                let rsp_rx = self.send_header_chain_insert(prepared);
                async move {
                    rsp_rx
                        .await
                        .map_err(|_| BoxError::from("header-chain writer exited"))?
                        .map(Response::HeaderChainInsertApplied)
                        .map_err(BoxError::from)
                }
                .boxed()
            }
            Request::RecordHeaderChainBodyUnavailable { prepared } => {
                let rsp_rx = self.send_header_chain_body_unavailable(prepared);
                async move {
                    rsp_rx
                        .await
                        .map_err(|_| BoxError::from("header-chain writer exited"))?
                        .map(Response::HeaderChainBodyUnavailableRecorded)
                        .map_err(BoxError::from)
                }
                .boxed()
            }
            Request::RecordHeaderChainBodyInvalid { prepared } => {
                let rsp_rx = self.send_header_chain_body_invalid(prepared);
                async move {
                    rsp_rx
                        .await
                        .map_err(|_| BoxError::from("header-chain writer exited"))?
                        .map(Response::HeaderChainBodyInvalidRecorded)
                        .map_err(BoxError::from)
                }
                .boxed()
            }
            Request::RestartHeaderChainBodyAvailability { prepared } => {
                let rsp_rx = self.send_header_chain_body_availability_restart(prepared);
                async move {
                    rsp_rx
                        .await
                        .map_err(|_| BoxError::from("header-chain writer exited"))?
                        .map(Response::HeaderChainBodyAvailabilityRestarted)
                        .map_err(BoxError::from)
                }
                .boxed()
            }
            Request::RetryHeaderChainBodyAvailability { prepared } => {
                let rsp_rx = self.send_header_chain_body_availability_retry(prepared);
                async move {
                    rsp_rx
                        .await
                        .map_err(|_| BoxError::from("header-chain writer exited"))?
                        .map(Response::HeaderChainBodyAvailabilityRetried)
                        .map_err(BoxError::from)
                }
                .boxed()
            }
            // Uses non_finalized_state_queued_blocks and pending_utxos in the StateService
            // Accesses shared writeable state in the StateService, NonFinalizedState, and ZakuraDb.
            //
            // The expected error type for this request is `CommitSemanticallyVerifiedError`.
            Request::CommitSemanticallyVerifiedBlock(semantically_verified) => {
                let timer = CodeTimer::start();
                self.assert_block_can_be_validated(&semantically_verified);

                self.pending_utxos
                    .check_against_ordered(&semantically_verified.new_outputs);

                // # Performance
                //
                // Allow other async tasks to make progress while blocks are being verified
                // and written to disk. But wait for the blocks to finish committing,
                // so that `StateService` multi-block queries always observe a consistent state.
                //
                // Since each block is spawned into its own task,
                // there shouldn't be any other code running in the same task,
                // so we don't need to worry about blocking it:
                // https://docs.rs/tokio/latest/tokio/task/fn.block_in_place.html

                let rsp_rx = tokio::task::block_in_place(move || {
                    span.in_scope(|| {
                        self.queue_and_commit_to_non_finalized_state(semantically_verified)
                    })
                });

                // TODO:
                //   - check for panics in the block write task here,
                //     as well as in poll_ready()

                // The work is all done, the future just waits on a channel for the result
                timer.finish_desc("CommitSemanticallyVerifiedBlock");

                // Await the channel response, flatten the result, map receive errors to
                // `CommitSemanticallyVerifiedError::WriteTaskExited`.
                // Then flatten the nested Result and convert any errors to a BoxError.
                let span = Span::current();
                async move {
                    rsp_rx
                        .await
                        .map_err(|_recv_error| CommitBlockError::WriteTaskExited.into())
                        .and_then(|result| result)
                        .map_err(BoxError::from)
                        .map(Response::Committed)
                }
                .instrument(span)
                .boxed()
            }

            // Uses finalized_state_queued_blocks and pending_utxos in the StateService.
            // Accesses shared writeable state in the StateService.
            //
            // The expected error type for this request is `CommitCheckpointVerifiedError`.
            Request::CommitCheckpointVerifiedBlock(finalized) => {
                let timer = CodeTimer::start();
                // # Consensus
                //
                // A semantic block verification could have called AwaitUtxo
                // before this checkpoint verified block arrived in the state.
                // So we need to check for pending UTXO requests sent by running
                // semantic block verifications.
                //
                // This check is redundant for most checkpoint verified blocks,
                // because semantic verification can only succeed near the final
                // checkpoint, when all the UTXOs are available for the verifying block.
                //
                // (Checkpoint block UTXOs are verified using block hash checkpoints
                // and transaction merkle tree block header commitments.)
                self.pending_utxos
                    .check_against_ordered(&finalized.new_outputs);

                // # Performance
                //
                // This method doesn't block, access the database, or perform CPU-intensive tasks,
                // so we can run it directly in the tokio executor's Future threads.
                let rsp_rx = self.queue_and_commit_to_finalized_state(finalized);

                // TODO:
                //   - check for panics in the block write task here,
                //     as well as in poll_ready()

                // The work is all done, the future just waits on a channel for the result
                timer.finish_desc("CommitCheckpointVerifiedBlock");

                // Await the channel response, flatten the result, map receive errors to
                // `CommitCheckpointVerifiedError::WriteTaskExited`.
                // Then flatten the nested Result and convert any errors to a BoxError.
                async move {
                    rsp_rx
                        .await
                        .map_err(|_recv_error| CommitBlockError::WriteTaskExited.into())
                        .and_then(|result| result)
                        .map_err(BoxError::from)
                        .map(Response::Committed)
                }
                .instrument(span)
                .boxed()
            }

            // Uses pending_utxos and non_finalized_state_queued_blocks in the StateService.
            // If the UTXO isn't in the queued blocks, runs concurrently using the ReadStateService.
            Request::AwaitUtxo(outpoint) => {
                let timer = CodeTimer::start();
                // Prepare the AwaitUtxo future from PendingUxtos.
                let response_fut = self.pending_utxos.queue(outpoint);
                // Only instrument `response_fut`, the ReadStateService already
                // instruments its requests with the same span.

                let response_fut = response_fut.instrument(span).boxed();

                // Check the non-finalized block queue outside the returned future,
                // so we can access mutable state fields.
                if let Some(utxo) = self.non_finalized_state_queued_blocks.utxo(&outpoint) {
                    self.pending_utxos.respond(&outpoint, utxo);

                    // We're finished, the returned future gets the UTXO from the respond() channel.
                    timer.finish_desc("AwaitUtxo/queued-non-finalized");

                    return response_fut;
                }

                // Check the sent non-finalized blocks
                if let Some(utxo) = self.non_finalized_block_write_sent_hashes.utxo(&outpoint) {
                    self.pending_utxos.respond(&outpoint, utxo);

                    // We're finished, the returned future gets the UTXO from the respond() channel.
                    timer.finish_desc("AwaitUtxo/sent-non-finalized");

                    return response_fut;
                }

                // We ignore any UTXOs in FinalizedState.finalized_state_queued_blocks,
                // because it is only used during checkpoint verification.
                //
                // This creates a rare race condition, but it doesn't seem to happen much in practice.
                // See #5126 for details.

                // Manually send a request to the ReadStateService,
                // to get UTXOs from any non-finalized chain or the finalized chain.
                let read_service = self.read_service.clone();

                // Run the request in an async block, so we can await the response.
                async move {
                    let req = ReadRequest::AnyChainUtxo(outpoint);

                    let rsp = read_service.oneshot(req).await?;

                    // Optional TODO:
                    //  - make pending_utxos.respond() async using a channel,
                    //    so we can respond to all waiting requests here
                    //
                    // This change is not required for correctness, because:
                    // - any waiting requests should have returned when the block was sent to the state
                    // - otherwise, the request returns immediately if:
                    //   - the block is in the non-finalized queue, or
                    //   - the block is in any non-finalized chain or the finalized state
                    //
                    // And if the block is in the finalized queue,
                    // that's rare enough that a retry is ok.
                    if let ReadResponse::AnyChainUtxo(Some(utxo)) = rsp {
                        // We got a UTXO, so we replace the response future with the result own.
                        timer.finish_desc("AwaitUtxo/any-chain");

                        return Ok(Response::Utxo(utxo));
                    }

                    // We're finished, but the returned future is waiting on the respond() channel.
                    timer.finish_desc("AwaitUtxo/waiting");

                    response_fut.await
                }
                .boxed()
            }

            // Used by sync, inbound, and block verifier to check if a block is already in the state
            // before downloading or validating it.
            Request::KnownBlock(hash) => {
                let timer = CodeTimer::start();
                // The write task reports rejected bodies asynchronously.
                // Drain those reports before consulting the sent set.
                // This order prevents the sent set from classifying a different body with the
                // same header hash as a duplicate.
                self.drain_non_finalized_rejected_hashes();
                let sent_hash_response = self.known_sent_hash(&hash);
                let read_service = self.read_service.clone();

                async move {
                    if sent_hash_response.is_some() {
                        return Ok(Response::KnownBlock(sent_hash_response));
                    };

                    let response = read::non_finalized_state_contains_block_hash(
                        &read_service.latest_non_finalized_state(),
                        hash,
                    )
                    // TODO: Move this to a blocking task, perhaps by moving some of this logic to the ReadStateService.
                    .or_else(|| read::finalized_state_contains_block_hash(&read_service.db, hash));

                    timer.finish_desc("Request::KnownBlock");

                    Ok(Response::KnownBlock(response))
                }
                .boxed()
            }

            // The expected error type for this request is `InvalidateError`
            Request::InvalidateBlock(block_hash) => {
                let rsp_rx = tokio::task::block_in_place(move || {
                    span.in_scope(|| self.send_invalidate_block(block_hash))
                });

                // Await the channel response, flatten the result, map receive errors to
                // `InvalidateError::InvalidateRequestDropped`.
                // Then flatten the nested Result and convert any errors to a BoxError.
                let span = Span::current();
                async move {
                    rsp_rx
                        .await
                        .map_err(|_recv_error| InvalidateError::InvalidateRequestDropped)
                        .and_then(|result| result)
                        .map_err(BoxError::from)
                        .map(Response::Invalidated)
                }
                .instrument(span)
                .boxed()
            }

            // The expected error type for this request is `ReconsiderError`
            Request::ReconsiderBlock(block_hash) => {
                let rsp_rx = tokio::task::block_in_place(move || {
                    span.in_scope(|| self.send_reconsider_block(block_hash))
                });

                // Await the channel response, flatten the result, map receive errors to
                // `ReconsiderError::ReconsiderResponseDropped`.
                // Then flatten the nested Result and convert any errors to a BoxError.
                let span = Span::current();
                async move {
                    rsp_rx
                        .await
                        .map_err(|_recv_error| ReconsiderError::ReconsiderResponseDropped)
                        .and_then(|result| result)
                        .map_err(BoxError::from)
                        .map(Response::Reconsidered)
                }
                .instrument(span)
                .boxed()
            }

            // Runs concurrently using the ReadStateService
            Request::Tip
            | Request::Depth(_)
            | Request::BestChainNextMedianTimePast
            | Request::BestChainBlockHash(_)
            | Request::BlockLocator
            | Request::Transaction(_)
            | Request::UnspentBestChainUtxo(_)
            | Request::Block(_)
            | Request::AnyChainBlock(_)
            | Request::BlockHeader(_)
            | Request::FindBlockHashes { .. }
            | Request::FindBlockHeaders { .. }
            | Request::CheckBestChainTipNullifiersAndAnchors(_)
            | Request::CheckBlockProposalValidity(_) => {
                // Redirect the request to the concurrent ReadStateService
                let read_service = self.read_service.clone();

                async move {
                    let req = req
                        .try_into()
                        .expect("ReadRequest conversion should not fail");

                    let rsp = read_service.oneshot(req).await?;
                    let rsp = rsp.try_into().expect("Response conversion should not fail");

                    Ok(rsp)
                }
                .boxed()
            }
        }
    }
}

fn highest_common_body_header_frontier(
    mut height: block::Height,
    minimum_height: block::Height,
    mut body_hash: impl FnMut(block::Height) -> Option<block::Hash>,
    mut selected_hash: impl FnMut(
        block::Height,
    ) -> Result<
        Option<block::Hash>,
        finalized_state::header_chain::HeaderChainStoreError,
    >,
) -> Result<zakura_header_chain::Frontier, finalized_state::header_chain::HeaderChainStoreError> {
    loop {
        let body_hash = body_hash(height);
        let selected_hash = selected_hash(height)?;
        if let (Some(body_hash), Some(selected_hash)) = (body_hash, selected_hash) {
            if body_hash == selected_hash {
                return Ok(zakura_header_chain::Frontier::new(height, body_hash));
            }
        }
        if height <= minimum_height {
            return Err(
                finalized_state::header_chain::HeaderChainStoreError::Incoherent(
                    "selected headers and full state have no common ancestor",
                ),
            );
        }
        height = block::Height(height.0.saturating_sub(1));
    }
}

fn missing_block_body_metadata<C>(
    latest_chain: impl FnOnce() -> Option<C>,
    db: &ZakuraDb,
    header_chain: Option<&finalized_state::header_chain::HeaderChainReader>,
    from: block::Height,
    limit: u32,
) -> Result<crate::BlockSyncBodyMetadata, finalized_state::header_chain::HeaderChainStoreError>
where
    C: AsRef<Chain> + Clone,
{
    let (chain, verified_block_tip, selected_projection) = match header_chain {
        Some(reader) => {
            let ((chain, verified_block_tip), selected_projection) = reader
                .with_selected_projection(|| {
                    let chain = latest_chain();
                    let verified_block_tip = read::tip(chain.clone(), db);
                    (chain, verified_block_tip)
                })?;
            (chain, verified_block_tip, Some(selected_projection))
        }
        None => {
            let chain = latest_chain();
            let verified_block_tip = read::tip(chain.clone(), db);
            (chain, verified_block_tip, None)
        }
    };
    let best_header_tip = match &selected_projection {
        Some(selected_projection) => selected_projection.last().copied(),
        None => verified_block_tip
            .map(|(height, hash)| zakura_header_chain::Frontier::new(height, hash)),
    };
    let Some(best_header_tip) = best_header_tip else {
        return Err(
            finalized_state::header_chain::HeaderChainStoreError::Incoherent(
                "block sync has no selected or full-state frontier",
            ),
        );
    };

    let anchor = match (verified_block_tip, selected_projection.as_deref()) {
        (Some((verified_height, _verified_hash)), Some(selected_projection)) => {
            let minimum_height = selected_projection
                .first()
                .expect("the selected projection has a best header")
                .height;
            highest_common_body_header_frontier(
                verified_height.min(best_header_tip.height),
                minimum_height,
                |height| read::hash_by_height(chain.clone(), db, height),
                |height| {
                    Ok(selected_projection
                        .binary_search_by_key(&height, |frontier| frontier.height)
                        .ok()
                        .map(|index| selected_projection[index].hash))
                },
            )?
        }
        (Some((height, hash)), None) => zakura_header_chain::Frontier::new(height, hash),
        (None, Some(_)) => {
            return Err(
                finalized_state::header_chain::HeaderChainStoreError::Incoherent(
                    "selected headers exist without a full-state anchor",
                ),
            );
        }
        (None, None) => unreachable!("the absent-frontier case returned above"),
    };

    let first_selected = anchor.height.next().unwrap_or(anchor.height);
    let repairing_fork = verified_block_tip.is_some_and(|tip| tip != (anchor.height, anchor.hash));
    let verified_successor = verified_block_tip.and_then(|(height, _)| height.next().ok());
    let start = if repairing_fork && verified_successor == Some(from) {
        first_selected
    } else {
        first_selected.max(from)
    };

    if start > best_header_tip.height {
        return Ok(crate::BlockSyncBodyMetadata {
            anchor,
            blocks: Vec::new(),
        });
    }

    let count = limit.min(MAX_HEADER_SYNC_HEIGHT_RANGE).min(
        best_header_tip
            .height
            .0
            .saturating_sub(start.0)
            .saturating_add(1),
    );
    let size_hints: HashMap<_, _> = read::block_size_hints(chain.clone(), db, start, count)
        .into_iter()
        .collect();
    let selected_hashes: HashMap<_, _> = match selected_projection.as_deref() {
        Some(selected_projection) => selected_projection
            .iter()
            .copied()
            .filter(|frontier| {
                frontier.height >= start && frontier.height <= best_header_tip.height
            })
            .map(|frontier| (frontier.height, frontier.hash))
            .collect(),
        None => HashMap::new(),
    };

    let mut metadata = Vec::new();
    for offset in 0..count {
        let Some(height) = start.0.checked_add(offset).map(block::Height) else {
            break;
        };
        let body_hash = read::hash_by_height(chain.clone(), db, height);
        let selected_hash = match header_chain {
            Some(_) => selected_hashes.get(&height).copied(),
            None => body_hash,
        };
        let Some(hash) = selected_hash else {
            continue;
        };
        if db.contains_body_at_height(height) && body_hash == Some(hash) {
            continue;
        }
        metadata.push((height, hash, size_hints.get(&height).copied().flatten()));
    }

    Ok(crate::BlockSyncBodyMetadata {
        anchor,
        blocks: metadata,
    })
}

/// Returns the index range a subtree request covers, as a concrete range type.
///
/// Mirrors the read path's handling of an absent or overflowing end bound, where the request is
/// served to the end of what exists.
fn range_for(
    start_index: NoteCommitmentSubtreeIndex,
    end_index: Option<NoteCommitmentSubtreeIndex>,
) -> (
    Bound<NoteCommitmentSubtreeIndex>,
    Bound<NoteCommitmentSubtreeIndex>,
) {
    (
        Bound::Included(start_index),
        end_index.map_or(Bound::Unbounded, Bound::Excluded),
    )
}

/// Uses published subtrees when the node's own rows do not cover `start_index`.
///
/// The read result already applies the continuity contract and reports the absent band, so it
/// stands on its own when it covers `start_index`. Otherwise, this helper tries the skip-band
/// union supplied by `merge_published` and rechecks availability over that whole union — including
/// published records that complete above `verified_tip`. Serving then drops those not-yet-reached
/// records so a mid-sync prefix is returned instead of a permanent hole. If `start_index` is in
/// the union but not yet completed at this tip, the answer is an empty list, the same as asking
/// past the tip. If the union still does not contain `start_index`, the original result stands,
/// including its typed absent-band error.
fn subtrees_with_published_fallback<Node, Error>(
    stored: Result<BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<Node>>, Error>,
    start_index: NoteCommitmentSubtreeIndex,
    verified_tip: Option<block::Height>,
    merge_published: impl FnOnce() -> Option<
        BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<Node>>,
    >,
    check_available: impl FnOnce(
        &BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<Node>>,
    ) -> Result<(), Error>,
) -> Result<BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<Node>>, Error> {
    match stored {
        Ok(subtrees) if subtrees.contains_key(&start_index) => Ok(subtrees),
        result => {
            let Some(verified_tip) = verified_tip else {
                return result;
            };
            let Some(mut merged) = merge_published() else {
                return result;
            };

            check_available(&merged)?;
            let start_in_skip_band = merged.contains_key(&start_index);
            read::retain_subtrees_completed_at_or_below(&mut merged, verified_tip);
            let merged = read::contiguous_subtrees_from(merged, start_index);

            if merged.contains_key(&start_index) {
                Ok(merged)
            } else if start_in_skip_band {
                Ok(BTreeMap::new())
            } else {
                result
            }
        }
    }
}

/// A decoded frontier artifact waiting for the database-dependent coverage check.
struct LoadedHistoricalFrontierArtifact {
    cache: Arc<Mutex<read::HistoricalTreeCache>>,
    last_checkpoint: Option<block::Height>,
    source_path: Option<PathBuf>,
}

impl LoadedHistoricalFrontierArtifact {
    /// Discards a frontier grid that ends below this database's durable VCT handoff, leaving
    /// historical trees in the absent band unavailable instead of preventing startup.
    ///
    /// When the handoff marker has not been written yet, the grid is retained because its coverage
    /// cannot be checked. Serving checks again after ordinary fast-sync commits write the marker
    /// and reports the affected historical trees as unavailable if the grid is too old.
    fn discard_if_before_vct_handoff(
        self,
        config: &Config,
        db: &ZakuraDb,
    ) -> Arc<Mutex<read::HistoricalTreeCache>> {
        if config.derive_historical_trees(db.vct_synced_below().is_some()) {
            if let Some((artifact_checkpoint, vct_handoff)) =
                frontier_grid_ends_before_vct_handoff(self.last_checkpoint, db.vct_synced_below())
            {
                let source_path = self
                    .source_path
                    .expect("a loaded artifact has a source description");
                tracing::warn!(
                    path = ?source_path,
                    ?artifact_checkpoint,
                    ?vct_handoff,
                    "ignoring historical frontier artifact that does not cover the database's VCT \
                     handoff"
                );
                return Arc::new(Mutex::new(read::HistoricalTreeCache::default()));
            }
        }

        self.cache
    }
}

/// Returns the artifact checkpoint and database handoff when the published grid ends below this
/// database's skip band.
///
/// `None` means the comparison cannot be made yet — the grid is unloaded, or the durable last
/// checkpoint marker has not been written — or the grid already covers the band.
fn frontier_grid_ends_before_vct_handoff(
    artifact_checkpoint: Option<block::Height>,
    vct_handoff: Option<block::Height>,
) -> Option<(block::Height, block::Height)> {
    artifact_checkpoint
        .zip(vct_handoff)
        .filter(|(artifact_checkpoint, vct_handoff)| artifact_checkpoint < vct_handoff)
}

/// Returns the cold-request replay length when it exceeds `limit`.
///
/// Gaps are measured from genesis: a published grid is a chain-shaped artifact, so a file that
/// starts at a mid-chain `U` cannot serve a from-scratch node below `U`. `limit` is the same
/// bound serving applies per request, so a grid that passes here cannot produce a cold request
/// the read path would then refuse.
fn frontier_grid_gap_exceeds_replay_limit(
    artifact: &finalized_state::FrontierArtifact,
    limit: u64,
) -> Option<u64> {
    let blocks = artifact.max_cold_replay_blocks();
    (blocks > limit).then_some(blocks)
}

/// Loads the configured frontier grid override or the embedded Mainnet grid into a fresh cache.
///
/// Mainnet needs no deployment-time file: its reviewed grid is part of the binary. An explicit
/// path overrides that grid, and an unreadable or invalid override is fatal for a node that
/// derives ([`Config::derive_historical_trees`]) and a warning for one that does not. Networks
/// without an embedded or configured grid keep reporting the absent band as unavailable. A
/// well-framed artifact is still refused unless its entries tile genesis through
/// `last_checkpoint` at gaps of at most [`MAX_HISTORICAL_TREE_REPLAY_BLOCKS`].
fn load_historical_frontier_artifact(
    network: &Network,
    config: &Config,
    database_was_vct_fast_synced: bool,
) -> Result<LoadedHistoricalFrontierArtifact, StateInitError> {
    load_historical_frontier_artifact_if_enabled(
        network,
        config,
        config.derive_historical_trees(database_was_vct_fast_synced),
    )
}

fn load_historical_frontier_artifact_if_enabled(
    network: &Network,
    config: &Config,
    derivation_enabled: bool,
) -> Result<LoadedHistoricalFrontierArtifact, StateInitError> {
    let (artifact, source_path) = if let Some(path) = config.historical_frontier_artifact.as_ref() {
        (
            std::fs::read(path)
                .map_err(|error| Box::new(error) as BoxError)
                .and_then(|bytes| {
                    finalized_state::FrontierArtifact::decode(&bytes, network)
                        .map(Arc::new)
                        .map_err(|error| Box::new(error) as BoxError)
                }),
            path.clone(),
        )
    } else if derivation_enabled {
        let Some(artifact) = finalized_state::embedded_historical_frontier_artifact(network) else {
            tracing::info!(
                "historical tree derivation is idle: this network has no embedded historical \
                 frontier artifact"
            );

            return Ok(LoadedHistoricalFrontierArtifact {
                cache: Arc::new(Mutex::new(read::HistoricalTreeCache::default())),
                last_checkpoint: None,
                source_path: None,
            });
        };

        (
            Ok(artifact),
            PathBuf::from("embedded Mainnet historical frontier artifact"),
        )
    } else {
        return Ok(LoadedHistoricalFrontierArtifact {
            cache: Arc::new(Mutex::new(read::HistoricalTreeCache::default())),
            last_checkpoint: None,
            source_path: None,
        });
    };

    match artifact {
        Ok(artifact) => {
            if derivation_enabled {
                if let Some(blocks) = frontier_grid_gap_exceeds_replay_limit(
                    &artifact,
                    MAX_HISTORICAL_TREE_REPLAY_BLOCKS,
                ) {
                    return Err(StateInitError::HistoricalFrontierArtifactTooSparse {
                        path: source_path,
                        blocks,
                        limit: MAX_HISTORICAL_TREE_REPLAY_BLOCKS,
                    });
                }
            }

            tracing::info!(
                ?source_path,
                entries = artifact.entries.len(),
                spacing = artifact.spacing,
                "loaded historical frontier artifact"
            );
            Ok(LoadedHistoricalFrontierArtifact {
                last_checkpoint: Some(artifact.last_checkpoint),
                cache: Arc::new(Mutex::new(read::HistoricalTreeCache::with_artifact(
                    artifact,
                ))),
                source_path: Some(source_path),
            })
        }
        Err(source) if derivation_enabled => Err(StateInitError::HistoricalFrontierArtifact {
            path: source_path,
            source,
        }),
        Err(error) => {
            tracing::warn!(?source_path, %error, "ignoring historical frontier artifact");
            Ok(LoadedHistoricalFrontierArtifact {
                cache: Arc::new(Mutex::new(read::HistoricalTreeCache::default())),
                last_checkpoint: None,
                source_path: None,
            })
        }
    }
}

/// Derives the note commitment frontiers for `hash_or_height`, whose stored per-height trees are
/// absent because this is a verified-commitment-trees fast-synced database.
///
/// Callers reach this only once a tree read has already reported the absent band, so `unavailable`
/// is the error that stands if derivation is switched off or this node is pruned. Inside the band
/// the request either derives a root-checked frontier or fails: an absent tree there must never
/// reach a client as an empty treestate (see [`crate::HistoricalTreeUnavailable`]).
fn historical_frontiers(
    state: &ReadStateService,
    hash_or_height: HashOrHeight,
    unavailable: HistoricalTreeUnavailable,
) -> Result<Arc<read::DerivedFrontiers>, BoxError> {
    // Archive mode is part of this: replay needs every block body from the selected anchor through
    // the requested height, and pruned mode does not guarantee that range.
    if !state
        .db
        .config()
        .derive_historical_trees(state.db.vct_synced_below().is_some())
    {
        return Err(unavailable.into());
    }

    // Derivation anchors on the published grid. Without one the nearest anchor is the stored
    // frontier below the absent band, so a cold request would replay the band end to end — the
    // cost this design exists to avoid. Report the band as unavailable instead.
    if !state.has_usable_historical_frontier_grid() {
        return Err(unavailable.into());
    }

    let Some(height) = hash_or_height.height_or_else(|hash| state.db.height(hash)) else {
        // The absent-band check resolved this block to a height, so failing to resolve it again
        // means the database changed underneath the read. Report the original error rather than
        // an empty tree.
        return Err(unavailable.into());
    };

    read::derive_historical_frontiers(
        &state.db,
        &state.historical_trees,
        height,
        MAX_HISTORICAL_TREE_REPLAY_BLOCKS,
    )
    .map_err(BoxError::from)
}

fn block_roots_by_height_range<C>(
    chain: Option<C>,
    db: &ZakuraDb,
    header_chain: Option<&finalized_state::header_chain::HeaderChainReader>,
    start: block::Height,
    count: u32,
) -> Result<Vec<BlockCommitmentRoots>, HeaderChainStoreError>
where
    C: AsRef<Chain>,
{
    let capped_count = count.min(MAX_HEADER_SYNC_HEIGHT_RANGE);
    let end = (start + i64::from(capped_count.saturating_sub(1))).unwrap_or(start);
    let source = match db.vct_upgrade_height() {
        None => "trees",
        Some(upgrade) if start >= upgrade => "compact_index",
        Some(upgrade) if end < upgrade => "trees",
        Some(_) => "mixed",
    };
    metrics::counter!("state.block_roots.response", "source" => source).increment(1);
    let mut roots = Vec::new();
    let mut selected_aux_roots: Option<std::vec::IntoIter<BlockCommitmentRoots>> = None;

    for offset in 0..capped_count {
        let Some(height) = start + i64::from(offset) else {
            break;
        };

        let root = if let Some(selected_aux_roots) = selected_aux_roots.as_mut() {
            selected_aux_roots.next()
        } else if db
            .finalized_tip_height()
            .is_some_and(|finalized_tip| height <= finalized_tip)
        {
            finalized_state::serve_block_roots(db, height..=height)
                .into_iter()
                .next()
        } else if let Some(chain) = chain
            .as_ref()
            .map(|chain| chain.as_ref())
            .filter(|chain| chain.contains_block_height(height))
        {
            match (
                chain.sapling_tree(height.into()),
                chain.orchard_tree(height.into()),
                chain.ironwood_tree(height.into()),
            ) {
                (Some(sapling), Some(orchard), Some(ironwood)) => {
                    let (sapling_tx, orchard_tx, ironwood_tx, auth_data_root) = chain
                        .block(height.into())
                        .map(|block| {
                            (
                                block.block.sapling_transactions_count(),
                                block.block.orchard_transactions_count(),
                                block.block.ironwood_transactions_count(),
                                block.block.auth_data_root(),
                            )
                        })
                        .unwrap_or((
                            0,
                            0,
                            0,
                            zakura_chain::block::merkle::AuthDataRoot::from([0u8; 32]),
                        ));

                    Some(BlockCommitmentRoots {
                        height,
                        sapling_root: sapling.root(),
                        orchard_root: orchard.root(),
                        ironwood_root: ironwood.root(),
                        sapling_tx,
                        orchard_tx,
                        ironwood_tx,
                        auth_data_root,
                    })
                }
                _ => None,
            }
        } else {
            if selected_aux_roots.is_none() {
                let remaining = capped_count.saturating_sub(offset);
                selected_aux_roots = Some(
                    header_chain
                        .map(|reader| reader.selected_block_roots(height, remaining))
                        .transpose()?
                        .unwrap_or_default()
                        .into_iter(),
                );
            }
            selected_aux_roots.as_mut().and_then(Iterator::next)
        };

        let Some(root) = root else {
            break;
        };

        if root.height != height {
            break;
        }

        roots.push(root);
    }

    Ok(roots)
}

impl Service<ReadRequest> for ReadStateService {
    type Response = ReadResponse;
    type Error = BoxError;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if let Some(error) = self.block_write_failure.get() {
            return Poll::Ready(Err(Box::new(error.clone())));
        }

        // Check for panics in the block write task
        //
        // TODO: move into a check_for_panics() method
        if let Some(block_write_task) = self.block_write_task.take() {
            if block_write_task.is_finished() {
                match Arc::try_unwrap(block_write_task) {
                    Ok(block_write_task) => {
                        // This state owns the last task reference and can propagate any panic.
                        match block_write_task.join() {
                            Err(thread_panic) => std::panic::resume_unwind(thread_panic),
                            Ok(write::BlockWriteTaskExit::HeaderChainAttachmentFailed(error)) => {
                                return Poll::Ready(Err(Box::new(error)));
                            }
                            Ok(write::BlockWriteTaskExit::HeaderChainRuntimeFailed(error)) => {
                                return Poll::Ready(Err(Box::new(error)));
                            }
                            Ok(write::BlockWriteTaskExit::Completed) => {}
                        }
                    }
                    Err(block_write_task) => {
                        self.block_write_task = Some(block_write_task);
                    }
                }
            } else {
                // It hasn't finished, so we need to put it back
                self.block_write_task = Some(block_write_task);
            }
        }

        if let Some(error) = self.block_write_failure.get() {
            return Poll::Ready(Err(Box::new(error.clone())));
        }

        self.db.check_for_panics();

        Poll::Ready(Ok(()))
    }

    #[instrument(name = "read_state", skip(self, req))]
    fn call(&mut self, req: ReadRequest) -> Self::Future {
        req.count_metric();
        let timer = CodeTimer::start_desc(req.variant_name());
        let span = Span::current();
        let timed_span = TimedSpan::new(timer, span);
        let state = self.clone();

        if let ReadRequest::NonFinalizedBlocksListener { known_chain_tips } = req {
            // The non-finalized blocks listener is used to notify the state service
            // about new blocks that have been added to the non-finalized state.
            let non_finalized_blocks_listener = NonFinalizedBlocksListener::spawn(
                self.non_finalized_state_receiver.clone(),
                known_chain_tips,
            );

            return async move {
                Ok(ReadResponse::NonFinalizedBlocksListener(
                    non_finalized_blocks_listener,
                ))
            }
            .boxed();
        };

        let request_handler = move || match req {
            // Used by the `getblockchaininfo` RPC.
            ReadRequest::UsageInfo => Ok(ReadResponse::UsageInfo(state.db.cached_size())),

            // Used by the `getblockchaininfo` RPC.
            ReadRequest::PruningInfo => Ok(ReadResponse::PruningInfo {
                pruned: state.db.prunes_historical_data(),
                prune_height: state.db.prune_height(),
            }),

            // Used by the StateService.
            ReadRequest::Tip => Ok(ReadResponse::Tip(read::tip(
                state.latest_best_chain(),
                &state.db,
            ))),

            ReadRequest::FinalizedTip => Ok(ReadResponse::FinalizedTip(state.db.tip())),

            // Used by `getblockchaininfo` RPC method.
            ReadRequest::TipPoolValues => {
                let (tip_height, tip_hash, value_balance) =
                    read::tip_with_value_balance(state.latest_best_chain(), &state.db)?
                        .ok_or(BoxError::from("no chain tip available yet"))?;

                Ok(ReadResponse::TipPoolValues {
                    tip_height,
                    tip_hash,
                    value_balance,
                })
            }

            // Used by getblock
            ReadRequest::BlockInfo(hash_or_height) => Ok(ReadResponse::BlockInfo(
                read::block_info(state.latest_best_chain(), &state.db, hash_or_height),
            )),

            // Used by the StateService.
            ReadRequest::Depth(hash) => Ok(ReadResponse::Depth(read::depth(
                state.latest_best_chain(),
                &state.db,
                hash,
            ))),

            // Used by the StateService.
            ReadRequest::BestChainNextMedianTimePast => {
                Ok(ReadResponse::BestChainNextMedianTimePast(
                    read::next_median_time_past(&state.latest_non_finalized_state(), &state.db)?,
                ))
            }

            // Used by the get_block (raw) RPC and the StateService.
            ReadRequest::Block(hash_or_height) => Ok(ReadResponse::Block(read::block(
                state.latest_best_chain(),
                &state.db,
                hash_or_height,
            ))),

            ReadRequest::AnyChainBlock(hash_or_height) => Ok(ReadResponse::Block(read::any_block(
                state.latest_non_finalized_state().chain_iter(),
                &state.db,
                hash_or_height,
            ))),

            // Used by the get_block (raw) RPC and the StateService.
            ReadRequest::BlockAndSize(hash_or_height) => Ok(ReadResponse::BlockAndSize(
                read::block_and_size(state.latest_best_chain(), &state.db, hash_or_height),
            )),

            // Used by the get_block (verbose) RPC and the StateService.
            ReadRequest::BlockHeader(hash_or_height) => {
                let best_chain = state.latest_best_chain();

                let height = hash_or_height
                    .height_or_else(|hash| {
                        read::find::height_by_hash(best_chain.clone(), &state.db, hash)
                    })
                    .ok_or_else(|| BoxError::from("block hash or height not found"))?;

                let hash = hash_or_height
                    .hash_or_else(|height| {
                        read::find::hash_by_height(best_chain.clone(), &state.db, height)
                    })
                    .ok_or_else(|| BoxError::from("block hash or height not found"))?;

                let next_height = height.next()?;
                let next_block_hash =
                    read::find::hash_by_height(best_chain.clone(), &state.db, next_height);

                let header = read::block_header(best_chain, &state.db, height.into())
                    .ok_or_else(|| BoxError::from("block hash or height not found"))?;

                Ok(ReadResponse::BlockHeader {
                    header,
                    hash,
                    height,
                    next_block_hash,
                })
            }

            // For the get_raw_transaction RPC and the StateService.
            ReadRequest::Transaction(hash) => Ok(ReadResponse::Transaction(
                read::mined_transaction(state.latest_best_chain(), &state.db, hash),
            )),

            ReadRequest::AnyChainTransaction(hash) => {
                Ok(ReadResponse::AnyChainTransaction(read::any_transaction(
                    state.latest_non_finalized_state().chain_iter(),
                    &state.db,
                    hash,
                )))
            }

            // Used by the getblock (verbose) RPC.
            ReadRequest::TransactionIdsForBlock(hash_or_height) => Ok(
                ReadResponse::TransactionIdsForBlock(read::transaction_hashes_for_block(
                    state.latest_best_chain(),
                    &state.db,
                    hash_or_height,
                )),
            ),

            ReadRequest::AnyChainTransactionIdsForBlock(hash_or_height) => {
                Ok(ReadResponse::AnyChainTransactionIdsForBlock(
                    read::transaction_hashes_for_any_block(
                        state.latest_non_finalized_state().chain_iter(),
                        &state.db,
                        hash_or_height,
                    ),
                ))
            }

            #[cfg(feature = "indexer")]
            ReadRequest::SpendingTransactionId(spend) => Ok(ReadResponse::TransactionId(
                read::spending_transaction_hash(state.latest_best_chain(), &state.db, spend),
            )),

            ReadRequest::UnspentBestChainUtxo(outpoint) => Ok(ReadResponse::UnspentBestChainUtxo(
                read::unspent_utxo(state.latest_best_chain(), &state.db, outpoint),
            )),

            // Manually used by the StateService to implement part of AwaitUtxo.
            ReadRequest::AnyChainUtxo(outpoint) => Ok(ReadResponse::AnyChainUtxo(read::any_utxo(
                state.latest_non_finalized_state(),
                &state.db,
                outpoint,
            ))),

            // Used by the StateService.
            ReadRequest::BlockLocator => Ok(ReadResponse::BlockLocator(
                read::block_locator(state.latest_best_chain(), &state.db).unwrap_or_default(),
            )),

            // Used by the StateService.
            ReadRequest::FindBlockHashes { known_blocks, stop } => {
                Ok(ReadResponse::BlockHashes(read::find_chain_hashes(
                    state.latest_best_chain(),
                    &state.db,
                    known_blocks,
                    stop,
                    MAX_FIND_BLOCK_HASHES_RESULTS,
                )))
            }

            // Used by the StateService.
            ReadRequest::FindBlockHeaders { known_blocks, stop } => Ok(ReadResponse::BlockHeaders(
                read::find_chain_headers(
                    state.latest_best_chain(),
                    &state.db,
                    known_blocks,
                    stop,
                    MAX_FIND_BLOCK_HEADERS_RESULTS,
                )
                .into_iter()
                .map(|header| CountedHeader { header })
                .collect(),
            )),

            ReadRequest::HeaderLocator => {
                let reader = state.header_chain_reader_receiver.borrow().clone();
                let locator = reader
                    .map(|reader| reader.committed_selected_locator())
                    .transpose()?;
                Ok(ReadResponse::HeaderLocator(locator))
            }

            ReadRequest::HeaderValidationLease { parent_hash } => {
                let reader = state.header_chain_reader_receiver.borrow().clone();
                let lease = reader
                    .map(|reader| reader.validation_context(parent_hash))
                    .transpose()?
                    .flatten();
                Ok(ReadResponse::HeaderValidationLease(lease))
            }

            ReadRequest::VctRepairContext { owner, height } => {
                let reader = state.header_chain_reader_receiver.borrow().clone();
                let context = reader
                    .map(|reader| reader.vct_repair_context(owner, height))
                    .transpose()?
                    .flatten();
                Ok(ReadResponse::VctRepairContext(context))
            }

            ReadRequest::AcquireRetainedHeaderPath {
                peer,
                session_id,
                target_tip_hash,
                scope,
                locator_hashes,
            } => {
                let Some(reader) = state.header_chain_reader_receiver.borrow().clone() else {
                    return Ok(ReadResponse::RetainedHeaderPathLease(
                        crate::RetainedPathLeaseOutcome::TargetNotRetained,
                    ));
                };
                Ok(ReadResponse::RetainedHeaderPathLease(
                    reader.acquire_retained_path(
                        peer,
                        session_id,
                        target_tip_hash,
                        &locator_hashes,
                        scope,
                    )?,
                ))
            }

            ReadRequest::ReadRetainedHeaderPath {
                peer,
                session_id,
                lease_id,
                scope,
                after_hash,
                max_count,
            } => {
                let Some(reader) = state.header_chain_reader_receiver.borrow().clone() else {
                    return Ok(ReadResponse::RetainedHeaderPathPage(
                        crate::RetainedPathReadOutcome::Unavailable,
                    ));
                };
                Ok(ReadResponse::RetainedHeaderPathPage(
                    reader.read_retained_path(
                        peer, session_id, lease_id, scope, after_hash, max_count,
                    )?,
                ))
            }

            ReadRequest::ReleaseRetainedHeaderPath {
                peer,
                session_id,
                lease_id,
                scope,
            } => {
                let released = state
                    .header_chain_reader_receiver
                    .borrow()
                    .clone()
                    .map(|reader| reader.release_retained_path(peer, session_id, lease_id, scope))
                    .transpose()?
                    .unwrap_or(false);
                Ok(ReadResponse::RetainedHeaderPathReleased(released))
            }

            ReadRequest::BlockRoots {
                start_height,
                count,
            } => {
                let roots = if count == 0 {
                    Vec::new()
                } else {
                    block_roots_by_height_range(
                        state.latest_best_chain(),
                        &state.db,
                        state.header_chain_reader_receiver.borrow().as_ref(),
                        start_height,
                        count,
                    )?
                };

                Ok(ReadResponse::BlockRoots(roots))
            }

            ReadRequest::BestHeaderTip => {
                let header_chain_reader = state.header_chain_reader_receiver.borrow().clone();
                let tip = match header_chain_reader {
                    Some(reader) => reader
                        .selected_tip()
                        .map(|tip| Some((tip.height, tip.hash)))?,
                    None => read::tip(state.latest_best_chain(), &state.db),
                };
                Ok(ReadResponse::BestHeaderTip(tip))
            }

            ReadRequest::HeaderChainSnapshot => Ok(ReadResponse::HeaderChainSnapshot(
                state.header_chain_snapshot_receiver.borrow().clone(),
            )),

            ReadRequest::MissingBlockBodyMetadata { from, limit } => {
                let reader = state.header_chain_reader_receiver.borrow().clone();
                Ok(ReadResponse::MissingBlockBodyMetadata(
                    missing_block_body_metadata(
                        || state.latest_best_chain(),
                        &state.db,
                        reader.as_ref(),
                        from,
                        limit,
                    )?,
                ))
            }

            ReadRequest::BlocksByHeightRange { start, count } => {
                let best_chain = state.latest_best_chain();
                let blocks = (0..count)
                    .map_while(|offset| {
                        start
                            .0
                            .checked_add(offset)
                            .map(block::Height)
                            .and_then(|height| {
                                read::block_and_size(best_chain.clone(), &state.db, height.into())
                                    .map(|(block, size)| (height, block, size))
                            })
                    })
                    .collect();

                Ok(ReadResponse::Blocks(blocks))
            }

            // Used by the indexer gRPC server.
            #[cfg(feature = "indexer")]
            ReadRequest::RawBlocksByHeightRange { start, count } => {
                let blocks = (0..count)
                    .map_while(|offset| {
                        start
                            .0
                            .checked_add(offset)
                            .map(block::Height)
                            .and_then(|height| {
                                state
                                    .db
                                    .raw_block_bytes(height.into())
                                    .map(|bytes| (height, bytes))
                            })
                    })
                    .collect();

                Ok(ReadResponse::RawBlocks(blocks))
            }

            ReadRequest::SaplingTree(hash_or_height) => {
                let tree = match read::sapling_tree(
                    state.latest_best_chain(),
                    &state.db,
                    hash_or_height,
                ) {
                    Ok(tree) => tree,
                    Err(unavailable) => Some(
                        historical_frontiers(&state, hash_or_height, unavailable)?
                            .sapling
                            .clone(),
                    ),
                };
                Ok(ReadResponse::SaplingTree(tree))
            }

            ReadRequest::OrchardTree(hash_or_height) => {
                let tree = match read::orchard_tree(
                    state.latest_best_chain(),
                    &state.db,
                    hash_or_height,
                ) {
                    Ok(tree) => tree,
                    Err(unavailable) => Some(
                        historical_frontiers(&state, hash_or_height, unavailable)?
                            .orchard
                            .clone(),
                    ),
                };
                Ok(ReadResponse::OrchardTree(tree))
            }

            ReadRequest::IronwoodTree(hash_or_height) => {
                let tree =
                    match read::ironwood_tree(state.latest_best_chain(), &state.db, hash_or_height)
                    {
                        Ok(tree) => tree,
                        Err(unavailable) => Some(
                            historical_frontiers(&state, hash_or_height, unavailable)?
                                .ironwood
                                .clone(),
                        ),
                    };
                Ok(ReadResponse::IronwoodTree(tree))
            }

            ReadRequest::SaplingSubtrees { start_index, limit } => {
                let end_index = limit
                    .and_then(|limit| start_index.0.checked_add(limit.0))
                    .map(NoteCommitmentSubtreeIndex);

                let best_chain = state.latest_best_chain();
                let verified_tip = read::tip_height(best_chain.clone(), &state.db);
                let sapling_subtrees = if let Some(end_index) = end_index {
                    read::sapling_subtrees(best_chain.clone(), &state.db, start_index..end_index)
                } else {
                    // If there is no end bound, just return all the trees.
                    // If the end bound would overflow, just returns all the trees, because that's what
                    // `zcashd` does. (It never calculates an end bound, so it just keeps iterating until
                    // the trees run out.)
                    read::sapling_subtrees(best_chain.clone(), &state.db, start_index..)
                };

                let sapling_subtrees = subtrees_with_published_fallback(
                    sapling_subtrees,
                    start_index,
                    verified_tip,
                    || {
                        state.historical_subtrees_at_last_checkpoint().map(
                            |(artifact, vct_applied_below)| {
                                let range = range_for(start_index, end_index);
                                let mut merged =
                                    read::sapling_subtrees_with_gaps(best_chain, &state.db, range);
                                read::merge_published_subtrees(
                                    &mut merged,
                                    artifact.sapling_range(range),
                                    vct_applied_below,
                                );
                                merged
                            },
                        )
                    },
                    |merged| {
                        read::check_historical_sapling_subtrees_available(
                            &state.db,
                            start_index,
                            end_index,
                            merged,
                        )
                    },
                )?;

                Ok(ReadResponse::SaplingSubtrees(sapling_subtrees))
            }

            ReadRequest::OrchardSubtrees { start_index, limit } => {
                let end_index = limit
                    .and_then(|limit| start_index.0.checked_add(limit.0))
                    .map(NoteCommitmentSubtreeIndex);

                let best_chain = state.latest_best_chain();
                let verified_tip = read::tip_height(best_chain.clone(), &state.db);
                let orchard_subtrees = if let Some(end_index) = end_index {
                    read::orchard_subtrees(best_chain.clone(), &state.db, start_index..end_index)
                } else {
                    // If there is no end bound, just return all the trees.
                    // If the end bound would overflow, just returns all the trees, because that's what
                    // `zcashd` does. (It never calculates an end bound, so it just keeps iterating until
                    // the trees run out.)
                    read::orchard_subtrees(best_chain.clone(), &state.db, start_index..)
                };

                let orchard_subtrees = subtrees_with_published_fallback(
                    orchard_subtrees,
                    start_index,
                    verified_tip,
                    || {
                        state.historical_subtrees_at_last_checkpoint().map(
                            |(artifact, vct_applied_below)| {
                                let range = range_for(start_index, end_index);
                                let mut merged =
                                    read::orchard_subtrees_with_gaps(best_chain, &state.db, range);
                                read::merge_published_subtrees(
                                    &mut merged,
                                    artifact.orchard_range(range),
                                    vct_applied_below,
                                );
                                merged
                            },
                        )
                    },
                    |merged| {
                        read::check_historical_orchard_subtrees_available(
                            &state.db,
                            start_index,
                            end_index,
                            merged,
                        )
                    },
                )?;

                Ok(ReadResponse::OrchardSubtrees(orchard_subtrees))
            }

            ReadRequest::IronwoodSubtrees { start_index, limit } => {
                let end_index = limit
                    .and_then(|limit| start_index.0.checked_add(limit.0))
                    .map(NoteCommitmentSubtreeIndex);

                let best_chain = state.latest_best_chain();
                let verified_tip = read::tip_height(best_chain.clone(), &state.db);
                let ironwood_subtrees = if let Some(end_index) = end_index {
                    read::ironwood_subtrees(best_chain.clone(), &state.db, start_index..end_index)
                } else {
                    read::ironwood_subtrees(best_chain.clone(), &state.db, start_index..)
                };

                let ironwood_subtrees = subtrees_with_published_fallback(
                    ironwood_subtrees,
                    start_index,
                    verified_tip,
                    || {
                        state.historical_subtrees_at_last_checkpoint().map(
                            |(artifact, vct_applied_below)| {
                                let range = range_for(start_index, end_index);
                                let mut merged =
                                    read::ironwood_subtrees_with_gaps(best_chain, &state.db, range);
                                read::merge_published_subtrees(
                                    &mut merged,
                                    artifact.ironwood_range(range),
                                    vct_applied_below,
                                );
                                merged
                            },
                        )
                    },
                    |merged| {
                        read::check_historical_ironwood_subtrees_available(
                            &state.db,
                            start_index,
                            end_index,
                            merged,
                        )
                    },
                )?;

                Ok(ReadResponse::IronwoodSubtrees(ironwood_subtrees))
            }

            // For the get_address_balance RPC.
            ReadRequest::AddressBalance(addresses) => {
                let (balance, received) =
                    read::transparent_balance(state.latest_best_chain(), &state.db, addresses)?;
                Ok(ReadResponse::AddressBalance { balance, received })
            }

            // For the get_address_tx_ids RPC.
            ReadRequest::TransactionIdsByAddresses {
                addresses,
                height_range,
            } => read::transparent_tx_ids(
                state.latest_best_chain(),
                &state.db,
                addresses,
                height_range,
            )
            .map(ReadResponse::AddressesTransactionIds),

            // For the get_address_utxos RPC.
            ReadRequest::UtxosByAddresses(addresses) => read::address_utxos(
                &state.network,
                state.latest_best_chain(),
                &state.db,
                addresses,
            )
            .map(ReadResponse::AddressUtxos),

            ReadRequest::CheckBestChainTipNullifiersAndAnchors(unmined_tx) => {
                let latest_non_finalized_best_chain = state.latest_best_chain();

                check::nullifier::tx_no_duplicates_in_chain(
                    &state.db,
                    latest_non_finalized_best_chain.as_ref(),
                    unmined_tx.transaction(),
                )?;

                check::anchors::tx_anchors_refer_to_final_treestates(
                    &state.db,
                    latest_non_finalized_best_chain.as_ref(),
                    &unmined_tx,
                )?;

                Ok(ReadResponse::ValidBestChainTipNullifiersAndAnchors)
            }

            // Used by the get_block and get_block_hash RPCs.
            ReadRequest::BestChainBlockHash(height) => Ok(ReadResponse::BlockHash(
                read::hash_by_height(state.latest_best_chain(), &state.db, height),
            )),

            // Used by get_block_template and getblockchaininfo RPCs.
            ReadRequest::ChainInfo => {
                // # Correctness
                //
                // It is ok to do these lookups using multiple database calls. Finalized state updates
                // can only add overlapping blocks, and block hashes are unique across all chain forks.
                //
                // If there is a large overlap between the non-finalized and finalized states,
                // where the finalized tip is above the non-finalized tip,
                // Zebra is receiving a lot of blocks, or this request has been delayed for a long time.
                //
                // In that case, the `getblocktemplate` RPC will return an error because Zebra
                // is not synced to the tip. That check happens before the RPC makes this request.
                read::difficulty::get_block_template_chain_info(
                    &state.latest_non_finalized_state(),
                    &state.db,
                    &state.network,
                )
                .map(ReadResponse::ChainInfo)
            }

            // Used by getmininginfo, getnetworksolps, and getnetworkhashps RPCs.
            ReadRequest::SolutionRate { num_blocks, height } => {
                let latest_non_finalized_state = state.latest_non_finalized_state();
                // # Correctness
                //
                // It is ok to do these lookups using multiple database calls. Finalized state updates
                // can only add overlapping blocks, and block hashes are unique across all chain forks.
                //
                // The worst that can happen here is that the default `start_hash` will be below
                // the chain tip.
                let (tip_height, tip_hash) =
                    match read::tip(latest_non_finalized_state.best_chain(), &state.db) {
                        Some(tip_hash) => tip_hash,
                        None => return Ok(ReadResponse::SolutionRate(None)),
                    };

                let start_hash = match height {
                    Some(height) if height < tip_height => read::hash_by_height(
                        latest_non_finalized_state.best_chain(),
                        &state.db,
                        height,
                    ),
                    // use the chain tip hash if height is above it or not provided.
                    _ => Some(tip_hash),
                };

                let solution_rate = start_hash.and_then(|start_hash| {
                    read::difficulty::solution_rate(
                        &latest_non_finalized_state,
                        &state.db,
                        num_blocks,
                        start_hash,
                    )
                });

                Ok(ReadResponse::SolutionRate(solution_rate))
            }

            ReadRequest::CheckBlockProposalValidity(semantically_verified) => {
                tracing::debug!(
                    "attempting to validate and commit block proposal \
                         onto a cloned non-finalized state"
                );
                let mut latest_non_finalized_state = state.latest_non_finalized_state();

                // The previous block of a valid proposal must be on the best chain tip.
                let Some((_best_tip_height, best_tip_hash)) =
                    read::best_tip(&latest_non_finalized_state, &state.db)
                else {
                    return Err(
                        "state is empty: wait for Zakura to sync before submitting a proposal"
                            .into(),
                    );
                };

                if semantically_verified.block.header.previous_block_hash != best_tip_hash {
                    return Err("proposal is not based on the current best chain tip: \
                                    previous block hash must be the best chain tip"
                        .into());
                }

                // This clone of the non-finalized state is dropped when this closure returns.
                // The non-finalized state that's used in the rest of the state (including finalizing
                // blocks into the db) is not mutated here.
                //
                // TODO: Convert `CommitSemanticallyVerifiedError` to a new `ValidateProposalError`?
                latest_non_finalized_state.disable_metrics();

                write::validate_and_commit_non_finalized(
                    &state.db,
                    &mut latest_non_finalized_state,
                    semantically_verified,
                )?;

                Ok(ReadResponse::ValidBlockProposal)
            }

            ReadRequest::TipBlockSize => {
                // Respond with the length of the obtained block if any.
                Ok(ReadResponse::TipBlockSize(
                    state
                        .best_tip()
                        .and_then(|(tip_height, _)| {
                            read::block_info(
                                state.latest_best_chain(),
                                &state.db,
                                tip_height.into(),
                            )
                        })
                        .map(|info| info.size().try_into().expect("u32 should fit in usize"))
                        .or_else(|| {
                            find::tip_block(state.latest_best_chain(), &state.db)
                                .map(|b| b.zcash_serialized_size())
                        }),
                ))
            }

            // Used by the getchaintips RPC.
            ReadRequest::ChainTips => {
                // Capture the header tip and its overlap with the block chain from
                // one transition generation, so the two agree. The overlap stops at
                // the block tip: the fork is never above it, and headers-first sync
                // leaves tens of thousands of headers above it that would be copied
                // and searched for nothing.
                let header_chain_reader = state.header_chain_reader_receiver.borrow().clone();
                let (non_finalized_state, header_tip, overlap) = match header_chain_reader {
                    Some(reader) => {
                        let (non_finalized_state, header_tip, overlap) = reader
                            .with_selected_overlap(
                                || state.latest_non_finalized_state(),
                                |non_finalized_state| {
                                    read::tip_height(non_finalized_state.best_chain(), &state.db)
                                },
                            )?;
                        (non_finalized_state, Some(header_tip), overlap)
                    }
                    None => (state.latest_non_finalized_state(), None, Vec::new()),
                };

                Ok(ReadResponse::ChainTips(read::chain_tips(
                    &non_finalized_state,
                    &state.db,
                    header_tip.map(|tip| read::SelectedHeaders {
                        tip,
                        overlap: &overlap,
                    }),
                )))
            }

            ReadRequest::NonFinalizedBlocksListener { .. } => {
                unreachable!("should return early");
            }

            // Used by `gettxout` RPC method.
            ReadRequest::IsTransparentOutputSpent(outpoint) => {
                let is_spent = read::unspent_utxo(state.latest_best_chain(), &state.db, outpoint);
                Ok(ReadResponse::IsTransparentOutputSpent(is_spent.is_none()))
            }
        };

        timed_span.spawn_blocking(request_handler)
    }
}

/// Initialize a state service from the provided [`Config`].
/// Returns a boxed state service, a read-only state service,
/// and receivers for state chain tip updates.
///
/// Each `network` has its own separate on-disk database.
///
/// The state uses the `max_checkpoint_height` and `checkpoint_verify_concurrency_limit`
/// to work out when it is near the final checkpoint.
///
/// To share access to the state, wrap the returned service in a `Buffer`,
/// or clone the returned [`ReadStateService`].
///
/// It's possible to construct multiple state services in the same application (as
/// long as they, e.g., use different storage locations), but doing so is
/// probably not what you want.
///
/// # Errors
///
/// Returns a [`StateInitError`] if historical tree derivation is misconfigured or its frontier
/// artifact cannot be loaded.
pub async fn init(
    config: Config,
    network: &Network,
    max_checkpoint_height: block::Height,
    checkpoint_verify_concurrency_limit: usize,
) -> Result<
    (
        BoxService<Request, Response, BoxError>,
        ReadStateService,
        LatestChainTip,
        ChainTipChange,
    ),
    StateInitError,
> {
    let (state_service, read_only_state_service, latest_chain_tip, chain_tip_change) =
        StateService::new(
            config,
            network,
            max_checkpoint_height,
            checkpoint_verify_concurrency_limit,
        )
        .await?;

    Ok((
        BoxService::new(state_service),
        read_only_state_service,
        latest_chain_tip,
        chain_tip_change,
    ))
}

/// Initialize state and return the separate capability used to seal completion-gated body
/// evidence before it enters the general-purpose state request service.
///
/// # Errors
///
/// Returns a [`StateInitError`] if historical tree derivation is misconfigured or its frontier
/// artifact cannot be loaded.
pub async fn init_with_header_chain_body_evidence(
    config: Config,
    network: &Network,
    max_checkpoint_height: block::Height,
    checkpoint_verify_concurrency_limit: usize,
) -> Result<
    (
        BoxService<Request, Response, BoxError>,
        ReadStateService,
        LatestChainTip,
        ChainTipChange,
        crate::HeaderChainBodyEvidenceAuthority,
    ),
    StateInitError,
> {
    let (state, read_state, latest_chain_tip, chain_tip_change) = init(
        config,
        network,
        max_checkpoint_height,
        checkpoint_verify_concurrency_limit,
    )
    .await?;
    Ok((
        state,
        read_state,
        latest_chain_tip,
        chain_tip_change,
        crate::HeaderChainBodyEvidenceAuthority::new(),
    ))
}

/// Initialize a read state service from the provided [`Config`].
/// Returns a read-only state service,
///
/// Each `network` has its own separate on-disk database.
///
/// To share access to the state, clone the returned [`ReadStateService`].
pub fn init_read_only(
    config: Config,
    network: &Network,
) -> Result<
    (
        ReadStateService,
        ZakuraDb,
        tokio::sync::watch::Sender<NonFinalizedState>,
    ),
    StateInitError,
> {
    let finalized_state = FinalizedState::new_with_debug(&config, network, true, true)?;
    let historical_trees = load_historical_frontier_artifact(
        network,
        &config,
        finalized_state.db.vct_synced_below().is_some(),
    )?;
    let historical_trees =
        historical_trees.discard_if_before_vct_handoff(&config, &finalized_state.db);
    let (non_finalized_state_sender, non_finalized_state_receiver) =
        tokio::sync::watch::channel(NonFinalizedState::new(network));
    let (_vct_root_repair_sender, vct_root_repair_receiver) =
        tokio::sync::watch::channel(VctRootRepairStatus::default());
    let (_header_chain_snapshot_sender, header_chain_snapshot_receiver) =
        tokio::sync::watch::channel(None);
    let (_header_chain_view_sender, header_chain_view_receiver) = tokio::sync::watch::channel(None);
    let (_header_runtime_status_sender, header_runtime_status_receiver) =
        tokio::sync::watch::channel(
            zakura_node_services::sync_lifecycle::HeaderRuntimeStatus::Detached {
                epoch: zakura_node_services::sync_lifecycle::LifecycleEpoch::INITIAL,
                reason: zakura_node_services::sync_lifecycle::HeaderRuntimeDetachedReason::AwaitingSemanticHandoff,
            },
        );
    let (_header_chain_reader_sender, header_chain_reader_receiver) =
        tokio::sync::watch::channel(None);

    Ok((
        ReadStateService::new(
            &finalized_state,
            None,
            Arc::new(OnceLock::new()),
            WatchReceiver::new(non_finalized_state_receiver),
            vct_root_repair_receiver,
            HeaderChainSubscriptions {
                snapshots: header_chain_snapshot_receiver,
                views: header_chain_view_receiver,
                runtime_status: header_runtime_status_receiver,
                reader: header_chain_reader_receiver,
            },
            historical_trees,
        ),
        finalized_state.db.clone(),
        non_finalized_state_sender,
    ))
}

/// Calls [`init_read_only`] with the provided [`Config`] and [`Network`] from a blocking task.
///
/// Returns a [`tokio::task::JoinHandle`] whose output is a [`Result`]: awaiting it yields a
/// [`JoinError`](tokio::task::JoinError) if the blocking task panicked or was cancelled, and
/// otherwise an `Err(`[`StateInitError`]`)` if the read-only state could not be opened (for
/// example, a missing read-only database).
pub fn spawn_init_read_only(
    config: Config,
    network: &Network,
) -> tokio::task::JoinHandle<
    Result<
        (
            ReadStateService,
            ZakuraDb,
            tokio::sync::watch::Sender<NonFinalizedState>,
        ),
        StateInitError,
    >,
> {
    let network = network.clone();
    tokio::task::spawn_blocking(move || init_read_only(config, &network))
}

/// Returns a [`StateService`] with an ephemeral [`Config`] and a buffer with a single slot.
///
/// This can be used to create a state service for testing. See also [`init`].
#[cfg(any(test, feature = "proptest-impl"))]
pub async fn init_test(
    network: &Network,
) -> Buffer<BoxService<Request, Response, BoxError>, Request> {
    // TODO: pass max_checkpoint_height and checkpoint_verify_concurrency limit
    //       if we ever need to test final checkpoint sent UTXO queries
    let (state_service, _, _, _) =
        StateService::new(Config::ephemeral(), network, block::Height::MAX, 0)
            .await
            .expect("ephemeral state initialization succeeds");

    Buffer::new(BoxService::new(state_service), 1)
}

/// Initializes a state service with an ephemeral [`Config`] and a buffer with a single slot,
/// then returns the read-write service, read-only service, and tip watch channels.
///
/// This can be used to create a state service for testing. See also [`init`].
#[cfg(any(test, feature = "proptest-impl"))]
pub async fn init_test_services(
    network: &Network,
) -> (
    Buffer<BoxService<Request, Response, BoxError>, Request>,
    ReadStateService,
    LatestChainTip,
    ChainTipChange,
) {
    // TODO: pass max_checkpoint_height and checkpoint_verify_concurrency limit
    //       if we ever need to test final checkpoint sent UTXO queries
    let (state_service, read_state_service, latest_chain_tip, chain_tip_change) =
        StateService::new(Config::ephemeral(), network, block::Height::MAX, 0)
            .await
            .expect("ephemeral state initialization succeeds");

    let state_service = Buffer::new(BoxService::new(state_service), 1);

    (
        state_service,
        read_state_service,
        latest_chain_tip,
        chain_tip_change,
    )
}
