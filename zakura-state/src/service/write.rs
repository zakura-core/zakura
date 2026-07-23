//! Writing blocks to the finalized and non-finalized states.

use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use indexmap::IndexMap;
use tokio::sync::{
    mpsc::{error::TryRecvError, UnboundedReceiver, UnboundedSender},
    oneshot, watch,
};

use tracing::Span;
use zakura_chain::{
    block::{self, Height},
    parallel::{commitment_aux::BlockCommitmentRoots, tree::NoteCommitmentTrees},
};

use crate::{
    constants::MAX_BLOCK_REORG_HEIGHT,
    error::CommitHeaderRangeError,
    service::{
        check,
        finalized_state::{
            FinalizedState, HighestCompletedCheckpoint, HighestCompletedCheckpointTracker, ZakuraDb,
        },
        non_finalized_state::NonFinalizedState,
        queued_blocks::{QueuedCheckpointVerified, QueuedSemanticallyVerified},
        ChainTipBlock, ChainTipSender, InvalidateError, ReconsiderError,
    },
    CheckpointVerifiedBlock, SemanticallyVerifiedBlock, ValidateContextError,
};

// These types are used in doc links
#[allow(unused_imports)]
use crate::service::{
    chain_tip::{ChainTipChange, LatestChainTip},
    non_finalized_state::Chain,
};

mod vct_write;

use vct_write::VctWriteManager;

/// Status published by the finalized write loop when a VCT fast-sync height needs a
/// replacement supplied root.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct VctRootRepairStatus {
    /// The state of the current root repair need.
    pub state: VctRootRepairState,
    /// Monotonic generation for repair attempts. A new generation means the previous
    /// replacement candidate was absent or rejected and the networking layer should try
    /// another bounded repair candidate.
    pub generation: u64,
}

impl Default for VctRootRepairStatus {
    fn default() -> Self {
        Self {
            state: VctRootRepairState::Idle,
            generation: 0,
        }
    }
}

/// Dependency-neutral VCT root repair state.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum VctRootRepairState {
    /// No VCT root repair is currently required.
    Idle,
    /// The finalized writer cannot commit this height until a verifiable supplied root is
    /// re-delivered through header sync.
    Unavailable {
        /// Height whose supplied roots are missing from the VCT source.
        height: block::Height,
    },
}

/// The maximum size of the parent error map.
///
/// We allow enough space for multiple concurrent chain forks with errors.
const PARENT_ERROR_MAP_LIMIT: usize = MAX_BLOCK_REORG_HEIGHT as usize * 2;

/// Run contextual validation on the prepared block and add it to the
/// non-finalized state if it is contextually valid.
#[tracing::instrument(
    level = "debug",
    skip(finalized_state, non_finalized_state, prepared),
    fields(
        height = ?prepared.height,
        hash = %prepared.hash,
        chains = non_finalized_state.chain_count()
    )
)]
pub(crate) fn validate_and_commit_non_finalized(
    finalized_state: &ZakuraDb,
    non_finalized_state: &mut NonFinalizedState,
    prepared: SemanticallyVerifiedBlock,
) -> Result<(), ValidateContextError> {
    check::initial_contextual_validity(finalized_state, non_finalized_state, &prepared)?;
    let parent_hash = prepared.block.header.previous_block_hash;

    if finalized_state.finalized_tip_hash() == parent_hash {
        non_finalized_state.commit_new_chain(prepared, finalized_state)?;
    } else {
        non_finalized_state.commit_block(prepared, finalized_state)?;
    }

    Ok(())
}

/// Update the [`LatestChainTip`], [`ChainTipChange`], and `non_finalized_state_sender`
/// channels with the latest non-finalized [`ChainTipBlock`] and
/// [`Chain`].
///
/// `last_zebra_mined_log_height` is used to rate-limit logging.
///
/// If `backup_dir_path` is `Some`, the non-finalized state is written to the backup
/// directory before updating the channels.
///
/// Returns the latest non-finalized chain tip height.
///
/// # Panics
///
/// If the `non_finalized_state` is empty.
#[instrument(
    level = "debug",
    skip(
        non_finalized_state,
        chain_tip_sender,
        non_finalized_state_sender,
        backup_dir_path,
    ),
    fields(chains = non_finalized_state.chain_count())
)]
fn update_latest_chain_channels(
    non_finalized_state: &NonFinalizedState,
    chain_tip_sender: &mut ChainTipSender,
    non_finalized_state_sender: &watch::Sender<NonFinalizedState>,
    backup_dir_path: Option<&Path>,
) -> block::Height {
    let best_chain = non_finalized_state.best_chain().expect("unexpected empty non-finalized state: must commit at least one block before updating channels");

    let tip_block = best_chain
        .tip_block()
        .expect("unexpected empty chain: must commit at least one block before updating channels")
        .clone();
    let tip_block = ChainTipBlock::from(tip_block);

    let tip_block_height = tip_block.height;

    if let Some(backup_dir_path) = backup_dir_path {
        non_finalized_state.write_to_backup(backup_dir_path);
    }

    // If the final receiver was just dropped, ignore the error.
    let _ = non_finalized_state_sender.send(non_finalized_state.clone());

    chain_tip_sender.set_best_non_finalized_tip(tip_block);

    tip_block_height
}

/// Update the latest-chain channels after an invalidate or reconsider request.
///
/// Root invalidation can legitimately leave the non-finalized state empty. In
/// that case, publish the empty state and switch the shared tip back to the
/// finalized database tip.
fn update_latest_chain_channels_after_invalidate_or_reconsider(
    finalized_state: &FinalizedState,
    non_finalized_state: &NonFinalizedState,
    chain_tip_sender: &mut ChainTipSender,
    non_finalized_state_sender: &watch::Sender<NonFinalizedState>,
    backup_dir_path: Option<&Path>,
) {
    if !non_finalized_state.is_chain_set_empty() {
        update_latest_chain_channels(
            non_finalized_state,
            chain_tip_sender,
            non_finalized_state_sender,
            backup_dir_path,
        );
        return;
    }

    if let Some(backup_dir_path) = backup_dir_path {
        non_finalized_state.write_to_backup(backup_dir_path);
    }

    // If the final receiver was just dropped, ignore the error.
    let _ = non_finalized_state_sender.send(non_finalized_state.clone());

    let finalized_tip = finalized_state
        .db
        .tip_block()
        .map(CheckpointVerifiedBlock::from)
        .map(ChainTipBlock::from);
    chain_tip_sender.reset_to_finalized_tip(finalized_tip);
}

fn commit_header_range(
    finalized_state: &FinalizedState,
    completed_checkpoint: &mut HighestCompletedCheckpointTracker,
    anchor: block::Hash,
    headers: Vec<Arc<block::Header>>,
    body_sizes: Vec<u32>,
    tree_aux_roots: Vec<BlockCommitmentRoots>,
    rsp_tx: oneshot::Sender<Result<block::Hash, CommitHeaderRangeError>>,
) {
    if let Err(height) =
        completed_checkpoint.check_immutable_conflicts(&finalized_state.db, anchor, &headers)
    {
        let _ = rsp_tx.send(Err(CommitHeaderRangeError::ImmutableConflict { height }));
        return;
    }

    let mut batch = crate::service::finalized_state::DiskWriteBatch::new();
    let result = batch
        .prepare_header_range_batch_with_roots(
            &finalized_state.db,
            anchor,
            &headers,
            &body_sizes,
            &tree_aux_roots,
        )
        .and_then(|hash| {
            let proposed = completed_checkpoint.propose_after_headers(
                &finalized_state.db,
                anchor,
                &headers,
            )?;
            finalized_state
                .db
                .write_batch(batch)
                .map(|()| {
                    completed_checkpoint.commit_success(proposed);
                    hash
                })
                .map_err(|error| {
                    tracing::error!(?error, "failed to write validated header range");

                    CommitHeaderRangeError::StorageWriteError {
                        error: error.to_string(),
                    }
                })
        });

    let _ = rsp_tx.send(result);
}

/// A worker task that reads, validates, and writes blocks to the
/// `finalized_state` or `non_finalized_state`.
struct WriteBlockWorkerTask {
    finalized_block_write_receiver: UnboundedReceiver<QueuedCheckpointVerified>,
    non_finalized_block_write_receiver: UnboundedReceiver<NonFinalizedWriteMessage>,
    finalized_state: FinalizedState,
    non_finalized_state: NonFinalizedState,
    seed_zakura_header_from_best_chain_commits: bool,
    invalid_block_reset_sender: UnboundedSender<block::Hash>,
    /// Signals the [`crate::service::StateService`] that a non-finalized block was rejected by
    /// the write task, so its hash should be removed from
    /// `non_finalized_block_write_sent_hashes`.
    ///
    /// Without this, a rejected same-hash block locks out a later honest
    /// re-delivery of a block at the same hash as a "duplicate" until restart
    /// or reorg.
    non_finalized_rejected_sender: UnboundedSender<block::Hash>,
    chain_tip_sender: ChainTipSender,
    non_finalized_state_sender: watch::Sender<NonFinalizedState>,
    highest_completed_checkpoint: HighestCompletedCheckpointTracker,
    vct_root_repair_sender: watch::Sender<VctRootRepairStatus>,
    /// If `Some`, the non-finalized state is written to this backup directory
    /// synchronously before each channel update, instead of via the async backup task.
    backup_dir_path: Option<PathBuf>,
}

/// The message type for the non-finalized block write task channel.
pub enum NonFinalizedWriteMessage {
    /// A newly downloaded and semantically verified block prepared for
    /// contextual validation and insertion into the non-finalized state.
    Commit(QueuedSemanticallyVerified),
    /// A validated header range prepared for contextual storage checks and
    /// insertion into the durable header store.
    CommitHeaderRange {
        anchor: block::Hash,
        headers: Vec<Arc<block::Header>>,
        body_sizes: Vec<u32>,
        tree_aux_roots: Vec<BlockCommitmentRoots>,
        rsp_tx: oneshot::Sender<Result<block::Hash, CommitHeaderRangeError>>,
    },
    /// The hash of a block that should be invalidated and removed from
    /// the non-finalized state, if present.
    Invalidate {
        hash: block::Hash,
        rsp_tx: oneshot::Sender<Result<block::Hash, InvalidateError>>,
    },
    /// The hash of a block that was previously invalidated but should be
    /// reconsidered and reinserted into the non-finalized state.
    Reconsider {
        hash: block::Hash,
        rsp_tx: oneshot::Sender<Result<Vec<block::Hash>, ReconsiderError>>,
    },
}

impl From<QueuedSemanticallyVerified> for NonFinalizedWriteMessage {
    fn from(block: QueuedSemanticallyVerified) -> Self {
        NonFinalizedWriteMessage::Commit(block)
    }
}

/// A worker with a task that reads, validates, and writes blocks to the
/// `finalized_state` or `non_finalized_state` and channels for sending
/// it blocks.
#[derive(Clone, Debug)]
pub struct BlockWriteSender {
    /// A channel to send blocks to the `block_write_task`,
    /// so they can be written to the [`NonFinalizedState`].
    pub non_finalized: Option<tokio::sync::mpsc::UnboundedSender<NonFinalizedWriteMessage>>,

    /// A channel to send blocks to the `block_write_task`,
    /// so they can be written to the [`FinalizedState`].
    ///
    /// This sender is dropped after the state has finished sending all the checkpointed blocks,
    /// and the lowest semantically verified block arrives.
    pub finalized: Option<tokio::sync::mpsc::UnboundedSender<QueuedCheckpointVerified>>,
}

impl BlockWriteSender {
    /// Creates a new [`BlockWriteSender`] with the given receivers and states.
    #[instrument(
        level = "debug",
        skip_all,
        fields(
            network = %non_finalized_state.network
        )
    )]
    pub fn spawn(
        finalized_state: FinalizedState,
        non_finalized_state: NonFinalizedState,
        chain_tip_sender: ChainTipSender,
        non_finalized_state_sender: watch::Sender<NonFinalizedState>,
        should_use_finalized_block_write_sender: bool,
        backup_dir_path: Option<PathBuf>,
    ) -> (
        Self,
        tokio::sync::mpsc::UnboundedReceiver<block::Hash>,
        tokio::sync::mpsc::UnboundedReceiver<block::Hash>,
        watch::Receiver<Option<HighestCompletedCheckpoint>>,
        watch::Receiver<VctRootRepairStatus>,
        Option<Arc<std::thread::JoinHandle<()>>>,
    ) {
        // Security: The number of blocks in these channels is limited by
        //           the syncer and inbound lookahead limits.
        let (non_finalized_block_write_sender, non_finalized_block_write_receiver) =
            tokio::sync::mpsc::unbounded_channel();
        let (finalized_block_write_sender, finalized_block_write_receiver) =
            tokio::sync::mpsc::unbounded_channel();
        let (invalid_block_reset_sender, invalid_block_write_reset_receiver) =
            tokio::sync::mpsc::unbounded_channel();
        let (non_finalized_rejected_sender, non_finalized_rejected_receiver) =
            tokio::sync::mpsc::unbounded_channel();
        let (vct_root_repair_sender, vct_root_repair_receiver) =
            watch::channel(VctRootRepairStatus::default());
        let (highest_completed_checkpoint, highest_completed_checkpoint_receiver) =
            HighestCompletedCheckpointTracker::open(&finalized_state.db);

        let seed_zakura_header_from_best_chain_commits = finalized_state
            .db
            .config()
            .enable_zakura_header_seed_from_committed_blocks;

        let span = Span::current();
        let task = std::thread::spawn(move || {
            span.in_scope(|| {
                WriteBlockWorkerTask {
                    finalized_block_write_receiver,
                    non_finalized_block_write_receiver,
                    finalized_state,
                    non_finalized_state,
                    seed_zakura_header_from_best_chain_commits,
                    invalid_block_reset_sender,
                    non_finalized_rejected_sender,
                    chain_tip_sender,
                    non_finalized_state_sender,
                    highest_completed_checkpoint,
                    vct_root_repair_sender,
                    backup_dir_path,
                }
                .run()
            })
        });

        (
            Self {
                non_finalized: Some(non_finalized_block_write_sender),
                finalized: should_use_finalized_block_write_sender
                    .then_some(finalized_block_write_sender),
            },
            invalid_block_write_reset_receiver,
            non_finalized_rejected_receiver,
            highest_completed_checkpoint_receiver,
            vct_root_repair_receiver,
            Some(Arc::new(task)),
        )
    }
}

impl WriteBlockWorkerTask {
    /// Reads blocks from the channels, writes them to the `finalized_state` or `non_finalized_state`,
    /// sends any errors on the `invalid_block_reset_sender`, then updates the `chain_tip_sender` and
    /// `non_finalized_state_sender`.
    #[instrument(
        level = "debug",
        skip(self),
        fields(
            network = %self.non_finalized_state.network
        )
    )]
    pub fn run(mut self) {
        let Self {
            finalized_block_write_receiver,
            non_finalized_block_write_receiver,
            finalized_state,
            non_finalized_state,
            invalid_block_reset_sender,
            non_finalized_rejected_sender,
            chain_tip_sender,
            non_finalized_state_sender,
            highest_completed_checkpoint,
            vct_root_repair_sender,
            seed_zakura_header_from_best_chain_commits,
            backup_dir_path,
        } = &mut self;

        let mut prev_finalized_note_commitment_trees: Option<NoteCommitmentTrees> = None;
        let mut deferred_non_finalized_messages = VecDeque::new();

        // Look-ahead buffering and root-stall tracking for the VCT fast-sync
        // checkpoint path. See [`VctWriteManager`].
        let mut vct_write_manager = VctWriteManager::new(vct_root_repair_sender.clone());

        // Write all the finalized blocks sent by the state,
        // until the state closes the finalized block channel's sender.
        loop {
            match non_finalized_block_write_receiver.try_recv() {
                Ok(NonFinalizedWriteMessage::CommitHeaderRange {
                    anchor,
                    headers,
                    body_sizes,
                    tree_aux_roots,
                    rsp_tx,
                }) => {
                    commit_header_range(
                        finalized_state,
                        highest_completed_checkpoint,
                        anchor,
                        headers,
                        body_sizes,
                        tree_aux_roots,
                        rsp_tx,
                    );
                    continue;
                }
                Ok(msg) => deferred_non_finalized_messages.push_back(msg),
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => {}
            }

            let ordered_block = match vct_write_manager.take_ready() {
                Some(block) => block,
                None => match finalized_block_write_receiver.try_recv() {
                    Ok(block) => block,
                    Err(TryRecvError::Empty) => {
                        std::thread::park_timeout(Duration::from_millis(10));
                        continue;
                    }
                    Err(TryRecvError::Disconnected) => break,
                },
            };

            // TODO: split these checks into separate functions

            if invalid_block_reset_sender.is_closed() {
                info!("StateService closed the block reset channel. Is Zakura shutting down?");
                return;
            }

            // Discard any children of invalid blocks in the channel
            //
            // `commit_finalized()` requires blocks in height order.
            // So if there has been a block commit error,
            // we need to drop all the descendants of that block,
            // until we receive a block at the required next height.
            let next_valid_height = finalized_state
                .db
                .finalized_tip_height()
                .map(|height| (height + 1).expect("committed heights are valid"))
                .unwrap_or(Height(0));

            if ordered_block.0.height != next_valid_height {
                debug!(
                    ?next_valid_height,
                    invalid_height = ?ordered_block.0.height,
                    invalid_hash = ?ordered_block.0.hash,
                    "got a block that was the wrong height. \
                     Assuming a parent block failed, and dropping this block",
                );

                // The pipeline is broken; drop any look-ahead so commit resumes
                // from the real finalized tip.
                vct_write_manager.reset(finalized_state);

                // We don't want to send a reset here, because it could overwrite a valid sent hash
                std::mem::drop(ordered_block);
                continue;
            }

            // Peek the next block so VCT fast commits can verify the current
            // block's supplied roots against the successor's header.
            vct_write_manager.fill_successor(finalized_block_write_receiver, &ordered_block);

            // Fast VCT commits use the already-validated Zakura header store as their
            // successor witness. A checkpoint-verified body is not sufficient: NU5+
            // block hashes do not bind authorizing data, so an altered same-hash body
            // could supply the wrong auth-data root and make a valid current root look
            // invalid. The buffered body remains in the look-ahead for its own commit.
            let needs_vct_successor =
                finalized_state.vct_fast_needs_successor(ordered_block.0.height);
            let next_vct_block = if needs_vct_successor {
                finalized_state
                    .vct_successor_from_header_store(ordered_block.0.height, ordered_block.0.hash)
            } else {
                None
            };

            if needs_vct_successor && next_vct_block.is_none() {
                let height = ordered_block.0.height;
                let wait =
                    vct_write_manager.on_retryable_error(height, false, false, ordered_block);
                std::thread::park_timeout(wait);
                continue;
            }

            // The successor header authenticates the current block's supplied roots.
            // Header-sync stores its ZIP-244 auth-data root alongside the contextually
            // validated header, so this check does not require the successor body.
            let prev_note_commitment_trees = prev_finalized_note_commitment_trees.take();
            let prev_note_commitment_trees_for_retry = prev_note_commitment_trees.clone();

            let next_block_took_vct_path =
                finalized_state.vct_fast_will_apply(ordered_block.0.height);

            // Try committing the block
            match finalized_state.commit_finalized(
                ordered_block,
                prev_note_commitment_trees,
                next_vct_block,
            ) {
                Ok((finalized, note_commitment_trees)) => {
                    // Whether this successful commit consumed header-carried
                    // tree-aux roots to skip the note-commitment frontier rebuild.
                    if next_block_took_vct_path {
                        metrics::counter!("state.vct.fast_path.hit").increment(1);
                    } else {
                        metrics::counter!("state.vct.fast_path.miss").increment(1);
                    }

                    // A successful commit clears any VCT root stall: log recovery and reset
                    // the stalled-height gauge if it had been raised.
                    vct_write_manager.on_commit_success();

                    // Publish the tip before checkpoint rebind. `commit_finalized`
                    // already answered the oneshot, so tip waiters can race this
                    // path; rebind must not delay chain-tip notification.
                    let tip_block = ChainTipBlock::from(finalized);
                    prev_finalized_note_commitment_trees = Some(note_commitment_trees);
                    chain_tip_sender.set_finalized_tip(tip_block);

                    if let Err(error) =
                        highest_completed_checkpoint.rebind_from_db(&finalized_state.db)
                    {
                        tracing::warn!(
                            ?error,
                            "failed to refresh highest completed checkpoint after finalized block commit"
                        );
                    }
                }
                Err((ordered_block, error)) => {
                    // Retryable VCT root stalls (an absent/evicted root, or one not yet
                    // verifiable for lack of a stored successor header) park-and-retry the same
                    // block in place rather than resetting the queue. An absent root can only
                    // be filled by a re-delivery of its header range (roots are not
                    // individually re-requested), so it polls slowly; an await-successor
                    // stall just waits for the next header to be stored, so it polls faster.
                    if let Some(height) = error.vct_retryable_height() {
                        let root_unavailable = error.vct_supplied_root_unavailable_height();

                        prev_finalized_note_commitment_trees = prev_note_commitment_trees_for_retry;
                        let wait = vct_write_manager.on_retryable_error(
                            height,
                            root_unavailable.is_some(),
                            next_block_took_vct_path,
                            ordered_block,
                        );
                        std::thread::park_timeout(wait);
                        continue;
                    }

                    let finalized_tip = finalized_state.db.tip();
                    let _ = ordered_block.1.send(Err(error.clone()));

                    // The commit failed and the queue is being reset, so clear
                    // any buffered look-ahead block.
                    vct_write_manager.reset(finalized_state);

                    // The last block in the queue failed, so we can't commit the next block.
                    // Instead, we need to reset the state queue,
                    // and discard any children of the invalid block in the channel.
                    info!(
                        ?error,
                        last_valid_height = ?finalized_tip.map(|tip| tip.0),
                        last_valid_hash = ?finalized_tip.map(|tip| tip.1),
                        "committing a block to the finalized state failed, resetting state queue",
                    );

                    let send_result =
                        invalid_block_reset_sender.send(finalized_state.db.finalized_tip_hash());

                    if send_result.is_err() {
                        info!(
                            "StateService closed the block reset channel. Is Zakura shutting down?"
                        );
                        return;
                    }
                }
            }
        }

        // Do this check even if the channel got closed before any finalized blocks were sent.
        // This can happen if we're past the finalized tip.
        if invalid_block_reset_sender.is_closed() {
            info!("StateService closed the block reset channel. Is Zakura shutting down?");
            return;
        }

        // Save any errors to propagate down to queued child blocks
        let mut parent_error_map: IndexMap<block::Hash, ValidateContextError> = IndexMap::new();

        while let Some(msg) = deferred_non_finalized_messages
            .pop_front()
            .or_else(|| non_finalized_block_write_receiver.blocking_recv())
        {
            let queued_child_and_rsp_tx = match msg {
                NonFinalizedWriteMessage::Commit(queued_child) => Some(queued_child),
                NonFinalizedWriteMessage::CommitHeaderRange {
                    anchor,
                    headers,
                    body_sizes,
                    tree_aux_roots,
                    rsp_tx,
                } => {
                    commit_header_range(
                        finalized_state,
                        highest_completed_checkpoint,
                        anchor,
                        headers,
                        body_sizes,
                        tree_aux_roots,
                        rsp_tx,
                    );
                    continue;
                }
                NonFinalizedWriteMessage::Invalidate { hash, rsp_tx } => {
                    tracing::info!(?hash, "invalidating a block in the non-finalized state");
                    let _ = rsp_tx.send(non_finalized_state.invalidate_block(hash));
                    None
                }
                NonFinalizedWriteMessage::Reconsider { hash, rsp_tx } => {
                    tracing::info!(?hash, "reconsidering a block in the non-finalized state");
                    let _ = rsp_tx
                        .send(non_finalized_state.reconsider_block(hash, &finalized_state.db));
                    None
                }
            };

            let Some((queued_child, rsp_tx)) = queued_child_and_rsp_tx else {
                update_latest_chain_channels_after_invalidate_or_reconsider(
                    finalized_state,
                    non_finalized_state,
                    chain_tip_sender,
                    non_finalized_state_sender,
                    backup_dir_path.as_deref(),
                );
                continue;
            };

            let child_hash = queued_child.hash;
            let parent_hash = queued_child.block.header.previous_block_hash;
            let child_height = queued_child.height;
            let child_block = queued_child.block.clone();
            let parent_error = parent_error_map.get(&parent_hash);

            // If the parent block was marked as rejected, also reject all its children.
            //
            // At this point, we know that all the block's descendants
            // are invalid, because we checked all the consensus rules before
            // committing the failing ancestor block to the non-finalized state.
            let result = if let Some(parent_error) = parent_error {
                Err(parent_error.clone())
            } else {
                tracing::trace!(?child_hash, "validating queued child");
                validate_and_commit_non_finalized(
                    &finalized_state.db,
                    non_finalized_state,
                    queued_child,
                )
            };

            // TODO: fix the test timing bugs that require the result to be sent
            //       after `update_latest_chain_channels()`,
            //       and send the result on rsp_tx here

            if let Err(ref error) = result {
                // If the block is invalid, mark any descendant blocks as rejected.
                parent_error_map.insert(child_hash, error.clone());

                // Make sure the error map doesn't get too big.
                if parent_error_map.len() > PARENT_ERROR_MAP_LIMIT {
                    // We only add one hash at a time, so we only need to remove one extra here.
                    parent_error_map.shift_remove_index(0);
                }

                // Signal the StateService to drop this hash from
                // `non_finalized_block_write_sent_hashes`, so a subsequent
                // re-delivery of a block at the same hash is not short-circuited
                // as a "duplicate" against a rejected variant that never reached
                // any chain.
                //
                // If the receiver was dropped (the StateService is shutting
                // down), ignore the error: the lockout cannot matter once the
                // service exits.
                let _ = non_finalized_rejected_sender.send(child_hash);

                // Update the caller with the error.
                let _ = rsp_tx.send(result.map(|()| child_hash).map_err(Into::into));

                // Skip the things we only need to do for successfully committed blocks
                continue;
            }

            // A successfully committed block supersedes any contextual error
            // recorded for a different block body with the same header hash.
            parent_error_map.shift_remove(&child_hash);

            if should_seed_zakura_header_from_non_finalized_commit(
                *seed_zakura_header_from_best_chain_commits,
                non_finalized_state,
                child_height,
                child_hash,
            ) {
                seed_zakura_header_from_committed_block(
                    &finalized_state.db,
                    highest_completed_checkpoint,
                    child_height,
                    &child_block,
                );
            }

            // Committing blocks to the finalized state keeps the same chain,
            // so we can update the chain seen by the rest of the application now.
            //
            // TODO: if this causes state request errors due to chain conflicts,
            //       fix the `service::read` bugs,
            //       or do the channel update after the finalized state commit
            let tip_block_height = update_latest_chain_channels(
                non_finalized_state,
                chain_tip_sender,
                non_finalized_state_sender,
                backup_dir_path.as_deref(),
            );

            // Update the caller with the result.
            let _ = rsp_tx.send(result.map(|()| child_hash).map_err(Into::into));

            while non_finalized_state
                .best_chain_len()
                .expect("just successfully inserted a non-finalized block above")
                > MAX_BLOCK_REORG_HEIGHT
            {
                tracing::trace!("finalizing block past the reorg limit");
                let contextually_verified_with_trees = non_finalized_state.finalize();
                prev_finalized_note_commitment_trees = finalized_state
                    .commit_finalized_direct(
                        contextually_verified_with_trees,
                        prev_finalized_note_commitment_trees.take(),
                        None,
                        "commit contextually-verified request",
                    )
                    .expect(
                        "unexpected finalized block commit error: note commitment and history trees were already checked by the non-finalized state",
                    )
                    .1
                    .into();
                if let Err(error) = highest_completed_checkpoint.rebind_from_db(&finalized_state.db)
                {
                    tracing::warn!(
                        ?error,
                        "failed to refresh highest completed checkpoint after finalized block commit"
                    );
                }
            }

            // Update the metrics if semantic and contextual validation passes
            //
            // TODO: split this out into a function?
            metrics::counter!("state.full_verifier.committed.block.count").increment(1);
            metrics::counter!("zcash.chain.verified.block.total").increment(1);

            metrics::gauge!("state.full_verifier.committed.block.height")
                .set(tip_block_height.0 as f64);

            // This height gauge is updated for both fully verified and checkpoint blocks.
            // These updates can't conflict, because this block write task makes sure that blocks
            // are committed in order.
            metrics::gauge!("zcash.chain.verified.block.height").set(tip_block_height.0 as f64);

            tracing::trace!("finished processing queued block");
        }

        // We're finished receiving non-finalized blocks from the state, and
        // done writing to the finalized state, so we can force it to shut down.
        finalized_state.db.shutdown(true);
        std::mem::drop(self.finalized_state);
    }
}

fn seed_zakura_header_from_committed_block(
    finalized_state: &ZakuraDb,
    highest_completed_checkpoint: &mut HighestCompletedCheckpointTracker,
    height: block::Height,
    block: &Arc<block::Block>,
) {
    match finalized_state.seed_zakura_header_from_committed_block(height, block) {
        Ok(()) => {
            if let Err(error) = highest_completed_checkpoint.rebind_from_db(finalized_state) {
                tracing::warn!(
                    ?error,
                    "failed to refresh highest completed checkpoint after seeding a header"
                );
            }
            tracing::trace!(?height, hash = ?block.hash(), "seeded Zakura header from committed block");
        }
        Err(error) => {
            tracing::warn!(
                ?height,
                hash = ?block.hash(),
                ?error,
                "failed to seed Zakura header from committed block"
            );
        }
    }
}

fn should_seed_zakura_header_from_non_finalized_commit(
    enabled: bool,
    non_finalized_state: &NonFinalizedState,
    height: block::Height,
    hash: block::Hash,
) -> bool {
    enabled && non_finalized_state.best_tip() == Some((height, hash))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use zakura_chain::{
        chain_tip::ChainTip, parameters::Network, serialization::ZcashDeserializeInto,
        value_balance::ValueBalance,
    };

    use crate::{
        arbitrary::Prepare,
        service::{
            finalized_state::{
                DiskWriteBatch, FinalizedState, HighestCompletedCheckpointTracker, WriteDisk,
            },
            non_finalized_state::NonFinalizedState,
            write::{
                seed_zakura_header_from_committed_block,
                should_seed_zakura_header_from_non_finalized_commit,
                update_latest_chain_channels_after_invalidate_or_reconsider,
            },
            ChainTipBlock, ChainTipSender,
        },
        tests::{setup::new_state_with_mainnet_genesis, FakeChainHelper},
        Config,
    };

    #[test]
    fn empty_non_finalized_state_updates_channels_with_finalized_tip() {
        let _init_guard = zakura_test::init();

        let network = Network::Mainnet;
        let (finalized_state, non_finalized_state, finalized_tip) =
            new_state_with_mainnet_genesis();
        let finalized_tip = ChainTipBlock::from(finalized_tip);
        let non_finalized_tip = ChainTipBlock::from(
            finalized_state
                .db
                .tip_block()
                .expect("finalized genesis block should exist")
                .make_fake_child()
                .prepare(),
        );
        let (mut chain_tip_sender, latest_chain_tip, _chain_tip_change) =
            ChainTipSender::new(finalized_tip.clone(), &network);
        chain_tip_sender.set_best_non_finalized_tip(non_finalized_tip.clone());
        assert_eq!(
            latest_chain_tip.best_tip_height_and_hash(),
            Some((non_finalized_tip.height, non_finalized_tip.hash)),
        );

        let (non_finalized_state_sender, non_finalized_state_receiver) =
            tokio::sync::watch::channel(non_finalized_state.clone());
        assert!(!non_finalized_state_receiver.has_changed().unwrap());

        update_latest_chain_channels_after_invalidate_or_reconsider(
            &finalized_state,
            &non_finalized_state,
            &mut chain_tip_sender,
            &non_finalized_state_sender,
            None,
        );

        assert!(non_finalized_state_receiver.has_changed().unwrap());
        assert!(non_finalized_state_receiver.borrow().is_chain_set_empty());
        assert_eq!(
            latest_chain_tip.best_tip_height_and_hash(),
            Some((finalized_tip.height, finalized_tip.hash)),
        );
    }

    #[test]
    fn side_chain_commit_does_not_seed_zakura_headers() {
        let _init_guard = zakura_test::init();

        let network = Network::Mainnet;
        let mut config = Config::ephemeral();
        config.enable_zakura_header_seed_from_committed_blocks = true;
        let finalized_state = FinalizedState::new(
            &config,
            &network,
            #[cfg(feature = "elasticsearch")]
            false,
        )
        .expect("opening an ephemeral database should succeed");
        finalized_state.set_finalized_value_pool(ValueBalance::fake_populated_pool());

        let parent = zakura_test::vectors::BLOCK_MAINNET_434873_BYTES
            .zcash_deserialize_into::<Arc<zakura_chain::block::Block>>()
            .expect("block deserializes");
        let best_block = parent.make_fake_child().set_work(10);
        let side_block = parent.make_fake_child().set_work(1);
        let best_height = best_block
            .coinbase_height()
            .expect("fake child block has a coinbase height");

        let mut non_finalized_state = NonFinalizedState::new(&network);
        let (mut completed_checkpoint, _receiver) =
            HighestCompletedCheckpointTracker::open(&finalized_state.db);

        // The seed path refuses rows that do not link to the stored header row
        // below them, and the fake chain's parent block is not otherwise
        // committed to this state, so store its hash as a provisional Zakura
        // row (the consensus `hash_by_height` row cannot be written alone: a
        // finalized tip implies note commitment trees exist).
        let parent_height = parent
            .coinbase_height()
            .expect("test vector block has a coinbase height");
        let zakura_hash_by_height = finalized_state
            .db
            .db()
            .cf_handle("zakura_header_hash_by_height")
            .unwrap();
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(&zakura_hash_by_height, parent_height, parent.hash());
        finalized_state
            .db
            .db()
            .write(batch)
            .expect("parent hash row writes");

        non_finalized_state
            .commit_new_chain(best_block.clone().prepare(), &finalized_state)
            .expect("best block commits to a new chain");
        assert!(should_seed_zakura_header_from_non_finalized_commit(
            true,
            &non_finalized_state,
            best_height,
            best_block.hash(),
        ));
        seed_zakura_header_from_committed_block(
            &finalized_state.db,
            &mut completed_checkpoint,
            best_height,
            &best_block,
        );

        non_finalized_state
            .commit_new_chain(side_block.clone().prepare(), &finalized_state)
            .expect("side block commits to a losing fork");
        assert!(!should_seed_zakura_header_from_non_finalized_commit(
            true,
            &non_finalized_state,
            best_height,
            side_block.hash(),
        ));

        assert_eq!(
            finalized_state.db.best_header_tip(),
            Some((best_height, best_block.hash()))
        );
        assert_eq!(
            finalized_state.db.headers_by_height_range(best_height, 1),
            vec![(best_height, best_block.hash(), best_block.header.clone())],
        );
    }
}
