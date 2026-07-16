//! The primary implementation of the `zakura_state::Service` built upon rocksdb.
//!
//! Zebra's database is implemented in 4 layers:
//! - [`FinalizedState`]: queues, validates, and commits blocks, using...
//! - [`ZakuraDb`]: reads and writes [`zakura_chain`] types to the state database, using...
//! - [`DiskDb`]: reads and writes generic types to any column family in the database, using...
//! - [`disk_format`]: converts types to raw database bytes.
//!
//! These layers allow us to split [`zakura_chain`] types for efficient database storage.
//! They reduce the risk of data corruption bugs, runtime inconsistencies, and panics.
//!
//! # Correctness
//!
//! [`crate::constants::state_database_format_version_in_code()`] must be incremented
//! each time the database format (column, serialization, etc) changes.

use std::{
    io::{stderr, stdout, Write},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, LazyLock,
    },
};

use zakura_chain::{
    block, ironwood, orchard,
    parallel::tree::NoteCommitmentTrees,
    parameters::{Network, NetworkUpgrade},
    sapling,
};
use zakura_db::{
    block::{RetentionPlan, ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT},
    chain::BLOCK_INFO,
    transparent::{BALANCE_BY_TRANSPARENT_ADDR, TX_LOC_BY_SPENT_OUT_LOC},
};

use crate::{
    constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
    error::CommitCheckpointVerifiedError,
    request::{FinalizableBlock, FinalizedBlock, Treestate},
    service::{check, QueuedCheckpointVerified},
    CheckpointVerifiedBlock, Config, StateInitError, ValidateContextError,
};

/// Times `$body` and records its duration to the named histogram when the
/// `commit-metrics` feature is enabled; otherwise just evaluates `$body` with
/// zero overhead. Used to profile the finalized-state commit phases.
macro_rules! timed_commit_phase {
    ($name:expr, $body:expr) => {{
        #[cfg(feature = "commit-metrics")]
        let _start = std::time::Instant::now();
        let result = $body;
        #[cfg(feature = "commit-metrics")]
        metrics::histogram!($name).record(_start.elapsed().as_secs_f64());
        result
    }};
}

/// A dedicated rayon thread pool for checkpoint-commit treestate computation. Namely:
/// - the note-commitment tree update
/// - the ZIP-244 auth-data-root commitment leaf-hashes
///
/// These are the two dominant compute-steps during commit heavy shielded blocks.
///
/// This isolates the commit-compute phase from the main thread pool, allowing it
/// to keep making progress when download/verify work is busy.
static COMMIT_COMPUTE_POOL: LazyLock<rayon::ThreadPool> = LazyLock::new(|| {
    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|i| format!("commit-compute-{i}"))
        .build()
        .expect("rayon thread pool configuration is valid")
});

pub mod column_family;

pub(crate) mod commitment_aux;
pub(crate) mod commitment_aux_verify;
mod disk_db;
mod disk_format;
mod vct;
mod zakura_db;

use vct::{VctCommitState, VctState, VctWriteData};

#[cfg(any(test, feature = "proptest-impl"))]
mod arbitrary;

#[cfg(test)]
mod tests;

#[allow(unused_imports)]
pub use column_family::{TypedColumnFamily, WriteTypedBatch};
pub(crate) use commitment_aux::serve_block_roots;
pub use commitment_aux::{produce_final_frontiers_bytes, FinalFrontiersGenerationError};
#[allow(unused_imports)]
pub use disk_db::{DiskDb, DiskWriteBatch, ReadDisk, WriteDisk};
#[allow(unused_imports)]
pub use disk_format::{
    FromDisk, IntoDisk, OutputLocation, RawBytes, TransactionIndex, TransactionLocation,
    MAX_ON_DISK_HEIGHT,
};
pub use vct::{validate_final_frontiers_bytes, FinalFrontiersValidationError, NextVctBlock};
pub use zakura_db::ZakuraDb;

#[cfg(any(test, feature = "proptest-impl"))]
pub use disk_format::KV;

pub use disk_format::upgrade::restorable_db_versions;
pub use zakura_db::prune::{
    preview_prune_finalized_state, prune_finalized_state, PruneFinalizedStateError,
    PruneFinalizedStateOptions, PruneFinalizedStateSummary,
};
pub use zakura_db::rollback::{
    preview_rollback_finalized_state, rollback_finalized_state, RollbackBackupSummary,
    RollbackFinalizedStateError, RollbackFinalizedStateOptions, RollbackFinalizedStateSummary,
};

/// The column families supported by the running `zakura-state` database code.
///
/// Existing column families that aren't listed here are preserved when the database is opened.
pub const STATE_COLUMN_FAMILIES_IN_CODE: &[&str] = &[
    // Blocks
    "hash_by_height",
    "height_by_hash",
    "block_header_by_height",
    // Header sync
    "zakura_header_hash_by_height",
    "zakura_header_height_by_hash",
    "zakura_header_by_height",
    ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT,
    // Transactions
    "tx_by_loc",
    "hash_by_tx_loc",
    "tx_loc_by_hash",
    // Transparent
    BALANCE_BY_TRANSPARENT_ADDR,
    "tx_loc_by_transparent_addr_loc",
    "utxo_by_out_loc",
    "utxo_loc_by_transparent_addr_loc",
    TX_LOC_BY_SPENT_OUT_LOC,
    // Sprout
    "sprout_nullifiers",
    "sprout_anchors",
    "sprout_note_commitment_tree",
    // Sapling
    "sapling_nullifiers",
    "sapling_anchors",
    "sapling_note_commitment_tree",
    "sapling_note_commitment_subtree",
    // Orchard
    "orchard_nullifiers",
    "orchard_anchors",
    "orchard_note_commitment_tree",
    "orchard_note_commitment_subtree",
    // Ironwood
    "ironwood_nullifiers",
    "ironwood_anchors",
    "ironwood_note_commitment_tree",
    "ironwood_note_commitment_subtree",
    // Chain
    "history_tree",
    "tip_chain_value_pool",
    BLOCK_INFO,
    // Verified-commitment-trees serving index
    COMMITMENT_ROOTS_BY_HEIGHT,
    // Storage policy
    PRUNING_METADATA,
    VCT_SYNC_METADATA,
    VCT_UPGRADE_METADATA,
];

/// The name of the column family that records pruning progress.
///
/// In pruned storage mode this holds a single entry, keyed by the unit value
/// `()`, mapping to the next block height managed by online pruning. The
/// presence of this entry marks the database as pruned, which is a one-way state:
/// a pruned database cannot be reopened in archive mode.
pub const PRUNING_METADATA: &str = "pruning_metadata";

/// The name of the column family that marks a verified-commitment-trees
/// (vct) synced database.
///
/// A vct-synced database skips historical per-height note-commitment tree
/// writes below the checkpoint handoff height. This column family holds a
/// single entry with that handoff height.
pub const VCT_SYNC_METADATA: &str = "vct_sync_metadata";

/// The name of the column family that records the verified-commitment-trees
/// upgrade height.
///
/// This height is the first block committed by code that writes the
/// [`COMMITMENT_ROOTS_BY_HEIGHT`] serving index.
pub const VCT_UPGRADE_METADATA: &str = "vct_upgrade_metadata";

/// The name of the column family holding per-height Sapling/Orchard
/// note-commitment roots, keyed by [`block::Height`].
pub const COMMITMENT_ROOTS_BY_HEIGHT: &str = "commitment_roots_by_height";

/// The finalized part of the chain state, stored in the db.
///
/// `rocksdb` allows concurrent writes through a shared reference,
/// so clones of the finalized state represent the same database instance.
/// When the final clone is dropped, the database is closed.
///
/// This is different from `NonFinalizedState::clone()`,
/// which returns an independent copy of the chains.
#[derive(Clone, Debug)]
pub struct FinalizedState {
    // Configuration
    //
    // This configuration cannot be modified after the database is initialized,
    // because some clones would have different values.
    //
    /// The configured stop height.
    ///
    /// Commit blocks to the finalized state up to this height, then exit Zebra.
    debug_stop_at_height: Option<block::Height>,

    /// The lowest checkpoint-verified block height whose raw transaction bytes
    /// should be retained during checkpoint sync in pruned mode.
    checkpoint_raw_tx_retention_start: Option<block::Height>,

    /// `true` if raw transactions from an archive-mode sync may still exist
    /// before `checkpoint_raw_tx_retention_start`.
    ///
    /// Shared via `Arc<AtomicBool>` because [`FinalizedState`] is `Clone` and the
    /// commit path mutates this flag (clearing it once the archive backlog is
    /// drained), so per the shared-state invariant below it must be shared across
    /// clones rather than an owned `bool`.
    checkpoint_raw_tx_archive_backlog: Arc<AtomicBool>,

    // Owned State
    //
    // Everything contained in this state must be shared by all clones, or read-only.
    //
    /// The underlying database.
    ///
    /// `rocksdb` allows reads and writes via a shared reference,
    /// so this database object can be freely cloned.
    /// The last instance that is dropped will close the underlying database.
    pub db: ZakuraDb,

    #[cfg(feature = "elasticsearch")]
    /// The elasticsearch handle.
    pub elastic_db: Option<elasticsearch::Elasticsearch>,

    #[cfg(feature = "elasticsearch")]
    /// A collection of blocks to be sent to elasticsearch as a bulk.
    pub elastic_blocks: Vec<String>,

    /// Commit-time verified-commitment-trees state.
    vct: VctCommitState,
}

impl FinalizedState {
    /// Returns an on-disk database instance for `config`, `network`, and `elastic_db`.
    /// If there is no existing database, creates a new database on disk.
    pub fn new(
        config: &Config,
        network: &Network,
        #[cfg(feature = "elasticsearch")] enable_elastic_db: bool,
    ) -> Result<Self, StateInitError> {
        Self::new_with_debug(
            config,
            network,
            false,
            #[cfg(feature = "elasticsearch")]
            enable_elastic_db,
            false,
        )
    }

    /// Opens (or creates) the on-disk finalized state database read-write, for
    /// offline tooling (e.g. the replay benchmark).
    ///
    /// Equivalent to [`FinalizedState::new`] but without the `elasticsearch`
    /// feature's `enable_elastic_db` parameter, so callers compile unchanged
    /// regardless of feature flags (elasticsearch is never enabled here).
    pub fn new_writable(config: &Config, network: &Network) -> Self {
        Self::new_with_debug(
            config,
            network,
            false,
            #[cfg(feature = "elasticsearch")]
            false,
            false,
        )
        .expect(
            "opening the read-write finalized state database failed; check that the \
             state cache directory is writable and not locked by another Zakura instance, \
             and that there is free disk space",
        )
    }

    /// Returns an on-disk database instance with the supplied production and debug settings.
    /// If there is no existing database, creates a new database on disk.
    ///
    /// This method is intended for use in tests.
    #[allow(clippy::unwrap_in_result)]
    pub(crate) fn new_with_debug(
        config: &Config,
        network: &Network,
        debug_skip_format_upgrades: bool,
        #[cfg(feature = "elasticsearch")] enable_elastic_db: bool,
        read_only: bool,
    ) -> Result<Self, StateInitError> {
        Self::new_with_debug_and_storage_validation(
            config,
            network,
            debug_skip_format_upgrades,
            #[cfg(feature = "elasticsearch")]
            enable_elastic_db,
            read_only,
            true,
            true,
        )
    }

    /// Returns an on-disk database instance with storage mode validation disabled.
    ///
    /// This method is intended for tests that use intentionally invalid storage
    /// configuration values to exercise lower-level pruning behavior.
    #[cfg(test)]
    pub(crate) fn new_with_debug_without_storage_validation(
        config: &Config,
        network: &Network,
        debug_skip_format_upgrades: bool,
        #[cfg(feature = "elasticsearch")] enable_elastic_db: bool,
        read_only: bool,
    ) -> Result<Self, StateInitError> {
        Self::new_with_debug_and_storage_validation(
            config,
            network,
            debug_skip_format_upgrades,
            #[cfg(feature = "elasticsearch")]
            enable_elastic_db,
            read_only,
            false,
            true,
        )
    }

    #[allow(clippy::unwrap_in_result)]
    fn new_with_debug_and_storage_validation(
        config: &Config,
        network: &Network,
        debug_skip_format_upgrades: bool,
        #[cfg(feature = "elasticsearch")] enable_elastic_db: bool,
        read_only: bool,
        validate_storage_mode: bool,
        enforce_resume_guard: bool,
    ) -> Result<Self, StateInitError> {
        // Fail fast on an invalid storage configuration, before opening the database.
        if validate_storage_mode {
            if let Err(error) = config.validate_storage_mode(network) {
                panic!("{error}");
            }
        }

        #[cfg(feature = "elasticsearch")]
        let elastic_db = if enable_elastic_db {
            use elasticsearch::{
                auth::Credentials::Basic,
                cert::CertificateValidation,
                http::transport::{SingleNodeConnectionPool, TransportBuilder},
                http::Url,
                Elasticsearch,
            };

            let conn_pool = SingleNodeConnectionPool::new(
                Url::parse(config.elasticsearch_url.as_str())
                    .expect("configured elasticsearch url is invalid"),
            );
            let transport = TransportBuilder::new(conn_pool)
                .cert_validation(CertificateValidation::None)
                .auth(Basic(
                    config.clone().elasticsearch_username,
                    config.clone().elasticsearch_password,
                ))
                .build()
                .expect("elasticsearch transport builder should not fail");

            Some(Elasticsearch::new(transport))
        } else {
            None
        };

        let db = ZakuraDb::new(
            config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            network,
            debug_skip_format_upgrades,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            read_only,
        )?;

        let vct = VctState::from_config(
            config.checkpoint_sync,
            config.vct_fast_sync,
            network,
            db.clone(),
        );

        // Re-derive this flag from the durable fast-sync marker, so reopening
        // before the checkpoint handoff still refuses roots below the last
        // checkpoint. The checkpoint height itself has the real frontier.
        let is_vct_sync_below_last_checkpoint = db
            .vct_synced_below()
            .zip(db.finalized_tip_height())
            .is_some_and(|(last_checkpoint_height, tip)| tip < last_checkpoint_height);

        #[cfg(feature = "elasticsearch")]
        let new_state = Self {
            debug_stop_at_height: config.debug_stop_at_height.map(block::Height),
            checkpoint_raw_tx_retention_start: None,
            checkpoint_raw_tx_archive_backlog: Arc::new(AtomicBool::new(false)),
            db,
            elastic_db,
            elastic_blocks: vec![],
            vct: VctCommitState::new(vct, is_vct_sync_below_last_checkpoint),
        };

        #[cfg(not(feature = "elasticsearch"))]
        let new_state = Self {
            debug_stop_at_height: config.debug_stop_at_height.map(block::Height),
            checkpoint_raw_tx_retention_start: None,
            checkpoint_raw_tx_archive_backlog: Arc::new(AtomicBool::new(false)),
            db,
            vct: VctCommitState::new(vct, is_vct_sync_below_last_checkpoint),
        };

        // Pruning is a one-way storage mode. Refuse to open a database that has
        // already pruned historical data in archive mode, because the data it
        // would be expected to serve has been irreversibly deleted.
        if config.pruning_config().is_none() && new_state.db.is_pruned() {
            panic!(
                "this database has been pruned and cannot be opened in archive storage mode; \
                 configure pruned storage mode (`storage_mode.pruned`), or delete the cache \
                 directory and re-sync from genesis"
            );
        }

        // Interrupted VCT syncs below the checkpoint handoff need the VCT root
        // source to resume. Without it, the legacy committer would refuse every
        // remaining checkpoint block.
        if enforce_resume_guard
            && new_state.vct.is_below_last_checkpoint()
            && new_state.vct.source().is_none()
        {
            panic!(
                "this database was previously synced in verified commitment tree mode that was \
                 interrupted below the last checkpoint height. the fast path that supplies \
                 the verified roots needed to resume the VCT sync is disabled. Set \
                 `consensus.checkpoint_sync = true` and `consensus.vct_fast_sync = true` to \
                 finish the VCT sync, or delete the cache directory and re-sync from genesis"
            );
        }

        // TODO: move debug_stop_at_height into a task in the start command (#3442)
        if let Some(tip_height) = new_state.db.finalized_tip_height() {
            if new_state.is_at_stop_height(tip_height) {
                let debug_stop_at_height = new_state
                    .debug_stop_at_height
                    .expect("true from `is_at_stop_height` implies `debug_stop_at_height` is Some");
                let tip_hash = new_state.db.finalized_tip_hash();

                if tip_height > debug_stop_at_height {
                    tracing::error!(
                        ?debug_stop_at_height,
                        ?tip_height,
                        ?tip_hash,
                        "previous state height is greater than the stop height",
                    );
                }

                tracing::info!(
                    ?debug_stop_at_height,
                    ?tip_height,
                    ?tip_hash,
                    "state is already at the configured height"
                );

                // RocksDB can do a cleanup when column families are opened.
                // So we want to drop it before we exit.
                std::mem::drop(new_state);

                // Drops tracing log output that's hasn't already been written to stdout
                // since this exits before calling drop on the WorkerGuard for the logger thread.
                // This is okay for now because this is test-only code
                //
                // TODO: Call ZakuradApp.shutdown or drop its Tracing component before calling exit_process to flush logs to stdout
                Self::exit_process();
            }
        }

        Ok(new_state)
    }

    /// Configure checkpoint raw transaction retention for pruned checkpoint sync.
    ///
    /// Checkpoint-verified blocks before the configured start can skip `tx_by_loc`
    /// writes, because they are outside the retention window relative to the
    /// known final checkpoint target.
    pub(crate) fn with_checkpoint_raw_tx_retention(
        mut self,
        max_checkpoint_height: block::Height,
        config: &Config,
    ) -> Self {
        self.checkpoint_raw_tx_retention_start = config.pruning_config().and_then(|pruning| {
            compute_checkpoint_raw_tx_retention_start(max_checkpoint_height, pruning.tx_retention)
        });

        let has_archive_backlog = config.pruning_config().is_some()
            && self.checkpoint_raw_tx_retention_start.is_some_and(|start| {
                let prune_from = self.db.lowest_retained_height().unwrap_or(block::Height(1));

                self.db.raw_transactions_exist_in_range(prune_from, start)
            });

        self.checkpoint_raw_tx_archive_backlog
            .store(has_archive_backlog, Ordering::Relaxed);

        self
    }

    /// Returns `true` when raw transaction bytes should be stored for a
    /// checkpoint-verified block at `height`.
    fn store_checkpoint_raw_transactions(&self, height: block::Height) -> bool {
        height.is_min()
            || self
                .checkpoint_raw_tx_retention_start
                .is_none_or(|start| height >= start)
    }

    /// Resolves the [`RetentionPlan`] for committing the finalized block at
    /// `height` in the current storage mode.
    ///
    /// This is the single place the raw-transaction retention decision is made:
    /// whether to write this block's raw transactions, which aged-out or backlog
    /// range to delete, and how to advance the pruning marker.
    /// [`ZakuraDb::write_block`] applies the returned plan without re-deriving it.
    ///
    /// `is_checkpoint` selects the checkpoint-sync policy (a retention start
    /// before which raw transactions are skipped, plus bounded archive-backlog
    /// draining) rather than the near-tip policy (ordinary online pruning). In
    /// archive mode the plan is always [`RetentionPlan::Store`].
    fn retention_plan(&self, height: block::Height, is_checkpoint: bool) -> RetentionPlan {
        let Some(pruning) = self.db.config().pruning_config() else {
            return RetentionPlan::Store;
        };

        let lowest_retained = self.db.lowest_retained_height();

        // Checkpoint blocks before the retention start: skip raw transactions,
        // draining any pre-existing archive backlog in bounded chunks first.
        if is_checkpoint && !self.store_checkpoint_raw_transactions(height) {
            let skipped_until = (height + 1).expect("checkpoint block height plus one is valid");

            if self
                .checkpoint_raw_tx_archive_backlog
                .load(Ordering::Relaxed)
            {
                if let Some((from, until)) = self
                    .db
                    .checkpoint_raw_transaction_prune_range(skipped_until)
                {
                    // The marker can only advance past this block once the
                    // backlog below it is fully drained, so the last chunk (which
                    // reaches `skipped_until`) is the one that skips this block's
                    // raw transactions and clears the backlog flag afterwards.
                    let final_chunk =
                        !checkpoint_prune_range_retains_current_height(height, Some((from, until)));

                    return RetentionPlan::DrainBacklog {
                        from,
                        until,
                        final_chunk,
                    };
                }
            }

            // No archive backlog left to drain: skip this block's raw
            // transactions and advance the pruning marker so readers know raw
            // data below it may be unavailable.
            return RetentionPlan::Skip {
                lowest_retained: skipped_until,
                write_marker: lowest_retained < Some(skipped_until),
            };
        }

        // Archive-equivalent path for this block (contextual blocks, or
        // checkpoint blocks within the retention window): keep raw transactions
        // and run ordinary online pruning.
        match ZakuraDb::prune_height_range(height, pruning.tx_retention, lowest_retained) {
            Some((from, until)) => RetentionPlan::Prune { from, until },
            None => RetentionPlan::Store,
        }
    }

    /// Returns `true` if the cached archive raw transaction backlog flag is set.
    #[cfg(test)]
    pub(crate) fn has_checkpoint_raw_tx_archive_backlog(&self) -> bool {
        self.checkpoint_raw_tx_archive_backlog
            .load(Ordering::Relaxed)
    }

    /// Returns the configured network for this database.
    pub fn network(&self) -> Network {
        self.db.network()
    }

    /// Commit a checkpoint-verified block to the state.
    ///
    /// It's the caller's responsibility to ensure that blocks are committed in
    /// order.
    pub fn commit_finalized(
        &mut self,
        ordered_block: QueuedCheckpointVerified,
        prev_note_commitment_trees: Option<NoteCommitmentTrees>,
        next_vct_block: Option<NextVctBlock>,
    ) -> Result<
        (CheckpointVerifiedBlock, NoteCommitmentTrees),
        (QueuedCheckpointVerified, CommitCheckpointVerifiedError),
    > {
        let (checkpoint_verified, rsp_tx) = ordered_block;
        let result = self.commit_finalized_direct(
            checkpoint_verified.clone().into(),
            prev_note_commitment_trees,
            next_vct_block,
            "commit checkpoint-verified request",
        );

        if result.is_ok() {
            metrics::counter!("state.checkpoint.finalized.block.count").increment(1);
            metrics::gauge!("state.checkpoint.finalized.block.height")
                .set(checkpoint_verified.height.0 as f64);

            // This height gauge is updated for both fully verified and checkpoint blocks.
            // These updates can't conflict, because the state makes sure that blocks
            // are committed in order.
            metrics::gauge!("zcash.chain.verified.block.height")
                .set(checkpoint_verified.height.0 as f64);
            metrics::counter!("zcash.chain.verified.block.total").increment(1);
        } else {
            metrics::counter!("state.checkpoint.error.block.count").increment(1);
            metrics::gauge!("state.checkpoint.error.block.height")
                .set(checkpoint_verified.height.0 as f64);
        };

        match result {
            Ok((hash, note_commitment_trees)) => {
                let _ = rsp_tx.send(Ok(hash));
                Ok((checkpoint_verified, note_commitment_trees))
            }
            Err(error) => Err(((checkpoint_verified, rsp_tx), error)),
        }
    }

    /// Immediately commit a `finalized` block to the finalized state.
    ///
    /// This can be called either by the non-finalized state (when finalizing
    /// a block) or by the checkpoint verifier.
    ///
    /// Use `source` as the source of the block in log messages.
    ///
    /// # Errors
    ///
    /// - Propagates any errors from writing to the DB
    /// - Propagates any errors from updating history and note commitment trees
    /// - If `hashFinalSaplingRoot` / `hashLightClientRoot` / `hashBlockCommitments`
    ///   does not match the expected value
    #[allow(clippy::unwrap_in_result)]
    pub fn commit_finalized_direct(
        &mut self,
        finalizable_block: FinalizableBlock,
        prev_note_commitment_trees: Option<NoteCommitmentTrees>,
        next_vct_block: Option<NextVctBlock>,
        source: &str,
    ) -> Result<(block::Hash, NoteCommitmentTrees), CommitCheckpointVerifiedError> {
        let (height, hash, finalized, prev_note_commitment_trees, retention, fast_write) =
            match finalizable_block {
                FinalizableBlock::Checkpoint {
                    checkpoint_verified,
                } => {
                    // Checkpoint-verified blocks don't have an associated treestate, so we retrieve the
                    // treestate of the finalized tip from the database and update it for the block
                    // being committed, assuming the retrieved treestate is the parent block's
                    // treestate. Later on, this function proves this assumption by asserting that the
                    // finalized tip is the parent block of the block being committed.

                    let block = checkpoint_verified.block.clone();
                    let precomputed_auth_data_root = checkpoint_verified.auth_data_root;
                    let mut history_tree = self.db.history_tree();
                    let prev_note_commitment_trees = prev_note_commitment_trees
                        .unwrap_or_else(|| self.db.note_commitment_trees_for_tip());

                    let mut note_commitment_trees = prev_note_commitment_trees.clone();
                    let network = self.network();
                    let height = checkpoint_verified.height;

                    // The last checkpoint height (boundary below which the vct
                    // path skips per-height trees).
                    let vct_last_checkpoint_height = self
                        .vct
                        .source()
                        .map(|v| v.vct_sync_last_checkpoint_height());

                    // In vct mode, if the source has this height's roots at or below the
                    // last checkpoint height, we skip the per-block note-commitment frontier recompute
                    // (`update_trees_parallel`). Instead, we validate the peer-supplied roots
                    // against the successor block's header/MMR.
                    let vct_roots = self.vct.source().and_then(|v| {
                        if vct_last_checkpoint_height
                            .is_some_and(|last_checkpoint_height| height > last_checkpoint_height)
                        {
                            None
                        } else {
                            v.vct_roots_at_height(height)
                        }
                    });

                    let mut vct_write = VctWriteData::default();

                    if let Some((sapling_root, orchard_root, ironwood_root)) = vct_roots {
                        // The last checkpoint frontiers are the only non-successor authority that
                        // can authenticate this block's own supplied roots before they are
                        // persisted.
                        let last_checkpoint_frontiers = self
                            .vct
                            .source()
                            .and_then(|v| v.final_frontiers_for_last_checkpoint(height));

                        // This block's own commitment check is identical to the
                        // previous vct block's look-ahead. When that look-ahead
                        // already validated this exact header, skip the duplicate.
                        let block_hash = block.hash();

                        // Defense in depth: only a witness that links to this block can
                        // authenticate its roots — a non-successor's commitment binds a
                        // different parent tree, so verifying against it would fail and
                        // wrongly evict a good supplied root. Treat a non-linking witness
                        // as absent, so the await-successor deferral below handles it. The
                        // write worker only buffers direct successors, so this should
                        // never fire.
                        let next_vct_block = next_vct_block.filter(|next_vct_block| {
                            let links = next_vct_block.header.previous_block_hash == block_hash;
                            if !links {
                                tracing::warn!(
                                    ?height,
                                    witness_parent = ?next_vct_block.header.previous_block_hash,
                                    expected_parent = ?block_hash,
                                    "VCT: ignoring a successor witness that does not link \
                                     to the block being committed"
                                );
                            }
                            links
                        });

                        // NU5+ block hashes do not commit to authorizing data. Include the
                        // body's auth-data root when matching a header-only prevalidation.
                        let block_auth_data_root = (NetworkUpgrade::current(&network, height)
                            >= NetworkUpgrade::Nu5)
                            .then(|| {
                                precomputed_auth_data_root.unwrap_or_else(|| block.auth_data_root())
                            });

                        // A successful look-ahead check authenticates both this header and
                        // its NU5+ auth-data root. A same-header body with a different root
                        // can never become valid by replacing the supplied note-commitment
                        // roots, so reject the body without evicting those roots or entering
                        // the write loop's retry path.
                        if let (
                            Some((
                                prevalidated_height,
                                prevalidated_hash,
                                Some(expected_auth_data_root),
                            )),
                            Some(actual_auth_data_root),
                        ) = (self.vct.prevalidated_next(), block_auth_data_root)
                        {
                            if prevalidated_height == height
                                && prevalidated_hash == block_hash
                                && expected_auth_data_root != actual_auth_data_root
                            {
                                metrics::counter!("state.vct.block.auth_data_root_mismatch.count")
                                    .increment(1);
                                tracing::warn!(
                                    ?height,
                                    ?block_hash,
                                    ?expected_auth_data_root,
                                    ?actual_auth_data_root,
                                    "VCT: checkpoint body auth-data root differs from its \
                                     authenticated header prevalidation"
                                );
                                return Err(ValidateContextError::VctBlockAuthDataRootMismatch {
                                    height,
                                    expected: expected_auth_data_root,
                                    actual: actual_auth_data_root,
                                }
                                .into());
                            }
                        }

                        let is_prevalidated = self.vct.prevalidated_next()
                            == Some((height, block_hash, block_auth_data_root));
                        if is_prevalidated {
                            if let Some(v) = self.vct.source() {
                                v.record_prevalidated();
                            }
                            // Observability: the previous fast block's look-ahead already
                            // validated this header, so its commitment check was skipped (the
                            // dedup). A subset of `state.vct.fast.block.count`.
                            metrics::counter!("state.vct.prevalidated.block.count").increment(1);
                        }

                        let mut verification_items = vec![
                            commitment_aux_verify::CommitmentRootVerification::with_roots(
                                block.clone(),
                                sapling_root,
                                orchard_root,
                                ironwood_root,
                                precomputed_auth_data_root,
                                is_prevalidated,
                            ),
                        ];

                        // If a buffered VCT successor block is available, we verify the current block's
                        // supplied roots against the successor block's header/MMR.
                        if let Some(next_vct_block) = &next_vct_block {
                            verification_items.push(
                                commitment_aux_verify::CommitmentRootVerification::header_only(
                                    next_vct_block.header.clone(),
                                    next_vct_block.height,
                                    next_vct_block.auth_data_root,
                                ),
                            );
                        }

                        // Verifies this block's own header, folds its supplied roots into
                        // the candidate tree, and when buffered checks the successor header
                        // against that candidate (the one-block lag).
                        let candidate = COMMIT_COMPUTE_POOL
                            .install(|| {
                                commitment_aux_verify::verify_commitment_roots(
                                    &network,
                                    (*history_tree).clone(),
                                    verification_items,
                                )
                            })
                            .map_err(|(_fail_height, error)| {
                                self.vct.clear_prevalidated_next();
                                self.vct_reject_supplied_root(height, error)
                            })?;

                        if let Some(next_vct_block) = &next_vct_block {
                            let next_auth_data_root =
                                (NetworkUpgrade::current(&network, next_vct_block.height)
                                    >= NetworkUpgrade::Nu5)
                                    .then_some(next_vct_block.auth_data_root)
                                    .flatten();
                            self.vct.mark_prevalidated(
                                next_vct_block.height,
                                next_vct_block.hash,
                                next_auth_data_root,
                            );
                        } else if self
                            .vct
                            .source()
                            .is_some_and(|v| v.vct_root_needs_successor(height, &network))
                        {
                            // Untrusted root at/above Heartwood, no successor to confirm it,
                            // not the last checkpoint: defer rather than persist it unverified. Leaves
                            // the database untouched; the block re-commits once the successor
                            // is buffered.
                            metrics::counter!("state.vct.root.await_successor.count").increment(1);
                            return Err(ValidateContextError::VctSuppliedRootAwaitingSuccessor {
                                height,
                            }
                            .into());
                        } else {
                            self.vct.clear_prevalidated_next();
                        }

                        history_tree = Arc::new(candidate);
                        if let Some(v) = self.vct.source() {
                            v.record_fast_block();
                        }
                        // Observability: this block folded supplied roots and skipped the
                        // note-commitment frontier recompute (the verified-commitment-trees
                        // fast path). Paired with `state.vct.legacy.block.count` below, this
                        // gives a live fast-vs-legacy ratio.
                        metrics::counter!("state.vct.fast.block.count").increment(1);

                        // When final frontiers are loaded, this is a persistent fast
                        // sync: mark the database fast-synced (per-height trees absent
                        // below the handoff height).
                        vct_write.sync_below = vct_last_checkpoint_height;

                        if let Some((
                            sapling_frontier,
                            orchard_frontier,
                            sprout_frontier,
                            ironwood_frontier,
                        )) = last_checkpoint_frontiers
                        {
                            // Last checkpoint verification: verify the supplied frontiers against
                            // this block's verified roots.
                            self.vct_verify_last_checkpoint_frontier_roots(
                                height,
                                &sapling_frontier,
                                &orchard_frontier,
                                &ironwood_frontier,
                                &sapling_root,
                                &orchard_root,
                                &ironwood_root,
                            )?;

                            // Subtree tips are left `None`: the resuming chain recomputes
                            // them from the frontier position.
                            note_commitment_trees = NoteCommitmentTrees {
                                sprout: sprout_frontier,
                                sapling: sapling_frontier,
                                sapling_subtree: None,
                                orchard: orchard_frontier,
                                orchard_subtree: None,
                                ironwood: ironwood_frontier,
                                ironwood_subtree: None,
                            };

                            // The handoff writes the real final frontier as the tip
                            // treestate, so the frontier is no longer frozen: heights at and
                            // above the handoff resume legacy recompute from a correct frontier.
                            self.vct.stop_vct_sync_at_last_checkpoint();
                        } else {
                            vct_write.anchor_roots =
                                Some((sapling_root, orchard_root, ironwood_root));

                            // A non-handoff fast block leaves the note-commitment frontier
                            // frozen (it folds roots instead of advancing the trees), so a
                            // later height with no valid supplied root must not legacy-recompute
                            // against this stale frontier (see the `else` branch below).
                            self.vct.start_vct_sync_below_last_checkpoint();
                        }
                    } else if self.vct.is_below_last_checkpoint() {
                        // Frozen-frontier safety: a fast sync has already frozen the
                        // note-commitment frontier, but this height has no valid supplied root
                        // (never fetched, or evicted after failing verification). Recomputing
                        // here would fold a wrong root into the history MMR and corrupt state,
                        // so refuse with a retryable error and leave the database untouched —
                        // the block is committed once a verifiable root is fetched from a peer.
                        metrics::counter!("state.vct.root.unavailable.count").increment(1);
                        tracing::warn!(
                            ?height,
                            "VCT: no verifiable supplied root for a frozen-frontier height; \
                         refusing to recompute (retryable)"
                        );
                        return Err(
                            ValidateContextError::VctSuppliedRootUnavailable { height }.into()
                        );
                    } else {
                        // Not a fast block: any cached pre-validation does not apply to
                        // the next fast block (its parent frontier differs), so clear it.
                        self.vct.clear_prevalidated_next();

                        // Observability: this block recomputed the note-commitment frontier
                        // (the legacy path) — either VCT is off, or the fast path's roots were
                        // unavailable for this height and it safely fell back.
                        metrics::counter!("state.vct.legacy.block.count").increment(1);

                        // Legacy / capture path: recompute the note-commitment frontier.
                        //
                        // Run two independent CPU-intensive crypto operations concurrently
                        // on the rayon pool: updating the note commitment trees, and
                        // checking this block's commitment against the *parent* history
                        // tree. They are independent; the history push below joins them.
                        #[cfg(feature = "commit-metrics")]
                        metrics::histogram!("zakura.state.write.block_tx_count")
                            .record(block.transactions.len() as f64);
                        #[cfg(feature = "commit-metrics")]
                        let _ckpt_compute = std::time::Instant::now();
                        let mut commitment_result = None;
                        // Run the two CPU-intensive operations inside the dedicated
                        // commit-compute pool so their nested rayon work uses isolated workers instead of
                        // contending with the verifier on the global pool.
                        let tree_result = COMMIT_COMPUTE_POOL.install(|| {
                            rayon::in_place_scope_fifo(|scope| {
                                scope.spawn_fifo(|_scope| {
                                    commitment_result = Some(timed_commit_phase!(
                                        "zakura.state.write.commitment_check.duration_seconds",
                                        check::block_commitment_is_valid_for_chain_history(
                                            block.clone(),
                                            &network,
                                            &history_tree,
                                            precomputed_auth_data_root,
                                        )
                                    ));
                                });

                                timed_commit_phase!(
                                    "zakura.state.write.update_trees.duration_seconds",
                                    note_commitment_trees.update_trees_parallel(&block)
                                )
                            })
                        });

                        // Surface the tree-update error first, preserving the error
                        // precedence of the previous sequential code.
                        tree_result.map_err(ValidateContextError::from)?;
                        // `in_place_scope_fifo` joins all spawned tasks, so this is `Some`.
                        commitment_result.expect("scope has already finished")?;

                        // Update the history tree (depends on both operations above).
                        let history_tree_mut = Arc::make_mut(&mut history_tree);
                        let sapling_root = note_commitment_trees.sapling.root();
                        let orchard_root = note_commitment_trees.orchard.root();
                        let ironwood_root = note_commitment_trees.ironwood.root();
                        history_tree_mut
                            .push(
                                &network,
                                block.clone(),
                                &sapling_root,
                                &orchard_root,
                                &ironwood_root,
                            )
                            .map_err(Arc::new)
                            .map_err(ValidateContextError::from)?;

                        #[cfg(feature = "commit-metrics")]
                        metrics::histogram!(
                            "zakura.state.write.checkpoint_compute.duration_seconds"
                        )
                        .record(_ckpt_compute.elapsed().as_secs_f64());
                    }

                    let treestate = Treestate {
                        note_commitment_trees,
                        history_tree,
                    };

                    let hash = checkpoint_verified.hash;

                    (
                        height,
                        hash,
                        FinalizedBlock::from_checkpoint_verified(checkpoint_verified, treestate),
                        Some(prev_note_commitment_trees),
                        self.retention_plan(height, true),
                        vct_write,
                    )
                }
                FinalizableBlock::Contextual {
                    contextually_verified,
                    treestate,
                } => {
                    let height = contextually_verified.height;

                    (
                        height,
                        contextually_verified.hash,
                        FinalizedBlock::from_contextually_verified(
                            contextually_verified,
                            *treestate,
                        ),
                        prev_note_commitment_trees,
                        self.retention_plan(height, false),
                        VctWriteData::default(),
                    )
                }
            };

        let committed_tip_hash = self.db.finalized_tip_hash();
        let committed_tip_height = self.db.finalized_tip_height();

        // Assert that callers (including unit tests) get the chain order correct
        if self.db.is_empty() {
            assert_eq!(
                committed_tip_hash, finalized.block.header.previous_block_hash,
                "the first block added to an empty state must be a genesis block, source: {source}",
            );
            assert_eq!(
                block::Height(0),
                height,
                "cannot commit genesis: invalid height, source: {source}",
            );
        } else {
            assert_eq!(
                committed_tip_height.expect("state must have a genesis block committed") + 1,
                Some(height),
                "committed block height must be 1 more than the finalized tip height, source: {source}",
            );

            assert_eq!(
                committed_tip_hash, finalized.block.header.previous_block_hash,
                "committed block must be a child of the finalized tip, source: {source}",
            );
        }

        #[cfg(feature = "elasticsearch")]
        let finalized_inner_block = finalized.block.clone();
        let note_commitment_trees = finalized.treestate.note_commitment_trees.clone();

        // Run `write_block` directly on the committer thread rather than entering the
        // dedicated commit-compute pool via `install()`.
        //
        // The committer is not a member of `COMMIT_COMPUTE_POOL`, so `install()` is a
        // synchronous cross-thread handoff: the committer parks until a pool worker
        // picks up the job, runs it, and signals back. That wait can dominate the
        // isolation it was meant to provide for `write_block`'s internal rayon
        // (`join`/`par_iter`). Running `write_block` here removes the per-block
        // round-trip; its internal rayon uses the global pool instead. Measured net
        // win on the sandblast region (see PR).
        let network = self.network();
        let result = self.db.write_block(
            finalized,
            prev_note_commitment_trees,
            &network,
            source,
            retention,
            fast_write,
        );

        if result.is_ok() {
            if retention.clears_archive_backlog() {
                self.checkpoint_raw_tx_archive_backlog
                    .store(false, Ordering::Relaxed);
            }

            // Save blocks to elasticsearch if the feature is enabled.
            #[cfg(feature = "elasticsearch")]
            self.elasticsearch(&finalized_inner_block);

            // TODO: move the stop height check to the syncer (#3442)
            if self.is_at_stop_height(height) {
                tracing::info!(
                    ?height,
                    ?hash,
                    block_source = ?source,
                    "stopping at configured height, flushing database to disk"
                );

                // POC: emit the equivalence digest + fast-path summary before exit.
                self.vct_log_equivalence_digest();

                // We're just about to do a forced exit, so it's ok to do a forced db shutdown
                self.db.shutdown(true);

                // Drops tracing log output that's hasn't already been written to stdout
                // since this exits before calling drop on the WorkerGuard for the logger thread.
                // This is okay for now because this is test-only code
                //
                // TODO: Call ZakuradApp.shutdown or drop its Tracing component before calling exit_process to flush logs to stdout
                Self::exit_process();
            }
        }

        result.map(|hash| (hash, note_commitment_trees))
    }

    /// POC: `true` when the verified-commitment-trees fast (skip-recompute) path will
    /// apply to `height` — i.e. fast mode is active *and* the source already holds this
    /// height's roots, so the committer will fold them in and skip the frontier recompute.
    pub(crate) fn vct_fast_will_apply(&self, height: block::Height) -> bool {
        self.vct
            .source()
            .is_some_and(|v| v.is_enabled() && v.vct_roots_at_height(height).is_some())
    }

    /// Clears any cached successor prevalidation.
    ///
    /// The finalized write loop calls this when it discards checkpoint queue state, so a
    /// look-ahead header that no longer corresponds to the next committed block cannot
    /// authorize a later fast-path skip.
    pub(crate) fn clear_vct_prevalidated_next(&mut self) {
        self.vct.clear_prevalidated_next();
    }

    /// `true` when committing `height` on the fast path needs a buffered successor before
    /// it can safely persist this block's supplied roots.
    ///
    /// Only untrusted peer-supplied roots at or above Heartwood require this. The
    /// checkpoint handoff is exempt because its embedded final frontiers are verified
    /// against this block's roots before the real tip treestate is written; trusted
    /// local fixtures can commit their tip root on the in-arrears check.
    pub(crate) fn vct_fast_needs_successor(&self, height: block::Height) -> bool {
        self.vct
            .source()
            .is_some_and(|v| v.vct_root_needs_successor(height, &self.network()))
    }

    /// Returns a VCT successor witness from the contextually validated Zakura
    /// header store, without requiring the successor's block body.
    pub(crate) fn vct_successor_from_header_store(
        &self,
        height: block::Height,
        hash: block::Hash,
    ) -> Option<NextVctBlock> {
        let successor_height = (height + 1)?;
        let header = self.db.zakura_header(successor_height)?;
        if header.previous_block_hash != hash {
            tracing::warn!(
                ?height,
                ?hash,
                ?successor_height,
                successor_parent = ?header.previous_block_hash,
                "VCT: ignoring a stored successor header that does not link to the block being committed"
            );
            return None;
        }

        let roots = self
            .db
            .zakura_header_commitment_roots_by_height_range(successor_height..=successor_height)
            .into_iter()
            .next()
            .filter(|roots| roots.height == successor_height)?;

        Some(NextVctBlock::from_header(
            header,
            successor_height,
            roots.auth_data_root,
        ))
    }

    /// Verify checkpoint handoff frontiers against this block's supplied roots.
    #[allow(clippy::too_many_arguments)]
    fn vct_verify_last_checkpoint_frontier_roots(
        &mut self,
        height: block::Height,
        sapling_frontier: &sapling::tree::NoteCommitmentTree,
        orchard_frontier: &orchard::tree::NoteCommitmentTree,
        ironwood_frontier: &ironwood::tree::NoteCommitmentTree,
        sapling_root: &sapling::tree::Root,
        orchard_root: &orchard::tree::Root,
        ironwood_root: &ironwood::tree::Root,
    ) -> Result<(), CommitCheckpointVerifiedError> {
        if sapling_frontier.root() != *sapling_root
            || orchard_frontier.root() != *orchard_root
            || ironwood_frontier.root() != *ironwood_root
        {
            self.vct.clear_prevalidated_next();
            return Err(self.vct_reject_supplied_root(
                height,
                ValidateContextError::VctSuppliedRootUnavailable { height },
            ));
        }

        Ok(())
    }

    /// Reject a supplied fast-path root that failed verification for `height`.
    ///
    /// Evicts the bad root from the source so it is never re-read, and returns a typed,
    /// retryable error. In fast mode the note-commitment frontier is frozen, so the
    /// committer cannot recompute the root locally (that would fold a wrong root into the
    /// history MMR); it must refuse and leave the database untouched rather than persist
    /// or corrupt state. Roots are not individually re-requested: the hole is only filled
    /// if the same header range is re-delivered (for example by another fanout peer's
    /// in-flight response), otherwise the commit stays parked and the §8 stall
    /// metrics/logs surface it. A wrong root therefore never corrupts state, at the cost
    /// of stalling the sync at this height.
    fn vct_reject_supplied_root(
        &self,
        height: block::Height,
        error: ValidateContextError,
    ) -> CommitCheckpointVerifiedError {
        if let Some(v) = self.vct.source() {
            v.invalidate_fast_root(height);
        }
        metrics::counter!("state.vct.root.rejected.count").increment(1);
        tracing::warn!(
            ?height,
            ?error,
            "VCT: supplied commitment root failed verification; evicted so it is never re-read"
        );
        ValidateContextError::VctSuppliedRootUnavailable { height }.into()
    }

    /// Test-only: enable fast mode reading roots/frontiers from an arbitrary
    /// [`commitment_aux::CommitmentRootSource`] (e.g. a payload produced from a
    /// database via [`commitment_aux::produce_block_roots`]), so the producer→consumer
    /// round-trip can be exercised in-process. `requires_verified_successor` marks
    /// whether the installed source is untrusted and must defer tip roots until their
    /// successor is buffered.
    #[cfg(test)]
    pub(in crate::service::finalized_state) fn enable_vct_fast_source(
        &mut self,
        source: Box<dyn commitment_aux::CommitmentRootSource>,
        requires_verified_successor: bool,
    ) {
        self.vct
            .install_test_source(source, requires_verified_successor);
    }

    /// Test-only: the fast-sync handoff height recorded in the database marker, if any.
    #[cfg(test)]
    pub(crate) fn vct_fast_synced_below(&self) -> Option<block::Height> {
        self.db.vct_synced_below()
    }

    /// Test-only: number of blocks that took the fast (skip-recompute) path so far.
    #[cfg(test)]
    pub(crate) fn vct_fast_count(&self) -> u64 {
        self.vct.source().map(|v| v.vct_count()).unwrap_or(0)
    }

    /// Test-only: number of fast blocks whose own commitment check was skipped by
    /// the dedup (the previous block's look-ahead already validated them).
    #[cfg(test)]
    pub(crate) fn vct_prevalidated_count(&self) -> u64 {
        self.vct
            .source()
            .map(|v| v.prevalidated_count())
            .unwrap_or(0)
    }

    /// POC: log the consensus-equivalence digest (anchor sets + history root) and
    /// the fast-path block count at the stop height, so a legacy run and a fast run
    /// can be compared. Gated by `VCT_DIGEST` so normal runs pay nothing.
    fn vct_log_equivalence_digest(&self) {
        if std::env::var_os("VCT_DIGEST").is_none() {
            return;
        }

        let fast_count = if let Some(v) = self.vct.source() {
            v.vct_count()
        } else {
            0
        };

        let (
            sapling_anchor_count,
            sapling_anchor_digest,
            orchard_anchor_count,
            orchard_anchor_digest,
        ) = self.db.vct_anchor_digest();
        let history_root = self.db.history_tree().hash();

        tracing::info!(
            sapling_anchor_count,
            sapling_anchor_digest,
            orchard_anchor_count,
            orchard_anchor_digest,
            ?history_root,
            vct_fast_blocks = fast_count,
            "VCT-DIGEST"
        );
    }

    #[cfg(feature = "elasticsearch")]
    /// Store finalized blocks into an elasticsearch database.
    ///
    /// We use the elasticsearch bulk api to index multiple blocks at a time while we are
    /// synchronizing the chain, when we get close to tip we index blocks one by one.
    pub fn elasticsearch(&mut self, block: &Arc<block::Block>) {
        if let Some(client) = self.elastic_db.clone() {
            let block_time = block.header.time.timestamp();
            let local_time = chrono::Utc::now().timestamp();

            // Bulk size is small enough to avoid the elasticsearch 100mb content length limitation.
            // MAX_BLOCK_BYTES = 2MB but each block use around 4.1 MB of JSON.
            // Each block count as 2 as we send them with a operation/header line. A value of 48
            // is 24 blocks.
            const AWAY_FROM_TIP_BULK_SIZE: usize = 48;

            // The number of blocks the bulk will have when we are in sync.
            // A value of 2 means only 1 block as we want to insert them as soon as we get
            // them for a real time experience. This is the same for mainnet and testnet.
            const CLOSE_TO_TIP_BULK_SIZE: usize = 2;

            // We consider in sync when the local time and the blockchain time difference is
            // less than this number of seconds.
            const CLOSE_TO_TIP_SECONDS: i64 = 14400; // 4 hours

            let mut blocks_size_to_dump = AWAY_FROM_TIP_BULK_SIZE;

            // If we are close to the tip, index one block per bulk call.
            if local_time - block_time < CLOSE_TO_TIP_SECONDS {
                blocks_size_to_dump = CLOSE_TO_TIP_BULK_SIZE;
            }

            // Insert the operation line.
            let height_number = block.coinbase_height().unwrap_or(block::Height(0)).0;
            self.elastic_blocks.push(
                serde_json::json!({
                    "index": {
                        "_id": height_number.to_string().as_str()
                    }
                })
                .to_string(),
            );

            // Insert the block itself.
            self.elastic_blocks
                .push(serde_json::json!(block).to_string());

            // We are in bulk time, insert to ES all we have.
            if self.elastic_blocks.len() >= blocks_size_to_dump {
                let rt = tokio::runtime::Runtime::new()
                    .expect("runtime creation for elasticsearch should not fail.");
                let blocks = self.elastic_blocks.clone();
                let network = self.network();

                rt.block_on(async move {
                    // Send a ping to the server to check if it is available before inserting.
                    if client.ping().send().await.is_err() {
                        tracing::error!("Elasticsearch is not available, skipping block indexing");
                        return;
                    }

                    let response = client
                        .bulk(elasticsearch::BulkParts::Index(
                            format!("zcash_{}", network.to_string().to_lowercase()).as_str(),
                        ))
                        .body(blocks)
                        .send()
                        .await
                        .expect("ES Request should never fail");

                    // Make sure no errors ever.
                    let response_body = response
                        .json::<serde_json::Value>()
                        .await
                        .expect("ES response parsing error. Maybe we are sending more than 100 mb of data (`http.max_content_length`)");
                    let errors = response_body["errors"].as_bool().unwrap_or(true);
                    assert!(!errors, "{}", format!("ES error: {response_body}"));
                });

                // Clean the block storage.
                self.elastic_blocks.clear();
            }
        }
    }

    /// Stop the process if `block_height` is greater than or equal to the
    /// configured stop height.
    fn is_at_stop_height(&self, block_height: block::Height) -> bool {
        let debug_stop_at_height = match self.debug_stop_at_height {
            Some(debug_stop_at_height) => debug_stop_at_height,
            None => return false,
        };

        if block_height < debug_stop_at_height {
            return false;
        }

        true
    }

    /// Exit the host process.
    ///
    /// Designed for debugging and tests.
    ///
    /// TODO: move the stop height check to the syncer (#3442)
    fn exit_process() -> ! {
        tracing::info!("exiting Zebra");

        // Some OSes require a flush to send all output to the terminal.
        // Zebra's logging doesn't depend on `tokio`, so we flush the stdlib sync streams.
        //
        // TODO: if this doesn't work, send an empty line as well.
        let _ = stdout().lock().flush();
        let _ = stderr().lock().flush();

        // Give some time to logger thread to flush out any remaining lines to stdout
        // and yield so that tests pass on MacOS
        std::thread::sleep(std::time::Duration::from_secs(3));

        // Exits before calling drop on the WorkerGuard for the logger thread,
        // dropping any lines that haven't already been written to stdout.
        // This is okay for now because this is test-only code
        std::process::exit(0);
    }
}

/// Returns `true` when archive-backlog pruning does not prune the current
/// checkpoint block, so its raw transactions still need to be written.
fn checkpoint_prune_range_retains_current_height(
    height: block::Height,
    checkpoint_prune_range: Option<(block::Height, block::Height)>,
) -> bool {
    checkpoint_prune_range.is_some_and(|(_, prune_until)| prune_until <= height)
}

/// Returns the lowest checkpoint height whose raw transactions should be kept.
fn compute_checkpoint_raw_tx_retention_start(
    max_checkpoint_height: block::Height,
    tx_retention: u32,
) -> Option<block::Height> {
    let max_skipped_height = max_checkpoint_height.0.checked_sub(tx_retention)?;

    max_skipped_height.checked_add(1).map(block::Height)
}
