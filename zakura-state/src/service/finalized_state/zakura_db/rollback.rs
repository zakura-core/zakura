//! Offline rollback support for the finalized state database.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

use semver::Version;
use zakura_chain::{
    amount::{self, Amount, DeferredPoolBalanceChange, NonNegative},
    block::{self, Block, Height},
    history_tree::{HistoryTree, HistoryTreeError},
    ironwood, orchard,
    parallel::tree::{NoteCommitmentTreeError, NoteCommitmentTrees},
    parameters::{
        subsidy::{block_subsidy, funding_stream_values, FundingStreamReceiver, SubsidyError},
        Network, NetworkUpgrade,
    },
    sapling,
    subtree::NoteCommitmentSubtreeIndex,
    transaction,
    transparent::{self, Input},
    value_balance::ValueBalance,
};

use crate::{
    config::state_database_format_version_on_disk,
    constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
    service::{
        finalized_state::{
            disk_db::{DiskWriteBatch, ReadDisk, WriteDisk},
            disk_format::{
                transparent::{
                    AddressBalanceLocation, AddressTransaction, AddressUnspentOutput,
                    OutputLocation,
                },
                TransactionLocation,
            },
            zakura_db::{
                chain::BLOCK_INFO,
                transparent::{BALANCE_BY_TRANSPARENT_ADDR, TX_LOC_BY_SPENT_OUT_LOC},
                ZakuraDb,
            },
            STATE_COLUMN_FAMILIES_IN_CODE,
        },
        non_finalized_state::write_semantically_verified_backup_block,
    },
    Config, SemanticallyVerifiedBlock,
};

/// Options for rolling back the finalized state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RollbackFinalizedStateOptions {
    /// Roll the finalized tip back to this height.
    pub target_height: block::Height,

    /// Write removed finalized blocks to the non-finalized backup cache.
    pub keep_rolled_back_blocks: bool,

    /// The maximum checkpoint height the node will use at its next startup.
    ///
    /// When `keep_rolled_back_blocks` is set, the kept blocks are only loaded back into the
    /// non-finalized state on the next start if the new finalized tip is at or above this height
    /// (see `StateService::new`'s `is_finalized_tip_past_max_checkpoint` gate). Rolling back below
    /// it would silently discard the backup, so rollback refuses that combination when this is
    /// `Some`. Callers that cannot determine the height (or want to skip the check) pass `None`.
    pub max_checkpoint_height: Option<block::Height>,
}

/// Details about non-finalized backup files written by rollback.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RollbackBackupSummary {
    /// Backup directory used for rolled-back blocks.
    pub path: PathBuf,

    /// Number of rolled-back blocks written to the backup cache.
    pub block_count: usize,
}

/// Summary of a finalized-state rollback.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RollbackFinalizedStateSummary {
    /// Finalized tip before rollback.
    pub old_tip: (block::Height, block::Hash),

    /// Finalized tip after rollback.
    pub new_tip: (block::Height, block::Hash),

    /// Number of finalized blocks removed.
    pub rolled_back_count: u32,

    /// Backup details when rolled-back blocks were kept.
    pub backup: Option<RollbackBackupSummary>,
}

/// Errors returned by finalized-state rollback.
#[derive(Debug, thiserror::Error)]
pub enum RollbackFinalizedStateError {
    /// The on-disk state database format does not match the running code.
    #[error(
        "state database format mismatch: on disk {on_disk:?}, running code {in_code}; \
         use a Zebra binary with the same state format"
    )]
    FormatMismatch {
        /// Version read from disk.
        on_disk: Option<Version>,
        /// Version implemented by the running code.
        in_code: Version,
    },

    /// The state database has no finalized tip.
    #[error("state database is empty")]
    EmptyState,

    /// The requested target height is above the finalized tip.
    #[error("target height {target:?} is above finalized tip {tip:?}")]
    TargetAboveTip {
        /// Requested rollback height.
        target: block::Height,
        /// Current finalized tip height.
        tip: block::Height,
    },

    /// The requested target height is the finalized tip.
    #[error("target height {target:?} is already the finalized tip")]
    TargetIsTip {
        /// Requested rollback height.
        target: block::Height,
    },

    /// The requested target height is missing from the finalized chain.
    #[error("target height {target:?} is missing from hash_by_height")]
    MissingTarget {
        /// Requested rollback height.
        target: block::Height,
    },

    /// Keeping rolled-back blocks is bounded by Zebra's non-finalized restore window.
    #[error(
        "cannot keep {count} rolled-back blocks: maximum non-finalized restore depth is {max}"
    )]
    KeepDepthTooLarge {
        /// Requested number of kept blocks.
        count: u32,
        /// Maximum supported number of kept blocks.
        max: u32,
    },

    /// Keeping rolled-back blocks below the max checkpoint height would silently discard them.
    #[error(
        "cannot keep rolled-back blocks: new finalized tip height {target:?} is below the max \
         checkpoint height {max_checkpoint:?}. The non-finalized restore only reloads blocks once \
         the finalized tip is past the last checkpoint, so the kept blocks would be deleted on the \
         next start. Roll back to a height at or above the max checkpoint, or omit \
         --keep-rolled-back-blocks."
    )]
    KeepBelowMaxCheckpoint {
        /// Requested new finalized tip height.
        target: block::Height,
        /// Max checkpoint height the node will use at its next startup.
        max_checkpoint: block::Height,
    },

    /// Non-finalized backup is disabled for this state configuration.
    #[error("cannot keep rolled-back blocks because non-finalized backup is disabled")]
    BackupDisabled,

    /// A block required for rollback could not be loaded.
    #[error("missing finalized block at height {height:?}")]
    MissingBlock {
        /// Missing block height.
        height: block::Height,
    },

    /// A transaction required for rollback could not be loaded.
    #[error("missing finalized transaction {hash:?}")]
    MissingTransaction {
        /// Missing transaction hash.
        hash: transaction::Hash,
    },

    /// A transparent output required for rollback could not be loaded.
    #[error("missing transparent output {outpoint:?}")]
    MissingTransparentOutput {
        /// Missing transparent outpoint.
        outpoint: transparent::OutPoint,
    },

    /// A transparent address balance required for rollback could not be loaded.
    #[error("missing transparent address balance for {address:?}")]
    MissingAddressBalance {
        /// Missing transparent address.
        address: transparent::Address,
    },

    /// A Sapling note commitment tree required for rollback could not be loaded.
    #[error("missing Sapling note commitment tree at height {height:?}")]
    MissingSaplingTree {
        /// Missing tree height.
        height: block::Height,
    },

    /// An Orchard note commitment tree required for rollback could not be loaded.
    #[error("missing Orchard note commitment tree at height {height:?}")]
    MissingOrchardTree {
        /// Missing tree height.
        height: block::Height,
    },

    /// An Ironwood note commitment tree required for rollback could not be loaded.
    #[error("missing Ironwood note commitment tree at height {height:?}")]
    MissingIronwoodTree {
        /// Missing tree height.
        height: block::Height,
    },

    /// The finalized tip's Sprout note commitment tree is missing.
    #[error("missing Sprout note commitment tree at finalized tip {height:?}")]
    MissingSproutTree {
        /// Missing tree height.
        height: block::Height,
    },

    /// Address balance arithmetic failed while reversing transparent indexes.
    #[error("transparent address balance update failed")]
    AddressBalance(#[from] amount::Error),

    /// Rebuilding note commitment trees failed.
    #[error("failed to rebuild note commitment trees")]
    NoteCommitmentTree(#[from] NoteCommitmentTreeError),

    /// Rebuilding the history tree failed.
    #[error("failed to rebuild history tree")]
    HistoryTree(#[from] HistoryTreeError),

    /// Computing a block subsidy failed.
    #[error("failed to compute block subsidy")]
    Subsidy(#[from] SubsidyError),

    /// Amount arithmetic failed while computing deferred pool balance changes.
    #[error("failed to compute deferred pool balance change")]
    DeferredPoolBalance(#[source] amount::Error),

    /// RocksDB failed while writing the rollback batch.
    #[error("failed to write rollback batch")]
    RocksDb(#[from] rocksdb::Error),

    /// Non-finalized backup file I/O failed.
    #[error("failed to update non-finalized backup cache")]
    BackupIo(#[from] std::io::Error),
}

/// Preview a finalized-state rollback without mutating the database.
pub fn preview_rollback_finalized_state(
    config: Config,
    network: &Network,
    options: RollbackFinalizedStateOptions,
) -> Result<RollbackFinalizedStateSummary, RollbackFinalizedStateError> {
    check_format_version(&config, network)?;

    let db = open_rollback_db(&config, network, true);
    let bounds = validate_rollback(&db, &options)?;

    // A dry run only reports the plan, so it deliberately skips the genesis-to-target treestate
    // rebuild and batch construction that `rollback_finalized_state` performs. The real run
    // recomputes those; doing them here would make the preview as slow as the rollback itself.
    Ok(bounds.summary(None))
}

/// Roll back the finalized state database to `options.target_height`.
///
/// The database is opened writable. If another Zebra process is running, RocksDB's
/// lock prevents this function from opening the state.
pub fn rollback_finalized_state(
    config: Config,
    network: &Network,
    options: RollbackFinalizedStateOptions,
) -> Result<RollbackFinalizedStateSummary, RollbackFinalizedStateError> {
    check_format_version(&config, network)?;

    let db = open_rollback_db(&config, network, false);
    let prepared = prepare_rollback(&db, network, &options)?;

    let backup = if options.keep_rolled_back_blocks {
        let backup_dir = config
            .non_finalized_state_backup_dir(network)
            .ok_or(RollbackFinalizedStateError::BackupDisabled)?;

        clear_backup_dir(&backup_dir)?;

        for block in prepared.removed_blocks.iter().rev() {
            write_semantically_verified_backup_block(&backup_dir, block)?;
        }

        Some(RollbackBackupSummary {
            path: backup_dir,
            block_count: prepared.removed_blocks.len(),
        })
    } else {
        if let Some(backup_dir) = config.non_finalized_state_backup_dir(network) {
            clear_backup_dir(&backup_dir)?;
        }

        None
    };

    let summary = prepared.summary(backup);

    db.write_batch(prepared.batch)?;

    Ok(summary)
}

fn check_format_version(
    config: &Config,
    network: &Network,
) -> Result<(), RollbackFinalizedStateError> {
    let in_code = state_database_format_version_in_code();
    let on_disk = state_database_format_version_on_disk(config, network).map_err(|_| {
        RollbackFinalizedStateError::FormatMismatch {
            on_disk: None,
            in_code: in_code.clone(),
        }
    })?;

    match on_disk {
        Some(on_disk) if on_disk == in_code => Ok(()),
        Some(on_disk) => Err(RollbackFinalizedStateError::FormatMismatch {
            on_disk: Some(on_disk),
            in_code,
        }),
        None => Err(RollbackFinalizedStateError::EmptyState),
    }
}

fn open_rollback_db(config: &Config, network: &Network, read_only: bool) -> ZakuraDb {
    ZakuraDb::new(
        config,
        STATE_DATABASE_KIND,
        &state_database_format_version_in_code(),
        network,
        // Skip format upgrades: `check_format_version` already confirmed the on-disk format matches
        // the running code, so no upgrade is needed. Skipping also avoids spawning the background
        // format-change thread, which could otherwise mutate the database concurrently with the
        // rollback's own batch write.
        true,
        STATE_COLUMN_FAMILIES_IN_CODE
            .iter()
            .map(ToString::to_string),
        read_only,
    ).expect("opening the finalized state database failed; the configured cache directory must contain a readable Zakura database")
}

/// The validated bounds of a rollback: where the finalized tip is now, and where it will end up.
///
/// Computing these is cheap (a couple of index lookups), so the dry-run preview stops here
/// instead of building the full rollback batch.
struct RollbackBounds {
    old_tip: (block::Height, block::Hash),
    new_tip: (block::Height, block::Hash),
}

impl RollbackBounds {
    fn summary(&self, backup: Option<RollbackBackupSummary>) -> RollbackFinalizedStateSummary {
        RollbackFinalizedStateSummary {
            old_tip: self.old_tip,
            new_tip: self.new_tip,
            rolled_back_count: self.old_tip.0 .0 - self.new_tip.0 .0,
            backup,
        }
    }
}

struct PreparedRollback {
    bounds: RollbackBounds,
    batch: DiskWriteBatch,
    removed_blocks: Vec<SemanticallyVerifiedBlock>,
}

impl PreparedRollback {
    fn summary(&self, backup: Option<RollbackBackupSummary>) -> RollbackFinalizedStateSummary {
        self.bounds.summary(backup)
    }
}

/// Validate a rollback request against the finalized state, without rebuilding any treestate or
/// building the rollback batch.
fn validate_rollback(
    db: &ZakuraDb,
    options: &RollbackFinalizedStateOptions,
) -> Result<RollbackBounds, RollbackFinalizedStateError> {
    let old_tip = db.tip().ok_or(RollbackFinalizedStateError::EmptyState)?;
    let (old_tip_height, _) = old_tip;

    if options.target_height > old_tip_height {
        return Err(RollbackFinalizedStateError::TargetAboveTip {
            target: options.target_height,
            tip: old_tip_height,
        });
    }

    if options.target_height == old_tip_height {
        return Err(RollbackFinalizedStateError::TargetIsTip {
            target: options.target_height,
        });
    }

    let target_hash =
        db.hash(options.target_height)
            .ok_or(RollbackFinalizedStateError::MissingTarget {
                target: options.target_height,
            })?;

    if options.keep_rolled_back_blocks {
        let rollback_count = old_tip_height.0 - options.target_height.0;
        if rollback_count > crate::MAX_BLOCK_REORG_HEIGHT {
            return Err(RollbackFinalizedStateError::KeepDepthTooLarge {
                count: rollback_count,
                max: crate::MAX_BLOCK_REORG_HEIGHT,
            });
        }

        // The non-finalized restore only reloads the kept blocks once the finalized tip is past
        // the last checkpoint, so refuse rather than silently discarding them on the next start.
        if let Some(max_checkpoint) = options
            .max_checkpoint_height
            .filter(|&max| options.target_height < max)
        {
            return Err(RollbackFinalizedStateError::KeepBelowMaxCheckpoint {
                target: options.target_height,
                max_checkpoint,
            });
        }
    }

    Ok(RollbackBounds {
        old_tip,
        new_tip: (options.target_height, target_hash),
    })
}

fn prepare_rollback(
    db: &ZakuraDb,
    network: &Network,
    options: &RollbackFinalizedStateOptions,
) -> Result<PreparedRollback, RollbackFinalizedStateError> {
    let bounds = validate_rollback(db, options)?;
    let (old_tip_height, _) = bounds.old_tip;

    let target_value_pool = db
        .block_info(options.target_height.into())
        .ok_or(RollbackFinalizedStateError::MissingTarget {
            target: options.target_height,
        })?
        .value_pools()
        .to_owned();

    let mut batch = DiskWriteBatch::new();
    let mut address_balances = HashMap::new();
    let mut removed_blocks = Vec::new();
    let mut removed_blocks_have_sprout_commitments = false;

    for height in ((options.target_height.0 + 1)..=old_tip_height.0)
        .rev()
        .map(Height)
    {
        let block = db
            .block(height.into())
            .ok_or(RollbackFinalizedStateError::MissingBlock { height })?;
        let semantically_verified = SemanticallyVerifiedBlock::from(block.clone())
            .with_deferred_pool_balance_change(deferred_pool_balance_change(height, network)?);

        reverse_transparent_block(
            db,
            network,
            &mut batch,
            &mut address_balances,
            height,
            &block,
        )?;
        delete_shielded_block(db, &mut batch, &block);
        delete_block_and_transaction_data(db, &mut batch, height, &block)?;

        removed_blocks_have_sprout_commitments |= block_has_sprout_commitments(&block);
        removed_blocks.push(semantically_verified);
    }

    // The Zakura header store races ahead of the body chain and is keyed independently of the
    // block CFs above, so roll it back too: otherwise the rolled-back database keeps a Zakura
    // header tip far above the new body tip, which starves Zakura block-sync (the floor body
    // becomes un-requestable; see BUG_SUMMARY.md / `delete_zakura_headers_above`).
    delete_zakura_headers_above(db, &mut batch, options.target_height);

    let target_treestate = prepare_target_treestate(
        db,
        network,
        options.target_height,
        removed_blocks_have_sprout_commitments,
    )?;

    write_address_balances(db, &mut batch, address_balances);
    reset_tip_trees(db, &mut batch, &target_treestate);
    reset_value_pool(db, &mut batch, &target_value_pool);
    prune_tree_indexes(
        db,
        &mut batch,
        options.target_height,
        &target_treestate.retained_sprout_roots,
    );

    Ok(PreparedRollback {
        bounds,
        batch,
        removed_blocks,
    })
}

struct RebuiltTreestate {
    sprout_tree: Arc<zakura_chain::sprout::tree::NoteCommitmentTree>,
    history_tree: HistoryTree,
    retained_sprout_roots: Option<HashSet<zakura_chain::sprout::tree::Root>>,
}

fn prepare_target_treestate(
    db: &ZakuraDb,
    network: &Network,
    target_height: Height,
    removed_blocks_have_sprout_commitments: bool,
) -> Result<RebuiltTreestate, RollbackFinalizedStateError> {
    if NetworkUpgrade::current(network, target_height) >= NetworkUpgrade::Canopy
        && !removed_blocks_have_sprout_commitments
    {
        return load_modern_treestate_at_height(db, network, target_height);
    }

    rebuild_treestate_to_height(db, network, target_height)
}

fn load_modern_treestate_at_height(
    db: &ZakuraDb,
    network: &Network,
    target_height: Height,
) -> Result<RebuiltTreestate, RollbackFinalizedStateError> {
    let sprout_tree = db
        .sprout_tree_for_tip()
        .map_err(|error| RollbackFinalizedStateError::MissingSproutTree { height: error.tip })?;
    let history_tree = rebuild_history_tree_from_upgrade_activation(db, network, target_height)?;

    Ok(RebuiltTreestate {
        sprout_tree,
        history_tree,
        // No removed block changed the Sprout note commitment tree, so the current tip tree is
        // exactly the target tree and no Sprout anchors above the target were created.
        retained_sprout_roots: None,
    })
}

fn block_has_sprout_commitments(block: &Block) -> bool {
    block.sprout_note_commitments().next().is_some()
}

fn rebuild_history_tree_from_upgrade_activation(
    db: &ZakuraDb,
    network: &Network,
    target_height: Height,
) -> Result<HistoryTree, RollbackFinalizedStateError> {
    let network_upgrade = NetworkUpgrade::current(network, target_height);

    if network_upgrade < NetworkUpgrade::Heartwood {
        return Ok(HistoryTree::default());
    }

    let start_height = network_upgrade
        .activation_height(network)
        .expect("current network upgrade must have an activation height");

    let (block, sapling_root, orchard_root, ironwood_root) =
        history_rebuild_inputs_at_height(db, start_height)?;
    let mut history_tree =
        HistoryTree::from_block(network, block, &sapling_root, &orchard_root, &ironwood_root)?;

    for height in ((start_height.0 + 1)..=target_height.0).map(Height) {
        let (block, sapling_root, orchard_root, ironwood_root) =
            history_rebuild_inputs_at_height(db, height)?;

        history_tree.push(network, block, &sapling_root, &orchard_root, &ironwood_root)?;
    }

    Ok(history_tree)
}

fn history_rebuild_inputs_at_height(
    db: &ZakuraDb,
    height: Height,
) -> Result<
    (
        Arc<Block>,
        zakura_chain::sapling::tree::Root,
        zakura_chain::orchard::tree::Root,
        zakura_chain::ironwood::tree::Root,
    ),
    RollbackFinalizedStateError,
> {
    let block = db
        .block(height.into())
        .ok_or(RollbackFinalizedStateError::MissingBlock { height })?;

    if let Some(roots) = db
        .commitment_roots_by_height_range(height..=height)
        .into_iter()
        .next()
    {
        return Ok((
            block,
            roots.sapling_root,
            roots.orchard_root,
            roots.ironwood_root,
        ));
    }

    let sapling_root = db
        .sapling_tree_by_height(&height)
        .ok_or(RollbackFinalizedStateError::MissingSaplingTree { height })?
        .root();
    let orchard_root = db
        .orchard_tree_by_height(&height)
        .ok_or(RollbackFinalizedStateError::MissingOrchardTree { height })?
        .root();
    let ironwood_root = db
        .ironwood_tree_by_height(&height)
        .ok_or(RollbackFinalizedStateError::MissingIronwoodTree { height })?
        .root();

    Ok((block, sapling_root, orchard_root, ironwood_root))
}

fn rebuild_treestate_to_height(
    db: &ZakuraDb,
    network: &Network,
    target_height: Height,
) -> Result<RebuiltTreestate, RollbackFinalizedStateError> {
    let mut note_commitment_trees = NoteCommitmentTrees::default();
    let mut history_tree = HistoryTree::default();
    let mut retained_sprout_roots = HashSet::new();

    for height in (Height::MIN.0..=target_height.0).map(Height) {
        let block = db
            .block(height.into())
            .ok_or(RollbackFinalizedStateError::MissingBlock { height })?;

        note_commitment_trees.update_trees_parallel(&block)?;
        retained_sprout_roots.insert(note_commitment_trees.sprout.root());

        let sapling_root = note_commitment_trees.sapling.root();
        let orchard_root = note_commitment_trees.orchard.root();
        let ironwood_root = note_commitment_trees.ironwood.root();
        history_tree.push(network, block, &sapling_root, &orchard_root, &ironwood_root)?;
    }

    Ok(RebuiltTreestate {
        sprout_tree: note_commitment_trees.sprout,
        history_tree,
        retained_sprout_roots: Some(retained_sprout_roots),
    })
}

fn deferred_pool_balance_change(
    height: Height,
    network: &Network,
) -> Result<Option<DeferredPoolBalanceChange>, RollbackFinalizedStateError> {
    if height <= network.slow_start_interval() {
        return Ok(None);
    }

    let deferred_amount = funding_stream_values(height, network, block_subsidy(height, network)?)?
        .remove(&FundingStreamReceiver::Deferred)
        .unwrap_or_default()
        .checked_sub(network.lockbox_disbursement_total_amount(height))
        .ok_or_else(|| {
            RollbackFinalizedStateError::DeferredPoolBalance(amount::Error::Constraint {
                value: i64::MIN,
                range: -zakura_chain::amount::MAX_MONEY..=zakura_chain::amount::MAX_MONEY,
            })
        })?;

    Ok(Some(DeferredPoolBalanceChange::new(deferred_amount)))
}

fn reverse_transparent_block(
    db: &ZakuraDb,
    network: &Network,
    batch: &mut DiskWriteBatch,
    address_balances: &mut HashMap<transparent::Address, Option<AddressBalanceLocation>>,
    height: Height,
    block: &Arc<Block>,
) -> Result<(), RollbackFinalizedStateError> {
    // Undo the forward write transaction-by-transaction in reverse order, un-crediting each
    // transaction's created outputs before un-debiting its spent inputs.
    //
    // The forward write debits inputs before crediting outputs within each transaction so that
    // every intermediate per-address balance stays within the consensus range, even for a
    // same-address self-spend chain whose credit-first intermediate balance would exceed MAX_MONEY
    // (see `prepare_transparent_transaction_batch`). Undoing the operations in the exact reverse
    // order retraces those same in-range intermediate balances, so the checked balance arithmetic
    // below cannot spuriously overflow or underflow.
    for (tx_index, transaction) in block.transactions.iter().enumerate().rev() {
        let tx_location = TransactionLocation::from_usize(height, tx_index);

        // Un-credit the outputs this transaction created.
        for (output_index, output) in transaction.outputs().iter().enumerate() {
            let created_output_location =
                OutputLocation::from_usize(height, tx_index, output_index);

            if let Some(address) = output.address(network) {
                let address_location =
                    cached_address_balance(db, address_balances, &address)?.address_location();

                batch.zs_delete(
                    db.db.cf_handle("tx_loc_by_transparent_addr_loc").unwrap(),
                    AddressTransaction::new(address_location, tx_location),
                );
                batch.zs_delete(
                    db.db.cf_handle("utxo_loc_by_transparent_addr_loc").unwrap(),
                    AddressUnspentOutput::new(address_location, created_output_location),
                );

                sub_address_balance(db, address_balances, &address, output.value())?;
                sub_address_received(db, address_balances, &address, output.value());
            }

            batch.zs_delete(
                db.db.cf_handle("utxo_by_out_loc").unwrap(),
                created_output_location,
            );
        }

        // Un-debit the outputs this transaction spent.
        for spent_outpoint in transaction.inputs().iter().filter_map(Input::outpoint) {
            let (spent_output_location, spent_utxo) = finalized_output(db, &spent_outpoint)?;

            if let Some(address) = spent_utxo.output.address(network) {
                let address_location =
                    cached_address_balance(db, address_balances, &address)?.address_location();

                batch.zs_delete(
                    db.db.cf_handle("tx_loc_by_transparent_addr_loc").unwrap(),
                    AddressTransaction::new(address_location, tx_location),
                );
                batch.zs_insert(
                    db.db.cf_handle("utxo_loc_by_transparent_addr_loc").unwrap(),
                    AddressUnspentOutput::new(address_location, spent_output_location),
                    (),
                );

                add_address_balance(db, address_balances, &address, spent_utxo.output.value())?;
            }

            batch.zs_insert(
                db.db.cf_handle("utxo_by_out_loc").unwrap(),
                spent_output_location,
                &spent_utxo.output,
            );
            batch.zs_delete(
                db.db.cf_handle(TX_LOC_BY_SPENT_OUT_LOC).unwrap(),
                spent_output_location,
            );
        }
    }

    Ok(())
}

#[allow(clippy::unwrap_in_result)]
fn finalized_output(
    db: &ZakuraDb,
    outpoint: &transparent::OutPoint,
) -> Result<(OutputLocation, transparent::Utxo), RollbackFinalizedStateError> {
    let transaction_location = db.transaction_location(outpoint.hash).ok_or(
        RollbackFinalizedStateError::MissingTransaction {
            hash: outpoint.hash,
        },
    )?;
    let transaction =
        db.transaction(outpoint.hash)
            .ok_or(RollbackFinalizedStateError::MissingTransaction {
                hash: outpoint.hash,
            })?;
    let output = transaction
        .0
        .outputs()
        .get(usize::try_from(outpoint.index).expect("valid output indexes fit in usize"))
        .ok_or(RollbackFinalizedStateError::MissingTransparentOutput {
            outpoint: *outpoint,
        })?
        .clone();
    let output_location = OutputLocation::from_outpoint(transaction_location, outpoint);

    Ok((
        output_location,
        transparent::Utxo::from_location(
            output,
            transaction_location.height,
            transaction_location.index.as_usize(),
        ),
    ))
}

fn cached_address_balance<'a>(
    db: &ZakuraDb,
    address_balances: &'a mut HashMap<transparent::Address, Option<AddressBalanceLocation>>,
    address: &transparent::Address,
) -> Result<&'a mut AddressBalanceLocation, RollbackFinalizedStateError> {
    if !address_balances.contains_key(address) {
        address_balances.insert(*address, db.address_balance_location(address));
    }

    address_balances
        .get_mut(address)
        .and_then(Option::as_mut)
        .ok_or(RollbackFinalizedStateError::MissingAddressBalance { address: *address })
}

fn add_address_balance(
    db: &ZakuraDb,
    address_balances: &mut HashMap<transparent::Address, Option<AddressBalanceLocation>>,
    address: &transparent::Address,
    value: Amount<NonNegative>,
) -> Result<(), RollbackFinalizedStateError> {
    let balance = cached_address_balance(db, address_balances, address)?;
    *balance.balance_mut() = (balance.balance() + value)?;
    Ok(())
}

fn sub_address_balance(
    db: &ZakuraDb,
    address_balances: &mut HashMap<transparent::Address, Option<AddressBalanceLocation>>,
    address: &transparent::Address,
    value: Amount<NonNegative>,
) -> Result<(), RollbackFinalizedStateError> {
    let balance = cached_address_balance(db, address_balances, address)?;
    *balance.balance_mut() = (balance.balance() - value)?;
    Ok(())
}

fn sub_address_received(
    db: &ZakuraDb,
    address_balances: &mut HashMap<transparent::Address, Option<AddressBalanceLocation>>,
    address: &transparent::Address,
    value: Amount<NonNegative>,
) {
    let balance = cached_address_balance(db, address_balances, address)
        .expect("address balance was loaded before subtracting received value");
    let value = u64::try_from(value.zatoshis()).expect("non-negative zatoshi amount fits in u64");
    *balance.received_mut() = balance.received().saturating_sub(value);
}

fn write_address_balances(
    db: &ZakuraDb,
    batch: &mut DiskWriteBatch,
    address_balances: HashMap<transparent::Address, Option<AddressBalanceLocation>>,
) {
    let balance_cf = db.db.cf_handle(BALANCE_BY_TRANSPARENT_ADDR).unwrap();

    for (address, balance) in address_balances {
        match balance {
            Some(balance) if balance.balance().is_zero() && balance.received() == 0 => {
                batch.zs_delete(&balance_cf, address);
            }
            Some(balance) => batch.zs_insert(&balance_cf, address, balance),
            None => batch.zs_delete(&balance_cf, address),
        }
    }
}

fn delete_shielded_block(db: &ZakuraDb, batch: &mut DiskWriteBatch, block: &Block) {
    let sprout_nullifiers = db.db.cf_handle("sprout_nullifiers").unwrap();
    let sapling_nullifiers = db.db.cf_handle("sapling_nullifiers").unwrap();
    let orchard_nullifiers = db.db.cf_handle("orchard_nullifiers").unwrap();
    let ironwood_nullifiers = db.db.cf_handle("ironwood_nullifiers").unwrap();

    for transaction in &block.transactions {
        for nullifier in transaction.sprout_nullifiers() {
            batch.zs_delete(&sprout_nullifiers, nullifier);
        }
        for nullifier in transaction.sapling_nullifiers() {
            batch.zs_delete(&sapling_nullifiers, nullifier);
        }
        for nullifier in transaction.orchard_nullifiers() {
            batch.zs_delete(&orchard_nullifiers, nullifier);
        }
        for nullifier in transaction.ironwood_nullifiers() {
            batch.zs_delete(&ironwood_nullifiers, nullifier);
        }
    }
}

/// Roll the Zakura header store back so it is consistent with `target_height`.
///
/// The block CFs rolled back above are keyed by the body chain, but the Zakura header store
/// (`zakura_header_*`) is maintained independently and races ahead of bodies. Rollback otherwise
/// leaves it untouched, so a rolled-back database keeps header rows — and a `BestHeaderTip` — far
/// above the new body tip. That inconsistency starves Zakura block-sync: `missing_block_bodies`
/// only offers heights that already have a stored header, so the contiguous floor body
/// (`target_height + 1`) is never requestable and body-sync stalls until it times out and falls
/// back to legacy ChainSync. Delete every Zakura header entry above `target_height`, scanning from
/// the (possibly higher) Zakura header tip down.
fn delete_zakura_headers_above(db: &ZakuraDb, batch: &mut DiskWriteBatch, target_height: Height) {
    let hash_by_height = db.db.cf_handle("zakura_header_hash_by_height").unwrap();
    let height_by_hash = db.db.cf_handle("zakura_header_height_by_hash").unwrap();
    let header_by_height = db.db.cf_handle("zakura_header_by_height").unwrap();
    let body_size_by_height = db
        .db
        .cf_handle("zakura_header_body_size_by_height")
        .unwrap();
    let Some((tip_height, _tip_hash)) = db
        .db
        .zs_last_key_value::<_, Height, block::Hash>(&hash_by_height)
    else {
        return;
    };

    for height in ((target_height.0 + 1)..=tip_height.0).map(Height) {
        if let Some(hash) = db.db.zs_get::<_, _, block::Hash>(&hash_by_height, &height) {
            batch.zs_delete(&height_by_hash, hash);
        }
        batch.zs_delete(&hash_by_height, height);
        batch.zs_delete(&header_by_height, height);
        batch.zs_delete(&body_size_by_height, height);
    }

    if let Ok(first_deleted) = target_height.next() {
        batch.delete_header_reorg_commitment_roots(db, first_deleted, tip_height);
    }
}

fn delete_block_and_transaction_data(
    db: &ZakuraDb,
    batch: &mut DiskWriteBatch,
    height: Height,
    block: &Block,
) -> Result<(), RollbackFinalizedStateError> {
    let block_hash = db
        .hash(height)
        .ok_or(RollbackFinalizedStateError::MissingBlock { height })?;

    batch.zs_delete(db.db.cf_handle("block_header_by_height").unwrap(), height);
    batch.zs_delete(db.db.cf_handle("hash_by_height").unwrap(), height);
    batch.zs_delete(db.db.cf_handle("height_by_hash").unwrap(), block_hash);
    batch.zs_delete(db.db.cf_handle(BLOCK_INFO).unwrap(), height);

    for tx_index in 0..block.transactions.len() {
        let tx_location = TransactionLocation::from_usize(height, tx_index);
        let tx_hash = db
            .transaction_hash(tx_location)
            .ok_or(RollbackFinalizedStateError::MissingBlock { height })?;

        batch.zs_delete(db.db.cf_handle("tx_by_loc").unwrap(), tx_location);
        batch.zs_delete(db.db.cf_handle("hash_by_tx_loc").unwrap(), tx_location);
        batch.zs_delete(db.db.cf_handle("tx_loc_by_hash").unwrap(), tx_hash);
    }

    Ok(())
}

fn reset_tip_trees(db: &ZakuraDb, batch: &mut DiskWriteBatch, treestate: &RebuiltTreestate) {
    // The sprout and history tip trees live in single-entry column families, so overwrite them
    // with the trees rebuilt up to the target height.
    batch.update_sprout_tree(db, &treestate.sprout_tree);
    batch.update_history_tree(db, &treestate.history_tree);

    // The sapling, orchard, and ironwood trees are height-keyed and de-duplicated: the forward write only
    // stores a tree when its root changes, and reads find the tip tree by searching backwards.
    // Deleting the trees above the target height (see `prune_tree_indexes`) therefore already
    // leaves the correct de-duplicated trees for the new tip. Writing a tree at the target height
    // here would instead create a duplicate entry whenever the target block added no notes, which
    // the de-duplicate-tree format check rejects on the next startup.
}

fn reset_value_pool(
    db: &ZakuraDb,
    batch: &mut DiskWriteBatch,
    value_pool: &ValueBalance<NonNegative>,
) {
    let _ = db
        .chain_value_pools_cf()
        .with_batch_for_writing(batch)
        .zs_insert(&(), value_pool);
}

fn prune_tree_indexes(
    db: &ZakuraDb,
    batch: &mut DiskWriteBatch,
    target_height: Height,
    retained_sprout_roots: &Option<HashSet<zakura_chain::sprout::tree::Root>>,
) {
    let retained_shielded_roots = retained_shielded_roots(db, target_height);

    let sapling_trees: BTreeMap<_, _> = db
        .sapling_tree_by_height_range((
            std::ops::Bound::Excluded(target_height),
            std::ops::Bound::Unbounded,
        ))
        .collect();
    for (height, tree) in sapling_trees {
        let root = tree.root();
        batch.delete_sapling_tree(db, &height);
        if !retained_shielded_roots.sapling.contains(&root) {
            batch.delete_sapling_anchor(db, &root);
        }
    }

    let orchard_trees: BTreeMap<_, _> = db
        .orchard_tree_by_height_range((
            std::ops::Bound::Excluded(target_height),
            std::ops::Bound::Unbounded,
        ))
        .collect();
    for (height, tree) in orchard_trees {
        let root = tree.root();
        batch.delete_orchard_tree(db, &height);
        if !retained_shielded_roots.orchard.contains(&root) {
            batch.delete_orchard_anchor(db, &root);
        }
    }

    let ironwood_trees: BTreeMap<_, _> = db
        .ironwood_tree_by_height_range((
            std::ops::Bound::Excluded(target_height),
            std::ops::Bound::Unbounded,
        ))
        .collect();
    for (height, tree) in ironwood_trees {
        let root = tree.root();
        batch.delete_ironwood_tree(db, &height);
        if !retained_shielded_roots.ironwood.contains(&root) {
            batch.delete_ironwood_anchor(db, &root);
        }
    }

    // Fast-sync writes anchors and this root index without writing per-height trees. Use the
    // index to remove anchors introduced only by rolled-back fast-path heights before truncating
    // it, but retain any repeated root that is still valid at or below the target.
    prune_fast_commitment_anchors_from_index(db, batch, target_height, &retained_shielded_roots);

    // Truncate the per-height commitment-roots serving index above the target, so a rolled-back
    // database does not serve roots for heights it no longer holds.
    batch.truncate_commitment_roots_after(db, target_height);

    // Delete every sapling/orchard/ironwood subtree whose notes extend past the target height. Subtree
    // indexes are read back from the database and number far fewer than `u16::MAX`, so `index.0 + 1`
    // (the exclusive end of the single-index delete range) cannot overflow.
    for (index, _) in db
        .sapling_subtree_list_by_index_range(..)
        .into_iter()
        .filter(|(_, subtree)| subtree.end_height > target_height)
    {
        batch.delete_range_sapling_subtree(db, index, NoteCommitmentSubtreeIndex(index.0 + 1));
    }

    for (index, _) in db
        .orchard_subtree_list_by_index_range(..)
        .into_iter()
        .filter(|(_, subtree)| subtree.end_height > target_height)
    {
        batch.delete_range_orchard_subtree(db, index, NoteCommitmentSubtreeIndex(index.0 + 1));
    }

    for (index, _) in db
        .ironwood_subtree_list_by_index_range(..)
        .into_iter()
        .filter(|(_, subtree)| subtree.end_height > target_height)
    {
        batch.delete_range_ironwood_subtree(db, index, NoteCommitmentSubtreeIndex(index.0 + 1));
    }

    // Sprout has no by-height anchor index, so enumerate every anchor and drop the ones not seen
    // while rebuilding the tree up to the target. This loads each historical sprout tree, but the
    // sprout pool has been inactive since before Sapling, so the anchor set is small and fixed.
    if let Some(retained_sprout_roots) = retained_sprout_roots {
        for (root, _) in db.sprout_trees_full_map() {
            if !retained_sprout_roots.contains(&root) {
                batch.delete_sprout_anchor(db, &root);
            }
        }
    }

    let next_height = Height(target_height.0 + 1);
    batch.delete_range_history_tree(db, &next_height, &Height::MAX);
    batch.delete_range_sprout_tree(db, &next_height, &Height::MAX);
}

#[derive(Default)]
struct RetainedShieldedRoots {
    sapling: HashSet<sapling::tree::Root>,
    orchard: HashSet<orchard::tree::Root>,
    ironwood: HashSet<ironwood::tree::Root>,
}

fn retained_shielded_roots(db: &ZakuraDb, target_height: Height) -> RetainedShieldedRoots {
    let mut retained = RetainedShieldedRoots::default();

    for (_height, roots) in db.commitment_roots_for_migration(..=target_height) {
        retained.sapling.insert(roots.sapling_root);
        retained.orchard.insert(roots.orchard_root);
        retained.ironwood.insert(roots.ironwood_root);
    }

    for (_height, tree) in db.sapling_tree_by_height_range(..=target_height) {
        retained.sapling.insert(tree.root());
    }

    for (_height, tree) in db.orchard_tree_by_height_range(..=target_height) {
        retained.orchard.insert(tree.root());
    }

    for (_height, tree) in db.ironwood_tree_by_height_range(..=target_height) {
        retained.ironwood.insert(tree.root());
    }

    retained
}

fn prune_fast_commitment_anchors_from_index(
    db: &ZakuraDb,
    batch: &mut DiskWriteBatch,
    target_height: Height,
    retained_roots: &RetainedShieldedRoots,
) {
    let rolled_back_roots = db.commitment_roots_for_migration((
        std::ops::Bound::Excluded(target_height),
        std::ops::Bound::Unbounded,
    ));

    for (_height, roots) in rolled_back_roots {
        if !retained_roots.sapling.contains(&roots.sapling_root) {
            batch.delete_sapling_anchor(db, &roots.sapling_root);
        }
        if !retained_roots.orchard.contains(&roots.orchard_root) {
            batch.delete_orchard_anchor(db, &roots.orchard_root);
        }
        if !retained_roots.ironwood.contains(&roots.ironwood_root) {
            batch.delete_ironwood_anchor(db, &roots.ironwood_root);
        }
    }
}

fn clear_backup_dir(path: &PathBuf) -> Result<(), std::io::Error> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    std::fs::create_dir_all(path)
}

#[cfg(test)]
mod tests {
    use zakura_chain::serialization::ZcashDeserializeInto;

    use crate::service::finalized_state::disk_format::RawBytes;

    use super::*;

    fn ephemeral_mainnet_db() -> ZakuraDb {
        let network = Network::Mainnet;
        ZakuraDb::new(
            &Config::ephemeral(),
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &network,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("opening the finalized state database should succeed")
    }

    fn sapling_note_commitment(value: u64) -> sapling::tree::NoteCommitmentUpdate {
        let mut bytes = [0; 32];
        bytes[..8].copy_from_slice(&value.to_le_bytes());

        Option::<sapling::tree::NoteCommitmentUpdate>::from(
            sapling::tree::NoteCommitmentUpdate::from_bytes(&bytes),
        )
        .expect("small little-endian integers are canonical Jubjub field elements")
    }

    fn sapling_root(value: u64) -> sapling::tree::Root {
        let mut tree = sapling::tree::NoteCommitmentTree::default();
        tree.append(sapling_note_commitment(value))
            .expect("single-note Sapling tree is not full");
        tree.root()
    }

    fn orchard_root(value: u64) -> orchard::tree::Root {
        let mut tree = orchard::tree::NoteCommitmentTree::default();
        tree.append(halo2::pasta::pallas::Base::from(value))
            .expect("single-note Orchard tree is not full");
        tree.root()
    }

    fn ironwood_root(value: u64) -> zakura_chain::ironwood::tree::Root {
        let mut tree = zakura_chain::ironwood::tree::NoteCommitmentTree::default();
        tree.append(halo2::pasta::pallas::Base::from(value))
            .expect("single-note Ironwood tree is not full");
        tree.root()
    }

    /// Fast-path VCT commits write Sapling/Orchard/Ironwood anchors and the compact
    /// `commitment_roots_by_height` index, but skip per-height tree rows. Rollback must
    /// therefore prune stale anchors from the index before truncating it; otherwise anchors
    /// from rolled-back fast commits stay valid for contextual verification.
    #[test]
    fn prune_tree_indexes_drops_fast_index_anchors_above_target() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_mainnet_db();

        let retained_sapling = sapling_root(1);
        let removed_sapling = sapling_root(2);
        let retained_orchard = orchard_root(1);
        let removed_orchard = orchard_root(2);
        let retained_ironwood = ironwood_root(1);
        let removed_ironwood = ironwood_root(2);

        let mut batch = DiskWriteBatch::new();
        batch.insert_sapling_anchor(&db, &retained_sapling);
        batch.insert_sapling_anchor(&db, &removed_sapling);
        batch.insert_orchard_anchor(&db, &retained_orchard);
        batch.insert_orchard_anchor(&db, &removed_orchard);
        batch.insert_ironwood_anchor(&db, &retained_ironwood);
        batch.insert_ironwood_anchor(&db, &removed_ironwood);

        // Heights 1 and 2 are retained. Height 3 is rolled back and has a unique stale
        // anchor. Height 4 is also rolled back, but repeats the retained root, so its anchor
        // must remain valid after the index row is truncated.
        batch.insert_commitment_roots_by_height(
            &db,
            Height(1),
            &retained_sapling,
            &retained_orchard,
            &retained_ironwood,
            0,
            0,
            0,
            &zakura_chain::block::merkle::AuthDataRoot::from([0u8; 32]),
        );
        batch.insert_commitment_roots_by_height(
            &db,
            Height(2),
            &retained_sapling,
            &retained_orchard,
            &retained_ironwood,
            0,
            0,
            0,
            &zakura_chain::block::merkle::AuthDataRoot::from([0u8; 32]),
        );
        batch.insert_commitment_roots_by_height(
            &db,
            Height(3),
            &removed_sapling,
            &removed_orchard,
            &removed_ironwood,
            0,
            0,
            0,
            &zakura_chain::block::merkle::AuthDataRoot::from([0u8; 32]),
        );
        batch.insert_commitment_roots_by_height(
            &db,
            Height(4),
            &retained_sapling,
            &retained_orchard,
            &retained_ironwood,
            0,
            0,
            0,
            &zakura_chain::block::merkle::AuthDataRoot::from([0u8; 32]),
        );
        db.write_batch(batch)
            .expect("seeding fast-path roots succeeds");

        let mut batch = DiskWriteBatch::new();
        prune_tree_indexes(&db, &mut batch, Height(2), &None);
        db.write_batch(batch)
            .expect("pruning fast-path roots succeeds");

        assert!(
            db.contains_sapling_anchor(&retained_sapling),
            "rollback retains Sapling anchors still valid at or below the target"
        );
        assert!(
            db.contains_orchard_anchor(&retained_orchard),
            "rollback retains Orchard anchors still valid at or below the target"
        );
        assert!(
            !db.contains_sapling_anchor(&removed_sapling),
            "rollback removes Sapling anchors introduced only by rolled-back fast commits"
        );
        assert!(
            !db.contains_orchard_anchor(&removed_orchard),
            "rollback removes Orchard anchors introduced only by rolled-back fast commits"
        );
        assert!(
            db.contains_ironwood_anchor(&retained_ironwood),
            "rollback retains Ironwood anchors still valid at or below the target"
        );
        assert!(
            !db.contains_ironwood_anchor(&removed_ironwood),
            "rollback removes Ironwood anchors introduced only by rolled-back fast commits"
        );
        assert_eq!(
            db.commitment_roots_by_height_range(Height(1)..=Height(4))
                .into_iter()
                .map(|roots| roots.height)
                .collect::<Vec<_>>(),
            vec![Height(1), Height(2)],
            "rollback truncates the serving index above the target"
        );
    }

    /// `vct_tree_absent` marks exactly the half-open band `[U, H)`: heights below the upgrade
    /// height `U` keep their pre-upgrade trees, and heights at or above the handoff `H` get trees
    /// again from semantic sync. With no handoff marker the database is a normal archive and no
    /// height is ever absent.
    #[test]
    fn vct_tree_absent_marks_only_the_upgrade_to_handoff_band() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_mainnet_db();

        // No markers: a normally-synced archive database, never absent.
        assert!(!db.vct_tree_absent(Height(0)));
        assert!(!db.vct_tree_absent(Height(100)));

        // Upgrade U = 4, handoff H = 10: per-height trees absent exactly in [4, 10).
        let mut batch = DiskWriteBatch::new();
        batch.update_vct_upgrade_marker(&db, Height(4));
        batch.update_vct_sync_marker(&db, Height(10));
        db.write_batch(batch).expect("seeding vct markers succeeds");

        assert!(
            !db.vct_tree_absent(Height(3)),
            "below U: the pre-upgrade tree is present"
        );
        assert!(db.vct_tree_absent(Height(4)), "at U: the tree is absent");
        assert!(db.vct_tree_absent(Height(9)), "below H: the tree is absent");
        assert!(
            !db.vct_tree_absent(Height(10)),
            "at H: the handoff tree is present"
        );
        assert!(
            !db.vct_tree_absent(Height(11)),
            "above H: the semantic-sync tree is present"
        );
    }

    /// When the upgrade height is at or above the handoff — a node upgraded after the last
    /// checkpoint, where semantic sync keeps writing trees — the band `[U, H)` is empty, so every
    /// height is servable regardless of the upgrade height.
    #[test]
    fn vct_tree_absent_empty_band_when_upgraded_above_handoff() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_mainnet_db();

        let mut batch = DiskWriteBatch::new();
        batch.update_vct_upgrade_marker(&db, Height(15));
        batch.update_vct_sync_marker(&db, Height(10));
        db.write_batch(batch).expect("seeding vct markers succeeds");

        for height in [0, 9, 10, 12, 15, 20] {
            assert!(
                !db.vct_tree_absent(Height(height)),
                "U >= H leaves an empty band, so height {height} is servable"
            );
        }
    }

    /// `ironwood_tree_by_height` returns `None` in the `[U, H)` absent band, matching its
    /// Sapling/Orchard siblings — without the guard, the backward search would silently
    /// return the genesis-backfilled (or pre-upgrade) tree instead.
    #[test]
    fn ironwood_tree_by_height_is_guarded_in_the_absent_band() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_mainnet_db();

        // Genesis's Ironwood tree row (real databases always have one: written at genesis
        // commit, or backfilled by the `add_ironwood_tree` upgrade), and a finalized tip at
        // height 10 so heights up to 10 are within the "known" range `ironwood_tree_by_height`
        // searches, rather than short-circuiting on "above the tip".
        let mut batch = DiskWriteBatch::new();
        batch.create_ironwood_tree(
            &db,
            &Height(0),
            &zakura_chain::ironwood::tree::NoteCommitmentTree::default(),
        );
        let hash_by_height = db.db().cf_handle("hash_by_height").unwrap();
        batch.zs_insert(&hash_by_height, Height(10), block::Hash([10; 32]));
        db.write_batch(batch)
            .expect("seeding genesis tree and finalized tip succeeds");

        assert!(
            db.ironwood_tree_by_height(&Height(0)).is_some(),
            "before any vct markers, the seeded genesis tree is present"
        );

        // Upgrade U = 4, handoff H = 10: per-height trees absent exactly in [4, 10). Without
        // the backward-search guard, a read in this band would silently return the height-0
        // row above instead of `None`.
        let mut batch = DiskWriteBatch::new();
        batch.update_vct_upgrade_marker(&db, Height(4));
        batch.update_vct_sync_marker(&db, Height(10));
        db.write_batch(batch).expect("seeding vct markers succeeds");

        assert!(
            db.ironwood_tree_by_height(&Height(3)).is_some(),
            "below U: the pre-upgrade tree is present"
        );
        assert!(
            db.ironwood_tree_by_height(&Height(4)).is_none(),
            "at U: the tree is absent"
        );
        assert!(
            db.ironwood_tree_by_height(&Height(9)).is_none(),
            "below H: the tree is absent"
        );
    }

    /// `serve_block_roots` reads a request that starts at or above the upgrade height `U` straight
    /// from the serving index, without touching the per-height trees.
    #[test]
    fn serve_block_roots_serves_at_or_above_upgrade_from_index() {
        let _init_guard = zakura_test::init();
        let db = ephemeral_mainnet_db();

        // Index covers [4, 6]; the upgrade height is U = 4.
        let mut batch = DiskWriteBatch::new();
        batch.update_vct_upgrade_marker(&db, Height(4));
        for height in 4u32..=6 {
            batch.insert_commitment_roots_by_height(
                &db,
                Height(height),
                &sapling_root(height.into()),
                &orchard_root(height.into()),
                &zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
                0,
                0,
                0,
                &zakura_chain::block::merkle::AuthDataRoot::from([0u8; 32]),
            );
        }
        db.write_batch(batch)
            .expect("seeding the serving index succeeds");

        let served = crate::service::finalized_state::serve_block_roots(&db, Height(4)..=Height(6));
        assert_eq!(
            served
                .into_iter()
                .map(|root| root.height)
                .collect::<Vec<_>>(),
            vec![Height(4), Height(5), Height(6)],
            "a request at or above U is served from the index"
        );
    }

    /// `delete_zakura_headers_above` must truncate every Zakura header CF above the target,
    /// including the hash→height index, while leaving rows at or below the target intact. This
    /// is the consistency guarantee that lets a rolled-back snapshot re-sync bodies from its tip
    /// instead of stalling on an un-requestable floor (see the function doc).
    #[test]
    fn delete_zakura_headers_above_truncates_the_header_store() {
        let _init_guard = zakura_test::init();

        let db = ephemeral_mainnet_db();

        // A real header value for `zakura_header_by_height`; the height math is what matters, so
        // every seeded height can reuse the same header.
        let header = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into::<Block>()
            .expect("mainnet genesis test vector deserializes")
            .header;

        let hash_by_height = db.db.cf_handle("zakura_header_hash_by_height").unwrap();
        let height_by_hash = db.db.cf_handle("zakura_header_height_by_hash").unwrap();
        let header_by_height = db.db.cf_handle("zakura_header_by_height").unwrap();
        let body_size_by_height = db
            .db
            .cf_handle("zakura_header_body_size_by_height")
            .unwrap();

        // Distinct hash per height so the hash→height index entries are independent.
        let hash_at = |h: u32| block::Hash([h as u8; 32]);

        // Seed heights 1..=5 across all four Zakura header CFs.
        let mut batch = DiskWriteBatch::new();
        for h in 1..=5u32 {
            let height = Height(h);
            let hash = hash_at(h);
            batch.zs_insert(&hash_by_height, height, hash);
            batch.zs_insert(&height_by_hash, hash, height);
            batch.zs_insert(&header_by_height, height, &header);
            // The value type is irrelevant to deletion-by-key; reuse `Height` as a stand-in.
            batch.zs_insert(&body_size_by_height, height, height);
            batch.insert_commitment_roots_by_height(
                &db,
                height,
                &sapling_root(h.into()),
                &orchard_root(h.into()),
                &ironwood_root(h.into()),
                0,
                0,
                0,
                &zakura_chain::block::merkle::AuthDataRoot::from([0u8; 32]),
            );
        }
        db.write_batch(batch)
            .expect("seeding the header store succeeds");

        // Roll the Zakura header store back to height 3.
        let mut batch = DiskWriteBatch::new();
        delete_zakura_headers_above(&db, &mut batch, Height(3));
        db.write_batch(batch)
            .expect("truncating the header store succeeds");

        // Heights 1..=3 (<= target) are retained across every CF, including the index.
        for h in 1..=3u32 {
            let height = Height(h);
            assert!(
                db.db
                    .zs_get::<_, _, block::Hash>(&hash_by_height, &height)
                    .is_some(),
                "hash_by_height retains height {h}",
            );
            assert!(
                db.db
                    .zs_get::<_, _, RawBytes>(&header_by_height, &height)
                    .is_some(),
                "header_by_height retains height {h}",
            );
            assert!(
                db.db
                    .zs_get::<_, _, Height>(&body_size_by_height, &height)
                    .is_some(),
                "body_size_by_height retains height {h}",
            );
            assert!(
                db.db
                    .zs_get::<_, _, Height>(&height_by_hash, &hash_at(h))
                    .is_some(),
                "height_by_hash retains the index for height {h}",
            );
            assert!(
                db.commitment_roots(height).is_some(),
                "commitment roots retain height {h}",
            );
        }

        // Heights 4..=5 (> target) are gone from every CF, including the hash→height index.
        for h in 4..=5u32 {
            let height = Height(h);
            assert!(
                db.db
                    .zs_get::<_, _, block::Hash>(&hash_by_height, &height)
                    .is_none(),
                "hash_by_height drops height {h}",
            );
            assert!(
                db.db
                    .zs_get::<_, _, RawBytes>(&header_by_height, &height)
                    .is_none(),
                "header_by_height drops height {h}",
            );
            assert!(
                db.db
                    .zs_get::<_, _, Height>(&body_size_by_height, &height)
                    .is_none(),
                "body_size_by_height drops height {h}",
            );
            assert!(
                db.db
                    .zs_get::<_, _, Height>(&height_by_hash, &hash_at(h))
                    .is_none(),
                "height_by_hash drops the index for height {h}",
            );
            assert!(
                db.commitment_roots(height).is_none(),
                "commitment roots drop height {h}",
            );
        }
    }

    /// On a database with no Zakura header rows, truncation is a no-op (and must not panic on the
    /// empty-tip lookup).
    #[test]
    fn delete_zakura_headers_above_is_a_noop_on_an_empty_store() {
        let _init_guard = zakura_test::init();

        let db = ephemeral_mainnet_db();

        let mut batch = DiskWriteBatch::new();
        delete_zakura_headers_above(&db, &mut batch, Height(3));
        db.write_batch(batch)
            .expect("an empty truncate batch writes cleanly");
    }
}
