//! Tracks transaction locations by their inputs and revealed nullifiers.

use crossbeam_channel::{Receiver, TryRecvError};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use zakura_chain::block::Height;

use crate::service::finalized_state::ZakuraDb;

use super::{super::super::DiskWriteBatch, CancelFormatChange, FormatChangeError};

#[cfg(all(test, not(feature = "indexer")))]
type RangeDeleteHook = std::sync::Arc<dyn Fn() -> Result<(), String> + Send + Sync>;

#[cfg(all(test, not(feature = "indexer")))]
static RANGE_DELETE_HOOKS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, RangeDeleteHook>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

#[cfg(all(test, not(feature = "indexer")))]
struct RangeDeleteHookGuard {
    database_path: std::path::PathBuf,
}

#[cfg(all(test, not(feature = "indexer")))]
impl Drop for RangeDeleteHookGuard {
    fn drop(&mut self) {
        RANGE_DELETE_HOOKS
            .lock()
            .expect("range-delete hook mutex is not poisoned")
            .remove(&self.database_path);
    }
}

#[cfg(all(test, not(feature = "indexer")))]
fn register_range_delete_hook(
    database_path: impl Into<std::path::PathBuf>,
    hook: RangeDeleteHook,
) -> RangeDeleteHookGuard {
    let database_path = database_path.into();
    let replaced = RANGE_DELETE_HOOKS
        .lock()
        .expect("range-delete hook mutex is not poisoned")
        .insert(database_path.clone(), hook);
    assert!(
        replaced.is_none(),
        "database already has a range-delete hook"
    );

    RangeDeleteHookGuard { database_path }
}

#[cfg(all(test, not(feature = "indexer")))]
fn run_range_delete_hook(database_path: &std::path::Path) -> Result<(), String> {
    let hook = RANGE_DELETE_HOOKS
        .lock()
        .map_err(|_| "range-delete hook mutex is poisoned".to_string())?
        .get(database_path)
        .cloned();
    hook.map_or(Ok(()), |hook| hook())
}

fn delete_transparent_spend_indexes(zakura_db: &ZakuraDb) -> Result<(), FormatChangeError> {
    #[cfg(all(test, not(feature = "indexer")))]
    run_range_delete_hook(zakura_db.path()).map_err(FormatChangeError::MigrationStorage)?;

    zakura_db
        .tx_loc_by_spent_output_loc_cf()
        .new_batch_for_writing()
        .zs_delete_range(
            &crate::OutputLocation::from_output_index(crate::TransactionLocation::MIN, 0),
            &crate::OutputLocation::from_output_index(crate::TransactionLocation::MAX, u32::MAX),
        )
        .write_batch()
        .map_err(|error| FormatChangeError::MigrationStorage(error.to_string()))
}

/// Runs disk format upgrade for tracking transaction locations by their inputs and revealed nullifiers.
///
/// Returns `Ok` if the upgrade completed, and an error if it was cancelled or a write failed.
#[instrument(skip(zakura_db, cancel_receiver))]
pub fn run(
    initial_finalized_tip_height: Height,
    zakura_db: &ZakuraDb,
    cancel_receiver: &Receiver<CancelFormatChange>,
) -> Result<(), FormatChangeError> {
    if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
        return Err(CancelFormatChange.into());
    }

    delete_transparent_spend_indexes(zakura_db)?;

    if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
        return Err(CancelFormatChange.into());
    }

    (0..=initial_finalized_tip_height.0)
        .into_par_iter()
        .try_for_each(|height| {
            let height = Height(height);
            let mut batch = DiskWriteBatch::new();

            let transactions = zakura_db.transactions_by_location_range(
                crate::TransactionLocation::from_index(height, 1)
                    ..=crate::TransactionLocation::max_for_height(height),
            );

            for (_tx_loc, tx) in transactions {
                if tx.is_coinbase() {
                    continue;
                }

                batch.prepare_nullifier_batch(zakura_db, &tx);
            }

            if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
                return Err(CancelFormatChange.into());
            }

            zakura_db
                .write_batch(batch)
                .map_err(|error| FormatChangeError::MigrationStorage(error.to_string()))?;

            if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
                return Err(CancelFormatChange.into());
            }

            Ok(())
        })
}

#[cfg(all(test, not(feature = "indexer")))]
mod tests {
    use std::sync::Arc;

    use semver::BuildMetadata;

    use super::*;
    use crate::{
        constants::state_database_format_version_in_code,
        service::finalized_state::{
            disk_format::upgrade::{DbFormatChange, FormatChangeError},
            FinalizedState,
        },
        CheckpointVerifiedBlock, Config,
    };
    use zakura_chain::{block, parameters::Network, serialization::ZcashDeserializeInto};
    use zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES;

    fn mainnet_state_with_genesis() -> (tempfile::TempDir, FinalizedState) {
        let cache = tempfile::tempdir().expect("temporary cache directory is created");
        let config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: false,
            debug_skip_non_finalized_state_backup_task: true,
            ..Config::default()
        };
        let mut state = FinalizedState::new(&config, &Network::Mainnet)
            .expect("temporary finalized state opens");
        let genesis: Arc<block::Block> = BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into()
            .expect("mainnet genesis deserializes");
        state
            .commit_finalized_direct(
                CheckpointVerifiedBlock::from(genesis).into(),
                None,
                None,
                "index removal range-delete failure test",
            )
            .expect("mainnet genesis commits");
        (cache, state)
    }

    #[test]
    fn range_delete_error_preserves_the_indexer_marker() {
        let (_cache, state) = mainnet_state_with_genesis();
        let db = &state.db;
        let running_version = state_database_format_version_in_code();
        let mut indexed_version = running_version.clone();
        indexed_version.build = BuildMetadata::new("indexer").expect("indexer is valid metadata");
        db.update_format_version_on_disk(&indexed_version)
            .expect("indexed fixture version writes");

        let spent_output = crate::OutputLocation::from_output_index(
            crate::TransactionLocation::from_index(Height::MIN, 1),
            0,
        );
        let spending_transaction = crate::TransactionLocation::from_index(Height::MIN, 2);
        db.tx_loc_by_spent_output_loc_cf()
            .new_batch_for_writing()
            .zs_insert(&spent_output, &spending_transaction)
            .write_batch()
            .expect("transparent index fixture writes");

        let hook = register_range_delete_hook(
            db.path(),
            std::sync::Arc::new(|| Err("injected transparent range-delete failure".to_string())),
        );
        let (_cancel_tx, cancel_rx) = crossbeam_channel::bounded(1);
        let format_change =
            DbFormatChange::open_database(&running_version, Some(indexed_version.clone()));

        assert_eq!(db.finalized_tip_height(), Some(Height::MIN));
        assert_eq!(
            db.format_version_on_disk()
                .expect("the indexed fixture version is readable"),
            Some(indexed_version.clone())
        );
        assert_eq!(
            format_change,
            DbFormatChange::CheckOpenCurrent {
                running_version: running_version.clone()
            }
        );

        let result = format_change.run_format_change_or_check(db, Some(Height::MIN), &cancel_rx);

        assert!(
            matches!(
                &result,
                Err(FormatChangeError::MigrationStorage(message))
                    if message == "injected transparent range-delete failure"
            ),
            "a transparent range-delete failure must stop index removal: {result:?}"
        );
        assert_eq!(
            db.format_version_on_disk()
                .expect("the indexed version remains readable"),
            Some(indexed_version),
            "failed index removal must preserve the +indexer marker"
        );
        assert_eq!(
            db.tx_location_by_spent_output_location(&spent_output),
            Some(spending_transaction),
            "the injected failure must leave the transparent index entry readable"
        );

        drop(hook);
        format_change
            .run_format_change_or_check(db, Some(Height::MIN), &cancel_rx)
            .expect("index removal retries after the injected failure is cleared");
        assert_eq!(
            db.format_version_on_disk()
                .expect("the updated version is readable"),
            Some(running_version),
            "successful retry must remove the +indexer marker"
        );
        assert_eq!(
            db.tx_location_by_spent_output_location(&spent_output),
            None,
            "successful retry must remove the transparent index entry"
        );
    }
}
