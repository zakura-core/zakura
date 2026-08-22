//! Removes transaction-location indexes for spent outputs and revealed nullifiers.

use crossbeam_channel::{Receiver, TryRecvError};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use zakura_chain::block::Height;

use crate::service::finalized_state::ZakuraDb;

use super::{super::super::DiskWriteBatch, CancelFormatChange, FormatChangeError};

fn delete_transparent_spend_indexes(zakura_db: &ZakuraDb) -> Result<(), FormatChangeError> {
    zakura_db
        .tx_loc_by_spent_output_loc_cf()
        .new_batch_for_writing()
        .zs_delete_range(
            &crate::OutputLocation::from_output_index(crate::TransactionLocation::MIN, 0),
            // This upper bound is exclusive, but its transaction and output indexes cannot occur
            // in a valid block because they exceed the consensus block size limit.
            &crate::OutputLocation::from_output_index(crate::TransactionLocation::MAX, u32::MAX),
        )
        .write_batch()
        .map_err(|error| FormatChangeError::MigrationStorage(error.to_string()))
}

/// Removes transaction-location indexes for spent outputs and revealed nullifiers.
///
/// Returns `Ok` if the upgrade completed, and an error if it was cancelled or the transparent
/// range deletion failed.
#[allow(clippy::unwrap_in_result)]
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
                .expect("unexpected database write failure");

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
        config::database_format_version_on_disk,
        constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
        service::finalized_state::{
            column_family::register_typed_batch_write_error, FinalizedState,
            STATE_COLUMN_FAMILIES_IN_CODE, TX_LOC_BY_SPENT_OUT_LOC,
        },
        CheckpointVerifiedBlock, Config, StateInitError,
    };
    use zakura_chain::{block, parameters::Network, serialization::ZcashDeserializeInto};
    use zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES;

    fn persistent_config() -> (tempfile::TempDir, Config) {
        let cache = tempfile::tempdir().expect("temporary cache directory is created");
        let config = Config {
            cache_dir: cache.path().to_owned(),
            ephemeral: false,
            debug_skip_non_finalized_state_backup_task: true,
            ..Config::default()
        };
        (cache, config)
    }

    fn open_persistent(
        config: &Config,
        debug_skip_format_upgrades: bool,
    ) -> Result<ZakuraDb, StateInitError> {
        ZakuraDb::new(
            config,
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &Network::Mainnet,
            debug_skip_format_upgrades,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
    }

    fn mainnet_state_with_genesis(config: &Config) -> FinalizedState {
        let mut state = FinalizedState::new(config, &Network::Mainnet)
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
        state
    }

    fn injected_rocksdb_error() -> rocksdb::Error {
        match rocksdb::DB::open_default(std::path::Path::new("\0")) {
            Ok(_) => panic!("a path containing a null byte must be rejected"),
            Err(error) => error,
        }
    }

    #[test]
    fn range_delete_error_preserves_indexer_marker_and_retries_on_startup() {
        let (_cache, config) = persistent_config();
        let state = mainnet_state_with_genesis(&config);
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

        assert_eq!(db.finalized_tip_height(), Some(Height::MIN));
        assert_eq!(
            db.format_version_on_disk()
                .expect("the indexed fixture version is readable"),
            Some(indexed_version.clone())
        );
        let db_path = db.path().to_owned();
        drop(state);

        let injected_error = injected_rocksdb_error();
        let injected_error_message = injected_error.to_string();
        let write_error =
            register_typed_batch_write_error(db_path, TX_LOC_BY_SPENT_OUT_LOC, injected_error);
        let Err(StateInitError::DatabaseFormatUpgrade { source, .. }) =
            open_persistent(&config, false)
        else {
            panic!("a transparent range-delete failure must stop database startup");
        };
        assert!(matches!(
            source.downcast_ref::<FormatChangeError>(),
            Some(FormatChangeError::MigrationStorage(message))
                if message == &injected_error_message
        ));
        assert_eq!(
            database_format_version_on_disk(
                &config,
                STATE_DATABASE_KIND,
                running_version.major,
                &Network::Mainnet,
            )
            .expect("the indexed version remains readable"),
            Some(indexed_version.clone()),
            "failed index removal must preserve the +indexer marker"
        );

        let preserved = open_persistent(&config, true)
            .expect("the failed removal database opens when format changes are disabled");
        assert_eq!(
            preserved.tx_location_by_spent_output_location(&spent_output),
            Some(spending_transaction),
            "the injected failure must leave the transparent index entry readable"
        );
        drop(preserved);

        drop(write_error);
        let reopened = open_persistent(&config, false)
            .expect("the next writable startup retries index removal");
        assert_eq!(
            reopened
                .format_version_on_disk()
                .expect("the updated version is readable"),
            Some(running_version),
            "successful retry must remove the +indexer marker"
        );
        assert_eq!(
            reopened.tx_location_by_spent_output_location(&spent_output),
            None,
            "the marker must not be cleared until the transparent rebuild skip-trigger is removed"
        );
    }
}
