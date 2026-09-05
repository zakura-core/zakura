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
        request::{FinalizedBlock, Treestate},
        service::finalized_state::{
            column_family::register_typed_batch_write_error,
            disk_format::upgrade::track_tx_locs_by_spends, DiskWriteBatch, FinalizedState,
            WriteDisk, STATE_COLUMN_FAMILIES_IN_CODE, TX_LOC_BY_SPENT_OUT_LOC,
        },
        CheckpointVerifiedBlock, Config, StateInitError,
    };
    use zakura_chain::{
        block, orchard, parameters::Network, serialization::ZcashDeserializeInto, transaction,
        transparent,
    };
    use zakura_test::vectors::{BLOCK_TESTNET_1842468_BYTES, BLOCK_TESTNET_GENESIS_BYTES};

    struct SpendIndexFixture {
        spent_outpoint: transparent::OutPoint,
        orchard_nullifier: orchard::Nullifier,
        spending_transaction: crate::TransactionLocation,
        spending_transaction_hash: transaction::Hash,
    }

    fn test_network() -> Network {
        Network::new_default_testnet()
    }

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
            &test_network(),
            debug_skip_format_upgrades,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
    }

    fn testnet_state_with_genesis(config: &Config) -> (FinalizedState, Arc<block::Block>) {
        let mut state =
            FinalizedState::new(config, &test_network()).expect("temporary finalized state opens");
        let genesis: Arc<block::Block> = BLOCK_TESTNET_GENESIS_BYTES
            .zcash_deserialize_into()
            .expect("testnet genesis deserializes");
        state
            .commit_finalized_direct(
                CheckpointVerifiedBlock::from(genesis.clone()).into(),
                None,
                None,
                "index removal range-delete failure test",
            )
            .expect("testnet genesis commits");
        (state, genesis)
    }

    fn seed_mixed_spend_indexes(db: &ZakuraDb, genesis: &Arc<block::Block>) -> SpendIndexFixture {
        let source_block: Arc<block::Block> = BLOCK_TESTNET_1842468_BYTES
            .zcash_deserialize_into()
            .expect("mixed-spend testnet block deserializes");
        let mixed_transaction = source_block
            .transactions
            .iter()
            .find(|transaction| {
                transaction
                    .inputs()
                    .iter()
                    .any(|input| input.outpoint().is_some())
                    && transaction.orchard_nullifiers().next().is_some()
            })
            .cloned()
            .expect("testnet block 1,842,468 contains a transparent and Orchard spend");
        let spent_outpoints: Vec<_> = mixed_transaction
            .inputs()
            .iter()
            .filter_map(|input| input.outpoint())
            .collect();
        assert_eq!(
            spent_outpoints.len(),
            1,
            "the mixed-spend test vector has one transparent input"
        );
        let spent_outpoint = spent_outpoints[0];
        let orchard_nullifier = mixed_transaction
            .orchard_nullifiers()
            .next()
            .cloned()
            .expect("the mixed-spend test vector has an Orchard nullifier");
        let spending_transaction = crate::TransactionLocation::from_index(Height::MIN, 1);
        let spending_transaction_hash = mixed_transaction.hash();

        let synthetic_block = Arc::new(block::Block {
            header: genesis.header.clone(),
            transactions: vec![genesis.transactions[0].clone(), mixed_transaction],
        });
        let finalized = FinalizedBlock::from_checkpoint_verified(
            CheckpointVerifiedBlock::from(synthetic_block),
            Treestate::default(),
        );
        let mut batch = DiskWriteBatch::new();
        batch
            .prepare_block_header_and_transaction_data_batch(db, &finalized, true, None)
            .expect("the synthetic mixed-spend block is valid test data");

        let source_transaction = crate::TransactionLocation::from_index(Height::MIN, 0);
        let tx_loc_by_hash = db.db().cf_handle("tx_loc_by_hash").unwrap();
        batch.zs_insert(&tx_loc_by_hash, spent_outpoint.hash, source_transaction);
        let spent_output =
            crate::OutputLocation::from_outpoint(source_transaction, &spent_outpoint);
        let _ = db
            .tx_loc_by_spent_output_loc_cf()
            .with_batch_for_writing(&mut batch)
            .zs_insert(&spent_output, &spending_transaction);
        let orchard_nullifiers = db.db().cf_handle("orchard_nullifiers").unwrap();
        batch.zs_insert(&orchard_nullifiers, orchard_nullifier, spending_transaction);
        db.write_batch(batch)
            .expect("mixed transparent and Orchard spend indexes write");

        SpendIndexFixture {
            spent_outpoint,
            orchard_nullifier,
            spending_transaction,
            spending_transaction_hash,
        }
    }

    fn injected_rocksdb_error() -> rocksdb::Error {
        match rocksdb::DB::open_default(std::path::Path::new("\0")) {
            Ok(_) => panic!("a path containing a null byte must be rejected"),
            Err(error) => error,
        }
    }

    #[test]
    fn range_delete_error_preserves_and_rebuilds_spend_indexes() {
        let (_cache, config) = persistent_config();
        let (state, genesis) = testnet_state_with_genesis(&config);
        let db = &state.db;
        let fixture = seed_mixed_spend_indexes(db, &genesis);
        let running_version = state_database_format_version_in_code();
        let mut indexed_version = running_version.clone();
        indexed_version.build = BuildMetadata::new("indexer").expect("indexer is valid metadata");
        db.update_format_version_on_disk(&indexed_version)
            .expect("indexed fixture version writes");

        assert_eq!(db.finalized_tip_height(), Some(Height::MIN));
        assert_eq!(
            db.spending_tx_loc(&fixture.spent_outpoint),
            Some(fixture.spending_transaction)
        );
        assert_eq!(
            db.orchard_revealing_tx_loc(&fixture.orchard_nullifier),
            Some(fixture.spending_transaction)
        );
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
                &test_network(),
            )
            .expect("the indexed version remains readable"),
            Some(indexed_version.clone()),
            "failed index removal must preserve the +indexer marker"
        );

        let preserved = open_persistent(&config, true)
            .expect("the failed removal database opens when format changes are disabled");
        assert_eq!(
            preserved.spending_tx_loc(&fixture.spent_outpoint),
            Some(fixture.spending_transaction),
            "the injected failure must leave the transparent index entry readable"
        );
        assert_eq!(
            preserved.orchard_revealing_tx_loc(&fixture.orchard_nullifier),
            Some(fixture.spending_transaction),
            "a stopped removal must leave the shielded index entry readable"
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
            reopened.spending_tx_loc(&fixture.spent_outpoint),
            None,
            "the marker must not be cleared until the transparent rebuild skip-trigger is removed"
        );
        assert_eq!(
            reopened.orchard_revealing_tx_loc(&fixture.orchard_nullifier),
            None,
            "successful removal must clear the Orchard spend location"
        );
        assert!(
            reopened.contains_orchard_nullifier(&fixture.orchard_nullifier),
            "index removal must preserve the consensus nullifier"
        );

        reopened
            .update_format_version_on_disk(&indexed_version)
            .expect("the test indexer marker writes before rebuilding");
        let (_cancel_sender, cancel_receiver) = crossbeam_channel::bounded(1);
        track_tx_locs_by_spends::run(Height::MIN, &reopened, &cancel_receiver)
            .expect("the real index rebuild loop succeeds");

        assert_eq!(
            reopened
                .format_version_on_disk()
                .expect("the rebuilt version is readable"),
            Some(indexed_version),
            "the rebuilt database retains the +indexer marker"
        );
        assert_eq!(
            reopened.spending_tx_loc(&fixture.spent_outpoint),
            Some(fixture.spending_transaction),
            "the transparent spend location is rebuilt"
        );
        assert_eq!(
            reopened.orchard_revealing_tx_loc(&fixture.orchard_nullifier),
            Some(fixture.spending_transaction),
            "the Orchard spend location is rebuilt"
        );
        assert_eq!(
            reopened.transaction_hash(fixture.spending_transaction),
            Some(fixture.spending_transaction_hash),
            "both rebuilt spend locations resolve to the spending transaction"
        );
    }
}
