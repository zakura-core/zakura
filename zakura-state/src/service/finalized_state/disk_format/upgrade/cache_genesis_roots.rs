//! Updating the genesis note commitment trees to cache their roots.
//!
//! This reduces CPU usage when the genesis tree roots are used for transaction validation.
//! Since mempool transactions are cheap to create, this is a potential remote denial of service.

use crossbeam_channel::{Receiver, TryRecvError};
use zakura_chain::{block::Height, sprout};

use crate::service::finalized_state::{disk_db::DiskWriteBatch, ZakuraDb};

use super::CancelFormatChange;

/// Runs disk format upgrade for changing the sprout and history tree key types.
///
/// Returns `Ok` if the upgrade completed, and `Err` if it was cancelled.
///
/// # Panics
///
/// If the state is empty.
#[allow(clippy::unwrap_in_result)]
#[instrument(skip(upgrade_db, cancel_receiver))]
pub fn run(
    _initial_tip_height: Height,
    upgrade_db: &ZakuraDb,
    cancel_receiver: &Receiver<CancelFormatChange>,
) -> Result<(), CancelFormatChange> {
    let sprout_genesis_tree = sprout::tree::NoteCommitmentTree::default();
    let sprout_tip_tree = upgrade_db.sprout_tree_for_tip();

    let sapling_genesis_tree = upgrade_db
        .sapling_tree_by_height(&Height(0))
        .expect("caller has checked for genesis block");
    let orchard_genesis_tree = upgrade_db
        .orchard_tree_by_height(&Height(0))
        .expect("caller has checked for genesis block");

    // Writing the trees back to the database automatically caches their roots.
    let mut batch = DiskWriteBatch::new();

    // Fix the cached root of the Sprout genesis tree in its anchors column family.

    // It's ok to write the genesis tree to the tip tree index, because it's overwritten by
    // the actual tip before the batch is written to the database.
    batch.update_sprout_tree(upgrade_db, &sprout_genesis_tree);
    // This method makes sure the sprout tip tree has a cached root, even if it's the genesis tree.
    batch.update_sprout_tree(upgrade_db, &sprout_tip_tree);

    batch.create_sapling_tree(upgrade_db, &Height(0), &sapling_genesis_tree);
    batch.create_orchard_tree(upgrade_db, &Height(0), &orchard_genesis_tree);

    // Return before we write if the upgrade is cancelled.
    if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
        return Err(CancelFormatChange);
    }

    upgrade_db
        .write_batch(batch)
        .expect("updating tree cached roots should always succeed");

    Ok(())
}

/// Quickly check that the genesis trees and sprout tip tree have cached roots.
///
/// This allows us to fail the upgrade quickly in tests and during development,
/// rather than waiting to see if it failed.
///
/// # Panics
///
/// If the state is empty.
pub fn quick_check(db: &ZakuraDb) -> Result<(), String> {
    // An empty database doesn't have any trees, so its format is trivially correct.
    if db.is_empty() {
        return Ok(());
    }

    // A fast-synced database deliberately has no per-height note-commitment trees
    // below the checkpoint handoff height, including the genesis trees this check
    // reads. The genesis-root-caching invariant does not apply to it.
    if db.is_vct_synced() {
        return Ok(());
    }

    // A fast-sync commit can set the VCT marker after the `is_vct_synced()`
    // check above but before these tree reads. In that case the readers return
    // `None` for the absent per-height trees, and this genesis-root check no
    // longer applies. Recheck the marker on each `None`: Sprout anchor lookup
    // also returns `None` for a genuinely missing non-VCT anchor, which must not
    // be mistaken for the race.
    let sprout_genesis_tree = sprout::tree::NoteCommitmentTree::default();
    let Some(sprout_genesis_tree) = db.sprout_tree_by_anchor(&sprout_genesis_tree.root()) else {
        return missing_genesis_tree_or_vct_race(db, "sprout");
    };
    let sprout_tip_tree = db.sprout_tree_for_tip();

    let Some(sapling_genesis_tree) = db.sapling_tree_by_height(&Height(0)) else {
        return missing_genesis_tree_or_vct_race(db, "sapling");
    };
    let Some(orchard_genesis_tree) = db.orchard_tree_by_height(&Height(0)) else {
        return missing_genesis_tree_or_vct_race(db, "orchard");
    };

    // Check the entire format before returning any errors.
    let sprout_result = sprout_genesis_tree
        .cached_root()
        .ok_or("no cached root in sprout genesis tree");
    let sprout_tip_result = sprout_tip_tree
        .cached_root()
        .ok_or("no cached root in sprout tip tree");

    let sapling_result = sapling_genesis_tree
        .cached_root()
        .ok_or("no cached root in sapling genesis tree");
    let orchard_result = orchard_genesis_tree
        .cached_root()
        .ok_or("no cached root in orchard genesis tree");

    if sprout_result.is_err()
        || sprout_tip_result.is_err()
        || sapling_result.is_err()
        || orchard_result.is_err()
    {
        let err = Err(format!(
            "missing cached genesis root: sprout: {sprout_result:?}, {sprout_tip_result:?} \
             sapling: {sapling_result:?}, orchard: {orchard_result:?}"
        ));
        warn!(?err);
        return err;
    }

    Ok(())
}

fn missing_genesis_tree_or_vct_race(db: &ZakuraDb, pool: &'static str) -> Result<(), String> {
    if db.is_vct_synced() {
        Ok(())
    } else {
        Err(format!("missing {pool} genesis tree in non-VCT database"))
    }
}

/// Detailed check that all trees have cached roots.
///
/// # Panics
///
/// If the state is empty.
pub fn detailed_check(
    db: &ZakuraDb,
    cancel_receiver: &Receiver<CancelFormatChange>,
) -> Result<Result<(), String>, CancelFormatChange> {
    // A fast-synced database deliberately has no per-height note-commitment trees
    // below the checkpoint handoff height, so the per-height tree scans below do
    // not apply to it.
    if db.is_vct_synced() {
        return Ok(Ok(()));
    }

    // This is redundant in some code paths, but not in others. But it's quick anyway.
    // Check the entire format before returning any errors.
    let mut result = quick_check(db);

    for (root, tree) in db.sprout_trees_full_map() {
        // Return early if the format check is cancelled.
        if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
            return Err(CancelFormatChange);
        }

        if tree.cached_root().is_none() {
            result = Err(format!(
                "found un-cached sprout tree root after running genesis tree root fix \
                 {root:?}"
            ));
            error!(?result);
        }
    }

    for (height, tree) in db.sapling_tree_by_height_range(..) {
        // Return early if the format check is cancelled.
        if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
            return Err(CancelFormatChange);
        }

        if tree.cached_root().is_none() {
            result = Err(format!(
                "found un-cached sapling tree root after running genesis tree root fix \
                 {height:?}"
            ));
            error!(?result);
        }
    }

    for (height, tree) in db.orchard_tree_by_height_range(..) {
        // Return early if the format check is cancelled.
        if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
            return Err(CancelFormatChange);
        }

        if tree.cached_root().is_none() {
            result = Err(format!(
                "found un-cached orchard tree root after running genesis tree root fix \
                 {height:?}"
            ));
            error!(?result);
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use zakura_chain::{
        block::Block, orchard, parameters::Network, sapling, serialization::ZcashDeserializeInto,
    };

    use crate::{
        constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
        request::{FinalizedBlock, Treestate},
        service::finalized_state::STATE_COLUMN_FAMILIES_IN_CODE,
        CheckpointVerifiedBlock, Config,
    };

    use super::*;

    fn db_with_genesis_trees() -> ZakuraDb {
        let db = ZakuraDb::new(
            &Config::ephemeral(),
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &Network::Mainnet,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("opening an ephemeral finalized state database should succeed");
        let genesis = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into::<Arc<Block>>()
            .expect("mainnet genesis block deserializes");
        let finalized = FinalizedBlock::from_checkpoint_verified(
            CheckpointVerifiedBlock::from(genesis),
            Treestate::default(),
        );

        let mut batch = DiskWriteBatch::new();
        batch
            .prepare_block_header_and_transaction_data_batch(&db, &finalized, true, None)
            .expect("genesis header and transaction rows are valid");
        batch.update_sprout_tree(&db, &sprout::tree::NoteCommitmentTree::default());
        batch.create_sapling_tree(
            &db,
            &Height::MIN,
            &sapling::tree::NoteCommitmentTree::default(),
        );
        batch.create_orchard_tree(
            &db,
            &Height::MIN,
            &orchard::tree::NoteCommitmentTree::default(),
        );
        db.write_batch(batch)
            .expect("genesis-format rows write successfully");

        db
    }

    #[test]
    fn quick_check_rejects_missing_sprout_genesis_anchor_without_vct_marker() {
        let db = db_with_genesis_trees();
        assert!(quick_check(&db).is_ok());

        let mut batch = DiskWriteBatch::new();
        batch.delete_sprout_anchor(&db, &sprout::tree::NoteCommitmentTree::default().root());
        db.write_batch(batch)
            .expect("deleting the test Sprout anchor succeeds");

        let error = quick_check(&db).expect_err("missing non-VCT Sprout anchor must fail");
        assert_eq!(error, "missing sprout genesis tree in non-VCT database");
    }
}
