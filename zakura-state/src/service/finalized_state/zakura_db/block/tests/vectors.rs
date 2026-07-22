//! Fixed database test vectors for blocks and transactions.
//!
//! These tests check that the database correctly serializes
//! and deserializes large heights, blocks and transactions.
//!
//! # TODO
//!
//! Test large blocks and transactions with shielded data,
//! including data activated in Overwinter and later network upgrades.
//!
//! Check transparent address indexes, UTXOs, etc.

use std::{iter, sync::Arc};

use zakura_chain::{
    block::{
        tests::generate::{
            large_multi_transaction_block, large_single_transaction_block_many_inputs,
            large_single_transaction_block_many_outputs,
        },
        Block, Height,
    },
    parameters::Network::{self, *},
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    transparent::new_ordered_outputs_with_height,
};
use zakura_test::vectors::{MAINNET_BLOCKS, TESTNET_BLOCKS};

use crate::{
    constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
    request::{FinalizedBlock, Treestate},
    service::finalized_state::{
        disk_db::DiskWriteBatch, ZakuraDb, PRUNING_METADATA, STATE_COLUMN_FAMILIES_IN_CODE,
    },
    CheckpointVerifiedBlock, Config, SemanticallyVerifiedBlock,
};

/// Storage round-trip test for block and transaction data in the finalized state database.
#[test]
fn test_block_db_round_trip() {
    let mainnet_test_cases = MAINNET_BLOCKS
        .values()
        .map(|block| block.zcash_deserialize_into().unwrap());
    let testnet_test_cases = TESTNET_BLOCKS
        .values()
        .map(|block| block.zcash_deserialize_into().unwrap());

    test_block_db_round_trip_with(&Mainnet, mainnet_test_cases);
    test_block_db_round_trip_with(&Network::new_default_testnet(), testnet_test_cases);

    // It doesn't matter if these blocks are mainnet or testnet,
    // because there is no validation at this level of the database.
    //
    // These blocks have the same height and header hash, so they each need a new state.
    test_block_db_round_trip_with(&Mainnet, iter::once(large_multi_transaction_block()));

    // These blocks are unstable under serialization, so we apply a round-trip first.
    //
    // TODO: fix the bug in the generated test vectors.
    let block = large_single_transaction_block_many_inputs();
    let block_data = block
        .zcash_serialize_to_vec()
        .expect("serialization to vec never fails");
    let block: Block = block_data
        .zcash_deserialize_into()
        .expect("deserialization of valid serialized block never fails");
    test_block_db_round_trip_with(&Mainnet, iter::once(block));

    let block = large_single_transaction_block_many_outputs();
    let block_data = block
        .zcash_serialize_to_vec()
        .expect("serialization to vec never fails");
    let block: Block = block_data
        .zcash_deserialize_into()
        .expect("deserialization of valid serialized block never fails");
    test_block_db_round_trip_with(&Mainnet, iter::once(block));
}

fn test_block_db_round_trip_with(
    network: &Network,
    block_test_cases: impl IntoIterator<Item = Block>,
) {
    let _init_guard = zakura_test::init();

    let state = ZakuraDb::new(
        &Config::ephemeral(),
        STATE_DATABASE_KIND,
        &state_database_format_version_in_code(),
        network,
        // The raw database accesses in this test create invalid database formats.
        true,
        STATE_COLUMN_FAMILIES_IN_CODE
            .iter()
            .map(ToString::to_string),
        false,
    )
    .expect("opening an ephemeral database should succeed");

    // Check that each block round-trips to the database
    for original_block in block_test_cases.into_iter() {
        // First, check that the block round-trips without using the database
        let block_data = original_block
            .zcash_serialize_to_vec()
            .expect("serialization to vec never fails");
        let round_trip_block: Block = block_data
            .zcash_deserialize_into()
            .expect("deserialization of valid serialized block never fails");
        let round_trip_data = round_trip_block
            .zcash_serialize_to_vec()
            .expect("serialization to vec never fails");

        assert_eq!(
            original_block, round_trip_block,
            "test block structure must round-trip",
        );
        assert_eq!(
            block_data, round_trip_data,
            "test block data must round-trip",
        );

        // Now, use the database
        let original_block = Arc::new(original_block);
        let checkpoint_verified = if original_block.coinbase_height().is_some() {
            CheckpointVerifiedBlock::from(original_block.clone())
        } else {
            // Fake a zero height
            let hash = original_block.hash();
            let transaction_hashes: Arc<[_]> = original_block
                .transactions
                .iter()
                .map(|tx| tx.hash())
                .collect();
            let new_outputs =
                new_ordered_outputs_with_height(&original_block, Height(0), &transaction_hashes);

            CheckpointVerifiedBlock(SemanticallyVerifiedBlock {
                block: original_block.clone(),
                hash,
                height: Height(0),
                new_outputs,
                transaction_hashes,
                deferred_pool_balance_change: None,
                auth_data_root: None,
            })
        };

        let dummy_treestate = Treestate::default();
        let finalized =
            FinalizedBlock::from_checkpoint_verified(checkpoint_verified, dummy_treestate);

        // Skip validation by writing the block directly to the database
        let mut batch = DiskWriteBatch::new();
        batch
            .prepare_block_header_and_transaction_data_batch(&state, &finalized, true, None)
            .expect("test block header and transaction batch is valid");
        state.db.write(batch).expect("block is valid for writing");

        // Now read it back from the state
        let stored_block = state
            .block(finalized.height.into())
            .expect("block was stored at height");

        if stored_block != original_block {
            error!(
                "
                detailed block mismatch report:
                original: {:?}\n\
                original data: {:?}\n\
                stored: {:?}\n\
                stored data: {:?}\n\
                ",
                original_block,
                hex::encode(original_block.zcash_serialize_to_vec().unwrap()),
                stored_block,
                hex::encode(stored_block.zcash_serialize_to_vec().unwrap()),
            );
        }

        assert_eq!(stored_block, original_block);
    }
}

/// Missing pruning metadata means this is an archive database from before pruning was added.
#[test]
fn missing_pruning_metadata_cf_is_archive_database() {
    let _init_guard = zakura_test::init();

    let column_families_without_pruning_metadata = STATE_COLUMN_FAMILIES_IN_CODE
        .iter()
        .filter(|cf_name| **cf_name != PRUNING_METADATA)
        .map(ToString::to_string);

    let state = ZakuraDb::new(
        &Config::ephemeral(),
        STATE_DATABASE_KIND,
        &state_database_format_version_in_code(),
        &Mainnet,
        true,
        column_families_without_pruning_metadata,
        false,
    )
    .expect("opening the finalized state database should succeed");

    assert!(state.lowest_retained_height().is_none());
    assert!(!state.is_pruned());
}

/// POC (verified-commitment-trees): the anchor-only fast write produces the same
/// `sapling_anchors` / `orchard_anchors` contents as the legacy full write, while
/// skipping the per-height note-commitment tree CFs, and is idempotent.
/// See `docs/design/verified-commitment-trees.md`.
#[test]
fn vct_anchor_only_write_matches_legacy_and_skips_per_height_trees() {
    use zakura_chain::{orchard, sapling};

    fn ephemeral_db() -> ZakuraDb {
        ZakuraDb::new(
            &Config::ephemeral(),
            STATE_DATABASE_KIND,
            &state_database_format_version_in_code(),
            &Mainnet,
            true,
            STATE_COLUMN_FAMILIES_IN_CODE
                .iter()
                .map(ToString::to_string),
            false,
        )
        .expect("opening the finalized state database should succeed")
    }

    let sapling_tree = sapling::tree::NoteCommitmentTree::default();
    let orchard_tree = orchard::tree::NoteCommitmentTree::default();
    let sapling_root = sapling_tree.root();
    let orchard_root = orchard_tree.root();

    // Legacy path: the full write inserts the anchor *and* a per-height tree at each
    // of two heights (the anchor set collapses to one key; two tree entries).
    let legacy = ephemeral_db();
    {
        let mut batch = DiskWriteBatch::new();
        batch.create_sapling_tree(&legacy, &Height(10), &sapling_tree);
        batch.create_sapling_tree(&legacy, &Height(11), &sapling_tree);
        batch.create_orchard_tree(&legacy, &Height(10), &orchard_tree);
        legacy.db.write(batch).expect("legacy batch writes");
    }

    // Fast path: anchor-only writes for the same roots, no per-height trees.
    let fast = ephemeral_db();
    {
        let mut batch = DiskWriteBatch::new();
        batch.insert_sapling_anchor(&fast, &sapling_root);
        batch.insert_orchard_anchor(&fast, &orchard_root);
        fast.db.write(batch).expect("fast batch writes");
    }

    // The anchor sets are byte-identical (same count, same digest): the fast
    // anchor-only write reproduces exactly the legacy anchor index.
    assert_eq!(
        legacy.vct_anchor_digest(),
        fast.vct_anchor_digest(),
        "fast anchor-only write must match legacy anchor set"
    );

    // The fast DB skipped the per-height Sapling tree CF; the legacy DB did not.
    let count_sapling_trees = |db: &ZakuraDb| -> usize {
        let cf = db.db.cf_handle("sapling_note_commitment_tree").unwrap();
        db.db
            .zs_forward_range_iter::<_, Height, sapling::tree::NoteCommitmentTree, _>(&cf, ..)
            .count()
    };
    assert_eq!(
        count_sapling_trees(&legacy),
        2,
        "legacy path writes a per-height tree at each height"
    );
    assert_eq!(
        count_sapling_trees(&fast),
        0,
        "fast path skips per-height trees entirely"
    );

    // Re-inserting an unchanged root is idempotent (anchor CF is a set).
    let before = fast.vct_anchor_digest();
    {
        let mut batch = DiskWriteBatch::new();
        batch.insert_sapling_anchor(&fast, &sapling_root);
        fast.db.write(batch).expect("idempotent write");
    }
    assert_eq!(
        fast.vct_anchor_digest(),
        before,
        "anchor insert is idempotent"
    );
}
