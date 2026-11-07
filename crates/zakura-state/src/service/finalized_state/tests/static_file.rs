//! Tests for moving finalized transaction bodies into static files.

use std::{path::Path, sync::Arc};

use tempfile::TempDir;
use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{Block, Height},
    parameters::{Network, NetworkKind},
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    transparent::Address,
};

use crate::{
    config::{Config, StorageMode},
    rollback_finalized_state,
    service::finalized_state::{
        disk_format::{block::TransactionLocation, FromDisk},
        static_file::{StaticFiles, STATIC_FILE_DIR},
        zakura_db::static_file::STATIC_FILE_MOVE_THRESHOLD,
        CheckpointVerifiedBlock, FinalizedState, RawBytes, ZakuraDb,
    },
    RollbackFinalizedStateOptions, StateInitError,
};

use super::rollback::{child_block, coinbase_tx};

/// The tip of the test chain. Moves happen at heights 49 and 99, so blocks
/// `0..=99` end up in static files and `100..=TEST_TIP` stay in `tx_by_loc`.
const TEST_TIP: u32 = 120;

/// The last block moved into static files by committing the test chain.
const MOVED_TIP: Height = Height(2 * STATIC_FILE_MOVE_THRESHOLD - 1);

/// Returns a mainnet-shaped chain of `0..=TEST_TIP` with one or two coinbase-like
/// transactions per block, so blocks have different transaction counts.
fn test_chain() -> Vec<Arc<Block>> {
    let address = Address::from_script_hash(NetworkKind::Mainnet, [0x42; 20]);
    let genesis: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into()
        .expect("mainnet genesis test vector deserializes");

    let mut blocks = vec![genesis];
    for height in 1..=TEST_TIP {
        let value = Amount::<NonNegative>::try_from(1_000 + i64::from(height))
            .expect("test amounts are valid");
        let mut transactions = vec![coinbase_tx(Height(height), value, &address)];
        if height % 3 == 0 {
            // A second coinbase-shaped transaction varies the transaction count. The
            // checkpoint commit path stores it without contextual checks.
            transactions.push(coinbase_tx(Height(height), value, &address));
        }
        let parent = blocks.last().expect("the chain has a genesis block");
        blocks.push(child_block(parent, transactions));
    }
    blocks
}

/// Returns an archive config for a persistent database in `dir`.
fn config_at(dir: &Path) -> Config {
    Config {
        cache_dir: dir.to_path_buf(),
        ephemeral: false,
        storage_mode: StorageMode::Archive,
        ..Config::default()
    }
}

/// Commits `blocks` through the checkpoint path.
fn commit(state: &mut FinalizedState, blocks: &[Arc<Block>]) {
    for block in blocks {
        state
            .commit_finalized_direct(
                CheckpointVerifiedBlock::from(block.clone()).into(),
                None,
                None,
                "static file tests",
            )
            .expect("committing a test block succeeds");
    }
}

/// Returns the heights that still have rows in `tx_by_loc`.
fn tx_by_loc_heights(db: &ZakuraDb) -> Vec<Height> {
    let mut heights: Vec<Height> = db
        .raw_column_family_entries_for_test("tx_by_loc")
        .into_iter()
        .map(|(key, _)| TransactionLocation::from_bytes(key).height)
        .collect();
    heights.dedup();
    heights
}

/// Checks every block and transaction of `blocks` reads back from `db`.
fn assert_chain_reads(db: &ZakuraDb, blocks: &[Arc<Block>]) {
    for (height, block) in blocks.iter().enumerate() {
        let height = Height(u32::try_from(height).expect("test heights fit in u32"));
        assert!(db.contains_body_at_height(height), "body at {height:?}");
        let stored = db
            .stored_block(height.into())
            .unwrap_or_else(|| panic!("block {height:?} is readable"));
        assert_eq!(stored.as_ref(), block.as_ref(), "block {height:?}");

        for transaction in &block.transactions {
            let (stored, stored_height, _) = db
                .stored_transaction(transaction.hash())
                .expect("transaction is readable");
            assert_eq!(stored_height, height);
            assert_eq!(stored.as_ref(), transaction.as_ref());
        }
    }
}

#[test]
fn committed_bodies_move_to_static_files_in_batches() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let blocks = test_chain();
    let dir = TempDir::new().expect("temp dir");

    let mut state = FinalizedState::new(&config_at(dir.path()), &network).expect("state opens");
    commit(&mut state, &blocks);

    assert_eq!(state.db.static_file_transactions_tip(), Some(MOVED_TIP));
    assert_eq!(
        tx_by_loc_heights(&state.db),
        (MOVED_TIP.0 + 1..=TEST_TIP).map(Height).collect::<Vec<_>>(),
        "moved blocks are deleted from tx_by_loc",
    );
    assert_chain_reads(&state.db, &blocks);

    // A range across the static file tip merges both stores in location order.
    let start = TransactionLocation::min_for_height(Height(MOVED_TIP.0 - 3));
    let end = TransactionLocation::max_for_height(Height(MOVED_TIP.0 + 3));
    let expected: Vec<_> = blocks[MOVED_TIP.0 as usize - 3..=MOVED_TIP.0 as usize + 3]
        .iter()
        .flat_map(|block| block.transactions.iter().map(|tx| tx.hash()))
        .collect();
    let read: Vec<_> = state
        .db
        .stored_transactions_by_location_range(start..=end)
        .map(|(_, transaction)| transaction.hash())
        .collect();
    assert_eq!(read, expected);
    drop(state);

    let state = FinalizedState::new(&config_at(dir.path()), &network).expect("state reopens");
    assert_eq!(state.db.static_file_transactions_tip(), Some(MOVED_TIP));
    assert_chain_reads(&state.db, &blocks);
}

#[test]
fn reopening_truncates_static_files_the_database_did_not_record() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let blocks = test_chain();
    let dir = TempDir::new().expect("temp dir");
    let config = config_at(dir.path());

    let static_dir = {
        let mut state = FinalizedState::new(&config, &network).expect("state opens");
        commit(&mut state, &blocks);
        state.db.path().join(STATIC_FILE_DIR)
    };

    // A crash after the static file commit, before the database batch: the
    // blocks are in static files but still in `tx_by_loc`, and the database
    // tip didn't move.
    let ahead: Vec<_> = blocks[MOVED_TIP.0 as usize + 1..]
        .iter()
        .enumerate()
        .map(|(offset, block)| {
            let height = Height(MOVED_TIP.0 + 1 + u32::try_from(offset).expect("small offset"));
            let transactions = block
                .transactions
                .iter()
                .map(|tx| RawBytes::new_raw_bytes(tx.zcash_serialize_to_vec().unwrap()))
                .collect();
            (height, transactions)
        })
        .collect();
    StaticFiles::open(static_dir.clone(), true, 500_000)
        .expect("static files open")
        .append_blocks(&ahead)
        .expect("uncommitted blocks append");

    let state = FinalizedState::new(&config, &network).expect("state reopens");
    assert_eq!(state.db.static_file_transactions_tip(), Some(MOVED_TIP));
    assert_eq!(
        StaticFiles::open(static_dir, false, 500_000).unwrap().tip(),
        Some(MOVED_TIP),
        "the database wins",
    );
    assert_chain_reads(&state.db, &blocks);
}

#[test]
fn reopening_refuses_static_files_behind_the_database() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let dir = TempDir::new().expect("temp dir");
    let config = config_at(dir.path());

    let static_dir = {
        let mut state = FinalizedState::new(&config, &network).expect("state opens");
        commit(&mut state, &test_chain());
        state.db.path().join(STATIC_FILE_DIR)
    };
    std::fs::remove_dir_all(static_dir).expect("static files can be deleted");

    assert!(matches!(
        FinalizedState::new(&config, &network),
        Err(StateInitError::StaticFilesBehindDatabase { .. })
    ));
}

#[test]
fn rollback_truncates_static_files() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let blocks = test_chain();
    let dir = TempDir::new().expect("temp dir");
    let config = config_at(dir.path());
    let target = Height(60);

    {
        let mut state = FinalizedState::new(&config, &network).expect("state opens");
        commit(&mut state, &blocks);
    }
    rollback_finalized_state(
        config.clone(),
        &network,
        RollbackFinalizedStateOptions {
            target_height: target,
            keep_rolled_back_blocks: false,
            max_checkpoint_height: None,
        },
    )
    .expect("rollback succeeds");

    let mut state = FinalizedState::new(&config, &network).expect("state reopens");
    assert_eq!(state.db.static_file_transactions_tip(), Some(target));
    assert_eq!(state.db.finalized_tip_height(), Some(target));
    assert_chain_reads(&state.db, &blocks[..=target.0 as usize]);
    assert!(!state.db.contains_body_at_height(Height(target.0 + 1)));

    // The rolled back blocks commit and move again, in a batch that starts
    // after the rollback target.
    commit(&mut state, &blocks[target.0 as usize + 1..]);
    assert_eq!(
        state.db.static_file_transactions_tip(),
        Some(Height(target.0 + STATIC_FILE_MOVE_THRESHOLD)),
    );
    assert_chain_reads(&state.db, &blocks);
}
