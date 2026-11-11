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
        disk_db::{DiskWriteBatch, WriteDisk},
        disk_format::{block::TransactionLocation, FromDisk},
        static_file::{StaticFiles, STATIC_FILE_DIR},
        zakura_db::static_file::{StaticFileMover, STATIC_FILE_MOVE_THRESHOLD},
        CheckpointVerifiedBlock, FinalizedState, RawBytes, ZakuraDb,
    },
    RollbackFinalizedStateOptions, StateInitError,
};

use super::rollback::{child_block, coinbase_tx};

/// The tip of the test chain. Moves happen at heights 49 and 99, so blocks
/// `0..=99` end up in static files and `100..=TEST_TIP` stay in `tx_by_loc`.
const TEST_TIP: u32 = 120;

/// The last block moved into static files by committing the test chain. Moves
/// start at height 1, because genesis stays in `tx_by_loc`.
const MOVED_TIP: Height = Height(2 * STATIC_FILE_MOVE_THRESHOLD);

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

/// Commits `blocks` through the checkpoint path, waiting for the static file
/// mover after each one, so moves happen at the same heights in every run.
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
        state.static_file_mover.wait();
    }
}

/// Returns the raw transactions of `blocks` at `heights`, as static files store them.
fn raw_blocks(
    blocks: &[Arc<Block>],
    heights: std::ops::RangeInclusive<u32>,
) -> Vec<(Height, Vec<RawBytes>)> {
    heights
        .map(|height| {
            let transactions = blocks[height as usize]
                .transactions
                .iter()
                .map(|tx| RawBytes::new_raw_bytes(tx.zcash_serialize_to_vec().unwrap()))
                .collect();
            (Height(height), transactions)
        })
        .collect()
}

/// Writes `raw` to `tx_by_loc` at `location`, like a row a deleted SST file
/// brought back.
fn plant_tx_by_loc_row(db: &ZakuraDb, location: TransactionLocation, raw: RawBytes) {
    let tx_by_loc = db
        .db()
        .cf_handle("tx_by_loc")
        .expect("column family exists");
    let mut batch = DiskWriteBatch::new();
    batch.zs_insert(&tx_by_loc, location, raw);
    db.write_batch(batch).expect("batch writes");
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
        std::iter::once(Height(0))
            .chain((MOVED_TIP.0 + 1..=TEST_TIP).map(Height))
            .collect::<Vec<_>>(),
        "moved blocks are deleted from tx_by_loc, and genesis never moves",
    );
    assert!(!state.db.static_file_contains(Height(0)));
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
fn secondary_range_reads_include_blocks_the_primary_moved_later() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let blocks = test_chain();
    let dir = TempDir::new().expect("temp dir");
    let config = config_at(dir.path());

    // The secondary lists the static files after the first move only.
    let first_move = STATIC_FILE_MOVE_THRESHOLD as usize + 1;
    let mut primary = FinalizedState::new(&config, &network).expect("state opens");
    commit(&mut primary, &blocks[..first_move]);
    let (_read_state, secondary, _non_finalized) =
        crate::init_read_only(config, &network).expect("secondary opens alongside the primary");

    commit(&mut primary, &blocks[first_move..]);
    secondary
        .try_catch_up_with_primary()
        .expect("secondary catches up with the second move");

    // Point reads of blocks the primary moved after the secondary listed its files.
    assert_chain_reads(&secondary, &blocks);

    let expected: Vec<_> = blocks
        .iter()
        .flat_map(|block| block.transactions.iter().map(|tx| tx.hash()))
        .collect();
    let read: Vec<_> = secondary
        .stored_transactions_by_location_range(
            TransactionLocation::min_for_height(Height(0))
                ..=TransactionLocation::max_for_height(Height(TEST_TIP)),
        )
        .map(|(_, transaction)| transaction.hash())
        .collect();
    assert_eq!(read, expected);
}

#[test]
fn static_files_win_over_a_rocksdb_row_at_a_moved_height() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let blocks = test_chain();
    let dir = TempDir::new().expect("temp dir");
    let mut state = FinalizedState::new(&config_at(dir.path()), &network).expect("state opens");
    commit(&mut state, &blocks);

    // A deleted SST file can bring back a row a rollback replaced. Plant block
    // 11's coinbase where block 10's moved coinbase is.
    let location = TransactionLocation::from_usize(Height(10), 0);
    let stale = RawBytes::new_raw_bytes(
        blocks[11].transactions[0]
            .zcash_serialize_to_vec()
            .expect("transactions serialize"),
    );
    plant_tx_by_loc_row(&state.db, location, stale);

    let expected = blocks[10].transactions[0].hash();
    let (read, height, _) = state
        .db
        .stored_transaction(expected)
        .expect("block 10's coinbase reads back");
    assert_eq!((read.hash(), height), (expected, Height(10)));

    let range: Vec<_> = state
        .db
        .stored_transactions_by_location_range(location..=location)
        .map(|(_, transaction)| transaction.hash())
        .collect();
    assert_eq!(range, vec![expected]);

    // A row past the moved block's last transaction, as if the old chain's
    // block had more transactions than its replacement.
    let extra = TransactionLocation::from_usize(Height(10), blocks[10].transactions.len());
    plant_tx_by_loc_row(&state.db, extra, RawBytes::new_raw_bytes(vec![0; 100]));

    let whole_block: Vec<_> = state
        .db
        .stored_transactions_by_location_range(
            TransactionLocation::min_for_height(Height(10))
                ..=TransactionLocation::max_for_height(Height(10)),
        )
        .map(|(_, transaction)| transaction.hash())
        .collect();
    let block_10: Vec<_> = blocks[10].transactions.iter().map(|tx| tx.hash()).collect();
    assert_eq!(whole_block, block_10);
    assert_eq!(
        state
            .db
            .raw_transactions_by_location_range(extra..=extra)
            .count(),
        0
    );
    assert_chain_reads(&state.db, &blocks);
}

#[test]
fn a_moved_height_never_falls_back_to_a_rocksdb_row() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let blocks = test_chain();
    let dir = TempDir::new().expect("temp dir");
    let config = config_at(dir.path());
    let mut primary = FinalizedState::new(&config, &network).expect("state opens");
    commit(&mut primary, &blocks);

    // A row a deleted SST file brought back, at a moved height.
    let location = TransactionLocation::from_usize(Height(10), 0);
    let stale = RawBytes::new_raw_bytes(
        blocks[11].transactions[0]
            .zcash_serialize_to_vec()
            .expect("transactions serialize"),
    );
    plant_tx_by_loc_row(&primary.db, location, stale);

    // The jar disappears before a secondary ever opens it.
    let static_dir = primary.db.path().join(STATIC_FILE_DIR);
    for entry in std::fs::read_dir(&static_dir).expect("static files exist") {
        std::fs::remove_file(entry.expect("entry reads").path()).expect("file deletes");
    }
    let (_read_state, secondary, _non_finalized) =
        crate::init_read_only(config, &network).expect("secondary opens alongside the primary");

    // The body reads as missing, never as the stale row.
    let coinbase = blocks[10].transactions[0].hash();
    assert!(secondary.stored_transaction(coinbase).is_none());
    assert!(!secondary.contains_body_at_height(Height(10)));
    assert_eq!(
        secondary
            .raw_transactions_by_location_range(location..=location)
            .count(),
        0
    );
}

#[test]
fn a_secondary_reports_a_jar_that_shrank_under_it_as_missing() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let blocks = test_chain();
    let dir = TempDir::new().expect("temp dir");
    let config = config_at(dir.path());

    let mut primary = FinalizedState::new(&config, &network).expect("state opens");
    commit(&mut primary, &blocks);
    let (_read_state, secondary, _non_finalized) =
        crate::init_read_only(config, &network).expect("secondary opens alongside the primary");
    assert_chain_reads(&secondary, &blocks);
    let static_dir = primary.db.path().join(STATIC_FILE_DIR);
    drop(primary);

    // Damage the files under the secondary, which already has them open.
    StaticFiles::open(static_dir, true, 500_000)
        .expect("static files open")
        .truncate_above(Some(Height(60)))
        .expect("static files truncate");

    // The secondary rescans and retries instead of panicking.
    assert!(secondary
        .block(Height(70).into())
        .expect("block reads succeed")
        .is_none());
    assert!(secondary
        .block(Height(50).into())
        .expect("block reads succeed")
        .is_some());
}

#[test]
fn the_mover_frees_moved_files_after_the_delay_without_more_commits() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let blocks = test_chain();
    let dir = TempDir::new().expect("temp dir");
    let mut state = FinalizedState::new(&config_at(dir.path()), &network).expect("state opens");
    let delay = std::time::Duration::from_secs(3);
    state.static_file_mover = Arc::new(StaticFileMover::with_file_delete_delay(delay));

    // Genesis never moves, so its own SST file stays. Like an upgraded
    // database's history, the next blocks reach SST files before they move.
    commit(&mut state, &blocks[..1]);
    state.db.compact_raw_transaction_range(Height(0), Height(1));
    let genesis_bytes = state.db.live_sst_bytes_for_test("tx_by_loc");
    assert!(genesis_bytes > 0);
    let before_move = STATIC_FILE_MOVE_THRESHOLD as usize;
    commit(&mut state, &blocks[1..before_move]);
    state
        .db
        .compact_raw_transaction_range(Height(1), Height(TEST_TIP + 1));
    commit(&mut state, &blocks[before_move..]);
    assert_eq!(state.db.static_file_transactions_tip(), Some(MOVED_TIP));
    assert!(
        state.db.live_sst_bytes_for_test("tx_by_loc") > genesis_bytes,
        "the files stay while secondaries catch up",
    );

    // No commit arrives after the last move, so only the timed wakeup frees
    // them, without compaction.
    let deadline = std::time::Instant::now() + 10 * delay;
    while state.db.live_sst_bytes_for_test("tx_by_loc") > genesis_bytes {
        assert!(
            std::time::Instant::now() < deadline,
            "the mover frees moved files on its own"
        );
        std::thread::sleep(delay / 4);
    }
    assert_chain_reads(&state.db, &blocks);
}

#[test]
fn moves_record_blocks_a_failed_move_left_in_static_files() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let blocks = test_chain();
    let dir = TempDir::new().expect("temp dir");
    let mut state = FinalizedState::new(&config_at(dir.path()), &network).expect("state opens");

    // A move whose database write failed: its blocks are in static files but
    // still in `tx_by_loc`, and the recorded tip didn't move.
    let appended = MOVED_TIP.0 + 10;
    commit(&mut state, &blocks[..=appended as usize]);
    state
        .db
        .append_static_file_blocks_for_test(&raw_blocks(&blocks, MOVED_TIP.0 + 1..=appended));
    assert_eq!(state.db.static_file_transactions_tip(), Some(MOVED_TIP));

    commit(&mut state, &blocks[appended as usize + 1..]);
    assert_eq!(
        state.db.static_file_transactions_tip(),
        Some(Height(appended)),
        "the next move records the appended blocks instead of truncating them",
    );
    assert_eq!(
        tx_by_loc_heights(&state.db),
        std::iter::once(Height(0))
            .chain((appended + 1..=TEST_TIP).map(Height))
            .collect::<Vec<_>>(),
    );
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
    let ahead = raw_blocks(&blocks, MOVED_TIP.0 + 1..=TEST_TIP);
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
        Err(StateInitError::StaticFilesMissingBlocks {
            first_missing: Height(1),
            database_tip: MOVED_TIP,
            ..
        })
    ));
}

#[test]
fn a_refused_open_keeps_a_jar_that_lost_its_config() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let dir = TempDir::new().expect("temp dir");
    let config = config_at(dir.path());

    let static_dir = {
        let mut state = FinalizedState::new(&config, &network).expect("state opens");
        commit(&mut state, &test_chain());
        state.db.path().join(STATIC_FILE_DIR)
    };
    let conf = std::fs::read_dir(&static_dir)
        .expect("static files exist")
        .map(|entry| entry.expect("entry reads").path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "conf")
        })
        .expect("the moved blocks have a committed jar");
    std::fs::remove_file(&conf).expect("config can be deleted");
    let base = conf.with_extension("");

    assert!(matches!(
        FinalizedState::new(&config, &network),
        Err(StateInitError::StaticFilesMissingBlocks {
            first_missing: Height(1),
            ..
        })
    ));
    for suffix in ["", "off", "blk"] {
        assert!(
            base.with_extension(suffix).exists(),
            "the open keeps the jar's {suffix:?} file for recovery",
        );
    }
}

#[test]
fn reopening_refuses_static_files_missing_interior_blocks() {
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

    // Rewrite the moved blocks into 50 block files, without the second file's
    // first block, so the tip still matches the database.
    let static_files = StaticFiles::open(static_dir, true, 50).expect("static files open");
    static_files
        .truncate_above(None)
        .expect("static files truncate");
    static_files
        .append_blocks(&raw_blocks(&blocks, 1..=49))
        .expect("first file appends");
    static_files
        .append_blocks(&raw_blocks(&blocks, 51..=MOVED_TIP.0))
        .expect("second file appends");
    drop(static_files);

    assert!(matches!(
        FinalizedState::new(&config, &network),
        Err(StateInitError::StaticFilesMissingBlocks {
            first_missing: Height(50),
            database_tip: MOVED_TIP,
            ..
        })
    ));
}

#[cfg(unix)]
#[test]
fn a_failed_static_file_write_stops_moves_until_reopen() {
    use std::os::unix::fs::PermissionsExt;

    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let blocks = test_chain();
    let dir = TempDir::new().expect("temp dir");
    let config = config_at(dir.path());
    let first_move = STATIC_FILE_MOVE_THRESHOLD as usize + 1;

    let mut state = FinalizedState::new(&config, &network).expect("state opens");
    commit(&mut state, &blocks[..first_move]);
    let first_tip = state.db.static_file_transactions_tip();

    // The second move can't write its config into a read-only directory.
    let static_dir = state.db.path().join(STATIC_FILE_DIR);
    let set_mode = |mode| {
        std::fs::set_permissions(&static_dir, std::fs::Permissions::from_mode(mode)).unwrap()
    };
    set_mode(0o500);
    commit(&mut state, &blocks[first_move..=2 * first_move]);
    set_mode(0o700);
    commit(&mut state, &blocks[2 * first_move + 1..]);

    assert_eq!(
        state.db.static_file_transactions_tip(),
        first_tip,
        "moves stop after the failed write",
    );
    assert_chain_reads(&state.db, &blocks);
    drop(state);

    // The reopened store moves everything waiting in one batch.
    let state = FinalizedState::new(&config, &network).expect("state reopens");
    state.static_file_mover.wait();
    assert_eq!(
        state.db.static_file_transactions_tip(),
        Some(Height(TEST_TIP))
    );
    assert_chain_reads(&state.db, &blocks);
}

#[test]
fn rollback_deletes_rows_a_file_delete_brought_back() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let blocks = test_chain();
    let dir = TempDir::new().expect("temp dir");
    let config = config_at(dir.path());
    let target = Height(60);

    {
        let mut state = FinalizedState::new(&config, &network).expect("state opens");
        commit(&mut state, &blocks);

        // A row past a moved block's last index, as if a deleted SST file
        // brought back an earlier chain's longer block.
        let extra = TransactionLocation::from_usize(Height(70), blocks[70].transactions.len());
        plant_tx_by_loc_row(&state.db, extra, RawBytes::new_raw_bytes(vec![0; 100]));
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

    // Height 70 reads `tx_by_loc` again, and the stale row is gone with the rest.
    let mut state = FinalizedState::new(&config, &network).expect("state reopens");
    assert!(tx_by_loc_heights(&state.db)
        .iter()
        .all(|height| *height <= target || *height > MOVED_TIP));
    commit(&mut state, &blocks[target.0 as usize + 1..]);
    assert_chain_reads(&state.db, &blocks);
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

/// Returns [`config_at`] with static files under `static_files_root`.
fn external_config_at(dir: &Path, static_files_root: &Path) -> Config {
    Config {
        static_files_dir: Some(static_files_root.to_path_buf()),
        ..config_at(dir)
    }
}

/// Returns the names of the files in `dir`, sorted, or none if it's missing.
fn file_names(dir: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .map(|entry| {
            entry
                .expect("entry reads")
                .file_name()
                .into_string()
                .expect("test file names are UTF-8")
        })
        .collect();
    names.sort();
    names
}

#[test]
fn external_static_files_open_reopen_and_serve_a_secondary() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let blocks = test_chain();
    let dir = TempDir::new().expect("temp dir");
    let root = TempDir::new().expect("temp dir");
    let config = external_config_at(dir.path(), root.path());
    let major = crate::constants::state_database_format_version_in_code().major;
    let external_dir = root.path().join(format!("v{major}")).join("mainnet");
    assert_eq!(
        config.external_static_files_path(major, &network),
        Some(external_dir.clone())
    );

    let mut primary = FinalizedState::new(&config, &network).expect("state opens");
    commit(&mut primary, &blocks);
    assert_eq!(primary.db.static_file_transactions_tip(), Some(MOVED_TIP));
    assert!(
        !primary.db.path().join(STATIC_FILE_DIR).exists(),
        "nothing is written inside the state directory"
    );
    assert!(!file_names(&external_dir).is_empty());

    let (_read_state, secondary, _non_finalized) = crate::init_read_only(config.clone(), &network)
        .expect("secondary opens alongside the primary");
    assert_chain_reads(&secondary, &blocks);
    drop(secondary);
    drop(primary);

    let state = FinalizedState::new(&config, &network).expect("state reopens");
    assert_eq!(state.db.static_file_transactions_tip(), Some(MOVED_TIP));
    assert_chain_reads(&state.db, &blocks);
}

#[test]
fn external_static_files_are_isolated_per_network() {
    let _init_guard = zakura_test::init();
    let blocks = test_chain();
    let root = TempDir::new().expect("temp dir");
    let mainnet_dir = TempDir::new().expect("temp dir");
    let testnet_dir = TempDir::new().expect("temp dir");
    let mainnet_config = external_config_at(mainnet_dir.path(), root.path());

    let mut mainnet = FinalizedState::new(&mainnet_config, &Network::Mainnet).expect("state opens");
    commit(&mut mainnet, &blocks);
    drop(mainnet);

    // A new testnet database sharing the root heals its own, empty directory.
    let testnet = Network::new_default_testnet();
    drop(
        FinalizedState::new(
            &external_config_at(testnet_dir.path(), root.path()),
            &testnet,
        )
        .expect("a testnet state shares the static file root"),
    );

    let state = FinalizedState::new(&mainnet_config, &Network::Mainnet).expect("state reopens");
    assert_eq!(state.db.static_file_transactions_tip(), Some(MOVED_TIP));
    assert_chain_reads(&state.db, &blocks);
}

#[test]
fn a_reused_major_version_moves_external_static_files() {
    use crate::{constants::STATE_DATABASE_KIND, service::finalized_state::disk_db::DiskDb};

    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let blocks = test_chain();
    let dir = TempDir::new().expect("temp dir");
    let root = TempDir::new().expect("temp dir");
    let config = external_config_at(dir.path(), root.path());
    let current = crate::constants::state_database_format_version_in_code();
    let previous = semver::Version::new(current.major - 1, 0, 0);
    let current_db = config.db_path(STATE_DATABASE_KIND, current.major, &network);
    let previous_db = config.db_path(STATE_DATABASE_KIND, previous.major, &network);
    let current_static = config
        .external_static_files_path(current.major, &network)
        .expect("static files are external");
    let previous_static = config
        .external_static_files_path(previous.major, &network)
        .expect("static files are external");

    // Make the current database look like one the previous release left.
    {
        let mut state = FinalizedState::new(&config, &network).expect("state opens");
        commit(&mut state, &blocks);
        crate::write_database_format_version_to_disk(
            &config,
            STATE_DATABASE_KIND,
            current.major,
            &previous,
            &network,
        )
        .expect("fixture version is written");
    }
    let static_names = file_names(&current_static);
    for (from, to) in [
        (&current_db, &previous_db),
        (&current_static, &previous_static),
    ] {
        std::fs::create_dir_all(to.parent().expect("versioned paths have a parent"))
            .expect("older version parent exists");
        std::fs::rename(from, to).expect("fixture moves to the older version directory");
    }

    assert_eq!(
        DiskDb::try_reusing_previous_db_after_major_upgrade(
            &[current.major],
            &current,
            &config,
            STATE_DATABASE_KIND,
            &network,
        ),
        Some(previous.clone()),
    );
    assert_eq!(file_names(&current_static), static_names);
    assert!(!previous_static.exists());
    assert!(
        !root.path().join(format!("v{}", previous.major)).exists(),
        "the empty old version directory is removed"
    );

    // Put the version back, so the reused database opens without a format change.
    crate::write_database_format_version_to_disk(
        &config,
        STATE_DATABASE_KIND,
        current.major,
        &current,
        &network,
    )
    .expect("fixture version is written");
    let state = FinalizedState::new(&config, &network).expect("reused state opens");
    assert_eq!(state.db.static_file_transactions_tip(), Some(MOVED_TIP));
    assert_chain_reads(&state.db, &blocks);
}

#[test]
fn a_new_database_refuses_external_static_files_it_did_not_write() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let root = TempDir::new().expect("temp dir");
    let first = TempDir::new().expect("temp dir");
    let second = TempDir::new().expect("temp dir");

    let mut state = FinalizedState::new(&external_config_at(first.path(), root.path()), &network)
        .expect("state opens");
    commit(&mut state, &test_chain());
    drop(state);

    // A second node, or a resync after deleting only the state, would delete them.
    assert!(matches!(
        FinalizedState::new(&external_config_at(second.path(), root.path()), &network),
        Err(StateInitError::StaticFilesWithoutDatabase { .. })
    ));
    let state = FinalizedState::new(&external_config_at(first.path(), root.path()), &network)
        .expect("the first state still opens");
    assert_eq!(state.db.static_file_transactions_tip(), Some(MOVED_TIP));
}

#[test]
fn static_files_left_in_the_state_directory_are_reported() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let dir = TempDir::new().expect("temp dir");
    let root = TempDir::new().expect("temp dir");

    // Like a snapshot taken with the default layout.
    let mut state = FinalizedState::new(&config_at(dir.path()), &network).expect("state opens");
    commit(&mut state, &test_chain());
    drop(state);

    let config = external_config_at(dir.path(), root.path());
    assert!(matches!(
        FinalizedState::new(&config, &network),
        Err(StateInitError::StaticFilesInStateDir { .. })
    ));
    assert!(matches!(
        crate::init_read_only(config, &network),
        Err(StateInitError::StaticFilesInStateDir { .. })
    ));
}
