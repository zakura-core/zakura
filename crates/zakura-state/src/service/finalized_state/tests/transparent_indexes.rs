//! Tests for stopping optional transparent index writes in the existing state database.
//!
//! Pruned checkpoint sync keeps consensus state, stops index updates in place, and rejects
//! stale address queries. A durable marker preserves this across restarts and rollback.

use std::{
    collections::{BTreeMap, HashSet},
    path::Path,
    sync::Arc,
};

use crate::service::finalized_state::disk_format::upgrade::cancel_channel;
use tempfile::TempDir;
use tower::{Service, ServiceExt};

use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{Block, Height},
    parameters::{Network, NetworkKind},
    serialization::ZcashDeserializeInto,
    transaction::{LockTime, Transaction},
    transparent::{Address, Input, OutPoint, Script},
};

use crate::{
    config::{Config, PruningConfig, StorageMode},
    constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
    init_read_only,
    request::{FinalizableBlock, Treestate},
    rollback_finalized_state,
    service::{
        check,
        finalized_state::{
            disk_format::upgrade::DbFormatChange,
            zakura_db::transparent::{BALANCE_BY_TRANSPARENT_ADDR, TX_LOC_BY_SPENT_OUT_LOC},
            CheckpointVerifiedBlock, FinalizedState, ZakuraDb, PRUNING_METADATA,
            STATE_COLUMN_FAMILIES_IN_CODE,
        },
        non_finalized_state::{CreatedUtxos, SharedMap},
    },
    ContextuallyVerifiedBlock, ReadRequest, ReadResponse, RollbackFinalizedStateError,
    RollbackFinalizedStateOptions, SemanticallyVerifiedBlock, StateInitError,
    TransparentIndexesUnavailable, ValidateContextError,
};

use super::rollback::{child_block, coinbase_tx, ironwood_v6_tx, spend_tx};

/// Column families that only hold transparent address or spender indexes.
const TRANSPARENT_INDEX_COLUMN_FAMILIES: [&str; 4] = [
    BALANCE_BY_TRANSPARENT_ADDR,
    "tx_loc_by_transparent_addr_loc",
    "utxo_loc_by_transparent_addr_loc",
    TX_LOC_BY_SPENT_OUT_LOC,
];

/// How a test node is configured.
#[derive(Clone, Copy, Debug)]
enum NodeConfig {
    /// Pruned storage with checkpoint sync: new databases skip transparent indexes.
    PrunedCheckpoint,
    /// Pruned storage with checkpoint sync disabled: new databases build transparent indexes.
    PrunedFullVerification,
    /// Archive storage with checkpoint sync.
    Archive,
}

fn config_at(dir: &Path, node: NodeConfig) -> Config {
    let (storage_mode, checkpoint_sync) = match node {
        NodeConfig::PrunedCheckpoint => (StorageMode::Pruned(PruningConfig::default()), true),
        NodeConfig::PrunedFullVerification => {
            (StorageMode::Pruned(PruningConfig::default()), false)
        }
        NodeConfig::Archive => (StorageMode::Archive, true),
    };

    Config {
        cache_dir: dir.to_path_buf(),
        ephemeral: false,
        storage_mode,
        checkpoint_sync,
        ..Config::default()
    }
}

fn amount(zatoshis: i64) -> Amount<NonNegative> {
    Amount::try_from(zatoshis).expect("test amounts are valid")
}

fn outpoint(transaction: &Transaction, index: u32) -> OutPoint {
    OutPoint {
        hash: transaction.hash(),
        index,
    }
}

/// A transaction spending `outpoints` with no transparent outputs, which is how a valid
/// transaction spends a coinbase output on networks that require coinbase spends to be shielded.
fn shielding_spend_tx(outpoints: &[OutPoint]) -> Arc<Transaction> {
    Arc::new(Transaction::V1 {
        inputs: outpoints
            .iter()
            .map(|outpoint| Input::PrevOut {
                outpoint: *outpoint,
                unlock_script: Script::new(&[]),
                sequence: u32::MAX,
            })
            .collect(),
        outputs: Vec::new(),
        lock_time: LockTime::unlocked(),
    })
}

/// A short Mainnet chain with coinbase outputs and transparent spends that cross blocks and stay
/// inside one block.
struct TestChain {
    network: Network,
    address: Address,
    other: Address,
    blocks: Vec<Arc<Block>>,
    /// The non-coinbase output created at height 3, unspent at the tip.
    unspent_output: OutPoint,
    /// The height 1 coinbase output, spent at height 2.
    spent_coinbase: OutPoint,
    /// The height 3 coinbase output, unspent at the tip.
    tip_coinbase: OutPoint,
}

fn test_chain() -> TestChain {
    let network = Network::Mainnet;
    let address = Address::from_script_hash(NetworkKind::Mainnet, [0x42; 20]);
    let other = Address::from_script_hash(NetworkKind::Mainnet, [0x17; 20]);

    let genesis: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into()
        .expect("mainnet genesis test vector deserializes");

    let coinbase1 = coinbase_tx(Height(1), amount(50_000), &address);
    let block1 = child_block(&genesis, vec![coinbase1.clone()]);

    // Height 2 spends the height 1 coinbase, then spends that output again in the same block.
    let spend1 = spend_tx(outpoint(&coinbase1, 0), amount(40_000), &other);
    let spend2 = spend_tx(outpoint(&spend1, 0), amount(30_000), &address);
    let block2 = child_block(
        &block1,
        vec![
            coinbase_tx(Height(2), amount(1_000), &other),
            spend1,
            spend2.clone(),
        ],
    );

    // Height 3 spends an output from height 2.
    let coinbase3 = coinbase_tx(Height(3), amount(2_000), &address);
    let spend3 = spend_tx(outpoint(&spend2, 0), amount(20_000), &other);
    let block3 = child_block(&block2, vec![coinbase3.clone(), spend3.clone()]);

    TestChain {
        network,
        address,
        other,
        blocks: vec![genesis, block1, block2, block3],
        unspent_output: outpoint(&spend3, 0),
        spent_coinbase: outpoint(&coinbase1, 0),
        tip_coinbase: outpoint(&coinbase3, 0),
    }
}

fn open(config: &Config, network: &Network) -> FinalizedState {
    FinalizedState::new(config, network).expect("test state database opens")
}

fn commit_checkpoint(state: &mut FinalizedState, blocks: &[Arc<Block>]) {
    for block in blocks {
        state
            .commit_finalized_direct(
                CheckpointVerifiedBlock::from(block.clone()).into(),
                None,
                None,
                "transparent index tests",
            )
            .expect("committing a test block succeeds");
    }
}

/// Commits `block` the way a block above the checkpoint is finalized: its spends are checked
/// against the finalized UTXO set, then it is committed as a contextually verified block.
fn commit_contextual(state: &mut FinalizedState, block: Arc<Block>) {
    let network = state.network();
    let semantically_verified = SemanticallyVerifiedBlock::from(block);
    let spent_utxos = check::utxo::transparent_spend(
        &semantically_verified,
        &CreatedUtxos::default(),
        &SharedMap::default(),
        &state.db,
    )
    .expect("the test block only spends valid finalized outputs");
    let contextually_verified = ContextuallyVerifiedBlock::with_block_and_spent_utxos(
        &network,
        semantically_verified,
        spent_utxos,
    )
    .expect("the test block has a valid value balance");

    state
        .commit_finalized_direct(
            FinalizableBlock::new(contextually_verified, Treestate::default()),
            None,
            None,
            "transparent index tests",
        )
        .expect("committing a contextually verified test block succeeds");
}

/// Returns the raw contents of every column family that consensus or block serving reads, which
/// excludes the transparent index column families and the storage policy metadata.
fn consensus_column_families(db: &ZakuraDb) -> Vec<(&'static str, Vec<(Vec<u8>, Vec<u8>)>)> {
    STATE_COLUMN_FAMILIES_IN_CODE
        .iter()
        .filter(|cf| !TRANSPARENT_INDEX_COLUMN_FAMILIES.contains(cf) && **cf != PRUNING_METADATA)
        .map(|cf| (*cf, db.raw_column_family_entries_for_test(cf)))
        .collect()
}

fn transparent_index_entry_count(db: &ZakuraDb) -> usize {
    TRANSPARENT_INDEX_COLUMN_FAMILIES
        .iter()
        .map(|cf| db.raw_column_family_entries_for_test(cf).len())
        .sum()
}

/// Runs the UTXO contextual checks for a candidate block at `height` with `transactions`.
fn check_spends(
    db: &ZakuraDb,
    parent: &Block,
    height: Height,
    transactions: Vec<Arc<Transaction>>,
) -> Result<HashSet<OutPoint>, ValidateContextError> {
    let address = Address::from_script_hash(NetworkKind::Mainnet, [0x99; 20]);
    let mut all_transactions = vec![coinbase_tx(height, amount(1), &address)];
    all_transactions.extend(transactions);
    let block = SemanticallyVerifiedBlock::from(child_block(parent, all_transactions));

    check::utxo::transparent_spend(&block, &CreatedUtxos::default(), &SharedMap::default(), db)
        .map(|spent| spent.into_keys().collect())
}

/// Runs every spend check this file cares about, returning a comparable summary.
fn spend_check_results(db: &ZakuraDb, chain: &TestChain) -> Vec<String> {
    let tip = chain.blocks.last().expect("the chain has blocks");
    let next = Height(4);
    let missing = OutPoint {
        hash: zakura_chain::transaction::Hash([0xab; 32]),
        index: 0,
    };

    let valid = check_spends(
        db,
        tip,
        next,
        vec![spend_tx(
            chain.unspent_output,
            amount(10_000),
            &chain.address,
        )],
    );
    assert_eq!(
        valid,
        Ok(HashSet::from([chain.unspent_output])),
        "a valid transparent spend succeeds"
    );

    let double_spend = check_spends(
        db,
        tip,
        next,
        vec![spend_tx(chain.spent_coinbase, amount(1), &chain.address)],
    );
    assert!(
        matches!(double_spend, Err(ValidateContextError::MissingTransparentOutput { outpoint, .. }) if outpoint == chain.spent_coinbase),
        "spending an output a finalized block already spent is rejected: {double_spend:?}"
    );

    let same_block_double_spend = check_spends(
        db,
        tip,
        next,
        vec![
            spend_tx(chain.unspent_output, amount(1), &chain.address),
            spend_tx(chain.unspent_output, amount(1), &chain.other),
        ],
    );
    assert!(
        matches!(
            same_block_double_spend,
            Err(ValidateContextError::DuplicateTransparentSpend { .. })
        ),
        "spending one output twice in a block is rejected: {same_block_double_spend:?}"
    );

    let missing_spend = check_spends(
        db,
        tip,
        next,
        vec![spend_tx(missing, amount(1), &chain.address)],
    );
    assert!(
        matches!(missing_spend, Err(ValidateContextError::MissingTransparentOutput { outpoint, .. }) if outpoint == missing),
        "spending an output that never existed is rejected: {missing_spend:?}"
    );

    let immature = check_spends(
        db,
        tip,
        next,
        vec![shielding_spend_tx(&[chain.tip_coinbase])],
    );
    assert!(
        matches!(
            immature,
            Err(ValidateContextError::ImmatureTransparentCoinbaseSpend { .. })
        ),
        "spending an immature coinbase output is rejected: {immature:?}"
    );

    let mature_height = Height(3 + zakura_chain::transparent::MIN_TRANSPARENT_COINBASE_MATURITY);
    let mature = check_spends(
        db,
        tip,
        mature_height,
        vec![shielding_spend_tx(&[chain.tip_coinbase])],
    );
    assert_eq!(
        mature,
        Ok(HashSet::from([chain.tip_coinbase])),
        "spending a mature coinbase output succeeds"
    );

    [
        valid.map(|_| ()),
        double_spend.map(|_| ()),
        same_block_double_spend.map(|_| ()),
        missing_spend.map(|_| ()),
        immature.map(|_| ()),
        mature.map(|_| ()),
    ]
    .into_iter()
    .map(|result| format!("{result:?}"))
    .collect()
}

#[test]
fn index_writes_follow_pruned_checkpoint_sync_configuration() {
    let dir = TempDir::new().expect("temp dir");
    for (node, skips) in [
        (NodeConfig::PrunedCheckpoint, true),
        (NodeConfig::PrunedFullVerification, false),
        (NodeConfig::Archive, false),
    ] {
        let config = config_at(dir.path(), node);
        assert_eq!(config.skips_transparent_indexes(), skips, "{node:?}");
        assert_eq!(
            config.non_finalized_state_backup_dir(&Network::Mainnet),
            Some(dir.path().join("non_finalized_state").join("mainnet"))
        );
    }
}

/// The database without transparent indexes stores exactly the same consensus state as an
/// indexed database, answers the same spend checks, and keeps doing so after it restarts with
/// checkpoint sync disabled and commits another spending block above the checkpoint.
#[test]
fn skipped_and_indexed_databases_keep_identical_validation_state() {
    let _init_guard = zakura_test::init();
    let chain = test_chain();
    let network = chain.network.clone();

    let skipped_dir = TempDir::new().expect("temp dir");
    let indexed_dir = TempDir::new().expect("temp dir");
    let skipped_config = config_at(skipped_dir.path(), NodeConfig::PrunedCheckpoint);
    let indexed_config = config_at(indexed_dir.path(), NodeConfig::PrunedFullVerification);

    {
        let mut skipped = open(&skipped_config, &network);
        let mut indexed = open(&indexed_config, &network);
        assert!(skipped.db.skips_transparent_indexes());
        assert!(!indexed.db.skips_transparent_indexes());

        commit_checkpoint(&mut skipped, &chain.blocks);
        commit_checkpoint(&mut indexed, &chain.blocks);

        assert_eq!(
            consensus_column_families(&skipped.db),
            consensus_column_families(&indexed.db),
            "consensus column families are identical"
        );
        assert_eq!(transparent_index_entry_count(&skipped.db), 0);
        assert!(
            transparent_index_entry_count(&indexed.db) > 0,
            "the indexed database built transparent indexes"
        );
        assert_eq!(
            skipped.db.transparent_indexes_skipped_from(),
            Some(Height(0)),
            "the first committed block is marked"
        );
        assert_eq!(indexed.db.transparent_indexes_skipped_from(), None);

        assert_eq!(
            spend_check_results(&skipped.db, &chain),
            spend_check_results(&indexed.db, &chain),
            "both databases answer every spend check the same way"
        );
    }

    // Restart with checkpoint sync disabled. The existing database keeps skipping indexes.
    let next = child_block(
        chain.blocks.last().expect("the chain has blocks"),
        vec![
            coinbase_tx(Height(4), amount(3_000), &chain.other),
            spend_tx(chain.unspent_output, amount(5_000), &chain.address),
        ],
    );
    let restarted_config = config_at(skipped_dir.path(), NodeConfig::PrunedFullVerification);
    assert!(!restarted_config.skips_transparent_indexes());

    let mut skipped = open(&restarted_config, &network);
    let mut indexed = open(&indexed_config, &network);
    assert!(
        skipped.db.skips_transparent_indexes(),
        "restarting with checkpoint sync disabled keeps index writes disabled"
    );

    commit_contextual(&mut skipped, next.clone());
    commit_contextual(&mut indexed, next);

    assert_eq!(skipped.db.finalized_tip_height(), Some(Height(4)));
    assert_eq!(
        consensus_column_families(&skipped.db),
        consensus_column_families(&indexed.db),
        "consensus column families stay identical after a commit above the checkpoint"
    );
    assert_eq!(
        transparent_index_entry_count(&skipped.db),
        0,
        "the restarted database never resumes transparent index writes"
    );
    assert_eq!(
        skipped.db.transparent_indexes_skipped_from(),
        Some(Height(0)),
        "the marker keeps the first skipped height"
    );
    assert!(skipped.db.utxo(&chain.unspent_output).is_none());
    assert!(indexed.db.utxo(&chain.unspent_output).is_none());
}

/// The marker is written with the first skipped block, before raw transaction pruning starts.
#[test]
fn transparent_index_marker_precedes_raw_transaction_pruning() {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let chain = test_chain();
    let dir = TempDir::new().expect("temp dir");
    let config = Config {
        // A tiny retention, below the production floor, so this short chain is pruned.
        storage_mode: StorageMode::Pruned(PruningConfig { tx_retention: 1 }),
        ..config_at(dir.path(), NodeConfig::PrunedCheckpoint)
    };

    let mut state =
        FinalizedState::new_with_debug_without_storage_validation(&config, &network, false, false)
            .expect("test state database opens");

    commit_checkpoint(&mut state, &chain.blocks[..1]);
    assert_eq!(state.db.transparent_indexes_skipped_from(), Some(Height(0)));
    assert_eq!(
        state.db.lowest_retained_height(),
        None,
        "nothing is pruned when the marker is written"
    );

    commit_checkpoint(&mut state, &chain.blocks[1..]);
    assert!(state.db.is_pruned(), "later commits prune raw transactions");
    assert_eq!(
        state.db.transparent_indexes_skipped_from(),
        Some(Height(0)),
        "pruning does not move the marker"
    );
    assert_eq!(transparent_index_entry_count(&state.db), 0);
}

/// An existing indexed database keeps its chain state but stops optional index writes in place.
#[test]
fn existing_indexed_database_stops_index_writes_in_place() {
    let _init_guard = zakura_test::init();
    let chain = test_chain();
    let dir = TempDir::new().expect("temp dir");
    let initial_config = config_at(dir.path(), NodeConfig::PrunedFullVerification);
    let indexes_before;
    let path_before;
    {
        let mut state = open(&initial_config, &chain.network);
        commit_checkpoint(&mut state, &chain.blocks[..3]);
        path_before = state.db.path().to_owned();
        indexes_before = TRANSPARENT_INDEX_COLUMN_FAMILIES
            .map(|cf| state.db.raw_column_family_entries_for_test(cf));
        assert!(transparent_index_entry_count(&state.db) > 0);
        assert_eq!(state.db.transparent_indexes_skipped_from(), None);
    }

    let config = config_at(dir.path(), NodeConfig::PrunedCheckpoint);
    {
        let mut state = open(&config, &chain.network);
        assert_eq!(state.db.path(), path_before);
        assert_eq!(state.db.finalized_tip_height(), Some(Height(2)));
        commit_checkpoint(&mut state, &chain.blocks[3..]);
        assert!(state.db.skips_transparent_indexes());
        assert_eq!(state.db.transparent_indexes_skipped_from(), Some(Height(3)));
        assert_eq!(
            TRANSPARENT_INDEX_COLUMN_FAMILIES
                .map(|cf| { state.db.raw_column_family_entries_for_test(cf) }),
            indexes_before,
            "existing index rows are retained without further writes"
        );
        assert!(state.db.address_balance(&chain.other).is_err());
        assert!(state.db.address_utxos(&chain.other).is_err());
        assert!(state
            .db
            .address_tx_ids(&chain.other, Height(0)..=Height(3))
            .is_err());
        assert!(state.db.utxo(&chain.unspent_output).is_some());
        assert_eq!(state.db.finalized_tip_height(), Some(Height(3)));
    }

    // Disabling checkpoint sync must not resume updates to incomplete indexes.
    let state = open(&initial_config, &chain.network);
    assert!(state.db.skips_transparent_indexes());
    assert_eq!(state.db.transparent_indexes_skipped_from(), Some(Height(3)));
    assert_eq!(
        TRANSPARENT_INDEX_COLUMN_FAMILIES
            .map(|cf| { state.db.raw_column_family_entries_for_test(cf) }),
        indexes_before
    );
    drop(state);
    rollback_finalized_state(
        initial_config.clone(),
        &chain.network,
        RollbackFinalizedStateOptions {
            target_height: Height(1),
            keep_rolled_back_blocks: false,
            max_checkpoint_height: None,
        },
    )
    .expect("retained block bodies permit rollback");
    let mut state = open(&initial_config, &chain.network);
    assert_eq!(state.db.finalized_tip_height(), Some(Height(1)));
    assert_eq!(
        state.db.transparent_indexes_skipped_from(),
        Some(Height(3)),
        "rolling back before the first skipped height preserves the marker"
    );
    assert_eq!(
        TRANSPARENT_INDEX_COLUMN_FAMILIES.map(|cf| state.db.raw_column_family_entries_for_test(cf)),
        indexes_before
    );
    assert!(
        state.db.utxo(&chain.spent_coinbase).is_some(),
        "rollback restores the spent canonical output"
    );
    commit_checkpoint(&mut state, &chain.blocks[2..3]);
    assert_eq!(state.db.finalized_tip_height(), Some(Height(2)));
    assert_eq!(
        TRANSPARENT_INDEX_COLUMN_FAMILIES.map(|cf| state.db.raw_column_family_entries_for_test(cf)),
        indexes_before
    );
    assert!(state.db.address_balance(&chain.other).is_err());
}

/// A rollback before any skipped commit must still reverse complete index rows.
#[test]
fn indexed_rollback_before_first_skipped_commit_keeps_indexes_complete() {
    let _init_guard = zakura_test::init();
    let chain = test_chain();
    let dir = TempDir::new().expect("temp dir");
    let indexed_config = config_at(dir.path(), NodeConfig::PrunedFullVerification);
    {
        let mut state = open(&indexed_config, &chain.network);
        commit_checkpoint(&mut state, &chain.blocks);
        assert_eq!(state.db.transparent_indexes_skipped_from(), None);
    }
    rollback_finalized_state(
        config_at(dir.path(), NodeConfig::PrunedCheckpoint),
        &chain.network,
        RollbackFinalizedStateOptions {
            target_height: Height(1),
            keep_rolled_back_blocks: false,
            max_checkpoint_height: None,
        },
    )
    .expect("complete indexes can be rolled back");
    let state = open(&indexed_config, &chain.network);
    assert_eq!(state.db.transparent_indexes_skipped_from(), None);
    assert!(!state.db.skips_transparent_indexes());
    assert_eq!(
        state
            .db
            .address_balance(&chain.address)
            .expect("indexes remain complete")
            .map(|(balance, _)| balance),
        Some(amount(50_000))
    );
    assert!(state
        .db
        .address_utxos(&chain.other)
        .expect("indexes remain complete")
        .is_empty());
    assert!(state.db.utxo(&chain.spent_coinbase).is_some());
}

/// Every transparent address and spender query fails on a database without transparent indexes,
/// even when stale index entries exist, while outpoint and shielded queries keep working.
#[tokio::test(flavor = "multi_thread")]
async fn transparent_queries_fail_even_with_stale_index_entries() {
    let _init_guard = zakura_test::init();
    let chain = test_chain();
    let network = chain.network.clone();
    let dir = TempDir::new().expect("temp dir");
    let config = config_at(dir.path(), NodeConfig::PrunedCheckpoint);

    let (ironwood_tx, ironwood_nullifier) = ironwood_v6_tx(Height(5));
    let ironwood_block = child_block(
        chain.blocks.last().expect("the chain has blocks"),
        vec![coinbase_tx(Height(4), amount(1), &chain.other), ironwood_tx],
    );

    {
        let mut state = open(&config, &network);
        commit_checkpoint(&mut state, &chain.blocks);
        commit_checkpoint(&mut state, &[ironwood_block]);

        // Plant stale entries that look like a complete index: a balance for the address, and a
        // spender for the spent coinbase output.
        let indexed_dir = TempDir::new().expect("temp dir");
        let mut indexed = open(
            &config_at(indexed_dir.path(), NodeConfig::PrunedFullVerification),
            &network,
        );
        commit_checkpoint(&mut indexed, &chain.blocks);
        for cf in TRANSPARENT_INDEX_COLUMN_FAMILIES {
            for (key, value) in indexed.db.raw_column_family_entries_for_test(cf) {
                state.db.insert_raw_entry_for_test(cf, &key, &value);
            }
        }
        let spent_location = state
            .db
            .output_location(&chain.spent_coinbase)
            .expect("the spent output has a location");
        state.db.insert_raw_entry_for_test(
            TX_LOC_BY_SPENT_OUT_LOC,
            &crate::IntoDisk::as_bytes(&spent_location),
            &crate::IntoDisk::as_bytes(
                &state
                    .db
                    .transaction_location(chain.blocks[2].transactions[1].hash())
                    .expect("the spender has a location"),
            ),
        );
        assert!(state.db.address_balance_location(&chain.address).is_some());
        assert!(state
            .db
            .tx_location_by_spent_output_location(&spent_location)
            .is_some());

        // Direct database queries.
        let addresses = HashSet::from([chain.address]);
        assert_eq!(
            state.db.address_balance(&chain.address),
            Err(TransparentIndexesUnavailable)
        );
        assert_eq!(
            state.db.address_utxos(&chain.address),
            Err(TransparentIndexesUnavailable)
        );
        assert_eq!(
            state
                .db
                .address_tx_ids(&chain.address, Height(0)..=Height(4)),
            Err(TransparentIndexesUnavailable)
        );
        assert_eq!(
            state.db.partial_finalized_transparent_balance(&addresses),
            Err(TransparentIndexesUnavailable)
        );
        assert_eq!(
            state.db.partial_finalized_address_utxos(&addresses),
            Err(TransparentIndexesUnavailable)
        );
        assert_eq!(
            state
                .db
                .partial_finalized_transparent_tx_ids(&addresses, Height(0)..=Height(4)),
            Err(TransparentIndexesUnavailable)
        );
        assert_eq!(
            state.db.spending_tx_loc(&chain.spent_coinbase),
            Err(TransparentIndexesUnavailable),
            "a stale spender entry is not served"
        );

        // An empty address set is not answered with an empty result either.
        let no_addresses = HashSet::new();
        assert_eq!(
            state
                .db
                .partial_finalized_transparent_balance(&no_addresses),
            Err(TransparentIndexesUnavailable)
        );
        assert_eq!(
            state.db.partial_finalized_address_utxos(&no_addresses),
            Err(TransparentIndexesUnavailable)
        );
        assert_eq!(
            state
                .db
                .partial_finalized_transparent_tx_ids(&no_addresses, Height(0)..=Height(4)),
            Err(TransparentIndexesUnavailable)
        );

        // Indexed databases still answer an empty address set with empty results.
        assert_eq!(
            indexed
                .db
                .partial_finalized_transparent_balance(&no_addresses),
            Ok((Amount::zero(), 0))
        );
        assert_eq!(
            indexed.db.partial_finalized_address_utxos(&no_addresses),
            Ok(BTreeMap::new())
        );
        assert_eq!(
            indexed
                .db
                .partial_finalized_transparent_tx_ids(&no_addresses, Height(0)..=Height(4)),
            Ok(BTreeMap::new())
        );

        // Outpoint UTXO and Ironwood nullifier queries still work.
        assert!(state.db.utxo(&chain.unspent_output).is_some());
        assert!(state.db.contains_ironwood_nullifier(&ironwood_nullifier));
    }

    // The read service, which backs the RPC methods, returns the typed error.
    let (mut read_state, _db, _sender) =
        init_read_only(config.clone(), &network).expect("read-only state opens");

    let is_unavailable = |error: crate::BoxError| {
        error.downcast_ref::<TransparentIndexesUnavailable>()
            == Some(&TransparentIndexesUnavailable)
    };

    let addresses = HashSet::from([chain.address, chain.other]);
    for request in [
        ReadRequest::AddressBalance(addresses.clone()),
        ReadRequest::UtxosByAddresses(addresses.clone()),
        ReadRequest::TransactionIdsByAddresses {
            addresses: addresses.clone(),
            height_range: Height(0)..=Height(4),
        },
        #[cfg(feature = "indexer")]
        ReadRequest::SpendingTransactionId(crate::Spend::OutPoint(chain.spent_coinbase)),
    ] {
        let error = read_state
            .ready()
            .await
            .expect("read service is ready")
            .call(request.clone())
            .await
            .expect_err("transparent index queries fail");
        assert!(is_unavailable(error), "{request:?} returns the typed error");
    }

    let response = read_state
        .ready()
        .await
        .expect("read service is ready")
        .call(ReadRequest::UnspentBestChainUtxo(chain.unspent_output))
        .await
        .expect("outpoint UTXO queries are answered");
    assert!(matches!(
        response,
        ReadResponse::UnspentBestChainUtxo(Some(_))
    ));

    #[cfg(feature = "indexer")]
    {
        let response = read_state
            .ready()
            .await
            .expect("read service is ready")
            .call(ReadRequest::SpendingTransactionId(crate::Spend::Ironwood(
                ironwood_nullifier,
            )))
            .await
            .expect("shielded spender queries are answered");
        assert!(
            matches!(response, ReadResponse::TransactionId(Some(_))),
            "the Ironwood spender is indexed: {response:?}"
        );
    }
}

/// A database with incomplete transparent indexes can't be reopened in archive mode.
#[test]
fn skipped_database_is_refused_in_archive_mode() {
    let _init_guard = zakura_test::init();
    let chain = test_chain();
    let network = chain.network.clone();

    let dir = TempDir::new().expect("temp dir");
    {
        let mut state = open(
            &config_at(dir.path(), NodeConfig::PrunedCheckpoint),
            &network,
        );
        commit_checkpoint(&mut state, &chain.blocks);
    }
    assert!(matches!(
        FinalizedState::new(&config_at(dir.path(), NodeConfig::Archive), &network),
        Err(StateInitError::TransparentIndexesMissingInArchiveMode { .. })
    ));

    // The durable marker alone also refuses archive mode.
    let marked = TempDir::new().expect("temp dir");
    {
        let state = open(
            &config_at(marked.path(), NodeConfig::PrunedFullVerification),
            &network,
        );
        let mut batch = crate::DiskWriteBatch::new();
        batch.prepare_transparent_indexes_skipped_marker_batch(&state.db, Height(0));
        state.db.write_batch(batch).expect("marker write succeeds");
        assert!(state.db.skips_transparent_indexes());
    }
    assert!(matches!(
        FinalizedState::new(&config_at(marked.path(), NodeConfig::Archive), &network),
        Err(StateInitError::TransparentIndexesMissingInArchiveMode { .. })
    ));

    // The refused database is still usable as a pruned database.
    let state = open(
        &config_at(dir.path(), NodeConfig::PrunedCheckpoint),
        &network,
    );
    assert_eq!(state.db.finalized_tip_height(), Some(Height(3)));
}

/// The existing major-format upgrade preserves indexed chain state before writes stop.
#[test]
fn indexed_database_upgrades_in_place_before_index_updates_stop() {
    let _init_guard = zakura_test::init();
    let chain = test_chain();
    let dir = TempDir::new().expect("temp dir");
    let initial_config = config_at(dir.path(), NodeConfig::PrunedFullVerification);
    let current_version = state_database_format_version_in_code();
    let previous_version = semver::Version::new(29, 1, 0);
    let current_path =
        initial_config.db_path(STATE_DATABASE_KIND, current_version.major, &chain.network);
    let previous_path =
        initial_config.db_path(STATE_DATABASE_KIND, previous_version.major, &chain.network);
    let consensus_before;
    let indexes_before;
    {
        let mut state = open(&initial_config, &chain.network);
        commit_checkpoint(&mut state, &chain.blocks[..3]);
        consensus_before = consensus_column_families(&state.db);
        indexes_before = TRANSPARENT_INDEX_COLUMN_FAMILIES
            .map(|cf| state.db.raw_column_family_entries_for_test(cf));
        // These column families have unchanged encodings in the prior format. Recreate its
        // version metadata and directory to exercise the real cache-reuse and upgrade path.
        crate::write_database_format_version_to_disk(
            &initial_config,
            STATE_DATABASE_KIND,
            current_version.major,
            &previous_version,
            &chain.network,
        )
        .expect("fixture version is written");
    }
    std::fs::create_dir_all(
        previous_path
            .parent()
            .expect("version directory has a parent"),
    )
    .expect("previous version directory exists");
    std::fs::rename(&current_path, &previous_path)
        .expect("fixture moves to the previous version path");
    let config = config_at(dir.path(), NodeConfig::PrunedCheckpoint);
    {
        let mut state = open(&config, &chain.network);
        assert!(
            !previous_path.exists(),
            "the existing database was moved, not copied or resynced"
        );
        assert_eq!(state.db.path(), current_path);
        assert_eq!(
            state
                .db
                .format_version_on_disk()
                .expect("version is readable"),
            Some(current_version.clone())
        );
        assert_eq!(consensus_column_families(&state.db), consensus_before);
        assert_eq!(
            TRANSPARENT_INDEX_COLUMN_FAMILIES
                .map(|cf| state.db.raw_column_family_entries_for_test(cf)),
            indexes_before
        );
        assert_eq!(state.db.transparent_indexes_skipped_from(), None);
        commit_checkpoint(&mut state, &chain.blocks[3..]);
        assert_eq!(state.db.transparent_indexes_skipped_from(), Some(Height(3)));
        assert_eq!(
            TRANSPARENT_INDEX_COLUMN_FAMILIES
                .map(|cf| state.db.raw_column_family_entries_for_test(cf)),
            indexes_before
        );
        assert_eq!(state.db.finalized_tip_height(), Some(Height(3)));
    }
    // A binary opening through the released format's normal path cannot see stale rows.
    let released = ZakuraDb::new(
        &initial_config,
        STATE_DATABASE_KIND,
        &previous_version,
        &chain.network,
        true,
        STATE_COLUMN_FAMILIES_IN_CODE
            .iter()
            .map(ToString::to_string),
        true,
    );
    assert!(matches!(
        released,
        Err(StateInitError::ReadOnlyDatabaseNotFound { .. })
    ));
    let state = open(&config, &chain.network);
    assert_eq!(state.db.finalized_tip_height(), Some(Height(3)));
    assert!(state.db.skips_transparent_indexes());
}

/// Skipping a release can reuse a cache only through explicitly reusable major upgrades.
#[test]
fn cache_reuse_requires_every_intermediate_major_upgrade() {
    use crate::service::finalized_state::disk_db::DiskDb;

    let _init_guard = zakura_test::init();
    let chain = test_chain();
    let dir = TempDir::new().expect("temp dir");
    let config = config_at(dir.path(), NodeConfig::PrunedFullVerification);
    let current = state_database_format_version_in_code();
    let previous = semver::Version::new(28, 2, 0);
    let current_path = config.db_path(STATE_DATABASE_KIND, current.major, &chain.network);
    let previous_path = config.db_path(STATE_DATABASE_KIND, previous.major, &chain.network);
    {
        let mut state = open(&config, &chain.network);
        commit_checkpoint(&mut state, &chain.blocks[..3]);
        crate::write_database_format_version_to_disk(
            &config,
            STATE_DATABASE_KIND,
            current.major,
            &previous,
            &chain.network,
        )
        .expect("fixture version is written");
    }
    std::fs::create_dir_all(
        previous_path
            .parent()
            .expect("version directory has a parent"),
    )
    .expect("older version parent exists");
    std::fs::rename(&current_path, &previous_path)
        .expect("fixture moves to the older version directory");
    assert_eq!(
        DiskDb::try_reusing_previous_db_after_major_upgrade(
            &[30],
            &current,
            &config,
            STATE_DATABASE_KIND,
            &chain.network
        ),
        None,
        "an unsupported intermediate major upgrade prevents reuse"
    );
    assert!(previous_path.exists());
    assert!(!current_path.exists());
    assert_eq!(
        DiskDb::try_reusing_previous_db_after_major_upgrade(
            &[29, 30],
            &current,
            &config,
            STATE_DATABASE_KIND,
            &chain.network
        ),
        Some(previous),
        "all intermediate upgrades explicitly permit cache reuse"
    );
    assert!(!previous_path.exists());
    assert!(current_path.exists());
}

/// Offline rollback reverses the canonical UTXO set of a database without transparent indexes,
/// keeps its marker, and decides that from durable state rather than the checkpoint setting.
#[test]
fn rollback_without_transparent_indexes_reverses_consensus_state() {
    let _init_guard = zakura_test::init();
    let chain = test_chain();
    let network = chain.network.clone();

    let skipped_dir = TempDir::new().expect("temp dir");
    let fresh_dir = TempDir::new().expect("temp dir");
    let indexed_dir = TempDir::new().expect("temp dir");
    {
        let mut skipped = open(
            &config_at(skipped_dir.path(), NodeConfig::PrunedCheckpoint),
            &network,
        );
        commit_checkpoint(&mut skipped, &chain.blocks);
        let mut fresh = open(
            &config_at(fresh_dir.path(), NodeConfig::PrunedCheckpoint),
            &network,
        );
        commit_checkpoint(&mut fresh, &chain.blocks[..2]);
        let mut indexed = open(
            &config_at(indexed_dir.path(), NodeConfig::PrunedFullVerification),
            &network,
        );
        commit_checkpoint(&mut indexed, &chain.blocks);
    }

    let options = RollbackFinalizedStateOptions {
        target_height: Height(1),
        keep_rolled_back_blocks: false,
        max_checkpoint_height: None,
    };

    // The rollback config disables checkpoint sync, like an operator config that differs from
    // the one the database was synced with.
    rollback_finalized_state(
        config_at(skipped_dir.path(), NodeConfig::PrunedFullVerification),
        &network,
        options.clone(),
    )
    .expect("rollback succeeds when every rolled-back block is available");
    rollback_finalized_state(
        config_at(indexed_dir.path(), NodeConfig::PrunedFullVerification),
        &network,
        options,
    )
    .expect("indexed rollback succeeds");

    let skipped = open(
        &config_at(skipped_dir.path(), NodeConfig::PrunedFullVerification),
        &network,
    );
    let fresh = open(
        &config_at(fresh_dir.path(), NodeConfig::PrunedCheckpoint),
        &network,
    );
    let indexed = open(
        &config_at(indexed_dir.path(), NodeConfig::PrunedFullVerification),
        &network,
    );

    assert_eq!(skipped.db.finalized_tip_height(), Some(Height(1)));
    assert_eq!(
        skipped
            .db
            .raw_column_family_entries_for_test("utxo_by_out_loc"),
        fresh
            .db
            .raw_column_family_entries_for_test("utxo_by_out_loc"),
        "the UTXO set matches a database synced only to the target"
    );
    assert_eq!(
        skipped
            .db
            .raw_column_family_entries_for_test("utxo_by_out_loc"),
        indexed
            .db
            .raw_column_family_entries_for_test("utxo_by_out_loc"),
        "the UTXO set matches an indexed database rolled back to the target"
    );
    assert_eq!(
        skipped.db.finalized_value_pool(),
        indexed.db.finalized_value_pool()
    );
    assert!(skipped.db.utxo(&chain.spent_coinbase).is_some());
    assert_eq!(transparent_index_entry_count(&skipped.db), 0);
    assert_eq!(
        skipped.db.transparent_indexes_skipped_from(),
        Some(Height(0)),
        "rollback keeps the marker"
    );
    assert_eq!(
        indexed
            .db
            .address_balance(&chain.address)
            .expect("indexed databases answer balance queries")
            .map(|(balance, _)| balance),
        Some(amount(50_000)),
        "indexed rollback restores the address balance"
    );
}

/// Rollback fails without writing anything when a rolled-back block body was pruned.
#[test]
fn rollback_without_transparent_indexes_does_not_mutate_when_bodies_are_missing() {
    let _init_guard = zakura_test::init();
    let chain = test_chain();
    let network = chain.network.clone();
    let dir = TempDir::new().expect("temp dir");
    let config = config_at(dir.path(), NodeConfig::PrunedCheckpoint);

    let before = {
        let mut state = open(&config, &network);
        commit_checkpoint(&mut state, &chain.blocks);

        let mut batch = crate::DiskWriteBatch::new();
        batch.prepare_prune_batch(&state.db, Height(2), Height(3));
        state.db.write_batch(batch).expect("pruning succeeds");

        consensus_column_families(&state.db)
    };

    let error = rollback_finalized_state(
        config.clone(),
        &network,
        RollbackFinalizedStateOptions {
            target_height: Height(1),
            keep_rolled_back_blocks: false,
            max_checkpoint_height: None,
        },
    )
    .expect_err("rollback needs every rolled-back block body");
    // Reversing height 3 needs the output it spent from the pruned height 2 body, and reversing
    // height 2 needs that body itself. Either way rollback reports the existing missing-data error.
    assert!(
        matches!(
            error,
            RollbackFinalizedStateError::MissingBlock { height } if height == Height(2)
        ) || matches!(
            error,
            RollbackFinalizedStateError::MissingTransaction { hash }
                if chain.blocks[2].transactions.iter().any(|tx| tx.hash() == hash)
        ),
        "{error:?}"
    );

    let state = open(&config, &network);
    assert_eq!(
        consensus_column_families(&state.db),
        before,
        "nothing was written"
    );
    assert_eq!(state.db.transparent_indexes_skipped_from(), Some(Height(0)));
}

/// Startup, shutdown, and periodic format checks pass on a database without transparent indexes,
/// and never write the omitted indexes.
#[test]
fn format_checks_never_rebuild_omitted_transparent_indexes() {
    let _init_guard = zakura_test::init();
    let chain = test_chain();
    let network = chain.network.clone();
    let dir = TempDir::new().expect("temp dir");
    let config = config_at(dir.path(), NodeConfig::PrunedCheckpoint);

    let (ironwood_tx, ironwood_nullifier) = ironwood_v6_tx(Height(5));
    let ironwood_block = child_block(
        chain.blocks.last().expect("the chain has blocks"),
        vec![coinbase_tx(Height(4), amount(1), &chain.other), ironwood_tx],
    );

    {
        let mut state = open(&config, &network);
        commit_checkpoint(&mut state, &chain.blocks);
        commit_checkpoint(&mut state, &[ironwood_block]);
    }

    // Reopening runs the startup format check, which includes the indexer spender rebuild when
    // that feature is enabled.
    let state = open(&config, &network);
    let (_cancel_sender, cancel_receiver) = cancel_channel();
    assert_eq!(
        DbFormatChange::format_validity_checks_detailed(&state.db, &cancel_receiver)
            .expect("checks are not cancelled"),
        Ok(()),
        "the address received check does not demand balances that were never built"
    );

    assert_eq!(transparent_index_entry_count(&state.db), 0);
    assert!(state.db.contains_ironwood_nullifier(&ironwood_nullifier));
    #[cfg(feature = "indexer")]
    assert!(
        state
            .db
            .spending_transaction_hash(&crate::Spend::Ironwood(ironwood_nullifier))
            .expect("shielded spender queries are answered")
            .is_some(),
        "the Ironwood spender is still indexed"
    );
}

/// The marker key can't be mistaken for the raw transaction pruning cursor.
#[test]
fn transparent_index_marker_does_not_mark_a_database_pruned() {
    let _init_guard = zakura_test::init();
    let chain = test_chain();
    let network = chain.network.clone();
    let dir = TempDir::new().expect("temp dir");

    let mut state = open(
        &config_at(dir.path(), NodeConfig::PrunedCheckpoint),
        &network,
    );
    commit_checkpoint(&mut state, &chain.blocks);

    assert!(state.db.transparent_indexes_skipped_from().is_some());
    assert_eq!(state.db.lowest_retained_height(), None);
    assert!(!state.db.is_pruned());
    assert!(state.db.block(Height(1).into()).is_some());
    assert_eq!(
        state
            .db
            .raw_column_family_entries_for_test(PRUNING_METADATA)
            .len(),
        1,
        "only the marker is stored"
    );
}
