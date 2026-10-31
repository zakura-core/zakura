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

/// A real disk result keeps admission after its worker and database handles end.
#[tokio::test(flavor = "multi_thread")]
async fn disk_block_results_keep_shared_admission_through_final_payload_holders() {
    use crate::{service::StateService, ReadRequest, ReadResponse};
    use std::{num::NonZeroU16, time::Duration};
    use tokio::time::timeout;
    use tower::ServiceExt;
    use zakura_node_services::block_memory::{model::Probe, AdmissionFailure, BlockHandle};

    let _guard = zakura_test::init();
    let deadline = Duration::from_secs(10);
    let config = Config {
        read_block_limit: NonZeroU16::MIN,
        ..Config::ephemeral()
    };
    let (mut state, read, _, _) = StateService::new(config, &Mainnet, Height::MAX, 0)
        .await
        .unwrap();
    let genesis: Arc<Block> = Arc::new(
        zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into()
            .unwrap(),
    );
    timeout(
        deadline,
        state.queue_and_commit_to_finalized_state(CheckpointVerifiedBlock::from(genesis.clone())),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    let ReadResponse::Block(Some(block)) = timeout(
        deadline,
        read.clone().oneshot(ReadRequest::Block(Height(0).into())),
    )
    .await
    .unwrap()
    .unwrap() else {
        panic!("the disk contains genesis")
    };
    assert_eq!(block.hash(), genesis.hash());
    let BlockHandle::Owned(owner) = &block else {
        panic!("disk reads return an admitted owner")
    };
    let probe = Probe::new(owner);
    let bytes = owner.decoded_bytes();
    let pool = read.db.read_blocks.clone();
    let header = block.retained_header();
    let transaction = block.transaction(0).unwrap();
    drop(block);

    let error = timeout(
        deadline,
        read.clone().oneshot(ReadRequest::Block(Height(0).into())),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(
        error.downcast_ref::<AdmissionFailure>(),
        Some(&AdmissionFailure::Capacity)
    );
    assert_eq!(
        read.db
            .clone()
            .block_and_size(Height(0).into())
            .unwrap_err(),
        AdmissionFailure::Capacity
    );
    assert_eq!(pool.reserved_blocks(), Some(1));
    let actual = probe.read(1, std::slice::from_ref(&pool)).unwrap();
    assert_eq!(actual.measured_bytes, Some(bytes));
    assert_eq!(actual.header_holders, 1);
    assert_eq!(actual.transaction_holders, vec![1]);
    assert_eq!(pool.reserved_bytes(), bytes);

    // Control reads and tip publication do not spend the data-result allowance.
    timeout(
        deadline,
        read.clone()
            .oneshot(ReadRequest::BestChainNextMedianTimePast),
    )
    .await
    .unwrap()
    .unwrap();
    let tip = crate::service::finalized_chain_tip(&read.db)
        .unwrap()
        .unwrap();
    assert_eq!(tip.hash, genesis.hash());
    assert_eq!(pool.reserved_blocks(), Some(1));
    drop(tip);

    drop(header);
    assert_eq!(transaction.hash(), genesis.transactions[0].hash());
    assert_eq!(pool.reserved_blocks(), Some(1));
    drop(transaction);
    assert_eq!(pool.reserved_blocks(), Some(0));
    assert_eq!(pool.reserved_bytes(), 0);
    let actual = probe.read(1, std::slice::from_ref(&pool)).unwrap();
    assert_eq!(actual.holders, 0);
    assert_eq!(actual.header_holders, 0);
    assert_eq!(actual.transaction_holders, vec![0]);
    assert!(read.db.block(Height(1).into()).unwrap().is_none());
    assert_eq!(pool.reserved_blocks(), Some(0));

    let (block, size) = read.db.block_and_size(Height(0).into()).unwrap().unwrap();
    assert_eq!(size, genesis.zcash_serialized_size());
    let BlockHandle::Owned(owner) = &block else {
        panic!("sized reads retain admission")
    };
    let probe = Probe::new(owner);
    drop(state);
    drop(read);
    assert_eq!(block.hash(), genesis.hash());
    assert_eq!(pool.reserved_blocks(), Some(1));
    assert_eq!(
        probe
            .read(2, std::slice::from_ref(&pool))
            .unwrap()
            .measured_bytes,
        Some(bytes)
    );
    drop(block);
    assert_eq!(pool.reserved_blocks(), Some(0));
    assert_eq!(
        probe
            .read(2, std::slice::from_ref(&pool))
            .unwrap()
            .header_holders,
        0
    );
}

/// Direct reads, services, and iterators share final-holder transaction admission.
#[tokio::test(flavor = "multi_thread")]
async fn disk_transactions_keep_shared_admission_through_clones_and_database_drop() {
    use crate::{service::StateService, ReadRequest, ReadResponse};
    use std::{num::NonZeroU16, time::Duration};
    use tokio::time::timeout;
    use tower::ServiceExt;
    use zakura_node_services::block_memory::{
        model::PayloadProbe, PayloadAdmissionFailure, TransactionHandle,
    };

    let _guard = zakura_test::init();
    let deadline = Duration::from_secs(10);
    let config = Config {
        read_transaction_limit: NonZeroU16::MIN,
        ..Config::ephemeral()
    };
    let (mut state, read, _, _) = StateService::new(config, &Mainnet, Height::MAX, 0)
        .await
        .unwrap();
    let genesis: Arc<Block> = Arc::new(
        zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into()
            .unwrap(),
    );
    timeout(
        deadline,
        state.queue_and_commit_to_finalized_state(CheckpointVerifiedBlock::from(genesis.clone())),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    let hash = genesis.transactions[0].hash();
    let ReadResponse::Transaction(Some(result)) = timeout(
        deadline,
        read.clone().oneshot(ReadRequest::Transaction(hash)),
    )
    .await
    .unwrap()
    .unwrap() else {
        panic!("the stored genesis transaction is available")
    };
    assert_eq!(result.tx.hash(), hash);
    assert_eq!(result.height, Height(0));
    let pool = read.db.read_transactions.clone();
    assert_eq!(
        pool.available_slots(),
        0,
        "a returned disk allocation must retain admission"
    );
    let TransactionHandle::Standalone(owner) = &result.tx else {
        panic!("disk transaction results carry standalone admission")
    };
    let probe = PayloadProbe::new(owner);
    let retained = result.tx.clone();
    drop(result);
    assert_eq!(probe.holders(), 1);
    assert_eq!(
        probe.serialized_bytes(),
        Some(genesis.transactions[0].zcash_serialized_size())
    );
    assert_eq!(
        pool.available_slots(),
        0,
        "live payloads retain their original admission"
    );
    assert_eq!(
        read.db.clone().transaction(hash).unwrap_err(),
        PayloadAdmissionFailure::Capacity
    );
    let error = timeout(
        deadline,
        read.clone().oneshot(ReadRequest::AnyChainTransaction(hash)),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(
        error.downcast_ref::<PayloadAdmissionFailure>(),
        Some(&PayloadAdmissionFailure::Capacity)
    );
    let mut refused = read.db.transactions_by_height(Height(0));
    assert_eq!(
        refused.next().unwrap().unwrap_err(),
        PayloadAdmissionFailure::Capacity
    );
    assert!(
        refused.next().is_none(),
        "a failed iterator ends after its explicit error"
    );
    drop(refused);
    timeout(deadline, read.clone().oneshot(ReadRequest::Tip))
        .await
        .unwrap()
        .unwrap();
    use crate::service::finalized_state::disk_format::upgrade::{
        cancel_channel, DbFormatChange, FormatChangeError,
    };
    let (_cancel, cancel_receiver) = cancel_channel();
    let error = DbFormatChange::check_new_blocks(&read.db)
        .run_format_change_or_check(&read.db, None, &cancel_receiver)
        .unwrap_err();
    assert!(matches!(
        error,
        FormatChangeError::PayloadAdmission(PayloadAdmissionFailure::Capacity)
    ));
    use crate::service::read::historical_tree::{
        replay_with_subtrees, DerivedFrontiers, HistoricalTreeDerivationError,
    };
    let replay = || {
        replay_with_subtrees(&read.db, Height(0), 0, DerivedFrontiers::empty(), |_, _| {
            panic!("genesis has no shielded commitments")
        })
    };
    assert!(matches!(
        replay(),
        Err(HistoricalTreeDerivationError::PayloadAdmission(
            PayloadAdmissionFailure::Capacity
        ))
    ));
    drop(retained);
    assert_eq!(probe.holders(), 0);
    assert_eq!(probe.serialized_bytes(), None);
    assert_eq!(pool.available_slots(), 1);
    DbFormatChange::check_new_blocks(&read.db)
        .run_format_change_or_check(&read.db, None, &cancel_receiver)
        .unwrap();
    let replayed = replay().expect("historical replay progresses with one transaction slot");
    assert_eq!(
        replayed.sapling.root(),
        DerivedFrontiers::empty().sapling.root()
    );
    assert_eq!(pool.available_slots(), 1);
    assert!(read
        .db
        .transaction(zakura_chain::transaction::Hash([0xff; 32]))
        .unwrap()
        .is_none());
    assert_eq!(pool.available_slots(), 1);

    let mut transactions = read.db.transactions_by_height(Height(0));
    let (location, transaction) = transactions.next().unwrap().unwrap();
    assert_eq!(location.height, Height(0));
    assert_eq!(transaction.hash(), hash);
    assert_eq!(
        read.db.transaction(hash).unwrap_err(),
        PayloadAdmissionFailure::Capacity
    );
    let TransactionHandle::Standalone(owner) = &transaction else {
        panic!("decoded iterators return admitted owners")
    };
    let probe = PayloadProbe::new(owner);
    drop(transactions);
    drop(state);
    drop(read);
    assert_eq!(probe.holders(), 1);
    assert_eq!(pool.available_slots(), 0);
    assert_eq!(transaction.hash(), hash);
    drop(transaction);
    assert_eq!(probe.holders(), 0);
    assert_eq!(pool.available_slots(), 1);
}

/// Header and serialized block results retain shared admission through final holders.
#[tokio::test(flavor = "multi_thread")]
async fn disk_header_and_raw_block_results_keep_shared_admission() {
    use crate::{service::StateService, ReadRequest, ReadResponse};
    use std::{num::NonZeroU16, time::Duration};
    use tokio::time::timeout;
    use tower::ServiceExt;
    use zakura_node_services::block_memory::{
        model::PayloadProbe, HeaderHandle, PayloadAdmissionFailure,
    };

    let _guard = zakura_test::init();
    let deadline = Duration::from_secs(10);
    let config = Config {
        read_header_limit: NonZeroU16::MIN,
        read_raw_block_limit: NonZeroU16::MIN,
        ..Config::ephemeral()
    };
    let (mut state, read, _, _) = StateService::new(config, &Mainnet, Height::MAX, 0)
        .await
        .unwrap();
    let genesis: Arc<Block> = Arc::new(
        zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into()
            .unwrap(),
    );
    timeout(
        deadline,
        state.queue_and_commit_to_finalized_state(CheckpointVerifiedBlock::from(genesis.clone())),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    let height_zero = crate::HashOrHeight::Height(Height(0));
    let header_read = ReadRequest::BlockHeader(height_zero);
    let ReadResponse::BlockHeader { header, .. } =
        timeout(deadline, read.clone().oneshot(header_read.clone()))
            .await
            .unwrap()
            .unwrap()
    else {
        panic!("the stored genesis header is available")
    };
    assert_eq!(*header, *genesis.header);
    let pool = read.db.read_headers.clone();
    assert_eq!(
        pool.available_slots(),
        0,
        "a returned disk header must retain admission"
    );
    let HeaderHandle::Standalone(owner) = &header else {
        panic!("disk header results carry standalone admission")
    };
    let probe = PayloadProbe::new(owner);
    let clone = header.clone();
    drop(header);
    assert_eq!(probe.holders(), 1);
    assert_eq!(
        probe.serialized_bytes(),
        Some(genesis.header.zcash_serialized_size())
    );
    assert_eq!(pool.available_slots(), 0, "live clones retain admission");
    let error = timeout(deadline, read.clone().oneshot(header_read))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<PayloadAdmissionFailure>(),
        Some(&PayloadAdmissionFailure::Capacity)
    );
    let error = timeout(
        deadline,
        read.clone().oneshot(ReadRequest::FindBlockHeaders {
            known_blocks: vec![],
            stop: None,
        }),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(
        error.downcast_ref::<PayloadAdmissionFailure>(),
        Some(&PayloadAdmissionFailure::Capacity),
        "an empty Find response cannot hide a capacity refusal"
    );
    // Chain-context reads and control queries stay available at full capacity.
    assert!(read.db.block_header(height_zero).is_some());
    timeout(deadline, read.clone().oneshot(ReadRequest::Tip))
        .await
        .unwrap()
        .unwrap();
    drop(clone);
    assert_eq!(probe.holders(), 0);
    assert_eq!(pool.available_slots(), 1);
    assert!(read
        .db
        .block_header_result(crate::HashOrHeight::Height(Height(9)))
        .unwrap()
        .is_none());
    assert_eq!(pool.available_slots(), 1);

    let raw_pool = read.db.read_raw_blocks.clone();
    let raw = read.db.raw_block_result(height_zero).unwrap().unwrap();
    assert_eq!(
        raw.as_slice(),
        zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES.as_slice()
    );
    assert_eq!(
        raw_pool.available_slots(),
        0,
        "a serialized block result must retain admission"
    );
    assert_eq!(
        read.db.raw_block_result(height_zero).unwrap_err(),
        PayloadAdmissionFailure::Capacity
    );
    #[cfg(feature = "indexer")]
    {
        let error = timeout(
            deadline,
            read.clone().oneshot(ReadRequest::RawBlocksByHeightRange {
                start: Height(0),
                count: 1,
            }),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(
            error.downcast_ref::<PayloadAdmissionFailure>(),
            Some(&PayloadAdmissionFailure::Capacity),
            "a short range means the chain ended, so refusal stays explicit"
        );
    }
    let probe = PayloadProbe::new(&raw);
    let clone = raw.clone();
    drop(raw);
    assert_eq!(probe.holders(), 1);
    drop(state);
    drop(read);
    assert_eq!(
        raw_pool.available_slots(),
        0,
        "database teardown keeps the charge"
    );
    assert_eq!(clone.len(), genesis.zcash_serialized_size());
    drop(clone);
    assert_eq!(probe.holders(), 0);
    assert_eq!(raw_pool.available_slots(), 1);
}

/// Index migration reads progress when four workers share one decode slot.
#[test]
fn transaction_index_migrations_progress_with_one_decode_slot() {
    use crate::service::finalized_state::disk_db::WriteDisk;
    use crate::service::finalized_state::disk_format::upgrade;
    use std::num::NonZeroU16;

    let _guard = zakura_test::init();
    let config = Config {
        read_transaction_limit: NonZeroU16::MIN,
        ..Config::ephemeral()
    };
    let db = ZakuraDb::new(
        &config,
        STATE_DATABASE_KIND,
        &state_database_format_version_in_code(),
        &Mainnet,
        true,
        STATE_COLUMN_FAMILIES_IN_CODE
            .iter()
            .map(ToString::to_string),
        false,
    )
    .unwrap();
    let block: Block = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let mut batch = DiskWriteBatch::new();
    let transactions = db.db.cf_handle("tx_by_loc").unwrap();
    // These rows exercise migration reads; they are not a complete chain fixture.
    for height in 0..32 {
        batch.zs_insert(
            &transactions,
            crate::TransactionLocation::from_index(Height(height), 1),
            block.transactions[0].clone(),
        );
    }
    db.write_batch(batch).unwrap();
    let (_, transaction) = db
        .transactions_by_height(Height(0))
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(transaction.hash(), block.transactions[0].hash());
    drop(transaction);
    let (_cancel, receiver) = upgrade::cancel_channel();
    let workers = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    workers.install(|| {
        #[cfg(feature = "indexer")]
        upgrade::track_tx_locs_by_spends::run(Height(31), &db, &receiver).unwrap();
        #[cfg(not(feature = "indexer"))]
        upgrade::drop_tx_locs_by_spends::run(Height(31), &db, &receiver).unwrap();
    });
    assert_eq!(db.read_transactions.available_slots(), 1);
}

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
                block: zakura_node_services::block_memory::BlockHandle::Shared(
                    original_block.clone(),
                ),
                hash,
                height: Height(0),
                new_outputs,
                transaction_hashes,
                deferred_pool_balance_change: None,
                auth_data_root: None,
                receipt_order: None,
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
            .unwrap()
            .expect("block was stored at height");

        if stored_block != original_block.clone().into() {
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

        assert_eq!(stored_block, original_block.clone().into());

        // The raw read path assembles exactly the bytes the block serializes
        // to, including the CompactSize transaction count and the transaction
        // order in multi-transaction blocks.
        assert_eq!(
            state
                .raw_block_result(finalized.height.into())
                .unwrap()
                .expect("raw block was stored at height")
                .to_vec(),
            original_block
                .zcash_serialize_to_vec()
                .expect("serialization to vec never fails"),
            "raw block bytes must match the block's consensus serialization",
        );
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

/// Variable-size disk reads charge decoded bytes to their final holder, separately
/// from parser memory, and a refusal is not reported as missing data.
#[tokio::test(flavor = "multi_thread")]
async fn disk_results_charge_retained_bytes_until_their_final_holders_drop() {
    use crate::service::StateService;
    use std::{num::NonZeroU64, time::Duration};
    use tokio::time::timeout;
    use zakura_node_services::block_memory::{
        AdmissionFailure, PayloadAdmissionFailure, ReadMemoryBudget,
    };

    let _guard = zakura_test::init();
    let (mut state, read, _, _) = StateService::new(Config::ephemeral(), &Mainnet, Height::MAX, 0)
        .await
        .unwrap();
    let genesis: Arc<Block> = Arc::new(
        zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into()
            .unwrap(),
    );
    timeout(
        Duration::from_secs(10),
        state.queue_and_commit_to_finalized_state(CheckpointVerifiedBlock::from(genesis.clone())),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    let mut db = read.db.clone();
    let budget = ReadMemoryBudget::new(NonZeroU64::MAX, NonZeroU64::MIN);
    db.read_memory = budget.clone();
    let height = Height(0).into();

    let block = db.block(height).unwrap().unwrap();
    let decoded = genesis.attributed_memory_size_bytes();
    assert_eq!(budget.retained_bytes(), decoded);
    assert_eq!(
        budget.parser_bytes(),
        0,
        "the parser charge ended with the read"
    );

    assert_eq!(
        db.block(height).unwrap_err(),
        AdmissionFailure::Capacity,
        "the first result holds the only retained capacity"
    );
    assert_eq!(
        db.block_and_size(height).unwrap_err(),
        AdmissionFailure::Capacity
    );
    assert_eq!(
        db.raw_block_result(height).unwrap_err(),
        PayloadAdmissionFailure::Capacity
    );
    let hash = genesis.transactions[0].hash();
    assert_eq!(
        db.transaction(hash).unwrap_err(),
        PayloadAdmissionFailure::Capacity
    );
    assert_eq!(budget.retained_bytes(), decoded, "refusals leave no charge");
    assert_eq!(budget.parser_bytes(), 0);

    let transaction = block.transaction(0).unwrap();
    drop(block);
    assert_eq!(budget.retained_bytes(), decoded);
    drop(transaction);
    assert_eq!(budget.retained_bytes(), 0);

    let (raw, stored_size) = db
        .raw_block_result(height)
        .map(|raw| {
            let raw = raw.expect("the block is stored");
            let size = raw.len();
            (raw, size)
        })
        .unwrap();
    assert_eq!(
        raw.as_slice(),
        &zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES[..]
    );
    assert_eq!(
        budget.retained_bytes(),
        u64::try_from(raw.capacity()).unwrap()
    );
    drop(raw);

    let (_, size) = db.block_and_size(height).unwrap().unwrap();
    assert_eq!(size, stored_size, "both read paths report the stored size");

    assert!(db.block(Height(1).into()).unwrap().is_none());
    assert_eq!(budget.retained_bytes(), 0, "a missing block has no charge");
    assert_eq!(budget.parser_bytes(), 0);
}

/// The parser limit refuses rows before a read copies the rest of a block.
#[tokio::test(flavor = "multi_thread")]
async fn parser_limit_refuses_reads_while_another_parser_holds_it() {
    use crate::service::StateService;
    use std::{num::NonZeroU64, time::Duration};
    use tokio::time::timeout;
    use zakura_node_services::block_memory::{
        AdmissionFailure, DecodedBlockPool, ReadMemoryBudget,
    };

    let _guard = zakura_test::init();
    let (mut state, read, _, _) = StateService::new(Config::ephemeral(), &Mainnet, Height::MAX, 0)
        .await
        .unwrap();
    let genesis: Arc<Block> = Arc::new(
        zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into()
            .unwrap(),
    );
    timeout(
        Duration::from_secs(10),
        state.queue_and_commit_to_finalized_state(CheckpointVerifiedBlock::from(genesis)),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    let mut db = read.db.clone();
    let budget = ReadMemoryBudget::new(NonZeroU64::new(8).unwrap(), NonZeroU64::MAX);
    db.read_memory = budget.clone();

    let pool = DecodedBlockPool::for_block_count(NonZeroU64::new(4).unwrap());
    let outer = pool.try_decode_bounded(&budget, |outer_scope| {
        outer_scope.try_charge(8)?;
        assert_eq!(
            db.block(Height(0).into()).unwrap_err(),
            AdmissionFailure::Capacity,
            "an active parser holds the whole limit"
        );
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(budget.parser_bytes(), 8);
        Ok(None)
    });
    assert!(outer.unwrap().is_none());
    assert_eq!(budget.parser_bytes(), 0);

    assert!(
        db.block(Height(0).into()).unwrap().is_some(),
        "a lone parser finishes above its limit"
    );
}
