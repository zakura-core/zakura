//! Fixed test vectors for the ReadStateService.

use std::sync::Arc;

use tower::ServiceExt;
use zakura_chain::{
    block::{Block, Height},
    ironwood, orchard,
    parameters::{Network, Network::*},
    serialization::ZcashDeserializeInto,
    subtree::{
        NoteCommitmentSubtree, NoteCommitmentSubtreeData, NoteCommitmentSubtreeIndex,
        TRACKED_SUBTREE_HEIGHT,
    },
    transaction,
};

use zakura_test::{
    prelude::Result,
    transcript::{ExpectedTranscriptError, Transcript},
};

use crate::{
    constants::{state_database_format_version_in_code, STATE_DATABASE_KIND},
    init_test_services, populated_state,
    response::MinedTx,
    service::{
        finalized_state::{
            embedded_last_checkpoint_leaf_counts, DiskWriteBatch, SubtreeArtifact, SubtreeRecord,
            ZakuraDb, STATE_COLUMN_FAMILIES_IN_CODE,
        },
        non_finalized_state::Chain,
        read::{
            contiguous_subtrees_from, ironwood_subtrees, merge_published_subtrees,
            orchard_subtrees, retain_subtrees_completed_at_or_below, sapling_subtrees,
            tree::{
                first_missing_subtree_index, is_syncing_below_last_checkpoint,
                sapling_subtrees_with_gaps, subtree_completed_by_last_checkpoint,
            },
        },
    },
    Config, HistoricalSubtreeUnavailable, HistoricalSubtreeUnavailableReason, ReadRequest,
    ReadResponse,
};

/// Test that ReadStateService responds correctly when empty.
#[tokio::test]
async fn empty_read_state_still_responds_to_requests() -> Result<()> {
    let _init_guard = zakura_test::init();

    let transcript = Transcript::from(empty_state_test_cases());

    let network = Mainnet;
    let (_state, read_state, _latest_chain_tip, _chain_tip_change) =
        init_test_services(&network).await;

    transcript.check(read_state).await?;

    Ok(())
}

/// Test that ReadStateService responds correctly when the state contains blocks.
#[tokio::test(flavor = "multi_thread")]
async fn populated_read_state_responds_correctly() -> Result<()> {
    let _init_guard = zakura_test::init();

    // Create a continuous chain of mainnet blocks from genesis
    let blocks: Vec<Arc<Block>> = zakura_test::vectors::CONTINUOUS_MAINNET_BLOCKS
        .values()
        .map(|block_bytes| block_bytes.zcash_deserialize_into().unwrap())
        .collect();

    let (_state, read_state, _latest_chain_tip, _chain_tip_change) =
        populated_state(blocks.clone(), &Mainnet).await;

    let tip_height = Height(blocks.len() as u32 - 1);

    let empty_cases = Transcript::from(empty_state_test_cases());
    empty_cases.check(read_state.clone()).await?;

    for block in blocks {
        let block_cases = vec![
            (
                ReadRequest::Block(block.hash().into()),
                Ok(ReadResponse::Block(Some(block.clone()))),
            ),
            (
                ReadRequest::Block(block.coinbase_height().unwrap().into()),
                Ok(ReadResponse::Block(Some(block.clone()))),
            ),
        ];

        let block_cases = Transcript::from(block_cases);
        block_cases.check(read_state.clone()).await?;

        // Spec: transactions in the genesis block are ignored.
        if block.coinbase_height().unwrap().0 == 0 {
            continue;
        }

        for transaction in &block.transactions {
            let transaction_cases = vec![(
                ReadRequest::Transaction(transaction.hash()),
                Ok(ReadResponse::Transaction(Some(MinedTx {
                    tx: transaction.clone(),
                    height: block.coinbase_height().unwrap(),
                    confirmations: 1 + tip_height.0 - block.coinbase_height().unwrap().0,
                    block_time: block.header.time,
                }))),
            )];

            let transaction_cases = Transcript::from(transaction_cases);
            transaction_cases.check(read_state.clone()).await?;
        }
    }

    Ok(())
}

/// Tests if Zebra combines the note commitment subtrees from the finalized and
/// non-finalized states correctly.
#[tokio::test]
async fn test_read_subtrees() -> Result<()> {
    use std::ops::Bound::*;

    let dummy_subtree = |(index, height)| {
        NoteCommitmentSubtree::new(
            u16::try_from(index).expect("should fit in u16"),
            Height(height),
            sapling_crypto::Node::from_bytes([0; 32]).unwrap(),
        )
    };

    let num_db_subtrees = 10;
    let num_chain_subtrees = 2;
    let index_offset = usize::try_from(num_db_subtrees).expect("constant should fit in usize");
    let db_height_range = 0..num_db_subtrees;
    let chain_height_range = num_db_subtrees..(num_db_subtrees + num_chain_subtrees);

    // Prepare the finalized state.
    let db = {
        let db = new_ephemeral_db();

        let db_subtrees = db_height_range.enumerate().map(dummy_subtree);
        for db_subtree in db_subtrees {
            let mut db_batch = DiskWriteBatch::new();
            db_batch.insert_sapling_subtree(&db, &db_subtree);
            db.write(db_batch)
                .expect("Writing a batch with a Sapling subtree should succeed.");
        }
        db
    };

    // Prepare the non-finalized state.
    let chain = {
        let mut chain = Chain::default();
        let chain_subtrees = chain_height_range
            .enumerate()
            .map(|(index, height)| dummy_subtree((index_offset + index, height)));

        for chain_subtree in chain_subtrees {
            chain.insert_sapling_subtree(chain_subtree);
        }

        Arc::new(chain)
    };

    let modify_chain = |chain: &Arc<Chain>, index: usize, height| {
        let mut chain = chain.as_ref().clone();
        chain.insert_sapling_subtree(dummy_subtree((index, height)));
        Some(Arc::new(chain))
    };

    // There should be 10 entries in db and 2 in chain with no overlap

    // Unbounded range should start at 0
    let all_subtrees = sapling_subtrees(Some(chain.clone()), &db, ..)?;
    assert_eq!(all_subtrees.len(), 12, "should have 12 subtrees in state");

    // Add a subtree to `chain` that overlaps and is not consistent with the db subtrees
    let first_chain_index = index_offset - 1;
    let end_height = Height(400_000);
    let modified_chain = modify_chain(&chain, first_chain_index, end_height.0);

    // The inconsistent entry and any later entries should be omitted
    let all_subtrees = sapling_subtrees(modified_chain.clone(), &db, ..)?;
    assert_eq!(all_subtrees.len(), 10, "should have 10 subtrees in state");

    let first_chain_index =
        NoteCommitmentSubtreeIndex(u16::try_from(first_chain_index).expect("should fit in u16"));

    // Entries should be returned without reading from disk if the chain contains the first subtree index in the range
    let mut chain_subtrees = sapling_subtrees(modified_chain, &db, first_chain_index..)?;
    assert_eq!(chain_subtrees.len(), 3, "should have 3 subtrees in chain");

    let (index, subtree) = chain_subtrees
        .pop_first()
        .expect("chain_subtrees should not be empty");
    assert_eq!(first_chain_index, index, "subtree indexes should match");
    assert_eq!(
        end_height, subtree.end_height,
        "subtree end heights should match"
    );

    // Check that Zebra retrieves subtrees correctly when using a range with an Excluded start bound

    let start = 0.into();
    let range = (Excluded(start), Unbounded);
    let subtrees = sapling_subtrees(Some(chain), &db, range)?;
    assert_eq!(subtrees.len(), 11);
    assert!(
        !subtrees.contains_key(&start),
        "should not contain excluded start bound"
    );

    Ok(())
}

/// Tests if Zebra combines the Sapling note commitment subtrees from the finalized and
/// non-finalized states correctly.
#[tokio::test]
async fn test_sapling_subtrees() -> Result<()> {
    let dummy_subtree_root = sapling_crypto::Node::from_bytes([0; 32]).unwrap();

    // Prepare the finalized state.
    let db_subtree = NoteCommitmentSubtree::new(0, Height(1), dummy_subtree_root);

    let db = new_ephemeral_db();
    let mut db_batch = DiskWriteBatch::new();
    db_batch.insert_sapling_subtree(&db, &db_subtree);
    db.write(db_batch)
        .expect("Writing a batch with a Sapling subtree should succeed.");

    // Prepare the non-finalized state.
    let chain_subtree = NoteCommitmentSubtree::new(1, Height(3), dummy_subtree_root);
    let mut chain = Chain::default();
    chain.insert_sapling_subtree(chain_subtree);
    let chain = Some(Arc::new(chain));

    // At this point, we have one Sapling subtree in the finalized state and one Sapling subtree in
    // the non-finalized state.

    // Retrieve only the first subtree and check its properties.
    let subtrees = sapling_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(0)..1.into())?;
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 1);
    assert!(subtrees_eq(subtrees.next().unwrap(), &db_subtree));

    // Retrieve both subtrees using a limit and check their properties.
    let subtrees = sapling_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(0)..2.into())?;
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 2);
    assert!(subtrees_eq(subtrees.next().unwrap(), &db_subtree));
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    // Retrieve both subtrees without using a limit and check their properties.
    let subtrees = sapling_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(0)..)?;
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 2);
    assert!(subtrees_eq(subtrees.next().unwrap(), &db_subtree));
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    // Retrieve only the second subtree and check its properties.
    let subtrees = sapling_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(1)..2.into())?;
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 1);
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    // Retrieve only the second subtree, using a limit that would allow for more trees if they were
    // present, and check its properties.
    let subtrees = sapling_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(1)..3.into())?;
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 1);
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    // Retrieve only the second subtree, without using any limit, and check its properties.
    let subtrees = sapling_subtrees(chain, &db, NoteCommitmentSubtreeIndex(1)..)?;
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 1);
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    Ok(())
}

/// Tests if Zebra combines the Orchard note commitment subtrees from the finalized and
/// non-finalized states correctly.
#[tokio::test]
async fn test_orchard_subtrees() -> Result<()> {
    let dummy_subtree_root = orchard::tree::Node::default();

    // Prepare the finalized state.
    let db_subtree = NoteCommitmentSubtree::new(0, Height(1), dummy_subtree_root);

    let db = new_ephemeral_db();
    let mut db_batch = DiskWriteBatch::new();
    db_batch.insert_orchard_subtree(&db, &db_subtree);
    db.write(db_batch)
        .expect("Writing a batch with an Orchard subtree should succeed.");

    // Prepare the non-finalized state.
    let chain_subtree = NoteCommitmentSubtree::new(1, Height(3), dummy_subtree_root);
    let mut chain = Chain::default();
    chain.insert_orchard_subtree(chain_subtree);
    let chain = Some(Arc::new(chain));

    // At this point, we have one Orchard subtree in the finalized state and one Orchard subtree in
    // the non-finalized state.

    // Retrieve only the first subtree and check its properties.
    let subtrees = orchard_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(0)..1.into())?;
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 1);
    assert!(subtrees_eq(subtrees.next().unwrap(), &db_subtree));

    // Retrieve both subtrees using a limit and check their properties.
    let subtrees = orchard_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(0)..2.into())?;
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 2);
    assert!(subtrees_eq(subtrees.next().unwrap(), &db_subtree));
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    // Retrieve both subtrees without using a limit and check their properties.
    let subtrees = orchard_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(0)..)?;
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 2);
    assert!(subtrees_eq(subtrees.next().unwrap(), &db_subtree));
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    // Retrieve only the second subtree and check its properties.
    let subtrees = orchard_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(1)..2.into())?;
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 1);
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    // Retrieve only the second subtree, using a limit that would allow for more trees if they were
    // present, and check its properties.
    let subtrees = orchard_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(1)..3.into())?;
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 1);
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    // Retrieve only the second subtree, without using any limit, and check its properties.
    let subtrees = orchard_subtrees(chain, &db, NoteCommitmentSubtreeIndex(1)..)?;
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 1);
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    Ok(())
}

/// Tests if Zebra combines the Ironwood note commitment subtrees from the finalized and
/// non-finalized states correctly.
#[tokio::test]
async fn test_ironwood_subtrees() -> Result<()> {
    let dummy_subtree_root = ironwood::tree::Node::default();

    // Prepare the finalized state.
    let db_subtree = NoteCommitmentSubtree::new(0, Height(1), dummy_subtree_root);

    let db = new_ephemeral_db();
    let mut db_batch = DiskWriteBatch::new();
    db_batch.insert_ironwood_subtree(&db, &db_subtree);
    db.write(db_batch)
        .expect("Writing a batch with an Ironwood subtree should succeed.");

    // Prepare the non-finalized state.
    let chain_subtree = NoteCommitmentSubtree::new(1, Height(3), dummy_subtree_root);
    let mut chain = Chain::default();
    chain.insert_ironwood_subtree(chain_subtree);
    let chain = Some(Arc::new(chain));

    // At this point, we have one Ironwood subtree in the finalized state and one Ironwood subtree in
    // the non-finalized state.

    // Retrieve only the first subtree and check its properties.
    let subtrees = ironwood_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(0)..1.into())?;
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 1);
    assert!(subtrees_eq(subtrees.next().unwrap(), &db_subtree));

    // Retrieve both subtrees using a limit and check their properties.
    let subtrees = ironwood_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(0)..2.into())?;
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 2);
    assert!(subtrees_eq(subtrees.next().unwrap(), &db_subtree));
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    // Retrieve both subtrees without using a limit and check their properties.
    let subtrees = ironwood_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(0)..)?;
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 2);
    assert!(subtrees_eq(subtrees.next().unwrap(), &db_subtree));
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    // Retrieve only the second subtree and check its properties.
    let subtrees = ironwood_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(1)..2.into())?;
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 1);
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    // Retrieve only the second subtree, using a limit that would allow for more trees if they were
    // present, and check its properties.
    let subtrees = ironwood_subtrees(chain.clone(), &db, NoteCommitmentSubtreeIndex(1)..3.into())?;
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 1);
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    // Retrieve only the second subtree, without using any limit, and check its properties.
    let subtrees = ironwood_subtrees(chain, &db, NoteCommitmentSubtreeIndex(1)..)?;
    let mut subtrees = subtrees.iter();
    assert_eq!(subtrees.len(), 1);
    assert!(subtrees_eq(subtrees.next().unwrap(), &chain_subtree));

    Ok(())
}

#[test]
fn excluded_max_subtree_range_is_empty() {
    use std::ops::Bound::*;

    let db = new_ephemeral_db();
    let no_chain = Option::<Arc<Chain>>::None;
    let range = (Excluded(NoteCommitmentSubtreeIndex(u16::MAX)), Unbounded);

    assert!(sapling_subtrees(no_chain, &db, range)
        .expect("an empty range is available")
        .is_empty());
}

/// Returns test cases for the empty state and missing blocks.
fn empty_state_test_cases() -> Vec<(ReadRequest, Result<ReadResponse, ExpectedTranscriptError>)> {
    let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_419200_BYTES
        .zcash_deserialize_into()
        .unwrap();

    vec![
        (
            ReadRequest::Transaction(transaction::Hash([0; 32])),
            Ok(ReadResponse::Transaction(None)),
        ),
        (
            ReadRequest::Block(block.hash().into()),
            Ok(ReadResponse::Block(None)),
        ),
        (
            ReadRequest::Block(block.coinbase_height().unwrap().into()),
            Ok(ReadResponse::Block(None)),
        ),
    ]
}

/// Returns `true` if `index` and `subtree_data` match the contents of `subtree`. Otherwise, returns
/// `false`.
fn subtrees_eq<N>(
    (index, subtree_data): (&NoteCommitmentSubtreeIndex, &NoteCommitmentSubtreeData<N>),
    subtree: &NoteCommitmentSubtree<N>,
) -> bool
where
    N: PartialEq + Copy,
{
    index == &subtree.index && subtree_data == &subtree.into_data()
}

/// Returns a new ephemeral database with no consistency checks.
fn new_ephemeral_db() -> ZakuraDb {
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
    .expect("opening an ephemeral database should succeed")
}

/// Test that AnyChainBlock can find blocks by hash and height.
#[tokio::test(flavor = "multi_thread")]
async fn any_chain_block_test() -> Result<()> {
    let _init_guard = zakura_test::init();

    // Create a continuous chain of mainnet blocks from genesis
    let blocks: Vec<Arc<Block>> = zakura_test::vectors::CONTINUOUS_MAINNET_BLOCKS
        .values()
        .map(|block_bytes| block_bytes.zcash_deserialize_into().unwrap())
        .collect();

    let (_state, read_state, _latest_chain_tip, _chain_tip_change) =
        populated_state(blocks.clone(), &Mainnet).await;

    // Test: AnyChainBlock should find blocks by hash (same as Block)
    for block in &blocks {
        let request = ReadRequest::AnyChainBlock(block.hash().into());
        let response = read_state
            .clone()
            .oneshot(request)
            .await
            .expect("request should succeed");
        assert!(
            matches!(
                response,
                ReadResponse::Block(Some(found_block)) if found_block.hash() == block.hash()
            ),
            "AnyChainBlock should find block by hash"
        );
    }

    // Test: AnyChainBlock should find blocks by height (same as Block)
    for block in &blocks {
        let height = block.coinbase_height().unwrap();
        let request = ReadRequest::AnyChainBlock(height.into());
        let response = read_state
            .clone()
            .oneshot(request)
            .await
            .expect("request should succeed");
        assert!(
            matches!(
                response,
                ReadResponse::Block(Some(found_block)) if found_block.hash() == block.hash()
            ),
            "AnyChainBlock should find block by height"
        );
    }

    // Test: Non-existent block should return None
    let fake_hash = zakura_chain::block::Hash([0xff; 32]);
    let request = ReadRequest::AnyChainBlock(fake_hash.into());
    let response = read_state
        .clone()
        .oneshot(request)
        .await
        .expect("request should succeed");
    assert!(
        matches!(response, ReadResponse::Block(None)),
        "AnyChainBlock should return None for non-existent block"
    );

    Ok(())
}

/// Test that AnyChainBlock finds blocks in side chains, while Block does not.
#[tokio::test(flavor = "multi_thread")]
async fn any_chain_block_finds_side_chain_blocks() -> Result<()> {
    use crate::{
        arbitrary::Prepare,
        service::{finalized_state::FinalizedState, non_finalized_state::NonFinalizedState},
        tests::FakeChainHelper,
    };
    use zakura_chain::{amount::NonNegative, value_balance::ValueBalance};

    let _init_guard = zakura_test::init();

    let network = Mainnet;

    // Use pre-Heartwood blocks to avoid history tree complications
    let genesis: Arc<Block> = Arc::new(network.test_block(653599, 583999).unwrap());

    // Create two different blocks from genesis
    // They have the same parent but different work, making them compete
    let best_chain_block = genesis.make_fake_child().set_work(100);
    let side_chain_block = genesis.make_fake_child().set_work(50);

    // Even though they have the same structure, changing work changes the header hash
    // because difficulty_threshold is part of the header
    let best_hash = best_chain_block.hash();
    let side_hash = side_chain_block.hash();

    // If hashes are the same, we can't test side chains properly
    // This would mean our fake block generation isn't working as expected
    if best_hash == side_hash {
        tracing::warn!("unable to create different block hashes, skipping side chain test");
        return Ok(());
    }

    // Create state with a finalized and non-finalized component
    let mut non_finalized_state = NonFinalizedState::new(&network);
    let finalized_state = FinalizedState::new(&Config::ephemeral(), &network)
        .expect("opening an ephemeral database should succeed");

    let fake_value_pool = ValueBalance::<NonNegative>::fake_populated_pool();
    finalized_state.set_finalized_value_pool(fake_value_pool);

    // Commit genesis as the first chain
    non_finalized_state.commit_new_chain(genesis.prepare(), &finalized_state)?;

    // Commit best chain block (higher work) - extends the genesis chain
    non_finalized_state.commit_block(best_chain_block.clone().prepare(), &finalized_state)?;

    // Commit side chain block (lower work) - also tries to extend genesis, creating a fork
    non_finalized_state.commit_block(side_chain_block.clone().prepare(), &finalized_state)?;

    // Verify we have 2 chains (genesis extended by best_chain_block, and genesis extended by side_chain_block)
    assert_eq!(
        non_finalized_state.chain_count(),
        2,
        "Should have 2 competing chains"
    );

    // Now test with the read interface
    // We'll use the low-level block lookup functions directly
    use crate::service::read::block::{any_block, block};

    // Test 1: any_block with all chains should find the side chain block by hash
    let found = any_block(
        non_finalized_state.chain_iter(),
        &finalized_state.db,
        side_hash.into(),
    );
    assert!(
        found.is_some(),
        "any_block should find side chain block by hash"
    );
    assert_eq!(found.unwrap().hash(), side_hash);

    // Test 2: block with only best chain should NOT find the side chain block by hash
    let found = block(
        non_finalized_state.best_chain(),
        &finalized_state.db,
        side_hash.into(),
    );
    assert!(
        found.is_none(),
        "block should NOT find side chain block by hash"
    );

    // Test 3: any_block should find the best chain block by hash
    let found = any_block(
        non_finalized_state.chain_iter(),
        &finalized_state.db,
        best_hash.into(),
    );
    assert!(
        found.is_some(),
        "any_block should find best chain block by hash"
    );
    assert_eq!(found.unwrap().hash(), best_hash);

    // Test 4: block should also find the best chain block by hash
    let found = block(
        non_finalized_state.best_chain(),
        &finalized_state.db,
        best_hash.into(),
    );
    assert!(
        found.is_some(),
        "block should find best chain block by hash"
    );
    assert_eq!(found.unwrap().hash(), best_hash);

    Ok(())
}

/// The absent-band subtree bound must cover exactly the subtrees the fast path skipped.
///
/// The bound is what keeps [`crate::HistoricalSubtreeUnavailable`] from firing on an ordinary
/// "you asked past the tip" query: only indices that completed at or below the last checkpoint were
/// skipped, everything at or above that is genuinely absent on any node.
#[test]
fn subtree_absent_band_bound_is_exact() {
    const LEAVES_PER_SUBTREE: u64 = 1 << TRACKED_SUBTREE_HEIGHT;

    // No subtree has completed yet, so no index is in the band, whatever the client asks for.
    for leaves in [0, 1, LEAVES_PER_SUBTREE - 1] {
        assert!(
            !subtree_completed_by_last_checkpoint(0.into(), leaves),
            "no subtree completes before {LEAVES_PER_SUBTREE} leaves, but {leaves} claimed one"
        );
    }

    // Exactly one subtree (index 0) completed. Index 1 has not, so it stays an empty list.
    assert!(subtree_completed_by_last_checkpoint(
        0.into(),
        LEAVES_PER_SUBTREE
    ));
    assert!(!subtree_completed_by_last_checkpoint(
        1.into(),
        LEAVES_PER_SUBTREE
    ));

    // A partly-filled second subtree does not count as completed.
    assert!(subtree_completed_by_last_checkpoint(
        0.into(),
        LEAVES_PER_SUBTREE + 1
    ));
    assert!(!subtree_completed_by_last_checkpoint(
        1.into(),
        LEAVES_PER_SUBTREE + 1
    ));

    // Mainnet-scale: 73,934,658 Sapling commitments at the last checkpoint is 1,128 completed
    // subtrees, indexes 0..=1127. Index 1128 is the one still filling.
    let sapling_leaves_at_last_checkpoint = 73_934_658;
    assert!(subtree_completed_by_last_checkpoint(
        1127.into(),
        sapling_leaves_at_last_checkpoint
    ));
    assert!(!subtree_completed_by_last_checkpoint(
        1128.into(),
        sapling_leaves_at_last_checkpoint
    ));
}

/// A persisted last-checkpoint marker does not mean the tree should exist before sync reaches it.
#[test]
fn last_checkpoint_tree_is_only_expected_after_sync_reaches_last_checkpoint() {
    let last_checkpoint = Height(10);

    assert!(is_syncing_below_last_checkpoint(
        Some(Height(9)),
        last_checkpoint
    ));
    assert!(!is_syncing_below_last_checkpoint(
        Some(last_checkpoint),
        last_checkpoint
    ));
    assert!(!is_syncing_below_last_checkpoint(
        Some(Height(11)),
        last_checkpoint
    ));

    // A marker without any finalized block cannot be produced by the atomic commit path, so keep
    // treating that state as an invariant failure rather than ordinary sync progress.
    assert!(!is_syncing_below_last_checkpoint(None, last_checkpoint));
}

/// A missing subtree's availability is undecided while the finalized tip is below the last checkpoint.
///
/// The node cannot tell a skipped subtree from one the chain has not reached until it has the
/// pool's leaf count at the last checkpoint, so the error must not advise a retry: every subtree
/// that completed below the last checkpoint is skipped, and this node never records it.
#[tokio::test]
async fn missing_subtree_before_last_checkpoint_reports_indeterminate_reason() {
    let _init_guard = zakura_test::init();
    let blocks: Vec<Arc<Block>> = zakura_test::vectors::CONTINUOUS_MAINNET_BLOCKS
        .values()
        .take(2)
        .map(|block_bytes| block_bytes.zcash_deserialize_into().unwrap())
        .collect();
    let (_state, read_state, _latest_chain_tip, _chain_tip_change) =
        populated_state(blocks, &Mainnet).await;
    let last_checkpoint = Height(10);

    let mut batch = DiskWriteBatch::new();
    batch.update_vct_sync_marker(&read_state.db, last_checkpoint);
    read_state
        .db
        .write_batch(batch)
        .expect("seeding a future last checkpoint succeeds");

    let error = sapling_subtrees(
        None::<Arc<Chain>>,
        &read_state.db,
        NoteCommitmentSubtreeIndex(0)..1.into(),
    )
    .expect_err("a subtree below an unreached last checkpoint must fail closed");

    assert_eq!(
        error.reason,
        HistoricalSubtreeUnavailableReason::Indeterminate
    );
    assert!(error
        .to_string()
        .contains("cannot yet tell whether the subtree was skipped"));
    assert!(
        !error.to_string().contains("retry"),
        "a subtree skipped below the last checkpoint never arrives, so the error must not advise a \
         retry, got: {error}"
    );
}

/// A subtree that the authenticated last-checkpoint frontier proves was not completed is an
/// ordinary empty result, even while the finalized tip is below the last checkpoint.
///
/// Ask for the first incomplete Ironwood index from the embedded frontier leaf count so the
/// regression stays valid after release-state imports complete earlier Ironwood subtrees.
#[tokio::test]
async fn incomplete_ironwood_subtree_before_mainnet_last_checkpoint_is_empty() {
    let _init_guard = zakura_test::init();
    let blocks: Vec<Arc<Block>> = zakura_test::vectors::CONTINUOUS_MAINNET_BLOCKS
        .values()
        .take(2)
        .map(|block_bytes| block_bytes.zcash_deserialize_into().unwrap())
        .collect();
    let (_state, read_state, _latest_chain_tip, _chain_tip_change) =
        populated_state(blocks, &Mainnet).await;
    let last_checkpoint = Mainnet.checkpoint_list().max_height();
    let (_, _, ironwood_leaves) = embedded_last_checkpoint_leaf_counts(&Mainnet, last_checkpoint)
        .expect("Mainnet embeds a last-checkpoint frontier matching the checkpoint list");
    let first_incomplete = NoteCommitmentSubtreeIndex(
        u16::try_from(ironwood_leaves >> TRACKED_SUBTREE_HEIGHT)
            .expect("completed Ironwood subtree count at the last checkpoint fits in u16"),
    );

    assert!(
        read_state.db.finalized_tip_height() < Some(last_checkpoint),
        "the regression requires a finalized tip below the Mainnet last checkpoint"
    );
    assert!(
        !subtree_completed_by_last_checkpoint(first_incomplete, ironwood_leaves),
        "the chosen Ironwood index must still be incomplete at the Mainnet last checkpoint"
    );

    let mut batch = DiskWriteBatch::new();
    batch.update_vct_sync_marker(&read_state.db, last_checkpoint);
    read_state
        .db
        .write_batch(batch)
        .expect("seeding the Mainnet VCT last checkpoint succeeds");

    let end = NoteCommitmentSubtreeIndex(
        first_incomplete
            .0
            .checked_add(1)
            .expect("first incomplete Ironwood index is below u16::MAX"),
    );
    let subtrees = ironwood_subtrees(None::<Arc<Chain>>, &read_state.db, first_incomplete..end)
        .expect(
            "the authenticated last checkpoint proves the first incomplete Ironwood subtree was \
             not completed",
        );

    assert!(subtrees.is_empty());
}

/// A missing last-checkpoint tree is an error once the finalized tip has reached the checkpoint.
#[tokio::test]
async fn missing_last_checkpoint_tree_fails_closed_after_sync_reaches_last_checkpoint() {
    let _init_guard = zakura_test::init();
    let blocks: Vec<Arc<Block>> = zakura_test::vectors::CONTINUOUS_MAINNET_BLOCKS
        .values()
        .take(2)
        .map(|block_bytes| block_bytes.zcash_deserialize_into().unwrap())
        .collect();
    let (_state, read_state, _latest_chain_tip, _chain_tip_change) =
        populated_state(blocks, &Mainnet).await;
    let last_checkpoint = read_state
        .db
        .finalized_tip_height()
        .expect("the populated state has a finalized tip");

    let mut batch = DiskWriteBatch::new();
    batch.update_vct_sync_marker(&read_state.db, last_checkpoint);
    batch.delete_range_sapling_tree(&read_state.db, &Height::MIN, &last_checkpoint);
    batch.delete_sapling_tree(&read_state.db, &last_checkpoint);
    read_state
        .db
        .write_batch(batch)
        .expect("seeding a missing last-checkpoint tree succeeds");

    let error = sapling_subtrees(
        None::<Arc<Chain>>,
        &read_state.db,
        NoteCommitmentSubtreeIndex(0)..1.into(),
    )
    .expect_err("a reached last checkpoint without its tree must fail closed");

    assert_eq!(error.pool, "sapling");
    assert_eq!(error.index, NoteCommitmentSubtreeIndex(0));
    assert_eq!(error.last_checkpoint, last_checkpoint);
    assert_eq!(error.reason, HistoricalSubtreeUnavailableReason::NotStored);
    assert!(error.to_string().contains("use another node"));
}

/// Missing pre-activation trees are returned as consensus-defined empty frontiers.
#[tokio::test]
async fn pre_activation_tree_requests_return_empty_frontiers() {
    let _init_guard = zakura_test::init();
    let blocks: Vec<Arc<Block>> = zakura_test::vectors::CONTINUOUS_MAINNET_BLOCKS
        .values()
        .take(2)
        .map(|block_bytes| block_bytes.zcash_deserialize_into().unwrap())
        .collect();
    let (_state, read_state, _latest_chain_tip, _chain_tip_change) =
        populated_state(blocks, &Mainnet).await;
    let requested_height = Height::MIN;
    let last_checkpoint = Height(10);

    let mut batch = DiskWriteBatch::new();
    batch.update_vct_sync_marker(&read_state.db, last_checkpoint);
    read_state
        .db
        .write_batch(batch)
        .expect("seeding the VCT absent band succeeds");

    assert_eq!(
        read_state
            .clone()
            .oneshot(ReadRequest::SaplingTree(requested_height.into()))
            .await
            .expect("pre-activation Sapling tree request succeeds"),
        ReadResponse::SaplingTree(Some(Default::default()))
    );
    assert_eq!(
        read_state
            .clone()
            .oneshot(ReadRequest::OrchardTree(requested_height.into()))
            .await
            .expect("pre-activation Orchard tree request succeeds"),
        ReadResponse::OrchardTree(Some(Default::default()))
    );
    assert_eq!(
        read_state
            .clone()
            .oneshot(ReadRequest::IronwoodTree(requested_height.into()))
            .await
            .expect("pre-activation Ironwood tree request succeeds"),
        ReadResponse::IronwoodTree(Some(Default::default()))
    );
    assert_eq!(
        read_state
            .oneshot(ReadRequest::SaplingTree(last_checkpoint.into()))
            .await
            .expect("missing pre-activation block request succeeds"),
        ReadResponse::SaplingTree(None),
        "an empty frontier is only returned for a block that exists"
    );
}

/// An artifact-backed subtree response must be checked through the end of its contiguous run.
#[tokio::test]
async fn artifact_subtree_gaps_return_typed_errors_for_every_pool() {
    let _init_guard = zakura_test::init();
    let blocks: Vec<Arc<Block>> = zakura_test::vectors::CONTINUOUS_MAINNET_BLOCKS
        .values()
        .take(2)
        .map(|block_bytes| block_bytes.zcash_deserialize_into().unwrap())
        .collect();
    let (_state, mut read_state, _latest_chain_tip, _chain_tip_change) =
        populated_state(blocks, &Mainnet).await;
    let last_checkpoint = Height(10);

    let records = |root| {
        [0u16, 1, 3]
            .into_iter()
            .map(|index| SubtreeRecord {
                index: NoteCommitmentSubtreeIndex(index),
                // Availability checks the skip-band union, then serving drops records above the
                // verified tip. Keep every height eligible so the index gap, not the tip clip,
                // truncates the run.
                end_height: Height::MIN,
                root,
            })
            .collect()
    };
    read_state.historical_subtrees = Some(Arc::new(SubtreeArtifact {
        last_checkpoint,
        sapling: records([0; 32]),
        orchard: records([3; 32]),
        ironwood: records([3; 32]),
    }));

    let mut batch = DiskWriteBatch::new();
    batch.update_vct_sync_marker(&read_state.db, last_checkpoint);
    read_state
        .db
        .write_batch(batch)
        .expect("seeding a future last checkpoint succeeds");

    let requests = [
        (
            "sapling",
            ReadRequest::SaplingSubtrees {
                start_index: NoteCommitmentSubtreeIndex(0),
                limit: None,
            },
        ),
        (
            "orchard",
            ReadRequest::OrchardSubtrees {
                start_index: NoteCommitmentSubtreeIndex(0),
                limit: None,
            },
        ),
        (
            "ironwood",
            ReadRequest::IronwoodSubtrees {
                start_index: NoteCommitmentSubtreeIndex(0),
                limit: None,
            },
        ),
    ];

    for (pool, request) in requests {
        let error = read_state
            .clone()
            .oneshot(request)
            .await
            .expect_err("an artifact gap must not return the short prefix [0, 1]");
        let error = error
            .downcast_ref::<HistoricalSubtreeUnavailable>()
            .expect("subtree gaps return HistoricalSubtreeUnavailable");

        assert_eq!(error.pool, pool);
        assert_eq!(error.index, NoteCommitmentSubtreeIndex(2));
        assert_eq!(error.last_checkpoint, last_checkpoint);
        assert_eq!(
            error.reason,
            HistoricalSubtreeUnavailableReason::Indeterminate
        );
    }
}

/// A published subtree that completes above the verified tip is not a permanent hole.
///
/// The skip-band union still contains it, so availability succeeds; serving then drops it and
/// returns the prefix completed at this tip. Treating it as `NotStored` would tell a client that
/// continuing to sync cannot restore a subtree the artifact will serve once the tip reaches its
/// end height.
#[tokio::test]
async fn published_subtrees_above_verified_tip_return_the_completed_prefix() {
    let _init_guard = zakura_test::init();
    let blocks: Vec<Arc<Block>> = zakura_test::vectors::CONTINUOUS_MAINNET_BLOCKS
        .values()
        .take(2)
        .map(|block_bytes| block_bytes.zcash_deserialize_into().unwrap())
        .collect();
    let (_state, mut read_state, _latest_chain_tip, _chain_tip_change) =
        populated_state(blocks, &Mainnet).await;
    let last_checkpoint = Mainnet.checkpoint_list().max_height();
    let verified_tip = read_state
        .db
        .finalized_tip_height()
        .expect("the populated state has a finalized tip");

    assert!(
        verified_tip < last_checkpoint,
        "the regression requires a finalized tip below the Mainnet last checkpoint"
    );

    let above_tip = Height(
        verified_tip
            .0
            .checked_add(1)
            .expect("the populated tip is far below Height::MAX"),
    );

    read_state.historical_subtrees = Some(Arc::new(SubtreeArtifact {
        last_checkpoint,
        sapling: vec![
            SubtreeRecord {
                index: NoteCommitmentSubtreeIndex(0),
                end_height: verified_tip,
                root: [0; 32],
            },
            SubtreeRecord {
                index: NoteCommitmentSubtreeIndex(1),
                end_height: above_tip,
                root: [0; 32],
            },
        ],
        orchard: Vec::new(),
        ironwood: Vec::new(),
    }));

    let mut batch = DiskWriteBatch::new();
    batch.update_vct_sync_marker(&read_state.db, last_checkpoint);
    read_state
        .db
        .write_batch(batch)
        .expect("seeding the Mainnet VCT last checkpoint succeeds");

    let response = read_state
        .clone()
        .oneshot(ReadRequest::SaplingSubtrees {
            start_index: NoteCommitmentSubtreeIndex(0),
            limit: Some(NoteCommitmentSubtreeIndex(2)),
        })
        .await
        .expect("a not-yet-reached published subtree must not fail as a permanent hole");

    let ReadResponse::SaplingSubtrees(subtrees) = response else {
        panic!("unexpected response to a sapling subtrees request: {response:?}");
    };
    assert_eq!(
        subtrees.keys().copied().collect::<Vec<_>>(),
        vec![NoteCommitmentSubtreeIndex(0)],
        "the served run is the prefix completed at this tip"
    );

    let response = read_state
        .oneshot(ReadRequest::SaplingSubtrees {
            start_index: NoteCommitmentSubtreeIndex(1),
            limit: Some(NoteCommitmentSubtreeIndex(1)),
        })
        .await
        .expect("asking for a published subtree above the tip is an empty list, not NotStored");

    let ReadResponse::SaplingSubtrees(subtrees) = response else {
        panic!("unexpected response to a sapling subtrees request: {response:?}");
    };
    assert!(
        subtrees.is_empty(),
        "a start index that completes above this tip is not yet available"
    );
}

/// The served run must be checked to its end, not just at its start.
///
/// `z_getsubtreesbyindex` returns one contiguous run, so a gap anywhere truncates the response.
/// A single unbounded request from index 0 against a truncated artifact would otherwise return a
/// short list with no error, which a client reads as "that is every subtree on this chain" — the
/// same silent-truncation failure the typed errors exist to remove.
#[test]
fn first_missing_subtree_index_finds_the_end_of_the_run() {
    let data = || {
        NoteCommitmentSubtreeData::new(
            Height(1),
            sapling_crypto::Node::from_bytes([0; 32]).unwrap(),
        )
    };
    let map = |indexes: &[u16]| {
        indexes
            .iter()
            .map(|i| (NoteCommitmentSubtreeIndex(*i), data()))
            .collect::<std::collections::BTreeMap<_, _>>()
    };

    // A run that stops early reports the index just past its end, not "nothing missing".
    assert_eq!(
        first_missing_subtree_index(&map(&[0, 1, 2]), NoteCommitmentSubtreeIndex(0), None),
        Some(NoteCommitmentSubtreeIndex(3))
    );

    // An upgraded database can contain a pre-U row and a post-checkpoint row around a subtree
    // skipped by VCT fast sync. The later row must not hide the internal gap.
    assert_eq!(
        first_missing_subtree_index(&map(&[0, 2]), NoteCommitmentSubtreeIndex(0), None),
        Some(NoteCommitmentSubtreeIndex(1)),
        "the first internal gap must be reported instead of one past the last key"
    );

    // Nothing served at all: the requested start is the first missing index.
    assert_eq!(
        first_missing_subtree_index(&map(&[]), NoteCommitmentSubtreeIndex(7), None),
        Some(NoteCommitmentSubtreeIndex(7))
    );

    // An index the client did not ask for is not missing from its answer.
    assert_eq!(
        first_missing_subtree_index(
            &map(&[0, 1, 2]),
            NoteCommitmentSubtreeIndex(0),
            Some(NoteCommitmentSubtreeIndex(3))
        ),
        None,
        "a fully satisfied bounded request has no gap"
    );
    assert_eq!(
        first_missing_subtree_index(
            &map(&[0, 1]),
            NoteCommitmentSubtreeIndex(0),
            Some(NoteCommitmentSubtreeIndex(5))
        ),
        Some(NoteCommitmentSubtreeIndex(2)),
        "a bounded request served short still reports the gap"
    );

    // `u16::MAX` is the last index that can exist, so a run reaching it has no successor.
    assert_eq!(
        first_missing_subtree_index(
            &map(&[u16::MAX]),
            NoteCommitmentSubtreeIndex(u16::MAX),
            None
        ),
        None,
        "the final index must not overflow into a phantom gap"
    );
}

/// Merging the node's own subtree rows with published ones must still serve a continuous list.
///
/// The gated read drops everything when it has no row at the requested start, so a node holding
/// rows only *above* the last checkpoint contributes nothing until the published records below it are
/// merged in. A client doing spend-before-sync asks from index 0 and needs one list spanning both
/// halves; serving only the published half would silently truncate its witness data.
#[test]
fn contiguous_subtrees_spans_published_and_stored_rows() {
    let data = |height: u32| {
        NoteCommitmentSubtreeData::new(
            Height(height),
            sapling_crypto::Node::from_bytes([0; 32]).unwrap(),
        )
    };

    let merged: std::collections::BTreeMap<_, _> = [0u16, 1, 2, 3]
        .into_iter()
        .map(|index| (NoteCommitmentSubtreeIndex(index), data(index as u32 + 1)))
        .collect();

    let served = contiguous_subtrees_from(merged.clone(), NoteCommitmentSubtreeIndex(0));
    assert_eq!(served.len(), 4, "a gapless union is served whole");

    // A gap makes everything past it unusable, so it is dropped rather than served.
    let mut holed = merged.clone();
    holed.remove(&NoteCommitmentSubtreeIndex(2));
    let served = contiguous_subtrees_from(holed, NoteCommitmentSubtreeIndex(0));
    assert_eq!(
        served.keys().copied().collect::<Vec<_>>(),
        vec![NoteCommitmentSubtreeIndex(0), NoteCommitmentSubtreeIndex(1)],
        "the run stops at the first gap"
    );

    // A missing start index means there is nothing to serve, not a list starting later.
    let mut no_start = merged.clone();
    no_start.remove(&NoteCommitmentSubtreeIndex(0));
    assert!(
        contiguous_subtrees_from(no_start, NoteCommitmentSubtreeIndex(0)).is_empty(),
        "a missing start index serves nothing"
    );

    // Indexes below the request are not served.
    let served = contiguous_subtrees_from(merged, NoteCommitmentSubtreeIndex(2));
    assert_eq!(
        served.keys().copied().collect::<Vec<_>>(),
        vec![NoteCommitmentSubtreeIndex(2), NoteCommitmentSubtreeIndex(3)],
        "the run starts at the requested index"
    );
}

/// Artifact fallback merging must retain verified rows from the non-finalized best chain.
#[test]
fn published_subtree_merge_includes_non_finalized_rows() {
    let node = |root: u8| {
        NoteCommitmentSubtreeData::new(
            Height(11),
            sapling_crypto::Node::from_bytes([root; 32]).unwrap(),
        )
    };

    let mut chain = Chain::default();
    chain.insert_sapling_subtree(NoteCommitmentSubtree::new(1, Height(11), node(2).root));

    let db = new_ephemeral_db();
    let mut merged = sapling_subtrees_with_gaps(
        Some(Arc::new(chain)),
        &db,
        NoteCommitmentSubtreeIndex(0)..NoteCommitmentSubtreeIndex(2),
    );

    merge_published_subtrees(
        &mut merged,
        [
            (NoteCommitmentSubtreeIndex(0), node(1)),
            (NoteCommitmentSubtreeIndex(1), node(3)),
        ],
        Height(11),
    );
    let served = contiguous_subtrees_from(merged, NoteCommitmentSubtreeIndex(0));

    assert_eq!(served.len(), 2);
    assert_eq!(
        served[&NoteCommitmentSubtreeIndex(1)].root,
        node(2).root,
        "the verified best-chain row must win over the artifact"
    );
}

/// A published subtree record must never displace the node's own row.
///
/// The node computed and verified its rows; a published record is trusted only after a digest the
/// artifact carries itself, which is not a signature. A correct artifact never overlaps, so an
/// overlap is exactly the corrupt-or-hostile case where precedence decides whether a wrong root
/// reaches a client and builds a wrong witness.
#[test]
fn published_subtrees_never_displace_the_nodes_own_rows() {
    let node = |root: u8| {
        NoteCommitmentSubtreeData::new(
            Height(11),
            sapling_crypto::Node::from_bytes([root; 32]).unwrap(),
        )
    };

    let mut stored = std::collections::BTreeMap::new();
    stored.insert(NoteCommitmentSubtreeIndex(0), node(1));
    stored.insert(NoteCommitmentSubtreeIndex(1), node(2));

    merge_published_subtrees(
        &mut stored,
        [
            // Collides with a row the node holds.
            (NoteCommitmentSubtreeIndex(1), node(4)),
            // Fills a genuine gap, which is what the artifact is for.
            (NoteCommitmentSubtreeIndex(2), node(3)),
        ],
        Height(11),
    );

    assert_eq!(
        stored[&NoteCommitmentSubtreeIndex(1)].root,
        node(2).root,
        "the node's own row wins on collision"
    );
    assert_eq!(
        stored[&NoteCommitmentSubtreeIndex(2)].root,
        node(3).root,
        "a published record still fills an index the node lacks"
    );
    assert_eq!(stored.len(), 3);
}

/// Serving drops published records completed above the node's verified tip.
///
/// Availability still sees those records in the skip-band union; this helper is what prevents
/// them from reaching a client.
#[test]
fn published_subtrees_are_bounded_by_the_verified_tip() {
    let node = |height: u32| {
        NoteCommitmentSubtreeData::new(
            Height(height),
            sapling_crypto::Node::from_bytes([0; 32]).unwrap(),
        )
    };
    let mut stored = std::collections::BTreeMap::new();

    merge_published_subtrees(
        &mut stored,
        [
            (NoteCommitmentSubtreeIndex(0), node(9)),
            (NoteCommitmentSubtreeIndex(1), node(10)),
            (NoteCommitmentSubtreeIndex(2), node(11)),
        ],
        Height(100),
    );
    assert_eq!(
        stored.len(),
        3,
        "the skip-band union keeps records above the verified tip"
    );

    retain_subtrees_completed_at_or_below(&mut stored, Height(10));

    assert_eq!(
        stored.keys().copied().collect::<Vec<_>>(),
        vec![NoteCommitmentSubtreeIndex(0), NoteCommitmentSubtreeIndex(1)],
        "records at the verified tip are eligible, but records above it are not"
    );
}

/// A newer artifact may fill history skipped at an older fast-sync marker, but not heights the
/// node synced itself.
#[test]
fn newer_artifact_fills_only_the_skipped_band() {
    let node = |height: u32, root: u8| {
        NoteCommitmentSubtreeData::new(
            Height(height),
            sapling_crypto::Node::from_bytes([root; 32]).unwrap(),
        )
    };
    let mut stored = std::collections::BTreeMap::new();
    stored.insert(NoteCommitmentSubtreeIndex(1), node(12, 2));

    merge_published_subtrees(
        &mut stored,
        [
            (NoteCommitmentSubtreeIndex(0), node(5, 1)),
            (NoteCommitmentSubtreeIndex(1), node(12, 9)),
            (NoteCommitmentSubtreeIndex(2), node(18, 3)),
        ],
        Height(10),
    );

    assert_eq!(
        stored[&NoteCommitmentSubtreeIndex(0)].root,
        node(5, 1).root,
        "the H1 skip band comes from the newer H2 artifact"
    );
    assert_eq!(
        stored[&NoteCommitmentSubtreeIndex(1)].root,
        node(12, 2).root,
        "a local row after H1 wins over the H2 artifact"
    );
    assert!(
        !stored.contains_key(&NoteCommitmentSubtreeIndex(2)),
        "a post-H1 hole is not filled from the H2 artifact"
    );
    assert_eq!(stored.len(), 2);
}

/// The embedded artifact may fill this database's skip band when it is at least as new as the
/// durable fast-sync marker.
#[tokio::test]
async fn historical_subtrees_accept_a_newer_artifact_for_an_older_fast_sync_marker() {
    let _init_guard = zakura_test::init();
    let (_state, mut read_state, _latest_chain_tip, _chain_tip_change) =
        init_test_services(&Network::new_default_testnet()).await;
    let artifact_checkpoint = Height(10);

    read_state.historical_subtrees = Some(Arc::new(SubtreeArtifact {
        last_checkpoint: artifact_checkpoint,
        ..SubtreeArtifact::default()
    }));

    assert!(
        read_state
            .historical_subtrees_at_last_checkpoint()
            .is_none(),
        "an ordinary database without a fast-sync marker must not use the artifact"
    );

    let mut batch = DiskWriteBatch::new();
    batch.update_vct_sync_marker(&read_state.db, Height(11));
    read_state
        .db
        .write_batch(batch)
        .expect("seeding a newer last checkpoint succeeds");
    assert!(
        read_state
            .historical_subtrees_at_last_checkpoint()
            .is_none(),
        "an older artifact cannot cover a newer skip band"
    );

    let mut batch = DiskWriteBatch::new();
    batch.update_vct_sync_marker(&read_state.db, artifact_checkpoint);
    read_state
        .db
        .write_batch(batch)
        .expect("seeding the matching last checkpoint succeeds");
    assert_eq!(
        read_state
            .historical_subtrees_at_last_checkpoint()
            .map(|(_, vct_applied_below)| vct_applied_below),
        Some(artifact_checkpoint),
        "the artifact is eligible once its checkpoint matches the durable last checkpoint"
    );

    let mut batch = DiskWriteBatch::new();
    batch.update_vct_sync_marker(&read_state.db, Height(9));
    read_state
        .db
        .write_batch(batch)
        .expect("seeding an older last checkpoint succeeds");
    assert_eq!(
        read_state
            .historical_subtrees_at_last_checkpoint()
            .map(|(_, vct_applied_below)| vct_applied_below),
        Some(Height(9)),
        "a newer artifact may fill the skip band of an older fast-sync marker"
    );
}

/// An H1-fast-synced database may use a newer H2 artifact only for history skipped at H1.
#[tokio::test]
async fn older_fast_sync_marker_uses_newer_artifact_only_for_skipped_history() {
    let _init_guard = zakura_test::init();
    let blocks: Vec<Arc<Block>> = zakura_test::vectors::CONTINUOUS_TESTNET_BLOCKS
        .values()
        .take(5)
        .map(|block_bytes| block_bytes.zcash_deserialize_into().unwrap())
        .collect();
    let (_state, mut read_state, _latest_chain_tip, _chain_tip_change) =
        populated_state(blocks, &Network::new_default_testnet()).await;

    let vct_applied_below = Height(2);
    let artifact_checkpoint = Height(10);
    let skipped_root = sapling_crypto::Node::from_bytes([1; 32]).unwrap();
    let local_root = sapling_crypto::Node::from_bytes([2; 32]).unwrap();
    let post_handoff_root = sapling_crypto::Node::from_bytes([3; 32]).unwrap();

    read_state.historical_subtrees = Some(Arc::new(SubtreeArtifact {
        last_checkpoint: artifact_checkpoint,
        sapling: vec![
            SubtreeRecord {
                index: NoteCommitmentSubtreeIndex(0),
                end_height: Height(1),
                root: skipped_root.to_bytes(),
            },
            SubtreeRecord {
                index: NoteCommitmentSubtreeIndex(1),
                end_height: Height(3),
                root: post_handoff_root.to_bytes(),
            },
            SubtreeRecord {
                index: NoteCommitmentSubtreeIndex(2),
                end_height: Height(3),
                root: post_handoff_root.to_bytes(),
            },
        ],
        ..SubtreeArtifact::default()
    }));

    let mut batch = DiskWriteBatch::new();
    batch.update_vct_sync_marker(&read_state.db, vct_applied_below);
    batch.insert_sapling_subtree(
        &read_state.db,
        &NoteCommitmentSubtree::new(1, Height(3), local_root),
    );
    read_state
        .db
        .write_batch(batch)
        .expect("seeding an older marker and a post-handoff row succeeds");

    let ReadResponse::SaplingSubtrees(served) = read_state
        .clone()
        .oneshot(ReadRequest::SaplingSubtrees {
            start_index: NoteCommitmentSubtreeIndex(0),
            limit: Some(NoteCommitmentSubtreeIndex(2)),
        })
        .await
        .expect("the H1 skip band plus the local post-H1 row are servable")
    else {
        panic!("SaplingSubtrees must return SaplingSubtrees");
    };

    assert_eq!(
        served[&NoteCommitmentSubtreeIndex(0)].root,
        skipped_root,
        "the H2 artifact fills history skipped at H1"
    );
    assert_eq!(
        served[&NoteCommitmentSubtreeIndex(1)].root,
        local_root,
        "the node's own row after H1 wins over the H2 artifact"
    );
    assert_eq!(served.len(), 2);

    let post_handoff = read_state
        .oneshot(ReadRequest::SaplingSubtrees {
            start_index: NoteCommitmentSubtreeIndex(2),
            limit: Some(NoteCommitmentSubtreeIndex(1)),
        })
        .await;
    match post_handoff {
        Ok(ReadResponse::SaplingSubtrees(subtrees)) => {
            assert!(
                subtrees.is_empty(),
                "a post-H1 hole must not be filled from the H2 artifact"
            );
        }
        Err(_) => {}
        Ok(other) => panic!("unexpected response for a post-H1 hole: {other:?}"),
    }
}
