//! Fixed test vectors for the syncer.

#![allow(clippy::unwrap_in_result)]

use std::{
    collections::{HashMap, HashSet},
    iter,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use color_eyre::Report;
use futures::{Future, FutureExt, StreamExt};
use tower::timeout::Timeout;

use zakura_chain::{
    block::{self, Block, Height},
    chain_tip::mock::{MockChainTip, MockChainTipSender},
    serialization::ZcashDeserializeInto,
};
use zakura_consensus::{
    Config as ConsensusConfig, RouterError, VerifyBlockError, VerifyCheckpointError,
};
use zakura_network::{InventoryResponse, PeerSocketAddr};
use zakura_state::Config as StateConfig;
use zakura_test::mock_service::{MockService, PanicAssertion};

use zakura_network as zn;
use zakura_state as zs;

use crate::{
    components::{
        sync::{
            self,
            downloads::{BlockDownloadVerifyError, Downloads},
            legacy_trace::LegacySyncTrace,
            SyncStatus,
        },
        ChainSync,
    },
    config::ZakuradConfig,
};

use InventoryResponse::*;

type TestChainSync = ChainSync<
    MockService<zn::Request, zn::Response, PanicAssertion>,
    MockService<zs::Request, zs::Response, PanicAssertion>,
    MockService<zakura_consensus::Request, block::Hash, PanicAssertion>,
    MockChainTip,
>;

/// Maximum time to wait for a request to any test service.
///
/// The default [`MockService`] value can be too short for some of these tests that take a little
/// longer than expected to actually send the request.
///
/// Increasing this value causes the tests to take longer to complete, so it can't be too large.
const MAX_SERVICE_REQUEST_DELAY: Duration = Duration::from_millis(1000);

/// Maximum time to wait for a request in tests that must outlast [`sync::BLOCK_VERIFY_TIMEOUT`].
///
/// A stalled syncer only recovers by waiting out that timeout and restarting. A mock service that
/// gives up first would panic before the syncer ever gets there, hiding the stall behind an
/// unrelated failure. These tests pause time, so a long delay costs no wall-clock time.
const STALLED_SERVICE_REQUEST_DELAY: Duration = Duration::from_secs(30 * 60);

#[test]
fn oversized_find_blocks_response_is_rejected() {
    let hash = block::Hash([0; 32]);

    // The guard is applied after ObtainTips/ExtendTips strip zcashd's extra
    // trailing hash, so 500 remaining hashes are accepted and 501 are not.
    assert!(sync::has_valid_tips_response_hash_count(&vec![
        hash;
        sync::MAX_TIPS_RESPONSE_HASH_COUNT
    ]));
    assert!(!sync::has_valid_tips_response_hash_count(&vec![
        hash;
        sync::MAX_TIPS_RESPONSE_HASH_COUNT
            + 1
    ]));

    // A zcashd response with 500 chain hashes plus one appended hash must remain
    // usable after the trailing hash is discarded.
    let raw = vec![hash; sync::MAX_TIPS_RESPONSE_HASH_COUNT + 1];
    let stripped = &raw[..raw.len() - 1];
    assert_eq!(stripped.len(), sync::MAX_TIPS_RESPONSE_HASH_COUNT);
    assert!(sync::has_valid_tips_response_hash_count(stripped));
}

/// Test that the syncer downloads genesis, blocks 1-2 using obtain_tips, and blocks 3-4 using extend_tips.
///
/// This test also makes sure that the syncer downloads blocks in order.
#[tokio::test]
async fn sync_blocks_ok() -> Result<(), crate::BoxError> {
    // Get services
    let (
        chain_sync_future,
        _sync_status,
        mut block_verifier_router,
        mut peer_set,
        mut state_service,
        _mock_chain_tip_sender,
    ) = setup();

    // Get blocks
    let block0: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES.zcash_deserialize_into()?;
    let block0_hash = block0.hash();

    let block1: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_1_BYTES.zcash_deserialize_into()?;
    let block1_hash = block1.hash();

    let block2: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_2_BYTES.zcash_deserialize_into()?;
    let block2_hash = block2.hash();

    let block3: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_3_BYTES.zcash_deserialize_into()?;
    let block3_hash = block3.hash();

    let block4: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_4_BYTES.zcash_deserialize_into()?;
    let block4_hash = block4.hash();

    let block5: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_5_BYTES.zcash_deserialize_into()?;
    let block5_hash = block5.hash();

    // Start the syncer
    let chain_sync_task_handle = tokio::spawn(chain_sync_future);

    // ChainSync::request_genesis

    // State is checked for genesis
    state_service
        .expect_request(zs::Request::KnownBlock(block0_hash))
        .await
        .respond(zs::Response::KnownBlock(None));

    // Block 0 is fetched and committed to the state
    peer_set
        .expect_request(zn::Request::BlocksByHash(iter::once(block0_hash).collect()))
        .await
        .respond(zn::Response::Blocks(vec![Available((
            block0.clone(),
            None,
        ))]));

    block_verifier_router
        .expect_request(zakura_consensus::Request::Commit(block0))
        .await
        .respond(block0_hash);

    // Check that nothing unexpected happened.
    // We expect more requests to the state service, because the syncer keeps on running.
    peer_set.expect_no_requests().await;
    block_verifier_router.expect_no_requests().await;

    // State is checked for genesis again
    state_service
        .expect_request(zs::Request::KnownBlock(block0_hash))
        .await
        .respond(zs::Response::KnownBlock(Some(zs::KnownBlock::BestChain)));

    // ChainSync::obtain_tips

    // State is asked for a block locator.
    state_service
        .expect_request(zs::Request::BlockLocator)
        .await
        .respond(zs::Response::BlockLocator(vec![block0_hash]));

    // Network is sent the block locator
    peer_set
        .expect_request(zn::Request::FindBlocks {
            known_blocks: vec![block0_hash],
            stop: None,
        })
        .await
        .respond(zn::Response::BlockHashes(vec![
            block1_hash, // tip
            block2_hash, // expected_next
            block3_hash, // (discarded - last hash, possibly incorrect)
        ]));

    // State is checked for each candidate hash before it is queued.
    state_service
        .expect_request(zs::Request::KnownBlock(block1_hash))
        .await
        .respond(zs::Response::KnownBlock(None));
    state_service
        .expect_request(zs::Request::KnownBlock(block2_hash))
        .await
        .respond(zs::Response::KnownBlock(None));

    // Clear remaining block locator requests
    for _ in 0..(sync::FANOUT - 1) {
        peer_set
            .expect_request(zn::Request::FindBlocks {
                known_blocks: vec![block0_hash],
                stop: None,
            })
            .await
            .respond(Err(zn::BoxError::from("synthetic test obtain tips error")));
    }

    // Check that nothing unexpected happened.
    peer_set.expect_no_requests().await;
    block_verifier_router.expect_no_requests().await;

    // State is checked for all non-tip blocks (blocks 1 & 2) in response order
    state_service
        .expect_request(zs::Request::KnownBlock(block1_hash))
        .await
        .respond(zs::Response::KnownBlock(None));
    state_service
        .expect_request(zs::Request::KnownBlock(block2_hash))
        .await
        .respond(zs::Response::KnownBlock(None));

    // Blocks 1 & 2 are fetched in order, then verified concurrently
    peer_set
        .expect_request(zn::Request::BlocksByHash(iter::once(block1_hash).collect()))
        .await
        .respond(zn::Response::Blocks(vec![Available((
            block1.clone(),
            None,
        ))]));
    peer_set
        .expect_request(zn::Request::BlocksByHash(iter::once(block2_hash).collect()))
        .await
        .respond(zn::Response::Blocks(vec![Available((
            block2.clone(),
            None,
        ))]));

    // We can't guarantee the verification request order
    let mut remaining_blocks: HashMap<block::Hash, Arc<Block>> =
        [(block1_hash, block1), (block2_hash, block2)]
            .iter()
            .cloned()
            .collect();

    for _ in 1..=2 {
        block_verifier_router
            .expect_request_that(|req| remaining_blocks.remove(&req.block().hash()).is_some())
            .await
            .respond_with(|req| req.block().hash());
    }
    assert_eq!(
        remaining_blocks,
        HashMap::new(),
        "expected all non-tip blocks to be verified by obtain tips"
    );

    // Check that nothing unexpected happened.
    block_verifier_router.expect_no_requests().await;
    state_service.expect_no_requests().await;

    // ChainSync::extend_tips

    // Network is sent a block locator based on the tip
    peer_set
        .expect_request(zn::Request::FindBlocks {
            known_blocks: vec![block1_hash],
            stop: None,
        })
        .await
        .respond(zn::Response::BlockHashes(vec![
            block2_hash, // tip (discarded - already fetched)
            block3_hash, // expected_next
            block4_hash,
            block5_hash, // (discarded - last hash, possibly incorrect)
        ]));

    for hash in [block3_hash, block4_hash] {
        state_service
            .expect_request(zs::Request::KnownBlock(hash))
            .await
            .respond(zs::Response::KnownBlock(None));
    }

    // Clear remaining block locator requests
    for _ in 0..(sync::FANOUT - 1) {
        peer_set
            .expect_request(zn::Request::FindBlocks {
                known_blocks: vec![block1_hash],
                stop: None,
            })
            .await
            .respond(Err(zn::BoxError::from("synthetic test extend tips error")));
    }

    // Check that nothing unexpected happened.
    block_verifier_router.expect_no_requests().await;
    state_service.expect_no_requests().await;

    // Blocks 3 & 4 are fetched in order, then verified concurrently
    peer_set
        .expect_request(zn::Request::BlocksByHash(iter::once(block3_hash).collect()))
        .await
        .respond(zn::Response::Blocks(vec![Available((
            block3.clone(),
            None,
        ))]));
    peer_set
        .expect_request(zn::Request::BlocksByHash(iter::once(block4_hash).collect()))
        .await
        .respond(zn::Response::Blocks(vec![Available((
            block4.clone(),
            None,
        ))]));

    // We can't guarantee the verification request order
    let mut remaining_blocks: HashMap<block::Hash, Arc<Block>> =
        [(block3_hash, block3), (block4_hash, block4)]
            .iter()
            .cloned()
            .collect();

    for _ in 3..=4 {
        block_verifier_router
            .expect_request_that(|req| remaining_blocks.remove(&req.block().hash()).is_some())
            .await
            .respond_with(|req| req.block().hash());
    }
    assert_eq!(
        remaining_blocks,
        HashMap::new(),
        "expected all non-tip blocks to be verified by extend tips"
    );

    // Check that nothing unexpected happened.
    block_verifier_router.expect_no_requests().await;
    state_service.expect_no_requests().await;

    let chain_sync_result = chain_sync_task_handle.now_or_never();
    assert!(
        chain_sync_result.is_none(),
        "unexpected error or panic in chain sync task: {chain_sync_result:?}",
    );

    Ok(())
}

/// An incomplete checkpoint range must not strand the blocks already parked in the verifier.
///
/// This scales the production failure at checkpoints 400 and 800 down to a four-block checkpoint
/// range. Every peer response here is one a real peer would send:
///
/// - the peer's tip is block 3, so `FindBlocks` from genesis answers `[1, 2, 3]`. `obtain_tips`
///   discards the trailing hash for zcashd's quirk of appending an unrelated one, leaving blocks
///   1-2 to download.
/// - extending that tip answers `[2, 3]`, and the same trailing-hash rule discards block 3, so the
///   response extends by nothing and the prospective tip set empties. The peer did extend the tip;
///   the workaround is what drops the block, on the promise that a later fanout re-discovers it.
/// - blocks 1-2 are now parked in the checkpoint verifier, which cannot verify *any* block in a
///   range until the whole range up to checkpoint 4 has arrived. So they stay in flight forever.
///
/// Discovering the rest of the range is the only way out, and only a fresh fanout can do it. The
/// syncer must therefore keep discovering hashes while blocks are parked, rather than waiting for
/// the download queue to drain first — that wait cannot terminate. The chain grows to block 5, a
/// refreshed fanout picks up blocks 3-4, and the range verifies.
///
/// Time is paused, so a syncer that instead waits out [`sync::BLOCK_VERIFY_TIMEOUT`] and restarts
/// fails here in milliseconds of wall-clock time.
#[tokio::test(start_paused = true)]
async fn incomplete_checkpoint_range_refreshes_tips_without_verifier_timeout(
) -> Result<(), crate::BoxError> {
    let (
        chain_sync,
        _sync_status,
        mut block_verifier_router,
        mut peer_set,
        mut state_service,
        mock_chain_tip_sender,
    ) = setup_chain_sync_with_options(Height(4), STALLED_SERVICE_REQUEST_DELAY);
    mock_chain_tip_sender.send_best_tip_height(Height(0));

    let blocks: Vec<Arc<Block>> = vec![
        zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES.zcash_deserialize_into()?,
        zakura_test::vectors::BLOCK_MAINNET_1_BYTES.zcash_deserialize_into()?,
        zakura_test::vectors::BLOCK_MAINNET_2_BYTES.zcash_deserialize_into()?,
        zakura_test::vectors::BLOCK_MAINNET_3_BYTES.zcash_deserialize_into()?,
        zakura_test::vectors::BLOCK_MAINNET_4_BYTES.zcash_deserialize_into()?,
        zakura_test::vectors::BLOCK_MAINNET_5_BYTES.zcash_deserialize_into()?,
    ];
    let hashes: Vec<_> = blocks.iter().map(|block| block.hash()).collect();

    let sync_task = tokio::spawn(chain_sync.sync());

    state_service
        .expect_request(zs::Request::KnownBlock(hashes[0]))
        .await
        .respond(zs::Response::KnownBlock(Some(zs::KnownBlock::BestChain)));

    // The peer's tip is block 3. The trailing hash is discarded, leaving blocks 1-2 to download,
    // which is only half of the checkpoint range ending at block 4.
    state_service
        .expect_request(zs::Request::BlockLocator)
        .await
        .respond(zs::Response::BlockLocator(vec![hashes[0]]));
    peer_set
        .expect_request(zn::Request::FindBlocks {
            known_blocks: vec![hashes[0]],
            stop: None,
        })
        .await
        .respond(zn::Response::BlockHashes(vec![
            hashes[1], hashes[2], hashes[3],
        ]));
    state_service
        .expect_request(zs::Request::KnownBlock(hashes[1]))
        .await
        .respond(zs::Response::KnownBlock(None));
    state_service
        .expect_request(zs::Request::KnownBlock(hashes[2]))
        .await
        .respond(zs::Response::KnownBlock(None));
    for _ in 0..(sync::FANOUT - 1) {
        peer_set
            .expect_request(zn::Request::FindBlocks {
                known_blocks: vec![hashes[0]],
                stop: None,
            })
            .await
            .respond(Err(zn::BoxError::from("synthetic short-tip peer error")));
    }
    for hash in &hashes[1..=2] {
        state_service
            .expect_request(zs::Request::KnownBlock(*hash))
            .await
            .respond(zs::Response::KnownBlock(None));
    }
    for (hash, block) in hashes[1..=2].iter().zip(&blocks[1..=2]) {
        peer_set
            .expect_request(zn::Request::BlocksByHash(iter::once(*hash).collect()))
            .await
            .respond(zn::Response::Blocks(vec![Available((block.clone(), None))]));
    }

    // Hold blocks 1-2 in the verifier. A checkpoint verifier cannot verify a partial range, so these
    // requests stay pending — and the blocks stay in flight — until blocks 3-4 arrive.
    let mut pending_verifications = Vec::new();
    let mut expected_verifications: HashSet<_> = hashes[1..=2].iter().copied().collect();
    for _ in 0..2 {
        pending_verifications.push(
            block_verifier_router
                .expect_request_that(|request| {
                    expected_verifications.remove(&request.block().hash())
                })
                .await,
        );
    }
    assert!(expected_verifications.is_empty());

    // The peer extends the tip with block 3, but the trailing-hash workaround discards it, so the
    // response extends by nothing and the prospective tip set empties.
    peer_set
        .expect_request(zn::Request::FindBlocks {
            known_blocks: vec![hashes[1]],
            stop: None,
        })
        .await
        .respond(zn::Response::BlockHashes(vec![hashes[2], hashes[3]]));
    for _ in 0..(sync::FANOUT - 1) {
        peer_set
            .expect_request(zn::Request::FindBlocks {
                known_blocks: vec![hashes[1]],
                stop: None,
            })
            .await
            .respond(Err(zn::BoxError::from(
                "synthetic empty-extension peer error",
            )));
    }

    // Nothing is left to discover, but blocks 1-2 are parked in the verifier and can only be
    // verified once the rest of the range arrives. The syncer must run a fresh fanout to find it,
    // rather than waiting for its in-flight downloads to drain: that wait cannot terminate.
    let refresh_requested_at = tokio::time::Instant::now();
    let refreshed_locator = tokio::time::timeout(
        sync::BLOCK_VERIFY_TIMEOUT,
        state_service.expect_request(zs::Request::BlockLocator),
    )
    .await
    .expect(
        "syncer must discover the rest of the checkpoint range while its blocks are parked in the \
         verifier: waiting out BLOCK_VERIFY_TIMEOUT and restarting discards the partial range",
    );

    // The chain has grown to block 5, so the refreshed fanout covers the rest of the range. Blocks
    // 1-2 are still in flight, so they are re-dispatched as duplicates and not downloaded again.
    refreshed_locator.respond(zs::Response::BlockLocator(vec![hashes[0]]));
    peer_set
        .expect_request(zn::Request::FindBlocks {
            known_blocks: vec![hashes[0]],
            stop: None,
        })
        .await
        .respond(zn::Response::BlockHashes(vec![
            hashes[1], hashes[2], hashes[3], hashes[4], hashes[5],
        ]));
    state_service
        .expect_request(zs::Request::KnownBlock(hashes[1]))
        .await
        .respond(zs::Response::KnownBlock(None));
    for hash in &hashes[2..=4] {
        state_service
            .expect_request(zs::Request::KnownBlock(*hash))
            .await
            .respond(zs::Response::KnownBlock(None));
    }
    for _ in 0..(sync::FANOUT - 1) {
        peer_set
            .expect_request(zn::Request::FindBlocks {
                known_blocks: vec![hashes[0]],
                stop: None,
            })
            .await
            .respond(Err(zn::BoxError::from("synthetic refreshed-tip error")));
    }
    for hash in &hashes[1..=4] {
        state_service
            .expect_request(zs::Request::KnownBlock(*hash))
            .await
            .respond(zs::Response::KnownBlock(None));
    }
    for (hash, block) in hashes[3..=4].iter().zip(&blocks[3..=4]) {
        peer_set
            .expect_request(zn::Request::BlocksByHash(iter::once(*hash).collect()))
            .await
            .respond(zn::Response::Blocks(vec![Available((block.clone(), None))]));
    }
    let mut expected_verifications: HashSet<_> = hashes[3..=4].iter().copied().collect();
    for _ in 0..2 {
        pending_verifications.push(
            block_verifier_router
                .expect_request_that(|request| {
                    expected_verifications.remove(&request.block().hash())
                })
                .await,
        );
    }
    assert!(expected_verifications.is_empty());

    // Extending the refreshed tip discards block 5 by the same trailing-hash rule, so this response
    // extends by nothing. The checkpoint range is already complete, so that no longer matters.
    peer_set
        .expect_request(zn::Request::FindBlocks {
            known_blocks: vec![hashes[3]],
            stop: None,
        })
        .await
        .respond(zn::Response::BlockHashes(vec![hashes[4], hashes[5]]));
    for _ in 0..(sync::FANOUT - 1) {
        peer_set
            .expect_request(zn::Request::FindBlocks {
                known_blocks: vec![hashes[3]],
                stop: None,
            })
            .await
            .respond(Err(zn::BoxError::from("synthetic final-tip error")));
    }

    // The whole range is downloaded, so the checkpoint verifier can now verify all four blocks.
    for verification in pending_verifications {
        verification.respond_with(|request| request.block().hash());
    }

    assert!(
        refresh_requested_at.elapsed() < sync::BLOCK_VERIFY_TIMEOUT,
        "the checkpoint range must complete without waiting out BLOCK_VERIFY_TIMEOUT, but it took {:?}",
        refresh_requested_at.elapsed(),
    );

    tokio::task::yield_now().await;
    assert!(
        !sync_task.is_finished(),
        "legacy sync should continue after the checkpoint range completes"
    );
    sync_task.abort();

    Ok(())
}

/// Test that the syncer downloads genesis, blocks 1-2 using obtain_tips, and blocks 3-4 using extend_tips,
/// with unrelated trailing hashes that are discarded.
///
/// This test also makes sure that the syncer downloads blocks in order.
#[tokio::test]
async fn sync_blocks_trailing_hashes_ok() -> Result<(), crate::BoxError> {
    // Get services
    let (
        chain_sync_future,
        _sync_status,
        mut block_verifier_router,
        mut peer_set,
        mut state_service,
        _mock_chain_tip_sender,
    ) = setup();

    // Get blocks
    let block0: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES.zcash_deserialize_into()?;
    let block0_hash = block0.hash();

    let block1: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_1_BYTES.zcash_deserialize_into()?;
    let block1_hash = block1.hash();

    let block2: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_2_BYTES.zcash_deserialize_into()?;
    let block2_hash = block2.hash();

    let block3: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_3_BYTES.zcash_deserialize_into()?;
    let block3_hash = block3.hash();

    let block4: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_4_BYTES.zcash_deserialize_into()?;
    let block4_hash = block4.hash();

    let block5: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_5_BYTES.zcash_deserialize_into()?;
    let block5_hash = block5.hash();

    // Start the syncer
    let chain_sync_task_handle = tokio::spawn(chain_sync_future);

    // ChainSync::request_genesis

    // State is checked for genesis
    state_service
        .expect_request(zs::Request::KnownBlock(block0_hash))
        .await
        .respond(zs::Response::KnownBlock(None));

    // Block 0 is fetched and committed to the state
    peer_set
        .expect_request(zn::Request::BlocksByHash(iter::once(block0_hash).collect()))
        .await
        .respond(zn::Response::Blocks(vec![Available((
            block0.clone(),
            None,
        ))]));

    block_verifier_router
        .expect_request(zakura_consensus::Request::Commit(block0))
        .await
        .respond(block0_hash);

    // Check that nothing unexpected happened.
    // We expect more requests to the state service, because the syncer keeps on running.
    peer_set.expect_no_requests().await;
    block_verifier_router.expect_no_requests().await;

    // State is checked for genesis again
    state_service
        .expect_request(zs::Request::KnownBlock(block0_hash))
        .await
        .respond(zs::Response::KnownBlock(Some(zs::KnownBlock::BestChain)));

    // ChainSync::obtain_tips

    // State is asked for a block locator.
    state_service
        .expect_request(zs::Request::BlockLocator)
        .await
        .respond(zs::Response::BlockLocator(vec![block0_hash]));

    // Network is sent the block locator
    peer_set
        .expect_request(zn::Request::FindBlocks {
            known_blocks: vec![block0_hash],
            stop: None,
        })
        .await
        .respond(zn::Response::BlockHashes(vec![
            block1_hash, // tip
            block2_hash, // expected_next
            block3_hash, // (discarded - last hash, possibly incorrect)
        ]));

    // State is checked for each candidate hash before it is queued.
    state_service
        .expect_request(zs::Request::KnownBlock(block1_hash))
        .await
        .respond(zs::Response::KnownBlock(None));
    state_service
        .expect_request(zs::Request::KnownBlock(block2_hash))
        .await
        .respond(zs::Response::KnownBlock(None));

    // Clear remaining block locator requests
    for _ in 0..(sync::FANOUT - 1) {
        peer_set
            .expect_request(zn::Request::FindBlocks {
                known_blocks: vec![block0_hash],
                stop: None,
            })
            .await
            .respond(Err(zn::BoxError::from("synthetic test obtain tips error")));
    }

    // Check that nothing unexpected happened.
    peer_set.expect_no_requests().await;
    block_verifier_router.expect_no_requests().await;

    // State is checked for all non-tip blocks (blocks 1 & 2) in response order
    state_service
        .expect_request(zs::Request::KnownBlock(block1_hash))
        .await
        .respond(zs::Response::KnownBlock(None));
    state_service
        .expect_request(zs::Request::KnownBlock(block2_hash))
        .await
        .respond(zs::Response::KnownBlock(None));

    // Blocks 1 & 2 are fetched in order, then verified concurrently
    peer_set
        .expect_request(zn::Request::BlocksByHash(iter::once(block1_hash).collect()))
        .await
        .respond(zn::Response::Blocks(vec![Available((
            block1.clone(),
            None,
        ))]));
    peer_set
        .expect_request(zn::Request::BlocksByHash(iter::once(block2_hash).collect()))
        .await
        .respond(zn::Response::Blocks(vec![Available((
            block2.clone(),
            None,
        ))]));

    // We can't guarantee the verification request order
    let mut remaining_blocks: HashMap<block::Hash, Arc<Block>> =
        [(block1_hash, block1), (block2_hash, block2)]
            .iter()
            .cloned()
            .collect();

    for _ in 1..=2 {
        block_verifier_router
            .expect_request_that(|req| remaining_blocks.remove(&req.block().hash()).is_some())
            .await
            .respond_with(|req| req.block().hash());
    }
    assert_eq!(
        remaining_blocks,
        HashMap::new(),
        "expected all non-tip blocks to be verified by obtain tips"
    );

    // Check that nothing unexpected happened.
    block_verifier_router.expect_no_requests().await;
    state_service.expect_no_requests().await;

    // ChainSync::extend_tips

    // Network is sent a block locator based on the tip
    peer_set
        .expect_request(zn::Request::FindBlocks {
            known_blocks: vec![block1_hash],
            stop: None,
        })
        .await
        .respond(zn::Response::BlockHashes(vec![
            block2_hash, // tip (discarded - already fetched)
            block3_hash, // expected_next
            block4_hash,
            block5_hash, // (discarded - last hash, possibly incorrect)
        ]));

    for hash in [block3_hash, block4_hash] {
        state_service
            .expect_request(zs::Request::KnownBlock(hash))
            .await
            .respond(zs::Response::KnownBlock(None));
    }

    // Clear remaining block locator requests
    for _ in 0..(sync::FANOUT - 1) {
        peer_set
            .expect_request(zn::Request::FindBlocks {
                known_blocks: vec![block1_hash],
                stop: None,
            })
            .await
            .respond(Err(zn::BoxError::from("synthetic test extend tips error")));
    }

    // Check that nothing unexpected happened.
    block_verifier_router.expect_no_requests().await;
    state_service.expect_no_requests().await;

    // Blocks 3 & 4 are fetched in order, then verified concurrently
    peer_set
        .expect_request(zn::Request::BlocksByHash(iter::once(block3_hash).collect()))
        .await
        .respond(zn::Response::Blocks(vec![Available((
            block3.clone(),
            None,
        ))]));
    peer_set
        .expect_request(zn::Request::BlocksByHash(iter::once(block4_hash).collect()))
        .await
        .respond(zn::Response::Blocks(vec![Available((
            block4.clone(),
            None,
        ))]));

    // We can't guarantee the verification request order
    let mut remaining_blocks: HashMap<block::Hash, Arc<Block>> =
        [(block3_hash, block3), (block4_hash, block4)]
            .iter()
            .cloned()
            .collect();

    for _ in 3..=4 {
        block_verifier_router
            .expect_request_that(|req| remaining_blocks.remove(&req.block().hash()).is_some())
            .await
            .respond_with(|req| req.block().hash());
    }
    assert_eq!(
        remaining_blocks,
        HashMap::new(),
        "expected all non-tip blocks to be verified by extend tips"
    );

    // Check that nothing unexpected happened.
    block_verifier_router.expect_no_requests().await;
    state_service.expect_no_requests().await;

    let chain_sync_result = chain_sync_task_handle.now_or_never();
    assert!(
        chain_sync_result.is_none(),
        "unexpected error or panic in chain sync task: {chain_sync_result:?}",
    );

    Ok(())
}

/// Test that zakura-network rejects blocks that are a long way ahead of the state tip.
#[tokio::test]
async fn sync_block_lookahead_drop() -> Result<(), crate::BoxError> {
    // Get services
    let (
        chain_sync_future,
        _sync_status,
        mut block_verifier_router,
        mut peer_set,
        mut state_service,
        _mock_chain_tip_sender,
    ) = setup();

    // Get blocks
    let block0: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES.zcash_deserialize_into()?;
    let block0_hash = block0.hash();

    // Get a block that is a long way away from genesis
    let block982k: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_982681_BYTES.zcash_deserialize_into()?;

    // Start the syncer
    let chain_sync_task_handle = tokio::spawn(chain_sync_future);

    // State is checked for genesis
    state_service
        .expect_request(zs::Request::KnownBlock(block0_hash))
        .await
        .respond(zs::Response::KnownBlock(None));

    // Block 0 is fetched, but the peer returns a much higher block.
    // (Mismatching hashes are usually ignored by the network service,
    // but we use them here to test the syncer lookahead.)
    peer_set
        .expect_request(zn::Request::BlocksByHash(iter::once(block0_hash).collect()))
        .await
        .respond(zn::Response::Blocks(vec![Available((
            block982k.clone(),
            None,
        ))]));

    // Block is dropped because it is too far ahead of the tip.
    // We expect more requests to the state service, because the syncer keeps on running.
    peer_set.expect_no_requests().await;
    block_verifier_router.expect_no_requests().await;

    let chain_sync_result = chain_sync_task_handle.now_or_never();
    assert!(
        chain_sync_result.is_none(),
        "unexpected error or panic in chain sync task: {chain_sync_result:?}",
    );

    Ok(())
}

/// Test that the sync downloader rejects blocks that are too high in obtain_tips.
///
/// TODO: also test that it rejects blocks behind the tip limit. (Needs ~100 fake blocks.)
#[tokio::test]
async fn sync_block_too_high_obtain_tips() -> Result<(), crate::BoxError> {
    // Get services
    let (
        chain_sync_future,
        _sync_status,
        mut block_verifier_router,
        mut peer_set,
        mut state_service,
        _mock_chain_tip_sender,
    ) = setup();

    // Get blocks
    let block0: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES.zcash_deserialize_into()?;
    let block0_hash = block0.hash();

    let block1: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_1_BYTES.zcash_deserialize_into()?;
    let block1_hash = block1.hash();

    let block2: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_2_BYTES.zcash_deserialize_into()?;
    let block2_hash = block2.hash();

    let block3: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_3_BYTES.zcash_deserialize_into()?;
    let block3_hash = block3.hash();

    // Also get a block that is a long way away from genesis
    let block982k: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_982681_BYTES.zcash_deserialize_into()?;
    let block982k_hash = block982k.hash();

    // Start the syncer
    let chain_sync_task_handle = tokio::spawn(chain_sync_future);

    // ChainSync::request_genesis

    // State is checked for genesis
    state_service
        .expect_request(zs::Request::KnownBlock(block0_hash))
        .await
        .respond(zs::Response::KnownBlock(None));

    // Block 0 is fetched and committed to the state
    peer_set
        .expect_request(zn::Request::BlocksByHash(iter::once(block0_hash).collect()))
        .await
        .respond(zn::Response::Blocks(vec![Available((
            block0.clone(),
            None,
        ))]));

    block_verifier_router
        .expect_request(zakura_consensus::Request::Commit(block0))
        .await
        .respond(block0_hash);

    // Check that nothing unexpected happened.
    // We expect more requests to the state service, because the syncer keeps on running.
    peer_set.expect_no_requests().await;
    block_verifier_router.expect_no_requests().await;

    // State is checked for genesis again
    state_service
        .expect_request(zs::Request::KnownBlock(block0_hash))
        .await
        .respond(zs::Response::KnownBlock(Some(zs::KnownBlock::BestChain)));

    // ChainSync::obtain_tips

    // State is asked for a block locator.
    state_service
        .expect_request(zs::Request::BlockLocator)
        .await
        .respond(zs::Response::BlockLocator(vec![block0_hash]));

    // Network is sent the block locator
    peer_set
        .expect_request(zn::Request::FindBlocks {
            known_blocks: vec![block0_hash],
            stop: None,
        })
        .await
        .respond(zn::Response::BlockHashes(vec![
            block982k_hash,
            block1_hash, // tip
            block2_hash, // expected_next
            block3_hash, // (discarded - last hash, possibly incorrect)
        ]));

    // State is checked for each candidate hash before it is queued.
    state_service
        .expect_request(zs::Request::KnownBlock(block982k_hash))
        .await
        .respond(zs::Response::KnownBlock(None));
    state_service
        .expect_request(zs::Request::KnownBlock(block1_hash))
        .await
        .respond(zs::Response::KnownBlock(None));
    state_service
        .expect_request(zs::Request::KnownBlock(block2_hash))
        .await
        .respond(zs::Response::KnownBlock(None));

    // Clear remaining block locator requests
    for _ in 0..(sync::FANOUT - 1) {
        peer_set
            .expect_request(zn::Request::FindBlocks {
                known_blocks: vec![block0_hash],
                stop: None,
            })
            .await
            .respond(Err(zn::BoxError::from("synthetic test obtain tips error")));
    }

    // Check that nothing unexpected happened.
    peer_set.expect_no_requests().await;
    block_verifier_router.expect_no_requests().await;

    // State is checked for all non-tip blocks (blocks 982k, 1, 2) in response order
    state_service
        .expect_request(zs::Request::KnownBlock(block982k_hash))
        .await
        .respond(zs::Response::KnownBlock(None));
    state_service
        .expect_request(zs::Request::KnownBlock(block1_hash))
        .await
        .respond(zs::Response::KnownBlock(None));
    state_service
        .expect_request(zs::Request::KnownBlock(block2_hash))
        .await
        .respond(zs::Response::KnownBlock(None));

    // Blocks 982k, 1, 2 are fetched in order, then verified concurrently,
    // but block 982k verification is skipped because it is too high.
    peer_set
        .expect_request(zn::Request::BlocksByHash(
            iter::once(block982k_hash).collect(),
        ))
        .await
        .respond(zn::Response::Blocks(vec![Available((
            block982k.clone(),
            None,
        ))]));
    peer_set
        .expect_request(zn::Request::BlocksByHash(iter::once(block1_hash).collect()))
        .await
        .respond(zn::Response::Blocks(vec![Available((
            block1.clone(),
            None,
        ))]));
    peer_set
        .expect_request(zn::Request::BlocksByHash(iter::once(block2_hash).collect()))
        .await
        .respond(zn::Response::Blocks(vec![Available((
            block2.clone(),
            None,
        ))]));

    // At this point, the following tasks race:
    // - The valid chain verifier requests
    // - The block too high error, which causes a syncer reset and ChainSync::obtain_tips
    // - ChainSync::extend_tips for the next tip

    let chain_sync_result = chain_sync_task_handle.now_or_never();
    assert!(
        chain_sync_result.is_none(),
        "unexpected error or panic in chain sync task: {chain_sync_result:?}",
    );

    Ok(())
}

/// Test that the sync downloader rejects blocks that are too high in extend_tips.
///
/// TODO: also test that it rejects blocks behind the tip limit. (Needs ~100 fake blocks.)
#[tokio::test]
async fn sync_block_too_high_extend_tips() -> Result<(), crate::BoxError> {
    // Get services
    let (
        chain_sync_future,
        _sync_status,
        mut block_verifier_router,
        mut peer_set,
        mut state_service,
        _mock_chain_tip_sender,
    ) = setup();

    // Get blocks
    let block0: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES.zcash_deserialize_into()?;
    let block0_hash = block0.hash();

    let block1: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_1_BYTES.zcash_deserialize_into()?;
    let block1_hash = block1.hash();

    let block2: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_2_BYTES.zcash_deserialize_into()?;
    let block2_hash = block2.hash();

    let block3: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_3_BYTES.zcash_deserialize_into()?;
    let block3_hash = block3.hash();

    let block4: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_4_BYTES.zcash_deserialize_into()?;
    let block4_hash = block4.hash();

    let block5: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_5_BYTES.zcash_deserialize_into()?;
    let block5_hash = block5.hash();

    // Also get a block that is a long way away from genesis
    let block982k: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_982681_BYTES.zcash_deserialize_into()?;
    let block982k_hash = block982k.hash();

    // Start the syncer
    let chain_sync_task_handle = tokio::spawn(chain_sync_future);

    // ChainSync::request_genesis

    // State is checked for genesis
    state_service
        .expect_request(zs::Request::KnownBlock(block0_hash))
        .await
        .respond(zs::Response::KnownBlock(None));

    // Block 0 is fetched and committed to the state
    peer_set
        .expect_request(zn::Request::BlocksByHash(iter::once(block0_hash).collect()))
        .await
        .respond(zn::Response::Blocks(vec![Available((
            block0.clone(),
            None,
        ))]));

    block_verifier_router
        .expect_request(zakura_consensus::Request::Commit(block0))
        .await
        .respond(block0_hash);

    // Check that nothing unexpected happened.
    // We expect more requests to the state service, because the syncer keeps on running.
    peer_set.expect_no_requests().await;
    block_verifier_router.expect_no_requests().await;

    // State is checked for genesis again
    state_service
        .expect_request(zs::Request::KnownBlock(block0_hash))
        .await
        .respond(zs::Response::KnownBlock(Some(zs::KnownBlock::BestChain)));

    // ChainSync::obtain_tips

    // State is asked for a block locator.
    state_service
        .expect_request(zs::Request::BlockLocator)
        .await
        .respond(zs::Response::BlockLocator(vec![block0_hash]));

    // Network is sent the block locator
    peer_set
        .expect_request(zn::Request::FindBlocks {
            known_blocks: vec![block0_hash],
            stop: None,
        })
        .await
        .respond(zn::Response::BlockHashes(vec![
            block1_hash, // tip
            block2_hash, // expected_next
            block3_hash, // (discarded - last hash, possibly incorrect)
        ]));

    // State is checked for each candidate hash before it is queued.
    state_service
        .expect_request(zs::Request::KnownBlock(block1_hash))
        .await
        .respond(zs::Response::KnownBlock(None));
    state_service
        .expect_request(zs::Request::KnownBlock(block2_hash))
        .await
        .respond(zs::Response::KnownBlock(None));

    // Clear remaining block locator requests
    for _ in 0..(sync::FANOUT - 1) {
        peer_set
            .expect_request(zn::Request::FindBlocks {
                known_blocks: vec![block0_hash],
                stop: None,
            })
            .await
            .respond(Err(zn::BoxError::from("synthetic test obtain tips error")));
    }

    // Check that nothing unexpected happened.
    peer_set.expect_no_requests().await;
    block_verifier_router.expect_no_requests().await;

    // State is checked for all non-tip blocks (blocks 1 & 2) in response order
    state_service
        .expect_request(zs::Request::KnownBlock(block1_hash))
        .await
        .respond(zs::Response::KnownBlock(None));
    state_service
        .expect_request(zs::Request::KnownBlock(block2_hash))
        .await
        .respond(zs::Response::KnownBlock(None));

    // Blocks 1 & 2 are fetched in order, then verified concurrently
    peer_set
        .expect_request(zn::Request::BlocksByHash(iter::once(block1_hash).collect()))
        .await
        .respond(zn::Response::Blocks(vec![Available((
            block1.clone(),
            None,
        ))]));
    peer_set
        .expect_request(zn::Request::BlocksByHash(iter::once(block2_hash).collect()))
        .await
        .respond(zn::Response::Blocks(vec![Available((
            block2.clone(),
            None,
        ))]));

    // We can't guarantee the verification request order
    let mut remaining_blocks: HashMap<block::Hash, Arc<Block>> =
        [(block1_hash, block1), (block2_hash, block2)]
            .iter()
            .cloned()
            .collect();

    for _ in 1..=2 {
        block_verifier_router
            .expect_request_that(|req| remaining_blocks.remove(&req.block().hash()).is_some())
            .await
            .respond_with(|req| req.block().hash());
    }
    assert_eq!(
        remaining_blocks,
        HashMap::new(),
        "expected all non-tip blocks to be verified by obtain tips"
    );

    // Check that nothing unexpected happened.
    block_verifier_router.expect_no_requests().await;
    state_service.expect_no_requests().await;

    // ChainSync::extend_tips

    // Network is sent a block locator based on the tip
    peer_set
        .expect_request(zn::Request::FindBlocks {
            known_blocks: vec![block1_hash],
            stop: None,
        })
        .await
        .respond(zn::Response::BlockHashes(vec![
            block2_hash, // tip (discarded - already fetched)
            block3_hash, // expected_next
            block4_hash,
            block982k_hash,
            block5_hash, // (discarded - last hash, possibly incorrect)
        ]));

    for hash in [block3_hash, block4_hash, block982k_hash] {
        state_service
            .expect_request(zs::Request::KnownBlock(hash))
            .await
            .respond(zs::Response::KnownBlock(None));
    }

    // Clear remaining block locator requests
    for _ in 0..(sync::FANOUT - 1) {
        peer_set
            .expect_request(zn::Request::FindBlocks {
                known_blocks: vec![block1_hash],
                stop: None,
            })
            .await
            .respond(Err(zn::BoxError::from("synthetic test extend tips error")));
    }

    // Check that nothing unexpected happened.
    block_verifier_router.expect_no_requests().await;
    state_service.expect_no_requests().await;

    // Blocks 3, 4, 982k are fetched in order, then verified concurrently,
    // but block 982k verification is skipped because it is too high.
    peer_set
        .expect_request(zn::Request::BlocksByHash(iter::once(block3_hash).collect()))
        .await
        .respond(zn::Response::Blocks(vec![Available((
            block3.clone(),
            None,
        ))]));
    peer_set
        .expect_request(zn::Request::BlocksByHash(iter::once(block4_hash).collect()))
        .await
        .respond(zn::Response::Blocks(vec![Available((
            block4.clone(),
            None,
        ))]));
    peer_set
        .expect_request(zn::Request::BlocksByHash(
            iter::once(block982k_hash).collect(),
        ))
        .await
        .respond(zn::Response::Blocks(vec![Available((
            block982k.clone(),
            None,
        ))]));

    // At this point, the following tasks race:
    // - The valid chain verifier requests
    // - The block too high error, which causes a syncer reset and ChainSync::obtain_tips
    // - ChainSync::extend_tips for the next tip

    let chain_sync_result = chain_sync_task_handle.now_or_never();
    assert!(
        chain_sync_result.is_none(),
        "unexpected error or panic in chain sync task: {chain_sync_result:?}",
    );

    Ok(())
}

/// Tests that a `BlockDownloadVerifyError::Invalid` wrapping a
/// `CommitBlockError::Duplicate` error does NOT trigger a sync restart.
#[tokio::test]
async fn should_restart_sync_returns_false() {
    let commit_error = zs::CommitBlockError::Duplicate {
        hash_or_height: None,
        location: zakura_state::KnownBlock::BestChain,
    };

    let verify_block_error = VerifyBlockError::Commit(commit_error);
    let router_error = RouterError::Block {
        source: Box::new(verify_block_error),
    };

    let err = BlockDownloadVerifyError::Invalid {
        error: router_error,
        height: block::Height(42),
        hash: block::Hash::from([0xAA; 32]),
        advertiser_addr: None,
    };

    let restart = ChainSync::<
        MockService<zn::Request, zn::Response, PanicAssertion>,
        MockService<zs::Request, zs::Response, PanicAssertion>,
        MockService<zakura_consensus::Request, block::Hash, PanicAssertion>,
        MockChainTip,
    >::should_restart_sync(&err, false);
    assert!(
        !restart,
        "duplicate commit block errors should NOT trigger sync restart"
    );
}

/// A scratch state can have finalized genesis tip metadata before
/// `KnownBlock(genesis)` can find a block body. In that state, committing the
/// downloaded genesis block returns duplicate/finalized; the genesis bootstrap
/// loop must treat that as success instead of retrying forever.
#[tokio::test]
async fn request_genesis_accepts_duplicate_finalized_genesis() -> Result<(), crate::BoxError> {
    let block0: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES.zcash_deserialize_into()?;
    let block0_hash = block0.hash();

    let state_requests = Arc::new(AtomicUsize::new(0));
    let state_requests_in_service = Arc::clone(&state_requests);
    let state_service = tower::service_fn(move |request| {
        state_requests_in_service.fetch_add(1, Ordering::SeqCst);
        async move {
            assert_eq!(request, zs::Request::KnownBlock(block0_hash));
            Ok::<_, crate::BoxError>(zs::Response::KnownBlock(None))
        }
    });

    let peer_requests = Arc::new(AtomicUsize::new(0));
    let peer_requests_in_service = Arc::clone(&peer_requests);
    let peer_block = block0.clone();
    let peer_set = tower::service_fn(move |request| {
        peer_requests_in_service.fetch_add(1, Ordering::SeqCst);
        let peer_block = peer_block.clone();
        async move {
            assert_eq!(
                request,
                zn::Request::BlocksByHash(iter::once(block0_hash).collect())
            );
            Ok::<_, crate::BoxError>(zn::Response::Blocks(vec![Available((peer_block, None))]))
        }
    });

    let verifier_requests = Arc::new(AtomicUsize::new(0));
    let verifier_requests_in_service = Arc::clone(&verifier_requests);
    let verifier_service = tower::service_fn(move |request| {
        verifier_requests_in_service.fetch_add(1, Ordering::SeqCst);
        async move {
            let zakura_consensus::Request::Commit(block) = request else {
                unreachable!("no other verifier request is allowed")
            };
            assert_eq!(block.hash(), block0_hash);

            let duplicate = zs::CommitBlockError::Duplicate {
                hash_or_height: None,
                location: zs::KnownBlock::Finalized,
            };
            let duplicate = zs::CommitCheckpointVerifiedError::from(duplicate);
            let router_error = RouterError::Checkpoint {
                source: Box::new(VerifyCheckpointError::CommitCheckpointVerified(Box::new(
                    duplicate,
                ))),
            };

            Err::<block::Hash, crate::BoxError>(Box::new(router_error) as crate::BoxError)
        }
    });

    let consensus_config = ConsensusConfig::default();
    let state_config = StateConfig::ephemeral();
    let config = ZakuradConfig {
        consensus: consensus_config,
        state: state_config,
        ..Default::default()
    };
    let (mock_chain_tip, _mock_chain_tip_sender) = MockChainTip::new();
    let (misbehavior_tx, _misbehavior_rx) = tokio::sync::mpsc::channel(1);
    let (mut chain_sync, _sync_status) = ChainSync::new(
        &config,
        Height(0),
        peer_set,
        verifier_service,
        state_service,
        mock_chain_tip,
        misbehavior_tx,
    );

    tokio::time::timeout(Duration::from_secs(2), chain_sync.request_genesis())
        .await
        .expect("duplicate finalized genesis should not sleep and retry")
        .expect("duplicate finalized genesis is accepted");

    assert_eq!(state_requests.load(Ordering::SeqCst), 1);
    assert_eq!(peer_requests.load(Ordering::SeqCst), 1);
    assert_eq!(verifier_requests.load(Ordering::SeqCst), 1);

    Ok(())
}

/// In-flight checkpoint downloads can finish after a later contiguous range has
/// already reached finalized state. Those duplicate/finalized responses are
/// stale work, not a reason to restart the whole sync loop.
#[test]
fn duplicate_finalized_checkpoint_block_does_not_restart_sync() -> Result<(), crate::BoxError> {
    let block1: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_1_BYTES.zcash_deserialize_into()?;
    let block1_hash = block1.hash();

    let duplicate = zs::CommitBlockError::Duplicate {
        hash_or_height: None,
        location: zs::KnownBlock::Finalized,
    };
    let duplicate = zs::CommitCheckpointVerifiedError::from(duplicate);
    let router_error = RouterError::Checkpoint {
        source: Box::new(VerifyCheckpointError::CommitCheckpointVerified(Box::new(
            duplicate,
        ))),
    };
    let err = BlockDownloadVerifyError::Invalid {
        error: router_error,
        height: Height(1),
        hash: block1_hash,
        advertiser_addr: None,
    };

    let restart = TestChainSync::should_restart_sync(&err, false);

    assert!(
        !restart,
        "duplicate finalized checkpoint blocks are stale in-flight work, not sync restarts"
    );

    Ok(())
}

/// Verifies fix for GHSA-gvjc-3w7c-92jx: `AboveLookaheadHeightLimit` now has
/// an explicit match arm in `should_restart_sync` that returns `false`.
#[tokio::test]
async fn above_lookahead_does_not_restart_sync() {
    let err = BlockDownloadVerifyError::AboveLookaheadHeightLimit {
        height: block::Height(60_000),
        hash: block::Hash::from([0xBB; 32]),
        advertiser_addr: None,
    };

    let restart = ChainSync::<
        MockService<zn::Request, zn::Response, PanicAssertion>,
        MockService<zs::Request, zs::Response, PanicAssertion>,
        MockService<zakura_consensus::Request, block::Hash, PanicAssertion>,
        MockChainTip,
    >::should_restart_sync(&err, false);

    assert!(
        !restart,
        "AboveLookaheadHeightLimit should NOT trigger sync restart (GHSA-gvjc-3w7c-92jx fix)"
    );
}

/// Verifies fix for GHSA-gvjc-3w7c-92jx: `AboveLookaheadHeightLimit` now
/// carries `advertiser_addr` so the offending peer can be scored.
#[tokio::test]
async fn above_lookahead_has_peer_attribution() {
    let addr: PeerSocketAddr = "127.0.0.1:8233".parse().unwrap();
    let err = BlockDownloadVerifyError::AboveLookaheadHeightLimit {
        height: block::Height(60_000),
        hash: block::Hash::from([0xCC; 32]),
        advertiser_addr: Some(addr),
    };

    assert_eq!(
        err.advertiser_addr(),
        Some(addr),
        "AboveLookaheadHeightLimit should carry advertiser_addr for peer scoring \
         (GHSA-gvjc-3w7c-92jx fix)"
    );
}

/// Verifies fix for GHSA-gvjc-3w7c-92jx: both height-limit errors now
/// return `false` from `should_restart_sync` — symmetric handling.
#[tokio::test]
async fn both_height_limits_do_not_restart_sync() {
    let below = BlockDownloadVerifyError::BehindTipHeightLimit {
        height: block::Height(1),
        hash: block::Hash::from([0xDD; 32]),
    };

    let above = BlockDownloadVerifyError::AboveLookaheadHeightLimit {
        height: block::Height(60_000),
        hash: block::Hash::from([0xEE; 32]),
        advertiser_addr: None,
    };

    let restart_below = ChainSync::<
        MockService<zn::Request, zn::Response, PanicAssertion>,
        MockService<zs::Request, zs::Response, PanicAssertion>,
        MockService<zakura_consensus::Request, block::Hash, PanicAssertion>,
        MockChainTip,
    >::should_restart_sync(&below, false);

    let restart_above = ChainSync::<
        MockService<zn::Request, zn::Response, PanicAssertion>,
        MockService<zs::Request, zs::Response, PanicAssertion>,
        MockService<zakura_consensus::Request, block::Hash, PanicAssertion>,
        MockChainTip,
    >::should_restart_sync(&above, false);

    assert!(
        !restart_below,
        "BehindTipHeightLimit should NOT restart sync"
    );
    assert!(
        !restart_above,
        "AboveLookaheadHeightLimit should NOT restart sync (GHSA-gvjc-3w7c-92jx fix)"
    );
}

/// Verifies fix for GHSA-rj6c-83wx-jxf2: `InvalidHeight` does not trigger
/// sync restart and carries `advertiser_addr` for peer scoring.
#[tokio::test]
async fn invalid_height_does_not_restart_sync() {
    let addr: PeerSocketAddr = "127.0.0.1:8233".parse().unwrap();
    let err = BlockDownloadVerifyError::InvalidHeight {
        hash: block::Hash::from([0xFF; 32]),
        advertiser_addr: Some(addr),
    };

    let restart = ChainSync::<
        MockService<zn::Request, zn::Response, PanicAssertion>,
        MockService<zs::Request, zs::Response, PanicAssertion>,
        MockService<zakura_consensus::Request, block::Hash, PanicAssertion>,
        MockChainTip,
    >::should_restart_sync(&err, false);

    assert!(
        !restart,
        "InvalidHeight should NOT trigger sync restart (GHSA-rj6c-83wx-jxf2 fix)"
    );

    assert_eq!(
        err.advertiser_addr(),
        Some(addr),
        "InvalidHeight should carry advertiser_addr for peer scoring"
    );
}

/// Tests that a `notfound` block download failure requeues the missing block
/// before the syncer restarts the whole sync round.
#[tokio::test]
async fn not_found_download_requeues_missing_block() -> Result<(), crate::BoxError> {
    let (
        mut chain_sync,
        _sync_status,
        mut block_verifier_router,
        mut peer_set,
        _state_service,
        _mock_chain_tip_sender,
    ) = setup_chain_sync();

    let block1: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_1_BYTES.zcash_deserialize_into()?;
    let block1_hash = block1.hash();

    let error = BlockDownloadVerifyError::DownloadFailed {
        error: not_found_block_error(block1_hash),
        hash: block1_hash,
    };

    let requeue = tokio::spawn(async move {
        chain_sync
            .handle_block_response_with_missing_retry(Err(error))
            .await
    });

    peer_set
        .expect_request(zn::Request::BlocksByHash(iter::once(block1_hash).collect()))
        .await
        .respond(Err(not_found_block_error(block1_hash)));

    requeue
        .await
        .expect("missing block retry task should not panic")?;

    block_verifier_router.expect_no_requests().await;

    Ok(())
}

/// Tests that queue-level `notfound` retries are bounded.
#[tokio::test]
async fn not_found_download_restarts_after_queue_retry_limit() {
    let (
        mut chain_sync,
        _sync_status,
        _block_verifier_router,
        mut peer_set,
        _state_service,
        _mock_chain_tip_sender,
    ) = setup_chain_sync();

    let block_hash = block::Hash::from([0xAB; 32]);
    chain_sync
        .missing_block_retry_counts
        .insert(block_hash, sync::MISSING_BLOCK_DOWNLOAD_RETRY_LIMIT);

    let error = BlockDownloadVerifyError::DownloadFailed {
        error: not_found_block_error(block_hash),
        hash: block_hash,
    };

    let result = chain_sync
        .handle_block_response_with_missing_retry(Err(error))
        .await;

    assert!(
        result.is_err(),
        "notfound downloads should restart sync after queue retry limit"
    );

    peer_set.expect_no_requests().await;
}

/// Tests that a `notfound` block download triggers sync restart once the
/// queue-level retry handler has exhausted its retries.
#[tokio::test]
async fn not_found_download_restarts_sync() {
    let block_hash = block::Hash::from([0xCD; 32]);
    let err = BlockDownloadVerifyError::DownloadFailed {
        error: not_found_block_error(block_hash),
        hash: block_hash,
    };

    let restart = TestChainSync::should_restart_sync(&err, false);
    assert!(
        restart,
        "notfound block downloads should restart sync after queue retries"
    );
}

/// Unit test for the refactored [`ChainSync::build_extend`]: it must *discover* the next batch of
/// download hashes from a prospective tip, performing the FindBlocks fan-out and parsing the
/// response, **without** dispatching any block downloads or otherwise touching syncer state.
///
/// This is the core property the continuous-refill `sync_round` `select!` loop relies on: discovery
/// (`build_extend`) runs concurrently as a `self`-free future, while dispatch happens separately via
/// the reserve.
#[tokio::test]
async fn build_extend_discovers_hashes_without_dispatching() -> Result<(), crate::BoxError> {
    let (
        _chain_sync,
        _sync_status,
        mut block_verifier_router,
        mut peer_set,
        mut state_service,
        _mock_chain_tip_sender,
    ) = setup_chain_sync();

    let block1: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_1_BYTES.zcash_deserialize_into()?;
    let block1_hash = block1.hash();
    let block2: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_2_BYTES.zcash_deserialize_into()?;
    let block2_hash = block2.hash();
    let block3: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_3_BYTES.zcash_deserialize_into()?;
    let block3_hash = block3.hash();
    let block4: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_4_BYTES.zcash_deserialize_into()?;
    let block4_hash = block4.hash();
    let block5: Arc<Block> =
        zakura_test::vectors::BLOCK_MAINNET_5_BYTES.zcash_deserialize_into()?;
    let block5_hash = block5.hash();

    // A single prospective tip: peers are asked to extend `block1`, expecting `block2` next.
    let tip = sync::CheckedTip {
        tip: block1_hash,
        expected_next: block2_hash,
    };
    let tips: HashSet<_> = iter::once(tip).collect();

    // `build_extend` owns a clone of the tip network so it can run without borrowing `self`.
    let tip_network = Timeout::new(peer_set.clone(), sync::TIPS_RESPONSE_TIMEOUT);
    let extend_handle = tokio::spawn(TestChainSync::build_extend(
        tip_network,
        state_service.clone(),
        tips,
    ));

    // One peer extends the tip. The response starts with the expected hash (the match anchor) and
    // ends with a possibly-incorrect trailing hash that the syncer discards.
    peer_set
        .expect_request(zn::Request::FindBlocks {
            known_blocks: vec![block1_hash],
            stop: None,
        })
        .await
        .respond(zn::Response::BlockHashes(vec![
            block2_hash, // expected_next (match anchor, not downloaded)
            block3_hash,
            block4_hash,
            block5_hash, // (discarded - last hash, possibly incorrect)
        ]));

    for hash in [block3_hash, block4_hash] {
        state_service
            .expect_request(zs::Request::KnownBlock(hash))
            .await
            .respond(zs::Response::KnownBlock(None));
    }

    // The remaining fan-out requests fail and are ignored.
    for _ in 0..(sync::FANOUT - 1) {
        peer_set
            .expect_request(zn::Request::FindBlocks {
                known_blocks: vec![block1_hash],
                stop: None,
            })
            .await
            .respond(Err(zn::BoxError::from("synthetic test extend tips error")));
    }

    let (download_set, prospective_tips, discovered) = extend_handle
        .await
        .expect("build_extend task should not panic")?;

    // Discovery: blocks 3 & 4 are queued for download, in response order. Block 2 is the match
    // anchor and block 5 is the discarded trailing hash, so neither is downloaded.
    assert_eq!(
        download_set.into_iter().collect::<Vec<_>>(),
        vec![block3_hash, block4_hash],
        "build_extend should discover the inner hashes in response order",
    );
    assert_eq!(
        discovered, 2,
        "discovered count should match the download set length",
    );

    // The new prospective tip extends from block3, expecting block4 next.
    let expected_tip = sync::CheckedTip {
        tip: block3_hash,
        expected_next: block4_hash,
    };
    assert_eq!(
        prospective_tips,
        iter::once(expected_tip).collect(),
        "build_extend should return the next prospective tip",
    );

    // The key property: discovery dispatched no downloads and verified nothing. Only the FindBlocks
    // fan-out was sent — no `BlocksByHash`, no verifier requests.
    peer_set.expect_no_requests().await;
    block_verifier_router.expect_no_requests().await;

    Ok(())
}

#[tokio::test]
async fn obtain_tips_ignores_known_hash_after_first_unknown() -> Result<(), crate::BoxError> {
    let (
        mut chain_sync,
        _sync_status,
        mut block_verifier_router,
        mut peer_set,
        mut state_service,
        _mock_chain_tip_sender,
    ) = setup_chain_sync();

    let locator = block::Hash::from([0x01; 32]);
    let unknown = block::Hash::from([0x02; 32]);
    let known_not_in_locator = block::Hash::from([0x03; 32]);
    let trailing = block::Hash::from([0x04; 32]);

    let respond_to_requests = async {
        state_service
            .expect_request(zs::Request::BlockLocator)
            .await
            .respond(zs::Response::BlockLocator(vec![locator]));

        peer_set
            .expect_request(zn::Request::FindBlocks {
                known_blocks: vec![locator],
                stop: None,
            })
            .await
            .respond(zn::Response::BlockHashes(vec![
                unknown,
                known_not_in_locator,
                trailing,
            ]));

        state_service
            .expect_request(zs::Request::KnownBlock(unknown))
            .await
            .respond(zs::Response::KnownBlock(None));
        state_service
            .expect_request(zs::Request::KnownBlock(known_not_in_locator))
            .await
            .respond(zs::Response::KnownBlock(Some(zs::KnownBlock::BestChain)));

        for _ in 0..(sync::FANOUT - 1) {
            peer_set
                .expect_request(zn::Request::FindBlocks {
                    known_blocks: vec![locator],
                    stop: None,
                })
                .await
                .respond(Err(zn::BoxError::from("synthetic test obtain tips error")));
        }

        Ok::<_, crate::BoxError>(())
    };

    let (extra_hashes, responded) = futures::join!(chain_sync.obtain_tips(), respond_to_requests);
    responded?;
    let extra_hashes = extra_hashes?;
    assert!(extra_hashes.is_empty());

    peer_set.expect_no_requests().await;
    block_verifier_router.expect_no_requests().await;
    state_service.expect_no_requests().await;

    Ok(())
}

#[tokio::test]
async fn build_extend_ignores_malformed_find_blocks_responses() -> Result<(), crate::BoxError> {
    let (
        _chain_sync,
        _sync_status,
        mut block_verifier_router,
        mut peer_set,
        mut state_service,
        _mock_chain_tip_sender,
    ) = setup_chain_sync();

    let tip = block::Hash::from([0x10; 32]);
    let expected_next = block::Hash::from([0x11; 32]);
    let unknown = block::Hash::from([0x12; 32]);
    let known_suffix = block::Hash::from([0x13; 32]);
    let trailing = block::Hash::from([0x14; 32]);
    let random = block::Hash::from([0x15; 32]);

    let tips = HashSet::from([sync::CheckedTip { tip, expected_next }]);
    let extend_handle = tokio::spawn(TestChainSync::build_extend(
        Timeout::new(peer_set.clone(), sync::TIPS_RESPONSE_TIMEOUT),
        state_service.clone(),
        tips,
    ));

    peer_set
        .expect_request(zn::Request::FindBlocks {
            known_blocks: vec![tip],
            stop: None,
        })
        .await
        .respond(zn::Response::BlockHashes(vec![
            expected_next,
            unknown,
            known_suffix,
            trailing,
        ]));
    state_service
        .expect_request(zs::Request::KnownBlock(unknown))
        .await
        .respond(zs::Response::KnownBlock(None));
    state_service
        .expect_request(zs::Request::KnownBlock(known_suffix))
        .await
        .respond(zs::Response::KnownBlock(Some(zs::KnownBlock::BestChain)));

    peer_set
        .expect_request(zn::Request::FindBlocks {
            known_blocks: vec![tip],
            stop: None,
        })
        .await
        .respond(zn::Response::BlockHashes(vec![
            expected_next,
            unknown,
            unknown,
            trailing,
        ]));
    peer_set
        .expect_request(zn::Request::FindBlocks {
            known_blocks: vec![tip],
            stop: None,
        })
        .await
        .respond(zn::Response::BlockHashes(vec![random]));

    let (download_set, prospective_tips, discovered) = extend_handle
        .await
        .expect("build_extend task should not panic")?;
    assert!(download_set.is_empty());
    assert!(prospective_tips.is_empty());
    assert_eq!(discovered, 0);

    peer_set.expect_no_requests().await;
    block_verifier_router.expect_no_requests().await;
    state_service.expect_no_requests().await;

    Ok(())
}

#[tokio::test]
async fn build_extend_rejects_oversized_response_before_state_queries(
) -> Result<(), crate::BoxError> {
    let (
        _chain_sync,
        _sync_status,
        mut block_verifier_router,
        mut peer_set,
        mut state_service,
        _mock_chain_tip_sender,
    ) = setup_chain_sync();

    let tip = block::Hash::from([0x20; 32]);
    let expected_next = block::Hash::from([0x21; 32]);
    let trailing = block::Hash::from([0x22; 32]);
    let unknown_hashes = (0..=sync::MAX_TIPS_RESPONSE_HASH_COUNT).map(|index| {
        let index = u64::try_from(index).expect("test hash count fits in u64");
        let mut bytes = [0; 32];
        bytes[..8].copy_from_slice(&index.to_le_bytes());
        block::Hash::from(bytes)
    });
    let response_hashes = std::iter::once(expected_next)
        .chain(unknown_hashes)
        .chain(std::iter::once(trailing))
        .collect();

    let tips = HashSet::from([sync::CheckedTip { tip, expected_next }]);
    let extend_handle = tokio::spawn(TestChainSync::build_extend(
        Timeout::new(peer_set.clone(), sync::TIPS_RESPONSE_TIMEOUT),
        state_service.clone(),
        tips,
    ));

    peer_set
        .expect_request(zn::Request::FindBlocks {
            known_blocks: vec![tip],
            stop: None,
        })
        .await
        .respond(zn::Response::BlockHashes(response_hashes));

    for _ in 0..(sync::FANOUT - 1) {
        peer_set
            .expect_request(zn::Request::FindBlocks {
                known_blocks: vec![tip],
                stop: None,
            })
            .await
            .respond(Err(zn::BoxError::from(
                "synthetic test oversized response error",
            )));
    }

    let (download_set, prospective_tips, discovered) = extend_handle
        .await
        .expect("build_extend task should not panic")?;
    assert!(download_set.is_empty());
    assert!(prospective_tips.is_empty());
    assert_eq!(discovered, 0);

    peer_set.expect_no_requests().await;
    block_verifier_router.expect_no_requests().await;
    state_service.expect_no_requests().await;

    Ok(())
}

#[tokio::test]
async fn build_extend_ignores_known_trailing_find_blocks_hash() -> Result<(), crate::BoxError> {
    let (
        _chain_sync,
        _sync_status,
        mut block_verifier_router,
        mut peer_set,
        mut state_service,
        _mock_chain_tip_sender,
    ) = setup_chain_sync();

    let tip = block::Hash::from([0x20; 32]);
    let expected_next = block::Hash::from([0x21; 32]);
    let unknown_a = block::Hash::from([0x22; 32]);
    let unknown_b = block::Hash::from([0x23; 32]);

    let tips = HashSet::from([sync::CheckedTip { tip, expected_next }]);
    let extend_handle = tokio::spawn(TestChainSync::build_extend(
        Timeout::new(peer_set.clone(), sync::TIPS_RESPONSE_TIMEOUT),
        state_service.clone(),
        tips,
    ));

    peer_set
        .expect_request(zn::Request::FindBlocks {
            known_blocks: vec![tip],
            stop: None,
        })
        .await
        .respond(zn::Response::BlockHashes(vec![
            expected_next,
            unknown_a,
            unknown_b,
            tip, // zcashd can append an unrelated known hash here.
        ]));

    for hash in [unknown_a, unknown_b] {
        state_service
            .expect_request(zs::Request::KnownBlock(hash))
            .await
            .respond(zs::Response::KnownBlock(None));
    }

    for _ in 0..(sync::FANOUT - 1) {
        peer_set
            .expect_request(zn::Request::FindBlocks {
                known_blocks: vec![tip],
                stop: None,
            })
            .await
            .respond(Err(zn::BoxError::from("synthetic test extend tips error")));
    }

    let (download_set, prospective_tips, discovered) = extend_handle
        .await
        .expect("build_extend task should not panic")?;

    assert_eq!(
        download_set.into_iter().collect::<Vec<_>>(),
        vec![unknown_a, unknown_b],
        "build_extend should keep valid inner hashes and discard the trailing known hash",
    );
    assert_eq!(discovered, 2);
    assert_eq!(
        prospective_tips,
        HashSet::from([sync::CheckedTip {
            tip: unknown_a,
            expected_next: unknown_b,
        }]),
    );

    peer_set.expect_no_requests().await;
    block_verifier_router.expect_no_requests().await;
    state_service.expect_no_requests().await;

    Ok(())
}

/// A registry miss (every ready peer marked missing the block) within budget schedules a backoff
/// retry instead of blocking the loop or restarting the round, and does not re-request the block
/// inline — the retry is deferred to the sync loop's timer arm so peers can drain meanwhile.
#[tokio::test]
async fn registry_miss_schedules_backoff_retry() {
    let (
        mut chain_sync,
        _sync_status,
        _block_verifier_router,
        mut peer_set,
        _state_service,
        _mock_chain_tip_sender,
    ) = setup_chain_sync();

    let block_hash = block::Hash::from([0xAB; 32]);
    let error = BlockDownloadVerifyError::DownloadFailed {
        error: not_found_registry_error(block_hash),
        hash: block_hash,
    };

    let result = chain_sync
        .handle_block_response_with_missing_retry(Err(error))
        .await;

    assert!(
        result.is_ok(),
        "a registry miss within budget should keep the round alive, not restart"
    );
    assert!(
        chain_sync.registry_miss_retry.contains_key(&block_hash),
        "the missing block should be scheduled for a backoff retry"
    );
    assert_eq!(
        chain_sync.registry_miss_retry_counts.get(&block_hash),
        Some(&1),
        "the registry-miss retry budget should be consumed once",
    );

    // The retry fires from the sync loop's timer arm, not inline, so no block is re-requested here.
    peer_set.expect_no_requests().await;
}

/// A registry miss past its retry budget restarts the round and clears its retry state.
#[tokio::test]
async fn registry_miss_restarts_after_retry_limit() {
    let (
        mut chain_sync,
        _sync_status,
        _block_verifier_router,
        mut peer_set,
        _state_service,
        _mock_chain_tip_sender,
    ) = setup_chain_sync();

    let block_hash = block::Hash::from([0xCD; 32]);
    chain_sync
        .registry_miss_retry_counts
        .insert(block_hash, sync::MISSING_BLOCK_REGISTRY_RETRY_LIMIT);

    let error = BlockDownloadVerifyError::DownloadFailed {
        error: not_found_registry_error(block_hash),
        hash: block_hash,
    };

    let result = chain_sync
        .handle_block_response_with_missing_retry(Err(error))
        .await;

    assert!(
        result.is_err(),
        "a registry miss should restart sync once the retry budget is exhausted"
    );
    assert!(
        !chain_sync.registry_miss_retry.contains_key(&block_hash),
        "exhausted retry schedule should be cleared"
    );
    assert!(
        !chain_sync
            .registry_miss_retry_counts
            .contains_key(&block_hash),
        "exhausted retry budget should be cleared"
    );

    peer_set.expect_no_requests().await;
}

/// A second block registry-missing while the first is still backing off must not drop the first:
/// both stay scheduled, because the retry state is a per-hash map rather than a single slot.
#[tokio::test]
async fn registry_miss_schedules_multiple_blocks() {
    let (
        mut chain_sync,
        _sync_status,
        _block_verifier_router,
        mut peer_set,
        _state_service,
        _mock_chain_tip_sender,
    ) = setup_chain_sync();

    let first_hash = block::Hash::from([0x11; 32]);
    let second_hash = block::Hash::from([0x22; 32]);

    for hash in [first_hash, second_hash] {
        let error = BlockDownloadVerifyError::DownloadFailed {
            error: not_found_registry_error(hash),
            hash,
        };
        chain_sync
            .handle_block_response_with_missing_retry(Err(error))
            .await
            .expect("a registry miss within budget should not restart");
    }

    assert!(
        chain_sync.registry_miss_retry.contains_key(&first_hash)
            && chain_sync.registry_miss_retry.contains_key(&second_hash),
        "both registry-missed blocks should stay scheduled for retry",
    );

    peer_set.expect_no_requests().await;
}

/// A successful block response clears that block's registry-miss retry schedule and budget, so the
/// head-of-line gate (which pauses speculative dispatch while a retry is pending) lifts and the round
/// resumes once the missing block finally arrives.
#[tokio::test]
async fn registry_miss_retry_clears_on_successful_block() {
    let (
        mut chain_sync,
        _sync_status,
        _block_verifier_router,
        mut peer_set,
        _state_service,
        _mock_chain_tip_sender,
    ) = setup_chain_sync();

    let block_hash = block::Hash::from([0xAB; 32]);

    // A registry miss schedules the block for a backoff retry.
    chain_sync
        .handle_block_response_with_missing_retry(Err(BlockDownloadVerifyError::DownloadFailed {
            error: not_found_registry_error(block_hash),
            hash: block_hash,
        }))
        .await
        .expect("a registry miss within budget should not restart");
    assert!(chain_sync.registry_miss_retry.contains_key(&block_hash));

    // The block then downloads successfully (a peer connected, or the inventory marker expired).
    chain_sync
        .handle_block_response_with_missing_retry(Ok((Height(42), block_hash)))
        .await
        .expect("a successful response should not restart");

    assert!(
        !chain_sync.registry_miss_retry.contains_key(&block_hash),
        "a successful block should clear its scheduled registry-miss retry"
    );
    assert!(
        !chain_sync
            .registry_miss_retry_counts
            .contains_key(&block_hash),
        "a successful block should clear its consumed registry-miss budget"
    );

    peer_set.expect_no_requests().await;
}

/// A successful response clears only the responded block's retry state, leaving other registry-missed
/// blocks scheduled — the retry state is keyed per-hash, so one block arriving must not drop another.
#[tokio::test]
async fn registry_miss_retry_clears_only_the_responded_block() {
    let (
        mut chain_sync,
        _sync_status,
        _block_verifier_router,
        mut peer_set,
        _state_service,
        _mock_chain_tip_sender,
    ) = setup_chain_sync();

    let kept_hash = block::Hash::from([0x11; 32]);
    let arrived_hash = block::Hash::from([0x22; 32]);

    for hash in [kept_hash, arrived_hash] {
        chain_sync
            .handle_block_response_with_missing_retry(Err(
                BlockDownloadVerifyError::DownloadFailed {
                    error: not_found_registry_error(hash),
                    hash,
                },
            ))
            .await
            .expect("a registry miss within budget should not restart");
    }

    // One of the two missing blocks arrives; the other is still missing.
    chain_sync
        .handle_block_response_with_missing_retry(Ok((Height(7), arrived_hash)))
        .await
        .expect("a successful response should not restart");

    assert!(
        !chain_sync.registry_miss_retry.contains_key(&arrived_hash),
        "the arrived block's retry should be cleared"
    );
    assert!(
        chain_sync.registry_miss_retry.contains_key(&kept_hash),
        "a different block's retry must not be cleared by an unrelated success"
    );

    peer_set.expect_no_requests().await;
}

/// The scheduled retry is deferred by the backoff rather than run inline: the recorded deadline is one
/// [`REGISTRY_MISS_RETRY_BACKOFF`] in the future. This is what lets `sync_round` keep draining peers
/// during the wait (the whole point of moving the backoff off the inline blocking `sleep`).
///
/// [`REGISTRY_MISS_RETRY_BACKOFF`]: sync::REGISTRY_MISS_RETRY_BACKOFF
#[tokio::test]
async fn registry_miss_retry_is_deferred_by_the_backoff() {
    let (
        mut chain_sync,
        _sync_status,
        _block_verifier_router,
        mut peer_set,
        _state_service,
        _mock_chain_tip_sender,
    ) = setup_chain_sync();

    let block_hash = block::Hash::from([0xAB; 32]);

    let before = tokio::time::Instant::now();
    chain_sync
        .handle_block_response_with_missing_retry(Err(BlockDownloadVerifyError::DownloadFailed {
            error: not_found_registry_error(block_hash),
            hash: block_hash,
        }))
        .await
        .expect("a registry miss within budget should not restart");
    let after = tokio::time::Instant::now();

    let deadline = chain_sync
        .registry_miss_retry
        .get(&block_hash)
        .copied()
        .expect("the missing block should be scheduled");

    // deadline == insert_time + backoff, and before <= insert_time <= after, so the deadline lands
    // exactly one backoff interval ahead — strictly in the future, never immediate.
    assert!(
        deadline >= before + sync::REGISTRY_MISS_RETRY_BACKOFF
            && deadline <= after + sync::REGISTRY_MISS_RETRY_BACKOFF,
        "the retry deadline should be one backoff interval in the future, not immediate"
    );

    peer_set.expect_no_requests().await;
}

/// Repeated registry misses for the same block accumulate its retry budget and re-arm the backoff
/// deadline each time, so a persistently-missing block keeps head-of-line priority until its budget
/// is exhausted (at which point [`registry_miss_restarts_after_retry_limit`] takes over).
#[tokio::test]
async fn registry_miss_retry_accumulates_budget_for_the_same_block() {
    let (
        mut chain_sync,
        _sync_status,
        _block_verifier_router,
        mut peer_set,
        _state_service,
        _mock_chain_tip_sender,
    ) = setup_chain_sync();

    let block_hash = block::Hash::from([0xAB; 32]);
    let miss = || BlockDownloadVerifyError::DownloadFailed {
        error: not_found_registry_error(block_hash),
        hash: block_hash,
    };

    chain_sync
        .handle_block_response_with_missing_retry(Err(miss()))
        .await
        .expect("a registry miss within budget should not restart");
    let first_deadline = chain_sync
        .registry_miss_retry
        .get(&block_hash)
        .copied()
        .expect("the missing block should be scheduled");

    chain_sync
        .handle_block_response_with_missing_retry(Err(miss()))
        .await
        .expect("a second registry miss within budget should not restart");
    let second_deadline = chain_sync
        .registry_miss_retry
        .get(&block_hash)
        .copied()
        .expect("the missing block should still be scheduled");

    assert_eq!(
        chain_sync.registry_miss_retry_counts.get(&block_hash),
        Some(&2),
        "each registry miss for the same block should consume one more retry from its budget"
    );
    assert!(
        second_deadline >= first_deadline,
        "each miss should re-arm the backoff deadline"
    );

    peer_set.expect_no_requests().await;
}

fn setup() -> (
    // ChainSync
    impl Future<Output = Result<(), Report>> + Send,
    SyncStatus,
    // BlockVerifierRouter
    MockService<zakura_consensus::Request, block::Hash, PanicAssertion>,
    // PeerSet
    MockService<zakura_network::Request, zakura_network::Response, PanicAssertion>,
    // StateService
    MockService<zakura_state::Request, zakura_state::Response, PanicAssertion>,
    MockChainTipSender,
) {
    let (
        chain_sync,
        sync_status,
        block_verifier_router,
        peer_set,
        state_service,
        mock_chain_tip_sender,
    ) = setup_chain_sync();

    let chain_sync_future = chain_sync.sync();

    (
        chain_sync_future,
        sync_status,
        block_verifier_router,
        peer_set,
        state_service,
        mock_chain_tip_sender,
    )
}

fn setup_chain_sync() -> (
    TestChainSync,
    SyncStatus,
    MockService<zakura_consensus::Request, block::Hash, PanicAssertion>,
    MockService<zakura_network::Request, zakura_network::Response, PanicAssertion>,
    MockService<zakura_state::Request, zakura_state::Response, PanicAssertion>,
    MockChainTipSender,
) {
    setup_chain_sync_with_options(Height(0), MAX_SERVICE_REQUEST_DELAY)
}

fn setup_chain_sync_with_options(
    max_checkpoint_height: Height,
    max_service_request_delay: Duration,
) -> (
    TestChainSync,
    SyncStatus,
    MockService<zakura_consensus::Request, block::Hash, PanicAssertion>,
    MockService<zakura_network::Request, zakura_network::Response, PanicAssertion>,
    MockService<zakura_state::Request, zakura_state::Response, PanicAssertion>,
    MockChainTipSender,
) {
    let _init_guard = zakura_test::init();

    let consensus_config = ConsensusConfig::default();
    let state_config = StateConfig::ephemeral();
    let config = ZakuradConfig {
        consensus: consensus_config,
        state: state_config,
        ..Default::default()
    };

    // These tests run multiple tasks in parallel.
    // So machines under heavy load need a longer delay.
    // (For example, CI machines with limited cores.)
    let peer_set = MockService::build()
        .with_max_request_delay(max_service_request_delay)
        .for_unit_tests();

    let block_verifier_router = MockService::build()
        .with_max_request_delay(max_service_request_delay)
        .for_unit_tests();

    let state_service = MockService::build()
        .with_max_request_delay(max_service_request_delay)
        .for_unit_tests();

    let (mock_chain_tip, mock_chain_tip_sender) = MockChainTip::new();

    let (misbehavior_tx, _misbehavior_rx) = tokio::sync::mpsc::channel(1);
    let (chain_sync, sync_status) = ChainSync::new(
        &config,
        max_checkpoint_height,
        peer_set.clone(),
        block_verifier_router.clone(),
        state_service.clone(),
        mock_chain_tip,
        misbehavior_tx,
    );

    (
        chain_sync,
        sync_status,
        block_verifier_router,
        peer_set,
        state_service,
        mock_chain_tip_sender,
    )
}

fn not_found_block_error(_hash: block::Hash) -> crate::BoxError {
    zn::SharedPeerError::from(zn::PeerError::NotFoundResponse(Vec::new())).into()
}

/// Builds a download error representing a registry miss: the peer set found
/// every ready peer marked missing the block (a synthetic
/// `NotFoundRegistry`), as opposed to a single peer's `notfound`.
fn not_found_registry_error(_hash: block::Hash) -> crate::BoxError {
    zn::SharedPeerError::from(zn::PeerError::NotFoundRegistry(Vec::new())).into()
}

#[test]
fn debug_skip_regtest_genesis_self_seed_defaults_off_and_is_opt_in() {
    use crate::components::sync::Config;

    // Default is off, so standalone Regtest nodes keep self-seeding genesis.
    assert!(!Config::default().debug_skip_regtest_genesis_self_seed);
    assert_eq!(
        Config::default().debug_blocksync_throughput_target_height,
        None
    );

    // Opt-in still parses despite `deny_unknown_fields`.
    let config: Config = toml::from_str("debug_skip_regtest_genesis_self_seed = true")
        .expect("sync config with the genesis-bootstrap flag parses");
    assert!(config.debug_skip_regtest_genesis_self_seed);
    let config: Config = toml::from_str("debug_blocksync_throughput_target_height = 100")
        .expect("sync config with the block-sync throughput flag parses");
    assert_eq!(config.debug_blocksync_throughput_target_height, Some(100));

    // Skipped on serialize, so `zakurad generate` output (and the stored-config
    // compatibility snapshot) stays stable.
    let serialized = toml::to_string(&Config::default()).expect("sync config serializes");
    assert!(
        !serialized.contains("debug_skip_regtest_genesis_self_seed"),
        "debug bootstrap flag must not appear in generated config output"
    );
    assert!(
        !serialized.contains("debug_blocksync_throughput_target_height"),
        "debug block-sync throughput flag must not appear in generated config output"
    );
}

/// A peer that returns *zero* blocks for a single-hash download request must be
/// treated as a retryable download failure, not crash the whole node.
///
/// Regression for a `downloads.rs` `assert_eq!(blocks.len(), 1)` panic: a
/// gossiped single-hash fetch that raced an empty response took down a Zakura
/// node mid catch-up (`thread 'tokio-rt-worker' panicked ... wrong number of
/// blocks in response to a single hash`, propagated to a fatal syncer panic). A
/// misbehaving or racing peer must not be able to kill the node, so an
/// unexpected block count is surfaced as a `DownloadFailed` the syncer retries.
#[tokio::test]
async fn empty_block_response_is_retryable_download_failure() {
    let _init_guard = zakura_test::init();

    let mut peer_set = MockService::build().for_unit_tests::<zn::Request, zn::Response, _>();
    let verifier =
        MockService::build().for_unit_tests::<zakura_consensus::Request, block::Hash, _>();
    let (chain_tip, _chain_tip_sender) = MockChainTip::new();
    let (past_lookahead_limit_sender, _past_lookahead_limit_receiver) =
        tokio::sync::watch::channel(false);

    let mut downloads = Downloads::new(
        peer_set.clone(),
        verifier,
        chain_tip,
        past_lookahead_limit_sender,
        sync::MIN_CONCURRENCY_LIMIT,
        Height(0),
        LegacySyncTrace::new(None, false),
    );

    let block0: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into()
        .expect("test vector deserializes");
    let hash = block0.hash();

    downloads
        .download_and_verify(hash)
        .await
        .expect("queuing a fresh hash succeeds");

    // The peer responds to the single-hash request with an empty block list.
    peer_set
        .expect_request(zn::Request::BlocksByHash(iter::once(hash).collect()))
        .await
        .respond(zn::Response::Blocks(vec![]));

    let result = downloads
        .next()
        .await
        .expect("the download task produces a result instead of panicking");

    assert!(
        matches!(result, Err(BlockDownloadVerifyError::DownloadFailed { .. })),
        "an empty block response must be a retryable DownloadFailed, got {result:?}",
    );
}
