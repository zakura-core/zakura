//! Ordering contracts for incremental discovery and optional block hints.

use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use futures::{FutureExt, StreamExt};
use indexmap::IndexSet;
use tower::{timeout::Timeout, ServiceExt};
use zakura_chain::{
    block::{self, Block, Height},
    parameters::Network,
    serialization::ZcashDeserializeInto,
};
use zakura_network as zn;
use zakura_state as zs;
use zakura_test::mock_service::{MockService, PanicAssertion};

use super::vectors::{setup_chain_sync, TestChainSync};
use crate::{
    components::sync::{self, discovery::Event, CheckedTip, FANOUT, TIPS_RESPONSE_TIMEOUT},
    BoxError,
};

#[tokio::test(start_paused = true)]
async fn round_accounting_waits_for_every_reply_and_keeps_the_aggregate() {
    let (mut sync, _, mut verifier, mut peers, mut state, _) = setup_chain_sync();
    let (lengths, mut changes) = sync::RecentSyncLengths::new();
    sync.recent_syncs = lengths;
    let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let hash = block.hash();
    let locator = block.header.previous_block_hash;
    let round = sync::discovery::discover(
        Timeout::new(peers.clone(), TIPS_RESPONSE_TIMEOUT),
        state.clone(),
        vec![(vec![locator], None)],
        false,
    );
    let syncing = sync.sync_round(IndexSet::new(), Some(round), None);
    let scenario = async {
        let mut replies = Vec::new();
        for _ in 0..FANOUT {
            replies.push(
                peers
                    .expect_request(zn::Request::FindBlocks {
                        known_blocks: vec![locator],
                        stop: None,
                    })
                    .await,
            );
        }
        replies
            .remove(0)
            .respond(zn::Response::BlockHashes(vec![hash]));
        for _ in 0..2 {
            state
                .expect_request(zs::Request::KnownBlock(hash))
                .await
                .respond(zs::Response::KnownBlock(None));
        }
        let body = peers
            .expect_request(zn::Request::BlocksByHash(IndexSet::from([hash])))
            .await;
        assert!(
            changes.borrow().is_empty(),
            "an unfinished round cannot announce caught-up status"
        );
        for reply in replies {
            reply.respond(zn::Response::BlockHashes(Vec::new()));
        }
        changes.changed().await.unwrap();
        assert_eq!(*changes.borrow(), vec![1]);
        body.respond(zn::Response::Blocks(vec![
            zn::InventoryResponse::Available((block.clone(), None)),
        ]));
        verifier
            .expect_request(zakura_consensus::Request::Commit(block))
            .await
            .respond(hash);
    };
    let (result, ()) = futures::join!(syncing, scenario);
    result.unwrap();
    assert_eq!(*changes.borrow(), vec![1], "one aggregate update per round");
}

#[tokio::test(start_paused = true)]
async fn validated_batches_take_priority_over_ready_optional_hints() {
    let (mut sync, _, _, mut peers, mut state, _) = setup_chain_sync();
    let hint = block::Hash([8; 32]);
    let required = block::Hash([9; 32]);
    let round = futures::stream::iter(vec![
        Ok(Event::Hint(hint)),
        Ok(Event::Batch(IndexSet::from([required]))),
        Ok(Event::Finished(HashSet::new())),
    ])
    .boxed();
    let syncing = sync.sync_round(IndexSet::new(), Some(round), None);
    tokio::pin!(syncing);
    let scenario = async {
        state
            .expect_request(zs::Request::KnownBlock(required))
            .await
            .respond(zs::Response::KnownBlock(None));
        let required_request = peers
            .expect_request(zn::Request::BlocksByHash(IndexSet::from([required])))
            .await;
        state
            .expect_request(zs::Request::KnownBlock(hint))
            .await
            .respond(zs::Response::KnownBlock(None));
        let hint_request = peers
            .expect_request(zn::Request::BlocksByHash(IndexSet::from([hint])))
            .await;
        required_request.respond(Err(BoxError::from("test cancelled")));
        hint_request.respond(Err(BoxError::from("test cancelled")));
    };
    tokio::select! {
        result = &mut syncing => panic!("required work should still be pending: {result:?}"),
        () = scenario => {},
    }
}

#[tokio::test(start_paused = true)]
async fn extension_releases_a_batch_before_other_peers_finish_and_cancels_on_drop() {
    let (_sync, _, _, mut peers, mut state, _) = setup_chain_sync();
    let tip = CheckedTip {
        tip: block::Hash([1; 32]),
        expected_next: block::Hash([2; 32]),
    };
    let child = block::Hash([3; 32]);
    let start = tokio::time::Instant::now();
    let round = TestChainSync::build_extend(
        Timeout::new(peers.clone(), TIPS_RESPONSE_TIMEOUT),
        state.clone(),
        HashSet::from([tip]),
    );
    let next = tokio::spawn(round.into_future());
    let mut replies = Vec::new();
    for _ in 0..FANOUT {
        replies.push(
            peers
                .expect_request(zn::Request::FindBlocks {
                    known_blocks: vec![tip.tip],
                    stop: None,
                })
                .await,
        );
    }
    replies
        .remove(0)
        .respond(zn::Response::BlockHashes(vec![tip.expected_next, child]));
    state
        .expect_request(zs::Request::KnownBlock(child))
        .await
        .respond(zs::Response::KnownBlock(None));
    let (event, round) = next.await.unwrap();
    assert!(
        matches!(event.unwrap().unwrap(), Event::Batch(hashes) if hashes == IndexSet::from([child]))
    );
    assert!(start.elapsed() < TIPS_RESPONSE_TIMEOUT);
    drop(round);
    // Releasing cancelled responses must not leave a detached discovery task to process them.
    for reply in replies {
        reply.respond(zn::Response::BlockHashes(Vec::new()));
    }
    state.expect_no_requests().await;
    peers.expect_no_requests().await;
}

#[tokio::test(start_paused = true)]
async fn later_overlapping_reply_does_not_recheck_blocks_accepted_in_this_round() {
    let (_sync, _, _, mut peers, mut state, _) = setup_chain_sync();
    let tip = CheckedTip {
        tip: block::Hash([1; 32]),
        expected_next: block::Hash([2; 32]),
    };
    let child = block::Hash([3; 32]);
    let round = TestChainSync::build_extend(
        Timeout::new(peers.clone(), TIPS_RESPONSE_TIMEOUT),
        state.clone(),
        HashSet::from([tip]),
    );
    let next = tokio::spawn(round.into_future());
    let mut replies = Vec::new();
    for _ in 0..FANOUT {
        replies.push(
            peers
                .expect_request(zn::Request::FindBlocks {
                    known_blocks: vec![tip.tip],
                    stop: None,
                })
                .await,
        );
    }
    replies
        .remove(0)
        .respond(zn::Response::BlockHashes(vec![tip.expected_next, child]));
    state
        .expect_request(zs::Request::KnownBlock(child))
        .await
        .respond(zs::Response::KnownBlock(None));
    let (_, round) = next.await.unwrap();
    let next = tokio::spawn(round.into_future());
    // The block may have committed now. Re-querying state would make the valid suffix look bad.
    replies
        .remove(0)
        .respond(zn::Response::BlockHashes(vec![tip.expected_next, child]));
    replies
        .remove(0)
        .respond(zn::Response::BlockHashes(Vec::new()));
    assert!(matches!(
        next.await.unwrap().0.unwrap().unwrap(),
        Event::Finished(_)
    ));
    state.expect_no_requests().await;
}

#[tokio::test(start_paused = true)]
async fn unmatched_singleton_is_a_hint_and_does_not_create_a_tip() {
    let (_sync, _, _, mut peers, state, _) = setup_chain_sync();
    let tip = CheckedTip {
        tip: block::Hash([1; 32]),
        expected_next: block::Hash([2; 32]),
    };
    let hint = block::Hash([9; 32]);
    let round = TestChainSync::build_extend(
        Timeout::new(peers.clone(), TIPS_RESPONSE_TIMEOUT),
        state,
        HashSet::from([tip]),
    );
    let next = tokio::spawn(round.into_future());
    let mut replies = Vec::new();
    for _ in 0..FANOUT {
        replies.push(
            peers
                .expect_request(zn::Request::FindBlocks {
                    known_blocks: vec![tip.tip],
                    stop: None,
                })
                .await,
        );
    }
    replies
        .remove(0)
        .respond(zn::Response::BlockHashes(vec![hint]));
    let (event, round) = next.await.unwrap();
    assert!(matches!(event.unwrap().unwrap(), Event::Hint(hash) if hash == hint));
    let next = tokio::spawn(round.into_future());
    for reply in replies {
        reply.respond(zn::Response::BlockHashes(Vec::new()));
    }
    assert!(
        matches!(next.await.unwrap().0.unwrap().unwrap(), Event::Finished(tips) if tips.is_empty())
    );
}

#[tokio::test(start_paused = true)]
async fn optional_failures_release_slots_without_required_block_retries() {
    let (mut sync, _, _, mut peers, _, _) = setup_chain_sync();
    let hash = block::Hash([7; 32]);
    sync.downloads.download_hint(hash).await.unwrap();
    assert_eq!(sync.downloads.hint_count(), 1);
    peers
        .expect_request(zn::Request::BlocksByHash(IndexSet::from([hash])))
        .await
        .respond(Err(BoxError::from(zn::SharedPeerError::from(
            zn::PeerError::NotFoundResponse(Vec::new()),
        ))));
    let result = sync.downloads.next().await.unwrap();
    sync.handle_block_response_with_missing_retry(result)
        .await
        .unwrap();
    assert_eq!(sync.downloads.hint_count(), 0);
    assert!(sync.missing_block_retry_counts.is_empty());
    assert!(sync.registry_miss_retry.is_empty());
    peers.expect_no_requests().await;
}

#[tokio::test(start_paused = true)]
async fn hint_limit_deduplication_and_promotion_share_download_ownership() {
    let (mut sync, _, _, mut peers, _, _) = setup_chain_sync();
    let mut held = Vec::new();
    for byte in 1..=u8::try_from(FANOUT).unwrap() {
        let hash = block::Hash([byte; 32]);
        sync.downloads.download_hint(hash).await.unwrap();
        held.push(
            peers
                .expect_request(zn::Request::BlocksByHash(IndexSet::from([hash])))
                .await,
        );
    }
    sync.downloads
        .download_hint(block::Hash([99; 32]))
        .await
        .unwrap();
    sync.downloads
        .download_hint(block::Hash([1; 32]))
        .await
        .unwrap();
    assert_eq!(sync.downloads.hint_count(), FANOUT);
    let promoted = block::Hash([1; 32]);
    assert!(sync.downloads.download_and_verify(promoted).await.is_err());
    assert_eq!(sync.downloads.hint_count(), FANOUT - 1);
    held.remove(0)
        .respond(Err(BoxError::from("download failed after promotion")));
    let result = sync.downloads.next().await.unwrap();
    assert!(matches!(
        result,
        Err(sync::downloads::BlockDownloadVerifyError::DownloadFailed { .. })
    ));
    peers.expect_no_requests().await;
    sync.downloads.cancel_all();
    assert_eq!(sync.downloads.hint_count(), 0);
    drop(held);
}

/// The connection really serializes `inv` and `getdata` over an in-memory transport. Two other
/// discovery requests never reply, so an early block request cannot come from round completion.
#[tokio::test(start_paused = true)]
async fn singleton_wire_reply_starts_sync_download_without_inbound_announcement() {
    let _guard = zakura_test::init();
    let network = Network::Mainnet;
    let (left, right) = tokio::io::duplex(64 * 1024);
    let mut inbound: MockService<zn::Request, zn::Response, PanicAssertion> =
        MockService::build().for_unit_tests();
    let mut peer: MockService<zn::Request, zn::Response, PanicAssertion> =
        MockService::build().for_unit_tests();
    let (client, server) = tokio::join!(
        zn::connect_isolated_with_inbound(&network, left, String::new(), inbound.clone()),
        zn::connect_isolated_with_inbound(&network, right, String::new(), peer.clone()),
    );
    let client = tower::util::BoxCloneSyncService::new(tower::buffer::Buffer::new(
        client
            .unwrap()
            .map_err(|error| -> BoxError { Box::new(error) })
            .boxed(),
        4,
    ));
    let _server = server.unwrap();
    let requests = Arc::new(AtomicUsize::new(0));
    let routing = requests.clone();
    let peers =
        tower::util::BoxCloneSyncService::new(tower::service_fn(move |request: zn::Request| {
            let client = client.clone();
            let withheld = matches!(request, zn::Request::FindBlocks { .. })
                && routing.fetch_add(1, Ordering::SeqCst) > 0;
            async move {
                if withheld {
                    futures::future::pending().await
                } else {
                    client.oneshot(request).await
                }
            }
            .boxed()
        }));
    let mut state: MockService<zs::Request, zs::Response, PanicAssertion> =
        MockService::build().for_unit_tests();
    let verifier: MockService<zakura_consensus::Request, block::Hash, PanicAssertion> =
        MockService::build().for_unit_tests();
    let (tip, _sender) = zakura_chain::chain_tip::mock::MockChainTip::new();
    let (misbehavior, _receiver) = tokio::sync::mpsc::channel(1);
    let (mut sync, _) = sync::ChainSync::new(
        &crate::config::ZakuradConfig::default(),
        Height(0),
        peers,
        verifier.clone(),
        state.clone(),
        tip,
        misbehavior,
    );
    let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let hash = block.hash();
    let locator = block.header.previous_block_hash;
    let start = tokio::time::Instant::now();
    let syncing = sync.try_to_sync(None);
    tokio::pin!(syncing);
    let scenario = async {
        state
            .expect_request(zs::Request::BlockLocator)
            .await
            .respond(zs::Response::BlockLocator(vec![locator]));
        peer.expect_request(zn::Request::FindBlocks {
            known_blocks: vec![locator],
            stop: None,
        })
        .await
        .respond(zn::Response::BlockHashes(vec![hash]));
        for _ in 0..2 {
            state
                .expect_request(zs::Request::KnownBlock(hash))
                .await
                .respond(zs::Response::KnownBlock(None));
        }
        let body_request = peer
            .expect_request(zn::Request::BlocksByHash(IndexSet::from([hash])))
            .await;
        assert!(start.elapsed() < TIPS_RESPONSE_TIMEOUT);
        assert_eq!(requests.load(Ordering::SeqCst), FANOUT);
        inbound.expect_no_requests().await;
        // Only the request is needed to prove early dispatch. Block decoding uses
        // block_in_place and is covered separately on multi-threaded runtimes.
        body_request.respond(zn::Response::Blocks(vec![zn::InventoryResponse::Missing(
            hash,
        )]));
    };
    tokio::select! {
        result = &mut syncing => panic!("sync must still be waiting for the other replies: {result:?}"),
        () = scenario => {},
    }
}
