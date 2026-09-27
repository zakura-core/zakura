//! Early tip downloads and their discovery, capacity, and gossip boundaries.
use super::*;
use crate::components::inbound::downloads::{DownloadAction, Downloads as GossipDownloads};

/// With a usable result in hand, obtain_tips still waits for every query before dispatching.
async fn gated_discovery_waits(
    bootstrap: bool,
    checkpoint: Height,
    known_tip: Option<Height>,
    trailing_hash: bool,
    existing_download: bool,
) -> Result<(), crate::BoxError> {
    let (mut sync, _, mut verifier, mut peers, mut state, tip) =
        setup_chain_sync_with_options(checkpoint, MAX_SERVICE_REQUEST_DELAY);
    if let Some(height) = known_tip {
        tip.send_best_tip_height(height);
    }
    let held_download = if existing_download {
        let hash = block::Hash([4; 32]);
        sync.downloads.download_and_verify(hash).await?;
        Some(
            peers
                .expect_request(zn::Request::BlocksByHash(iter::once(hash).collect()))
                .await,
        )
    } else {
        None
    };
    let locator = block::Hash([1; 32]);
    let hash = block::Hash([2; 32]);
    let responses = async {
        state
            .expect_request(zs::Request::BlockLocator)
            .await
            .respond(zs::Response::BlockLocator(vec![locator]));
        let mut held = Vec::new();
        for _ in 0..sync::FANOUT {
            held.push(
                peers
                    .expect_request(zn::Request::FindBlocks {
                        known_blocks: vec![locator],
                        stop: None,
                    })
                    .await,
            );
        }
        let wire_hashes = if trailing_hash {
            vec![hash, block::Hash([3; 32])]
        } else {
            vec![hash]
        };
        held.remove(0)
            .respond(zn::Response::BlockHashes(wire_hashes));
        state
            .expect_request(zs::Request::KnownBlock(hash))
            .await
            .respond(zs::Response::KnownBlock(None));
        // The mock's one-second observation window ends well before the six-second query timeout.
        peers.expect_no_requests().await;
        verifier.expect_no_requests().await;
        for request in held {
            request.respond(Err(zn::BoxError::from(
                "finish outstanding discovery query",
            )));
        }
        state
            .expect_request(zs::Request::KnownBlock(hash))
            .await
            .respond(zs::Response::KnownBlock(None));
        peers
            .expect_request(zn::Request::BlocksByHash(iter::once(hash).collect()))
            .await
    };
    let (result, body_request) = futures::join!(sync.obtain_tips(bootstrap), responses);
    assert!(result
        .map_err(|error| -> crate::BoxError { error.into() })?
        .is_empty());
    assert_eq!(
        sync.downloads.in_flight(),
        1 + usize::from(existing_download)
    );
    body_request.respond(Err(zn::BoxError::from("end test")));
    if let Some(request) = held_download {
        request.respond(Err(zn::BoxError::from("end test")));
    }
    while let Some(result) = sync.downloads.next().await {
        assert!(result.is_err());
    }
    Ok(())
}

/// A later gossip download can finish while an earlier sync body request is still unresolved.
/// A global skip-if-downloading flag would remove this existing route to progress.
#[tokio::test]
async fn early_discovery_later_gossip_can_finish_before_sync() -> Result<(), crate::BoxError> {
    let (mut sync, _, mut verifier, mut peers, mut state, _) = setup_chain_sync();
    let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES.zcash_deserialize_into()?;
    let hash = block.hash();
    let (_sender, tip, _change) = zs::ChainTipSender::new(None, &Network::Mainnet);
    let mut gossip = GossipDownloads::new(
        10,
        false,
        peers.clone(),
        verifier.clone(),
        state.clone(),
        tip,
        Network::Mainnet,
    );
    sync.downloads.download_and_verify(hash).await?;
    let slow_sync_request = peers
        .expect_request(zn::Request::BlocksByHash(iter::once(hash).collect()))
        .await;

    let source = zn::PeerSource::LegacySocket(([127, 0, 0, 2], 8233).into());
    assert_eq!(
        gossip.download_and_verify(hash, Some(source.clone())),
        DownloadAction::AddedToQueue
    );
    state
        .expect_request(zs::Request::AnyChainBlock(hash.into()))
        .await
        .respond(zs::Response::Block(None));
    peers
        .expect_request(zn::Request::BlocksByHashFrom {
            hashes: iter::once(hash).collect(),
            source,
        })
        .await
        .respond(zn::Response::Blocks(vec![Available((block.clone(), None))]));
    verifier
        .expect_request(zakura_consensus::Request::Commit(block))
        .await
        .respond(hash);
    let completed = tokio::time::timeout(Duration::from_secs(2), gossip.next())
        .await?
        .unwrap()
        .map_err(|(error, _)| error)?;
    assert_eq!(completed, hash);
    assert_eq!(sync.downloads.in_flight(), 1);
    assert!(futures::poll!(sync.downloads.next()).is_pending());
    slow_sync_request.respond(Err(zn::BoxError::from("end test")));
    assert!(sync.downloads.next().await.unwrap().is_err());
    Ok(())
}

/// Exercise the real discovery loop with a completed early block, then a duplicate reply.
#[tokio::test(start_paused = true)]
async fn early_discovery_commits_before_fanout_and_deduplicates() -> Result<(), crate::BoxError> {
    let (mut sync, _, mut verifier, mut peers, mut state, tip) = setup_chain_sync();
    tip.send_best_tip_height(Height(0));
    let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES.zcash_deserialize_into()?;
    let hash = block.hash();
    let locator = block.header.previous_block_hash;
    let started = tokio::time::Instant::now();
    let responses = async {
        state
            .expect_request(zs::Request::BlockLocator)
            .await
            .respond(zs::Response::BlockLocator(vec![locator]));
        let mut held = Vec::new();
        for _ in 0..sync::FANOUT {
            held.push(
                peers
                    .expect_request(zn::Request::FindBlocks {
                        known_blocks: vec![locator],
                        stop: None,
                    })
                    .await,
            );
        }
        held.remove(0)
            .respond(zn::Response::BlockHashes(vec![hash]));
        state
            .expect_request(zs::Request::KnownBlock(hash))
            .await
            .respond(zs::Response::KnownBlock(None));
        let body = peers
            .expect_request(zn::Request::BlocksByHash(iter::once(hash).collect()))
            .await;
        let dispatch = started.elapsed();
        tokio::time::sleep(Duration::from_millis(280)).await;
        body.respond(zn::Response::Blocks(vec![Available((block.clone(), None))]));
        let verify = verifier
            .expect_request(zakura_consensus::Request::Commit(block))
            .await;
        tokio::time::sleep(Duration::from_millis(16)).await;
        verify.respond(hash);
        tokio::task::yield_now().await;
        let completion = started.elapsed();
        // The state service now reports the early block as committed. Later duplicate
        // discovery replies must not cause a second download or abort the round.
        tip.send_best_tip_height(Height(1));
        held.remove(0)
            .respond(zn::Response::BlockHashes(vec![hash]));
        state
            .expect_request(zs::Request::KnownBlock(hash))
            .await
            .respond(zs::Response::KnownBlock(Some(zs::KnownBlock::BestChain)));
        peers.expect_no_requests().await;
        state.expect_no_requests().await;
        for request in held {
            request.respond(Err(zn::BoxError::from("finish discovery")));
        }
        (dispatch, completion)
    };
    let (result, (dispatch, completion)) = futures::join!(sync.obtain_tips(false), responses);
    assert!(result
        .map_err(|e| -> crate::BoxError { e.into() })?
        .is_empty());
    assert_eq!(
        sync.downloads.next().await.unwrap().unwrap(),
        (Height(1), hash)
    );
    assert_eq!(sync.downloads.in_flight(), 0);
    assert!(dispatch < Duration::from_millis(10));
    assert!(completion < Duration::from_millis(310));
    Ok(())
}

/// A later batch must reserve space for the early task, even before it is drained.
#[tokio::test(start_paused = true)]
async fn early_discovery_reserves_aggregate_capacity() -> Result<(), crate::BoxError> {
    let (mut sync, _, _verifier, mut peers, mut state, tip) = setup_chain_sync();
    tip.send_best_tip_height(Height(0));
    sync.full_verify_concurrency_limit = 2;
    let locator = block::Hash([0; 32]);
    let hashes: Vec<_> = (1..=4).map(|n| block::Hash([n; 32])).collect();
    let responses = async {
        state
            .expect_request(zs::Request::BlockLocator)
            .await
            .respond(zs::Response::BlockLocator(vec![locator]));
        let mut held = Vec::new();
        for _ in 0..sync::FANOUT {
            held.push(
                peers
                    .expect_request(zn::Request::FindBlocks {
                        known_blocks: vec![locator],
                        stop: None,
                    })
                    .await,
            );
        }
        held.remove(0)
            .respond(zn::Response::BlockHashes(vec![hashes[0]]));
        state
            .expect_request(zs::Request::KnownBlock(hashes[0]))
            .await
            .respond(zs::Response::KnownBlock(None));
        let first = peers
            .expect_request(zn::Request::BlocksByHash(iter::once(hashes[0]).collect()))
            .await;
        // Repeated first hash, two additional hashes, and zcashd's discarded tail.
        held.remove(0)
            .respond(zn::Response::BlockHashes(hashes.clone()));
        for hash in &hashes[..3] {
            state
                .expect_request(zs::Request::KnownBlock(*hash))
                .await
                .respond(zs::Response::KnownBlock(None));
        }
        held.remove(0)
            .respond(Err(zn::BoxError::from("finish discovery")));
        for hash in &hashes[1..3] {
            state
                .expect_request(zs::Request::KnownBlock(*hash))
                .await
                .respond(zs::Response::KnownBlock(None));
        }
        let second = peers
            .expect_request(zn::Request::BlocksByHash(iter::once(hashes[1]).collect()))
            .await;
        (first, second)
    };
    let (result, (first, second)) = futures::join!(sync.obtain_tips(false), responses);
    assert_eq!(
        result
            .map_err(|e| -> crate::BoxError { e.into() })?
            .into_iter()
            .collect::<Vec<_>>(),
        vec![hashes[2]]
    );
    assert_eq!(sync.downloads.in_flight(), 2);
    peers.expect_no_requests().await;
    first.respond(Err(zn::BoxError::from("end test")));
    second.respond(Err(zn::BoxError::from("end test")));
    while let Some(result) = sync.downloads.next().await {
        assert!(result.is_err());
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn early_discovery_unknown_tip_stays_batched() -> Result<(), crate::BoxError> {
    gated_discovery_waits(false, Height(0), None, false, false).await
}

#[tokio::test(start_paused = true)]
async fn early_discovery_checkpoint_paths_stay_batched() -> Result<(), crate::BoxError> {
    gated_discovery_waits(false, Height(10), Some(Height(0)), false, false).await?;
    gated_discovery_waits(true, Height(10), Some(Height(0)), false, false).await
}

/// Trimming a compatibility tail does not make a wire batch an advertisement.
#[tokio::test(start_paused = true)]
async fn early_discovery_requires_singleton_wire_response() -> Result<(), crate::BoxError> {
    gated_discovery_waits(false, Height(0), Some(Height(0)), true, false).await
}

/// Early body failures remain queued until discovery gives control back to sync.
#[tokio::test(start_paused = true)]
async fn early_discovery_failure_retry_waits_for_discovery() -> Result<(), crate::BoxError> {
    let (mut sync, _, mut verifier, mut peers, mut state, tip) = setup_chain_sync();
    tip.send_best_tip_height(Height(0));
    let hash = block::Hash([2; 32]);
    let locator = block::Hash([1; 32]);
    let started = tokio::time::Instant::now();
    let responses = async {
        state
            .expect_request(zs::Request::BlockLocator)
            .await
            .respond(zs::Response::BlockLocator(vec![locator]));
        let mut held = Vec::new();
        for _ in 0..sync::FANOUT {
            held.push(
                peers
                    .expect_request(zn::Request::FindBlocks {
                        known_blocks: vec![locator],
                        stop: None,
                    })
                    .await,
            );
        }
        held.remove(0)
            .respond(zn::Response::BlockHashes(vec![hash]));
        state
            .expect_request(zs::Request::KnownBlock(hash))
            .await
            .respond(zs::Response::KnownBlock(None));
        peers
            .expect_request(zn::Request::BlocksByHash(iter::once(hash).collect()))
            .await
            .respond(Err(not_found_block_error(hash)));
        peers.expect_no_requests().await;
        verifier.expect_no_requests().await;
        for request in held {
            request.respond(Err(zn::BoxError::from("finish discovery")));
        }
    };
    let (result, ()) = futures::join!(sync.obtain_tips(false), responses);
    assert!(result
        .map_err(|e| -> crate::BoxError { e.into() })?
        .is_empty());
    let failure = sync.downloads.next().await.unwrap();
    assert!(failure.is_err());
    assert!(started.elapsed() >= Duration::from_secs(2));
    sync.handle_block_response_with_missing_retry(failure)
        .await?;
    let retry = peers
        .expect_request(zn::Request::BlocksByHash(iter::once(hash).collect()))
        .await;
    assert_eq!(sync.downloads.in_flight(), 1);
    sync.downloads.cancel_all();
    assert_eq!(sync.downloads.in_flight(), 0);
    retry.respond(Err(zn::BoxError::from("end cancelled retry")));
    verifier.expect_no_requests().await;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn early_discovery_existing_downloads_stay_batched() -> Result<(), crate::BoxError> {
    gated_discovery_waits(false, Height(0), Some(Height(0)), false, true).await
}
