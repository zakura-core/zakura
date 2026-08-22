//! StateService test vectors.

#![allow(clippy::unwrap_in_result)]

// TODO: move these tests into tests::vectors and tests::prop modules.

use std::{env, sync::Arc, time::Duration};

use tokio::{runtime::Runtime, time::timeout};
use tower::{buffer::Buffer, util::BoxService};

use zakura_chain::{
    block::{self, Block, CountedHeader, Height},
    chain_tip::ChainTip,
    fmt::SummaryDebug,
    parameters::{Network, NetworkUpgrade},
    serialization::{ZcashDeserialize, ZcashDeserializeInto},
    transaction, transparent,
    value_balance::ValueBalance,
};

use zakura_test::{prelude::*, transcript::Transcript};

use crate::{
    arbitrary::Prepare,
    init_test,
    service::{
        arbitrary::populated_state,
        chain_tip::TipAction,
        finalized_state::{DiskWriteBatch, FinalizedState, FrontierArtifact, FrontierEntry},
        StateService,
    },
    tests::setup::{partial_nu5_chain_strategy, transaction_v4_from_coinbase},
    BoxError, CheckpointVerifiedBlock, Config, HistoricalTreeUnavailable, PruningConfig, Request,
    Response, SemanticallyVerifiedBlock, StateInitError, StorageMode, CHAIN_TIP_UPDATE_WAIT_LIMIT,
    MAX_HISTORICAL_TREE_REPLAY_BLOCKS,
};

const LAST_BLOCK_HEIGHT: u32 = 10;

#[tokio::test]
async fn historical_frontier_load_errors_are_returned_from_state_init() {
    let network = Network::Mainnet;
    let temp_dir = tempfile::tempdir().expect("temporary directory is created");
    let missing_path = temp_dir.path().join("missing.bin");
    let missing_config = Config {
        historical_frontier_artifact: Some(missing_path.clone()),
        ..Config::ephemeral()
    };

    assert!(matches!(
        super::init(missing_config, &network, Height::MAX, 0).await,
        Err(StateInitError::HistoricalFrontierArtifact { path, .. }) if path == missing_path
    ));

    let corrupt_path = temp_dir.path().join("corrupt.bin");
    std::fs::write(&corrupt_path, b"not a frontier artifact")
        .expect("corrupt test artifact is written");
    let corrupt_config = Config {
        historical_frontier_artifact: Some(corrupt_path.clone()),
        ..Config::ephemeral()
    };

    assert!(matches!(
        super::init(corrupt_config.clone(), &network, Height::MAX, 0).await,
        Err(StateInitError::HistoricalFrontierArtifact { path, .. }) if path == corrupt_path
    ));

    // A node that does not derive never reads the file, so the same broken path is a warning
    // rather than a refusal to start.
    let legacy_recompute = Config {
        vct_fast_sync: false,
        ..corrupt_config
    };
    assert!(
        super::load_historical_frontier_artifact(&network, &legacy_recompute, false).is_ok(),
        "an unusable grid must not stop a node that would never have read it"
    );
}

#[test]
fn durable_vct_marker_keeps_historical_derivation_enabled_after_config_change() {
    let network = Network::Mainnet;
    let initial_config = Config::ephemeral();
    let finalized_state =
        FinalizedState::new(&initial_config, &network).expect("ephemeral finalized state opens");
    let artifact_checkpoint = Height(11);
    let artifact_file =
        tempfile::NamedTempFile::new().expect("temporary frontier artifact is created");
    let artifact = FrontierArtifact {
        spacing: 1,
        last_checkpoint: artifact_checkpoint,
        entries: vec![FrontierEntry {
            height: Height(0),
            sapling: Arc::new(Default::default()),
            orchard: Arc::new(Default::default()),
            ironwood: Arc::new(Default::default()),
        }],
    };
    std::fs::write(artifact_file.path(), artifact.encode(&network))
        .expect("historical frontier artifact is written");

    let mut batch = DiskWriteBatch::new();
    batch.update_vct_sync_marker(&finalized_state.db, Height(10));
    finalized_state
        .db
        .write_batch(batch)
        .expect("VCT handoff marker is written");

    let reopened_config = Config {
        checkpoint_sync: false,
        vct_fast_sync: false,
        historical_frontier_artifact: Some(artifact_file.path().to_path_buf()),
        ..initial_config
    };
    assert!(
        !reopened_config.derive_historical_trees(false),
        "the current configuration does not start a VCT fast sync"
    );

    let loaded = super::load_historical_frontier_artifact(
        &network,
        &reopened_config,
        finalized_state.db.vct_synced_below().is_some(),
    )
    .expect("the durable marker loads the configured grid after sync settings change");
    assert_eq!(
        loaded.last_checkpoint,
        Some(artifact_checkpoint),
        "the reopened archive keeps the grid needed to serve its absent band"
    );
}

#[test]
fn historical_frontier_artifact_older_than_database_vct_handoff_is_ignored() {
    let network = Network::Mainnet;
    let temp_dir = tempfile::tempdir().expect("temporary directory is created");
    let state_config = Config::ephemeral();
    let finalized_state =
        FinalizedState::new(&state_config, &network).expect("ephemeral finalized state opens");
    let vct_handoff = Height(10);
    let mut batch = DiskWriteBatch::new();
    batch.update_vct_sync_marker(&finalized_state.db, vct_handoff);
    finalized_state
        .db
        .write_batch(batch)
        .expect("VCT handoff marker is written");

    let artifact_path = |checkpoint: Height| {
        let path = temp_dir
            .path()
            .join(format!("frontiers-{}.bin", checkpoint.0));
        let artifact = FrontierArtifact {
            spacing: 1,
            last_checkpoint: checkpoint,
            entries: vec![FrontierEntry {
                height: checkpoint,
                sapling: Arc::new(Default::default()),
                orchard: Arc::new(Default::default()),
                ironwood: Arc::new(Default::default()),
            }],
        };
        std::fs::write(&path, artifact.encode(&network))
            .expect("historical frontier artifact is written");
        path
    };

    let stale_path = artifact_path(Height(9));
    let stale_config = Config {
        historical_frontier_artifact: Some(stale_path.clone()),
        ..state_config.clone()
    };
    let stale_cache = super::load_historical_frontier_artifact(&network, &stale_config, false)
        .expect("the stale artifact decodes")
        .discard_if_before_vct_handoff(&stale_config, &finalized_state.db);
    assert_eq!(
        stale_cache
            .lock()
            .expect("historical tree cache lock is available")
            .last_checkpoint(),
        None,
        "a stale artifact is unavailable rather than preventing startup"
    );

    for checkpoint in [vct_handoff, Height(11)] {
        let config = Config {
            historical_frontier_artifact: Some(artifact_path(checkpoint)),
            ..state_config.clone()
        };
        let cache = super::load_historical_frontier_artifact(&network, &config, false)
            .expect("the covering artifact decodes")
            .discard_if_before_vct_handoff(&config, &finalized_state.db);
        assert_eq!(
            cache
                .lock()
                .expect("historical tree cache lock is available")
                .last_checkpoint(),
            Some(checkpoint),
            "an artifact at or above the VCT handoff must cover the absent band"
        );
    }
}

#[test]
fn historical_frontier_artifact_must_tile_the_band_within_the_replay_limit() {
    let network = Network::Mainnet;
    let temp_dir = tempfile::tempdir().expect("temporary directory is created");

    let write = |name: &str, last_checkpoint: Height, entries: Vec<Height>| {
        let path = temp_dir.path().join(name);
        let artifact = FrontierArtifact {
            spacing: 1,
            last_checkpoint,
            entries: entries
                .into_iter()
                .map(|height| FrontierEntry {
                    height,
                    sapling: Arc::new(Default::default()),
                    orchard: Arc::new(Default::default()),
                    ironwood: Arc::new(Default::default()),
                })
                .collect(),
        };
        std::fs::write(&path, artifact.encode(&network)).expect("artifact writes");
        path
    };

    let sparse_path = write("sparse.bin", Height(200_000), vec![Height(0)]);
    let sparse = Config {
        historical_frontier_artifact: Some(sparse_path.clone()),
        ..Config::ephemeral()
    };
    assert!(matches!(
        super::load_historical_frontier_artifact(&network, &sparse, false),
        Err(StateInitError::HistoricalFrontierArtifactTooSparse {
            path,
            blocks: 199_999,
            limit: MAX_HISTORICAL_TREE_REPLAY_BLOCKS,
        }) if path == sparse_path
    ));

    let empty_path = write("empty.bin", Height(200_000), vec![]);
    let empty = Config {
        historical_frontier_artifact: Some(empty_path.clone()),
        ..Config::ephemeral()
    };
    assert!(matches!(
        super::load_historical_frontier_artifact(&network, &empty, false),
        Err(StateInitError::HistoricalFrontierArtifactTooSparse {
            path,
            blocks: 200_000,
            limit: MAX_HISTORICAL_TREE_REPLAY_BLOCKS,
        }) if path == empty_path
    ));

    let covering = Config {
        historical_frontier_artifact: Some(write(
            "covering.bin",
            Height(10),
            vec![Height(0), Height(10)],
        )),
        ..Config::ephemeral()
    };
    assert!(
        super::load_historical_frontier_artifact(&network, &covering, false).is_ok(),
        "a grid whose gaps fit the serving replay limit must load"
    );

    let mid_chain_path = write("mid-chain.bin", Height(2_000_100), vec![Height(2_000_000)]);
    let mid_chain = Config {
        historical_frontier_artifact: Some(mid_chain_path.clone()),
        ..Config::ephemeral()
    };
    assert!(matches!(
        super::load_historical_frontier_artifact(&network, &mid_chain, false),
        Err(StateInitError::HistoricalFrontierArtifactTooSparse {
            path,
            blocks: 2_000_000,
            limit: MAX_HISTORICAL_TREE_REPLAY_BLOCKS,
        }) if path == mid_chain_path
    ));

    let unused_sparse = Config {
        historical_frontier_artifact: Some(sparse_path),
        storage_mode: StorageMode::Pruned(PruningConfig::default()),
        ..Config::ephemeral()
    };
    assert!(
        super::load_historical_frontier_artifact(&network, &unused_sparse, false).is_ok(),
        "a sparse grid is ignored when derivation is off"
    );
}

#[test]
fn frontier_grid_coverage_is_incomparable_until_both_sides_exist() {
    assert_eq!(
        super::frontier_grid_ends_before_vct_handoff(None, None),
        None,
        "neither side is comparable"
    );
    assert_eq!(
        super::frontier_grid_ends_before_vct_handoff(Some(Height(9)), None),
        None,
        "an unmarked database cannot fail coverage"
    );
    assert_eq!(
        super::frontier_grid_ends_before_vct_handoff(None, Some(Height(10))),
        None,
        "an unloaded grid cannot fail coverage"
    );
    assert_eq!(
        super::frontier_grid_ends_before_vct_handoff(Some(Height(9)), Some(Height(10))),
        Some((Height(9), Height(10))),
        "a grid that ends below the handoff is uncovered"
    );
    assert_eq!(
        super::frontier_grid_ends_before_vct_handoff(Some(Height(10)), Some(Height(10))),
        None,
        "a grid that ends on the handoff covers the band"
    );
    assert_eq!(
        super::frontier_grid_ends_before_vct_handoff(Some(Height(11)), Some(Height(10))),
        None,
        "a newer grid may cover an older handoff"
    );
}

#[test]
fn historical_frontier_coverage_is_rechecked_once_the_vct_marker_exists() {
    use std::sync::OnceLock;

    use zakura_node_services::sync_lifecycle::{
        HeaderRuntimeDetachedReason, HeaderRuntimeStatus, LifecycleEpoch,
    };

    use crate::service::{
        non_finalized_state::NonFinalizedState, watch_receiver::WatchReceiver,
        HeaderChainSubscriptions, ReadStateService, VctRootRepairStatus,
    };

    let network = Network::Mainnet;
    let temp_dir = tempfile::tempdir().expect("temporary directory is created");
    let artifact_path = temp_dir.path().join("frontiers.bin");
    let artifact = FrontierArtifact {
        spacing: 1,
        last_checkpoint: Height(9),
        entries: vec![FrontierEntry {
            height: Height(9),
            sapling: Arc::new(Default::default()),
            orchard: Arc::new(Default::default()),
            ironwood: Arc::new(Default::default()),
        }],
    };
    std::fs::write(&artifact_path, artifact.encode(&network))
        .expect("historical frontier artifact is written");

    let config = Config {
        historical_frontier_artifact: Some(artifact_path),
        ..Config::ephemeral()
    };
    let finalized_state =
        FinalizedState::new(&config, &network).expect("ephemeral finalized state opens");
    let historical_trees = super::load_historical_frontier_artifact(&network, &config, false)
        .expect("the historical frontier artifact loads")
        .discard_if_before_vct_handoff(&config, &finalized_state.db);

    let (_non_finalized_sender, non_finalized_receiver) =
        tokio::sync::watch::channel(NonFinalizedState::new(&network));
    let (_repair_sender, repair_receiver) =
        tokio::sync::watch::channel(VctRootRepairStatus::default());
    let (_header_chain_snapshot_sender, header_chain_snapshot_receiver) =
        tokio::sync::watch::channel(None);
    let (_header_chain_view_sender, header_chain_view_receiver) = tokio::sync::watch::channel(None);
    let (_header_runtime_status_sender, header_runtime_status_receiver) =
        tokio::sync::watch::channel(HeaderRuntimeStatus::Detached {
            epoch: LifecycleEpoch::INITIAL,
            reason: HeaderRuntimeDetachedReason::AwaitingSemanticHandoff,
        });
    let (_header_chain_reader_sender, header_chain_reader_receiver) =
        tokio::sync::watch::channel(None);

    let read_state = ReadStateService::new(
        &finalized_state,
        None,
        Arc::new(OnceLock::new()),
        WatchReceiver::new(non_finalized_receiver),
        repair_receiver,
        HeaderChainSubscriptions {
            snapshots: header_chain_snapshot_receiver,
            views: header_chain_view_receiver,
            runtime_status: header_runtime_status_receiver,
            reader: header_chain_reader_receiver,
        },
        historical_trees,
    );

    assert!(
        read_state.has_usable_historical_frontier_grid(),
        "the grid is usable before the durable marker exists"
    );

    let mut batch = DiskWriteBatch::new();
    batch.update_vct_sync_marker(&finalized_state.db, Height(10));
    finalized_state
        .db
        .write_batch(batch)
        .expect("a newer VCT handoff marker is written");
    let unavailable = HistoricalTreeUnavailable {
        hash_or_height: Height(9).into(),
        last_checkpoint: Height(10),
    };
    let error = super::historical_frontiers(&read_state, Height(9).into(), unavailable.clone())
        .expect_err("a stale grid must not serve the uncovered band");
    assert_eq!(
        error.downcast_ref::<HistoricalTreeUnavailable>(),
        Some(&unavailable),
        "a stale grid makes historical trees unavailable without surfacing an artifact error"
    );
    assert_eq!(
        read_state
            .historical_trees
            .lock()
            .expect("historical tree cache lock is available")
            .last_checkpoint(),
        None,
        "the stale artifact is discarded after the first serving-time check"
    );
    assert!(!read_state.has_usable_historical_frontier_grid());
}

#[test]
fn block_sync_body_anchor_rolls_back_to_the_selected_fork_intersection() {
    let shared = block::Hash([1; 32]);
    let body_fork = block::Hash([2; 32]);
    let selected_fork = block::Hash([3; 32]);
    let anchor = super::highest_common_body_header_frontier(
        block::Height(2),
        block::Height(0),
        |height| match height.0 {
            1 => Some(shared),
            2 => Some(body_fork),
            _ => None,
        },
        |height| {
            Ok(match height.0 {
                1 => Some(shared),
                2 => Some(selected_fork),
                _ => None,
            })
        },
    )
    .expect("the selected and full-state forks share height one");

    assert_eq!(
        anchor,
        zakura_header_chain::Frontier::new(block::Height(1), shared)
    );
}

async fn test_populated_state_responds_correctly(
    mut state: Buffer<BoxService<Request, Response, BoxError>, Request>,
) -> Result<()> {
    let blocks: Vec<Arc<Block>> = zakura_test::vectors::MAINNET_BLOCKS
        .range(0..=LAST_BLOCK_HEIGHT)
        .map(|(_, block_bytes)| block_bytes.zcash_deserialize_into().unwrap())
        .collect();

    let block_hashes: Vec<block::Hash> = blocks.iter().map(|block| block.hash()).collect();
    let block_headers: Vec<CountedHeader> = blocks
        .iter()
        .map(|block| CountedHeader {
            header: block.header.clone(),
        })
        .collect();

    for (ind, block) in blocks.into_iter().enumerate() {
        let mut transcript = vec![];
        let height = block.coinbase_height().unwrap();
        let hash = block.hash();

        transcript.push((
            Request::Depth(block.hash()),
            Ok(Response::Depth(Some(LAST_BLOCK_HEIGHT - height.0))),
        ));

        // these requests don't have any arguments, so we just do them once
        if ind == LAST_BLOCK_HEIGHT as usize {
            transcript.push((Request::Tip, Ok(Response::Tip(Some((height, hash))))));

            let locator_hashes = vec![
                block_hashes[LAST_BLOCK_HEIGHT as usize],
                block_hashes[(LAST_BLOCK_HEIGHT - 1) as usize],
                block_hashes[(LAST_BLOCK_HEIGHT - 2) as usize],
                block_hashes[(LAST_BLOCK_HEIGHT - 4) as usize],
                block_hashes[(LAST_BLOCK_HEIGHT - 8) as usize],
                block_hashes[0],
            ];

            transcript.push((
                Request::BlockLocator,
                Ok(Response::BlockLocator(locator_hashes)),
            ));
        }

        // Spec: transactions in the genesis block are ignored.
        if height.0 != 0 {
            for transaction in &block.transactions {
                let transaction_hash = transaction.hash();

                transcript.push((
                    Request::Transaction(transaction_hash),
                    Ok(Response::Transaction(Some(transaction.clone()))),
                ));
            }
        }

        transcript.push((
            Request::Block(hash.into()),
            Ok(Response::Block(Some(block.clone()))),
        ));

        transcript.push((
            Request::Block(height.into()),
            Ok(Response::Block(Some(block.clone()))),
        ));

        // Spec: transactions in the genesis block are ignored.
        if height.0 != 0 {
            for transaction in &block.transactions {
                let transaction_hash = transaction.hash();

                let from_coinbase = transaction.is_coinbase();
                for (index, output) in transaction.outputs().iter().cloned().enumerate() {
                    let outpoint = transparent::OutPoint::from_usize(transaction_hash, index);

                    let utxo = transparent::Utxo {
                        output,
                        height,
                        from_coinbase,
                    };

                    transcript.push((Request::AwaitUtxo(outpoint), Ok(Response::Utxo(utxo))));
                }
            }
        }

        let mut append_locator_transcript = |split_ind| {
            let block_hashes = block_hashes.clone();
            let (known_hashes, next_hashes) = block_hashes.split_at(split_ind);

            let block_headers = block_headers.clone();
            let (_, next_headers) = block_headers.split_at(split_ind);

            // no stop
            transcript.push((
                Request::FindBlockHashes {
                    known_blocks: known_hashes.iter().rev().cloned().collect(),
                    stop: None,
                },
                Ok(Response::BlockHashes(next_hashes.to_vec())),
            ));

            transcript.push((
                Request::FindBlockHeaders {
                    known_blocks: known_hashes.iter().rev().cloned().collect(),
                    stop: None,
                },
                Ok(Response::BlockHeaders(next_headers.to_vec())),
            ));

            // stop at the next block
            transcript.push((
                Request::FindBlockHashes {
                    known_blocks: known_hashes.iter().rev().cloned().collect(),
                    stop: next_hashes.first().cloned(),
                },
                Ok(Response::BlockHashes(
                    next_hashes.first().iter().cloned().cloned().collect(),
                )),
            ));

            transcript.push((
                Request::FindBlockHeaders {
                    known_blocks: known_hashes.iter().rev().cloned().collect(),
                    stop: next_hashes.first().cloned(),
                },
                Ok(Response::BlockHeaders(
                    next_headers.first().iter().cloned().cloned().collect(),
                )),
            ));

            // stop at a block that isn't actually in the chain
            // tests bug #2789
            transcript.push((
                Request::FindBlockHashes {
                    known_blocks: known_hashes.iter().rev().cloned().collect(),
                    stop: Some(block::Hash([0xff; 32])),
                },
                Ok(Response::BlockHashes(next_hashes.to_vec())),
            ));

            transcript.push((
                Request::FindBlockHeaders {
                    known_blocks: known_hashes.iter().rev().cloned().collect(),
                    stop: Some(block::Hash([0xff; 32])),
                },
                Ok(Response::BlockHeaders(next_headers.to_vec())),
            ));
        };

        // split before the current block, and locate the current block
        append_locator_transcript(ind);

        // split after the current block, and locate the next block
        append_locator_transcript(ind + 1);

        let transcript = Transcript::from(transcript);
        transcript.check(&mut state).await?;
    }

    Ok(())
}

#[tokio::main]
async fn populate_and_check(blocks: Vec<Arc<Block>>) -> Result<()> {
    let (state, _, _, _) = populated_state(blocks, &Network::Mainnet).await;
    test_populated_state_responds_correctly(state).await?;
    Ok(())
}

fn out_of_order_committing_strategy() -> BoxedStrategy<Vec<Arc<Block>>> {
    let blocks = zakura_test::vectors::MAINNET_BLOCKS
        .range(0..=LAST_BLOCK_HEIGHT)
        .map(|(_, block_bytes)| block_bytes.zcash_deserialize_into::<Arc<Block>>().unwrap())
        .collect::<Vec<_>>();

    Just(blocks).prop_shuffle().boxed()
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_state_still_responds_to_requests() -> Result<()> {
    let _init_guard = zakura_test::init();

    let block =
        zakura_test::vectors::BLOCK_MAINNET_419200_BYTES.zcash_deserialize_into::<Arc<Block>>()?;

    let iter = vec![
        // No checks for SemanticallyVerifiedBlock or CommitCheckpointVerifiedBlock because empty state
        // precondition doesn't matter to them
        (Request::Depth(block.hash()), Ok(Response::Depth(None))),
        (Request::Tip, Ok(Response::Tip(None))),
        (Request::BlockLocator, Ok(Response::BlockLocator(vec![]))),
        (
            Request::Transaction(transaction::Hash([0; 32])),
            Ok(Response::Transaction(None)),
        ),
        (
            Request::Block(block.hash().into()),
            Ok(Response::Block(None)),
        ),
        (
            Request::Block(block.coinbase_height().unwrap().into()),
            Ok(Response::Block(None)),
        ),
        // No check for AwaitUTXO because it will wait if the UTXO isn't present
        (
            Request::FindBlockHashes {
                known_blocks: vec![block.hash()],
                stop: None,
            },
            Ok(Response::BlockHashes(Vec::new())),
        ),
        (
            Request::FindBlockHeaders {
                known_blocks: vec![block.hash()],
                stop: None,
            },
            Ok(Response::BlockHeaders(Vec::new())),
        ),
    ]
    .into_iter();
    let transcript = Transcript::from(iter);

    let network = Network::Mainnet;
    let state = init_test(&network).await;

    transcript.check(state).await?;

    Ok(())
}

/// Regression test for the checkpoint-to-non-finalized sync handoff stall.
///
/// The block write task switches from committing checkpoint verified blocks (finalized state) to
/// committing semantically verified blocks (non-finalized state) once the final checkpoint block is
/// durably written to disk. That handoff used to also require a semantically verified child to be
/// queued, so the pipeline could stall at the checkpoint boundary until the first fully-verified
/// block arrived (or the syncer restarted).
///
/// This test sets the maximum checkpoint height to the last finalized block, commits the checkpoint
/// blocks, and asserts that `poll_ready()` performs the handoff with an **empty** non-finalized
/// queue — i.e. it no longer waits for a semantically verified block to arrive.
///
/// It deliberately does not commit a semantically verified block afterwards: the first two Mainnet
/// blocks predate the Canopy checkpoint, and the non-finalized write path treats their transaction
/// versions as `unreachable!()` (pre-Canopy blocks are only ever checkpoint verified). The handoff
/// itself is what this test covers.
#[tokio::test(flavor = "multi_thread")]
async fn poll_ready_hands_off_at_max_checkpoint_height() -> Result<()> {
    use std::task::{Context, Waker};

    use tower::Service;

    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;

    // Blocks 0 and 1 are committed as checkpoint verified (finalized) blocks.
    let blocks: Vec<Arc<Block>> = zakura_test::vectors::MAINNET_BLOCKS
        .range(0..=1)
        .map(|(_, block_bytes)| block_bytes.zcash_deserialize_into::<Arc<Block>>().unwrap())
        .collect();

    // Set the maximum checkpoint height to block 1, so the checkpoint phase ends once block 1 is
    // committed to the finalized state.
    let max_checkpoint_height = blocks[1].coinbase_height().unwrap();
    let mut config = Config::ephemeral();
    config.enable_zakura_header_seed_from_committed_blocks = true;
    // The state-only fixture commits bodies directly.
    // The fixture has no network-supplied auxiliary root.
    config.vct_fast_sync = false;
    let (mut state_service, read, _tip, _tip_change) =
        StateService::new(config, &network, max_checkpoint_height, 0)
            .await
            .expect("test state initialization succeeds");

    // Commit blocks 0 and 1 to the finalized state and wait for each write to land on disk, so the
    // finalized tip catches up to the maximum checkpoint height and the last block hash we sent.
    for (index, block) in blocks[0..=1].iter().enumerate() {
        let checkpoint = CheckpointVerifiedBlock::from(block.clone());
        let result = state_service
            .queue_and_commit_to_finalized_state(checkpoint)
            .await;
        assert!(
            matches!(result, Ok(Ok(_))),
            "checkpoint verified block should commit: {result:?}",
        );

        let expected_height = block.coinbase_height().expect("test block has a height");
        let expected_hash = block.hash();
        timeout(Duration::from_secs(5), async {
            loop {
                let snapshot = read.subscribe_header_chain_snapshots().borrow().clone();
                if snapshot.as_ref().is_some_and(|snapshot| {
                    snapshot.frontiers.finalized.height == expected_height
                        && snapshot.frontiers.finalized.hash == expected_hash
                        && snapshot.frontiers.verified_best == snapshot.frontiers.finalized
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("checkpoint commits atomically advance the durable header runtime");
        if index == 0 {
            assert!(
                state_service.block_write_sender.finalized.is_some(),
                "the header runtime must become ready after genesis while checkpoint writes remain open",
            );
            assert!(read.subscribe_header_runtime_status().borrow().is_ready());
        }
    }

    let last_finalized_hash = blocks[1].hash();
    assert_eq!(
        state_service.read_service.db.finalized_tip_height(),
        Some(max_checkpoint_height),
        "finalized tip should have reached the maximum checkpoint height",
    );
    assert_eq!(
        state_service.read_service.db.finalized_tip_hash(),
        last_finalized_hash,
        "finalized tip on disk should have caught up to block 1",
    );

    // Preconditions: still in finalized-write mode, and crucially **no** semantically verified block
    // is queued. The old behavior would not hand off in this state.
    assert!(
        state_service.block_write_sender.finalized.is_some(),
        "write task should still be committing finalized blocks before the handoff",
    );
    assert!(
        !state_service
            .non_finalized_state_queued_blocks
            .has_queued_children(last_finalized_hash),
        "no semantically verified block should be queued before the handoff",
    );

    // Trigger the handoff. Nothing is queued, so this exercises the height-based path.
    let mut cx = Context::from_waker(Waker::noop());
    let _ = state_service.poll_ready(&mut cx);

    // The handoff should have happened purely because the final checkpoint write is durable, with no
    // semantically verified block queued. Before this fix, the finalized sender would still be open.
    assert!(
        state_service.block_write_sender.finalized.is_none(),
        "poll_ready should have handed off to non-finalized writes at the max checkpoint height",
    );

    timeout(Duration::from_secs(5), async {
        loop {
            let store = crate::service::finalized_state::header_chain::HeaderChainStore::new(
                state_service.read_service.db.header_chain_disk_db(),
            );
            if store
                .is_initialized()
                .expect("the header-chain format marker is readable")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the production writer attaches the header runtime at handoff");

    Ok(())
}

/// Legacy-only nodes must preserve the ordinary state handoff without creating or
/// reconstructing the native header runtime.
#[tokio::test(flavor = "multi_thread")]
async fn legacy_mode_handoff_keeps_header_runtime_detached() -> Result<()> {
    use std::task::{Context, Waker};

    use tower::Service;
    use zakura_node_services::sync_lifecycle::HeaderRuntimeStatus;

    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let genesis = zakura_test::vectors::MAINNET_BLOCKS
        .get(&0)
        .expect("the mainnet genesis vector is available")
        .zcash_deserialize_into::<Arc<Block>>()?;

    let (mut state, read, _tip, _tip_change) =
        StateService::new(Config::ephemeral(), &network, Height(0), 0)
            .await
            .expect("ephemeral state initialization succeeds");
    let result = state
        .queue_and_commit_to_finalized_state(CheckpointVerifiedBlock::from(genesis))
        .await;
    assert!(
        matches!(result, Ok(Ok(_))),
        "genesis should commit: {result:?}"
    );

    let mut cx = Context::from_waker(Waker::noop());
    let _ = state.poll_ready(&mut cx);
    assert!(
        state.block_write_sender.finalized.is_none(),
        "legacy state still hands off to semantic writes"
    );

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(matches!(
        &*read.subscribe_header_runtime_status().borrow(),
        HeaderRuntimeStatus::Detached { .. }
    ));
    assert!(read.subscribe_header_chain_snapshots().borrow().is_none());
    assert!(
        !crate::service::finalized_state::header_chain::HeaderChainStore::new(
            state.read_service.db.header_chain_disk_db(),
        )
        .is_initialized()?,
        "legacy handoff must not create durable header-runtime state"
    );

    Ok(())
}

/// Micro-benchmark for the cost added to `poll_ready()` by the handoff trigger.
///
/// `poll_ready()` runs on essentially every state service readiness poll, so the added
/// `try_handoff_to_non_finalized_write()` call must be cheap. This measures three regimes:
///
/// - Raw `finalized_tip_hash()` DB read — a RocksDB seek-to-last. During checkpoint sync the
///   last-sent hash usually runs ahead of the on-disk tip, so the helper short-circuits after this
///   single read; it is the dominant per-poll cost in that phase.
/// - Full guard (still in finalized mode, on-disk tip matches the last-sent hash but below the max
///   checkpoint height and with no queued child): the helper runs the whole condition — two tip
///   reads plus a `HashMap::contains_key` — without transitioning. This is the most expensive
///   non-transitioning path, hit only at the checkpoint boundary.
/// - Steady state (after the handoff, `block_write_sender.finalized == None`): the call
///   short-circuits on a single `Option::is_some()` check and never touches the database. This is
///   what runs for the entire post-sync life of the node.
///
/// Run with:
/// `cargo test -p zakura-state --release -- --ignored --nocapture handoff_trigger_microbench`
#[ignore]
#[allow(clippy::print_stdout)]
#[tokio::test(flavor = "multi_thread")]
async fn handoff_trigger_microbench() -> Result<()> {
    use std::time::Instant;

    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;

    let blocks: Vec<Arc<Block>> = zakura_test::vectors::MAINNET_BLOCKS
        .range(0..=1)
        .map(|(_, block_bytes)| block_bytes.zcash_deserialize_into::<Arc<Block>>().unwrap())
        .collect();

    // Use `Height::MAX` so the height condition is never met: the helper runs its full guard but
    // never transitions, which is exactly the non-transitioning path we want to measure.
    let (mut state_service, _read, _tip, _tip_change) =
        StateService::new(Config::ephemeral(), &network, Height::MAX, 0)
            .await
            .expect("ephemeral state initialization succeeds");

    for block in &blocks[0..=1] {
        let checkpoint = CheckpointVerifiedBlock::from(block.clone());
        state_service
            .queue_and_commit_to_finalized_state(checkpoint)
            .await
            .expect("commit channel open")
            .expect("checkpoint block commits");
    }

    const ITERS: u32 = 1_000_000;

    // Regime 1: raw `finalized_tip_hash()` DB read.
    let start = Instant::now();
    for _ in 0..ITERS {
        std::hint::black_box(state_service.read_service.db.finalized_tip_hash());
    }
    let tip_ns = start.elapsed().as_nanos() as f64 / f64::from(ITERS);

    // Regime 2: full guard cost. The on-disk tip equals the last-sent hash, the height condition is
    // false (`Height::MAX`), and no child is queued, so the helper evaluates every condition but
    // does not transition.
    let start = Instant::now();
    for _ in 0..ITERS {
        std::hint::black_box(state_service.try_handoff_to_non_finalized_write());
    }
    let guard_ns = start.elapsed().as_nanos() as f64 / f64::from(ITERS);
    assert!(
        state_service.block_write_sender.finalized.is_some(),
        "the benchmark must not transition: finalized sender should still be open",
    );

    // Regime 3: steady-state cost (post-handoff). Drop the finalized sender so the helper
    // short-circuits immediately, exactly as it does for the rest of the node's life.
    state_service.block_write_sender.finalized = None;
    let start = Instant::now();
    for _ in 0..ITERS {
        std::hint::black_box(state_service.try_handoff_to_non_finalized_write());
    }
    let steady_ns = start.elapsed().as_nanos() as f64 / f64::from(ITERS);

    println!("handoff trigger micro-benchmark ({ITERS} iters each):");
    println!("  finalized_tip_hash() DB read : {tip_ns:>8.2} ns/call");
    println!("  helper, full guard           : {guard_ns:>8.2} ns/call");
    println!("  helper, steady state         : {steady_ns:>8.2} ns/call");

    Ok(())
}

#[test]
fn state_behaves_when_blocks_are_committed_in_order() -> Result<()> {
    let _init_guard = zakura_test::init();

    let blocks = zakura_test::vectors::MAINNET_BLOCKS
        .range(0..=LAST_BLOCK_HEIGHT)
        .map(|(_, block_bytes)| block_bytes.zcash_deserialize_into::<Arc<Block>>().unwrap())
        .collect();

    populate_and_check(blocks)?;

    Ok(())
}

const DEFAULT_PARTIAL_CHAIN_PROPTEST_CASES: u32 = 2;

/// The legacy chain limit for tests.
const TEST_LEGACY_CHAIN_LIMIT: usize = 100;

/// Check more blocks than the legacy chain limit.
const OVER_LEGACY_CHAIN_LIMIT: u32 = TEST_LEGACY_CHAIN_LIMIT as u32 + 10;

/// Check fewer blocks than the legacy chain limit.
const UNDER_LEGACY_CHAIN_LIMIT: u32 = TEST_LEGACY_CHAIN_LIMIT as u32 - 10;

proptest! {
    #![proptest_config(
        proptest::test_runner::Config::with_cases(env::var("PROPTEST_CASES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_PARTIAL_CHAIN_PROPTEST_CASES))
    )]

    /// Test out of order commits of continuous block test vectors from genesis onward.
    #[test]
    fn state_behaves_when_blocks_are_committed_out_of_order(blocks in out_of_order_committing_strategy()) {
        let _init_guard = zakura_test::init();

        populate_and_check(blocks).unwrap();
    }

    /// Test blocks that are less than the NU5 activation height.
    #[test]
    fn some_block_less_than_network_upgrade(
        (network, nu_activation_height, chain) in partial_nu5_chain_strategy(4, true, UNDER_LEGACY_CHAIN_LIMIT, NetworkUpgrade::Canopy)
    ) {
        let response = crate::service::check::legacy_chain(nu_activation_height, chain.into_iter().rev(), &network, TEST_LEGACY_CHAIN_LIMIT)
            .map_err(|error| error.to_string());

        prop_assert_eq!(response, Ok(()));
    }

    /// Test the maximum amount of blocks to check before chain is declared to be legacy.
    #[test]
    fn no_transaction_with_network_upgrade(
        (network, nu_activation_height, chain) in partial_nu5_chain_strategy(4, true, OVER_LEGACY_CHAIN_LIMIT, NetworkUpgrade::Canopy)
    ) {
        let tip_height = chain
            .last()
            .expect("chain contains at least one block")
            .coinbase_height()
            .expect("chain contains valid blocks");

        let response = crate::service::check::legacy_chain(nu_activation_height, chain.into_iter().rev(), &network, TEST_LEGACY_CHAIN_LIMIT)
            .map_err(|error| error.to_string());

        prop_assert_eq!(
            response,
            Err(format!(
                "could not find any transactions in recent blocks: checked {TEST_LEGACY_CHAIN_LIMIT} blocks back from {tip_height:?}",
            ))
        );
    }

    /// Test the `Block.check_transaction_network_upgrade()` error inside the legacy check.
    #[test]
    fn at_least_one_transaction_with_inconsistent_network_upgrade(
        (network, nu_activation_height, chain) in partial_nu5_chain_strategy(5, false, OVER_LEGACY_CHAIN_LIMIT, NetworkUpgrade::Nu5)
    ) {
        // this test requires that an invalid block is encountered
        // before a valid block (and before the check gives up),
        // but setting `transaction_has_valid_network_upgrade` to false
        // sometimes generates blocks with all valid (or missing) network upgrades

        // we must check at least one block, and the first checked block must be invalid
        let first_checked_block = chain
            .iter()
            .rev()
            .take_while(|block| block.coinbase_height().unwrap() >= nu_activation_height)
            .take(100)
            .next();
        prop_assume!(first_checked_block.is_some());
        prop_assume!(
            first_checked_block
                .unwrap()
                .check_transaction_network_upgrade_consistency(&network)
                .is_err()
        );

        let response = crate::service::check::legacy_chain(
            nu_activation_height,
            chain.clone().into_iter().rev(),
            &network,
            TEST_LEGACY_CHAIN_LIMIT,
        ).map_err(|error| error.to_string());

        prop_assert_eq!(
            response,
            Err("inconsistent network upgrade found in transaction: WrongTransactionConsensusBranchId".into()),
            "first: {:?}, last: {:?}",
            chain.first().map(|block| block.coinbase_height()),
            chain.last().map(|block| block.coinbase_height()),
        );
    }

    /// Test there is at least one transaction with a valid `network_upgrade` in the legacy check.
    #[test]
    fn at_least_one_transaction_with_valid_network_upgrade(
        (network, nu_activation_height, chain) in partial_nu5_chain_strategy(5, true, UNDER_LEGACY_CHAIN_LIMIT, NetworkUpgrade::Nu5)
    ) {
        let response = crate::service::check::legacy_chain(nu_activation_height, chain.into_iter().rev(), &network, TEST_LEGACY_CHAIN_LIMIT)
            .map_err(|error| error.to_string());

        prop_assert_eq!(response, Ok(()));
    }

    /// Test that the value pool is updated accordingly.
    ///
    /// 1. Generate a finalized chain and some non-finalized blocks.
    /// 2. Check that initially the value pool is empty.
    /// 3. Commit the finalized blocks and check that the value pool is updated accordingly.
    /// 4. Commit the non-finalized blocks and check that the value pool is also updated
    ///    accordingly.
    #[test]
    fn value_pool_is_updated(
        (network, finalized_blocks, non_finalized_blocks)
            in continuous_empty_blocks_from_test_vectors(),
    ) {
        let _init_guard = zakura_test::init();
        let (mut state_service, _, _, _) = Runtime::new().unwrap().block_on(async {
            // We're waiting to verify each block here, so we don't need the maximum checkpoint height.
            StateService::new(Config::ephemeral(), &network, Height::MAX, 0).await
        }).expect("ephemeral state initialization succeeds");

        prop_assert_eq!(state_service.read_service.db.finalized_value_pool(), ValueBalance::zero());
        prop_assert_eq!(
            state_service.read_service.latest_non_finalized_state().best_chain().map(|chain| chain.chain_value_pools).unwrap_or_else(ValueBalance::zero),
            ValueBalance::zero()
        );

        // the slow start rate for the first few blocks, as in the spec
        const SLOW_START_RATE: i64 = 62500;
        // the expected transparent pool value, calculated using the slow start rate
        let mut expected_transparent_pool = ValueBalance::zero();

        let mut expected_finalized_value_pool = Ok(ValueBalance::zero());
        for block in finalized_blocks {
            // the genesis block has a zero-valued transparent output,
            // which is not included in the UTXO set
            if block.height > block::Height(0) {
                let utxos = &block.new_outputs.iter().map(|(k, ordered_utxo)| (*k, ordered_utxo.utxo.clone())).collect();
                let block_value_pool = &block.block.chain_value_pool_change(utxos, None)?;
                expected_finalized_value_pool += *block_value_pool;
            }

            let result_receiver = state_service.queue_and_commit_to_finalized_state(block.clone());
            let result = result_receiver.blocking_recv();

            prop_assert!(result.is_ok(), "unexpected failed finalized block commit: {:?}", result);

            prop_assert_eq!(
                state_service.read_service.db.finalized_value_pool(),
                expected_finalized_value_pool.clone()?.constrain()?
            );

            let transparent_value = SLOW_START_RATE * i64::from(block.height.0);
            let transparent_value = transparent_value.try_into().unwrap();
            let transparent_value = ValueBalance::from_transparent_amount(transparent_value);
            expected_transparent_pool = (expected_transparent_pool + transparent_value).unwrap();
            prop_assert_eq!(
                state_service.read_service.db.finalized_value_pool(),
                expected_transparent_pool
            );
        }

        let mut expected_non_finalized_value_pool = Ok(expected_finalized_value_pool?);
        for block in non_finalized_blocks {
            let utxos = block.new_outputs.clone();
            let block_value_pool = &block.block.chain_value_pool_change(&transparent::utxos_from_ordered_utxos(utxos), None)?;
            expected_non_finalized_value_pool += *block_value_pool;

            let result_receiver = state_service.queue_and_commit_to_non_finalized_state(block.clone());
            let result = result_receiver.blocking_recv();

            prop_assert!(result.is_ok(), "unexpected failed non-finalized block commit: {:?}", result);

            prop_assert_eq!(
                state_service.read_service.latest_non_finalized_state().best_chain().unwrap().chain_value_pools,
                expected_non_finalized_value_pool.clone()?.constrain()?
            );

            let transparent_value = SLOW_START_RATE * i64::from(block.height.0);
            let transparent_value = transparent_value.try_into().unwrap();
            let transparent_value = ValueBalance::from_transparent_amount(transparent_value);
            expected_transparent_pool = (expected_transparent_pool + transparent_value).unwrap();
            prop_assert_eq!(
                state_service.read_service.latest_non_finalized_state().best_chain().unwrap().chain_value_pools,
                expected_transparent_pool
            );
        }
    }
}

// This test sleeps for every block, so we only ever want to run it once
proptest! {
    #![proptest_config(
        proptest::test_runner::Config::with_cases(1)
    )]

    /// Test that the best tip height is updated accordingly.
    ///
    /// 1. Generate a finalized chain and some non-finalized blocks.
    /// 2. Check that initially the best tip height is empty.
    /// 3. Commit the finalized blocks and check that the best tip height is updated accordingly.
    /// 4. Commit the non-finalized blocks and check that the best tip height is also updated
    ///    accordingly.
    #[test]
    fn chain_tip_sender_is_updated(
        (network, finalized_blocks, non_finalized_blocks)
            in continuous_empty_blocks_from_test_vectors(),
    ) {
        let _init_guard = zakura_test::init();

        let runtime = Runtime::new().unwrap();
        let (mut state_service, _read_only_state_service, latest_chain_tip, mut chain_tip_change) = runtime.block_on(async {
            // We're waiting to verify each block here, so we don't need the maximum checkpoint height.
            StateService::new(Config::ephemeral(), &network, Height::MAX, 0).await
        }).expect("ephemeral state initialization succeeds");

        prop_assert_eq!(latest_chain_tip.best_tip_height(), None);
        prop_assert_eq!(chain_tip_change.last_tip_change(), None);

        for block in finalized_blocks {
            let expected_block = block.clone();

            let expected_action = if expected_block.height == block::Height(0) {
                // Height 0 is reset by initialization. The BeforeOverwinter upgrade
                // (activation height 1) also resets at height 0 rather than at height 1,
                // because `ChainTipChange` resets one block *before* an activation height
                // (it checks `height.next()`, matching the height the mempool verifies
                // against). See `ChainTipChange::action`.
                TipAction::reset_with(expected_block.clone().into())
            } else {
                TipAction::grow_with(expected_block.clone().into())
            };

            let result_receiver = state_service.queue_and_commit_to_finalized_state(block);
            let result = result_receiver.blocking_recv();

            prop_assert!(result.is_ok(), "unexpected failed finalized block commit: {:?}", result);

            let actual_action = runtime
                .block_on(async {
                    timeout(
                        CHAIN_TIP_UPDATE_WAIT_LIMIT,
                        chain_tip_change.wait_for_tip_change(),
                    )
                    .await
                })
                .expect("tip change arrives because the committed block updates the channel")
                .expect("tip sender remains open while the state service is alive");

            prop_assert_eq!(latest_chain_tip.best_tip_height(), Some(expected_block.height));
            prop_assert_eq!(actual_action, expected_action);
        }

        for block in non_finalized_blocks {
            let expected_block = block.clone();

            // The genesis block (height 0) is always finalized, and the BeforeOverwinter
            // reset fires at height 0 (one block before its activation height of 1), so
            // every non-finalized block (height >= 1) grows the chain.
            let expected_action = TipAction::grow_with(expected_block.clone().into());

            let result_receiver = state_service.queue_and_commit_to_non_finalized_state(block);
            let result = result_receiver.blocking_recv();

            prop_assert!(result.is_ok(), "unexpected failed non-finalized block commit: {:?}", result);

            let actual_action = runtime
                .block_on(async {
                    timeout(
                        CHAIN_TIP_UPDATE_WAIT_LIMIT,
                        chain_tip_change.wait_for_tip_change(),
                    )
                    .await
                })
                .expect("tip change arrives because the committed block updates the channel")
                .expect("tip sender remains open while the state service is alive");

            prop_assert_eq!(latest_chain_tip.best_tip_height(), Some(expected_block.height));
            prop_assert_eq!(actual_action, expected_action);
        }
    }
}

/// Test strategy to generate a chain split in two from the test vectors.
///
/// Selects either the mainnet or testnet chain test vector and randomly splits the chain in two
/// lists of blocks. The first containing the blocks to be finalized (which always includes at
/// least the genesis block) and the blocks to be stored in the non-finalized state.
fn continuous_empty_blocks_from_test_vectors() -> impl Strategy<
    Value = (
        Network,
        SummaryDebug<Vec<CheckpointVerifiedBlock>>,
        SummaryDebug<Vec<SemanticallyVerifiedBlock>>,
    ),
> {
    any::<Network>()
        .prop_flat_map(|network| {
            // Select the test vector based on the network
            let raw_blocks = network.blockchain_map();

            // Transform the test vector's block bytes into a vector of `SemanticallyVerifiedBlock`s.
            let blocks: Vec<_> = raw_blocks
                .iter()
                .map(|(_height, &block_bytes)| {
                    let mut block_reader: &[u8] = block_bytes;
                    let mut block = Block::zcash_deserialize(&mut block_reader)
                        .expect("Failed to deserialize block from test vector");

                    let coinbase = transaction_v4_from_coinbase(&block.transactions[0]);
                    block.transactions = vec![Arc::new(coinbase)];

                    Arc::new(block).prepare()
                })
                .collect();

            // Always finalize the genesis block
            let finalized_blocks_count = 1..=blocks.len();

            (Just(network), Just(blocks), finalized_blocks_count)
        })
        .prop_map(|(network, mut blocks, finalized_blocks_count)| {
            let non_finalized_blocks = blocks.split_off(finalized_blocks_count);
            let finalized_blocks: Vec<_> =
                blocks.into_iter().map(CheckpointVerifiedBlock).collect();

            (
                network,
                finalized_blocks.into(),
                non_finalized_blocks.into(),
            )
        })
}

/// Opening a read-only state against an existing but empty cache directory (no database on
/// disk) must fail with [`StateInitError::ReadOnlyDatabaseNotFound`] rather than silently
/// creating a new, empty database.
#[test]
fn read_only_open_with_no_database_returns_error() {
    let network = Network::Mainnet;

    // An existing, readable, but empty cache directory: it contains no database.
    let cache_dir =
        tempfile::tempdir().expect("creating a temporary cache directory should succeed");
    let config = Config {
        cache_dir: cache_dir.path().to_path_buf(),
        ephemeral: false,
        ..Config::default()
    };

    match super::init_read_only(config, &network) {
        Err(crate::StateInitError::ReadOnlyDatabaseNotFound { .. }) => {}
        Err(other) => panic!("expected ReadOnlyDatabaseNotFound, got: {other:?}"),
        Ok(_) => panic!("expected an error when opening a read-only state with no database"),
    }
}

#[test]
fn read_only_secondary_workspace_is_deleted_on_drop() {
    let network = Network::Mainnet;
    let cache_dir =
        tempfile::tempdir().expect("creating a temporary cache directory should succeed");
    let config = Config {
        cache_dir: cache_dir.path().to_path_buf(),
        ephemeral: false,
        ..Config::default()
    };

    let mut finalized_state = super::finalized_state::FinalizedState::new(&config, &network)
        .expect("writable state creates the database");
    finalized_state.db.shutdown(true);
    drop(finalized_state);

    let (read_service, db, non_finalized_sender) =
        super::init_read_only(config, &network).expect("read-only state opens");
    let secondary_path = db
        .secondary_path()
        .expect("read-only state has a secondary workspace")
        .to_path_buf();
    assert!(secondary_path.is_dir());

    drop(read_service);
    drop(non_finalized_sender);
    drop(db);

    assert!(
        !secondary_path.exists(),
        "the secondary workspace is owned by the read-only database"
    );
}

/// Opening a read-only state against a missing or unreadable cache directory must fail with a
/// typed [`StateInitError::ReadOnlyCacheDirUnreadable`] rather than panicking while reading the
/// on-disk format version.
#[test]
fn read_only_open_with_unreadable_cache_dir_returns_error() {
    let network = Network::Mainnet;

    // A cache directory that does not exist. `read_dir` fails for a missing directory the same way
    // it does for an unreadable one, without depending on filesystem permissions (which `root`
    // ignores, so a chmod-based unreadable directory would not be a reliable test under CI).
    let parent = tempfile::tempdir().expect("creating a temporary directory should succeed");
    let config = Config {
        cache_dir: parent.path().join("missing"),
        ephemeral: false,
        ..Config::default()
    };

    match super::init_read_only(config, &network) {
        Err(crate::StateInitError::ReadOnlyCacheDirUnreadable { .. }) => {}
        Err(other) => panic!("expected ReadOnlyCacheDirUnreadable, got: {other:?}"),
        Ok(_) => {
            panic!("expected an error when opening a read-only state with an unreadable cache dir")
        }
    }
}

/// Opening a read-only state with an ephemeral database configured must fail with
/// [`StateInitError::ReadOnlyEphemeralConflict`]: a read-only secondary follows another
/// process's primary database and must never delete it, so it cannot also be ephemeral
/// (which would delete the primary's files on drop).
#[test]
fn read_only_open_with_ephemeral_config_returns_error() {
    let network = Network::Mainnet;

    let config = Config {
        ephemeral: true,
        ..Config::default()
    };

    match super::init_read_only(config, &network) {
        Err(crate::StateInitError::ReadOnlyEphemeralConflict) => {}
        Err(other) => panic!("expected ReadOnlyEphemeralConflict, got: {other:?}"),
        Ok(_) => {
            panic!("expected an error when opening a read-only state with an ephemeral config")
        }
    }
}

#[test]
fn read_only_open_with_malformed_version_returns_typed_error() {
    let network = Network::Mainnet;
    let cache_dir = tempfile::tempdir().expect("creating a temporary cache directory succeeds");
    let config = Config {
        cache_dir: cache_dir.path().to_path_buf(),
        ephemeral: false,
        ..Config::default()
    };
    let version_path = config.version_file_path(
        crate::constants::STATE_DATABASE_KIND,
        crate::state_database_format_version_in_code().major,
        &network,
    );
    std::fs::create_dir_all(
        version_path
            .parent()
            .expect("the version path has a cache-directory parent"),
    )
    .expect("the version-file parent is created");
    std::fs::write(&version_path, "not-a-semantic-version")
        .expect("the malformed version fixture is written");

    match super::init_read_only(config, &network) {
        Err(crate::StateInitError::DatabaseFormatVersion { path, .. }) => {
            assert_eq!(path, version_path);
        }
        Err(other) => panic!("expected DatabaseFormatVersion, got: {other:?}"),
        Ok(_) => panic!("expected malformed state version to fail closed"),
    }
}
