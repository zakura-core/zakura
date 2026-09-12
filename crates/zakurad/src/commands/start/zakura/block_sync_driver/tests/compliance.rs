//! Wire-to-consensus composition for the GetBlocks body commitment requirement.

use super::*;
use crate::BoxError;
use tokio_util::task::AbortOnDropHandle;
use zakura_chain::{parameters::Network, transaction::Transaction, transparent};
use zakura_network::zakura::{
    spawn_block_sync_reactor, testkit::SyntheticBlockSyncPeers, BlockSyncMessage, BlockSyncStartup,
    ZakuraBlockSyncConfig, ZakuraPeerId,
};

const DEADLINE: Duration = Duration::from_secs(10);

#[derive(Clone, Copy)]
enum Case {
    Valid,
    ChangedBody,
    LocalStateFailure,
}

async fn check_ingress(case: Case) {
    let _init = zakura_test::init();
    let good = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    let genesis = mainnet_block(&zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES);
    let mut supplied = good.clone();
    if matches!(case, Case::ChangedBody) {
        let transaction = Arc::make_mut(&mut Arc::make_mut(&mut supplied).transactions[0]);
        let inputs = match transaction {
            Transaction::V1 { inputs, .. }
            | Transaction::V2 { inputs, .. }
            | Transaction::V3 { inputs, .. }
            | Transaction::V4 { inputs, .. }
            | Transaction::V5 { inputs, .. }
            | Transaction::V6 { inputs, .. } => inputs,
        };
        let transparent::Input::Coinbase { data, .. } = &mut inputs[0] else {
            panic!("real coinbase fixture");
        };
        data.push(0x47);
        assert_eq!(supplied.header, good.header);
        assert_eq!(
            supplied.hash(),
            good.hash(),
            "R13 header identity is still authorized"
        );
        assert_ne!(supplied.transactions, good.transactions);
    }
    let config = ZakuraBlockSyncConfig::default();
    let anchor = zakura_header_chain::Frontier::new(block::Height(0), genesis.hash());
    let snapshot = zakura_header_chain::EngineSnapshot {
        mode: zakura_header_chain::EngineMode::Integrated,
        state_version: zakura_header_chain::StateVersion::new(1),
        header_generation: zakura_header_chain::HeaderGeneration::new(1),
        verified_generation: zakura_header_chain::VerifiedGeneration::new(1),
        frontiers: zakura_header_chain::FrontierSet {
            finalized: anchor,
            verified_best: anchor,
            header_best: zakura_header_chain::Frontier::new(block::Height(1), good.hash()),
        },
        header_best_score: zakura_header_chain::ChainScore::new(
            zakura_header_chain::SuffixWork::zero(),
            good.hash(),
        ),
        oldest_retained_height: block::Height(0),
        alarms: zakura_header_chain::AlarmSet::default(),
    };
    let (_views, committed_views) =
        watch::channel(Some(zakura_header_chain::CommittedHeaderChainView::new(
            snapshot,
            zakura_header_chain::BodyWorkEpoch::default(),
        )));
    let startup = BlockSyncStartup::new_with_committed_views(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: genesis.hash(),
        },
        (block::Height(1), good.hash()),
        committed_views,
        config.clone(),
    );
    let (handle, mut actions, reactor) = spawn_block_sync_reactor(startup);
    let _reactor = AbortOnDropHandle::new(reactor);
    let peers = SyntheticBlockSyncPeers::new(config.clone(), handle.clone(), 4);
    let peer_id = ZakuraPeerId::new(vec![0x89; 32]).unwrap();
    let mut peer = peers
        .add_peer(
            peer_id.clone(),
            zakura_network::zakura::BlockSyncStatus {
                servable_low: block::Height(1),
                servable_high: block::Height(1),
                tip_hash: good.hash(),
                ..config.initial_status()
            },
        )
        .await
        .unwrap();
    let (query_id, scope) = tokio::time::timeout(DEADLINE, async {
        loop {
            if let BlockSyncAction::QueryNeededBlocks {
                query_id, scope, ..
            } = actions.recv().await.unwrap()
            {
                break (query_id, scope);
            }
        }
    })
    .await
    .expect("the reactor requests scoped body metadata");
    handle
        .send(BlockSyncEvent::ScopedNeededBlocks {
            query_id,
            scope,
            body_anchor: zakura_header_chain::Frontier::new(block::Height(0), genesis.hash()),
            blocks: vec![BlockSyncBlockMeta {
                height: block::Height(1),
                hash: good.hash(),
                size: BlockSizeEstimate::Unknown,
            }],
        })
        .await
        .unwrap();
    tokio::time::timeout(DEADLINE, async {
        loop {
            if let Some(BlockSyncMessage::GetBlocks {
                start_height,
                count,
            }) = peer.recv().await.unwrap()
            {
                assert_eq!((start_height, count), (block::Height(1), 1));
                break;
            }
        }
    })
    .await
    .expect("production receiver publishes the matching request");
    peer.send(BlockSyncMessage::Block(supplied)).await.unwrap();
    let (owner, source, token, supplied) = tokio::time::timeout(DEADLINE, async {
        loop {
            match actions.recv().await.unwrap() {
                BlockSyncAction::SubmitBlock {
                    owner,
                    source,
                    token,
                    block,
                } => break (owner, source, token, block),
                BlockSyncAction::QueryNeededBlocks { .. } => {}
                other => panic!("unexpected ingress action: {other:?}"),
            }
        }
    })
    .await
    .expect("matched body reaches the real node verifier boundary");
    assert_eq!(
        source,
        zakura_header_chain::SourceId::from_digest(peer_id.digest())
    );

    let commits = Arc::new(AtomicUsize::new(0));
    let attempts = commits.clone();
    let genesis_hash = genesis.hash();
    let state =
        service_fn(
            move |request: zakura_state::Request| -> futures::future::BoxFuture<
                'static,
                Result<zakura_state::Response, BoxError>,
            > {
                let attempts = attempts.clone();
                Box::pin(async move {
                    match request {
                        zakura_state::Request::CommitCheckpointVerifiedBlock(block) => {
                            attempts.fetch_add(1, Ordering::SeqCst);
                            if matches!(case, Case::LocalStateFailure) {
                                return Err(std::io::Error::other(
                                    "controlled local state failure",
                                )
                                .into());
                            }
                            Ok(zakura_state::Response::Committed(block.hash))
                        }
                        zakura_state::Request::Tip => Ok(zakura_state::Response::Tip(Some((
                            block::Height(0),
                            genesis_hash,
                        )))),
                        other => panic!("unexpected state request: {other:?}"),
                    }
                })
            },
        );
    let state = tower::util::BoxCloneService::new(state);
    let verifier = zakura_consensus::CheckpointVerifier::from_list(
        [
            (block::Height(0), genesis.hash()),
            (block::Height(1), good.hash()),
        ],
        &Network::Mainnet,
        Some((block::Height(0), genesis.hash())),
        state,
    )
    .unwrap();
    let verifier = Arc::new(std::sync::Mutex::new(verifier));
    let verifier = service_fn(move |request: zakura_consensus::Request| -> futures::future::BoxFuture<'static, Result<block::Hash, BoxError>> {
        let zakura_consensus::Request::Commit(block) = request else { panic!("driver must commit the body"); };
        // CheckpointVerifier is always ready. Keep its state across requests,
        // and release the mutex before awaiting its actual verification future.
        let future = tower::Service::call(&mut *verifier.lock().unwrap(), block);
        Box::pin(async move { future.await.map_err(|error| Box::new(error) as BoxError) })
    });
    let outcome = tokio::time::timeout(
        DEADLINE,
        commit_block_sync_body(
            verifier,
            owner,
            source,
            supplied,
            BlockApplyClass::Checkpoint,
        ),
    )
    .await
    .expect("real checkpoint validation finishes");
    match case {
        Case::Valid => {
            assert_eq!(outcome.result(), BlockApplyResult::Committed);
            assert_eq!(commits.load(Ordering::SeqCst), 1);
        }
        Case::ChangedBody => {
            assert_eq!(
                outcome.result(),
                BlockApplyResult::Rejected,
                "R13 unchanged header does not validate a different body"
            );
            assert_eq!(
                commits.load(Ordering::SeqCst),
                0,
                "R13 body commitment is checked before state commit"
            );
        }
        Case::LocalStateFailure => {
            assert_eq!(
                outcome.result(),
                BlockApplyResult::Unavailable,
                "C06 local state failure is retryable"
            );
            assert_eq!(commits.load(Ordering::SeqCst), 1);
        }
    }
    handle
        .send(BlockSyncEvent::BlockApplyFinished {
            owner,
            source,
            token,
            height: block::Height(1),
            hash: good.hash(),
            outcome,
        })
        .await
        .unwrap();
    if matches!(case, Case::ChangedBody) {
        tokio::time::timeout(DEADLINE, async {
            loop {
                if let BlockSyncAction::Misbehavior { peer, .. } = actions.recv().await.unwrap() {
                    assert_eq!(
                        peer, peer_id,
                        "R13 rejection is attributed to the exact supplier"
                    );
                    break;
                }
            }
        })
        .await
        .expect("invalid supplied body is attributed through the sequencer");
    }
    if matches!(case, Case::LocalStateFailure) {
        let local_fault_window = tokio::time::sleep(Duration::from_millis(50));
        tokio::pin!(local_fault_window);
        loop {
            tokio::select! {
                _ = &mut local_fault_window => break,
                action = actions.recv() => {
                    let Some(action) = action else { break; };
                    assert!(!matches!(action, BlockSyncAction::Misbehavior { .. }),
                        "C06 local storage failure must not blame the peer: {action:?}");
                }
            }
        }
    }
    peer.cancel();
}

#[tokio::test]
async fn r13_authorized_body_with_unchanged_header_fails_real_commitment_verification() {
    check_ingress(Case::ChangedBody).await;
}

#[tokio::test]
async fn r13_valid_authorized_block_reaches_state_after_real_verification() {
    check_ingress(Case::Valid).await;
}

#[tokio::test]
async fn r13_local_state_failure_after_verification_is_not_peer_misconduct() {
    check_ingress(Case::LocalStateFailure).await;
}
