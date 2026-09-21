//! Fallback template recovery across tip changes and delayed state responses.

use std::{sync::Arc, time::Duration};

use tower::buffer::Buffer;
use zakura_chain::{
    block::Hash,
    chain_sync_status::MockSyncStatus,
    parameters::{Network, NetworkUpgrade},
    work::difficulty::{CompactDifficulty, ExpandedDifficulty, U256},
};
use zakura_network::address_book_peers::MockAddressBookPeers;
use zakura_node_services::{mempool, BoxError};
use zakura_state::{
    ChainTipBlock, ChainTipSender, GetBlockTemplateChainInfo, ReadRequest, ReadResponse,
};
use zakura_test::mock_service::MockService;
use zcash_address::{ToAddress, ZcashAddress};
use zcash_protocol::consensus::NetworkType;

use super::{super::*, utils::fake_history_tree};
use crate::methods::types::get_block_template::GetBlockTemplateRequestMode;

#[derive(Clone, Copy, Debug)]
enum Interleaving {
    StaleFallback {
        success: bool,
        notice_lags: bool,
    },
    FallbackDeadline {
        validation_fails_after: Option<Duration>,
    },
    FastPathFallback,
}

#[tokio::test]
async fn mining_template_retries_successful_fallback_for_old_parent() {
    check_interleaving(Interleaving::StaleFallback {
        success: true,
        notice_lags: false,
    })
    .await;
}

#[tokio::test]
async fn mining_template_retries_failed_fallback_for_old_parent() {
    check_interleaving(Interleaving::StaleFallback {
        success: false,
        notice_lags: false,
    })
    .await;
}

#[tokio::test]
async fn mining_template_retries_stale_fallback_before_tip_notice() {
    check_interleaving(Interleaving::StaleFallback {
        success: false,
        notice_lags: true,
    })
    .await;
}

#[tokio::test]
async fn mining_template_tip_wakeup_retries_stale_fallback() {
    check_interleaving(Interleaving::FastPathFallback).await;
}

#[tokio::test]
async fn mining_template_fallback_timeout_includes_tip_check() {
    check_interleaving(Interleaving::FallbackDeadline {
        validation_fails_after: None,
    })
    .await;
}

#[tokio::test]
async fn mining_template_fallback_error_leaves_only_remaining_time_for_tip_check() {
    check_interleaving(Interleaving::FallbackDeadline {
        validation_fails_after: Some(Duration::from_secs(20)),
    })
    .await;
}

async fn check_interleaving(interleaving: Interleaving) {
    let _init_guard = zakura_test::init();
    let network = Network::Mainnet;
    let height = NetworkUpgrade::Nu5.activation_height(&network).unwrap();
    let mut parent_a = Hash([0; 32]);
    parent_a.0[0] = 1;
    let mut parent_b = Hash([0; 32]);
    parent_b.0[0] = 2;
    let make_tip = |hash| ChainTipBlock {
        hash,
        height,
        time: chrono::Utc::now(),
        transactions: vec![],
        transaction_hashes: Arc::from([]),
        previous_block_hash: Hash([0; 32]),
    };
    let (mut tip_sender, tip, _tip_change) = ChainTipSender::new(make_tip(parent_a), &network);
    let make_info = |tip_hash| GetBlockTemplateChainInfo {
        value_pools: Default::default(),
        expected_difficulty: CompactDifficulty::from(ExpandedDifficulty::from(U256::one())),
        tip_height: height,
        tip_hash,
        cur_time: 1654008617.into(),
        min_time: 1654008606.into(),
        max_time: 1654008728.into(),
        chain_history_root: fake_history_tree(&network).hash(),
    };
    let make_mempool = |last_seen_tip_hash| mempool::Response::FullTransactions {
        transactions: vec![],
        transaction_dependencies: Default::default(),
        last_seen_tip_hash,
    };
    let mut mempool: MockService<_, _, _, BoxError> = MockService::build()
        .with_max_request_delay(Duration::from_secs(5))
        .for_unit_tests();
    let mut read_state: MockService<_, _, _, BoxError> = MockService::build()
        .with_max_request_delay(Duration::from_secs(5))
        .for_unit_tests();
    let state: MockService<_, _, _, BoxError> = MockService::build().for_unit_tests();
    let mut verifier: MockService<_, _, _, BoxError> = MockService::build()
        .with_max_request_delay(Duration::from_secs(5))
        .for_unit_tests();
    let mut sync = MockSyncStatus::default();
    sync.set_is_close_to_tip(true);
    let (_tx, rx) = tokio::sync::watch::channel(None);
    let (rpc, queue) = RpcImpl::new(
        network.clone(),
        config::mining::Config {
            miner_address: Some(ZcashAddress::from_transparent_p2pkh(
                NetworkType::Main,
                [0x7e; 20],
            )),
            ..Default::default()
        },
        false,
        "0.0.1",
        "template concurrency test",
        Buffer::new(mempool.clone(), 1),
        state,
        Buffer::new(read_state.clone(), 1),
        Buffer::new(verifier.clone(), 1),
        sync,
        tip.clone(),
        MockAddressBookPeers::default(),
        rx,
        None,
    );
    let rpc = Arc::new(rpc);
    let first = tokio::spawn({
        let rpc = rpc.clone();
        async move { rpc.get_block_template(None).await }
    });
    read_state
        .expect_request(ReadRequest::ChainInfo)
        .await
        .respond(ReadResponse::ChainInfo(make_info(parent_a)));
    mempool
        .expect_request(mempool::Request::FullTransactions)
        .await
        .respond(make_mempool(parent_a));
    let first = first.await.unwrap().unwrap().try_into_template().unwrap();
    let mut preparation = Some(
        verifier
            .expect_request_that(|request| {
                matches!(request, zakura_consensus::Request::Prepare { .. })
            })
            .await,
    );

    if matches!(
        interleaving,
        Interleaving::StaleFallback { .. } | Interleaving::FallbackDeadline { .. }
    ) {
        preparation
            .take()
            .unwrap()
            .respond_error(Box::new(zakura_consensus::RouterError::Block {
                source: Box::new(zakura_consensus::BlockError::DuplicateTransaction.into()),
            }));
        tokio::time::timeout(
            Duration::from_secs(2),
            rpc.wait_for_mining_template_rejection(&first.work_id),
        )
        .await
        .unwrap();
        let pending = tokio::spawn({
            let rpc = rpc.clone();
            async move { rpc.get_block_template(None).await }
        });
        read_state
            .expect_request(ReadRequest::ChainInfo)
            .await
            .respond(ReadResponse::ChainInfo(make_info(parent_a)));
        mempool
            .expect_request(mempool::Request::FullTransactions)
            .await
            .respond(make_mempool(parent_a));
        let fallback = verifier
            .expect_request_that(|request| {
                matches!(request, zakura_consensus::Request::Prepare { .. })
            })
            .await;
        if let Interleaving::FallbackDeadline {
            validation_fails_after,
        } = interleaving
        {
            // Construction uses blocking threads. Pause only once validation is pending.
            tokio::time::pause();
            let started = tokio::time::Instant::now();
            let mut fallback = Some(fallback);
            let stalled_tip = if let Some(delay) = validation_fails_after {
                tokio::time::advance(delay).await;
                fallback
                    .take()
                    .unwrap()
                    .respond_error("fallback validation failed".into());
                Some(read_state.expect_request(ReadRequest::Tip).await)
            } else {
                None
            };
            // Leave validation or the tip read pending. Neither can extend recovery's deadline.
            let error = tokio::time::timeout_at(started + Duration::from_secs(31), pending)
                .await
                .expect("fallback recovery must finish within its original 30-second budget")
                .unwrap()
                .expect_err("stalled recovery must return an error");
            assert!(error.message().contains("deadline has elapsed"));
            assert!(started.elapsed() <= Duration::from_secs(30));
            assert!(started.elapsed() >= Duration::from_secs(29));
            drop(fallback);
            drop(stalled_tip);
            queue.abort();
            return;
        }
        let Interleaving::StaleFallback {
            success,
            notice_lags,
        } = interleaving
        else {
            unreachable!("deadline cases return before stale fallback cases")
        };
        if !notice_lags {
            tip_sender.set_best_non_finalized_tip(make_tip(parent_b));
        }
        if success {
            fallback.respond(parent_a);
        } else {
            fallback.respond_error(Box::new(zakura_consensus::RouterError::Block {
                source: Box::new(zakura_consensus::VerifyBlockError::ValidateProposal(
                    "proposal is not based on the current best chain tip: previous block hash must be the best chain tip".into(),
                )),
            }));
        }
        if notice_lags {
            read_state
                .expect_request(ReadRequest::Tip)
                .await
                .respond(ReadResponse::Tip(Some((height, parent_b))));
        }
        let fresh_read = read_state.expect_request(ReadRequest::ChainInfo).await;
        tip_sender.set_best_non_finalized_tip(make_tip(parent_b));
        fresh_read.respond(ReadResponse::ChainInfo(make_info(parent_b)));
        mempool
            .expect_request(mempool::Request::FullTransactions)
            .await
            .respond(make_mempool(parent_b));
        let replacement = tokio::time::timeout(Duration::from_secs(2), pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .try_into_template()
            .unwrap();
        assert_eq!(replacement.previous_block_hash, parent_b);
        assert_eq!(replacement.height, height.0 + 1);
        assert!(!rpc.gbt.template_rejections.borrow().needs_fallback());
        queue.abort();
        return;
    }

    let mut pending = tokio::spawn({
        let rpc = rpc.clone();
        let old_id = first.long_poll_id;
        async move {
            rpc.get_block_template(Some(GetBlockTemplateParameters::new(
                GetBlockTemplateRequestMode::Template,
                None,
                vec![],
                Some(old_id),
                None,
            )))
            .await
        }
    });

    match interleaving {
        Interleaving::StaleFallback { .. } | Interleaving::FallbackDeadline { .. } => {
            unreachable!("handled before the long-poll case")
        }
        Interleaving::FastPathFallback => {
            read_state
                .expect_request(ReadRequest::ChainInfo)
                .await
                .respond(ReadResponse::ChainInfo(make_info(parent_a)));
            mempool
                .expect_request(mempool::Request::FullTransactions)
                .await
                .respond(make_mempool(parent_a));
            assert!(
                tokio::time::timeout(Duration::from_millis(30), &mut pending)
                    .await
                    .is_err()
            );
            tip_sender.set_best_non_finalized_tip(make_tip(parent_b));
            let new_tip_read = read_state.expect_request(ReadRequest::ChainInfo).await;
            // Record a rejection while the tip-wakeup branch is fetching B's context.
            assert!(rpc.select_mining_template_parent(parent_b));
            rpc.gbt.template_rejections.send_modify(|state| {
                state.reject(parent_b, "invalid-b-work");
            });
            new_tip_read.respond(ReadResponse::ChainInfo(make_info(parent_b)));
            let fallback = verifier.expect_request_that(|request| matches!(request, zakura_consensus::Request::Prepare { block, .. } if block.header.previous_block_hash == parent_b)).await;
            let mut parent_c = Hash([0; 32]);
            parent_c.0[0] = 3;
            tip_sender.set_best_non_finalized_tip(make_tip(parent_c));
            fallback.respond_error("proposal parent was superseded".into());
            read_state
                .expect_request(ReadRequest::ChainInfo)
                .await
                .respond(ReadResponse::ChainInfo(make_info(parent_c)));
            mempool
                .expect_request(mempool::Request::FullTransactions)
                .await
                .respond(make_mempool(parent_c));
            let replacement = tokio::time::timeout(Duration::from_secs(2), pending)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .try_into_template()
                .unwrap();
            assert_eq!(replacement.previous_block_hash, parent_c);
            assert_eq!(replacement.height, height.0 + 1);
            assert_eq!(replacement.submit_old, Some(false));
            preparation
                .take()
                .unwrap()
                .respond_error("old parent preparation cancelled".into());
            queue.abort();
        }
    }
}
