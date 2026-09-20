//! Template generation across tip changes and concurrent callers.

use std::{sync::Arc, time::Duration};

use tower::buffer::Buffer;
use zakura_chain::{
    block::Hash,
    chain_sync_status::MockSyncStatus,
    chain_tip::ChainTip,
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
    StateBeforeNotice,
    NoticeDuringRead,
    NoticeWhileWaiting,
    StalePreparation,
    ConcurrentParentChange,
    StaleParentSelection,
    StaleFallback { success: bool, notice_lags: bool },
    FastPathFallback,
}

#[tokio::test]
async fn mining_template_state_before_tip_notice_recovers_without_next_block() {
    check_interleaving(Interleaving::StateBeforeNotice).await;
}

#[tokio::test]
async fn mining_template_notice_during_read_and_mempool_lag_recovers_without_next_block() {
    check_interleaving(Interleaving::NoticeDuringRead).await;
}

#[tokio::test]
async fn mining_template_waiting_longpoll_retries_read_and_mempool_without_next_block() {
    check_interleaving(Interleaving::NoticeWhileWaiting).await;
}

#[tokio::test]
async fn mining_template_stale_preparation_does_not_withdraw_and_same_height_reorg_recovers() {
    check_interleaving(Interleaving::StalePreparation).await;
}

#[tokio::test]
async fn mining_template_concurrent_call_retries_changed_parent() {
    check_interleaving(Interleaving::ConcurrentParentChange).await;
}

#[tokio::test]
async fn mining_template_stale_caller_preserves_new_parent_rejections() {
    check_interleaving(Interleaving::StaleParentSelection).await;
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

    if matches!(interleaving, Interleaving::ConcurrentParentChange) {
        // Caller A has passed the agreement guard and selected parent A, but its mempool
        // response has not completed. Hold that response while caller B serves the new tip.
        let old_caller = tokio::spawn({
            let rpc = rpc.clone();
            async move { rpc.get_block_template(None).await }
        });
        read_state
            .expect_request(ReadRequest::ChainInfo)
            .await
            .respond(ReadResponse::ChainInfo(make_info(parent_a)));
        let old_mempool = mempool
            .expect_request(mempool::Request::FullTransactions)
            .await;
        assert_eq!(rpc.gbt.template_rejections.borrow().parent, Some(parent_a));

        tip_sender.set_best_non_finalized_tip(make_tip(parent_b));
        let new_caller = tokio::spawn({
            let rpc = rpc.clone();
            async move { rpc.get_block_template(None).await }
        });
        read_state
            .expect_request(ReadRequest::ChainInfo)
            .await
            .respond(ReadResponse::ChainInfo(make_info(parent_b)));
        mempool
            .expect_request(mempool::Request::FullTransactions)
            .await
            .respond(make_mempool(parent_b));
        let replacement = tokio::time::timeout(Duration::from_secs(2), new_caller)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .try_into_template()
            .unwrap();
        assert_eq!(replacement.previous_block_hash, parent_b);
        assert_eq!(rpc.gbt.template_rejections.borrow().parent, Some(parent_b));

        old_mempool.respond(make_mempool(parent_a));
        read_state
            .expect_request(ReadRequest::ChainInfo)
            .await
            .respond(ReadResponse::ChainInfo(make_info(parent_b)));
        mempool
            .expect_request(mempool::Request::FullTransactions)
            .await
            .respond(make_mempool(parent_b));
        let retried = tokio::time::timeout(Duration::from_secs(2), old_caller)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .try_into_template()
            .unwrap();
        assert_eq!(retried.previous_block_hash, parent_b);
        assert_eq!(retried.height, height.0 + 1);
        assert_eq!(tip.best_tip_height_and_hash(), Some((height, parent_b)));
        assert!(!rpc.gbt.template_rejections.borrow().needs_fallback());
        preparation
            .take()
            .unwrap()
            .respond_error("old parent preparation cancelled".into());
        queue.abort();
        return;
    }

    if matches!(interleaving, Interleaving::StaleParentSelection) {
        tip_sender.set_best_non_finalized_tip(make_tip(parent_b));
        assert!(rpc.select_mining_template_parent(parent_b));
        rpc.gbt.template_rejections.send_modify(|state| {
            state.reject(parent_b, "invalid-new-parent-work");
            state.mark_prepared(parent_b, "validated-replacement");
        });
        let revision = rpc.gbt.template_rejections.borrow().revision;
        assert!(!rpc.select_mining_template_parent(parent_a));
        let state = rpc.gbt.template_rejections.borrow();
        assert_eq!(state.parent, Some(parent_b));
        assert_eq!(state.revision, revision);
        assert!(state.contains("invalid-new-parent-work"));
        assert!(state.is_prepared("validated-replacement"));
        assert!(state.needs_fallback());
        drop(state);
        preparation
            .take()
            .unwrap()
            .respond_error("old parent preparation cancelled".into());
        queue.abort();
        return;
    }

    if let Interleaving::StaleFallback {
        success,
        notice_lags,
    } = interleaving
    {
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

    if matches!(interleaving, Interleaving::StalePreparation) {
        // Deliver the exact state error while the tip notification still reports A. The next
        // preparation request proves the background worker consumed and classified that error.
        preparation.take().unwrap().respond_error(Box::new(zakura_consensus::RouterError::Block {
            source: Box::new(zakura_consensus::VerifyBlockError::ValidateProposal(
                "proposal is not based on the current best chain tip: previous block hash must be the best chain tip".into(),
            )),
        }));
        let retry = tokio::spawn({
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
        tokio::time::timeout(Duration::from_secs(2), retry)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        preparation = Some(
            verifier
                .expect_request_that(|request| {
                    matches!(request, zakura_consensus::Request::Prepare { .. })
                })
                .await,
        );
        assert!(!rpc.mining_template_rejected(&first.work_id));
        assert!(!rpc.gbt.template_rejections.borrow().needs_fallback());
        assert_eq!(rpc.gbt.template_rejections.borrow().revision, 0);
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
        Interleaving::ConcurrentParentChange
        | Interleaving::StaleParentSelection
        | Interleaving::StaleFallback { .. } => unreachable!("handled before the long-poll cases"),
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
            return;
        }
        Interleaving::StateBeforeNotice | Interleaving::StalePreparation => {
            // The state publishes B before the tip watch does. The first agreement guard must
            // retry, then finish when that same publication completes, without a later block.
            read_state
                .expect_request(ReadRequest::ChainInfo)
                .await
                .respond(ReadResponse::ChainInfo(make_info(parent_b)));
            let retry = read_state.expect_request(ReadRequest::ChainInfo).await;
            assert_eq!(tip.best_tip_hash(), Some(parent_a));
            tip_sender.set_best_non_finalized_tip(make_tip(parent_b));
            retry.respond(ReadResponse::ChainInfo(make_info(parent_b)));
        }
        Interleaving::NoticeDuringRead => {
            let old_snapshot = read_state.expect_request(ReadRequest::ChainInfo).await;
            tip_sender.set_best_non_finalized_tip(make_tip(parent_b));
            old_snapshot.respond(ReadResponse::ChainInfo(make_info(parent_a)));
            read_state
                .expect_request(ReadRequest::ChainInfo)
                .await
                .respond(ReadResponse::ChainInfo(make_info(parent_b)));
        }
        Interleaving::NoticeWhileWaiting => {
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
            read_state
                .expect_request(ReadRequest::ChainInfo)
                .await
                .respond(ReadResponse::ChainInfo(make_info(parent_a)));
            read_state
                .expect_request(ReadRequest::ChainInfo)
                .await
                .respond(ReadResponse::ChainInfo(make_info(parent_b)));
        }
    }

    if matches!(
        interleaving,
        Interleaving::NoticeDuringRead | Interleaving::NoticeWhileWaiting
    ) {
        // Hold the mempool at A for two complete retries. It then catches up without any new
        // tip notification. This models a transient mismatch, not an invented permanent stall.
        for _ in 0..2 {
            mempool
                .expect_request(mempool::Request::FullTransactions)
                .await
                .respond(make_mempool(parent_a));
            read_state
                .expect_request(ReadRequest::ChainInfo)
                .await
                .respond(ReadResponse::ChainInfo(make_info(parent_b)));
        }
    }
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
    assert_eq!(replacement.submit_old, Some(false));
    assert_eq!(tip.best_tip_height_and_hash(), Some((height, parent_b)));
    assert!(!rpc.gbt.template_rejections.borrow().needs_fallback());
    assert_ne!(first.long_poll_id, replacement.long_poll_id);
    preparation
        .unwrap()
        .respond_error("old parent preparation cancelled".into());
    queue.abort();
}
