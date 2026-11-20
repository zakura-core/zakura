//! Duplicate verdicts for trusted submissions, settled against the committed block.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use tokio::time::{Duration, Instant};
use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};
use zakura_state::{KnownBlock, ReadRequest, ReadResponse};

use crate::methods::{
    submission_lifecycle::{
        committed_block_lookup, DuplicateBody, RelayPolicy, Submission, COMMITTED_BLOCK_WAIT,
    },
    types::submit_block::{MinedBlockSubmission, MinedSubmissions, PendingBlockRegistry},
};

/// State reports a duplicate while another copy is still in its write channel, then that copy
/// becomes readable `reads_before_commit` reads later, or never.
async fn resolve_duplicate(reads_before_commit: Option<usize>) -> (Option<DuplicateBody>, bool) {
    let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into()
        .expect("the genesis block is valid");
    let hash = block.hash();
    let reads = Arc::new(AtomicUsize::new(0));
    let committed = block.clone();
    let read_state = tower::service_fn({
        let reads = reads.clone();
        move |request: ReadRequest| {
            assert_eq!(request, ReadRequest::AnyChainBlock(hash.into()));
            let read = reads.fetch_add(1, Ordering::SeqCst);
            let block = reads_before_commit
                .is_some_and(|misses| read >= misses)
                .then(|| committed.clone().into());
            async move { Ok::<_, tower::BoxError>(ReadResponse::Block(block)) }
        }
    });
    let verifier = tower::service_fn(move |_request: zakura_consensus::Request| async move {
        Err::<zakura_chain::block::Hash, _>(tower::BoxError::from(
            zakura_consensus::RouterError::from(zakura_consensus::VerifyBlockError::from(
                zakura_consensus::BlockError::AlreadyInChain(hash, KnownBlock::WriteChannel),
            )),
        ))
    });
    let registry = PendingBlockRegistry::default();
    let registration = registry.insert(block.clone()).unwrap();
    let permit = registry.get_for_peer(hash).unwrap().pending_send.unwrap();
    let (sender, _events) = tokio::sync::mpsc::unbounded_channel();
    let started = Instant::now();
    let duplicate_body = Submission {
        block: block.clone(),
        hash,
        height: block.coinbase_height().unwrap(),
        size: 0,
        submitted_at: std::time::Instant::now(),
        method: MinedBlockSubmission::TrustedBlock,
        reservation: MinedSubmissions::default().reserve(hash).unwrap(),
    }
    .spawn(
        verifier,
        sender,
        RelayPolicy::TrustedSubmission {
            registration,
            early_relay: false,
            committed_block: committed_block_lookup(read_state),
        },
        |outcome| outcome.duplicate_body,
    )
    .await
    .unwrap();
    if reads_before_commit.is_none() {
        assert!(started.elapsed() >= COMMITTED_BLOCK_WAIT - Duration::from_millis(50));
    } else {
        assert!(started.elapsed() < COMMITTED_BLOCK_WAIT);
    }
    assert!(registry.get(hash).is_none());
    (duplicate_body, permit.try_start_write(|| ()).is_some())
}

#[tokio::test(start_paused = true)]
async fn identical_duplicate_waits_for_the_other_copy_to_commit_and_keeps_its_sends() {
    assert_eq!(
        resolve_duplicate(Some(3)).await,
        (Some(DuplicateBody::Identical), true)
    );
}

#[tokio::test(start_paused = true)]
async fn duplicate_whose_other_copy_never_commits_is_withdrawn_at_the_deadline() {
    assert_eq!(
        resolve_duplicate(None).await,
        (Some(DuplicateBody::Unconfirmed), false)
    );
}

/// Both an in-flight hash and an unavailable hash lookup suppress early peer access.
/// A verifier duplicate with no readable committed copy is retryable in either case.
#[tokio::test(start_paused = true)]
async fn trusted_rpc_keeps_uncertain_hashes_private_and_reports_an_unconfirmed_duplicate() {
    use crate::tests::test_rpc::TestRpcImpl;
    use zakura_chain::{chain_tip::NoChainTip, parameters::Network};
    use zakura_test::mock_service::MockService;

    let bytes = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES.to_vec();
    let hash = Network::Mainnet.genesis_hash();
    for in_flight in [true, false] {
        let mempool: MockService<_, _, _, tower::BoxError> = MockService::build().for_unit_tests();
        let mut verifier: MockService<_, _, _, tower::BoxError> =
            MockService::build().for_unit_tests();
        let state = tower::service_fn(move |request| async move {
            assert!(matches!(request, zakura_state::Request::KnownBlock(h) if h == hash));
            if in_flight {
                Ok(zakura_state::Response::KnownBlock(Some(
                    KnownBlock::WriteChannel,
                )))
            } else {
                Err(tower::BoxError::from("hash lookup unavailable"))
            }
        });
        let read = tower::service_fn(|request| async move {
            Ok::<_, tower::BoxError>(match request {
                ReadRequest::BlockInfo(_) => ReadResponse::BlockInfo(Some(Default::default())),
                ReadRequest::AnyChainBlock(_) => ReadResponse::Block(None),
                other => panic!("unexpected state request {other:?}"),
            })
        });
        let (_logs, log_rx) = tokio::sync::watch::channel(None);
        let (events, mut early) = tokio::sync::mpsc::unbounded_channel();
        let (rpc, queue) = TestRpcImpl::new_for_tests(
            Network::Mainnet,
            Default::default(),
            false,
            "test",
            "test",
            tower::buffer::Buffer::new(mempool, 1),
            state,
            read,
            verifier.clone(),
            Default::default(),
            NoChainTip,
            Default::default(),
            log_rx,
            Some(events),
        );
        let pending = rpc.gbt.pending_blocks();
        let bytes = bytes.clone();
        let result = tokio::spawn(async move { rpc.submit_trusted_block(&bytes).await });
        let verification = verifier
            .expect_request_that(|req| matches!(req, zakura_consensus::Request::CommitMined { .. }))
            .await;
        let resolution = pending
            .resolution(hash)
            .expect("local gossip can await verification");
        assert!(pending.get_for_peer(hash).is_none());
        verification.respond_error(tower::BoxError::from(zakura_consensus::RouterError::from(
            zakura_consensus::VerifyBlockError::from(zakura_consensus::BlockError::AlreadyInChain(
                hash,
                KnownBlock::WriteChannel,
            )),
        )));
        let Ok(response) = result.await.unwrap() else {
            panic!("a complete submission within capacity returns a verification outcome")
        };
        assert_eq!(
            crate::server::trusted_block::outcome(&response),
            "inconclusive"
        );
        assert!(!resolution.committed().await);
        assert!(early.try_recv().is_err());
        assert!(pending.resolution(hash).is_none());
        queue.abort();
    }
}
