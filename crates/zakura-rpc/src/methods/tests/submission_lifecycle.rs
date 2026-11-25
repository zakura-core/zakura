//! Trusted submission outcomes, pending-body withdrawal, and duplicate resolution.

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
            early_relay: None,
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

#[tokio::test(start_paused = true)]
async fn trusted_rpc_withdraws_on_verifier_unavailability_and_accepts_a_retry() {
    use crate::tests::test_rpc::TestRpcImpl;
    use zakura_chain::{chain_tip::NoChainTip, parameters::Network};
    use zakura_consensus::{RouterError, VerifyBlockError, VerifyCheckpointError};
    use zakura_test::mock_service::MockService;

    let hash = Network::Mainnet.genesis_hash();
    let mut verifier: MockService<_, _, _, tower::BoxError> = MockService::build().for_unit_tests();
    let mempool: MockService<_, _, _, tower::BoxError> = MockService::build().for_unit_tests();
    let (events, mut early) = tokio::sync::mpsc::unbounded_channel();
    let (rpc, queue) = TestRpcImpl::new_for_tests(
        Network::Mainnet,
        Default::default(),
        false,
        "test",
        "test",
        tower::buffer::Buffer::new(mempool, 1),
        tower::service_fn(move |req| async move {
            assert_eq!(req, zakura_state::Request::KnownBlock(hash));
            Ok(zakura_state::Response::KnownBlock(None))
        }),
        tower::service_fn(|req| async move {
            assert!(matches!(req, ReadRequest::BlockInfo(_)));
            Ok(ReadResponse::BlockInfo(Some(Default::default())))
        }),
        verifier.clone(),
        Default::default(),
        NoChainTip,
        Default::default(),
        tokio::sync::watch::channel(None).1,
        Some(events),
    );
    let pending = rpc.gbt.pending_blocks();
    let errors: [tower::BoxError; 6] = [
        RouterError::from(VerifyBlockError::Depth {
            source: "parent lookup failed".into(),
            hash,
        })
        .into(),
        RouterError::from(VerifyCheckpointError::Tip("state unavailable".into())).into(),
        RouterError::from(VerifyCheckpointError::CommitCheckpointVerified(
            "checkpoint storage unavailable".into(),
        ))
        .into(),
        RouterError::from(VerifyBlockError::Transaction(
            zakura_consensus::error::TransactionError::StateUnavailable(
                "transaction verifier state unavailable".into(),
            ),
        ))
        .into(),
        RouterError::from(VerifyBlockError::Commit(
            zakura_state::CommitBlockError::HeaderChainError {
                error: "header store unavailable".into(),
            },
        ))
        .into(),
        "verifier buffer closed".into(),
    ];
    for result in errors.into_iter().map(Err).chain([Ok(hash)]) {
        let rpc = rpc.clone();
        let response = tokio::spawn(async move {
            rpc.submit_trusted_block(&zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES)
                .await
        });
        let request = verifier
            .expect_request_that(|req| matches!(req, zakura_consensus::Request::CommitMined { .. }))
            .await;
        let event = tokio::time::timeout(Duration::from_secs(1), early.recv())
            .await
            .expect("early relay starts before verification completes")
            .expect("relay channel is open");
        let permit = event.pending.send_permit();
        let accepted = result.is_ok();
        request.respond(result);
        let Ok(response) = response.await.unwrap() else {
            panic!("a complete request returns a verdict")
        };
        assert_eq!(
            crate::server::trusted_block::outcome(&response),
            if accepted { "accepted" } else { "inconclusive" }
        );
        assert_eq!(permit.try_start_write(|| ()).is_some(), accepted);
        assert!(pending.resolution(hash).is_none());
    }
    queue.abort();
}

#[test]
fn a_failed_header_attribution_still_rejects_the_exact_invalid_submission() {
    use crate::tests::test_rpc::TestRpcImpl;
    use zakura_chain::parameters::Network;
    use zakura_consensus::{BlockError, RouterError, VerifyBlockError};

    let hash = Network::Mainnet.genesis_hash();
    let error = VerifyBlockError::AuthDataUnconfirmed {
        parent: hash,
        source: Box::new(BlockError::DuplicateTransaction.into()),
    };
    let response = TestRpcImpl::mined_block_response(
        Err(RouterError::from(error).into()),
        hash,
        zakura_chain::block::Height(1),
        MinedBlockSubmission::TrustedBlock,
    )
    .unwrap();
    assert_eq!(crate::server::trusted_block::outcome(&response), "rejected");
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

/// What a [`RecordingBpv1Proposer`] saw, in call order.
#[derive(Debug, PartialEq, Eq)]
enum Bpv1Call {
    /// `publish_solved_block`, and whether the early announcement was already queued.
    Publish { announced_first: bool },
    /// `retire_failed_block`.
    Retire,
}

/// Records BPv1 hook calls and whether each publish came after the early announcement.
#[derive(Debug)]
struct RecordingBpv1Proposer {
    early: std::sync::Mutex<
        tokio::sync::mpsc::UnboundedReceiver<crate::methods::types::submit_block::EarlyMinedBlock>,
    >,
    calls: std::sync::Mutex<Vec<Bpv1Call>>,
}

impl zakura_node_services::bpv1_proposer::Bpv1ProposerHook for RecordingBpv1Proposer {
    fn prepare_template_commitment(
        &self,
        _light_client_root: [u8; 32],
        _height: zakura_chain::block::Height,
        _tx_count: u32,
        _body_after_coinbase: &[u8],
    ) -> Option<[u8; zakura_node_services::bpv1_proposer::BPV1_COMMITMENT_NULL_DATA_LEN]> {
        None
    }

    fn empty_template_commitment(
        &self,
        _height: zakura_chain::block::Height,
    ) -> Option<[u8; zakura_node_services::bpv1_proposer::BPV1_COMMITMENT_NULL_DATA_LEN]> {
        None
    }

    fn publish_solved_block(
        &self,
        _block: Arc<Block>,
        _height: zakura_chain::block::Height,
    ) -> bool {
        let announced_first = self.early.lock().unwrap().try_recv().is_ok();
        self.calls
            .lock()
            .unwrap()
            .push(Bpv1Call::Publish { announced_first });
        true
    }

    fn retire_failed_block(&self, _hash: zakura_chain::block::Hash) {
        self.calls.lock().unwrap().push(Bpv1Call::Retire);
    }
}

/// Submits a mined block whose state admission authorizes early relay, then
/// answers its commit with `verdict`. Returns the BPv1 hook calls.
async fn bpv1_calls_after_admission(
    verdict: fn(zakura_chain::block::Hash) -> Result<zakura_chain::block::Hash, tower::BoxError>,
) -> Vec<Bpv1Call> {
    let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .zcash_deserialize_into()
        .expect("the genesis block is valid");
    let hash = block.hash();
    let (sender, early) = tokio::sync::mpsc::unbounded_channel();
    let bpv1 = Arc::new(RecordingBpv1Proposer {
        early: std::sync::Mutex::new(early),
        calls: Default::default(),
    });
    let verifier = tower::service_fn(move |request: zakura_consensus::Request| async move {
        let zakura_consensus::Request::CommitMined { admission, .. } = request else {
            unreachable!("a mined submission commits through CommitMined");
        };
        admission.authorize_optimistic_relay();
        admission.admit_for_test();
        // The writer commits after the submission observes admission.
        tokio::time::sleep(Duration::from_secs(1)).await;
        verdict(hash)
    });
    Submission {
        block: block.clone(),
        hash,
        height: block.coinbase_height().unwrap(),
        size: 0,
        submitted_at: std::time::Instant::now(),
        method: MinedBlockSubmission::FullBlock,
        reservation: MinedSubmissions::default().reserve(hash).unwrap(),
    }
    .spawn(
        verifier,
        sender,
        RelayPolicy::AfterStateAdmission {
            pending_blocks: PendingBlockRegistry::default(),
            optimistic_block_inventory: true,
            bpv1_proposer: Some(bpv1.clone()),
        },
        |_| (),
    )
    .await
    .unwrap();
    let calls = std::mem::take(&mut *bpv1.calls.lock().unwrap());
    calls
}

/// BPv1 publishes after the early announcement is queued, so its serialization
/// never delays that announcement. A commit that fails after BPv1 published
/// retires the block; a commit that succeeds, or a duplicate verdict for a
/// block already in state, keeps it relaying.
#[tokio::test(start_paused = true)]
async fn bpv1_publishes_after_the_early_announcement_and_retires_a_failed_commit() {
    let published = Bpv1Call::Publish {
        announced_first: true,
    };
    assert_eq!(
        bpv1_calls_after_admission(|_| Err("the write queue rejected the block".into())).await,
        [
            Bpv1Call::Publish {
                announced_first: true
            },
            Bpv1Call::Retire
        ],
    );
    assert_eq!(bpv1_calls_after_admission(Ok).await, [published]);
    assert_eq!(
        bpv1_calls_after_admission(|hash| Err(tower::BoxError::from(
            zakura_consensus::RouterError::from(zakura_consensus::VerifyBlockError::from(
                zakura_consensus::BlockError::AlreadyInChain(hash, KnownBlock::BestChain),
            )),
        )))
        .await,
        [Bpv1Call::Publish {
            announced_first: true
        }],
    );
}
