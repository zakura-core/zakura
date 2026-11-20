//! The second request for a gossiped block, sent when the peer set expects another announcer to
//! deliver it sooner.
//!
//! The network service hands each request to the test with a channel for its answer, so the test
//! decides when, and how, each request is answered. The download asks for a second request
//! once, and the peer set holds that request until it sends one. Leaving it unanswered stands
//! for a peer set that has not sent one yet. Answering it with [`zn::NoFasterBlockSource`]
//! stands for a window that closed without one. Time is paused.

use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tower::{service_fn, util::BoxCloneService};

use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};

use super::*;

type TestNetwork = BoxCloneService<zn::Request, zn::Response, BoxError>;

/// Long enough on the paused clock for a request to be sent, or for a finished download to
/// return.
const SOON: Duration = Duration::from_millis(10);

/// A request the network service received, and the channel that answers it.
struct Sent {
    request: zn::Request,
    answer: oneshot::Sender<Result<zn::Response, BoxError>>,
}

impl Sent {
    /// Answers a held second request as a peer set whose window closed without sending one.
    fn window_closed(self) {
        assert!(
            matches!(self.request, zn::Request::BlockByHashHedge { .. }),
            "only a second request can end without a faster source: {:?}",
            self.request
        );
        self.answer
            .send(Err(zn::NoFasterBlockSource.into()))
            .expect("the download is waiting for its answer");
    }
}

fn network() -> (TestNetwork, mpsc::UnboundedReceiver<Sent>) {
    let (sent_tx, sent_rx) = mpsc::unbounded_channel();
    let network = BoxCloneService::new(service_fn(move |request| {
        let sent_tx = sent_tx.clone();
        async move {
            let (answer, answered) = oneshot::channel();
            sent_tx
                .send(Sent { request, answer })
                .map_err(|_| "test dropped the request receiver")?;
            answered.await.map_err(|_| "test dropped the answer")?
        }
    }));
    (network, sent_rx)
}

fn body(bytes: &[u8]) -> Arc<Block> {
    bytes.zcash_deserialize_into().expect("valid block vector")
}

async fn test_download<ZN>(
    network: ZN,
    hash: block::Hash,
    source: Option<zn::PeerSource>,
    hedge: bool,
) -> Result<BlockCandidate, DownloadError>
where
    ZN: Service<zn::Request, Response = zn::Response, Error = BoxError> + Send + Clone + 'static,
    ZN::Future: Send,
{
    let (_, tip, _) = zs::ChainTipSender::new(None, &Network::Mainnet);
    let checks = CandidateChecks {
        tip,
        misbehavior: crate::components::test_misbehavior::reporter(),
        kind: DownloadKind::Advertised,
    };
    download_block(network, hash, source, hedge, checks)
        .await
        .map(|candidates| candidates.first)
}

fn hash() -> block::Hash {
    body(&zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES).hash()
}

fn first_peer() -> PeerSocketAddr {
    "127.0.0.1:8233".parse().unwrap()
}

fn second_peer() -> PeerSocketAddr {
    "127.0.0.2:8233".parse().unwrap()
}

fn available(bytes: &[u8], peer: PeerSocketAddr) -> zn::Response {
    zn::Response::Blocks(vec![zn::InventoryResponse::Available((
        body(bytes).into(),
        Some(peer),
    ))])
}

fn assert_copy((block, source, _): BlockCandidate, peer: PeerSocketAddr) {
    assert_eq!(block.hash(), hash());
    assert_eq!(source, Some(peer));
}

fn first_request() -> zn::Request {
    zn::Request::BlocksByHash([hash()].into_iter().collect())
}

fn from_first() -> zn::Response {
    available(
        &zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES,
        first_peer(),
    )
}

fn from_second() -> zn::Response {
    available(
        &zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES,
        second_peer(),
    )
}

/// Drive the two dispatched exchanges independently of a controlled verifier.
#[tokio::test(start_paused = true)]
async fn verification_can_commit_the_already_requested_copy_after_a_payload_rejection() {
    use futures::StreamExt;
    use zakura_consensus::{BlockError, RouterError, VerifyBlockError, VerifyCheckpointError};

    let bad_root = || {
        RouterError::from(VerifyBlockError::from(BlockError::BadMerkleRoot {
            actual: block::merkle::Root([0; 32]),
            expected: block::merkle::Root([1; 32]),
        }))
    };
    let semantic_duplicate =
        RouterError::from(VerifyBlockError::from(BlockError::DuplicateTransaction));
    let checkpoint_duplicate = RouterError::from(VerifyCheckpointError::DuplicateTransaction);
    let unrelated = RouterError::from(VerifyBlockError::from(BlockError::NoTransactions));
    enum Expected {
        CommitOther,
        ReturnFirst,
        RetryHeight { arrived_first: bool },
    }
    use Expected::*;
    let cases: [(bool, BoxError, Expected); 9] = [
        (false, bad_root().into(), CommitOther),
        (true, bad_root().into(), CommitOther),
        (false, semantic_duplicate.into(), CommitOther),
        (true, checkpoint_duplicate.into(), CommitOther),
        (false, unrelated.into(), ReturnFirst),
        (true, "verifier unavailable".into(), ReturnFirst),
        (
            false,
            bad_root().into(),
            RetryHeight {
                arrived_first: false,
            },
        ),
        (
            false,
            bad_root().into(),
            RetryHeight {
                arrived_first: true,
            },
        ),
        (
            true,
            bad_root().into(),
            RetryHeight {
                arrived_first: true,
            },
        ),
    ];
    for (hedge_first, error, expected) in cases {
        let (network, mut sent) = network();
        let (commits, mut verification) = mpsc::unbounded_channel();
        let verifier = service_fn(move |request| {
            let (reply, decision) = oneshot::channel::<Result<block::Hash, BoxError>>();
            commits.send((request, reply)).unwrap();
            async { decision.await.unwrap() }
        });
        let state = service_fn(|request| async move {
            Ok(match request {
                zs::Request::AnyChainBlock(_) => zs::Response::Block(None),
                zs::Request::KnownBlock(_) => {
                    zs::Response::KnownBlock(Some(zs::KnownBlock::BestChain))
                }
                _ => panic!("unexpected state request"),
            })
        });
        let (mut tip_sender, tip, _) = zs::ChainTipSender::new(None, &Network::Mainnet);
        let reporter = crate::components::test_misbehavior::reporter();
        let mut downloads = Downloads::new(
            reporter.clone(),
            MAX_INBOUND_CONCURRENCY,
            false,
            network,
            verifier,
            state,
            tip,
            Network::Mainnet,
        )
        .with_second_block_request(true);
        assert_eq!(
            downloads.download_and_verify(hash(), None),
            DownloadAction::AddedToQueue
        );
        let primary = next_sent(&mut sent).await;
        let secondary = next_dispatched(&mut sent).await;
        let (early, early_peer, late, late_peer) = if hedge_first {
            (secondary, second_peer(), primary, first_peer())
        } else {
            (primary, first_peer(), secondary, second_peer())
        };
        let bytes = &zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES;
        let reply = |block: &Arc<Block>, peer| {
            zn::Response::Blocks(vec![zn::InventoryResponse::Available((
                block.clone().into(),
                Some(peer),
            ))])
        };
        let first_body = body(bytes);
        let other_body = body(bytes);
        let mut other_answer = Some(late.answer);
        if matches!(
            expected,
            RetryHeight {
                arrived_first: true
            }
        ) {
            super::tests::set_tip(
                &mut tip_sender,
                block::Height(5),
                other_body.header.previous_block_hash,
            );
            other_answer
                .take()
                .unwrap()
                .send(Ok(reply(&other_body, late_peer)))
                .unwrap();
            tokio::time::sleep(SOON).await;
            assert!(reporter.is_banned(late_peer.ip()));
            super::tests::set_tip(&mut tip_sender, block::Height(6), block::Hash([6; 32]));
        }
        early
            .answer
            .send(Ok(reply(&first_body, early_peer)))
            .unwrap();
        let (request, reject) = tokio::time::timeout(SOON, verification.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(request, zakura_consensus::Request::Commit(BlockHandle::Shared(body))
                if Arc::ptr_eq(&body, &first_body))
        );
        if matches!(
            expected,
            RetryHeight {
                arrived_first: false
            }
        ) {
            super::tests::set_tip(
                &mut tip_sender,
                block::Height(5),
                other_body.header.previous_block_hash,
            );
        }
        if let Some(answer) = other_answer {
            answer.send(Ok(reply(&other_body, late_peer))).unwrap();
        }
        tokio::task::yield_now().await;
        assert!(verification.try_recv().is_err());
        let score = error
            .downcast_ref::<RouterError>()
            .map_or(0, RouterError::misbehavior_score);
        let message = error.to_string();
        reject.send(Err(error)).unwrap();
        match expected {
            CommitOther => {
                let (request, accept) = tokio::time::timeout(SOON, verification.recv())
                    .await
                    .unwrap()
                    .unwrap();
                assert!(
                    matches!(request, zakura_consensus::Request::Commit(BlockHandle::Shared(body))
                    if Arc::ptr_eq(&body, &other_body))
                );
                accept.send(Ok(hash())).unwrap();
                assert_eq!(downloads.next().await.unwrap().unwrap(), hash());
                assert_eq!(
                    reporter.is_banned(early_peer.ip()),
                    score >= zn::constants::MAX_PEER_MISBEHAVIOR_SCORE
                );
                assert!(!reporter.is_banned(late_peer.ip()));
            }
            ReturnFirst => {
                let (error, source) = downloads.next().await.unwrap().unwrap_err();
                assert_eq!(error.to_string(), message);
                assert_eq!(source, Some(early_peer));
            }
            RetryHeight { .. } => {
                let (error, source) = downloads.next().await.unwrap().unwrap_err();
                let mismatch = error
                    .downcast_ref::<GossipedTipChildHeightMismatch>()
                    .expect("inbound receives the bounded retry signal");
                assert_eq!(source, Some(late_peer));
                assert_eq!(mismatch.hash, hash());
                assert!(!mismatch.parent_recovery);
                assert!(
                    reporter.is_banned(early_peer.ip()),
                    "the discarded commitment error is attributed"
                );
                assert!(
                    reporter.is_banned(late_peer.ip()),
                    "the height error is attributed at receipt"
                );
            }
        }
        assert!(verification.try_recv().is_err());
        assert_nothing_more_sent(
            &mut sent,
            "verification uses only the two original requests",
        )
        .await;
    }
}

/// Waits for the next request, which the download sends without waiting.
async fn next_sent(sent: &mut mpsc::UnboundedReceiver<Sent>) -> Sent {
    tokio::time::timeout(SOON * 2, sent.recv())
        .await
        .expect("a request is sent promptly")
        .expect("the network service is alive")
}

/// Simulates the peer set sending the held second request to a peer.
async fn next_dispatched(sent: &mut mpsc::UnboundedReceiver<Sent>) -> Sent {
    let held = next_sent(sent).await;
    let zn::Request::BlockByHashHedge { ref attempt, .. } = held.request else {
        panic!("expected a second request");
    };
    assert!(attempt.claim_dispatch());
    held
}

/// Waits past the whole second-request window, and asserts nothing further was sent.
async fn assert_nothing_more_sent(sent: &mut mpsc::UnboundedReceiver<Sent>, why: &str) {
    tokio::time::sleep(zn::BlockHedge::WINDOW * 2).await;
    assert!(sent.try_recv().is_err(), "{why}");
}

#[tokio::test(start_paused = true)]
async fn without_second_requests_only_the_first_request_is_sent() {
    let (network, mut sent) = network();
    let download = tokio::spawn(test_download(network, hash(), None, false));

    let first = next_sent(&mut sent).await;
    assert_eq!(first.request, first_request());
    assert_nothing_more_sent(&mut sent, "nothing is asked when second requests are off").await;

    first.answer.send(Ok(from_first())).unwrap();
    assert_copy(download.await.unwrap().unwrap(), first_peer());
}

/// The download does not ask again on a timer: the peer set holds the one request and decides
/// when to send it.
#[tokio::test(start_paused = true)]
async fn the_peer_set_is_asked_for_a_second_request_once() {
    let (network, mut sent) = network();
    let download = tokio::spawn(test_download(network, hash(), None, true));

    let first = next_sent(&mut sent).await;
    assert!(
        matches!(first.request, zn::Request::BlockByHashPrimary { .. }),
        "the first request comes first: {:?}",
        first.request
    );
    let held = next_sent(&mut sent).await;
    let zn::Request::BlockByHashHedge {
        hash: requested,
        ref attempt,
    } = held.request
    else {
        panic!("expected a second request");
    };
    assert_eq!(requested, hash());
    assert!(
        attempt.elapsed_if_open().is_some(),
        "the peer set may still send it"
    );
    assert_nothing_more_sent(&mut sent, "the download asks once, however long it waits").await;

    first.answer.send(Ok(from_first())).unwrap();
    assert_copy(download.await.unwrap().unwrap(), first_peer());
    assert!(
        held.answer.is_closed(),
        "the unsent second request is withdrawn"
    );
}

#[tokio::test(start_paused = true)]
async fn a_second_request_that_answers_first_wins() {
    let (network, mut sent) = network();
    let download = tokio::spawn(test_download(network, hash(), None, true));

    let first = next_sent(&mut sent).await;
    // The peer set sent this one, so it answers with the block.
    let second = next_dispatched(&mut sent).await;

    second.answer.send(Ok(from_second())).unwrap();
    assert_copy(download.await.unwrap().unwrap(), second_peer());

    // The losing request still runs to completion, so its answer is received, not dropped.
    first
        .answer
        .send(Ok(from_first()))
        .expect("the losing request is still waiting for its answer");
}

#[tokio::test(start_paused = true)]
async fn after_a_second_request_is_sent_nothing_more_is_asked() {
    let (network, mut sent) = network();
    let download = tokio::spawn(test_download(network, hash(), None, true));

    let first = next_sent(&mut sent).await;
    let second = next_dispatched(&mut sent).await;
    assert_nothing_more_sent(&mut sent, "a block gets at most one second request").await;

    first.answer.send(Ok(from_first())).unwrap();
    assert_copy(download.await.unwrap().unwrap(), first_peer());
    second
        .answer
        .send(Ok(from_second()))
        .expect("the losing request is still waiting for its answer");
}

#[tokio::test(start_paused = true)]
async fn a_failed_second_request_waits_for_the_first() {
    let (network, mut sent) = network();
    let download = tokio::spawn(test_download(network, hash(), None, true));

    let first = next_sent(&mut sent).await;
    let second = next_dispatched(&mut sent).await;
    second
        .answer
        .send(Err("the second peer disconnected".into()))
        .unwrap();
    assert_nothing_more_sent(&mut sent, "a failed second request is not replaced").await;
    assert!(
        !download.is_finished(),
        "still waiting for the first request"
    );

    first.answer.send(Ok(from_first())).unwrap();
    assert_copy(download.await.unwrap().unwrap(), first_peer());
}

#[tokio::test(start_paused = true)]
async fn a_failed_first_request_withdraws_the_second_request_and_returns_its_error() {
    let (network, mut sent) = network();
    let download = tokio::spawn(test_download(network, hash(), None, true));

    let first = next_sent(&mut sent).await;
    let held = next_sent(&mut sent).await;
    first.answer.send(Err("peer disconnected".into())).unwrap();

    let error = tokio::time::timeout(SOON, download)
        .await
        .expect("the failed first request does not wait for an unsent second request")
        .unwrap()
        .unwrap_err();
    assert!(
        held.answer.is_closed(),
        "the unsent second request is withdrawn"
    );
    assert_eq!(error.0.to_string(), "peer disconnected");
    assert_eq!(error.1, None);
    assert_nothing_more_sent(&mut sent, "a failed download asks for nothing more").await;
}

#[tokio::test(start_paused = true)]
async fn a_window_that_closes_without_a_second_request_leaves_the_first_to_finish() {
    let (network, mut sent) = network();
    let download = tokio::spawn(test_download(network, hash(), None, true));

    let first = next_sent(&mut sent).await;
    next_sent(&mut sent).await.window_closed();
    assert_nothing_more_sent(&mut sent, "a closed window is not asked about again").await;
    assert!(
        !download.is_finished(),
        "still waiting for the first request"
    );

    first.answer.send(Ok(from_first())).unwrap();
    assert_copy(download.await.unwrap().unwrap(), first_peer());
}

#[tokio::test(start_paused = true)]
async fn completing_the_first_request_closes_the_window_of_an_unsent_second_request() {
    let (network, mut sent) = network();
    let download = tokio::spawn(test_download(network, hash(), None, true));
    let first = next_sent(&mut sent).await;
    let queued = next_sent(&mut sent).await;
    let zn::Request::BlockByHashHedge { ref attempt, .. } = queued.request else {
        panic!("expected a second request");
    };
    assert!(attempt.elapsed_if_open().is_some());

    first.answer.send(Ok(from_first())).unwrap();
    assert_copy(download.await.unwrap().unwrap(), first_peer());
    assert!(attempt.elapsed_if_open().is_none());
    assert!(
        queued.answer.is_closed(),
        "the unsent second request is withdrawn"
    );
    assert_nothing_more_sent(&mut sent, "a finished download asks for nothing more").await;
}

#[tokio::test(start_paused = true)]
async fn a_failed_first_request_waits_for_an_already_dispatched_second_request() {
    let (network, mut sent) = network();
    let mut download = tokio::spawn(test_download(network, hash(), None, true));
    let first = next_sent(&mut sent).await;
    let second = next_dispatched(&mut sent).await;

    first.answer.send(Err("peer disconnected".into())).unwrap();
    assert!(
        tokio::time::timeout(SOON, &mut download).await.is_err(),
        "the dispatched second request is still outstanding"
    );

    second.answer.send(Ok(from_second())).unwrap();
    assert_copy(download.await.unwrap().unwrap(), second_peer());
}

/// A native fallback can return an all-missing inventory as a successful service response.
/// Other unusable responses must also leave an already-dispatched copy eligible to win.
#[tokio::test(start_paused = true)]
async fn a_reply_without_the_requested_body_cannot_win() {
    let missing = zn::InventoryResponse::Missing(hash());
    for response in [
        zn::Response::Blocks(vec![missing.clone()]),
        zn::Response::Blocks(vec![]),
        zn::Response::Blocks(vec![missing.clone(), missing]),
        available(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES, first_peer()),
        zn::Response::Nil,
    ] {
        let (network, mut sent) = network();
        let mut download = tokio::spawn(test_download(network, hash(), None, true));
        let first = next_sent(&mut sent).await;
        let second = next_dispatched(&mut sent).await;
        first.answer.send(Ok(response)).unwrap();
        assert!(
            tokio::time::timeout(SOON, &mut download).await.is_err(),
            "the other request can still deliver"
        );
        second.answer.send(Ok(from_second())).unwrap();
        assert_copy(download.await.unwrap().unwrap(), second_peer());
    }
}

/// Apply each local height limit to an ordinary, immutable block using a simulated tip.
/// The alternate is dispatched before the rejection and must pass the window independently.
#[tokio::test(start_paused = true)]
async fn a_height_window_rejection_uses_the_retained_copy_once() {
    use futures::StreamExt;

    let bytes = &zakura_test::vectors::BLOCK_MAINNET_395_BYTES;
    for (initial_tip, accept_other) in [
        (0, true),
        (396 + zs::MAX_BLOCK_REORG_HEIGHT, true),
        (0, false),
    ] {
        let (network, mut sent) = network();
        let (commits, mut committed) = mpsc::unbounded_channel();
        let verifier = service_fn(move |request| {
            let zakura_consensus::Request::Commit(block) = request else {
                panic!("expected a commit")
            };
            let hash = block.hash();
            commits.send(block).unwrap();
            std::future::ready(Ok(hash))
        });
        let state = service_fn(|request| async move {
            Ok(match request {
                zs::Request::AnyChainBlock(_) => zs::Response::Block(None),
                zs::Request::KnownBlock(_) => {
                    zs::Response::KnownBlock(Some(zs::KnownBlock::BestChain))
                }
                _ => panic!("unexpected state request"),
            })
        });
        let (mut tip_sender, tip) =
            super::tests::chain_tip_at(block::Height(initial_tip), block::Hash([1; 32]));
        let reporter = crate::components::test_misbehavior::reporter();
        let mut downloads = Downloads::new(
            reporter.clone(),
            MIN_CONCURRENCY_LIMIT,
            false,
            network,
            verifier,
            state,
            tip,
            Network::Mainnet,
        )
        .with_second_block_request(true);
        let other_body = body(bytes);
        let hash = other_body.hash();
        assert_eq!(
            downloads.download_and_verify(hash, None),
            DownloadAction::AddedToQueue
        );
        let first = next_sent(&mut sent).await;
        let other = next_dispatched(&mut sent).await;
        first
            .answer
            .send(Ok(available(bytes, first_peer())))
            .unwrap();
        assert!(
            tokio::time::timeout(SOON, downloads.next()).await.is_err(),
            "the rejected first body leaves the alternate pending"
        );
        assert!(committed.try_recv().is_err());

        if accept_other {
            super::tests::set_tip(&mut tip_sender, block::Height(394), block::Hash([2; 32]));
        }
        other
            .answer
            .send(Ok(zn::Response::Blocks(vec![
                zn::InventoryResponse::Available((other_body.clone().into(), Some(second_peer()))),
            ])))
            .unwrap();
        let result = tokio::time::timeout(SOON, downloads.next())
            .await
            .unwrap()
            .unwrap();
        if accept_other {
            assert_eq!(result.unwrap(), hash);
            assert!(
                matches!(committed.try_recv(), Ok(BlockHandle::Shared(block)) if Arc::ptr_eq(&block, &other_body))
            );
        } else {
            let (error, source) = result.unwrap_err();
            assert!(matches!(
                error.downcast_ref::<GossipHeightWindowError>(),
                Some(GossipHeightWindowError::Ahead)
            ));
            assert!(source.is_none());
        }
        assert!(committed.try_recv().is_err());
        assert!(!reporter.is_banned(first_peer().ip()));
        assert!(!reporter.is_banned(second_peer().ip()));
        assert_nothing_more_sent(&mut sent, "a height rejection adds no network request").await;
    }
}

/// Exercise candidate admission with a moving local tip and an ordinary immutable body.
#[tokio::test(start_paused = true)]
async fn a_height_rejection_leaves_the_other_dispatch_eligible() {
    let block = body(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES);
    let parent = block.header.previous_block_hash;
    for first_rejected in [true, false] {
        let (mut tip_sender, tip) = super::tests::chain_tip_at(block::Height(5), parent);
        let reporter = crate::components::test_misbehavior::reporter();
        let checks = CandidateChecks {
            tip,
            misbehavior: reporter.clone(),
            kind: DownloadKind::Advertised,
        };
        let (network, mut sent) = network();
        let mut download = tokio::spawn(download_block(network, block.hash(), None, true, checks));
        let first = next_sent(&mut sent).await;
        let second = next_dispatched(&mut sent).await;
        let ((early, early_addr), (late, late_addr)) = if first_rejected {
            ((first, first_peer()), (second, second_peer()))
        } else {
            ((second, second_peer()), (first, first_peer()))
        };
        early
            .answer
            .send(Ok(available(
                &zakura_test::vectors::BLOCK_MAINNET_1_BYTES,
                early_addr,
            )))
            .unwrap();
        assert!(tokio::time::timeout(SOON, &mut download).await.is_err());
        assert!(reporter.is_banned(early_addr.ip()));
        super::tests::set_tip(&mut tip_sender, block::Height(6), block::Hash([6; 32]));
        late.answer
            .send(Ok(available(
                &zakura_test::vectors::BLOCK_MAINNET_1_BYTES,
                late_addr,
            )))
            .unwrap();
        let (accepted, source, height) = download.await.unwrap().unwrap().first;
        assert_eq!(
            (accepted.hash(), source, height),
            (block.hash(), Some(late_addr), block::Height(1))
        );
        assert!(!reporter.is_banned(late_addr.ip()));
        assert_nothing_more_sent(&mut sent, "the other dispatch supplied the block").await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_missing_second_reply_leaves_the_first_peer_eligible() {
    let (network, mut sent) = network();
    let mut download = tokio::spawn(test_download(network, hash(), None, true));
    let first = next_sent(&mut sent).await;
    let second = next_dispatched(&mut sent).await;
    second
        .answer
        .send(Ok(zn::Response::Blocks(vec![
            zn::InventoryResponse::Missing(hash()),
        ])))
        .unwrap();
    assert!(
        tokio::time::timeout(SOON, &mut download).await.is_err(),
        "the other request can still deliver"
    );
    assert_nothing_more_sent(&mut sent, "a miss spends the single hedge").await;
    first.answer.send(Ok(from_first())).unwrap();
    assert_copy(download.await.unwrap().unwrap(), first_peer());
}

#[tokio::test(start_paused = true)]
async fn two_failed_replies_preserve_the_first_peers_hash_error() {
    for first_finishes_first in [true, false] {
        let (network, mut sent) = network();
        let mut download = tokio::spawn(test_download(network, hash(), None, true));
        let first = next_sent(&mut sent).await;
        let second = next_dispatched(&mut sent).await;
        let wrong = available(&zakura_test::vectors::BLOCK_MAINNET_1_BYTES, first_peer());
        let missing = zn::Response::Blocks(vec![zn::InventoryResponse::Missing(hash())]);
        let (early, late) = if first_finishes_first {
            ((first, wrong), (second, missing))
        } else {
            ((second, missing), (first, wrong))
        };
        early.0.answer.send(Ok(early.1)).unwrap();
        assert!(
            tokio::time::timeout(SOON, &mut download).await.is_err(),
            "the other request can still deliver"
        );
        late.0.answer.send(Ok(late.1)).unwrap();
        let (error, source) = download.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("peer returned block"));
        assert_eq!(source, Some(first_peer()));
    }
}

/// Drive the downloader through a buffered dual-stack router. Wait for its original
/// composite request to finish before counting what the native peer actually received.
#[tokio::test]
async fn native_fallback_only_runs_while_the_download_needs_a_body() -> Result<(), BoxError> {
    use zn::zakura::{
        testkit::{ZakuraTestNode, TEST_NET_TIMEOUT},
        LegacyGossipSink,
    };
    let (observed, mut native_requests) = mpsc::unbounded_channel();
    let native = ZakuraTestNode::builder(2191)
        .service_from_supervisor(move |supervisor| {
            Arc::new(LegacyGossipSink::spawn(
                service_fn(move |req: zn::Request| {
                    observed.send(req).unwrap();
                    std::future::ready(Ok::<_, BoxError>(from_first()))
                }),
                supervisor,
            ))
        })
        .spawn()
        .await?;
    let node = ZakuraTestNode::builder(2192).spawn().await?;
    node.connect_native(&native, TEST_NET_TIMEOUT).await?;

    for outcome in ["fallback needed", "missing", "error", "timeout"] {
        let (legacy, mut requests) = network();
        let composite = node.dual_stack_inventory(legacy);
        let (finished, mut completions) = mpsc::unbounded_channel();
        let service = service_fn(move |req| {
            let primary = !matches!(req, zn::Request::BlockByHashHedge { .. });
            let response = composite.clone().oneshot(req);
            let finished = finished.clone();
            async move {
                let result = response.await;
                if primary {
                    finished.send(()).unwrap();
                }
                result
            }
        });
        let buffered = tower::buffer::Buffer::new(service, 4);
        let download = tokio::spawn(test_download(buffered, hash(), None, true));
        let first = tokio::time::timeout(TEST_NET_TIMEOUT, requests.recv())
            .await?
            .unwrap();
        let mut download = Some(download);
        if outcome != "fallback needed" {
            let later = tokio::time::timeout(TEST_NET_TIMEOUT, requests.recv())
                .await?
                .unwrap();
            let zn::Request::BlockByHashHedge { ref attempt, .. } = later.request else {
                panic!("expected a second request");
            };
            assert!(attempt.claim_dispatch());
            later.answer.send(Ok(from_second())).unwrap();
            let result = tokio::time::timeout(TEST_NET_TIMEOUT, download.take().unwrap())
                .await??
                .map_err(|(error, _)| error)?;
            assert_copy(result, second_peer());
        }
        match outcome {
            "error" => first.answer.send(Err("peer disconnected".into())).unwrap(),
            "timeout" => {}
            _ => first
                .answer
                .send(Ok(zn::Response::Blocks(vec![
                    zn::InventoryResponse::Missing(hash()),
                ])))
                .unwrap(),
        }
        if let Some(download) = download {
            let (body, _, _) = tokio::time::timeout(TEST_NET_TIMEOUT, download)
                .await??
                .map_err(|(error, _)| error)?;
            assert_eq!(body.hash(), hash());
        }
        tokio::time::timeout(TEST_NET_TIMEOUT, completions.recv())
            .await?
            .unwrap();
        assert_eq!(
            native_requests.try_recv().is_ok(),
            outcome == "fallback needed",
            "{outcome}"
        );
        assert!(native_requests.try_recv().is_err());
    }
    node.shutdown().await;
    native.shutdown().await;
    Ok(())
}
