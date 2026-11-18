//! The second request for a gossiped block, sent when the peer set expects another announcer to
//! deliver it sooner.
//!
//! The network service hands each request to the test with a channel for its answer, so the test
//! decides when, and how, each request is answered. Answering a second-request check with
//! [`zn::NoFasterBlockSource`] stands for a peer set that sent nothing. Time is paused.

use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tower::{service_fn, util::BoxCloneService};

use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};

use super::*;

type TestNetwork = BoxCloneService<zn::Request, zn::Response, BoxError>;

const PROMPT: Duration = Duration::from_millis(10);

/// A request the network service received, and the channel that answers it.
struct Sent {
    request: zn::Request,
    answer: oneshot::Sender<Result<zn::Response, BoxError>>,
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

fn assert_copy((block, source): (BlockHandle, Option<PeerSocketAddr>), peer: PeerSocketAddr) {
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

/// Waits for the next request, with a short timeout on the paused clock.
async fn next_sent(sent: &mut mpsc::UnboundedReceiver<Sent>) -> Sent {
    tokio::time::timeout(PROMPT * 2, sent.recv())
        .await
        .expect("a request is sent promptly")
        .expect("the network service is alive")
}

/// Simulates the peer set accepting a check and sending it to a peer.
async fn next_dispatched(sent: &mut mpsc::UnboundedReceiver<Sent>) -> Sent {
    let check = next_sent(sent).await;
    let zn::Request::BlockByHashHedge { ref attempt, .. } = check.request else {
        panic!("expected a second-request check");
    };
    assert!(attempt.claim_dispatch());
    check
}

/// Waits long enough for any further check to be sent, and asserts none was.
async fn assert_nothing_more_sent(sent: &mut mpsc::UnboundedReceiver<Sent>, why: &str) {
    tokio::time::sleep(PROMPT * 10).await;
    assert!(sent.try_recv().is_err(), "{why}");
}

#[tokio::test(start_paused = true)]
async fn without_second_requests_only_the_first_request_is_sent() {
    let (network, mut sent) = network();
    let download = tokio::spawn(download_block(network, hash(), None, false));

    let first = next_sent(&mut sent).await;
    assert_eq!(first.request, first_request());
    assert_nothing_more_sent(&mut sent, "no checks when second requests are off").await;

    first.answer.send(Ok(from_first())).unwrap();
    assert_copy(download.await.unwrap().unwrap(), first_peer());
}

#[tokio::test(start_paused = true)]
async fn a_second_request_that_answers_first_wins() {
    let (network, mut sent) = network();
    let download = tokio::spawn(download_block(network, hash(), None, true));

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
async fn after_a_second_request_is_sent_no_more_checks_are_made() {
    let (network, mut sent) = network();
    let download = tokio::spawn(download_block(network, hash(), None, true));

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
    let download = tokio::spawn(download_block(network, hash(), None, true));

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
async fn a_failed_first_request_stops_the_checks_and_returns_its_error() {
    let (network, mut sent) = network();
    let download = tokio::spawn(download_block(network, hash(), None, true));

    let first = next_sent(&mut sent).await;
    let check = next_sent(&mut sent).await;
    first.answer.send(Err("peer disconnected".into())).unwrap();

    let error = tokio::time::timeout(PROMPT, download)
        .await
        .expect("the failed first request does not wait for a queued check")
        .unwrap()
        .unwrap_err();
    assert!(check.answer.is_closed(), "the queued check was dropped");
    assert_eq!(error.0.to_string(), "peer disconnected");
    assert_eq!(error.1, None);
    assert_nothing_more_sent(&mut sent, "a failed download is not checked again").await;
}

#[tokio::test(start_paused = true)]
async fn checks_stop_after_the_check_window() {
    let (network, mut sent) = network();
    let download = tokio::spawn(download_block(network, hash(), None, true));

    let first = next_sent(&mut sent).await;
    let check = next_sent(&mut sent).await;
    tokio::time::advance(zn::BlockHedge::WINDOW).await;
    tokio::task::yield_now().await;
    assert!(check.answer.is_closed(), "an undispatched check expires");
    assert!(sent.try_recv().is_err(), "no periodic checks");

    first.answer.send(Ok(from_first())).unwrap();
    assert_copy(download.await.unwrap().unwrap(), first_peer());
}

#[tokio::test(start_paused = true)]
async fn completing_the_first_request_closes_an_already_submitted_check() {
    let (network, mut sent) = network();
    let download = tokio::spawn(download_block(network, hash(), None, true));
    let first = next_sent(&mut sent).await;
    let queued = next_sent(&mut sent).await;
    let zn::Request::BlockByHashHedge { ref attempt, .. } = queued.request else {
        panic!("expected a second-request check");
    };
    assert!(attempt.elapsed_if_open().is_some());

    first.answer.send(Ok(from_first())).unwrap();
    assert_copy(download.await.unwrap().unwrap(), first_peer());
    assert!(attempt.elapsed_if_open().is_none());
    assert!(queued.answer.is_closed(), "the queued check was dropped");
    assert_nothing_more_sent(&mut sent, "a queued check must not be retried").await;
}

#[tokio::test(start_paused = true)]
async fn a_failed_first_request_waits_for_an_already_dispatched_second_request() {
    let (network, mut sent) = network();
    let mut download = tokio::spawn(download_block(network, hash(), None, true));
    let first = next_sent(&mut sent).await;
    let second = next_dispatched(&mut sent).await;

    first.answer.send(Err("peer disconnected".into())).unwrap();
    assert!(
        tokio::time::timeout(PROMPT, &mut download).await.is_err(),
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
        let mut download = tokio::spawn(download_block(network, hash(), None, true));
        let first = next_sent(&mut sent).await;
        let second = next_dispatched(&mut sent).await;
        first.answer.send(Ok(response)).unwrap();
        assert!(
            tokio::time::timeout(PROMPT, &mut download).await.is_err(),
            "the other request can still deliver"
        );
        second.answer.send(Ok(from_second())).unwrap();
        assert_copy(download.await.unwrap().unwrap(), second_peer());
    }
}

#[tokio::test(start_paused = true)]
async fn a_missing_second_reply_leaves_the_first_peer_eligible() {
    let (network, mut sent) = network();
    let mut download = tokio::spawn(download_block(network, hash(), None, true));
    let first = next_sent(&mut sent).await;
    let second = next_dispatched(&mut sent).await;
    second
        .answer
        .send(Ok(zn::Response::Blocks(vec![
            zn::InventoryResponse::Missing(hash()),
        ])))
        .unwrap();
    assert!(
        tokio::time::timeout(PROMPT, &mut download).await.is_err(),
        "the other request can still deliver"
    );
    first.answer.send(Ok(from_first())).unwrap();
    assert_copy(download.await.unwrap().unwrap(), first_peer());
}

#[tokio::test(start_paused = true)]
async fn two_failed_replies_preserve_the_first_peers_hash_error() {
    for first_finishes_first in [true, false] {
        let (network, mut sent) = network();
        let mut download = tokio::spawn(download_block(network, hash(), None, true));
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
            tokio::time::timeout(PROMPT, &mut download).await.is_err(),
            "the other request can still deliver"
        );
        late.0.answer.send(Ok(late.1)).unwrap();
        let (error, source) = download.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("peer returned block"));
        assert_eq!(source, Some(first_peer()));
    }
}

#[tokio::test(start_paused = true)]
async fn cancelling_the_download_closes_a_queued_hedge() {
    let (network, mut sent) = network();
    let download = tokio::spawn(download_block(network, hash(), None, true));
    let _first = next_sent(&mut sent).await;
    let queued = next_sent(&mut sent).await;
    download.abort();
    assert!(download.await.unwrap_err().is_cancelled());
    let zn::Request::BlockByHashHedge { attempt, .. } = queued.request else {
        panic!("expected hedge")
    };
    assert!(!attempt.claim_dispatch());
}

/// Drive the normal downloader entry point and count every call received by the network.
#[tokio::test(start_paused = true)]
async fn a_declined_check_retries_once_per_event_or_deadline() {
    let (network, mut sent) = network();
    let start = tokio::time::Instant::now();
    let download = tokio::spawn(download_block(network, hash(), None, true));
    let first = next_sent(&mut sent).await;
    let (signal, changed) = oneshot::channel();
    let (score, score_changed) = tokio::sync::watch::channel(());
    let deadline = start + Duration::from_millis(242);
    let initial = next_sent(&mut sent).await;
    initial
        .answer
        .send(Err(zn::NoFasterBlockSource::waiting(
            changed,
            vec![score_changed],
            deadline,
        )
        .into()))
        .unwrap();
    tokio::time::advance(Duration::from_millis(195)).await;
    tokio::task::yield_now().await;
    assert!(sent.try_recv().is_err());
    score.send(()).unwrap();
    drop(signal);
    let announced = next_sent(&mut sent).await;
    let (_keep_open, changed) = oneshot::channel();
    announced
        .answer
        .send(Err(zn::NoFasterBlockSource::waiting(
            changed,
            vec![],
            deadline,
        )
        .into()))
        .unwrap();
    tokio::time::advance(Duration::from_millis(46)).await;
    tokio::task::yield_now().await;
    assert!(
        sent.try_recv().is_err(),
        "both concurrent events produced just one check"
    );
    tokio::time::advance(Duration::from_millis(1)).await;
    let dispatched = next_dispatched(&mut sent).await;
    assert_eq!(tokio::time::Instant::now(), deadline);
    tokio::time::advance(zn::BlockHedge::WINDOW).await;
    dispatched.answer.send(Ok(from_second())).unwrap();
    assert_copy(download.await.unwrap().unwrap(), second_peer());
    first.answer.send(Ok(from_first())).unwrap();
    assert!(sent.try_recv().is_err());
}
