//! The second request for a gossiped block, sent when the peer set expects another announcer to
//! deliver it sooner.
//!
//! The network service hands each request to the test with a channel for its answer, so the test
//! decides when, and how, each request is answered. Answering a second-request check with
//! [`zn::NoFasterBlockSource`] stands for a peer set that sent nothing. Time is paused.

use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tower::{service_fn, util::BoxCloneService};

use super::*;

type TestNetwork = BoxCloneService<zn::Request, zn::Response, BoxError>;

const CHECK: Duration = SECOND_REQUEST_CHECK_INTERVAL;

/// A request the network service received, and the channel that answers it.
struct Sent {
    request: zn::Request,
    answer: oneshot::Sender<Result<zn::Response, BoxError>>,
}

impl Sent {
    /// Answers a second-request check as a peer set that found no faster peer.
    fn not_yet(self) {
        assert!(
            matches!(self.request, zn::Request::BlockByHashHedge { .. }),
            "only a second-request check can be answered with not yet: {:?}",
            self.request
        );
        self.answer
            .send(Err(zn::NoFasterBlockSource.into()))
            .expect("the check is waiting for its answer");
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

fn hash() -> block::Hash {
    block::Hash([7; 32])
}

fn first_request() -> zn::Request {
    zn::Request::BlocksByHash([hash()].into_iter().collect())
}

/// Tells the two answers apart: the first request answers `Nil`, the second an empty block list.
fn from_first() -> zn::Response {
    zn::Response::Nil
}

fn from_second() -> zn::Response {
    zn::Response::Blocks(Vec::new())
}

/// Waits for the next request, at most two check intervals on the paused clock.
async fn next_sent(sent: &mut mpsc::UnboundedReceiver<Sent>) -> Sent {
    tokio::time::timeout(CHECK * 2, sent.recv())
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
    tokio::time::sleep(CHECK * 10).await;
    assert!(sent.try_recv().is_err(), "{why}");
}

#[tokio::test(start_paused = true)]
async fn without_second_requests_only_the_first_request_is_sent() {
    let (network, mut sent) = network();
    let download = tokio::spawn(request_block(network, first_request(), hash(), false));

    let first = next_sent(&mut sent).await;
    assert_eq!(first.request, first_request());
    assert_nothing_more_sent(&mut sent, "no checks when second requests are off").await;

    first.answer.send(Ok(from_first())).unwrap();
    assert_eq!(download.await.unwrap().unwrap(), from_first());
}

#[tokio::test(start_paused = true)]
async fn the_peer_set_is_asked_again_until_the_first_request_answers() {
    let (network, mut sent) = network();
    let download = tokio::spawn(request_block(network, first_request(), hash(), true));

    let first = next_sent(&mut sent).await;
    for checks in 1..=3 {
        let check = next_sent(&mut sent).await;
        let zn::Request::BlockByHashHedge {
            hash: requested,
            ref attempt,
        } = check.request
        else {
            panic!("expected a second-request check");
        };
        assert_eq!(requested, hash());
        assert_eq!(attempt.elapsed_if_open(), Some(CHECK * checks));
        check.not_yet();
    }

    first.answer.send(Ok(from_first())).unwrap();
    assert_eq!(download.await.unwrap().unwrap(), from_first());
    assert_nothing_more_sent(&mut sent, "an answered block is not checked again").await;
}

#[tokio::test(start_paused = true)]
async fn a_second_request_that_answers_first_wins() {
    let (network, mut sent) = network();
    let download = tokio::spawn(request_block(network, first_request(), hash(), true));

    let first = next_sent(&mut sent).await;
    next_sent(&mut sent).await.not_yet();
    // The peer set sent this one, so it answers with the block.
    let second = next_dispatched(&mut sent).await;

    second.answer.send(Ok(from_second())).unwrap();
    assert_eq!(download.await.unwrap().unwrap(), from_second());

    // The losing request still runs to completion, so its answer is received, not dropped.
    first
        .answer
        .send(Ok(from_first()))
        .expect("the losing request is still waiting for its answer");
}

#[tokio::test(start_paused = true)]
async fn after_a_second_request_is_sent_no_more_checks_are_made() {
    let (network, mut sent) = network();
    let download = tokio::spawn(request_block(network, first_request(), hash(), true));

    let first = next_sent(&mut sent).await;
    let second = next_dispatched(&mut sent).await;
    assert_nothing_more_sent(&mut sent, "a block gets at most one second request").await;

    first.answer.send(Ok(from_first())).unwrap();
    assert_eq!(download.await.unwrap().unwrap(), from_first());
    second
        .answer
        .send(Ok(from_second()))
        .expect("the losing request is still waiting for its answer");
}

#[tokio::test(start_paused = true)]
async fn a_failed_second_request_waits_for_the_first() {
    let (network, mut sent) = network();
    let download = tokio::spawn(request_block(network, first_request(), hash(), true));

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
    assert_eq!(download.await.unwrap().unwrap(), from_first());
}

#[tokio::test(start_paused = true)]
async fn a_failed_first_request_stops_the_checks_and_returns_its_error() {
    let (network, mut sent) = network();
    let download = tokio::spawn(request_block(network, first_request(), hash(), true));

    let first = next_sent(&mut sent).await;
    let check = next_sent(&mut sent).await;
    first.answer.send(Err("peer disconnected".into())).unwrap();

    let error = tokio::time::timeout(CHECK, download)
        .await
        .expect("the failed first request does not wait for a queued check")
        .unwrap()
        .unwrap_err();
    assert!(check.answer.is_closed(), "the queued check was dropped");
    assert_eq!(error.to_string(), "peer disconnected");
    assert_nothing_more_sent(&mut sent, "a failed download is not checked again").await;
}

#[tokio::test(start_paused = true)]
async fn checks_stop_after_the_check_window() {
    let (network, mut sent) = network();
    let download = tokio::spawn(request_block(network, first_request(), hash(), true));

    let first = next_sent(&mut sent).await;
    let mut checks = 0;
    while let Ok(Some(check)) = tokio::time::timeout(CHECK * 2, sent.recv()).await {
        check.not_yet();
        checks += 1;
    }
    let expected = zn::BlockHedge::WINDOW.as_millis() / CHECK.as_millis();
    assert!(
        (expected - 1..=expected).contains(&checks),
        "about one check per interval during the window, got {checks}"
    );

    first.answer.send(Ok(from_first())).unwrap();
    assert_eq!(download.await.unwrap().unwrap(), from_first());
}

#[tokio::test(start_paused = true)]
async fn completing_the_first_request_closes_an_already_submitted_check() {
    let (network, mut sent) = network();
    let download = tokio::spawn(request_block(network, first_request(), hash(), true));
    let first = next_sent(&mut sent).await;
    let queued = next_sent(&mut sent).await;
    let zn::Request::BlockByHashHedge { ref attempt, .. } = queued.request else {
        panic!("expected a second-request check");
    };
    assert!(attempt.elapsed_if_open().is_some());

    first.answer.send(Ok(from_first())).unwrap();
    assert_eq!(download.await.unwrap().unwrap(), from_first());
    assert!(attempt.elapsed_if_open().is_none());
    assert!(queued.answer.is_closed(), "the queued check was dropped");
    assert_nothing_more_sent(&mut sent, "a queued check must not be retried").await;
}

#[tokio::test(start_paused = true)]
async fn a_failed_first_request_waits_for_an_already_dispatched_second_request() {
    let (network, mut sent) = network();
    let mut download = tokio::spawn(request_block(network, first_request(), hash(), true));
    let first = next_sent(&mut sent).await;
    let second = next_dispatched(&mut sent).await;

    first.answer.send(Err("peer disconnected".into())).unwrap();
    assert!(
        tokio::time::timeout(CHECK, &mut download).await.is_err(),
        "the dispatched second request is still outstanding"
    );

    second.answer.send(Ok(from_second())).unwrap();
    assert_eq!(download.await.unwrap().unwrap(), from_second());
}
