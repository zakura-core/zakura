//! The second request for a gossiped block that is slow to arrive.
//!
//! The network service hands each request to the test with a channel for its answer, so the test
//! decides when, and how, each request is answered. Time is paused.

use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tower::{service_fn, util::BoxCloneService};

use super::*;

type TestNetwork = BoxCloneService<zn::Request, zn::Response, BoxError>;

const DELAY: Duration = Duration::from_millis(50);

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

fn from_hedge() -> zn::Response {
    zn::Response::Blocks(Vec::new())
}

/// Waits for the next request without moving the paused clock past `DELAY`.
async fn next_sent(sent: &mut mpsc::UnboundedReceiver<Sent>) -> Sent {
    tokio::time::timeout(DELAY / 2, sent.recv())
        .await
        .expect("a request is sent promptly")
        .expect("the network service is alive")
}

#[tokio::test(start_paused = true)]
async fn no_delay_sends_only_the_first_request() {
    let (network, mut sent) = network();
    let download = tokio::spawn(request_block(network, first_request(), hash(), None));

    let first = next_sent(&mut sent).await;
    assert_eq!(first.request, first_request());
    tokio::time::sleep(DELAY * 10).await;
    assert!(
        sent.try_recv().is_err(),
        "no second request without a delay"
    );

    first.answer.send(Ok(from_first())).unwrap();
    assert_eq!(download.await.unwrap().unwrap(), from_first());
}

#[tokio::test(start_paused = true)]
async fn an_answer_within_the_delay_sends_no_second_request() {
    let (network, mut sent) = network();
    let download = tokio::spawn(request_block(network, first_request(), hash(), Some(DELAY)));

    let first = next_sent(&mut sent).await;
    tokio::time::sleep(DELAY / 2).await;
    first.answer.send(Ok(from_first())).unwrap();
    assert_eq!(download.await.unwrap().unwrap(), from_first());

    tokio::time::sleep(DELAY * 10).await;
    assert!(
        sent.try_recv().is_err(),
        "an answered request is not hedged"
    );
}

#[tokio::test(start_paused = true)]
async fn a_slow_first_request_gets_a_second_request_and_the_first_answer_wins() {
    let (network, mut sent) = network();
    let download = tokio::spawn(request_block(network, first_request(), hash(), Some(DELAY)));

    let first = next_sent(&mut sent).await;
    tokio::time::sleep(DELAY).await;
    let hedge = next_sent(&mut sent).await;
    assert_eq!(hedge.request, zn::Request::BlockByHashHedge(hash()));

    hedge.answer.send(Ok(from_hedge())).unwrap();
    assert_eq!(download.await.unwrap().unwrap(), from_hedge());

    // The losing request still runs to completion, so its answer is received, not dropped.
    first
        .answer
        .send(Ok(from_first()))
        .expect("the losing request is still waiting for its answer");
}

#[tokio::test(start_paused = true)]
async fn the_first_request_still_wins_after_the_second_is_sent() {
    let (network, mut sent) = network();
    let download = tokio::spawn(request_block(network, first_request(), hash(), Some(DELAY)));

    let first = next_sent(&mut sent).await;
    tokio::time::sleep(DELAY).await;
    let hedge = next_sent(&mut sent).await;

    first.answer.send(Ok(from_first())).unwrap();
    assert_eq!(download.await.unwrap().unwrap(), from_first());
    hedge
        .answer
        .send(Ok(from_hedge()))
        .expect("the losing request is still waiting for its answer");
}

#[tokio::test(start_paused = true)]
async fn a_failed_second_request_waits_for_the_first() {
    let (network, mut sent) = network();
    let download = tokio::spawn(request_block(network, first_request(), hash(), Some(DELAY)));

    let first = next_sent(&mut sent).await;
    tokio::time::sleep(DELAY).await;
    let hedge = next_sent(&mut sent).await;

    hedge
        .answer
        .send(Err("no other peer announced it".into()))
        .unwrap();
    tokio::time::sleep(DELAY).await;
    assert!(
        !download.is_finished(),
        "still waiting for the first request"
    );

    first.answer.send(Ok(from_first())).unwrap();
    assert_eq!(download.await.unwrap().unwrap(), from_first());
}

#[tokio::test(start_paused = true)]
async fn a_first_request_that_fails_early_is_hedged_at_once() {
    let (network, mut sent) = network();
    let download = tokio::spawn(request_block(network, first_request(), hash(), Some(DELAY)));

    let first = next_sent(&mut sent).await;
    first.answer.send(Err("peer disconnected".into())).unwrap();
    let hedge = next_sent(&mut sent).await;
    assert_eq!(hedge.request, zn::Request::BlockByHashHedge(hash()));

    hedge.answer.send(Ok(from_hedge())).unwrap();
    assert_eq!(download.await.unwrap().unwrap(), from_hedge());
}

#[tokio::test(start_paused = true)]
async fn when_both_fail_the_first_error_is_returned() {
    let (network, mut sent) = network();
    let download = tokio::spawn(request_block(network, first_request(), hash(), Some(DELAY)));

    let first = next_sent(&mut sent).await;
    tokio::time::sleep(DELAY).await;
    let hedge = next_sent(&mut sent).await;

    hedge
        .answer
        .send(Err("no other peer announced it".into()))
        .unwrap();
    first.answer.send(Err("peer disconnected".into())).unwrap();
    let error = download.await.unwrap().unwrap_err();
    assert_eq!(error.to_string(), "peer disconnected");
}
