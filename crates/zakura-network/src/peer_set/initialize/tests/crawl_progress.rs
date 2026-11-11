//! Candidate selection must not wait behind network crawls.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::{channel::mpsc, FutureExt};
use tokio::{
    sync::{mpsc as tokio_mpsc, oneshot},
    time::{self, timeout},
};
use tower::service_fn;
use tracing::Span;

use crate::{
    constants::{DEFAULT_MAX_CONNS_PER_IP, MIN_PEER_GET_ADDR_INTERVAL, PEER_GET_ADDR_TIMEOUT},
    peer_set::CandidateSet,
    types::MetaAddr,
    AddressBook, BoxError, Request, Response,
};
use zakura_chain::parameters::Network::Mainnet;

use super::super::crawl;

const TEST_TIMEOUT: Duration = Duration::from_secs(2);

/// Exercise the production crawl wrapper while getaddr is deliberately unresolved.
#[tokio::test]
async fn pending_crawl_does_not_hold_candidate_selection_lock() {
    let _init_guard = zakura_test::init();
    let mut book = AddressBook::new(
        "0.0.0.0:0".parse().unwrap(),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        Span::none(),
    );
    let candidate = "127.0.0.10:8233".parse().unwrap();
    book.update(MetaAddr::new_initial_peer(candidate)).unwrap();
    let (request_tx, mut requests) = tokio_mpsc::unbounded_channel();
    let service = service_fn(move |request| {
        assert!(matches!(request, Request::Peers));
        let (response_tx, response_rx) = oneshot::channel();
        request_tx.send(response_tx).unwrap();
        async move {
            response_rx
                .await
                .map_err(|error| Box::new(error) as BoxError)
        }
    });
    let candidates = Arc::new(futures::lock::Mutex::new(CandidateSet::new(
        Arc::new(Mutex::new(book)),
        service,
    )));
    let (demand, _demand_rx) = mpsc::channel(10);

    time::pause();
    let first = tokio::spawn(crawl(candidates.clone(), demand.clone()));
    let response = timeout(TEST_TIMEOUT, requests.recv())
        .await
        .unwrap()
        .unwrap();
    let selected = timeout(TEST_TIMEOUT, async { candidates.lock().await.next().await })
        .await
        .expect("candidate selection must not wait for the crawl timeout")
        .unwrap();
    assert_eq!(selected.addr, candidate);
    assert!(!first.is_finished());

    // A concurrent demand crawl still shares the same crawl gate and cooldown.
    let second = tokio::spawn(crawl(candidates.clone(), demand.clone()));
    tokio::task::yield_now().await;
    assert!(requests.try_recv().is_err());
    response.send(Response::Peers(Vec::new())).unwrap();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    assert!(requests.try_recv().is_err());

    time::advance(MIN_PEER_GET_ADDR_INTERVAL - Duration::from_millis(1)).await;
    crawl(candidates.clone(), demand.clone()).await.unwrap();
    assert!(requests.try_recv().is_err());
    time::advance(Duration::from_millis(1)).await;
    let next = tokio::spawn(crawl(candidates, demand));
    timeout(TEST_TIMEOUT, requests.recv())
        .await
        .unwrap()
        .unwrap()
        .send(Response::Peers(Vec::new()))
        .unwrap();
    next.await.unwrap().unwrap();
}

/// Timeout releases the crawl gate but retains the completion-based cooldown.
#[tokio::test]
async fn timed_out_crawl_keeps_shared_cooldown() {
    let _init_guard = zakura_test::init();
    let book = AddressBook::new(
        "0.0.0.0:0".parse().unwrap(),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        Span::none(),
    );
    let (request_tx, mut requests) = tokio_mpsc::unbounded_channel();
    let service = service_fn(move |_| {
        request_tx.send(()).unwrap();
        futures::future::pending::<Result<Response, BoxError>>()
    });
    let mut candidates = CandidateSet::new(Arc::new(Mutex::new(book)), service);
    time::pause();
    let initial = tokio::spawn(candidates.update_initial(1));
    timeout(TEST_TIMEOUT, requests.recv())
        .await
        .unwrap()
        .unwrap();
    let queued = candidates.update();
    time::advance(PEER_GET_ADDR_TIMEOUT).await;
    assert!(initial.await.unwrap().unwrap().is_none());
    assert!(queued.await.unwrap().is_none());
    assert!(requests.try_recv().is_err());

    time::advance(MIN_PEER_GET_ADDR_INTERVAL - Duration::from_millis(1)).await;
    assert!(candidates.update().await.unwrap().is_none());
    assert!(requests.try_recv().is_err());
    time::advance(Duration::from_millis(1)).await;
    let retry = tokio::spawn(candidates.update());
    timeout(TEST_TIMEOUT, requests.recv())
        .await
        .unwrap()
        .unwrap();
    retry.abort();
    assert!(retry.await.unwrap_err().is_cancelled());
}

/// Cancellation releases the separate gate; it must not strand a queued update.
#[tokio::test]
async fn cancelled_crawl_releases_gate() {
    let _init_guard = zakura_test::init();
    let book = AddressBook::new(
        "0.0.0.0:0".parse().unwrap(),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        Span::none(),
    );
    let (request_tx, mut requests) = tokio_mpsc::unbounded_channel();
    let service = service_fn(move |_| {
        request_tx.send(()).unwrap();
        futures::future::pending::<Result<Response, BoxError>>()
    });
    let mut candidates = CandidateSet::new(Arc::new(Mutex::new(book)), service);
    let first = tokio::spawn(candidates.update());
    timeout(TEST_TIMEOUT, requests.recv())
        .await
        .unwrap()
        .unwrap();
    let mut queued = candidates.update();
    assert!(queued.as_mut().now_or_never().is_none());
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let next = tokio::spawn(queued);
    timeout(TEST_TIMEOUT, requests.recv())
        .await
        .unwrap()
        .unwrap();
    next.abort();
    assert!(next.await.unwrap_err().is_cancelled());
}
