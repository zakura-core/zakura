//! Candidate selection must not wait behind network crawls.

use std::{
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use futures::{channel::mpsc, FutureExt};
use tokio::{
    sync::{mpsc as tokio_mpsc, oneshot},
    time::{self, timeout},
};
use tower::{service_fn, Service};
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

/// After the initial cooldown, known candidates must not wait for a stalled getaddr.
#[tokio::test]
async fn crawl_after_initial_cooldown_does_not_block_candidate_selection() {
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
    // Match initialization: the first crawl arms a cooldown before demand dials begin.
    let initial_update = { candidates.lock().await.update_initial(1) };
    let initial = tokio::spawn(initial_update);
    timeout(TEST_TIMEOUT, requests.recv())
        .await
        .unwrap()
        .unwrap()
        .send(Response::Peers(Vec::new()))
        .unwrap();
    initial.await.unwrap().unwrap();
    crawl(candidates.clone(), demand.clone()).await.unwrap();
    assert!(requests.try_recv().is_err());

    // The startup cooldown only delays the problem: exercise a later allowed crawl.
    time::advance(MIN_PEER_GET_ADDR_INTERVAL).await;
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

struct UnreadyPeerService {
    ready_polled: Option<oneshot::Sender<()>>,
}

impl Service<Request> for UnreadyPeerService {
    type Response = Response;
    type Error = BoxError;
    type Future = futures::future::Ready<Result<Response, BoxError>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        if let Some(ready_polled) = self.ready_polled.take() {
            ready_polled.send(()).unwrap();
        }
        Poll::Pending
    }

    fn call(&mut self, _request: Request) -> Self::Future {
        panic!("a service that never becomes ready must not be called")
    }
}

/// A crawl waiting for a usable peer must not block selection of the next dial.
#[tokio::test]
async fn crawl_waiting_for_readiness_does_not_block_candidate_selection() {
    let _init_guard = zakura_test::init();
    let mut book = AddressBook::new(
        "0.0.0.0:0".parse().unwrap(),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        Span::none(),
    );
    let candidate = "127.0.0.11:8233".parse().unwrap();
    book.update(MetaAddr::new_initial_peer(candidate)).unwrap();
    let (ready_polled, readiness) = oneshot::channel();
    let candidates = Arc::new(futures::lock::Mutex::new(CandidateSet::new(
        Arc::new(Mutex::new(book)),
        UnreadyPeerService {
            ready_polled: Some(ready_polled),
        },
    )));
    let (demand, _demand_rx) = mpsc::channel(10);

    time::pause();
    let task = tokio::spawn(crawl(candidates.clone(), demand));
    timeout(TEST_TIMEOUT, readiness).await.unwrap().unwrap();
    let selected = timeout(TEST_TIMEOUT, async { candidates.lock().await.next().await })
        .await
        .expect("known candidates must not wait for peer-set readiness")
        .unwrap();
    assert_eq!(selected.addr, candidate);
    assert!(!task.is_finished());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
}
