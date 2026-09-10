//! Fixed test cases for batch worker tasks.

use std::{
    future::Ready,
    panic::AssertUnwindSafe,
    task::{Context, Poll},
    time::Duration,
};

use tokio::sync::oneshot;
use tokio_test::{assert_pending, assert_ready, assert_ready_err, task};
use tower::{Service, ServiceExt};
use tower_batch_control::{error, Batch, BatchControl, RequestWeight};
use tower_test::mock;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Sends on `tx` when it is dropped, including while a panic unwinds the worker task.
struct SendOnDrop(Option<oneshot::Sender<()>>);

impl Drop for SendOnDrop {
    fn drop(&mut self) {
        let _ = self
            .0
            .take()
            .expect("the sender is only taken by this drop impl")
            .send(());
    }
}

/// An inner service that panics when the batch worker checks its readiness.
struct PanicService;

impl Service<BatchControl<()>> for PanicService {
    type Response = ();
    type Error = BoxError;
    type Future = Ready<Result<(), BoxError>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        panic!("inner service panicked");
    }

    fn call(&mut self, _req: BatchControl<()>) -> Self::Future {
        unreachable!("the worker panics before it calls the inner service");
    }
}

#[tokio::test]
async fn wakes_pending_waiters_on_close() {
    let _init_guard = zakura_test::init();

    let (service, mut handle) = mock::pair::<_, ()>();

    let (mut service, worker) = Batch::pair(service, 1, 1, Duration::from_secs(1));
    let mut worker = task::spawn(worker.run());

    // // keep the request in the worker
    handle.allow(0);
    let service1 = service.ready().await.unwrap();
    let poll = worker.poll();
    assert_pending!(poll);
    let mut response = task::spawn(service1.call(()));

    let mut service1 = service.clone();
    let mut ready1 = task::spawn(service1.ready());
    assert_pending!(worker.poll());
    assert_pending!(ready1.poll(), "no capacity");

    let mut service1 = service.clone();
    let mut ready2 = task::spawn(service1.ready());
    assert_pending!(worker.poll());
    assert_pending!(ready2.poll(), "no capacity");

    // kill the worker task
    drop(worker);

    let err = assert_ready_err!(response.poll(), "worker close should fail the response");
    assert!(
        err.is::<error::Closed>(),
        "response should fail with a Closed, got: {err:?}",
    );

    assert!(
        ready1.is_woken(),
        "dropping worker should wake ready task 1",
    );
    let err = assert_ready_err!(ready1.poll(), "worker close should fail ready task 1");
    assert!(
        err.is::<error::ServiceError>(),
        "ready 1 should fail with a ServiceError {{ Closed }}, got: {err:?}",
    );

    assert!(
        ready2.is_woken(),
        "dropping worker should wake ready task 2",
    );
    let err = assert_ready_err!(ready2.poll(), "worker close should fail ready task 2");
    assert!(
        err.is::<error::ServiceError>(),
        "ready 2 should fail with a ServiceError {{ Closed }}, got: {err:?}",
    );
}

#[tokio::test]
async fn wakes_pending_waiters_on_failure() {
    let _init_guard = zakura_test::init();

    let (service, mut handle) = mock::pair::<_, ()>();

    let (mut service, worker) = Batch::pair(service, 1, 1, Duration::from_secs(1));
    let mut worker = task::spawn(worker.run());

    // keep the request in the worker
    handle.allow(0);
    let service1 = service.ready().await.unwrap();
    assert_pending!(worker.poll());
    let mut response = task::spawn(service1.call("hello"));

    let mut service1 = service.clone();
    let mut ready1 = task::spawn(service1.ready());
    assert_pending!(worker.poll());
    assert_pending!(ready1.poll(), "no capacity");

    let mut service1 = service.clone();
    let mut ready2 = task::spawn(service1.ready());
    assert_pending!(worker.poll());
    assert_pending!(ready2.poll(), "no capacity");

    // fail the inner service
    handle.send_error("foobar");
    // worker task terminates
    assert_ready!(worker.poll());

    let err = assert_ready_err!(response.poll(), "worker failure should fail the response");
    assert!(
        err.is::<error::ServiceError>(),
        "response should fail with a ServiceError, got: {err:?}"
    );

    assert!(
        ready1.is_woken(),
        "dropping worker should wake ready task 1"
    );
    let err = assert_ready_err!(ready1.poll(), "worker failure should fail ready task 1");
    assert!(
        err.is::<error::ServiceError>(),
        "ready 1 should fail with a ServiceError, got: {err:?}"
    );

    assert!(
        ready2.is_woken(),
        "dropping worker should wake ready task 2"
    );
    let err = assert_ready_err!(ready2.poll(), "worker failure should fail ready task 2");
    assert!(
        err.is::<error::ServiceError>(),
        "ready 2 should fail with a ServiceError, got: {err:?}"
    );
}

#[tokio::test]
async fn try_flush_skips_when_queue_saturated() {
    let _init_guard = zakura_test::init();

    let (service, mut handle) = mock::pair::<_, ()>();
    let (mut service, worker) = Batch::pair(service, 1, 1, Duration::from_secs(1000));
    let mut worker = task::spawn(worker.run());

    handle.allow(2);
    service.ready().await.unwrap();
    let _response = service.call(());

    // The queued item holds the only permit, so a non-blocking flush is skipped.
    let mut flush_service = service.clone();
    assert!(matches!(flush_service.try_flush(), Ok(false)));

    // Once the worker drains the queue, the permit frees and try_flush queues.
    assert_pending!(worker.poll());
    assert!(matches!(flush_service.try_flush(), Ok(true)));
}

#[tokio::test]
async fn try_flush_completes_zero_weight_items() {
    use tokio::time::timeout;
    let _init_guard = zakura_test::init();

    #[derive(Debug)]
    struct ZeroWeight;
    impl RequestWeight for ZeroWeight {
        fn request_weight(&self) -> usize {
            0
        }
    }

    let (service, mut handle) = mock::pair::<BatchControl<ZeroWeight>, ()>();
    // High max weight and latency: only the explicit flush can flush this batch.
    let (mut service, worker) = Batch::pair(service, 100, 1, Duration::from_secs(1000));
    tokio::spawn(worker.run());

    handle.allow(2);
    service.ready().await.unwrap();
    let response = service.call(ZeroWeight);

    let mut flush_service = service.clone();
    assert!(
        flush_service.try_flush().expect("flush should not fail"),
        "flush should queue"
    );

    // The worker must forward the zero-weight item and then the flush command:
    // without the weight floor, the empty-batch guard skips this flush and the
    // item's response future never completes.
    let (request, send_item) = timeout(Duration::from_secs(5), handle.next_request())
        .await
        .expect("item should reach the inner service")
        .expect("inner service should stay open");
    assert!(matches!(request, BatchControl::Item(ZeroWeight)));
    send_item.send_response(());

    let (request, send_flush) = timeout(Duration::from_secs(5), handle.next_request())
        .await
        .expect("flush should reach the inner service")
        .expect("inner service should stay open");
    assert!(matches!(request, BatchControl::Flush));
    send_flush.send_response(());

    timeout(Duration::from_secs(5), response)
        .await
        .expect("item response should complete")
        .expect("zero-weight item should verify");
}

/// The batch worker exits when its inner service returns a recoverable error. Every later
/// readiness check must report that failure, rather than polling the completed
/// [`JoinHandle`](tokio::task::JoinHandle) again and panicking.
#[tokio::test]
async fn poll_ready_reports_worker_exit_every_time() {
    let _init_guard = zakura_test::init();

    let (inner_service, mut inner_handle) = mock::pair::<BatchControl<()>, ()>();
    let (mut service, worker) = Batch::pair(inner_service, 1, 1, Duration::from_secs(1));

    let (worker_exited_tx, worker_exited_rx) = oneshot::channel();
    let worker_handle = tokio::spawn(async move {
        let _signal_exit = SendOnDrop(Some(worker_exited_tx));
        worker.run().await;
    });
    service.register_worker(worker_handle);

    // Hold the item in the worker, so the worker is waiting on the inner service.
    inner_handle.allow(0);
    service.ready().await.expect("batch starts ready");
    let _response = service.call(());

    // A recoverable inner error permanently fails the worker, which then drains its queue
    // and returns. This is the `Poll::Ready(Ok(()))` case in `Batch::poll_ready()`.
    inner_handle.send_error("inner service error");
    worker_exited_rx.await.expect("worker task should exit");

    let Err(first_error) = service.ready().await else {
        panic!("readiness should fail once the worker has exited");
    };
    assert!(
        first_error.is::<error::ServiceError>(),
        "the first check should return the worker error, got: {first_error:?}",
    );

    // Before the fix, this check polled the completed `JoinHandle` again, and panicked.
    let Err(second_error) = service.ready().await else {
        panic!("readiness should keep failing once the worker has exited");
    };
    assert_eq!(
        first_error.to_string(),
        second_error.to_string(),
        "every check should return the same worker error",
    );

    // Clones share the worker handle, so they must not poll it again either.
    let mut cloned_service = service.clone();
    let Err(cloned_error) = cloned_service.ready().await else {
        panic!("clones should fail once the worker has exited");
    };
    assert_eq!(
        first_error.to_string(),
        cloned_error.to_string(),
        "clones should return the same worker error",
    );
}

/// A panicking worker propagates its panic to one readiness check. Every later check must
/// report the worker failure, rather than panicking on a poisoned worker handle mutex.
#[tokio::test]
async fn poll_ready_after_worker_panic_does_not_poison_the_handle_mutex() {
    let _init_guard = zakura_test::init();

    let (mut service, worker) = Batch::pair(PanicService, 1, 1, Duration::from_secs(1));

    let (worker_exited_tx, worker_exited_rx) = oneshot::channel();
    let worker_handle = tokio::spawn(async move {
        let _signal_exit = SendOnDrop(Some(worker_exited_tx));
        worker.run().await;
    });
    service.register_worker(worker_handle);

    service.ready().await.expect("batch starts ready");
    let _response = service.call(());
    worker_exited_rx.await.expect("worker task should panic");

    // The first check resumes the worker panic.
    let mut context_task = task::spawn(());
    let panic_payload = std::panic::catch_unwind(AssertUnwindSafe(|| {
        context_task.enter(|cx, _| service.poll_ready(cx))
    }))
    .expect_err("the first check should resume the worker panic");
    assert_eq!(
        panic_payload.downcast_ref::<&str>().copied(),
        Some("inner service panicked"),
        "the first check should resume the inner service panic",
    );

    // Before the fix, the panic unwound through the locked worker handle mutex, so this
    // check panicked on the poisoned mutex instead of returning the worker error.
    let Err(error) = service.ready().await else {
        panic!("readiness should fail once the worker has panicked");
    };
    assert!(
        error.is::<error::ServiceError>(),
        "later checks should return the worker error, got: {error:?}",
    );

    let mut cloned_service = service.clone();
    let Err(cloned_error) = cloned_service.ready().await else {
        panic!("clones should fail once the worker has panicked");
    };
    assert!(
        cloned_error.is::<error::ServiceError>(),
        "clones should return the worker error, got: {cloned_error:?}",
    );
}

#[tokio::test]
async fn zero_max_batches_still_runs_batches() {
    use tokio::time::timeout;
    let _init_guard = zakura_test::init();

    let (service, mut handle) = mock::pair::<BatchControl<()>, ()>();
    // A limit of zero concurrent batches must be clamped to one: otherwise every branch of
    // the worker's `select!` is disabled, so the worker panics on its first poll and fails
    // every request with `Closed`.
    let (mut service, worker) = Batch::pair(service, 1, 0, Duration::from_secs(1000));
    tokio::spawn(worker.run());

    handle.allow(2);
    service.ready().await.unwrap();
    let response = service.call(());

    let (request, send_item) = timeout(Duration::from_secs(5), handle.next_request())
        .await
        .expect("item should reach the inner service")
        .expect("inner service should stay open");
    assert!(matches!(request, BatchControl::Item(())));
    send_item.send_response(());

    let (request, send_flush) = timeout(Duration::from_secs(5), handle.next_request())
        .await
        .expect("flush should reach the inner service")
        .expect("inner service should stay open");
    assert!(matches!(request, BatchControl::Flush));
    send_flush.send_response(());

    timeout(Duration::from_secs(5), response)
        .await
        .expect("item response should complete")
        .expect("item should verify");
}

#[tokio::test]
async fn overflowing_request_weight_still_flushes() {
    use tokio::time::timeout;
    let _init_guard = zakura_test::init();

    #[derive(Debug)]
    struct WeightedItem(usize);

    impl RequestWeight for WeightedItem {
        fn request_weight(&self) -> usize {
            self.0
        }
    }

    let (service, mut handle) = mock::pair::<BatchControl<WeightedItem>, ()>();
    // A long latency, so only the item weights can flush this batch.
    let (mut service, worker) = Batch::pair(service, 100, 1, Duration::from_secs(1000));
    tokio::spawn(worker.run());

    handle.allow(3);
    service.ready().await.unwrap();
    let light_response = service.call(WeightedItem(1));

    // The weight of these two items sums to zero if the counter wraps. A wrapped counter
    // means "no queued items", so the worker would skip the flush for items the inner
    // service has already been given, and neither response would ever complete.
    service.ready().await.unwrap();
    let heavy_response = service.call(WeightedItem(usize::MAX));

    for _ in 0..2 {
        let (request, send_item) = timeout(Duration::from_secs(5), handle.next_request())
            .await
            .expect("item should reach the inner service")
            .expect("inner service should stay open");
        assert!(matches!(request, BatchControl::Item(_)));
        send_item.send_response(());
    }

    let (request, send_flush) = timeout(Duration::from_secs(5), handle.next_request())
        .await
        .expect("flush should reach the inner service")
        .expect("inner service should stay open");
    assert!(matches!(request, BatchControl::Flush));
    send_flush.send_response(());

    timeout(Duration::from_secs(5), light_response)
        .await
        .expect("light item response should complete")
        .expect("light item should verify");
    timeout(Duration::from_secs(5), heavy_response)
        .await
        .expect("heavy item response should complete")
        .expect("heavy item should verify");
}
