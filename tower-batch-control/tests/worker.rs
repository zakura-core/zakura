//! Fixed test cases for batch worker tasks.

use std::time::Duration;

use tokio_test::{assert_pending, assert_ready, assert_ready_err, assert_ready_ok, task};
use tower::{Service, ServiceExt};
use tower_batch_control::{error, Batch, BatchControl, RequestWeight};
use tower_test::mock;

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

    let err = assert_ready_err!(response.poll());
    assert!(
        err.is::<error::Closed>(),
        "response should fail with a Closed, got: {err:?}",
    );

    assert!(
        ready1.is_woken(),
        "dropping worker should wake ready task 1",
    );
    let err = assert_ready_err!(ready1.poll());
    assert!(
        err.is::<error::ServiceError>(),
        "ready 1 should fail with a ServiceError {{ Closed }}, got: {err:?}",
    );

    assert!(
        ready2.is_woken(),
        "dropping worker should wake ready task 2",
    );
    let err = assert_ready_err!(ready1.poll());
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

    let err = assert_ready_err!(response.poll());
    assert!(
        err.is::<error::ServiceError>(),
        "response should fail with a ServiceError, got: {err:?}"
    );

    assert!(
        ready1.is_woken(),
        "dropping worker should wake ready task 1"
    );
    let err = assert_ready_err!(ready1.poll());
    assert!(
        err.is::<error::ServiceError>(),
        "ready 1 should fail with a ServiceError, got: {err:?}"
    );

    assert!(
        ready2.is_woken(),
        "dropping worker should wake ready task 2"
    );
    let err = assert_ready_err!(ready1.poll());
    assert!(
        err.is::<error::ServiceError>(),
        "ready 2 should fail with a ServiceError, got: {err:?}"
    );
}

#[tokio::test]
async fn explicit_flush_waits_for_queue_capacity() {
    let _init_guard = zakura_test::init();

    let (service, mut handle) = mock::pair::<_, ()>();
    let (mut service, worker) = Batch::pair(service, 1, 1, Duration::from_secs(1000));
    let mut worker = task::spawn(worker.run());

    service.ready().await.unwrap();
    let _response = service.call(());

    let mut flush_service = service.clone();
    let mut flush = task::spawn(async move { flush_service.flush().await });
    assert_pending!(flush.poll(), "the queued item holds the only permit");

    handle.allow(2);
    assert_pending!(worker.poll());
    assert_ready_ok!(flush.poll());
}

#[tokio::test]
async fn explicit_flush_completes_zero_weight_items() {
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
    timeout(Duration::from_secs(5), flush_service.flush())
        .await
        .expect("flush should not time out")
        .expect("flush should queue");

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
