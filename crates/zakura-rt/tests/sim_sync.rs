//! Tests for the simulator build's sync wrappers. They only build with
//! `RUSTFLAGS="--cfg zakura_sim"`, in a separate `CARGO_TARGET_DIR`.
#![cfg(zakura_sim)]

mod support;

use std::{
    future::{poll_fn, Future},
    pin::pin,
    sync::Arc,
    task::Poll,
    time::Duration,
};

use support::run;
use zakura_rt::{
    sim::Op,
    sync::{mpsc, oneshot, watch, CancellationToken, Notify, Semaphore},
    task, time,
};

#[tokio::test]
async fn outside_a_run_wrappers_behave_like_tokio() {
    let (tx, mut rx) = mpsc::channel(1);
    tx.send(1).await.unwrap();
    assert!(tx.try_send(2).is_err(), "the channel holds one message");
    assert_eq!(rx.recv().await, Some(1));

    let (wtx, mut wrx) = watch::channel(0);
    wtx.send_replace(5);
    wrx.changed().await.unwrap();
    assert_eq!(*wrx.borrow_and_update(), 5);

    let (otx, orx) = oneshot::channel();
    otx.send(3).unwrap();
    assert_eq!(orx.await, Ok(3));

    let notify = Notify::new();
    notify.notify_one();
    notify.notified().await;

    let semaphore = Arc::new(Semaphore::new(1));
    let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
    assert_eq!(semaphore.available_permits(), 0);
    drop(permit);
    assert_eq!(semaphore.available_permits(), 1);

    let token = CancellationToken::new();
    let child = token.child_token();
    drop(token.clone().drop_guard());
    child.cancelled().await;
}

#[test]
fn mpsc_points_before_each_operation_and_after_a_released_slot() {
    let (received, report) = run(async {
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(1).await.unwrap();
        let permit = tx.try_reserve();
        assert!(permit.is_err(), "the only slot is taken");
        let first = rx.recv().await;
        let permit = tx.reserve().await.unwrap();
        drop(permit);
        let closed = tx.is_closed();
        (first, closed, rx.try_recv().is_err())
    });
    assert_eq!(received, (Some(1), false, true));
    assert_eq!(
        report.count(Op::Send),
        4,
        "send, try_reserve, reserve, is_closed"
    );
    assert_eq!(report.count(Op::Recv), 2, "recv, try_recv");
    assert_eq!(report.count(Op::Release), 1, "the unused permit");
}

#[test]
fn owned_permit_send_is_not_a_release() {
    let (value, report) = run(async {
        let (tx, mut rx) = mpsc::channel(1);
        let tx = tx.reserve_owned().await.unwrap().send(7);
        drop(tx);
        rx.recv().await
    });
    assert_eq!(value, Some(7));
    assert_eq!(report.count(Op::Release), 0);
    assert_eq!(report.count(Op::Send), 2, "reserve_owned, send");
}

#[test]
fn each_send_through_a_reserved_permit_is_a_point() {
    // Without a point per send, two sends through reserved permits ran as
    // one step, so a consumer could never run between them.
    let (received, report) = run(async {
        let (tx, mut rx) = mpsc::channel(2);
        for (permit, value) in tx.try_reserve_many(2).unwrap().zip([1, 2]) {
            permit.send(value);
        }
        (rx.recv().await, rx.recv().await)
    });
    assert_eq!(received, (Some(1), Some(2)));
    assert_eq!(report.count(Op::Send), 3, "try_reserve_many, two sends");
}

#[test]
fn watch_reads_and_writes_are_points() {
    let (seen, report) = run(async {
        let (tx, mut rx) = watch::channel(0);
        tx.send_modify(|v| *v = 4);
        rx.changed().await.unwrap();
        let seen = *rx.borrow_and_update();
        let waited = *rx.wait_for(|v| *v == 4).await.unwrap();
        (seen, waited)
    });
    assert_eq!(seen, (4, 4));
    assert_eq!(report.count(Op::Watch), 4);
}

#[test]
fn oneshot_receive_is_one_point() {
    let (value, report) = run(async {
        let (tx, rx) = oneshot::channel();
        let receiver = task::spawn(rx);
        time::sleep(Duration::from_millis(1)).await;
        tx.send(9).unwrap();
        receiver.await.unwrap()
    });
    assert_eq!(value, Ok(9));
    assert_eq!(report.count(Op::Send), 1);
    assert_eq!(report.count(Op::Recv), 1, "only the first poll is a point");
}

#[test]
fn notified_point_is_at_creation() {
    let (polled, report) = run(async {
        let notify = Notify::new();
        // Created, never polled, before the notify.
        let mut notified = pin!(notify.notified());
        notify.notify_waiters();
        poll_fn(|cx| Poll::Ready(notified.as_mut().poll(cx).is_ready())).await
    });
    assert!(polled, "notify_waiters wakes a future created before it");
    assert_eq!(
        report.points[0].1,
        Op::Notified,
        "the point comes at creation"
    );
    assert_eq!(report.count(Op::Notify), 1);
}

#[test]
fn semaphore_points_and_owned_permits_keep_the_semaphore_alive() {
    let ((counts, alive), report) = run(async {
        let semaphore = Arc::new(Semaphore::new(2));
        let weak = Arc::downgrade(&semaphore);
        let permit = Arc::clone(&semaphore).acquire_many_owned(2).await.unwrap();
        let before = semaphore.available_permits();
        drop(semaphore);
        let alive = weak.strong_count();
        drop(permit);
        ((before, weak.strong_count()), alive)
    });
    assert_eq!(counts, (0, 0));
    assert_eq!(alive, 1, "the permit holds the semaphore, as tokio's does");
    assert_eq!(report.count(Op::Acquire), 2);
    assert_eq!(report.count(Op::Release), 1);
}

#[test]
fn back_to_back_permit_drops_are_separate_points() {
    let ((), report) = run(async {
        let a = Semaphore::new(1);
        let b = Semaphore::new(1);
        let (pa, pb) = (a.try_acquire().unwrap(), b.try_acquire().unwrap());
        drop(pa);
        drop(pb);
    });
    assert_eq!(report.count(Op::Release), 2);
}

#[test]
fn cancellation_points_and_drop_guard() {
    let (cancelled, report) = run(async {
        let token = CancellationToken::new();
        let child = token.child_token();
        let waiter = task::spawn(async move { child.cancelled().await });
        time::sleep(Duration::from_millis(1)).await;
        drop(token.clone().drop_guard());
        waiter.await.unwrap();
        let disarmed = token.child_token().drop_guard().disarm();
        (token.is_cancelled(), disarmed.is_cancelled())
    });
    assert_eq!(
        cancelled,
        (true, true),
        "the child of a cancelled token is cancelled"
    );
    assert_eq!(report.count(Op::Cancel), 1, "only the armed guard cancels");
    assert_eq!(
        report.count(Op::Cancelled),
        3,
        "the first poll and two reads"
    );
}
