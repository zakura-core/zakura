//! Tests for the simulator build's sync wrappers. They only build with
//! `RUSTFLAGS="--cfg zakura_sim"`, in a separate `CARGO_TARGET_DIR`.
#![cfg(zakura_sim)]

mod support;

use std::{
    future::{poll_fn, Future},
    pin::pin,
    sync::{Arc, TryLockError},
    task::Poll,
    time::Duration,
};

use support::run;
use zakura_rt::{
    sim::Op,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        counter, mpsc, oneshot, watch, CancellationToken, Mutex, Notify, PoisonError, Semaphore,
    },
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
    assert_eq!(
        report.count(Op::Release),
        3,
        "the unused permit, and both halves closing the channel at the end"
    );
}

#[test]
fn owned_permit_send_is_not_a_release() {
    let (value, report) = run(async {
        let (tx, mut rx) = mpsc::channel(1);
        let tx = tx.reserve_owned().await.unwrap().send(7);
        let value = rx.recv().await;
        std::mem::forget((tx, rx));
        value
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

#[test]
fn mutex_points_at_lock_and_after_release() {
    let (value, report) = run(async {
        let mutex = Mutex::new(1);
        *mutex.lock().unwrap() += 1;
        let held = mutex.lock().unwrap();
        let contended = matches!(mutex.try_lock(), Err(TryLockError::WouldBlock));
        drop(held);
        let value = *mutex.lock().unwrap();
        (value, contended)
    });
    assert_eq!(value, (2, true));
    assert_eq!(report.count(Op::Lock), 4, "three locks and a try_lock");
    assert_eq!(report.count(Op::Release), 3, "one per guard dropped");
}

#[test]
fn mutex_poisoning_matches_std() {
    let (recovered, _) = run(async {
        let mutex = Arc::new(Mutex::new(5));
        let holder = Arc::clone(&mutex);
        let panicked = task::spawn(async move {
            let _guard = holder.lock().unwrap();
            panic!("poison");
        })
        .await;
        assert!(panicked.unwrap_err().is_panic());
        assert!(mutex.is_poisoned());
        let value = *mutex.lock().unwrap_or_else(PoisonError::into_inner);
        value
    });
    assert_eq!(recovered, 5);
}

#[test]
#[should_panic(expected = "where the caller can't park: run ")]
fn contended_lock_that_cant_park_names_its_run() {
    run(async {
        let mutex = Mutex::new(());
        let _held = mutex.lock().unwrap();
        // The test executor never parks, as with a task being unwound.
        let _again = mutex.lock();
    });
}

#[test]
fn mutex_outside_a_run_is_std() {
    let mutex = Mutex::new(vec![1]);
    mutex.lock().unwrap().push(2);
    assert!(mutex.try_lock().is_ok());
    assert_eq!(mutex.into_inner().unwrap(), [1, 2]);
}

#[test]
fn coordination_atomics_point_and_counters_do_not() {
    let (values, report) = run(async {
        let flag = AtomicBool::new(false);
        let count = AtomicU64::new(0);
        flag.store(true, Ordering::Release);
        count.fetch_add(2, Ordering::Relaxed);
        let swapped = count.compare_exchange(2, 3, Ordering::AcqRel, Ordering::Acquire);
        let metric = counter::AtomicU64::new(0);
        metric.fetch_add(1, Ordering::Relaxed);
        (
            flag.load(Ordering::Acquire),
            swapped,
            count.load(Ordering::Relaxed),
        )
    });
    assert_eq!(values, (true, Ok(2), 3));
    assert_eq!(report.count(Op::AtomicStore), 1);
    assert_eq!(report.count(Op::AtomicRmw), 2, "the counter adds no point");
    assert_eq!(report.count(Op::AtomicLoad), 2);
}

#[test]
fn sim_atomics_cover_the_rest_of_std() {
    let (values, report) = run(async {
        let bits = zakura_rt::sync::atomic::AtomicI8::new(0b0110);
        let old = bits.fetch_nand(0b0011, Ordering::Relaxed);
        let flag = AtomicBool::new(false);
        let flipped = flag.fetch_not(Ordering::Relaxed);
        let mut target = 7_u32;
        let expected: *mut u32 = &mut target;
        let ptr = zakura_rt::sync::atomic::AtomicPtr::default();
        ptr.store(expected, Ordering::Release);
        zakura_rt::sync::atomic::fence(Ordering::SeqCst);
        let read = ptr.load(Ordering::Acquire);
        (
            old,
            bits.into_inner(),
            flipped,
            flag.into_inner(),
            read == expected,
        )
    });
    assert_eq!(values, (0b0110, !0b0010, false, true, true));
    assert_eq!(report.count(Op::AtomicRmw), 2, "fetch_nand and fetch_not");
    assert_eq!(report.count(Op::AtomicStore), 1);
    assert_eq!(report.count(Op::AtomicLoad), 1, "the fence is no point");
}

#[test]
fn closing_drops_are_releases() {
    let ((), report) = run(async {
        let (tx, rx) = mpsc::channel::<u8>(1);
        let spare = tx.clone();
        drop(tx);
        drop(spare);
        drop(rx);
        let (tx, rx) = mpsc::unbounded_channel::<u8>();
        drop(rx);
        drop(tx);
        let (sent, unsent) = (oneshot::channel::<u8>(), oneshot::channel::<u8>());
        sent.0.send(1).unwrap();
        drop(unsent.0);
        let (tx, _rx) = watch::channel(0);
        let spare = tx.clone();
        drop(tx);
        drop(spare);
    });
    assert_eq!(
        report.count(Op::Release),
        6,
        "the last bounded sender, its receiver, both unbounded halves, the \
         unsent oneshot sender and the last watch sender"
    );
}

#[test]
fn gating_reads_are_points() {
    let ((), report) = run(async {
        let (tx, rx) = mpsc::channel::<u8>(2);
        assert_eq!(tx.capacity() + rx.capacity(), 4);
        let semaphore = Semaphore::new(1);
        assert!(!semaphore.is_closed());
        let owner = Arc::new(5);
        let weak = zakura_rt::sync::downgrade(&owner);
        assert_eq!(weak.strong_count(), 1);
        assert_eq!(weak.upgrade().as_deref(), Some(&5));
        let (watched, _rx) = watch::channel(0);
        drop(watched.subscribe());
        std::mem::forget((tx, rx));
    });
    assert_eq!(report.count(Op::Send), 1, "the sender's capacity");
    assert_eq!(report.count(Op::Recv), 1, "the receiver's capacity");
    assert_eq!(report.count(Op::Acquire), 1, "is_closed");
    assert_eq!(report.count(Op::AtomicLoad), 1, "strong_count");
    assert_eq!(report.count(Op::AtomicRmw), 1, "upgrade");
    assert_eq!(report.count(Op::Watch), 1, "subscribe");
}

#[test]
fn points_are_skipped_while_a_watch_lock_is_held() {
    let (skipped, report) = run(async {
        let (tx, mut rx) = watch::channel(0);
        let flag = AtomicBool::new(false);
        {
            let value = rx.borrow_and_update();
            // A switch here could run a writer, which would block on the
            // read lock this task holds.
            flag.store(*value == 0, Ordering::Relaxed);
        }
        tx.send_modify(|value| {
            flag.store(true, Ordering::Relaxed);
            *value += 1;
        });
        let seen = *rx
            .wait_for(|value| flag.load(Ordering::Relaxed) && *value == 1)
            .await
            .unwrap();
        assert_eq!(seen, 1);
        flag.load(Ordering::Relaxed);
        zakura_rt::sim::suppressed_points()
    });
    assert_eq!(
        skipped, 3,
        "the store under the Ref, the store in send_modify and the load in wait_for"
    );
    assert_eq!(report.count(Op::AtomicStore), 0);
    assert_eq!(
        report.count(Op::AtomicLoad),
        1,
        "only the load after the locks"
    );
}
