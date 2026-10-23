//! Tests for the simulator build of `zakura-rt`. They only build with
//! `RUSTFLAGS="--cfg zakura_sim"`. Use a separate `CARGO_TARGET_DIR`, since
//! changing `RUSTFLAGS` rebuilds every crate.
#![cfg(zakura_sim)]

mod support;

use std::{
    future::{pending, poll_fn, Future},
    pin::{pin, Pin},
    task::Poll,
    time::Duration,
};

use support::run;
use zakura_rt::{
    sim::{self, Op},
    task::{self, AbortOnDropHandle, JoinSet, JoinSetExt},
    time::{self, Instant, MissedTickBehavior},
};

/// Polls `future` once and reports the result.
async fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    poll_fn(|cx| Poll::Ready(Pin::new(&mut *future).poll(cx))).await
}

#[test]
fn executor_is_installed_only_during_a_run() {
    assert!(sim::executor().is_none());
    let ((), _) = run(async {
        assert!(sim::executor().is_some());
    });
    assert!(sim::executor().is_none());
}

#[test]
fn each_run_gets_a_new_id() {
    let (first, _) = run(async { sim::run_id().unwrap() });
    let (second, _) = run(async { sim::run_id().unwrap() });
    assert_ne!(first, second);
    assert!(sim::run_id().is_none());
}

#[test]
fn stale_abort_handle_leaves_a_later_runs_task_alone() {
    // Executors reuse task ids, so both runs' first spawn gets the same id.
    let (stale, _) = run(async { task::spawn(async {}).abort_handle() });
    let (value, _) = run(async move {
        let handle = task::spawn(async { 7 });
        stale.abort();
        assert!(stale.is_finished());
        handle.await.unwrap()
    });
    assert_eq!(value, 7);
}

#[test]
fn stale_sleep_leaves_a_later_runs_timer_alone() {
    // Both runs' first timer gets the same deadline and sequence number. The
    // runs hand the futures out in a `Some` so clippy doesn't take them for a
    // missing `.await`.
    let (stale, _) = run(async {
        let mut sleep = time::sleep(Duration::from_millis(10));
        assert!(poll_once(&mut sleep).await.is_pending());
        Some(sleep)
    });
    let ((), _) = run(async move {
        let mut sleep = time::sleep(Duration::from_millis(10));
        assert!(poll_once(&mut sleep).await.is_pending());
        drop(stale);
        sleep.await;
    });
}

#[test]
#[should_panic(expected = "only polled inside the run that spawned it")]
fn stale_join_handle_panics_instead_of_joining_a_later_runs_task() {
    let (stale, _) = run(async { Some(task::spawn(async { 1 })) });
    run(async move {
        let _later = task::spawn(async { 2 });
        stale.unwrap().await.unwrap()
    });
}

#[test]
#[should_panic(expected = "already active")]
fn runs_do_not_nest() {
    run(async {
        run(async {});
    });
}

#[tokio::test]
async fn outside_a_run_names_delegate_to_tokio() {
    let start = Instant::now();
    assert_eq!(task::spawn(async { 7 }).await.unwrap(), 7);

    let mut set = JoinSet::new();
    set.spawn(async { 1 });
    set.spawn(async { 2 });
    let mut joined = vec![
        set.join_next().await.unwrap().unwrap(),
        set.join_next().await.unwrap().unwrap(),
    ];
    joined.sort_unstable();
    assert_eq!(joined, [1, 2]);
    assert!(set.join_next().await.is_none());

    let aborted = task::spawn(pending::<()>());
    aborted.abort();
    assert!(aborted.await.unwrap_err().is_cancelled());

    task::yield_now().await;
    time::sleep(Duration::from_millis(1)).await;
    assert!(time::timeout(Duration::from_millis(1), pending::<()>())
        .await
        .is_err());
    let mut ticks = time::interval(Duration::from_millis(1));
    ticks.tick().await;
    ticks.tick().await;
    assert!(start.elapsed() >= Duration::from_millis(3));
}

#[tokio::test]
async fn timeout_outside_a_run_fires_when_the_future_uses_up_its_budget() {
    // The future spends the whole cooperative budget on every poll, so the
    // timer only fires if the timeout checks it outside the budget.
    let busy = async {
        loop {
            task::coop::consume_budget().await;
        }
    };
    let inner = time::timeout(Duration::from_millis(10), busy);
    let outer = tokio::time::timeout(Duration::from_secs(10), inner).await;
    assert!(outer.expect("the sim timeout fired first").is_err());
}

#[test]
fn spawned_output_reaches_the_join_handle() {
    let (value, report) = run(async { task::spawn(async { 5 }).await.unwrap() });
    assert_eq!(value, 5);
    assert_eq!(report.points, [(Some(0), Op::Spawn)]);
}

#[test]
fn try_spawn_runs_on_the_executor_inside_a_run() {
    let (value, report) = run(async {
        task::try_spawn(async { 5 })
            .expect("the test executor takes every task")
            .await
            .unwrap()
    });
    assert_eq!(value, 5);
    assert_eq!(report.points, [(Some(0), Op::Spawn)]);
}

#[test]
fn try_spawn_outside_a_runtime_returns_none() {
    assert!(task::try_spawn(async {}).is_none());
}

#[test]
fn panic_payload_reaches_the_join_error() {
    let (error, _) = run(async { task::spawn(async { panic!("boom") }).await.unwrap_err() });
    assert!(error.is_panic());
    assert!(!error.is_cancelled());
    assert_eq!(*error.into_panic().downcast::<&str>().unwrap(), "boom");
}

#[test]
fn abort_cancels_a_waiting_task() {
    let (error, report) = run(async {
        let handle = task::spawn(time::sleep(Duration::from_secs(10)));
        // Let the task start and register its timer.
        time::sleep(Duration::from_millis(1)).await;
        handle.abort();
        handle.await.unwrap_err()
    });
    assert!(error.is_cancelled());
    assert_eq!(report.count(Op::Abort), 1);
    assert_eq!(report.timers, 0, "the aborted sleep cancels its timer");
    assert_eq!(report.now, Duration::from_millis(1));
}

#[test]
fn abort_on_drop_handle_aborts_unless_detached() {
    let ((dropped, detached), _) = run(async {
        let dropped = AbortOnDropHandle::new(task::spawn(pending::<()>()));
        let dropped_abort = dropped.abort_handle();
        drop(dropped);
        let detached = AbortOnDropHandle::new(task::spawn(async {
            time::sleep(Duration::from_secs(1)).await;
            3
        }))
        .detach();
        // Give the aborted task a turn to be dropped.
        time::sleep(Duration::from_millis(1)).await;
        (dropped_abort.is_finished(), detached.await.unwrap())
    });
    assert!(dropped);
    assert_eq!(detached, 3);
}

#[test]
fn join_set_returns_each_task_and_aborts_the_rest_on_drop() {
    let ((joined, left), _) = run(async {
        let mut set = JoinSet::new();
        set.spawn(async {
            time::sleep(Duration::from_secs(2)).await;
            2
        });
        set.spawn(async {
            time::sleep(Duration::from_secs(1)).await;
            1
        });
        let first = set.join_next().await.unwrap().unwrap();
        let second = set.join_next().await.unwrap().unwrap();
        assert!(set.join_next().await.is_none());

        let mut rest = JoinSet::new();
        let left = rest.spawn(pending::<i32>());
        drop(rest);
        time::sleep(Duration::from_millis(1)).await;
        ([first, second], left.is_finished())
    });
    assert_eq!(joined, [1, 2], "tasks join in the order they finish");
    assert!(left);
}

#[test]
fn join_set_spawn_named_labels_the_task() {
    let ((joined, aborted), report) = run(async {
        let mut set = JoinSet::new();
        set.spawn_named("worker", async { 4 });
        set.spawn(async { 5 });
        set.spawn_named("parked", pending::<i32>()).abort();
        let mut joined = Vec::new();
        let mut aborted = 0;
        while let Some(result) = set.join_next().await {
            match result {
                Ok(value) => joined.push(value),
                Err(error) if error.is_cancelled() => aborted += 1,
                Err(error) => panic!("unexpected join error: {error}"),
            }
        }
        (joined, aborted)
    });
    assert_eq!(joined, [4, 5]);
    assert_eq!(aborted, 1);
    assert_eq!(report.labels, ["root", "worker", file!(), "parked"]);
    assert_eq!(report.count(Op::Spawn), 3);
}

#[test]
fn virtual_time_moves_only_through_the_executor() {
    let wall = std::time::Instant::now();
    let (elapsed, report) = run(async {
        let start = Instant::now();
        time::sleep(Duration::from_secs(3600)).await;
        start.elapsed()
    });
    assert_eq!(elapsed, Duration::from_secs(3600));
    assert_eq!(report.now, Duration::from_secs(3600));
    assert!(wall.elapsed() < Duration::from_secs(60));
}

#[test]
fn sleep_points_at_registration_and_expiry() {
    let ((), waited) = run(async {
        time::sleep(Duration::from_secs(1)).await;
    });
    assert_eq!(waited.count(Op::Sleep), 2);

    let ((), immediate) = run(async {
        time::sleep(Duration::ZERO).await;
    });
    assert_eq!(immediate.count(Op::Sleep), 1);
}

#[test]
fn dropped_sleep_cancels_its_timer() {
    let ((), report) = run(async {
        let mut sleep = Box::pin(time::sleep(Duration::from_secs(5)));
        assert!(poll_once(&mut sleep).await.is_pending());
    });
    assert_eq!(report.timers, 0);
    assert_eq!(
        report.now,
        Duration::ZERO,
        "time never jumped to the dropped timer"
    );
}

#[test]
fn reset_moves_a_registered_timer() {
    let (elapsed, _) = run(async {
        let start = Instant::now();
        let mut sleep = Box::pin(time::sleep(Duration::from_secs(1)));
        assert!(poll_once(&mut sleep).await.is_pending());
        sleep.as_mut().reset(start + Duration::from_secs(4));
        assert_eq!(sleep.deadline(), start + Duration::from_secs(4));
        sleep.await;
        start.elapsed()
    });
    assert_eq!(elapsed, Duration::from_secs(4));
}

#[test]
fn timeout_elapses_at_its_deadline() {
    let ((result, elapsed), report) = run(async {
        let start = Instant::now();
        let result = time::timeout(Duration::from_secs(5), pending::<()>()).await;
        (result, start.elapsed())
    });
    assert!(result.is_err());
    assert_eq!(elapsed, Duration::from_secs(5));
    assert_eq!(report.timers, 0);
}

#[test]
fn timeout_returns_a_ready_value_without_waiting() {
    let (result, report) = run(async { time::timeout(Duration::from_secs(5), async { 1 }).await });
    assert_eq!(result, Ok(1));
    assert_eq!(report.now, Duration::ZERO);
    assert_eq!(report.timers, 0);
}

#[test]
fn interval_ticks_on_the_virtual_period() {
    let (due, _) = run(async {
        let start = Instant::now();
        let mut ticks = time::interval(Duration::from_secs(10));
        ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut due = vec![ticks.tick().await - start, ticks.tick().await - start];
        // Stall past two more ticks. Skip drops the missed ones.
        time::sleep(Duration::from_secs(25)).await;
        due.push(ticks.tick().await - start);
        due.push(ticks.tick().await - start);
        due
    });
    let secs: Vec<u64> = due.iter().map(Duration::as_secs).collect();
    assert_eq!(secs, [0, 10, 20, 40]);
}

#[test]
fn yield_now_is_pending_once_and_wakes_itself() {
    let (polls, report) = run(async {
        let mut yielded = pin!(task::yield_now());
        let mut polls = Vec::new();
        poll_fn(|cx| {
            let poll = yielded.as_mut().poll(cx);
            polls.push(poll.is_ready());
            poll
        })
        .await;
        polls
    });
    // The second poll only happens because the first one woke the task.
    assert_eq!(polls, [false, true]);
    assert!(
        report.points.is_empty(),
        "yield_now isn't a preemption point"
    );
}

#[test]
fn yield_now_loses_a_biased_select_to_a_ready_branch() {
    let (winner, _) = run(async {
        zakura_rt::select! {
            biased;
            () = task::yield_now() => "yield",
            () = std::future::ready(()) => "ready",
        }
    });
    assert_eq!(winner, "ready");
}

#[test]
fn is_finished_is_a_point() {
    let (finished, report) = run(async {
        let handle = task::spawn(async {});
        let before = handle.is_finished();
        time::sleep(Duration::from_millis(1)).await;
        (before, handle.is_finished())
    });
    assert_eq!(finished, (false, true));
    assert_eq!(report.count(Op::Join), 2);
}

#[test]
#[should_panic(expected = "spawn_blocking isn't simulated")]
fn spawn_blocking_is_refused_inside_a_run() {
    run(async {
        drop(task::spawn_blocking(|| ()));
    });
}
