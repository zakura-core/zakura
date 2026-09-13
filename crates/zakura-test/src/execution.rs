//! Pause background jobs and count how many have started or are still running.
//!
//! A database read can continue after its caller stops waiting. Tests use this
//! helper to pause that read, cancel the caller, and check that the node still
//! counts the unfinished read against its work limit.
//!
//! Call [`ExecutionProbe::start`] inside the job and keep its [`RunningOperation`]
//! guard until the job ends. The helper counts that guard's lifetime independently
//! of the node's capacity counters. Jobs can pause at entry or before returning a
//! result. [`ExecutionProbe::release_on_drop`] releases them if the test panics.

use crate::allocations::AllocationStats;
use std::{
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};
use tokio::sync::Notify;

/// Counts recorded inside jobs. Reserving capacity or queuing a job does not count.
#[derive(Debug, Default, Clone, Copy)]
pub struct ExecutionSnapshot {
    /// Jobs that called [`ExecutionProbe::start`], including jobs paused there.
    pub started: usize,
    /// Jobs that still hold their [`RunningOperation`] guard.
    pub running: usize,
    /// Most jobs counted as running at the same time.
    pub peak_running: usize,
    /// Guards that were dropped, whether the job succeeded, failed, or panicked.
    pub finished: usize,
    /// Largest allocation request observed inside a measured operation.
    pub largest_allocation: usize,
    /// Largest measured live allocation total within one operation.
    pub peak_operation_bytes: usize,
}

#[derive(Debug, Default)]
struct State {
    hold_start: bool,
    hold_finish: bool,
    snapshot: ExecutionSnapshot,
}

/// Pause actual execution at its start or before returning its result.
#[derive(Debug, Default)]
pub struct ExecutionProbe {
    state: Mutex<State>,
    released: Condvar,
    changed: Notify,
}

impl ExecutionProbe {
    /// Create a probe that can pause jobs at entry, before returning, or both.
    ///
    /// `hold_start` pauses calls to [`Self::start`]. `hold_finish` pauses calls to
    /// [`RunningOperation::finish`]. [`Self::release`] releases both pause points.
    pub fn new(hold_start: bool, hold_finish: bool) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                hold_start,
                hold_finish,
                ..State::default()
            }),
            ..Self::default()
        })
    }

    /// Count a job as running, then pause it if `hold_start` was set.
    ///
    /// Call inside the blocking job, not when queuing it. Keep the returned guard
    /// until the job ends. This can block the thread, so do not call on an async worker.
    pub fn start(self: &Arc<Self>) -> RunningOperation {
        {
            let mut state = self.state.lock().unwrap();
            state.snapshot.started += 1;
            state.snapshot.running += 1;
            state.snapshot.peak_running = state.snapshot.peak_running.max(state.snapshot.running);
        }
        self.changed.notify_waiters();
        let running = RunningOperation(self.clone());
        self.wait_blocked(false);
        running
    }

    fn wait_blocked(&self, finished: bool) {
        let (state, timed) = self
            .released
            .wait_timeout_while(
                self.state.lock().unwrap(),
                Duration::from_secs(10),
                |state| {
                    if finished {
                        state.hold_finish
                    } else {
                        state.hold_start
                    }
                },
            )
            .unwrap();
        let blocked = if finished {
            state.hold_finish
        } else {
            state.hold_start
        };
        drop(state);
        assert!(
            !timed.timed_out() || !blocked,
            "test must release its controlled operation"
        );
    }

    /// Add memory measurements collected inside a job to the snapshot's maxima.
    pub fn allocations(&self, measured: AllocationStats) {
        let mut state = self.state.lock().unwrap();
        state.snapshot.largest_allocation = state
            .snapshot
            .largest_allocation
            .max(measured.largest_request);
        state.snapshot.peak_operation_bytes = state
            .snapshot
            .peak_operation_bytes
            .max(measured.peak_live_bytes);
    }

    /// Read the recorded job counts and memory measurements.
    pub fn snapshot(&self) -> ExecutionSnapshot {
        self.state.lock().unwrap().snapshot
    }

    /// Let jobs proceed through both pause points, including future calls.
    pub fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.hold_start = false;
        state.hold_finish = false;
        self.released.notify_all();
    }

    /// Return a guard that releases paused jobs when it goes out of scope.
    ///
    /// Keep it in the test so a failed assertion does not leave jobs waiting.
    pub fn release_on_drop(self: &Arc<Self>) -> impl Drop {
        struct Release(Arc<ExecutionProbe>);
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.release();
            }
        }
        Release(self.clone())
    }

    /// Wait up to five seconds for at least `count` jobs to call [`Self::start`].
    pub async fn wait_started(&self, count: usize) {
        self.wait_for(count, false).await;
    }

    /// Wait up to five seconds for at least `count` job guards to be dropped.
    pub async fn wait_finished(&self, count: usize) {
        self.wait_for(count, true).await;
    }

    async fn wait_for(&self, count: usize, finished: bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let snapshot = self.snapshot();
                if (if finished {
                    snapshot.finished
                } else {
                    snapshot.started
                }) >= count
                {
                    break;
                }
                changed.await;
            }
        })
        .await
        .expect("controlled operations reach the requested phase");
    }
}

/// Count a job as running until this guard is dropped, including during a panic.
///
/// Keep it inside the job for the entire operation being measured.
#[derive(Debug)]
pub struct RunningOperation(Arc<ExecutionProbe>);

impl RunningOperation {
    /// Pause here if `hold_finish` was set, then stop counting this job as running.
    ///
    /// Call just before the job returns its result.
    pub fn finish(self) {
        self.0.wait_blocked(true);
    }
}

impl Drop for RunningOperation {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap();
        state.snapshot.running -= 1;
        state.snapshot.finished += 1;
        self.0.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn release_guard_unblocks_real_work_at_entry() {
        let probe = ExecutionProbe::new(true, false);
        let release = probe.release_on_drop();
        let worker_probe = probe.clone();
        let (entered, mut observed) = tokio::sync::oneshot::channel();
        let worker = tokio::task::spawn_blocking(move || {
            let operation = worker_probe.start();
            entered.send(()).unwrap();
            operation.finish();
        });

        probe.wait_started(1).await;
        assert_eq!(probe.snapshot().running, 1);
        assert_eq!(probe.snapshot().finished, 0);
        assert!(matches!(
            observed.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        drop(release);
        tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(observed.await, Ok(()));
        probe.wait_finished(1).await;
        assert_eq!(probe.snapshot().running, 0);
        assert_eq!(probe.snapshot().peak_running, 1);
    }

    #[tokio::test]
    async fn completed_work_remains_running_until_its_result_is_released() {
        let probe = ExecutionProbe::new(false, true);
        let release = probe.release_on_drop();
        let worker_probe = probe.clone();
        let (computed, observed) = tokio::sync::oneshot::channel();
        let worker = tokio::task::spawn_blocking(move || {
            let operation = worker_probe.start();
            computed.send(()).unwrap();
            operation.finish();
            42
        });

        tokio::time::timeout(Duration::from_secs(5), observed)
            .await
            .unwrap()
            .unwrap();
        assert!(!worker.is_finished());
        assert_eq!(probe.snapshot().running, 1);
        assert_eq!(probe.snapshot().finished, 0);
        drop(release);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), worker)
                .await
                .unwrap()
                .unwrap(),
            42
        );
        probe.wait_finished(1).await;
        assert_eq!(probe.snapshot().running, 0);
    }

    #[tokio::test]
    async fn panicking_work_releases_its_execution_observation() {
        let probe = ExecutionProbe::new(false, false);
        let worker_probe = probe.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let _operation = worker_probe.start();
            panic!("controlled worker failure");
        });
        assert!(tokio::time::timeout(Duration::from_secs(5), worker)
            .await
            .unwrap()
            .unwrap_err()
            .is_panic());
        probe.wait_finished(1).await;
        assert_eq!(probe.snapshot().started, 1);
        assert_eq!(probe.snapshot().running, 0);
    }
}
