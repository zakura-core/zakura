//! Controlled real blocking operations and independent execution observations.

use crate::allocations::AllocationStats;
use std::{
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};
use tokio::sync::Notify;

/// Actual starts/completions, distinct from reserved permits or queued tasks.
#[derive(Debug, Default, Clone, Copy)]
pub struct ExecutionSnapshot {
    /// Operations that entered their blocking closure.
    pub started: usize,
    /// Operations whose closure still owns its execution guard.
    pub running: usize,
    /// Largest simultaneous running count.
    pub peak_running: usize,
    /// Execution guards that ended, including cancelled delivery and errors.
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
    /// Construct an independently controlled probe.
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

    /// Count a real operation and optionally pause it. Call on a blocking thread.
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

    /// Record allocator evidence gathered on this operation's blocking thread.
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

    /// Read actual operation counts without observing the production permits.
    pub fn snapshot(&self) -> ExecutionSnapshot {
        self.state.lock().unwrap().snapshot
    }

    /// Release every blocked phase.
    pub fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.hold_start = false;
        state.hold_finish = false;
        self.released.notify_all();
    }

    /// Ensure a test failure also releases its blocking jobs.
    pub fn release_on_drop(self: &Arc<Self>) -> impl Drop {
        struct Release(Arc<ExecutionProbe>);
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.release();
            }
        }
        Release(self.clone())
    }

    /// Wait for a finite number of actual starts, with a bounded test deadline.
    pub async fn wait_started(&self, count: usize) {
        self.wait_for(count, false).await;
    }

    /// Wait for a finite number of actual completions.
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

/// Tracks the actual closure lifetime independently of any production work lease.
#[derive(Debug)]
pub struct RunningOperation(Arc<ExecutionProbe>);

impl RunningOperation {
    /// Pause just before returning the completed result, then end execution.
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
