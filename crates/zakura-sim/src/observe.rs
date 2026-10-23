//! What a run observed, step by step.
//!
//! Checkers read only the [`ObservationLog`]. Each entry carries the step
//! that appended it. Step 0 is setup, before the first pick.

use serde::Serialize;

/// One thing a step made happen.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "obs", rename_all = "kebab-case")]
#[non_exhaustive]
pub enum Observation {
    /// A task the run hadn't seen before became ready. A task spawned while
    /// every worker is busy shows up at the step that first offers it.
    TaskSpawned {
        /// The task's name.
        task: String,
    },
    /// A task stopped at a preemption point and stays ready.
    TaskReached {
        /// The task's name.
        task: String,
        /// The point, such as `lock#2`.
        at: String,
        /// The point's source location, as `file:line`.
        #[serde(skip_serializing_if = "Option::is_none")]
        site: Option<String>,
    },
    /// A task's poll returned `Pending`.
    TaskWaiting {
        /// The task's name.
        task: String,
    },
    /// A task parked on a synchronous wait, keeping its worker.
    TaskParked {
        /// The task's name.
        task: String,
    },
    /// A task ended.
    TaskExited {
        /// The task's name.
        task: String,
        /// How it ended.
        exit: Exit,
    },
    /// Virtual time moved.
    TimeAdvanced {
        /// The new time, in nanoseconds since the run's start.
        to_ns: u64,
        /// How many timers fired.
        fired: usize,
    },
    /// An event from a test-only fake action source.
    Fake {
        /// The source's name.
        source: String,
        /// What happened.
        event: String,
    },
}

/// How a task ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Exit {
    /// The future returned `Ready`.
    Completed,
    /// The task was aborted.
    Cancelled,
    /// The task panicked.
    Panicked,
}

/// An observation and the step that appended it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// The step, from 1. Step 0 is setup.
    pub step: u64,
    /// What happened.
    pub obs: Observation,
}

/// Every observation of a run, in order.
#[derive(Clone, Debug, Default)]
pub struct ObservationLog {
    entries: Vec<Entry>,
    step: u64,
    frozen: bool,
}

impl ObservationLog {
    /// Appends `obs` to the current step. Does nothing once the log is
    /// frozen, so cleanup at shutdown leaves no trace in it.
    pub fn push(&mut self, obs: Observation) {
        if !self.frozen {
            self.entries.push(Entry {
                step: self.step,
                obs,
            });
        }
    }

    /// Every entry, in order.
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    /// The observations `step` appended.
    pub fn of_step(&self, step: u64) -> impl Iterator<Item = &Observation> {
        // Entries are appended in step order.
        let start = self.entries.partition_point(|e| e.step < step);
        self.entries[start..]
            .iter()
            .take_while(move |e| e.step == step)
            .map(|e| &e.obs)
    }

    /// Starts appending to `step`.
    pub(crate) fn begin(&mut self, step: u64) {
        self.step = step;
    }

    /// Stops recording. Live observations end before cleanup starts.
    pub(crate) fn freeze(&mut self) {
        self.frozen = true;
    }
}
