//! A thread that runs explorer inputs, retired after an abandoned run
//! (#1694).
//!
//! When a task gets stuck in cleanup, shutdown abandons the run: it leaks
//! the run's tasks and state as they are, and runs no more of their code.
//! The thread then still holds those tasks and anything they share, such as
//! thread-local state, so a later run on it could wait on something a
//! leaked task never releases. A [`Worker`] runs each input on a thread of
//! its own, and after an abandoned run it parks that thread for good and
//! starts a fresh one for the next input.

use std::{
    any::Any,
    fmt,
    panic::{self, AssertUnwindSafe},
    sync::mpsc,
    thread,
};

use super::{classify, feedback::Feedback, Classified, ExploreRun};
use crate::{harness::HarnessEntry, Checker, NullSink};

/// One input's work, run on the worker's thread. Returns true if the thread
/// must retire.
type Job = Box<dyn FnOnce() -> bool + Send>;

/// Runs explorer inputs one at a time on a thread, and replaces the thread
/// after a run whose shutdown leaked tasks, or a job that panicked.
#[derive(Default)]
pub struct Worker {
    /// The live thread's job queue. `None` until the first input, and after
    /// a retirement until the next one.
    jobs: Option<mpsc::Sender<Job>>,
    /// Threads retired so far.
    retired: usize,
}

impl Worker {
    /// A worker with no thread yet. The first input starts one.
    pub fn new() -> Self {
        Worker::default()
    }

    /// Runs the full fuzz `input` on `entry`'s harness with a
    /// [`NullSink`], classifies the run, and calls `then` with the input,
    /// the run and its classification, all on the worker's thread. Returns
    /// what `then` returns.
    ///
    /// If the run leaked tasks, the thread retires after `then` returns,
    /// whatever the outcome. If the run or `then` panicked, the thread
    /// retires too, and the panic resumes on the caller's thread.
    pub fn explore<R: Send + 'static>(
        &mut self,
        entry: &'static HarnessEntry,
        input: &[u8],
        then: impl FnOnce(&[u8], &ExploreRun, &Classified) -> R + Send + 'static,
    ) -> R {
        self.explore_with(entry, input, None, then)
    }

    /// [`Worker::explore`], with `feedback`'s collector running after the
    /// harness's checker. The collector starts fresh for the input, on the
    /// worker's thread, and its counters hold the run's features once this
    /// returns.
    pub fn explore_with<R: Send + 'static>(
        &mut self,
        entry: &'static HarnessEntry,
        input: &[u8],
        feedback: Option<Feedback>,
        then: impl FnOnce(&[u8], &ExploreRun, &Classified) -> R + Send + 'static,
    ) -> R {
        let input = input.to_vec();
        let (done, result) = mpsc::sync_channel::<(Result<R, Box<dyn Any + Send>>, bool)>(1);
        let job: Job = Box::new(move || {
            let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
                let mut collecting = feedback.as_ref().map(Feedback::start);
                let extra = collecting.as_mut().map(|c| c as &mut dyn Checker);
                let run = (entry.run_bytes)(&input, &mut NullSink, extra);
                let classified = classify(entry, &run);
                (then(&input, &run, &classified), run.leaked() > 0)
            }));
            let (outcome, retire) = match outcome {
                Ok((value, retire)) => (Ok(value), retire),
                Err(payload) => (Err(payload), true),
            };
            // The caller waits on this, so the send can't fail.
            let _ = done.send((outcome, retire));
            retire
        });
        self.jobs
            .get_or_insert_with(spawn)
            .send(job)
            .expect("a live worker thread waits for jobs until its sender drops");
        let (outcome, retire) = result
            .recv()
            .expect("the job sends its result before its thread retires");
        if retire {
            // Dropping the sender leaves the retired thread parked.
            self.jobs = None;
            self.retired += 1;
        }
        match outcome {
            Ok(value) => value,
            Err(payload) => panic::resume_unwind(payload),
        }
    }

    /// How many threads this worker has retired.
    pub fn retired(&self) -> usize {
        self.retired
    }
}

impl fmt::Debug for Worker {
    /// Whether a thread is live, and how many retired.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Worker")
            .field("live", &self.jobs.is_some())
            .field("retired", &self.retired)
            .finish()
    }
}

/// Starts a worker thread and returns its job queue. The thread runs jobs
/// until one says to retire, then parks for good, so nothing the leaked
/// tasks hold is ever dropped. It exits when the queue's sender drops.
fn spawn() -> mpsc::Sender<Job> {
    let (jobs, queue) = mpsc::channel::<Job>();
    thread::Builder::new()
        .name("zakura-sim-explore".to_owned())
        .spawn(move || {
            for job in queue {
                if job() {
                    loop {
                        thread::park();
                    }
                }
            }
        })
        .expect("the OS can start a worker thread");
    jobs
}
