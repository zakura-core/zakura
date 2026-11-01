//! The process's stats file, for its campaign's evidence (explorer
//! implementation plan, E4b-min).
//!
//! With `ZAKURA_SIM_STATS` set, the target counts every run in a
//! [`Recorder`] and writes the counts to the variable's directory: every
//! 10 seconds, as a panic of the target's own unwinds out of its action on a
//! run, before it exits at its abandonment limit, and at a clean exit, which
//! libFuzzer makes with `exit`, so an `atexit` handler runs. A crash or a
//! kill skips the handler, and the file stays incomplete.

use std::{
    env, process,
    sync::{Mutex, OnceLock, PoisonError},
    thread,
};

use zakura_sim::{
    explore::{
        feedback::Counters,
        stats::{Recorder, STATS_VAR},
        Classified, ExploreRun,
    },
    harness::HarnessEntry,
};

extern "C" {
    /// The C library's `atexit`.
    fn atexit(callback: extern "C" fn()) -> std::ffi::c_int;
}

/// The process's recorder, made as its first run begins. `None` without
/// `ZAKURA_SIM_STATS`.
static RECORDER: OnceLock<Option<Mutex<Recorder>>> = OnceLock::new();

/// Marks a run of `entry`'s harness in progress, so an exit inside it
/// leaves the file incomplete. The first call makes the recorder, so a
/// process that crashes in its first run still leaves a file. Does nothing
/// without `ZAKURA_SIM_STATS`.
///
/// # Panics
///
/// With `harness:`, if the stats file can't be written.
pub(crate) fn begin(entry: &'static HarnessEntry) {
    if let Some(recorder) = recorder(entry) {
        recorder
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .begin();
    }
}

/// Counts `run` of `entry`'s harness, with the features `counters` hold,
/// and writes the counts when they're due. The returned guard writes them
/// if the target panics while it acts on the run, so a crash the target
/// raises leaves every run counted. Does nothing without
/// `ZAKURA_SIM_STATS`.
///
/// # Panics
///
/// With `harness:`, if a due write fails.
pub(crate) fn record(
    entry: &'static HarnessEntry,
    run: &ExploreRun,
    classified: &Classified,
    counters: Option<&Counters>,
) -> Acting {
    let Some(recorder) = recorder(entry) else {
        return Acting;
    };
    let mut recorder = recorder.lock().unwrap_or_else(PoisonError::into_inner);
    let features = counters.map(Counters::hits).unwrap_or_default();
    recorder.record(run, classified, &features);
    recorder.checkpoint_if_due().unwrap_or_else(|error| {
        panic!(
            "harness: writing {} failed: {error}",
            recorder.path().display()
        )
    });
    Acting
}

/// Writes the counts now, still incomplete: the process is about to stop
/// without a clean exit. Logs a failed write instead of panicking, since
/// it may run while a panic unwinds.
pub(crate) fn checkpoint() {
    let Some(Some(recorder)) = RECORDER.get() else {
        return;
    };
    let mut recorder = recorder.lock().unwrap_or_else(PoisonError::into_inner);
    if let Err(error) = recorder.checkpoint() {
        eprintln!(
            "zakura-sim stats: writing {} failed: {error}",
            recorder.path().display()
        );
    }
}

/// The target acting on a counted run. Dropped by a panic, it writes the
/// counts.
#[must_use]
pub(crate) struct Acting;

impl Drop for Acting {
    /// Writes the counts if a panic unwinds out of the action.
    fn drop(&mut self) {
        if thread::panicking() {
            checkpoint();
        }
    }
}

/// The process's recorder for `entry`'s harness, made on the first call.
fn recorder(entry: &'static HarnessEntry) -> Option<&'static Mutex<Recorder>> {
    RECORDER
        .get_or_init(|| {
            let dir = env::var_os(STATS_VAR).filter(|dir| !dir.is_empty())?;
            let recorder = Recorder::new(dir.as_ref(), entry, process::id())
                .unwrap_or_else(|error| panic!("harness: {STATS_VAR}: {error}"));
            // SAFETY: `finish` is a plain function that never unwinds, so
            // the C library may call it at exit.
            if unsafe { atexit(finish) } != 0 {
                panic!("harness: {STATS_VAR}: atexit refused the stats handler");
            }
            Some(Mutex::new(recorder))
        })
        .as_ref()
}

/// Writes the final counts as the process exits cleanly. If the exit comes
/// from inside a run, which holds the lock or has begun, the file stays
/// incomplete.
extern "C" fn finish() {
    let Some(Some(recorder)) = RECORDER.get() else {
        return;
    };
    let Ok(mut recorder) = recorder.try_lock() else {
        return;
    };
    if let Err(error) = recorder.finish() {
        eprintln!(
            "zakura-sim stats: writing {} failed: {error}",
            recorder.path().display()
        );
    }
}
