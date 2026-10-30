//! The process's stats file, for its campaign's evidence (explorer
//! implementation plan, E4b-min).
//!
//! With `ZAKURA_SIM_STATS` set, the target counts every run in a
//! [`Recorder`] and writes the counts to the variable's directory: every
//! 10 seconds, before it acts on a run that isn't clean, and at a clean
//! exit, which libFuzzer makes with `exit`, so an `atexit` handler runs.
//! A crash or a kill skips the handler, and the file stays incomplete.

use std::{
    env, process,
    sync::{Mutex, OnceLock, PoisonError},
};

use zakura_sim::{
    explore::{
        feedback::Counters,
        stats::{Recorder, STATS_VAR},
        Classified, ExploreOutcome, ExploreRun,
    },
    harness::HarnessEntry,
};

extern "C" {
    /// The C library's `atexit`.
    fn atexit(callback: extern "C" fn()) -> std::ffi::c_int;
}

/// The process's recorder, made at its first run. `None` without
/// `ZAKURA_SIM_STATS`.
static RECORDER: OnceLock<Option<Mutex<Recorder>>> = OnceLock::new();

/// Counts `run` of `entry`'s harness, with the features `counters` hold,
/// and writes the counts when they're due, or now if the target will act
/// on the run. Does nothing without `ZAKURA_SIM_STATS`.
///
/// # Panics
///
/// With `harness:`, if the stats file can't be written.
pub(crate) fn record(
    entry: &'static HarnessEntry,
    run: &ExploreRun,
    classified: &Classified,
    counters: Option<&Counters>,
) {
    let Some(recorder) = recorder(entry) else {
        return;
    };
    let mut recorder = recorder.lock().unwrap_or_else(PoisonError::into_inner);
    let features = counters.map(Counters::hits).unwrap_or_default();
    recorder.record(run, classified, &features);
    let written = if classified.outcome == ExploreOutcome::Clean {
        recorder.checkpoint_if_due()
    } else {
        recorder.checkpoint()
    };
    written.unwrap_or_else(|error| {
        panic!(
            "harness: writing {} failed: {error}",
            recorder.path().display()
        )
    });
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

/// Writes the final counts as the process exits cleanly. A run in progress
/// holds the lock, and then the file stays incomplete.
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
