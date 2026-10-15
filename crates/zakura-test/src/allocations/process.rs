//! Cross-thread allocation accounting confined to a single-test child process.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex, PoisonError,
    },
    time::Duration,
};

use super::{AllocationStats, Observation};
use crate::command::CommandExt;

const CHILD_TEST: &str = "ZAKURA_ALLOCATION_CHILD_TEST";
static ENABLED: AtomicBool = AtomicBool::new(false);
static OBSERVATION: Mutex<Option<Observation>> = Mutex::new(None);
thread_local! {
    static OBSERVING: Cell<bool> = const { Cell::new(false) };
}

/// Exclude allocations made by the observer itself, including lock initialization.
struct Observing;

impl Drop for Observing {
    fn drop(&mut self) {
        let _ = OBSERVING.try_with(|observing| observing.set(false));
    }
}

/// Fast path used by the thread-local helper to reject mixed measurement scopes.
pub(super) fn is_active() -> bool {
    ENABLED.load(Ordering::Acquire)
}

/// Serialize allocation identity changes, skipping recursive allocator calls.
fn with_observation<T>(operation: impl FnOnce(&mut Observation) -> T) -> Option<T> {
    if !is_active() || OBSERVING.try_with(|observing| observing.replace(true)) != Ok(false) {
        return None;
    }
    let _observing = Observing;
    let mut observation = OBSERVATION.lock().unwrap_or_else(PoisonError::into_inner);
    observation.as_mut().map(operation)
}

/// Record an allocation or free on whichever thread owns it now.
pub(super) fn observe(operation: impl FnOnce(&mut Observation)) {
    with_observation(operation);
}

/// Keep realloc and its identity update atomic with respect to other threads.
/// Otherwise another allocation could reuse the old address before it is removed.
///
/// # Safety
/// The pointer, layout and size must satisfy `GlobalAlloc::realloc`.
pub(super) unsafe fn reallocate(pointer: *mut u8, layout: Layout, size: usize) -> Option<*mut u8> {
    with_observation(|observation| {
        // SAFETY: The caller forwards System's realloc contract unchanged.
        let replacement = unsafe { System.realloc(pointer, layout, size) };
        if !replacement.is_null() {
            observation.freed(pointer);
            observation.allocated(replacement, size);
        }
        replacement
    })
}

/// Run exactly this test in a child process, with no other tests sharing its allocator.
/// Pass the full libtest name, without the crate name. The child must finish in two minutes.
/// A misspelled name fails instead of silently measuring zero tests.
#[allow(clippy::print_stderr)]
pub fn in_isolated_process(test_name: &str, test: impl FnOnce()) {
    if std::env::var(CHILD_TEST).as_deref() == Ok(test_name) {
        test();
        return;
    }
    let executable = std::env::current_exe().expect("the running test binary has a path");
    let output = Command::new(&executable)
        .args([test_name, "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD_TEST, test_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn2((), executable.display())
        .expect("the isolated test process starts")
        .wait_with_output_or_timeout(Duration::from_secs(120))
        .expect("the isolated test finishes within its deadline")
        .assert_success()
        .expect("the isolated allocation assertions pass");
    output
        .stdout_line_contains("1 passed;")
        .expect("exactly one test ran");
    eprint!("{}", String::from_utf8_lossy(&output.output.stderr));
}

/// One continuous process-wide observation, with checkpoints across async transitions.
///
/// Start inside [`in_isolated_process`] after constructing fixtures. All worker allocations
/// and frees count until this guard drops, including frees on a different thread. Snapshotting
/// does not reset the observation. Allocator metadata and reserved virtual memory are excluded.
/// Do not combine this with thread-local `measure` or start another process observation.
#[derive(Debug)]
pub struct ProcessMeasurement {
    _private: (),
}

impl ProcessMeasurement {
    /// Begin an isolated lifecycle and verify this binary installed `TrackingAllocator`.
    pub fn start() -> Self {
        assert!(!is_active(), "process measurements cannot overlap");
        assert!(
            std::env::var_os(CHILD_TEST).is_some(),
            "use in_isolated_process first"
        );
        super::ACTIVE
            .with_borrow(|active| assert!(active.is_none(), "measurements cannot overlap"));
        let mut observation = OBSERVATION.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(observation.is_none(), "process measurements cannot overlap");
        *observation = Some(Observation::default());
        drop(observation);
        ENABLED.store(true, Ordering::Release);
        let measurement = Self { _private: () };
        let probe = std::hint::black_box(Vec::<u8>::with_capacity(64));
        assert!(
            measurement.snapshot().requests > 0,
            "install TrackingAllocator first"
        );
        drop(probe);
        measurement
    }

    /// Return cumulative peak and currently retained bytes without losing older allocations.
    pub fn snapshot(&self) -> AllocationStats {
        with_observation(|observation| observation.stats).expect("this observation is active")
    }
}

impl Drop for ProcessMeasurement {
    fn drop(&mut self) {
        ENABLED.store(false, Ordering::Release);
        let observation = OBSERVATION
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        drop(observation);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Storage allocated here is released by another thread within the same observation.
    #[test]
    fn cross_thread_free_updates_retained_bytes() {
        in_isolated_process(
            "allocations::process::tests::cross_thread_free_updates_retained_bytes",
            || {
                let (send, receive) = std::sync::mpsc::sync_channel::<Vec<u8>>(0);
                let (done, finished) = std::sync::mpsc::sync_channel(0);
                let worker = std::thread::spawn(move || {
                    drop(receive.recv_timeout(Duration::from_secs(5)).unwrap());
                    done.send(()).unwrap();
                });
                let measurement = ProcessMeasurement::start();
                let bytes = std::hint::black_box(vec![0u8; 1024 * 1024]);
                let allocated = measurement.snapshot();
                assert!(allocated.retained_bytes >= bytes.len());
                send.send(bytes).unwrap();
                finished.recv_timeout(Duration::from_secs(5)).unwrap();
                worker.join().unwrap();
                let freed = measurement.snapshot();
                assert!(freed.retained_bytes < 4096, "{freed:?}");
                assert!(freed.peak_live_bytes >= 1024 * 1024);
            },
        );
    }

    /// Resizing is measured as replacement storage, and cleanup restores the baseline.
    #[test]
    fn cross_thread_reallocation_keeps_one_live_identity() {
        in_isolated_process(
            "allocations::process::tests::cross_thread_reallocation_keeps_one_live_identity",
            || {
                let measurement = ProcessMeasurement::start();
                let bytes = vec![0u8; 1024];
                let bytes = std::thread::spawn(move || {
                    let mut bytes = bytes;
                    bytes.reserve_exact(1024 * 1024);
                    bytes
                })
                .join()
                .unwrap();
                assert!(measurement.snapshot().retained_bytes >= bytes.capacity());
                drop(bytes);
                assert!(measurement.snapshot().retained_bytes < 4096);
            },
        );
    }
}
