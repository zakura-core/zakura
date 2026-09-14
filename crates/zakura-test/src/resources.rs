//! Measure CPU time, peak memory, and lock delays during load tests.
//!
//! Capacity counters show how much work the node has allowed. These helpers let
//! tests also observe what that work costs. [`ProcessUsage`] reads CPU and memory
//! totals from the operating system. [`LockProbe`] records how long code waits
//! for a lock and how long it holds the lock.
//!
//! For example, a test can compare CPU totals before and after serving a batch
//! of requests. Those totals include other work in the same test program, so
//! they do not isolate one request. Lock timings also depend on thread scheduling.

// The operating system fills a local structure through a raw pointer. Rust
// requires `unsafe` for that call. We read the structure only if the call succeeds.
#![allow(
    unsafe_code,
    reason = "reading operating system statistics requires a raw pointer call"
)]

use std::{io, time::Duration};

/// CPU and memory totals for the entire test program, including other tests.
#[derive(Clone, Copy, Debug)]
pub struct ProcessUsage {
    /// CPU time used since the program started, including operating system work
    /// on its behalf. This is time spent executing, not elapsed wall clock time.
    pub cpu: Duration,
    /// Highest recorded number of bytes held in RAM for this program.
    /// This peak does not fall when memory is freed.
    pub peak_resident_bytes: u64,
}

impl ProcessUsage {
    /// Read the program's CPU and peak memory totals from the operating system.
    /// Returns an error on unsupported systems or if the query fails.
    pub fn sample() -> io::Result<Self> {
        #[cfg(unix)]
        {
            let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
            // SAFETY: getrusage writes a complete rusage on success. No value is
            // read on failure, and the pointer refers to writable local storage.
            if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: the successful getrusage call initialized every field.
            let usage = unsafe { usage.assume_init() };
            let time = |value: libc::timeval| -> io::Result<Duration> {
                Ok(
                    Duration::from_secs(u64::try_from(value.tv_sec).map_err(io::Error::other)?)
                        + Duration::from_micros(
                            u64::try_from(value.tv_usec).map_err(io::Error::other)?,
                        ),
                )
            };
            let rss_units = if cfg!(target_os = "macos") { 1 } else { 1024 };
            Ok(Self {
                cpu: time(usage.ru_utime)? + time(usage.ru_stime)?,
                peak_resident_bytes: u64::try_from(usage.ru_maxrss).map_err(io::Error::other)?
                    * rss_units,
            })
        }
        #[cfg(not(unix))]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "process usage observation requires Unix",
        ))
    }
}

/// Time spent waiting for and holding a lock. Timings include scheduling delays.
#[derive(Clone, Copy, Debug, Default)]
pub struct LockSnapshot {
    /// Number of times the lock was acquired.
    pub acquisitions: u64,
    /// Total time spent waiting to acquire the lock.
    pub wait: Duration,
    /// Longest wait for the lock.
    pub max_wait: Duration,
    /// Longest time the lock was held.
    pub max_hold: Duration,
}

/// Record wait and hold times around a lock used by the code being tested.
#[derive(Debug, Default)]
pub struct LockProbe(std::sync::Mutex<LockSnapshot>);

impl LockProbe {
    /// Record the wait and begin timing how long the lock is held.
    ///
    /// Capture `before` just before trying to acquire the lock, then call this
    /// immediately after acquiring it. Drop the returned guard just before
    /// releasing the lock. Hold time includes delays in the probe's bookkeeping,
    /// because the measured lock remains held during those delays.
    pub fn acquired_since(&self, before: std::time::Instant) -> LockHold<'_> {
        let acquired = std::time::Instant::now();
        let waited = acquired.duration_since(before);
        let mut snapshot = self.0.lock().unwrap();
        snapshot.acquisitions += 1;
        snapshot.wait += waited;
        snapshot.max_wait = snapshot.max_wait.max(waited);
        LockHold {
            probe: self,
            acquired,
        }
    }

    /// Read the collected observations.
    pub fn snapshot(&self) -> LockSnapshot {
        *self.0.lock().unwrap()
    }
}

/// Record how long the lock was held when this guard is dropped.
/// Drop it immediately before releasing the lock being measured.
pub struct LockHold<'a> {
    probe: &'a LockProbe,
    acquired: std::time::Instant,
}

impl Drop for LockHold<'_> {
    fn drop(&mut self) {
        let mut snapshot = self.probe.0.lock().unwrap();
        snapshot.max_hold = snapshot.max_hold.max(self.acquired.elapsed());
    }
}

/// Number of times a load test repeats its workload. Defaults to four.
/// `ZAKURA_REGULATION_LOAD_ROUNDS` overrides the default, clamped to 1 through 256.
///
/// # Panics
///
/// Panics if the override is not Unicode text or cannot be parsed as a `usize`.
/// The failure message includes the variable name, its value, and the reason.
pub fn load_rounds() -> usize {
    parse_load_rounds(std::env::var("ZAKURA_REGULATION_LOAD_ROUNDS"))
}

// Separate environment access so tests can supply invalid values without
// changing the settings of other tests running in the same process.
fn parse_load_rounds(value: Result<String, std::env::VarError>) -> usize {
    let rounds = match value {
        Ok(rounds) => rounds,
        Err(std::env::VarError::NotPresent) => return 4,
        Err(std::env::VarError::NotUnicode(rounds)) => {
            panic!("invalid ZAKURA_REGULATION_LOAD_ROUNDS value {rounds:?}: expected Unicode text")
        }
    };

    rounds
        .parse::<usize>()
        .unwrap_or_else(|error| {
            panic!("invalid ZAKURA_REGULATION_LOAD_ROUNDS value {rounds:?}: {error}")
        })
        .clamp(1, 256)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_rounds_defaults_only_when_absent_and_clamps_integers() {
        assert_eq!(parse_load_rounds(Err(std::env::VarError::NotPresent)), 4);
        for (value, expected) in [("0", 1), ("1", 1), ("64", 64), ("256", 256), ("257", 256)] {
            assert_eq!(parse_load_rounds(Ok(value.to_owned())), expected);
        }
    }

    #[test]
    #[should_panic(expected = "invalid ZAKURA_REGULATION_LOAD_ROUNDS value \"abc\":")]
    fn load_rounds_rejects_invalid_integers() {
        parse_load_rounds(Ok("abc".to_owned()));
    }

    #[cfg(unix)]
    #[test]
    #[should_panic(
        expected = "invalid ZAKURA_REGULATION_LOAD_ROUNDS value \"64\\xFF\": expected Unicode text"
    )]
    fn load_rounds_rejects_non_unicode_overrides() {
        use std::os::unix::ffi::OsStringExt;

        let value = std::ffi::OsString::from_vec(b"64\xff".to_vec());
        parse_load_rounds(Err(std::env::VarError::NotUnicode(value)));
    }

    #[test]
    fn lock_observation_ends_with_the_actual_guard() {
        let mutex = std::sync::Mutex::new(());
        let probe = LockProbe::default();
        let before = std::time::Instant::now();
        let guard = mutex.lock().unwrap();
        let observation = probe.acquired_since(before);
        assert_eq!(probe.snapshot().acquisitions, 1);
        assert_eq!(probe.snapshot().max_hold, Duration::ZERO);
        let held_since = std::time::Instant::now();
        std::thread::sleep(Duration::from_millis(1));
        let held = held_since.elapsed();
        drop(observation);
        drop(guard);
        assert!(probe.snapshot().max_hold >= held);
    }

    #[test]
    fn lock_observation_has_no_gap_between_wait_and_hold() {
        let probe = LockProbe::default();
        let before = std::time::Instant::now();
        let observation = probe.acquired_since(before);

        // Bookkeeping runs while the measured lock is held. Its time must not
        // disappear between the end of the wait and the start of the hold.
        let wait_ended = before + probe.snapshot().wait;
        assert_eq!(observation.acquired, wait_ended);
    }

    #[cfg(unix)]
    #[test]
    fn process_totals_do_not_decrease_between_samples() {
        let before = ProcessUsage::sample().unwrap();
        let after = ProcessUsage::sample().unwrap();
        assert!(after.cpu >= before.cpu);
        assert!(after.peak_resident_bytes >= before.peak_resident_bytes);
    }
}
