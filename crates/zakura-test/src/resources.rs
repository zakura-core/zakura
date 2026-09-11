//! Process observations for declared load fixtures, independent of permit counts.

#![allow(
    unsafe_code,
    reason = "read-only getrusage initializes a local C value"
)]

use std::{io, time::Duration};

/// Process-wide observations, including any concurrently running tests.
#[derive(Clone, Copy, Debug)]
pub struct ProcessUsage {
    /// Total user and kernel CPU consumed since this process started.
    pub cpu: Duration,
    /// Process high-water resident memory. This is not current retained memory.
    pub peak_resident_bytes: u64,
}

impl ProcessUsage {
    /// Sample CPU and peak RSS where the operating system exposes `getrusage`.
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

/// Real mutex acquisition/hold times. Waits include scheduling and observation cost.
#[derive(Clone, Copy, Debug, Default)]
pub struct LockSnapshot {
    /// Number of acquisitions observed.
    pub acquisitions: u64,
    /// Sum of time spent acquiring the observed mutex.
    pub wait: Duration,
    /// Longest acquisition interval.
    pub max_wait: Duration,
    /// Longest observed hold interval.
    pub max_hold: Duration,
}

/// Opt-in observations around a production lock, shared across test adapters.
#[derive(Debug, Default)]
pub struct LockProbe(std::sync::Mutex<LockSnapshot>);

impl LockProbe {
    /// Call immediately after acquiring the target mutex.
    pub fn acquired_since(&self, before: std::time::Instant) -> LockHold<'_> {
        let waited = before.elapsed();
        let mut snapshot = self.0.lock().unwrap();
        snapshot.acquisitions += 1;
        snapshot.wait += waited;
        snapshot.max_wait = snapshot.max_wait.max(waited);
        LockHold {
            probe: self,
            acquired: std::time::Instant::now(),
        }
    }

    /// Read the collected observations.
    pub fn snapshot(&self) -> LockSnapshot {
        *self.0.lock().unwrap()
    }
}

/// Drop immediately before releasing the mutex being observed.
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

/// Bounded CI default, with an explicit expansion for scheduled qualification.
pub fn load_rounds() -> usize {
    std::env::var("ZAKURA_REGULATION_LOAD_ROUNDS")
        .ok()
        .map(|rounds| {
            rounds
                .parse::<usize>()
                .expect("load rounds must be an integer")
        })
        .unwrap_or(4)
        .clamp(1, 256)
}
