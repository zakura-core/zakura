//! Monotonic clock abstraction for deterministic regulation tests.

use tokio::time::Instant;

/// Clock used by native Zakura regulation.
pub trait Clock: Clone + Send + Sync + 'static {
    /// Return the current monotonic instant.
    fn now(&self) -> Instant;
}

/// Production clock backed by [`Instant::now`].
#[derive(Copy, Clone, Debug, Default)]
pub struct RealClock;

impl Clock for RealClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}
