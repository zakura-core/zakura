//! Clock abstraction used by Zakura transport rate-limit logic.
//!
//! The clock is `zakura_rt::time`'s. [`RealClock`] reads it, so the
//! simulator's virtual clock drives rate limits too. The trait only lets unit
//! tests step time by hand.

use zakura_rt::time::Instant;

/// Clock used by Zakura rate-limit logic.
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
