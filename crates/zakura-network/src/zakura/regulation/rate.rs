//! Refundable reservations from a monotonic rate budget.

use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use thiserror::Error;
use tokio::{
    sync::{futures::Notified, Notify},
    time::sleep,
};

use crate::zakura::transport::{Clock, RealClock};

const NANOS_PER_SECOND: u128 = 1_000_000_000;

/// Invalid local configuration for a rate budget.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum RateBudgetConfigError {
    /// A zero capacity cannot admit any positive work.
    #[error("rate budget capacity must be greater than zero")]
    ZeroCapacity,
    /// A zero refill would make spent capacity unavailable forever.
    #[error("rate budget refill must be greater than zero")]
    ZeroRefill,
}

/// A rate reservation could not be admitted.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum RateReservationError {
    /// The requested amount can never fit in this budget.
    #[error("requested rate reservation {requested} exceeds capacity {capacity}")]
    ExceedsCapacity {
        /// Requested units.
        requested: u64,
        /// Configured burst capacity.
        capacity: u64,
    },
    /// The request can fit after refill or an earlier reservation is returned.
    #[error("rate reservation is temporarily unavailable for {retry_after:?}")]
    TemporarilyUnavailable {
        /// Refill time assuming no earlier return.
        retry_after: Duration,
    },
}

impl RateReservationError {
    /// Return the refill delay for a temporary rejection.
    pub(crate) fn retry_after(self) -> Option<Duration> {
        match self {
            Self::TemporarilyUnavailable { retry_after } => Some(retry_after),
            Self::ExceedsCapacity { .. } => None,
        }
    }
}

/// A caller tried to spend more than its refundable reservation.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
#[error("rate spend {spent} exceeds refundable reservation {refundable}")]
pub(crate) struct RateReservationSpendError {
    /// Units the caller tried to spend.
    pub(crate) spent: u64,
    /// Units that were still refundable.
    pub(crate) refundable: u64,
}

/// Shared tokens that bound a burst and sustained work rate.
///
/// Each instance has one caller-defined unit, such as response bytes or a
/// measured work unit. Incomparable resources use separate budgets. Time
/// replenishes spent tokens; dropping an uncommitted reservation returns its
/// tokens immediately.
#[derive(Clone, Debug)]
pub(crate) struct RateBudget<C: Clock = RealClock> {
    inner: Arc<RateBudgetInner<C>>,
}

#[derive(Debug)]
struct RateBudgetInner<C: Clock> {
    capacity: u64,
    refill_per_second: u64,
    clock: C,
    state: Mutex<RateState>,
    tokens_returned: Notify,
}

#[derive(Debug)]
struct RateState {
    available: u64,
    /// Fractional unit numerator in unit-nanoseconds.
    refill_remainder: u128,
    last_refill: tokio::time::Instant,
}

impl RateBudget<RealClock> {
    /// Create a production budget initialized at full capacity.
    pub(crate) fn new(
        capacity: u64,
        refill_per_second: u64,
    ) -> Result<Self, RateBudgetConfigError> {
        Self::with_clock(capacity, refill_per_second, RealClock)
    }

    /// Wait until `units` could be reserved, without consuming them.
    ///
    /// The notification is registered before the balance is rechecked, so a
    /// concurrent return cannot be missed.
    pub(crate) async fn wait_for(&self, units: u64) -> Result<(), RateReservationError> {
        self.ensure_reservation_fits(units)?;

        loop {
            let returned = self.inner.tokens_returned.notified();
            tokio::pin!(returned);
            Notified::enable(returned.as_mut());

            let Some(retry_after) = self.time_until_available(units) else {
                return Ok(());
            };

            tokio::select! {
                _ = &mut returned => {}
                _ = sleep(retry_after) => {}
            }
        }
    }
}

impl<C: Clock> RateBudget<C> {
    /// Create a budget with an injected monotonic clock.
    pub(crate) fn with_clock(
        capacity: u64,
        refill_per_second: u64,
        clock: C,
    ) -> Result<Self, RateBudgetConfigError> {
        if capacity == 0 {
            return Err(RateBudgetConfigError::ZeroCapacity);
        }
        if refill_per_second == 0 {
            return Err(RateBudgetConfigError::ZeroRefill);
        }

        let now = clock.now();
        Ok(Self {
            inner: Arc::new(RateBudgetInner {
                capacity,
                refill_per_second,
                clock,
                state: Mutex::new(RateState {
                    available: capacity,
                    refill_remainder: 0,
                    last_refill: now,
                }),
                tokens_returned: Notify::new(),
            }),
        })
    }

    /// Return the configured burst capacity.
    pub(crate) fn capacity(&self) -> u64 {
        self.inner.capacity
    }

    /// Return the configured refill rate in units per second.
    pub(crate) fn refill_per_second(&self) -> u64 {
        self.inner.refill_per_second
    }

    /// Return the currently available units after applying elapsed refill.
    pub(crate) fn available(&self) -> u64 {
        let mut state = self.lock_state();
        self.refill(&mut state);
        state.available
    }

    /// Reserve units now or return why they are unavailable.
    pub(crate) fn try_reserve(
        &self,
        units: u64,
    ) -> Result<RateReservation<C>, RateReservationError> {
        self.ensure_reservation_fits(units)?;

        let mut state = self.lock_state();
        self.refill(&mut state);
        if state.available < units {
            return Err(RateReservationError::TemporarilyUnavailable {
                retry_after: self.retry_after(&state, units),
            });
        }

        state.available -= units;
        Ok(RateReservation {
            budget: self.clone(),
            refundable: units,
        })
    }

    fn ensure_reservation_fits(&self, units: u64) -> Result<(), RateReservationError> {
        if units > self.capacity() {
            return Err(RateReservationError::ExceedsCapacity {
                requested: units,
                capacity: self.capacity(),
            });
        }

        Ok(())
    }

    fn time_until_available(&self, units: u64) -> Option<Duration> {
        let mut state = self.lock_state();
        self.refill(&mut state);
        (state.available < units).then(|| self.retry_after(&state, units))
    }

    fn retry_after(&self, state: &RateState, units: u64) -> Duration {
        let deficit = units.saturating_sub(state.available);
        let scaled_deficit = u128::from(deficit)
            .saturating_mul(NANOS_PER_SECOND)
            .saturating_sub(state.refill_remainder);
        let refill = u128::from(self.refill_per_second().max(1));
        let nanos = scaled_deficit.saturating_add(refill - 1) / refill;
        Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
    }

    fn refill(&self, state: &mut RateState) {
        let now = self.inner.clock.now();
        let elapsed = now.saturating_duration_since(state.last_refill);
        if elapsed.is_zero() || state.available == self.capacity() {
            state.last_refill = now;
            if state.available == self.capacity() {
                state.refill_remainder = 0;
            }
            return;
        }

        let produced = elapsed
            .as_nanos()
            .saturating_mul(u128::from(self.refill_per_second()))
            .saturating_add(state.refill_remainder);
        let whole_units = u64::try_from(produced / NANOS_PER_SECOND).unwrap_or(u64::MAX);
        state.available = self
            .capacity()
            .min(state.available.saturating_add(whole_units));
        state.refill_remainder = if state.available == self.capacity() {
            0
        } else {
            produced % NANOS_PER_SECOND
        };
        state.last_refill = now;
    }

    fn return_units(&self, units: u64) {
        if units == 0 {
            return;
        }

        let mut state = self.lock_state();
        self.refill(&mut state);
        state.available = self.capacity().min(state.available.saturating_add(units));
        if state.available == self.capacity() {
            state.refill_remainder = 0;
        }
        drop(state);
        self.inner.tokens_returned.notify_waiters();
    }

    fn lock_state(&self) -> MutexGuard<'_, RateState> {
        self.inner
            .state
            .lock()
            .expect("rate budget mutex should not be poisoned")
    }
}

/// Provisional ownership of a fully refundable rate reservation.
#[derive(Debug)]
#[must_use = "dropping a provisional rate reservation returns it"]
pub(crate) struct RateReservation<C: Clock = RealClock> {
    budget: RateBudget<C>,
    refundable: u64,
}

impl<C: Clock> RateReservation<C> {
    /// Commit the reservation and permanently spend `initial_spend` units.
    pub(crate) fn commit(
        mut self,
        initial_spend: u64,
    ) -> Result<CommittedRateReservation<C>, RateReservationSpendError> {
        if initial_spend > self.refundable {
            return Err(RateReservationSpendError {
                spent: initial_spend,
                refundable: self.refundable,
            });
        }

        self.refundable -= initial_spend;
        let refundable = self.refundable;
        self.refundable = 0;
        Ok(CommittedRateReservation {
            budget: self.budget.clone(),
            refundable,
        })
    }

    /// Return the units held by this provisional reservation.
    pub(crate) fn reserved(&self) -> u64 {
        self.refundable
    }
}

impl<C: Clock> Drop for RateReservation<C> {
    fn drop(&mut self) {
        self.budget.return_units(self.refundable);
        self.refundable = 0;
    }
}

/// A committed rate reservation whose unused units remain refundable.
#[derive(Debug)]
#[must_use = "dropping a committed rate reservation returns its unused units"]
pub(crate) struct CommittedRateReservation<C: Clock = RealClock> {
    budget: RateBudget<C>,
    refundable: u64,
}

impl<C: Clock> CommittedRateReservation<C> {
    /// Permanently spend units consumed by completed work.
    pub(crate) fn spend(&mut self, units: u64) -> Result<(), RateReservationSpendError> {
        if units > self.refundable {
            return Err(RateReservationSpendError {
                spent: units,
                refundable: self.refundable,
            });
        }

        self.refundable -= units;
        Ok(())
    }

    /// Return the units that will be returned when this owner is dropped.
    pub(crate) fn refundable(&self) -> u64 {
        self.refundable
    }

    /// Finish this reservation and return its unused units now.
    pub(crate) fn finish(self) {
        drop(self);
    }
}

impl<C: Clock> Drop for CommittedRateReservation<C> {
    fn drop(&mut self) {
        self.budget.return_units(self.refundable);
        self.refundable = 0;
    }
}
