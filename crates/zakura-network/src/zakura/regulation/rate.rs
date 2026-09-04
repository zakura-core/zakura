//! Refundable byte-rate charges with deterministic refill arithmetic.

use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use thiserror::Error;
use tokio::{
    sync::{futures::Notified, Notify},
    time::sleep,
};

use super::{Clock, RealClock};

const NANOS_PER_SECOND: u128 = 1_000_000_000;

/// A rate-bucket charge could not be admitted.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum RateChargeError {
    /// The request can never fit in this bucket.
    #[error("requested rate charge {requested} exceeds bucket capacity {capacity}")]
    ExceedsCapacity {
        /// Requested byte charge.
        requested: u64,
        /// Configured bucket capacity.
        capacity: u64,
    },
    /// The request fits after tokens refill or another charge is refunded.
    #[error("rate charge is temporarily unavailable for {retry_after:?}")]
    TemporarilyUnavailable {
        /// Refill time assuming no earlier refund.
        retry_after: Duration,
    },
}

impl RateChargeError {
    /// Return the refill delay for a temporary rejection.
    #[allow(dead_code)] // consumed by the first message policy built on this facade
    pub(crate) fn retry_after(self) -> Option<Duration> {
        match self {
            Self::TemporarilyUnavailable { retry_after } => Some(retry_after),
            Self::ExceedsCapacity { .. } => None,
        }
    }
}

/// A commit tried to make more bytes permanent than the original charge.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
#[error("non-refundable charge {non_refundable} exceeds original charge {charged}")]
pub(crate) struct RateCommitError {
    /// Requested non-refundable bytes.
    pub(crate) non_refundable: u64,
    /// Original charge bytes.
    pub(crate) charged: u64,
}

/// Recorded frame usage exceeded a committed charge's refundable remainder.
#[derive(Copy, Clone, Debug, Eq, Error, PartialEq)]
#[error("rate usage {used} exceeds refundable remainder {remaining}")]
pub(crate) struct RateUsageError {
    /// Bytes being marked used.
    pub(crate) used: u64,
    /// Bytes that were still refundable.
    pub(crate) remaining: u64,
}

/// Shared byte-rate tokens that bound bursts and sustained throughput.
///
/// Time refills this balance, unlike [`OutstandingByteBudget`](
/// super::OutstandingByteBudget). A provisional [`RateCharge`] refunds in full
/// on drop; committing it leaves a caller-selected overhead permanently spent.
#[derive(Clone, Debug)]
pub(crate) struct ByteRateBucket<C: Clock = RealClock> {
    inner: Arc<ByteRateBucketInner<C>>,
}

#[derive(Debug)]
struct ByteRateBucketInner<C: Clock> {
    capacity: u64,
    refill_per_second: u64,
    clock: C,
    state: Mutex<ByteRateState>,
    capacity_returned: Notify,
}

#[derive(Debug)]
struct ByteRateState {
    tokens: u64,
    /// Fractional byte numerator in byte-nanoseconds.
    refill_remainder: u128,
    last_refill: tokio::time::Instant,
}

impl ByteRateBucket<RealClock> {
    /// Create a production bucket initialized at full capacity.
    pub(crate) fn new(capacity: u64, refill_per_second: u64) -> Self {
        Self::with_clock(capacity, refill_per_second, RealClock)
    }

    /// Wait until `bytes` could be charged, without consuming tokens.
    ///
    /// The wait races the calculated refill deadline with refund notifications.
    /// It registers the notification before rechecking, so an early refund can
    /// neither be missed nor leave the caller sleeping until the old deadline.
    pub(crate) async fn wait_for(&self, bytes: u64) -> Result<(), RateChargeError> {
        self.ensure_request_fits(bytes)?;

        loop {
            let refunded = self.inner.capacity_returned.notified();
            tokio::pin!(refunded);
            Notified::enable(refunded.as_mut());

            let Some(retry_after) = self.time_until_available(bytes) else {
                return Ok(());
            };

            tokio::select! {
                _ = &mut refunded => {}
                _ = sleep(retry_after) => {}
            }
        }
    }
}

impl<C: Clock> ByteRateBucket<C> {
    /// Create a bucket with an injected monotonic clock for deterministic tests.
    pub(crate) fn with_clock(capacity: u64, refill_per_second: u64, clock: C) -> Self {
        let now = clock.now();
        Self {
            inner: Arc::new(ByteRateBucketInner {
                capacity,
                refill_per_second,
                clock,
                state: Mutex::new(ByteRateState {
                    tokens: capacity,
                    refill_remainder: 0,
                    last_refill: now,
                }),
                capacity_returned: Notify::new(),
            }),
        }
    }

    /// Return the configured burst capacity.
    pub(crate) fn capacity(&self) -> u64 {
        self.inner.capacity
    }

    /// Return the configured refill rate in bytes per second.
    pub(crate) fn refill_per_second(&self) -> u64 {
        self.inner.refill_per_second
    }

    /// Return the current token balance after applying elapsed-time refill.
    pub(crate) fn balance(&self) -> u64 {
        let mut state = self.lock_state();
        self.refill(&mut state);
        state.tokens
    }

    /// Charge tokens now or return why the charge cannot proceed.
    pub(crate) fn try_charge(&self, bytes: u64) -> Result<RateCharge<C>, RateChargeError> {
        self.ensure_request_fits(bytes)?;

        let mut state = self.lock_state();
        self.refill(&mut state);
        if state.tokens < bytes {
            return Err(RateChargeError::TemporarilyUnavailable {
                retry_after: self.retry_after(&state, bytes),
            });
        }

        state.tokens -= bytes;
        Ok(RateCharge {
            bucket: self.clone(),
            refundable: bytes,
        })
    }

    fn ensure_request_fits(&self, bytes: u64) -> Result<(), RateChargeError> {
        if bytes > self.capacity() {
            return Err(RateChargeError::ExceedsCapacity {
                requested: bytes,
                capacity: self.capacity(),
            });
        }
        if bytes > 0 && self.refill_per_second() == 0 {
            return Err(RateChargeError::ExceedsCapacity {
                requested: bytes,
                capacity: 0,
            });
        }

        Ok(())
    }

    fn time_until_available(&self, bytes: u64) -> Option<Duration> {
        let mut state = self.lock_state();
        self.refill(&mut state);
        (state.tokens < bytes).then(|| self.retry_after(&state, bytes))
    }

    fn retry_after(&self, state: &ByteRateState, bytes: u64) -> Duration {
        let deficit = bytes.saturating_sub(state.tokens);
        let scaled_deficit = u128::from(deficit)
            .saturating_mul(NANOS_PER_SECOND)
            .saturating_sub(state.refill_remainder);
        let refill = u128::from(self.refill_per_second().max(1));
        let nanos = scaled_deficit.saturating_add(refill - 1) / refill;
        Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
    }

    fn refill(&self, state: &mut ByteRateState) {
        let now = self.inner.clock.now();
        let elapsed = now.saturating_duration_since(state.last_refill);
        if elapsed.is_zero() || state.tokens == self.capacity() {
            state.last_refill = now;
            if state.tokens == self.capacity() {
                state.refill_remainder = 0;
            }
            return;
        }

        let produced = elapsed
            .as_nanos()
            .saturating_mul(u128::from(self.refill_per_second()))
            .saturating_add(state.refill_remainder);
        let whole_bytes = produced / NANOS_PER_SECOND;
        let whole_bytes = u64::try_from(whole_bytes).unwrap_or(u64::MAX);
        state.tokens = self
            .capacity()
            .min(state.tokens.saturating_add(whole_bytes));
        state.refill_remainder = if state.tokens == self.capacity() {
            0
        } else {
            produced % NANOS_PER_SECOND
        };
        state.last_refill = now;
    }

    fn refund(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }

        let mut state = self.lock_state();
        self.refill(&mut state);
        state.tokens = self.capacity().min(state.tokens.saturating_add(bytes));
        if state.tokens == self.capacity() {
            state.refill_remainder = 0;
        }
        drop(state);
        self.inner.capacity_returned.notify_waiters();
    }

    fn lock_state(&self) -> MutexGuard<'_, ByteRateState> {
        self.inner
            .state
            .lock()
            .expect("byte-rate state mutex should not be poisoned")
    }
}

/// Provisional ownership of a refundable rate charge.
#[derive(Debug)]
#[must_use = "dropping a provisional rate charge refunds it"]
pub(crate) struct RateCharge<C: Clock = RealClock> {
    bucket: ByteRateBucket<C>,
    refundable: u64,
}

impl<C: Clock> RateCharge<C> {
    /// Commit the charge while leaving `non_refundable` bytes permanently spent.
    pub(crate) fn commit(
        mut self,
        non_refundable: u64,
    ) -> Result<CommittedRateCharge<C>, RateCommitError> {
        if non_refundable > self.refundable {
            return Err(RateCommitError {
                non_refundable,
                charged: self.refundable,
            });
        }

        let refundable = self.refundable - non_refundable;
        self.refundable = 0;
        Ok(CommittedRateCharge {
            bucket: self.bucket.clone(),
            refundable,
        })
    }

    /// Return the complete provisional charge.
    pub(crate) fn charged(&self) -> u64 {
        self.refundable
    }
}

impl<C: Clock> Drop for RateCharge<C> {
    fn drop(&mut self) {
        self.bucket.refund(self.refundable);
        self.refundable = 0;
    }
}

/// Committed rate charge whose remaining response capacity is still refundable.
#[derive(Debug)]
#[must_use = "dropping a committed rate charge refunds its unused response capacity"]
pub(crate) struct CommittedRateCharge<C: Clock = RealClock> {
    bucket: ByteRateBucket<C>,
    refundable: u64,
}

impl<C: Clock> CommittedRateCharge<C> {
    /// Permanently spend response bytes that were successfully queued.
    pub(crate) fn record_usage(&mut self, bytes: u64) -> Result<(), RateUsageError> {
        if bytes > self.refundable {
            return Err(RateUsageError {
                used: bytes,
                remaining: self.refundable,
            });
        }

        self.refundable -= bytes;
        Ok(())
    }

    /// Return the response bytes that would still be refunded on settlement.
    pub(crate) fn refundable(&self) -> u64 {
        self.refundable
    }

    /// Settle now, refunding all unused response capacity.
    pub(crate) fn settle(self) {
        drop(self);
    }
}

impl<C: Clock> Drop for CommittedRateCharge<C> {
    fn drop(&mut self) {
        self.bucket.refund(self.refundable);
        self.refundable = 0;
    }
}
