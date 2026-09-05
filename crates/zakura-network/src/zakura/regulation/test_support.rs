//! Independent arithmetic shared by regulation tests, with no production transitions.

use std::time::Duration;

const NANOS: u128 = 1_000_000_000;

/// A token balance represented as a rational number of units.
///
/// Keeping the fraction in the balance avoids copying production's separate
/// whole-token and refill-remainder implementation. Excess credit is discarded.
#[derive(Clone, Debug)]
pub(crate) struct RateModel {
    capacity: u128,
    balance: u128,
    refill: u64,
}

impl RateModel {
    pub(crate) fn new(capacity: u64, refill: u64) -> Self {
        let capacity = u128::from(capacity) * NANOS;
        Self {
            capacity,
            balance: capacity,
            refill,
        }
    }

    pub(crate) fn available(&self) -> u64 {
        u64::try_from(self.balance / NANOS).expect("the balance fits its u64 capacity")
    }

    pub(crate) fn reserve(&mut self, units: u64) -> bool {
        if units > self.available() {
            return false;
        }
        self.balance -= u128::from(units) * NANOS;
        true
    }

    pub(crate) fn refund(&mut self, units: u64) {
        self.balance = self.capacity.min(self.balance + u128::from(units) * NANOS);
    }

    pub(crate) fn advance(&mut self, elapsed: Duration) {
        self.balance = self
            .capacity
            .min(self.balance + elapsed.as_nanos() * u128::from(self.refill));
    }
}
