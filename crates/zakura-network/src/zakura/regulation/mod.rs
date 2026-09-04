//! Reusable resource regulation for native Zakura services.
//!
//! This facade owns the clocks and linear byte-accounting primitives shared by
//! service policy and the transport. Message-specific admission and settlement
//! remain in the service that defines their contract.

mod clock;
#[allow(dead_code)] // activated when the first message policy adopts owned reservations
mod outstanding;
#[allow(dead_code)] // activated when the first message policy bounds pending inputs
mod pending;
#[allow(dead_code)] // activated when the first message policy adopts byte-rate charges
mod rate;

pub use clock::{Clock, RealClock};
#[allow(unused_imports)] // message policy follow-ups consume the typed errors
pub(crate) use outstanding::{
    FrameLease, OutstandingByteBudget, OutstandingByteReservation, OutstandingCapacityError,
};
#[allow(unused_imports)] // message policy follow-ups consume the pending-input owner
pub(crate) use pending::{PendingInputBudget, PendingInputCapacityError, PendingInputPermit};
#[allow(unused_imports)] // message policy follow-ups consume the typed errors
pub(crate) use rate::{
    ByteRateBucket, CommittedRateCharge, RateCharge, RateChargeError, RateCommitError,
    RateUsageError,
};

#[cfg(test)]
mod tests;
