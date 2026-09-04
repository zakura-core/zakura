//! Reusable resource accounting for native Zakura services.
//!
//! This facade provides the ownership mechanics shared by message-specific
//! policies. It does not decide what a message costs or what should happen
//! when capacity is unavailable. Those decisions stay with each service.

#[allow(dead_code)] // used by the first message policy in the stacked PR
mod outstanding_bytes;
#[allow(dead_code)] // used by the first message policy in the stacked PR
mod rate;
#[allow(dead_code)] // used by the first message policy in the stacked PR
mod slots;

#[allow(unused_imports)] // used by the first message policy in the stacked PR
pub(crate) use outstanding_bytes::{
    FrameLease, OutstandingByteBudget, OutstandingByteReservation, OutstandingCapacityError,
};
#[allow(unused_imports)] // used by the first message policy in the stacked PR
pub(crate) use rate::{
    CommittedRateReservation, RateBudget, RateBudgetConfigError, RateReservation,
    RateReservationError, RateReservationSpendError,
};
#[allow(unused_imports)] // used by the first message policy in the stacked PR
pub(crate) use slots::{SlotBudget, SlotBudgetCapacityError, SlotPermit};

#[cfg(test)]
mod tests;
