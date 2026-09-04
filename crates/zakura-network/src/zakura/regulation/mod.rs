//! Reusable resource accounting for native Zakura services.
//!
//! This facade provides the ownership mechanics shared by message-specific
//! policies. It does not decide what a message costs or what should happen
//! when capacity is unavailable. Those decisions stay with each service.

mod outstanding_bytes;
mod rate;
mod slots;

#[allow(unused_imports)] // the facade keeps configuration and transition errors discoverable
pub(crate) use outstanding_bytes::{
    FrameLease, OutstandingByteBudget, OutstandingByteReservation, OutstandingCapacityError,
};
#[allow(unused_imports)] // the facade keeps configuration and transition errors discoverable
pub(crate) use rate::{
    CommittedRateReservation, RateBudget, RateBudgetConfigError, RateReservation,
    RateReservationError, RateReservationSpendError,
};
#[allow(unused_imports)] // the facade keeps configuration errors discoverable
pub(crate) use slots::{SlotBudget, SlotBudgetCapacityError, SlotPermit};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod properties;

#[cfg(test)]
pub(crate) mod test_support;
