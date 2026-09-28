#![allow(dead_code, unused_imports)] // activated by the first reactor adoption

//! Shared tools that regulate native Zakura message exchanges.
//!
//! - [`Serve`] admits requests without waiting, bounds execution and output,
//!   and ends every admitted request exactly once.
//! - [`Reservations`] admit a response only if this node requested it.
//! - [`CadenceBuckets`] and [`CadenceSender`] enforce and obey the rate that
//!   a row declares.
//! - [`sizing`] derives every capacity default from the throughput target.
//!
//! Every tool acts against a peer only on an unambiguous violation: an event
//! that no conformant peer could cause. Anything a conformant peer could cause
//! is traced instead. Local capacity limits make this node wait; they never
//! fault a peer.
//!
//! Main's `request` module stays until the block sync adoption replaces it.

mod cadence;
pub(crate) use cadence::{CadenceBuckets, CadenceCharge, CadenceSendError, CadenceSender};

mod request;
pub(crate) use request::{
    RequestAdmission, RequestPolicy, RequestSession, ResponsePermit, WorkAttempt,
};

mod serve;
pub(crate) use serve::{
    Produce, Responded, ResponseCap, ResponseSink, Serve, ServeCapacity, ServeConfigError,
    ServeEnd, ServeLimits, ServeViolation, SinkError, SinkProgress, WorkLease,
};

mod reservations;
pub(crate) use reservations::{
    ClaimRefused, Claimed, Ended, PoolEntry, PrecheckSlot, ReservationPool, Reservations,
    ReserveRefused, ResponsePrecheck, SharedReservations,
};

pub(crate) mod sizing;

mod slots;
pub(crate) use slots::{OutputByteBudget, OutputGrant, SlotBudget, SlotPermit};

#[cfg(test)]
pub(crate) mod test_family;
#[cfg(test)]
mod tests;
