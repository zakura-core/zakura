#![allow(dead_code, unused_imports)] // activated by the serving migration

//! Shared admission and ownership for native Zakura message policies.
//!
//! Finite request policies supply their codec and response bound. The shared
//! admission path owns concurrency, rollback, execution, and response lifetimes.
//! Peer routines retain protocol dispatch and scheduling decisions.

mod request;
pub(crate) use request::{
    RequestAdmission, RequestPolicy, RequestSession, ResponsePermit, WorkAttempt, WorkLease,
};

mod reservations;
pub(crate) use reservations::{ClaimRefused, Reservations, ReserveRefused};

mod slots;
pub(crate) use slots::{SlotBudget, SlotPermit};

mod verdict;
pub(crate) use verdict::Verdict;

#[cfg(test)]
mod tests;
