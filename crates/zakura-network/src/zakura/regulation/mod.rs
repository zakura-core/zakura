//! Shared admission and ownership for native Zakura message policies.
//!
//! Finite request policies supply their codec and response bound. The shared
//! admission path owns concurrency, rollback, execution, and response lifetimes.
//! Peer routines retain protocol dispatch and scheduling decisions.

mod request;
pub(crate) use request::{
    AcquiredWorkSlot, RequestAdmission, RequestPolicy, RequestSession, ResponsePermit, WorkAttempt,
    WorkBlocked, WorkLease,
};
#[cfg(test)]
pub(crate) use request::{WorkBound, WorkResources};

mod slots;
pub(crate) use slots::{SlotBudget, SlotPermit};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod properties;
