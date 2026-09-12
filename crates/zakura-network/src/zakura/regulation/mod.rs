//! Shared admission and ownership for native Zakura message policies.
//!
//! Finite request policies supply their codec and response bound. The shared
//! admission path owns concurrency, rollback, execution, and response lifetimes.
//! Peer routines retain protocol dispatch and scheduling decisions.

mod request;
pub(crate) use request::{
    RequestAdmission, RequestPolicy, RequestSession, ResponsePermit, WorkAttempt, WorkLease,
};

mod slots;
pub(crate) use slots::{SlotBudget, SlotPermit};

mod response;
pub(crate) use response::ResponseCredit;

mod response_scope;
pub(crate) use response_scope::{
    ResponseAdmissionError, ResponseAuthorization, ResponseScope, ResponseWritePermission,
};

mod response_memory;
pub(crate) use response_memory::{ConnectionResponseMemory, ResponseMemory};

#[cfg(test)]
mod tests;
