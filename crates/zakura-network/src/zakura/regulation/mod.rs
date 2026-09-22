//! Shared, role-based regulation for native Zakura messages.
//!
//! - [`Serve`] and [`ServeSession`] own capacity and lifetimes for requests
//!   that produce one response.
//! - [`Reservations`] admit a response only if a request reserved it.
//! - [`Verdict`] names the result of checking one message.
//!
//! Services keep protocol dispatch in plain `match` arms.

mod serve;
#[cfg(test)]
pub(crate) use serve::tests::kit as serving_kit;
pub(crate) use serve::{
    PeerServeLimits, Responded, ResponseSink, Serve, ServeCapacity, ServeEnd, ServeSession,
    WorkLease,
};

mod reservations;
pub(crate) use reservations::{ClaimRefused, Reservations};

mod slots;
pub(crate) use slots::{OutputByteBudget, OutputGrant, SlotBudget, SlotPermit};

mod verdict;
pub(crate) use verdict::Verdict;

#[cfg(test)]
mod tests;
