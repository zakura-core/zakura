//! Authority, ownership, and completion gates for asynchronous chain work.

mod authority;
mod completion;

pub use authority::{
    BodyWorkAuthority, BodyWorkOwner, HeaderSyncWorkOwner, HeaderWorkAuthority, HeaderWorkOwner,
};
pub use completion::{CompletionDecision, CompletionOwner, Gate, PendingOwners, StaleReason};
