//! GetBlocks-specific adapters for the shared regulation tools.
//!
//! Production serving uses the shared runtime and state-owned reads. The live
//! requester authorizes responses and fences publication through replacement.
//! Transport frame-table activation is staged separately.
#![allow(dead_code)]

pub(super) mod live_requester;
mod requester;
mod serving;
pub(super) mod session;
mod wire;

#[cfg(test)]
mod requester_tests;
#[cfg(test)]
mod tests;
