//! GetBlocks-specific adapters for the shared regulation tools.
//!
//! Production serving uses the shared runtime and state-owned reads. Requester
//! authorization and frame-rule activation are staged for the next integration.
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
