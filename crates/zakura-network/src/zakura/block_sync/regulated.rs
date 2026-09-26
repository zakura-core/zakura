//! GetBlocks-specific adapters for the shared regulation tools.
//!
//! Activation follows requester and session integration. The existing service
//! remains in use until those pieces preserve the whole exchange contract.
#![allow(dead_code)]

mod requester;
mod serving;
pub(super) mod session;
mod wire;

#[cfg(test)]
mod requester_tests;
#[cfg(test)]
mod tests;
