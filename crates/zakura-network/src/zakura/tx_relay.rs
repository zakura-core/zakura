//! `tx-relay`: the native transaction relay.
//!
//! Only the contract exists so far: [`reactor_model`] restates the fetch
//! rules of the node-level reactor and drives the pool port's model. The
//! machines, the wire messages and the service come later and are tested
//! against it.

pub(crate) mod reactor_model;
#[cfg(test)]
mod tests;
