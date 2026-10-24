//! Controllable storage for Zakura's P2P simulator.
//!
//! The simulator answers each storage request that production code sends
//! through its existing interface, and lets the run choose when and how each
//! one completes. The plan is `storage.md`.
//!
//! So far the crate holds the header side in [`header`]: the contract for
//! the operations a header-chain
//! [`Port`](zakura_node_services::header_chain::Port) serves, a port over the
//! real header-chain engine, and the fixture chains it loads. The
//! controllable operation lifecycle builds on both.

pub mod header;
