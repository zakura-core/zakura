//! Controllable storage for Zakura's P2P simulator.
//!
//! The simulator answers each storage request that production code sends
//! through its existing interface, and lets the run choose when and how each
//! one completes. The plan is `storage.md`.
//!
//! So far the crate holds only the header contract in [`header`]: the
//! operations a header-chain [`Port`](zakura_node_services::header_chain::Port)
//! serves, the outcomes each one may complete with, and the deadlines and
//! releases around them. The engine-backed adapter and the controllable
//! operation lifecycle build on it.

pub mod header;
