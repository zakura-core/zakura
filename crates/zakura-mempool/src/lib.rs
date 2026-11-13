//! Zakura's transaction pool.
//!
//! This crate holds the pool's storage and its policy, which both pools use,
//! and the pool core: a pure state machine and the one task that runs it
//! behind the port in `zakura_node_services::mempool::port`.
