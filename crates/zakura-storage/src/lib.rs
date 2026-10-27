//! Controllable storage for Zakura's P2P simulator.
//!
//! The simulator answers each storage request that production code sends
//! through its existing interface, and lets the run choose when and how each
//! one completes. The plan is `storage.md`.
//!
//! The header side is in [`header`]: the contract for the operations a
//! header-chain [`Port`](zakura_node_services::header_chain::Port) serves, a
//! port over the real header-chain engine, the fixture chains it loads, and
//! a port whose operations wait for the run to complete them. The block side
//! is in [`block`]: the contract for the block-sync actions and range reads
//! a block service answers, and the fixture blocks it serves.

pub mod block;
pub mod header;
