//! Iroh interop probe (SPEC COMPAT-1 to COMPAT-3).
//!
//! `interop-node` runs one zakura-quic or Iroh-backend node as an agent driven
//! over stdin; `matrix` spawns node processes and runs the COMPAT cells.
//! `run-matrix.sh` builds everything and runs both passes.

pub mod iroh_backend;
pub mod proto;
pub mod quic_backend;
