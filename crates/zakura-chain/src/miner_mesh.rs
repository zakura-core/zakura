//! The miner mesh: how a mining pool names its gateway node on chain.
//!
//! A pool that wants other pools to connect to it directly adds one
//! [`MinerMeshCommitment`] output to the coinbase of each block it mines. The
//! commitment carries 32 opaque bytes, the gateway's node ID as the networking
//! layer defines it. This crate only writes and reads those bytes.
//!
//! A commitment has no effect on block validity.

mod commitment;

#[cfg(test)]
mod tests;

pub use commitment::MinerMeshCommitment;
