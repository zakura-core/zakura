//! Counts the gateways that recent best-chain blocks name in their coinbase.
//!
//! Mining nodes will use the counts to choose which mining gateways to keep
//! connections to. The tracker follows the best chain's tip, reads each block
//! once it is 100 deep, reads older blocks in the window when it has nothing
//! newer to do, and repairs its counts after a reorg.

mod chain_reads;
mod tracker;

#[cfg(test)]
mod tests;

pub use tracker::spawn;
