//! Counts the gateways that recent best-chain blocks name in their coinbase,
//! and tells the network which mining gateways to prefer.
//!
//! A mining node, or a relay that runs the miner mesh, keeps connections to
//! the gateways named most often. The
//! tracker follows the best chain's tip, reads each block once it is
//! [`QUALIFYING_DEPTH`](zakura_chain::miner_mesh::QUALIFYING_DEPTH) deep, reads
//! older blocks in the window when it has nothing newer to do, and repairs its
//! counts after a reorg. It hands the ranking to the network while the node is
//! synced, and an empty ranking otherwise.

mod chain_reads;
mod ranking;
mod tracker;

#[cfg(test)]
mod tests;

pub use ranking::ranking_channel;
pub use tracker::spawn;
