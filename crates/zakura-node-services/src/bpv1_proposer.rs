//! Narrow RPC-to-BPv1 mining template and solved-block publication hook
//! (`decision-1`, `decision-9`).
//!
//! Lives in `zakura-node-services` (below both `zakura-network` and `zakura-rpc`)
//! so `zakura-rpc` embeds BPv1 coinbase commitments during `getblocktemplate`
//! and triggers BPv1 solved-block publication beside `CommitMined` without
//! depending on BPv1's internal state machines.

use std::{fmt, sync::Arc};

use zakura_chain::block::{Block, Height};

/// Exact null-data push payload length for a BPv1 coinbase commitment
/// (`ASCII("BPV1") || 0x01 || C`, 37 bytes -> 39-byte `OP_RETURN` script ->
/// 48-byte serialized transparent output; decision-25).
pub const BPV1_COMMITMENT_NULL_DATA_LEN: usize = 37;

/// Narrow proposer hook connecting `zakura-rpc` (`getblocktemplate` &
/// `submitblock`) to BPv1's [`ProposerState`].
pub trait Bpv1ProposerHook: Send + Sync + fmt::Debug {
    /// Cheap pre-check indicating whether a template at `height` is eligible
    /// for a BPv1 commitment (post-NU7 and at least one live BPv1 session).
    ///
    /// Used by `zakura-rpc` to avoid serializing mempool transactions or
    /// reserving the 48-byte coinbase commitment output when BPv1 is inactive.
    fn should_prepare_template(&self, height: Height) -> bool {
        let _ = height;
        true
    }

    /// Prepares (or reuses by body identity, `opt-8`) a BPv1 encoding for a
    /// post-NU7 mining template and returns the 37-byte `OP_RETURN` null-data
    /// push payload (`ASCII("BPV1") || 0x01 || C`) to embed in a zero-value
    /// transparent coinbase output.
    ///
    /// The commitment keeps `light_client_root`, which the block's
    /// `HeaderMeta` carries once it is solved.
    ///
    /// Returns `None` when:
    /// - `height` is below NU7 (`decision-9`),
    /// - no eligible BPv1 peers are connected, or
    /// - encoding fails.
    ///
    /// Implementations must NEVER index an unsolved template as a servable block.
    fn prepare_template_commitment(
        &self,
        light_client_root: [u8; 32],
        height: Height,
        tx_count: u32,
        body_after_coinbase: &[u8],
    ) -> Option<[u8; BPV1_COMMITMENT_NULL_DATA_LEN]>;

    /// Publishes a solved block over BPv1 (`HeaderMeta` + `BlockDone` +
    /// interleaved seed parts) beside `CommitMined`, waiting on nothing in
    /// legacy broadcast.
    ///
    /// Returns `true` if the block's v6 coinbase held a valid commitment
    /// matching a prepared encoding for the same body, and `false` otherwise
    /// (e.g. pool-built coinbases or external submissions that missed the
    /// prepared encoding — left cleanly to existing relay).
    fn publish_solved_block(&self, block: Arc<Block>, height: Height) -> bool;
}
