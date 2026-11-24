//! Part sampling and per-pull trace records for fleet logs.

use zakura_chain::block;
use zakura_quic::NodeId;

/// Deterministic part sampling predicate keyed by `(block_id, index)`.
///
/// Selects roughly 1 in 16 parts uniformly across all nodes so fleet traces can
/// correlate the same part across proposer, entry peers, and relays without logging
/// every part.
pub fn is_sampled_part(block_id: &block::Hash, index: u32) -> bool {
    let mut input = [0u8; 36];
    input[..32].copy_from_slice(&block_id.0);
    input[32..].copy_from_slice(&index.to_le_bytes());
    let digest = blake3::hash(&input);
    (digest.as_bytes()[0] & 0x0f) == 0
}

/// Final outcome of a single BPv1 pull (`Want` exchange) for per-pull tracing.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PullOutcome {
    /// All parts arrived within `SLOW_X * estimated`.
    OnTime,
    /// Pull triggered `SLOW_X` late release (`bpv1-64`).
    Late,
    /// Pull hit its `Want` request deadline (`bpv1-31`).
    Deadline,
}

/// One per-pull trace row, logged when a pull completes.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PullTraceRecord {
    /// Serving peer identity.
    pub peer: NodeId,
    /// Block consensus hash.
    pub block_id: block::Hash,
    /// Stripe index.
    pub stripe: u32,
    /// Requested bytes (`parts * S`).
    pub bytes: u64,
    /// Peer RTT estimate used when planning, in milliseconds (`None` if unmeasured).
    pub rtt_ms: Option<u64>,
    /// Peer rate estimate used when planning, in bytes/sec (`None` if unmeasured).
    pub rate_bps: Option<u64>,
    /// Estimated transfer duration (`estimated_arrival - issued_at`) in milliseconds.
    pub estimated_ms: u64,
    /// Observed time from issue to first part arrival, in milliseconds (`None` if none arrived).
    pub observed_first_ms: Option<u64>,
    /// Observed time from issue to `WantEnd` / final part arrival, in milliseconds.
    pub observed_last_ms: u64,
    /// Outcome classification.
    pub outcome: PullOutcome,
}
