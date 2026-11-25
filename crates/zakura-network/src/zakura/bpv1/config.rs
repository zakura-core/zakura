//! Configuration and derived resource sizing for BPv1.
//!
//! BPv1 is opt-in (`enabled: false` by default) until fleet runs meet its
//! activation criteria. Every parameter has a documented default derived from
//! the W2 2 MB block budget and the 500 ms propagation target.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{
    constants::{
        DEFAULT_ADVERT_DELAY, DEFAULT_ADVERT_WINDOW_FULL_BLOCK,
        DEFAULT_ADVERT_WINDOW_FULL_SIZE_BYTES, DEFAULT_ADVERT_WINDOW_SMALL_BLOCK,
        DEFAULT_ADVERT_WINDOW_SMALL_SIZE_BYTES,
    },
    peer::block_gossip::advert_window,
};

use super::{
    blocks::{BlockConfig, DEFAULT_MAX_CONCURRENT_BLOCKS, DEFAULT_PART_BUFFER_BYTES},
    peer::{PeerConfig, DEFAULT_EARLY_AGE, DEFAULT_EARLY_PEER_BYTES_CAP},
    profile::Profile,
    wire::{CONTROL_FRAME_CAP, DATA_FRAME_CAP, PART_RULE},
};

// The machine and the operator default share one per-peer early cap, and it
// holds exactly two maximum `Part` frames.
const _: () = assert!(
    PeerConfig::for_profile(Profile::W2).early_peer_bytes_cap == DEFAULT_EARLY_PEER_BYTES_CAP
        && DEFAULT_EARLY_PEER_BYTES_CAP == 2 * PART_RULE.payload.max()
);

/// Default BPv1 retained part-buffer budget (32 MiB: 4 active + ~7 retained W2 blocks).
pub const DEFAULT_PART_RETENTION_BUDGET_BYTES: u64 = DEFAULT_PART_BUFFER_BYTES;

/// Default inline BLAKE3 proof-verification concurrency (`opt-3`).
pub const DEFAULT_VERIFY_CONCURRENCY: usize = 8;

/// Default Reed–Solomon decode/re-encode worker concurrency.
pub const DEFAULT_DECODE_CONCURRENCY: usize = 2;

/// Default maximum simultaneous BPv1 peer sessions.
pub const DEFAULT_MAX_BPV1_SESSIONS: usize = 64;

/// `zakura-quic` `ConnLoad` poll interval.
pub const PATH_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Bytes one session holds for the transport outside its output gates: one
/// encoded frame per stream, waiting for its grant. Each stream has one
/// writer that encodes a frame only after the previous one entered the
/// transport's queue. The node envelope counts them for each connection.
// Widening `u32` to `u64` is lossless.
pub const BPV1_SESSION_HELD_OUTPUT_BYTES: u64 = CONTROL_FRAME_CAP as u64 + DATA_FRAME_CAP as u64;

/// Main's block advertisement pacing, which unflagged `Have`s follow
/// (decision-26). Its values come from the network `Config`'s
/// `advert_window_*` and `advert_delay` settings; BPv1 adds none.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HavePacing {
    /// Initial group size for blocks at or above `full_size_bytes`.
    pub full_block_window: usize,
    /// Initial group size for blocks at or below `small_size_bytes`.
    pub small_block_window: usize,
    /// Body size at or below which the small-block window applies.
    pub small_size_bytes: u64,
    /// Body size at or above which the full-block window applies.
    pub full_size_bytes: u64,
    /// Delay from admission before peers outside the initial group hear of
    /// any part.
    pub delay: Duration,
}

impl Default for HavePacing {
    fn default() -> Self {
        Self {
            full_block_window: DEFAULT_ADVERT_WINDOW_FULL_BLOCK,
            small_block_window: DEFAULT_ADVERT_WINDOW_SMALL_BLOCK,
            small_size_bytes: DEFAULT_ADVERT_WINDOW_SMALL_SIZE_BYTES,
            full_size_bytes: DEFAULT_ADVERT_WINDOW_FULL_SIZE_BYTES,
            delay: DEFAULT_ADVERT_DELAY,
        }
    }
}

impl HavePacing {
    /// The pacing that main's block gossip uses under `config`.
    pub fn from_network_config(config: &crate::Config) -> Self {
        Self {
            full_block_window: config.advert_window_full_block,
            small_block_window: config.advert_window_small_block,
            small_size_bytes: config.advert_window_small_size_bytes,
            full_size_bytes: config.advert_window_full_size_bytes,
            delay: config.advert_delay,
        }
    }

    /// The initial group size for a block of `body_bytes`, as main's
    /// advertisement window computes it.
    pub fn window(&self, body_bytes: u64) -> usize {
        advert_window(
            self.full_block_window,
            self.small_block_window,
            self.small_size_bytes,
            self.full_size_bytes,
            body_bytes,
        )
    }
}

/// Operator configuration for `[network.zakura.bpv1]`.
///
/// Opt-in (`enabled = false` by default). When disabled, BPv1 advertises no
/// capability bit, opens no streams, and adds zero bytes to `NodeEnvelope`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ZakuraBpv1Config {
    /// Enable BPv1 block propagation on `Zakura` and `Dual` stacks.
    ///
    /// Ignored in `Legacy` mode (no headerchain). Default: `false` (opt-in).
    pub enabled: bool,
    /// Enable experimental proposer direct copies. Default: `false`.
    ///
    /// The session writer can enqueue a full stripe per peer before feedback
    /// arrives. Leave copies off unless measuring that policy explicitly.
    pub proposer_direct_copies: bool,
    /// Maximum simultaneous BPv1 peer sessions.
    pub max_sessions: usize,
    /// Maximum concurrently active (non-retired) blocks admitted for collection.
    pub max_concurrent_blocks: usize,
    /// Per-peer data early-record byte cap (default: two maximum W2 `Part` frames).
    pub early_peer_bytes_cap: usize,
    /// Maximum age of an unresolved early-record entry (milliseconds).
    pub early_age_ms: u64,
    /// BPv1 retained part-buffer byte budget (`part_buffer.bytes` cap).
    pub part_retention_budget_bytes: u64,
    /// Maximum concurrent inline BLAKE3 part-proof verifications (`opt-3`).
    pub verify_concurrency: usize,
    /// Maximum concurrent Reed–Solomon stripe decode / re-encode check jobs.
    pub decode_concurrency: usize,
}

impl Default for ZakuraBpv1Config {
    fn default() -> Self {
        Self {
            enabled: false,
            proposer_direct_copies: false,
            max_sessions: DEFAULT_MAX_BPV1_SESSIONS,
            max_concurrent_blocks: DEFAULT_MAX_CONCURRENT_BLOCKS,
            early_peer_bytes_cap: DEFAULT_EARLY_PEER_BYTES_CAP,
            early_age_ms: DEFAULT_EARLY_AGE.as_millis() as u64,
            part_retention_budget_bytes: DEFAULT_PART_RETENTION_BUDGET_BYTES,
            verify_concurrency: DEFAULT_VERIFY_CONCURRENCY,
            decode_concurrency: DEFAULT_DECODE_CONCURRENCY,
        }
    }
}

impl ZakuraBpv1Config {
    /// Validates that configured bounds are non-zero and self-consistent.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.max_sessions == 0 {
            return Err("bpv1.max_sessions must be > 0");
        }
        if self.max_concurrent_blocks == 0 {
            return Err("bpv1.max_concurrent_blocks must be > 0");
        }
        if self.verify_concurrency == 0 || self.decode_concurrency == 0 {
            return Err("bpv1 worker concurrencies must be > 0");
        }
        if self.early_peer_bytes_cap == 0 {
            return Err("bpv1.early_peer_bytes_cap must be > 0");
        }
        if self.part_retention_budget_bytes == 0 {
            return Err("bpv1.part_retention_budget_bytes must be > 0");
        }
        Ok(())
    }

    /// Derives [`PeerConfig`] for `profile` from this operator config.
    pub fn peer_config(&self, profile: Profile) -> PeerConfig {
        PeerConfig {
            early_peer_bytes_cap: self.early_peer_bytes_cap,
            early_age: Duration::from_millis(self.early_age_ms),
            ..PeerConfig::for_profile(profile)
        }
    }

    /// Derives [`BlockConfig`] for `profile` from this operator config and
    /// the network's advertisement pacing.
    pub fn block_config(&self, profile: Profile, have_pacing: HavePacing) -> BlockConfig {
        BlockConfig {
            profile,
            max_concurrent_blocks: self.max_concurrent_blocks,
            part_buffer_bytes: self.part_retention_budget_bytes,
            have_pacing,
        }
    }
}
