//! Declarative scenario types for the block-sync fuzzer.
//!
//! A [`Scenario`] describes a synthetic chain, the node-under-test config, the peers
//! it downloads from (each with a [`ServeProfile`]), and a [`TipEvent`] timeline that
//! drives header growth, reanchors, and verified-tip resets (the "large → small"
//! changes). Everything is a deterministic function of `seed`, so a failing run
//! replays from its seed (bit-exact once the Phase-2 clock lands).

use std::time::Duration;

use rand::{rngs::StdRng, Rng, SeedableRng};
use zebra_chain::block;

use crate::zakura::{BlockSyncStatus, ZakuraBlockSyncConfig, ZakuraPeerId, MAX_BS_RESPONSE_BYTES};

/// A latency draw, fixed or uniform over `[low, high]`.
#[derive(Clone, Copy, Debug)]
pub(crate) enum LatencyDist {
    /// Always this duration.
    Fixed(Duration),
    /// Uniform in `[low, high]`.
    Uniform { low: Duration, high: Duration },
}

impl LatencyDist {
    /// Zero latency.
    pub(crate) fn zero() -> Self {
        Self::Fixed(Duration::ZERO)
    }

    pub(crate) fn sample(&self, rng: &mut StdRng) -> Duration {
        match *self {
            Self::Fixed(duration) => duration,
            Self::Uniform { low, high } => {
                let lo = low.as_micros().min(high.as_micros());
                let hi = low.as_micros().max(high.as_micros());
                if hi == lo {
                    Duration::from_micros(u64::try_from(lo).unwrap_or(u64::MAX))
                } else {
                    let micros = rng.gen_range(lo..=hi);
                    Duration::from_micros(u64::try_from(micros).unwrap_or(u64::MAX))
                }
            }
        }
    }

    fn is_zero(&self) -> bool {
        matches!(self, Self::Fixed(d) if d.is_zero())
    }
}

/// Periodic serve stall: every `every_responses` answered requests, sleep `duration`
/// before serving. Models an intermittently-stalling peer (a head-of-line inducer).
#[derive(Clone, Copy, Debug)]
pub(crate) struct IdleGap {
    pub(crate) every_responses: u64,
    pub(crate) duration: Duration,
}

/// How a synthetic peer answers the node's `GetBlocks`. This is where slow / fast /
/// idle / withholding / reordering peers are realised; the node's real `PeerRoutine`
/// reacts to whatever this produces.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ServeProfile {
    /// Delay before the first block of each response (models RTT + serve setup). The
    /// trace-proven head-of-line effect emerges from queue depth on top of this.
    pub(crate) first_block_latency: LatencyDist,
    /// Delay between blocks inside a response (models a rate-limited serve path;
    /// effective serve rate ≈ `1 / per_block_latency`). Ignored when
    /// [`bandwidth_bytes_per_sec`](Self::bandwidth_bytes_per_sec) is set.
    pub(crate) per_block_latency: LatencyDist,
    /// Optional **byte-accurate** serve bandwidth. When set, each block's serve delay is
    /// `block_bytes / bandwidth` instead of the fixed `per_block_latency`, so a response
    /// takes `first_block_latency + Σ bytes / bandwidth` — the realistic model where a
    /// big block genuinely takes longer to transmit. This is what lets the byte-cwnd
    /// controller observe a true bytes/sec BtlBw and size-dependent transfer time.
    pub(crate) bandwidth_bytes_per_sec: Option<u64>,
    /// Optional periodic stall.
    pub(crate) idle_gap: Option<IdleGap>,
    /// Probability in `[0, 1]` that a request is silently dropped (no response),
    /// forcing the node's request timeout / re-request path.
    pub(crate) drop_probability: f64,
    /// Inclusive height window this peer refuses to serve (answers `RangeUnavailable`),
    /// modelling a peer that is missing a range.
    pub(crate) withhold: Option<(block::Height, block::Height)>,
    /// Serve the blocks of a response in reverse order, exercising the reorder buffer.
    pub(crate) reorder: bool,
}

impl ServeProfile {
    /// Fast, lossless, in-order serving with no added latency.
    pub(crate) fn fast() -> Self {
        Self {
            first_block_latency: LatencyDist::zero(),
            per_block_latency: LatencyDist::zero(),
            bandwidth_bytes_per_sec: None,
            idle_gap: None,
            drop_probability: 0.0,
            withhold: None,
            reorder: false,
        }
    }

    /// A slow peer: a fixed RTT before the first block plus per-block serve latency.
    pub(crate) fn slow(rtt: Duration, per_block: Duration) -> Self {
        Self {
            first_block_latency: LatencyDist::Fixed(rtt),
            per_block_latency: LatencyDist::Fixed(per_block),
            ..Self::fast()
        }
    }

    /// A byte-accurate peer: a fixed base RTT plus a finite serve `bandwidth` (bytes/sec),
    /// so each block takes `bytes / bandwidth` to transmit. This is the model the
    /// byte-cwnd controller is meant to track — `elapsed ≈ base_rtt + bytes / bandwidth`.
    pub(crate) fn byte_rate(base_rtt: Duration, bandwidth_bytes_per_sec: u64) -> Self {
        Self {
            first_block_latency: LatencyDist::Fixed(base_rtt),
            bandwidth_bytes_per_sec: Some(bandwidth_bytes_per_sec.max(1)),
            ..Self::fast()
        }
    }

    pub(crate) fn first_block_is_zero(&self) -> bool {
        self.first_block_latency.is_zero()
    }

    pub(crate) fn per_block_is_zero(&self) -> bool {
        self.per_block_latency.is_zero()
    }
}

/// How the harness's mock commit pipeline drains the applyQ.
///
/// The default applies each contiguous body instantly. A stall profile injects a steady
/// per-commit delay and/or a periodic burst stall, modelling a slow/bursty commit drain
/// (the trace-proven 27–53 s `commit_finish` tails). Because the commit driver only
/// releases the byte budget once it reports the durable frontier *after* applying, a slow
/// drain lets the apply backlog (and the reserved bytes that bound it) build — so a run
/// can prove the queue is bounded by the memory ceiling, not by throttling download.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CommitProfile {
    /// Fixed delay applied before each body is committed (steady slow commit).
    pub(crate) per_commit_delay: Duration,
    /// Optional periodic burst stall: every `every_commits` applied bodies, pause for
    /// `duration` before continuing (the sawtooth the durable-watch must absorb).
    pub(crate) burst: Option<CommitBurstStall>,
}

/// A periodic burst stall in the mock commit pipeline.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CommitBurstStall {
    /// Pause after every this-many applied bodies.
    pub(crate) every_commits: u64,
    /// How long each pause lasts.
    pub(crate) duration: Duration,
}

/// One synthetic peer the node downloads from.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PeerSpec {
    /// Distinct identity byte (the peer id is `[id_byte; 32]`).
    pub(crate) id_byte: u8,
    /// Lowest height this peer advertises it can serve.
    pub(crate) servable_low: block::Height,
    /// Highest height this peer advertises it can serve.
    pub(crate) servable_high: block::Height,
    /// Advertised per-response block cap.
    pub(crate) max_blocks_per_response: u32,
    /// Advertised concurrent-request cap.
    pub(crate) max_inflight_requests: u32,
    /// When (relative to run start) this peer connects.
    pub(crate) connect_at: Duration,
    /// Optional time at which this peer disconnects (models churn).
    pub(crate) disconnect_at: Option<Duration>,
    /// How this peer serves.
    pub(crate) serve: ServeProfile,
}

impl PeerSpec {
    /// A fast, full-range peer present from the start, serving `[1, servable_high]`.
    pub(crate) fn fast(id_byte: u8, servable_high: block::Height) -> Self {
        Self {
            id_byte,
            servable_low: block::Height(1),
            servable_high,
            max_blocks_per_response: 16,
            max_inflight_requests: 64,
            connect_at: Duration::ZERO,
            disconnect_at: None,
            serve: ServeProfile::fast(),
        }
    }

    /// A full-range peer with a custom serve profile.
    pub(crate) fn with_serve(
        id_byte: u8,
        servable_high: block::Height,
        serve: ServeProfile,
    ) -> Self {
        Self {
            serve,
            ..Self::fast(id_byte, servable_high)
        }
    }

    pub(crate) fn status(&self, tip_hash: block::Hash) -> BlockSyncStatus {
        BlockSyncStatus {
            servable_low: self.servable_low,
            servable_high: self.servable_high,
            tip_hash,
            max_blocks_per_response: self.max_blocks_per_response,
            max_inflight_requests: self.max_inflight_requests,
            max_response_bytes: MAX_BS_RESPONSE_BYTES,
        }
    }

    pub(crate) fn peer_id(&self) -> ZakuraPeerId {
        ZakuraPeerId::new(vec![self.id_byte; 32]).expect("synthetic peer id is within bounds")
    }

    /// Deterministic per-peer RNG seed derived from the scenario seed and identity.
    pub(crate) fn rng_seed(&self, scenario_seed: u64) -> u64 {
        scenario_seed ^ u64::from(self.id_byte).wrapping_mul(0x9e37_79b9_7f4a_7c15)
    }
}

/// What changes at a scheduled point in a run (relative to run start).
#[derive(Clone, Copy, Debug)]
pub(crate) enum TipEventKind {
    /// Advance the best-header download target to `height` (`HeaderAdvanced`).
    GrowTo(block::Height),
    /// Move the best-header target down to `height` (`HeaderReanchored`).
    HeaderReanchor(block::Height),
    /// Reset the verified-body tip down to `height` (`VerifiedReset`) — a reorg/rollback.
    VerifiedReset(block::Height),
}

/// A timed change to the shared sync frontier (header growth / reanchor / reset).
#[derive(Clone, Copy, Debug)]
pub(crate) struct TipEvent {
    /// When, relative to run start, the change is published.
    pub(crate) at: Duration,
    /// The change.
    pub(crate) kind: TipEventKind,
}

/// A single fuzzer scenario.
#[derive(Clone, Debug)]
pub(crate) struct Scenario {
    /// Number of blocks in the synthetic chain `[1, blocks]`.
    pub(crate) blocks: u32,
    /// Corpus PRNG seed (block hashes/sizes are a deterministic function of it).
    pub(crate) seed: u64,
    /// Optional fixed per-block byte target (else random-small bodies).
    pub(crate) target_block_bytes: Option<usize>,
    /// Best-header download target at start (defaults to the full corpus height when
    /// built via [`Scenario::new`]). Lower values let `GrowTo` events grow the target.
    pub(crate) initial_best_header: block::Height,
    /// Block-sync config for the node under test.
    pub(crate) config: ZakuraBlockSyncConfig,
    /// The peers the node downloads from.
    pub(crate) peers: Vec<PeerSpec>,
    /// Timed frontier changes (header growth, reanchor, verified reset).
    pub(crate) timeline: Vec<TipEvent>,
    /// How the mock commit pipeline drains the applyQ (default: instant).
    pub(crate) commit: CommitProfile,
    /// Wall-clock bound for the run.
    pub(crate) deadline: Duration,
}

impl Scenario {
    /// A scenario over `blocks` heights whose initial header target is the full chain
    /// (so the node has everything to download immediately) and no timeline events.
    pub(crate) fn new(
        blocks: u32,
        seed: u64,
        config: ZakuraBlockSyncConfig,
        peers: Vec<PeerSpec>,
    ) -> Self {
        Self {
            blocks,
            seed,
            target_block_bytes: None,
            initial_best_header: block::Height(blocks),
            config,
            peers,
            timeline: Vec::new(),
            commit: CommitProfile::default(),
            deadline: Duration::from_secs(30),
        }
    }
}

/// What a finished run reports. Invariant checks consume this and the flushed trace.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FuzzOutcome {
    /// Highest contiguous committed height the mock commit pipeline reached.
    pub(crate) committed_tip: block::Height,
    /// The target the corpus defined.
    pub(crate) target: block::Height,
}

impl FuzzOutcome {
    pub(crate) fn reached_target(&self) -> bool {
        self.committed_tip >= self.target
    }
}

/// A default block-sync config for harness runs: generous byte budget (memory is not
/// the constraint under test by default), moderate per-response/inflight caps.
///
/// The BBR ProbeRTT cadence is scaled down to sub-second so the mechanism is exercised
/// within a fuzzer run's compressed wall-clock (production defaults are 10 s / 200 ms,
/// which never fire in a ~1 s run). `rtprop_window` matches the probe interval so a
/// stale (queue-inflated) RTprop sample ages out one interval after the probe that
/// replaced it.
pub(crate) fn fuzz_config() -> ZakuraBlockSyncConfig {
    ZakuraBlockSyncConfig {
        max_blocks_per_response: 16,
        max_inflight_requests: 256,
        max_inflight_block_bytes: u64::MAX,
        request_timeout: Duration::from_secs(30),
        bbr_probe_rtt_interval: Duration::from_millis(150),
        bbr_probe_rtt_duration: Duration::from_millis(30),
        bbr_rtprop_window: Duration::from_millis(150),
        ..ZakuraBlockSyncConfig::default()
    }
}

/// Construct a deterministic per-peer RNG.
pub(crate) fn peer_rng(scenario_seed: u64, spec: &PeerSpec) -> StdRng {
    StdRng::seed_from_u64(spec.rng_seed(scenario_seed))
}
