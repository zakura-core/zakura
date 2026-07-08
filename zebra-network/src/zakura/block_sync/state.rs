use super::{
    bbr::{rounded_usize, BbrState},
    config::*,
    request::*,
    work_queue::WorkQueue,
    *,
};
use crate::zakura::{
    chain_frontier_from_parts, Frontier, FrontierUpdate, ServicePeerDirection, ServicePeerSnapshot,
    ZakuraBlockSyncCandidateState,
};

/// Hard ceiling on outbound block-range requests kept in flight to one peer.
///
/// A safety bound only; the binding per-peer concurrency is the peer's advertised
/// `max_inflight_requests` (config `max_inflight_requests`, clamped to
/// [`MAX_BS_INFLIGHT_REQUESTS`]).
// `MAX_BS_INFLIGHT_REQUESTS` is a `u32`, which fits in `usize` on supported targets.
pub(super) const EFFECTIVE_BS_OUTBOUND_INFLIGHT_PER_PEER: usize = MAX_BS_INFLIGHT_REQUESTS as usize;

/// Cached chain frontiers used by the block-sync reactor.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct BlockSyncFrontiers {
    /// Shared finalized height supplied by state.
    pub finalized_height: block::Height,
    /// Highest verified block-body height supplied by state.
    pub verified_block_tip: block::Height,
    /// Hash of [`verified_block_tip`](Self::verified_block_tip).
    pub verified_block_hash: block::Hash,
}

/// Startup inputs for the dependency-neutral block-sync reactor.
#[derive(Clone, Debug)]
pub struct BlockSyncStartup {
    /// Cached state frontiers at startup.
    pub frontiers: BlockSyncFrontiers,
    /// Durable best header tip at startup.
    pub best_header_tip: (block::Height, block::Hash),
    /// Header-sync best-tip watch used as the moving body-download target.
    pub header_tip: Option<watch::Receiver<(block::Height, block::Hash)>>,
    /// Shared sync exchange frontier stream used as the moving body-download target.
    pub frontier_updates: Option<watch::Receiver<FrontierUpdate>>,
    /// Local stream-6 configuration.
    pub config: ZakuraBlockSyncConfig,
    /// Shared shutdown signal owned by the embedding endpoint or test harness.
    pub shutdown: CancellationToken,
    /// Enables query actions for state-backed metadata.
    pub state_queries_enabled: bool,
    /// JSONL trace emitter for block-sync scheduling, download, and commit rows.
    pub trace: ZakuraTrace,
}

impl BlockSyncStartup {
    /// Build block-sync startup config from durable/frontier facts.
    pub fn new(
        frontiers: BlockSyncFrontiers,
        best_header_tip: (block::Height, block::Hash),
        header_tip: watch::Receiver<(block::Height, block::Hash)>,
        config: ZakuraBlockSyncConfig,
    ) -> Self {
        Self {
            frontiers,
            best_header_tip,
            header_tip: Some(header_tip),
            frontier_updates: None,
            config,
            shutdown: CancellationToken::new(),
            state_queries_enabled: true,
            trace: ZakuraTrace::noop(),
        }
    }

    /// Build block-sync startup config from shared sync exchange frontiers.
    pub fn new_with_exchange(
        frontiers: BlockSyncFrontiers,
        best_header_tip: (block::Height, block::Hash),
        frontier_updates: watch::Receiver<FrontierUpdate>,
        config: ZakuraBlockSyncConfig,
    ) -> Self {
        Self {
            frontiers,
            best_header_tip,
            header_tip: None,
            frontier_updates: Some(frontier_updates),
            config,
            shutdown: CancellationToken::new(),
            state_queries_enabled: true,
            trace: ZakuraTrace::noop(),
        }
    }

    /// Build a latest-value frontier update stream from legacy startup pieces.
    pub fn frontier_update_from_parts(
        frontiers: BlockSyncFrontiers,
        best_header_tip: (block::Height, block::Hash),
    ) -> FrontierUpdate {
        FrontierUpdate {
            frontier: chain_frontier_from_parts(
                frontiers.finalized_height,
                Frontier::new(frontiers.verified_block_tip, frontiers.verified_block_hash),
                Frontier::new(best_header_tip.0, best_header_tip.1),
            ),
            change: crate::zakura::FrontierChange::Snapshot,
        }
    }

    pub(super) fn inert(config: ZakuraBlockSyncConfig) -> Self {
        Self {
            frontiers: BlockSyncFrontiers {
                finalized_height: block::Height::MIN,
                verified_block_tip: block::Height::MIN,
                verified_block_hash: block::Hash([0; 32]),
            },
            best_header_tip: (block::Height::MIN, block::Hash([0; 32])),
            header_tip: None,
            frontier_updates: None,
            config,
            shutdown: CancellationToken::new(),
            state_queries_enabled: false,
            trace: ZakuraTrace::noop(),
        }
    }
}

/// Cheap cloneable handle used by services and drivers to inform block sync.
///
/// per-peer routines carries the shared per-peer download primitives here too, so
/// `service::add_peer` (the pipe-routine spawn point) can wire each per-peer
/// pipe-routine with the same `WorkQueue`/`ByteBudget`/`PeerRegistry`/Sequencer/
/// action/routine-to-reactor channels the reactor created.
#[derive(Clone, Debug)]
pub struct BlockSyncHandle {
    pub(super) events: mpsc::Sender<BlockSyncEvent>,
    pub(super) lifecycle: mpsc::UnboundedSender<BlockSyncEvent>,
    pub(super) peers: watch::Receiver<ServicePeerSnapshot>,
    pub(super) status: watch::Receiver<BlockSyncStatus>,
    pub(super) candidates: watch::Receiver<ZakuraBlockSyncCandidateState>,
    /// Shared primitives every per-peer pipe-routine is wired with at spawn
    /// (`service::add_peer`). `None` for the inert/handle-less test constructors
    /// that never spawn routines.
    pub(super) routine_wiring: Option<RoutineWiring>,
}

/// The shared download primitives a per-peer pipe-routine is constructed with.
/// Created once in `spawn_block_sync_reactor` and threaded through the handle to
/// `service::add_peer`.
#[derive(Clone, Debug)]
pub(super) struct RoutineWiring {
    pub(super) config: ZakuraBlockSyncConfig,
    pub(super) budget: ByteBudget,
    pub(super) work: Arc<WorkQueue>,
    pub(super) registry: Arc<super::peer_registry::PeerRegistry>,
    pub(super) received_throughput: Arc<std::sync::Mutex<ThroughputMeter>>,
    pub(super) sequencer_input: mpsc::Sender<super::sequencer_task::SequencedBody>,
    pub(super) sequencer_input_bytes: Arc<std::sync::atomic::AtomicU64>,
    pub(super) sequencer_control:
        mpsc::UnboundedSender<super::sequencer_task::SequencerControlInput>,
    pub(super) actions: mpsc::Sender<BlockSyncAction>,
    pub(super) routine_to_reactor: mpsc::Sender<super::events::RoutineToReactor>,
    pub(super) view: watch::Receiver<super::sequencer_task::SequencerView>,
    pub(super) trace: ZakuraTrace,
}

impl BlockSyncHandle {
    /// Send a fact/event to the block-sync reactor.
    pub async fn send(
        &self,
        event: BlockSyncEvent,
    ) -> Result<(), mpsc::error::SendError<BlockSyncEvent>> {
        self.events.send(event).await
    }

    /// Try to send a fact/event without awaiting.
    pub fn try_send(
        &self,
        event: BlockSyncEvent,
    ) -> Result<(), mpsc::error::TrySendError<BlockSyncEvent>> {
        self.events.try_send(event)
    }

    /// Send a control-plane event without sharing the bounded wire-event queue.
    pub fn send_control(
        &self,
        event: BlockSyncEvent,
    ) -> Result<(), mpsc::error::SendError<BlockSyncEvent>> {
        self.lifecycle
            .send(event)
            .map_err(|error| mpsc::error::SendError(error.0))
    }

    /// Send a peer lifecycle event without sharing the bounded wire-event queue.
    pub fn send_lifecycle(
        &self,
        event: BlockSyncEvent,
    ) -> Result<(), mpsc::error::SendError<BlockSyncEvent>> {
        self.send_control(event)
    }

    /// Return the currently cached peer slot snapshot.
    pub fn peer_snapshot(&self) -> ServicePeerSnapshot {
        *self.peers.borrow()
    }

    /// Subscribe to local block-sync status advertisements.
    pub fn subscribe_status(&self) -> watch::Receiver<BlockSyncStatus> {
        self.status.clone()
    }

    /// Return the currently cached local status advertisement.
    pub fn local_status(&self) -> BlockSyncStatus {
        *self.status.borrow()
    }

    /// Subscribe to block-sync candidate-selection hints.
    pub fn subscribe_candidate_state(&self) -> watch::Receiver<ZakuraBlockSyncCandidateState> {
        self.candidates.clone()
    }

    /// Return the currently cached block-sync candidate-selection hints.
    pub fn candidate_state(&self) -> ZakuraBlockSyncCandidateState {
        self.candidates.borrow().clone()
    }
}

#[derive(Debug)]
pub(super) struct BlockSyncState {
    pub(super) finalized_height: block::Height,
    pub(super) verified_block_hash: block::Hash,
    pub(super) servable_high: block::Height,
    pub(super) servable_hash: block::Hash,
    pub(super) best_header_tip: block::Height,
    pub(super) best_header_hash: block::Hash,
    /// Thin per-peer handles the reactor keeps for demux/serving/admission. The
    /// per-peer *download* state moved into the spawned [`PeerRoutine`](super::peer_routine)
    /// (per-peer routines); the cross-peer facts the reactor/producer need live in the
    /// [`PeerRegistry`](super::peer_registry).
    pub(super) peers: HashMap<ZakuraPeerId, PeerBlockState>,
    pub(super) parked_peers: HashSet<ZakuraPeerId>,
    /// Sorted set of needed download heights. Replaces the central
    /// `BlockRangeScheduler`: the per-peer issuance path pulls work in its own
    /// servable range, dedup/covered are `in_flight`, and the floor is GC only.
    /// `Arc` so the state stays cheaply `Clone` and the queue is shared with the
    /// Sequencer task and the per-peer routines.
    pub(super) work_queue: Arc<WorkQueue>,
    pub(super) budget: ByteBudget,
    pub(super) needed_heights: Vec<block::Height>,
    pub(super) status_refresh: RateMeter,
    pub(super) pending_status_refresh: bool,
    pub(super) last_advertised_status: BlockSyncStatus,
    /// Throughput of bodies received off the wire (the download rate). Shared
    /// with the per-peer routines (they `record` on receipt); the reactor samples
    /// it each trace tick. Compared against the Sequencer task's committed
    /// throughput it separates a download-limited sync from a commit-limited one.
    pub(super) received_throughput: Arc<std::sync::Mutex<ThroughputMeter>>,
}

impl BlockSyncState {
    pub(super) fn new(startup: &BlockSyncStartup) -> Self {
        let last_advertised_status = BlockSyncStatus {
            servable_low: block::Height::MIN,
            servable_high: startup.frontiers.verified_block_tip,
            tip_hash: startup.frontiers.verified_block_hash,
            max_blocks_per_response: startup.config.advertised_max_blocks_per_response(),
            max_inflight_requests: startup.config.advertised_max_inflight_requests(),
            max_response_bytes: startup.config.advertised_max_response_bytes(),
        };

        Self {
            finalized_height: startup.frontiers.finalized_height,
            verified_block_hash: startup.frontiers.verified_block_hash,
            servable_high: startup.frontiers.verified_block_tip,
            servable_hash: startup.frontiers.verified_block_hash,
            best_header_tip: startup.best_header_tip.0,
            best_header_hash: startup.best_header_tip.1,
            peers: HashMap::new(),
            parked_peers: HashSet::new(),
            work_queue: Arc::new(WorkQueue::new(startup.frontiers.verified_block_tip)),
            budget: ByteBudget::new(startup.config.max_inflight_block_bytes),
            needed_heights: Vec::new(),
            status_refresh: RateMeter::new(startup.config.status_refresh_interval),
            pending_status_refresh: false,
            last_advertised_status,
            received_throughput: Arc::new(std::sync::Mutex::new(ThroughputMeter::new(
                Instant::now(),
            ))),
        }
    }

    pub(super) fn peer_snapshot(&self, limits: ServicePeerLimits) -> ServicePeerSnapshot {
        let inbound = self
            .peers
            .values()
            .filter(|peer| peer.direction == ServicePeerDirection::Inbound)
            .count();
        let outbound = self
            .peers
            .values()
            .filter(|peer| peer.direction == ServicePeerDirection::Outbound)
            .count();
        ServicePeerSnapshot::new(inbound, outbound, limits)
    }
}

/// Carved out of `PeerBlockState` so the window math stays unit-testable
/// while the per-peer download state moves into the spawned
/// [`PeerRoutine`](super::peer_routine) (per-peer routines). The routine embeds one of these.
#[derive(Clone, Debug)]
pub(super) struct DownloadWindow {
    pub(super) max_inflight_requests: u32,
    pub(super) outstanding: Vec<OutstandingBlockRange>,
    /// Per-peer BBR-lite estimators + cwnd — the sole congestion controller. Under
    /// [`CwndUnit::Bytes`] the cwnd is itself a byte budget sourced from header size
    /// hints (no fixed per-request byte weight), so there is no `nominal_request_bytes`.
    bbr: BbrState,
    /// Whether the cwnd budgets outstanding work in request slots or reserved bytes.
    cwnd_unit: CwndUnit,
    /// Request-count cap used while byte-cwnd has no fresh BDP sample.
    pub(super) startup_request_cap: usize,
    /// Deadline by which an active peer must send another accepted full block.
    pub(super) block_liveness_deadline: Option<Instant>,
    /// Last time this peer was sent a block-body request.
    pub(super) last_request_at: Option<Instant>,
    /// Last time this peer sent an accepted full block body.
    pub(super) last_block_at: Option<Instant>,
    /// Consecutive `GetBlocks` requests sent since the last accepted full block body.
    pub(super) requests_without_block_progress: u32,
    /// Maximum no-progress requests this peer may receive in its current proof state.
    max_requests_without_block_progress: u32,
    /// Maximum no-progress requests this peer may receive before its first accepted body.
    initial_block_probe_requests: u32,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum LivenessOutcome {
    Ok,
    Disarm,
    Disconnect,
}

impl DownloadWindow {
    pub(super) fn new(config: &ZakuraBlockSyncConfig) -> Self {
        Self {
            max_inflight_requests: config.advertised_max_inflight_requests(),
            outstanding: Vec::new(),
            bbr: BbrState::new(config),
            cwnd_unit: config.bbr_cwnd_unit,
            startup_request_cap: usize::try_from(config.initial_inflight_requests)
                .unwrap_or(usize::MAX)
                .max(1),
            block_liveness_deadline: None,
            last_request_at: None,
            last_block_at: None,
            requests_without_block_progress: 0,
            max_requests_without_block_progress: config.max_requests_without_block_progress,
            initial_block_probe_requests: config.initial_block_probe_requests,
        }
    }

    pub(super) fn delivery_snapshot(&self, now: Instant) -> DeliverySnapshot {
        self.bbr.delivery_snapshot(now)
    }

    /// Record a completed request into the BBR estimators (RTprop / BtlBw / delivered)
    /// and advance the ProbeRtt phase machine. `delivered_bytes` is the request's total
    /// delivered body bytes — under the single-block-per-request invariant
    /// (`DEFAULT_BS_BLOCKS_PER_RESPONSE = 1`) this is the completing body's
    /// `serialized_bytes`. Call after removing the completed request from `outstanding`,
    /// so the in-flight measure reflects the post-completion queue depth.
    pub(super) fn record_delivery(
        &mut self,
        now: Instant,
        elapsed: Duration,
        blocks: u32,
        delivered_bytes: u64,
        snapshot: DeliverySnapshot,
    ) {
        // The ProbeRtt drain check compares this against `min_cwnd`, so the in-flight
        // measure MUST be in the cwnd's unit: request count under `Blocks`, reserved
        // body bytes under `Bytes`. Passing the raw request count under `Bytes` made the
        // drain check (`count <= min_cwnd_bytes`) trivially true, so the hold timer
        // started before the byte queue had actually drained and the RTprop sample could
        // still be contended.
        let inflight = match self.cwnd_unit {
            // `outstanding.len()` (a `usize` request count) widens to `u64` losslessly.
            CwndUnit::Blocks => self.outstanding.len() as u64,
            CwndUnit::Bytes => self.outstanding_reserved_bytes(),
        };
        self.bbr
            .record_delivery(now, elapsed, blocks, delivered_bytes, inflight, snapshot);
    }

    /// The effective BBR cwnd as a **request count**, for diagnostics that compare
    /// against the request-count hard cap (the periodic slot trace, cross-peer floor
    /// bias). Under `Blocks` this is the cwnd directly; under `Bytes` it is the byte
    /// cwnd divided by a representative body size, so it reads as "requests this peer's
    /// byte window admits". The byte cwnd itself is available via
    /// [`bbr_effective_cwnd_bytes`](Self::bbr_effective_cwnd_bytes).
    pub(super) fn bbr_effective_cwnd(&self) -> usize {
        match self.cwnd_unit {
            CwndUnit::Blocks => self.bbr.effective_cwnd(),
            CwndUnit::Bytes => {
                let cwnd_bytes = self.bbr.effective_cwnd() as u64;
                let rep = self.representative_body_bytes();
                usize::try_from((cwnd_bytes / rep.max(1)).max(1)).unwrap_or(usize::MAX)
            }
        }
    }

    /// The effective byte cwnd under `Bytes` (`None` under `Blocks`), for tracing.
    pub(super) fn bbr_effective_cwnd_bytes(&self) -> Option<u64> {
        matches!(self.cwnd_unit, CwndUnit::Bytes).then(|| self.bbr.effective_cwnd() as u64)
    }

    /// A representative body size in bytes for converting a byte cwnd into a request
    /// count: the mean reserved bytes across in-flight requests, falling back to the
    /// per-block worst case when nothing is outstanding. Used only for diagnostics and
    /// the floor-bypass byte bonus, never for admission.
    fn representative_body_bytes(&self) -> u64 {
        let outstanding = self.outstanding.len() as u64;
        if outstanding == 0 {
            return block::MAX_BLOCK_BYTES;
        }
        (self.outstanding_reserved_bytes() / outstanding).max(1)
    }

    /// Current RTprop estimate in ms (windowed min as of `now`), for tracing and
    /// floor-server preference. Filtering by `now` reports `None` (worst floor server)
    /// once a deteriorating peer's only fast samples age past the horizon, rather than a
    /// stale-low RTprop.
    pub(super) fn bbr_rtprop_ms(&self, now: Instant) -> Option<u64> {
        self.bbr.rtprop_ms(now)
    }

    /// The current BtlBw estimate in milli-blocks/sec (blocks/sec × 1000), for tracing.
    /// `None` under `Bytes`, where [`bbr_btlbw_bytes_per_sec`](Self::bbr_btlbw_bytes_per_sec)
    /// is the meaningful rate.
    pub(super) fn bbr_btlbw_milliblocks(&self, now: Instant) -> Option<u64> {
        matches!(self.cwnd_unit, CwndUnit::Blocks)
            .then(|| self.bbr.btlbw_milliblocks_per_sec(now))
            .flatten()
    }

    /// Current BtlBw estimate in bytes/sec under `Bytes` (`None` under `Blocks`), as of
    /// `now`. Filtering by `now` keeps a stale-high rate from tightening the above-floor
    /// request deadline after the peer has stopped delivering.
    pub(super) fn bbr_btlbw_bytes_per_sec(&self, now: Instant) -> Option<u64> {
        if !matches!(self.cwnd_unit, CwndUnit::Bytes) {
            return None;
        }
        self.bbr
            .btlbw_units_per_sec(now)
            // A non-negative finite bytes/sec rate rounds into u64 for any real link.
            .map(|rate| rate.round() as u64)
    }

    /// Bytes reserved across this peer's in-flight requests, for tracing the byte window
    /// occupancy.
    pub(super) fn bbr_inflight_bytes(&self) -> u64 {
        self.outstanding_reserved_bytes()
    }

    /// Total delivered through this peer's completed requests, for tracing — blocks
    /// under `Blocks`, bytes under `Bytes`.
    pub(super) fn bbr_delivered(&self) -> u64 {
        self.bbr.delivered()
    }

    /// The current BBR phase as a numeric code (0 = ProbeBw, 1 = ProbeRtt), for tracing.
    pub(super) fn bbr_phase_code(&self) -> u64 {
        self.bbr.phase_code()
    }

    /// The smoothed request round-trip in milliseconds the delay-gradient tracks.
    pub(super) fn bbr_smoothed_elapsed_ms(&self) -> Option<u64> {
        self.bbr.smoothed_elapsed_ms()
    }

    /// The delay-gradient cwnd ceiling in blocks once it binds (`None` while unbounded).
    pub(super) fn bbr_delay_cap(&self) -> Option<u64> {
        self.bbr
            .delay_cap()
            .map(|cap| u64::try_from(cap).unwrap_or(u64::MAX))
    }

    /// This peer's reliability estimate (goodput fraction) in per-mille (0–1000), for
    /// tracing the cwnd discount applied to a request-dropping carrier.
    pub(super) fn bbr_reliability_permille(&self) -> u64 {
        self.bbr.reliability_permille()
    }

    pub(super) fn available_slots(&self) -> usize {
        self.available_slots_at(Instant::now())
    }

    pub(super) fn available_slots_at(&self, now: Instant) -> usize {
        self.available_slots_with_bonus_at(0, now)
    }

    /// Available headroom allowing `bonus` extra in-flight requests beyond the BBR cwnd,
    /// still clamped to the peer's advertised hard cap. `bonus == 0` is the normal
    /// (above-floor) capacity used by [`Self::available_slots`]; a small positive `bonus` is
    /// the floor bypass — it lets the lowest missing height be fetched even when the
    /// peer is saturated at its cwnd, without ever exceeding the advertised inflight.
    ///
    /// The return value is non-zero exactly when there is room for at least one more
    /// request; callers use it as a gate, not an absolute count. Under
    /// [`CwndUnit::Bytes`] the cwnd is itself a byte budget (`BtlBw_bytes × RTprop ×
    /// gain`, from header size hints) compared against reserved body bytes, so a peer
    /// serving large bodies holds fewer in flight and a peer serving small bodies holds
    /// many — the in-flight *request* count falls out of `cwnd_bytes / body_size`. The
    /// controller is unit-agnostic; only this comparison differs — the seam that makes
    /// switching units a small change.
    pub(super) fn available_slots_with_bonus_at(&self, bonus: usize, now: Instant) -> usize {
        // BBR-lite is the sole congestion controller: cap in-flight at the BDP-derived
        // cwnd so a peer's queue stays at ~one BDP and head-of-line latency tracks
        // RTprop. The floor bypass adds `bonus` on top.
        let hard_cap = self.hard_outbound_capacity();
        match self.cwnd_unit {
            CwndUnit::Blocks => {
                let cwnd_slots = self
                    .bbr
                    .effective_cwnd()
                    .saturating_add(bonus)
                    .min(hard_cap);
                cwnd_slots.saturating_sub(self.outstanding.len())
            }
            CwndUnit::Bytes => {
                // The peer's advertised request-count cap still binds in byte mode: a peer
                // serving tiny bodies must never be issued more in-flight *requests* than it
                // advertised it will service, however much byte headroom the cwnd still
                // shows. Once the request count reaches the hard cap there is no slot,
                // regardless of bytes — mirroring the blocks-unit ceiling (review fix F2).
                let outstanding = self.outstanding.len();
                if outstanding >= hard_cap {
                    return 0;
                }
                if !self.bbr.has_fresh_bdp(now) {
                    let startup_cap = self.startup_request_cap.saturating_add(bonus).min(hard_cap);
                    if outstanding >= startup_cap {
                        return 0;
                    }
                }
                // The cwnd is already a byte budget. The floor bypass grants `bonus`
                // *representative* bodies of extra byte headroom — sized to the recent
                // per-request reservation, NOT the 2 MB worst case — so a starved floor
                // can still be fetched when the byte window is full without ballooning
                // the in-flight bytes far past the cwnd (which would defeat the byte
                // denomination's head-of-line bound). The take is still count-capped to
                // one block and passes the real `ByteBudget` reservation.
                let reserved = self.outstanding_reserved_bytes();
                let representative = self.representative_body_bytes();
                let bonus_bytes = (bonus as u64).saturating_mul(representative);
                let cwnd_bytes = (self.bbr.effective_cwnd() as u64).saturating_add(bonus_bytes);
                usize::try_from(cwnd_bytes.saturating_sub(reserved)).unwrap_or(usize::MAX)
            }
        }
    }

    /// Remaining cwnd **byte** headroom for a take under [`CwndUnit::Bytes`]: byte window
    /// (plus `bonus` representative bodies of floor-bypass headroom) less bytes already
    /// reserved in-flight. `None` under [`CwndUnit::Blocks`], where the window is a request
    /// count (via `available_slots_with_bonus` + per-request cap), not a byte ceiling.
    ///
    /// Used as the byte cap of the work-queue take, this makes the byte cwnd a real
    /// admission limit (outstanding reserved bytes ≤ window) rather than a nonzero gate, so
    /// a small window cannot issue a large multi-body request. The take always admits its
    /// first item for floor progress, so the only permitted overshoot is that single body.
    pub(super) fn cwnd_byte_headroom_at(&self, bonus: usize, now: Instant) -> Option<u64> {
        match self.cwnd_unit {
            CwndUnit::Blocks => None,
            // `available_slots_with_bonus` already returns the remaining byte headroom
            // (cwnd bytes + bonus bodies − reserved) under `Bytes`.
            CwndUnit::Bytes => Some(self.available_slots_with_bonus_at(bonus, now) as u64),
        }
    }

    #[cfg(test)]
    pub(super) fn available_slots_with_bonus(&self, bonus: usize) -> usize {
        self.available_slots_with_bonus_at(bonus, Instant::now())
    }

    #[cfg(test)]
    pub(super) fn cwnd_byte_headroom(&self, bonus: usize) -> Option<u64> {
        self.cwnd_byte_headroom_at(bonus, Instant::now())
    }

    /// Scale a base floor-bypass slot count by the peer's reliability discount, so the
    /// above-window bypass (slots granted *beyond* the cwnd to keep the lowest missing
    /// height moving through a saturated carrier) shrinks with the same signal that shrinks
    /// the window. A healthy peer (factor ≈ 1) keeps the full bypass; a sealed peer
    /// (factor → 0) gets none, so a wedged peer receives no requests of any kind.
    pub(super) fn scaled_floor_bonus(&self, base: usize) -> usize {
        // Shares the finite/non-negative rounding policy with `effective_cwnd` via
        // `rounded_usize`: the fallback `0` seals the bypass on a non-finite factor rather
        // than opening it (base ≥ 0 and factor ∈ [0, 1], so the fallback is defensive only).
        rounded_usize(base as f64 * self.bbr.reliability_factor(), 0)
    }

    #[cfg(test)]
    pub(super) fn reliability_factor(&self) -> f64 {
        self.bbr.reliability_factor()
    }

    /// Bytes reserved across this peer's in-flight requests (the per-request size
    /// estimates of heights not yet received). Recomputed on demand — the byte unit is
    /// experimental; a hot path would maintain a running counter instead.
    fn outstanding_reserved_bytes(&self) -> u64 {
        self.outstanding.iter().fold(0u64, |acc, range| {
            acc.saturating_add(range.reserved_bytes())
        })
    }

    /// Record `timed_out` requests that expired without a body. Applies the BBR cwnd dip
    /// once (one multiplicative, min-cwnd-bounded dip per batch) and ages the reliability
    /// EWMA once per timed-out request, so a chronically dropping peer keeps a suppressed
    /// cwnd (a smaller share of the work) rather than fully recovering on its next success.
    pub(super) fn record_timeout(&mut self, timed_out: usize) {
        self.bbr.dip_on_timeout();
        self.bbr.penalize_reliability(timed_out);
    }

    /// Age the reliability EWMA by **one** goodput failure for a short response (a
    /// `BlocksDone` terminator or `RangeUnavailable`) that left `missing > 0` heights
    /// unreceived. One failure *per request*, matching the per-request timeout charge
    /// ([`record_timeout`](Self::record_timeout)) and the per-request delivery credit
    /// ([`credit_late_delivery`](Self::credit_late_delivery)): the EWMA is a per-request
    /// goodput fraction, so charging one-per-missing-height would near-seal a peer for a
    /// single protocol-legal short answer once `max_blocks_per_response > 1` (at the shipped
    /// default of 1 the two denominations coincide). Unlike a timeout this does *not* dip the
    /// cwnd — a short response is a goodput, not a latency/congestion, signal — but it must
    /// still count so a peer cannot deliver one body per request to keep its
    /// liveness/no-progress accounting reset while dropping the rest.
    pub(super) fn penalize_short_response(&mut self, missing: usize) {
        if missing > 0 {
            self.bbr.penalize_reliability(1);
        }
    }

    /// Credit the reliability EWMA for a body that arrived *late* — after its request had
    /// timed out and been charged as a failure. Offsets that charge: a suddenly-slower peer
    /// whose fast-window backlog drains past the per-request deadline stays "weaker but
    /// kept" instead of being sealed like a dropping/wedged peer (which sends no late body).
    pub(super) fn credit_late_delivery(&mut self) {
        self.bbr.credit_late_success();
    }

    pub(super) fn has_block_progress(&self) -> bool {
        self.last_block_at.is_some()
    }

    pub(super) fn no_progress_request_cap(&self) -> u32 {
        if self.has_block_progress() {
            self.max_requests_without_block_progress
        } else {
            self.initial_block_probe_requests
        }
    }

    pub(super) fn arm_liveness(&mut self, now: Instant, timeout: Duration) {
        self.last_request_at = Some(now);
        self.requests_without_block_progress =
            self.requests_without_block_progress.saturating_add(1);
        if self.block_liveness_deadline.is_none() {
            self.block_liveness_deadline = Some(now + timeout);
        }
    }

    pub(super) fn note_block_progress(&mut self, now: Instant, timeout: Duration) {
        self.last_block_at = Some(now);
        self.requests_without_block_progress = 0;
        self.block_liveness_deadline = if self.outstanding.is_empty() {
            None
        } else {
            Some(now + timeout)
        };
    }

    pub(super) fn disarm_liveness_after_progress_if_idle(&mut self) {
        if self.outstanding.is_empty()
            && matches!(
                (self.last_request_at, self.last_block_at),
                (Some(request_at), Some(block_at)) if block_at >= request_at
            )
        {
            self.block_liveness_deadline = None;
        }
    }

    pub(super) fn clear_liveness_if_idle(&mut self) {
        if self.outstanding.is_empty() {
            self.block_liveness_deadline = None;
        }
    }

    /// Reset per-view no-progress accounting after a destructive view reset. The reset
    /// returned this peer's outstanding to the queue on *our* initiative (a reorg/rollback,
    /// not the peer's fault), so the in-flight probe streak must not stay charged against
    /// it: clearing `requests_without_block_progress` lets an unproven peer probe again
    /// instead of wedging at its one-probe cap forever (the reset also cleared its liveness
    /// deadline, so nothing would disconnect it). Proof state (`last_block_at`) is preserved.
    pub(super) fn note_view_reset(&mut self) {
        self.requests_without_block_progress = 0;
        self.clear_liveness_if_idle();
    }

    /// Push the block-liveness deadline out by `timeout` when a would-be disconnect is
    /// attributable to *local* outbound backpressure, not the peer: while our outbound queue
    /// is full the routine stops draining inbound, so a useful body may be sitting unread.
    /// Avoids punishing the peer for our own write-side congestion.
    pub(super) fn extend_liveness_deadline(&mut self, now: Instant, timeout: Duration) {
        self.block_liveness_deadline = Some(now + timeout);
    }

    pub(super) fn check_liveness(&self, now: Instant) -> LivenessOutcome {
        match self.block_liveness_deadline {
            None => LivenessOutcome::Ok,
            // Defensive: a deadline that exists with no recorded request was never armed by
            // `arm_liveness` (which always sets `last_request_at`), so it was not actually
            // earned by an outstanding request. Unreachable in production — every deadline
            // setter runs after a request is sent — so disarm it rather than disconnect the
            // peer over a deadline it never earned. Reached only by tests that set the
            // deadline directly.
            Some(deadline) if self.last_request_at.is_none() && now >= deadline => {
                LivenessOutcome::Disarm
            }
            Some(deadline) if now < deadline => LivenessOutcome::Ok,
            Some(_) => LivenessOutcome::Disconnect,
        }
    }

    pub(super) fn hard_outbound_capacity(&self) -> usize {
        usize::try_from(self.max_inflight_requests)
            .expect("u32 max inflight requests fits in usize on supported targets")
            .min(EFFECTIVE_BS_OUTBOUND_INFLIGHT_PER_PEER)
    }

    pub(super) fn outstanding_index_for_height(&self, height: block::Height) -> Option<usize> {
        self.outstanding
            .iter()
            .position(|outstanding| outstanding.request.contains(height))
    }

    pub(super) fn outstanding_index_for_start(&self, start_height: block::Height) -> Option<usize> {
        self.outstanding
            .iter()
            .position(|outstanding| outstanding.request.start_height == start_height)
    }
}

/// Thin per-peer handle the reactor keeps to serve inbound
/// `GetBlocks` (the session clone + serving meters), advertise our `Status`, count
/// admission, and tear down. The per-peer *download* state + inbound decode live
/// in the per-peer pipe-routine ([`PeerRoutine`](super::peer_routine)); servable/
/// caps live in the [`PeerRegistry`](super::peer_registry). There is no reactor→
/// routine channel (inverted data flow): the routine owns its own `FramedRecv`.
#[derive(Debug)]
pub(super) struct PeerBlockState {
    pub(super) session: BlockSyncPeerSession,
    pub(super) direction: ServicePeerDirection,
    /// Per-peer rate meter for the reactor's `Status` *advertisement* refresh
    /// (serving-tip change broadcast + retry to peers that have not acknowledged
    /// our Status). The inbound-status *reply* half lives on the routine's
    /// `status_reply_meter`; this half stays reactor-side because the reactor owns
    /// serving-tip advertisement.
    pub(super) refresh_meter: RateMeter,
    pub(super) served_blocks_inflight: u32,
    pub(super) served_block_requests: VecDeque<(block::Height, Instant)>,
}

impl PeerBlockState {
    pub(super) fn new(session: BlockSyncPeerSession, config: &ZakuraBlockSyncConfig) -> Self {
        Self {
            direction: session.direction(),
            session,
            refresh_meter: RateMeter::new(config.status_refresh_interval),
            served_blocks_inflight: 0,
            served_block_requests: VecDeque::new(),
        }
    }

    pub(super) fn try_start_serving_blocks(
        &mut self,
        local_inflight_cap: u32,
        start_height: block::Height,
    ) -> bool {
        if self.served_blocks_inflight >= local_inflight_cap {
            return false;
        }
        self.served_blocks_inflight = self.served_blocks_inflight.saturating_add(1);
        self.served_block_requests
            .push_back((start_height, Instant::now()));
        true
    }

    pub(super) fn serving_blocks_elapsed(&self, start_height: block::Height) -> Option<Duration> {
        self.served_block_requests
            .iter()
            .find_map(|(start, started)| (*start == start_height).then(|| started.elapsed()))
    }

    pub(super) fn finish_serving_blocks(
        &mut self,
        start_height: block::Height,
    ) -> Option<Duration> {
        self.served_blocks_inflight = self.served_blocks_inflight.saturating_sub(1);
        self.served_block_requests
            .iter()
            .position(|(start, _)| *start == start_height)
            .and_then(|index| self.served_block_requests.remove(index))
            .map(|(_, started)| started.elapsed())
    }
}

#[derive(Clone, Debug)]
pub(super) struct OutstandingBlockRange {
    pub(super) request: BlockRangeRequest,
    pub(super) queued_at: Instant,
    pub(super) deadline: Instant,
    pub(super) delivery_snapshot: DeliverySnapshot,
    pub(super) delivered_bytes: u64,
    pub(super) received: ReceivedBlockTracker,
}

#[derive(Copy, Clone, Debug)]
pub(super) struct DeliverySnapshot {
    pub(super) delivered: u64,
    pub(super) delivered_at: Instant,
}

impl OutstandingBlockRange {
    /// Bytes still reserved for this request: the sum of the per-height size
    /// estimates for every requested height not yet received. Each received body
    /// shrinks its estimate toward the actual size, so releasing this (on
    /// timeout/disconnect/short response) never over-releases bytes already handed
    /// to the reorder buffer.
    pub(super) fn reserved_bytes(&self) -> u64 {
        self.request
            .expected_blocks
            .iter()
            .filter(|expected| !self.has_received(expected.height))
            .fold(0u64, |acc, expected| {
                acc.saturating_add(expected.estimated_bytes)
            })
    }

    pub(super) fn estimated_bytes_for_height(&self, height: block::Height) -> Option<u64> {
        self.request.estimated_bytes_for_height(height)
    }

    pub(super) fn has_received(&self, height: block::Height) -> bool {
        self.request
            .offset_for_height(height)
            .is_some_and(|offset| self.received.contains_offset(offset))
    }

    pub(super) fn mark_received(&mut self, height: block::Height) {
        if let Some(offset) = self.request.offset_for_height(height) {
            self.received.insert_offset(offset);
        }
    }

    pub(super) fn record_body_bytes(&mut self, bytes: u64) {
        self.delivered_bytes = self.delivered_bytes.saturating_add(bytes);
    }

    /// Mark every requested height at or below `tip` as received and return the
    /// sum of the per-height size estimates those newly-received heights still
    /// held, so the caller releases exactly the reservation those heights held.
    pub(super) fn mark_received_through(&mut self, tip: block::Height) -> u64 {
        self.request
            .expected_blocks
            .iter()
            .filter(|expected| {
                expected.height <= tip
                    && self
                        .request
                        .offset_for_height(expected.height)
                        .is_some_and(|offset| self.received.insert_offset(offset))
            })
            .fold(0u64, |acc, expected| {
                acc.saturating_add(expected.estimated_bytes)
            })
    }

    pub(super) fn is_complete(&self) -> bool {
        self.received.len() == self.request.expected_blocks.len()
    }
}

/// Pure per-height byte-accounting state.
///
/// The shared [`ByteBudget`] is just the atomic sink. This ledger owns the
/// lifecycle arithmetic for one requested height:
/// `Reserved(estimate) -> Held(actual) -> Released`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum BlockBudgetLedger {
    Reserved(u64),
    Held(u64),
    Released,
}

impl BlockBudgetLedger {
    pub(super) fn reserved(estimate: u64) -> Self {
        Self::Reserved(estimate)
    }

    pub(super) fn current_charge(self) -> u64 {
        match self {
            Self::Reserved(bytes) | Self::Held(bytes) => bytes,
            Self::Released => 0,
        }
    }

    pub(super) fn release_reserved(&mut self) -> u64 {
        let released = match *self {
            Self::Reserved(bytes) => bytes,
            Self::Held(_) | Self::Released => 0,
        };
        *self = Self::Released;
        released
    }

    pub(super) fn reserved_charge(self) -> u64 {
        match self {
            Self::Reserved(bytes) => bytes,
            Self::Held(_) | Self::Released => 0,
        }
    }

    pub(super) fn is_reserved(self) -> bool {
        matches!(self, Self::Reserved(_))
    }

    /// Move a reserved height to held bytes and return the signed budget delta.
    ///
    /// Positive means charge more bytes; negative means release bytes.
    pub(super) fn settle(&mut self, actual: u64) -> i128 {
        match *self {
            Self::Reserved(reserved) => {
                *self = Self::Held(actual);
                i128::from(actual) - i128::from(reserved)
            }
            Self::Released => 0,
            Self::Held(_) => 0,
        }
    }

    /// Release the current charge exactly once.
    pub(super) fn release(&mut self) -> u64 {
        let charge = self.current_charge();
        *self = Self::Released;
        charge
    }
}

/// Number of distinct request offsets the [`ReceivedBlockTracker`] bitset can hold —
/// one per bit of its `u128`.
const RECEIVED_TRACKER_OFFSET_CAPACITY: u32 = u128::BITS;

// A request range carries one received-offset bit per requested height (offsets
// `0..count`). If the advertised block-count cap ever exceeded the bitset width,
// `bit_for_offset` would return `None` for the overflowing heights, so they could
// never be marked received, `is_complete()` would be unreachable, and the range would
// wedge (its reservation never released). Couple the two so a future cap bump that
// outgrows the bitset fails to compile instead of silently wedging.
const _: () = assert!(MAX_BS_BLOCKS_PER_REQUEST <= RECEIVED_TRACKER_OFFSET_CAPACITY);

#[derive(Clone, Debug, Default)]
pub(super) struct ReceivedBlockTracker {
    bits: u128,
    count: usize,
}

impl ReceivedBlockTracker {
    pub(super) fn len(&self) -> usize {
        self.count
    }

    fn contains_offset(&self, offset: u32) -> bool {
        Self::bit_for_offset(offset).is_some_and(|bit| self.bits & bit != 0)
    }

    fn insert_offset(&mut self, offset: u32) -> bool {
        let Some(bit) = Self::bit_for_offset(offset) else {
            return false;
        };
        if self.bits & bit != 0 {
            return false;
        }
        self.bits |= bit;
        self.count = self.count.saturating_add(1);
        true
    }

    fn bit_for_offset(offset: u32) -> Option<u128> {
        1u128.checked_shl(offset)
    }
}

#[derive(Clone, Debug)]
pub(super) struct RateMeter {
    pub(super) next_allowed: Instant,
    pub(super) interval: Duration,
}

impl RateMeter {
    pub(super) fn new(interval: Duration) -> Self {
        Self {
            next_allowed: Instant::now(),
            interval,
        }
    }

    pub(super) fn try_take(&mut self, now: Instant) -> bool {
        if now < self.next_allowed {
            return false;
        }
        self.next_allowed = now + self.interval;
        true
    }

    pub(super) fn is_ready(&self, now: Instant) -> bool {
        now >= self.next_allowed
    }

    pub(super) fn mark_taken(&mut self, now: Instant) {
        self.next_allowed = now + self.interval;
    }
}

/// Tracks block-body throughput (bytes and block counts) over the interval
/// between samples, so the trace snapshot can report download/commit rates while
/// driving toward the 1–2 Gbps target. `record` accumulates; `sample` snapshots
/// the per-second rate since the last sample and resets the window. The last
/// computed rate is cached so it can be read from the immutable trace path. Cost
/// is two saturating adds per body and one division per sample tick.
#[derive(Clone, Debug)]
pub(super) struct ThroughputMeter {
    bytes: u64,
    blocks: u64,
    window_start: Instant,
    last_bytes_per_sec: u64,
    last_blocks_per_sec: u64,
}

impl ThroughputMeter {
    pub(super) fn new(now: Instant) -> Self {
        Self {
            bytes: 0,
            blocks: 0,
            window_start: now,
            last_bytes_per_sec: 0,
            last_blocks_per_sec: 0,
        }
    }

    pub(super) fn record(&mut self, bytes: u64) {
        self.bytes = self.bytes.saturating_add(bytes);
        self.blocks = self.blocks.saturating_add(1);
    }

    /// Recompute the cached per-second rates from the bytes/blocks accumulated
    /// since the last sample, then reset the window. A non-positive interval
    /// (clock not advanced between samples) leaves the cached rates untouched.
    pub(super) fn sample(&mut self, now: Instant) {
        let elapsed = now
            .saturating_duration_since(self.window_start)
            .as_secs_f64();
        if elapsed <= 0.0 {
            return;
        }
        // `as u64` truncates a finite, non-negative rate; both numerator and
        // denominator are non-negative so the cast cannot wrap or go negative.
        self.last_bytes_per_sec = (self.bytes as f64 / elapsed) as u64;
        self.last_blocks_per_sec = (self.blocks as f64 / elapsed) as u64;
        self.bytes = 0;
        self.blocks = 0;
        self.window_start = now;
    }

    pub(super) fn bytes_per_sec(&self) -> u64 {
        self.last_bytes_per_sec
    }

    pub(super) fn blocks_per_sec(&self) -> u64 {
        self.last_blocks_per_sec
    }
}

// `ByteBudget` was promoted to `transport/guard.rs` so byte-rate protection is
// reusable across services. Re-exported here so existing block_sync call sites
// (`reorder.rs`, `scheduler.rs`, `tests.rs`, and the field on this module's
// state) keep resolving unchanged.
pub(crate) use crate::zakura::transport::ByteBudget;

pub(super) fn next_height(height: block::Height) -> Option<block::Height> {
    height.0.checked_add(1).map(block::Height)
}

pub(super) fn previous_height(height: block::Height) -> Option<block::Height> {
    height.0.checked_sub(1).map(block::Height)
}

pub(super) fn height_after_count(start: block::Height, count: u32) -> Option<block::Height> {
    start.0.checked_add(count).map(block::Height)
}
