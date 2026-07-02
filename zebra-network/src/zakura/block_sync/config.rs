use super::{error::*, wire::*, *};

/// Default number of blocks advertised per response.
///
/// Keep block-body ranges narrow so a missing response only holds one height at
/// the body-download floor.
pub const DEFAULT_BS_BLOCKS_PER_RESPONSE: u32 = 1;
/// Default advertised hard cap on concurrent in-flight block requests per peer.
///
/// This is the safety ceiling the BBR-lite cwnd is clamped to, **not** the
/// operating point: the binding per-peer concurrency is the measured
/// bandwidth-delay product (see `DownloadWindow`'s BBR controller), which is
/// normally far below this. In a homogeneous fleet this is the per-peer
/// concurrency ceiling every peer offers.
pub const DEFAULT_BS_MAX_INFLIGHT: u32 = 32000;
/// Initial per-peer cwnd (cold-start point), in blocks.
///
/// The BBR cwnd opens here and converges to the BDP-derived target once the first
/// delivery sample arrives, rather than opening at the full `max_inflight`. This
/// keeps the opening burst modest before a peer's rate/latency is known.
pub const DEFAULT_BS_INITIAL_INFLIGHT: u32 = 64;
/// Maximum peer-advertised in-flight request count accepted by this node.
///
/// This is the hard ceiling the default advertisement ([`DEFAULT_BS_MAX_INFLIGHT`]
/// = 32,000) is clamped to, and also the per-peer outstanding-request safety bound
/// (`EFFECTIVE_BS_OUTBOUND_INFLIGHT_PER_PEER`). It bounds how many concurrent
/// requests a remote peer can make us hold against it, so it doubles as a DoS bound.
pub const MAX_BS_INFLIGHT_REQUESTS: u32 = 32_768;
/// Default total response byte target advertised per range response.
pub const DEFAULT_BS_MAX_RESPONSE_BYTES: u32 = 32 * 1024 * 1024;
/// Default global byte budget reserved for later block-download scheduling.
pub const DEFAULT_BS_MAX_INFLIGHT_BLOCK_BYTES: u64 = 6 * 1024 * 1024 * 1024;
/// Worst-case serialized bytes reserved per requested block body.
///
/// Block-sync reserves this much per requested block at send time and only ever
/// shrinks the reservation toward the actual serialized size on receipt, so a
/// valid, already-downloaded body is never discarded for a full budget. Each
/// body arrives in its own `Block` frame bounded by [`block::MAX_BLOCK_BYTES`]
/// at decode (`MAX_BS_MESSAGE_BYTES > MAX_BLOCK_BYTES`), so the actual size can
/// never exceed this worst case and the shrink is always non-negative.
pub const BS_PER_BLOCK_WORST_CASE_BYTES: u64 = block::MAX_BLOCK_BYTES;
/// Default byte cap for speculative reorder look-ahead above the download floor.
///
/// The default leaves one advertised response worth of headroom below the global
/// byte budget. The synchronous floor-pop path is the funding guarantee when
/// that headroom has been consumed by races or changed configuration.
pub const DEFAULT_BS_MAX_REORDER_LOOKAHEAD_BYTES: u64 =
    // `DEFAULT_BS_MAX_RESPONSE_BYTES` is a `u32`, so widening to `u64` is lossless.
    DEFAULT_BS_MAX_INFLIGHT_BLOCK_BYTES - DEFAULT_BS_MAX_RESPONSE_BYTES as u64;
/// Default block-count cap for speculative reorder look-ahead bookkeeping.
pub const DEFAULT_BS_MAX_REORDER_LOOKAHEAD_BLOCKS: u32 = 4096;
/// Minimum submitted block applies required to resolve one checkpoint range.
///
/// The checkpoint verifier resolves a checkpoint window only after the whole
/// window, including the resolving checkpoint block, is queued. A node that
/// starts one height before a checkpoint-gap boundary can therefore need one
/// maximum checkpoint gap plus the boundary block in flight.
pub const MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES: usize =
    zebra_chain::parameters::checkpoint::constants::MAX_CHECKPOINT_HEIGHT_GAP + 1;
/// Default maximum submitted block applies awaiting verifier completion.
pub const DEFAULT_BS_MAX_SUBMITTED_BLOCK_APPLIES: usize = MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES;
/// The byte budget required to hold one full worst-case checkpoint range in
/// flight.
///
/// The checkpoint verifier resolves a block's commit only once the entire
/// contiguous range to the next checkpoint has been submitted, and every
/// submitted body stays reserved against `max_inflight_block_bytes` until it is
/// durable. A budget that cannot hold a whole worst-case range can never
/// complete one: the verifier never commits, nothing becomes durable, and no
/// bytes are ever released.
pub const BS_CHECKPOINT_RANGE_BYTE_FLOOR: u64 =
    // `MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES` is `MAX_CHECKPOINT_HEIGHT_GAP + 1`
    // (= 401), which fits `u64` losslessly; the product (~802 MB) cannot overflow.
    MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES as u64 * BS_PER_BLOCK_WORST_CASE_BYTES;
/// Default block-sync request timeout.
pub const DEFAULT_BS_REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
/// Default short leash on a floor (lowest-missing-height) request.
///
/// A floor request that has not been served within this window is rescued to a
/// faster carrier (returned to the queue + the peer retry-avoided), never letting
/// the contiguous download floor wait on a slow peer. Far tighter than the base
/// `request_timeout`, which governs patient above-floor speculation instead.
pub const DEFAULT_BS_FLOOR_RESCUE_TIMEOUT: Duration = Duration::from_secs(2);
/// Request-timeout windows allowed before block-progress liveness disconnects.
const BLOCK_PROGRESS_TIMEOUT_REQUESTS: u32 = 4;
/// Default cooldown before a no-progress peer may be admitted again.
pub const DEFAULT_BS_NO_PROGRESS_PEER_COOLDOWN: Duration = Duration::from_secs(180);
/// Default hard floor-peer avoid cooldown after a watchdog cancellation.
pub const DEFAULT_BS_FLOOR_PEER_AVOID_COOLDOWN: Duration = DEFAULT_BS_REQUEST_TIMEOUT;
/// Default block-sync status refresh interval after local frontier changes.
pub const DEFAULT_BS_STATUS_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
/// Default tolerated size-hint deviation percentage before a peer is reported.
pub const DEFAULT_BS_SIZE_DEVIATION_TOLERANCE: u32 = 200;
/// Default legacy range-fanout reservation multiplier.
pub const DEFAULT_BS_FANOUT: usize = 1;
/// Maximum peer-advertised aggregate byte target accepted per requested range.
///
/// A range response is sent as one `Block` frame per body, and each body frame
/// remains independently bounded by `MAX_BS_MESSAGE_BYTES`. This aggregate cap
/// only controls how many bounded body frames a server sends before `BlocksDone`.
pub const MAX_BS_RESPONSE_BYTES: u32 = DEFAULT_BS_MAX_RESPONSE_BYTES;

/// Default steady-state cwnd gain, as a percent of the bandwidth-delay product.
pub const DEFAULT_BS_BBR_CWND_GAIN_PERCENT: u32 = 200;
/// Default ProbeBW up-probe pacing gain, percent.
pub const DEFAULT_BS_BBR_PROBE_BW_GAIN_PERCENT: u32 = 125;
/// Default ProbeRTT cadence: how often to drain to re-measure the min-RTT.
pub const DEFAULT_BS_BBR_PROBE_RTT_INTERVAL: Duration = Duration::from_secs(10);
/// Default ProbeRTT hold time at the drained cwnd.
pub const DEFAULT_BS_BBR_PROBE_RTT_DURATION: Duration = Duration::from_millis(200);
/// Default windowed-min horizon for the RTprop (min-RTT) estimate. Kept equal to
/// the ProbeRTT interval so a stale min is always re-probed before it expires.
pub const DEFAULT_BS_BBR_RTPROP_WINDOW: Duration = Duration::from_secs(10);
/// Default max-filter horizon for the BtlBw (delivery-rate) estimate.
pub const DEFAULT_BS_BBR_DELIVERY_RATE_WINDOW: Duration = Duration::from_secs(10);
/// Default per-RTT Startup growth (percent ⇒ ≈2×/RTT exponential ramp).
pub const DEFAULT_BS_BBR_STARTUP_GROWTH_PERCENT: u32 = 200;
/// Default minimum cwnd in blocks — keeps the pipe primed and lets ProbeRTT send.
pub const DEFAULT_BS_BBR_MIN_CWND: u32 = 4;
/// Default minimum cwnd in **bytes** (the floor under [`CwndUnit::Bytes`]).
///
/// Under byte denomination the steady cwnd is `BtlBw_bytes × RTprop × gain`. For a
/// low-latency peer that product is small (a fast link with ~1 ms base RTT needs
/// little in flight to stay busy), so this floor — not the BDP — is the binding
/// operating window most of the time. It is sized to keep enough concurrent
/// single-block requests in flight to actually pipeline a server that has spare
/// capacity (the trace showed peers 60–86% idle at a pinned cwnd of 4), while the
/// size-aware delay-gradient still shrinks it back if a real standing queue forms.
/// **This is the primary live-A/B tuning lever** (`byte-cwnd-1`): raise it to push
/// more concurrency, lower it if floor head-of-line latency regresses.
pub const DEFAULT_BS_BBR_MIN_CWND_BYTES: u64 = 4 * 1024 * 1024;
/// Default delay-gradient down-adjust threshold, percent of RTprop.
pub const DEFAULT_BS_BBR_DELAY_GRADIENT_PERCENT: u32 = 150;
/// Default number of slots the floor request may borrow beyond the BBR cwnd, so the
/// lowest missing height is fetched even when every servable peer is at its cwnd.
pub const DEFAULT_BS_FLOOR_BYPASS_SLOTS: u32 = 2;

/// Unit the per-peer BBR cwnd budgets in-flight work against. The controller itself is
/// unit-agnostic (it sizes a cwnd from measured delivery rate × RTprop); the unit only
/// changes how outstanding work is counted against that cwnd in `available_slots`.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CwndUnit {
    /// Count outstanding *requests* against the cwnd (one request ≈ one slot), equal
    /// weight regardless of body size. The A/B baseline — retained for comparison
    /// against the byte controller and for tests.
    Blocks,
    /// Count reserved body *bytes* against the cwnd, where the cwnd is itself a byte
    /// bandwidth-delay product sourced from the header size hints (`BtlBw_bytes ×
    /// RTprop × gain`), so a peer serving large bodies holds fewer in flight and one
    /// serving small bodies holds many. The shipped default.
    #[default]
    Bytes,
}

/// Block-sync peer status advertisement.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct BlockSyncStatus {
    /// Earliest block body this peer can serve.
    pub servable_low: block::Height,
    /// Highest contiguous verified block body this peer can serve.
    pub servable_high: block::Height,
    /// Hash of `servable_high`.
    pub tip_hash: block::Hash,
    /// Maximum blocks the sender will serve per requested range.
    pub max_blocks_per_response: u32,
    /// Maximum concurrent `GetBlocks` requests the sender will service.
    pub max_inflight_requests: u32,
    /// Maximum total response bytes the sender targets per requested range.
    pub max_response_bytes: u32,
}

impl BlockSyncStatus {
    pub(super) fn encode_to<W: Write>(&self, writer: &mut W) -> Result<(), BlockSyncWireError> {
        write_height(writer, self.servable_low)?;
        write_height(writer, self.servable_high)?;
        self.tip_hash.zcash_serialize(&mut *writer)?;
        writer.write_u32::<LittleEndian>(clamp_advertised_blocks(self.max_blocks_per_response))?;
        writer.write_u32::<LittleEndian>(self.max_inflight_requests)?;
        writer.write_u32::<LittleEndian>(self.max_response_bytes.max(1))?;
        Ok(())
    }

    pub(super) fn decode_from<R: Read>(reader: &mut R) -> Result<Self, BlockSyncWireError> {
        Ok(Self {
            servable_low: read_height(reader)?,
            servable_high: read_height(reader)?,
            tip_hash: block::Hash::zcash_deserialize(&mut *reader)?,
            max_blocks_per_response: clamp_advertised_blocks(reader.read_u32::<LittleEndian>()?),
            max_inflight_requests: clamp_advertised_inflight(reader.read_u32::<LittleEndian>()?),
            max_response_bytes: clamp_advertised_response_bytes(reader.read_u32::<LittleEndian>()?),
        })
    }
}

impl Default for BlockSyncStatus {
    fn default() -> Self {
        Self {
            servable_low: block::Height::MIN,
            servable_high: block::Height::MIN,
            tip_hash: block::Hash([0; 32]),
            max_blocks_per_response: DEFAULT_BS_BLOCKS_PER_RESPONSE,
            max_inflight_requests: DEFAULT_BS_MAX_INFLIGHT,
            max_response_bytes: DEFAULT_BS_MAX_RESPONSE_BYTES,
        }
    }
}

/// Block-sync configuration nested under the Zakura P2P-v2 config.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ZakuraBlockSyncConfig {
    /// Deprecated compatibility key for older rollout configs.
    ///
    /// Zakura block sync is now selected by the top-level `v2_p2p` flag. This
    /// field is accepted but ignored so older configs keep parsing.
    #[doc(hidden)]
    #[serde(
        default,
        skip_serializing,
        deserialize_with = "deserialize_ignored_replace_legacy_syncer"
    )]
    pub replace_legacy_syncer: bool,
    /// Maximum blocks this node advertises per `GetBlocks` response.
    pub max_blocks_per_response: u32,
    /// Maximum concurrent `GetBlocks` requests this node advertises per peer.
    pub max_inflight_requests: u32,
    /// Initial per-peer BBR cwnd (cold-start point), in blocks; converges to the
    /// BDP-derived target once the first delivery is measured.
    pub initial_inflight_requests: u32,
    /// Maximum total response bytes this node advertises per `GetBlocks` response.
    pub max_response_bytes: u32,
    /// Maximum estimated bytes reserved for in-flight and buffered block bodies.
    pub max_inflight_block_bytes: u64,
    /// Maximum speculative body bytes held above the download floor.
    pub max_reorder_lookahead_bytes: u64,
    /// Maximum speculative body heights tracked above the download floor.
    pub max_reorder_lookahead_blocks: u32,
    /// How long to avoid reassigning an expired floor height to the same peer.
    #[serde(with = "humantime_serde")]
    pub floor_peer_avoid_cooldown: Duration,
    /// Depth for block-sync action/body channels, clamped to at least one full
    /// checkpoint range.
    pub max_submitted_block_applies: usize,
    /// Timeout for an outstanding block-body range request.
    #[serde(with = "humantime_serde")]
    pub request_timeout: Duration,
    /// Short leash on a floor request before its height is rescued to a faster
    /// carrier. Clamped positive and never above `request_timeout`.
    #[serde(with = "humantime_serde")]
    pub floor_rescue_timeout: Duration,
    /// How long to keep a peer disconnected after it makes no accepted block progress.
    #[serde(with = "humantime_serde")]
    pub no_progress_peer_cooldown: Duration,
    /// How often this node sends unsolicited status refreshes after local frontier changes.
    #[serde(with = "humantime_serde")]
    pub status_refresh_interval: Duration,
    /// Percentage deviation from advertised body-size hints tolerated before soft scoring.
    pub size_deviation_tolerance: u32,
    /// Legacy range-fanout reservation multiplier.
    ///
    /// No active scheduler currently sends the same range to multiple peers; this
    /// only keeps old configs parsing and sizes the validation floor for one
    /// worst-case floor request.
    pub fanout: usize,
    /// Steady-state cwnd as a percent of the measured bandwidth-delay product.
    pub bbr_cwnd_gain_percent: u32,
    /// ProbeBW up-probe pacing gain, percent. Reserved: the ProbeBW gain cycle is not
    /// yet wired into the controller, so this knob is currently inert (the BtlBw
    /// max-filter already adopts higher delivery rates without an explicit up-probe).
    pub bbr_probe_bw_gain_percent: u32,
    /// How often to enter ProbeRTT to refresh the min-RTT estimate.
    #[serde(with = "humantime_serde")]
    pub bbr_probe_rtt_interval: Duration,
    /// How long to hold the drained cwnd during ProbeRTT.
    #[serde(with = "humantime_serde")]
    pub bbr_probe_rtt_duration: Duration,
    /// Windowed-min horizon for the RTprop (min-RTT) estimate.
    #[serde(with = "humantime_serde")]
    pub bbr_rtprop_window: Duration,
    /// Max-filter horizon for the delivery-rate (BtlBw) estimate.
    #[serde(with = "humantime_serde")]
    pub bbr_delivery_rate_window: Duration,
    /// Per-RTT Startup cwnd growth, percent. Reserved: there is no separate Startup ramp
    /// phase yet, so this knob is currently inert (cold start uses
    /// `initial_inflight_requests` and the BDP estimate takes over once samples arrive).
    pub bbr_startup_growth_percent: u32,
    /// Minimum cwnd, in blocks (the floor under [`CwndUnit::Blocks`]).
    pub bbr_min_cwnd: u32,
    /// Minimum cwnd, in bytes (the floor under [`CwndUnit::Bytes`]). Doubles as the
    /// cold-start byte window before the first delivery sample, and as the binding
    /// operating window for low-latency peers whose byte-BDP is below it.
    pub bbr_min_cwnd_bytes: u64,
    /// Delay-gradient down-adjust threshold, percent of RTprop.
    pub bbr_delay_gradient_percent: u32,
    /// Unit the BBR cwnd budgets in-flight work against (`bytes` = header-hinted
    /// reserved body bytes, default; `blocks` = request count, the A/B baseline).
    pub bbr_cwnd_unit: CwndUnit,
    /// Slots a floor (lowest-missing-height) request may borrow beyond the BBR cwnd, up
    /// to the peer's advertised hard cap. Lets the floor be fetched even when every
    /// servable peer is saturated at its cwnd; `0` disables the bypass.
    pub floor_bypass_slots: u32,
    /// Block-sync peer caps and queue limits owned by this service.
    pub peer_limits: ServicePeerLimits,
}

fn deserialize_ignored_replace_legacy_syncer<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let _ = bool::deserialize(deserializer)?;
    Ok(false)
}

impl Default for ZakuraBlockSyncConfig {
    fn default() -> Self {
        Self {
            replace_legacy_syncer: false,
            max_blocks_per_response: DEFAULT_BS_BLOCKS_PER_RESPONSE,
            max_inflight_requests: DEFAULT_BS_MAX_INFLIGHT,
            initial_inflight_requests: DEFAULT_BS_INITIAL_INFLIGHT,
            max_response_bytes: DEFAULT_BS_MAX_RESPONSE_BYTES,
            max_inflight_block_bytes: DEFAULT_BS_MAX_INFLIGHT_BLOCK_BYTES,
            max_reorder_lookahead_bytes: DEFAULT_BS_MAX_REORDER_LOOKAHEAD_BYTES,
            max_reorder_lookahead_blocks: DEFAULT_BS_MAX_REORDER_LOOKAHEAD_BLOCKS,
            floor_peer_avoid_cooldown: DEFAULT_BS_FLOOR_PEER_AVOID_COOLDOWN,
            max_submitted_block_applies: DEFAULT_BS_MAX_SUBMITTED_BLOCK_APPLIES,
            request_timeout: DEFAULT_BS_REQUEST_TIMEOUT,
            floor_rescue_timeout: DEFAULT_BS_FLOOR_RESCUE_TIMEOUT,
            no_progress_peer_cooldown: DEFAULT_BS_NO_PROGRESS_PEER_COOLDOWN,
            status_refresh_interval: DEFAULT_BS_STATUS_REFRESH_INTERVAL,
            size_deviation_tolerance: DEFAULT_BS_SIZE_DEVIATION_TOLERANCE,
            fanout: DEFAULT_BS_FANOUT,
            bbr_cwnd_gain_percent: DEFAULT_BS_BBR_CWND_GAIN_PERCENT,
            bbr_probe_bw_gain_percent: DEFAULT_BS_BBR_PROBE_BW_GAIN_PERCENT,
            bbr_probe_rtt_interval: DEFAULT_BS_BBR_PROBE_RTT_INTERVAL,
            bbr_probe_rtt_duration: DEFAULT_BS_BBR_PROBE_RTT_DURATION,
            bbr_rtprop_window: DEFAULT_BS_BBR_RTPROP_WINDOW,
            bbr_delivery_rate_window: DEFAULT_BS_BBR_DELIVERY_RATE_WINDOW,
            bbr_startup_growth_percent: DEFAULT_BS_BBR_STARTUP_GROWTH_PERCENT,
            bbr_min_cwnd: DEFAULT_BS_BBR_MIN_CWND,
            bbr_min_cwnd_bytes: DEFAULT_BS_BBR_MIN_CWND_BYTES,
            bbr_delay_gradient_percent: DEFAULT_BS_BBR_DELAY_GRADIENT_PERCENT,
            bbr_cwnd_unit: CwndUnit::Bytes,
            floor_bypass_slots: DEFAULT_BS_FLOOR_BYPASS_SLOTS,
            peer_limits: ServicePeerLimits::default(),
        }
    }
}

impl ZakuraBlockSyncConfig {
    /// Return the clamped block-count advertisement for wire status messages.
    pub fn advertised_max_blocks_per_response(&self) -> u32 {
        clamp_advertised_blocks(self.max_blocks_per_response)
    }

    /// Return the locally capped in-flight advertisement for status messages.
    pub fn advertised_max_inflight_requests(&self) -> u32 {
        clamp_advertised_inflight(self.max_inflight_requests)
    }

    /// Return the non-zero response byte advertisement for status messages.
    pub fn advertised_max_response_bytes(&self) -> u32 {
        clamp_advertised_response_bytes(self.max_response_bytes)
    }

    /// Return the non-zero block-sync action/body channel depth.
    pub fn submitted_apply_limit(&self) -> usize {
        self.max_submitted_block_applies
            .max(MIN_BS_CHECKPOINT_SUBMITTED_BLOCK_APPLIES)
    }

    /// Return the speculative look-ahead byte cap clamped to the global budget.
    pub fn effective_max_reorder_lookahead_bytes(&self) -> u64 {
        self.max_reorder_lookahead_bytes
            .min(self.max_inflight_block_bytes)
    }

    /// Return the floor avoid cooldown clamped to a positive duration.
    pub fn effective_floor_peer_avoid_cooldown(&self) -> Duration {
        self.floor_peer_avoid_cooldown.max(Duration::from_millis(1))
    }

    /// Return the maximum time an active block-sync peer may go without serving
    /// an accepted full block body.
    pub(super) fn effective_liveness_timeout(&self) -> Duration {
        self.request_timeout
            .saturating_mul(BLOCK_PROGRESS_TIMEOUT_REQUESTS)
    }

    /// Return the no-progress peer cooldown clamped to a positive duration.
    pub(super) fn effective_no_progress_peer_cooldown(&self) -> Duration {
        self.no_progress_peer_cooldown.max(Duration::from_millis(1))
    }

    /// Return the floor-rescue leash, clamped positive and no looser than the base
    /// request timeout (a floor request is never more patient than a normal one).
    pub(super) fn effective_floor_rescue_timeout(&self) -> Duration {
        self.floor_rescue_timeout
            .clamp(Duration::from_millis(1), self.request_timeout)
    }

    /// Return the largest byte reservation a single floor request can need.
    ///
    /// `fanout` is only a compatibility reservation multiplier; it does not mean
    /// current scheduling sends the same floor request to multiple peers.
    pub fn floor_request_byte_reservation(&self) -> u64 {
        let fanout = u64::try_from(self.fanout.max(1)).unwrap_or(u64::MAX);
        let worst_case_blocks = u64::from(self.advertised_max_blocks_per_response())
            .saturating_mul(BS_PER_BLOCK_WORST_CASE_BYTES)
            .saturating_mul(fanout);
        u64::from(self.advertised_max_response_bytes()).max(worst_case_blocks)
    }

    /// Validate production-safety bounds after deserialization.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.max_inflight_block_bytes == 0 {
            return Err("max_inflight_block_bytes must be greater than zero");
        }
        if self.max_reorder_lookahead_bytes == 0 {
            return Err("max_reorder_lookahead_bytes must be greater than zero");
        }
        if self.max_reorder_lookahead_blocks == 0 {
            return Err("max_reorder_lookahead_blocks must be greater than zero");
        }
        if self.max_inflight_block_bytes <= self.floor_request_byte_reservation() {
            return Err("max_inflight_block_bytes must exceed one floor request");
        }
        if self.request_timeout < Duration::from_millis(1) {
            return Err("request_timeout must be at least 1ms");
        }
        if self.bbr_min_cwnd == 0 {
            return Err("bbr_min_cwnd must be greater than zero");
        }
        if self.bbr_min_cwnd_bytes == 0 {
            return Err("bbr_min_cwnd_bytes must be greater than zero");
        }
        if self.bbr_cwnd_gain_percent < 100
            || self.bbr_probe_bw_gain_percent < 100
            || self.bbr_startup_growth_percent < 100
            || self.bbr_delay_gradient_percent < 100
        {
            return Err("bbr gain/threshold percentages must be at least 100");
        }
        if self.bbr_probe_rtt_interval <= self.bbr_probe_rtt_duration {
            return Err("bbr_probe_rtt_interval must exceed bbr_probe_rtt_duration");
        }
        Ok(())
    }

    /// Raise `max_inflight_block_bytes` up to the checkpoint-range floor when it
    /// is configured below it, warning once.
    ///
    /// A positive budget below [`BS_CHECKPOINT_RANGE_BYTE_FLOOR`] cannot hold one
    /// full worst-case checkpoint range. The checkpoint verifier only commits a
    /// range once the whole range is submitted, and every submitted body stays
    /// reserved against the budget until it is durable, so a budget below the
    /// floor would deadlock: the verifier never commits, nothing becomes durable,
    /// and no bytes are ever released. Rather than refuse to start -- which would
    /// break older configs that set a smaller budget -- clamp the budget up to the
    /// floor and warn. Zero is left untouched so [`validate`](Self::validate)
    /// still rejects it as an explicit misconfiguration.
    pub fn clamp_inflight_block_bytes_to_floor(&mut self) {
        if self.max_inflight_block_bytes > 0
            && self.max_inflight_block_bytes < BS_CHECKPOINT_RANGE_BYTE_FLOOR
        {
            tracing::warn!(
                configured_max_inflight_block_bytes = self.max_inflight_block_bytes,
                checkpoint_range_byte_floor = BS_CHECKPOINT_RANGE_BYTE_FLOOR,
                "zakura.block_sync.max_inflight_block_bytes is below the checkpoint-range \
                 floor; clamping it up so checkpoint sync cannot deadlock",
            );
            self.max_inflight_block_bytes = BS_CHECKPOINT_RANGE_BYTE_FLOOR;
        }
    }

    /// Build the inert local status used before the block-sync reactor is wired.
    pub fn initial_status(&self) -> BlockSyncStatus {
        BlockSyncStatus {
            max_blocks_per_response: self.advertised_max_blocks_per_response(),
            max_inflight_requests: self.advertised_max_inflight_requests(),
            max_response_bytes: self.advertised_max_response_bytes(),
            ..BlockSyncStatus::default()
        }
    }
}

/// Clamp an advertised block count to the hard stream-6 request cap.
pub fn clamp_advertised_blocks(count: u32) -> u32 {
    count.clamp(1, MAX_BS_BLOCKS_PER_REQUEST)
}

/// Clamp an advertised in-flight request count to the local status ceiling.
pub fn clamp_advertised_inflight(count: u32) -> u32 {
    count.clamp(1, MAX_BS_INFLIGHT_REQUESTS)
}

/// Clamp an advertised response byte target to the largest stream-6 message.
pub fn clamp_advertised_response_bytes(bytes: u32) -> u32 {
    bytes.clamp(1, MAX_BS_RESPONSE_BYTES)
}

/// Maximum inbound `GetBlocks.count` this node will serve before looking at body sizes.
pub fn inbound_get_blocks_count_limit(config: &ZakuraBlockSyncConfig) -> u32 {
    config
        .advertised_max_blocks_per_response()
        .clamp(1, MAX_BS_BLOCKS_PER_REQUEST)
}
