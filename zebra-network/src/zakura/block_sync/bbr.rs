use std::time::{Duration, Instant};

use super::{
    config::{CwndUnit, ZakuraBlockSyncConfig},
    state::DeliverySnapshot,
};

/// BBR-lite multiplicative cwnd dip applied on a real request timeout,
/// bounded below by `bbr_min_cwnd`.
const BBR_TIMEOUT_DIP: f64 = 0.85;
/// EWMA weight for the smoothed request round-trip the delay-gradient compares against
/// RTprop (higher = more responsive, noisier).
const BBR_DELAY_EWMA_ALPHA: f64 = 0.25;
/// Multiplicative shrink applied to the delay-gradient ceiling on each delivery whose
/// smoothed round-trip exceeds `RTprop × delay_gradient` (queue building).
const BBR_DELAY_CAP_DOWN: f64 = 0.9;

/// A time-windowed set of `f64` samples supporting `min` (RTprop) and `max` (BtlBw)
/// filters — the BBR-lite estimators. Samples older than `horizon` are pruned on
/// insert; the windows are small (seconds of per-request samples) so the linear
/// scan is cheap and runs once per completed request.
#[derive(Clone, Debug)]
struct WindowedSamples {
    horizon: Duration,
    samples: Vec<(Instant, f64)>,
}

impl WindowedSamples {
    fn new(horizon: Duration) -> Self {
        Self {
            horizon,
            samples: Vec::new(),
        }
    }

    fn observe(&mut self, now: Instant, value: f64) {
        self.samples.push((now, value));
        if let Some(cutoff) = now.checked_sub(self.horizon) {
            self.samples.retain(|(at, _)| *at >= cutoff);
        }
    }

    fn min(&self) -> Option<f64> {
        self.samples
            .iter()
            .map(|(_, value)| *value)
            .reduce(f64::min)
    }

    fn max(&self) -> Option<f64> {
        self.samples
            .iter()
            .map(|(_, value)| *value)
            .reduce(f64::max)
    }
}

/// Per-peer BBR-lite control parameters extracted from config (Copy, lock-free).
#[derive(Copy, Clone, Debug)]
struct BbrParams {
    /// Unit the cwnd/BtlBw/`delivered` are denominated in. `Blocks` keeps the
    /// request-counting controller (the A/B baseline); `Bytes` makes the controller
    /// reason in header-hinted body bytes so the in-flight request count falls out as
    /// `cwnd_bytes / advertised_block_size`.
    unit: CwndUnit,
    cwnd_gain: f64,
    /// Minimum / cold-start cwnd, in the active unit (`bbr_min_cwnd` blocks or
    /// `bbr_min_cwnd_bytes` bytes).
    min_cwnd: usize,
    startup_cwnd: usize,
    rtprop_window: Duration,
    delivery_rate_window: Duration,
    /// How long between ProbeRTT drains (the cadence at which RTprop is refreshed).
    probe_rtt_interval: Duration,
    /// How long to hold the cwnd at `min_cwnd` once the queue has drained, so at
    /// least one uncontended request completes and yields a clean RTprop sample.
    probe_rtt_duration: Duration,
    /// Smoothed-RTT / RTprop ratio above which the queue is judged to be building and
    /// the delay-gradient ceiling ratchets the cwnd down (e.g. 1.5 = shrink once the
    /// recent round-trip runs 50% over the uncontended minimum).
    delay_gradient: f64,
}

impl BbrParams {
    fn from_config(config: &ZakuraBlockSyncConfig) -> Self {
        let (min_cwnd, startup_cwnd) = match config.bbr_cwnd_unit {
            CwndUnit::Blocks => {
                let min = usize::try_from(config.bbr_min_cwnd).unwrap_or(1).max(1);
                // Cold start opens at the configured initial window until the first
                // BDP sample.
                let startup = usize::try_from(config.initial_inflight_requests)
                    .unwrap_or(min)
                    .max(min);
                (min, startup)
            }
            CwndUnit::Bytes => {
                // Byte denomination: the floor (and cold-start window) is the
                // configured minimum byte cwnd. The BDP estimate takes over once the
                // first delivery sample arrives; until then `bbr_min_cwnd_bytes`
                // primes the pipe with a few bodies' worth of in-flight budget.
                let min = usize::try_from(config.bbr_min_cwnd_bytes)
                    .unwrap_or(usize::MAX)
                    .max(1);
                (min, min)
            }
        };
        Self {
            unit: config.bbr_cwnd_unit,
            cwnd_gain: f64::from(config.bbr_cwnd_gain_percent) / 100.0,
            min_cwnd,
            startup_cwnd,
            rtprop_window: config.bbr_rtprop_window,
            delivery_rate_window: config.bbr_delivery_rate_window,
            probe_rtt_interval: config.bbr_probe_rtt_interval,
            probe_rtt_duration: config.bbr_probe_rtt_duration,
            delay_gradient: f64::from(config.bbr_delay_gradient_percent.max(100)) / 100.0,
        }
    }
}

/// BBR-lite control phase. `ProbeBw` is the steady state (cwnd tracks BDP × gain);
/// `ProbeRtt` periodically drains the queue to `min_cwnd` to take a fresh, uncontended
/// RTprop sample. Without ProbeRtt, a peer's RTprop min-filter stays inflated under a
/// sustained queue (the round-trip we measure is queue + serve + RTT), so the cwnd never
/// collapses for a genuinely slow peer.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum BbrPhase {
    ProbeBw,
    ProbeRtt,
}

impl BbrPhase {
    /// Numeric code for the JSONL trace (0 = ProbeBw, 1 = ProbeRtt).
    fn trace_code(self) -> u64 {
        match self {
            BbrPhase::ProbeBw => 0,
            BbrPhase::ProbeRtt => 1,
        }
    }
}

fn rounded_usize(value: f64, fallback: usize) -> usize {
    // BBR rates, windows, and gains are non-negative in normal operation. Keep a
    // fallback for NaN/inf or defensive underflow before casting.
    if value.is_finite() && value >= 0.0 {
        value.round() as usize
    } else {
        fallback
    }
}

fn secs_to_ms(secs: f64) -> u64 {
    // Round-trip samples are non-negative and finite for real requests.
    (secs * 1000.0).round() as u64
}

/// Per-peer BBR-lite estimators: an RTprop min-filter over request round-trips, a
/// BtlBw max-filter over per-ack delivery rate, and a delivered-block counter. The
/// owning routine samples these lock-free on each completed request. Stage 1 measures
/// and traces them; the control law (`available_slots`) consumes them in a later stage.
#[derive(Clone, Debug)]
pub(super) struct BbrState {
    params: BbrParams,
    /// Windowed-min of the **raw request round-trip** (seconds) — the BDP's RTprop term
    /// (`bdp = BtlBw × RTprop`) under both units: the genuine fastest observed round trip,
    /// which never collapses to zero. (The earlier byte-unit model fed this the
    /// *size-residual* `elapsed − bytes/BtlBw`; on a high-BtlBw carrier the fastest
    /// delivery's residual is ≈0, which zeroed the BDP and pinned the cwnd at the floor.
    /// The size-residual now lives in [`rtprop_residual_secs`](Self::rtprop_residual_secs),
    /// used only by the size-aware delay gate.)
    rtprop_secs: WindowedSamples,
    /// Windowed-min of the **size-residual** round-trip (`elapsed − bytes/BtlBw` under
    /// `Bytes`; the raw round-trip under `Blocks`) — the transmission-stripped propagation
    /// latency. Used **only** as the delay gate's healthy-round-trip base, so a big block's
    /// honest transfer time is not mistaken for a standing queue. It never feeds the BDP,
    /// which must reflect real in-flight depth rather than a residual that collapses to ~0
    /// on a fast carrier.
    rtprop_residual_secs: WindowedSamples,
    /// Max-filter over per-ack delivery rate, in **units per second** (blocks/s under
    /// [`CwndUnit::Blocks`], bytes/s under [`CwndUnit::Bytes`]). The byte denomination
    /// makes `BtlBw × RTprop` a true bandwidth-delay product over heterogeneous body
    /// sizes; the block denomination is the A/B baseline.
    btlbw_per_sec: WindowedSamples,
    /// Cumulative delivered amount in the active unit (blocks or bytes), used as the
    /// per-ack delivery-rate numerator via [`DeliverySnapshot`].
    delivered: u64,
    delivered_at: Option<Instant>,
    /// Effective cwnd in blocks currently applied by `available_slots`: the
    /// BDP-derived target once measured, the startup window before that, dipped on
    /// timeouts. Never below `min_cwnd`. Ignored while in `ProbeRtt` (which forces
    /// `min_cwnd`) but preserved so the cwnd restores on exit.
    cwnd_cap: usize,
    /// Current control phase.
    phase: BbrPhase,
    /// When the last ProbeRtt completed (or the first delivery, to anchor the first
    /// probe one interval out). `None` until the first delivery is recorded.
    last_probe_rtt_at: Option<Instant>,
    /// Set the moment the queue first drains to `min_cwnd` during a ProbeRtt; the
    /// `probe_rtt_duration` hold timer runs from here.
    probe_rtt_drained_at: Option<Instant>,
    /// EWMA of the request round-trip, compared against RTprop by the delay-gradient.
    smoothed_elapsed_secs: Option<f64>,
    /// Delay-gradient ceiling on the effective cwnd. Starts unbounded (`usize::MAX`) so
    /// it never limits an uncongested peer; ratchets down toward the true operating
    /// point whenever the smoothed round-trip rises above `RTprop × delay_gradient`, and
    /// relaxes back up when the queue clears. Guards against a `BtlBw × RTprop` BDP that
    /// overshoots the sustainable rate (max-rate and min-RTT can come from different
    /// samples under variable queueing), which would otherwise inflate the cwnd.
    delay_cap: usize,
}

impl BbrState {
    pub(super) fn new(config: &ZakuraBlockSyncConfig) -> Self {
        let params = BbrParams::from_config(config);
        Self {
            rtprop_secs: WindowedSamples::new(params.rtprop_window),
            rtprop_residual_secs: WindowedSamples::new(params.rtprop_window),
            btlbw_per_sec: WindowedSamples::new(params.delivery_rate_window),
            delivered: 0,
            delivered_at: None,
            cwnd_cap: params.startup_cwnd,
            phase: BbrPhase::ProbeBw,
            last_probe_rtt_at: None,
            probe_rtt_drained_at: None,
            smoothed_elapsed_secs: None,
            delay_cap: usize::MAX,
            params,
        }
    }

    pub(super) fn delivery_snapshot(&self, now: Instant) -> DeliverySnapshot {
        DeliverySnapshot {
            delivered: self.delivered,
            delivered_at: self.delivered_at.unwrap_or(now),
        }
    }

    /// Record a completed request: `elapsed` from send to the final body, `blocks` in
    /// it, and `inflight` = the work still outstanding to this peer *after* this
    /// completion, **denominated in the cwnd's unit** (request count under `Blocks`,
    /// reserved body bytes under `Bytes`) so the ProbeRtt drain check can compare it
    /// against `min_cwnd` consistently. The RTprop sample is the request round-trip.
    /// The BtlBw sample is measured over the request's pipe interval
    /// (`delivered_delta / elapsed_since_snapshot`), so one-block responses can still
    /// observe concurrent completions while the request was in flight. The interval is
    /// floored at the previous RTprop so a burst of buffered bodies arriving within one
    /// tick cannot inflate the bandwidth estimate. Re-derives the applied cwnd from the
    /// fresh BDP estimate, then advances the ProbeBw/ProbeRtt phase machine.
    pub(super) fn record_delivery(
        &mut self,
        now: Instant,
        elapsed: Duration,
        blocks: u32,
        delivered_bytes: u64,
        inflight: u64,
        snapshot: DeliverySnapshot,
    ) {
        let rtt_secs = elapsed.as_secs_f64();
        // Floor the delivery-rate interval at the *previous* RTprop min (captured
        // before this sample is observed) so a burst of buffered bodies arriving within
        // one tick cannot inflate the bandwidth estimate.
        let rate_floor = self.rtprop_secs.min().unwrap_or(rtt_secs).max(1e-4);

        // Accumulate the delivered amount in the active unit and push a per-ack rate
        // sample into the BtlBw max-filter (blocks/s under `Blocks`, bytes/s under
        // `Bytes`).
        let delivered_amount = match self.params.unit {
            CwndUnit::Blocks => u64::from(blocks),
            CwndUnit::Bytes => delivered_bytes,
        };
        let delivered_after = self.delivered.saturating_add(delivered_amount);
        let delivered_delta = delivered_after.saturating_sub(snapshot.delivered).max(1);
        let interval = now.saturating_duration_since(snapshot.delivered_at);
        // `delivered_delta` is a count/byte total over a short sampling window;
        // converting it to `f64` is exact for the operating ranges this controller sees.
        let rate = delivered_delta as f64 / interval.as_secs_f64().max(rate_floor);
        self.btlbw_per_sec.observe(now, rate);
        self.delivered = delivered_after;
        self.delivered_at = Some(now);

        // Observe the BDP's RTprop sample: the **raw** round trip under both units. Its
        // windowed min ≈ the base round trip of the fastest deliveries, which is the real
        // in-flight depth the BDP needs. Feeding the BDP the size residual instead would
        // collapse it to ~0 on a high-BtlBw carrier (the fastest delivery's residual
        // `elapsed − bytes/BtlBw` ≈ 0), pinning the cwnd at the floor.
        self.rtprop_secs.observe(now, rtt_secs);
        // Observe the size-residual separately, for the delay gate only: under `Bytes` it
        // strips the body's transmission time so a big block's honest transfer is not read
        // as a standing queue; under `Blocks` it is the raw round trip (A/B baseline).
        let residual_sample = match self.params.unit {
            CwndUnit::Blocks => rtt_secs,
            CwndUnit::Bytes => self.size_residual_rtprop(rtt_secs, delivered_bytes),
        };
        self.rtprop_residual_secs.observe(now, residual_sample);

        if let Some(target) = self.cwnd_target() {
            self.cwnd_cap = target;
        }
        // Delay-gradient runs in ProbeBw only: the drained round-trips ProbeRtt produces
        // are artificially short and would spuriously relax the ceiling. `phase` here is
        // still the pre-`advance_phase` value, so a tick that flips into ProbeRtt this
        // call last updated the ceiling under genuine ProbeBw conditions.
        if self.phase == BbrPhase::ProbeBw {
            self.update_delay_cap(rtt_secs, delivered_bytes);
        }
        self.advance_phase(now, inflight);
    }

    /// Size-residual RTprop sample (`Bytes` unit): subtract the body's transmission
    /// time at the bottleneck rate from the round trip, leaving the fixed-latency
    /// component. Falls back to the raw round trip before any rate is known, and is
    /// clamped to `[ε, elapsed]` (the residual can never exceed the time elapsed, and a
    /// tiny positive floor keeps the byte-BDP well-defined).
    fn size_residual_rtprop(&self, rtt_secs: f64, delivered_bytes: u64) -> f64 {
        let btlbw = self.btlbw_per_sec.max().unwrap_or(0.0);
        let residual = if btlbw > 0.0 {
            // `delivered_bytes as f64` is exact for real body sizes.
            rtt_secs - delivered_bytes as f64 / btlbw
        } else {
            rtt_secs
        };
        residual.clamp(1e-4, rtt_secs.max(1e-4))
    }

    /// Update the delay-gradient ceiling from this delivery's round-trip. When the
    /// smoothed round-trip rises above `RTprop × delay_gradient` the queue is building,
    /// so ratchet the ceiling down from the current operating cwnd; otherwise relax it
    /// back up so a cleared queue lets the cwnd re-probe for bandwidth.
    fn update_delay_cap(&mut self, rtt_secs: f64, delivered_bytes: u64) {
        let smoothed = match self.smoothed_elapsed_secs {
            Some(prev) => prev * (1.0 - BBR_DELAY_EWMA_ALPHA) + rtt_secs * BBR_DELAY_EWMA_ALPHA,
            None => rtt_secs,
        };
        self.smoothed_elapsed_secs = Some(smoothed);
        // The delay gate's base is the *residual* RTprop (transmission stripped), not the
        // raw round trip the BDP uses: the size-aware `expected` below adds the body's
        // transmission back, so basing it on the raw round trip would double-count it.
        let rtprop = self
            .rtprop_residual_secs
            .min()
            .unwrap_or(rtt_secs)
            .max(1e-4);
        // The expected round trip for a healthy (unqueued) delivery. Under `Bytes` it is
        // size-aware — `RTprop + transmission time` — so a big block's honest transfer
        // time is not mistaken for a standing queue; under `Blocks` it is just RTprop
        // (the A/B baseline).
        let expected = match self.params.unit {
            CwndUnit::Blocks => rtprop,
            CwndUnit::Bytes => {
                let btlbw = self.btlbw_per_sec.max().unwrap_or(0.0);
                let transmit = if btlbw > 0.0 {
                    delivered_bytes as f64 / btlbw
                } else {
                    0.0
                };
                rtprop + transmit
            }
        };
        if smoothed > expected * self.params.delay_gradient {
            // Queue building: shrink the ceiling relative to the current operating cwnd.
            let operating = self.cwnd_cap.min(self.delay_cap).max(self.params.min_cwnd);
            // `operating` is a non-negative cwnd; f64 precision is enough for tuning math.
            let operating_f64 = operating as f64;
            let shrunk = rounded_usize(operating_f64 * BBR_DELAY_CAP_DOWN, self.params.min_cwnd);
            self.delay_cap = shrunk.max(self.params.min_cwnd);
        } else {
            // Headroom: relax the ceiling up (~12%/delivery), saturating so an
            // uncongested peer's ceiling stays effectively unbounded.
            let grow = (self.delay_cap / 8).max(1);
            self.delay_cap = self.delay_cap.saturating_add(grow);
        }
    }

    /// Drive the ProbeBw/ProbeRtt cycle off completed deliveries (the only event that
    /// carries both a fresh timestamp and the current inflight measure). `inflight` is in
    /// the cwnd's unit (request count under `Blocks`, reserved bytes under `Bytes`) so it
    /// is comparable to `min_cwnd`. ProbeRtt forces the cwnd to `min_cwnd`, which drains
    /// the queue; once drained, it holds for `probe_rtt_duration` so an uncontended
    /// request completes and refreshes RTprop.
    fn advance_phase(&mut self, now: Instant, inflight: u64) {
        // Anchor the first probe one interval after the first delivery.
        let anchor = *self.last_probe_rtt_at.get_or_insert(now);
        match self.phase {
            BbrPhase::ProbeBw => {
                if now.saturating_duration_since(anchor) >= self.params.probe_rtt_interval {
                    self.phase = BbrPhase::ProbeRtt;
                    self.probe_rtt_drained_at = None;
                }
            }
            BbrPhase::ProbeRtt => {
                // Start the hold timer the moment the queue first reaches the floor.
                // `inflight` and `min_cwnd` are in the same unit; widen `min_cwnd`
                // (`usize`) to `u64` for the comparison (lossless on supported targets).
                if self.probe_rtt_drained_at.is_none() && inflight <= self.params.min_cwnd as u64 {
                    self.probe_rtt_drained_at = Some(now);
                }
                let Some(drained_at) = self.probe_rtt_drained_at else {
                    return;
                };
                if now.saturating_duration_since(drained_at) < self.params.probe_rtt_duration {
                    return;
                }

                // Exit: a clean RTprop sample has been taken at low queue depth.
                self.phase = BbrPhase::ProbeBw;
                self.last_probe_rtt_at = Some(now);
                self.probe_rtt_drained_at = None;
                if let Some(target) = self.cwnd_target() {
                    self.cwnd_cap = target;
                }
            }
        }
    }

    /// The effective cwnd in blocks currently applied (never below `min_cwnd`). During
    /// ProbeRtt the cwnd is pinned to `min_cwnd` to drain the queue; in ProbeBw it is the
    /// BDP-derived cwnd capped by the delay-gradient ceiling.
    pub(super) fn effective_cwnd(&self) -> usize {
        match self.phase {
            BbrPhase::ProbeRtt => self.params.min_cwnd,
            BbrPhase::ProbeBw => self.cwnd_cap.min(self.delay_cap).max(self.params.min_cwnd),
        }
    }

    /// Apply one multiplicative dip on a real timeout (BBR-style), bounded by the
    /// minimum cwnd. Suppressed during ProbeRtt,
    /// where the cwnd is already pinned to `min_cwnd` and timeouts are an expected
    /// consequence of the drain, not congestion signal. A timeout is strong congestion
    /// evidence, so it also ratchets the delay-gradient ceiling down to the dipped cwnd.
    pub(super) fn dip_on_timeout(&mut self) {
        if self.phase == BbrPhase::ProbeRtt {
            return;
        }
        // `cwnd_cap` is a non-negative cwnd; f64 precision is enough for tuning math.
        let cwnd_cap = self.cwnd_cap as f64;
        let dipped = rounded_usize(cwnd_cap * BBR_TIMEOUT_DIP, self.params.min_cwnd);
        self.cwnd_cap = dipped.max(self.params.min_cwnd);
        self.delay_cap = self.delay_cap.min(self.cwnd_cap);
    }

    /// Bandwidth-delay product in the active unit: BtlBw (units/s) × RTprop (s) — blocks
    /// under `Blocks`, bytes under `Bytes`. `None` until at least one delivery sample
    /// exists (cold start).
    fn bdp(&self) -> Option<f64> {
        match (self.btlbw_per_sec.max(), self.rtprop_secs.min()) {
            (Some(rate), Some(rtprop)) => Some(rate * rtprop),
            _ => None,
        }
    }

    /// Target cwnd in the active unit = `max(min_cwnd, BDP × gain)`. `None` until the
    /// first delivery sample exists, so the cwnd stays at the cold-start value until then.
    fn cwnd_target(&self) -> Option<usize> {
        let bdp = self.bdp()?;
        let cwnd = rounded_usize(bdp * self.params.cwnd_gain, self.params.min_cwnd);
        Some(cwnd.max(self.params.min_cwnd))
    }

    pub(super) fn rtprop_ms(&self) -> Option<u64> {
        self.rtprop_secs.min().map(secs_to_ms)
    }

    /// Raw BtlBw max-filter value in the active unit per second (`None` cold-start).
    pub(super) fn btlbw_units_per_sec(&self) -> Option<f64> {
        self.btlbw_per_sec.max()
    }

    pub(super) fn btlbw_milliblocks_per_sec(&self) -> Option<u64> {
        // A rounded non-negative rate scaled by 1000 fits u64 for any real rate. Only
        // meaningful under `Blocks`; the byte trace path reports bytes/sec instead.
        self.btlbw_per_sec
            .max()
            .map(|rate| (rate * 1000.0).round() as u64)
    }

    pub(super) fn delivered(&self) -> u64 {
        self.delivered
    }

    /// Numeric phase code for the trace (0 = ProbeBw, 1 = ProbeRtt).
    pub(super) fn phase_code(&self) -> u64 {
        self.phase.trace_code()
    }

    /// The smoothed request round-trip in milliseconds, for tracing the delay-gradient.
    pub(super) fn smoothed_elapsed_ms(&self) -> Option<u64> {
        self.smoothed_elapsed_secs.map(secs_to_ms)
    }

    /// The delay-gradient ceiling in blocks once it has bound the cwnd (`None` while
    /// still unbounded), for tracing.
    pub(super) fn delay_cap(&self) -> Option<usize> {
        (self.delay_cap != usize::MAX).then_some(self.delay_cap)
    }
}

#[cfg(test)]
mod bbr_tests {
    use super::super::{
        request::{BlockRangeRequest, ExpectedBlock},
        state::{DownloadWindow, OutstandingBlockRange, ReceivedBlockTracker},
    };
    use super::*;
    use zebra_chain::block;

    /// A config with a short ProbeRTT cadence and predictable cwnd math for the unit
    /// tests below. The probe interval/duration are scaled down so a handful of
    /// deliveries crosses a full ProbeBw → ProbeRtt → ProbeBw cycle.
    fn bbr_test_config() -> ZakuraBlockSyncConfig {
        ZakuraBlockSyncConfig {
            // These tests assert blocks-slot semantics; pin the unit so the production
            // default flip to `Bytes` does not change them.
            bbr_cwnd_unit: CwndUnit::Blocks,
            bbr_min_cwnd: 4,
            bbr_cwnd_gain_percent: 200,
            bbr_probe_rtt_interval: Duration::from_secs(1),
            bbr_probe_rtt_duration: Duration::from_millis(200),
            bbr_rtprop_window: Duration::from_secs(10),
            bbr_delivery_rate_window: Duration::from_secs(10),
            initial_inflight_requests: 16,
            ..Default::default()
        }
    }

    /// A clean delivery: 40 blocks in 10 ms ⇒ rate 4000 blk/s, RTprop 0.01 s,
    /// BDP 40 blocks, ×2 gain ⇒ cwnd target 80.
    const CLEAN_ELAPSED: Duration = Duration::from_millis(10);
    const CLEAN_BLOCKS: u32 = 40;
    const EXPECTED_CWND: usize = 80;

    /// Blocks-mode delivery helper (the `delivered_bytes` arg is ignored under
    /// `CwndUnit::Blocks`, so it passes 0). `inflight` is the request count, which is the
    /// Blocks-unit in-flight measure.
    fn record_delivery(
        bbr: &mut BbrState,
        now: Instant,
        elapsed: Duration,
        blocks: u32,
        inflight: usize,
    ) {
        let snapshot = DeliverySnapshot {
            delivered: bbr.delivered,
            delivered_at: now - elapsed,
        };
        // Blocks unit: the in-flight measure is the request count.
        bbr.record_delivery(now, elapsed, blocks, 0, inflight as u64, snapshot);
    }

    #[test]
    fn cwnd_tracks_bdp_after_first_delivery() {
        let mut bbr = BbrState::new(&bbr_test_config());
        let t0 = Instant::now();
        // Cold start: the configured initial window until the first BDP sample.
        assert_eq!(bbr.effective_cwnd(), 16);
        record_delivery(&mut bbr, t0, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        assert_eq!(bbr.effective_cwnd(), EXPECTED_CWND);
        assert_eq!(bbr.phase, BbrPhase::ProbeBw);
    }

    #[test]
    fn one_block_responses_observe_pipe_delivery_rate() {
        let mut bbr = BbrState::new(&bbr_test_config());
        let t0 = Instant::now();
        let rtprop = Duration::from_millis(100);
        let sent_at = t0 - rtprop;
        let snapshots: Vec<_> = (0..16).map(|_| bbr.delivery_snapshot(sent_at)).collect();

        for snapshot in snapshots {
            bbr.record_delivery(t0, rtprop, 1, 0, 16, snapshot);
        }

        // Sixteen one-block responses completed during the same request interval:
        // BtlBw = 16 / 100 ms, BDP = 16, cwnd gain = 2.
        assert_eq!(bbr.effective_cwnd(), 32);
        assert_eq!(bbr.btlbw_milliblocks_per_sec(), Some(160_000));
    }

    #[test]
    fn delivery_rate_floor_uses_previous_rtprop_sample() {
        let mut bbr = BbrState::new(&bbr_test_config());
        let t0 = Instant::now();

        // Establish a 100 ms RTprop and 100 blocks/s BtlBw sample.
        record_delivery(&mut bbr, t0, Duration::from_millis(100), 10, 10);
        assert_eq!(bbr.btlbw_milliblocks_per_sec(), Some(100_000));

        // A later 1 ms request is also the new RTprop, but it must not remove the
        // floor for its own delivery-rate sample. With the old ordering this sample
        // was 10 / 1 ms = 10_000 blocks/s and inflated BtlBw by 100x.
        record_delivery(
            &mut bbr,
            t0 + Duration::from_millis(10),
            Duration::from_millis(1),
            10,
            10,
        );
        assert_eq!(bbr.rtprop_ms(), Some(1));
        assert_eq!(bbr.btlbw_milliblocks_per_sec(), Some(100_000));
    }

    #[test]
    fn probe_rtt_pins_min_cwnd_then_drains_and_exits() {
        let cfg = bbr_test_config();
        let min_cwnd = usize::try_from(cfg.bbr_min_cwnd).unwrap();
        let mut bbr = BbrState::new(&cfg);
        let t0 = Instant::now();

        // Establish a healthy cwnd; anchors the first probe at t0.
        record_delivery(&mut bbr, t0, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        assert_eq!(bbr.effective_cwnd(), EXPECTED_CWND);

        // One interval later, a delivery trips ProbeRtt: cwnd pins to min_cwnd even
        // though the BDP estimate is unchanged.
        let t1 = t0 + Duration::from_millis(1_100);
        record_delivery(&mut bbr, t1, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        assert_eq!(bbr.phase, BbrPhase::ProbeRtt);
        assert_eq!(bbr.effective_cwnd(), min_cwnd);

        // Queue not yet drained (inflight still above min): hold ProbeRtt, no timer.
        let t2 = t1 + Duration::from_millis(50);
        record_delivery(&mut bbr, t2, CLEAN_ELAPSED, 10, min_cwnd + 5);
        assert_eq!(bbr.phase, BbrPhase::ProbeRtt);
        assert!(bbr.probe_rtt_drained_at.is_none());

        // Queue drains to the floor: the hold timer starts here.
        let t3 = t2 + Duration::from_millis(20);
        record_delivery(&mut bbr, t3, CLEAN_ELAPSED, 10, min_cwnd - 1);
        assert_eq!(bbr.phase, BbrPhase::ProbeRtt);
        assert_eq!(bbr.probe_rtt_drained_at, Some(t3));

        // Before the hold elapses, still draining.
        let t4 = t3 + Duration::from_millis(100);
        record_delivery(&mut bbr, t4, CLEAN_ELAPSED, 10, 1);
        assert_eq!(bbr.phase, BbrPhase::ProbeRtt);

        // After probe_rtt_duration past the drain, exit to ProbeBw and restore cwnd.
        let t5 = t3 + Duration::from_millis(200);
        record_delivery(&mut bbr, t5, CLEAN_ELAPSED, 10, 1);
        assert_eq!(bbr.phase, BbrPhase::ProbeBw);
        assert_eq!(bbr.effective_cwnd(), EXPECTED_CWND);
        assert_eq!(bbr.last_probe_rtt_at, Some(t5));
    }

    #[test]
    fn probe_rtt_collapses_cwnd_for_a_slow_peer() {
        // The headline case: a peer whose RTprop inflated under a deep queue. ProbeRtt
        // forces the cwnd to min_cwnd while it drains, regardless of the (stale, large)
        // BDP estimate — this is the slow-peer collapse the trace analysis motivated.
        let cfg = bbr_test_config();
        let min_cwnd = usize::try_from(cfg.bbr_min_cwnd).unwrap();
        let mut bbr = BbrState::new(&cfg);
        let t0 = Instant::now();
        record_delivery(&mut bbr, t0, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        let t1 = t0 + Duration::from_millis(1_100);
        record_delivery(&mut bbr, t1, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        assert_eq!(bbr.effective_cwnd(), min_cwnd);
    }

    #[test]
    fn timeout_dip_applies_in_probe_bw_but_is_suppressed_in_probe_rtt() {
        let cfg = bbr_test_config();
        let min_cwnd = usize::try_from(cfg.bbr_min_cwnd).unwrap();
        let mut bbr = BbrState::new(&cfg);
        let t0 = Instant::now();
        record_delivery(&mut bbr, t0, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        assert_eq!(bbr.effective_cwnd(), EXPECTED_CWND);

        // In ProbeBw a timeout dips the cwnd by the multiplicative factor.
        bbr.dip_on_timeout();
        let expected_dip = (EXPECTED_CWND as f64 * BBR_TIMEOUT_DIP).round() as usize;
        assert_eq!(bbr.effective_cwnd(), expected_dip);

        // Enter ProbeRtt; a timeout there is an expected drain consequence, not
        // congestion signal, so cwnd_cap is left untouched.
        let t1 = t0 + Duration::from_millis(1_100);
        record_delivery(&mut bbr, t1, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        assert_eq!(bbr.phase, BbrPhase::ProbeRtt);
        let cap_before = bbr.cwnd_cap;
        bbr.dip_on_timeout();
        assert_eq!(bbr.cwnd_cap, cap_before);
        assert_eq!(bbr.effective_cwnd(), min_cwnd);
    }

    /// Push `n` placeholder outstanding requests onto a window to drive its slot count.
    fn fill_outstanding(window: &mut DownloadWindow, n: usize) {
        let now = Instant::now();
        for _ in 0..n {
            window.outstanding.push(OutstandingBlockRange {
                request: BlockRangeRequest {
                    start_height: block::Height(0),
                    count: 1,
                    anchor_hash: block::Hash([0; 32]),
                    estimated_bytes: 0,
                    expected_blocks: Vec::new(),
                },
                queued_at: now,
                deadline: now,
                delivery_snapshot: window.delivery_snapshot(now),
                delivered_bytes: 0,
                received: ReceivedBlockTracker::default(),
            });
        }
    }

    #[test]
    fn floor_bypass_grants_bonus_slots_only_when_cwnd_is_saturated() {
        // Cold-start cwnd 8, hard cap well above it so the bonus is not clamped.
        let cfg = ZakuraBlockSyncConfig {
            initial_inflight_requests: 8,
            max_inflight_requests: 256,
            ..bbr_test_config()
        };
        let mut window = DownloadWindow::new(&cfg);
        assert_eq!(window.bbr_effective_cwnd(), 8);

        // Below cwnd: normal capacity already covers the floor, bonus adds nothing extra
        // beyond the same headroom.
        fill_outstanding(&mut window, 6);
        assert_eq!(window.available_slots(), 2);
        assert_eq!(window.available_slots_with_bonus(2), 4);

        // Saturated at cwnd: normal capacity is 0 but the floor may borrow the bonus.
        fill_outstanding(&mut window, 2);
        assert_eq!(window.available_slots(), 0);
        assert_eq!(window.available_slots_with_bonus(2), 2);

        // Saturated even into the bonus region: nothing left for anyone.
        fill_outstanding(&mut window, 2);
        assert_eq!(window.available_slots_with_bonus(2), 0);
    }

    #[test]
    fn delay_gradient_does_not_bind_an_uncongested_peer() {
        // Every delivery's round-trip equals RTprop (no queue), so the delay ceiling
        // stays unbounded and the cwnd tracks the full BDP target.
        let mut bbr = BbrState::new(&bbr_test_config());
        let mut now = Instant::now();
        for _ in 0..20 {
            record_delivery(&mut bbr, now, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
            now += Duration::from_millis(5);
        }
        assert_eq!(bbr.effective_cwnd(), EXPECTED_CWND);
        assert!(bbr.delay_cap().is_none(), "ceiling should stay unbounded");
    }

    #[test]
    fn delay_gradient_caps_cwnd_when_the_round_trip_inflates() {
        // RTprop is established low (10 ms), then every round-trip runs far above it
        // (queue building) while the BtlBw×RTprop target stays high — exactly the cwnd
        // overshoot the delay-gradient must contain. The ceiling ratchets the effective
        // cwnd well below the (inflated) BDP target.
        let cfg = bbr_test_config();
        let mut bbr = BbrState::new(&cfg);
        let t0 = Instant::now();
        // One clean delivery anchors RTprop at 10 ms and the BDP target at 80.
        record_delivery(&mut bbr, t0, CLEAN_ELAPSED, CLEAN_BLOCKS, 50);
        assert_eq!(bbr.effective_cwnd(), EXPECTED_CWND);

        // Now deliveries keep arriving at the same low RTprop sample for the min-filter
        // (so the target stays 80) but with long *smoothed* round-trips — model that with
        // a low-elapsed sample to hold RTprop and the cwnd target, interleaved with the
        // queue signal. Here we simply feed inflated round-trips: RTprop min stays 10 ms
        // (the first sample is in-window), smoothed climbs, the ceiling ratchets down.
        let inflated = Duration::from_millis(120);
        let mut now = t0;
        for _ in 0..40 {
            now += Duration::from_millis(5);
            record_delivery(&mut bbr, now, inflated, CLEAN_BLOCKS, 50);
        }
        assert_eq!(
            bbr.phase,
            BbrPhase::ProbeBw,
            "stay in ProbeBw for this test"
        );
        assert!(
            bbr.effective_cwnd() < EXPECTED_CWND,
            "delay-gradient should cap the cwnd below the BDP target, got {}",
            bbr.effective_cwnd(),
        );
        assert!(
            bbr.delay_cap().is_some(),
            "the ceiling should have bound the cwnd",
        );
    }

    /// Push `count` single-height requests each reserving `bytes_each` estimated bytes.
    fn push_outstanding_bytes(window: &mut DownloadWindow, count: usize, bytes_each: u64) {
        let now = Instant::now();
        for i in 0..count {
            // A `u32` index; the test count is tiny so the cast is safe.
            let height = block::Height(1 + i as u32);
            window.outstanding.push(OutstandingBlockRange {
                request: BlockRangeRequest {
                    start_height: height,
                    count: 1,
                    anchor_hash: block::Hash([0; 32]),
                    estimated_bytes: bytes_each,
                    expected_blocks: vec![ExpectedBlock {
                        height,
                        hash: block::Hash([0; 32]),
                        estimated_bytes: bytes_each,
                    }],
                },
                queued_at: now,
                deadline: now,
                delivery_snapshot: window.delivery_snapshot(now),
                delivered_bytes: 0,
                received: ReceivedBlockTracker::default(),
            });
        }
    }

    /// A byte-unit config whose cold-start byte cwnd is exactly `min_cwnd_bytes` (the
    /// floor doubles as the cold-start window) with a request-count cap well above it.
    fn byte_test_config(min_cwnd_bytes: u64, max_inflight: u32) -> ZakuraBlockSyncConfig {
        ZakuraBlockSyncConfig {
            bbr_cwnd_unit: CwndUnit::Bytes,
            bbr_min_cwnd_bytes: min_cwnd_bytes,
            max_inflight_requests: max_inflight,
            ..bbr_test_config()
        }
    }

    #[test]
    fn cwnd_unit_bytes_budgets_in_flight_by_reserved_bytes() {
        // The byte cwnd is the byte floor at cold start: an 8000 B in-flight budget,
        // sourced from the controller's byte denomination — independent of how many
        // *requests* that is.
        let cfg = byte_test_config(8000, 256);
        let mut window = DownloadWindow::new(&cfg);
        assert_eq!(window.available_slots(), 8000);

        // Six 1000 B requests (their header-hinted `estimated_bytes`) leave 2000 B...
        push_outstanding_bytes(&mut window, 6, 1000);
        assert_eq!(window.available_slots(), 2000);
        // ...and two more exhaust the byte budget.
        push_outstanding_bytes(&mut window, 2, 1000);
        assert_eq!(window.available_slots(), 0);

        // A peer serving 4 KB bodies fills the same byte cwnd with far fewer requests —
        // the point of the byte unit. Two 4000 B requests already saturate the 8000 B
        // budget, so the in-flight request count self-adjusts to the body size.
        let mut big = DownloadWindow::new(&cfg);
        push_outstanding_bytes(&mut big, 2, 4000);
        assert_eq!(big.available_slots(), 0);
    }

    #[test]
    fn cwnd_unit_bytes_enforces_the_request_count_hard_cap() {
        // A peer advertising a small inflight cap but serving tiny bodies must not be
        // issued more *requests* than it will service, however much byte headroom the
        // cwnd still shows — the advertised request-count cap binds first (review fix F2).
        let cfg = byte_test_config(400_000, 4); // 400 KB byte cwnd, hard cap 4 requests
        let mut window = DownloadWindow::new(&cfg);
        assert_eq!(window.hard_outbound_capacity(), 4);
        // 400_000 B of byte headroom — room for many tiny bodies.
        assert!(window.available_slots() > 0);

        // Four tiny (10 B) requests reach the request-count hard cap. The byte budget is
        // nowhere near exhausted (40 B of 400_000 B), but the advertised cap must bind:
        // no further request may be issued.
        push_outstanding_bytes(&mut window, 4, 10);
        assert_eq!(
            window.available_slots(),
            0,
            "the advertised request-count cap must bind even with byte headroom left",
        );
        // The floor bypass must not breach the advertised cap either.
        assert_eq!(window.available_slots_with_bonus(2), 0);
    }

    #[test]
    fn probe_rtt_drain_respects_reserved_bytes_under_byte_unit() {
        // Regression for the unit-inconsistent ProbeRtt drain gate under `CwndUnit::Bytes`.
        // The drain check compares the in-flight measure against `min_cwnd`, which under
        // `Bytes` is `min_cwnd_bytes`. The window must therefore feed the controller its
        // reserved *bytes*, not a request count — otherwise a small count is always below
        // the multi-KiB byte floor, the hold timer starts before the byte queue has
        // drained, and ProbeRtt exits while still contended. Here a still-full byte queue
        // must hold ProbeRtt open until the reserved bytes actually fall to the floor.
        let cfg = byte_test_config(8_000, 256); // 8 KB byte floor
        let mut window = DownloadWindow::new(&cfg);
        let t0 = Instant::now();

        // Five 4 KB reservations ⇒ 20 KB in flight, well above the 8 KB floor.
        push_outstanding_bytes(&mut window, 5, 4_000);
        let deliver = |window: &mut DownloadWindow, now: Instant| {
            let snapshot = window.delivery_snapshot(now - Duration::from_millis(10));
            window.record_delivery(now, Duration::from_millis(10), 1, 4_000, snapshot);
        };

        // First delivery anchors the probe at t0; stays in ProbeBw.
        deliver(&mut window, t0);
        assert_eq!(window.bbr_phase_code(), 0);

        // One interval later ProbeRtt trips.
        let t1 = t0 + Duration::from_millis(1_100);
        deliver(&mut window, t1);
        assert_eq!(window.bbr_phase_code(), 1, "ProbeRtt should have tripped");

        // The byte queue is still 20 KB (above the floor), so even well past
        // `probe_rtt_duration` the drain is NOT detected and ProbeRtt holds. The buggy
        // count-vs-bytes gate would have stamped the drain immediately and exited here.
        let t2 = t1 + Duration::from_millis(250);
        deliver(&mut window, t2);
        let t3 = t2 + Duration::from_millis(250);
        deliver(&mut window, t3);
        assert_eq!(
            window.bbr_phase_code(),
            1,
            "the byte queue never drained, so ProbeRtt must stay pinned to the floor",
        );

        // Drain the byte queue to the floor (4 KB <= 8 KB): the hold timer starts now...
        window.outstanding.truncate(1);
        let t4 = t3 + Duration::from_millis(10);
        deliver(&mut window, t4);
        assert_eq!(
            window.bbr_phase_code(),
            1,
            "draining, hold timer just started"
        );

        // ...and only after `probe_rtt_duration` past the drain does ProbeRtt exit.
        let t5 = t4 + Duration::from_millis(250);
        deliver(&mut window, t5);
        assert_eq!(
            window.bbr_phase_code(),
            0,
            "byte queue drained and held, ProbeRtt should exit to ProbeBw",
        );
    }

    #[test]
    fn byte_mode_btlbw_is_bytes_per_sec_and_floor_binds_at_low_bdp() {
        // A 20 KB body served in 10 ms: BtlBw = 2 MB/s, raw round trip 10 ms ⇒ a genuine
        // byte-BDP of 20 KB, ×2 gain = 40 KB, below the 100 KB `min_cwnd_bytes` floor. So
        // the floor is the binding operating window — the low-BDP regime the floor exists
        // for. (Unlike the old size-residual model, this binds because the *real* BDP is
        // small, not because the residual spuriously collapsed to ~0.)
        let cfg = byte_test_config(100_000, 256);
        let mut bbr = BbrState::new(&cfg);
        let t0 = Instant::now();
        let snapshot = DeliverySnapshot {
            delivered: 0,
            delivered_at: t0 - Duration::from_millis(10),
        };
        bbr.record_delivery(t0, Duration::from_millis(10), 1, 20_000, 50, snapshot);
        // BtlBw is denominated in bytes/sec now, not blocks/sec.
        assert_eq!(bbr.btlbw_units_per_sec(), Some(2_000_000.0));
        // The byte floor binds because BDP×gain (40 KB) < floor (100 KB).
        assert_eq!(bbr.effective_cwnd(), 100_000);
    }

    #[test]
    fn byte_bdp_uses_raw_rtt_so_a_fast_carrier_lifts_off_the_floor() {
        // The regression guard for the floor-pin fix. An 800 KB body served in 20 ms:
        // BtlBw = 40 MB/s, raw round trip 20 ms ⇒ byte-BDP 800 KB, ×2 gain = 1.6 MB, well
        // above the 256 KB floor. The cwnd lifts off the floor.
        //
        // The *size residual* of this same delivery collapses to the ε floor (its implied
        // transmission 800 KB / 40 MB/s = 20 ms equals the whole round trip), so the old
        // model would have computed BDP ≈ 0 and pinned the cwnd at 256 KB. Using the raw
        // round trip for the BDP is what keeps a genuinely fast carrier from being
        // under-pipelined.
        let cfg = byte_test_config(256_000, 256);
        let mut bbr = BbrState::new(&cfg);
        let t0 = Instant::now();
        let snapshot = DeliverySnapshot {
            delivered: 0,
            delivered_at: t0 - Duration::from_millis(20),
        };
        bbr.record_delivery(t0, Duration::from_millis(20), 1, 800_000, 50, snapshot);
        assert_eq!(bbr.btlbw_units_per_sec(), Some(40_000_000.0));
        // The residual would have zeroed the BDP; the raw round trip does not.
        assert_eq!(bbr.size_residual_rtprop(0.02, 800_000), 1e-4);
        assert_eq!(bbr.effective_cwnd(), 1_600_000);
    }

    #[test]
    fn byte_residual_rtprop_subtracts_transmission_time() {
        // With an established 1 MB/s BtlBw, a 100 ms round trip that carried 50 KB has a
        // residual RTprop of 100 ms − 50 ms = 50 ms (the fixed-latency component), while a
        // round trip whose implied transmission exceeds it clamps to the positive floor.
        let cfg = byte_test_config(1, 256);
        let mut bbr = BbrState::new(&cfg);
        let now = Instant::now();
        bbr.btlbw_per_sec.observe(now, 1_000_000.0);
        let residual = bbr.size_residual_rtprop(0.1, 50_000);
        assert!(
            (residual - 0.05).abs() < 1e-9,
            "residual should subtract 50 ms of transmission, got {residual}",
        );
        // 200 KB at 1 MB/s implies 200 ms of transmission > the 100 ms round trip: clamp.
        assert_eq!(bbr.size_residual_rtprop(0.1, 200_000), 1e-4);
    }

    #[test]
    fn byte_size_aware_delay_gate_does_not_ratchet_a_big_block() {
        // A long smoothed round-trip that is fully explained by a big block's transmission
        // time must NOT ratchet the delay ceiling under `Bytes` (size-aware expected RT),
        // whereas the identical round trip WOULD ratchet under `Blocks` (RTprop-only).
        let now = Instant::now();

        let bytes_cfg = byte_test_config(1, 256);
        let mut bytes = BbrState::new(&bytes_cfg);
        bytes.btlbw_per_sec.observe(now, 1_000_000.0); // 1 MB/s
                                                       // The delay gate's base is the *residual* RTprop estimator (10 ms base RTT here).
        bytes.rtprop_residual_secs.observe(now, 0.01);
        // 200 ms round trip carrying a 190 KB body: expected ≈ 10 ms + 190 ms = 200 ms.
        bytes.update_delay_cap(0.2, 190_000);
        assert!(
            bytes.delay_cap().is_none(),
            "a big block's honest transfer time must not look like a standing queue",
        );

        let blocks_cfg = bbr_test_config();
        let mut blocks = BbrState::new(&blocks_cfg);
        blocks.rtprop_residual_secs.observe(now, 0.01);
        // Same 200 ms round trip, blocks mode: expected = RTprop (10 ms) → ratchets.
        blocks.update_delay_cap(0.2, 190_000);
        assert!(
            blocks.delay_cap().is_some(),
            "blocks mode treats the inflated round trip as a queue and ratchets down",
        );
    }

    #[test]
    fn floor_bypass_never_exceeds_the_advertised_hard_cap() {
        // cwnd == hard cap (8): the bypass must not push in-flight past what the peer
        // advertised it will service.
        let cfg = ZakuraBlockSyncConfig {
            initial_inflight_requests: 8,
            max_inflight_requests: 8,
            ..bbr_test_config()
        };
        let mut window = DownloadWindow::new(&cfg);
        assert_eq!(window.hard_outbound_capacity(), 8);
        fill_outstanding(&mut window, 8);
        assert_eq!(window.available_slots(), 0);
        assert_eq!(window.available_slots_with_bonus(2), 0);
    }
}
