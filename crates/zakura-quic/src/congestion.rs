//! An instrumented congestion controller (OBS-12) and path-rate probe.
//!
//! quinn-proto doesn't report bytes in flight, the app-limited flag, acked
//! bytes, minimum RTT, or outbound delivery rate. The driver installs this
//! wrapper around the configured controller for each connection, so the
//! connection task can read them lock-free without a fork.

use std::{
    any::Any,
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use zakura_rt::{
    sync::{
        atomic::AtomicBool,
        counter::{AtomicU64, Ordering},
    },
    windowed::WindowedSamples,
};

use quinn_proto::{
    congestion::{Controller, ControllerFactory, ControllerMetrics},
    RttEstimator,
};

/// Horizon for windowed-max bandwidth estimates: 10 minutes (~8 Zcash blocks)
/// so estimates survive the 75-second inter-block idle gaps.
const RATE_WINDOW: Duration = Duration::from_secs(600);

/// Maximum in-flight send snapshots retained for delivery-rate attribution.
/// A full deque takes no new snapshots until acks free room.
const MAX_SENT_SAMPLES: usize = 512;

/// Minimum bytes delivered over one delivery-rate sample to avoid tiny-packet
/// noise inflating the estimate.
const MIN_DELIVERY_SAMPLE_BYTES: u64 = 4 * 1024;

/// Minimum elapsed time for one delivery-rate sample, guarding against
/// sub-millisecond timer quantization or batched local ACKs.
const MIN_DELIVERY_SAMPLE_INTERVAL: Duration = Duration::from_micros(500);

/// The windowed max of a rate in bytes/sec over [`RATE_WINDOW`].
///
/// Samples are stamped down to whole seconds since the filter started, so the
/// filter keeps at most one candidate per second of the window, however often
/// a peer's acks or reads produce samples.
#[derive(Clone, Debug)]
pub(crate) struct RateMax {
    start: Instant,
    samples: WindowedSamples,
}

impl RateMax {
    /// An empty filter whose stamps count from `now`.
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            start: now,
            samples: WindowedSamples::new(RATE_WINDOW),
        }
    }

    /// Adds a rate sampled at `now`.
    pub(crate) fn observe(&mut self, now: Instant, bps: u64) {
        let stamp =
            self.start + Duration::from_secs(now.saturating_duration_since(self.start).as_secs());
        // Rates stay far below 2^53 bytes/sec, where `f64` holds every integer.
        self.samples.observe(stamp.into(), bps as f64);
    }

    /// The largest rate sampled within the window before `now`, if any.
    pub(crate) fn get(&self, now: Instant) -> Option<u64> {
        // The filter holds only values converted from `u64` rates.
        self.samples.max(now.into()).map(|bps| bps as u64)
    }
}

/// Values the wrapped controller saw, shared with the connection task.
///
/// The RTT and rate describe the current path. quinn-proto builds a fresh
/// controller when the peer moves to a new path, and building one clears them.
#[derive(Debug, Default)]
pub(crate) struct CongestionProbe {
    in_flight: AtomicU64,
    app_limited: AtomicBool,
    acked_bytes: AtomicU64,
    /// Minimum RTT in nanoseconds, or 0 when unknown.
    min_rtt_ns: AtomicU64,
    /// Windowed-max outbound delivery rate in bytes/sec, or 0 when unknown.
    rate_out_bps: AtomicU64,
}

impl CongestionProbe {
    /// Bytes in flight after the last batch of ACKs.
    pub(crate) fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Whether the connection was application-limited before the last ACKs.
    pub(crate) fn app_limited(&self) -> bool {
        self.app_limited.load(Ordering::Relaxed)
    }

    /// Bytes acknowledged since the connection started.
    pub(crate) fn acked_bytes(&self) -> u64 {
        self.acked_bytes.load(Ordering::Relaxed)
    }

    /// Minimum RTT observed on the current path, if any ACK has arrived.
    pub(crate) fn min_rtt(&self) -> Option<Duration> {
        let ns = self.min_rtt_ns.load(Ordering::Relaxed);
        (ns != 0).then(|| Duration::from_nanos(ns))
    }

    /// Windowed-max outbound delivery rate in bytes/sec, if measured.
    pub(crate) fn rate_out(&self) -> Option<u64> {
        let bps = self.rate_out_bps.load(Ordering::Relaxed);
        (bps != 0).then_some(bps)
    }
}

/// Builds instrumented controllers that all report to one probe.
pub(crate) struct InstrumentedFactory {
    inner: Arc<dyn ControllerFactory + Send + Sync>,
    probe: Arc<CongestionProbe>,
}

impl InstrumentedFactory {
    pub(crate) fn new(
        inner: Arc<dyn ControllerFactory + Send + Sync>,
        probe: Arc<CongestionProbe>,
    ) -> Self {
        Self { inner, probe }
    }
}

impl ControllerFactory for InstrumentedFactory {
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller> {
        Box::new(Instrumented::new(
            self.inner.clone().build(now, current_mtu),
            self.probe.clone(),
            now,
        ))
    }
}

#[derive(Clone, Copy, Debug)]
struct SentSample {
    sent_time: Instant,
    delivered: u64,
    delivered_time: Instant,
}

/// Outbound delivery-rate state: the delivered total, a snapshot of it per
/// send burst, and the windowed max the samples feed.
#[derive(Clone)]
struct DeliveryRate {
    delivered: u64,
    delivered_time: Option<Instant>,
    bytes_in_flight: u64,
    sent_samples: VecDeque<SentSample>,
    rate_out: RateMax,
}

struct Instrumented {
    inner: Box<dyn Controller>,
    probe: Arc<CongestionProbe>,
    rate: DeliveryRate,
}

impl Instrumented {
    /// Wraps `inner`. A new controller means a new path, so this clears the
    /// probe's path estimates.
    fn new(inner: Box<dyn Controller>, probe: Arc<CongestionProbe>, now: Instant) -> Self {
        probe.min_rtt_ns.store(0, Ordering::Relaxed);
        probe.rate_out_bps.store(0, Ordering::Relaxed);
        Self {
            inner,
            probe,
            rate: DeliveryRate {
                delivered: 0,
                delivered_time: None,
                bytes_in_flight: 0,
                sent_samples: VecDeque::new(),
                rate_out: RateMax::new(now),
            },
        }
    }

    fn record_sent(&mut self, now: Instant, bytes: u64) {
        let rate = &mut self.rate;
        if rate.bytes_in_flight == 0 {
            rate.delivered_time = Some(now);
        }
        rate.bytes_in_flight = rate.bytes_in_flight.saturating_add(bytes);
        // When full, keep the old snapshots: an ack then matches an older one,
        // which gives a longer but still valid interval. Dropping the oldest
        // instead would leave every ack on a long, fast path without a match.
        if rate.sent_samples.len() >= MAX_SENT_SAMPLES
            || rate
                .sent_samples
                .back()
                .is_some_and(|back| back.sent_time == now)
        {
            return;
        }
        rate.sent_samples.push_back(SentSample {
            sent_time: now,
            delivered: rate.delivered,
            delivered_time: rate.delivered_time.unwrap_or(now),
        });
    }

    fn record_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        min_rtt: Duration,
    ) {
        // 0 means unknown, so a zero RTT reads as 1 ns.
        let min_ns = u64::try_from(min_rtt.as_nanos()).unwrap_or(u64::MAX).max(1);
        self.probe.min_rtt_ns.store(min_ns, Ordering::Relaxed);
        self.probe.acked_bytes.fetch_add(bytes, Ordering::Relaxed);

        let rate = &mut self.rate;
        rate.delivered = rate.delivered.saturating_add(bytes);
        rate.delivered_time = Some(now);

        // The latest snapshot at or before `sent`. Older ones are pruned, and
        // the match stays for later packets of the same burst.
        while rate
            .sent_samples
            .get(1)
            .is_some_and(|next| next.sent_time <= sent)
        {
            rate.sent_samples.pop_front();
        }
        let snapshot = rate
            .sent_samples
            .front()
            .filter(|front| front.sent_time <= sent);
        if let Some(snap) = snapshot.filter(|_| !app_limited) {
            let delta_bytes = rate.delivered.saturating_sub(snap.delivered);
            let interval = now
                .saturating_duration_since(snap.delivered_time)
                .max(sent.saturating_duration_since(snap.sent_time));
            if delta_bytes >= MIN_DELIVERY_SAMPLE_BYTES && interval >= MIN_DELIVERY_SAMPLE_INTERVAL
            {
                let bps =
                    u128::from(delta_bytes).saturating_mul(1_000_000_000) / interval.as_nanos();
                rate.rate_out
                    .observe(now, u64::try_from(bps).unwrap_or(u64::MAX));
            }
        }
        self.probe
            .rate_out_bps
            .store(rate.rate_out.get(now).unwrap_or(0), Ordering::Relaxed);
    }
}

impl Controller for Instrumented {
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {
        self.record_sent(now, bytes);
        self.inner.on_sent(now, bytes, last_packet_number);
    }

    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &RttEstimator,
    ) {
        self.record_ack(now, sent, bytes, app_limited, rtt.min());
        self.inner.on_ack(now, sent, bytes, app_limited, rtt);
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
        self.rate.bytes_in_flight = in_flight;
        self.probe.in_flight.store(in_flight, Ordering::Relaxed);
        self.probe.app_limited.store(app_limited, Ordering::Relaxed);
        self.inner
            .on_end_acks(now, in_flight, app_limited, largest_packet_num_acked);
    }

    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        lost_bytes: u64,
    ) {
        self.rate.bytes_in_flight = self.rate.bytes_in_flight.saturating_sub(lost_bytes);
        self.inner
            .on_congestion_event(now, sent, is_persistent_congestion, lost_bytes);
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.inner.on_mtu_update(new_mtu);
    }

    fn window(&self) -> u64 {
        self.inner.window()
    }

    fn metrics(&self) -> ControllerMetrics {
        self.inner.metrics()
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(Self {
            inner: self.inner.clone_box(),
            probe: self.probe.clone(),
            rate: self.rate.clone(),
        })
    }

    fn initial_window(&self) -> u64 {
        self.inner.initial_window()
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self.inner.into_any()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quinn_proto::congestion::CubicConfig;

    fn test_controller() -> (Instrumented, Arc<CongestionProbe>, Instant) {
        let now = Instant::now();
        let probe = Arc::new(CongestionProbe::default());
        let inner = Arc::new(CubicConfig::default()).build(now, 1200);
        (Instrumented::new(inner, probe.clone(), now), probe, now)
    }

    /// A peer can time its acks so each rate sample is smaller than the last.
    /// `RateMax` stamps every sample within one second alike, so the filter
    /// keeps one candidate for that second, and all of them expire together.
    #[test]
    fn rate_max_stamps_samples_to_whole_seconds() {
        let t0 = Instant::now();
        let mut max = RateMax::new(t0);
        for ms in 0..1_000u64 {
            max.observe(t0 + Duration::from_millis(ms), 2_000_000 - ms);
        }
        assert_eq!(max.get(t0 + Duration::from_secs(1)), Some(2_000_000));
        // The last sample came 999 ms after the first, but its stamp is t0.
        assert_eq!(max.get(t0 + RATE_WINDOW + Duration::from_millis(500)), None);
    }

    /// A fast, long path keeps more than `MAX_SENT_SAMPLES` sends in
    /// flight. Each ack must still find a send snapshot, so `rate_out` gets a
    /// sample.
    #[test]
    fn more_sends_in_flight_than_samples_still_yield_a_rate() {
        let (mut ctrl, probe, t0) = test_controller();
        let rtt = Duration::from_millis(50);
        let gap = Duration::from_micros(10);
        let in_flight = 2 * MAX_SENT_SAMPLES as u32;
        for i in 0..in_flight {
            ctrl.record_sent(t0 + gap * i, 1_200);
        }
        // Steady state: each ack frees room for one more send.
        for i in 0..in_flight {
            let now = t0 + rtt + gap * i;
            ctrl.record_ack(now, t0 + gap * i, 1_200, false, rtt);
            ctrl.record_sent(now, 1_200);
        }
        assert!(probe.rate_out().is_some());
    }

    #[test]
    fn delivery_rate_tracks_non_app_limited_flow_and_drops_app_limited() {
        let (mut ctrl, probe, t0) = test_controller();
        assert_eq!(probe.min_rtt(), None);
        assert_eq!(probe.rate_out(), None);

        // App-limited burst: 100 KB over 100 ms should be dropped from rate_out,
        // while min_rtt is still recorded.
        let rtt = Duration::from_millis(20);
        ctrl.record_sent(t0, 100_000);
        ctrl.record_ack(t0 + Duration::from_millis(100), t0, 100_000, true, rtt);
        ctrl.rate.bytes_in_flight = 0;
        assert_eq!(probe.min_rtt(), Some(rtt));
        assert_eq!(probe.rate_out(), None);

        // Non-app-limited flight after a 75 s idle gap: 200 KB over 100 ms = 2 MB/s.
        let t1 = t0 + Duration::from_secs(75);
        ctrl.record_sent(t1, 200_000);
        ctrl.record_ack(t1 + Duration::from_millis(100), t1, 200_000, false, rtt);
        ctrl.rate.bytes_in_flight = 0;
        assert_eq!(probe.rate_out(), Some(2_000_000));

        // A slower app-limited trickle later does not overwrite or distort the peak.
        let t2 = t1 + Duration::from_secs(10);
        ctrl.record_sent(t2, 10_000);
        ctrl.record_ack(t2 + Duration::from_millis(100), t2, 10_000, true, rtt);
        assert_eq!(probe.rate_out(), Some(2_000_000));

        // quinn-proto builds a new controller for a new path, which resets
        // both min_rtt and rate_out.
        let t3 = t2 + Duration::from_secs(1);
        let inner = Arc::new(CubicConfig::default()).build(t3, 1200);
        let mut ctrl = Instrumented::new(inner, probe.clone(), t3);
        assert_eq!(probe.min_rtt(), None);
        assert_eq!(probe.rate_out(), None);
        ctrl.record_sent(t3, 50_000);
        ctrl.record_ack(
            t3 + Duration::from_millis(100),
            t3,
            50_000,
            false,
            Duration::from_millis(40),
        );
        assert_eq!(probe.min_rtt(), Some(Duration::from_millis(40)));
        assert_eq!(probe.rate_out(), Some(500_000));
    }
}
