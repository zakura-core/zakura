//! An instrumented congestion controller (OBS-12).
//!
//! quinn-proto doesn't report bytes in flight, the app-limited flag or acked
//! bytes. The driver installs this wrapper around the configured controller
//! for each connection, so the connection task can read them without a fork.

use std::{
    any::Any,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use quinn_proto::{
    congestion::{Controller, ControllerFactory, ControllerMetrics},
    RttEstimator,
};

/// Values the wrapped controller saw, shared with the connection task.
#[derive(Debug, Default)]
pub(crate) struct CongestionProbe {
    in_flight: AtomicU64,
    app_limited: AtomicBool,
    acked_bytes: AtomicU64,
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
        Box::new(Instrumented {
            inner: self.inner.clone().build(now, current_mtu),
            probe: self.probe.clone(),
        })
    }
}

struct Instrumented {
    inner: Box<dyn Controller>,
    probe: Arc<CongestionProbe>,
}

impl Controller for Instrumented {
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {
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
        self.probe.acked_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.inner.on_ack(now, sent, bytes, app_limited, rtt);
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
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
        })
    }

    fn initial_window(&self) -> u64 {
        self.inner.initial_window()
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self.inner.into_any()
    }
}
