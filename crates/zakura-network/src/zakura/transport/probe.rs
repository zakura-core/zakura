//! A sim-only read of one connection's transport resources: its QUIC load
//! snapshot, the receive window it set, and each stream kind's frame gates.
//!
//! It holds each gate's semaphore through std's `Weak`, so it never keeps a
//! gate alive past its connection and its grants, and reads the semaphores
//! through peeks. `zakura_rt`'s `Weak::upgrade` and `available_permits` are
//! preemption points, and a boundary read may take none.

use std::sync::{Arc, Weak};

use zakura_quic::{ConnLoad, LoadProbe};
use zakura_rt::{sim::in_boundary_read, sync::Semaphore};

use crate::zakura::regulation::OutputByteBudget;

/// A weak read of the admission machine and its independent semaphore permits.
#[derive(Clone, Debug)]
pub struct HandshakeProbe {
    counts: Weak<zakura_rt::sync::Mutex<super::HandshakeCounts>>,
    pending: Weak<Semaphore>,
    inbound: Weak<Semaphore>,
    cap: usize,
    address_cap: usize,
}

/// Admission charges and permits sampled at one scheduler boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandshakeSample {
    /// The configured registered-connection limit per address.
    pub address_cap: usize,
    /// The machine's total pending charge.
    pub pending: usize,
    /// The machine's inbound pending charge.
    pub inbound: usize,
    /// Live total pending permits.
    pub pending_permits: usize,
    /// Live inbound pending permits.
    pub inbound_permits: usize,
    /// The total pending capacity.
    pub cap: usize,
    /// Canonical addresses with registered and control-handshake counts.
    pub hosts: Vec<(std::net::IpAddr, usize, usize)>,
}

impl HandshakeProbe {
    pub(crate) fn new(
        counts: &Arc<zakura_rt::sync::Mutex<super::HandshakeCounts>>,
        pending: &Arc<Semaphore>,
        inbound: &Arc<Semaphore>,
        cap: usize,
        address_cap: usize,
    ) -> Self {
        Self {
            counts: Arc::downgrade(counts),
            pending: Arc::downgrade(pending),
            inbound: Arc::downgrade(inbound),
            cap,
            address_cap,
        }
    }

    /// Reads without preemption. A held machine lock means its transition is unfinished.
    pub fn read(&self) -> Option<HandshakeSample> {
        assert!(
            in_boundary_read(),
            "admission probes require a boundary read"
        );
        let counts = self.counts.upgrade()?;
        let pending = self.pending.upgrade()?;
        let inbound = self.inbound.upgrade()?;
        match counts.peek(|counts| HandshakeSample {
            address_cap: self.address_cap,
            pending: counts.pending(),
            inbound: counts.inbound_pending(),
            pending_permits: self.cap - pending.peek_available(),
            inbound_permits: super::inbound_share(self.cap) - inbound.peek_available(),
            cap: self.cap,
            hosts: counts.hosts(),
        }) {
            zakura_rt::sim::Peek::Current(sample) | zakura_rt::sim::Peek::Poisoned(sample) => {
                Some(sample)
            }
            zakura_rt::sim::Peek::Held { .. } => None,
        }
    }
}

/// A read of one connection's transport resources for the simulator's
/// transport taps, which keep it past the connection.
#[derive(Clone, Debug)]
pub struct ConnectionProbe {
    quic: LoadProbe,
    receive_window: u64,
    kinds: Vec<KindProbe>,
}

/// One stream kind's raw and output gates on one connection.
#[derive(Clone, Debug)]
struct KindProbe {
    kind: u16,
    raw: GateProbe,
    output: GateProbe,
}

/// One gate's semaphore and its capacity.
#[derive(Clone, Debug)]
struct GateProbe {
    bytes: Weak<Semaphore>,
    capacity: u64,
}

impl GateProbe {
    fn of(budget: &OutputByteBudget) -> Self {
        GateProbe {
            bytes: Arc::downgrade(budget.semaphore()),
            capacity: budget.capacity(),
        }
    }

    /// The bytes granted now, 0 once the gate is gone.
    fn granted(&self) -> u64 {
        // While a read runs no task does, so a handle upgraded here is never
        // the last one, and dropping it releases nothing.
        self.bytes.upgrade().map_or(0, |bytes| {
            // Widening usize to u64 is lossless on supported targets.
            self.capacity.saturating_sub(bytes.peek_available() as u64)
        })
    }
}

/// One [`ConnectionProbe`] read.
#[derive(Clone, Debug)]
pub struct ConnectionSample {
    /// The QUIC receive window the connection set for the kinds it
    /// negotiated, before credit holds.
    pub receive_window: u64,
    /// The QUIC connection's latest load snapshot, or `None` once the
    /// connection and its handles are gone.
    pub quic: Option<ConnLoad>,
    /// Each negotiated kind's gates.
    pub kinds: Vec<KindSample>,
}

/// One kind's gates in a [`ConnectionSample`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KindSample {
    /// The stream kind.
    pub kind: u16,
    /// Raw inbound frame bytes the kind's frames hold now.
    pub raw_held: u64,
    /// The raw gate's capacity.
    pub raw_capacity: u64,
    /// Queued output frame bytes the kind's frames hold now.
    pub output_held: u64,
    /// The output gate's capacity.
    pub output_capacity: u64,
}

impl ConnectionProbe {
    /// A probe of the connection `quic` reads, whose QUIC receive window is
    /// `receive_window` and whose kinds' gates are `gates`.
    pub(crate) fn new<'g>(
        quic: LoadProbe,
        receive_window: u64,
        gates: impl IntoIterator<Item = (u16, &'g OutputByteBudget, &'g OutputByteBudget)>,
    ) -> Self {
        ConnectionProbe {
            quic,
            receive_window,
            kinds: gates
                .into_iter()
                .map(|(kind, raw, output)| KindProbe {
                    kind,
                    raw: GateProbe::of(raw),
                    output: GateProbe::of(output),
                })
                .collect(),
        }
    }

    /// The connection's resources now, read through peeks, so the read adds
    /// no scheduling point.
    ///
    /// # Panics
    ///
    /// Outside a [`boundary_read`](zakura_rt::sim::boundary_read).
    pub fn read(&self) -> ConnectionSample {
        assert!(
            in_boundary_read(),
            "a connection probe reads only inside a boundary read"
        );
        ConnectionSample {
            receive_window: self.receive_window,
            quic: self.quic.read(),
            kinds: self
                .kinds
                .iter()
                .map(|kind| KindSample {
                    kind: kind.kind,
                    raw_held: kind.raw.granted(),
                    raw_capacity: kind.raw.capacity,
                    output_held: kind.output.granted(),
                    output_capacity: kind.output.capacity,
                })
                .collect(),
        }
    }
}
