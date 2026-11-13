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

    /// The bytes granted now and the free bytes above the capacity, both 0
    /// once the gate is gone.
    fn read(&self) -> (u64, u64) {
        // While a read runs no task does, so a handle upgraded here is never
        // the last one, and dropping it releases nothing.
        self.bytes.upgrade().map_or((0, 0), |bytes| {
            // Widening usize to u64 is lossless on supported targets.
            let free = bytes.peek_available() as u64;
            (
                self.capacity.saturating_sub(free),
                free.saturating_sub(self.capacity),
            )
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
    /// The raw gate's free bytes above its capacity: bytes released more
    /// often than granted.
    pub raw_excess: u64,
    /// Queued output frame bytes the kind's frames hold now.
    pub output_held: u64,
    /// The output gate's capacity.
    pub output_capacity: u64,
    /// The output gate's free bytes above its capacity.
    pub output_excess: u64,
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
                .map(|kind| {
                    let (raw_held, raw_excess) = kind.raw.read();
                    let (output_held, output_excess) = kind.output.read();
                    KindSample {
                        kind: kind.kind,
                        raw_held,
                        raw_capacity: kind.raw.capacity,
                        raw_excess,
                        output_held,
                        output_capacity: kind.output.capacity,
                        output_excess,
                    }
                })
                .collect(),
        }
    }
}
