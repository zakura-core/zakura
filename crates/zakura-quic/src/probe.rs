//! A sim-only read of one connection's latest load snapshot (API-9).
//!
//! It holds the connection's shared state through std's `Weak`, so it never
//! keeps the connection open, and it reads the snapshot through the watch's
//! peek. `zakura_rt`'s `Weak::upgrade` and the watch's `borrow` are both
//! preemption points, and a boundary read may take none.

use std::sync::{Arc, Weak};

use zakura_rt::sim::in_boundary_read;

use crate::{conn::ConnLoad, driver::ConnShared};

/// A read of one connection's latest [`ConnLoad`] for the simulator's
/// transport taps, which keep it past the connection.
#[derive(Clone, Debug)]
pub struct LoadProbe(Weak<ConnShared>);

impl LoadProbe {
    /// A probe of the connection that shares `shared`.
    pub(crate) fn of(shared: &Arc<ConnShared>) -> Self {
        LoadProbe(Arc::downgrade(shared))
    }

    /// The connection task's latest snapshot, which lags by at most
    /// [`LOAD_INTERVAL`](crate::LOAD_INTERVAL) while the connection is
    /// active. `None` once every handle and the task are gone.
    ///
    /// # Panics
    ///
    /// Outside a [`boundary_read`](zakura_rt::sim::boundary_read).
    pub fn read(&self) -> Option<ConnLoad> {
        assert!(
            in_boundary_read(),
            "a load probe reads only inside a boundary read"
        );
        // While a read runs no task does, so a handle upgraded here is never
        // the last one, and dropping it releases nothing.
        let shared = self.0.upgrade()?;
        let load = shared.load.peek(|load| *load);
        Some(load)
    }
}
