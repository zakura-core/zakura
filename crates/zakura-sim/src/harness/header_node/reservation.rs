//! The node under test's header capacity accounting, as view-side evidence.
//!
//! The work queue reports each reservation it opens, binds and ends, each
//! page it stages, each lease that returns, and each round it releases. The reactor tap checks them
//! as they arrive, in its [`HeaderWorkLedger`], which fails the run at the
//! next drain when a rule breaks. It also keeps them here, in the order the
//! queue reported them, for a test or a later property to read. They stay
//! out of the observation log, so pinned Plans and traces keep their format.
//!
//! [`HeaderWorkLedger`]: zakura_network::zakura::observe::HeaderWorkLedger

use std::sync::{Arc, Mutex, PoisonError};

use zakura_network::zakura::observe::ReactorEvent;

/// One reactor's accounting facts, shared by its tap and whoever reads them.
/// Cloning shares the store.
#[derive(Clone, Debug, Default)]
pub struct ReservationEvidence(Arc<Mutex<Vec<ReactorEvent>>>);

impl ReservationEvidence {
    /// Whether this store keeps `event`: a reservation's opening, binding or
    /// end, a staged page, a returned lease, or a round's release.
    pub(super) fn keeps(event: &ReactorEvent) -> bool {
        matches!(
            event,
            ReactorEvent::ReservationOpened { .. }
                | ReactorEvent::ReservationBound { .. }
                | ReactorEvent::ReservationEnded { .. }
                | ReactorEvent::PageStaged { .. }
                | ReactorEvent::LeaseReturned { .. }
                | ReactorEvent::RoundReleased { .. }
        )
    }

    /// Records `event`. Nothing else is called while the lock is held.
    pub(super) fn record(&self, event: &ReactorEvent) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(event.clone());
    }

    /// Every fact so far, in the order the queue reported them.
    pub fn facts(&self) -> Vec<ReactorEvent> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}
