//! Session slots for a service, from reservation through the last owner.
//!
//! A session is one admission of a service's layout on one connection: one
//! stream, or several streams admitted together. The transport calls
//! `Service::reserve_session` once per session and shares the reservation with
//! every member, every worker, and every application sender. [`SessionCapacity`]
//! returns that reservation. It holds two kinds of slot:
//!
//! - A **direction slot**, inbound or outbound, from `reserve` until the last
//!   owner drops. A retiring session keeps its slot while its workers drain.
//! - A **setup slot**, from `reserve` until every member is ready
//!   (`SessionResources::admitted`). It bounds layouts whose members have not
//!   all arrived.
//!
//! While outbound sessions are enabled, inbound setup cannot take the last
//! setup slot. Inbound peers that open one member of a layout and withhold the
//! rest therefore cannot block this node's own outbound sessions.
//!
//! Waiting never takes a slot. [`SessionCapacity::demand`] subscribes to
//! capacity changes before it checks, so a release between the check and the
//! wait still wakes the caller.
//!
//! # Properties and their tests
//!
//! | Property | Test |
//! | --- | --- |
//! | Direction and setup slots bound sessions | `operation_sequences_keep_every_count_in_bounds` |
//! | The last setup slot stays with outbound | `inbound_setups_leave_an_outbound_setup_slot` |
//! | A failed reservation holds nothing | `a_failed_setup_returns_inbound_capacity` |
//! | `admitted` releases setup once | `setup_and_retirement_keep_their_session_slot` |
//! | The last owner's drop returns every slot and wakes waiters | `an_abandoned_setup_returns_capacity_and_wakes_demand` |
//! | `available` agrees with `reserve` | `minimal_setup_limits_keep_outbound_and_inbound_only_modes` |

use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};

use crate::zakura::{
    ServicePeerDirection, ServicePeerLimits, SessionDemand, SessionFull, SessionResources,
};

#[cfg(test)]
mod tests;

/// Session slots per connection direction, plus setup slots for layouts whose
/// members have not all arrived.
#[derive(Debug)]
pub(crate) struct SessionCapacity {
    service: &'static str,
    inbound: Arc<Semaphore>,
    outbound: Arc<Semaphore>,
    setup: Arc<Semaphore>,
    /// The setup slots inbound sessions may take: one fewer than `setup` while
    /// outbound sessions are enabled.
    inbound_setup: Arc<Semaphore>,
    changed: watch::Sender<()>,
}

/// Free slots of each kind.
#[cfg(test)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct FreeSlots {
    pub(crate) inbound: usize,
    pub(crate) outbound: usize,
    pub(crate) setup: usize,
    pub(crate) inbound_setup: usize,
}

impl SessionCapacity {
    /// Slots from `limits`. `service` labels the metrics.
    pub(crate) fn new(service: &'static str, limits: &ServicePeerLimits) -> Self {
        let pool = |count: usize| Arc::new(Semaphore::new(count.min(Semaphore::MAX_PERMITS)));
        let setup = limits.max_pending_escalations.min(Semaphore::MAX_PERMITS);
        let inbound_setup = setup.saturating_sub(usize::from(limits.max_outbound_peers > 0));
        Self {
            service,
            inbound: pool(limits.max_inbound_peers),
            outbound: pool(limits.max_outbound_peers),
            setup: pool(setup),
            inbound_setup: pool(inbound_setup),
            changed: watch::channel(()).0,
        }
    }

    fn direction(&self, direction: ServicePeerDirection) -> &Arc<Semaphore> {
        match direction {
            ServicePeerDirection::Inbound => &self.inbound,
            ServicePeerDirection::Outbound => &self.outbound,
        }
    }

    /// Watch for released slots. Subscribe before checking [`Self::available`].
    pub(crate) fn subscribe(&self) -> watch::Receiver<()> {
        self.changed.subscribe()
    }

    /// Whether [`Self::reserve`] would succeed now.
    ///
    /// Another task can take the slots first, so this is only a hint.
    pub(crate) fn available(&self, direction: ServicePeerDirection) -> bool {
        self.direction(direction).available_permits() > 0
            && self.setup.available_permits() > 0
            && (direction == ServicePeerDirection::Outbound
                || self.inbound_setup.available_permits() > 0)
    }

    /// `OpenNow` if a session could be reserved now. Otherwise `WaitForChange`
    /// on a receiver that subscribed before the check, so no release is missed.
    pub(crate) fn demand(&self, direction: ServicePeerDirection) -> SessionDemand {
        let mut changed = self.subscribe();
        if self.available(direction) {
            return SessionDemand::OpenNow;
        }
        SessionDemand::WaitForChange(Box::pin(async move {
            // A closed sender means the service is gone; re-checking then is
            // harmless.
            let _ = changed.changed().await;
        }))
    }

    /// Take one direction slot and one setup slot. Return this from
    /// `Service::reserve_session`.
    ///
    /// Returns [`SessionFull`] at once if either limit is reached, holding no
    /// slot afterwards.
    pub(crate) fn reserve(
        &self,
        direction: ServicePeerDirection,
    ) -> Result<Arc<dyn SessionResources>, SessionFull> {
        let inbound_setup = match direction {
            ServicePeerDirection::Inbound => Some(
                self.inbound_setup
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| SessionFull)?,
            ),
            ServicePeerDirection::Outbound => None,
        };
        let setup = match self.setup.clone().try_acquire_owned() {
            Ok(setup) => SetupSlots {
                _setup: setup,
                _inbound_setup: inbound_setup,
            },
            Err(_) => {
                if inbound_setup.is_some() {
                    // Wake callers that saw the inbound setup slot taken.
                    drop(inbound_setup);
                    self.changed.send_replace(());
                }
                return Err(SessionFull);
            }
        };
        let Ok(session) = self.direction(direction).clone().try_acquire_owned() else {
            // Wake callers that saw the setup slot taken.
            drop(setup);
            self.changed.send_replace(());
            return Err(SessionFull);
        };
        let reserved = metrics::gauge!(
            "zakura.p2p.sessions.reserved",
            "service" => self.service,
            "direction" => direction.trace_label(),
        );
        let setting_up = metrics::gauge!("zakura.p2p.sessions.setup", "service" => self.service);
        reserved.increment(1.0);
        setting_up.increment(1.0);
        Ok(Arc::new(SessionReservation {
            reserved,
            setting_up,
            session: Some(session),
            setup: Mutex::new(Some(setup)),
            changed: self.changed.clone(),
        }))
    }

    /// Free slots of each kind.
    #[cfg(test)]
    pub(crate) fn free(&self) -> FreeSlots {
        FreeSlots {
            inbound: self.inbound.available_permits(),
            outbound: self.outbound.available_permits(),
            setup: self.setup.available_permits(),
            inbound_setup: self.inbound_setup.available_permits(),
        }
    }
}

/// One session's setup slots: the shared one, and the inbound one if the
/// session is inbound.
#[derive(Debug)]
struct SetupSlots {
    _setup: OwnedSemaphorePermit,
    _inbound_setup: Option<OwnedSemaphorePermit>,
}

/// A session's slots, held by every owner of the session.
///
/// The direction slot returns when the last owner drops, even after the
/// session is cancelled.
#[derive(Debug)]
struct SessionReservation {
    reserved: metrics::Gauge,
    setting_up: metrics::Gauge,
    session: Option<OwnedSemaphorePermit>,
    setup: Mutex<Option<SetupSlots>>,
    changed: watch::Sender<()>,
}

impl SessionResources for SessionReservation {
    /// Release the setup slots once every member is ready. Later calls
    /// release nothing.
    fn admitted(&self) {
        if self
            .setup
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .is_some()
        {
            self.setting_up.decrement(1.0);
        }
        self.changed.send_replace(());
    }
}

impl Drop for SessionReservation {
    /// Return the direction slot and any setup slots an incomplete layout
    /// left, then wake waiting callers.
    fn drop(&mut self) {
        if self
            .setup
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .is_some()
        {
            self.setting_up.decrement(1.0);
        }
        self.session.take();
        self.reserved.decrement(1.0);
        self.changed.send_replace(());
    }
}
